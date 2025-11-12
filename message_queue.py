"""
消息队列实现
支持高并发、自动阈值补给、任务不阻塞执行
"""
import asyncio
import threading
from typing import Callable, Any, Optional, List, Dict
from queue import Queue, Empty
from collections import deque
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MessageQueue:
    """消息队列类"""
    
    def __init__(
        self,
        max_concurrent: int = 1000,
        threshold_callback: Optional[Callable] = None,
        task_callback: Optional[Callable] = None
    ):
        """
        初始化消息队列
        
        Args:
            max_concurrent: 最大并发数（每一时刻执行的任务数）
            threshold_callback: 阈值补给回调函数，当任务完成且队列为空时调用
                                函数签名: async def threshold_callback() -> List[Any] 或 def threshold_callback() -> List[Any]
                                返回任务列表，如果返回空列表或 None 表示没有更多任务
            task_callback: 任务执行回调函数，每个任务会调用此函数
                          函数签名: async def task_callback(task: Any) 或 def task_callback(task: Any)
        """
        self.max_concurrent = max_concurrent
        self.threshold_callback = threshold_callback
        self.task_callback = task_callback
        
        # 任务队列
        self.task_queue = asyncio.Queue()
        
        # 当前正在执行的任务数
        self.running_tasks = 0
        self.running_tasks_lock = asyncio.Lock()
        
        # 队列状态
        self.is_running = False
        self.is_stopped = False
        self.stop_event = asyncio.Event()
        
        # 阈值回调锁（确保阈值回调排队执行）
        self.threshold_lock = asyncio.Lock()
        self.threshold_calling = False
        
        # 统计信息
        self.total_tasks = 0
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.stats_lock = asyncio.Lock()
        
        # 工作协程列表
        self.worker_tasks = []
        
        # 事件循环
        self.loop = None
        self.loop_thread = None
    
    async def _execute_task(self, task: Any):
        """
        执行单个任务
        
        Args:
            task: 要执行的任务
        """
        async with self.running_tasks_lock:
            self.running_tasks += 1
        
        try:
            # 执行任务回调
            if self.task_callback:
                if asyncio.iscoroutinefunction(self.task_callback):
                    await self.task_callback(task)
                else:
                    # 如果是同步函数，在线程池中执行
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self.task_callback, task)
            
            # 更新统计
            async with self.stats_lock:
                self.completed_tasks += 1
            
            logger.debug(f"任务执行完成: {task}")
            
        except Exception as e:
            async with self.stats_lock:
                self.failed_tasks += 1
            logger.error(f"任务执行失败: {task}, 错误: {e}")
        finally:
            async with self.running_tasks_lock:
                self.running_tasks -= 1
    
    async def _threshold_replenish(self):
        """
        阈值补给（排队执行）
        """
        # 使用锁确保阈值回调排队执行
        async with self.threshold_lock:
            if self.threshold_calling:
                logger.debug("阈值回调正在执行，跳过本次调用")
                return []
            
            self.threshold_calling = True
        
        try:
            if not self.threshold_callback:
                return []
            
            logger.info("调用阈值补给回调...")
            
            # 调用阈值回调
            if asyncio.iscoroutinefunction(self.threshold_callback):
                new_tasks = await self.threshold_callback()
            else:
                # 如果是同步函数，在线程池中执行
                loop = asyncio.get_event_loop()
                new_tasks = await loop.run_in_executor(None, self.threshold_callback)
            
            if new_tasks:
                logger.info(f"阈值补给获取到 {len(new_tasks)} 个新任务")
                return new_tasks
            else:
                logger.info("阈值补给返回空，没有更多任务")
                return []
                
        except Exception as e:
            logger.error(f"阈值补给回调执行失败: {e}")
            return []
        finally:
            self.threshold_calling = False
    
    async def _worker(self, worker_id: int):
        """
        工作协程（不断从队列中取任务执行）
        
        Args:
            worker_id: 工作协程 ID
        """
        logger.debug(f"工作协程 {worker_id} 启动")
        
        while self.is_running:
            try:
                # 从队列中获取任务（超时 1 秒，避免阻塞）
                try:
                    task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    # 超时，检查是否需要停止
                    if self.is_stopped and self.task_queue.empty():
                        break
                    continue
                
                # 执行任务（不阻塞）
                asyncio.create_task(self._execute_task(task))
                
                # 标记任务完成
                self.task_queue.task_done()
                
            except Exception as e:
                logger.error(f"工作协程 {worker_id} 出错: {e}")
        
        logger.debug(f"工作协程 {worker_id} 停止")
    
    async def _maintain_concurrency(self):
        """
        维护并发数：确保始终有 max_concurrent 个任务在执行
        阈值回调条件：队列中任务数 < 2 * max_concurrent
        """
        logger.info(f"开始维护并发数: {self.max_concurrent}, 阈值: {2 * self.max_concurrent}")
        
        threshold_size = 2 * self.max_concurrent
        
        while self.is_running:
            try:
                async with self.running_tasks_lock:
                    current_running = self.running_tasks
                    queue_size = self.task_queue.qsize()
                
                # 阈值回调条件：队列中任务数 < 2 * max_concurrent
                if queue_size < threshold_size and not self.threshold_calling and not self.is_stopped:
                    # 调用阈值补给
                    new_tasks = await self._threshold_replenish()
                    
                    if new_tasks:
                        # 将新任务加入队列
                        for task in new_tasks:
                            await self.task_queue.put(task)
                            async with self.stats_lock:
                                self.total_tasks += 1
                    else:
                        # 没有更多任务，检查是否可以停止
                        # 如果队列为空且没有正在执行的任务，则停止
                        if queue_size == 0 and current_running == 0:
                            logger.info("没有更多任务且没有正在执行的任务，准备停止队列")
                            self.is_stopped = True
                            self.stop_event.set()
                            break
                
                # 检查是否可以停止（队列为空且没有正在执行的任务）
                if queue_size == 0 and current_running == 0 and not self.is_stopped:
                    # 如果阈值回调不在执行，再次尝试获取任务
                    if not self.threshold_calling:
                        new_tasks = await self._threshold_replenish()
                        
                        if new_tasks:
                            for task in new_tasks:
                                await self.task_queue.put(task)
                                async with self.stats_lock:
                                    self.total_tasks += 1
                        else:
                            logger.info("没有更多任务，停止队列")
                            self.is_stopped = True
                            self.stop_event.set()
                            break
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"维护并发数出错: {e}")
                await asyncio.sleep(0.1)
    
    async def _run(self):
        """运行队列（内部方法）"""
        logger.info("消息队列开始运行")
        self.is_running = True
        self.is_stopped = False
        
        # 创建工作协程
        self.worker_tasks = [
            asyncio.create_task(self._worker(i))
            for i in range(self.max_concurrent)
        ]
        
        # 创建维护并发数的协程
        maintain_task = asyncio.create_task(self._maintain_concurrency())
        
        # 等待停止事件
        await self.stop_event.wait()
        
        # 等待所有工作协程完成
        logger.info("等待所有工作协程完成...")
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        maintain_task.cancel()
        
        # 等待队列中剩余任务完成
        await self.task_queue.join()
        
        logger.info("消息队列停止")
    
    def start(self):
        """启动队列（同步方法）"""
        if self.is_running:
            logger.warning("队列已经在运行")
            return
        
        # 在新线程中运行事件循环
        def run_loop():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._run())
        
        self.loop_thread = threading.Thread(target=run_loop, daemon=True)
        self.loop_thread.start()
        logger.info("消息队列已启动")
    
    def stop(self):
        """停止队列（同步方法）"""
        if not self.is_running:
            logger.warning("队列未运行")
            return
        
        logger.info("正在停止消息队列...")
        self.is_stopped = True
        self.stop_event.set()
        
        # 等待线程结束
        if self.loop_thread and self.loop_thread.is_alive():
            self.loop_thread.join(timeout=10)
        
        self.is_running = False
        logger.info("消息队列已停止")
    
    def add_task(self, task: Any):
        """
        添加任务到队列（同步方法）
        
        Args:
            task: 要添加的任务
        """
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(self.task_queue.put(task), self.loop)
            self.total_tasks += 1
        else:
            logger.warning("队列未运行，无法添加任务")
    
    def add_tasks(self, tasks: List[Any]):
        """
        批量添加任务到队列（同步方法）
        
        Args:
            tasks: 要添加的任务列表
        """
        if self.loop and self.loop.is_running():
            for task in tasks:
                asyncio.run_coroutine_threadsafe(self.task_queue.put(task), self.loop)
            self.total_tasks += len(tasks)
        else:
            logger.warning("队列未运行，无法添加任务")
    
    async def add_task_async(self, task: Any):
        """
        添加任务到队列（异步方法）
        
        Args:
            task: 要添加的任务
        """
        await self.task_queue.put(task)
        async with self.stats_lock:
            self.total_tasks += 1
    
    async def add_tasks_async(self, tasks: List[Any]):
        """
        批量添加任务到队列（异步方法）
        
        Args:
            tasks: 要添加的任务列表
        """
        for task in tasks:
            await self.task_queue.put(task)
        async with self.stats_lock:
            self.total_tasks += len(tasks)
    
    def wait(self, timeout: Optional[float] = None):
        """
        等待队列停止（同步方法）
        
        Args:
            timeout: 超时时间（秒），None 表示无限等待
        """
        if self.loop_thread and self.loop_thread.is_alive():
            self.loop_thread.join(timeout=timeout)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息（同步方法）
        
        Returns:
            统计信息字典
        """
        if self.loop and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self._get_stats_async(), self.loop)
            return future.result()
        else:
            return {
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "running_tasks": 0,
                "queue_size": 0,
                "is_running": self.is_running
            }
    
    async def _get_stats_async(self) -> Dict[str, Any]:
        """获取统计信息（异步方法）"""
        async with self.running_tasks_lock:
            running = self.running_tasks
        async with self.stats_lock:
            return {
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "running_tasks": running,
                "queue_size": self.task_queue.qsize(),
                "is_running": self.is_running,
                "is_stopped": self.is_stopped
            }

