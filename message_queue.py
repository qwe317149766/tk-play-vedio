"""
消息队列实现
支持高并发、自动阈值补给、任务不阻塞执行
"""
import asyncio
import threading
import time
from typing import Callable, Any, Optional, List, Dict
from queue import Queue, Empty
from collections import deque
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# 设置 message_queue 日志级别为 INFO，关键信息使用 INFO 级别
logger.setLevel(logging.INFO)


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
        
        # 并发控制信号量（确保实际并发数不超过 max_concurrent）
        self.concurrency_semaphore = asyncio.Semaphore(max_concurrent)
        
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
        执行单个任务（完全异步，不阻塞，但受并发控制）
        
        Args:
            task: 要执行的任务
        """
        # 获取并发信号量（控制实际并发数）
        # 如果已达到并发上限，这里会等待直到有任务完成
        # 注意：在获取信号量之前，running_tasks 可能不准确，因为其他任务可能正在执行
        async with self.concurrency_semaphore:
            # 获取到信号量后，更新运行任务数
            async with self.running_tasks_lock:
                self.running_tasks += 1
                current_running = self.running_tasks
            logger.info(f"任务 {task} 获取到信号量，开始执行（当前并发: {current_running}/{self.max_concurrent}）")
            
            try:
                # 执行任务回调（完全异步，不阻塞其他任务）
                if self.task_callback:
                    logger.debug(f"调用任务回调: {task}")
                    if asyncio.iscoroutinefunction(self.task_callback):
                        # 异步函数，直接 await（不会阻塞其他任务，因为每个任务都是独立的协程）
                        await self.task_callback(task)
                    else:
                        # 如果是同步函数，在线程池中执行（不阻塞事件循环）
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, self.task_callback, task)
                    logger.debug(f"任务回调执行完成: {task}")
                else:
                    logger.warning(f"没有任务回调函数，任务 {task} 未执行")
                
                # 更新统计
                async with self.stats_lock:
                    self.completed_tasks += 1
                
                logger.info(f"任务执行完成: {task}")
                
            except Exception as e:
                async with self.stats_lock:
                    self.failed_tasks += 1
                logger.error(f"任务执行失败: {task}, 错误: {e}", exc_info=True)
            finally:
                async with self.running_tasks_lock:
                    self.running_tasks -= 1
                # 标记任务完成（在任务真正执行完成后）
                self.task_queue.task_done()
                logger.info(f"任务 {task} 释放信号量（当前并发: {self.running_tasks}/{self.max_concurrent}）")
    
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
            
            # 调用阈值回调（添加超时保护，避免长时间阻塞）
            try:
                if asyncio.iscoroutinefunction(self.threshold_callback):
                    new_tasks = await asyncio.wait_for(self.threshold_callback(), timeout=2.0)
                else:
                    # 如果是同步函数，直接调用（通常很快，不需要在线程池中执行）
                    # 如果回调函数可能阻塞，可以使用 run_in_executor
                    # 但为了性能，先尝试直接调用
                    try:
                        new_tasks = self.threshold_callback()
                    except Exception:
                        # 如果直接调用失败，在线程池中执行
                        loop = asyncio.get_event_loop()
                        new_tasks = await asyncio.wait_for(
                            loop.run_in_executor(None, self.threshold_callback),
                            timeout=2.0
                        )
            except asyncio.TimeoutError:
                logger.error("阈值补给回调执行超时（2秒），可能回调函数执行时间过长")
                return []
            
            if new_tasks:
                logger.info(f"阈值补给获取到 {len(new_tasks)} 个新任务")
                return new_tasks
            else:
                logger.info("阈值补给返回空，没有更多任务")
                return []
                
        except Exception as e:
            logger.error(f"阈值补给回调执行失败: {e}", exc_info=True)
            return []
        finally:
            # 在锁外清除标志，避免死锁
            async with self.threshold_lock:
                self.threshold_calling = False
    
    async def _worker(self, worker_id: int):
        """
        工作协程（不断从队列中取任务执行，完全异步，不阻塞）
        注意：worker 数量可以大于 max_concurrent，但实际并发由信号量控制
        
        Args:
            worker_id: 工作协程 ID
        """
        logger.debug(f"工作协程 {worker_id} 启动")
        
        while self.is_running:
            try:
                # 从队列中获取任务（超时 1 秒，避免阻塞）
                try:
                    task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                    logger.debug(f"工作协程 {worker_id} 获取到任务: {task}")
                except asyncio.TimeoutError:
                    # 超时，检查是否需要停止
                    if self.is_stopped and self.task_queue.empty():
                        logger.debug(f"工作协程 {worker_id} 准备停止（队列为空且已停止）")
                        break
                    continue
                
                # 创建异步任务执行（完全异步，不阻塞 worker）
                # 任务完成后会在 _execute_task 中调用 task_done()
                # 并发控制由 _execute_task 中的信号量实现
                # 注意：任务会立即创建，但实际执行受信号量控制
                logger.debug(f"工作协程 {worker_id} 创建任务执行: {task} (等待信号量)")
                # 创建任务并立即调度（不等待）
                asyncio.create_task(self._execute_task(task))
                
                # 注意：不在这里调用 task_done()，因为任务可能还在执行
                # task_done() 会在 _execute_task 的 finally 块中调用
                
            except Exception as e:
                logger.error(f"工作协程 {worker_id} 出错: {e}", exc_info=True)
        
        logger.debug(f"工作协程 {worker_id} 停止")
    
    async def _maintain_concurrency(self):
        """
        维护并发数：确保始终有 max_concurrent 个任务在执行
        阈值回调条件：队列中任务数 < 2 * max_concurrent
        """
        logger.info(f"开始维护并发数: {self.max_concurrent}, 阈值: {2 * self.max_concurrent}")
        
        threshold_size = 2 * self.max_concurrent
        last_replenish_time = 0
        replenish_cooldown = 2.0  # 阈值补给冷却时间（秒），避免频繁调用（增加到2秒）
        last_queue_size = threshold_size  # 记录上次队列大小，用于判断是否需要补充
        
        while self.is_running:
            try:
                async with self.running_tasks_lock:
                    current_running = self.running_tasks
                    queue_size = self.task_queue.qsize()
                
                current_time = time.time()
                
                # 阈值回调条件：队列中任务数 < 2 * max_concurrent
                # 改进逻辑：只有当队列大小明显下降时才补充（避免频繁回调）
                # 条件1：队列大小小于阈值
                # 条件2：队列大小比上次检查时明显减少（至少减少10%或减少100个任务）
                #        或者队列大小很小（小于 max_concurrent）且没有任务在执行（说明任务执行很快）
                # 条件3：冷却时间已过
                # 条件4：阈值回调不在执行
                queue_decreased = (last_queue_size - queue_size) >= max(100, last_queue_size * 0.1)
                # 只有当队列很小且没有任务在执行时，才认为需要立即补充
                # 如果队列很小但有任务在执行，说明任务执行很快，不需要立即补充
                queue_too_small = queue_size < self.max_concurrent and current_running == 0
                
                should_replenish = (
                    queue_size < threshold_size and 
                    (queue_decreased or queue_too_small) and  # 队列明显减少或太小且没有任务在执行
                    not self.threshold_calling and 
                    not self.is_stopped and
                    current_time - last_replenish_time >= replenish_cooldown
                )
                
                if should_replenish:
                    
                    # 调用阈值补给
                    logger.info(f"触发阈值补给: 队列大小={queue_size}, 上次队列大小={last_queue_size}, 正在执行={current_running}")
                    new_tasks = await self._threshold_replenish()
                    last_replenish_time = current_time
                    # 更新上次队列大小（只有在成功补充任务时才更新）
                    if new_tasks:
                        last_queue_size = queue_size + len(new_tasks)
                    else:
                        # 没有新任务，保持当前队列大小
                        last_queue_size = queue_size
                    
                    if new_tasks:
                        # 将新任务加入队列
                        logger.info(f"准备将 {len(new_tasks)} 个任务加入队列")
                        for task in new_tasks:
                            await self.task_queue.put(task)
                            async with self.stats_lock:
                                self.total_tasks += 1
                        queue_size_after = self.task_queue.qsize()
                        logger.info(f"已补充 {len(new_tasks)} 个任务，当前队列大小: {queue_size_after}")
                    else:
                        # 没有更多任务，检查是否可以停止
                        # 如果队列为空且没有正在执行的任务，则停止
                        if queue_size == 0 and current_running == 0:
                            logger.info("没有更多任务且没有正在执行的任务，准备停止队列")
                            self.is_stopped = True
                            self.stop_event.set()
                            break
                else:
                    # 更新上次队列大小（只有在队列大小变化时才更新，避免在队列大小不变时一直触发补给）
                    if queue_size != last_queue_size:
                        last_queue_size = queue_size
                
                # 检查是否可以停止（队列为空且没有正在执行的任务）
                if queue_size == 0 and current_running == 0 and not self.is_stopped:
                    # 如果阈值回调不在执行，再次尝试获取任务（但要有冷却时间）
                    if not self.threshold_calling and current_time - last_replenish_time >= replenish_cooldown:
                        new_tasks = await self._threshold_replenish()
                        last_replenish_time = current_time
                        last_queue_size = len(new_tasks) if new_tasks else 0
                        
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
                
                await asyncio.sleep(0.2)  # 增加检查间隔，从0.1秒增加到0.2秒
                
            except Exception as e:
                logger.error(f"维护并发数出错: {e}", exc_info=True)
                await asyncio.sleep(0.2)
    
    async def _run(self):
        """运行队列（内部方法）"""
        logger.info("消息队列开始运行")
        self.is_running = True
        self.is_stopped = False
        
        # 创建工作协程
        # 注意：worker 数量可以设置为 max_concurrent，但实际并发由信号量控制
        # 如果 worker 数量少于 max_concurrent，可能无法充分利用并发
        # 如果 worker 数量大于 max_concurrent，可以更快地从队列获取任务，但实际执行受信号量限制
        worker_count = self.max_concurrent  # 使用 max_concurrent 个 worker
        logger.info(f"创建 {worker_count} 个工作协程（实际并发数由信号量控制为 {self.max_concurrent}）")
        self.worker_tasks = [
            asyncio.create_task(self._worker(i))
            for i in range(worker_count)
        ]
        logger.info(f"已创建 {len(self.worker_tasks)} 个工作协程")
        
        # 创建维护并发数的协程
        maintain_task = asyncio.create_task(self._maintain_concurrency())
        logger.info("维护并发数协程已创建")
        
        # 等待停止事件
        await self.stop_event.wait()
        
        logger.info("收到停止信号，开始关闭队列...")
        
        # 等待所有工作协程完成（给它们时间处理完当前任务）
        logger.info("等待所有工作协程完成...")
        try:
            # 等待最多30秒
            await asyncio.wait_for(
                asyncio.gather(*self.worker_tasks, return_exceptions=True),
                timeout=30.0
            )
        except asyncio.TimeoutError:
            logger.warning("等待工作协程超时，强制取消...")
            for task in self.worker_tasks:
                task.cancel()
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        
        # 取消维护并发数的协程
        maintain_task.cancel()
        try:
            await maintain_task
        except asyncio.CancelledError:
            pass
        
        # 等待队列中剩余任务完成（最多等待10秒）
        logger.info("等待队列中剩余任务完成...")
        try:
            await asyncio.wait_for(self.task_queue.join(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("等待队列任务完成超时，强制停止")
        
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

