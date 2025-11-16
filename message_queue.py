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
        task_callback: Optional[Callable] = None,
        task_timeout: float = 600.0
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
            task_timeout: 任务超时时间（秒），超过此时间会警告，默认 600 秒（10 分钟）
        """
        self.max_concurrent = max_concurrent
        self.threshold_callback = threshold_callback
        self.task_callback = task_callback
        self.task_timeout = task_timeout
        
        # 任务队列
        self.task_queue = asyncio.Queue()
        
        # 并发控制信号量（确保实际并发数不超过 max_concurrent）
        self.concurrency_semaphore = asyncio.Semaphore(max_concurrent)
        
        # 当前正在执行的任务数
        self.running_tasks = 0
        self.running_tasks_lock = asyncio.Lock()
        
        # 任务执行时间跟踪（用于检测阻塞任务）
        # key: worker_id, value: (task_start_time, task_timeout)
        self.task_execution_times = {}
        self.task_execution_times_lock = asyncio.Lock()
        
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
    
    async def _trigger_replenish_check(self):
        """
        触发一次补充检查（任务完成后立即调用，确保及时补充）
        只要队列大小 < 阈值，就立即补充，不等待所有任务完成
        但需要避免重复补充（通过检查阈值回调状态）
        """
        try:
            async with self.running_tasks_lock:
                current_running = self.running_tasks
                queue_size = self.task_queue.qsize()
            
            threshold_size = 3 * self.max_concurrent  # 与 _maintain_concurrency 保持一致
            critical_size = int(self.max_concurrent * 1.5)  # 关键阈值（整数）
            
            # 只要队列大小小于阈值，就立即触发补充（不检查 running_tasks）
            # 但需要检查阈值回调是否正在执行，避免重复补充
            # 如果队列小于关键阈值，立即补充（不等待）
            if (queue_size < threshold_size and 
                not self.threshold_calling and 
                not self.is_stopped):
                new_tasks = await self._threshold_replenish()
                if new_tasks:
                    # 批量添加任务到队列
                    # 注意：每个 put 操作都会唤醒一个等待的 get 操作
                    # 所以如果有多个 worker 在等待，它们会依次被唤醒
                    for task in new_tasks:
                        await self.task_queue.put(task)
                        async with self.stats_lock:
                            self.total_tasks += 1
        except Exception as e:
            logger.error(f"任务完成后触发补充检查异常: {e}", exc_info=True)
    
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
                return new_tasks
            else:
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
        注意：worker 会先获取信号量，然后从队列取任务执行，确保实际执行的任务数不超过 max_concurrent
        这样可以避免队列被快速消耗，但任务都在等待信号量
        
        Args:
            worker_id: 工作协程 ID
        """
        while self.is_running:
            try:
                # 先获取信号量（控制实际并发数）
                async with self.concurrency_semaphore:
                    # 从队列中获取任务
                    try:
                        task = await self.task_queue.get()
                    except Exception as e:
                        if self.is_stopped:
                            break
                        logger.debug(f"Worker {worker_id} 获取任务异常: {e}")
                        continue
                    
                    # 更新运行任务数
                    self.running_tasks += 1
                    
                    # 执行任务回调
                    task_start_time = time.time()
                    task_succeeded = False
                    task_failed = False
                    
                    # 记录任务开始时间（用于检测阻塞任务）
                    async with self.task_execution_times_lock:
                        self.task_execution_times[worker_id] = (task_start_time, self.task_timeout)
                    
                    try:
                        if self.task_callback:
                            if asyncio.iscoroutinefunction(self.task_callback):
                                await self.task_callback(task)
                            else:
                                loop = asyncio.get_event_loop()
                                await loop.run_in_executor(None, self.task_callback, task)
                            task_succeeded = True
                        else:
                            logger.warning(f"Worker {worker_id} 没有任务回调函数")
                            task_succeeded = True
                        
                    except Exception as e:
                        task_elapsed = time.time() - task_start_time
                        task_failed = True
                        logger.error(f"Worker {worker_id} 任务执行异常: {e}", exc_info=True)
                    finally:
                        # 清除任务执行时间记录
                        async with self.task_execution_times_lock:
                            self.task_execution_times.pop(worker_id, None)
                        
                        # 更新运行任务数（在任务执行完成后立即减少，直接更新，不使用锁，避免死锁）
                        try:
                            self.running_tasks -= 1
                            current_running_after = self.running_tasks
                        except Exception as e:
                            logger.error(f"工作协程 {worker_id} 更新 running_tasks 失败: {e}", exc_info=True)
                            try:
                                self.running_tasks -= 1
                            except:
                                pass
                        
                        # 更新统计（无论成功还是失败，任务都算完成）- 在 finally 块中确保一定会执行
                        # 直接更新，不使用锁（避免死锁和超时问题）
                        # 虽然可能不是完全线程安全的，但在高并发场景下，性能更重要
                        try:
                            self.completed_tasks += 1
                            if task_failed:
                                self.failed_tasks += 1
                        except Exception as stats_error:
                            logger.error(f"工作协程 {worker_id} 更新统计信息失败: {stats_error}", exc_info=True)
                        
                        # 标记任务完成（在任务真正执行完成后）
                        try:
                            self.task_queue.task_done()
                        except Exception as e:
                            logger.error(f"工作协程 {worker_id} 调用 task_done() 失败: {e}", exc_info=True)
                    
                    # 任务完成后，立即检查并补充任务（在 finally 块外，可以使用 break）
                    queue_size_after = self.task_queue.qsize()
                    threshold_size = 3 * self.max_concurrent  # 与 _maintain_concurrency 保持一致
                    critical_size = int(self.max_concurrent * 1.5)  # 关键阈值：如果队列小于此值，立即补充（整数）
                    
                    # 更积极的补充策略：如果队列小于关键阈值，立即触发补充（不等待冷却时间）
                    # 这样可以确保队列中始终有足够的任务，所有 worker 都能立即获取任务
                    if queue_size_after < critical_size and not self.threshold_calling and not self.is_stopped:
                        try:
                            # 使用 create_task 创建异步任务，但不等待（不阻塞）
                            asyncio.create_task(self._trigger_replenish_check())
                        except Exception as e:
                            logger.error(f"工作协程 {worker_id} 任务完成后创建补充检查任务失败: {e}", exc_info=True)
                    elif queue_size_after < threshold_size and not self.threshold_calling and not self.is_stopped:
                        try:
                            asyncio.create_task(self._trigger_replenish_check())
                        except Exception as e:
                            logger.error(f"工作协程 {worker_id} 任务完成后创建补充检查任务失败: {e}", exc_info=True)
                
            except Exception as e:
                logger.error(f"Worker {worker_id} 循环异常: {e}", exc_info=True)
                await asyncio.sleep(0.1)
    
    async def _maintain_concurrency(self):
        """
        维护并发数：监控队列状态（不再主动触发阈值回调，阈值回调只在任务完成后触发）
        同时检测被阻塞的任务
        """
        logger.info(f"开始维护并发数监控: {self.max_concurrent}")
        
        while self.is_running:
            try:
                async with self.running_tasks_lock:
                    current_running = self.running_tasks
                    queue_size = self.task_queue.qsize()
                
                # 检测被阻塞的任务（只在超过超时阈值时警告，避免噪音）
                current_time = time.time()
                async with self.task_execution_times_lock:
                    blocked_tasks = []
                    for worker_id, (start_time, timeout) in list(self.task_execution_times.items()):
                        elapsed = current_time - start_time
                        # 只在超过超时阈值时才认为是阻塞（不再使用 80% 预警）
                        if elapsed > timeout:
                            blocked_tasks.append((worker_id, elapsed))
                    
                    # 只有当超过 50% 的任务都超时时才警告（减少日志噪音）
                    if blocked_tasks and len(blocked_tasks) >= current_running * 0.5:
                        logger.warning(f"检测到 {len(blocked_tasks)}/{current_running} 个任务执行超时（超过 {self.task_timeout}秒）")
                        # 如果所有运行中的任务都被阻塞，强制重置 running_tasks（紧急恢复）
                        if len(blocked_tasks) >= current_running and current_running > 0:
                            logger.error(f"所有 {current_running} 个运行中的任务都超时，强制重置 running_tasks 计数")
                            self.running_tasks = 0
                            # 清除所有任务执行时间记录
                            self.task_execution_times.clear()
                
                # 检测信号量死锁：如果队列中有任务，但 running_tasks 达到上限，且没有任务在执行（没有执行时间记录）
                # 说明信号量被占用但没有实际任务在运行
                if queue_size > 0 and current_running >= self.max_concurrent:
                    async with self.task_execution_times_lock:
                        active_tasks_count = len(self.task_execution_times)
                        # 如果实际执行的任务数明显少于 running_tasks，说明有信号量死锁
                        if active_tasks_count < current_running:
                            logger.warning(f"检测到信号量死锁：队列中有 {queue_size} 个任务，running_tasks={current_running}，但只有 {active_tasks_count} 个任务在执行")
                            logger.warning(f"强制重置 running_tasks 从 {current_running} 到 {active_tasks_count}，释放 {current_running - active_tasks_count} 个信号量")
                            self.running_tasks = active_tasks_count
                        elif active_tasks_count == 0 and current_running > 0:
                            logger.error(f"检测到严重信号量死锁：队列中有 {queue_size} 个任务，running_tasks={current_running}，但没有任务在执行")
                            logger.error(f"强制重置 running_tasks 为 0，释放所有 {current_running} 个信号量")
                            self.running_tasks = 0
                            self.task_execution_times.clear()
                        # 其他情况：队列正常，任务正在执行
                        # 不需要额外警告，超时检测已经在上面处理
                
                # 只监控队列状态，不触发阈值回调
                # 阈值回调只在任务完成后通过 _trigger_replenish_check() 触发
                
                # 降低检查间隔，监控队列状态（更频繁的检查，以便更快检测到死锁）
                await asyncio.sleep(0.5)  # 每0.5秒检查一次队列状态，更快检测到死锁
                
            except Exception as e:
                logger.error(f"维护并发数监控出错: {e}", exc_info=True)
                await asyncio.sleep(0.2)
    
    async def _run(self):
        """运行队列（内部方法）"""
        logger.info("消息队列开始运行")
        self.is_running = True
        self.is_stopped = False
        
        # 创建工作协程
        # 注意：worker 数量应该 >= max_concurrent * 2，因为：
        # 1. 每个 worker 会先获取信号量再取任务，确保实际执行的任务数不超过 max_concurrent
        # 2. 如果 worker 数量 = max_concurrent，当所有 worker 都在执行任务时，队列中的任务无法被获取
        # 3. 如果 worker 数量 = max_concurrent * 2，即使所有信号量都被占用，也有 worker 在等待，一旦任务完成就能立即获取新任务
        # 关键：确保队列中始终有足够的任务（至少 max_concurrent * 1.5），这样所有 worker 都能立即获取任务
        worker_count = max(self.max_concurrent * 2, 10)
        logger.info(f"✓ 启动消息队列：{worker_count} 个工作协程，实际并发: {self.max_concurrent}")
        
        self.worker_tasks = [
            asyncio.create_task(self._worker(i))
            for i in range(worker_count)
        ]
        
        # 创建维护并发数的协程
        maintain_task = asyncio.create_task(self._maintain_concurrency())
        
        # 立即触发一次初始任务补充
        try:
            initial_tasks = await self._threshold_replenish()
            if initial_tasks:
                for task in initial_tasks:
                    await self.task_queue.put(task)
                    async with self.stats_lock:
                        self.total_tasks += 1
        except Exception as e:
            logger.error(f"初始任务补充失败: {e}", exc_info=True)
        
        # 等待停止事件
        await self.stop_event.wait()
        
        logger.info("停止消息队列...")
        
        # 等待所有工作协程完成
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.worker_tasks, return_exceptions=True),
                timeout=30.0
            )
        except asyncio.TimeoutError:
            logger.debug("等待超时，强制取消")
            for task in self.worker_tasks:
                task.cancel()
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        
        # 取消维护并发数的协程
        maintain_task.cancel()
        try:
            await maintain_task
        except asyncio.CancelledError:
            pass
        
        # 等待队列中剩余任务完成
        try:
            await asyncio.wait_for(self.task_queue.join(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.debug("队列任务未完成，强制停止")
        
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
            if self.loop_thread.is_alive():
                logger.warning("事件循环线程仍在运行，超时")
        
        self.is_running = False
        logger.info("消息队列已停止")
    
    def add_task(self, task: Any) -> bool:
        """
        添加任务到队列（同步方法）
        
        Args:
            task: 要添加的任务
            
        Returns:
            bool: True表示成功添加，False表示失败
        """
        if not self.loop or not self.loop.is_running():
            # 队列停止时不输出错误日志（避免停止时的噪音）
            logger.debug(f"队列未运行，无法添加任务")
            return False
        
        try:
            # 使用 call_soon_threadsafe 直接添加任务到队列
            # 这种方式不会阻塞，即使事件循环繁忙也能快速完成
            success = [False]
            error = [None]
            
            def _add_task_sync():
                try:
                    # 使用 put_nowait 非阻塞添加
                    self.task_queue.put_nowait(task)
                    success[0] = True
                except Exception as e:
                    error[0] = e
            
            # 在事件循环中调用
            self.loop.call_soon_threadsafe(_add_task_sync)
            
            # 短暂等待确认（但不阻塞太久）
            import time
            time.sleep(0.01)  # 10毫秒，足够让事件循环处理
            
            if success[0]:
                self.total_tasks += 1
                logger.debug(f"✓ 任务已添加到队列，total_tasks={self.total_tasks}，队列大小: {self.task_queue.qsize()}")
                return True
            elif error[0]:
                # 如果队列满了或其他错误，尝试使用 run_coroutine_threadsafe（慢但可靠）
                logger.debug(f"put_nowait失败: {error[0]}，尝试使用异步方式...")
                future = asyncio.run_coroutine_threadsafe(
                    self.task_queue.put(task), 
                    self.loop
                )
                try:
                    future.result(timeout=1.0)  # 降低超时时间到1秒
                    self.total_tasks += 1
                    logger.debug(f"✓ 任务已添加到队列（异步方式），total_tasks={self.total_tasks}")
                    return True
                except Exception as e:
                    logger.error(f"✗ 添加任务失败: {e}")
                    return False
            else:
                # 没有成功标记，可能事件循环太忙，乐观认为已提交
                self.total_tasks += 1
                logger.debug(f"✓ 任务已提交（未确认），total_tasks={self.total_tasks}")
                return True
                
        except Exception as e:
            logger.error(f"✗ 提交任务到事件循环失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
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
        直接读取值，不使用异步方法，避免阻塞
        
        Returns:
            统计信息字典
        """
        try:
            # 直接读取值，不使用锁（可能不是最新的，但至少不会阻塞）
            # 这样可以避免死锁和超时问题
            result = {
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "running_tasks": self.running_tasks,  # 直接读取，不使用锁
                "queue_size": self.task_queue.qsize(),  # 直接读取，不使用锁
                "max_concurrent": self.max_concurrent,
                "is_running": self.is_running,
                "is_stopped": self.is_stopped
            }
            return result
        except Exception as e:
            logger.error(f"获取统计信息异常: {e}", exc_info=True)
            # 返回默认值
            return {
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "running_tasks": 0,
                "queue_size": 0,
                "max_concurrent": self.max_concurrent,
                "is_running": self.is_running,
                "is_stopped": self.is_stopped
            }
    
    async def _get_stats_async(self) -> Dict[str, Any]:
        """获取统计信息（异步方法）"""
        try:
            # 尝试快速获取锁，如果获取不到就使用当前值（避免阻塞）
            try:
                running = await asyncio.wait_for(
                    self._get_running_tasks_async(),
                    timeout=0.5
                )
            except asyncio.TimeoutError:
                running = 0
            
            queue_size = self.task_queue.qsize()
            
            # 尝试快速获取统计信息，如果获取不到就使用当前值（避免阻塞）
            try:
                result = await asyncio.wait_for(
                    self._get_stats_data_async(running, queue_size),
                    timeout=0.5
                )
            except asyncio.TimeoutError:
                result = {
                    "total_tasks": self.total_tasks,
                    "completed_tasks": self.completed_tasks,
                    "failed_tasks": self.failed_tasks,
                    "running_tasks": running,
                    "queue_size": queue_size,
                    "max_concurrent": self.max_concurrent,
                    "is_running": self.is_running,
                    "is_stopped": self.is_stopped
                }
            
            return result
        except Exception as e:
            logger.error(f"获取统计信息异常: {e}", exc_info=True)
            # 返回默认值
            return {
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "running_tasks": 0,
                "queue_size": self.task_queue.qsize(),
                "max_concurrent": self.max_concurrent,
                "is_running": self.is_running,
                "is_stopped": self.is_stopped
            }
    
    async def _get_running_tasks_async(self) -> int:
        """安全地获取 running_tasks"""
        async with self.running_tasks_lock:
            return self.running_tasks
    
    async def _get_stats_data_async(self, running: int, queue_size: int) -> Dict[str, Any]:
        """安全地获取统计信息（带超时保护）"""
        try:
            # 尝试快速获取锁，如果获取不到就使用当前值
            async with asyncio.timeout(0.3):  # 0.3秒超时
                async with self.stats_lock:
                    return {
                        "total_tasks": self.total_tasks,
                        "completed_tasks": self.completed_tasks,
                        "failed_tasks": self.failed_tasks,
                        "running_tasks": running,
                        "queue_size": queue_size,
                        "max_concurrent": self.max_concurrent,
                        "is_running": self.is_running,
                        "is_stopped": self.is_stopped
                    }
        except (asyncio.TimeoutError, TimeoutError):
            # 如果获取锁超时，直接读取值（不使用锁，可能不是最新的）
            return {
                "total_tasks": self.total_tasks,
                "completed_tasks": self.completed_tasks,
                "failed_tasks": self.failed_tasks,
                "running_tasks": running,
                "queue_size": queue_size,
                "max_concurrent": self.max_concurrent,
                "is_running": self.is_running,
                "is_stopped": self.is_stopped
            }

