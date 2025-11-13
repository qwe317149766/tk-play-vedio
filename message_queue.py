"""
消息队列实现
支持高并发、自动阈值补给、任务不阻塞执行
"""
import asyncio
import threading
import time
import sys
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
    
    async def _trigger_replenish_check(self):
        """
        触发一次补充检查（任务完成后立即调用，确保及时补充）
        只要队列大小 < 阈值，就立即补充，不等待所有任务完成
        但需要避免重复补充（通过检查阈值回调状态）
        """
        try:
            print(f"[_trigger_replenish_check] 开始执行补充检查")
            logger.info(f"[_trigger_replenish_check] 开始执行补充检查")
            async with self.running_tasks_lock:
                current_running = self.running_tasks
                queue_size = self.task_queue.qsize()
            
            threshold_size = 3 * self.max_concurrent  # 与 _maintain_concurrency 保持一致
            critical_size = self.max_concurrent * 1.5  # 关键阈值
            
            # 只要队列大小小于阈值，就立即触发补充（不检查 running_tasks）
            # 但需要检查阈值回调是否正在执行，避免重复补充
            # 如果队列小于关键阈值，立即补充（不等待）
            if (queue_size < threshold_size and 
                not self.threshold_calling and 
                not self.is_stopped):
                print(f"[_trigger_replenish_check] 任务完成后触发补充检查: 队列大小={queue_size}, 正在执行={current_running}, 阈值={threshold_size}")
                logger.info(f"任务完成后触发补充检查: 队列大小={queue_size}, 正在执行={current_running}, 阈值={threshold_size}")
                new_tasks = await self._threshold_replenish()
                if new_tasks:
                    print(f"[_trigger_replenish_check] 任务完成后补充检查获取到 {len(new_tasks)} 个新任务，开始添加到队列")
                    logger.info(f"任务完成后补充检查获取到 {len(new_tasks)} 个新任务，开始添加到队列")
                    # 批量添加任务到队列
                    # 注意：每个 put 操作都会唤醒一个等待的 get 操作
                    # 所以如果有多个 worker 在等待，它们会依次被唤醒
                    for i, task in enumerate(new_tasks):
                        await self.task_queue.put(task)
                        async with self.stats_lock:
                            self.total_tasks += 1
                        # 每添加一个任务，就记录一次（方便调试，但减少日志频率）
                        if (i + 1) % 200 == 0 or i == len(new_tasks) - 1:
                            print(f"[_trigger_replenish_check] 任务补充进度: {i + 1}/{len(new_tasks)}")
                            logger.info(f"任务补充进度: {i + 1}/{len(new_tasks)}")
                    final_queue_size = self.task_queue.qsize()
                    print(f"[_trigger_replenish_check] 任务完成后立即补充了 {len(new_tasks)} 个任务，补充后队列大小: {final_queue_size}")
                    logger.info(f"任务完成后立即补充了 {len(new_tasks)} 个任务，补充后队列大小: {final_queue_size}")
                    # 通知所有等待的 worker，有新任务了
                    # 由于 asyncio.Queue 是线程安全的，put 操作会自动唤醒等待的 get 操作
                else:
                    print(f"[_trigger_replenish_check] 任务完成后补充检查返回空任务列表，队列大小={queue_size}, 正在执行={current_running}, 可能没有更多任务了")
                    logger.warning(f"任务完成后补充检查返回空任务列表，队列大小={queue_size}, 正在执行={current_running}, 可能没有更多任务了")
                    # 不在这里停止队列，让 _maintain_concurrency 统一管理停止逻辑
            else:
                print(f"[_trigger_replenish_check] 任务完成后不触发补充检查: 队列大小={queue_size}, 阈值={threshold_size}, 阈值回调状态={self.threshold_calling}, 队列状态={self.is_stopped}")
                logger.info(f"任务完成后不触发补充检查: 队列大小={queue_size}, 阈值={threshold_size}, 阈值回调状态={self.threshold_calling}, 队列状态={self.is_stopped}")
                # 不在这里停止队列，让 _maintain_concurrency 统一管理停止逻辑
        except Exception as e:
            print(f"[_trigger_replenish_check] 异常: {e}")
            logger.error(f"任务完成后触发补充检查异常: {e}", exc_info=True)
    
    async def _threshold_replenish(self):
        """
        阈值补给（排队执行）
        """
        # 使用锁确保阈值回调排队执行
        async with self.threshold_lock:
            if self.threshold_calling:
                print(f"[_threshold_replenish] 阈值回调正在执行，跳过本次调用")
                logger.debug("阈值回调正在执行，跳过本次调用")
                return []
            
            self.threshold_calling = True
        
        try:
            if not self.threshold_callback:
                print(f"[_threshold_replenish] 没有阈值回调函数")
                return []
            
            print(f"[_threshold_replenish] 调用阈值补给回调...")
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
                print(f"[_threshold_replenish] 阈值补给回调执行超时（2秒）")
                logger.error("阈值补给回调执行超时（2秒），可能回调函数执行时间过长")
                return []
            
            if new_tasks:
                print(f"[_threshold_replenish] 阈值补给获取到 {len(new_tasks)} 个新任务")
                logger.info(f"阈值补给获取到 {len(new_tasks)} 个新任务")
                return new_tasks
            else:
                print(f"[_threshold_replenish] 阈值补给返回空，没有更多任务")
                logger.info("阈值补给返回空，没有更多任务")
                return []
                
        except Exception as e:
            print(f"[_threshold_replenish] 异常: {e}")
            logger.error(f"阈值补给回调执行失败: {e}", exc_info=True)
            return []
        finally:
            # 在锁外清除标志，避免死锁
            async with self.threshold_lock:
                self.threshold_calling = False
                print(f"[_threshold_replenish] 阈值回调执行完成，清除标志")
    
    async def _worker(self, worker_id: int):
        """
        工作协程（不断从队列中取任务执行，完全异步，不阻塞）
        注意：worker 会先获取信号量，然后从队列取任务执行，确保实际执行的任务数不超过 max_concurrent
        这样可以避免队列被快速消耗，但任务都在等待信号量
        
        Args:
            worker_id: 工作协程 ID
        """
        print(f"[WORKER {worker_id}] 工作协程启动")
        logger.info(f"工作协程 {worker_id} 启动")
        
        worker_loop_count = 0
        while self.is_running:
            worker_loop_count += 1
            try:
                # 先获取信号量（控制实际并发数）
                # 如果已达到并发上限，这里会等待直到有任务完成
                print(f"[WORKER {worker_id}] 循环第 {worker_loop_count} 次，准备获取信号量... (队列状态: is_running={self.is_running}, is_stopped={self.is_stopped}, 队列大小={self.task_queue.qsize()}, 正在执行={self.running_tasks}/{self.max_concurrent})")
                sys.stdout.flush()
                async with self.concurrency_semaphore:
                    print(f"[WORKER {worker_id}] 获取到信号量")
                    sys.stdout.flush()
                    # 获取到信号量后，更新运行任务数（直接更新，不使用锁，避免死锁）
                    self.running_tasks += 1
                    current_running = self.running_tasks
                    
                    # 从队列中获取任务（不设置超时，持续等待，确保能立即获取新任务）
                    # 当任务完成后，队列中会有新任务，worker 会立即获取
                    try:
                        queue_size_before = self.task_queue.qsize()
                        print(f"[WORKER {worker_id}] 获取到信号量，准备从队列获取任务，当前队列大小: {queue_size_before}, 当前并发: {current_running}/{self.max_concurrent}")
                        logger.debug(f"工作协程 {worker_id} 获取到信号量，准备从队列获取任务，当前队列大小: {queue_size_before}, 当前并发: {current_running}/{self.max_concurrent}")
                        task = await self.task_queue.get()
                        queue_size_after = self.task_queue.qsize()
                        print(f"[WORKER {worker_id}] 获取到任务: {task}, 获取后队列大小: {queue_size_after}, 当前并发: {current_running}/{self.max_concurrent}")
                        logger.info(f"工作协程 {worker_id} 获取到任务: {task}, 获取后队列大小: {queue_size_after}, 当前并发: {current_running}/{self.max_concurrent}")
                    except Exception as e:
                        # 队列可能已关闭
                        async with self.running_tasks_lock:
                            self.running_tasks -= 1
                        if self.is_stopped:
                            logger.debug(f"工作协程 {worker_id} 准备停止（队列已关闭）")
                            break
                        logger.debug(f"工作协程 {worker_id} 获取任务异常: {e}")
                        continue
                    
                    # 执行任务回调（完全异步，不阻塞其他任务）
                    task_start_time = time.time()
                    task_succeeded = False
                    task_failed = False
                    print(f"[WORKER {worker_id}] 准备执行任务回调: task={task}, callback={self.task_callback}")
                    sys.stdout.flush()
                    try:
                        if self.task_callback:
                            print(f"[WORKER {worker_id}] 开始执行任务: {task}")
                            logger.info(f"工作协程 {worker_id} 开始执行任务: {task}")
                            sys.stdout.flush()
                            if asyncio.iscoroutinefunction(self.task_callback):
                                # 异步函数，直接 await（不会阻塞其他任务，因为每个任务都是独立的协程）
                                print(f"[WORKER {worker_id}] 调用异步任务回调...")
                                sys.stdout.flush()
                                await self.task_callback(task)
                                print(f"[WORKER {worker_id}] 异步任务回调返回")
                                sys.stdout.flush()
                            else:
                                # 如果是同步函数，在线程池中执行（不阻塞事件循环）
                                print(f"[WORKER {worker_id}] 调用同步任务回调（在线程池中）...")
                                sys.stdout.flush()
                                loop = asyncio.get_event_loop()
                                await loop.run_in_executor(None, self.task_callback, task)
                                print(f"[WORKER {worker_id}] 同步任务回调返回")
                                sys.stdout.flush()
                            task_elapsed = time.time() - task_start_time
                            print(f"[WORKER {worker_id}] 任务执行完成: {task}, 耗时: {task_elapsed:.3f}s")
                            logger.info(f"工作协程 {worker_id} 任务执行完成: {task}, 耗时: {task_elapsed:.3f}s")
                            sys.stdout.flush()
                            task_succeeded = True
                        else:
                            print(f"[WORKER {worker_id}] 警告: 没有任务回调函数，任务 {task} 未执行")
                            logger.warning(f"工作协程 {worker_id} 没有任务回调函数，任务 {task} 未执行")
                            sys.stdout.flush()
                            task_succeeded = True  # 没有回调也算成功（已处理）
                        
                    except Exception as e:
                        task_elapsed = time.time() - task_start_time
                        task_failed = True
                        print(f"[WORKER {worker_id}] 任务执行异常: {task}, 错误: {e}, 耗时: {task_elapsed:.3f}s")
                        import traceback
                        print(f"[WORKER {worker_id}] 异常堆栈: {traceback.format_exc()}")
                        sys.stdout.flush()
                        logger.error(f"工作协程 {worker_id} 任务执行失败: {task}, 错误: {e}", exc_info=True)
                    finally:
                        print(f"[WORKER {worker_id}] 进入 finally 块，任务: {task}, 成功: {task_succeeded}, 失败: {task_failed}")
                        sys.stdout.flush()
                        
                        # 更新统计（无论成功还是失败，任务都算完成）- 在 finally 块中确保一定会执行
                        print(f"[WORKER {worker_id}] 准备更新统计信息（finally块），任务: {task}")
                        sys.stdout.flush()
                        # 直接更新，不使用锁（避免死锁和超时问题）
                        # 虽然可能不是完全线程安全的，但在高并发场景下，性能更重要
                        try:
                            self.completed_tasks += 1
                            if task_failed:
                                self.failed_tasks += 1
                            current_completed = self.completed_tasks
                            current_failed = self.failed_tasks
                            print(f"[WORKER {worker_id}] 任务 {task} 统计更新成功: 完成总数={current_completed}, 失败数={current_failed}")
                            sys.stdout.flush()
                        except Exception as stats_error:
                            print(f"[WORKER {worker_id}] 更新统计信息失败: {stats_error}")
                            import traceback
                            print(f"[WORKER {worker_id}] 统计更新异常堆栈: {traceback.format_exc()}")
                            sys.stdout.flush()
                        
                        # 标记任务完成（在任务真正执行完成后）
                        try:
                            self.task_queue.task_done()
                            print(f"[WORKER {worker_id}] 任务 {task} task_done() 已调用")
                            sys.stdout.flush()
                        except Exception as e:
                            print(f"[WORKER {worker_id}] 调用 task_done() 失败: {e}")
                            sys.stdout.flush()
                        
                        # 更新运行任务数（直接更新，不使用锁，避免死锁）
                        try:
                            self.running_tasks -= 1
                            current_running_after = self.running_tasks
                            print(f"[WORKER {worker_id}] 任务 {task} 完成，释放信号量（当前并发: {current_running_after}/{self.max_concurrent}）")
                            logger.info(f"工作协程 {worker_id} 任务 {task} 完成，释放信号量（当前并发: {current_running_after}/{self.max_concurrent}）")
                            sys.stdout.flush()
                        except Exception as e:
                            print(f"[WORKER {worker_id}] 更新 running_tasks 失败: {e}")
                            import traceback
                            print(f"[WORKER {worker_id}] 更新 running_tasks 异常堆栈: {traceback.format_exc()}")
                            sys.stdout.flush()
                            try:
                                self.running_tasks -= 1
                                current_running_after = self.running_tasks
                                print(f"[WORKER {worker_id}] 任务 {task} 完成，释放信号量（异常后直接更新，当前并发: {current_running_after}/{self.max_concurrent}）")
                                sys.stdout.flush()
                            except:
                                pass
                    
                    # 任务完成后，立即检查并补充任务（在 finally 块外，可以使用 break）
                    queue_size_after = self.task_queue.qsize()
                    threshold_size = 3 * self.max_concurrent  # 与 _maintain_concurrency 保持一致
                    critical_size = self.max_concurrent * 1.5  # 关键阈值：如果队列小于此值，立即补充
                    
                    print(f"[WORKER {worker_id}] 任务 {task} 完成后，队列大小: {queue_size_after}, 关键阈值: {critical_size}, 阈值: {threshold_size}, 阈值回调状态: {self.threshold_calling}, 队列状态: {self.is_stopped}, 正在执行: {current_running_after}")
                    logger.info(f"工作协程 {worker_id} 任务 {task} 完成后，队列大小: {queue_size_after}, 关键阈值: {critical_size}, 阈值: {threshold_size}, 阈值回调状态: {self.threshold_calling}, 队列状态: {self.is_stopped}, 正在执行: {current_running_after}")
                    
                    # 更积极的补充策略：如果队列小于关键阈值，立即触发补充（不等待冷却时间）
                    # 这样可以确保队列中始终有足够的任务，所有 worker 都能立即获取任务
                    if queue_size_after < critical_size and not self.threshold_calling and not self.is_stopped:
                        print(f"[WORKER {worker_id}] 任务 {task} 完成后立即触发补充检查（队列大小: {queue_size_after} < 关键阈值: {critical_size}）")
                        logger.info(f"工作协程 {worker_id} 任务 {task} 完成后立即触发补充检查（队列大小: {queue_size_after} < 关键阈值: {critical_size}）")
                        try:
                            # 使用 create_task 创建异步任务，但不等待（不阻塞）
                            task_replenish = asyncio.create_task(self._trigger_replenish_check())
                            print(f"[WORKER {worker_id}] 任务 {task} 完成后已创建补充检查任务")
                            logger.info(f"工作协程 {worker_id} 任务 {task} 完成后已创建补充检查任务")
                        except Exception as e:
                            print(f"[WORKER {worker_id}] 任务 {task} 完成后创建补充检查任务失败: {e}")
                            logger.error(f"工作协程 {worker_id} 任务 {task} 完成后创建补充检查任务失败: {e}", exc_info=True)
                    elif queue_size_after < threshold_size and not self.threshold_calling and not self.is_stopped:
                        print(f"[WORKER {worker_id}] 任务 {task} 完成后触发补充检查（队列大小: {queue_size_after} < 阈值: {threshold_size}）")
                        logger.info(f"工作协程 {worker_id} 任务 {task} 完成后触发补充检查（队列大小: {queue_size_after} < 阈值: {threshold_size}）")
                        try:
                            task_replenish = asyncio.create_task(self._trigger_replenish_check())
                            print(f"[WORKER {worker_id}] 任务 {task} 完成后已创建补充检查任务")
                            logger.info(f"工作协程 {worker_id} 任务 {task} 完成后已创建补充检查任务")
                        except Exception as e:
                            print(f"[WORKER {worker_id}] 任务 {task} 完成后创建补充检查任务失败: {e}")
                            logger.error(f"工作协程 {worker_id} 任务 {task} 完成后创建补充检查任务失败: {e}", exc_info=True)
                    else:
                        print(f"[WORKER {worker_id}] 任务 {task} 完成后不触发补充检查: 队列大小={queue_size_after}, 关键阈值={critical_size}, 阈值={threshold_size}, 阈值回调状态={self.threshold_calling}, 队列状态={self.is_stopped}")
                        logger.debug(f"工作协程 {worker_id} 任务 {task} 完成后不触发补充检查: 队列大小={queue_size_after}, 关键阈值={critical_size}, 阈值={threshold_size}, 阈值回调状态={self.threshold_calling}, 队列状态={self.is_stopped}")
                    
                    # 任务完成后，继续循环获取下一个任务
                    print(f"[WORKER {worker_id}] 任务 {task} 处理完成，退出信号量块，准备继续循环获取下一个任务...")
                    sys.stdout.flush()
                # 退出信号量块后，继续循环
                print(f"[WORKER {worker_id}] 已退出信号量块，继续循环（第 {worker_loop_count} 次）...")
                sys.stdout.flush()
                
            except Exception as e:
                print(f"[WORKER {worker_id}] Worker 循环异常: {e}")
                import traceback
                print(f"[WORKER {worker_id}] Worker 循环异常堆栈: {traceback.format_exc()}")
                sys.stdout.flush()
                logger.error(f"工作协程 {worker_id} 循环异常: {e}", exc_info=True)
                # 发生异常时，等待一小段时间后继续循环
                await asyncio.sleep(0.1)
        
        logger.debug(f"工作协程 {worker_id} 停止")
    
    async def _maintain_concurrency(self):
        """
        维护并发数：确保始终有 max_concurrent 个任务在执行
        阈值回调条件：队列中任务数 < 3 * max_concurrent（提高阈值，确保队列中始终有足够任务）
        """
        print(f"[_maintain_concurrency] 开始维护并发数: {self.max_concurrent}, 阈值: {3 * self.max_concurrent}")
        logger.info(f"开始维护并发数: {self.max_concurrent}, 阈值: {3 * self.max_concurrent}")
        
        threshold_size = 3 * self.max_concurrent  # 提高阈值，确保队列中始终有足够任务
        last_replenish_time = 0
        replenish_cooldown = 0.5  # 阈值补给冷却时间（秒），降低到0.5秒，更及时地补充任务
        last_queue_size = threshold_size  # 记录上次队列大小，用于判断是否需要补充
        
        while self.is_running:
            try:
                async with self.running_tasks_lock:
                    current_running = self.running_tasks
                    queue_size = self.task_queue.qsize()
                
                current_time = time.time()
                
                # 阈值回调条件：队列中任务数 < 2 * max_concurrent
                # 改进逻辑：更积极地补充任务，确保始终有足够的任务在执行
                # 条件1：队列大小小于阈值
                # 条件2：队列大小比上次检查时减少（至少减少50个任务或减少5%）
                #        或者队列大小小于 max_concurrent（需要立即补充）
                #        或者队列大小小于阈值且正在执行的任务数 < max_concurrent（有执行能力但任务不足）
                # 条件3：冷却时间已过
                # 条件4：阈值回调不在执行
                queue_decreased = (last_queue_size - queue_size) >= max(10, last_queue_size * 0.02)  # 降低阈值，更敏感
                # 队列太小，需要立即补充
                queue_too_small = queue_size < self.max_concurrent
                # 有执行能力但任务不足（正在执行的任务数小于最大并发数）
                has_capacity = current_running < self.max_concurrent and queue_size < threshold_size
                # 队列大小小于阈值的一半，需要补充
                queue_below_half = queue_size < (threshold_size // 2)
                
                # 更积极的补充条件：确保队列中始终有足够的任务
                # 关键：队列大小应该始终 >= max_concurrent，这样所有 worker 都能立即获取任务
                should_replenish = False
                
                # 如果队列太小（小于 max_concurrent * 1.5），立即补充，不等待冷却时间
                # 这样可以确保队列中始终有足够的任务，所有 worker 都能立即获取任务
                if queue_size < (self.max_concurrent * 1.5) and not self.threshold_calling and not self.is_stopped:
                    should_replenish = True
                    print(f"[_maintain_concurrency] 队列太小（{queue_size} < {self.max_concurrent * 1.5}），立即触发补充（不等待冷却）")
                    logger.info(f"队列太小（{queue_size} < {self.max_concurrent * 1.5}），立即触发补充（不等待冷却）")
                # 如果队列小于阈值，且冷却时间已过，也触发补充
                elif (queue_size < threshold_size and 
                      not self.threshold_calling and 
                      not self.is_stopped and
                      current_time - last_replenish_time >= replenish_cooldown):
                    should_replenish = True
                    print(f"[_maintain_concurrency] 队列小于阈值（{queue_size} < {threshold_size}），触发补充")
                    logger.info(f"队列小于阈值（{queue_size} < {threshold_size}），触发补充")
                
                if should_replenish:
                    
                    # 调用阈值补给
                    print(f"[_maintain_concurrency] 触发阈值补给: 队列大小={queue_size}, 上次队列大小={last_queue_size}, 正在执行={current_running}")
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
                        print(f"[_maintain_concurrency] 准备将 {len(new_tasks)} 个任务加入队列")
                        logger.info(f"准备将 {len(new_tasks)} 个任务加入队列")
                        for task in new_tasks:
                            await self.task_queue.put(task)
                            async with self.stats_lock:
                                self.total_tasks += 1
                        queue_size_after = self.task_queue.qsize()
                        print(f"[_maintain_concurrency] 已补充 {len(new_tasks)} 个任务，当前队列大小: {queue_size_after}")
                        logger.info(f"已补充 {len(new_tasks)} 个任务，当前队列大小: {queue_size_after}")
                    else:
                        # 没有更多任务，但不立即停止
                        # 等待一段时间，让任务完成后的补充检查有机会执行
                        # 如果队列为空且没有正在执行的任务，等待一小段时间后再检查
                        # 注意：只有在 threshold_callback 明确返回空列表时才考虑停止
                        # 如果 threshold_callback 返回 None 或抛出异常，不应该停止
                        # 但是，如果队列为空且没有正在执行的任务，可能是任务执行太快，需要等待补充
                        if queue_size == 0 and current_running == 0:
                            print(f"[_maintain_concurrency] [停止检查] 阈值补给返回空列表，但队列为空且没有正在执行的任务，等待补充检查完成...")
                            logger.info("阈值补给返回空列表，但队列为空且没有正在执行的任务，等待补充检查完成...")
                            # 等待更长时间，让所有补充检查都有机会执行
                            await asyncio.sleep(1.0)
                            # 再次检查队列状态
                            async with self.running_tasks_lock:
                                current_running_after = self.running_tasks
                                queue_size_after = self.task_queue.qsize()
                            print(f"[_maintain_concurrency] [停止检查] 等待后队列状态: 队列大小={queue_size_after}, 正在执行={current_running_after}")
                            if queue_size_after == 0 and current_running_after == 0:
                                # 再次尝试获取任务，确保真的没有更多任务
                                print(f"[_maintain_concurrency] [停止检查] 等待后队列仍为空，再次尝试获取任务...")
                                logger.info("等待后队列仍为空，再次尝试获取任务...")
                                final_tasks = await self._threshold_replenish()
                                if final_tasks:
                                    print(f"[_maintain_concurrency] [停止检查] 再次尝试获取到 {len(final_tasks)} 个任务")
                                    logger.info(f"再次尝试获取到 {len(final_tasks)} 个任务")
                                    for task in final_tasks:
                                        await self.task_queue.put(task)
                                        async with self.stats_lock:
                                            self.total_tasks += 1
                                else:
                                    # 只有在多次尝试后仍然没有任务时才停止
                                    print(f"[_maintain_concurrency] [停止检查] 再次尝试后仍然没有任务，但继续运行等待后续补充...")
                                    logger.info("再次尝试后仍然没有任务，但继续运行等待后续补充...")
                                    # 不立即停止，继续运行，等待后续的补充检查
                                    # 因为任务可能是异步完成的，补充检查可能稍后才触发
                                    pass
                else:
                    # 更新上次队列大小（只有在队列大小变化时才更新，避免在队列大小不变时一直触发补给）
                    if queue_size != last_queue_size:
                        last_queue_size = queue_size
                
                # 检查是否可以停止（队列为空且没有正在执行的任务）
                # 注意：这个检查应该在补充检查之后，给补充检查足够的时间完成
                # 只有在多次尝试后仍然没有任务时才停止
                # 但是，如果队列为空，可能是任务执行太快，需要等待补充，不应该立即停止
                # 只有在 threshold_callback 明确返回空列表且多次尝试后仍然没有任务时才停止
                if queue_size == 0 and current_running == 0 and not self.is_stopped:
                    # 如果阈值回调不在执行，再次尝试获取任务（但要有冷却时间）
                    if not self.threshold_calling and current_time - last_replenish_time >= replenish_cooldown:
                        print(f"[_maintain_concurrency] [停止检查] 队列为空且没有正在执行的任务，尝试补充... (队列大小={queue_size}, 正在执行={current_running}, 阈值回调状态={self.threshold_calling}, 队列状态={self.is_stopped})")
                        logger.info("队列为空且没有正在执行的任务，尝试补充...")
                        new_tasks = await self._threshold_replenish()
                        last_replenish_time = current_time
                        last_queue_size = len(new_tasks) if new_tasks else 0
                        
                        if new_tasks:
                            print(f"[_maintain_concurrency] [停止检查] 补充获取到 {len(new_tasks)} 个任务")
                            logger.info(f"补充获取到 {len(new_tasks)} 个任务")
                            for task in new_tasks:
                                await self.task_queue.put(task)
                                async with self.stats_lock:
                                    self.total_tasks += 1
                        else:
                            # 没有获取到任务，但不立即停止
                            # 继续运行，等待后续的补充检查
                            # 因为任务可能是异步完成的，补充检查可能稍后才触发
                            print(f"[_maintain_concurrency] [停止检查] 补充没有获取到任务，但继续运行等待后续补充... (队列大小={queue_size}, 正在执行={current_running}, 阈值回调状态={self.threshold_calling}, 队列状态={self.is_stopped})")
                            logger.info("补充没有获取到任务，但继续运行等待后续补充...")
                            # 不停止队列，继续运行
                            # 但是，如果队列为空且没有正在执行的任务，可能是真的没有更多任务了
                            # 在这种情况下，我们应该等待一段时间，让补充检查有机会执行
                            if queue_size == 0 and current_running == 0:
                                print(f"[_maintain_concurrency] [停止检查] 队列为空且没有正在执行的任务，等待补充检查...")
                                await asyncio.sleep(1.0)
                                # 再次检查队列状态
                                async with self.running_tasks_lock:
                                    current_running_check = self.running_tasks
                                    queue_size_check = self.task_queue.qsize()
                                print(f"[_maintain_concurrency] 等待后检查: 队列大小={queue_size_check}, 正在执行={current_running_check}")
                                # 如果仍然为空，继续运行，不停止
                                pass
                
                # 降低检查间隔，更及时地补充任务（从0.05秒降低到0.02秒）
                # 更频繁的检查确保队列中始终有足够的任务
                await asyncio.sleep(0.02)  # 每0.02秒检查一次，更及时地补充任务
                
            except Exception as e:
                logger.error(f"维护并发数出错: {e}", exc_info=True)
                await asyncio.sleep(0.2)
    
    async def _run(self):
        """运行队列（内部方法）"""
        print(f"[_run] 消息队列开始运行")
        logger.info("消息队列开始运行")
        self.is_running = True
        self.is_stopped = False
        
        # 创建工作协程
        # 注意：worker 数量应该 >= max_concurrent * 2，因为：
        # 1. 每个 worker 会先获取信号量再取任务，确保实际执行的任务数不超过 max_concurrent
        # 2. 如果 worker 数量 = max_concurrent，当所有 worker 都在执行任务时，队列中的任务无法被获取
        # 3. 如果 worker 数量 = max_concurrent * 2，即使所有信号量都被占用，也有 worker 在等待，一旦任务完成就能立即获取新任务
        # 关键：确保队列中始终有足够的任务（至少 max_concurrent * 1.5），这样所有 worker 都能立即获取任务
        worker_count = max(self.max_concurrent * 2, 10)  # worker 数量 = max_concurrent * 2，至少10个，确保有足够的 worker 等待获取任务
        print(f"[_run] 创建 {worker_count} 个工作协程（实际并发数由信号量控制为 {self.max_concurrent}）")
        print(f"[_run] 队列维护策略：确保队列中始终有至少 {self.max_concurrent * 1.5} 个任务，阈值: {self.max_concurrent * 3}")
        logger.info(f"创建 {worker_count} 个工作协程（实际并发数由信号量控制为 {self.max_concurrent}）")
        logger.info(f"队列维护策略：确保队列中始终有至少 {self.max_concurrent * 1.5} 个任务，阈值: {self.max_concurrent * 3}")
        self.worker_tasks = [
            asyncio.create_task(self._worker(i))
            for i in range(worker_count)
        ]
        print(f"[_run] 已创建 {len(self.worker_tasks)} 个工作协程")
        logger.info(f"已创建 {len(self.worker_tasks)} 个工作协程")
        
        # 创建维护并发数的协程
        maintain_task = asyncio.create_task(self._maintain_concurrency())
        print(f"[_run] 维护并发数协程已创建")
        logger.info("维护并发数协程已创建")
        
        # 立即触发一次初始任务补充
        print(f"[_run] 触发初始任务补充...")
        try:
            initial_tasks = await self._threshold_replenish()
            if initial_tasks:
                print(f"[_run] 初始补充 {len(initial_tasks)} 个任务")
                for task in initial_tasks:
                    await self.task_queue.put(task)
                    async with self.stats_lock:
                        self.total_tasks += 1
                print(f"[_run] 初始任务已加入队列，当前队列大小: {self.task_queue.qsize()}")
            else:
                print(f"[_run] 初始补充未获取到任务")
        except Exception as e:
            print(f"[_run] 初始任务补充失败: {e}")
            logger.error(f"初始任务补充失败: {e}", exc_info=True)
        
        # 等待停止事件
        print(f"[_run] 等待停止事件...")
        await self.stop_event.wait()
        
        print(f"[_run] [停止] 收到停止信号，开始关闭队列...")
        logger.info("收到停止信号，开始关闭队列...")
        
        # 等待所有工作协程完成（给它们时间处理完当前任务）
        print(f"[_run] [停止] 等待所有工作协程完成...")
        logger.info("等待所有工作协程完成...")
        try:
            # 等待最多30秒
            print(f"[_run] [停止] 等待工作协程完成，超时30秒...")
            await asyncio.wait_for(
                asyncio.gather(*self.worker_tasks, return_exceptions=True),
                timeout=30.0
            )
            print(f"[_run] [停止] 所有工作协程已完成")
        except asyncio.TimeoutError:
            print(f"[_run] [停止] 等待工作协程超时，强制取消...")
            logger.warning("等待工作协程超时，强制取消...")
            for task in self.worker_tasks:
                task.cancel()
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
            print(f"[_run] [停止] 工作协程已强制取消")
        
        # 取消维护并发数的协程
        print(f"[_run] [停止] 取消维护并发数协程...")
        maintain_task.cancel()
        try:
            await maintain_task
        except asyncio.CancelledError:
            pass
        print(f"[_run] [停止] 维护并发数协程已取消")
        
        # 等待队列中剩余任务完成（最多等待10秒）
        print(f"[_run] [停止] 等待队列中剩余任务完成，超时10秒...")
        logger.info("等待队列中剩余任务完成...")
        try:
            await asyncio.wait_for(self.task_queue.join(), timeout=10.0)
            print(f"[_run] [停止] 队列中剩余任务已完成")
        except asyncio.TimeoutError:
            print(f"[_run] [停止] 等待队列任务完成超时，强制停止")
            logger.warning("等待队列任务完成超时，强制停止")
        
        print(f"[_run] [停止] 消息队列已停止")
        logger.info("消息队列停止")
    
    def start(self):
        """启动队列（同步方法）"""
        if self.is_running:
            print(f"[start] 队列已经在运行")
            logger.warning("队列已经在运行")
            return
        
        print(f"[start] 启动消息队列，max_concurrent={self.max_concurrent}")
        # 在新线程中运行事件循环
        def run_loop():
            print(f"[start] 事件循环线程启动")
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._run())
        
        self.loop_thread = threading.Thread(target=run_loop, daemon=True)
        self.loop_thread.start()
        print(f"[start] 消息队列已启动，线程ID: {self.loop_thread.ident}")
        logger.info("消息队列已启动")
    
    def stop(self):
        """停止队列（同步方法）"""
        print(f"[stop] [停止] 开始停止消息队列...")
        if not self.is_running:
            print(f"[stop] [停止] 队列未运行，无需停止")
            logger.warning("队列未运行")
            return
        
        print(f"[stop] [停止] 设置停止标志和停止事件...")
        logger.info("正在停止消息队列...")
        self.is_stopped = True
        self.stop_event.set()
        print(f"[stop] [停止] 停止标志已设置，停止事件已触发")
        
        # 等待线程结束
        print(f"[stop] [停止] 等待事件循环线程结束，超时10秒...")
        if self.loop_thread and self.loop_thread.is_alive():
            self.loop_thread.join(timeout=10)
            if self.loop_thread.is_alive():
                print(f"[stop] [停止] 事件循环线程仍在运行，超时")
            else:
                print(f"[stop] [停止] 事件循环线程已结束")
        
        self.is_running = False
        print(f"[stop] [停止] 消息队列已停止")
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
            print(f"[get_stats] 获取统计信息异常: {e}")
            import traceback
            print(f"[get_stats] 异常堆栈: {traceback.format_exc()}")
            sys.stdout.flush()
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
            print(f"[_get_stats_async] 开始获取统计信息（异步）")
            sys.stdout.flush()
            # 尝试快速获取锁，如果获取不到就使用当前值（避免阻塞）
            try:
                running = await asyncio.wait_for(
                    self._get_running_tasks_async(),
                    timeout=0.5
                )
            except asyncio.TimeoutError:
                print(f"[_get_stats_async] 获取 running_tasks 超时，使用默认值 0")
                sys.stdout.flush()
                running = 0
            
            queue_size = self.task_queue.qsize()
            
            # 尝试快速获取统计信息，如果获取不到就使用当前值（避免阻塞）
            try:
                result = await asyncio.wait_for(
                    self._get_stats_data_async(running, queue_size),
                    timeout=0.5
                )
            except asyncio.TimeoutError:
                print(f"[_get_stats_async] 获取统计信息超时，使用当前值")
                sys.stdout.flush()
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
            
            print(f"[_get_stats_async] 获取统计信息成功: {result}")
            sys.stdout.flush()
            return result
        except Exception as e:
            print(f"[_get_stats_async] 获取统计信息异常: {e}")
            import traceback
            print(f"[_get_stats_async] 异常堆栈: {traceback.format_exc()}")
            sys.stdout.flush()
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
            print(f"[_get_stats_data_async] 获取 stats_lock 超时，直接读取值")
            sys.stdout.flush()
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

