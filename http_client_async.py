"""
基于 user_id 的全局 Session 管理 HTTP 客户端（完全异步版本）
特性：
 - 完全异步，不阻塞
 - 每个 user_id 对应一个独立的 session
 - 最多保持 5000 个 session，超过时移除最早的
 - 异步清理队列，避免阻塞主流程
 - 可配置每个 user_id 的 session 使用次数
 - 完整的重试机制和错误处理
 - 修复事件循环绑定问题
"""
import time
import asyncio
from typing import Optional, Dict, Any
from collections import OrderedDict
import atexit
import weakref

from curl_cffi.requests import AsyncSession
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# 全局 Session 池（基于 user_id）
_client_pool_lock = asyncio.Lock()
_client_pool: OrderedDict[str, Dict[str, Any]] = OrderedDict()
_client_pool_max_size = 5000

# 异步清理队列和任务
_cleanup_queue: Optional[asyncio.Queue] = None
_cleanup_task: Optional[asyncio.Task] = None
_cleanup_loop: Optional[asyncio.AbstractEventLoop] = None
_shutdown_event: Optional[asyncio.Event] = None


async def _start_cleanup_task():
    """启动清理任务（单例，带事件循环检查）"""
    global _cleanup_task, _cleanup_queue, _cleanup_loop, _shutdown_event
    
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        # 如果没有运行中的事件循环，无法启动清理任务
        return
    
    # 如果事件循环已改变，重新创建队列和任务
    if _cleanup_loop is not None and _cleanup_loop != current_loop:
        # 停止旧的任务
        if _cleanup_task and not _cleanup_task.done():
            _shutdown_event.set()
            try:
                await asyncio.wait_for(_cleanup_task, timeout=2.0)
            except (asyncio.TimeoutError, RuntimeError):
                _cleanup_task.cancel()
                try:
                    await _cleanup_task
                except (asyncio.CancelledError, RuntimeError):
                    pass
        
        # 重置全局变量
        _cleanup_queue = None
        _cleanup_task = None
        _cleanup_loop = None
        _shutdown_event = None
    
    # 创建新的队列和任务
    if _cleanup_queue is None:
        _cleanup_queue = asyncio.Queue()
        _shutdown_event = asyncio.Event()
        _cleanup_loop = current_loop
    
    if _cleanup_task is None or _cleanup_task.done():
        _cleanup_task = asyncio.create_task(_cleanup_worker())
        _cleanup_loop = current_loop


async def _cleanup_worker():
    """异步清理 session 的工作协程（带事件循环检查）"""
    global _cleanup_queue, _shutdown_event
    
    if _cleanup_queue is None:
        _cleanup_queue = asyncio.Queue()
    
    if _shutdown_event is None:
        _shutdown_event = asyncio.Event()
    
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        # 事件循环已关闭，退出
        return
    
    while True:
        try:
            # 检查是否应该关闭
            if _shutdown_event and _shutdown_event.is_set():
                # 处理队列中剩余的任务
                while not _cleanup_queue.empty():
                    try:
                        session_info = _cleanup_queue.get_nowait()
                        if session_info and session_info is not None:
                            session = session_info.get("session")
                            try:
                                if session:
                                    await session.close()
                            except Exception:
                                pass
                    except asyncio.QueueEmpty:
                        break
                break
            
            # 等待任务或超时
            try:
                session_info = await asyncio.wait_for(_cleanup_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                # 检查事件循环是否仍然有效
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    # 事件循环已关闭，退出
                    break
                continue
            
            if session_info is None:  # 退出信号
                break
            
            session = session_info.get("session")
            user_id = session_info.get("user_id", "unknown")
            
            try:
                if session:
                    # 检查事件循环是否匹配
                    try:
                        await session.close()
                    except RuntimeError as e:
                        if "different event loop" in str(e):
                            # 事件循环不匹配，跳过清理
                            continue
                        raise
            except Exception:
                pass  # 忽略清理错误
            
        except RuntimeError as e:
            if "different event loop" in str(e) or "no running event loop" in str(e):
                # 事件循环已关闭或不匹配，退出
                break
            continue
        except Exception:
            continue


async def _stop_cleanup_task():
    """停止清理任务"""
    global _cleanup_task, _cleanup_queue, _shutdown_event
    
    if _shutdown_event:
        _shutdown_event.set()
    
    if _cleanup_task and not _cleanup_task.done():
        try:
            # 等待任务完成，最多等待2秒
            await asyncio.wait_for(_cleanup_task, timeout=2.0)
        except (asyncio.TimeoutError, RuntimeError):
            # 超时或事件循环已关闭，取消任务
            _cleanup_task.cancel()
            try:
                await _cleanup_task
            except (asyncio.CancelledError, RuntimeError):
                pass
    
    _cleanup_task = None
    _cleanup_queue = None
    _cleanup_loop = None
    _shutdown_event = None


# 注册退出时的清理函数
_http_client_instances = weakref.WeakSet()

def _cleanup_on_exit():
    """程序退出时的清理函数"""
    # 尝试同步清理所有实例
    for client in list(_http_client_instances):
        try:
            if hasattr(client, '_closed') and not client._closed:
                # 尝试在事件循环中关闭
                loop = None
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    except Exception:
                        pass
                
                if loop and loop.is_running():
                    # 如果事件循环正在运行，创建任务
                    asyncio.create_task(client.close())
                elif loop:
                    # 如果事件循环未运行，运行直到完成
                    try:
                        loop.run_until_complete(client.close())
                    except Exception:
                        pass
        except Exception:
            pass
    
    # 停止清理任务
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(_stop_cleanup_task())
        else:
            loop.run_until_complete(_stop_cleanup_task())
    except Exception:
        pass

atexit.register(_cleanup_on_exit)


class _StreamWrapper:
    """包装流响应，close 时释放 session"""
    def __init__(self, client: "HttpClient", user_id: str, resp):
        self._client = client
        self._user_id = user_id
        self._resp = resp

    def __getattr__(self, item):
        return getattr(self._resp, item)

    async def close(self):
        try:
            await self._resp.close()
        finally:
            # 流关闭时增加使用次数
            await self._client._increment_usage(self._user_id)


class FlowSessionWrapper:
    """Flow session 包装器"""
    def __init__(self, client: "HttpClient", user_id: str):
        self._client = client
        self._user_id = user_id
    
    @property
    def user_id(self):
        return self._user_id
    
    def __repr__(self):
        return f"<FlowSessionWrapper user_id={self._user_id}>"


class HttpClient:
    """基于 user_id 的全局 Session 管理 HTTP 客户端（异步版本）"""

    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        verify: bool = False,
        default_impersonate: str = "okhttp4_android",
        enable_keep_alive: bool = True,
        max_session_usage: int = 100,  # 每个 user_id 的 session 最大使用次数
        max_pool_size: int = 5000,  # 全局最大 session 数量
        debug: bool = False,
    ):
        self.proxy = proxy
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.verify = verify
        self.default_impersonate = default_impersonate
        self.enable_keep_alive = enable_keep_alive
        self.debug = debug
        
        # Session 管理参数
        self._max_session_usage = max_session_usage
        global _client_pool_max_size
        _client_pool_max_size = max_pool_size
        
        # 统计信息
        self._stats = {
            "requests": 0,
            "failures": 0,
            "retries": 0,
            "sessions_created": 0,
            "sessions_recycled": 0,
            "proxy_close_errors": 0,
            "dead_sessions_removed": 0,
        }
        
        self._closed = False
        self._cleanup_task_started = False  # 标记清理任务是否已启动
        
        # 注册到全局实例集合
        _http_client_instances.add(self)

    def _create_session(self) -> AsyncSession:
        """创建新的 AsyncSession（带事件循环检查）"""
        try:
            # 确保在正确的事件循环中创建
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            # 如果没有运行中的事件循环，仍然创建（可能在同步上下文中）
            pass
        
        session = AsyncSession(
            impersonate=self.default_impersonate,
            verify=self.verify,
        )
        
        # 设置代理
        if self.proxy:
            session.proxies = {
                "http": self.proxy,
                "https": self.proxy,
            }
        
        self._stats["sessions_created"] += 1
        
        return session

    async def _is_session_alive(self, session: AsyncSession) -> bool:
        """检查 session 是否存活（带事件循环检查）"""
        try:
            # 检查事件循环是否匹配
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return False
            
            if not hasattr(session, "close"):
                return False
            
            if hasattr(session, "_closed") and session._closed:
                return False
            
            return True
        except RuntimeError:
            # 事件循环问题
            return False
        except Exception:
            return False

    async def _ensure_cleanup_task(self):
        """确保清理任务已启动（带事件循环检查）"""
        try:
            # 检查是否有运行中的事件循环
            asyncio.get_running_loop()
        except RuntimeError:
            # 没有运行中的事件循环，无法启动清理任务
            return
        
        if not self._cleanup_task_started:
            self._cleanup_task_started = True
            await _start_cleanup_task()
    
    async def _get_or_create_session(self, user_id: str) -> AsyncSession:
        """
        获取或创建 user_id 对应的 session
        如果 session 不存在或已失效，则创建新的
        如果池已满，移除最早的 session
        """
        # 确保清理任务已启动
        await self._ensure_cleanup_task()
        
        async with _client_pool_lock:
            # 检查是否存在
            if user_id in _client_pool:
                session_info = _client_pool[user_id]
                session = session_info["session"]
                
                # 检查是否存活
                if await self._is_session_alive(session):
                    # 移到末尾（LRU 更新）
                    _client_pool.move_to_end(user_id)
                    
                    # 如果使用次数超限，记录日志但继续使用，交给回收池异步处理
                    if session_info["usage_count"] >= self._max_session_usage:
                        if self.debug:
                            print(f"[HttpClient] Session 使用次数已超限({session_info['usage_count']}/{self._max_session_usage})，"
                                  f"继续使用，交给回收池处理: user_id={user_id}")
                    
                    return session
                else:
                    # Session 已失效，移除
                    if self.debug:
                        print(f"[HttpClient] Session 已失效，重新创建: user_id={user_id}")
                    
                    if _cleanup_queue:
                        try:
                            await _cleanup_queue.put({
                                "session": session,
                                "user_id": user_id,
                            })
                        except RuntimeError as e:
                            if "different event loop" not in str(e):
                                raise
                            # 事件循环不匹配，跳过清理
                    
                    del _client_pool[user_id]
                    self._stats["dead_sessions_removed"] += 1
            
            # 检查池是否已满
            if len(_client_pool) >= _client_pool_max_size:
                # 移除最早的（FIFO）
                oldest_user_id, oldest_info = _client_pool.popitem(last=False)
                
                if self.debug:
                    print(f"[HttpClient] Session 池已满({_client_pool_max_size})，"
                          f"移除最早的: user_id={oldest_user_id}, "
                          f"使用次数={oldest_info['usage_count']}")
                
                # 加入清理队列
                if _cleanup_queue:
                    try:
                        await _cleanup_queue.put({
                            "session": oldest_info["session"],
                            "user_id": oldest_user_id,
                        })
                    except RuntimeError as e:
                        if "different event loop" not in str(e):
                            raise
                        # 事件循环不匹配，跳过清理
                
                # 如果被移除的 session 使用次数达到上限，计入回收统计
                if oldest_info["usage_count"] >= self._max_session_usage:
                    self._stats["sessions_recycled"] += 1
            
            # 创建新的 session
            new_session = self._create_session()
            
            _client_pool[user_id] = {
                "session": new_session,
                "usage_count": 0,
                "created_at": time.time(),
            }
            
            if self.debug:
                print(f"[HttpClient] 创建新 session: user_id={user_id}, 当前池大小={len(_client_pool)}")
            
            return new_session

    async def _increment_usage(self, user_id: str):
        """增加 user_id 的 session 使用次数"""
        async with _client_pool_lock:
            if user_id in _client_pool:
                old_count = _client_pool[user_id]["usage_count"]
                _client_pool[user_id]["usage_count"] = old_count + 1
                new_count = old_count + 1
                
                # 当达到或超过使用上限时记录日志
                if self.debug and new_count >= self._max_session_usage:
                    print(f"[HttpClient] Session 使用次数: {new_count}/{self._max_session_usage}, "
                          f"user_id={user_id}, 继续使用，交给回收池处理")

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        user_id: str = "default",
        **kwargs
    ):
        """带重试机制的请求（异步，带事件循环检查）"""
        self._stats["requests"] += 1
        
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                # 检查事件循环
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    raise RuntimeError("没有运行中的事件循环")
                
                # 获取 session
                session = await self._get_or_create_session(user_id)
                
                # 设置超时
                if "timeout" not in kwargs:
                    kwargs["timeout"] = self.timeout
                
                # 发起请求
                try:
                    response = await session.request(method, url, **kwargs)
                except RuntimeError as e:
                    if "different event loop" in str(e):
                        # 事件循环不匹配，重新获取 session
                        if self.debug:
                            print(f"[HttpClient] 事件循环不匹配，重新获取 session: {e}")
                        session = await self._get_or_create_session(user_id)
                        response = await session.request(method, url, **kwargs)
                    else:
                        raise
                
                # 请求成功，增加使用次数
                await self._increment_usage(user_id)
                
                return response
                
            except Exception as e:
                last_exception = e
                self._stats["failures"] += 1
                
                # 检查是否是 proxy closed 错误
                error_str = str(e).lower()
                if "proxy" in error_str and ("closed" in error_str or "close" in error_str):
                    self._stats["proxy_close_errors"] += 1
                    
                    if self.debug or True:  # 总是显示 proxy close 错误
                        print(f"[HttpClient] 代理关闭错误 (第 {attempt + 1}/{self.max_retries} 次): "
                              f"user_id={user_id}, url={url[:50]}, 错误={e}")
                
                # 检查是否是事件循环错误
                if "different event loop" in str(e) or "no running event loop" in str(e):
                    if self.debug:
                        print(f"[HttpClient] 事件循环错误: {e}")
                    # 如果是最后一次重试，直接抛出
                    if attempt == self.max_retries - 1:
                        raise
                    # 否则等待后重试
                    await asyncio.sleep(self.retry_delay)
                    continue
                
                if attempt < self.max_retries - 1:
                    self._stats["retries"] += 1
                    
                    if self.debug:
                        print(f"[HttpClient] 请求失败，{self.retry_delay}秒后重试 "
                              f"(第 {attempt + 1}/{self.max_retries} 次): {e}")
                    
                    await asyncio.sleep(self.retry_delay)
        
        # 所有重试都失败
        if self.debug:
            print(f"[HttpClient] 请求最终失败，已重试 {self.max_retries} 次: {last_exception}")
        
        raise last_exception

    async def get(self, url: str, user_id: str = "default", session: FlowSessionWrapper = None, **kwargs):
        """GET 请求（异步）"""
        if session is not None:
            user_id = session.user_id
        return await self._request_with_retry("GET", url, user_id=user_id, **kwargs)

    async def post(self, url: str, user_id: str = "default", session: FlowSessionWrapper = None, **kwargs):
        """POST 请求（异步）"""
        if session is not None:
            user_id = session.user_id
        return await self._request_with_retry("POST", url, user_id=user_id, **kwargs)

    async def put(self, url: str, user_id: str = "default", session: FlowSessionWrapper = None, **kwargs):
        """PUT 请求（异步）"""
        if session is not None:
            user_id = session.user_id
        return await self._request_with_retry("PUT", url, user_id=user_id, **kwargs)

    async def delete(self, url: str, user_id: str = "default", session: FlowSessionWrapper = None, **kwargs):
        """DELETE 请求（异步）"""
        if session is not None:
            user_id = session.user_id
        return await self._request_with_retry("DELETE", url, user_id=user_id, **kwargs)

    async def update_proxy(self, proxy: str):
        """更新代理配置（异步）"""
        self.proxy = proxy
        
        if self.debug:
            print(f"[HttpClient] 代理已更新: {proxy}")

    # ========== 向后兼容 API（flow_session 接口）==========

    def get_flow_session(self, device_id: str = None) -> FlowSessionWrapper:
        """
        获取 flow session（同步版本，简化）
        
        注意：在异步上下文中，建议使用 await get_flow_session_async()
        
        Args:
            device_id: 可选的设备ID（用作 user_id）
                      如果提供，则使用该 device_id 绑定的 session
                      如果不提供，则自动生成一个 user_id
            
        Returns:
            FlowSessionWrapper 对象
        """
        if device_id:
            # 传统模式：使用 device_id 绑定的 session
            user_id = f"device_{device_id}"
        else:
            # 自动模式：生成新的 user_id
            # 实际的 session 创建会在第一次请求时进行
            user_id = f"auto_session_{int(time.time() * 1000000)}"
        
        return FlowSessionWrapper(self, user_id)
    
    async def get_flow_session_async(self, device_id: str = None) -> FlowSessionWrapper:
        """
        获取 flow session（异步版本，推荐）
        
        如果不传 device_id，则从全局池中智能获取一个可用的 session
        
        Args:
            device_id: 可选的设备ID（用作 user_id）
                      如果提供，则使用该 device_id 绑定的 session
                      如果不提供，则自动从池中获取可用的 session
            
        Returns:
            FlowSessionWrapper 对象
        """
        if device_id:
            # 传统模式：使用 device_id 绑定的 session
            user_id = f"device_{device_id}"
        else:
            # 自动模式：从池中智能获取
            user_id = await self._get_available_session_id()
        
        return FlowSessionWrapper(self, user_id)
    
    async def _get_available_session_id(self) -> str:
        """
        从全局池中获取一个可用的 session ID
        策略：
        1. 优先查找 usage_count < max_session_usage 的 session
        2. 如果没有可用的：
           - 池未满：创建新的
           - 池已满：触发后台清理，然后重试
        
        Returns:
            可用的 user_id
        """
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # 检查事件循环
                asyncio.get_running_loop()
            except RuntimeError:
                raise RuntimeError("没有运行中的事件循环")
            
            async with _client_pool_lock:
                # 第一优先级：查找使用次数未达到上限的 session
                for user_id, session_info in _client_pool.items():
                    if session_info["usage_count"] < self._max_session_usage:
                        # 找到可用的 session
                        if self.debug:
                            print(f"[HttpClient] 复用现有 session: {user_id}, "
                                  f"使用次数={session_info['usage_count']}/{self._max_session_usage}")
                        return user_id
                
                # 第二优先级：检查池是否已满
                if len(_client_pool) < _client_pool_max_size:
                    # 池未满，创建新的
                    new_user_id = f"auto_session_{int(time.time() * 1000)}"
                    
                    if self.debug:
                        print(f"[HttpClient] 池未满，创建新的自动 session: {new_user_id} "
                              f"(池大小: {len(_client_pool)}/{_client_pool_max_size})")
                    
                    return new_user_id
                else:
                    # 池已满且没有可用 session，触发后台清理
                    if self.debug:
                        print(f"[HttpClient] 池已满且无可用 session，触发后台清理 "
                              f"(重试 {retry_count + 1}/{max_retries})")
            
            # 在锁外触发清理
            await self._trigger_cleanup_full_sessions()
            
            # 等待一小段时间让清理完成
            await asyncio.sleep(0.1)
            retry_count += 1
        
        # 重试失败，抛出异常
        raise RuntimeError(f"无法获取可用 session: 池已满({_client_pool_max_size})且所有 session 都已达到使用上限")

    def release_flow_session(self, session: Any):
        """
        向后兼容：释放 flow session
        
        在新的设计中，session 是自动管理的，这个方法什么都不做
        
        Args:
            session: session 对象（忽略）
        """
        pass  # 新设计中自动管理，不需要手动释放

    async def _trigger_cleanup_full_sessions(self):
        """
        触发后台清理：分批清除所有达到使用上限的 session
        优先清除最早创建的 session（按 created_at 排序）
        """
        sessions_to_cleanup = []
        
        async with _client_pool_lock:
            # 找出所有达到使用上限的 session
            for user_id, session_info in _client_pool.items():
                if session_info["usage_count"] >= self._max_session_usage:
                    sessions_to_cleanup.append((
                        user_id,
                        session_info["session"],
                        session_info["created_at"],
                        session_info["usage_count"]
                    ))
            
            if not sessions_to_cleanup:
                if self.debug:
                    print(f"[HttpClient] 无需清理，没有达到上限的 session")
                return
            
            # 按创建时间排序，最早的优先
            sessions_to_cleanup.sort(key=lambda x: x[2])  # x[2] 是 created_at
            
            # 分批清理：每次清理 20% 或至少 1 个
            batch_size = max(1, len(sessions_to_cleanup) // 5)
            batch_to_cleanup = sessions_to_cleanup[:batch_size]
            
            if self.debug:
                print(f"[HttpClient] 开始分批清理: 共 {len(sessions_to_cleanup)} 个达到上限的 session，"
                      f"本次清理 {len(batch_to_cleanup)} 个")
            
            # 移除并加入清理队列
            for user_id, session, created_at, usage_count in batch_to_cleanup:
                # 从池中移除
                if user_id in _client_pool:
                    del _client_pool[user_id]
                
                # 加入清理队列（带事件循环检查）
                if _cleanup_queue:
                    try:
                        await _cleanup_queue.put({
                            "session": session,
                            "user_id": user_id,
                        })
                    except RuntimeError as e:
                        if "different event loop" not in str(e):
                            raise
                        # 事件循环不匹配，跳过清理
                
                # 更新统计
                self._stats["sessions_recycled"] += 1
                
                if self.debug:
                    age = time.time() - created_at
                    print(f"[HttpClient] 清理 session: user_id={user_id}, "
                          f"使用次数={usage_count}, 存活时间={age:.1f}秒")

    async def close(self):
        """关闭客户端（异步，带事件循环检查）"""
        self._closed = True
        
        try:
            # 检查事件循环
            asyncio.get_running_loop()
        except RuntimeError:
            # 没有运行中的事件循环，无法异步关闭
            return
        
        async with _client_pool_lock:
            # 将所有 session 加入清理队列
            for user_id, session_info in _client_pool.items():
                if _cleanup_queue:
                    try:
                        await _cleanup_queue.put({
                            "session": session_info["session"],
                            "user_id": user_id,
                        })
                    except RuntimeError as e:
                        if "different event loop" not in str(e):
                            raise
                        # 事件循环不匹配，跳过清理
            
            _client_pool.clear()
        
        if self.debug:
            print(f"[HttpClient] 已关闭，清理了所有 session")

    def __del__(self):
        try:
            # 在析构函数中无法使用 await，只清理标志
            if not self._closed:
                self._closed = True
        except:
            pass

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            **self._stats,
            "pool_size": len(_client_pool),
            "max_pool_size": _client_pool_max_size,
            "avg_usage_count": sum(info["usage_count"] for info in _client_pool.values()) / len(_client_pool) if _client_pool else 0,
        }


def get_global_pool_stats() -> Dict[str, Any]:
    """获取全局池统计信息"""
    if not _client_pool:
        return {
            "pool_size": 0,
            "max_pool_size": _client_pool_max_size,
            "avg_usage_count": 0,
            "min_usage_count": 0,
            "max_usage_count": 0,
        }
    
    usage_counts = [info["usage_count"] for info in _client_pool.values()]
    
    return {
        "pool_size": len(_client_pool),
        "max_pool_size": _client_pool_max_size,
        "avg_usage_count": sum(usage_counts) / len(usage_counts),
        "min_usage_count": min(usage_counts),
        "max_usage_count": max(usage_counts),
    }


def clear_global_pool():
    """清空全局池（用于测试）"""
    _client_pool.clear()


async def shutdown_all():
    """关闭所有 HTTP 客户端和清理任务（用于程序退出）"""
    # 关闭所有客户端实例
    for client in list(_http_client_instances):
        try:
            if hasattr(client, 'close') and not client._closed:
                await client.close()
        except Exception:
            pass
    
    # 停止清理任务
    await _stop_cleanup_task()
