"""
基于 user_id 的全局 Session 管理 HTTP 客户端
特性：
 - 每个 user_id 对应一个独立的 session
 - 最多保持 5000 个 session，超过时移除最早的
 - 异步清理队列，避免阻塞主流程
 - 可配置每个 user_id 的 session 使用次数
 - 完整的重试机制和错误处理
"""
import time
import threading
import asyncio
import queue
from typing import Optional, Dict, Any
from collections import OrderedDict

from curl_cffi import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# 全局 Session 池（基于 user_id）
_client_pool_lock = threading.RLock()
_client_pool: OrderedDict[str, Dict[str, Any]] = OrderedDict()
_client_pool_max_size = 5000

# 异步清理队列
_cleanup_queue = queue.Queue()
_cleanup_thread = None


def _start_cleanup_thread():
    """启动清理线程（单例）"""
    global _cleanup_thread
    if _cleanup_thread is None or not _cleanup_thread.is_alive():
        _cleanup_thread = threading.Thread(target=_cleanup_worker, daemon=True)
        _cleanup_thread.start()


def _cleanup_worker():
    """异步清理 session 的工作线程"""
    while True:
        try:
            session_info = _cleanup_queue.get(timeout=1)
            if session_info is None:  # 退出信号
                break
            
            session = session_info.get("session")
            user_id = session_info.get("user_id", "unknown")
            
            try:
                if session:
                    session.close()
            except Exception as e:
                pass  # 忽略清理错误
            
        except queue.Empty:
            continue
        except Exception:
            continue


class _StreamWrapper:
    """包装流响应，close 时释放 session"""
    def __init__(self, client: "HttpClient", user_id: str, resp):
        self._client = client
        self._user_id = user_id
        self._resp = resp

    def __getattr__(self, item):
        return getattr(self._resp, item)

    def close(self):
        try:
            self._resp.close()
        finally:
            # 流关闭时增加使用次数
            self._client._increment_usage(self._user_id)


class HttpClient:
    """基于 user_id 的全局 Session 管理 HTTP 客户端"""

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

        # Session 管理配置
        self._max_session_usage = max(10, max_session_usage)
        self._max_pool_size = max(100, max_pool_size)
        
        # 更新全局最大值
        global _client_pool_max_size
        _client_pool_max_size = self._max_pool_size

        # 统计信息
        self._stats = {
            "requests": 0,
            "failures": 0,
            "retries": 0,
            "proxy_close_errors": 0,
            "dead_sessions_removed": 0,
            "sessions_created": 0,
            "sessions_recycled": 0,
        }

        self._closed = False

        # 启动清理线程
        _start_cleanup_thread()

        if self.debug:
            print(f"[HttpClient] Init completed, max_pool_size={self._max_pool_size}, "
                  f"max_session_usage={self._max_session_usage}")

    def _create_session(self) -> requests.Session:
        """创建新的 Session"""
        session = requests.Session(
            timeout=self.timeout,
            verify=self.verify,
            impersonate=self.default_impersonate,
            proxies={"http": self.proxy, "https": self.proxy} if self.proxy else None,
        )

        # 配置 keep-alive
        if self.enable_keep_alive:
            session.headers.update({"Connection": "keep-alive"})
        else:
            session.headers.update({"Connection": "close"})

        self._stats["sessions_created"] += 1
        return session

    def _is_session_alive(self, session) -> bool:
        """检查 session 是否存活"""
        try:
            if not hasattr(session, "request"):
                return False
            
            if hasattr(session, "_closed") and session._closed:
                return False
            
            if hasattr(session, "curl"):
                if session.curl is None:
                    return False
            
            if hasattr(session, "cookies"):
                try:
                    _ = len(session.cookies)
                except Exception:
                    return False
            
            return True
        except Exception:
            return False

    def _get_or_create_session(self, user_id: str) -> requests.Session:
        """
        获取或创建 user_id 对应的 session
        如果 session 不存在或已失效，则创建新的
        如果池已满，移除最早的 session
        """
        with _client_pool_lock:
            # 检查是否存在
            if user_id in _client_pool:
                session_info = _client_pool[user_id]
                session = session_info["session"]
                
                # 检查是否存活
                if self._is_session_alive(session):
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
                    
                    _cleanup_queue.put({
                        "session": session,
                        "user_id": user_id,
                    })
                    
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
                _cleanup_queue.put({
                    "session": oldest_info["session"],
                    "user_id": oldest_user_id,
                })
                
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

    def _increment_usage(self, user_id: str):
        """增加 user_id 的 session 使用次数"""
        with _client_pool_lock:
            if user_id in _client_pool:
                old_count = _client_pool[user_id]["usage_count"]
                _client_pool[user_id]["usage_count"] = old_count + 1
                new_count = old_count + 1
                
                # 当达到或超过使用上限时记录日志
                if self.debug and new_count >= self._max_session_usage:
                    print(f"[HttpClient] Session 使用次数: {new_count}/{self._max_session_usage}, "
                          f"user_id={user_id}, 继续使用，交给回收池处理")

    def _request_with_retry(
        self,
        method: str,
        url: str,
        user_id: str = "default",
        **kwargs
    ) -> requests.Response:
        """带重试机制的请求"""
        self._stats["requests"] += 1
        
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                # 获取或创建 session
                session = self._get_or_create_session(user_id)
                
                # 执行请求
                response = session.request(method, url, **kwargs)
                
                # 成功，增加使用次数
                self._increment_usage(user_id)
                
                return response
                
            except Exception as e:
                last_exception = e
                self._stats["retries"] += 1
                
                # 判断错误类型
                error_str = str(e).lower()
                is_proxy_closed = "proxy" in error_str and "close" in error_str
                is_timeout = "timeout" in error_str or "timed out" in error_str
                is_network_error = "connection" in error_str or "network" in error_str
                
                # 统计 proxy close 错误
                if is_proxy_closed:
                    self._stats["proxy_close_errors"] += 1
                
                if self.debug or is_proxy_closed:
                    if is_proxy_closed:
                        error_type = "🔴 代理/连接关闭"
                        print(f"[HttpClient] ⚠️ PROXY CLOSE 错误！重试 {attempt + 1}/{self.max_retries}")
                        print(f"  URL: {url[:100]}")
                        print(f"  user_id: {user_id}")
                        print(f"  错误: {str(e)[:200]}")
                        with _client_pool_lock:
                            print(f"  池状态: 总数={len(_client_pool)}")
                    elif is_timeout:
                        error_type = "请求超时"
                    elif is_network_error:
                        error_type = "网络错误"
                    else:
                        error_type = "请求失败"
                    
                    if not is_proxy_closed or self.debug:
                        print(f"[HttpClient] 重试 {attempt + 1}/{self.max_retries}: {error_type} -> {e}")
                
                # 如果是 proxy close 或网络错误，强制重新创建 session
                if is_proxy_closed or is_network_error:
                    with _client_pool_lock:
                        if user_id in _client_pool:
                            session_info = _client_pool[user_id]
                            _cleanup_queue.put({
                                "session": session_info["session"],
                                "user_id": user_id,
                            })
                            del _client_pool[user_id]
                            self._stats["dead_sessions_removed"] += 1
                
                # 最后一次重试失败
                if attempt == self.max_retries - 1:
                    self._stats["failures"] += 1
                    raise last_exception
                
                # 延迟后重试
                if self.retry_delay > 0:
                    time.sleep(self.retry_delay)
        
        # 不应该到达这里
        self._stats["failures"] += 1
        raise last_exception

    # ========== 公共 API ==========

    def request(
        self,
        method: str,
        url: str,
        user_id: str = None,
        session: Any = None,  # 向后兼容：旧的 session 参数
        stream: bool = False,
        **kwargs
    ) -> Any:
        """
        发送 HTTP 请求
        
        Args:
            method: HTTP 方法
            url: 请求 URL
            user_id: 用户ID（用于 session 管理）
            session: 向后兼容的 session 参数（优先级高于 user_id）
            stream: 是否返回流
            **kwargs: 其他请求参数
        """
        # 向后兼容：如果提供了 session，从中提取 user_id
        if session is not None:
            # 如果是 FlowSessionWrapper，提取 user_id
            if isinstance(session, FlowSessionWrapper):
                user_id = session._user_id
            else:
                # 使用 session 对象的 id 作为 user_id
                user_id = f"session_{id(session)}"
        
        # 如果还没有 user_id，使用默认值
        if user_id is None:
            user_id = "default"
        
        # 从 kwargs 中移除 session 参数（避免传递给底层）
        kwargs.pop('session', None)
        
        response = self._request_with_retry(method, url, user_id, stream=stream, **kwargs)
        
        if stream:
            return _StreamWrapper(self, user_id, response)
        else:
            # 非流式请求，增加使用次数
            self._increment_usage(user_id)
            return response

    def get(self, url: str, user_id: str = None, session: Any = None, **kwargs) -> requests.Response:
        """GET 请求"""
        return self.request("GET", url, user_id, session, **kwargs)

    def post(self, url: str, user_id: str = None, session: Any = None, **kwargs) -> requests.Response:
        """POST 请求"""
        return self.request("POST", url, user_id, session, **kwargs)

    def put(self, url: str, user_id: str = None, session: Any = None, **kwargs) -> requests.Response:
        """PUT 请求"""
        return self.request("PUT", url, user_id, session, **kwargs)

    def delete(self, url: str, user_id: str = None, session: Any = None, **kwargs) -> requests.Response:
        """DELETE 请求"""
        return self.request("DELETE", url, user_id, session, **kwargs)

    def head(self, url: str, user_id: str = None, session: Any = None, **kwargs) -> requests.Response:
        """HEAD 请求"""
        return self.request("HEAD", url, user_id, session, **kwargs)

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with _client_pool_lock:
            pool_size = len(_client_pool)
            
            # 计算平均使用次数
            if pool_size > 0:
                total_usage = sum(info["usage_count"] for info in _client_pool.values())
                avg_usage = total_usage / pool_size
            else:
                avg_usage = 0
        
        return {
            **self._stats,
            "pool_size": pool_size,
            "pool_max_size": _client_pool_max_size,
            "avg_usage_count": avg_usage,
        }

    def clear_user_session(self, user_id: str):
        """清除指定 user_id 的 session"""
        with _client_pool_lock:
            if user_id in _client_pool:
                session_info = _client_pool[user_id]
                _cleanup_queue.put({
                    "session": session_info["session"],
                    "user_id": user_id,
                })
                del _client_pool[user_id]
                
                if self.debug:
                    print(f"[HttpClient] 清除 session: user_id={user_id}")

    def update_proxy(self, proxy: str):
        """更新代理配置"""
        self.proxy = proxy
        if self.debug:
            print(f"[HttpClient] 代理已更新: {proxy}")

    # ========== 向后兼容 API（flow_session 接口）==========

    def get_flow_session(self, device_id: str = None) -> FlowSessionWrapper:
        """
        获取 flow session（自动管理版本）
        
        如果不传 device_id，则从全局池中自动获取一个可用的 session
        （使用次数未达到上限的 session）
        
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
            # 自动模式：从池中获取可用的 session
            user_id = self._get_available_session_id()
        
        return FlowSessionWrapper(self, user_id)
    
    def _get_available_session_id(self) -> str:
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
            with _client_pool_lock:
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
                    import time
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
            self._trigger_cleanup_full_sessions()
            
            # 等待一小段时间让清理完成
            import time
            time.sleep(0.1)
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

    def _trigger_cleanup_full_sessions(self):
        """
        触发后台清理：分批清除所有达到使用上限的 session
        优先清除最早创建的 session（按 created_at 排序）
        """
        sessions_to_cleanup = []
        
        with _client_pool_lock:
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
                
                # 加入清理队列
                _cleanup_queue.put({
                    "session": session,
                    "user_id": user_id,
                })
                
                # 更新统计
                self._stats["sessions_recycled"] += 1
                
                if self.debug:
                    import time
                    age = time.time() - created_at
                    print(f"[HttpClient] 清理 session: user_id={user_id}, "
                          f"使用次数={usage_count}, 存活时间={age:.1f}秒")

    def close(self):
        """关闭客户端"""
        self._closed = True
        
        with _client_pool_lock:
            # 将所有 session 加入清理队列
            for user_id, session_info in _client_pool.items():
                _cleanup_queue.put({
                    "session": session_info["session"],
                    "user_id": user_id,
                })
            
            _client_pool.clear()
        
        if self.debug:
            print(f"[HttpClient] 已关闭，清理了所有 session")

    def __del__(self):
        try:
            self.close()
        except:
            pass


# ========== 向后兼容 API（基于 flow_session 的旧接口）==========

def get_flow_session_client(
    proxy: Optional[str] = None,
    timeout: int = 30,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    max_session_usage: int = 100,
    max_pool_size: int = 5000,
    debug: bool = False,
) -> HttpClient:
    """
    创建一个 HttpClient 实例（向后兼容）
    
    注意：新的设计中，session 管理基于 user_id
    """
    return HttpClient(
        proxy=proxy,
        timeout=timeout,
        max_retries=max_retries,
        retry_delay=retry_delay,
        max_session_usage=max_session_usage,
        max_pool_size=max_pool_size,
        debug=debug,
    )


class FlowSessionWrapper:
    """
    向后兼容的 flow_session 包装器
    将旧的 flow_session API 适配到新的 user_id 模式
    """
    def __init__(self, client: HttpClient, user_id: str):
        self._client = client
        self._user_id = user_id

    def request(self, method: str, url: str, **kwargs):
        return self._client.request(method, url, self._user_id, **kwargs)

    def get(self, url: str, **kwargs):
        return self._client.get(url, self._user_id, **kwargs)

    def post(self, url: str, **kwargs):
        return self._client.post(url, self._user_id, **kwargs)


# ========== 全局辅助函数 ==========

def get_global_pool_stats() -> Dict[str, Any]:
    """获取全局池统计信息"""
    with _client_pool_lock:
        pool_size = len(_client_pool)
        
        if pool_size > 0:
            usage_counts = [info["usage_count"] for info in _client_pool.values()]
            avg_usage = sum(usage_counts) / len(usage_counts)
            max_usage = max(usage_counts)
            min_usage = min(usage_counts)
        else:
            avg_usage = max_usage = min_usage = 0
        
        return {
            "pool_size": pool_size,
            "pool_max_size": _client_pool_max_size,
            "avg_usage_count": avg_usage,
            "max_usage_count": max_usage,
            "min_usage_count": min_usage,
            "cleanup_queue_size": _cleanup_queue.qsize(),
        }


def clear_global_pool():
    """清空全局池"""
    with _client_pool_lock:
        for user_id, session_info in _client_pool.items():
            _cleanup_queue.put({
                "session": session_info["session"],
                "user_id": user_id,
            })
        
        _client_pool.clear()


if __name__ == "__main__":
    # 简单测试
    client = HttpClient(debug=True, max_session_usage=5, max_pool_size=10)
    
    print("\n测试1: 同一 user_id 多次请求")
    for i in range(7):
        try:
            resp = client.get("https://httpbin.org/get", user_id="user_001")
            print(f"  请求 {i+1}: 成功, 状态码={resp.status_code}")
        except Exception as e:
            print(f"  请求 {i+1}: 失败, 错误={e}")
    
    print(f"\n当前池状态: {client.get_stats()}")
    
    print("\n测试2: 多个不同 user_id")
    for i in range(15):
        try:
            resp = client.get("https://httpbin.org/get", user_id=f"user_{i:03d}")
            print(f"  user_{i:03d}: 成功")
        except Exception as e:
            print(f"  user_{i:03d}: 失败")
    
    print(f"\n最终池状态: {client.get_stats()}")
    print(f"全局池统计: {get_global_pool_stats()}")
    
    client.close()
