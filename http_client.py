"""
高性能统一 HTTP 客户端（优化版）
支持连接池、自动扩容、代理切换、重试、健康检测、Keep-Alive 等
线程安全 + 低CPU占用 + 实时统计信息
"""

import time
import threading
import asyncio
import urllib3
from typing import Optional, Dict, Any, Union, List
from curl_cffi import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class HttpClient:
    """支持连接池与自动扩容的高性能 HTTP 客户端"""

    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        verify: bool = False,
        default_impersonate: Optional[str] = "okhttp4_android",
        enable_keep_alive: bool = True,
        pool_initial_size: int = 1000,
        pool_max_size: int = 5000,
        pool_grow_step: int = 50,
        health_check_interval: int = 15,
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

        # 连接池配置
        self.pool_initial_size = max(1, pool_initial_size)
        self.pool_max_size = max(self.pool_initial_size, pool_max_size)
        self.pool_grow_step = max(1, pool_grow_step)
        self.health_check_interval = max(5, health_check_interval)

        # 池结构
        self._pool: List[requests.Session] = []
        self._lock = threading.Lock()
        self._pool_cond = threading.Condition(self._lock)
        self._session_count = 0
        self._closed = False

        # 统计数据
        self._stats = {"requests": 0, "failures": 0, "retries": 0}

        # 初始化连接池
        for _ in range(self.pool_initial_size):
            self._create_session()

        # 后台健康检测线程
        self._health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self._health_thread.start()

        if self.debug:
            print(f"[HttpClient] 初始化完成，连接数={self._session_count}")

    # ---------------------- 连接池管理 ----------------------

    def _create_session(self):
        """创建新 Session"""
        try:
            s = requests.Session()
            s.headers.update({"Connection": "keep-alive"})
            if self.proxy:
                s.proxies = {"http": self.proxy, "https": self.proxy}
            with self._lock:
                self._pool.append(s)
                self._session_count += 1
            if self.debug:
                print(f"[HttpClient] 创建新连接，总数={self._session_count}")
        except Exception as e:
            if self.debug:
                print(f"[HttpClient] 创建 Session 失败: {e}")
            raise

    def _grow_pool(self):
        """扩容连接池"""
        grow = min(self.pool_grow_step, self.pool_max_size - self._session_count)
        for _ in range(grow):
            self._create_session()
        if self.debug:
            print(f"[HttpClient] 池扩容: 当前连接={self._session_count}")

    def _get_session(self) -> requests.Session:
        """线程安全地获取 Session"""
        import time
        wait_start = time.time()
        max_wait_time = 30.0  # 最多等待30秒
        
        with self._pool_cond:
            while not self._closed:
                if self._pool:
                    elapsed = time.time() - wait_start
                    if elapsed > 0.1 and self.debug:
                        print(f"[HttpClient] 等待 Session 耗时: {elapsed:.3f}s")
                    return self._pool.pop()
                
                elapsed = time.time() - wait_start
                if elapsed > max_wait_time:
                    if self.debug:
                        print(f"[HttpClient] 等待 Session 超时（{max_wait_time}秒），当前池大小: {len(self._pool)}, 总连接数: {self._session_count}")
                    raise TimeoutError(f"等待 Session 超时（{max_wait_time}秒）")
                
                if self._session_count < self.pool_max_size:
                    self._grow_pool()
                    continue
                
                remaining_time = max_wait_time - elapsed
                self._pool_cond.wait(timeout=min(0.5, remaining_time))
        raise RuntimeError("HttpClient 已关闭，无法获取连接")

    def _release_session(self, session: requests.Session):
        """释放 Session"""
        if self._closed:
            try:
                session.close()
            except Exception:
                pass
            return

        # 检查 Session 是否已关闭
        try:
            # 尝试检查 Session 状态
            if hasattr(session, 'closed') and session.closed:
                # Session 已关闭，不放入池中
                with self._lock:
                    self._session_count -= 1
                if self.debug:
                    print("[HttpClient] Session 已关闭，移除连接")
                return
        except Exception:
            # 无法检查状态，假设 Session 可能有问题，不放入池中
            try:
                session.close()
            except Exception:
                pass
            with self._lock:
                self._session_count -= 1
            if self.debug:
                print("[HttpClient] Session 状态异常，移除连接")
            return

        if not self.enable_keep_alive:
            try:
                session.close()
            except Exception:
                pass
            with self._lock:
                self._session_count -= 1
            if self.debug:
                print("[HttpClient] Keep-Alive关闭，销毁连接")
        else:
            # 将 Session 放回池中
            with self._pool_cond:
                self._pool.append(session)
                self._pool_cond.notify()

    # ---------------------- 健康检测 ----------------------

    def _health_check_loop(self):
        """后台健康检测"""
        while not self._closed:
            time.sleep(self.health_check_interval)
            with self._lock:
                sessions_snapshot = list(self._pool)

            alive = []
            for s in sessions_snapshot:
                try:
                    # 检查 Session 是否已关闭
                    if hasattr(s, 'closed') and s.closed:
                        try:
                            s.close()
                        except Exception:
                            pass
                        if self.debug:
                            print("[HttpClient] 检测到已关闭的连接，移除")
                        continue
                    
                    # 尝试检查 Session 状态
                    # 注意：curl_cffi 的 Session 对象可能没有 curl 属性，或者 getinfo 需要参数
                    # 我们只检查 closed 属性，不调用 getinfo
                    if hasattr(s, "closed") and s.closed:
                        continue
                    
                    # 如果 Session 有 curl 属性，尝试简单检查（但不调用 getinfo）
                    # 因为 getinfo 需要参数，我们只检查对象是否存在
                    if hasattr(s, "curl"):
                        # 不调用 getinfo，只检查 curl 对象是否存在
                        pass
                    
                    alive.append(s)
                except Exception as e:
                    # Session 已损坏或关闭
                    try:
                        s.close()
                    except Exception:
                        pass
                    if self.debug:
                        print(f"[HttpClient] 检测到失效连接，已关闭: {e}")

            with self._lock:
                before = len(self._pool)
                self._pool = [s for s in self._pool if s in alive]
                after = len(self._pool)
                if before != after and self.debug:
                    print(f"[HttpClient] 健康检测: 移除失效连接 {before - after}")

    # ---------------------- 请求构造 ----------------------

    def _prepare_request_kwargs(
        self,
        headers: Optional[Dict[str, Any]] = None,
        data: Optional[Union[str, bytes]] = None,
        json: Optional[Dict] = None,
        params: Optional[Dict] = None,
        impersonate: Optional[str] = None,
        http_version: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        req = {"verify": self.verify, "timeout": self.timeout}
        if headers:
            req["headers"] = headers
        if data is not None:
            req["data"] = data
        if json is not None:
            req["json"] = json
        if params is not None:
            req["params"] = params
        impersonate = impersonate or self.default_impersonate
        if impersonate:
            req["impersonate"] = impersonate
        if http_version:
            req["http_version"] = http_version
        req.update(kwargs)
        return req

    # ---------------------- 请求执行 ----------------------

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """带重试机制的请求执行"""
        import time
        request_start = time.time()
        attempt = 0
        session = None
        while attempt < self.max_retries:
            try:
                if self.debug:
                    print(f"[HttpClient] 尝试获取 Session (尝试 {attempt + 1}/{self.max_retries})...")
                session_start = time.time()
                session = self._get_session()
                session_elapsed = time.time() - session_start
                if session_elapsed > 0.1 or self.debug:
                    print(f"[HttpClient] 获取 Session 耗时: {session_elapsed:.3f}s")
                
                if self.debug:
                    print(f"[HttpClient] 开始执行 {method} 请求: {url[:80]}...")
                func = getattr(session, method.lower())
                request_elapsed_start = time.time()
                resp = func(url, **kwargs)
                request_elapsed = time.time() - request_elapsed_start
                if self.debug or request_elapsed > 5.0:
                    print(f"[HttpClient] 请求完成，耗时: {request_elapsed:.3f}s, 状态码: {resp.status_code}")
                
                self._stats["requests"] += 1
                # 请求成功，释放 Session 回池
                self._release_session(session)
                session = None
                
                total_elapsed = time.time() - request_start
                if total_elapsed > 5.0 or self.debug:
                    print(f"[HttpClient] 总耗时: {total_elapsed:.3f}s")
                return resp
            except (requests.RequestsError, ConnectionError, TimeoutError) as e:
                self._stats["failures"] += 1
                # Session 可能已损坏，关闭并移除
                if session is not None:
                    try:
                        session.close()
                    except Exception:
                        pass
                    with self._lock:
                        self._session_count -= 1
                    session = None
                
                attempt += 1
                if attempt >= self.max_retries:
                    raise
                self._stats["retries"] += 1
                if self.debug:
                    print(f"[HttpClient] 网络错误({e})，重试 {attempt}/{self.max_retries}")
                time.sleep(self.retry_delay)
            except Exception as e:
                self._stats["failures"] += 1
                # 检查是否是网络相关错误（需要重试）
                error_str = str(e).lower()
                is_network_error = (
                    "session is closed" in error_str or 
                    "closed" in error_str or
                    "connection to proxy closed" in error_str or
                    "connection closed" in error_str or
                    "proxy" in error_str and "closed" in error_str or
                    "connection" in error_str and "closed" in error_str
                )
                
                if is_network_error:
                    # 网络错误，移除 Session 并重试
                    if session is not None:
                        try:
                            session.close()
                        except Exception:
                            pass
                        with self._lock:
                            self._session_count -= 1
                        session = None
                    
                    attempt += 1
                    if attempt >= self.max_retries:
                        raise
                    self._stats["retries"] += 1
                    if self.debug:
                        print(f"[HttpClient] 网络连接错误({e})，重试 {attempt}/{self.max_retries}")
                    time.sleep(self.retry_delay)
                    continue
                
                # 其他错误，释放 Session 后抛出
                if session is not None:
                    self._release_session(session)
                    session = None
                if self.debug:
                    print(f"[HttpClient] 未知错误: {e}")
                raise

    # ---------------------- 公共接口 ----------------------

    def get(self, url: str, headers=None, params=None, impersonate=None, **kwargs):
        req = self._prepare_request_kwargs(headers=headers, params=params, impersonate=impersonate, **kwargs)
        return self._request_with_retry("GET", url, **req)

    def post(self, url: str, headers=None, data=None, json=None, impersonate=None, http_version=None, **kwargs):
        req = self._prepare_request_kwargs(
            headers=headers, data=data, json=json, impersonate=impersonate, http_version=http_version, **kwargs
        )
        return self._request_with_retry("POST", url, **req)

    def put(self, url: str, headers=None, data=None, json=None, **kwargs):
        req = self._prepare_request_kwargs(headers=headers, data=data, json=json, **kwargs)
        return self._request_with_retry("PUT", url, **req)

    def delete(self, url: str, headers=None, **kwargs):
        req = self._prepare_request_kwargs(headers=headers, **kwargs)
        return self._request_with_retry("DELETE", url, **kwargs)
    
    # ---------------------- 异步接口 ----------------------
    
    async def get_async(self, url: str, headers=None, params=None, impersonate=None, **kwargs):
        """异步 GET 请求（在线程池中执行，不阻塞事件循环）"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get, url, headers, params, impersonate, **kwargs)
    
    async def post_async(self, url: str, headers=None, data=None, json=None, impersonate=None, http_version=None, **kwargs):
        """异步 POST 请求（在线程池中执行，不阻塞事件循环）"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.post, url, headers, data, json, impersonate, http_version, **kwargs)
    
    async def put_async(self, url: str, headers=None, data=None, json=None, **kwargs):
        """异步 PUT 请求（在线程池中执行，不阻塞事件循环）"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.put, url, headers, data, json, **kwargs)
    
    async def delete_async(self, url: str, headers=None, **kwargs):
        """异步 DELETE 请求（在线程池中执行，不阻塞事件循环）"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.delete, url, headers, **kwargs)

    # ---------------------- 实用工具 ----------------------

    def update_proxy(self, proxy: Optional[str], force_recreate=False):
        """动态更新代理"""
        self.proxy = proxy
        with self._lock:
            if force_recreate:
                for s in self._pool:
                    try:
                        s.close()
                    except Exception:
                        pass
                self._pool.clear()
                self._session_count = 0
                self._grow_pool()
            else:
                for s in self._pool:
                    s.proxies = {"http": proxy, "https": proxy}
        if self.debug:
            print(f"[HttpClient] 已更新代理为: {proxy}")

    def get_stats(self):
        """返回当前统计数据"""
        return dict(self._stats)

    def close(self):
        """关闭所有连接并停止健康检测"""
        self._closed = True
        with self._lock:
            for s in self._pool:
                try:
                    s.close()
                except Exception:
                    pass
            self._pool.clear()
            self._session_count = 0
        if self.debug:
            print("[HttpClient] 已关闭连接池")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
