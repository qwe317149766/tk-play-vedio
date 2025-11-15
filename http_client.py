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
        
        # Session 使用计数（用于跟踪每个 Session 的使用次数）
        # key: Session 对象 id, value: 使用次数
        self._session_usage_count: Dict[int, int] = {}
        # 每个 Session 最多使用次数：根据连接池大小动态调整
        # 如果连接池较小，增加使用次数；如果连接池较大，减少使用次数
        # 公式：max_usage = max(3, pool_max_size // 10)
        self._max_session_usage = max(3, self.pool_max_size // 10) if self.pool_max_size < 100 else 3

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
        """创建新 Session（线程安全）"""
        try:
            s = requests.Session()
            s.headers.update({"Connection": "keep-alive"})
            if self.proxy:
                s.proxies = {"http": self.proxy, "https": self.proxy}
            # 使用 _pool_cond（它内部包含 _lock），避免死锁
            with self._pool_cond:
                self._pool.append(s)
                self._session_count += 1
                # 初始化 Session 使用计数
                session_id = id(s)
                self._session_usage_count[session_id] = 0
                # 通知等待的线程
                self._pool_cond.notify()
            if self.debug:
                print(f"[HttpClient] 创建新连接，总数={self._session_count}, 池大小={len(self._pool)}")
        except Exception as e:
            if self.debug:
                print(f"[HttpClient] 创建 Session 失败: {e}")
            raise

    def _grow_pool(self):
        """扩容连接池"""
        # 如果连接池接近耗尽，加快扩容速度
        available_sessions = len(self._pool)
        if available_sessions == 0 and self._session_count < self.pool_max_size:
            # 紧急扩容：一次创建更多连接
            grow = min(max(self.pool_grow_step * 2, 5), self.pool_max_size - self._session_count)
        else:
            grow = min(self.pool_grow_step, self.pool_max_size - self._session_count)
        
        for _ in range(grow):
            self._create_session()
        if self.debug:
            print(f"[HttpClient] 池扩容: 当前连接={self._session_count}, 可用连接={len(self._pool)}")

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
                        print(f"[HttpClient] 等待 Session 超时（{max_wait_time}秒），当前池大小: {len(self._pool)}, 总连接数: {self._session_count}, 最大连接数: {self.pool_max_size}")
                    raise TimeoutError(f"等待 Session 超时（{max_wait_time}秒），连接池可能已耗尽")
                
                # 如果连接池为空且未达到上限，立即扩容
                if self._session_count < self.pool_max_size:
                    # 紧急扩容：如果池为空，加快扩容
                    if len(self._pool) == 0:
                        # 一次创建更多连接
                        grow_count = min(max(self.pool_grow_step * 3, 10), self.pool_max_size - self._session_count)
                        for _ in range(grow_count):
                            try:
                                self._create_session()
                            except Exception as e:
                                if self.debug:
                                    print(f"[HttpClient] 紧急扩容时创建连接失败: {e}")
                                break
                    else:
                        self._grow_pool()
                    continue
                
                remaining_time = max_wait_time - elapsed
                self._pool_cond.wait(timeout=min(0.5, remaining_time))
        raise RuntimeError("HttpClient 已关闭，无法获取连接")

    def get_flow_session(self) -> requests.Session:
        """
        获取流程专用 Session（流程级Session）
        每个流程应该使用同一个Session，流程结束后调用 release_flow_session 销毁
        如果连接池为空，立即创建新连接，避免等待超时
        
        Returns:
            Session 对象
        """
        # 优化：先尝试快速从连接池获取，如果池为空或无法获取锁，直接创建新 session 而不等待
        # 使用非阻塞方式获取锁，避免死锁和超时
        session = None
        try:
            # 尝试非阻塞获取锁（不等待，避免阻塞）
            if self._pool_cond.acquire(blocking=False):
                try:
                    if self._pool:
                        # 连接池中有可用 session，直接获取
                        session = self._pool.pop()
                        # 检查 session 是否可用（尝试发送一个简单的请求测试）
                        # requests.Session 没有 closed 属性，需要通过其他方式检查
                        # 简单策略：总是创建新的 session，不从池中复用（避免 session 已关闭的问题）
                        if self.debug:
                            print(f"[HttpClient] 从连接池获取Session，但为避免Session关闭问题，销毁并创建新Session")
                        try:
                            session.close()
                        except:
                            pass
                        session = None
                        # if self.debug:
                        #     print(f"[HttpClient] 从连接池获取流程Session，剩余池大小: {len(self._pool)}")
                finally:
                    self._pool_cond.release()
            else:
                # 无法获取锁，直接创建新 session（避免阻塞）
                if self.debug:
                    print(f"[HttpClient] 无法获取连接池锁，直接创建新Session（避免阻塞）")
        except Exception as e:
            if self.debug:
                print(f"[HttpClient] 获取流程Session时异常: {e}")
        
        # 如果从连接池获取失败或session已关闭，直接创建新 session（不等待，不阻塞，不调用 _create_session 避免锁竞争）
        if session is None:
            try:
                # 直接创建新 session，不经过连接池，避免锁竞争和阻塞
                session = requests.Session()
                session.headers.update({"Connection": "keep-alive"})
                if self.proxy:
                    session.proxies = {"http": self.proxy, "https": self.proxy}
                # 更新计数（使用独立的锁，避免与连接池锁竞争）
                # 注意：这里使用 _lock 而不是 _pool_cond，避免死锁
                # 使用非阻塞方式获取锁，避免阻塞
                try:
                    if self._lock.acquire(blocking=False):
                        try:
                            self._session_count += 1
                            session_id = id(session)
                            self._session_usage_count[session_id] = 0
                        finally:
                            self._lock.release()
                    else:
                        # 如果无法获取锁，仍然创建 session，但不更新计数（避免阻塞）
                        if self.debug:
                            print(f"[HttpClient] 无法获取锁更新计数，但继续创建Session")
                except Exception as lock_error:
                    # 锁操作失败，仍然创建 session，但不更新计数（避免阻塞）
                    if self.debug:
                        print(f"[HttpClient] 更新计数时异常: {lock_error}，但继续创建Session")
                
                if self.debug:
                    print(f"[HttpClient] 直接创建新流程Session（不经过连接池），总数={self._session_count}")
            except Exception as e:
                if self.debug:
                    print(f"[HttpClient] 直接创建流程Session失败: {e}")
                raise
        
        return session
    
    def release_flow_session(self, session: requests.Session):
        """
        释放流程专用 Session（流程级Session）
        流程结束后调用此方法，直接销毁Session，不放入池中
        释放后立即触发连接池扩容，确保后续流程能及时获取Session
        
        Args:
            session: 要释放的 Session
        """
        if session is None:
            return
            
        try:
            session.close()
        except Exception:
            pass
        
        # 更新计数（使用非阻塞方式获取锁，避免阻塞）
        try:
            if self._lock.acquire(blocking=False):
                try:
                    self._session_count -= 1
                    session_id = id(session)
                    if session_id in self._session_usage_count:
                        del self._session_usage_count[session_id]
                finally:
                    self._lock.release()
            else:
                # 无法获取锁，仍然关闭 session，但不更新计数（避免阻塞）
                if self.debug:
                    print(f"[HttpClient] 释放流程Session时无法获取锁更新计数，但继续关闭Session（避免阻塞）")
        except Exception as e:
            # 锁操作失败，仍然关闭 session，但不更新计数（避免阻塞）
            if self.debug:
                print(f"[HttpClient] 释放流程Session时更新计数异常: {e}，但继续关闭Session（避免阻塞）")
        
        # 释放流程Session后，尝试触发连接池扩容，确保后续流程能及时获取Session
        # 策略：至少保持连接池大小 >= pool_initial_size，以便下一轮流程能立即获取
        # 使用非阻塞方式，避免死锁和超时
        # 注意：不调用 _create_session()，而是直接创建 session 并添加到池中，避免锁竞争
        try:
            # 尝试快速获取锁，如果获取不到就跳过扩容（避免阻塞）
            if self._pool_cond.acquire(blocking=False):
                try:
                    pool_size = len(self._pool)
                    # 计算需要补充的连接数：至少保持 pool_initial_size
                    target_size = max(self.pool_initial_size, pool_size + 1)  # 至少补充1个，确保有可用连接
                    if pool_size < target_size and self._session_count < self.pool_max_size:
                        # 连接池不足，立即扩容（但限制扩容数量，避免耗时过长）
                        # 注意：直接创建 session 并添加到池中，不调用 _create_session() 避免锁竞争
                        try:
                            grow_count = min(
                                target_size - pool_size,
                                self.pool_max_size - self._session_count,
                                3  # 最多一次扩容3个，避免阻塞太久
                            )
                            if grow_count > 0:
                                created_count = 0
                                for _ in range(grow_count):
                                    try:
                                        # 直接创建 session，不调用 _create_session() 避免锁竞争
                                        new_session = requests.Session()
                                        new_session.headers.update({"Connection": "keep-alive"})
                                        if self.proxy:
                                            new_session.proxies = {"http": self.proxy, "https": self.proxy}
                                        # 直接添加到池中（已经在 _pool_cond 锁内）
                                        self._pool.append(new_session)
                                        self._session_count += 1
                                        session_id = id(new_session)
                                        self._session_usage_count[session_id] = 0
                                        created_count += 1
                                    except Exception as e:
                                        if self.debug:
                                            print(f"[HttpClient] 释放流程Session后扩容失败: {e}")
                                        break
                                if self.debug and created_count > 0:
                                    print(f"[HttpClient] 流程Session已销毁，连接池已扩容 {created_count} 个连接，当前池大小: {len(self._pool)}")
                        except Exception as e:
                            if self.debug:
                                print(f"[HttpClient] 释放流程Session后扩容异常: {e}")
                finally:
                    self._pool_cond.release()
            else:
                # 无法获取锁，跳过扩容（避免阻塞）
                if self.debug:
                    print(f"[HttpClient] 释放流程Session时无法获取锁，跳过扩容（避免阻塞）")
        except Exception as e:
            # 扩容失败不影响流程，只记录日志
            if self.debug:
                print(f"[HttpClient] 释放流程Session后扩容异常: {e}")
        
        if self.debug:
            print(f"[HttpClient] 流程Session已销毁，当前连接池大小: {len(self._pool)}, 总连接数: {self._session_count}")

    def _release_session(self, session: requests.Session, is_flow_session: bool = False):
        """
        释放 Session
        
        Args:
            session: 要释放的 Session
            is_flow_session: 是否为流程级Session（流程级Session直接销毁，不放入池中）
        """
        if self._closed:
            try:
                session.close()
            except Exception:
                pass
            return

        # 流程级Session直接销毁，不放入池中
        if is_flow_session:
            self.release_flow_session(session)
            return

        # 检查 Session 是否已关闭
        try:
            # 尝试检查 Session 状态
            if hasattr(session, 'closed') and session.closed:
                # Session 已关闭，不放入池中
                with self._lock:
                    self._session_count -= 1
                    session_id = id(session)
                    if session_id in self._session_usage_count:
                        del self._session_usage_count[session_id]
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
                session_id = id(session)
                if session_id in self._session_usage_count:
                    del self._session_usage_count[session_id]
            if self.debug:
                print("[HttpClient] Session 状态异常，移除连接")
            return

        # 检查 Session 使用次数
        session_id = id(session)
        with self._lock:
            usage_count = self._session_usage_count.get(session_id, 0)
            usage_count += 1
            self._session_usage_count[session_id] = usage_count
            
            # 如果使用次数超过限制，关闭并移除该 Session
            if usage_count >= self._max_session_usage:
                try:
                    session.close()
                except Exception:
                    pass
                self._session_count -= 1
                if session_id in self._session_usage_count:
                    del self._session_usage_count[session_id]
                if self.debug:
                    print(f"[HttpClient] Session 使用次数达到上限({self._max_session_usage}次)，重新创建连接")
                return

        if not self.enable_keep_alive:
            try:
                session.close()
            except Exception:
                pass
            with self._lock:
                self._session_count -= 1
                session_id = id(session)
                if session_id in self._session_usage_count:
                    del self._session_usage_count[session_id]
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

    def _request_with_session(self, method: str, url: str, session: requests.Session, **kwargs) -> requests.Response:
        """
        使用指定的Session发送请求（不管理Session生命周期）
        包含重试机制，处理代理连接错误
        
        Args:
            method: HTTP方法
            url: 请求URL
            session: 要使用的Session对象
            **kwargs: 请求参数
        """
        import time
        request_start = time.time()
        
        # 验证 session 参数类型（检查是否有 post 方法，更通用）
        if not hasattr(session, 'post') or not hasattr(session, 'get'):
            raise TypeError(f"session 参数必须是 Session 对象（有 post/get 方法），但收到: {type(session)} ({session})")
        
        attempt = 0
        while attempt < self.max_retries:
            try:
                print(f"[HttpClient] [发起请求] 使用指定Session执行 {method} 请求 (尝试 {attempt + 1}/{self.max_retries}): {url[:80]}...")
                if self.debug:
                    print(f"[HttpClient] 使用指定Session执行 {method} 请求 (尝试 {attempt + 1}/{self.max_retries}): {url[:80]}...")
                
                # 确保超时参数被正确传递
                if "timeout" not in kwargs:
                    kwargs["timeout"] = self.timeout
                
                func = getattr(session, method.lower())
                request_elapsed_start = time.time()
                print(f"[HttpClient] [发起请求] 开始调用 {method.lower()}(), url={url[:80]}..., timeout={kwargs.get('timeout', self.timeout)}")
                resp = func(url, **kwargs)
                request_elapsed = time.time() - request_elapsed_start
                
                print(f"[HttpClient] [请求完成] 请求完成，耗时: {request_elapsed:.3f}s, 状态码: {resp.status_code}, url={url[:80]}...")
                if self.debug or request_elapsed > 5.0:
                    print(f"[HttpClient] 请求完成，耗时: {request_elapsed:.3f}s, 状态码: {resp.status_code}")
                
                self._stats["requests"] += 1
                
                total_elapsed = time.time() - request_start
                if total_elapsed > 5.0 or self.debug:
                    print(f"[HttpClient] 总耗时: {total_elapsed:.3f}s")
                return resp
            except (requests.RequestsError, ConnectionError, TimeoutError) as e:
                self._stats["failures"] += 1
                error_str = str(e).lower()
                
                # 检查是否是网络相关错误（需要重试）
                is_network_error = (
                    "proxy connect" in error_str or
                    "proxy" in error_str and ("aborted" in error_str or "closed" in error_str) or
                    "connection" in error_str and ("closed" in error_str or "aborted" in error_str) or
                    "session is closed" in error_str or
                    "closed" in error_str
                )
                
                if is_network_error:
                    attempt += 1
                    if attempt >= self.max_retries:
                        print(f"[HttpClient] [请求失败] 网络连接错误，已达到最大重试次数: {e}, url={url[:80]}...")
                        if self.debug:
                            print(f"[HttpClient] 网络连接错误，已达到最大重试次数: {e}")
                        raise
                    self._stats["retries"] += 1
                    print(f"[HttpClient] [请求重试] 网络连接错误({e})，等待 {self.retry_delay} 秒后重试 {attempt}/{self.max_retries}, url={url[:80]}...")
                    if self.debug:
                        print(f"[HttpClient] 网络连接错误({e})，重试 {attempt}/{self.max_retries}")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    # 非网络错误，直接抛出
                    print(f"[HttpClient] [请求失败] 请求失败（非网络错误）: {e}, url={url[:80]}...")
                    if self.debug:
                        print(f"[HttpClient] 请求失败（非网络错误）: {e}")
                    raise
            except Exception as e:
                self._stats["failures"] += 1
                # 检查是否是网络相关错误（需要重试）
                error_str = str(e).lower()
                is_network_error = (
                    "proxy connect" in error_str or
                    "proxy" in error_str and ("aborted" in error_str or "closed" in error_str) or
                    "connection" in error_str and ("closed" in error_str or "aborted" in error_str) or
                    "session is closed" in error_str or
                    "closed" in error_str
                )
                
                if is_network_error:
                    attempt += 1
                    if attempt >= self.max_retries:
                        print(f"[HttpClient] [请求失败] 网络连接错误，已达到最大重试次数: {e}, url={url[:80]}...")
                        if self.debug:
                            print(f"[HttpClient] 网络连接错误，已达到最大重试次数: {e}")
                        raise
                    self._stats["retries"] += 1
                    print(f"[HttpClient] [请求重试] 网络连接错误({e})，等待 {self.retry_delay} 秒后重试 {attempt}/{self.max_retries}, url={url[:80]}...")
                    if self.debug:
                        print(f"[HttpClient] 网络连接错误({e})，重试 {attempt}/{self.max_retries}")
                    time.sleep(self.retry_delay)
                    continue
                else:
                    # 其他错误，直接抛出
                    print(f"[HttpClient] [请求失败] 请求失败: {e}, url={url[:80]}...")
                    if self.debug:
                        print(f"[HttpClient] 请求失败: {e}")
                    raise

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
                
                print(f"[HttpClient] [发起请求] 开始执行 {method} 请求: {url[:80]}...")
                if self.debug:
                    print(f"[HttpClient] 开始执行 {method} 请求: {url[:80]}...")
                func = getattr(session, method.lower())
                request_elapsed_start = time.time()
                # 确保超时参数被正确传递
                if "timeout" not in kwargs:
                    kwargs["timeout"] = self.timeout
                print(f"[HttpClient] [发起请求] 开始调用 {method.lower()}(), url={url[:80]}..., timeout={kwargs.get('timeout', self.timeout)}")
                resp = func(url, **kwargs)
                request_elapsed = time.time() - request_elapsed_start
                print(f"[HttpClient] [请求完成] 请求完成，耗时: {request_elapsed:.3f}s, 状态码: {resp.status_code}, url={url[:80]}...")
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
                print(f"[HttpClient] [请求错误] 网络错误: {e}, url={url[:80]}..., 尝试 {attempt + 1}/{self.max_retries}")
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
                    print(f"[HttpClient] [请求失败] 达到最大重试次数，抛出异常: {e}, url={url[:80]}...")
                    raise
                self._stats["retries"] += 1
                print(f"[HttpClient] [请求重试] 网络错误({e})，等待 {self.retry_delay} 秒后重试 {attempt}/{self.max_retries}")
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

    def get(self, url: str, headers=None, params=None, impersonate=None, session=None, **kwargs):
        """
        发送 GET 请求
        
        Args:
            url: 请求URL
            headers: 请求头
            params: URL参数
            impersonate: 模拟浏览器类型
            session: 可选的Session对象
                - 如果为 None：从连接池中获取Session，使用后放回池中（原有流程）
                - 如果提供：使用指定的Session，不管理Session生命周期
            **kwargs: 其他参数
        """
        # 从 kwargs 中移除 session，避免被传递到 _prepare_request_kwargs
        kwargs_without_session = {k: v for k, v in kwargs.items() if k != 'session'}
        req = self._prepare_request_kwargs(headers=headers, params=params, impersonate=impersonate, **kwargs_without_session)
        # 确保 req 中不包含 session 键
        req.pop('session', None)
        if session is not None:
            # 使用指定的Session（流程级Session）
            return self._request_with_session("GET", url, session, **req)
        else:
            # session 为 None，使用连接池（原有流程）
            return self._request_with_retry("GET", url, **req)

    def post(self, url: str, headers=None, data=None, json=None, impersonate=None, http_version=None, session=None, **kwargs):
        """
        发送 POST 请求
        
        Args:
            url: 请求URL
            headers: 请求头
            data: 请求体数据
            json: JSON数据
            impersonate: 模拟浏览器类型
            http_version: HTTP版本
            session: 可选的Session对象
                - 如果为 None：从连接池中获取Session，使用后放回池中（原有流程）
                - 如果提供：使用指定的Session，不管理Session生命周期
            **kwargs: 其他参数
        """
        # 从 kwargs 中移除 session，避免被传递到 _prepare_request_kwargs
        kwargs_without_session = {k: v for k, v in kwargs.items() if k != 'session'}
        req = self._prepare_request_kwargs(
            headers=headers, data=data, json=json, impersonate=impersonate, http_version=http_version, **kwargs_without_session
        )
        # 确保 req 中不包含 session 键
        req.pop('session', None)
        if session is not None:
            # 使用指定的Session（流程级Session）
            return self._request_with_session("POST", url, session, **req)
        else:
            # session 为 None，使用连接池（原有流程）
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
