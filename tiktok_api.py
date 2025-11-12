"""
TikTok API 接口封装类
封装 main_3.py 中涉及的所有接口调用，使用统一的 HttpClient
"""
from typing import Dict, Tuple, Optional, Any
import asyncio
from concurrent.futures import ThreadPoolExecutor
from http_client import HttpClient
from mssdk.get_seed.seed_test import get_get_seed as _get_get_seed
from mssdk.get_token.token_test import get_get_token as _get_get_token
from demos.stats.tem3 import stats_3 as _stats_3
from test_10_24 import make_did_iid as _make_did_iid, alert_check as _alert_check

# 全局 HttpClient 实例
_global_http_client: Optional[HttpClient] = None

# 全局线程池（用于执行同步 HTTP 请求，确保高并发）
# 线程池大小设置为 2000，确保可以支持大量并发任务
_global_executor: Optional[ThreadPoolExecutor] = None

def get_global_executor() -> ThreadPoolExecutor:
    """获取全局线程池实例（懒加载）"""
    global _global_executor
    if _global_executor is None:
        # 创建一个足够大的线程池，支持高并发
        # 线程数设置为 2000，确保可以支持大量并发任务
        _global_executor = ThreadPoolExecutor(max_workers=2000, thread_name_prefix="tiktok_api")
    return _global_executor

def get_global_http_client() -> Optional[HttpClient]:
    """获取全局 HttpClient 实例"""
    return _global_http_client

def set_global_http_client(http_client: HttpClient):
    """设置全局 HttpClient 实例"""
    global _global_http_client
    _global_http_client = http_client


class TikTokAPI:
    """TikTok API 接口封装类"""
    
    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 1,
        retry_delay: float = 2.0,
        pool_initial_size: int = 1000,
        pool_max_size: int = 5000,
        pool_grow_step: int = 50,
        use_global_client: bool = True
    ):
        """
        初始化 TikTok API 客户端
        
        Args:
            proxy: 代理地址，格式如 "socks5://user:pass@host:port"
            timeout: 请求超时时间（秒）
            max_retries: 最大重试次数
            retry_delay: 重试延迟时间（秒）
            pool_initial_size: 连接池初始大小
            pool_max_size: 连接池最大大小
            pool_grow_step: 连接池增长步长
            use_global_client: 是否使用全局 HttpClient 实例（默认 True）
        """
        global _global_http_client
        
        if use_global_client and _global_http_client is not None:
            # 使用全局 HttpClient 实例
            self.http_client = _global_http_client
            # 如果代理不同，更新代理
            if proxy and proxy != self.http_client.proxy:
                self.http_client.update_proxy(proxy)
            # 更新重试参数（如果全局客户端已存在，更新其参数）
            if hasattr(self.http_client, 'max_retries'):
                self.http_client.max_retries = max(max_retries, self.http_client.max_retries)
            if hasattr(self.http_client, 'retry_delay'):
                self.http_client.retry_delay = retry_delay
        else:
            # 创建新的 HttpClient 实例
            self.http_client = HttpClient(
                proxy=proxy,
                timeout=timeout,
                max_retries=max_retries,
                retry_delay=retry_delay,
                verify=False,
                default_impersonate="okhttp4_android",
                pool_initial_size=pool_initial_size,
                pool_max_size=pool_max_size,
                pool_grow_step=pool_grow_step
            )
            # 如果使用全局客户端，设置为全局实例
            if use_global_client:
                _global_http_client = self.http_client
        
        self.proxy = proxy or (self.http_client.proxy if hasattr(self.http_client, 'proxy') else None)
    
    def get_seed(
        self,
        device: Dict[str, Any]
    ) -> Tuple[str, int]:
        """
        获取 seed
        使用 HttpClient 发送请求（必须使用 http_client，不使用 requests）
        
        Args:
            device: 设备信息字典，包含 device_id, install_id, ua, device_type 等字段
            
        Returns:
            Tuple[seed, seed_type]: seed 字符串和 seed_type 整数
        """
        # 使用全局 HttpClient 实例（确保所有请求都通过 http_client）
        http_client = get_global_http_client() or self.http_client
        if http_client is None:
            raise RuntimeError("HttpClient 未初始化")
        return _get_get_seed(device, proxy=self.proxy or "", http_client=http_client)
    
    def get_token(
        self,
        device: Dict[str, Any]
    ) -> str:
        """
        获取 token
        使用 HttpClient 发送请求（必须使用 http_client，不使用 requests）
        
        Args:
            device: 设备信息字典，包含 device_id, install_id, ua 等字段
            
        Returns:
            token 字符串
        """
        # 使用全局 HttpClient 实例（确保所有请求都通过 http_client）
        http_client = get_global_http_client() or self.http_client
        if http_client is None:
            raise RuntimeError("HttpClient 未初始化")
        return _get_get_token(device, proxy=self.proxy or "", http_client=http_client)
    
    def stats(
        self,
        aweme_id: str,
        seed: str,
        seed_type: int,
        token: str,
        device: Dict[str, Any],
        signcount: int
    ) -> str:
        """
        统计数据接口
        使用 HttpClient 发送请求（必须使用 http_client，不使用 requests）
        
        Args:
            aweme_id: 视频 ID
            seed: seed 字符串
            seed_type: seed 类型
            token: token 字符串
            device: 设备信息字典
            signcount: 签名计数
            
        Returns:
            响应文本
        """
        # 使用全局 HttpClient 实例（确保所有请求都通过 http_client）
        http_client = get_global_http_client() or self.http_client
        if http_client is None:
            raise RuntimeError("HttpClient 未初始化")
        return _stats_3(
            aweme_id=aweme_id,
            seed=seed,
            seed_type=seed_type,
            token=token,
            device=device,
            signcount=signcount,
            proxy=self.proxy or "",
            http_client=http_client
        )
    
    def update_proxy(self, proxy: Optional[str]):
        """
        更新代理设置
        
        Args:
            proxy: 新的代理地址
        """
        self.proxy = proxy
        self.http_client.update_proxy(proxy)
    
    def make_did_iid(self, device: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """
        注册设备并获取 device_id 和 install_id
        使用 HttpClient 发送请求（必须使用 http_client，不使用 requests）
        
        Args:
            device: 设备信息字典
        
        Returns:
            Tuple[device, device_id]: 更新后的设备字典和 device_id
        """
        # 使用全局 HttpClient 实例（确保所有请求都通过 http_client）
        http_client = get_global_http_client() or self.http_client
        if http_client is None:
            raise RuntimeError("HttpClient 未初始化")
        # 调用 test_10_24.py 中的函数，传入 HttpClient 实例
        return _make_did_iid(device, proxy=self.proxy or "", http_client=http_client)
    
    async def make_did_iid_async(self, device: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """
        注册设备并获取 device_id 和 install_id（异步版本）
        使用全局 HttpClient 发送请求（在线程池中执行，不阻塞）
        
        Args:
            device: 设备信息字典
        
        Returns:
            Tuple[device, device_id]: 更新后的设备字典和 device_id
        """
        import time
        start_time = time.time()
        print(f"[make_did_iid_async] 开始执行，device_type={device.get('device_type', 'unknown')}")
        
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # 事件循环已关闭，直接调用同步方法
                print(f"[make_did_iid_async] 事件循环已关闭，使用同步方法")
                return self.make_did_iid(device)
        
        try:
            # 使用全局线程池，确保高并发
            executor = get_global_executor()
            print(f"[make_did_iid_async] 提交到线程池执行，耗时: {time.time() - start_time:.3f}s")
            
            # 添加超时保护（最多等待20秒）
            result = await asyncio.wait_for(
                loop.run_in_executor(executor, self.make_did_iid, device),
                timeout=20.0
            )
            
            elapsed = time.time() - start_time
            print(f"[make_did_iid_async] 执行完成，耗时: {elapsed:.3f}s, device_id={result[1] if result else 'None'}")
            return result
        except asyncio.TimeoutError:
            elapsed = time.time() - start_time
            print(f"[make_did_iid_async] 执行超时（20秒），耗时: {elapsed:.3f}s")
            raise TimeoutError(f"make_did_iid_async 执行超时（20秒）")
        except RuntimeError as e:
            if "cannot schedule new futures" in str(e):
                # 事件循环已关闭，直接调用同步方法
                print(f"[make_did_iid_async] 事件循环已关闭，使用同步方法")
                return self.make_did_iid(device)
            raise
    
    def alert_check(self, device: Dict[str, Any]) -> str:
        """
        检查设备告警
        使用 HttpClient 发送请求（必须使用 http_client，不使用 requests）
        
        Args:
            device: 设备信息字典
        
        Returns:
            "success" 或错误信息
        """
        # 使用全局 HttpClient 实例（确保所有请求都通过 http_client）
        http_client = get_global_http_client() or self.http_client
        if http_client is None:
            raise RuntimeError("HttpClient 未初始化")
        # 调用 test_10_24.py 中的函数，传入 HttpClient 实例
        return _alert_check(device, proxy=self.proxy or "", http_client=http_client)
    
    async def alert_check_async(self, device: Dict[str, Any]) -> str:
        """
        检查设备告警（异步版本）
        使用全局 HttpClient 发送请求（在线程池中执行，不阻塞）
        
        Args:
            device: 设备信息字典
        
        Returns:
            "success" 或错误信息
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # 事件循环已关闭，直接调用同步方法
                return self.alert_check(device)
        
        try:
            # 使用全局线程池，确保高并发
            executor = get_global_executor()
            return await loop.run_in_executor(executor, self.alert_check, device)
        except RuntimeError as e:
            if "cannot schedule new futures" in str(e):
                # 事件循环已关闭，直接调用同步方法
                return self.alert_check(device)
            raise
    
    async def get_seed_async(self, device: Dict[str, Any]) -> Tuple[str, int]:
        """
        获取 seed（异步版本）
        使用全局 HttpClient 发送请求（在线程池中执行，不阻塞）
        
        Args:
            device: 设备信息字典，包含 device_id, install_id, ua, device_type 等字段
            
        Returns:
            Tuple[seed, seed_type]: seed 字符串和 seed_type 整数
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # 事件循环已关闭，直接调用同步方法
                return self.get_seed(device)
        
        try:
            # 使用全局线程池，确保高并发
            executor = get_global_executor()
            return await loop.run_in_executor(executor, self.get_seed, device)
        except RuntimeError as e:
            if "cannot schedule new futures" in str(e):
                # 事件循环已关闭，直接调用同步方法
                return self.get_seed(device)
            raise
    
    async def get_token_async(self, device: Dict[str, Any]) -> str:
        """
        获取 token（异步版本）
        使用全局 HttpClient 发送请求（在线程池中执行，不阻塞）
        
        Args:
            device: 设备信息字典，包含 device_id, install_id, ua 等字段
            
        Returns:
            token 字符串
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # 事件循环已关闭，直接调用同步方法
                return self.get_token(device)
        
        try:
            # 使用全局线程池，确保高并发
            executor = get_global_executor()
            return await loop.run_in_executor(executor, self.get_token, device)
        except RuntimeError as e:
            if "cannot schedule new futures" in str(e):
                # 事件循环已关闭，直接调用同步方法
                return self.get_token(device)
            raise

