"""
TikTok API 接口封装类
封装 main_3.py 中涉及的所有接口调用，使用统一的 HttpClient
"""
from typing import Dict, Tuple, Optional, Any
from http_client import HttpClient
from mssdk.get_seed.seed_test import get_get_seed as _get_get_seed
from mssdk.get_token.token_test import get_get_token as _get_get_token
from demos.stats.tem3 import stats_3 as _stats_3


class TikTokAPI:
    """TikTok API 接口封装类"""
    
    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        pool_initial_size: int = 1000,
        pool_max_size: int = 5000,
        pool_grow_step: int = 50
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
        """
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
        self.proxy = proxy
    
    def get_seed(
        self,
        device: Dict[str, Any]
    ) -> Tuple[str, int]:
        """
        获取 seed
        
        Args:
            device: 设备信息字典，包含 device_id, install_id, ua, device_type 等字段
            
        Returns:
            Tuple[seed, seed_type]: seed 字符串和 seed_type 整数
        """
        return _get_get_seed(device, self.proxy or "")
    
    def get_token(
        self,
        device: Dict[str, Any]
    ) -> str:
        """
        获取 token
        
        Args:
            device: 设备信息字典，包含 device_id, install_id, ua 等字段
            
        Returns:
            token 字符串
        """
        return _get_get_token(device, self.proxy or "")
    
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
        return _stats_3(
            aweme_id=aweme_id,
            seed=seed,
            seed_type=seed_type,
            token=token,
            device=device,
            signcount=signcount,
            proxy=self.proxy or ""
        )
    
    def update_proxy(self, proxy: Optional[str]):
        """
        更新代理设置
        
        Args:
            proxy: 新的代理地址
        """
        self.proxy = proxy
        self.http_client.update_proxy(proxy)

