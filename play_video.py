"""
播放视频接口
支持从 device 字典中直接读取 seed、seed_type 和 token，如果不存在则请求获取
"""
import json
import random
import time
from typing import Dict, Any, Optional, Tuple
from tiktok_api import TikTokAPI
from config_loader import ConfigLoader

# 从配置文件加载配置
config = ConfigLoader._load_config_file()
mq_config = config.get("message_queue", {})

# 初始化 API 客户端
proxy = mq_config.get("proxy", r"socks5h://accountId-5086-tunnelId-12988-area-us:a123456@proxyas.starryproxy.com:10000")
api = TikTokAPI(
    proxy=proxy,
    timeout=30,
    max_retries=3,
    pool_initial_size=mq_config.get("pool_initial_size", 10),
    pool_max_size=mq_config.get("pool_max_size", 100),
    pool_grow_step=mq_config.get("pool_grow_step", 1),
    use_global_client=True
)


def parse_device_config(device_config: str) -> Dict[str, Any]:
    """
    解析设备配置 JSON 字符串
    
    Args:
        device_config: JSON 字符串
    
    Returns:
        解析后的设备配置字典
    """
    try:
        if not device_config:
            return {}
        return json.loads(device_config)
    except Exception as e:
        print(f"[play_video] 解析 device_config 失败: {e}")
        return {}


async def play_video_async(
    aweme_id: str,
    device: Dict[str, Any],
    device_config: Optional[str] = None
) -> bool:
    """
    播放视频（异步版本）
    
    Args:
        aweme_id: 视频ID
        device: 设备信息字典（可能包含 seed、seed_type、token 字段）
        device_config: 设备配置 JSON 字符串（可选，如果提供会解析并合并到 device 中）
    
    Returns:
        是否成功
    """
    http_client = api.http_client
    flow_session = http_client.get_flow_session()
    
    try:
        # 如果提供了 device_config，先解析并合并到 device 字典中
        if device_config:
            config_dict = parse_device_config(device_config)
            # 将 device_config 中的字段合并到 device 中（device 中的字段优先）
            device = {**config_dict, **device}
        
        # 直接从 device 字典中获取 seed 和 seed_type
        seed = device.get('seed')
        seed_type = device.get('seed_type')
        
        # 如果 device 中没有 seed 或 seed_type，或者为空，则请求获取
        if not seed or seed_type is None:
            seed, seed_type = await api.get_seed_async(device, session=flow_session)
            print(f"[play_video] 获取 seed: {seed[:20] if seed else None}..., seed_type: {seed_type}")
        else:
            print(f"[play_video] 从 device 读取 seed: {seed[:20] if seed else None}..., seed_type: {seed_type}")
        
        # 直接从 device 字典中获取 token
        token = device.get('token')
        
        # 如果 device 中没有 token 或为空，则请求获取
        if not token:
            token = await api.get_token_async(device, session=flow_session)
            print(f"[play_video] 获取 token: {token[:20] if token else None}...")
        else:
            print(f"[play_video] 从 device 读取 token: {token[:20] if token else None}...")
        
        # 调用 stats 接口播放视频
        signcount = random.randint(200, 300)  # 随机签名计数
        result = api.stats(
            aweme_id=aweme_id,
            seed=seed,
            seed_type=seed_type,
            token=token,
            device=device,
            signcount=signcount,
            session=flow_session
        )
        
        # 如果返回结果不为空，表示成功
        success = result != "" if result else False
        return success
        
    except Exception as e:
        print(f"[play_video] 播放视频失败: {e}")
        import traceback
        print(traceback.format_exc())
        return False
    finally:
        # 释放 flow_session
        http_client.release_flow_session(flow_session)


def play_video(
    aweme_id: str,
    device: Dict[str, Any],
    device_config: Optional[str] = None
) -> bool:
    """
    播放视频（同步版本）
    
    Args:
        aweme_id: 视频ID
        device: 设备信息字典（可能包含 seed、seed_type、token 字段）
        device_config: 设备配置 JSON 字符串（可选，如果提供会解析并合并到 device 中）
    
    Returns:
        是否成功
    """
    import asyncio
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(play_video_async(aweme_id, device, device_config))


if __name__ == "__main__":
    # 测试代码
    device = {"create_time": "2025-11-13 15:58:29", "device_id": "7572115224145577486", "install_id": "7572115927602726670", "ua": "com.zhiliaoapp.musically/2024204030 (Linux; U; Android 15; en_US; HUAWEI Mate 30; Build/VP1A.256272.76.A3; Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30)", "web_ua": "Dalvik/2.1.0 (Linux; U; Android 15; HUAWEI Mate 30 Build/VP1A.256272.76.A3)", "resolution": "1920*1080", "dpi": 420, "device_type": "HUAWEI Mate 30", "device_brand": "Huawei", "device_manufacturer": "Huawei", "os_api": 35, "os_version": 15, "resolution_v2": "1080*1920", "rom": "stock", "rom_version": "VP1A.256272.76.A3", "clientudid": "d1d471c1-0bcd-41a4-a41a-bf8c61d3e9d4", "google_aid": "615ed510-09b4-4c99-b031-ab590c052512", "release_build": "VP1A.256272.76.A3", "display_density_v2": "xxhdpi", "ram_size": "16GB", "dark_mode_setting_value": 0, "is_foldable": 0, "screen_height_dp": 731, "screen_width_dp": 411, "apk_last_update_time": 1761001283498, "apk_first_install_time": 1761001244469, "filter_warn": 0, "priority_region": "US", "user_period": 7, "is_kids_mode": 0, "user_mode": 1, "cdid": "40a4a9a5-57c2-45e2-bfc2-922b33d2fe2c", "openudid": "535e9ae77adf885d", "version_name": "42.4.3", "update_version_code": "2024204030", "version_code": "420403", "sdk_version_code": 2051090, "sdk_target_version": 30, "sdk_version": "2.5.10", "_tt_ok_quic_version": "Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30", "mssdk_version_str": "v05.02.02-ov-android", "gorgon_sdk_version": "0000000020020205", "mssdk_version": 84017696, "seed": "MDGiGJrbpHsAIzl+yT0ylYfszb0qHiQhBDdy8/xdS0/RIE6AQGNsFVQoFR60NQem2ekBJQDEwhWSvg7OUKynVAaWk2GJVrYFLohnmkQS6jRqqZRHFurn8i1BMS2K64fMA/E=", "seed_type": 6, "token": "A18Jp8pBuB0GXQoJxzt8ncvC8"}
    
    aweme_id = "7569608169052212501"
    
    # 测试：device 字典中已经包含 seed、seed_type、token，直接从 device 中读取
    result = play_video(aweme_id, device)
    print(f"播放结果: {result}")

