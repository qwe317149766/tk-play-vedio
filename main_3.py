import ast
import random
import time
import json
from tiktok_api import TikTokAPI
from config_loader import ConfigLoader

# 从配置文件加载配置
config = ConfigLoader._load_config_file()
mq_config = config.get("message_queue", {})

# 初始化 API 客户端
proxy = mq_config.get("proxy", r"socks5h://1accountId-5086-tunnelId-12988-area-us:a123456@proxyas.starryproxy.com:10000")
print("proxy:",proxy)
api = TikTokAPI(
    proxy=proxy,
    timeout=30,
    max_retries=3,
    pool_initial_size=mq_config.get("pool_initial_size", 10),
    pool_max_size=mq_config.get("pool_max_size", 100),
    pool_grow_step=mq_config.get("pool_grow_step", 1)
)


def parse_device_config(device_config: str) -> dict:
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
        print(f"[main_3] 解析 device_config 失败: {e}")
        return {}


def get_seed_and_token(device: dict, flow_session=None):
    """
    获取 seed 和 token
    如果 device 中有 seed、seed_type 和 token 字段，则直接使用；否则请求获取
    
    Args:
        device: 设备信息字典（可能包含 seed、seed_type、token 字段）
        flow_session: 流程Session（可选）
    
    Returns:
        Tuple[seed, seed_type, token]
    """
    http_client = api.http_client
    
    # 步骤1：调用 alert_check 检查设备告警
    # print(f"[main_3] 开始检查设备告警...")
    # alert_result = api.alert_check(device, session=flow_session)
    # print(f"[main_3] 设备告警检查结果: {alert_result}")
    
    # 如果告警检查失败，可以选择抛出异常或返回空值
    # if alert_result != "success":
    #     raise RuntimeError(f"设备告警检查失败: {alert_result}")
    
    # 步骤2：直接从 device 字典中获取 seed 和 seed_type
    seed = device.get('seed')
    seed_type = device.get('seed_type')
    
    # 如果 device 中没有 seed 或 seed_type，或者为空，则请求获取
    if not seed or seed_type is None:
        seed, seed_type = api.get_seed(device, session=flow_session)
        print(f"[main_3] 获取 seed: {seed[:20] if seed else None}..., seed_type: {seed_type}")
    else:
        print(f"[main_3] 从 device 读取 seed: {seed[:20] if seed else None}..., seed_type: {seed_type}")
    
    # 步骤3：直接从 device 字典中获取 token
    token = device.get('token')
    
    # 如果 device 中没有 token 或为空，则请求获取
    if not token:
        token = api.get_token(device, session=flow_session)
        print(f"[main_3] 获取 token: {token[:20] if token else None}...")
    else:
        print(f"[main_3] 从 device 读取 token: {token[:20] if token else None}...")
    
    return seed, seed_type, token


device = {"create_time": "2025-11-16 00:39:24", "device_id": "7572992123600848398", "install_id": "7572992583447185165", "ua": "com.zhiliaoapp.musically/2024204030 (Linux; U; Android 15; en_US; Pixel 7; Build/TP1A.206170.261.A3; Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30)", "web_ua": "Dalvik/2.1.0 (Linux; U; Android 15; Pixel 7 Build/TP1A.206170.261.A3)", "resolution": "2209*1080", "dpi": 420, "device_type": "Pixel 7", "device_brand": "Google", "device_manufacturer": "Google", "os_api": 35, "os_version": 15, "resolution_v2": "1080*2209", "rom": "stock", "rom_version": "TP1A.206170.261.A3", "clientudid": "178f0c30-59ab-413c-9626-674ac3705319", "google_aid": "c1c5d5ce-89a2-48bb-98da-91c140ffe17b", "release_build": "TP1A.206170.261.A3", "display_density_v2": "xxhdpi", "ram_size": "2GB", "dark_mode_setting_value": 1, "is_foldable": 0, "screen_height_dp": 841, "screen_width_dp": 411, "apk_last_update_time": 1762294522426, "apk_first_install_time": 1762294482306, "filter_warn": 0, "priority_region": "US", "user_period": 2, "is_kids_mode": 0, "user_mode": 1, "cdid": "5deca172-751f-4a98-88a2-6bf633c74257", "openudid": "285443848d4f05bf", "version_name": "42.4.3", "update_version_code": "2024204030", "version_code": "420403", "sdk_version_code": 2051090, "sdk_target_version": 30, "sdk_version": "2.5.10", "_tt_ok_quic_version": "Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30", "mssdk_version_str": "v05.02.02-ov-android", "gorgon_sdk_version": "0000000020020205", "mssdk_version": 84017696, "device_guard_data0": "{\"device_token\":\"1|{\\\"aid\\\":1233,\\\"av\\\":\\\"42.4.3\\\",\\\"did\\\":\\\"7572992123600848398\\\",\\\"iid\\\":\\\"7572992583447185165\\\",\\\"fit\\\":\\\"1763224767\\\",\\\"s\\\":1,\\\"idc\\\":\\\"useast8\\\",\\\"ts\\\":\\\"1763224770\\\"}\",\"dtoken_sign\":\"ts.1.MEYCIQCErlm+U7GNiJzQsUVIpgXPSOY999yUdNJPDFfNKXc9jgIhAN40/4+e/ewu0f0D+eFTJkJvoR8yEb0rl8MGQgA17oIs\"}", "tt_ticket_guard_public_key": "BAomfPZ5I0so7mZMCgHINIZwiveCBUTXgcGOTsrj7OjIoTMf7j7jL6Tc+70TwAGPHubwrhVAznbWOJHk4bnRYno=", "priv_key": "5c0bd5b8c3249d68dc6bd5c6820e22c78f65562cab20c694873e68ba99ffb08f"}



aweme_id = "7571031981614451975"

# 获取流程专用Session（同一流程复用同一个Session）
http_client = api.http_client
flow_session = http_client.get_flow_session()
print(f"[main_3] 获取流程Session: {id(flow_session)}")

try:
    # 使用统一的 API 接口，传入流程Session
    # 注意：seed、seed_type、token 会从 device 字典中直接获取，如果不存在则请求获取
    seed, seed_type, token = get_seed_and_token(device, flow_session=flow_session)
    print(seed, seed_type)
    print(token)
    
    signCount = 200
    success = 0
    total = 0
    # with open(r"D:\vscode\reverse\app\shizhan\tt\code_11_9\device_register\deive1.txt","r",encoding="utf-8") as f:
    #     devices = f.readlines()
    for i in range(20000):
    # for i in range(len(devices)):
    #     device = ast.literal_eval(devices[i])
    #     # 每个设备流程使用新的Session
    #     flow_session = http_client.get_flow_session()
    #     try:
    #         seed, seed_type = api.get_seed(device, session=flow_session)
    #         print(seed, seed_type)
    #         token = api.get_token(device, session=flow_session)
    #         print(token)
    #     finally:
    #         http_client.release_flow_session(flow_session)
        res = api.stats(aweme_id, seed, seed_type, token, device, 212, session=flow_session)
        if res != "":
            success += 1
        total += 1
        print("success===>", success, "total===>", total)
        st = random.randint(3, 5)
        time.sleep(st)
        print("sleep", st)
finally:
    # 流程结束，释放流程Session
    http_client.release_flow_session(flow_session)
    print(f"[main_3] 流程Session已释放: {id(flow_session)}")

# 26