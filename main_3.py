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
    print(f"[main_3] 开始检查设备告警...")
    alert_result = api.alert_check(device, session=flow_session)
    print(f"[main_3] 设备告警检查结果: {alert_result}")
    
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


device = {"create_time": "2025-11-14 11:17:16", "device_id": "7571031545047270933", "install_id": "7571032910167213845", "ua": "com.zhiliaoapp.musically/2024204030 (Linux; U; Android 15; en_US; OPPO A9; Build/RP1A.208887.416; Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30)", "web_ua": "Dalvik/2.1.0 (Linux; U; Android 15; OPPO A9 Build/RP1A.208887.416)", "resolution": "3200*1440", "dpi": 560, "device_type": "OPPO A9", "device_brand": "Oppo", "device_manufacturer": "Oppo", "os_api": 35, "os_version": 15, "resolution_v2": "1440*3200", "rom": "OxygenOS", "rom_version": "RP1A.208887.416", "clientudid": "cc296e8a-51e0-4cad-887b-b375a959372a", "google_aid": "61453c41-6bdb-49ce-8176-2f9ebd51d7f3", "release_build": "RP1A.208887.416", "display_density_v2": "xxhdpi", "ram_size": "6GB", "dark_mode_setting_value": 1, "is_foldable": 0, "screen_height_dp": 914, "screen_width_dp": 411, "apk_last_update_time": 1761054436416, "apk_first_install_time": 1761054377455, "filter_warn": 0, "priority_region": "US", "user_period": 2, "is_kids_mode": 0, "user_mode": 1, "cdid": "cc7380f6-b7eb-46db-b1c3-c6df2e047b97", "openudid": "361697225c03a3a8", "version_name": "42.4.3", "update_version_code": "2024204030", "version_code": "420403", "sdk_version_code": 2051090, "sdk_target_version": 30, "sdk_version": "2.5.10", "_tt_ok_quic_version": "Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30", "mssdk_version_str": "v05.02.02-ov-android", "gorgon_sdk_version": "0000000020020205", "mssdk_version": 84017696}


aweme_id = "7572772064793283848"

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