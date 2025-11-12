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
proxy = mq_config.get("proxy", r"socks5h://accountId-5086-tunnelId-12988-area-us:a123456@proxyas.starryproxy.com:10000")
api = TikTokAPI(
    proxy=proxy,
    timeout=30,
    max_retries=3,
    pool_initial_size=mq_config.get("pool_initial_size", 10),
    pool_max_size=mq_config.get("pool_max_size", 100),
    pool_grow_step=mq_config.get("pool_grow_step", 1)
)

device = {'ua': 'com.zhiliaoapp.musically/2024204030 (Linux; U; Android 14; en_US; OPPO A9; Build/SP1A.227394.390; Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30)', 'web_ua': 'Dalvik/2.1.0 (Linux; U; Android 14; OPPO A9 Build/SP1A.227394.390)', 'resolution': '3200x1440', 'dpi': 560, 'device_type': 'OPPO A9', 'device_brand': 'Oppo', 'device_manufacturer': 'Oppo', 'os_api': 34, 'os_version': '14', 'resolution_v2': '1440x3200', 'rom': 'EMUI', 'rom_version': 'SP1A.227394.390', 'clientudid': '525b90c4-e772-4a71-a58b-16e34feeb477', 'google_aid': '688c32cb-ce5d-4d72-9b26-fd9b935e08c9', 'release_build': 'SP1A.227394.390', 'display_density_v2': 'xxhdpi', 'ram_size': '16GB', 'dark_mode_setting_value': 1, 'is_foldable': 0, 'screen_height_dp': 914, 'screen_width_dp': 411, 'apk_last_update_time': 1761893365721, 'apk_first_install_time': 1761893332521, 'filter_warn': 0, 'priority_region': 'US', 'user_period': 1, 'is_kids_mode': 0, 'user_mode': 1, 'cdid': '175f6ec1-3d83-4f5a-8b69-0d6911711824', 'openudid': 'b7404db06440a59d', 'version_name': '42.4.3', 'update_version_code': '2024204030', 'version_code': '420403', 'sdk_version_code': 2051090, 'sdk_target_version': 30, 'sdk_version': '2.5.10', '_tt_ok_quic_version': 'Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30', 'mssdk_version_str': 'v05.02.02-ov-android', 'gorgon_sdk_version': '0000000020020205', 'mssdk_version': 84017696, 'device_id': '7571450376835139127', 'install_id': '7571450920178435895', 'write_time': '2025-11-11 20:56:40'}

aweme_id = "7569608169052212501"

# 使用统一的 API 接口
seed, seed_type = api.get_seed(device)
print(seed, seed_type)
token = api.get_token(device)
print(token)
signCount = 200
success = 0
total = 0
# with open(r"D:\vscode\reverse\app\shizhan\tt\code_11_9\device_register\deive1.txt","r",encoding="utf-8") as f:
#     devices = f.readlines()
for i in range(20000):
# for i in range(len(devices)):
#     device = ast.literal_eval(devices[i])
#     seed, seed_type = api.get_seed(device)
#     print(seed, seed_type)
#     token = api.get_token(device)
#     print(token)
    res = api.stats(aweme_id, seed, seed_type, token, device, 212)
    if res != "":
        success += 1
    total += 1
    print("success===>", success, "total===>", total)
    st = random.randint(3, 5)
    time.sleep(st)
    print("sleep", st)

# 26