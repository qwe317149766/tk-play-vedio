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
    pool_initial_size=mq_config.get("pool_initial_size", 1000),
    pool_max_size=mq_config.get("pool_max_size", 5000),
    pool_grow_step=mq_config.get("pool_grow_step", 50)
)

device = {'ua': 'com.zhiliaoapp.musically/2024204030 (Linux; U; Android 10; en_US; SM-G975F (S10+); Build/QP1A.203782.82; Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30)', 'web_ua': 'Dalvik/2.1.0 (Linux; U; Android 10; SM-G975F (S10+) Build/QP1A.203782.82)', 'resolution': '1280x800', 'dpi': 213, 'device_type': 'SM-G975F (S10+)', 'device_brand': 'Samsung', 'device_manufacturer': 'Samsung', 'os_api': 29, 'os_version': '10', 'resolution_v2': '800x1280', 'rom': 'MIUI', 'rom_version': 'QP1A.203782.82', 'clientudid': '5e299542-571b-4ddc-a57e-4542453af686', 'google_aid': '837a1af4-833c-40b3-ad65-b283616f0bbd', 'release_build': 'QP1A.203782.82', 'display_density_v2': 'hdpi', 'ram_size': '4GB', 'dark_mode_setting_value': 0, 'is_foldable': 0, 'screen_height_dp': 961, 'screen_width_dp': 600, 'apk_last_update_time': 1762578766733, 'apk_first_install_time': 1762578707172, 'filter_warn': 0, 'priority_region': 'US', 'user_period': 7, 'is_kids_mode': 0, 'user_mode': 1, 'cdid': '97e7a2cf-b21d-47a0-b9a9-81f7e2cfc507', 'openudid': '227543a27bdbc459', 'version_name': '42.4.3', 'update_version_code': '2024204030', 'version_code': '420403', 'sdk_version_code': 2051090, 'sdk_target_version': 30, 'sdk_version': '2.5.10', '_tt_ok_quic_version': 'Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30', 'mssdk_version_str': 'v05.02.02-ov-android', 'gorgon_sdk_version': '0000000020020205', 'mssdk_version': 84017696, 'device_id': '7571450148842391053', 'install_id': '7571450858442671885', 'write_time': '2025-11-11 20:56:26'}

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