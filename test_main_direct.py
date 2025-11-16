"""
直接调用 tem3.py 测试，不使用 HttpClient
"""
import time
import random
from tiktok_api import TikTokAPI

# 从配置文件加载配置
from config_loader import ConfigLoader
config = ConfigLoader._load_config_file()
mq_config = config.get("message_queue", {})

# 初始化 API 客户端
proxy = mq_config.get("proxy", r"socks5h://accountId-5086-tunnelId-12988-area-us:a123456@proxyas.starryproxy.com:10000")
print("proxy:", proxy)
api = TikTokAPI(
    proxy=proxy,
    timeout=30,
    max_retries=3,
    pool_initial_size=mq_config.get("pool_initial_size", 10),
    pool_max_size=mq_config.get("pool_max_size", 100),
    pool_grow_step=mq_config.get("pool_grow_step", 1)
)

device = {'create_time': '2025-11-16 15:50:10', 'device_id': '7573226347225826829', 'install_id': '7573226956382373646', 'ua': 'com.zhiliaoapp.musically/2024204030 (Linux; U; Android 15; en_US; Pixel 3; Build/VP1A.181574.137.B1; Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30)', 'web_ua': 'Dalvik/2.1.0 (Linux; U; Android 15; Pixel 3 Build/VP1A.181574.137.B1)', 'resolution': '1920*1080', 'dpi': 420, 'device_type': 'Pixel 3', 'device_brand': 'Google', 'device_manufacturer': 'Google', 'os_api': 35, 'os_version': 15, 'resolution_v2': '1080*1920', 'rom': 'MIUI', 'rom_version': 'VP1A.181574.137.B1', 'clientudid': '2cf02f20-b3b7-4ce1-a4be-b7e768fc0a97', 'google_aid': 'da3565d7-be68-4f34-b6cb-7fe7fc37b988', 'release_build': 'VP1A.181574.137.B1', 'display_density_v2': 'xxhdpi', 'ram_size': '3GB', 'dark_mode_setting_value': 1, 'is_foldable': 0, 'screen_height_dp': 731, 'screen_width_dp': 411, 'apk_last_update_time': 1762280332243, 'apk_first_install_time': 1762280292647, 'filter_warn': 0, 'priority_region': 'US', 'user_period': 7, 'is_kids_mode': 0, 'user_mode': 1, 'cdid': 'ffbf712e-5947-43d9-84dc-319023062758', 'openudid': 'd5dc389d1e726164', 'version_name': '42.4.3', 'update_version_code': '2024204030', 'version_code': '420403', 'sdk_version_code': 2051090, 'sdk_target_version': 30, 'sdk_version': '2.5.10', '_tt_ok_quic_version': 'Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30', 'mssdk_version_str': 'v05.02.02-ov-android', 'gorgon_sdk_version': '0000000020020205', 'mssdk_version': 84017696, 'device_guard_data0': '{"device_token":"1|{\\"aid\\":1233,\\"av\\":\\"42.4.3\\",\\"did\\":\\"7573226347225826829\\",\\"iid\\":\\"7573226956382373646\\",\\"fit\\":\\"1763279412\\",\\"s\\":1,\\"idc\\":\\"useast8\\",\\"ts\\":\\"1763279412\\"}","dtoken_sign":"ts.1.MEUCIQDiK5HRpT9M5sVo9VcBwFIGxkMd484989+OjVmsTpreWwIgMThIX/kLfCLAarFyoeuKaeHlC/cC5xZLg8bQUK9Qwt0="}', 'tt_ticket_guard_public_key': 'BOIZqGiN7bsVDVPURRjA7wmXKY2tK4hrfZZNULwFmp4n+/2cg+ARAjHqmFFzCBJ+A/ircykKz/+G0Vq0/bC25g4=', 'priv_key': '935d10543be6f30584fe4b29f5c36803419cbe82d1872773ed02577e3173cda3'}

aweme_id = "7571031981614451975"

# 获取 seed 和 token
print("=" * 80)
print("获取 seed 和 token...")
print("=" * 80)
seed, seed_type = api.get_seed(device)
print(f"Seed: {seed[:30]}..., Seed Type: {seed_type}")

token = api.get_token(device)
print(f"Token: {token[:20]}...")
print("=" * 80)

# 直接导入 tem3.py 的 stats_3 函数，不使用 HttpClient
from demos.stats.tem3 import stats_3

success = 0
total = 0

print("\n开始测试（不使用 HttpClient）...")
print("=" * 80)

for i in range(10):
    print(f"\n[测试 {i+1}/10] 开始...")
    try:
        # 直接调用 stats_3，不传递 http_client 参数
        result = stats_3(
            aweme_id=aweme_id,
            seed=seed,
            seed_type=seed_type,
            token=token,
            device=device,
            signcount=200 + i,
            proxy=proxy,
            http_client=None,  # 不使用 HttpClient
            session=None
        )
        
        if result and result != "":
            success += 1
            print(f"[测试 {i+1}/10] ✓ 成功")
        else:
            print(f"[测试 {i+1}/10] ✗ 失败（返回空）")
        
        total += 1
        
        # 随机延迟
        sleep_time = random.randint(3, 5)
        print(f"[测试 {i+1}/10] 等待 {sleep_time} 秒...")
        time.sleep(sleep_time)
        
    except Exception as e:
        print(f"[测试 {i+1}/10] ✗ 异常: {e}")
        total += 1

print("\n" + "=" * 80)
print("测试统计")
print("=" * 80)
print(f"总测试次数: {total}")
print(f"成功次数: {success}")
print(f"失败次数: {total - success}")
print(f"成功率: {(success / total * 100) if total > 0 else 0:.1f}%")
print("=" * 80)

