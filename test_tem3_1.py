"""
测试 tem3-1.py 的脚本
"""
import time
import random
import importlib.util
import sys

# 动态导入 tem3-1.py（因为文件名包含连字符，不能直接 import）
spec = importlib.util.spec_from_file_location("tem3_1", "demos/stats/tem3-1.py")
tem3_1 = importlib.util.module_from_spec(spec)
sys.modules["tem3_1"] = tem3_1
spec.loader.exec_module(tem3_1)

# 获取 stats_3 函数
stats_3 = tem3_1.stats_3

# 导入 TikTokAPI 用于获取 seed 和 token
from tiktok_api import TikTokAPI

# 测试设备数据（示例）
device = {"create_time": "2025-11-16 00:39:23", "device_id": "7572992123600766478", "install_id": "7572992565059733262", "ua": "com.zhiliaoapp.musically/2024204030 (Linux; U; Android 15; en_US; Pixel 8; Build/UP1A.252529.266.A1; Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30)", "web_ua": "Dalvik/2.1.0 (Linux; U; Android 15; Pixel 8 Build/UP1A.252529.266.A1)", "resolution": "1520*720", "dpi": 320, "device_type": "Pixel 8", "device_brand": "Google", "device_manufacturer": "Google", "os_api": 35, "os_version": 15, "resolution_v2": "720*1520", "rom": "stock", "rom_version": "UP1A.252529.266.A1", "clientudid": "d146f17f-f404-4d7e-b5b9-e7e4812381d7", "google_aid": "d39facd0-e679-417a-9fd3-83ff43565382", "release_build": "UP1A.252529.266.A1", "display_density_v2": "xhdpi", "ram_size": "4GB", "dark_mode_setting_value": 1, "is_foldable": 0, "screen_height_dp": 760, "screen_width_dp": 360, "apk_last_update_time": 1761280491143, "apk_first_install_time": 1761280460611, "filter_warn": 0, "priority_region": "US", "user_period": 9, "is_kids_mode": 0, "user_mode": 1, "cdid": "4d838434-5bf8-4aee-8941-cd08f77f797f", "openudid": "945b17ec3a1648a8", "version_name": "42.4.3", "update_version_code": "2024204030", "version_code": "420403", "sdk_version_code": 2051090, "sdk_target_version": 30, "sdk_version": "2.5.10", "_tt_ok_quic_version": "Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30", "mssdk_version_str": "v05.02.02-ov-android", "gorgon_sdk_version": "0000000020020205", "mssdk_version": 84017696, "device_guard_data0": "{\"device_token\":\"1|{\\\"aid\\\":1233,\\\"av\\\":\\\"42.4.3\\\",\\\"did\\\":\\\"7572992123600766478\\\",\\\"iid\\\":\\\"7572992565059733262\\\",\\\"fit\\\":\\\"1763224767\\\",\\\"s\\\":1,\\\"idc\\\":\\\"useast8\\\",\\\"ts\\\":\\\"1763224770\\\"}\",\"dtoken_sign\":\"ts.1.MEQCIHL8jv8kZ0coplY27mbSeeQIPlXaCpxkQVfW2Lk7AUXcAiB7Tz87n3P4+Ac2Ev9guhfQsbgoMZM1fdcuEpdoKl/NCQ==\"}", "tt_ticket_guard_public_key": "BJ2N306a9QwXfJ3JGcIgiLcVesuIMHTx+9UI1hVRaAFe4pNgRGpWDlVZV0/uvzA1MY3vxaih8eRgLN5BhfxLfG8=", "priv_key": "63388fa8f48b3e8e1fb0c9ac133c88f34092d8d453ec3d1d5d6dea6756c98470"}


# 测试视频 ID
aweme_id = "7571031981614451975"

# 代理设置（根据实际情况修改）
proxy = "socks5h://accountId-5086-tunnelId-12988-area-us:a123456@proxyas.starryproxy.com:10000"

# 初始化 TikTokAPI
print("=" * 80)
print("初始化 TikTokAPI...")
print("=" * 80)
api = TikTokAPI(
    proxy=proxy,
    timeout=30,
    max_retries=3,
    pool_initial_size=10,
    pool_max_size=100
)
print("✓ TikTokAPI 初始化完成")

# 从 device 中获取或请求 seed, seed_type, token
seed = device.get('seed')
seed_type = device.get('seed_type')
token = device.get('token')

print("\n" + "=" * 80)
print("检查并获取 Seed 和 Token")
print("=" * 80)

# 如果没有 seed 或 seed_type，则请求获取
if not seed or seed_type is None:
    print("⚠️ 设备中没有 seed，正在请求获取...")
    try:
        seed, seed_type = api.get_seed(device)
        print(f"✓ Seed 获取成功: {seed[:30]}...")
        print(f"✓ Seed Type: {seed_type}")
        # 保存到 device 中
        device['seed'] = seed
        device['seed_type'] = seed_type
    except Exception as e:
        print(f"✗ Seed 获取失败: {e}")
        print("⚠️ 将使用空 seed 继续测试（可能会失败）")
else:
    print(f"✓ 使用设备中的 seed: {seed[:30]}...")
    print(f"✓ Seed Type: {seed_type}")

# 如果没有 token，则请求获取
if not token:
    print("⚠️ 设备中没有 token，正在请求获取...")
    try:
        token = api.get_token(device)
        print(f"✓ Token 获取成功: {token[:20]}...")
        # 保存到 device 中
        device['token'] = token
    except Exception as e:
        print(f"✗ Token 获取失败: {e}")
        print("⚠️ 将使用空 token 继续测试（可能会失败）")
else:
    print(f"✓ 使用设备中的 token: {token[:20]}...")

print("\n" + "=" * 80)
print("开始测试 tem3-1.py - stats_3 函数")
print("=" * 80)
print(f"设备ID: {device['device_id']}")
print(f"安装ID: {device['install_id']}")
print(f"视频ID: {aweme_id}")
print(f"Seed: {seed[:30] if seed else 'None'}...")
print(f"Seed Type: {seed_type}")
print(f"Token: {token[:20] if token else 'None'}...")
print("=" * 80)

# 测试循环
success_count = 0
total_count = 0

try:
    for i in range(10):  # 测试 10 次
        print(f"\n[测试 {i+1}/10] 开始...")
        
        try:
            # 调用 stats_3
            result = stats_3(
                aweme_id=aweme_id,
                seed=seed,
                seed_type=seed_type,
                token=token,
                device=device,
                signcount=200 + i,  # 每次递增
                proxy=proxy
            )
            
            # 检查结果
            if result and result != "":
                success_count += 1
                print(f"[测试 {i+1}/10] ✓ 成功")
            else:
                print(f"[测试 {i+1}/10] ✗ 失败（返回空）")
            
            total_count += 1
            
            # 随机延迟（3-5秒）
            sleep_time = random.randint(3, 5)
            print(f"[测试 {i+1}/10] 等待 {sleep_time} 秒...")
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"[测试 {i+1}/10] ✗ 异常: {e}")
            total_count += 1
            
except KeyboardInterrupt:
    print("\n\n用户中断测试")

# 打印统计结果
print("\n" + "=" * 80)
print("测试统计")
print("=" * 80)
print(f"总测试次数: {total_count}")
print(f"成功次数: {success_count}")
print(f"失败次数: {total_count - success_count}")
print(f"成功率: {(success_count / total_count * 100) if total_count > 0 else 0:.1f}%")
print("=" * 80)

# 关闭 API 连接
print("\n关闭 TikTokAPI...")
try:
    if hasattr(api, 'http_client') and api.http_client:
        api.http_client.close()
    print("✓ TikTokAPI 已关闭")
except Exception as e:
    print(f"⚠️ 关闭 TikTokAPI 时出错: {e}")

