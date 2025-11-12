import datetime
import json
import random
import secrets
import uuid
import time
from typing import Dict, Optional, Tuple

# 机型/品牌池（可扩充）
_BRANDS_MODELS = {
    "Google": ["Pixel", "Pixel 2", "Pixel 3", "Pixel 4", "Pixel 5", "Pixel 6", "Pixel 7", "Pixel 8"],
    "Samsung": ["SM-G973F (S10)", "SM-G975F (S10+)", "SM-G991B (S21)", "SM-G998B (S21 Ultra)", "SM-S906B (S22)"],
    "Xiaomi": ["Mi 9", "Mi 10", "Mi 11", "Mi 12", "Redmi Note 8", "Redmi Note 10"],
    "OnePlus": ["ONEPLUS A5000", "IN2010", "PHX110"],
    "Huawei": ["HUAWEI P30", "HUAWEI Mate 30"],
    "Oppo": ["OPPO CPH1909", "OPPO A9"],
    "Vivo": ["vivo 1901", "Vivo V20"],
    "Sony": ["Xperia 1", "Xperia 5"],
    # "Motorola": ["Moto G7", "Moto G8"],
    # "Nokia": ["Nokia 7.2"],
    "Asus": ["ZenFone 6"]
}

# 分辨率池（宽, 高, dpi）——注意最终 resolution 走“高x宽”
_RESOLUTIONS = [
    (1080, 1920, 420),
    (1080, 2340, 420),
    (1080, 2400, 440),
    (1440, 3040, 560),
    (720, 1520, 320),
    (1080, 1794, 420),
    (1080, 2209, 420),
    (1440, 3200, 560),
    (1080, 2160, 420),
    (800, 1280, 213),
]

_ANDROID_VERSIONS = [
    # ("10", 29),
    # ("11", 30),
    # ("12", 31),
    # ("12L", 32),
    # ("13", 33),
    # ("14", 34),
    ("15", 35)
]

_DISPLAY_DENSITY_BUCKETS = [
    (160, "mdpi"),
    (240, "hdpi"),
    (320, "xhdpi"),
    (480, "xxhdpi"),
    (640, "xxxhdpi")
]

_RAM_CHOICES = ["2GB", "3GB", "4GB", "6GB", "8GB", "12GB", "16GB"]

def _make_build_id() -> str:
    """生成如 QP1A.191005.007.A3 的构建号（和你示例风格一致）"""
    branch = random.choice(["QP1A", "RP1A", "SP1A", "TP1A", "UP1A", "VP1A"])
    date = f"{random.randint(180000, 260000)}"  # 模拟 yymmdd 风格段位
    patch_major = random.randint(1, 999)
    suffix = random.choice(["", ".A1", ".A2", ".A3", ".B1"])
    return f"{branch}.{date}.{patch_major}{suffix}"

def _density_bucket_for_dpi(dpi: int) -> str:
    return min(_DISPLAY_DENSITY_BUCKETS, key=lambda x: abs(x[0] - dpi))[1]

def getANewDevice(
    seed: Optional[int] = None,
    app_version: Tuple[str, str,str] = ("42.4.3", "2024204030","420403"),
    tt_ok_quic_version: str = "Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30",
    # version_name = "42.4.3"
    sdk_version_code = 2051090,
    sdk_target_version = 30,
    sdk_version = "2.5.10",
    prefer_recent_android: bool = True,
    mssdk_version =("v05.02.02-ov-android","0000000020020205",0x05020220)
) -> Dict:
    """
    生成一组“完整设备信息”字典（非请求头），字段与语义按你给的代码/示例。
    纠正点：
      - rom_version == release_build（同一个构建号，如 QP1A.191005.007.A3）
      - resolution 为 "高x宽"，resolution_v2 为 "宽x高"
      - is_foldable 绝大多数为 0
      - user_mode 合理分布
    """
    if seed is not None:
        random.seed(seed)
    else:
        random.seed(secrets.randbelow(2**32))

    # 品牌/机型
    brand = random.choice(list(_BRANDS_MODELS.keys()))
    model = random.choice(_BRANDS_MODELS[brand])
    device_type = model
    device_brand = brand
    device_manufacturer = brand

    # Android 版本（偏向新）
    # if prefer_recent_android:
    #     weights = [1, 1, 2, 2, 3, 3, 4]
    #     os_version, os_api = random.choices(_ANDROID_VERSIONS, weights=weights, k=1)[0]
    # else:
    #     os_version, os_api = random.choice(_ANDROID_VERSIONS)
    os_version,os_api = 15,35
    # 分辨率 / dpi（注意输出风格）
    w, h, dpi = random.choice(_RESOLUTIONS)
    resolution     = f"{h}*{w}"  # 高x宽（符合你示例）
    resolution_v2  = f"{w}*{h}"  # 宽x高
    display_density_v2 = _density_bucket_for_dpi(dpi)

    # dp
    screen_width_dp  = max(1, int(w * 160 / dpi))
    screen_height_dp = max(1, int(h * 160 / dpi))

    # ROM 信息：按你要求，rom_version ≡ release_build（都等于构建号）
    build_id = _make_build_id()
    rom = random.choice(["stock", "MIUI", "OxygenOS", "ColorOS", "FuntouchOS", "EMUI", "One UI", "RealmeUI"])
    rom_version = build_id
    release_build = build_id

    # 其它
    ram_size = random.choice(_RAM_CHOICES)
    dark_mode_setting_value = random.choice([0, 1])
    is_foldable = 1 if random.random() < 0.02 else 0  # 2% 折叠机
    is_kids_mode = 0
    user_mode = 1
    user_period = random.randint(0, 10)
    filter_warn = 0
    priority_region = random.choice(["US"])
    apk_last_update_time = int((time.time() - random.randint(3600, 60 * 60 * 24 * 30)) * 1000)
    apk_first_install_time = apk_last_update_time - random.randint(30000, 60000)

    # ids
    clientudid = str(uuid.uuid4())
    google_aid = str(uuid.uuid4())
    cdid = str(uuid.uuid4())
    open_udid = secrets.token_bytes(8).hex()

    # UA（保持你原格式）
    manifest_code = app_version[1]
    ua_comment_parts = [
        "Linux",
        "U",
        f"Android {os_version}",
        "en_US",
        device_type,
        f"Build/{rom_version}",      # 这里和示例一致：放 Build/构建号
        # resolution,                  # 高x宽
        # f"dpi/{dpi}",
        f"{tt_ok_quic_version}"
    ]
    ua = f"com.zhiliaoapp.musically/{manifest_code} ({'; '.join(ua_comment_parts)})"

    # web_ua（Dalvik 风格）
    web_ua = f"Dalvik/2.1.0 (Linux; U; Android {os_version}; {device_type} Build/{rom_version})"

    profile = {
        "create_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "device_id":"",
        "install_id":"",
        "ua": ua,
        "web_ua": web_ua,
        "resolution": resolution,            # "高x宽"
        "dpi": dpi,
        "device_type": device_type,
        "device_brand": device_brand,
        "device_manufacturer": device_manufacturer,
        "os_api": os_api,
        "os_version": os_version,
        "resolution_v2": resolution_v2,      # "宽x高"
        "rom": rom,
        "rom_version": rom_version,          # = release_build
        "clientudid": clientudid,
        "google_aid": google_aid,
        "release_build": release_build,      # = rom_version
        "display_density_v2": display_density_v2,
        "ram_size": ram_size,
        "dark_mode_setting_value": dark_mode_setting_value,
        "is_foldable": is_foldable,
        "screen_height_dp": screen_height_dp,
        "screen_width_dp": screen_width_dp,
        "apk_last_update_time": apk_last_update_time,
        "apk_first_install_time": apk_first_install_time,
        "filter_warn": filter_warn,
        "priority_region": priority_region,
        "user_period": user_period,
        "is_kids_mode": is_kids_mode,
        "user_mode": user_mode,
        "cdid": cdid,
        "openudid": open_udid,
        # 便于复用
        "version_name": app_version[0], # 42.4.3
        "update_version_code": app_version[1], # 2024204030
        "version_code": app_version[2], # '420403'
        "sdk_version_code":sdk_version_code,
        "sdk_target_version":sdk_target_version,
        "sdk_version":sdk_version,
        "_tt_ok_quic_version": tt_ok_quic_version,
        "mssdk_version_str":mssdk_version[0],   # "v05.02.02-ov-android"
        "gorgon_sdk_version":mssdk_version[1],  # '0000000020020205'
        "mssdk_version":mssdk_version[2]        # 0x05020220
    }

    return profile
# d = getANewDevice()
# print(json.dumps(d, indent=2, ensure_ascii=False))