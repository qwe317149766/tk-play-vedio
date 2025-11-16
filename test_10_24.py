import base64
import datetime
import hashlib
import random
import secrets
import time
import urllib
# import urllib.parse
from urllib.parse import quote, urlparse, urlencode, unquote
import uuid
# import requests
import json
from curl_cffi import requests
import requests
import urllib3

from mssdk.get_seed.seed_test import get_get_seed
from mssdk.get_token.token_test import get_get_token

# from mssdk.ms_dyn.report.test import DynReport
# from mssdk.ms_dyn.task.test import task

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# from example.login.pwd_login.gorgon import make_gorgon
from headers import make_headers
from devices import getANewDevice
def url_params_to_json(url):
    result = {}
    if '?' in url:
        query_string = url.split('?', 1)[1]
    else:
        query_string=url
    pairs = query_string.split('&')
    for pair in pairs:
        if '=' in pair:
            key, value = pair.split('=', 1)
        else:
            key = pair
            value = ''
        key = unquote(key)
        value = value
        if key in result:
            if isinstance(result[key], list):
                result[key].append(value)
            else:
                result[key] = [result[key], value]
        else:
            result[key] = value
    return result
def build_query_string(params):
    return urlencode(params,safe='*').replace('%25', '%').replace('=&', '&').replace('+', '%20')
def make_did_iid(device, proxy="", http_client=None, session=None):
    """
    注册设备并获取 device_id 和 install_id
    
    Args:
        device: 设备信息字典
        proxy: 代理地址（如果 http_client 为 None 时使用）
        http_client: HttpClient 实例（优先使用）
        session: 可选的Session对象（如果提供，使用此Session）
    """
    use_http_client = http_client is not None
    if not use_http_client:
        proxies = {
           'http': proxy,
           'https': proxy,
        }
    # --- 小工具：生成紧凑 JSON，按真机习惯转义斜杠 ---
    def to_compact_json(d: dict, escape_slash: bool = True) -> str:
        s = json.dumps(d, ensure_ascii=False, separators=(',', ':'))
        if escape_slash:
            s = s.replace('/', r'\/')
        return s

    # --- 时间与设备字段 ---
    req_id = str(uuid.uuid4())
    now = time.time()
    stime = int(now)
    utime = int(now * 1000)

    cdid = device['cdid']
    device_type = device['device_type']
    device_brand = device['device_brand']
    os_api = device['os_api']
    os_version = device['os_version']
    last_install = device['apk_last_update_time'] // 1000
    openudid = device['openudid']
    resolution = device['resolution']           # 形如 "2209x1080"
    dpi = device['dpi']
    rom = device['rom']
    resolution_v2 = device['resolution_v2']
    rom_version = device['rom_version']
    clientudid = device['clientudid']
    release_build = device['release_build']
    google_aid = device['google_aid']
    ram_size = device['ram_size']
    screen_height_dp = device['screen_height_dp']
    apk_last_update_time = device['apk_last_update_time']
    last_install_time = device['apk_last_update_time']//1000
    screen_width_dp = device['screen_width_dp']
    web_ua = device['web_ua']
    apk_first_install_time = device['apk_first_install_time']
    ua = device['ua']
    update_version_code = device['update_version_code']
    version_name = device['version_name']
    version_code = device['version_code']
    sdk_version_code = device['sdk_version_code']
    sdk_target_version = device['sdk_target_version']
    sdk_version = device['sdk_version']
    # -------- 1) 构造 EXACT 同真机格式的 query_string（参与签名 & 实际发送都用这一个）--------
    # 注意：用手拼 + 再逐项 quote，确保 America/New_York 的 '/' 被编码为 %2F，分隔符等不变
    query_kv = [
        ("req_id", req_id),
        ("device_platform", "android"),
        ("os", "android"),
        ("ssmix", "a"),
        ("_rticket", str(utime)),
        ("cdid", cdid),
        ("channel", "googleplay"),
        ("aid", "1233"),
        ("app_name", "musical_ly"),
        ("version_code", "400603"),
        ("version_name", "40.6.3"),
        ("manifest_version_code", "2024006030"),
        ("update_version_code", "2024006030"),
        ("ab_version", "40.6.3"),
        ("resolution", resolution),   # 注意：真机是 1080*2209 这种星号
        ("dpi", str(dpi)),
        ("device_type", device_type),
        ("device_brand", device_brand),        # 真机常见小写 'google'，视你抓包而定
        ("language", "en"),
        ("os_api", str(os_api)),
        ("os_version", str(os_version)),
        ("ac", "wifi"),
        ("is_pad", "0"),
        ("app_type", "normal"),
        ("sys_region", "US"),
        ("last_install_time", str(last_install)),
        ("timezone_name", "America/New_York"),         # 先放原值，下面统一编码
        ("app_language", "en"),
        ("ac2", "wifi"),
        ("uoo", "0"),
        ("op_region", "US"),
        ("timezone_offset", "-18000"),
        ("build_number", "40.6.3"),
        ("host_abi", "arm64-v8a"),
        ("locale", "en"),
        ("region", "US"),
        ("ts", str(stime)),
        ("openudid", openudid),
        ("okhttp_version", "4.2.228.18-tiktok"),
        ("use_store_region_cookie", "1"),
    ]
    # 逐项做 RFC3986 安全编码（和你之前一致），确保 “/” -> %2F
    # query_string = "&".join(f"{k}={quote(v, safe='')}" for k, v in query_kv)
    query_string1 = f"rticket={utime}&ab_version={version_name}&ac=wifi&ac2=wifi&aid=1233&app_language=en&app_name=musical_ly&app_type=normal&build_number={version_name}&carrier_region=US&carrier_region_v2=310&cdid={cdid}&channel=googleplay&device_brand={device_brand}&device_platform=android&device_type={device_type}&dpi={dpi}&host_abi=arm64-v8a&is_pad=0&language=en&last_install_time={last_install_time}&locale=en&manifest_version_code={update_version_code}&mcc_mnc=310004&op_region=US&openudid={openudid}&os=android&os_api={os_api}&os_version={os_version}&redirect_from_idc=maliva&region=US&req_id={req_id}&resolution={resolution}&ssmix=a&sys_region=US&timezone_name=America/New_York&timezone_offset=-18000&ts={stime}&uoo=0&update_version_code={update_version_code}&version_code={version_code}&version_name={version_name}"
    query_string = "&".join(
        f"{k}={quote(v, safe='*').replace('%25', '%')}"
        for param in query_string1.split("&")
        if "=" in param
        for k, v in [param.split("=", 1)]
    ).replace('+', '%20')
    # 用这个 EXACT 的 query 作为 URL 的一部分（不要再用另一个变量混用）
    url = f"https://log-boot.tiktokv.com/service/2/device_register/?{query_string}"
    params = build_query_string(url_params_to_json(url))
    query_string = params
    # -------- 2) 构造 EXACT 同真机格式的 JSON body（参与签名 & 实际发送都用这一个）--------
    body_dict = {
        "header": {
            "os": "Android",
            "os_version": str(os_version),
            "os_api": int(os_api),
            "device_model": device_type,
            "device_brand": device_brand,      # 抓包里多见小写
            "device_manufacturer": "Google",
            "cpu_abi": "arm64-v8a",
            "density_dpi": int(dpi),
            "display_density": "mdpi",
            "resolution": resolution,                   # 这里依你的真机抓包：body 是 "2209x1080"
            "display_density_v2": "xxhdpi",
            "resolution_v2": resolution_v2,
            "access": "wifi",
            "rom": rom,
            "rom_version": rom_version,
            "language": "en",
            "timezone": -4,
            "region": "US",                             # 抓包 body 里有 "region":"US"
            "tz_name": "America/New_York",
            "tz_offset": -14400,
            "clientudid": clientudid,
            "openudid": openudid,
            "channel": "googleplay",
            "not_request_sender": 1,
            "aid": 1233,
            "release_build": release_build,
            "ab_version": version_name,
            "google_aid": google_aid,
            "gaid_limited": 0,
            "custom": {
                "ram_size": str(ram_size),
                "dark_mode_setting_value": 1,
                "is_foldable": 0,
                "screen_height_dp": int(screen_height_dp),
                "apk_last_update_time": int(apk_last_update_time),
                "filter_warn": 0,
                "priority_region": "US",
                "user_period": 0,
                "is_kids_mode": 0,
                "web_ua": web_ua,
                "screen_width_dp": int(screen_width_dp),
                "user_mode": 1,                        # 抓包可见 -1；若你已固定为 1，也要与抓包一致
            },
            "package": "com.zhiliaoapp.musically",
            "app_version": version_name,
            "app_version_minor": "",
            "version_code": int(version_code),
            "update_version_code": int(update_version_code),
            "manifest_version_code": int(update_version_code),
            "app_name": "musical_ly",
            "tweaked_channel": "googleplay",
            "display_name": "TikTok",
            # "sig_hash": "194326e82c84a639a52e5c023116f12a",  # 若抓包里有且参与签名，必须放开并一致
            "cdid": cdid,
            "device_platform": "android",
            # "git_hash": "5151884",
            "sdk_version_code": sdk_version_code,
            "sdk_target_version": sdk_target_version,
            "req_id": req_id,
            "sdk_version": sdk_version,
            "guest_mode": 0,
            "sdk_flavor": "i18nInner",
            "apk_first_install_time": int(apk_first_install_time),
            "is_system_app": 0
        },
        "magic_tag": "ss_app_log",
        "_gen_time": utime
    }

    body_json = to_compact_json(body_dict,
                                escape_slash=False
                                )     # 与真机字节一致（\/）
    # print(body_json)
    body_bytes = body_json.encode('utf-8')
    body_hex   = body_bytes.hex()                                 # 视你的 make_headers 需要

    # -------- 3) 生成签名：务必用 “相同的 query_string + 相同的 body_hex” --------
    # 你自己的签名器接口：请用与“真实发送”一致的两个参数
    x_ss_stub, x_khronos, x_argus, x_ladon, x_gorgon = make_headers.make_headers(
        "",                         # device_id 可空时就空（与抓包一致）
        stime,                      # Khronos = 秒级时间戳
        random.randint(20, 40),
        random.randint(100, 500),
        random.randint(100, 500),
        stime - random.randint(50, 100),
        "",                         # token
        device_type,
        '', '', '', '', '',
        query_string,               # ⚠️一定是“最终发送的”query（和 URL 完全一致）
        body_hex                    # ⚠️一定是“最终发送的”body 的 hex
    )
    # -------- 4) 发送：URL + body 与签名输入完全一致；修正 header 值 --------
    headers = {
        "Host": "log-boot.tiktokv.com",
        "x-ss-stub": x_ss_stub,
        "x-tt-app-init-region": "carrierregion=;mccmnc=;sysregion=US;appregion=US",
        "x-tt-request-tag": "t=0;n=1",
        "x-tt-dm-status": "login=0;ct=0;rt=7",
        "x-ss-req-ticket": str(utime),
        "sdk-version": "2",
        "passport-sdk-version": "-1",
        "x-vc-bdturing-sdk-version": "2.3.13.i18n",
        "user-agent": ua,
        "x-ladon": x_ladon,
        "x-khronos": str(x_khronos),    # ✅ 这里必须是秒级时间戳，不要写成 x_gorgon
        "x-argus": x_argus,
        "x-gorgon": x_gorgon,
        "content-type": "application/json; charset=utf-8",
        "accept-encoding": "gzip",
    }

    # 发送时：URL里就是上面那个 query_string；body 就是 body_json
    request_start = time.time()
    
    if use_http_client:
        # 使用 HttpClient
        print(f"[make_did_iid] 开始调用 http_client.post, url={url[:80]}...")
        try:
            resp = http_client.post(url, headers=headers, data=body_json, session=session)
            elapsed = time.time() - request_start
            print(f"[make_did_iid] http_client.post 完成, 耗时: {elapsed:.3f}s, 状态码: {resp.status_code}")
        except Exception as e:
            elapsed = time.time() - request_start
            print(f"[make_did_iid] http_client.post 出错, 耗时: {elapsed:.3f}s, 错误: {e}")
            raise
    elif proxy != "":
        resp = requests.post(url, headers=headers, data=body_json, timeout=15,
                             proxies=proxies,
                             verify=False,
                             impersonate="okhttp4_android"
                             )
    else:
        resp = requests.post(url, headers=headers, data=body_json, timeout=15,
                             verify=False,
                             impersonate="okhttp4_android"
                             )
    # print(resp.text)
    # 调试
    # print("REQUEST URL:", url)
    # print("REQUEST BODY:", body_json)
    # print("STATUS:", resp.status_code)
    # print("RESP:", resp.text)

    resp.raise_for_status()
    j = resp.json()
    device_id = j.get('device_id')
    install_id = j.get('install_id')
    device["device_id"] = str(device_id) if device_id is not None else ""
    device["install_id"] = str(install_id) if install_id is not None else ""
    # print("device_id:", device_id,"install_id:", install_id)
    return [device,device_id]
def alert_check(device, proxy="", http_client=None, session=None):
    """
    检查设备告警
    
    Args:
        device: 设备信息字典
        proxy: 代理地址（如果 http_client 为 None 时使用）
        http_client: HttpClient 实例（优先使用）
        session: 可选的Session对象（如果提供，使用此Session）
    """
    use_http_client = http_client is not None
    if not use_http_client:
        proxies = {
            'http': proxy,
            'https': proxy,
        }
    now = time.time()
    utime = int(now * 1000)  # 毫秒
    stime = int(now)  # 秒
    # install_time_s = stime - random.randint(300, 1000)  # 你原来的习惯
    req_id = str(uuid.uuid4())
    cdid = device["cdid"]
    open_uid = device["openudid"]
    # phoneInfo = device["phoneInfo"]
    device_id = device["device_id"]
    # device_id = "7566195049035286030"
    install_id = device["install_id"]
    # install_id = "7566195504888792845"

    apk_last_update_time = device["apk_last_update_time"]
    last_install_time = apk_last_update_time // 1000
    resolution = device["resolution"]
    dpi = device["dpi"]
    device_type = device["device_type"]
    device_brand = device["device_brand"]
    os_api = device["os_api"]
    os_version = device["os_version"]
    ua = device["ua"]
    iid = device["install_id"]
    update_version_code = device['update_version_code']
    version_name = device['version_name']
    version_code = device['version_code']
    sdk_version_code = device['sdk_version_code']
    sdk_target_version = device['sdk_target_version']
    sdk_version = device['sdk_version']
    openudid = device['openudid']
    # 1. Use the full URL with all parameters to prevent auto-encoding issues.
    # Note: The 'tt_info' parameter already seems to be base64 encoded, which is fine.
    tt_info = f"device_platform=android&os=android&ssmix=a&_rticket={utime}&cdid={cdid}&channel=googleplay&aid=1233&app_name=musical_ly&version_code={version_code}&version_name={version_name}&manifest_version_code={update_version_code}&update_version_code={update_version_code}&ab_version={version_name}&resolution={resolution}&dpi={dpi}&device_type={device_type}&device_brand={device_brand}&language=en&os_api={os_api}&os_version={os_version}&ac=wifi&is_pad=0&current_region=US&app_type=normal&sys_region=US&last_install_time={last_install_time}&timezone_name=America/New_York&residence=US&app_language=en&timezone_offset=-18000&host_abi=arm64-v8a&locale=en&ac2=wifi&uoo=0&op_region=US&build_number={version_name}&region=US&ts={stime}&iid={iid}&device_id={device_id}&openudid={open_uid}&req_id={req_id}&google_aid={device['google_aid']}&gaid_limited=0&timezone=-5.0&custom_bt=1761217864104"
    tt_info = base64.b64encode(bytes(tt_info, "utf-8")).decode("utf-8")
    # print(tt_info)
    query_string1 = f"rticket={utime}&ab_version={version_name}&ac=wifi&ac2=wifi&aid=1233&app_language=en&app_name=musical_ly&app_type=normal&build_number={version_name}&carrier_region=US&carrier_region_v2=310&cdid={cdid}&channel=googleplay&device_brand={device_brand}&device_platform=android&device_type={device_type}&dpi={dpi}&host_abi=arm64-v8a&is_pad=0&language=en&last_install_time={last_install_time}&locale=en&manifest_version_code={update_version_code}&mcc_mnc=310004&op_region=US&openudid={openudid}&os=android&os_api={os_api}&os_version={os_version}&redirect_from_idc=maliva&region=US&req_id={req_id}&resolution={resolution}&ssmix=a&sys_region=US&timezone_name=America/New_York&timezone_offset=-18000&ts={stime}&uoo=0&update_version_code={update_version_code}&version_code={version_code}&version_name={version_name}"
    query_string = "&".join(
        f"{k}={quote(v, safe='*').replace('%25', '%')}"
        for param in query_string1.split("&")
        if "=" in param
        for k, v in [param.split("=", 1)]
    ).replace('+', '%20')
    url = f"https://log-boot.tiktokv.com/service/2/app_alert_check/?{query_string}"


    post_data = ""
    # ---------- 生成签名（传入真实 query 与 body 原始 bytes） ----------
    x_ss_stub, x_khronos, x_argus, x_ladon, x_gorgon = make_headers.make_headers(
        device_id,  # 依你的实现
        stime,
        random.randint(20, 40),
        2,
        4,
        stime - random.randint(1, 10),
        "",
        device_type,  # 用真实 model
        "", "", "", "", "",
        query_string,
        post_data  # 注意：传 bytes，不是 hex
    )
    # 2. Use a list of tuples for headers to strictly preserve the exact order.
    # Note: The ":authority:", ":method:", ":path:", ":scheme:" are HTTP/2 pseudo-headers
    # and are handled differently by HTTP clients. We map them to standard HTTP/1.1 headers.
    headers = [
        ('accept-encoding', 'gzip'),
        ('x-tt-app-init-region', 'carrierregion=;mccmnc=;sysregion=US;appregion=US'),
        ('x-tt-dm-status', 'login=0;ct=0;rt=7'),
        ('x-ss-req-ticket', f'{utime}'),
        ('sdk-version', '2'),
        ('passport-sdk-version', '-1'),
        ('x-vc-bdturing-sdk-version', '2.3.13.i18n'),
        ('user-agent',
         device['ua']),
        ('x-ladon', f'{x_ladon}'),
        ('x-khronos', f'{x_gorgon}'),
        ('x-argus',
         f'{x_argus}'),
        ('x-gorgon', f'{x_gorgon}'),
        # Map HTTP/2 pseudo-headers to standard headers
        ('Host', 'log-boot.tiktokv.com'),  # from :authority:
        # No direct mapping for :method:, :path:, :scheme: as they are part of the request line/URL
    ]

    # Note: The original request didn't show a Cookie header, so we omit it here.
    # If cookies are needed, add them similarly to previous examples:
    # ('Cookie', 'your_cookie_string_here'),

    try:
        # 3. Send the GET request.
        # The 'impersonate' parameter mimics a real Android app's TLS fingerprint.
        # Based on 'tt-ok' in the User-Agent, we choose to impersonate an OkHttp client.
        if use_http_client:
            # 使用 HttpClient（已包含重试和超时机制）
            response = http_client.get(url, headers=dict(headers), session=session)
            return "success"
        elif proxy != "":
            response = requests.get(
                url,
                headers=dict(headers),
                proxies=proxies,
                verify=False,
                timeout=30,
                impersonate="okhttp4_android"
            )
        else:
            response = requests.get(
                url,
                headers=dict(headers),
                timeout=30,
                verify=False,
                impersonate="okhttp4_android"
            )
        # print(response.text)
        # if response.text=='{"message":"success"}':
        #     print("did、iid 激活成功")
        return "success"

    except Exception as e:
        print(f"An error occurred during the request: {e}")
        return f"error: {str(e)}"
# if __name__ == '__main__':
#     proxy = "socks5h://1pjw6067-region-US-sid-rRpeJ8LA-t-6:wmc4qbge@us.novproxy.io:1000"
#     for i in range(10000):
#         try:
#             device = getANewDevice()
#
#             #
#             # DynReport(devicei["install_id"],devicei["device_id"],seed,seed_type,devicei['ua'])
#             device1,device_id= make_did_iid(device,proxy)
#             if device_id !=0:
#
#                 res1 = alert_check(device1,proxy)
#                 if res1 and res1=="success":
#                     seed, seed_type = get_get_seed(device, proxy)
#                     token = get_get_token(device, proxy)
#                     device1["seed"] = seed
#                     device1["seed_type"] = seed_type
#                     device1["token"] = token
#                     with open("11_12.txt","a",encoding="utf-8") as f:
#                         f.write(f"{device1}\n")
#             s = random.randint(5,8)
#             print(s)
#             time.sleep(s)
#         except Exception as e:
#             print(e)
#             continue