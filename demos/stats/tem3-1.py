import gzip
import json
import random
import secrets
import time

from curl_cffi import requests, curl
import urllib3

from headers import make_headers
from headers.device_ticket_data1 import build_guard
from headers.device_ticket_data import make_device_ticket_data
from headers.make_trace_id import make_x_tt_trace_id
from mssdk.get_seed.seed_test import get_get_seed
from mssdk.get_token.token_test import get_get_token

async def stats_3(aweme_id, seed, seed_type, token, device, signcount, proxy='socks5://1pjw6067-region-US-sid-rRpeJ8LA-t-6:wmc4qbge@us.novproxy.io:1000', http_client=None, session=None):
    """
    统计数据接口（异步版本）
    
    Args:
        aweme_id: 视频 ID
        seed: seed 字符串
        seed_type: seed 类型
        token: token 字符串
        device: 设备信息字典
        signcount: 签名计数
        proxy: 代理地址（如果 http_client 为 None 时使用）
        http_client: HttpClient 实例（优先使用）
        session: 可选的Session对象（如果提供，使用此Session）
    """
    use_http_client = http_client is not None
    timee = time.time()
    stime = int(timee)
    # 禁用 HTTPS 警告
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    device_id = device["device_id"]
    install_id = device["install_id"]
    ua = device["ua"]
    apk_first_install_time = device["apk_first_install_time"]
    apk_last_update_time = device["apk_last_update_time"]
    last_install_time = apk_last_update_time // 1000
    priv_key = device.get('priv_key','')
    # tt_ticket_guard_public_key = device["tt_ticket_guard_public_key"]
    device_guard_data0 = device.get('device_guard_data0','')
    if not device_guard_data0:
        device_guard = make_device_ticket_data(device,stime,"/aweme/v1/aweme/stats/")
        device_guard_data_json = {"device_token":device_guard,"dtoken_sign":""} 
        device_guard_data0 = json.dumps(device_guard_data_json)
    device_guard_data0 = json.loads(device_guard_data0)
    # print(type(device_guard_data0))
    header1 = build_guard(device_guard_data0,priv_hex=priv_key)
    # print(header1)
    # device = {"device_type":"Pixel 6"
    #           ,"ua":"com.zhiliaoapp.musically/2024204030 (Linux; U; Android 15; en; Pixel 6; Build/BP1A.250505.005; Cronet/TTNetVersion:efce646d 2025-10-16 QuicVersion:c785494a 2025-09-30)"
    #           ,"device_id":device_id,
    #           "install_id":"{install_id}",
    #           "apk_first_install_time":1762624604}

    # 代理设置 (来自你的上一个脚本)
    # proxy = 'socks5://1pjw6067-region-US-sid-rRpeJ8LA-t-6:wmc4qbge@us.novproxy.io:1000'

    # seed,seed_type = get_get_seed(device,proxy)
    # token = get_get_token(device,proxy)
    
    if not use_http_client:
        # 只有在不使用 HttpClient 时才需要设置 proxies
        if proxy=="":
            proxies = {
                'http': 'http://127.0.0.1:7777',
                'https': 'http://127.0.0.1:7777',
            }
        else:
            proxies = {
                'http': proxy,
                'https': proxy,
            }
    else:
        proxies = None
    utime = int(timee * 1000)
   
    # 目标 URL
    query_string = f"os=android&_rticket={utime}&is_pad=0&last_install_time={last_install_time}&host_abi=arm64-v8a&ts={stime}&ab_version=42.4.3&ac=wifi&ac2=wifi&aid=1233&app_language=en&app_name=musical_ly&app_type=normal&build_number=42.4.3&carrier_region=US&carrier_region_v2=310&channel=googleplay&current_region=US&device_brand=google&device_id={device_id}&device_platform=android&device_type=Pixel%206&dpi=420&iid={install_id}&language=en&locale=en&manifest_version_code=2024204030&mcc_mnc=310004&op_region=US&os_api=35&os_version=15&region=US&residence=US&resolution=1080*2209&ssmix=a&sys_region=US&timezone_name=America%2FNew_York&timezone_offset=-18000&uoo=0&update_version_code=2024204030&version_code=420403&version_name=42.4.3"
    url = f"https://aggr16-normal.tiktokv.us/aweme/v1/aweme/stats/?os=android&_rticket={utime}&is_pad=0&last_install_time={last_install_time}&host_abi=arm64-v8a&ts={stime}&"
    pre_play_time = random.randint(100,1000)
    dt = f"pre_item_playtime={pre_play_time}&user_algo_refresh_status=false&first_install_time={apk_first_install_time}&item_id={aweme_id}&is_ad=0&follow_status=0&pre_item_watch_time={utime-pre_play_time}&sync_origin=false&follower_status=0&action_time={stime}&tab_type=22&pre_hot_sentence=&play_delta=1&request_id=&aweme_type=0&order=&pre_item_id="
    post_data = dt.encode("utf-8").hex()
    data = gzip.compress(dt.encode("utf-8"))
    # POST 数据 (来自你的上一个脚本, 长度为 215 字节)
    # data = bytes.fromhex(
    #     "1f8b08000000000000ff6d8f416ec4200c456fe335610899597016cb0dce0489400a8ea2dc7e98b451abaa5b4beff9fdb53006e105d748878485ddad1b1eb0552e48f199b1f054b8ce588564ab6ea25819a650aa6048ed18239e5837586db5b1cac0e90bde0dbd7dd8db608deecdbd53c63471a848de2998728c79bfac1dac57c74e32cebf94f7de34b457508f34622ee119d255713a5ae8b745018d1272fa4b83d007cab1b2d3fa7c3467c1ca49388dece0bd1c3d47a1d651f873e3f736ef80765ef80b54908be7e27e3affdff702e43b98564f010000")
    # post_data = gzip.decompress(data).hex()
    # x_ss_stub, x_khronos, x_argus, x_ladon, x_gorgon = make_headers.make_headers(
    #         "device_id",  # 依你的实现
    #         1762685412,
    #         210,
    #         2,
    #         4,
    #         stime - random.randint(1, 10),
    #         "AtehUvuakUNaxD3fltTD4utYE",
    #         "Pixel 6",  # 用真实 model
    #         "MDGnHZ7VqnsBIDh8yTpmlIK0y7Z/TXV2UDcp9PkHSh2BLEmFFzA7FwV4Qhm3NQSm2ekBJAbOxxKfvQ7OUKWjBFOVkmDdVrABd95nmk5AuDQx/MkTFrnkoHoTMSrauYXOBPE=", 5, '', "", "",
    #         query_string,
    #         post_data  # 注意：传 bytes，不是 hex
    #     )
    x_ss_stub, x_khronos, x_argus, x_ladon, x_gorgon = make_headers.make_headers(
            device_id,  # 依你的实现
            stime,
            signcount,
            2,
            4,
            stime - random.randint(1, 10),
            token,
            device['device_type'],  # 用真实 model
            seed,seed_type , '', "", "",
            query_string,
            post_data  # 注意：传 bytes，不是 hex
        )
    # 合并所有 cookie 到一个字符串中
    cookie_string = (
        "store-idc=useast5; "
        f"passport_csrf_token={secrets.token_bytes(16).hex()}; "
        f"passport_csrf_token_default={secrets.token_bytes(16).hex()}; "
        "store-country-code=us; "
        "tt_ticket_guard_has_set_public_key=1; "
        "tt-target-idc=useast8; "
        f"d_ticket={secrets.token_bytes(16).hex()}; "
        f"multi_sids={secrets.token_bytes(16).hex()}; "
        f"cmpl_token={secrets.token_bytes(16).hex()}; "
        f"sid_guard={secrets.token_bytes(16).hex()}; "
        f"uid_tt={secrets.token_bytes(16).hex()}; "
        f"uid_tt_ss={secrets.token_bytes(16).hex()}; "
        f"sid_tt={secrets.token_bytes(16).hex()}; "
        f"sessionid={secrets.token_bytes(16).hex()}; "
        f"sessionid_ss={secrets.token_bytes(16).hex()}; "
        f"tt_session_tlb_tag={secrets.token_bytes(16).hex()}; "
        "store-country-code-src=uid; "
        f"install_id={install_id}; "
        f"ttreq=1${secrets.token_bytes(16).hex()}; "
        "store-country-sign=MEIEDNmnx6usf2uBOFnKGQQgLJE2zo3vMzsnMd6LC5zuEcdezShTDUeMFn-wLc3i2hIEEJk1fDTjlBFN13gUuf49v5I; "
        f"odin_tt={secrets.token_bytes(16).hex()}; "
        f"msToken={secrets.token_bytes(16).hex()}; "
        # "BUYER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjc1NzA2MzYxMDA1MjQ4ODUwNDcsIk9lY1VpZCI6NzQ5NDI0NTM3OTIyNDAxMjE0MCwiUGlnZW9uVWlkIjo3NTcwNjM2NTA3MzgwMjg3MjQ2LCJleHAiOjE3NjI3NzE1NzQsIm5iZiI6MTc2MjY4NDE3NH0.k6_mWm8VhDQCFEgItvXvVNzgNip-DopHKZse0Aur6ys; "
        # "user_oec_info=0a538d1b0d819252819936cbd6b421ba70d87c1048e0e6f136e6959b1729975f7b474138ec525072851ca9914bca193941cc574b6974626aef0bb21299f201ff07a5a875af1ba01a614cb331d2f5ffadd15aa03b9b1a490a3c000000000000000000004fb17bfaa1b1b3ff8bada28bc46f05d89b16123a720932414dd2770cadbd7fe1a93826601c5dafd1d4562e9f2e6e3ba3d36e10d88a810e1886d2f6f20d220104a5eec27d"
    )

    # 所有的请求头
    headers = {
        "authority": "aggr16-normal.tiktokv.us",
        "cookie": cookie_string,
        "x-tt-pba-enable": "1",
        "x-bd-kmsv": "0",
        "x-tt-dm-status": "login=1;ct=1;rt=8",
        "x-ss-req-ticket": f"{utime}",
        "sdk-version": "2",
        # "x-tt-token": "04fdb15013f227a68c1d9b3d4c712869dc0027311ad9c409182af4b9145ce6e3b5b4a5eb6bb8c2ae2e2a12a7a1f15683255921f13485b2872150c3b9b4cf4c85dbce56bec258468469c6a3ebeda1d94bd799231b1cd320d81459b3a8e5d27eea45935--0a4e0a20e8918dd7b268d857d868a762ff6663b78ffc64dd2a594098567bfbd8b3e469cf12205940a7251d2535f13c85c2164250087d3f978aef2a8759417889650f5b4682321801220674696b746f6b-3.0.1",
        "passport-sdk-version": "-1",
        "x-vc-bdturing-sdk-version": "2.3.17.i18n",
        "rpc-persist-pns-region-1": "US|6252001|5332921",
        "rpc-persist-pns-region-2": "US|6252001|5332921",
        "rpc-persist-pns-region-3": "US|6252001|5332921",
        # "tt-device-guard-iteration-version": "1",
        # "tt-ticket-guard-public-key": tt_ticket_guard_public_key,
        # "tt-device-guard-client-data": "eyJkZXZpY2VfdG9rZW4iOiIxfHtcImFpZFwiOjEyMzMsXCJhdlwiOlwiNDIuNC4zXCIsXCJkaWRcIjpcIjc1NzA0MTQ0MjQ3ODA4MzQzMThcIixcImlpZFwiOlwiNzU3MDQxNDg4ODkzODgzMzcxOVwiLFwiZml0XCI6XCIxNzYyNjI0NjA0XCIsXCJzXCI6MSxcImlkY1wiOlwidXNlYXN0OFwiLFwidHNcIjpcIjE3NjI2ODUwNTBcIn0iLCJ0aW1lc3RhbXAiOjE3NjI2ODU0MTAsInJlcV9jb250ZW50IjoiZGV2aWNlX3Rva2VuLHBhdGgsdGltZXN0YW1wIiwiZHRva2VuX3NpZ24iOiJ0cy4xLk1FVUNJUUNVUVhZZXV4aHVjeXZjdFI2M29lTEZOSTRzc3k1QkRDRkt0bUorRGZYMHJBSWdhU005UWZSWFNhQW1hSEhCOVwvWWpWQkszVTh0V1Vqa3BFR1FXV2RMVlpVaz0iLCJkcmVxX3NpZ24iOiJNRVVDSURpbmlUeWNBMWZMUGZxK3lIbHlqbHZxNVFGR1dZOXhTXC85VmhWZGtORzJYQWlFQTRjdlwva21LVXZHZVVvaURJemtWbFNcLzEzSFVYK1gzVlhCMmVOK0NsZ3IrRT0ifQ==",
        # "tt-device-guard-client-data": make_device_ticket_data(device,stime,"/aweme/v1/aweme/stats/"),
        "oec-vc-sdk-version": "3.2.1.i18n",
        "x-tt-request-tag": "n=0;nr=111;bg=0;rs=112",

        # 这一行是关键，它告诉服务器 body 是 gzip 压缩过的
        "x-bd-content-encoding": "gzip",

        # Content-Type 仍然是 x-www-form-urlencoded
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",

        "x-ss-stub": f"{x_ss_stub}",
        "rpc-persist-pyxis-policy-state-law-is-ca": "1",
        "rpc-persist-pyxis-policy-v-tnc": "1",
        "x-tt-ttnet-origin-host": "api16-core-useast8.tiktokv.us",
        "x-ss-dp": "1233",
        # "x-tt-trace-id": "00-683ce03b10690f83cf3f8e060e3b04d1-683ce03b10690f83-01",
        "x-tt-trace-id":make_x_tt_trace_id(1,device_id),
        "user-agent": ua,
        "accept-encoding": "gzip, deflate, br",

        # 你的签名参数
        "x-argus": f"{x_argus}",
        "x-gorgon": f"{x_gorgon}",
        "x-khronos": f"{stime}",
        "x-ladon": f"{x_ladon}",

        "x-common-params-v2": f"ab_version=42.4.3&ac=wifi&ac2=wifi&aid=1233&app_language=en&app_name=musical_ly&app_type=normal&build_number=42.4.3&carrier_region=US&carrier_region_v2=310&channel=googleplay&current_region=US&device_brand=google&device_id={device_id}&device_platform=android&device_type=Pixel%206&dpi=420&iid={install_id}&language=en&locale=en&manifest_version_code=2024204030&mcc_mnc=310004&op_region=US&os_api=35&os_version=15&region=US&residence=US&resolution=1080*2209&ssmix=a&sys_region=US&timezone_name=America%2FNew_York&timezone_offset=-18000&uoo=0&update_version_code=2024204030&version_code=420403&version_name=42.4.3",
    }
    headers.update(header1)
    
    # 由于 impersonate 在 Session 上可能不生效，stats 接口始终不使用 session
    # cookie 保留在 headers 中
    headers_copy = headers
    
    # print(headers)
    try:
        if use_http_client:
            # 使用 HttpClient（已包含重试和超时机制）
            # stats 接口不使用 session，让 HttpClient 从池中获取新连接
            resp = await http_client.post(
                url,
                headers=headers_copy,
                data=data,
                session=session,  # 不使用 session，确保 impersonate 生效
                impersonate="okhttp4_android",  # 模拟 OkHttp 4 Android 的 TLS 指纹
                http_version="v2"  # 强制使用 HTTP/2
            )
        else:
            resp = requests.post(
                url,
                headers=headers_copy,
                data=data,
                verify=False,
                proxies=proxies,
                impersonate="okhttp4_android",  # 模拟 OkHttp 4 Android 的 TLS 指纹
                http_version="v2"  # 强制使用 HTTP/2
            )

        print(f"[设备: {device_id}] Status Code: {resp.status_code}")
        if resp.text:
            try:
                resp_json = json.loads(resp.text)
                status_code = resp_json.get("status_code", -1)
                if status_code == 0:
                    print(f"[设备: {device_id}] ✓ 播放成功")
                else:
                    print(f"[设备: {device_id}] ✗ 播放失败，status_code={status_code}")
            except:
                print(f"[设备: {device_id}] 响应: {resp.text[:100]}")
        else:
            print(f"[设备: {device_id}] ✗ 响应为空")
        return resp.text

    except Exception as e:
        if use_http_client:
            # HttpClient 已包含重试机制，如果失败则抛出异常
            raise
        else:
            # 使用 requests 时，尝试 HTTP/1 降级
            resp = requests.post(
                url,
                headers=headers_copy,
                data=data,
                verify=False,
                proxies=proxies,
                impersonate="okhttp4_android",  # 模拟 OkHttp 4 Android 的 TLS 指纹
                http_version="v1"  # 强制使用 HTTP/1
            )

            print(f"[设备: {device_id}] Status Code: {resp.status_code} (HTTP/1 fallback)")
            if resp.text:
                try:
                    resp_json = json.loads(resp.text)
                    status_code = resp_json.get("status_code", -1)
                    if status_code == 0:
                        print(f"[设备: {device_id}] ✓ 播放成功")
                    else:
                        print(f"[设备: {device_id}] ✗ 播放失败，status_code={status_code}")
                except:
                    print(f"[设备: {device_id}] 响应: {resp.text[:100]}")
            else:
                print(f"[设备: {device_id}] ✗ 响应为空")
            return resp.text