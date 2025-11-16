import uuid
import ast
import base64
import random
import time
import json
import uuid
from urllib.parse import quote

from curl_cffi import requests


from headers import make_headers

def alert_check(device,proxy=""):
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
    ).replace(' ', '%20')
    url = f"https://aggr16-normal.tiktokv.us/service/2/app_alert_check/?{query_string}"


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
        # ('accept-encoding', 'gzip'),
        # ('x-tt-app-init-region', 'carrierregion=;mccmnc=;sysregion=US;appregion=US'),
        # ('x-tt-dm-status', 'login=0;ct=0;rt=7'),
        ('x-ss-req-ticket', f'{utime}'),
        # ('sdk-version', '2'),
        # ('passport-sdk-version', '-1'),
        # ('x-vc-bdturing-sdk-version', '2.3.13.i18n'),
        ('user-agent',
         device['ua']),
        ('x-ladon', f'{x_ladon}'),
        ('x-khronos', f'{x_khronos}'),
        ('x-argus',
         f'{x_argus}'),
        ('x-gorgon', f'{x_gorgon}'),
        # Map HTTP/2 pseudo-headers to standard headers
        # ('Host', 'aggr16-normal.tiktokv.us'),  # from :authority:
        # No direct mapping for :method:, :path:, :scheme: as they are part of the request line/URL
    ]
    headers=dict(headers)
    headers.update({
        'cookie': ' store-idc=useast5; store-country-code=us; store-country-code-src=did; store-country-sign=MEIEDPCmtopBVcElQCE0mwQg7okIIP5Cg8qaqwgPW6iso2W6v5mO9GVp15rVhuoS3v4EEPDECKyFRZ4aMH5szrqti0c; install_id=7572811438918960951',
        'sdk-version': '2',
        'x-tt-dm-status': 'login=0;ct=0;rt=7',
        'passport-sdk-version': '-1',
        'x-vc-bdturing-sdk-version': '2.3.17.i18n',
        'rpc-persist-pyxis-policy-state-law-is-ca': '1',
        'rpc-persist-pyxis-policy-v-tnc': '1',
        'x-tt-ttnet-origin-host': 'log16-normal-useast8.tiktokv.us',
        'x-ss-dp': '1233',
        # 'x-tt-trace-id': '00-8b35b4dd010d15ed4c5238eb10d604d1-8b35b4dd010d15ed-01',
        'accept-encoding': 'gzip, deflate',
    })

    # Note: The original request didn't show a Cookie header, so we omit it here.
    # If cookies are needed, add them similarly to previous examples:
    # ('Cookie', 'your_cookie_string_here'),

    try:
        # 3. Send the GET request.
        # The 'impersonate' parameter mimics a real Android app's TLS fingerprint.
        # Based on 'tt-ok' in the User-Agent, we choose to impersonate an OkHttp client.
        if proxy!="":
            response = requests.get(
                url,
                headers=dict(headers)
                ,proxies=proxies,verify=False,impersonate="chrome131_android"
            )
        else:
            response = requests.get(
                url,
                headers=dict(headers),impersonate="chrome131_android",proxies={"http":"http://127.0.0.1:7777",
                                                                               "https":"http://127.0.0.1:7777",
                                                                               },verify=False

            )
        print(response.text)
        if response.text=='{"message":"success"}':
            print("did、iid 激活成功")
        return "success"

    except Exception as e:
        print(f"An error occurred during the request: {e}")
