import random
import secrets
import uuid

from tt_protobuf import tk_pb2
import base64


def generate_fake_mediadrm_id(num_bytes=32):
    """
    生成一个模拟的 MediaDrm deviceId。
    您提供的示例是一个 32 字节原始数据的 Base64 编码。

    @param num_bytes: 原始随机字节的长度。
    @return: Base64编码的字符串。
    """

    # 1. 生成 32 字节的加密安全随机数据
    random_bytes = secrets.token_bytes(num_bytes)

    # 2. 将字节编码为 Base64 字符串
    # .decode('utf-8') 是为了将 bytes 类型的 b'...' 转换为 str 类型 '...'
    base64_id = base64.b64encode(random_bytes).decode('utf-8')

    return base64_id


def generate_random_apk_path():
    """
    按照 Android 10+ 的路径混淆模式，
    生成一个随机的 com.zhiliaoapp.musically (TikTok) APK 路径。
    """

    # 1. 生成第一个 16 字节的随机数据
    random_bytes_1 = secrets.token_bytes(16)

    # 2. 生成第二个 16 字节的随机数据
    random_bytes_2 = secrets.token_bytes(16)

    # 3. 将它们编码为标准 Base64 字符串
    #    (b64encode 会自动处理 '==' 填充)
    #    (使用 .decode('utf-8') 将 bytes 转换为 str)
    b64_str_1 = base64.b64encode(random_bytes_1).decode('utf-8')
    b64_str_2 = base64.b64encode(random_bytes_2).decode('utf-8')

    # 4. 按照固定模式拼接字符串
    path = (
        f"/data/app/~~{b64_str_1}/"
        f"com.zhiliaoapp.musically-{b64_str_2}/"
        f"base.apk"
    )

    return path
def make_token_encrypt(stime:int,device_id:str):
    token_encrypt = tk_pb2.TokenEncrypt()
    token_encrypt_one = token_encrypt.one
    token_encrypt_one.notset1 = "!notset!"
    token_encrypt_one.changshang = "google"
    token_encrypt_one.xinghao = "Pixel 6"
    token_encrypt_one.notset2 = "!netset!"
    token_encrypt_one.os = "Android"
    token_encrypt_one.os_version = "15"
    tokenencrypt_one_one = token_encrypt_one.tokenEncrypt_one_one
    tokenencrypt_one_one.unknown1 = 3472332702763464752
    token_encrypt_one.density_dpi = 840
    token_encrypt_one.build_id = "BP1A.250505.005"
    token_encrypt_one.os_build_time = 3346620910
    token_encrypt_one.appLanauge = "en_"
    token_encrypt_one.time_zone = "America/New_York,-5"
    token_encrypt_one.unknown2 = random.randint(1,50)<<1
    token_encrypt_one.unknown3 = random.randint(50,100)<<1
    token_encrypt_one.unknown4 = 15887769600
    token_encrypt_one.stable1 = 118396899328<<1
    token_encrypt_one.stable2 = 118396899328<<1
    token_encrypt_one.unknown5 = 141133357056  # 不清楚这个是什么
    token_encrypt_one.notset3 = "!netset!"
    token_encrypt_one.notset4 = "!netset!"
    token_encrypt_one.android_id = secrets.token_hex(8) # "c65ef8e45e3962e2"
    token_encrypt_one.notset5 = "!notset!"
    token_encrypt_one.notset6 = "!notset!"
    token_encrypt_one.MediaDrm = generate_fake_mediadrm_id() # "XGLBzRJRAagAiXYczSAvjtLEwT9VJYq86JBRjhtYTJQ="
    token_encrypt_one.laungh_time = (stime-random.randint(5,20))<<1
    token_encrypt_one.boot_id = str(uuid.uuid4()) #"0928238b-94f5-4b81-bb81-34aa8959c561"
    token_encrypt_one.unknown6 = 755285745664
    token_encrypt_one.notset7 = "!netset!"
    token_encrypt_one.stable3 = 1999997
    token_encrypt_one.stable4 = 1999997
    token_encrypt_one.notset8 = "!netset!"
    r1 = random.randint(1,3)
    r2 = random.randint(111,199)
    r3 = random.randint(50,166)
    token_encrypt_one.default_gateway = f"192.168.{r2}.{r1}" #"192.168.182.3"
    token_encrypt_one.ip_dns = f"192.168.{r2}.{r3}" # "192.168.182.93"
    token_encrypt_one.ip_array =f'["192.168.{r2}.{r3}","0.0.0.0"]' # '["192.168.182.93","0.0.0.0"]'
    token_encrypt_one.expired_time = (stime+14350)<<1
    token_encrypt_one.send_time = stime<<1
    token_encrypt_one.install_path = generate_random_apk_path() # "/data/app/~~30YWW5tbWW5r3Zr412T06w==/com.zhiliaoapp.musically-5xwrN8HWz4XeNjDkfiUimQ==/base.apk"
    token_encrypt_one.os_api = 70
    token_encrypt_one.notset9 = "!netset!"
    # token_encrypt_one.notset10 = "!netset!"
    token_encrypt_one.stable5 = 1999997
    token_encrypt_one.stable6 = 1999997
    token_encrypt_one.stable7 = 1999997
    token_encrypt_one.stable8 = 1999997
    token_encrypt_one.stable9 = 1999997
    token_encrypt_one.stable10 = 1999997
    token_encrypt_one.stable11 = 1999997
    token_encrypt_one.stable12 = 1999997
    token_encrypt_one.notset11 = "!netset!"
    token_encrypt_one.notset12 = "!netset!"
    token_encrypt_one.notset13 = "!netset!"
    token_encrypt_one.notset14 = "!netset!"
    token_encrypt_one.notset15 = "!netset!"
    token_encrypt_one.notset16 = "!netset!"
    token_encrypt_one.notset17 = "!netset!"

    token_encrypt.last_token = "" # "AXab6rI6tG9L1tsZbnJH-CY_z"
    token_encrypt.os = "android"
    token_encrypt.sdk_ver = "v05.02.00-alpha.9-ov-android"
    token_encrypt.sdk_ver_code= 84017184<<1
    token_encrypt.msAppID = "1233"
    token_encrypt.appVersion = "40.6.3"
    token_encrypt.device_id = f'{device_id}'

    token_encrypt_two = token_encrypt.two
    # token_encrypt_two.s1 = 48
    # token_encrypt_two.s2 = 48
    # token_encrypt_two.s3 = 48
    # token_encrypt_two.s4 = 48
    # token_encrypt_two.s5 = 48
    # token_encrypt_two.s6 = 48
    token_encrypt_two.s6.extend([48, 48, 48, 48,48, 48, 48, 48])
    # token_encrypt_two.s7 = 48
    # token_encrypt_two.s8 = 48


    token_encrypt.stable1 = 1999997
    token_encrypt.notset1 = "!netset!"
    # token_encrypt.unknown2 = "4e40c692-7c6b-45eb-9b7c-dd78ffa30e15"
    token_encrypt.unknown2 = str(uuid.uuid4())
    token_encrypt.stable2 = 1999997


    serialized_data = token_encrypt.SerializeToString()
    return serialized_data.hex()
    pass
def make_token_request(token_encrypt:hex, utime:int):
    token_request = tk_pb2.TokenRequest()
    token_request.s1 = 538969122<<1
    token_request.s2 = 2
    token_request.s3 = 4
    token_request.token_encrypt = bytes.fromhex(token_encrypt)
    token_request.utime = utime <<1
    serialized_data = token_request.SerializeToString()
    return serialized_data.hex()
    pass
def make_token_decrypt(decrypt:hex):
    token_decrypt = tk_pb2.TokenDecrypt()
    token_decrypt.ParseFromString(bytes.fromhex(decrypt))
    return token_decrypt
    pass
def make_token_response(response:hex):
    token_response = tk_pb2.TokenResponse()
    token_response.ParseFromString(bytes.fromhex(response))
    return token_response