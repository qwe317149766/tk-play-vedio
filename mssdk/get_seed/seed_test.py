import time,uuid
import urllib

# import requests

import requests
from curl_cffi import requests
import urllib3
from tt_protobuf import  make_seed_pb,tk_pb2
from mssdk.endecode import mssdk_endecode
from headers import make_headers
# 1. Define the URL and query parameters
import hashlib, time, datetime, base64, os, random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 禁用 HTTPS 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import jpype
from Crypto.Cipher import AES
# 我们需要 pad 和 unpad 分别用于加密填充和解密逆填充
from Crypto.Util.Padding import pad, unpad
# 原始代码导入了 gmssl，但在加密路径中并未使用
# from gmssl import sm3, func
import struct
import zlib


class tt_XTEA:
    """
    一个纯 Python 实现的 XTEA 算法类，支持自定义轮数。
    """

    def __init__(self, key: bytes, rounds: int = 32):
        if len(key) != 16:
            raise ValueError("密钥长度必须是 16 字节")
        self.rounds = rounds
        self.delta = 0x9E3779B9
        self.key = struct.unpack('>4I', key)

    def encrypt_block(self, block: bytes) -> bytes:
        """对单个 64位 (8字节) 块进行加密"""
        if len(block) != 8:
            raise ValueError("数据块长度必须是 8 字节")

        v0, v1 = struct.unpack('>2I', block)
        s = 0
        for _ in range(self.rounds):
            v0 = (v0 + ((((v1 << 4) ^ (v1 >> 5)) + v1) ^ (s + self.key[s & 3]))) & 0xFFFFFFFF
            s = (s + self.delta) & 0xFFFFFFFF
            v1 = (v1 + ((((v0 << 4) ^ (v0 >> 5)) + v0) ^ (s + self.key[(s >> 11) & 3]))) & 0xFFFFFFFF

        return struct.pack('>2I', v0, v1)

    def decrypt_block(self, block: bytes) -> bytes:
        """对单个 64位 (8字节) 块进行解密"""
        if len(block) != 8:
            raise ValueError("数据块长度必须是 8 字节")

        v0, v1 = struct.unpack('>2I', block)
        s = (self.delta * self.rounds) & 0xFFFFFFFF
        for _ in range(self.rounds):
            v1 = (v1 - ((((v0 << 4) ^ (v0 >> 5)) + v0) ^ (s + self.key[(s >> 11) & 3]))) & 0xFFFFFFFF
            s = (s - self.delta) & 0xFFFFFFFF
            v0 = (v0 - ((((v1 << 4) ^ (v1 >> 5)) + v1) ^ (s + self.key[s & 3]))) & 0xFFFFFFFF

        return struct.pack('>2I', v0, v1)


class tt_zlib:
    def __init__(self, hex_str: str):
        self.hex_str = hex_str
        self.hex_str1 = bytes.fromhex(hex_str)

    def compressed(self):
        # # # # 原始代码使用 level=0，这实际上不进行压缩，只是添加 zlib 头。
        # # # # 这里可以根据需要调整压缩等级，例如 level=9
        # #
        # # jvmPath = jpype.getDefaultJVMPath()
        # # d = r'D:\vscode\reverse\app\shizhan\tt\mwzzzh\mssdk\endecode\unidbg.jar'  # 对应jar地址
        # # # jpype.startJVM(jvmPath, "-ea", "-Djava.class.path=" + d + "")
        # # jpype.startJVM(jvmPath, "-Dfile.encoding=utf-8", "-Djava.class.path=" + d + "")  # 输出乱码时使用
        # java = jpype.JClass("com.tt4063.get_seed")(
        #     self.hex_str)  # 从com开始找到打包jar的类
        # signature = java.call_153B58()  # 调用java的com.bytedance.frameworks.core.encrypt.CS类的RSA_encrypt方法
        # # print(signature)
        # res = str(signature)
        # # jpype.shutdownJVM()
        # # return "78010190026ffde2a33328472bd648e4a5e9a1fe13c2b6f057fafe2a027c37ceb865566fa8fa767a3cfe49ff7eabb196870f11a25de809f520d46c9bcd3af32f1208224866fb48b9734e33dd84645357eef1430b8329a26e043f0276de009b9c91e1fe6c84a408747b9a8cde1b9392cc6a8cce5b6e56afa633833ff304aae5b24f86a2eb846f43efa7220619ae772777dc2024fe5a546945a44009178f8329a0cbb5f656fcf22f0458f6fe43bf201ac5581bd6a07c5c0fefd4476de0cb05394c3b518882f1af61e5adbf24f145cfee48489379c30e5d602d6c6d27e5abb3fdf7686c245b360b06cd6d0f69659ff14e47442b7df51df88c09348f4ac80ee6f7270a09228cc966a581c955597120a3ac57ff0c35af0f4278d667e60fb533e6bc3754b93965e1b8c0c86457cd9be1f1fce00678373d4b27d7b5d4ab9f59f9605b616456162c862af860f59783e36fbd574f4001626af58e37dce2a93e3218107603c50966065e4eb2e862a8c0f9b411a47942cf7f21df73c4c2c7c7013ddbc43ef29a2531000f2ee5829dc8130a2a592df3f7a134ac547a59b0b8859dae51d27c2862398538f90c0f1feed1d5e0d350a991ac209951114738b58147e433ad4c48abd897290280744fa5ee8fc19a81bd723fa02e2946af48b73603e3c5ae891b1987629de563e17cca5ee8742d937852f67222befc7d7480041b497a59294a1836b2f19f70bd97a723f0d066559618e0d28a9162a5aad24886e2775aa9d90468dafae14eb015fdc9d77374bf75cff7b79de37c98610a4f60fa024a0dd5483b031ddeb3ee4a08f4dd87274cb20b923d5b13dac0c6b8131ca4b78c53fcd6e51a1ce89c893ed1390e162ab9ef05038c111b8db0b9c55197d6c58f267970e45ca1e3bc52215d368501964a3ddbd936a26d04cd252de91fc278d1609b054332"
        # return res

        return zlib.compress(self.hex_str1, level=1).hex()
        # return "78010540b10d8020102c8d96da59190730f7073c308ef090d86862e1fc66dad44a704291a6e23d593476a7b5364847329b9718484d60ce8e502f88791dcedbdee7b27dfc100ef0007ea3e511d9"
    def uncompressed(self):
        return zlib.decompress(self.hex_str1).hex()


def make_rand():  # 生成一个四字节16进制随机数
    return os.urandom(4).hex()


def get_tea_report_key() -> hex:
    # data = bytes.fromhex("v05.02.00-ov-android")
    data = "v05.02.00-ov-android".encode("utf-8")  # 这个对应的是sdk_version_str,argus protobuf中的第八项
    w12 = 0x26000
    w11 = 0x280000
    w13 = 0x9000
    w17 = (w12 + (3 << 12)) & 0xFFFFFFFF
    w14 = 0x15000000
    w15 = data[2]
    w0 = data[5]
    w10 = data[8]
    w16 = 0x5f00000
    w2 = (w15 << 8) & 0xFFFFFFFF
    w17 = w2 ^ w17
    w13 = w2 & w13
    w2 = w12 | 0x200
    w11 = (w11 | (w0 << 20)) & 0xFFFFFFFF
    w0 = (w0 << 0x18) & 0xFFFFFFFF
    w13 = w17 | w13
    w17 = w17 & w2
    w2 = w0 & 0xfdffffff
    w0 = w0 & w14
    w14 = w2 ^ w14
    w2 = (w10 << 8) & 0xFFFFFFFF
    w15 = (w15 << 0x10) & 0xFFFFFFFF
    w10 = (w10 << 0x14) & 0xFFFFFFFF
    w16 = w15 ^ w16
    w15 = w15 & 0xf00000
    w10 = w10 & 0xfeffffff
    w15 = w15 | w16
    w16 = w2 & 0x6000
    w10 = w10 | 0x38000000
    w12 = w16 ^ w12
    w10 = w11 ^ w10
    w11 = w14 | w0
    w14 = 0x216249
    w16 = 0x3f47825
    w10 = w10 ^ w14

    w14 = w11 | w16
    w11 = w11 & 0x1000000
    w10 = w13 | w10
    w11 = w11 | 0x200000
    w10 = (w10 - w17) & 0xFFFFFFFF
    data_first_4_bytes = w10.to_bytes(4, "little")

    w11 = (w14 - w11) & 0xFFFFFFFF
    w8 = w15 & ~ w11
    w10 = w11 & ~w15
    w12 = (w12 + w2) & 0xFFFFFFFF
    w8 = w8 | w10
    w10 = w8 | w12
    w8 = w8 & w12
    w8 = (w10 - w8) & 0xFFFFFFFF
    data_second_4_bytes = w8.to_bytes(4, "little")
    return (data_first_4_bytes + data_second_4_bytes).hex()


# print(get_tea_report_key().hex())
word_19DED0 = [0, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6
    , 0x70E7, 0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD
    , 0xE1CE, 0xF1EF, 0x1231, 0x210, 0x3273, 0x2252, 0x52B5
    , 0x4294, 0x72F7, 0x62D6, 0x9339, 0x8318, 0xB37B, 0xA35A
    , 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE, 0x2462, 0x3443, 0x420
    , 0x1401, 0x64E6, 0x74C7, 0x44A4, 0x5485, 0xA56A, 0xB54B
    , 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D, 0x3653
    , 0x2672, 0x1611, 0x630, 0x76D7, 0x66F6, 0x5695, 0x46B4
    , 0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D
    , 0xC7BC, 0x48C4, 0x58E5, 0x6886, 0x78A7, 0x840, 0x1861
    , 0x2802, 0x3823, 0xC9CC, 0xD9ED, 0xE98E, 0xF9AF, 0x8948
    , 0x9969, 0xA90A, 0xB92B, 0x5AF5, 0x4AD4, 0x7AB7, 0x6A96
    , 0x1A71, 0xA50, 0x3A33, 0x2A12, 0xDBFD, 0xCBDC, 0xFBBF
    , 0xEB9E, 0x9B79, 0x8B58, 0xBB3B, 0xAB1A, 0x6CA6, 0x7C87
    , 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0xC60, 0x1C41, 0xEDAE
    , 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49
    , 0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13, 0x2E32, 0x1E51
    , 0xE70, 0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A
    , 0x9F59, 0x8F78, 0x9188, 0x81A9, 0xB1CA, 0xA1EB, 0xD10C
    , 0xC12D, 0xF14E, 0xE16F, 0x1080, 0xA1, 0x30C2, 0x20E3
    , 0x5004, 0x4025, 0x7046, 0x6067, 0x83B9, 0x9398, 0xA3FB
    , 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E, 0x2B1, 0x1290
    , 0x22F3, 0x32D2, 0x4235, 0x5214, 0x6277, 0x7256, 0xB5EA
    , 0xA5CB, 0x95A8, 0x8589, 0xF56E, 0xE54F, 0xD52C, 0xC50D
    , 0x34E2, 0x24C3, 0x14A0, 0x481, 0x7466, 0x6447, 0x5424
    , 0x4405, 0xA7DB, 0xB7FA, 0x8799, 0x97B8, 0xE75F, 0xF77E
    , 0xC71D, 0xD73C, 0x26D3, 0x36F2, 0x691, 0x16B0, 0x6657
    , 0x7676, 0x4615, 0x5634, 0xD94C, 0xC96D, 0xF90E, 0xE92F
    , 0x99C8, 0x89E9, 0xB98A, 0xA9AB, 0x5844, 0x4865, 0x7806
    , 0x6827, 0x18C0, 0x8E1, 0x3882, 0x28A3, 0xCB7D, 0xDB5C
    , 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A, 0x4A75
    , 0x5A54, 0x6A37, 0x7A16, 0xAF1, 0x1AD0, 0x2AB3, 0x3A92
    , 0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8
    , 0x8DC9, 0x7C26, 0x6C07, 0x5C64, 0x4C45, 0x3CA2, 0x2C83
    , 0x1CE0, 0xCC1, 0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B
    , 0xBFBA, 0x8FD9, 0x9FF8, 0x6E17, 0x7E36, 0x4E55, 0x5E74
    , 0x2E93, 0x3EB2, 0xED1, 0x1EF0]


def make_two_part(data: hex):
    data = bytes.fromhex(data)
    length = len(data)
    if length < 1:
        return 0

    # C 代码中用作累加器的 result 变量，我们用一个更清晰的名字 hash_val
    # LODWORD(result) = 0;
    hash_val = 0

    # 循环遍历每一个字节
    # do { ... } while (a2);
    for current_byte in data:
        # C: BYTE1(result)
        # 获取 hash_val 的第二个字节 (0x11223344 中的 0x33)
        byte1_of_hash = (hash_val >> 8) & 0xFF

        # C: (unsigned __int8)((v3 | BYTE1(result)) - (v3 & BYTE1(result)))
        # 这等价于 (current_byte ^ byte1_of_hash)
        table_index = current_byte ^ byte1_of_hash

        # C: word_19DED0[...]
        # 从查找表中获取值
        lookup_value = word_19DED0[table_index]

        # C: ((_DWORD)result << 8)
        # 将当前的哈希值左移8位
        shifted_hash = hash_val << 8

        # C: lookup_value ^ shifted_hash
        # 将查表值与移位后的哈希值进行异或
        new_hash = lookup_value ^ shifted_hash

        # 在Python中，整数是无限精度的。C 代码中的 `unsigned int` 或 `_DWORD`
        # 意味着结果会被限制在32位。我们通过与 0xFFFFFFFF 进行按位与操作来模拟这个行为。
        hash_val = new_hash & 0xFFFFFFFF
    # 确定长度
    dataLen = len(data)
    w9 = (-dataLen) & 0x7
    w10 = w9 ^ 7
    w9 = (w9 << 1) & 0b111
    w24 = (w9 + w10) & 0xFFFFFFFF
    w9 = (w24 + 3) & 0xFFFFFFFF
    w9 = w9 if w24 < 0 else w24
    w9 = w9 & 0xFFFFFFFC
    w21 = (w24 - w9)
    shift = (4 - w21) * 8
    return (((hash_val << shift) & 0xFFFFFFFF) >> shift).to_bytes(w21, "big").hex()


def int_to_hexstr(num: int):
    """将数字转换为16进制字符串并去除最前面的0x"""
    # 使用格式化 {:02x} 来确保一个字节总能表示为两个十六进制字符
    return f'{num:02x}'


def make_protobuf():
    return "0a2062626530613161633632613634383864613730616633636137303663333738341213373534393132303130353235313831363937381a07616e64726f696422097630352e30322e3030";


def xor_bytes(b1: bytes, b2: bytes) -> bytes:
    """对两个等长的字节串进行异或操作"""
    return bytes(x ^ y for x, y in zip(b1, b2))


def get_XTEA_key(is_report: bool) -> str:
    if is_report == False:
        return "782399bdfacedead3230313030343034"  # 非report协议的key
    else:
        return get_tea_report_key() + "3230313030343034"
        return "49d70939250823333230313030343034"  # report协议的key,与sdk的版本有关


def CBC_XTEA_encryptORdecrypt(iv: str, key: str, data: str, is_encrypt: bool) -> str:
    """
    复现 C++ 伪代码中的 XTEA-CBC 加解密流程。
    """
    # XTEA 处理的数据长度必须是块大小（8字节）的整数倍
    # 这个修正后的逻辑使用空字节（\x00）填充至8字节的倍数
    data_bytes = bytearray.fromhex(data)
    padding_len = 16 - (len(data_bytes) % 8)
    # print("xtea before padding:", data_bytes.hex())
    if padding_len != 16:
        data_bytes += bytearray(padding_len)
    # print("xtea after padding:", data_bytes.hex())
    data = data_bytes

    # 通过初始iv计算XTEA轮数
    iv_bytes = bytes.fromhex(iv)
    v14 = struct.unpack('<I', iv_bytes[:4])[0]
    rounds = (8 * (((2 * (v14 % 5)) & 8) | (v14 % 5))) ^ 0x20
    # print(f"XTEA 轮数: {rounds}")

    derived_key = bytes.fromhex(key)

    # 创建 XTEA 实例
    cipher = tt_XTEA(key=derived_key, rounds=rounds)

    # chaining_block 是 CBC 模式实际使用的初始化向量 (IV)
    chaining_block = bytes.fromhex(iv)

    output_data = b''

    for i in range(0, len(data), 8):
        current_block = data[i:i + 8]

        if is_encrypt == True:
            block_to_encrypt = xor_bytes(current_block, chaining_block)
            encrypted_block = cipher.encrypt_block(block_to_encrypt)
            output_data += encrypted_block
            chaining_block = encrypted_block
        else:
            decrypted_block = cipher.decrypt_block(current_block)
            plaintext_block = xor_bytes(decrypted_block, chaining_block)
            output_data += plaintext_block
            chaining_block = current_block

    return output_data.hex()


def last_aes_encrypt(data: str) -> str:
    data_bytes = bytearray.fromhex(data)
    key = bytes.fromhex("b8d72ddec05142948bbf2dc81d63759c")
    iv = bytes.fromhex("d6c3969582f9ac5313d39c180b54a2bc")
    cipher = AES.new(key, AES.MODE_CBC, iv)
    # 原始代码注释提到是魔改填充，但实际实现使用的是标准的 PKCS7 填充

    # if len(data_bytes) % 16 != 0:
    #     data_bytes += bytearray([0]*(15 - len(data_bytes) % 16)+[len(data_bytes) % 16])
    # print("after aes padding:", data_bytes.hex())
    ciphertext = cipher.encrypt(pad(data_bytes, AES.block_size))
    return ciphertext.hex()

def mssdk_encrypt(pb:hex,is_report: bool):
    # pb = make_protobuf()
    zlib1 = tt_zlib(pb)
    zlib_res = zlib1.compressed()
    # zlib_res = "78010540310e80200c1c8d8eba39191f604ea0b47d4ea190b868e2e0fbcdb479632d42d552cd894b33f16c1d9d948c8bf0bc3011348b08219e4191425c07bbfd7d2edfc70f74201cc00fe27712c2bf29"
    pb_length = len(pb) // 2
    # print(pb_length)
    three_part = struct.pack('<I', pb_length).hex()
    zlib_res = three_part + zlib_res
    # print("11",zlib_res)
    byte_one = int_to_hexstr((((int(zlib_res[-2:], 16) ^ (pb_length & 0xff)) << 1) & 0xf8) | 0x7)
    # print("22",zlib_res)
    part_two = make_two_part(zlib_res)
    for_xtea = byte_one + part_two + zlib_res
    # for_xtea = "cfed874b000000"
    # print(for_xtea)
    key = get_XTEA_key(is_report)
    iv_for_byte = make_rand()
    iv_for_byte =hex(random.randint( 0xc0133eb0,0xc0133ebf))[2:]
    # iv_for_byte = "20873a72"
    # print("four_byte", iv_for_byte)
    xtea_encrypted = CBC_XTEA_encryptORdecrypt(
        iv=iv_for_byte + "27042020",
        key=key,  # 此 key是固定的
        data=for_xtea,
        is_encrypt=True
    )
    # print("xtea_encrypted",xtea_encrypted)

    # 在 XTEA 密文前添加一个修改后的字节
    first_xtea_byte = int(xtea_encrypted[:2], 16)
    modified_byte = first_xtea_byte ^ 0x3
    # 重新组装十六进制字符串
    for_aes = int_to_hexstr(modified_byte) + xtea_encrypted[:] + iv_for_byte
    # print("for_res",for_aes)

    res = last_aes_encrypt(for_aes)
    # print("res",res)
    return res
def get_get_seed(cookie_data:dict, proxy="", http_client=None, session=None):
    """
    获取 seed
    
    Args:
        cookie_data: 设备信息字典
        proxy: 代理地址（如果 http_client 为 None 时使用）
        http_client: HttpClient 实例（优先使用）
        session: 可选的Session对象（如果提供，使用此Session）
    """
    use_http_client = http_client is not None
    # 配置代理
    if not use_http_client:
        if proxy:
            # 确保代理 URL 格式正确
            proxies = {
                'http': proxy,
                'https': proxy,
            }
        else:
            proxies = None
    else:
        proxies = None
    # iid = cookie_data["install_id"]
    # device_id = cookie_data["device_id"]
    # ttreq = cookie_data["ttreq"]
    # passport_csrf_token = cookie_data["passport_csrf_token"]
    # cmpl_token = cookie_data["cmpl_token"]
    # d_ticket = cookie_data["d_ticket"]
    # multi_sids = cookie_data["multi_sids"]
    # sessionid = cookie_data["sessionid"]
    # sid_guard = cookie_data["sid_guard"]
    # uid_tt = cookie_data["uid_tt"]
    # msToken = cookie_data["msToken"]
    # odin_tt = cookie_data["odin_tt"]
    # store_country_sign = cookie_data["store-country-sign"]
    # s_v_web_id = cookie_data["s_v_web_id"]
    # x_tt_token = cookie_data["X-Tt-Token"]
    ua = cookie_data["ua"]
    iid = cookie_data["install_id"]
    device_id = cookie_data["device_id"]
    device_type = cookie_data["device_type"]
    # sender_id = cookie_data["uid"]
    url = f"https://mssdk16-normal-useast5.tiktokv.us/ms/get_seed?lc_id=2142840551&platform=android&device_platform=android&sdk_ver=v05.02.02-alpha.12-ov-android&sdk_ver_code=84017696&app_ver=42.4.3&version_code=2024204030&aid=1233&sdkid&subaid&iid={iid}&did={device_id}&bd_did&client_type=inhouse&region_type=ov&mode=2"



    # session = "a930300ef0f5bad045b1d824bcb6a98e"   # 这个session的逻辑需要再确定一下，其他的都没什么问题的
    # session= str(uuid.uuid4()).replace("-", "")
    # print("session id is:",session)
    # session = "b20320890bb549a4843896818a1089aa"
    # 并行计算：使用多线程来加速查找满足条件的 session_id
    def check_session_id(_):
        """检查单个 session_id 是否满足条件"""
        session_id = str(uuid.uuid4()).replace("-", "")
        tem = make_seed_pb.make_seed_encrypt(session_id, device_id, sdk_version="v05.02.02")
        zlib1 = tt_zlib(tem)
        zlib_res = zlib1.compressed()
        if len(zlib_res) == 154:
            return session_id, tem
        return None, None
    
    # 使用线程池并行计算
    biaozhi = False
    session_id = None
    tem = None
    max_workers = min(8, os.cpu_count() or 1)  # 使用 CPU 核心数，最多8个线程
    batch_size = max_workers * 10  # 每批提交的任务数
    max_iterations = 1000000
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        found = threading.Event()  # 用于通知其他线程停止
        
        def check_with_stop(_):
            """带停止标志的检查函数"""
            if found.is_set():
                return None, None
            return check_session_id(_)
        
        # 分批提交任务，避免一次性创建太多任务
        for batch_start in range(0, max_iterations, batch_size):
            if found.is_set():
                break
            
            batch_end = min(batch_start + batch_size, max_iterations)
            futures = [executor.submit(check_with_stop, i) for i in range(batch_start, batch_end)]
            
            # 使用 as_completed 获取第一个成功的结果
            for future in as_completed(futures):
                if found.is_set():
                    break
                    
                try:
                    result_session_id, result_tem = future.result()
                    if result_session_id is not None:
                        session_id = result_session_id
                        tem = result_tem
                        biaozhi = True
                        found.set()  # 通知其他线程停止
                        # 取消当前批次剩余的任务
                        for f in futures:
                            f.cancel()
                        break
                except Exception as e:
                    # 忽略任务取消异常
                    pass
            
            if found.is_set():
                break
    
    if not biaozhi:
        print("get seed failed")
        return ["",""]

    seed_encrypt = mssdk_encrypt(tem,False)
    # print(seed_encrypt)
    timee = time.time()
    utime = int(timee* 1000)
    stime = int(timee)
    # print("utime is",utime)
    # print("stime is",stime)
    post_data = make_seed_pb.make_seed_request(seed_encrypt,utime);  #现在还是16进制字符串的形式
    # post_data = "08c49080820410021804227086e5c33eba0c828c5dc8ee7903104d4d1c7df80e51ee0ba39a6fa89dbc14e5d3e445fd303a76a434b6bde7a0710ea230324cc542c9288179b7f6222e7b1d1587f34d99563b3bee5373375947c9e5bbdc69a36f9e3e0998cd5ce4991099839f4156f596c7c85b1df20ebf5166e19b2b632888dd9993bb66"

    # print(post_data)

    query_string = f"lc_id=2142840551&platform=android&device_platform=android&sdk_ver=v05.02.00-alpha.9-ov-android&sdk_ver_code=84017184&app_ver=40.6.3&version_code=2024006030&aid=1233&sdkid&subaid&iid={iid}&did={device_id}&bd_did&client_type=inhouse&region_type=ov&mode=2"
    # print("query_string ===> ",query_string)
    # query_string 来自于params
    # seed = "MDGiGJ3VrXEFIz10yTAxw9OwyOAqHCAiAzcop/tdTkfUKUrWFGA/QFF/FUqyNQOm2ekBJgDBwBWZuA7OUPejAgCRk2fcVrMOd9FnmhNE6jQyrMVHFryzrnlHNXyLvoOcBvE="
    # token = "AHmq5CakgbjbQCOlQi4tj_xMH"
    # seed_encode_type = 2
    x_ss_stub,x_khronos,x_argus,x_ladon,x_gorgon = make_headers.make_headers(device_id,stime,
                                                                             52,2,4,stime-6,
                                                                             '',device_type
                                                                             ,'','',"","","",
                                                                             f"{query_string}",
                                                                             post_data)

    # url = "https://mssdk16-normal-useast5.tiktokv.us/ms/get_seed?lc_id=2142840551&platform=android&device_platform=android&sdk_ver=v05.02.00-alpha.9-ov-android&sdk_ver_code=84017184&app_ver=40.6.3&version_code=2024006030&aid=1233&sdkid&subaid&iid=7550970146955151118&did=7550968885031290423&bd_did&client_type=inhouse&region_type=ov&mode=2"

    # 2. Use a list of tuples for headers to strictly preserve the exact order.
    headers = [
        ('rpc-persist-pyxis-policy-v-tnc', '1'),
        ('rpc-persist-pyxis-policy-state-law-is-ca', '1'),
        ('X-SS-STUB', f'{x_ss_stub}'),
        ('Accept-Encoding', 'gzip'),
        ('rpc-persist-pns-region-3', 'US|6252001|5332921'),
        ('x-tt-request-tag', 'n=0;nr=111;bg=0;t=0'),
        ('rpc-persist-pns-region-2', 'US|6252001|5332921'),
        ('rpc-persist-pns-region-1', 'US|6252001|5332921'),
        ('x-tt-pba-enable', '1'),
        ('Accept', '*/*'),
        ('x-bd-kmsv', '0'),
        ('X-SS-REQ-TICKET', f'{utime}'),
        # ('x-bd-client-key', '#7XhgXG1xPDHCI3vftue5QnEDqKXYPbJp6uwMo9cxiO9OcRvy+Qp3rOun1iYALEujE/vC2OAuGh0vj5qS'),
        ('x-vc-bdturing-sdk-version', '2.3.13.i18n'),
        ('oec-vc-sdk-version', '3.0.12.i18n'),
        ('sdk-version', '2'),
        ('x-tt-dm-status', 'login=1;ct=1;rt=1'),
        # ('X-Tt-Token',
        #  '040d0a7625e16f6ec455774ebac4a3cb2400b905c0f08243f846d5b22fd1d59cdd82238a8c89c834eec05584347c72e3d0473b567188167fc1d476aec1edece247bc48bf2a21ddcbad472c2b6952ba7ab76174bca547bbfa1bc68573ae3a6e062521b--0a4e0a20eb0fc5d9308e2f041e8718e2461f368c062a3831e8293b70e51eb7a90ce8a1fb122049ba3a91495fee71c492f633d7df6c734d42af7a1948adf3e0102505849440e41801220674696b746f6b-3.0.1'),
        ('passport-sdk-version', '-1'),
        ('x-tt-store-region', 'us'),
        ('x-tt-store-region-src', 'uid'),
        ('User-Agent',
         ua),
        ('X-Ladon', f'{x_ladon}'),
        ('X-Khronos', f'{x_khronos}'),
        ('X-Argus',
         f'{x_argus}'),
        ('X-Gorgon', f'{x_gorgon}'),
        ('Content-Type', 'application/octet-stream'),
        ('Host', 'mssdk16-normal-useast5.tiktokv.us'),
        ('Connection', 'Keep-Alive'),
        # The full Cookie string is included as a header to ensure its format is identical to the raw request.
        # ('Cookie',
        #  f'store-idc=useast5; store-country-code=us; install_id={iid}; ttreq={ttreq}; passport_csrf_token={passport_csrf_token}; passport_csrf_token_default={passport_csrf_token}; store-country-code-src=uid; multi_sids={multi_sids}; cmpl_token={cmpl_token}; d_ticket={d_ticket}; sid_guard={sid_guard}; uid_tt={uid_tt}; uid_tt_ss={uid_tt}; sid_tt={sessionid}; sessionid={sessionid}; sessionid_ss={sessionid}; tt_session_tlb_tag=sttt%7C5%7CDQp2JeFvbsRVd066xKPLJP________-5NWxRwFjRS8rZNh5mBfI6XbTiVUUkftEYH0ToFGFa3-c%3D; tt-target-idc=useast5; tt_ticket_guard_has_set_public_key=1; store-country-sign={store_country_sign}; msToken={msToken}; odin_tt={odin_tt}')
    ]
    # 2. Define the Cookies

    data = bytes.fromhex(post_data)
    # data = bytes.fromhex("08c490808204100218042270108cb928cce537756f4f3def2b0078f02cce3a5122b3944c6f50e8fff108f467b8a09b27b189bf804fb9cea93184d60f52a47d06161650dc54baf08547c52fccca8b6d00d3b38fbf17d012a674de21ffb301cd77d26feb8aff01ed4ffb5cb02968d676a5fe1a4f623c3cb1b2c3851d9228b8dea7efa666")

    # 5. Make the request and print the response
    res = ""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            if use_http_client:
                # 使用 HttpClient（已包含重试和超时机制）
                response = http_client.post(url, headers=dict(headers), data=data, session=session)
                break  # 成功则跳出循环
            else:
                # 统一请求参数
                request_kwargs = {
                    'url': url,
                    'headers': dict(headers),
                    'data': data,
                    'verify': False,
                    'timeout': 30,
                    'impersonate': "okhttp4_android"  # 使用 impersonate 可能有助于解决 SSL 问题
                }
                
                # 如果有代理，添加代理配置
                if proxies:
                    request_kwargs['proxies'] = proxies
                
                response = requests.post(**request_kwargs)
                break  # 成功则跳出循环
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                raise  # 重试次数用完，抛出异常
            print(f"请求失败，正在重试 ({retry_count}/{max_retries}): {e}")
            time.sleep(2)  # 等待2秒后重试

    # print(f"Status Code: {response.status_code}")
    res = response.content.hex()
    # print(res)

    #     # Attempt to print the response, handling potential binary content
    #     try:
    #         print("Response JSON:", response.json())
    #     except requests.exceptions.JSONDecodeError:
    #         print("Response Content (first 200 bytes):", response.content[:200].hex())
    #
    # except requests.exceptions.RequestException as e:
    #     print(f"An error occurred: {e}")
    res_pb = make_seed_pb.make_seed_response(res)
    # print(res_pb)
    seedDecrypt = res_pb.seed_decrypt.hex()
    # print("before seed decrypt: ",seedDecrypt)
    if seedDecrypt != "":
        seedDecrypt_res = mssdk_endecode.mssdk_decrypt(seedDecrypt,False,False)
        # print("after seed decrypt: ",seedDecrypt_res)
        aftre_decrypt_seed = make_seed_pb.make_seed_decrypt(seedDecrypt_res)
        seed = aftre_decrypt_seed.seed

        # print("seed is ===>", seed)
        seed_type = int(aftre_decrypt_seed.extra_info.algorithm.encode("utf-8").hex(),16)//2

        # print("seed type ====>",seed_type)
        return [seed,seed_type]
    return ["",""]
# get_get_seed({"install_id":"7560648754732271373","ttreq":"1$084e38a4169433286a4d5b3624876b9a081fdb26","passport_csrf_token":"b1271b9e8b3200cc84af7068542ed385","passport_csrf_token_default":"b1271b9e8b3200cc84af7068542ed385","cmpl_token":"AgQQAPNSF-RPsLjSPIU4_p0O8o1I8YdL_4_ZYNw2xQ","d_ticket":"fd4617f2988fd35a13be0470ee38138f75ff0","multi_sids":"7560648920885118007%3Aa1787f5786d0d68bba95c02478ad08a7","sessionid":"a1787f5786d0d68bba95c02478ad08a7","sessionid_ss":"a1787f5786d0d68bba95c02478ad08a7","sid_guard":"a1787f5786d0d68bba95c02478ad08a7%7C1760351110%7C15552000%7CSat%2C+11-Apr-2026+10%3A25%3A10+GMT","sid_tt":"a1787f5786d0d68bba95c02478ad08a7","uid_tt":"868053b1a8bbc871eababd16bdac646cab192f7db7ef051d32b98af91ec80312","uid_tt_ss":"868053b1a8bbc871eababd16bdac646cab192f7db7ef051d32b98af91ec80312","msToken":"3_GH6YKViUaCI47ca0jK3JRcVHkxXiPfLElY8SBRz00SsdYcxmJMBmtAZtprvXwz0eQXgx6C5VdheQe3iuLNTt2mdkirkaMVpEB0dhRaa0Ua0_MMqwry0r2sVbGY","odin_tt":"bd4e6506b3c04fbeaacbaa258910260595b22fb8279109200481a0a6bf5d7967c75094734ea42322d45a13a2c1accde39a13fb9c487d6a4cd9ddb5e7fc68d024f2a1c438997c8b62aa91af62094cbaf6","store-country-code":"us","store-country-code-src":"uid","store-country-sign":"MEIEDHcKMd5AY9nmmyG-AgQgu4rvy4TjCuUrFIJp9gRls14g-YBiHF33hFihFd61Ys0EEHQAqhMHWqkeGVjMJbWEVtc","store-idc":"useast5","tt-target-idc":"useast8","s_v_web_id":"","username":"user3457778153976","password":"BDHlA2598@","X-Tt-Token":"04a1787f5786d0d68bba95c02478ad08a702bbe3f8d53c9b17000d05c94157772ebd1a7dd970394451dae4765b1b94548ccf0c0f36affb66d05de2df00bd336047d3115e4e79c653799ec66ae57516f601d4af84b3368445dc2093ae078d7b35e8551--0a4e0a200facb2b79eb92e14c95430de6a7c536d5d4f700a8f87fe6e7af40a85115d3cd81220dc99b4e6dc81c575c88ff18e6d6913cf36ceda5d7e229f576cd4efb115f903861801220674696b746f6b-3.0.1","phone":"9432409396","url":"https://a.62-us.com/api/get_sms?key=62sms_b965cb70222fc5ca31134d4a5c270936","ts_sign_ree":"ts.1.35c626f3611a9d7634bc8eb72936bd2d7cb83e463de0ce265a6c3427727d61937a50e8a417df069df9a555bd16c66ef8b3639a56b642d7d8f9c881f42b9329ec","User-Agent":"Mozilla/5.0 (Linux; Android 12; SM-S9010 Build/UP1A.231005.007; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Mobile Safari/537.36","uid":"7560648920885118007","device_id":"7560648506287113741","twofa":"CRKCXBPBSPCICREBFUZ22J5VVMM5FLGY"})