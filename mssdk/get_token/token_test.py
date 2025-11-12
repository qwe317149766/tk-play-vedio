import random
import time,uuid
import urllib

#
import requests
import urllib3
from curl_cffi import requests
from tt_protobuf import  make_token_pb,tk_pb2
# from mssdk.endecode import mssdk_endecode
from headers import make_headers
import hashlib, time, datetime, base64, os, random

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
        # return "78010190026ffde2a33328472bd648e4a5e9a1fe13c2b6f057fafe2a027c37ceb865566fa8fa767a3cfe49ff7eabb196870f11a25de809f520d46c9bcd3af32f1208224866fb48b9734e33dd84645357eef1430b8329a26e043f0276de009b9c91e1fe6c84a408747b9a8cde1b9392cc6a8cce5b6e56afa633833ff304aae5b24f86a2eb846f43efa7220619ae772777dc2024fe5a546945a44009178f8329a0cbb5f656fcf22f0458f6fe43bf201ac5581bd6a07c5c0fefd4476de0cb05394c3b518882f1af61e5adbf24f145cfee48489379c30e5d602d6c6d27e5abb3fdf7686c245b360b06cd6d0f69659ff14e47442b7df51df88c09348f4ac80ee6f7270a09228cc966a581c955597120a3ac57ff0c35af0f4278d667e60fb533e6bc3754b93965e1b8c0c86457cd9be1f1fce00678373d4b27d7b5d4ab9f59f9605b616456162c862af860f59783e36fbd574f4001626af58e37dce2a93e3218107603c50966065e4eb2e862a8c0f9b411a47942cf7f21df73c4c2c7c7013ddbc43ef29a2531000f2ee5829dc8130a2a592df3f7a134ac547a59b0b8859dae51d27c2862398538f90c0f1feed1d5e0d350a991ac209951114738b58147e433ad4c48abd897290280744fa5ee8fc19a81bd723fa02e2946af48b73603e3c5ae891b1987629de563e17cca5ee8742d937852f67222befc7d7480041b497a59294a1836b2f19f70bd97a723f0d066559618e0d28a9162a5aad24886e2775aa9d90468dafae14eb015fdc9d77374bf75cff7b79de37c98610a4f60fa024a0dd5483b031ddeb3ee4a08f4dd87274cb20b923d5b13dac0c6b8131ca4b78c53fcd6e51a1ce89c893ed1390e162ab9ef05038c111b8db0b9c55197d6c58f267970e45ca1e3bc52215d368501964a3ddbd936a26d04cd252de91fc278d1609b054332"
        # return res
        res = zlib.compress(self.hex_str1, level=1).hex()
        # print("zlib res length ===>",len(res))
        # print(res)
        return res+(1266-len(res))*"0"
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
def last_aes_decrypt(ciphertext_hex: str) -> str:
    """解密最外层的 AES-CBC，并移除填充。"""
    ciphertext = bytes.fromhex(ciphertext_hex)
    key = bytes.fromhex("b8d72ddec05142948bbf2dc81d63759c")
    iv = bytes.fromhex("d6c3969582f9ac5313d39c180b54a2bc")
    cipher = AES.new(key, AES.MODE_CBC, iv)

    decrypted_padded = cipher.decrypt(ciphertext).hex()
    # print("decrypted_padded", decrypted_padded)

    # 这里是可以修改的，改为去掉最后几个字节
    # try:
    #     decrypted = unpad(decrypted_padded, AES.block_size)
    #     return decrypted.hex()
    # except ValueError as e:
    #     print(f"移除 AES 填充时出错: {e}")
    # 修改为下面这种兼容pkcs7和000003两种填充方式
    pad_length = int(decrypted_padded[-2::], 16) * 2
    return decrypted_padded[:0 - pad_length]


def mssdk_decrypt(encrypted_hex: str, is_report: bool, is_request: bool) -> str:
    """
    逆向执行 mssdk_encrypt 函数以解密数据。
    """
    # 步骤 1: 解密最外层的 AES
    decrypted_aes_hex = last_aes_decrypt(encrypted_hex)

    decrypted_aes_bytes = bytes.fromhex(decrypted_aes_hex)

    # 步骤 2: 逆向处理前缀字节，还原 XTEA 密文
    # first_byte_modified = decrypted_aes_bytes[0]
    # original_first_byte = first_byte_modified ^ 0x3

    # xtea_encrypted_bytes = bytes([original_first_byte]) + decrypted_aes_bytes[1:]
    # xtea_encrypted_hex = xtea_encrypted_bytes.hex()
    xtea_encrypted_hex = (decrypted_aes_bytes[1:len(decrypted_aes_bytes) - 4]).hex()
    random_iv_four_byte = decrypted_aes_bytes[len(decrypted_aes_bytes) - 4:len(decrypted_aes_bytes)].hex()
    # print(len(xtea_encrypted_hex))
    # print(xtea_encrypted_hex)
    # 步骤 3: 解密 XTEA 层
    key = get_XTEA_key(is_report)
    # print(key)
    decrypted_xtea_hex = CBC_XTEA_encryptORdecrypt(
        iv=random_iv_four_byte + "27042020",
        key=key,
        data=xtea_encrypted_hex,
        is_encrypt=False
    )

    # 步骤 4: 解析 XTEA 解密结果，获取 zlib 压缩数据
    # 格式为: byte_one(1字节) + rand_two(1字节) + rand_three(1字节) + "4b000000"(4字节) + zlib_res
    # 前缀总长度为 7 字节 = 14 个十六进制字符
    # print(decrypted_xtea_hex)
    # 加密的头是7801 解密的头是78da
    if is_request:
        zlib_res_hex = "7801" + decrypted_xtea_hex.split("7801")[1]
    else:
        # print(decrypted_xtea_hex)
        zlib_res_hex = "78da" + decrypted_xtea_hex.split("78da")[1]
        # print(zlib_res_hex)
    # 步骤 5: 解压缩 zlib 数据，得到原始的 protobuf 十六进制字符串
    zlib_decompressor = tt_zlib(zlib_res_hex)
    original_pb_hex = zlib_decompressor.uncompressed()

    return original_pb_hex


def get_get_token(cookie_data:dict, proxy="", http_client=None):
    """
    获取 token
    
    Args:
        cookie_data: 设备信息字典
        proxy: 代理地址（如果 http_client 为 None 时使用）
        http_client: HttpClient 实例（优先使用）
    """
    use_http_client = http_client is not None
    # 1. Define the URL and query parameters
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not use_http_client:
        # Proxy settings for debugging with tools like Charles or Fiddler
        proxies = {
            'http': 'http://127.0.0.1:7777',
            'https': 'http://127.0.0.1:7777',
        }
        proxies = {
            'http': proxy,
            'https': proxy,
        }
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
    # sender_id = cookie_data["uid"]
    # 1. Use the full URL with all parameters to prevent auto-encoding issues
    # that would invalidate the request signatures.
    url = f"https://mssdk16-normal-useast5.tiktokv.us/sdi/get_token?lc_id=2142840551&platform=android&device_platform=android&sdk_ver=v05.02.02-alpha.12-ov-android&sdk_ver_code=84017696&app_ver=42.4.3&version_code=2024204030&aid=1233&sdkid&subaid&iid={iid}&did={device_id}&bd_did&client_type=inhouse&region_type=ov&mode=2"

    # session = "a930300ef0f5bad045b1d824bcb6a98e"   # 这个session的逻辑需要再确定一下，其他的都没什么问题的
    # session= str(uuid.uuid4()).replace("-", "")
    # print("session id is:",session)
    timee = time.time()
    utime = int(timee* 1000)
    stime = int(timee)
    # print("utime is",utime)
    # print("stime is",stime)
    # session = "50cba28547924f699d2f4b2f8469ef0a"  # 这个最终选择固定了...
    # device_id = "7550968885031290423"
    # install_id = "7550970146955151118"
    tem = make_token_pb.make_token_encrypt(stime,device_id)
    # print(tem)
    # print("tem length",len(tem))
    # print("tem:",tem)
    # tem = "0a99050a08216e6f74736574211206676f6f676c651a07506978656c20362208216e6f74736574212a07416e64726f6964320231353a09313038302a3234303040c8064a0f425031412e3235303530352e303035509099a4fe0c5a03656e5f6213416d65726963612f4e65775f596f726b2c2d35686070c8017880809ba63b800180c08a90f206880180c08a90f20690018080af8be2059a0108216e6f7473657421a20108216e6f7473657421aa011063623737336631633263393562623636b20108216e6f7473657421ba0108216e6f7473657421c2012c4439413259363244327974356e453431357a61556e6d55474e59636c526e514b4143363635634d48706b6b3dc80198eadc8c0dd2012431343063313935302d623033332d346237362d616338372d363338636431666434306262d80180c0af80b918e20108216e6f7473657421e801fd887af001fd887afa0108216e6f747365742182020e3139322e3136382e3230342e35348a020f3139322e3136382e3230342e313833920208216e6f74736574219a0208216e6f7473657421a002fd887aa802fd887ab20208216e6f7473657421ba0208216e6f7473657421c2021d5b223139322e3136382e3230342e313833222c22302e302e302e30225dc802fd887ad002fd887ae20208216e6f7473657421e802aeb2c78d0df002a4eadc8c0dfa025f2f646174612f6170702f7e7e526f4164364b43525f774158333451303737775754413d3d2f636f6d2e7a68696c69616f6170702e6d75736963616c6c792d344c4e45426a7a4f66595541745549796854714976773d3d2f626173652e61706b8003468803fd887a920308216e6f74736574219803fd887aa003fd887aaa0308216e6f7473657421b20308216e6f7473657421ba0308216e6f7473657421c003fd887a121941586162367249367447394c3174735a626e4a482d43595f7a1a07616e64726f6964221c7630352e30322e30302d616c7068612e392d6f762d616e64726f696428c08090503204313233333a0634302e362e334213373535303936383838353033313239303432334a103030303030303030303030303030303058fd887a622465613036376638612d393631392d343339352d623165352d6439313761316563386439317a08216e6f74736574218001fd887a"
    token_encrypt = mssdk_encrypt(tem,False)
    # print(token_encrypt)
    #
    post_data = make_token_pb.make_token_request(token_encrypt,utime);  #现在还是16进制字符串的形式
    post_data = "08c49080820410021802229005a3fb46261b10b4698b0ce5adc9671d7ca993df0a6cd9cf4abe937f374633c9f08ac06ff741324ab50699ccfd266334ad57f41174fe197b4277c865021a376878b2cc8d5b6319d178e947f7cb55e04b8ebdd399b80a9a9e3dff5377f75d40834270b7f8d9a6be1ed042d12792db7a9af3739a1a000108a689fab64f5eb51da9833024d5af1b7a1a41c2fc6af6c8594a7c9579a31beb97aa8bd788b355d07875d6f01427be80afea023e865ab6f8aa58ee51c8d82ce0fdf060f36662e79cf1de79ccb75957a624587b1eb1ac1b4c1fa151edd222845b2c9e171eb15d40a533c02b5d4ea6dce5b7d11bae93a8f0998a40754515b46f3ab687a26743669b33766d707f4d5f4fbfbe8d53e147910d3cabcf77f74567e99fdfa2346c6130c02795b7d09e880735c7e68dbfbd51174c7d8e2ea48ccb1c3b8cabfb8f534a00776a2ffa5642657ec5777350ea87845f94ee7d0d54f01329259634acae620e4ef1553084719c145d43c049a71dedd37b91b5ca3efbb8bc543143d1e879cc7a5e62450541fc79a378542ed4811c0da0ee77c40f47b4a06fc3161f4f05945e4e1c0f73c8ec702b5861bb30394fd9f18f0834ddbb2c192d0a52df15091fd722821c1f5ffbbc3c79aa60ab27d05cc6493c841445dec8a2cb0c9b29e7e6c45d308c5a05f5214e8b47c2997c0a92f54abf9d89eff63015a522ae329bb82f3cec8fd6ac6e212f0c135125062f7c7557689377ee473cc29bf3fb29c4e882466a5c7502782040aa96c52fcc8ed88cd06ebb7b545b01badce2b01fe2b6ad32ddb4554536ec7084c3bcf66b3ce2e0e2757b2faf563b38bb1036bca92b33c6d670d6d3051f0ec755ebda61cb5ec4ac1a40e7a803bf603480891718327cd474f2b666860985b5950723133210c3efaa9ab3e29e8ccd0684588b515128aac2adcbcc66"
    # print(post_data)

    # params = {
    #     'lc_id': '2142840551',
    #     'platform': 'android',
    #     'device_platform': 'android',
    #     'sdk_ver': 'v05.02.00-alpha.9-ov-android',
    #     'sdk_ver_code': '84017184',
    #     'app_ver': '40.6.3',
    #     'version_code': '2024006030',
    #     'aid': '1233',
    #     'sdkid': '',
    #     'subaid': '',
    #     'iid': f'{iid}',
    #     'did': f'{device_id}',
    #     'bd_did': '',
    #     'client_type': 'inhouse',
    #     'region_type': 'ov',
    #     'mode': '2',
    # }
    # query_string = urllib.parse.urlencode(params)
    query_string = f"lc_id=2142840551&platform=android&device_platform=android&sdk_ver=v05.02.02-alpha.12-ov-android&sdk_ver_code=84017696&app_ver=42.4.3&version_code=2024204030&aid=1233&sdkid&subaid&iid={iid}&did={device_id}&bd_did&client_type=inhouse&region_type=ov&mode=2"
    # print("query_string ===> ",query_string)
    # query_string 来自于params
    x_ss_stub,x_khronos,x_argus,x_ladon,x_gorgon = make_headers.make_headers(device_id,stime,
                                                                             53,2,4,stime-6,
                                                                             "","Pixel 6"
                                                                             ,"","","","","",
                                                                             f"{query_string}",
                                                                             post_data)




    # 3. Define the Headers
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
        ('x-tt-dm-status', 'login=1;ct=1;rt=8'),
        # ('X-Tt-Token',
        #  x_tt_token),
        ('passport-sdk-version', '-1'),
        ('x-tt-store-region', 'us'),
        ('x-tt-store-region-src', 'uid'),
        ('User-Agent',
         ua),
        ('X-Ladon', f'{x_ladon}'),
        ('X-Khronos', f'{stime}'),
        ('X-Argus',
         f'{x_argus}'),
        ('X-Gorgon', f'{x_gorgon}'),
        ('Content-Type', 'application/octet-stream'),
        ('Host', 'mssdk16-normal-useast5.tiktokv.us'),
        ('Connection', 'Keep-Alive'),
        # The full Cookie string is included as a header to ensure its format is identical to the raw request.
        # ('Cookie',
        #  f'store-idc=useast5; store-country-code=us; install_id={iid}; ttreq={ttreq}; passport_csrf_token={passport_csrf_token}; passport_csrf_token_default={passport_csrf_token}; store-country-code-src=uid; multi_sids={multi_sids}; cmpl_token={cmpl_token}; d_ticket={d_ticket}; sid_guard={sid_guard}; uid_tt={uid_tt}; uid_tt_ss={uid_tt}; sid_tt={sessionid}; sessionid={sessionid}; sessionid_ss={sessionid}; tt_session_tlb_tag=sttt%7C5%7CDQp2JeFvbsRVd066xKPLJP________-5NWxRwFjRS8rZNh5mBfI6XbTiVUUkftEYH0ToFGFa3-c%3D; tt-target-idc=useast5; tt_ticket_guard_has_set_public_key=1; store-country-sign={store_country_sign}; msToken={msToken}; odin_tt={odin_tt}'),
    ]

    # 4. Define the POST body (payload)
    # This is binary protobuf data, represented as a bytes object.
    data = bytes.fromhex(post_data)
    # data = bytes.fromhex("08c490808204100218042270108cb928cce537756f4f3def2b0078f02cce3a5122b3944c6f50e8fff108f467b8a09b27b189bf804fb9cea93184d60f52a47d06161650dc54baf08547c52fccca8b6d00d3b38fbf17d012a674de21ffb301cd77d26feb8aff01ed4ffb5cb02968d676a5fe1a4f623c3cb1b2c3851d9228b8dea7efa666")

    # 5. Make the request and print the response
    res = ""
    if use_http_client:
        # 使用 HttpClient（已包含重试和超时机制）
        response = http_client.post(url, headers=dict(headers), data=data)
    elif proxy != "":
        response = requests.post(
            url,
            headers=dict(headers),
            data=data,
            proxies=proxies,
            verify=False,
            timeout=30,
            impersonate="okhttp4_android"
        )
    else:
        response = requests.post(
            url,
            headers=dict(headers),
            data=data,
            verify=False,
            timeout=30,
            impersonate="okhttp4_android"
        )

    # print(f"Status Code: {response.status_code}")
    res = response.content.hex()
    # print(res)

    res_pb = make_token_pb.make_token_response(res)
    # print(res_pb)
    tokenDecrypt = res_pb.token_decrypt.hex()
    # print("before token decrypt: ",tokenDecrypt)
    if tokenDecrypt != "":
        tokenDecrypt_res = mssdk_decrypt(tokenDecrypt,False,False)
        # print("after token decrypt: ",tokenDecrypt_res)
        aftre_decrypt_token = make_token_pb.make_token_decrypt(tokenDecrypt_res)
        token = aftre_decrypt_token.token

        print("token is ===>", token)
        return token
    return ""
# print(get_get_token({"install_id":"7566039756053464887","ttreq":"1$3695ce3ea785c0078e1d025dba0abc3d2d595c7a","passport_csrf_token":"dfe879d35870e5f84e0f368080c5b69a","passport_csrf_token_default":"dfe879d35870e5f84e0f368080c5b69a","cmpl_token":"AgQQAPNSF-RPsLjBF4tl_V0O8p5hw4QSP4_ZYNxnEQ","d_ticket":"c4f88ddbfded437a1d19ba30122e6b6d44c08","multi_sids":"7566039870369399863:3b042fa537014bb504e6329ec3870156","sessionid":"3b042fa537014bb504e6329ec3870156","sessionid_ss":"3b042fa537014bb504e6329ec3870156","sid_guard":"3b042fa537014bb504e6329ec3870156|1761606079|15551999|Sat,+25-Apr-2026+23:01:18+GMT","sid_tt":"3b042fa537014bb504e6329ec3870156","uid_tt":"80f4a854230cf971af113d1106512b8e4566f11bc4894c70c2005cf1c3a62301","uid_tt_ss":"80f4a854230cf971af113d1106512b8e4566f11bc4894c70c2005cf1c3a62301","msToken":"_Hde4lbEnyZ73Evu6DEWhYieT6JmVB0-zAq1G5XP5mucE6g9l8uaLpIkV93bE7cx0Px468GrE5wllYkn4eChpYBCv7dwRUAO0ALTwWYuJaSaag452CRPEv6BoAQ_","odin_tt":"232457c0734c927b9be1a5c9dde45bf35ad922fdad39c99f66f9e70f6eb1ba847509a48d6fa9960bf79793bbd6b67958d06088b805e8b8522ba2f36b7c4231bb5eeb08f1dd33dbaa7a28843acad965ec","store-country-code":"us","store-country-code-src":"uid","store-country-sign":"MEIEDOB6UiLU7Eruy6NYdAQgBkklk4ihMYHt50RElRBaM_OVwYAo5jXzR-MYgpkdJZsEEC0UhimxDJ5fjwbjj_q8OcQ","store-idc":"useast5","tt-target-idc":"useast5","s_v_web_id":"","username":"pnwa.fvvh","password":"RGEP86259@","X-Tt-Token":"043b042fa537014bb504e6329ec387015604356f140dd46d4ced252976a4337efda7838a5047f9c5ad60c4815a5e03c882881e57917b30551966ee1a3bd13b917c5945c0c9f9adf01de893cce4dc484b2eeeb70ecad58b4b6c3c2dedaeb5b11c6b512--0a4e0a2039fd0392dc0c2037d6f189f77f15caa41bf9732b926aa7d50626f489c11c5ebb12204606b1aede211feaff35085c59d833c23d3a084c86f7c18724812f9470102fae1801220674696b746f6b-3.0.1","Email":"","Emailpassword":"","ts_sign_ree":"ts.1.6dea031aa254534533e27eee275a6def2b64d03ddded15f3472ace23f02f299e7a50e8a417df069df9a555bd16c66ef8b3639a56b642d7d8f9c881f42b9329ec","User-Agent":"Mozilla/5.0 (Linux; Android 12; SM-G9280 Build/52.1.B.0.422; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Mobile Safari/537.36","uid":"7566039870369399863","device_id":"7566039609412224525","twofa":""}))