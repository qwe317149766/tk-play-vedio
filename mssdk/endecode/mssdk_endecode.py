'''
created by mwzzzh
report的key来源目前我们还没有研究
还有zlib后填充的前几个字节目前也没看 9.15已看
不过现在解密函数还是有问题，我们需要重新再看一遍  已全部解决，原来是第一次iv找的有问题
压缩的头和解压缩的头并不一样  已看

'''

import hashlib, time, datetime, base64, os, random

# import jpype
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
        # # # 原始代码使用 level=0，这实际上不进行压缩，只是添加 zlib 头。
        # # # 这里可以根据需要调整压缩等级，例如 level=9
        #
        # jvmPath = jpype.getDefaultJVMPath()
        # d = r'D:\vscode\reverse\app\shizhan\tt\mwzzzh\mssdk\endecode\unidbg.jar'  # 对应jar地址
        # # jpype.startJVM(jvmPath, "-ea", "-Djava.class.path=" + d + "")
        # jpype.startJVM(jvmPath, "-Dfile.encoding=utf-8", "-Djava.class.path=" + d + "")  # 输出乱码时使用
        java = jpype.JClass("com.tt4063.get_seed")(
            self.hex_str)  # 从com开始找到打包jar的类
        signature = java.call_153B58()  # 调用java的com.bytedance.frameworks.core.encrypt.CS类的RSA_encrypt方法
        # print(signature)
        res = str(signature)
        # jpype.shutdownJVM()
        # return "78010190026ffde2a33328472bd648e4a5e9a1fe13c2b6f057fafe2a027c37ceb865566fa8fa767a3cfe49ff7eabb196870f11a25de809f520d46c9bcd3af32f1208224866fb48b9734e33dd84645357eef1430b8329a26e043f0276de009b9c91e1fe6c84a408747b9a8cde1b9392cc6a8cce5b6e56afa633833ff304aae5b24f86a2eb846f43efa7220619ae772777dc2024fe5a546945a44009178f8329a0cbb5f656fcf22f0458f6fe43bf201ac5581bd6a07c5c0fefd4476de0cb05394c3b518882f1af61e5adbf24f145cfee48489379c30e5d602d6c6d27e5abb3fdf7686c245b360b06cd6d0f69659ff14e47442b7df51df88c09348f4ac80ee6f7270a09228cc966a581c955597120a3ac57ff0c35af0f4278d667e60fb533e6bc3754b93965e1b8c0c86457cd9be1f1fce00678373d4b27d7b5d4ab9f59f9605b616456162c862af860f59783e36fbd574f4001626af58e37dce2a93e3218107603c50966065e4eb2e862a8c0f9b411a47942cf7f21df73c4c2c7c7013ddbc43ef29a2531000f2ee5829dc8130a2a592df3f7a134ac547a59b0b8859dae51d27c2862398538f90c0f1feed1d5e0d350a991ac209951114738b58147e433ad4c48abd897290280744fa5ee8fc19a81bd723fa02e2946af48b73603e3c5ae891b1987629de563e17cca5ee8742d937852f67222befc7d7480041b497a59294a1836b2f19f70bd97a723f0d066559618e0d28a9162a5aad24886e2775aa9d90468dafae14eb015fdc9d77374bf75cff7b79de37c98610a4f60fa024a0dd5483b031ddeb3ee4a08f4dd87274cb20b923d5b13dac0c6b8131ca4b78c53fcd6e51a1ce89c893ed1390e162ab9ef05038c111b8db0b9c55197d6c58f267970e45ca1e3bc52215d368501964a3ddbd936a26d04cd252de91fc278d1609b054332"
        return res

        # return zlib.compress(self.hex_str1, level=1).hex()
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


# ==============================================================================
# 解密函数
# ==============================================================================

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
        zlib_res_hex = "78da" + decrypted_xtea_hex.split("78da")[1]

    # 步骤 5: 解压缩 zlib 数据，得到原始的 protobuf 十六进制字符串
    zlib_decompressor = tt_zlib(zlib_res_hex)
    original_pb_hex = zlib_decompressor.uncompressed()

    return original_pb_hex


# ==============================================================================
# 主执行模块，用于测试加密和解密
# ==============================================================================
# if __name__ == "__main__":
    # print("--- 开始加密与解密测试 ---")

    # # 1. 获取原始明文数据
    # original_data = make_protobuf()
    # print(f"\n原始数据:\n{original_data}")

    # # 2. 加密数据
    # encrypted_data = mssdk_encrypt(False)
    # print(f"\n加密后的数据 (最终输出):\n{encrypted_data}")

    # 3. 解密数据
    # decrypted_data = mssdk_decrypt(
    #     "08c49080820410021804227039a7e1682f35461fa3c6d61a0eb3856f11f0cf803c785b7793a24b0e5fa3f9dc8537bd7fa886aba98528b7b7da0a1a9444da82837a9123fbdfdfdfe23d2dfe7608762898b98786257d9cb274e718ef8f71f9c9fa22c7c6581ece1ad865c9fded4855c0b18eafbe0b5730a1f99585554e28eaba86ddae66",
    #     True, False)
    # decrypted_data = mssdk_decrypt("3f853215d2bfff7977e644f1cf67358174581de9f547a596339945e14ab296551dadc149c46398e34069c8aa9512cfc21d1fb66eca4efdcd6fc565309ccc8086a859a9269827852004a1d73ffa2fb2d7f11def71b72995b6df926bc4db70fc48c9fa6e9bfbcf01e68e36d118571c3f2a6f0ec30b6893b8dc4c11a250b4999b6a7928d99aa5175d98da72c51d11e18cbdba87a174762cc312540baa636b4dd5b590d163b1930740390470fd4b6c67b7c1339c37dc9258248cfc968f4550070ad6050ade26e64d8193f05c8f49377f4affa26d668c952aba9a1cff0060cb7ce008dfd72a6900cd5e263fc6ff8d88f3731a2c8348a22e4f87ab354fa938aaaabb834e6ba969510eebf56efd3acca312aecd12374d92e8d03030bf780f9d018e94baaf762807021c4bf51ac7521ac7ca6ed9cf1604924b0de1fa2e11a713ea3d2c682401a165d5cd402d78a24eac7ba5f4fd376b6d21908d6afff4a0253b1f8d96bb813aa3839f6d9d03bc1c326c225b11e643d9bba58ba59c4a23873dd4cac8b5bcadb1ab8223becda75395eb5ed819982832cbe8bd351dce2fe7e1f0f4ce2cfc5f6457b4caaabee06ace5d3ab4724e86450e05c53172cdb1e70d0901f379633b9bc04183e5b7150a13df4a2be18c3d3451db65090eaf185e34cde8f48e87e32bd2eb90101461f68ba91c8832da0c0409be62a92380fb940f7dfb63bffaa9dc0d7f047db5f1b8c978a4aa7bf7ea462eba71bebc54721856a961e31c505031cf5e8da7b2f573349e188c309802da4a3d6f6ebe3ca0b6a906656375bb9ea059e50a5f74fa4fc397828a9c9038c1fc3288975ebde64808da0a8dc7c67f5f5fb7a23b85d4552372a44ad8e2d4885bb0923c2daf51e872dc5df077d6b6c5ff075b531ddf2d95600fab684d486681a2cb0a50af83e0a8e87aec264865856b392d478905e0ebb8570901b7c6b5bd06b42de15ad4e7739342c00f56876ad7d76b9a16c567648e73574aa6826d9d940903137dd5678bc309668fdfd3eb6be8e0414a360a52855e189a173f3f27ab93100c61c56c360cbb03e8f3e8fbb79e0dc396bb6bf83f50c68fe48f10c28aed2404395448a92c73a4a589c58b7b25e73702791bcf6eecc56e2261d128af6752be7130bd3036549c70241be6abe749755ef40a9cc3b588076fda7b45aef00447a620849b99165d95f7712d2061fe2597848046c43685747fd4418d34b0e6c31eac3c32f850b663d529e4989d09aa584a66819bf5a9ffec00568e9269d37f6a76cffbcd13761db98144ff4a227abee7d97fbf79f6e92b004034a33ce418af387f24842d655d426db3d1e79d2a9bd41e1f634c697c7924fe6137ef8e9d04567052798d014d53d773c6ba753da3223d9b509fe858d560f591640f1801ac3d2078415611a24c01fa16d7be0bd0c4124bee75b6b796b5c172d38016b95ce192e57c1d0ec404dcc3763a0efa74eb0415a642e5df343c8bc7d577957a9e23a9c21d924ab48ff312b6a5354e47158164a42713a2c522cf5665ec5988e9a45472e823bab980c050fa8e0c992d618a7b676f2d497876b3c2baac145ba429fa2bb8c094690c42fd32dafb8b2299aa8650796ae6ea1e8ee05bf877724c14345b417cf6d3decbbefc8285e41aa3feb55dbe17a272e8538dab91a65eacbb7cb2d0cff5080e2d333b2708e793bb7623897125debe8b54fad1e62c3c551ece3313feb56aa2d93001a09b3b7adafb999f26c7449ecff4df496249822813cc4cbb2028e38c624827d990ece3a6bfce6e555dd4217f09f734ad4846c466164eaf11e1c8f21e26417f218663cce6d6302368907f04f827048f94980d6507947aabd3fdb24605458b79d36e2ca92dc9acc827f8e0cfbe1d2170f9d22f2440e0deaeee9fb1a0d71219ef0fa77bcde0e142f8798efeb00f5172a602ac179c240f019dfea5468545f150f9c7d8a0d22ec443b245acb3bb71f8fac81f5b05672a744f140f3856fb3dfcf156c2db55d431f8d41b5008c14c0d9f0cc7a82490d39f4c174a905e85b8261348e065d3b73df280ec798d8d7ebdafb237e9e3e6144feb439e0ed68bdcd7e436ec9c895d42bd51c59f0f3297109db72679bc68f744b14cb428db72fbe1857b2494d02df44b4dc7f12b36f22a74f69654c85fd6d52ea6ea070f8878130d2d6e270c50528fa60e8e6cc4c2541012f23617e4b964f7f96e4e1aa8d4880a091290ea6e4c296508d748bc36f53a292276abf6f99b6b401cd845e968cc685116da479e30d3c1a1214a7cf44ebb383589db2f7ac1ace17e1a771a8376d7c731fc8dc22a304ae81708a86fcc542ab434775329375e22b9d5c16e847aaad6780e60cb1a61a0dd497702408d2910f86fb5165b4e957937f32b0ddcc1ec16ca707fff70418abaea454356bd08b22fcf88fbb0f31741e7df3ac79213737260367851aad6be3b4a2997bd129616d6eb33def5c37dc1e4802c19a218e2543e29d61b8bc807d4d65e970d99088b04e4b308f470c289ae294382ef9296f21e6c13bf7a51ab31fbd90320acaa54d56f743eb86e55c9b6779aef9d6f7f7634db775998a9c8fb53c949a85a3f0a5eb3a86f51c7afd604698d80e024bddbf860e2a7f61e4de3198c2db171034cf623b891c90cda1252642ff314b3f51c05ca4ba8c224f7229bec45ddd1e4050a57281def3b4ac738c1dabecfaa4409455666d72069966b7c9e30f9fe2b6d47ac7bfa18a15b578477746ffe412047c74fa84518f46b6514346b41562afa00344bfc4bcd74bb6d1eeb063c1029198228776f58c63d4e8a9b67919ab647a08939d942d3816c0fbed389d44ce49ee7b4ca9e2776084d2528c58a5986e1323f24d57fd953c234d0a08fc1dce42f1cde611b2f8c2c6876b1e72ed156be9acb823a7d5a6ddfde8a27f798cf971b745598d6eb4fdced511a7e4806498bb55137128f22cf94ec00d5d6452d24d85d0aea15e430323ff22686b2f1a2861c984ceb930e4e5dafd3a97045135ba4f1f53e0973c1fce3883493d72722ce57d9bff7b637d8d22bf5f168b42f59b8535a885a21a4d8364db53ca401ccc26ba4056c3e1d2024ccd4386b91633db502e7262a52e632ca4986ea5d5883e05ebbcb9978c1b5745e1ec7002dcaffc24a33ae0bc57436dc2e026c9a69815c1719b25d1aeaedf172a6567b861e1490afc6f5ad4a5fbd1e9c5ce120621b334714e40c7cf8cfc994ce2f273dc7d15db98cd2c970b5024aa754d7e3e98cbbd2937bbb4eceac063fb81095b6e7afea914ec862c32ba5a22876ddef59d88887088ada179aaa2d3e677d2a329c3586e472ce55d9a9a9b27254629414e87630576e6479a7f8a8e6eb4632455d958a7f805b625e5bc6cb32214d68d7e26fbe4a3a2dfa31943a063eca5e8b48e55e4a7ad637a807b2ba2978c51d06ca1234bb019250420ede57d7584ee3899f0774e800494ef3deacc1a702692123fc5261a1c0a5825457e4ab92cfd786f76f9b6071db26322c91e4a6a8261b90cad029f21f5b5f6cca8f9e61c08485a8d115dedc98f35599930dffb562a394d92d925c4a70877d5a08178b7c2febe3936cd82134541ea062bded5031b29467d9c5f4c96ce2de7f0cfe32dfced49f6c36e4535ed1c33aa3dcda06c5921525bac8c5179d146d30a0e8991811ff6eeb9a4d5044e02d98770f047468fa1e4c25d3551eb39c87f0f905e703d81bd30f135f8c97607fd7cc73a547f788057c97621ee2c306d4c9a59b55ae516a2d4f97404b7e6577ae911a14c84bdb09a32c917372d763432ceb6407787fb705b8f73f1d73e88f5512da87cc652de0dba786265082952025015536e3462ac4804bf37ecdcd001b17363499e73422581ad4641146d16b5ba4f07b87ddd07dbaa63039b78beab1e6c14588ce3ff8b9bdec67ffda6e20ac7c44260d5852c0a6bd19373bbfaac92e11f843f5acf2ff10b8ade2f9fd46ee22bf8d5d8c23b9d5bc7464b362551eaa2620e9973698edd168ee05ecf66160a97dc514d2ba1450c2436c145afa6afdfc62427b1278a8e7cddda9b25c73b5e9c428792efdb1763e6fd2a1a01812c9b5b1dcbf1d0ffe6a37635e722f2605ead76b8929a2673bea91db571e00b81f8db0f19f273044a1b227f43a2438546a34aa5999664c4f2d885fa8da204b879ffbe2cc520b3a2e7121bd69d9f5b73442472da4d05752533c9ff3b7f2611c1bd89270c6c339f31db385c4f2f25b5d300da8246e70fc455067a38d4eb497cd894bcb3b4b9bfd70e0af06925c3f88334e4675a7ad924b8b496deafc75f701bcf83d13526a64708bdb02e5b945b39b2ef80e1bdf25dc476d893cdbebbd01ffc01fd093bdfe3953d67cded351e96d85a59d93a138cc2a624d2849f5f737042507d36a23ac51ef4d0db228e24f252edf0194c5aa3beb04523b63c66ebf3fc9434375567962e54ec79143ea9aa112a3659ae9912182c02757199d9e7be8ccdc5ff74cc0ec0c2d69a4c436ae31efbcdc2d5d4072c0f18d9c23584905621f6a5c78e4cb19128d05a42c0631d90c03e401dd053c3261c608aa247c12c904832c50d364bbaa2303c8a33dcb90b4048569d15e64d97eaa5c2392e52da7d510c5bfe7b8fcfb9f57d5a0de2180f326fa32f49ab2167e29c475e9af00582f16ee4ec936bb5ced6f9880e150c0aadd1aaff64d992efc2690db66f5d446c7b06959936c81c90a70538fefa85232aecb56531811488116bfbe0f3b3490a5c70397f082278d25b8729669cd5bfa6cc654d5a3eaecda98838e53b0db4b097dd6c9d862a59f01f427bf0cf71b999d5a5da45ff37d5131631cbb7ddae3c3695f16052759995ca066672559c96f149cb9723aa8d836024278b2725ff8fe6de1fcff81c55d29b295155047514c7b62098d1933f6db55a0dabaee5644f63d56193f8d56d4b9c611b59f56cc69a49828def54b189c4e905708f61670d1b2bf6000e933bc2aaf733096d4e7d563575b3d5571d1f00c93169b9ee577b6b3adacbf72588434e3dcf57f52a9fbf8cbf0160a9e2101b843e6687f29f8674e15773ff1e0d2a7b8f60dad1185f9f24b7024012da30ac357f4424baa484084286d99b86b3865d4e54c33592a85983c28b6ac6ca3aec97f3b7158348aa76b0de2df7064dfa27a5a794151c96420d1ff294ce18d0e4de0a4fae83f985d6a8f5f3cac21fc3893b9fd16ebd2b4230c1ac382c0b5b978f7f386efc15325478ac028d2e3206b9a120a176f17825e60aa82f43766f07f4fad1de4bc751459b4f5b8bf6763a335a8652579b1a1d1f3820211e454ad692b876cb67dc5d65a91e2020eefbe116ef606089349c1f77a8f173dea4727583c1ef4749b5bd68a43fe6af06f047a46a41c85f6c8df1ad6b78b9af7a90fa04e4c830ab2e78f0d04970146d16e9c56b865c821a4c9f4a96c435e70ffae1daa8523bbb5c4a95f26f231981761fcc4cb1da472bdcf1cfae45a32bc671d556c08d1dd68a638db7300a0b04f5b2077f650dd45d80cb00dd862c333ad3d1686f7cc98eff2ee29e660d6c67c6c1cd1ff7e787339e32b733408a7d22f32497ef2fd2f064e5932e65c0bedc39c3a833f1537372f3be220f96af7c16aecb8082cfc353a22c8b9113668daae79bf6b8c62d5a1c4ab79d4db5a1f3ae57e34350507a148484c7739c9cf302419154ee3b10894b08c6161ba73380e35c497e755faa342f3929338f039644dc8bdedfd7f3492b14efffab85cc0e8e310d682a30d1c2efb17d413c209ed13155174cbd903bb2ee12373f55ab5bd3c16653dbc1ca059bf7da9eed701751bf13945fd9ee0451144a56da8cd1ab37c1c739c9192a513bff01ae98eeae71ecd5ed145ec227680db0a86976c5a337288094fca8ac9919354e4581727841eca4ace7e00cac1daa927ca734756bcc8008e6c2a28544b719b745611a2a910334756dd99fee446fc4260f870fc581ce2e36b28ea1271b2f272d7ed4649b2223e373c08ccc72652bb5ed26abc0f7ec310e15c46b17571cb13eae01b2472461ae2fb9bebb397b880dd930203aca8c034bfc99ee5165634efd62a88189e9b50002a02db8c5f9d241b3d2fb520b08aa1fc3ed7eddd3a15e1fdc7cb15234e71ff7a933405ba76a325b7c42bd3affed94e7d8a7600417666a2976659c29dddf962d9f27835ac120edf2b25c1d4a5dbf371091d369a068326e6ffa356dea6b3068388e17184acd7b93e11503c2739e68912a05bddd04893a7c804e83f4fd68b5205aa0877e172c642de0151c2076ad4442e40f376a3d7ca48d24121cc6b79bab750db7003e57e64c893b4e150a1742279ba7638389554c970513d835d32ceb1ddd85beb4120532382348d6003483448431a1135688021676ce4fb2af7a4b99ce2d9b539488b1019cb26eec4c88e212d4ea14d35d872ba5d9b2c4b636137db5cd6c6a881391121e4159bc3f86f0e8f36354d0e4db2efbb715c6f4e7c0358ec2540a7c87f316a060c13e3754a22dc2a6d5e8a99424fcbea07f1c16f23500acf1a4e40d8482448ad663fd28e413cd0125d450a91dae31120e1b3c4f7b588ea4a0d9f9b5f41267cca520763b9e7172a9943a064b4afcfa0760b8c0e2183348171f7a2c05ed6f994fb1b195bd7fcaa5cb857a4e79c509b037ec2f04544032d067355b7ca609eff582c5a4a6051c9e326c038c966c9452cb083e649adc9cf7fee0f50ee190cb01df8bb51306539bf69497005d57276d04a0e85a1228adfd831ce8976dedc6eb304b4119eda29e27f7407a77ba41a1233539d2cd739bb093d0fa8334bb34737d17b922e3969ecc419d6c09e54be9f2e1a552b787b449744010ed2066dc13843d75fe91db0647e8be22e528d6de262d9f5df71af7859f6b4ea42716bc2cefaaa5b6089ee18cf793fb56906d9f9b0070b2f3339019bae4d8c07c52e7f7b3f14229eb399d704cf0f895112ef7b3e070a22a48cbf781879d05ae8f30ac4fea0b3d8d447cf6524be91b10b47c2700ca3931c4cd1c40a091d701a586b067490c0d3116e9622cfc66128c2ec99640c738981d34ef3ac4231a96e41fc6c5b497684480a93ad5969d89e19e73649e5b997cc2c1f680fcb1dd1b2e991666146af5651b93f0c2a0ca4b7d4a7c7e0749a551d6a7d64fdc56749cda1d21a829071caaa7e91902b45b2bc4b494e906f1e3bf4217383466ae116b03cfb0895f3721b80f45f390dda009a6dc8e70d2038053a6a0db9da8ab72014d622ea2b2c3359d3c254d99d42e17c9a95d7998b7ab3719a8380fe97b44f1beac32db304a2e2e7443d301be218b7cc1626b036ef5cfa7419005ebd9b60938b7ca29107440d041b4349c4a4a33a8234a0e9cd020a80f77e9075f95674466eb6445a6f935c28c82ebbde18eb90b873dbfea4486f051741fe7199c5e1572af130740c860ee9ff997b28666ab656f8aab4feaabffc62121977557760f1fc12aefe88a026510da5e9cfc7b78a9520182c89781ed03ccf0aacabc01fc4bda3092c4aa724eb2f9fb718de4e9ea32aa9c5c8bf6d63d621390cea989d73e13055c1b3c3e34940ac51312414e7ca889d7ce36e3d56c82c4f17cc32de2d134b67e466cc5da9ae0b8a803d5d8051a357718aaacaf1aaef93432725e27f9200d82b6e57da1a24b1578109f7f86e2f22239dec9fb6bd59801d2f523a7ce7900d19db58c48945afedb1f7d04e994f10348d90ff3320e0798a700094fe7f916e050b04931995555223e0b4bee90ba493b2c4615443ffd6da84a917cff81b910a89a2f41c7a08be4f688b9177443c7c0ba9ceec434e88d666df93a2c66f99c297a4321e591afbe91c5f36890647f53f582d6b481883ea36b25c9b0ecba206c5eef44a48f8c20be628fdb463fd3a3bbced0c7953ead644582c43c6ca048d3c65d228a642bf78718bb4ad617e0e84221595c5d095cd7135290b4a8149517f1276f7e5f38f1e37e8e5c7e39279cb2e850da22fb4a03132953750ceb4f16fed3a949a19f5e2490e97547e77938d7306e28836b0b4ec45aa28740cae00f63615f9a1461a4cd5cf3e838c8f71a0734ccef8f1a8c637ead631dc5d343dd01cc14869eff61a9c56146a82fa46fce38543a45a74be39b067a89f3d1812de50655b45a83bf35547869c4ab0259b5b0b95093bb2789a50134a6826e4c70501128d02cb2f8756d8b19ed547def3812f711ae3f224d0ec0b20f3fde44603da4d72051eff0b96fe0ce9effa42ff0f472c1c7ff3c021b5a5b4beb815da71b81fd61798bf5d93433be5527c3c716981ecca2c5a5179b74b002df386262afd37d60a6f7e50c7e0ce7ff9d03fd4ea367adb9933d0f91064ea404ac9f013525246a49143f58e8188b629e09c03c7af9ee435d892e006193a21e0ace49735e4c9ed8cea85590c06fd0512f1452dce4d45292a5a498caa75cc41f56ab1d5c2c20fca27a65c77f0659f02c3eb5082fcac35bcfcb64bea97309baf25aba7a84891f67d29bc22ed4849be867b2df70ab97a7650e685823b0fdfbab5627e7b3ec63caf699a43cb454b1272a51efe1af4a89d00eeba3fa2bcfd084a5a8eee066d28121bc5deed40b3019566e9f9f2a60a4aa57ae88cfb093b262bd178602e5050e2cc9d1b3209c78459659259891d1cb7d2012ebb2d3c10ea6192bac1e1a466abbdb3f8406d2da4369fc7865b83bb9a1ddc1e458034e836fd930a496f45cdd0919dbc7afd065ee0d43c7ac3330082bc9adb43001b9026120744703055a9a8b240e40646df3b58636e6a7820f30b90968531b979ea8b84d0bda7364a30bc79ff76d0661598b369f2d42a2bd77549fab20ae578a51b91aba98bd74c2fe2a57a6452a4fb0b736be708defad5666fe46331cd3806ce678e1ddffdc26c7c7decc5fdbcf3c211ab5ec1043546011c60ed39f13875b65d3f4fa6c34c612e83448a26024236e3e4d3b8d52803532316730ca197f7d43e6478bfaa442274498266bf4e1f25542145a7e95e61a78f5b979f0546903cdee18c82f54857b36c65f7c0c130b7da516fef8a78e3db9de4a8a80ff01be4ea58173573f855724047f50610eb1f77f269ddecff5eef2d28cb78c1b87eced96500a38efecc23d956d3473b47c10b4fc0a9b1a07a43360c372f6930a821cb4161617f0b98060ae5901ff613cf74c5739e990632d842c469f7fad8ef6378bdfdace064b872d25907fd1965d2f3779c97d21498fecbe8dd0d95a600b220142fb1a91928a4b306f280557f75846cb156947514779f9ec5168f491b4fc894d8fac6dd53583c10085ff33eee760c4256f042b4b5704d77460d2d61fe5f40aef890134d40186521e196aef8b113e7ecf038c94fd5281ced224863abbc58bd2cbb8173e894b051db4617cf764d90e4d3ac3d75843dcd865c18d736b503d1616d6035db07bb8c91fb46f54195fd53348408cfa4291e41b84ff7fd58a0a520f615935846ec2eac3358053eea5894b95b9a0bcdb05869c73dc59c7d5a7503ff8a5b234e780311bfcbc6686118ec09b8715a3f8a897e6d240c6d7c39d096286709f2dcd64a2571c2ad0a4e3b7ecc722cf0488f2e3d492f6bbc338bbbc1b94d2587e6468521469655b1fe790d7d9321bb4849bd2cb22a407bb9143f0730813d1fbeda6b690c04c87ed38021ee8d776d02b104472395275e5883cdadfbcc50b605a0440c3c193c1e3e4738285e63e75e4448dd18626943ff1e13bd0837acc6dcd5a9bd35cc591c7989b7bf64e68b338eaa2f2040669f7ec3909ae641dbcae0aec1e5129e09bab013adb426d5aeec2118ddeb12542c00dca250788e60b99c9d03bb00d9818169bb969239939b87ed70f9750cce81830a1cfe30737940204ade0a085d0f84dd4df37aab27772fdb3562fb210adcd9552a5667f01ba0ea312cfe62a57236d5e985baa67a2679223ebdc16e0ece5732cfafd520d473931821c09799329a36cb089e9e133f64d670577e0679825b8aa824d2cbb46be9732037da5f6d592d04d72090dfaeceaa2577c77ab93c14063f8d2bf4a7b898b9aeba38f2b5abeda6cb6adf1603f61de6a56d21be87580bac6c2b21105d2c0cb716db0a6677a27385a323d38143a8155ef1ad1fcd6cef0976dd6ad481be7c2d77f451f87f9f6347baff719a9320de46cb0bdc398d546d0e019eab13f7e65addc434b605643fb8d268abfc0dfbd5bdead7654c83c398f6a5039195fcf37e3dbcba4f6c94181d5aa5ea810881d7747c9b7173a14ffd8be616d209af43f07fc7488e24b25330ec74bc49a23cfd5900df89fe25210d24297b964bd8c9b338075e19decb1dd13e79e7f84899b37da4a3c3556e00558645635f1ba188f9ab1d0e82cfc77ffd25558a8b88cbcd18118354884da271715b1d69ee706a4d8f03828c11c53fd330d94a9804c20b260e906d2855c007be3ab6a4260001afdb003ed3acee7ba1195d615527764951cfeebcd4adee46541f94a3dcb6da3a8555c36600530f558d5dcdc1e848f8883938fcec7c3ff8ff3460415869104d9af242979599a56b376444291dff54041a0e013fbbdeb4450f64842b9ea89dc726f56dacfbd5d60bbffaca59684d32494b66c6e4deff00d71d4ac1d7e4965ba7785fee16b2cee0736e52b791da71c4e38d29c408f49afbbdf81cf76f45c536f60040130fbd5b8bdd82cfcd67b295176af88e0cf62c77f6ef0209ff22469f816d10e223c2398004a9ec3d9dbb763c2dcbfff7776dd5990e9a2170d0f81652232fe4712bd1e4d7f603991898e73374f02d2b9ffb3a46b901c3dd8d57c82cdce4fa995f20c0693771f5aff0508469e02a46a72dc49fab78c497a7991b7f2d04e3003fbd6910344118e0a7d8246df25291e94da2cefd5c8c1b3e9e2047b078630ffc280fee60cf2b36f31292a8a4c2c97e5009e4c037b2e305ad4951ccff368f9bc85ee889b92946217f77cdf24987261df1984f418419657b0ab26c55574c88bf416f367dd08c89bc974db4cc500eeee74811d79230614ee31ff835bcb31eadc654b6607d60dbd35632ecae3e6b7b41de44311bc1256ad2ee2f655236142531f4a6233cf91a1c4798f0724a23f5dbb5f65aac97c414416a21a0471e8d6dc9dbd6235d8a153e152dcff41db653a58b6fa4ad304bc4650041b459ff70e6907caf0d537e842aa8c95af099df282d159c89b5116d7dd39d76c70452079ff1bb90eccde5ca7836fd10f6c232d77f54f484edd244a46b55f69208f99f0c457f1493f911805d104a5e24c3bc3a29c47096db3f16bbdc499f1504fd3d93ca369f56f6806966dc7c67ec95c9d07b5523dbbabc8c1966f1fcf045e09c324c873996908ff72df3f63bfaf5a23a49eba1af523ab8e8df548004ea165dab3903712de2a9a06f4e21f69aecb632dd635c6e3d67b4901ed1b5fc57b77bbfeb4e66a61ab0b1e22317b9128691ccf7c554997ca0523277224c93bfa291a08fc19d96597b930b65a6f42e6626302b099f0f3982d1c0878e0adecdfb00b3ce04fbddff87bdd5b12a796e909e80047dafe8c4f30741fb6473883349a0353d2c633f0f979ecd4744c24a97e95cd247aed654c6043d8a77760b152101401b3e41cde34afae86545139b81a3e6e888b74dd1112d61a9cd8c70994fb2d4404cd0fc2c518f2b51cded9c61d0a3f73ae7e8c34fc1f0f51d35",True  )
    # decrypted_data = mssdk_decrypt("e5e02cc07e462f7fb5e3d5bba65c222de8946c22b31e4a1a53a61616039103bb302e668d885b4b9e834e3fa6c43a3b4ebb7a479602d2c5c24caf967b45db107d546baa998657eb3126e9893b3bf75aaa5c2eb92e5e9b5b28ca273ea6ce9ffa7bbb5e8c9a6332e2ac1784883decb532e2301ebeec751816e3dd5f614983bf59e3ad1c6fe7cc2c10dff56662c550f8ca9be264ffd36c87fc97cb6a56c13bac3b87026dd8e340edda2ebb225209a21af191f6959fc84030ba0926375dec76bfb6624f098512adcafac312a6d8d8ec5e66f78130fb5ae11c78483a5f3dd6b92366d6d61469cfb3be3642bfd4f08dc37c2195c308acfd4b1f680bb020f1db078d1abcb65a0296d36f18ef80a101127d177e64a07b9b7dbed03629db9db5f1f2c9116f919ddc647d12d9356ec70104476e37c77e0fa7fecec3b16c4ca833ab5c966e5654806d26a695a8608afca988278608a072d4abe8a7506c35f02748b7760849b4e832f2fbf862aa4e5aa879dbe5124c16fc41fd2c710d53136110292b737fe0510eb72ebce03858d3d20cab58f8ff3fd5d36b1cd084c5689349edababd40b608af7b7b5180b0db6879f669a42a8c794f94801a980188e35a282961f8fe48996069ada8e5bb5ac1e8f5f59d521d9f140136666b4ad2e2f319470109510bb18a992c21d2df6e495f654ba4f013459d45d9b985de55349651eb9aa718307638d32b418f96bc6be7c4f8fbf89375ab2b2fc4544d11eabbe22d7eaadc11f2d4fc991c33682e75b9c9fcb56e59ae151f2271ddfcc9ed03d313829572a1ef8bbc6690306f732d96c81c47a7fe0d0c539488d0aa74ef379e88a08b5dd956d0665b0a7369d5854409cd93793a31ed3d98b65b9fedea5c9e480b5f0d15257b1e860b91ff3d90b16c7c4a67b27e96327256d15e360c6bef883dbe5234ca136eb46a16b13944691cb223b4d2ec3606bca9564d25da13836fc8d9005221bbf6f7880bff4ae01aea23e4b8ee3035cc0ee2b215e332c5a1ab89318af23388683bce473aecf7e64d122a591b12167982d81ae58d99d29c1502ec8b7b80aebbb4f244f39a096a823a224e5586e393c18292dc44d86f413aeac8290b9801d4ff56d5c3698397ddd5c62879ff49c0c863250d83e9e0b5e0df4e4834ad352d3c775f58259dad3ea38db7777ea3c4c69371a2e5ad7c16cfb8da88bca40731c498bd292f7fff1fe302aff57976052caa3df3920b8f43ea73adefe397a6e2d2ee4ab38c00e7cc5fbdf953e2924bfeb01d03f729a92916a5664c24f61bfd3006ce9be16dd84d6977f6af193c0ec01abe681fbdf77374f7bba6a0beb181041b3f08b3a87030d8d274cad12aab51680167c50017bd469f47c3a0d9f0e28889019aa834103e9aeef38d81fa9f3385fa30a4390a39e9f5c04c018d4e7428063ca62d1c5a6282a22b8abda31c825c3e3e963311c35124a1d630e364f81592aeb07631ca4e4fb56fb96c8a8ff944fb827dc266e4c308ed5a02289b68bb87cb06d2e9f566b18dc653825bc1e7437a1e0d68f7252ca13e2ecf93a72adbfcd687e42c81277bce738376772f4349f6a4f6c0a44cecdfcbab68c49bfded11f19a38715a8db8adede116fa68ac304bcf52255b156732f4525adff459c95778bc55710d1dcce8ac366426c0b40c748dc892febf8522ce38e1429d4b953db9055f804ad330792a83bdfc6d64d45d8d1501d4a9df7fc95291b310952d13709dafd0dbb8b63ac9ae752a6a520e09bd5a787d369ce1d72e433d09d3e6af7739ecf492af954abfaa6288a3cd3bfdc5e2a5a898cbe5f611fba4db5b2dd429d0d38a760ce08dec2295a1c33602ad2dcbe9512844968717b8cea553bdca036499a8008bee79653a0badaeda119ca0464434264a6528d669e0c9aa5295b320a476418622da2849ff7532f3e7cc63687cba64755d656c93fa5502675c4ad15a94bd1c1358d15220d3d8a7d72e817914e9fe5fbe9240530d930aa2d3ae06e2f338283135d579ee1d0e1584adc88f1c50c5f820a0020acf3247d8adf401a7e80c6dc9b6b6713c50759d26d5c9edf888084e23d0de2f8e4a30cca2cf95997df93af8ada3307677d01a3ff98502cf8354727a57e60f38f887694a84ed6c2c049d1af05a12575d9242e9da3a2916dfdfd802070cee1b52752fe77a3327a71b0360b81aa28684c7ab5f84ac8785deac01efd706709d0742820f2eee097355a41abcd0bfacfdb37a46635fc9637b4830d726575141bd8f02eccefb3b29b9d9491370daa5ddb2c5b12ca44fa6906ae2781bac8543e9da1818a1b409ad8f6e6b5ef51d7b2c9cda3d6eb0770d0a7791aa487c0a18761095e1acca11357316ac0bc0f115f6f017dd94faceb47e1d8103837c0ea9d7f21c915b7ae1f726e40a44841e645003eefc7760216c2578c6a7a3f3b104882eb72783ce4f158bf3dc730004287bbb7a33b4a968b3647a43876c8dbdec01923cb3a521223632aed35e70ef5435c5fabae8bfe1f24cd457ed78f9b411268b1c72bbc8e490f0ac80e1a0eb6ade9e7d30fef64d80729188ba04ba8260484c8940f40893e75eec3ee1a57a812683727457247102a72338804501c792ce6d2781bd2b04c0bf27a466ef74c4d212fc86a5f73d3d7decee191984ba24248ac8b74c9f14fe7e68ca38eacd172a288188059e45741fdb9b29b527cbe1251990e5509ec61a71ca6a77305a63f151cd64f99a01e8faa045d58e94cc8b8368ecd0663b8608728d9dd409d5a8c3fa942fb89f09b0b4db8fd555c584e3a22fafe03070b77251ebab3f9e3c0062dc70c5f2917c9bcc03a1ef70efd6c5bc7ea51976152c221708e73c216886ef0e5bd80f737cb13282204922e7f81ce309beedc51e390c42c46192aab70382bf08cf7cd969a6bd6cc7ec05ee791239b89e3616fd29e8dd1759ef4ef27f74fd6c7f2b4f10bace4986bd256eec3fa411f753551cbcc81f0f7db94b4f69b4cdf9e6e35b19f3d1caa66fa0633bfeee01c188cc6fb91b899940adab3c4df56ca1a16702eaf0af26269ac73527c42c3064d965987b2d7fafd0e1b84248fb0341e8c7c3cb94a6428ce3246c3f284ebca4e6d0a3ec8908f9026982cbc7be3d4a52ccb3807c25bc39ae90be6fbdc27e8030d548f32641e7283b21938a34b491956729078e9e0537cce0e80e8ce6135e344fd22691c4854aedbfd529bbe322cd5ad27e7e4f5ddb6d62cffc6e3ec4dc4b5c67375927b070fcad98f0ec45d928042ef1650ca1cfa624e07f2ad8fe0b02e4fd80856473be3ace638b1074bbec902652e56df6e61c6e585c0225eaea15fc3b77dcba8ab70dcd8cddcc2102b1cea583170465b446d844482dc9abf6d86f73a3632e5b234079fc9f59cae7e54966de9cd67343444d6aaa9b545147e26f02fcf4cb1f8a6fe43d55c358c81537429179027e825f696dfcaf57517156972f30367061aa1c126a91c039a08d0254ce5332cc5807607e4469e0b08478d0695fdef86c8abea3cfaf18b25b5bcbb2615c4820c8b916970e6208525367c6a8193dec37f81de43196bbcb51207f2868a88e80115d2c64036a872c106c189850089971dcbe873f0bc4295074ef5b5dd53a65e85f29f04d1d0eed932c42927e1c936acb43c14c98a9a9ace299fbd27dc8bfbc0706ec29e0e6c53a35643b40dd6404d71300ad959be272061ed927185456bb0231f8cf4d346351155e480b54978afed390e8e11db1c243047ff37d1f0b6a338c0e417da24290214ff34280a8e7986fa158b0c1d231bb63cf5570a2f077c74bae61c664a034ab810313e8f1db815ac9c4665e7684d90400c4d7e5d33a8ed0a1c6a22e9881d5ed93d49d02e095f9340aee683742502fde05266258626d60247418b84dad1d7a21eb364dc7a345ef899be6a5a1c02264532d8b50d0ce2be5b1ccce0fcd2311b735c406549d08bbdf529acc3ce5bc6de0813771e33ebf9fcdf295f14101feab0767d80ba2436c7a2f0f2543c556bde0482377e9ccad062123f024f94020c18a3e17fa1987206eb11ca999ac053a23731b73a6424ea4f8168b785e06d2fd7ebadb160be08ee4a240bcacc9e177c9a1352f53179fcaee503ea5cb28fc0eda54b59e229d2c1888eec80ec21e678b0ae793cb9e413bc8dd331ef938692bd98ea39289a8c399d1cb9684afb4c6d2d811cbefcebfd184e02feb6053517f82ee8ad05b42e6a3b672e5768ab08edd75fb566a11c763a587f26b8a682ddc2ba84f9b04a41c18eda05483c9792045b21ed6960fd8dfa09738ddb484d79d8dc87ddb2bd113464d4610df246061f8cf983bfdfe73ecb28e553b86b50f9e4cd23d70ef9a662ff310345d5c364d36cf00df1433a74eea8e4687aee40c95ee332c44cc685267ca3735f9b62cb1ce0ee532edcf1a079bb410457a368b86dbbb4953188863b5720a59776d90727d53dd9ded342ce67c12081e2840306dc4979c97f1668c73e49c97b6a775628ad4bd39c41b319f4f974684a415a88235899cc150967ad71ea15f73ff9fb768b9233e48728333b9d4db02e372b6fe2a6815c1221ad21ada2cae9ba3b53632ff89fd653aac46c5a2bdac68b0f84d83270f9be7391d339d17e8f9979265cf8dda9c1a7b854e6f67ec7831d91d4a5a4ba3fc20147edb6672e7d12d05f1e75846935fad97efb2c01933197c8aa471373a2dddcaacae27cc1b8668f3f9cdc752d071f718f626c2cfb587a6b7f3e031f7791c041dc1098a7ee8997bf7e36f77d1df4d7c2b8f3e681d7cb1887e1981f279452d5ed462cb028cf9f241e4f71b3e4fb9bc6bbf43ea015688797c2c420bcce1f23940d4f97285c9d69b4ba3c620387f6f8aaa2888229bb6e3deccdabb93a162528b041f6bbda740bac5f355ef01138fe48be2bfafe134789388c8ba7de64413b24457e68f3b8386062d3c3ebfd2512659c6050c24a705bb87bd3dd22cb0a7dc538da6402ea0a5e3c2f791fdd87ec9f5d40946eba215093cf127f27865c054c64c402e8e51ec3b561972a47c78f54e816b95ac8373e2a938011a91addc2741f3ba9a30869df8309cf0bffe2cbaf72f3f4727f6a9c2397b72187131eb68542340324c6fb29ad39dcf7d4b1b8d99e0f5404a8028a8d5de8d32e7ce8e952e0db31c73d6ab8bf25dbea124943bd96e831767dd9f29dda1d0d004d8e1af6cbef55c534a499d0a49033125266d566370caebf8cb66eb76187a3e5b6d690a69caa51f1ba6c6eba89184dd35d2fea8fe34ab927a91dbf2994669faf02cc17dc7cdabeb596cf850af71155b60af2d626f79b7b07b8e2869011d353216618568be869c4f2861159ae88597c1f3d52fa47d7b5d1e668ccde2f36c8d3194a9e3e50ad1c6fb7f632aa0f8fbbb2a1ddcbf91373dc9648f633492e054dc3c8cb86927811cb22ac9db81d3a8ee20789494c134ea924b555e4a990400c40e3687a177916064b127d33bdbf4d6fdb885e85f0c19d50a049b47ec1e13a1c0d3080d78d819896a28e1b85f2d3f5781a2d84193487755724273612d60321db48c09f5f79bd2747d49928cba3cb1a3835ecca3c0cc8140e85981a9c7abb987b4a38af93f3882fb6c2e681db2da37b7c6ff3ae71c42e63c17aa2c316aa8d2bec5eb1bb0c01a4439b0a6da6d23022ecc3fc017d351af80b688900b49626f02d7b47bf8ae2a9d6dcb51cd697f4206a15f0e977b04de1f39c98353e29d6b2a8fcd7f64e055603a5548ec0ca058436317a601f56b24140d7b3f730d561ede0ea0792788bd379451b74a57fc8533cf2c0d5e7debbdeafbef93a17b2607cf76d9cd849c5480e5b195b76424abb0f06257903d7f2cc3ca0ff49c59a884813485ac30092ab19d597da7dde0194d038825cae31d70df1ba50ad227a9c7826f55d28f543f7cfecad2a60ff1e1d337248c496be56efbeadbb05277a2d11da564cfad8b441a4215bacf3da165ff409cf4de9dddc71213c3a820b3d83790ba15f5f2b9f8674bdedd61833e93413ffdbc649c843caa536af23b6aec9cdad716046bc07151eb295b538b31ff4c15d34acaebd210f6e99d5d5db7e0e1dde24ad6a69e06b1778d15bef2b0886bbde931e57b5e05321d32218c1b45716fe20816fb57cb7f13186d808c573e9ebd9ceb2bcb39c98322c66d00955f5cfb13d74ec16e451e86fcca499af4c84a360be0fda6c9a6379b3d9b1458512f2488a20195d7fe9d7036b55d807c8b0a40d384a4550d260fefd87bf8439b9f89bf2c323b7c31a03f37eff7e4e64e82374bfb2dcbe2be63d2c62e813ad6d86f4f81279237d72d437777d1660c7b8e46ffec2ecd3c38667ef8d23c0eae3e8bf34aac2e8fc2ee77082f264fc0cad272ffea051e251c12381357728dd0ba88ee215785c88c4edd128608d1b04c196ed9336e94693c19ac99e3fa0b5ad7d1f736ad2c48d6b6fc6036b70afeb398435ddba6285e177f3b5460e415e5ad43521d21f405610fccd468393d46071bb9d7c60d3c081c6c1cea567972d8da58c36f0451e301f3d75efd13ee2ffc5fe4baeab2ec6ae4f70c5d695a53aed6e2bb8b665b7526197fa0298985e1d8b6f05e66ce93b7006dfd6eb81a6ad8584b56e2511278fff89fae44454078a4782f6da83dbe965f5b03adbacd61dff31630f71643537063ffb3e15337ff8d9b170578eec9404a6597a91303dedcd54fc079b088083cf03581093b10126e2be873b6b903610737db00359f4acdda0534edef192a6906dc798d191b735bfa504a224ac2c81af321a2c66127f70cd8be9a1113a44c391ef04c1aa",True,True)
    # decrypted_data = mssdk_decrypt("e119e2a328b5df84da6b5944ad9358293faed09507dc8403e546749c32328c20227dfe301e142d255ceda78b8e38be215286f55fd185e4d137b17652279af07621e0c6d7d7b2c31a327b6d3c4e6f683deea374c83d202a436ba8421170d10ec7",True)
    # print(f"\n解密后的数据:\n{decrypted_data}")

    # print(mssdk_decrypt(encrypted_data, False, True))
    # 4. 验证结果
    # print("\n--- 结果验证 ---")
    # if original_data == decrypted_data:
    #     print("成功: 解密后的数据与原始数据完全匹配。")
    # else:
    #     print("失败: 解密后的数据与原始数据不匹配。")
# print(mssdk_encrypt("0a2035373439353231333830616634376163613036613332346466316665383832611213373532323638303239393332303634313037391a07616e64726f696422097630352e30322e3030",False))