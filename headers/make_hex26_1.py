'''
created by mwzzzh.
待解决：
    randd 来源分析  运算过程中有一些内容找不到来源,暂时先将randd固定好了
    16进制字符串前导0的问题 已处理
'''
import hashlib, binascii
from Crypto.Cipher import ARC4
from gmssl.sm3 import sm3_hash
from Crypto.Cipher import AES
from Crypto.Util import Counter
from Crypto.Util.Padding import pad, unpad


def sub_960bc(wmz: hex):  # 入参为四字节hex...
    res = ""
    for i in range(len(wmz) // 2):
        tem = ((int(wmz[i * 2:i * 2 + 2], 16) & 0xaa) >> 1) | ((int(wmz[i * 2:i * 2 + 2], 16) & 0x55) << 1)
        res += f'{(((tem & 0xcc) >> 2) | ((tem & 0x33) << 2)) & 0xff:02x}'
    return res


def make_hex26_1(seed_encode_type: int, query_string: str, x_ss_stub: hex,randd:hex):
    res = ""
    # randd = "423A35C7"
    if seed_encode_type == 1:
        '''
        首先,对query_string做md5并取前四字节 这是结果的第一部分
        接着,对x-ss-stub做md5并取前四字节  这是结果的第二部分
        最后,对00000001做md5并取其前四字节 这是结果的第三部分
        '''
        part_one = hashlib.md5(query_string.encode("utf-8")).hexdigest()[:8]

        part_two = hashlib.md5(bytes.fromhex(x_ss_stub if x_ss_stub != "" else "00" * 16)).hexdigest()[:8]

        part_three = hashlib.md5(bytes.fromhex("00000001")).hexdigest()[:8]

        res = part_one + part_two + part_three
        # print(res)

    elif seed_encode_type == 2:
        '''
        首先,对query_string做md5、取其前四字节并转换每个字节的大小端序 这是结果的第一部分
        接着,对x-ss-stub做md5、取其前四字节并转换每个字节的大小端序 这是结果的第二部分
        最后,对00000001做md5、取其前四字节并转换每个字节的大小端序 这是结果的第三部分
        '''
        md5_query = hashlib.md5(query_string.encode("utf-8")).hexdigest()[:8]
        part_one = "".join([md5_query[i * 2:i * 2 + 2][::-1] for i in range(4)])

        md5_x = hashlib.md5(bytes.fromhex(x_ss_stub if x_ss_stub != "" else "00" * 16)).hexdigest()[:8]
        part_two = "".join([md5_x[i * 2:i * 2 + 2][::-1] for i in range(4)])

        md5_01 = hashlib.md5(bytes.fromhex("00000001")).hexdigest()[:8]
        part_three = "".join([md5_01[i * 2:i * 2 + 2][::-1] for i in range(4)])

        res = part_one + part_two + part_three
        # print(res)

    elif seed_encode_type == 3:
        '''
        首先,对query_string做md5、并在逐字节与0x5a进行异或后取其前四字节 这是结果的第一部分
        接着,对x-ss-stub做md5、并在逐字节与0x5a进行异或后取其前四字节 这是结果的第二部分
        最后,对00000001做md5、并在逐字节与0x5a进行异或后取其前四字节 这是结果的第二部分
        '''
        md5_query = hashlib.md5(query_string.encode("utf-8")).hexdigest()[:8]
        part_one = "".join([f'{int(md5_query[i * 2:i * 2 + 2], 16) ^ 0x5a:02x}' for i in range(4)])

        md5_x = hashlib.md5(bytes.fromhex(x_ss_stub if x_ss_stub != "" else "00" * 16)).hexdigest()[:8]
        part_two = "".join([f'{int(md5_x[i * 2:i * 2 + 2], 16) ^ 0x5a :02x}' for i in range(4)])

        md5_01 = hashlib.md5(bytes.fromhex("00000001")).hexdigest()[:8]
        part_three = "".join([f'{int(md5_01[i * 2:i * 2 + 2], 16) ^ 0x5a:02x}' for i in range(4)])

        res = part_one + part_two + part_three
        # print(res)

    elif seed_encode_type == 4:
        '''
        首先,对query_string做md5,然后取其前四字节参与后面运算并转换每单个字节的大小端序 这是第一部分
        接着,对x-ss-stub做md5,然后取其前四字节参与后面运算并转换每单个字节的大小端序 这是第二部分
        最后,对00000001做md5,然后取其前四字节参与后面运算并转换每单个字节的大小端序 这是第二部分
        '''
        md5_query = hashlib.md5(query_string.encode("utf-8")).hexdigest()[:8]
        after_960bc = sub_960bc(md5_query)
        part_one = "".join([after_960bc[i * 2:i * 2 + 2][::-1] for i in range(4)])

        md5_x = hashlib.md5(bytes.fromhex(x_ss_stub if x_ss_stub != "" else "00" * 16)).hexdigest()[:8]
        after_960bc = sub_960bc(md5_x)
        part_two = "".join([after_960bc[i * 2:i * 2 + 2][::-1] for i in range(4)])

        md5_01 = hashlib.md5(bytes.fromhex("00000001")).hexdigest()[:8]
        after_960bc = sub_960bc(md5_01)
        part_three = "".join([after_960bc[i * 2:i * 2 + 2][::-1] for i in range(4)])

        res = part_one + part_two + part_three
        # print(res)

    elif seed_encode_type == 5:
        '''
        首先,对query_string做sm3,并取最后4字节 这是第一部分
        接着,对x-ss-stub做sm3,并取最后4字节 这是第二部分
        最后,对00000001做sm3,并取最后四字节 这是第三部分
        '''
        part_one = sm3_hash(bytearray(query_string, encoding='utf-8'))[-8:]

        part_two = sm3_hash(bytearray.fromhex(x_ss_stub if x_ss_stub != "" else "00" * 16))[-8:]

        part_three = sm3_hash(bytearray.fromhex("00000001"))[-8:]

        res = part_one + part_two + part_three
        # print(res)

        pass

    elif seed_encode_type == 6:
        '''
        首先,对randd做md5取前8字节作为aes的key、后8字节作为aes的iv(注意:这里的key和iv都是当成utf8去处理的)
        接着,分别对query_string、x-ss-stub、00000001做AES-OFB,
        这里稍微多说一点关于AES-OFB,该算法的特点在于它是由key和iv去产生密钥流、然后由明文^密钥流得到密文,在处理时每组的密钥流作为下一组的iv参与运算
        '''
        md5_randd = hashlib.md5(bytes.fromhex(randd)).hexdigest()

        key = md5_randd[:16].encode("utf-8")
        iv = md5_randd[16:].encode("utf-8")

        cipher1 = AES.new(key, AES.MODE_OFB, iv=iv)
        part_one = cipher1.encrypt(pad(query_string.encode("utf-8"), 16)).hex()[-8:]

        cipher2 = AES.new(key, AES.MODE_OFB, iv=iv)
        part_two = cipher2.encrypt(pad(bytes.fromhex(x_ss_stub if x_ss_stub != "" else "00" * 16), 16)).hex()[-8:]

        cipher3 = AES.new(key, AES.MODE_OFB, iv=iv)
        part_three = cipher3.encrypt(pad(bytes.fromhex("00000001"), 16)).hex()[-8:]

        res = part_one + part_two + part_three
        # print(res)

    elif seed_encode_type == 7:
        '''
        首先,对query_string做sha256,并将其结果的每个字节与0x5a进行异或后取全四字节 这是第一部分
        接着,对x-ss-stub做md5并取最后4字节 这是第二部分
        最后,对00000001做rc4(key 为md5(randd).encode("utf-8))参与后面的计算并在最后转换每个字节的大小端序得到四字节 这是第三部分
        '''
        sha256_query = hashlib.sha256(query_string.encode("utf-8")).hexdigest()
        part_one = "".join([f'{int(sha256_query[i * 2:i * 2 + 2], 16) ^ 0x5a:02x}' for i in range(4)])

        part_two = hashlib.md5(bytes.fromhex(x_ss_stub if x_ss_stub != "" else "00" * 16)).hexdigest()[-8:]

        ciphertext = ARC4.new(hashlib.md5(bytes.fromhex(randd)).hexdigest().encode("utf-8")).encrypt(
            bytes.fromhex('00000001')).hex()
        after_960bc = sub_960bc(ciphertext)
        part_three = "".join([after_960bc[i * 2:i * 2 + 2][::-1] for i in range(4)])

        res = part_one + part_two + part_three
        # print(res)
    elif seed_encode_type == 8:
        '''
        首先,对query_string做sha1,接着将结果的每个字节与0x5a进行异或后取前四字节 这是第一部分
        接着是对x-sss-stub做crc32取其校验和参与后面的计算并转换每个字节的大小端序得到四字节 得到第二部分
        最后是对00000001做sha256 之后转换每个字节的大小端序并取前四字节 这是第三部分 
        '''
        sha1_query = hashlib.sha1(query_string.encode("utf-8")).hexdigest()
        part_one = "".join([f'{int(sha1_query[i * 2:i * 2 + 2], 16) ^ 0x5a:02x}' for i in range(4)])

        crc_val_binascii = binascii.crc32(bytes.fromhex(x_ss_stub if x_ss_stub != "" else "00" * 16))
        standard_crc_binascii = f'{crc_val_binascii & 0xFFFFFFFF:08x}'
        after_960bc = sub_960bc(standard_crc_binascii)
        part_two = "".join([after_960bc[i * 2:i * 2 + 2][::-1] for i in range(4)])

        sha256_01 = hashlib.sha256(bytes.fromhex("00000001")).hexdigest()
        part_three = "".join([sha256_01[i * 2:i * 2 + 2][::-1] for i in range(4)])

        res = part_one + part_two + part_three
        # print(res)
    ans = ""
    randdd = bytes.fromhex(randd)[::-1].hex()
    for i in range(len(res)//2):
        ans += f'{int(res[i*2:i*2+2],16)^int(randdd[(i%4*2):(i%4*2+2)],16):02x}'
    anss = ""
    for i in range(len(ans)//2):
        anss = ans[i*2:i*2+2]+anss
    # print("the last hex26_1_res is ====>",anss)
    return anss
# make_hex26_1(6,
#              "lc_id=2142840551&platform=android&device_platform=android&sdk_ver=v05.02.00-alpha.9-ov-android&sdk_ver_code=84017184&app_ver=40.6.3&version_code=2024006030&aid=1233&sdkid&subaid&iid=7548284017058154270&did=7522680299320641079&bd_did&client_type=inhouse&region_type=ov&mode=2",
#              "C96C5AC3DB20ACC02ECD267995BA890B")

