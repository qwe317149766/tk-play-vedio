

'''
生成函数为make_argus()，需要传入的内容为protobuf 序列化的内容以及p14
argus目前是百分之百确认正确
'''
import hashlib, time, datetime, base64, os, random
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from gmssl import sm3, func


def int_to_hexstr(num: int):
    """将数字转换为16进制字符串并去除最前面的0x"""
    return hex(num)[2:]


def ror64(value: str, shift: int):
    """8字节16进制字符串循环右移"""
    value = int(value, 16)
    shift %= 64
    return ((value >> shift) | (value << (64 - shift))) & 0xFFFFFFFFFFFFFFFF


def lsl64(value: str, shift: int):
    value = int(value, 16)
    """8字节16进制字符串左移"""
    return (value << shift) & 0xFFFFFFFFFFFFFFFF


def lsr64(value: str, shift: int):
    """8字节16进制字符串右移"""
    value = int(value, 16)
    return (value >> shift) & 0xFFFFFFFFFFFFFFFF


def big_endianTo_little(hex_str: str):  # 将16进制字符串由大端序转换为小端序
    num = int.from_bytes(bytes.fromhex(hex_str), byteorder='big')  # 先按大端序解析
    little_endian_hex = num.to_bytes(8, byteorder='little').hex()  # 再按小端序转换
    return little_endian_hex


def little_endianTo_big(hex_str: str):  # 将16进制字符串由小端序转换为大端序
    num = int.from_bytes(bytes.fromhex(hex_str), byteorder='little')  # 先按小端序解析
    big_endian_hex = num.to_bytes(8, byteorder='big').hex()  # 再按大端序转换
    return big_endian_hex


def to_base64(data: bytearray):
    # data = b"Hello world!"
    encoded = base64.b64encode(data)
    encoded_str = encoded.decode('utf-8')
    return encoded_str


def from_base64(data: str):  # 解码后返回其16进制字符串的形式
    return base64.b64decode(data).hex()


def md5(data):
    if isinstance(data, str):
        data_bytes = data.encode('utf-8')
        md5_hash = hashlib.md5(data_bytes)
        hex_result = md5_hash.hexdigest()
    else:
        md5_hash = hashlib.md5(data)
        hex_result = md5_hash.hexdigest()
    return hex_result


def make_rand():  # 生成一个四字节16进制随机数
    return os.urandom(4).hex()


def to_fixed_hex(value, shift=16):
    return value.zfill(shift)


def make_argus_protobuf(
        x_bd_client_key: str = "#KjkNpUINEMHujZRcpH0Np1d6aXTsydJgS75NJatFAC5EX3CHL8Ny0Y+OMeZLoQ1F1WZidw3sPqe9W48o",
        khronos: str = "1751607382", sdk_version="5020020", host_aid: str = "1233",
        device_id: str = "7522680299320641079", update_version_code: str = "40.6.3",
        app_version: str = "v05.02.00-ov-android", device_type: str = "Pixel 6", channel: str = "googleplay",
        query_string: str = "source=0&user_avatar_shrink=96_96&video_cover_shrink=248_330&screen_reader_enable=false&creator_assistant_banner_enable=0&sov_client_enable=1&max_cursor=0&sec_user_id=MS4wLjABAAAAvSaG0A40IB8MvcQZ_124a1LT58O0IujUBl3D7WMzpyiwFivlQKA4kMfLBnhkMAeo&count=9&locate_item_id=7521075840356994309&sort_type=0&device_platform=android&os=android&ssmix=a&_rticket=1752473458782&channel=googleplay&aid=1233&app_name=musical_ly&version_code=400603&version_name=40.6.3&manifest_version_code=2024006030&update_version_code=2024006030&ab_version=40.6.3&resolution=1080*2219&dpi=420&device_type=Pixel%206&device_brand=google&language=en&os_api=33&os_version=13&ac=wifi&is_pad=0&current_region=TW&app_type=normal&sys_region=US&last_install_time=1752464702&timezone_name=Asia%2FShanghai&residence=TW&app_language=en&timezone_offset=28800&host_abi=arm64-v8a&locale=en&content_language=en%2Czh-Hans%2C&ac2=wifi5g&uoo=0&op_region=TW&build_number=40.6.3&region=US&ts=1752473325&iid=7525819072048088853&device_id=7488214722874394113"):
    createTime = khronos
    p1_1, p1_2 = 1077940818, 538970409  # magic 是固定的 ，不用考虑版本
    p2_1, p_2 = 2, 1  # 可以当成是固定的
    rand = random.randint(1000000000, 1987654321)  # p3来源于这个随机数
    p3_1, p3_2, p3_3 = rand << 1, (rand << 1) - (1 << 32), rand
    p4 = host_aid
    p5 = device_id
    p6 = 2142840551  # licenseID 也是固定的，不用考虑版本
    p7 = update_version_code
    p8 = app_version
    p9_1, p9_2 = int(sdk_version, 16) << 1, int(sdk_version, 16)  # v9_1为sdk_version*2 ，v9_2为sdk_version
    p10 = ""
    p12_1, p12_2, p12_3 = int(createTime) << 1, (int(createTime) << 1) - (1 << 32), int(
        createTime)  # p12_1为time*2,p12_2为time*2-1<<32，p12_3为time
    # 似乎是对x-ss-stub做了sm3，这个headers中并没有x-ss-stub，所以是对16字节0做了sm3，但是很奇怪，另外一个存在headers中存在x-ss-stub，但是不知道对什么做了，在hook真机的里面有很多都是对0做的，所以写死好了
    p13 = sm3.sm3_hash(bytearray.fromhex("00000000000000000000000000000000"))
    p14 = sm3.sm3_hash(bytearray(query_string, encoding='utf-8'))[:12]  # 对query_string 做了sm3后取前6字节
    # p15_1_2表示请求次数、当前是第几次,随机一下好了,应该问题不大，不过最好还是搞个计数器，p15_1_1=前一个×2
    p15_1_2 = random.randint(1, 50)
    p15_1_1 = p15_1_2 * 2
    # 前面这几项都看成是固定的
    p15_2_1, p15_2_2, p15_3_1, p15_3_2 = 48, 24, 46, 23
    p15_4_3 = int(createTime) - random.randint(1, 100)  # 表示app启动时间，最好也是用一个，先用当前时间戳减去随机一个数字，后续考虑优化
    p15_4_1 = p15_4_3 << 1
    p15_4_2 = p15_4_1 - (1 << 32)  # 跟前面的类似的
    p16 = ""  # 服务器返回 From /sdi/get_token request
    p17_1, p17_2, p17_3 = p12_1, p12_2, p12_3  # isAppLicense,等于createTime p12
    p18 = md5(x_bd_client_key)  # 对x_bd_client_key进行md5
    p19 = sm3.sm3_hash(bytearray(query_string.encode("utf-8")) + bytearray.fromhex(
        "0000000000000000000000000000000030"))  # 对query_string拼接一段特定值后进行sm3
    p20 = 0  # 登陆之后是x-bd-kmsv(这里我们登录后为0)，不登录为none
    p21 = 738  # 真集中全都是这个值，这里可以固定
    p23_1, p23_2, p23_3, p23_4 = device_type, 18, channel, 415318016  # 第二个值不确定，第四个值跟版本有关,不过应该都可以固定
    p24 = ""  # 服务器返回 from /ms/get_seed
    p25 = 6  # 据说是2,6,8,10都有可能，但是我们真机中的只有6这一个，unidbg是2

    # p24=
    # return "08d2a4808204100218dada8bfc0d220433303139320a3136313139323137363442147630352e30322e30302d6f762d616e64726f696448c08090505208000000000000000060aca1bb860d6a0664a12b9845137206101ddde9c2267a08080238aca1bb860d920110e0ebdec0dd9d1a2ee5aea1eecc0b34399a012081ed7838e57f50e0aeaa8a4da24992439ee3f230e19ffc445fac1261701f4df6a2010130a801f205ba01100a084e65787573203558100420fd887ac80102e0010ae80102f00102f801d4ffa0da02880204"
    # return ["",p14[:2]]
    return [
        "08d2a4808204100218e29b8eb706220431323333320a323134323834303535313a0634302e362e3342147630352e30322e30302d6f762d616e64726f696448c08090505208000000000000000060fceaa68c0d6a0632d478d616c37206758cb008f2d27a0a082a300238f4eaa68c0d8801fceaa68c0da201046e6f6e65a801f004ba011d0a07506978656c203610121a0a676f6f676c65706c617920808085c601c80102e0010ae80104f00108f8019aa4b8eb01880204",
        "23"]
    # return "08c4908082041002180222a00573294a551f425797735a9a0f05d4fc2de225c177d6ef7e0a7ee47644d93b72ad11d51dcd227ccc51454fbf8bfdbcfe736d9fdb7df507603bdbeb9180ccaf6f26c39aab1a678c640424ecde6d6564caa3e79acc5f05963e4120528987bfafd755cb4519b260fce0906d4397e7a7c3b3dd2e391c336cbbc1a7231802cd85c7e200d6cc1c0b5571bcb00e95cd25d5840fad5b28f47e648dfadc8023cfb921fba17e06d5c80c392da445136aefd7637733324d2acf91a322055f0396a0a63d787550d053fe2e4dc07c91b66f5721c6030e79f92229cc20e420b8132ceb850fb40d5fd69ec6ea5d8099f872aeb13004c7c17eb580dd024c0b90a9f99c2a4508dee184834bee1eb640bfa1fd627d2bbb3d158d47a086ef53b87265f82e199dec522b82b0df4048cfc1b31682c5b3d6b20dc7403e7596634181a17a4f6aa0d84971890aac1dabc890d66325ef26f5fcb4611a152b101a152a21bcacb4cdac95ca3ae47f5219f5bc0ce3c40606373985fd5081bba02ec9edbcf832820325f0dd8c6f101e9166410770f0e1bf3c7b26c0f3993f01517dc810d796118214821ebdc3bd3ef9427c00fbd33114d622a382aba28489b856a4c0bf3957d6511f568210ec031e2ca1418631f6334801d9571934694dc9408b50b2ac2af2aad309cd44154885f956d443bb2055f84ead4b9858f0c94ec87313f1a4bbec371f8fd65977ce2695ae191f3dbac5df39f6f60146b4fa8d1d9db583e300258fea647e96ccd8eee6588a715f45efd534cb41c0c604e70256fe222f6edaf5e47e1abca1ef9e21fb53747e0ce774493d3b12496df956a34472c97c48c7d26409411e3220742286524191a138a23395c45864969d6c708f059b0268bfa5bf4580debaed6deee891e1ef8c851dd7b31bd23a2f4e0654e0a7660b7370e16e04d028ead75ffc2e4b189e92fd753028cec79ecd9c66"


def make_argus_res1_aes3_and_key(
        sign_key: str = "wC8lD4bMTxmNVwY5jSkqi3QWmrphr/58ugLko7UZgWM="):  # 传入base64后字符串，这个值是固定的，但是unidbg和真机中的值不同
    '''
    生成后续n*72轮加密运算所需要的key
    '''
    part1_3 = from_base64(sign_key)
    # print(part1_3)
    # part2 = "9433cb42"
    # part2="1b54f67a"   # 这个是随机的，我们也可以直接选择固定
    part2 = to_fixed_hex(make_rand(),4)
    res1, res3 = part2[:4], part2[4:]
    for_sm3 = part1_3 + part2 + part1_3
    # print(for_sm3)
    sm3_res = sm3.sm3_hash(bytearray.fromhex(for_sm3))
    # 这里我们需要转换一下sm3_res的顺序,从大端序转换到小端序
    res = big_endianTo_little(sm3_res[:16]) + big_endianTo_little(sm3_res[16:32]) + big_endianTo_little(
        sm3_res[32:48]) + big_endianTo_little(sm3_res[48:64])
    return [res1, res3, res]  # 返回类型为16进制字符串，都是能直接用的


def make_argus_eor_data_key_list(k: list[str]):  # 负责生成第五轮及其之后的key
    s1, s2, s3, s4, s5 = int("f2101d113b815d60", 16), int("defe2eec47ea29f", 16), int("8db0dcd8e81a9b3e", 16), int(
        "724f232717e564c1", 16), int("c236b3c5fb929874", 16)
    for i in range(4, 75):
        k4, k2, k1 = int(k[i - 1], 16), int(k[i - 3], 16), int(k[i - 4], 16)
        tem1, tem2 = s1 & ror64(int_to_hexstr(k4), 3), s2 & lsr64(int_to_hexstr(k4), 3)
        tem3 = tem1 | tem2
        tem4 = k2 ^ tem3
        tem5 = 0xe000000000000000 ^ tem4
        tem6, tem7 = s3 & ror64(int_to_hexstr(tem5), 1), s4 & lsr64(int_to_hexstr(tem5), 1)
        tem8 = lsr64(int_to_hexstr(s5), (i - 4) if (i - 4 <= 0x3d) else ((i - 4) % 0x3d - 1))
        tem9 = k1 ^ tem5
        tem10 = tem6 | tem7
        num = tem8 & 1
        tem11 = tem9 ^ tem10
        tem12 = 0xfffffffffffffffd ^ num
        tem13 = tem11 ^ 0x9000000000000000
        tem14 = tem13 & tem12
        tem15 = tem11 | tem12
        k5 = (tem15 - tem14) & 0xFFFFFFFFFFFFFFFF
        k.append(int_to_hexstr(k5))
    return k


def make_argus_eor_data_round(p1: str, p2: str, k: str):  # 单轮的运算,传入的p1和p2都为大端序，可以直接用于计算
    p2_1, p2_2, p2_4 = ror64(p2, 0x38), ror64(p2, 0x3f), ror64(p2, 0x3e)
    p2_3 = p2_1 & p2_2
    tem1 = int(p1, 16) ^ p2_3
    tem2 = p2_4 ^ tem1
    p1, p2 = p2, int_to_hexstr(int(k, 16) ^ tem2)
    return [p1, p2]


def make_argus_eor_data(protobuf: str, key: str):  # 传入protobuf格式下的小端序16进制字符串(直接hexdump的结果)，以及初始的key
    '''这里要先对protobuf进行pkcs7填充'''
    protobuf = pad(bytearray.fromhex(protobuf), 16).hex()
    # print(len(protobuf))
    res = ""
    k = [key[:16], key[16:32], key[32:48], key[48:64]]
    key_list = make_argus_eor_data_key_list(k)
    # print(key_list)
    for i in range(len(protobuf) // 32):
        p1, p2 = little_endianTo_big(protobuf[i * 32:i * 32 + 16]), little_endianTo_big(
            protobuf[i * 32 + 16:i * 32 + 32])
        for j in range(72):
            p1, p2 = make_argus_eor_data_round(p1, p2, key_list[j])
        res += to_fixed_hex(p1) + to_fixed_hex(p2)
    return res


def make_argus_aes_data(eor1: str, eor2: str, aes3: str, p14_1: str):  # 都传入16进制字符串,其中eor1为待异或数据，eor2为四字节值
    '''
    这个函数是将上面n*72轮运算之后的结果与一个固定字符串进行异或 该字符串目前还不清楚来源
    这里异或时需要现将eor置换顺序,其实也不用,倒序遍历eor1就行了
    '''
    res = ""
    tem = ""
    # 这里的a6不清楚来源，只好先固定了
    # 第一个字节是0xec(手机)或者0xa6(unidbg), 不确定怎么来的, 步骤很多· 确定了 大概率是固定的
    res += "ec"  # 跟app的包名有关，com.zhiliaoapp.musically包名下都是ec ，com.ss.android.ugc.trill下是d6来着
    # res+="a6"                          #unidbg
    rand_str = "2b49d56d"
    rand_str = to_fixed_hex(make_rand(),4)
    # rand_str="201754f5"
    # 1808d001 下面的这个1808d001跟接口有关 接口=>sm3=>proto14=>运算=>结果 这里我们还是不固定了

    x18 = to_fixed_hex(int_to_hexstr(((((int(p14_1, 16) & 0x3f) << 0x2e) | 0x1800000000000000) | 0x100000000000 | 0x100000000) >> 32),4)
    # x18 = "18015401"
    # print("x18:", x18)
    # print((int(p14_1,16)&0x3f)<<0x2e)
    # print(x18)
    # x18="1808d001"

    res += little_endianTo_big(x18 + rand_str)
    # print("res",res)
    # print(eor1)
    for i in range(len(eor1) // 32 - 1, -1, -1):
        hex1 = int_to_hexstr(int(eor1[i * 32:i * 32 + 8], 16) ^ int(eor2, 16))
        hex2 = int_to_hexstr(int(eor1[i * 32 + 8:i * 32 + 16], 16) ^ int(eor2, 16))
        hex3 = int_to_hexstr(int(eor1[i * 32 + 16:i * 32 + 24], 16) ^ int(eor2, 16))
        hex4 = int_to_hexstr(int(eor1[i * 32 + 24:i * 32 + 32], 16) ^ int(eor2, 16))
        # tem+=hex1+hex2
        res += to_fixed_hex(hex3, 8) + to_fixed_hex(hex4, 8) + to_fixed_hex(hex1, 8) + to_fixed_hex(hex2, 8)
    # print(tem)
    res += eor2 + eor2
    res += aes3
    # res+="49279c70c20b2cb2c82080110d"   #这个的来源也是后续需要分析的，不过这个可能不重要，我也很困惑摸不着头脑 (这个是对随机分配的地址进行一定的位移操作得到的12字节)
    # 最后一个字节代表填充长度为0xd 我们这里可以直接固定写死，不过最好还是不要 因为不确定一个固定值是否会成为风控点
    # res+="49279C70C20B2CB2C82080110D"
    malloc_adr = random.randint(0x7b0c611111, 0x7b0c6fffff)
    # 填充模式为对一个malloc_addr进行位移操作拼接再加aes加密内容前八字节异或+0xd
    res += to_fixed_hex(int_to_hexstr((malloc_adr >> 0x16) & 0xff), 2) + to_fixed_hex(
        int_to_hexstr((malloc_adr >> 0x14) & 0xff), 2) + to_fixed_hex(int_to_hexstr((malloc_adr >> 0x12) & 0xff),
                                                                      2) + to_fixed_hex(
        int_to_hexstr((malloc_adr >> 0x10) & 0xff), 2) + to_fixed_hex(int_to_hexstr((malloc_adr >> 0xe) & 0xff),
                                                                      2) + to_fixed_hex(
        int_to_hexstr((malloc_adr >> 0xc) & 0xff), 2) + to_fixed_hex(int_to_hexstr((malloc_adr >> 0xa) & 0xff),
                                                                     2) + to_fixed_hex(
        int_to_hexstr((malloc_adr >> 0x8) & 0xff), 2) + to_fixed_hex(int_to_hexstr((malloc_adr >> 0x6) & 0xff),
                                                                     2) + to_fixed_hex(
        int_to_hexstr((malloc_adr >> 0x4) & 0xff), 2) + to_fixed_hex(int_to_hexstr((malloc_adr >> 0x2) & 0xff),
                                                                     2) + to_fixed_hex(int_to_hexstr(
        int(res[:2], 16) ^ int(res[2:4], 16) ^ int(res[4:6], 16) ^ int(res[6:8], 16) ^ int(res[8:10], 16) ^ int(
            res[10:12], 16) ^ int(res[12:14], 16) ^ int(res[14:16], 16)), 2) + "0d"
    # res += "d249279d75d4524a2bacb0620d"
    # res = "ec6dd5492b01540118ee1df07c6f7b86e2579bd03f2c40093692bd6d0769a8aeb493acd89767a0a90d13e79adf6e6ed1fa945c1672e1dfdfa301dbb489db4b7c516075be572fb20804225986de592b445ce0b3af66a9e8a99e77c9070e7dfc83103697c32d6ab629a8b7a7113a125825ff3cfaf4d7b626e67cf4002269232e3f4c029f24e177239ef363d71613188936ab48884fb3d94b920be75a9d0738ec017d56ea0892bad2c573440ead9f2f04c84d23b69e648076f501e3843b4343d9a08c26b5d395504a0a7e85113305d91e85bec852c1a6b7bba041adfb1f8a1155b5612ce1cf93210daf33c9173226653d2e2fca6b007a44958b07f8295faec862dc428ae2f796a773dd25b5f1fd47bd9fd716b5ab6c40ff65acbd0b45c99ffbe67195426cda8e50b8f61dbe4a634bd53b23c8acb1c918b0bb40132817ab78795cafefdf6a8ea25ca2f762f6a9c9c609fbb2d73ff1b829d79688b4173c402ddb9615ea87b9d8b3141863011e4acbdcca2537abb3e6de726de16c69e39a30eee63a9ef3e06a27dd415a35f843db0c57a6475ddff6b8625f329d8004629181beb432fb3dabd9763f5118b87792241b2425abfbef1fecef81f3f4cf9685e8095cb4514439fe9f6066660eb510248923c6da2384b6b6467c66ee022508f615a9fd7b0bae062b55e39c8f17b1c0b23aece82cf2d57eed7cc641a9721b203630dffc6d79c121a98555ff85aea002fff9a770fff9a370cb42d249279d75d4524a2bacb0620d"
    # print("decrypted_data",res)
    # print(res)

    return res


def make_argus_aes(data: bytearray, sign_key: str):
    # key和iv是根据前面的sign_key来的
    # key="D31E3718288A1027BAAB59F146A09A9C" #16进制字符串 ,注意要传入大端序     #这个是unidbg中的值
    # iv= "ea180a0336ed352fcd24e4d50018ae54" #16进制字符串 ，也是要传入大端序    # unidbg中的值
    key = "8252970d959b06db102e17d85c0ec1af"  # 真机中的key
    iv = "4d207ea37a419f7d622f81c6a2f53594"  # 真机中的iv
    hexKey = base64.b64decode(sign_key).hex()
    key = hashlib.md5(bytes.fromhex(hexKey[:32])).hexdigest()
    iv = hashlib.md5(bytes.fromhex(hexKey[32:64])).hexdigest()
    cipher = AES.new(bytearray.fromhex(key), AES.MODE_CBC, bytearray.fromhex(iv))  # 目前还不确定填充模式 （魔改填充，并非是标准填充模式）
    # if len(data)%16!=0:
    #     ciphertext = cipher.encrypt(pad(data, AES.block_size)).hex()
    # else:
    ciphertext = cipher.encrypt(data).hex()  # 这里压根就不需要填充 因为他的填充模式是通过一串运算得来的，我们加在前面了...
    # print(ciphertext)
    return ciphertext


def make_argus(protobuf: hex, p14_1: hex, sign_key: str = "wC8lD4bMTxmNVwY5jSkqi3QWmrphr/58ugLko7UZgWM="):
    # protobuf = "08d2a4808204100218c0f6f5e30a2204313233332a1337353530393638383835303331323930343233320a323134323834303535313a0634302e362e3342147630352e30322e30302d6f762d616e64726f696448c080905052080000000000000000608eead48e0d6a06d4aca568560572065b64be8942d77a0f08880228da0230d80138ace9d48e0d820119414d4a74514550364165537539636e6275586c79345958335188018eead48e0d920110c955dcf9aab6502223da8ed220bc4d569a01209e3572bec60977212a45743656719924251830e91aac9fe1c245b258b8183fbaa2010130a801f004ba011d0a07506978656c203610161a0a676f6f676c65706c617920808085c601c20184014d444769474a335672584546497a313079573030775958747a62523654334238567a6373394b6865536b62644c6b7941524759784646522f515575304e514b6d32656b424a674c47787861617651374f55504833556c4f546d32654b56754e524b6f686e6d685645367a52722f70635446722b786f5842444e484464363961624250453dc8010ad201100806120c0a67a20b858c0d0823232c99d2011708e00f1212354dc967f4146deed1c00f0b94fa9460ecaee001ee07e80190c01ff00106f801be8cb50582021c62f8a4323c5efd1a90fdc545002c905e7d0ee1bc6dd8e620877d2390880204"
    # sign_key="rBrarpWnr5SlEUqzs6l92ABQqgo5MUxAUoyuyVJWwow="  #unidbg
    sign_key = "wC8lD4bMTxmNVwY5jSkqi3QWmrphr/58ugLko7UZgWM="  # tt 真机
    # protobuf,p14_1=make_argus_protobuf()
    res1, res3, key = make_argus_res1_aes3_and_key()
    # print(res3)
    # print(" make argus eor data: key",key)
    eor1 = make_argus_eor_data(protobuf, key)
    # print(eor1)
    # eor2="fffd37f7"                       #我们暂且先将eor2当成固定值，这个的来源还没找,应该是可以固定的，unidbg跑的两份日志中都是这个内容
    # 这个并不是固定的 这个跟eor2跟之前的随机值有关系
    tem = int(res3[:2], 16)
    tem1 = int(res3[2:], 16)
    # print(tem)
    eor2 = int_to_hexstr((~((((tem << 0xb) | (tem1)) ^ (tem >> 5)) ^ tem | 0)) & 0xffffffff)
    # print("eor2:",eor2)
    aes_data = make_argus_aes_data(eor1, eor2, res3, p14_1)
    # data=bytearray.fromhex("a6f55417200124041899758339e7ae1086909e9fbc0792b2356b853930b99095e1b591fb27e136f947564600b837bd51871639cb0a78d22f66dc2ffcd7c3fc953d86974e1508b5c443b7aae80d6999e133bf9bd2360782f7c3353906a77da1d5da11148d2a355adb8542766d557d916c53e6ec9a5e988ca955f7d9f3bc9dac4548253909b18cfd486a6b9e3d4487b274c457e2f741a0777265e496d5abbd6aa4e5a31854a39b534014c4db65a217c92a8e968216802958ae3570578973a09d94bab0a0ef02c00cac41b3afd54d38f5705cfab8b0ea0985ef0cfffaa720fffaa720ab7149279c70c20b2cb2c82080110d")
    res2 = make_argus_aes(bytearray.fromhex(aes_data), sign_key)
    # print("finalHexStr: ",res2)
    for_base64 = res1 + res2
    argus = to_base64(bytearray.fromhex(for_base64))
    # print("x-argus  ===>", argus)
    return argus
# make_argus();
# make_argus("08d2a4808204100218ca8299ba0a2204313233332a1337353530393638383835303331323930343233320a323134323834303535313a0634302e362e3342147630352e30322e30302d6f762d616e64726f696448c08090505208000400000000000060ee90c98e0d6a06d4aca5685605720685e1ca67e8987a13089007100628ae0230d201389a8bc98e0d4006820119416b6c4666374768714657586d6f4d6e34546f565f682d75318801ee90c98e0d920110c955dcf9aab6502223da8ed220bc4d569a0120262eeeb0780749ac90d39cd2e5b206dec439c8a31427dfbe6f39808cfb92a566a2010130a801f004ba011d0a07506978656c203610161a0a676f6f676c65706c617920808085c601c20184014d444769474a335672584546497a313079577867776f44737a37496f46534d6d417a642f70766c65534575484c303647527a452f526c463951456e674e51576d32656b424a674847797843637541374f554b536b426757576b7a766256754d426434396e6d6b6353365451793863455246756e6d705845584e537a63756436625550453dc8010ad201100808120c3363822ac2b477eaa260f7d5d2011708e00f1212abed8b08141c64ecd4c00f0b94fa9460ecaee001ee07e80190c01ff00106f801d4b2c69c0982021c62f8a4323c5efd1a90f3b66b002c905e7f0ee2ea6dd8df20847d2390880204","85e1ca67e898")

