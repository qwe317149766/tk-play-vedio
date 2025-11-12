'''
主调用函数为make_gorgon()，需要传入的内容为khronos、query_string，x-ss-stub
'''
import hashlib,time,datetime,base64,os,random
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from gmssl import sm3, func

def int_to_hexstr(num:int):
    """将数字转换为16进制字符串并去除最前面的0x"""
    return hex(num)[2:]
def ror64(value:str, shift:int):
    """8字节16进制字符串循环右移"""
    value=int(value,16)
    shift %= 64
    return ((value >> shift) | (value << (64 - shift))) & 0xFFFFFFFFFFFFFFFF
def lsl64(value:str, shift:int):
    value=int(value,16)
    """8字节16进制字符串左移"""
    return (value << shift) & 0xFFFFFFFFFFFFFFFF
def lsr64(value:str, shift:int):
    """8字节16进制字符串右移"""
    value=int(value,16)
    return (value >> shift) & 0xFFFFFFFFFFFFFFFF
def big_endianTo_little(hex_str:str): #将16进制字符串由大端序转换为小端序
    num = int.from_bytes(bytes.fromhex(hex_str), byteorder='big')  # 先按大端序解析
    little_endian_hex = num.to_bytes(8, byteorder='little').hex()  # 再按小端序转换
    return little_endian_hex
def little_endianTo_big(hex_str:str): #将16进制字符串由小端序转换为大端序
    num = int.from_bytes(bytes.fromhex(hex_str), byteorder='little')  # 先按小端序解析
    big_endian_hex = num.to_bytes(8, byteorder='big').hex()  # 再按大端序转换
    return big_endian_hex
def to_base64(data:bytearray):
    # data = b"Hello world!"
    encoded = base64.b64encode(data)
    encoded_str = encoded.decode('utf-8')
    return encoded_str
def from_base64(data:str):  #解码后返回其16进制字符串的形式
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
def make_rand():          #生成一个四字节16进制随机数
    return os.urandom(4).hex() 
def to_fixed_hex(value,shift=16):
    return value.zfill(shift)
def make_gorgon_rc4_init(key:str):
    Sbox = list(range(256))
    key = list(bytes.fromhex(key))
    prev_index = 0
    i = 0
    def get_key_byte(v87, idx):
        if 0 <= idx < len(v87):
            return v87[idx]
        else:
            return 0
    while i < 256:
        j = i + 7 if i < 0 else i
        key_offset = i - (j & 0xFFFFFFF8)
        k = get_key_byte(key, key_offset)
        b = Sbox[i]
        inner = 2 * (prev_index | b) - (prev_index ^ b)
        v47 = 2 * (inner | k) - (inner ^ k)
        temp2 = v47 if v47 >= 0 else v47 + 255
        j = v47 - (temp2 & 0xFFFFFF00)
        j = j % 256
        Sbox[i] = Sbox[j]
        i += 1
        prev_index = j
    # print(bytearray(Sbox).hex())
    return Sbox
def make_gorgon_rc4(data: bytearray, key_len: int,Sbox:list):
    Sbox = bytearray(Sbox) # 初始化状态表 v78
    v55 = 0
    v56 = 0
    v57 = 0
    while True:
        v59 = (v56 + 1) & 0xFF
        temp = (v55 ^ Sbox[v59]) + 2 * (v55 & Sbox[v59])
        v62 = temp & 0xFF
        v63 = Sbox[v62]
        Sbox[v59] = v63
        Sbox[v62] = v63
        index = (Sbox[v59] | v63) + (Sbox[v59] & v63)
        index &= 0xFF 
        # print(hex(v57),hex(data[v57]))
        # print(index,hex(Sbox[index]))
        data[v57] ^= Sbox[index]
        # print(hex(data[v57]))
        v57 = 2 * (v57 & 1) + (v57 ^ 1)
        v55 = v62
        v56 = v59
        if v57 >= key_len:
            break
        if v57 == 0:
            v55 = 0
            v56 = 0
            v57 = 0
    return data
def make_gorgon_last(data:bytearray):
    res=""
    for i in range(len(data)):
        data[i]=((data[i]>>4 | data[i]<<4)^(data[i+1] if i!=len(data)-1 else data[0]))&0xff
        tem1=((data[i]<<1)&0xffaa)
        tem2=((data[i]>>1)&0x55)
        tem3=tem1|tem2
        tem4=((tem3<<2)&0xffffcf)|((tem3>>2)&0x33)
        tem5=(tem4>>4)&0xf
        mask = (1 << 28) - 1  # 0x0FFFFFFF
        lsb = 4
        ans= (tem5 & ~(((1 << 28) - 1) << lsb)) | ((tem4 & mask) << lsb)
        # print(hex(ans))
        ans=(ans^0xffffffeb)&0xff
        data[i]=ans
        # print(hex(ans))
        res+=hex(ans).split("0x")[1]
    return res
def make_gorgon(khronos:str="1751607382",query_string:str="",key:str="4a0016a8476c0080",x_ss_stub:str="0000000000000000000000000000000",sdk_version ="0000000020020205"): #key除80、a8之外其他都是固定的，x_ss_stub来自请求体，
    # 这里的query_string指的是query_string
    # query_string="device_platform=android&os=android&ssmix=a&_rticket=1751607382364&channel=googleplay&aid=1180&app_name=trill&version_code=400603&version_name=40.6.3&manifest_version_code=400603&update_version_code=400603&ab_version=40.6.3&resolution=1080*2219&dpi=420&device_type=SM-C9000&device_brand=samsung&language=zh&os_api=33&os_version=13&ac=wifi&is_pad=0&current_region=CN&app_type=normal&sys_region=US&last_install_time=1751521608&mcc_mnc=46001&timezone_name=Asia%2FShanghai&carrier_region_v2=460&residence=US&app_language=zh-Hans&carrier_region=US&timezone_offset=28800&host_abi=arm64-v8a&locale=zh-Hans&content_language=en%2Czh-Hans%2C&ac2=wifi5g&uoo=0&op_region=US&build_number=40.6.3&region=US&ts=1751607262&iid=7522681282913011469&device_id=7522680299320641079"
    key = "4a0016a8476c0080" 
    Sbox=make_gorgon_rc4_init(key)
    # data=md5(query_string)[0:8]+x_ss_stub[:8]+"0000000020000205"+hex(int(khronos)).split("0x")[1]
    data = md5(query_string)[0:8] + x_ss_stub[:8] + sdk_version + hex(int(khronos)).split("0x")[1]
    # data=md5(query_string)[0:8]+x_ss_stub[:8]+"0000000020000205"+"68676856"
    # print(data)
    data=bytearray.fromhex(data)
    keylen=len(data)
    data=make_gorgon_rc4(data,keylen,Sbox)
    # print(data.hex())
    last_twnty=make_gorgon_last(data)
    res="840480a80000"+last_twnty
    # print("X-Gorgon ===>",res)
    return "840480a80000"+last_twnty  #80 a8和key中的两个字节相对应，两个都是随机取的