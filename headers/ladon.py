'''
主调用函数为make_ladon()，需要传入的参数为秒级时间戳
这个代码不知道哪里有问题，我们就不看了，直接用如画星球的版本的好了
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



def make_ladon_data_1Of1(aa:str,a0:str,i:int): #aa直接传8字节16进制小端序字符串,i表示当前轮数
    if i==0:
        return aa
    else:
        a0=int(a0,16)
        tem=(ror64(aa,8)+a0)&0xffffffffffffffff
        aa=tem^(i-1)
        a0=ror64(int_to_hexstr(a0),61)^aa
        return [int_to_hexstr(aa),int_to_hexstr(a0)] 
def make_ladon_data_2Of1(b0:str,b1:str): #小端序的b[1]
    b0=int(b0,16)
    tem=(b0+ror64(b1,8))&0xffffffffffffffff
    return int_to_hexstr(tem)
def make_ladon_data_1Of2(b0:str): #传入小端序的b[0]
    return int_to_hexstr(ror64(b0,0x3d))
def make_ladon_data(md5_res:str,time_sign:str): #传入大端序16进制字符串
    res=""
    a0,a1,a2,a3,b0,b1,b2,b3=big_endianTo_little(md5_res[:16]),big_endianTo_little(md5_res[16:32]),big_endianTo_little(md5_res[32:48]),big_endianTo_little(md5_res[48:64]),big_endianTo_little(time_sign[:16]),big_endianTo_little(time_sign[16:32]),big_endianTo_little(time_sign[32:48]),big_endianTo_little(time_sign[48:64])
    aa=[a1[::],a2[::],a3[::]]
    for i in range(34):
        if i!=0:
            cs=((i%3-1) if i%3!=0 else 2)
            aa[cs],a0=make_ladon_data_1Of1(aa[cs],a0,i)
        tem=make_ladon_data_2Of1(b0,b1)
        b1=to_fixed_hex(int_to_hexstr(int(a0,16)^int(tem,16)))
        b0=make_ladon_data_1Of2(b0)
        b0=to_fixed_hex(int_to_hexstr(int(b0,16)^int(b1,16)))
    # print(b0,b1)
    res+=little_endianTo_big(b0)+little_endianTo_big(b1)
    a0=big_endianTo_little(md5_res[:16])
    aa=[a1[::],a2[::],a3[::]]
    for i in range(34):
        if i!=0:
            cs=((i%3-1) if i%3!=0 else 2)
            aa[cs],a0=make_ladon_data_1Of1(aa[cs],a0,i)
        tem=make_ladon_data_2Of1(b2,b3)
        b3=to_fixed_hex(int_to_hexstr(int(a0,16)^int(tem,16)))
        b2=make_ladon_data_1Of2(b2)
        b2=to_fixed_hex(int_to_hexstr(int(b2,16)^int(b3,16)))
    # print("b2 is ===>",b2,"b3 is ===>",b3)
    res+=little_endianTo_big(b2)+little_endianTo_big(b3)

    return res #返回结果也是16进制字符串的形式 
def make_ladon(khronos:str="1758533246",aid:str="31323333"): #aid 来自于 uri，aid是固定的
    the_first_four=make_rand()
    # the_first_four="8d91c96c"
    md5_res=md5(bytearray.fromhex(the_first_four+aid)).encode().hex()
    # md5_res = md5(bytearray.fromhex(the_first_four+aid))
    # print(md5_res,md5(bytearray.fromhex(the_first_four+aid)).encode().hex())
    time_sign=(khronos+"-2142840551-1233").encode().hex()+"060606060606" #时间戳+lc_id+appID、最后填充至32字节，填充方式跟aes的pkcs7相同
    the_last_thirty_two=make_ladon_data(md5_res,time_sign)
    ladon=to_base64(bytearray.fromhex(the_first_four+the_last_thirty_two))
    # print("x-ladon  ===>",ladon)
    return ladon;
# for i in range(100):
#     print(i)
# print(make_ladon("1758533246","31323333"),len(make_ladon("1751607382","31323333")))
# print(len("hkCHELkbzhu0JJqr40cqwWMzXgF/3KkfuMLt6vLL/LiyD0xD"))