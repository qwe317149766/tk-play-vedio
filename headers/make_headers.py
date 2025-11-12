'''
生成argus、ladon、gorgon、khronos、x-ss-stub
'''
import random,time,hashlib
import secrets
from gmssl import sm3,func
from headers import argus,gorgon,ladon
from tt_protobuf import make_argus_pb
from headers.argus_hex26_2 import make_hex26_2
from headers.make_hex26_1 import make_hex26_1

def make_headers( deviceID: str,  create_time: int
                      , signCount: int, reportCount: int, settingCount: int,
                      appLaunchTime: int, secDeviceToken: hex,
                      phoneInfo: str, seed: str, seed_encode_type: int, seed_endcode_hex: hex,algorithmData1:hex,
                hex_32: hex,
                query_string:str,
                post_data:hex, # 这个post_data用16进制好了，因为毕竟post的有protobuf 类型的东西
                appVersion: str = "42.4.3" ,sdkVersionStr: str="v05.02.02-ov-android",sdkVersion: int=0x05020220, callType: int=738
                  , appVersionConstant: int= 0xC40A800):
    x_ss_stub = hashlib.md5(bytearray.fromhex(post_data if post_data!="" else "0000000000000000")).hexdigest()

    # print("x-ss-stub ===>",x_ss_stub)
    p13 = sm3.sm3_hash(bytearray.fromhex(x_ss_stub))
    bodyhash = p13[:12]
    p14 = sm3.sm3_hash(func.bytes_to_list(query_string.encode("utf8")))
    queryHash = p14[:12]
    pskHash = "c955dcf9aab6502223da8ed220bc4d56" # 这个值也没有校验
    pskHash = ""
    pskCalHash = sm3.sm3_hash(func.bytes_to_list((bytes.fromhex(query_string.encode("utf8").hex()+x_ss_stub+"30"))))
    # pskHash = ""  # 这个值也没有校验
    # pskCalHash = ""
    rand_26 = secrets.token_bytes(4).hex()
    rand_26 = "3A7CFFCC"
    if seed_encode_type !="":
        seed_endcode_hex = make_hex26_1(seed_encode_type,query_string,x_ss_stub,rand_26)
        # seed_endcode_hex = "0a67a20b858c0d0823232c99"
        algorithmData1 = make_hex26_2(p14,p13)
        hex_32 = "62f8a4323c5efd1a90fdc545002c905e7d0ee1bc6dd8e620877d2390" # 暂时先固定一下
        hex_32 = "62f8a4323c5efd1a90f3b66b002c905e7f0ee2ea6dd8df20847d2390"  # hex_32 也不校验
        hex_32 = "645ff7b92c8eb3dbbbd1a58875880f64265fc8194d"
        hex_32 = "62f8a4323c5efd1a9fe6aa050027805e4b0ee1fc6de46a018d"
        hex_32 = ""
    x_khronos = create_time
    x_ss_stub = x_ss_stub.upper()
    x_argus_protobuf = make_argus_pb.make_one_argus_pb(deviceID, appVersion, sdkVersionStr, sdkVersion, x_khronos,
                      bodyhash, queryHash, signCount, reportCount, settingCount,
                      appLaunchTime, secDeviceToken, pskHash, pskCalHash, callType,
                      phoneInfo, appVersionConstant, seed, seed_encode_type, seed_endcode_hex,
                      algorithmData1, hex_32,rand_26)
    # print(x_argus_protobuf)
    x_argus = argus.make_argus(x_argus_protobuf,queryHash)
    x_ladon = ladon.make_ladon(str(x_khronos))
    x_gorgon = gorgon.make_gorgon(khronos=str(x_khronos),query_string=query_string,x_ss_stub=x_ss_stub,sdk_version ="0000000020020205")
    return [x_ss_stub,x_khronos,x_argus,x_ladon,x_gorgon]