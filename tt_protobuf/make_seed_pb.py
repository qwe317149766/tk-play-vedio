from tt_protobuf import tk_pb2

def make_seed_encrypt(session_id:str,device_id:str,os:str="android",sdk_version:str ="v05.02.00"):
    seed_encrypt = tk_pb2.SeedEncrypt()
    seed_encrypt.session = session_id
    seed_encrypt.deviceid = device_id
    seed_encrypt.os = os
    seed_encrypt.sdk_version = sdk_version
    serialized_data = seed_encrypt.SerializeToString()
    return serialized_data.hex()
    pass
def make_seed_request(seed_encrypt:hex, utime:int):
    seed_request = tk_pb2.SeedRequest()
    seed_request.s1 = 538969122<<1
    seed_request.s2 = 2
    seed_request.s3 = 4
    seed_request.encrypt = bytes.fromhex(seed_encrypt)
    seed_request.utime = utime <<1
    serialized_data = seed_request.SerializeToString()
    return serialized_data.hex()
    pass
def make_seed_decrypt(decrypt:hex):
    seed_decrypt = tk_pb2.SeedDecrypt()
    seed_decrypt.ParseFromString(bytes.fromhex(decrypt))
    return seed_decrypt
    pass
def make_seed_response(response:hex):
    seed_response = tk_pb2.SeedResponse()
    seed_response.ParseFromString(bytes.fromhex(response))
    return seed_response