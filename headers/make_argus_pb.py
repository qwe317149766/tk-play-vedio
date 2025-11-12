import  random, time, os
import tk_pb2

def make_one_argus_pb(deviceID: str, appVersion: str, sdkVersionStr: str, sdkVersion: int, create_time: int,
                      bodyhash: hex, queryHash: hex, signCount: int, reportCount: int, settingCount: int,
                      appLaunchTime: int, secDeviceToken: hex, pskHash: hex, pskCalHash: hex, callType: int,
                      phoneInfo: str, appVersionConstant: int, seed: str, seed_encode_type: int, seed_endcode_hex: hex,
                      algorithmData1: hex, hex_32: hex):
    # 创建一个Argus结构体
    argus_msg = tk_pb2.Argus()

    argus_msg.magic = 0x20200929 << 1  # 0x20200929 << 1  固定值
    argus_msg.version = 2
    argus_msg.rand = int.from_bytes(os.urandom(4))  # 随机数
    argus_msg.rand = 2893904704
    #argus_msg.rand = 2222222222
    argus_msg.msAppID = "1233"  # 固定值
    if deviceID !="":
        argus_msg.deviceID = deviceID  # "7522680299320641079"  # 这里先固定，来源于device_register
    argus_msg.licenseID = "2142840551"  # 固定值，所有版本都相同
    argus_msg.appVersion = appVersion  # "40.6.3"             #
    argus_msg.sdkVersionStr = sdkVersionStr  # "v05.02.00-ov-android"
    argus_msg.sdkVersion = sdkVersion << 1  # 0x5020020 << 1
    argus_msg.envCode = bytes.fromhex("0000000000000000")
    argus_msg.createTime = create_time << 1
    argus_msg.bodyHash = bytes.fromhex(bodyhash)
    argus_msg.queryHash = bytes.fromhex(queryHash)

    action_record = argus_msg.actionRecord
    # action_record.signCount = signCount << 1
    # action_record.reportSuccessCount = reportCount << 1
    # # action_record.settingCount = settingCount << 1
    # action_record.actionIncremental = 210
    # action_record.appLaunchTime = appLaunchTime << 1
    action_record.signCount = 272
    action_record.reportSuccessCount = 262
    # action_record.settingCount = settingCount << 1
    action_record.actionIncremental = 186
    action_record.appLaunchTime = appLaunchTime << 1

    argus_msg.secDeviceToken = secDeviceToken
    argus_msg.isAppLicense = create_time << 1
    if pskHash!="":
        argus_msg.pskHash = bytes.fromhex("5b9dbbb114e05c886c574bd2a3d6257c")  # 这个值也没有校验，随便了
    if pskCalHash!="":
        argus_msg.pskCalHash = bytes.fromhex(pskCalHash)
    argus_msg.pskVersion = "0"
    # argus_msg.callType = callType
    argus_msg.callType = 738  # 这个值也无所谓,固定好了
    channelinfo = argus_msg.channelInfo
    channelinfo.phoneInfo = phoneInfo
    channelinfo.metasecConstant = 18 # 没有校验...
    channelinfo.channel = "googleplay"
    channelinfo.appVersionConstant = appVersionConstant << 1

    if seed!="":
        argus_msg.seed = seed
    # argus_msg.extType = random.randint(1, 6) << 1
    argus_msg.extType = 10
    if seed_encode_type!="":
        extra_info1 = argus_msg.extraInfo.add()
        extra_info1.algorithm = seed_encode_type<<1
        extra_info1.algorithmData = bytes.fromhex(seed_endcode_hex)

        extra_info2 = argus_msg.extraInfo.add()
        extra_info2.algorithm = 2016
        extra_info2.algorithmData = bytes.fromhex(algorithmData1)

    argus_msg.unknown28 = 1006
    argus_msg.unknown29 = 516112
    # argus_msg.unknown29 = 516112
    argus_msg.unknown30 = 6
    argus_msg.unknown31 = 11355710
    # argus_msg.unknown31 = 22222222 # 这个值无所谓
    if hex_32!="":
        argus_msg.unknown32 = bytes.fromhex(hex_32)
    argus_msg.unknown33 = 4

    serialized_data = argus_msg.SerializeToString()

    return serialized_data.hex()