import base64
import hashlib
import json
from dataclasses import dataclass
from typing import Tuple, Optional
import time
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.backends import default_backend


CURVE = ec.SECP256R1()  # 和 OpenSSL 里的 NID_X9_62_prime256v1 = 415 一致


@dataclass
class DeltaKeyPair:
    priv_hex: str                    # 32 字节私钥的 hex（等价于 Delta.generatePrivateKey 输出）
    tt_public_key_b64: str           # tt-ticket-guard-public-key
    sk: ec.EllipticCurvePrivateKey   # cryptography 的私钥对象


def generate_delta_keypair() -> DeltaKeyPair:
    """
    模拟 Delta.generatePrivateKey + initPrivateKey + getPublicKeyUncompressed
    生成一对 (私钥 hex, 公钥 tt-ticket-guard-public-key, 私钥对象)
    """
    # 1) 生成 EC 私钥
    sk = ec.generate_private_key(CURVE, default_backend())

    # 2) 私钥整数 → 32 字节 big-endian → hex
    d_int = sk.private_numbers().private_value
    priv_bytes = d_int.to_bytes(32, "big")
    priv_hex = priv_bytes.hex()

    # 3) 公钥未压缩点：04 || X(32) || Y(32)
    pk = sk.public_key()
    numbers = pk.public_numbers()
    x = numbers.x.to_bytes(32, "big")
    y = numbers.y.to_bytes(32, "big")
    uncompressed = b"\x04" + x + y

    # 4) 一次 Base64 → tt-ticket-guard-public-key
    tt_public_key_b64 = base64.b64encode(uncompressed).decode()

    return DeltaKeyPair(
        priv_hex=priv_hex,
        tt_public_key_b64=tt_public_key_b64,
        sk=sk,
    )


def load_keypair_from_priv_hex(priv_hex: str) -> DeltaKeyPair:
    """
    已经有 Frida 抓到的私钥 hex 的情况：还原私钥对象 + 算 tt-ticket-guard-public-key
    """
    priv_bytes = bytes.fromhex(priv_hex)
    d_int = int.from_bytes(priv_bytes, "big")
    sk = ec.derive_private_key(d_int, CURVE, default_backend())

    pk = sk.public_key()
    numbers = pk.public_numbers()
    x = numbers.x.to_bytes(32, "big")
    y = numbers.y.to_bytes(32, "big")
    uncompressed = b"\x04" + x + y
    tt_public_key_b64 = base64.b64encode(uncompressed).decode()

    return DeltaKeyPair(
        priv_hex=priv_hex,
        tt_public_key_b64=tt_public_key_b64,
        sk=sk,
    )


def delta_sign(unsigned: str, sk: ec.EllipticCurvePrivateKey) -> str:
    """
    模拟 C 里的 sub_3154 + Java DeltaSignerVerifier.LIZIZ：
    digest = SHA256(unsigned)  →  ECDSA_sign(digest)  → DER  → Base64
    """
    # 1) 和 C 层一样：先手动 SHA256
    digest = hashlib.sha256(unsigned.encode("utf-8")).digest()

    # 2) 用 Prehashed(SHA256) 做 ECDSA
    der_sig = sk.sign(
        digest,
        ec.ECDSA(utils.Prehashed(hashes.SHA256())),
    )

    # 3) Java 里是 Base64.encodeToString(...)
    return base64.b64encode(der_sig).decode()


def build_guard(

    device_guard_data0="",  # 如果是设备相关的话，直接传服务器返回的这个内容
    cookie:dict={}, # 如果是ticket相关的话，直接传cookie进来
    path: str = "/aweme/v1/aweme/stats/",
    timestamp: Optional[int] = None,
    priv_hex: Optional[str] = None,
    is_ticket=False,
) -> Tuple[dict, DeltaKeyPair, str]:
    """
    构造：
      - tt-ticket-guard-public-key 头
      - dreq_sign（或者 reeSign）
      - 返回 keypair 方便你持久化
    """
    if not is_ticket:

        if timestamp is None:
            timestamp = int(time.time())

        # 1) 拿 keypair：优先用你给的 priv_hex，否则就现生成一对
        if priv_hex and priv_hex != "...":
            kp = load_keypair_from_priv_hex(priv_hex)
        else:
            kp = generate_delta_keypair() 
        device_token = device_guard_data0["device_token"]

        # 2) unsigned 字符串 —— 注意这里要和真实请求完全一样
        unsigned = f"device_token={device_token}&path={path}&timestamp={timestamp}"

        # 3) 签名
        dreq_sign_b64 = delta_sign(unsigned, kp.sk)
        device_guard_data1 = {}
        device_guard_data1["device_token"] = device_guard_data0["device_token"]
        device_guard_data1["timestamp"] = timestamp
        device_guard_data1["req_content"] = "device_token,path,timestamp"
        device_guard_data1["dtoken_sign"] = device_guard_data0["dtoken_sign"]
        device_guard_data1["dreq_sign"] = dreq_sign_b64
        device_guard_data1 = json.dumps(device_guard_data1, separators=(',', ':'))
        tt_device_guard_client_data = base64.b64encode(device_guard_data1.encode("utf-8")).decode()
        # 4) 你要塞进 header 的东西（示例）
        headers = {
            "tt-device-guard-iteration-version": "1",
            "tt-ticket-guard-public-key": kp.tt_public_key_b64,
            # device_guard 用的是 dreq_sign / dtoken_sign 这种字段名
            # "X-Dreq-Sign": dreq_sign_b64,    # 名字你可以按自己服务改
            'tt-ticket-guard-version': '3',
            "tt-device-guard-client-data":tt_device_guard_client_data
        }
        return headers

    else:
        x_tt_token = cookie["X-Tt-Token"]
        ts_sign = cookie["ts_sign_ree"]
        unsigned = f"{x_tt_token}&path={path}&timestamp={int(time.time())}"
        if priv_hex and priv_hex != "...":
            kp = load_keypair_from_priv_hex(priv_hex)
        else:
            kp = generate_delta_keypair()
        req_sign = delta_sign(unsigned, kp.sk)
        ticket_guard_data = {}
        ticket_guard_data["req_content"] = "ticket,path,timestamp"
        ticket_guard_data["req_sign"] = req_sign
        ticket_guard_data["timestamp"] = timestamp
        ticket_guard_data["ts_sign"] = ts_sign
        ticket_guard_data = json.dumps(ticket_guard_data, separators=(',', ':'))
        tt_ticket_guard_client_data = base64.b64encode(ticket_guard_data.encode("utf-8")).decode()

        headers = {
            "tt-ticket-guard-client-data": tt_ticket_guard_client_data,
            "tt-ticket-guard-iteration-version": "0",
            "tt-ticket-guard-public-key": kp.tt_public_key_b64,
            "tt-ticket-guard-version": "3",
            # "tt-ticket-guard-web-version": "1"
        }
        return headers


# if __name__ == "__main__":
#     # 假设这是服务端回来的 device_token（你自己换成真实的）
#     device_token_example = '1|{"aid":1233,"av":"42.4.3","did":"7570...","iid":"7570...","fit":"1762624604","s":1,"idc":"useast8","ts":"1762625541"}'
#
#     headers, kp, unsigned = build_guard(device_token_example)
#
#     print("priv_hex (保存起来复用):", kp.priv_hex)
#     print("tt-ticket-guard-public-key:", headers["tt-ticket-guard-public-key"])
#     print("unsigned:", unsigned)
#     print("dreq_sign (Base64 DER):", headers["X-Dreq-Sign"])
