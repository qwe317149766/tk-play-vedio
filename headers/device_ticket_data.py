#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import json
import hashlib
from ecdsa import SigningKey, SECP256k1
from ecdsa.util import sigencode_der

def encode_payload(payload: dict) -> str:
    """
    Convert dict to JSON string and encode to Base64
    """
    json_str = json.dumps(payload, ensure_ascii=False)
    return base64.b64encode(json_str.encode("utf-8")).decode("ascii")

def decode_payload(b64_str: str) -> dict:
    """
    Decode Base64 string back to JSON dict
    """
    raw = base64.b64decode(b64_str, validate=True).decode("utf-8")
    return json.loads(raw)

def sign_data(data: str, sk: SigningKey, with_prefix: bool = False) -> str:
    """
    Sign arbitrary string using ECDSA and return signature.
    If with_prefix=True, prepend 'ts.1.' to the Base64 signature.
    """
    digest = hashlib.sha256(data.encode("utf-8")).digest()
    der_sig = sk.sign_digest_deterministic(
        digest,
        sigencode=sigencode_der,
        hashfunc=hashlib.sha256
    )
    b64_sig = base64.b64encode(der_sig).decode("ascii")
    return f"ts.1.{b64_sig}" if with_prefix else b64_sig

def make_device_ticket_data(device,stime,path="/aweme/v1/aweme/stats/"):
    sk = SigningKey.generate(curve=SECP256k1)

    device_id = device["device_id"]
    install_id = device["install_id"]
    apk_first_install_time = device['apk_first_install_time']
    ts = stime -60

    # Original JSON payload (all fields are modifiable)
    payload = {
        "device_token": f"1|{{\"aid\":1233,\"av\":\"42.4.3\",\"did\":\"{device_id}\","
                        f"\"iid\":\"{install_id}\",\"fit\":\"{apk_first_install_time}\",\"s\":0.8,"
                        "\"idc\":\"my\",\"ts\":\"{ts}\"}}",
        "timestamp": stime,
        "req_content": "device_token,path,timestamp",
        "path": path
    }

    # Generate signatures
    payload["dtoken_sign"] = sign_data(payload["device_token"], sk, with_prefix=True)
    canonical_req = f"{payload['device_token']}|{payload['path']}|{payload['timestamp']}"
    payload["dreq_sign"] = sign_data(canonical_req, sk, with_prefix=False)

    # Print original JSON with signatures
    # print("== Original JSON with signatures ==")
    # print(json.dumps(payload, ensure_ascii=False, indent=2))

    # Encode to Base64
    encoded = encode_payload(payload)
    # print("\n== Encoded Base64 ==")
    # print(encoded)
    return encoded
    # Decode back to JSON
    # decoded = decode_payload(encoded)
    # print("\n== Decoded JSON ==")
    # print(json.dumps(decoded, ensure_ascii=False, indent=2))
    #
    # # Modify a field and re-encode
    # decoded["timestamp"] = 9999999999  # Example modification
    # decoded["dtoken_sign"] = sign_data(decoded["device_token"], sk, with_prefix=True)
    # canonical_req = f"{decoded['device_token']}|{decoded['path']}|{decoded['timestamp']}"
    # decoded["dreq_sign"] = sign_data(canonical_req, sk, with_prefix=False)
    # new_encoded = encode_payload(decoded)
    # print("\n== Modified Base64 ==")
    # print(new_encoded)
def main():
    # Generate signing key (for demo; replace with fixed key if needed)
    sk = SigningKey.generate(curve=SECP256k1)

    # Original JSON payload (all fields are modifiable)
    payload = {
        "device_token": "1|{\"aid\":1233,\"av\":\"42.4.4\",\"did\":\"7569671398152095249\","
                        "\"iid\":\"7569672269414090497\",\"fit\":\"1762451702\",\"s\":0.8,"
                        "\"idc\":\"my\",\"ts\":\"1762506132\"}",
        "timestamp": 1762506266,
        "req_content": "device_token,path,timestamp",
        "path": "/aweme/v1/aweme/stats/"
    }

    # Generate signatures
    payload["dtoken_sign"] = sign_data(payload["device_token"], sk, with_prefix=True)
    canonical_req = f"{payload['device_token']}|{payload['path']}|{payload['timestamp']}"
    payload["dreq_sign"] = sign_data(canonical_req, sk, with_prefix=False)

    # Print original JSON with signatures
    print("== Original JSON with signatures ==")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    # Encode to Base64
    encoded = encode_payload(payload)
    print("\n== Encoded Base64 ==")
    print(encoded)

    # Decode back to JSON
    decoded = decode_payload(encoded)
    print("\n== Decoded JSON ==")
    print(json.dumps(decoded, ensure_ascii=False, indent=2))

    # Modify a field and re-encode
    decoded["timestamp"] = 9999999999  # Example modification
    decoded["dtoken_sign"] = sign_data(decoded["device_token"], sk, with_prefix=True)
    canonical_req = f"{decoded['device_token']}|{decoded['path']}|{decoded['timestamp']}"
    decoded["dreq_sign"] = sign_data(canonical_req, sk, with_prefix=False)
    new_encoded = encode_payload(decoded)
    print("\n== Modified Base64 ==")
    print(new_encoded)

# if __name__ == "__main__":
#     main()
