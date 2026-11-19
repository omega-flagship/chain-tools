"""Host-side signed deploy transport for the F1R3node uint256 HTTP API.

Install the deploy extra only in the host. Minds invoke policy-checked Hive
app actions and never receive the signing key or a generic signing skill.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.request

from chain_observer import ChainError, _NoRedirects, _observer_url


def _varint(number):
    data = bytearray()
    while number >= 128:
        data.append(128 | (number & 127))
        number >>= 7
    data.append(number)
    return bytes(data)


def sign(term, key, timestamp, block, shard, limit, price):
    from coincurve import PrivateKey

    # DeployDataProto fields in canonical order, omitting scalar defaults.
    # uint256 phloPrice is the 32-byte unsigned field 15.
    fields = [(2, term.encode()), (3, timestamp), (8, limit), (10, block),
              (11, shard.encode()), (15, price.to_bytes(32, "big"))]
    wire = bytearray()
    for index, value in fields:
        if not value:
            continue
        if isinstance(value, bytes):
            wire.extend(_varint(index * 8 + 2) + _varint(len(value)) + value)
        else:
            wire.extend(_varint(index * 8) + _varint(value))
    private = PrivateKey(bytes.fromhex(key))
    digest = hashlib.blake2b(wire, digest_size=32).digest()
    return {"data": {"term": term, "timestamp": timestamp, "phloLimit": limit,
                     "phloPrice": str(price), "shardId": shard, "validAfterBlockNumber": block},
            "sigAlgorithm": "secp256k1", "deployer": private.public_key.format(False).hex(),
            "signature": private.sign(digest, hasher=None).hex()}


def decode(expr):
    (tag, value), = expr.items()
    data = value["data"]
    if tag in ("ExprString", "ExprUri", "ExprBool"):
        return data
    if tag in ("ExprInt", "ExprBigInt"):
        return int(data)
    if tag in ("ExprTuple", "ExprList"):
        return [decode(item) for item in data]
    if tag == "ExprMap":
        return {json.loads(k) if k.startswith('"') else k: decode(v) for k, v in data.items()}
    raise ValueError(f"Unsupported Rholang result {tag}")


class Deployer:
    def __init__(self):
        self.base = _observer_url(os.getenv("CHAIN_DEPLOY_URL", "http://127.0.0.1:40413"))
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirects())
        self.shard = os.getenv("CHAIN_SHARD_ID", "root")
        self.network = os.getenv("CHAIN_NETWORK_ID", "testnet")

    def request(self, path, payload=None):
        body = None if payload is None else json.dumps(payload).encode()
        req = urllib.request.Request(self.base + "/api/" + path, data=body,
                                     headers={"Content-Type": "application/json"})
        try:
            with self.http.open(req, timeout=10) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            with exc:
                reason = exc.read(1000).decode(errors="replace")
            raise ChainError("node_error", f"{path}: HTTP {exc.code} {reason}", http_status=exc.code) from None

    def prepare(self, term):
        status = self.request("status")
        if (status.get("networkId") != self.network or status.get("shardId") != self.shard
                or status.get("isReady") is not True or status.get("isReadOnly") is not False):
            raise ChainError("wrong_chain", "The deploy endpoint must be a ready writer on the configured network and shard")
        key_path = os.getenv("CHAIN_DEPLOY_KEY_FILE")
        if not key_path:
            raise ValueError("Set CHAIN_DEPLOY_KEY_FILE on the Hive host")
        key = Path(key_path).read_text().strip()
        if not re.fullmatch("[0-9a-fA-F]{64}", key):
            raise ValueError("Signer file must contain one 32-byte hex private key")
        latest = self.request("last-finalized-block?view=summary")
        block = latest.get("blockInfo", latest)["blockNumber"]
        return sign(term, key, time.time_ns() // 1_000_000, block, self.shard,
                    5_000_000, int(status["minPhloPrice"]))

    def submit(self, signed):
        return self.request("deploy", signed)

    def receipt(self, deploy_id):
        if not re.fullmatch("[0-9a-f]{16,144}", deploy_id):
            raise ValueError("Invalid deploy signature")
        status = self.request("deploy-finalization-status/" + deploy_id)
        state = status["state"]
        if state in ("Failed", "Expired", "Rejected"):
            return {"state": "refused", "id": deploy_id, "message": state}
        if state != "Finalized":
            return {"state": "pending", "id": deploy_id, "message": state}
        detail = self.request("deploy/" + deploy_id)
        if detail.get("errored") or detail.get("systemDeployError"):
            return {"state": "refused", "id": deploy_id, "message": "Deploy execution failed"}
        result = self.request("data-at-name-by-block-hash", {
            "name": {"UnforgDeploy": {"data": deploy_id}}, "blockHash": detail["blockHash"],
            "usePreStateHash": False})
        values = [decode(item) for item in result["expr"]]
        if len(values) != 1:
            return {"state": "unknown", "id": deploy_id, "message": "Finalized without one contract result"}
        return {"state": "committed", "id": deploy_id, "value": values[0]}
