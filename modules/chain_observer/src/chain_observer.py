"""Read-only observer HTTP client, CLI, and OmegaHive skill adapter."""

from __future__ import annotations

import argparse
import base64
import binascii
import http.client
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_OBSERVER = "http://127.0.0.1:40453"
MAX_TERM_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
BLOCK_HASH = re.compile(r"[0-9a-fA-F]{64}\Z")
READ_ROUTES = frozenset({
    ("GET", "/api/status"),
    ("GET", "/api/last-finalized-block?view=summary"),
    ("POST", "/api/explore-deploy-by-block-hash"),
})


class ChainError(Exception):
    def __init__(self, code, message, *, http_status=None, retry_after=None):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.retry_after = retry_after

    def as_dict(self):
        error = {"code": self.code, "message": str(self)}
        if self.http_status is not None:
            error["http_status"] = self.http_status
        if self.retry_after is not None:
            error["retry_after"] = self.retry_after
        return {"ok": False, "error": error}


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _observer_url(value):
    try:
        parts = urllib.parse.urlsplit(value)
        host = parts.hostname
        loopback = host == "localhost" or ipaddress.ip_address(host).is_loopback
        valid_port = parts.port is None or 1 <= parts.port <= 65535
        if (parts.scheme != "http" or not loopback or not valid_port
                or parts.username is not None or parts.password is not None
                or parts.path not in ("", "/", "/api", "/api/")
                or parts.query or parts.fragment
                or any(c.isspace() for c in value)):
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise ChainError("invalid_observer", "Observer must be a local HTTP URL, e.g. http://127.0.0.1:40453") from None
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def _block_hash(value):
    if not isinstance(value, str) or not BLOCK_HASH.fullmatch(value):
        raise ChainError("invalid_block_hash", "Use a full 64-character hexadecimal block hash")
    return value.lower()


class ObserverClient:
    def __init__(self, observer=DEFAULT_OBSERVER, *, timeout=30, shard_id="root", network_id="testnet"):
        self.observer = _observer_url(observer)
        try:
            self.timeout = float(timeout)
            if not math.isfinite(self.timeout) or not 0 < self.timeout <= 120:
                raise ValueError()
        except (TypeError, ValueError):
            raise ChainError("invalid_timeout", "Timeout must be greater than zero and at most 120 seconds") from None
        if not isinstance(shard_id, str) or not shard_id or not isinstance(network_id, str) or not network_id:
            raise ChainError("invalid_identity", "Expected shard and network IDs must be nonempty strings")
        self.shard_id, self.network_id = shard_id, network_id
        # Local node traffic must not inherit proxy settings or follow redirects.
        self._http = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirects())

    @classmethod
    def from_env(cls):
        return cls(os.environ.get("CHAIN_OBSERVER_URL", DEFAULT_OBSERVER),
                   timeout=os.environ.get("CHAIN_QUERY_TIMEOUT", "30"),
                   shard_id=os.environ.get("CHAIN_SHARD_ID", "root"),
                   network_id=os.environ.get("CHAIN_NETWORK_ID", "testnet"))

    def _request(self, method, path, payload=None):
        if (method, path) not in READ_ROUTES:
            raise ChainError("route_refused", "Only the observer status, finalized-block and exploratory-query routes are allowed")
        data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(self.observer + path, data=data, method=method,
                                         headers={"Accept": "application/json", "Content-Type": "application/json"})
        try:
            with self._http.open(request, timeout=self.timeout) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            with exc:
                raw = exc.read(8192)
                try:
                    body = json.loads(raw)
                except (ValueError, UnicodeError):
                    body = {}
                if not isinstance(body, dict):
                    body = {}
                code = body.get("error", "http_error")
                if not isinstance(code, str):
                    code = "http_error"
                message = body.get("message", f"Observer returned HTTP {exc.code}")
                raise ChainError(code, str(message)[:2000], http_status=exc.code,
                                 retry_after=exc.headers.get("Retry-After")) from None
        except (TimeoutError, socket.timeout):
            raise ChainError("request_timeout", "Observer request timed out; it was not retried") from None
        except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
            raise ChainError("connection_error", f"Cannot reach observer: {exc.reason if isinstance(exc, urllib.error.URLError) else exc}") from None
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ChainError("response_too_large", "Observer response exceeds 2 MiB")
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            raise ChainError("invalid_response", "Observer did not return valid JSON") from None
        if not isinstance(value, dict):
            raise ChainError("invalid_response", "Expected a JSON object from the observer")
        if "error" in value:
            raise ChainError("observer_error", str(value.get("message", value["error"]))[:2000])
        return value

    def status(self):
        status = self._request("GET", "/api/status")
        if status.get("isReadOnly") is not True or status.get("isValidator") is not False:
            raise ChainError("observer_required", "Endpoint must explicitly report a read-only, non-validator node")
        if status.get("isReady") is not True:
            raise ChainError("observer_not_ready", "Observer is not ready")
        if status.get("shardId") != self.shard_id or status.get("networkId") != self.network_id:
            raise ChainError("identity_mismatch", "Observer shard/network differs from the configured identity")
        return status

    def _finalized_block(self):
        response = self._request("GET", "/api/last-finalized-block?view=summary")
        block = response.get("blockInfo", response)
        if not isinstance(block, dict):
            raise ChainError("invalid_response", "Finalized block is not an object")
        _block_hash(block.get("blockHash"))
        if block.get("shardId") != self.shard_id or block.get("isFinalized") is not True:
            raise ChainError("invalid_response", "Expected a finalized block from the configured shard")
        return block

    def finalized_block(self):
        self.status()
        return self._finalized_block()

    def query(self, term, *, block_hash=None, pre_state=False):
        if not isinstance(term, str) or not term.strip():
            raise ChainError("invalid_term", "Provide a nonempty Rholang program")
        try:
            term_bytes = term.encode("utf-8")
        except UnicodeError:
            raise ChainError("invalid_term", "Rholang source must be valid UTF-8") from None
        if len(term_bytes) > MAX_TERM_BYTES or "\0" in term:
            raise ChainError("invalid_term", "Rholang source must be at most 64 KiB and contain no NUL bytes")
        if type(pre_state) is not bool:
            raise ChainError("invalid_state", "pre_state must be a boolean")
        if pre_state and block_hash is None:
            raise ChainError("block_required", "A pre-state query requires an explicit block hash")
        if block_hash is not None:
            block_hash = _block_hash(block_hash)
        self.status()
        selection = "explicit" if block_hash is not None else "last-finalized"
        if block_hash is None:
            block_hash = _block_hash(self._finalized_block()["blockHash"])
        response = self._request("POST", "/api/explore-deploy-by-block-hash", {
            "term": term, "blockHash": block_hash, "usePreStateHash": pre_state,
        })
        block = response.get("block")
        if (not isinstance(response.get("expr"), list) or not isinstance(block, dict)
                or block.get("blockHash") != block_hash or block.get("shardId") != self.shard_id
                or type(response.get("cost")) is not int or response["cost"] < 0):
            raise ChainError("invalid_response", "Query response must contain expressions, cost and the exact requested block")
        return {"observer": self.observer, "networkId": self.network_id, "shardId": self.shard_id,
                "readOnly": True, "selection": selection, "state": "pre" if pre_state else "post",
                "expr": response["expr"], "block": block, "cost": response["cost"]}


def _hive_call(action, *args):
    try:
        client = ObserverClient.from_env()
        if action == "query-at":
            result = client.query(args[1], block_hash=args[0])
        else:
            result = getattr(client, action)(*args)
        return json.dumps({"ok": True, "result": result}, ensure_ascii=False)
    except ChainError as exc:
        return json.dumps(exc.as_dict(), ensure_ascii=False)


def observer_status():
    return _hive_call("status")


def observer_finalized_block():
    return _hive_call("finalized_block")


def query_state(source):
    return _hive_call("query", source)


def query_state_at(block_hash, source):
    return _hive_call("query-at", block_hash, source)


def _query_encoded(payload, block_hash=None):
    try:
        if not isinstance(payload, str) or len(payload) > 4 * ((MAX_TERM_BYTES + 2) // 3):
            raise ValueError()
        source = base64.b64decode(payload, validate=True).decode("utf-8")
    except (ValueError, UnicodeError, binascii.Error):
        return json.dumps(ChainError("invalid_source_encoding", "Expected base64-encoded UTF-8 Rholang, at most 64 KiB decoded").as_dict())
    return query_state(source) if block_hash is None else query_state_at(block_hash, source)


def query_state_base64(payload):
    return _query_encoded(payload)


def query_state_at_base64(block_hash, payload):
    return _query_encoded(payload, block_hash)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observer", default=os.environ.get("CHAIN_OBSERVER_URL", DEFAULT_OBSERVER))
    parser.add_argument("--timeout", default=os.environ.get("CHAIN_QUERY_TIMEOUT", "30"))
    parser.add_argument("--shard", default=os.environ.get("CHAIN_SHARD_ID", "root"))
    parser.add_argument("--network", default=os.environ.get("CHAIN_NETWORK_ID", "testnet"))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="Check the observer's readiness and identity")
    sub.add_parser("finalized-block", help="Read the last finalized block")
    query = sub.add_parser("query", help="Execute Rholang against an observer snapshot")
    source = query.add_mutually_exclusive_group(required=True)
    source.add_argument("--file", type=Path, help="UTF-8 Rholang file; use - for stdin")
    source.add_argument("--term", help="Rholang source text")
    query.add_argument("--block", help="Full block hash; default: last finalized block")
    query.add_argument("--pre-state", action="store_true", help="Use the explicit block's pre-state")
    args = parser.parse_args(argv)
    try:
        client = ObserverClient(args.observer, timeout=args.timeout, shard_id=args.shard, network_id=args.network)
        if args.command == "status":
            result = client.status()
        elif args.command == "finalized-block":
            result = client.finalized_block()
        else:
            term = args.term
            if args.file is not None:
                if str(args.file) == "-":
                    term = sys.stdin.read(MAX_TERM_BYTES + 1)
                else:
                    with args.file.open(encoding="utf-8", newline="") as stream:
                        term = stream.read(MAX_TERM_BYTES + 1)
            result = client.query(term, block_hash=args.block, pre_state=args.pre_state)
        print(json.dumps({"ok": True, "result": result}, ensure_ascii=False, indent=2))
        return 0
    except (OSError, UnicodeError) as exc:
        error = ChainError("source_error", str(exc))
    except ChainError as exc:
        error = exc
    print(json.dumps(error.as_dict(), ensure_ascii=False), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

