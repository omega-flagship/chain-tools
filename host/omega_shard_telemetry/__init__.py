"""Recent, read-only Casper telemetry for a local Docker shard."""

import asyncio
import hashlib
import http.client
import json
import re
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit

from chain_observer import ChainError, ObserverClient


WINDOW = 60
LOG_LINES = 1800
MAX_LINE = 64 * 1024
NODE = re.compile(r"rnode\.(?:bootstrap|validator\d+|readonly\d*)\Z")
HASH = r"([a-fA-F0-9]{8,64})(?:\.\.\.)?"
PROPOSE = re.compile(r"Propose finished: Success\(.+?\) Block #(\d+) \(" + HASH + r"\) created and added\.")
INVALID = re.compile(r"Block " + HASH + r" validated Left\(Invalid\((\w+).*\)\)\.")
EXCEPTION = re.compile(r"Block " + HASH + r" raised BlockException .*; recording as InvalidTransaction\.")
SENT = re.compile(r"Received request for block " + HASH + r" from (rnode://\S+)\. Response sent\.")
RECEIVED = re.compile(r"Incoming BlockMessage #(\d+) \(" + HASH + r"\) from ([\w.-]+)")
REFUSED = re.compile(r"Refused BlockMessage " + HASH + r" from (rnode://\S+): (Malformed|HashMismatch)")
VALID = re.compile(r"Block " + HASH + r" validated Right\(Valid\)\.")
DEPLOY_START = re.compile(r"Received DeployData #-?\d+ -- ")
DEPLOY_END = re.compile(r" Sig: ([0-9a-f]{20}\.\.\.[0-9a-f]{20}) SigAlgorithm: [\w-]+ ValidAfterBlockNumber: -?\d+\Z")
KINDS = ("proposal", "confirmation", "rejection", "deploy")
EXTERNAL = "external"



def hostname(address):
    try:
        return urlsplit(address or "").hostname or ""
    except ValueError:
        return ""


def members(status):
    observer = hostname(status.get("address"))
    peers = {p["host"] for p in status.get("peerList", [])
             if isinstance(p, dict) and p.get("isConnected") is True and isinstance(p.get("host"), str)}
    if observer:
        peers.add(observer)
    return [{"id": name, "role": "Observer" if name == observer else
             "Bootstrap" if name == "rnode.bootstrap" else
             "Validator" if re.fullmatch(r"rnode\.validator\d+", name) else "Peer"}
            for name in sorted(peers)]


@dataclass(frozen=True)
class Observation:
    micros: int
    kind: str
    node: str
    ref: str
    peer: str = ""
    height: int | None = None
    reason: str = ""


def parse_event(record, node, peers):
    message = record.get("message")
    target = record.get("target")
    if not isinstance(message, str):
        return None
    peer, height, reason = "", None, ""
    if target == "node::rust::instances::proposer_instance" and (m := PROPOSE.fullmatch(message)):
        kind, ref, height = "created", m[2], int(m[1])
    elif target == "casper::rust::engine::running":
        if m := SENT.fullmatch(message):
            kind, ref, peer = "sent", m[1], hostname(m[2])
        elif m := RECEIVED.fullmatch(message):
            kind, ref, peer, height = "received", m[2], m[3], int(m[1])
        elif m := REFUSED.fullmatch(message):
            kind, ref, peer, reason = "refused", m[1], hostname(m[2]), m[3]
        else:
            return None
        if peer == node or peer not in peers:
            return None
    elif target == "casper::rust::blocks::ingest::validation_worker":
        if m := VALID.fullmatch(message):
            kind, ref = "confirmation", m[1]
        elif m := INVALID.fullmatch(message):
            kind, ref, reason = "rejection", m[1], m[2]
        elif m := EXCEPTION.fullmatch(message):
            kind, ref, reason = "rejection", m[1], "InvalidTransaction"
        else:
            return None
    elif (target == "casper::rust::engine::multi_parent_casper::block_admission"
          and DEPLOY_START.match(message) and (m := DEPLOY_END.search(message))):
        kind, ref = "deploy", m[1]
    else:
        return None
    try:
        stamp = datetime.fromisoformat(record["timestamp"])
        if stamp.tzinfo is None:
            return None
        micros = round(stamp.timestamp() * 1_000_000)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    return Observation(micros, kind, node, ref.lower(), peer, height, reason)


def transfer(obs, src, dst, height=None):
    key = f"{obs.kind}|{obs.micros}|{src}|{dst}|{obs.ref}"
    deploy = obs.kind == "deploy"
    return {"id": hashlib.sha256(key.encode()).hexdigest()[:24], "at": obs.micros // 1000,
            "type": "deploy" if deploy else None, "from": src, "to": dst,
            "deploy" if deploy else "block": obs.ref, "height": height,
            "proposed": False, "decidedAt": None,
            "detail": "Signed deploy admitted" if deploy else "Block transfer"}


def set_verdict(event, obs):
    if event["decidedAt"] is not None:
        return
    event["type"] = "confirmation" if obs.kind == "confirmation" else "rejection"
    event["decidedAt"] = obs.micros // 1000
    event["detail"] = "Validated by receiver" if obs.kind == "confirmation" else f"Rejected by receiver · {obs.reason}"


def classify(observations):
    """Join a receiver's verdict only to a witnessed transfer of that block to that node."""
    origins = {(o.node, o.ref): o.height for o in observations if o.kind == "created"}
    inbound = defaultdict(list)
    received = defaultdict(list)
    events = []
    # A send precedes a receipt, then a verdict, even if their clocks round to the same microsecond.
    order = {"sent": 0, "received": 1, "refused": 1}
    for o in sorted(observations, key=lambda o: (o.micros, order.get(o.kind, 2), o.node, o.peer, o.kind)):
        if o.kind == "created":
            continue
        if o.kind == "deploy":
            events.append(transfer(o, EXTERNAL, o.node))
            continue
        if o.kind in ("confirmation", "rejection"):
            key = (o.node, o.ref)
            candidates = received.get(key) or inbound.get(key, [])
            if len(candidates) == 1:
                set_verdict(candidates[0], o)
            continue

        src, dst = (o.node, o.peer) if o.kind == "sent" else (o.peer, o.node)
        key = (dst, o.ref)
        # A DEBUG incoming record and the sender's INFO response describe the same block copy.
        candidates = [e for e in inbound[key] if e["from"] == src and e not in received[key]]
        if o.kind != "sent" and len(candidates) == 1:
            event = candidates[0]
        else:
            height = origins.get((src, o.ref))
            event = transfer(o, src, dst, height if height is not None else o.height)
            if height is not None:
                event.update(type="proposal", proposed=True, detail="Proposed block sent to peer")
            inbound[key].append(event)
            events.append(event)
        if o.kind != "sent":
            received[key].append(event)
        if o.kind == "refused":
            set_verdict(event, o)
    return [e for e in events if e["type"] is not None]


async def read_node(node, since, peers):
    if not NODE.fullmatch(node):
        return [], "No local container mapping"
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "logs", "--since", datetime.fromtimestamp(since, timezone.utc).isoformat(),
            "--tail", str(LOG_LINES), node, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    except OSError:
        return [], "Docker unavailable"
    events, count = [], 0
    try:
        async with asyncio.timeout(4):
            # Docker may include very large deploy payloads. Discard oversized lines incrementally.
            pending, discard = b"", False
            while chunk := await proc.stdout.read(MAX_LINE):
                parts = chunk.split(b"\n")
                for i, part in enumerate(parts):
                    if not discard:
                        pending += part
                        if len(pending) > MAX_LINE:
                            discard, pending = True, b""
                    if i == len(parts) - 1:
                        break
                    count += 1
                    if pending.startswith(b"{"):
                        try:
                            record = json.loads(pending)
                            event = parse_event(record, node, peers) if isinstance(record, dict) else None
                            if event:
                                events.append(event)
                        except (ValueError, UnicodeError):
                            pass
                    pending, discard = b"", False
            if await proc.wait():
                return [], "Container logs unavailable"
    except TimeoutError:
        return [], "Log read timed out"
    finally:
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            # Drain the pipe after cancellation so wait() cannot deadlock on a full buffer.
            await proc.communicate()
    return events, "Log window truncated" if count >= LOG_LINES else None


def queue_accepted(node):
    """Read the intake counter from this local validator's published queue port."""
    match = re.fullmatch(r"rnode\.validator(\d+)", node)
    if not match:
        return None
    try:
        ports = subprocess.run(
            ["docker", "port", "rnode.deploy-queue" + match[1], "40403/tcp"],
            capture_output=True, text=True, check=True, timeout=2,
        ).stdout.splitlines()
        # The standard shard publishes to loopback, or to all interfaces on CI.
        for binding in ports:
            host, _, port = binding.rpartition(":")
            local = {"127.0.0.1": "127.0.0.1", "0.0.0.0": "127.0.0.1", "[::1]": "::1", "[::]": "::1"}.get(host)
            if local and port.isdecimal():
                break
        else:
            return None
        conn = http.client.HTTPConnection(local, int(port), timeout=1.5)
        try:
            conn.request("GET", "/queue/metrics")
            response = conn.getresponse()
            if response.status != 200:
                return None
            body = response.read(MAX_LINE + 1)
        finally:
            conn.close()
    except (OSError, ValueError, subprocess.SubprocessError, http.client.HTTPException):
        return None
    if len(body) > MAX_LINE:
        return None
    # The Rust exporter creates the accepted series on its first increment.
    if not re.search(rb"^deploy_queue_connected [01]$", body, re.M):
        return None
    for line in body.splitlines():
        if line.startswith(b"deploy_queue_accepted "):
            value = line.removeprefix(b"deploy_queue_accepted ")
            return int(value) if value.isdigit() and len(value) <= 20 else None
    return 0


class ShardFeed:
    """Share one bounded rolling sample among concurrent viewers on the same event loop."""

    def __init__(self):
        self.lock = asyncio.Lock()
        self.cached = None
        self.next_poll = 0
        self.history = set()
        self.cursors = {}

    async def snapshot(self):
        async with self.lock:
            if self.cached is not None and time.monotonic() < self.next_poll:
                return self.cached
            self.cached = await self._sample()
            self.next_poll = time.monotonic() + 1.5
            return self.cached

    async def _sample(self):
        now = time.time()
        start = now - WINDOW
        data = {"at": int(now * 1000), "window": WINDOW, "state": "offline", "nodes": [], "events": [],
                "counts": dict.fromkeys(KINDS, 0), "queueAccepted": None, "tip": None, "finalized": None}
        try:
            client = ObserverClient.from_env()
            client.timeout = min(client.timeout, 3)
            data.update(network=client.network_id, shard=client.shard_id)
            status = await asyncio.to_thread(client.status)
        except ChainError as exc:
            self.history.clear()
            self.cursors.clear()
            return {**data, "note": str(exc)}

        nodes = members(status)
        peers = {n["id"] for n in nodes}
        self.cursors = {k: v for k, v in self.cursors.items() if k in peers}
        validators = [n["id"] for n in nodes if n["role"] == "Validator"]
        results, totals = await asyncio.gather(
            asyncio.gather(*(read_node(n["id"], max(start, self.cursors.get(n["id"], start) - 1), peers)
                             for n in nodes)),
            asyncio.gather(*(asyncio.to_thread(queue_accepted, node) for node in validators)),
        )
        if totals and None not in totals:
            data["queueAccepted"] = sum(totals)
        for node, (events, problem) in zip(nodes, results):
            node["issue"] = problem
            if not problem or problem == "Log window truncated":
                self.cursors[node["id"]] = now
            self.history.update(events)
        # Keep context just beyond the public window so an overlapping poll can resolve a late verdict.
        self.history = {o for o in self.history if o.micros >= (start - 30) * 1_000_000}
        active = [e for e in classify(self.history) if (e["decidedAt"] or e["at"]) >= start * 1000]
        ordered = sorted(active, key=lambda e: (e["decidedAt"] or e["at"], e["id"]))
        for event in ordered:
            if event["proposed"] and event["at"] >= start * 1000:
                data["counts"]["proposal"] += 1
            if event["type"] != "proposal":
                data["counts"][event["type"]] += 1
        return {**data, "state": "partial" if not nodes or any(n["issue"] for n in nodes) else "live",
                "nodes": nodes, "events": ordered[-240:], "tip": status.get("latestBlockNumber"),
                "finalized": status.get("lastFinalizedBlockNumber")}
