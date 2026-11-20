import asyncio
from dataclasses import replace
from datetime import datetime, timezone
import unittest
from unittest.mock import AsyncMock, Mock, patch

from chain_observer import ChainError
from omega_shard_telemetry import ShardFeed, classify, members, parse_event, queue_accepted

A, B, C = "rnode.validator1", "rnode.readonly", "rnode.validator2"
PEERS = {A, B, C}
BLOCK = "b1493b9f49"
SIG = "3045022100c7ae159839...2bc3456789abcdef0123"
VALIDATE = "casper::rust::blocks::ingest::validation_worker"
NETWORK = "casper::rust::engine::running"
INTAKE = "casper::rust::engine::multi_parent_casper::block_admission"
STATUS = {"address": "rnode://self@" + B, "peerList": [{"host": A, "isConnected": True}],
          "latestBlockNumber": 42, "lastFinalizedBlockNumber": 40}


def observation(second, node, target, message, **fields):
    record = {"timestamp": f"2026-11-20T10:25:{second:02d}.000001Z",
              "target": target, "message": message, **fields}
    return parse_event(record, node, PEERS)


def proposal():
    return observation(0, A, "node::rust::instances::proposer_instance",
                       f"Propose finished: Success(ProposeSuccess {{ result: Valid }}) Block #42 ({BLOCK}...) created and added.")


def sent(src=A, dst=B, second=1):
    return observation(second, src, NETWORK, f"Received request for block {BLOCK}... from rnode://abc@{dst}?protocol=40400. Response sent.")


def decision(result="Right(Valid)", node=B, second=3):
    return observation(second, node, VALIDATE, f"Block {BLOCK}... validated {result}.")


class TransferTests(unittest.TestCase):
    def test_proposal_and_confirmation_are_one_directed_transfer(self):
        self.assertEqual(classify([proposal()]), [])
        blue, = classify([proposal(), sent()])
        green, = classify({decision(), sent(), proposal(), sent()})
        self.assertEqual((blue["type"], green["type"]), ("proposal", "confirmation"))
        self.assertEqual(blue["id"], green["id"])
        self.assertEqual((green["from"], green["to"], green["height"], green["block"]), (A, B, 42, BLOCK))
        self.assertTrue(green["proposed"])
        self.assertGreater(green["decidedAt"], green["at"])
        self.assertEqual(classify([decision()]), [])

    def test_debug_receipt_resolves_the_actual_sender(self):
        received = observation(2, B, NETWORK, f"Incoming BlockMessage #42 ({BLOCK}...) from {C}")
        events = classify([proposal(), sent(), sent(src=C), received, decision()])
        self.assertEqual([(e["from"], e["to"], e["type"]) for e in events],
                         [(A, B, "proposal"), (C, B, "confirmation")])
        inbound, = classify([received, decision()])
        self.assertEqual((inbound["from"], inbound["to"], inbound["height"]), (C, B, 42))
        self.assertFalse(inbound["proposed"])

    def test_unrelated_verdicts_do_not_color_a_transfer(self):
        for other in (decision(node=A), decision(second=0), replace(decision(), ref="a" * 10)):
            event, = classify([proposal(), sent(), other])
            self.assertEqual(event["type"], "proposal")
        self.assertEqual(classify([sent(), sent(src=C), decision()]), [])
        self.assertIsNone(observation(2, A, "f1r3fly.casper.finalization_lifecycle", "finalization lifecycle",
                                      event="candidate_finalized", candidate_hash=BLOCK + "a" * 54))

    def test_rejections_require_a_receiver_verdict_or_explicit_peer_refusal(self):
        for local in (decision("Left(Invalid(InvalidBlockNumber))"), observation(3, B, VALIDATE,
                      f"Block {BLOCK}... raised BlockException (error); recording as InvalidTransaction.")):
            rejected, = classify([sent(), local])
            self.assertEqual((rejected["type"], rejected["from"], rejected["to"]), ("rejection", A, B))
        refused = observation(3, B, NETWORK, f"Refused BlockMessage {BLOCK}... from rnode://abc@{A}?protocol=40400: HashMismatch")
        for history in ([refused], [sent(), refused]):
            event, = classify(history)
            self.assertEqual((event["type"], event["from"], event["to"]), ("rejection", A, B))
            self.assertIn("HashMismatch", event["detail"])
        for verdict in ("Left(Undecidable(foo))", "Left(AwaitingState(root))", "Left(BlockException(error))"):
            self.assertIsNone(decision(verdict))

    def test_request_without_block_and_hash_only_announcements_do_not_fly(self):
        for line in (f"Received request for block {BLOCK}... from rnode://abc@{B}. No response given since block not found.",
                     f"Incoming HasBlock {BLOCK}... from {B}", f"Ignoring BlockHashMessage {BLOCK}... from {B}"):
            self.assertIsNone(observation(1, A, NETWORK, line))
        self.assertIsNone(sent(src=A, dst=A))
        self.assertIsNone(sent(src=A, dst="unknown"))

    def test_external_deploy_exposes_only_the_receipt_reference(self):
        term = "private multi-line term\n Sig: 01234567890123456789...01234567890123456789 SigAlgorithm: arbitrary ValidAfterBlockNumber: 99"
        message = f"Received DeployData #123 -- {term} Sig: {SIG} SigAlgorithm: secp256k1 ValidAfterBlockNumber: 42"
        receipt = observation(2, A, INTAKE, message)
        event, = classify([receipt])
        self.assertEqual((event["type"], event["from"], event["to"], event["deploy"]), ("deploy", "external", A, SIG))
        self.assertNotIn("block", event)
        self.assertNotIn("private", repr(receipt) + repr(event))
        self.assertIsNone(observation(2, A, NETWORK, message))
        self.assertIsNone(observation(2, A, INTAKE, "Received DeployData #123 -- just a term"))
        self.assertIsNone(observation(2, A, INTAKE, "deploy lifecycle", event="carrier_created", deploy_sig=SIG))

    def test_topology_contains_only_connected_network_nodes(self):
        status = {**STATUS, "peerList": STATUS["peerList"] + [{"host": "gone", "isConnected": False}]}
        self.assertEqual({n["id"] for n in members(status)}, {A, B})


class QueueTests(unittest.TestCase):
    def test_cumulative_acceptances_and_missing_series_on_a_new_queue(self):
        with patch("omega_shard_telemetry.subprocess.run") as port, patch("http.client.HTTPConnection") as http:
            port.return_value.stdout = "0.0.0.0:14413\n[::]:14413\n"
            response = http.return_value.getresponse.return_value
            response.status = 200
            for metrics, expected in [
                (b"deploy_queue_connected 1\ndeploy_queue_accepted 1235\ndeploy_queue_ready 3\n", 1235),
                (b"deploy_queue_connected 0\ndeploy_queue_refused{reason=\"expired\"} 9\n", 0),
                (b"deploy_queue_connected 1\ndeploy_queue_accepted unknown\n", None),
                (b"not queue metrics", None),
            ]:
                response.read.return_value = metrics
                self.assertEqual(queue_accepted(A), expected)
            http.assert_called_with("127.0.0.1", 14413, timeout=1.5)
            http.return_value.request.assert_called_with("GET", "/queue/metrics")
            response.status = 503
            self.assertIsNone(queue_accepted(A))


class SnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_overlapping_polls_update_the_same_transfer_and_recover(self):
        t = int(datetime.now(timezone.utc).timestamp() * 1_000_000) - 1_000_000
        origin, wire, verdict = [replace(obs, micros=t + i) for i, obs in enumerate((proposal(), sent(), decision()))]
        records = {A: [origin, wire], B: []}
        client = Mock(timeout=1, network_id="testnet", shard_id="root")
        client.status.return_value = STATUS
        feed = ShardFeed()
        with patch("omega_shard_telemetry.ObserverClient.from_env", return_value=client), patch(
            "omega_shard_telemetry.read_node", new=AsyncMock(side_effect=lambda node, *args: (records[node], None))
        ) as logs, patch("omega_shard_telemetry.queue_accepted", return_value=72) as intake:
            first, other = await asyncio.gather(feed.snapshot(), feed.snapshot())
            self.assertIs(first, other)
            self.assertEqual(first["queueAccepted"], 72)
            intake.assert_called_once_with(A)
            self.assertEqual(logs.await_count, 2)
            self.assertEqual(first["events"][0]["type"], "proposal")
            feed.next_poll = 0
            intake.return_value = None
            records[B] = [verdict]
            second = await feed.snapshot()
            self.assertIsNone(second["queueAccepted"])
            self.assertEqual(second["events"][0]["id"], first["events"][0]["id"])
            self.assertEqual(second["counts"], {"proposal": 1, "confirmation": 1, "rejection": 0, "deploy": 0})
            feed.next_poll = 0
            logs.side_effect = lambda *args: ([], "Container logs unavailable")
            self.assertEqual((await feed.snapshot())["state"], "partial")
            feed.next_poll = 0
            client.status.side_effect = ChainError("connection_error", "offline")
            offline = await feed.snapshot()
            self.assertEqual((offline["state"], offline["events"]), ("offline", []))
            feed.next_poll = 0
            client.status.side_effect = None
            logs.side_effect = lambda *args: ([], None)
            self.assertEqual((await feed.snapshot())["events"], [])
