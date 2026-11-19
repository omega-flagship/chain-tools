"""Host-side F1R3node serialization and finalization regressions."""
import importlib.util
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "host"), str(ROOT / "modules/chain_observer/src")]
from omega_chain_deploy import Deployer, sign


class DeployTests(unittest.TestCase):
    @unittest.skipUnless(importlib.util.find_spec("coincurve"), "install the deploy extra")
    def test_matches_independent_uint256_deploy_signer(self):
        signed = sign("new out in { out!(42) }", "1".zfill(64), 123456789, 7, "root", 5_000_000, 1)
        self.assertEqual(signed["data"]["phloPrice"], "1")
        self.assertEqual(signed["signature"],
                         "304402200a560a7d42b9cc8a8d4778e76a4563d97bcbdcb2a87ffb294a47acdafcbde7b0"
                         "02204ecbd57571180fa2f5734c3d716140297f78e13effde8c7cce7bdd231dbbcf03")

    def test_receipts_require_canonical_finalization_and_one_result(self):
        writer = Deployer()
        sig = "ab" * 70
        for state, want in [("Pending", "pending"), ("Rejected", "refused"), ("Expired", "refused")]:
            with patch.object(writer, "request", return_value={"state": state}):
                self.assertEqual(writer.receipt(sig)["state"], want)
        replies = [{"state": "Finalized"}, {"blockHash": "ba" * 32, "errored": False}, {"expr": []}]
        with patch.object(writer, "request", side_effect=replies):
            self.assertEqual(writer.receipt(sig)["state"], "unknown")
