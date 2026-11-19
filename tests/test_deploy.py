"""Host-side F1R3node serialization and finalization regressions."""
import importlib.util
import os
import sys
import tempfile
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

    @unittest.skipUnless(importlib.util.find_spec("omega_chain_wallet"), "requires the optional host wallet")
    def test_wallet_and_file_sign_the_same_deploy(self):
        from omega_chain_wallet import Wallet

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = {"OMEGA_CHAIN_WALLET_DIR": str(root / "vault"), "OMEGA_CHAIN_WALLET_CONFIG_DIR": str(root / "config"),
                   "CHAIN_DEPLOY_KEY_FILE": "", "CHAIN_DEPLOY_PUBLIC_KEY": ""}
            with patch.dict(os.environ, env):
                wallet = Wallet.from_env()
                wallet.initialize()
                key = os.urandom(32)
                public = wallet.import_secret(key)
                writer = Deployer()
                status = {"networkId": writer.network, "shardId": writer.shard, "isReady": True,
                          "isReadOnly": False, "minPhloPrice": "1"}
                finalized = {"blockInfo": {"blockNumber": 7}}
                with patch.object(writer, "request", side_effect=[status, finalized] * 2), \
                        patch("omega_chain_deploy.time.time_ns", return_value=123456789000000):
                    path = root / "old.hex"
                    path.write_text(key.hex())
                    with patch.dict(os.environ, {"CHAIN_DEPLOY_KEY_FILE": str(path)}):
                        original = writer.prepare("new out in { out!(42) }")
                    with patch.dict(os.environ, {"CHAIN_DEPLOY_PUBLIC_KEY": public.upper()}):
                        self.assertEqual(writer.prepare("new out in { out!(42) }"), original)
                with patch.object(writer, "request", return_value=status), patch.dict(os.environ, {
                        "CHAIN_DEPLOY_KEY_FILE": str(path), "CHAIN_DEPLOY_PUBLIC_KEY": public}):
                    with self.assertRaisesRegex(ValueError, "exactly one"):
                        writer.prepare("Nil")
                (wallet.config / "wrapping.key").unlink()
                with patch.object(writer, "request", side_effect=[status, finalized]), \
                        patch.dict(os.environ, {"CHAIN_DEPLOY_PUBLIC_KEY": public}):
                    with self.assertRaises(ValueError):
                        writer.prepare("Nil")

    def test_receipts_require_canonical_finalization_and_one_result(self):
        writer = Deployer()
        sig = "ab" * 70
        for state, want in [("Pending", "pending"), ("Rejected", "refused"), ("Expired", "refused")]:
            with patch.object(writer, "request", return_value={"state": state}):
                self.assertEqual(writer.receipt(sig)["state"], want)
        replies = [{"state": "Finalized"}, {"blockHash": "ba" * 32, "errored": False}, {"expr": []}]
        with patch.object(writer, "request", side_effect=replies):
            self.assertEqual(writer.receipt(sig)["state"], "unknown")
