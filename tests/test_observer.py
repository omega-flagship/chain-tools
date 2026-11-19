import contextlib
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "modules/chain_observer/src"))
import chain_observer as chain

HASH = "a" * 64
OTHER_HASH = "b" * 64
STATUS = {"isReadOnly": True, "isValidator": False, "isReady": True, "shardId": "root", "networkId": "testnet"}
BLOCK = {"blockHash": HASH, "shardId": "root", "isFinalized": True, "blockNumber": 12}


class Handler(BaseHTTPRequestHandler):
    def handle_request(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        body = json.loads(raw) if raw else None
        self.server.calls.append((self.command, self.path, body))
        status, response, headers = self.server.responses.get((self.command, self.path), (404, {"error": "not_found"}, {}))
        data = response if isinstance(response, bytes) else json.dumps(response).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    do_GET = handle_request
    do_POST = handle_request

    def log_message(self, *_args):
        pass


class ObserverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.server.calls = []
        self.server.responses = {
            ("GET", "/api/status"): (200, dict(STATUS), {}),
            ("GET", "/api/last-finalized-block?view=summary"): (200, {"blockInfo": dict(BLOCK)}, {}),
            ("POST", "/api/explore-deploy-by-block-hash"): (200, {
                "expr": [{"ExprInt": {"data": "42"}}], "block": dict(BLOCK), "cost": 210,
            }, {}),
        }
        self.client = chain.ObserverClient(self.url)

    def failure(self, expected, function, *args, **kwargs):
        with self.assertRaises(chain.ChainError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, expected)
        return caught.exception

    def test_finalized_snapshot_and_exact_source(self):
        source = 'new return in {\n  return!("quotes \\" and \\n, λ _quote_ _newline_")\n}\n'
        result = self.client.query(source)
        self.assertEqual(self.server.calls, [
            ("GET", "/api/status", None),
            ("GET", "/api/last-finalized-block?view=summary", None),
            ("POST", "/api/explore-deploy-by-block-hash", {
                "term": source, "blockHash": HASH, "usePreStateHash": False,
            }),
        ])
        self.assertEqual(result["selection"], "last-finalized")
        self.assertEqual(result["expr"], [{"ExprInt": {"data": "42"}}])
        self.assertEqual(result["block"]["blockHash"], HASH)
        self.assertTrue(result["readOnly"])

    def test_explicit_pre_state_skips_finalized_lookup(self):
        result = self.client.query("new return in { return!(42) }", block_hash=HASH.upper(), pre_state=True)
        self.assertEqual(len(self.server.calls), 2)
        self.assertTrue(self.server.calls[-1][2]["usePreStateHash"])
        self.assertEqual(result["state"], "pre")
        self.assertEqual(result["selection"], "explicit")

    def test_wrong_node_is_refused_before_execution(self):
        for updates, code in [({"isReadOnly": False}, "observer_required"),
                              ({"isValidator": True}, "observer_required"),
                              ({"isReadOnly": "true"}, "observer_required"),
                              ({"isValidator": None}, "observer_required"),
                              ({"isReady": False}, "observer_not_ready"),
                              ({"shardId": "other"}, "identity_mismatch"),
                              ({"networkId": "mainnet"}, "identity_mismatch")]:
            with self.subTest(updates=updates):
                self.server.calls = []
                self.server.responses[("GET", "/api/status")] = (200, STATUS | updates, {})
                self.failure(code, self.client.query, "Nil")
                self.assertEqual(self.server.calls, [("GET", "/api/status", None)])

    def test_unfinalized_default_is_refused(self):
        self.server.responses[("GET", "/api/last-finalized-block?view=summary")] = (200, BLOCK | {"isFinalized": False}, {})
        self.failure("invalid_response", self.client.query, "Nil")
        self.assertEqual(len(self.server.calls), 2)

    def test_direct_summary_response_is_supported(self):
        self.server.responses[("GET", "/api/last-finalized-block?view=summary")] = (200, BLOCK, {})
        self.assertEqual(self.client.finalized_block(), BLOCK)

    def test_bad_input_makes_no_requests(self):
        for term in ["", " ", None, "\0", "x" * (chain.MAX_TERM_BYTES + 1), "\ud800"]:
            with self.subTest(term_type=type(term).__name__):
                self.failure("invalid_term", self.client.query, term)
        self.failure("invalid_block_hash", self.client.query, "Nil", block_hash="abc")
        self.failure("block_required", self.client.query, "Nil", pre_state=True)
        self.failure("invalid_state", self.client.query, "Nil", pre_state="false")
        self.assertEqual(self.server.calls, [])

    def test_state_changing_routes_are_not_available(self):
        for method, path in [("POST", "/api/deploy"), ("POST", "/api/propose"), ("GET", "/api/status/../deploy")]:
            with self.subTest(path=path):
                self.failure("route_refused", self.client._request, method, path, {})
        self.assertEqual(self.server.calls, [])

    def test_only_local_observer_urls_are_accepted(self):
        for url in ["https://127.0.0.1:40453", "http://example.org", "http://192.168.1.2:40453",
                    "http://user:password@127.0.0.1", self.url + "/api/deploy", self.url + "?x=1",
                    self.url + "#frag", "http://localhost:0", "file:///tmp/query", "http://localh\nost"]:
            with self.subTest(url=url):
                self.failure("invalid_observer", chain.ObserverClient, url)
        for url in [self.url, self.url + "/api/", "http://localhost:40453", "http://[::1]:40453"]:
            self.assertTrue(chain.ObserverClient(url).observer.startswith("http://"))

    def test_invalid_timeouts(self):
        for timeout in [0, -1, 121, "nan", "inf", "invalid"]:
            with self.subTest(timeout=timeout):
                self.failure("invalid_timeout", chain.ObserverClient, self.url, timeout=timeout)

    def test_redirects_are_not_followed(self):
        self.server.responses[("GET", "/api/status")] = (302, {}, {"Location": self.url + "/api/deploy"})
        exc = self.failure("http_error", self.client.status)
        self.assertEqual(exc.http_status, 302)
        self.assertEqual(len(self.server.calls), 1)

    def test_node_errors_and_backpressure_are_preserved_without_retry(self):
        for status, code in [(400, "rholang_bad_term"), (503, "observer_busy"), (504, "exploratory_timeout")]:
            with self.subTest(status=status):
                self.server.calls = []
                headers = {"Retry-After": "15"} if status == 503 else {}
                self.server.responses[("POST", "/api/explore-deploy-by-block-hash")] = (
                    status, {"error": code, "message": "query refused"}, headers)
                exc = self.failure(code, self.client.query, "Nil", block_hash=HASH)
                self.assertEqual(exc.http_status, status)
                self.assertEqual(exc.retry_after, "15" if status == 503 else None)
                self.assertEqual(len(self.server.calls), 2)

    def test_query_protocol_checks_and_empty_results(self):
        for body in [{"expr": [], "block": BLOCK | {"blockHash": OTHER_HASH}, "cost": 1},
                     {"expr": [], "block": BLOCK, "cost": -1},
                     {"block": BLOCK, "cost": 1}, b"not JSON", []]:
            with self.subTest(body=body):
                self.server.responses[("POST", "/api/explore-deploy-by-block-hash")] = (200, body, {})
                self.failure("invalid_response", self.client.query, "Nil", block_hash=HASH)
        self.server.responses[("POST", "/api/explore-deploy-by-block-hash")] = (200, {"expr": [], "block": BLOCK, "cost": 0}, {})
        self.assertEqual(self.client.query("Nil", block_hash=HASH)["expr"], [])

    def test_response_size_is_bounded(self):
        self.server.responses[("GET", "/api/status")] = (200, b" " * 257, {})
        with patch.object(chain, "MAX_RESPONSE_BYTES", 256):
            self.failure("response_too_large", self.client.status)

    def test_hive_adapter_uses_configured_observer(self):
        with patch.dict("os.environ", {"CHAIN_OBSERVER_URL": self.url, "CHAIN_SHARD_ID": "root", "CHAIN_NETWORK_ID": "testnet"}):
            result = json.loads(chain.query_state_at(HASH, 'new return in { return!("x") }'))
            self.assertTrue(result["ok"])
            self.assertEqual(self.server.calls[-1][2]["term"], 'new return in { return!("x") }')
            self.server.responses[("GET", "/api/status")] = (200, STATUS | {"isReadOnly": False}, {})
            self.assertEqual(json.loads(chain.query_state("Nil"))["error"]["code"], "observer_required")

    def test_invalid_encoded_source_makes_no_requests(self):
        for source in ["%%invalid%%", "/w==", None, "A" * (4 * chain.MAX_TERM_BYTES)]:
            with self.subTest(source_type=type(source).__name__):
                result = json.loads(chain.query_state_base64(source))
                self.assertEqual(result["error"]["code"], "invalid_source_encoding")
        self.assertEqual(self.server.calls, [])

    def test_cli_outputs_json_and_error_exit_codes(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = chain.main(["--observer", self.url, "query", "--file", str(ROOT / "examples/hello.rho")])
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(output.getvalue())["ok"])
        self.assertEqual(self.server.calls[-1][2]["term"], (ROOT / "examples/hello.rho").read_text())
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            code = chain.main(["query", "--term", "Nil", "--pre-state"])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "block_required")


if __name__ == "__main__":
    unittest.main()

