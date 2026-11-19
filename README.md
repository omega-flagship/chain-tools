# Chain tools

A native OmegaHive module and Python CLI for read-only queries against the local
F1R3node Rust observer, plus an optional host-side signed deploy adapter. The
observer client requires Python 3.12 or later and has no runtime package dependencies.

The local default is **http://127.0.0.1:40453**, the HTTP API published by
`rnode.readonly`, with network `testnet` and shard `root`.

## Run

From this repository:

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e .

.venv/bin/chain-tools status
.venv/bin/chain-tools finalized-block
.venv/bin/chain-tools query --file examples/hello.rho
.venv/bin/chain-tools query --file examples/vault-system-present.rho
```

The hello query returns `ExprInt` with `data: "42"`. The vault example queries
the chain registry and returns whether `rho:vault:system` exists in the selected
snapshot. It does not create a vault or transfer tokens.

Installation is optional. The same CLI is available directly:

```bash
python3.12 modules/chain_observer/src/chain_observer.py query --file examples/hello.rho
```

Read source from stdin, or supply an inline term:

```bash
cat examples/hello.rho | .venv/bin/chain-tools query --file -
.venv/bin/chain-tools query --term 'new return in { return!(42) }'
```

To repeat a query at a particular snapshot, use the full `result.block.blockHash`
from a previous query:

```bash
.venv/bin/chain-tools query --block FULL_64_HEX_BLOCK_HASH --file examples/hello.rho
```

Add `--pre-state` to read that explicit block's pre-state. The default is
post-state. Without `--block`, the client first reads the last finalized block
and pins the query to its hash; it never substitutes an unfinalized tip.

Each successful command returns JSON with `ok: true` and `result`. Query results
contain typed `expr` values, the node's `block` metadata, interpreter `cost`,
observer URL and snapshot selection. Integer values retain the node's string
representation to avoid precision loss. `expr: []` is a successful query that
returned no values. Errors are JSON on stderr with a nonzero CLI exit status.

## Rholang return channel

Exploratory deploy reads the first fresh unforgeable name created by the program.
Declare an unbound return name first and send the result to it:

```rholang
new return, lookup(`rho:registry:lookup`), found in {
  lookup!(`rho:vault:system`, *found) |
  for (value <- found) { return!(*value) }
}
```

Do not bind this name to `rho:system:deployId`; that is a different channel and
does not return exploratory results.

## Configuration

| Environment variable | Default | CLI option |
| --- | --- | --- |
| `CHAIN_OBSERVER_URL` | `http://127.0.0.1:40453` | `--observer` |
| `CHAIN_SHARD_ID` | `root` | `--shard` |
| `CHAIN_NETWORK_ID` | `testnet` | `--network` |
| `CHAIN_QUERY_TIMEOUT` | `30` seconds per HTTP request | `--timeout` |

CLI options precede the subcommand. Configuration belongs to the operator; a
Hive skill takes source and, optionally, a block hash, never a destination URL.

Only HTTP loopback addresses are accepted. Proxy environment variables are
ignored and HTTP redirects are refused. Every query first requires the node to
report `isReadOnly: true`, `isValidator: false`, `isReady: true` and the expected
shard/network identity.

The client uses exactly three routes:

- `GET /api/status`
- `GET /api/last-finalized-block?view=summary`
- `POST /api/explore-deploy-by-block-hash`

The observer client and mind module have no signing keys, deploy submission or block-proposal methods.
Read-only means execution against a temporary chain-state snapshot: arbitrary
Rholang still consumes observer resources and uses the node's configured
runtime capabilities. The client does not statically prove program purity.
Interpreter resource limits are enforced by the node. Client limits are 64 KiB
of source, 2 MiB of response, and a configurable socket timeout up to 120 seconds.

`observer_busy` (503) retains `Retry-After` in the error, and
`exploratory_timeout` (504) is reported distinctly. Calls are never automatically
retried. A client timeout does not cancel an execution already running on the node.

## Native OmegaHive module

The module is [modules/chain_observer](modules/chain_observer). It provides:

| Skill | Input |
| --- | --- |
| `chain-status` | None |
| `chain-finalized-block` | None |
| `chain-query` | Rholang source; defaults to finalized post-state |
| `chain-query-at` | Full block hash, then Rholang source |
| `chain-query-base64` | Base64-encoded UTF-8 source |
| `chain-query-at-base64` | Full block hash, then base64-encoded UTF-8 source |

For example, a mind can issue:

```text
chain-query <<'RHO'
new return in {
  return!(42)
}
RHO
```

The plain-source commands lower to base64 before PeTTa evaluates them. Hive's
text parser first expands its legacy `_quote_` and `_newline_` placeholders, even
inside a heredoc. If the original source contains those literal strings, use an
explicit base64 command. The CLI sends the original UTF-8 source unchanged.

To enable the module in a Hive checkout, copy `modules/chain_observer` into that
checkout's `modules/` directory and add this line to the host-controlled loader
that its minds use:

```metta
!(import! &self (library OmegaClaw-Core ./modules/chain_observer/entry.metta))
```

Then restart the affected minds and set the relevant Hive policy rules for the
`chain-*` commands, including their `-base64` forms. Defaults and declared risk
cards do not grant permission. The bridge is self-contained in the module's
`src/chain_observer.py`; the Hive venv does not need an editable pip install.

This adapter connects directly to the observer. It works from an unsandboxed
local mind on this server. A sandboxed mind whose network permits only the Hive
API needs a host-side relay before using it. It is a native module; installing
it does not add an entry to the separate `omega-tools` app or expose an MCP tool.

Creation of this repository does not alter the running Hive's loader, policy or
service configuration. The module can be reviewed and enabled separately.

## Host-side deploy adapter

The DeFi Benchmark Hive program uses `omega_chain_deploy.Deployer` to prepare a
secp256k1-signed deploy, submit it to the local queue and follow its canonical
finalization receipt. Install the optional dependency in the Hive host environment:

```sh
uv pip install --python ../omegahive/.venv/bin/python -e '.[deploy]'
```

Set `CHAIN_DEPLOY_URL` (default `http://127.0.0.1:40413`) and choose one signer:

- For an encrypted host wallet, install the sibling `omega-chain-wallet` package in the same Python environment. Import the funded key using its operator CLI and set `CHAIN_DEPLOY_PUBLIC_KEY` to its full uncompressed public hex key. The wallet returns the deploy signature.
- For a host-only hex file, set `CHAIN_DEPLOY_KEY_FILE` to the file containing the funded 32-byte key.

Network and shard settings are shared with the observer. The adapter targets
F1R3node's uint256 phlo-price wire format. Prepare gives the caller the deploy
signature before submission so an interrupted write can be reconciled. Minds
interact through the Hive's existing authenticated `app-act` skill and the
benchmark's named actions. The signing adapter is installed on the host only.

## Tests

```bash
python3.12 -m unittest discover -s tests -v
```

The HTTP tests use a local fake observer, including validator refusal, wrong
shard, pinned blocks, invalid inputs, redirects, bounded responses and node
errors. The Hive parser/PeTTa test runs when those dependencies are available:

```bash
source ../omegahive/.runtime/env.sh
python -m unittest discover -s tests -v
```

`HIVE_CHECKOUT` and `PETTA_PATH` can select other checkouts. Tests never submit
normal deploys. Live smoke results from this server are kept in the ignored
`.runtime/verification.json` when present.

## Protocol reference

This implementation was checked against the local F1R3node Rust checkout at
`0380cbbd494cf8d1194d20830036fa56d8109c3d`, matching the running observer's status.
The relevant node sources are `docs/node/exploratory-deploy.md`,
`node/src/rust/web/shared_handlers.rs`, `node/src/rust/api/web_api.rs`, and
`casper/src/rust/api/block_api.rs`. OmegaHive's module template is
`omegahive/examples/hello-module/` in the parent workspace.
