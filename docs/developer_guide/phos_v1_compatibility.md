# phos v1 compatibility

The phos fork uses official `v1.231.0` as its v1 baseline. Its release wheel is built
from the repository-root `pyproject.toml` with Poetry and `build.py`, which compiles
the Cython package in `nautilus_trader/` and the shared Rust/PyO3 extension.

The `python/` package and v2 Rust implementations remain in the repository. They
are not the Python package selected by the default phos wheel workflows. Shared
Rust code still participates in the v1 build and must compile with the workspace.

## Build and release

The workspace uses Rust `1.97.1` and crate version `0.61.0`. The root Python
package is `1.231.0`, with Python `3.14`, uv `0.11.33`, Cython `3.2.9`, and
Poetry Core `2.3.1` pinned by the build configuration.

`phos-pr-build.yml` and `phos-publish.yml` build the root package on Linux only.
They append the configured version suffix (default `+phos`), commit SHA, and run
number, for example `1.231.0+phos.a1b2c3d.42`. The Rust runtime reports the upstream
base version; the complete wheel version is available through package metadata.

The workflows use Ubuntu 24.04 and auditwheel `6.8.2` to repair the wheel for
`manylinux_2_39_x86_64` before smoke testing or publishing. Publishing continues
through the existing CodeArtifact release environment and OIDC role.

For a local root wheel build:

```bash
uv build --wheel --python 3.14
bash scripts/ci/smoke-test-v1-wheel.sh
```

On machines with limited free memory, use `CARGO_BUILD_JOBS=1 BUILD_JOBS=2`
for the build. `BUILD_JOBS` limits both Cython translation and C compilation;
`PARALLEL_BUILD=false` runs both stages with one worker. The phos workflows
set `BUILD_JOBS=2`, independently of the Cargo job limit.

Both workflows reuse the Cargo dependency cache and use sccache for Rust and C
compilation. PR runs save their own dependency cache; GitHub scopes it to that
PR, while main builds can seed caches for future PRs. Cython's content cache
reuses generated C sources. Final linking, wheel packaging, and manylinux repair
still run, and the workflows print compiler cache statistics.

CI builds use `.venv-build` with the root `build-system.requires` installed before
`uv build --no-build-isolation`. This keeps NumPy header and PyO3 interpreter
paths stable for compiler caching. Neither compiled extensions nor the entire
`build/` directory are restored: they must be linked against the current Rust
libraries. Local `uv build` retains its default isolated environment.

Run the smoke script from the repository root. It installs the wheel from `dist/`
and imports it from a neutral directory. It checks the v1 `TradingNode`, strategy
claim configuration, Bitget factories/bindings, and Gate.io's expected rejection.
It does not connect to a live exchange.

`.docker/build-wheel.dockerfile` also builds the root wheel using the pinned Rust
and uv images. Its local output is a native Linux wheel; the phos publishing
workflows perform the manylinux repair.

## Bitget

The v1 adapter retains its Python `BitgetLiveDataClientFactory` and
`BitgetLiveExecClientFactory`, backed by the existing Rust HTTP, WebSocket, and
parsing code. Both Classic and UTA account modes remain available.

Private order and fill pushes are converted to v1 execution reports. Order status
and real fills are reconciled together when necessary so a filled status does not
first cause the engine to infer a synthetic trade. Missing earlier fills are
requested over HTTP, duplicate trade IDs are discarded using cached executions,
and incomplete report/fill pairs remain pending for matching data.

On a private WebSocket reconnect, the client refreshes account state and requests
order history for its loaded instruments using
`reconnect_reconciliation_lookback_mins`. Reports use the same real-fill dispatch
path. Position pushes defer to the execution engine's position reconciliation.

The shared v1 report bridge preserves `is_quote_quantity` from Rust reports,
including serialization and external-order creation. This retains the distinction
between quote-denominated spot MARKET BUY quantities and base-denominated fills.
Strategies claim their external instruments through v1's `external_order_claims`.

The v1 Python client still rejects active order modification with an
`OrderModifyRejected` event. Its existing v2 Rust implementation remains available
in the source tree. Offline fixtures and wheel smoke checks do not establish live
exchange validation.

## Gate.io

The v1 wheel provides importable Gate.io configuration types and
`GateioLiveDataClientFactory` / `GateioLiveExecClientFactory`. The shorter facade
factory names remain aliases. Configuration construction is supported, but
calling either factory's `create()` raises an explicit `NotImplementedError`
before creating a client or connecting to an exchange.

The Gate.io Rust adapter and v2 Python facade remain in place. Gate.io live client
support is not implemented for the v1 Python runtime.
