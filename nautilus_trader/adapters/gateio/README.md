# Gate.io v1 facade

The v1 wheel provides importable `GateioDataClientConfig`, `GateioExecClientConfig`,
`GateioLiveDataClientFactory`, and `GateioLiveExecClientFactory`. The shorter factory
names remain aliases for facade import compatibility.

Configuration objects can be created and placed in a `TradingNodeConfig`. Calling
either factory's `create()` raises `NotImplementedError` before any client,
connection, or account is created. Gate.io is unsupported in the v1 Python runtime.

The existing Rust adapter in `crates/adapters/gateio/` and v2 facade in
`python/nautilus_trader/adapters/gateio/` are retained for future v2 maintenance.
