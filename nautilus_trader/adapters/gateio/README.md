# Gate spot adapter for NT 1.231

This adapter implements public spot L2/trades/quotes and authenticated account,
order, fill and cancellation handling through NT v1's live client interfaces.
The existing Bitget CLASSIC/UTA adapter remains available in the same build.

Use `GateioLiveDataClientFactory` and `GateioLiveExecClientFactory` with
`TradingNode`. The shorter factory names remain aliases. Configure
`GateioEnvironment.TESTNET` or `LIVE` explicitly; execution credentials are
required in `GateioExecClientConfig`. Credentials are not read implicitly
from environment variables.

REST snapshots are bridged with ordered WS deltas before publishing L2 data.
Sequence gaps, reconnects and unknown command outcomes are handled explicitly.
HTTP reads/writes share the configured client throttle. Partial fills,
cancel/fill races and omitted zero balances are covered by protocol tests.

Gate support is spot only. See `docs/integrations/gateio.md` for configuration,
test coverage and current limitations.
