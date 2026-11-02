# Gate spot PMM on NautilusTrader 1.230

This integration connects the PMM program to Gate through the standard NT v1
Python `LiveMarketDataClient`, `LiveExecutionClient`, and client factories. It
uses the existing native NT HTTP/WebSocket transports. No Rust/Cython engine
changes or trading-bot / trading-bot-web integration are required.

## Supported interface

| Area | Implementation |
| --- | --- |
| Instruments | Regular cash spot, `BASE_QUOTE.GATEIO`, precision and minimums from Gate REST |
| Market data | Managed `L2_MBP`, REST snapshot + buffered `spot.order_book_update` |
| Orders | Base-sized GTC post-only limit orders (`time_in_force=poc`) |
| Cancellation | Individual, batch, or strategy-scoped cancel-all; wait for exchange confirmation |
| Account | Available + locked balances from `/spot/accounts`, reported to NT Cache |
| Fills | Actual trade ID, base quantity, price, maker/taker, fee, and fee currency |
| Recovery | Restore subscriptions, reconcile REST orders/trades/balances, deduplicate fills |

The book checks `U/u` against the snapshot sequence, ignores old updates, clears
the managed book on gaps, and obtains a new snapshot. Buffered WS intervals are
preserved while retrying a lagging REST snapshot. A sequence-advancing update
refreshes book time even when price and size have not changed.

The visible market book defaults to 100 levels per side. This is separate from
PMM's own 50 working orders per side and does not guarantee ±2% market coverage.

POST/DELETE requests with unknown outcomes are not blindly retried. NT order
queries and reconciliation determine their outcome; PMM keeps its existing
fault/shutdown policy. Partial-fill order totals are checked against trade
records rather than generating fictitious fills or fees. WS `event/finish_as`
and REST `status` are both supported. Immediate post-only cancellation is
reported as rejection.

This profile does not support margin, perpetuals, market orders, in-place
amendments, GT/points fee deductions, or unified-asset routing. The account
should be dedicated to this PMM; external orders are not claimed or canceled.

## PMM configuration

Use PMM branch `feature/gate-nt-v1.230` and its `config.gateio.example.toml`.
Set `exchange=gateio`, `instrument_id=BTC_USDT.GATEIO`,
`account_id=GATEIO-master`, and explicit API key/secret. No passphrase or
environment-variable credential fallback is used.

`demo` selects Gate spot testnet; `live` selects production. Both clients use
the same environment. Optional URL/proxy overrides support local protocol
testing. Defaults and authentication follow the official
[REST documentation](https://www.gate.com/docs/developers/apiv4/en/spot/) and
[spot WebSocket documentation](https://www.gate.com/docs/developers/apiv4/ws/en/).

The conservative HTTP throttle is 10 requests/second per client, not an asserted
Gate account limit. The Gate PMM example allows 30 seconds for command/in-flight
checks and 60 seconds for confirmed shutdown of a 100-order ladder.

## Build and test

Normal NT wheel builds include `nautilus_trader/adapters/gateio`. PMM deployment
needs a wheel built from this branch; an upstream PyPI 1.230 wheel alone does
not include this adapter. These source changes do not publish a release.

For adapter-only development, use a fresh Python 3.14 environment with the remote
`nautilus-trader==1.230.0` wheel, pytest, and aiohttp. The runner loads only this
branch's Gate Python package; native NT modules remain those in the installed
wheel. This validates compatibility without rebuilding unchanged native code.

```bash
python scripts/test_gateio_v1230.py -- -q
python scripts/test_gateio_v1230.py --pmm-root <pmm-checkout> -- -q
GATE_PUBLIC_SMOKE=1 python scripts/test_gateio_v1230.py -- -q -s -k public_rest
```

PMM protocol integration tests use real NT node/Cache/accounting/transport and a
local authenticated Gate server. They cover 50 quotes per side, partial and
duplicate fills, external cancellation/refill, reconnect catch-up, post-only
rejection, cancel/fill races, and confirmed shutdown with no open orders or
locked funds. Existing PMM engine, strategy, and POSIX-signal tests also run.
The optional public smoke test reads live REST and sequence-bridged WS data
without authentication or order requests. Authenticated Gate testnet/live
execution remains a separate validation step requiring exchange credentials.
