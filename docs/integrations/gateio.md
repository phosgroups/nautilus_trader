# Gate spot on NautilusTrader 1.231

This integration connects the PMM program to Gate through the standard NT v1
Python `LiveMarketDataClient`, `LiveExecutionClient`, and client factories. It
uses the existing native NT HTTP/WebSocket transports. No Rust/Cython engine
changes or trading-bot / trading-bot-web integration are required.

The base is Phos main commit `7a87e60ff84b181784cd6e9817a1950dd193fa8b`,
version 1.231.0. Its Bitget CLASSIC/UTA implementation is retained unchanged.
Gate and Bitget use the same compiled native runtime and public v1 factories.

## Supported interface

| Area | Implementation |
| --- | --- |
| Instruments | Regular cash spot, `BASE_QUOTE.GATEIO`, precision and minimums from Gate REST |
| Market data | Managed `L2_MBP`, REST snapshot + buffered `spot.order_book_update` |
| Public trades | `spot.trades` -> native `TradeTick`, taker side, original ID and exchange time |
| Best prices | `spot.book_ticker` -> native `QuoteTick`; empty sides do not produce zero quotes |
| Orders | Base-sized GTC post-only limit orders (`time_in_force=poc`) |
| Cancellation | Individual, batch, or strategy-scoped cancel-all; wait for exchange confirmation |
| Account | Available + locked balances from `/spot/accounts`, reported to NT Cache |
| Fills | Actual trade ID, base quantity, price, maker/taker, fee, and fee currency |
| Recovery | Restore subscriptions, reconcile REST orders/trades/balances, deduplicate fills |

The book checks `U/u` against the snapshot sequence, ignores old updates, clears
the managed book on gaps, and obtains a new snapshot. Buffered WS intervals are
preserved while retrying a lagging REST snapshot. A sequence-advancing update
refreshes book time even when price and size have not changed.

The visible market book defaults to at most 100 levels per side. The legacy
100 ms channel supplies a 100-level window, so `book_depth` accepts 1 through
100; requesting 1000 levels fails at startup instead of presenting a one-time
REST snapshot as a continuously updated 1000-level feed. Internal maintenance
retains the full channel window before clipping the published view. `full=true`
replaces the book with CLEAR + snapshot deltas, including after sequence gaps;
an older in-flight REST response cannot overwrite a newer full WS image.
This is separate from
PMM's own 50 working orders per side and does not guarantee ±2% market coverage.

POST/DELETE requests with unknown outcomes are not blindly retried. NT order
queries and reconciliation determine their outcome; PMM keeps its existing
fault/shutdown policy. Partial-fill order totals are checked against trade
records rather than generating fictitious fills or fees. Status and real fills
are reconciled together, including external orders already filled at startup,
so NT retains the exchange trade IDs, prices and fee currencies. WS `event/finish_as`
and REST `status` are both supported. Immediate post-only cancellation is
reported as rejection.

This profile does not support margin, perpetuals, market orders, in-place
amendments, GT/points fee deductions, or unified-asset routing. The account
should be dedicated to this PMM; external orders are not claimed or canceled.

## PMM configuration

Use PMM branch `feature/gate-nt-v1.231` and its `config.gateio.example.toml`.
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

Balance pushes are partial currency updates. They mark the account dirty rather
than making REST calls in the ordered private-stream consumer. One background
worker coalesces notifications and command completions into full `/spot/accounts`
reads, with `account_refresh_interval_ms=250` between request starts by default.
Order/cancel reports never wait for these reads or hold their report lock while
refreshing balances. Notifications arriving during a read trigger a follow-up;
failed reads retry with bounded backoff and preserve the last valid account.
Explicit account queries share an in-flight read. Reconnect cancels/fences old
reads before restoring orders, fills and balances; disconnect cancels background
work and performs a bounded final account reconciliation. Initial account
registration still completes before private-stream startup.

Quiet accounts are also polled before `account_stale_after_ms` (default 10000)
expires. New submissions wait for private-stream recovery, fresh balances, and
resolved execution facts for at most `submission_ready_timeout_secs` (default
10). Expiry produces an explicit local denial without sending a POST. Queries
and cancellations remain available; PMM retains its existing denial/fault policy.

Filled-order statuses are matched against Cache executions and buffered real
fills. Missing history is fetched by one background recovery worker without
holding the report lock. Empty/lagging REST history and transport errors leave
the report pending for retry. A single-order query returns no incomplete filled
status that NT could turn into inferred executions. Reconnection fences old
workers and retries failed business reconciliation even while the socket stays
connected. Private messages wait until recovery completes; overflow invalidates
the backlog and requests authoritative reconciliation.

This follows Binance/OKX's separation of account handling from execution reports,
while retaining full REST account reconciliation and paired order/fill recovery
as in Bitget. Gate balance pushes are not treated as complete wallet snapshots.

## Previous Gate implementation and PR review contracts

The v2 Gate fixes in `1262d9f0e2` and `0443ac771f`, and
[PR #4](https://github.com/phosgroups/nautilus_trader/pull/4), were checked against
this spot-only v1 profile:

| Review item | Current contract |
| --- | --- |
| 1. Private subscriptions | Explicit spot symbols, batches of at most 50, acknowledged subscriptions and retrying recovery; spot balances have no payload |
| 2. Commission sign | Positive fee debit, negative rebate credit; same-currency rebates net correctly; unsupported mixed-currency fees/rebates fail explicitly |
| 3. Post-only | GTC limit orders map to `poc`; other execution semantics are denied locally |
| 4. Market amount units | Market orders, quote-sized orders and reduce-only orders are explicitly unsupported; no silent base/quote reinterpretation |
| 5. Client IDs | Canonical UUIDs use a reversible 22-byte encoding; supported short ASCII IDs round-trip; other IDs are rejected without truncation |
| 6. Grouped open orders | Per-symbol `/spot/orders?status=open` returns a flat array; pagination deduplicates IDs and rejects repeated pages |
| 7. Candles | Unsupported in this PMM spot profile; no candle channel is falsely advertised |
| 8. Derivative prices/funding | Unsupported; public spot quotes use `spot.book_ticker` |
| 9. Balance models/times | Partial spot notifications trigger authoritative REST snapshots; those snapshots use local observation time, not a fabricated exchange event time; derivative balances are unsupported |
| 10. Order dedup/time | Real trade IDs deduplicate fills; price-only order updates are retained, business timestamps preserve fractional milliseconds, and stale lower cumulative fills cannot overwrite pending newer facts |

Reversible IDs restore identity without process-local mappings, while NT's
external-order ownership rules still apply. Existing hash IDs from earlier
versions can be resolved with a restored NT Cache; losing that Cache does not
make legacy hashed orders safely claimable. Do not claim or cancel unrelated
account orders to work around missing identity.

Instrument definitions retain maximum base quantity and quote notional limits,
and exchange price-protection cancellations map to an explicit terminal state.

## Queued commands and periodic reconciliation

The HTTP budget is shared by all methods. A cancellable Python queue runs before
signing and native dispatch; cancellations take priority over reads, and reads
over new submissions. This prevents full ladders from starving authoritative
account reads. Native quota enforcement remains enabled as a second limit.
Queued submissions recheck stream readiness, balances, execution facts and local
cancellation immediately before dispatch. An explicitly unsent request may wait
for readiness again; unknown POST outcomes are never retried. A cancellation
without a venue ID waits for an already dispatched submit response instead of
overtaking it. Shutdown prevents queued writes from appearing afterwards.

Gate overrides the engine's mass-status query to return paired order/trade facts.
Periodic order-list queries also defer filled totals until real executions have
been applied. A missing history window returns an incomplete-reconciliation
failure, never a fabricated fee-free fill. Recent fills of older orders and
cached active orders are queried explicitly when absent from creation-time
filters. Complete bulk history is reused rather than re-fetching every filled
order separately. Reconciliation flags are reset on failure and cancellation.

Subscriptions carry unique request IDs and require matching acknowledgements;
a late acknowledgement from an earlier attempt cannot confirm a newer batch.

Execution and book prices/sizes retain the exchange decimal values rather than
being rounded to the current instrument rules. Other wallets/instruments and
invalid execution numbers fail validation. The fill fast-dedup cache is bounded
to 100,000 IDs; native Cache trade IDs remain authoritative after eviction.
Reversible client IDs need no unbounded process-local mapping.

Native node tests additionally exercise the Gate example's unchanged 3-second
book timeout, 2x imbalance threshold, enabled depth-coverage check and 2% cancel
boundary at 50 levels per side. They verify imbalance/staleness withdrawal,
automatic recovery and confirmed clean shutdown. This controlled protocol test
does not assert that a quiet or imbalanced real testnet market satisfies those
guards, or that unsupported account/execution modes are production-ready.

## Build and test

Normal NT wheel builds include `nautilus_trader/adapters/gateio`. PMM deployment
needs a wheel built from this branch; an upstream PyPI wheel alone does
not include this adapter. These source changes do not publish a release.

Build and install the complete wheel in an isolated Python 3.14 environment.
Run adapter tests from outside the NT checkout so imports resolve to that
installed wheel. No checkout path injection or replacement of installed native
modules is needed. Gate protocol tests live under
`tests/integration_tests/adapters/gateio`; existing Bitget protocol, provider,
factory, execution and CLASSIC tests remain under their original test paths.

PMM protocol integration tests use real NT node/Cache/accounting/transport and a
local authenticated Gate server. They cover 50 quotes per side, partial and
duplicate fills, external cancellation/refill, reconnect catch-up, post-only
rejection, cancel/fill races, and confirmed shutdown with no open orders or
locked funds. Existing PMM engine, strategy, and POSIX-signal tests also run.
The optional public smoke test reads live REST and sequence-bridged WS data
without authentication or order requests.

Earlier NT 1.230 BTC_USDT spot testnet checks on 2026-11-03 confirmed post-only
placement at 3 and 50 levels per side, external cancellation and replacement,
and confirmed shutdown with zero open orders and zero locked BTC/USDT. They
exposed and fixed a missing native HTTP rate-limit key and cancellation races
where Gate returns ORDER_NOT_FOUND for an already closed order. A missing
order is queried before accepting a terminal state; an unresolved or still-open
order remains a cancellation failure.

The real-feed checks also found quiet L2 intervals longer than the default
3-second stale-book threshold. Those intervals deliberately cancel the ladder;
this is not proof of a disconnected socket. An independent 35-second feed
observation received 12 updates, with an 8.299-second maximum gap. A 50-level
diagnostic with a 10-second stale threshold still observed a protection cycle
and an 83.732-second replacement interval. Steady quoting is not validated.
The test disabled depth-coverage gating, kept the 200-bps quote boundary, used
a 220-bps cancellation boundary, and allowed 90 seconds for command outcomes;
PMM defaults were not changed. No natural fills occurred, so real fill/fee
handling and production trading remain unverified.
