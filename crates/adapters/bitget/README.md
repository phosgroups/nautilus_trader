# Nautilus Bitget Adapter

Bitget Spot and USDT Futures share one adapter, with explicitly selected account protocols:

| Configuration | REST | WebSocket | Account |
| --- | --- | --- | --- |
| `BitgetAccountMode.UTA` (default) | v3 | v3 | Unified Trading Account |
| `BitgetAccountMode.CLASSIC` | v2 | v2 | Classic account |

There is no account detection or automatic protocol fallback. Authentication failures are
reported on the selected protocol. Existing configurations continue to use UTA. Account mode
is independent of product type, mainnet/demo environment, and the Nautilus cash/margin account type.

```python
from nautilus_trader.adapters.bitget import (
    BitgetAccountMode,
    BitgetDataClientConfig,
    BitgetExecClientConfig,
    BitgetProductType,
)

# Select the same mode on the data and execution clients for this account.
data_config = BitgetDataClientConfig(
    product_type=BitgetProductType.USDT_FUTURES,
    account_mode=BitgetAccountMode.CLASSIC,
)
exec_config = BitgetExecClientConfig(
    product_type=BitgetProductType.USDT_FUTURES,
    account_mode=BitgetAccountMode.CLASSIC,
)
```

Use `BitgetProductType.SPOT` for Spot. Credentials and custom URLs use the existing config
fields. Custom URLs do not select or detect a protocol: set `account_mode` explicitly.
In Rust, use `BitgetAccountMode::Classic` on the config builder or the HTTP/WebSocket client's
`with_account_mode` builder. Serialized Rust configs accept `"account_mode": "classic"` or `"uta"`.

Classic has separate REST wire models and a WebSocket codec. Both protocols feed the existing
instrument, market data, account, order, fill, position, and reconciliation paths.
Classic coverage includes instrument discovery/status, books and snapshot recovery, tickers,
trades, candles, futures funding, account balances, positions, order placement/cancellation,
batch cancellation, symbol-scoped cancellation, futures amendments, and paginated reports.

Protocol-specific trading behavior:

- Classic Spot market buys require `quote_quantity=True`; the amount is in quote currency.
  Base-denominated market buys are rejected locally instead of sending that number as a quote budget.
  Spot sells and limit orders use base currency. UTA's existing quantity validation is unchanged.
- Classic futures account pushes trigger a coalesced REST refresh to retain margin amounts
  omitted from the WebSocket payload.
- The Rust execution client polls Classic Spot plan-order pending/history endpoints every five
  seconds because Classic Spot has no strategy-order push channel. Futures plans use `orders-algo`.
  A successful trigger is distinct from a fill. Reconciliation queries ordinary and plan orders.
- Classic futures regular amendments apply only to unfilled limit orders, as required by Bitget.
  The execution client supplies both price and size, generates a distinct venue client ID, and
  promotes the replacement venue order ID while retaining the Nautilus client order ID. The
  `-NTR-` plus 16-hex-digit suffix is reserved; original IDs for amendments must fit in 43 bytes.
  A timeout does not resend the amendment. Further individual changes are rejected while its
  outcome is unknown; WebSocket events and REST reconciliation can confirm it.
- Plan modification/cancellation uses the existing `params={"plan_type": "normal_plan"}` contract.
  Spot modification, plan batch cancellation, and plan cancel-all remain outside the existing
  adapter's mapped command surface.

The Python facade retains its existing execution-event capabilities; the Rust execution client
owns live reconciliation and the plan polling/amendment lifecycle described above. Selecting
Classic does not add missing event handling to the older Python execution facade.

Validation uses local mock HTTP/WebSocket servers, including login signatures, reconnect and
subscription replay, Classic payloads, pagination, plan states, and replacement identity handling.
These tests do not establish production account permissions or replace exchange-side acceptance
of your production account configuration.

Protocol references: [Classic REST](https://www.bitget.com/docs/classic/rest-api),
[Classic futures amendments](https://www.bitget.com/api-doc/classic/contract/trade/Modify-Order),
[Spot plans](https://www.bitget.com/api-doc/spot/plan/Get-History-Plan-Order),
[futures plans](https://www.bitget.com/api-doc/classic/contract/plan/orders-plan-history).
