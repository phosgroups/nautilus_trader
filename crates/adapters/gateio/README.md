# Gate.io Adapter

This crate provides NautilusTrader integrations for Gate.io Spot and USDT-margined
perpetual markets.

The adapter contains:

- REST and WebSocket market-data clients.
- REST order execution with private WebSocket event reconciliation.
- Spot `CurrencyPair` and USDT perpetual `CryptoPerpetual` instruments.
- HMAC-SHA512 authentication for Gate.io API requests.

The public API is exposed through the Rust crate and, when the `python` feature
is enabled, through `nautilus_trader.adapters.gateio`.
