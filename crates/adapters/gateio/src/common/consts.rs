use std::{num::NonZeroU32, sync::LazyLock};

use nautilus_model::identifiers::{ClientId, Venue};
use nautilus_network::ratelimiter::quota::Quota;
use ustr::Ustr;

pub const GATEIO: &str = "GATEIO";
pub static GATEIO_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(GATEIO)));
pub static GATEIO_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(GATEIO)));

pub const GATEIO_REST_URL: &str = "https://api.gateio.ws/api/v4";
pub const GATEIO_SPOT_TESTNET_REST_URL: &str = "https://api-testnet.gateapi.io";
pub const GATEIO_FUTURES_TESTNET_REST_URL: &str = "https://api-testnet.gateapi.io";
pub const GATEIO_SPOT_WS_URL: &str = "wss://api.gateio.ws/ws/v4/";
pub const GATEIO_SPOT_TESTNET_WS_URL: &str = "wss://ws-testnet.gate.com/v4/ws/spot";
pub const GATEIO_FUTURES_WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
pub const GATEIO_FUTURES_TESTNET_WS_URL: &str = "wss://ws-testnet.gate.com/v4/ws/futures/usdt";
pub const GATEIO_SPOT_PING_WS_CHANNEL: &str = "spot.ping";
pub const GATEIO_FUTURES_PING_WS_CHANNEL: &str = "futures.ping";

pub const GATEIO_SPOT_BOOK_TICKER_WS_CHANNEL: &str = "spot.book_ticker";
pub const GATEIO_SPOT_TICKER_WS_CHANNEL: &str = "spot.tickers";
pub const GATEIO_SPOT_TRADES_WS_CHANNEL: &str = "spot.trades";
pub const GATEIO_SPOT_ORDER_BOOK_WS_CHANNEL: &str = "spot.order_book";
pub const GATEIO_SPOT_ORDER_BOOK_UPDATE_WS_CHANNEL: &str = "spot.order_book_update";
pub const GATEIO_SPOT_CANDLES_WS_CHANNEL: &str = "spot.candlesticks";
pub const GATEIO_SPOT_ORDERS_WS_CHANNEL: &str = "spot.orders";
pub const GATEIO_SPOT_USER_TRADES_WS_CHANNEL: &str = "spot.usertrades";
pub const GATEIO_SPOT_BALANCES_WS_CHANNEL: &str = "spot.balances";

pub const GATEIO_FUTURES_BOOK_TICKER_WS_CHANNEL: &str = "futures.book_ticker";
pub const GATEIO_FUTURES_TICKER_WS_CHANNEL: &str = "futures.tickers";
pub const GATEIO_FUTURES_TRADES_WS_CHANNEL: &str = "futures.trades";
pub const GATEIO_FUTURES_ORDER_BOOK_WS_CHANNEL: &str = "futures.order_book";
pub const GATEIO_FUTURES_ORDER_BOOK_UPDATE_WS_CHANNEL: &str = "futures.order_book_update";
pub const GATEIO_FUTURES_CANDLES_WS_CHANNEL: &str = "futures.candlesticks";
pub const GATEIO_FUTURES_ORDERS_WS_CHANNEL: &str = "futures.orders";
pub const GATEIO_FUTURES_USER_TRADES_WS_CHANNEL: &str = "futures.usertrades";
pub const GATEIO_FUTURES_BALANCES_WS_CHANNEL: &str = "futures.balances";
pub const GATEIO_FUTURES_POSITIONS_WS_CHANNEL: &str = "futures.positions";
pub const GATEIO_FUTURES_LIQUIDATES_WS_CHANNEL: &str = "futures.liquidates";
pub const GATEIO_FUTURES_AUTO_DELEVERAGES_WS_CHANNEL: &str = "futures.auto_deleverages";
pub const GATEIO_FUTURES_POSITION_CLOSES_WS_CHANNEL: &str = "futures.position_closes";

pub const SPOT_CURRENCY_PAIRS: &str = "/spot/currency_pairs";
pub const ACCOUNT_DETAIL: &str = "/account/detail";
pub const SPOT_ORDER_BOOK: &str = "/spot/order_book";
pub const SPOT_TRADES: &str = "/spot/trades";
pub const SPOT_CANDLESTICKS: &str = "/spot/candlesticks";
pub const SPOT_ACCOUNTS: &str = "/spot/accounts";
pub const SPOT_ORDERS: &str = "/spot/orders";
pub const SPOT_OPEN_ORDERS: &str = "/spot/open_orders";
pub const SPOT_MY_TRADES: &str = "/spot/my_trades";
pub const SPOT_TICKERS: &str = "/spot/tickers";

pub const FUTURES_CONTRACTS: &str = "/futures/usdt/contracts";
pub const FUTURES_ORDER_BOOK: &str = "/futures/usdt/order_book";
pub const FUTURES_TRADES: &str = "/futures/usdt/trades";
pub const FUTURES_CANDLESTICKS: &str = "/futures/usdt/candlesticks";
pub const FUTURES_TICKERS: &str = "/futures/usdt/tickers";
pub const FUTURES_FUNDING_RATE: &str = "/futures/usdt/funding_rate";
pub const FUTURES_ACCOUNTS: &str = "/futures/usdt/accounts";
pub const FUTURES_POSITIONS: &str = "/futures/usdt/positions";
pub const FUTURES_ORDERS: &str = "/futures/usdt/orders";
pub const FUTURES_PRICE_ORDERS: &str = "/futures/usdt/price_orders";
pub const FUTURES_MY_TRADES: &str = "/futures/usdt/my_trades";
pub const FUTURES_ORDERS_TIMERANGE: &str = "/futures/usdt/orders_timerange";
pub const FUTURES_MY_TRADES_TIMERANGE: &str = "/futures/usdt/my_trades_timerange";

pub const GATEIO_KEY_HEADER: &str = "KEY";
pub const GATEIO_SIGN_HEADER: &str = "SIGN";
pub const GATEIO_TIMESTAMP_HEADER: &str = "Timestamp";

pub static GATEIO_REST_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(10).expect("non-zero")).expect("valid quota")
});
