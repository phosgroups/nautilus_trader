// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Bitget venue constants and API endpoints.

use std::{num::NonZeroU32, sync::LazyLock};

use nautilus_model::identifiers::{ClientId, Venue};
use nautilus_network::ratelimiter::quota::Quota;
use ustr::Ustr;

/// The Bitget venue identifier string.
pub const BITGET: &str = "BITGET";

/// Static venue instance for Bitget.
pub static BITGET_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(BITGET)));

/// Static client ID instance for Bitget.
pub static BITGET_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(BITGET)));

/// Bitget REST API base URL.
pub const BITGET_HTTP_URL: &str = "https://api.bitget.com";

/// Bitget public WebSocket URL.
pub const BITGET_WS_PUBLIC_URL: &str = "wss://ws.bitget.com/v3/ws/public";

/// Bitget private WebSocket URL.
pub const BITGET_WS_PRIVATE_URL: &str = "wss://ws.bitget.com/v3/ws/private";

/// Bitget demo public WebSocket URL.
pub const BITGET_WS_DEMO_PUBLIC_URL: &str = "wss://wspap.bitget.com/v3/ws/public";

/// Bitget demo private WebSocket URL.
pub const BITGET_WS_DEMO_PRIVATE_URL: &str = "wss://wspap.bitget.com/v3/ws/private";

/// Bitget demo trading HTTP header name.
pub const BITGET_PAPTRADING_HEADER: &str = "paptrading";

/// Bitget API key HTTP header name.
pub const BITGET_ACCESS_KEY_HEADER: &str = "ACCESS-KEY";

/// Bitget signature HTTP header name.
pub const BITGET_ACCESS_SIGN_HEADER: &str = "ACCESS-SIGN";

/// Bitget timestamp HTTP header name.
pub const BITGET_ACCESS_TIMESTAMP_HEADER: &str = "ACCESS-TIMESTAMP";

/// Bitget passphrase HTTP header name.
pub const BITGET_ACCESS_PASSPHRASE_HEADER: &str = "ACCESS-PASSPHRASE";

/// Bitget locale HTTP header name.
pub const BITGET_LOCALE_HEADER: &str = "locale";

/// Conservative default REST rate limit.
pub static BITGET_REST_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_second(NonZeroU32::new(10).expect("non-zero")).expect("valid constant")
});

/// Bitget UTA instrument definitions endpoint.
pub const BITGET_MARKET_INSTRUMENTS_ENDPOINT: &str = "/api/v3/market/instruments";

/// Bitget UTA order book endpoint.
pub const BITGET_MARKET_ORDERBOOK_ENDPOINT: &str = "/api/v3/market/orderbook";

/// Bitget UTA public fills endpoint.
pub const BITGET_MARKET_FILLS_ENDPOINT: &str = "/api/v3/market/fills";

/// Bitget UTA candles endpoint.
pub const BITGET_MARKET_CANDLES_ENDPOINT: &str = "/api/v3/market/candles";

/// Bitget UTA historical funding rate endpoint.
pub const BITGET_MARKET_FUNDING_HISTORY_ENDPOINT: &str = "/api/v3/market/history-fund-rate";

/// Bitget UTA place order endpoint.
pub const BITGET_SPOT_PLACE_ORDER_ENDPOINT: &str = "/api/v3/trade/place-order";

/// Bitget UTA place strategy order endpoint.
pub const BITGET_SPOT_PLACE_PLAN_ORDER_ENDPOINT: &str = "/api/v3/trade/place-strategy-order";

/// Bitget UTA cancel order endpoint.
pub const BITGET_SPOT_CANCEL_ORDER_ENDPOINT: &str = "/api/v3/trade/cancel-order";

/// Bitget UTA batch cancel orders endpoint.
pub const BITGET_SPOT_BATCH_CANCEL_ORDER_ENDPOINT: &str = "/api/v3/trade/cancel-batch";

/// Bitget UTA cancel all orders for a symbol endpoint.
pub const BITGET_SPOT_CANCEL_SYMBOL_ORDER_ENDPOINT: &str = "/api/v3/trade/cancel-symbol-order";

/// Bitget UTA query order endpoint.
pub const BITGET_SPOT_ORDER_INFO_ENDPOINT: &str = "/api/v3/trade/order-info";

/// Bitget UTA current unfilled orders endpoint.
pub const BITGET_SPOT_UNFILLED_ORDERS_ENDPOINT: &str = "/api/v3/trade/unfilled-orders";

/// Bitget UTA historical orders endpoint.
pub const BITGET_SPOT_HISTORY_ORDERS_ENDPOINT: &str = "/api/v3/trade/history-orders";

/// Bitget UTA private fills endpoint.
pub const BITGET_SPOT_FILLS_ENDPOINT: &str = "/api/v3/trade/fills";

/// Bitget UTA account assets endpoint.
pub const BITGET_SPOT_ACCOUNT_ASSETS_ENDPOINT: &str = "/api/v3/account/assets";

/// Bitget UTA account assets endpoint.
pub const BITGET_MIX_ACCOUNT_LIST_ENDPOINT: &str = "/api/v3/account/assets";

/// Bitget UTA place order endpoint.
pub const BITGET_MIX_PLACE_ORDER_ENDPOINT: &str = "/api/v3/trade/place-order";

/// Bitget UTA place strategy order endpoint.
pub const BITGET_MIX_PLACE_PLAN_ORDER_ENDPOINT: &str = "/api/v3/trade/place-strategy-order";

/// Bitget UTA modify order endpoint.
pub const BITGET_MIX_MODIFY_ORDER_ENDPOINT: &str = "/api/v3/trade/modify-order";

/// Bitget UTA modify strategy order endpoint.
pub const BITGET_MIX_MODIFY_PLAN_ORDER_ENDPOINT: &str = "/api/v3/trade/modify-strategy-order";

/// Bitget UTA cancel order endpoint.
pub const BITGET_MIX_CANCEL_ORDER_ENDPOINT: &str = "/api/v3/trade/cancel-order";

/// Bitget UTA batch cancel orders endpoint.
pub const BITGET_MIX_BATCH_CANCEL_ORDERS_ENDPOINT: &str = "/api/v3/trade/cancel-batch";

/// Bitget UTA cancel strategy order endpoint.
pub const BITGET_MIX_CANCEL_PLAN_ORDER_ENDPOINT: &str = "/api/v3/trade/cancel-strategy-order";

/// Bitget UTA query order endpoint.
pub const BITGET_MIX_ORDER_DETAIL_ENDPOINT: &str = "/api/v3/trade/order-info";

/// Bitget UTA private order fills endpoint.
pub const BITGET_MIX_ORDER_FILLS_ENDPOINT: &str = "/api/v3/trade/fills";

/// Bitget UTA current position endpoint.
pub const BITGET_MIX_SINGLE_POSITION_ENDPOINT: &str = "/api/v3/position/current-position";

/// Bitget UTA current position endpoint.
pub const BITGET_MIX_ALL_POSITIONS_ENDPOINT: &str = "/api/v3/position/current-position";

/// Bitget UTA current pending orders endpoint.
pub const BITGET_MIX_ORDERS_PENDING_ENDPOINT: &str = "/api/v3/trade/unfilled-orders";

/// Bitget UTA historical orders endpoint.
pub const BITGET_MIX_ORDERS_HISTORY_ENDPOINT: &str = "/api/v3/trade/history-orders";
