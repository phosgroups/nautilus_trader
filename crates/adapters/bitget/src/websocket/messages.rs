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

//! Bitget WebSocket command and event message structures.

use nautilus_network::{RECONNECTED, websocket::TEXT_PONG};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
    common::enums::BitgetProductType,
    http::models::{BitgetFillFeeDetail, BitgetMarketTrade, BitgetUtaAsset},
    websocket::error::BitgetWsError,
};

/// Deserializes Bitget WebSocket event codes.
///
/// Official Bitget exchange WebSocket docs define `code` as a string and show `"0"` for login
/// success. The demo/PAP gateway has been observed returning integer `0`; normalize integer codes
/// to the documented string form while rejecting unrelated JSON types.
fn deserialize_optional_code<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;

    match value {
        Some(Value::String(value)) => Ok(Some(value)),
        Some(Value::Number(value)) if value.is_i64() || value.is_u64() => {
            Ok(Some(value.to_string()))
        }
        Some(Value::Number(value)) => Err(serde::de::Error::custom(format!(
            "expected Bitget code as a string or integer, got {value}",
        ))),
        Some(Value::Null) | None => Ok(None),
        Some(value) => Err(serde::de::Error::custom(format!(
            "expected Bitget code as a string or integer, got {value}",
        ))),
    }
}

fn deserialize_optional_string_or_number<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;

    match value {
        Some(Value::String(value)) => Ok(Some(value)),
        Some(Value::Number(value)) => Ok(Some(value.to_string())),
        Some(Value::Null) | None => Ok(None),
        Some(value) => Err(serde::de::Error::custom(format!(
            "expected Bitget field as a string or number, got {value}",
        ))),
    }
}

/// Bitget WebSocket operation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BitgetWsOp {
    /// Subscribe operation.
    Subscribe,
    /// Unsubscribe operation.
    Unsubscribe,
    /// Login/authentication operation.
    Login,
}

/// Bitget UTA WebSocket topic argument.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsArg {
    /// Instrument type, e.g. `spot`, `usdt-futures`, or private `UTA`.
    pub inst_type: String,
    /// Topic name.
    pub topic: String,
    /// Raw Bitget symbol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// Optional v3 UTA kline interval, e.g. `1m` or `1H`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    /// Optional coin selector for account-like topics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coin: Option<String>,
}

impl BitgetWsArg {
    /// Creates a new public topic argument for a Bitget product/topic/symbol.
    #[must_use]
    pub fn new(
        product_type: BitgetProductType,
        topic: impl Into<String>,
        symbol: Option<String>,
    ) -> Self {
        Self {
            inst_type: product_type.as_ws_public_inst_type().to_string(),
            topic: topic.into(),
            symbol,
            interval: None,
            coin: None,
        }
    }

    /// Creates a new public v3 UTA kline topic argument for a Bitget product/symbol/interval.
    #[must_use]
    pub fn kline(
        product_type: BitgetProductType,
        symbol: impl Into<String>,
        interval: impl Into<String>,
    ) -> Self {
        Self {
            inst_type: product_type.as_ws_public_inst_type().to_string(),
            topic: "kline".to_string(),
            symbol: Some(symbol.into()),
            interval: Some(interval.into()),
            coin: None,
        }
    }

    /// Creates a new private UTA topic argument.
    #[must_use]
    pub fn private(topic: impl Into<String>, symbol: Option<String>) -> Self {
        Self {
            inst_type: "UTA".to_string(),
            topic: topic.into(),
            symbol,
            interval: None,
            coin: None,
        }
    }

    /// Creates a private account topic argument.
    #[must_use]
    pub fn account() -> Self {
        Self {
            inst_type: "UTA".to_string(),
            topic: "account".to_string(),
            symbol: None,
            interval: None,
            coin: None,
        }
    }

    /// Returns a stable key for de-duplicating and replaying subscriptions.
    #[must_use]
    pub fn topic_key(&self) -> String {
        let symbol = self.symbol.as_deref().unwrap_or("");
        let interval = self
            .interval
            .as_deref()
            .map(|interval| format!(":interval:{interval}"))
            .unwrap_or_default();
        let coin = self
            .coin
            .as_deref()
            .map(|coin| format!(":coin:{coin}"))
            .unwrap_or_default();

        format!("{}:{}:{symbol}{interval}{coin}", self.inst_type, self.topic)
    }
}

/// Bitget WebSocket command.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsCommand {
    /// Operation.
    pub op: BitgetWsOp,
    /// Channel or login arguments.
    pub args: Vec<serde_json::Value>,
}

impl BitgetWsCommand {
    /// Creates a subscribe command.
    ///
    /// # Errors
    ///
    /// Returns an error if the argument cannot be encoded as JSON.
    pub fn subscribe(args: Vec<BitgetWsArg>) -> serde_json::Result<Self> {
        Ok(Self {
            op: BitgetWsOp::Subscribe,
            args: args
                .into_iter()
                .map(serde_json::to_value)
                .collect::<serde_json::Result<Vec<_>>>()?,
        })
    }

    /// Creates an unsubscribe command.
    ///
    /// # Errors
    ///
    /// Returns an error if the argument cannot be encoded as JSON.
    pub fn unsubscribe(args: Vec<BitgetWsArg>) -> serde_json::Result<Self> {
        Ok(Self {
            op: BitgetWsOp::Unsubscribe,
            args: args
                .into_iter()
                .map(serde_json::to_value)
                .collect::<serde_json::Result<Vec<_>>>()?,
        })
    }

    /// Creates a login command.
    ///
    /// # Errors
    ///
    /// Returns an error if the login argument cannot be encoded as JSON.
    pub fn login(arg: BitgetWsLoginArg) -> serde_json::Result<Self> {
        Ok(Self {
            op: BitgetWsOp::Login,
            args: vec![serde_json::to_value(arg)?],
        })
    }
}

/// Bitget WebSocket login argument.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsLoginArg {
    /// API key.
    pub api_key: String,
    /// API passphrase.
    pub passphrase: String,
    /// Timestamp string.
    pub timestamp: String,
    /// Signature.
    pub sign: String,
}

/// Generic Bitget WebSocket event envelope.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsEvent<T> {
    /// Event type for operation acknowledgements.
    #[serde(default)]
    pub event: Option<String>,
    /// Action for market/private pushes, e.g. `snapshot` or `update`.
    #[serde(default)]
    pub action: Option<String>,
    /// Channel argument.
    #[serde(default)]
    pub arg: Option<BitgetWsArg>,
    /// Payload data.
    #[serde(default)]
    pub data: Vec<T>,
    /// Event timestamp in milliseconds.
    #[serde(default, deserialize_with = "deserialize_optional_string_or_number")]
    pub ts: Option<String>,
    /// Optional error code.
    #[serde(default, deserialize_with = "deserialize_optional_code")]
    pub code: Option<String>,
    /// Optional error message.
    #[serde(default)]
    pub msg: Option<String>,
}

/// Venue-typed WebSocket messages emitted by [`crate::websocket::client::BitgetWebSocketClient`].
#[derive(Clone, Debug, PartialEq)]
pub enum BitgetWsMessage {
    /// Transport reconnected and the client should restore session state.
    Reconnected,
    /// Text heartbeat pong received from Bitget.
    Pong,
    /// Private login acknowledgement.
    Login(BitgetWsEvent<Value>),
    /// Subscribe acknowledgement.
    Subscribe(BitgetWsEvent<Value>),
    /// Unsubscribe acknowledgement.
    Unsubscribe(BitgetWsEvent<Value>),
    /// Protocol error or failed acknowledgement.
    Error(BitgetWsEvent<Value>),
    /// Market/private data push.
    Data(BitgetWsEvent<Value>),
}

impl BitgetWsMessage {
    /// Parses a Bitget text frame into a typed message.
    ///
    /// # Errors
    ///
    /// Returns a JSON parse error if the text is not a control frame and cannot be decoded as
    /// a Bitget envelope.
    pub fn parse_text(text: &str) -> Result<Self, BitgetWsError> {
        if text == RECONNECTED {
            return Ok(Self::Reconnected);
        }

        if text == TEXT_PONG {
            return Ok(Self::Pong);
        }

        let event: BitgetWsEvent<Value> = serde_json::from_str(text)?;
        Ok(Self::from_event(event))
    }

    /// Classifies a generic Bitget event envelope.
    #[must_use]
    pub fn from_event(event: BitgetWsEvent<Value>) -> Self {
        let is_error = event
            .code
            .as_deref()
            .is_some_and(|code| code != "0" && !code.is_empty());

        if is_error {
            return Self::Error(event);
        }

        match event.event.as_deref() {
            Some("login") => Self::Login(event),
            Some("subscribe") => Self::Subscribe(event),
            Some("unsubscribe") => Self::Unsubscribe(event),
            Some("error") => Self::Error(event),
            _ => Self::Data(event),
        }
    }

    /// Returns the underlying envelope when this message carries one.
    #[must_use]
    pub const fn event(&self) -> Option<&BitgetWsEvent<Value>> {
        match self {
            Self::Login(event)
            | Self::Subscribe(event)
            | Self::Unsubscribe(event)
            | Self::Error(event)
            | Self::Data(event) => Some(event),
            Self::Reconnected | Self::Pong => None,
        }
    }

    /// Returns `true` when this is a successful login acknowledgement.
    #[must_use]
    pub fn is_login_success(&self) -> bool {
        matches!(self, Self::Login(event) if event.code.as_deref().unwrap_or("0") == "0")
    }

    /// Returns the best available error message from an event envelope.
    #[must_use]
    pub fn error_message(&self) -> Option<String> {
        let event = self.event()?;
        match (&event.code, &event.msg) {
            (Some(code), Some(msg)) => Some(format!("{code}: {msg}")),
            (Some(code), None) => Some(code.clone()),
            (None, Some(msg)) => Some(msg.clone()),
            (None, None) => None,
        }
    }
}

/// Bitget order book level.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BitgetBookLevel(pub String, pub String);

/// Bitget order book payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetBookData {
    /// Asks.
    #[serde(default, rename = "a")]
    pub asks: Vec<BitgetBookLevel>,
    /// Bids.
    #[serde(default, rename = "b")]
    pub bids: Vec<BitgetBookLevel>,
    /// Current sequence number.
    #[serde(default)]
    pub seq: Option<i64>,
    /// Previous sequence number.
    #[serde(default)]
    pub pseq: Option<i64>,
    /// Checksum when provided by Bitget.
    #[serde(default)]
    pub checksum: Option<i64>,
    /// Event timestamp in milliseconds.
    #[serde(default)]
    pub ts: Option<String>,
}

/// Bitget public trade payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetPublicTradeData {
    /// Raw Bitget symbol, when present.
    #[serde(default)]
    pub symbol: Option<String>,
    /// Exchange trade ID.
    #[serde(default, rename = "i")]
    pub trade_id: String,
    /// Trade price.
    #[serde(default, rename = "p")]
    pub price: String,
    /// Trade size.
    #[serde(default, rename = "v")]
    pub size: String,
    /// Aggressor side (`buy` or `sell`).
    #[serde(default, rename = "S")]
    pub side: String,
    /// Exchange timestamp in milliseconds.
    #[serde(default, rename = "T")]
    pub ts: String,
}

impl From<BitgetPublicTradeData> for BitgetMarketTrade {
    fn from(value: BitgetPublicTradeData) -> Self {
        Self {
            symbol: value.symbol,
            trade_id: value.trade_id,
            price: value.price,
            size: value.size,
            side: value.side,
            ts: value.ts,
        }
    }
}

/// Bitget ticker payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetTickerData {
    /// Raw Bitget symbol, when present.
    #[serde(default)]
    pub symbol: Option<String>,
    /// Last traded price.
    #[serde(default, rename = "lastPrice")]
    pub last_price: Option<String>,
    /// Best bid price.
    #[serde(default, rename = "bid1Price")]
    pub bid1_price: Option<String>,
    /// Best bid size.
    #[serde(default, rename = "bid1Size")]
    pub bid1_size: Option<String>,
    /// Best ask price.
    #[serde(default, rename = "ask1Price")]
    pub ask1_price: Option<String>,
    /// Best ask size.
    #[serde(default, rename = "ask1Size")]
    pub ask1_size: Option<String>,
    /// Mark price for derivatives.
    #[serde(default, rename = "markPrice")]
    pub mark_price: Option<String>,
    /// Index price for derivatives.
    #[serde(default, rename = "indexPrice")]
    pub index_price: Option<String>,
    /// Current funding rate for perpetuals.
    #[serde(default)]
    pub funding_rate: Option<String>,
    /// Next funding timestamp in milliseconds.
    #[serde(default)]
    pub next_funding_time: Option<String>,
}

/// Bitget private order payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsOrderData {
    /// Raw Bitget symbol/instrument.
    #[serde(default)]
    pub symbol: Option<String>,
    /// Bitget product type, when included.
    #[serde(default, rename = "category")]
    pub product_type: Option<String>,
    /// Venue order ID.
    #[serde(default)]
    pub order_id: Option<String>,
    /// Client order ID.
    #[serde(default)]
    pub client_oid: Option<String>,
    /// Order side.
    #[serde(default)]
    pub side: Option<String>,
    /// Futures trade side/open-close qualifier.
    #[serde(default)]
    pub trade_side: Option<String>,
    /// Futures position side.
    #[serde(default)]
    pub pos_side: Option<String>,
    /// Order type.
    #[serde(default)]
    pub order_type: Option<String>,
    /// Time in force.
    #[serde(default, rename = "timeInForce")]
    pub time_in_force: Option<String>,
    /// Order status.
    #[serde(default, rename = "orderStatus")]
    pub status: Option<String>,
    /// Order price.
    #[serde(default)]
    pub price: Option<String>,
    /// Average fill price.
    #[serde(default, rename = "avgPrice")]
    pub avg_price: Option<String>,
    /// Order size.
    #[serde(default, rename = "qty")]
    pub size: Option<String>,
    /// Filled size.
    #[serde(default, rename = "cumExecQty")]
    pub filled_size: Option<String>,
    /// Quote quantity/filled notional when provided.
    #[serde(default, rename = "cumExecValue")]
    pub quote_size: Option<String>,
    /// Reduce-only flag as represented by Bitget.
    #[serde(default)]
    pub reduce_only: Option<String>,
    /// Trigger price for plan/conditional orders.
    #[serde(default)]
    pub trigger_price: Option<String>,
    /// Trigger type for plan/conditional orders.
    #[serde(default)]
    pub trigger_type: Option<String>,
    /// Margin coin for futures.
    #[serde(default)]
    pub margin_coin: Option<String>,
    /// Creation timestamp in milliseconds.
    #[serde(default, rename = "createdTime")]
    pub c_time: Option<String>,
    /// Update timestamp in milliseconds.
    #[serde(default, rename = "updatedTime")]
    pub u_time: Option<String>,
}

/// Bitget private fill payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsFillData {
    /// Raw Bitget symbol/instrument.
    #[serde(default)]
    pub symbol: Option<String>,
    /// Bitget product type, when included.
    #[serde(default, rename = "category")]
    pub product_type: Option<String>,
    /// Venue trade/fill ID.
    #[serde(default, rename = "execId")]
    pub fill_id: Option<String>,
    /// Venue order ID.
    #[serde(default)]
    pub order_id: Option<String>,
    /// Client order ID.
    #[serde(default)]
    pub client_oid: Option<String>,
    /// Fill side.
    #[serde(default)]
    pub side: Option<String>,
    /// Futures trade side/open-close qualifier.
    #[serde(default)]
    pub trade_side: Option<String>,
    /// Liquidity role.
    #[serde(default, rename = "tradeScope")]
    pub role: Option<String>,
    /// Fill price.
    #[serde(default, rename = "execPrice")]
    pub price: Option<String>,
    /// Fill size.
    #[serde(default, rename = "execQty")]
    pub size: Option<String>,
    /// Fill notional/quote size.
    #[serde(default, rename = "execValue")]
    pub quote_size: Option<String>,
    /// Fee amount.
    #[serde(default)]
    pub fee: Option<String>,
    /// Fee currency.
    #[serde(default, rename = "feeCoin")]
    pub fee_currency: Option<String>,
    /// UTA fee details.
    #[serde(default)]
    pub fee_detail: Option<BitgetFillFeeDetail>,
    /// Margin coin for futures.
    #[serde(default)]
    pub margin_coin: Option<String>,
    /// Fill timestamp in milliseconds.
    #[serde(default, rename = "execTime")]
    pub c_time: Option<String>,
}

/// Bitget private account payload.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsAccountData {
    /// Asset coin for spot account pushes.
    #[serde(default)]
    pub coin: Option<String>,
    /// Margin coin for futures account pushes.
    #[serde(default)]
    pub margin_coin: Option<String>,
    /// UTA account asset rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assets: Vec<BitgetUtaAsset>,
    /// Available balance.
    #[serde(default, rename = "available")]
    pub available_balance: Option<String>,
    /// Frozen/locked amount.
    #[serde(default, rename = "locked")]
    pub locked: Option<String>,
    /// Futures equity.
    #[serde(default, rename = "totalEquity")]
    pub account_equity: Option<String>,
    /// USDT converted equity when provided.
    #[serde(default)]
    pub usdt_equity: Option<String>,
    /// Unrealized PnL.
    #[serde(default, rename = "unrealisedPnL")]
    pub unrealized_pnl: Option<String>,
    /// Initial margin requirement.
    #[serde(default)]
    pub imr: Option<String>,
    /// Maintenance margin requirement.
    #[serde(default)]
    pub mmr: Option<String>,
    /// Creation timestamp in milliseconds.
    #[serde(default, rename = "createdTime")]
    pub c_time: Option<String>,
    /// Update timestamp in milliseconds.
    #[serde(default, rename = "updatedTime")]
    pub u_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetWsAccountDataRaw {
    #[serde(default)]
    coin: Option<Value>,
    #[serde(default)]
    margin_coin: Option<String>,
    #[serde(default, rename = "available")]
    available_balance: Option<String>,
    #[serde(default, rename = "locked")]
    locked: Option<String>,
    #[serde(default, rename = "totalEquity")]
    account_equity: Option<String>,
    #[serde(default)]
    usdt_equity: Option<String>,
    #[serde(default, rename = "unrealisedPnL")]
    unrealized_pnl: Option<String>,
    #[serde(default)]
    imr: Option<String>,
    #[serde(default)]
    mmr: Option<String>,
    #[serde(default, rename = "createdTime")]
    c_time: Option<String>,
    #[serde(default, rename = "updatedTime")]
    u_time: Option<String>,
}

impl<'de> Deserialize<'de> for BitgetWsAccountData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw =
            BitgetWsAccountDataRaw::deserialize(deserializer).map_err(serde::de::Error::custom)?;

        let assets = match raw.coin {
            Some(value @ Value::Array(_)) => {
                serde_json::from_value(value).map_err(serde::de::Error::custom)?
            }
            Some(Value::Null) | None => Vec::new(),
            Some(other) => {
                return Err(serde::de::Error::custom(format!(
                    "unsupported Bitget account coin payload: {other:?}"
                )));
            }
        };

        Ok(Self {
            coin: None,
            margin_coin: raw.margin_coin,
            assets,
            available_balance: raw.available_balance,
            locked: raw.locked,
            account_equity: raw.account_equity,
            usdt_equity: raw.usdt_equity,
            unrealized_pnl: raw.unrealized_pnl,
            imr: raw.imr,
            mmr: raw.mmr,
            c_time: raw.c_time,
            u_time: raw.u_time,
        })
    }
}

/// Bitget private position payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetWsPositionData {
    /// Raw Bitget symbol/instrument.
    #[serde(default)]
    pub symbol: Option<String>,
    /// Position ID.
    #[serde(default)]
    pub pos_id: Option<String>,
    /// Margin coin.
    #[serde(default)]
    pub margin_coin: Option<String>,
    /// Hold side.
    #[serde(default, rename = "posSide")]
    pub hold_side: Option<String>,
    /// Position mode.
    #[serde(default, rename = "holdMode")]
    pub pos_mode: Option<String>,
    /// Margin mode.
    #[serde(default)]
    pub margin_mode: Option<String>,
    /// Position quantity.
    #[serde(default, rename = "size")]
    pub total: Option<String>,
    /// Available quantity.
    #[serde(default)]
    pub available: Option<String>,
    /// Average open price.
    #[serde(default, rename = "avgPrice")]
    pub average_open_price: Option<String>,
    /// Mark price.
    #[serde(default)]
    pub mark_price: Option<String>,
    /// Liquidation price.
    #[serde(default)]
    pub liquidation_price: Option<String>,
    /// Leverage.
    #[serde(default)]
    pub leverage: Option<String>,
    /// Realized PnL.
    #[serde(default, rename = "curRealisedPnl")]
    pub realized_pnl: Option<String>,
    /// Unrealized PnL.
    #[serde(default, rename = "unrealisedPnl")]
    pub unrealized_pnl: Option<String>,
    /// Creation timestamp in milliseconds.
    #[serde(default, rename = "createdTime")]
    pub c_time: Option<String>,
    /// Update timestamp in milliseconds.
    #[serde(default, rename = "updatedTime")]
    pub u_time: Option<String>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn subscribe_command_serializes_expected_shape() {
        let command = BitgetWsCommand::subscribe(vec![BitgetWsArg {
            inst_type: "usdt-futures".to_string(),
            topic: "books".to_string(),
            symbol: Some("BTCUSDT".to_string()),
            interval: None,
            coin: None,
        }])
        .unwrap();

        let value = serde_json::to_value(command).unwrap();

        assert_eq!(value["op"], "subscribe");
        assert_eq!(value["args"][0]["instType"], "usdt-futures");
        assert_eq!(value["args"][0]["topic"], "books");
        assert_eq!(value["args"][0]["symbol"], "BTCUSDT");
    }

    #[rstest]
    fn kline_arg_serializes_v3_shape() {
        let command = BitgetWsCommand::subscribe(vec![BitgetWsArg::kline(
            BitgetProductType::UsdtFutures,
            "BTCUSDT",
            "1m",
        )])
        .unwrap();

        let value = serde_json::to_value(command).unwrap();

        assert_eq!(value["args"][0]["instType"], "usdt-futures");
        assert_eq!(value["args"][0]["topic"], "kline");
        assert_eq!(value["args"][0]["symbol"], "BTCUSDT");
        assert_eq!(value["args"][0]["interval"], "1m");
        assert!(
            !value["args"][0]["topic"]
                .as_str()
                .unwrap()
                .starts_with("candle")
        );
    }

    #[rstest]
    fn book_event_deserializes_sequence_fields() {
        let raw = r#"{
            "action":"update",
            "arg":{"instType":"spot","topic":"books","symbol":"BTCUSDT"},
            "data":[{
                "a":[["100.1","0.2"]],
                "b":[["100.0","0.3"]],
                "seq":11,
                "pseq":10,
                "checksum":123,
                "ts":"1700000000000"
            }]
        }"#;

        let event: BitgetWsEvent<BitgetBookData> = serde_json::from_str(raw).unwrap();

        assert_eq!(event.action.as_deref(), Some("update"));
        assert_eq!(event.data[0].seq, Some(11));
        assert_eq!(event.data[0].pseq, Some(10));
        assert_eq!(event.data[0].checksum, Some(123));
    }

    #[rstest]
    fn arg_topic_key_includes_product_topic_and_symbol() {
        let arg = BitgetWsArg::new(
            BitgetProductType::UsdtFutures,
            "publicTrade",
            Some("BTCUSDT".to_string()),
        );

        assert_eq!(arg.topic_key(), "usdt-futures:publicTrade:BTCUSDT");
    }

    #[rstest]
    fn kline_arg_topic_key_includes_interval() {
        let arg = BitgetWsArg::kline(BitgetProductType::UsdtFutures, "BTCUSDT", "1m");

        assert_eq!(arg.topic_key(), "usdt-futures:kline:BTCUSDT:interval:1m");
    }

    #[rstest]
    fn account_arg_serializes_uta_private_topic() {
        let command = BitgetWsCommand::subscribe(vec![BitgetWsArg::account()]).unwrap();

        let value = serde_json::to_value(command).unwrap();

        assert_eq!(value["args"][0]["instType"], "UTA");
        assert_eq!(value["args"][0]["topic"], "account");
        assert!(value["args"][0].get("symbol").is_none());
    }

    #[rstest]
    fn text_pong_parses_as_control_message() {
        let msg = BitgetWsMessage::parse_text("pong").unwrap();

        assert_eq!(msg, BitgetWsMessage::Pong);
    }

    #[rstest]
    fn subscribe_ack_parses_as_subscribe_message() {
        let raw = r#"{
            "event":"subscribe",
            "arg":{"instType":"usdt-futures","topic":"publicTrade","symbol":"BTCUSDT"}
        }"#;

        let msg = BitgetWsMessage::parse_text(raw).unwrap();

        assert!(matches!(msg, BitgetWsMessage::Subscribe(_)));
    }

    #[rstest]
    fn login_ack_parses_documented_string_success_code() {
        let raw = r#"{"event":"login","code":"0","msg":""}"#;

        let msg = BitgetWsMessage::parse_text(raw).unwrap();

        assert!(msg.is_login_success());
        assert_eq!(
            msg.event().and_then(|event| event.code.as_deref()),
            Some("0"),
        );
    }

    #[rstest]
    fn login_ack_normalizes_demo_integer_success_code() {
        let raw = r#"{"event":"login","code":0,"msg":""}"#;

        let msg = BitgetWsMessage::parse_text(raw).unwrap();

        assert!(msg.is_login_success());
        assert_eq!(
            msg.event().and_then(|event| event.code.as_deref()),
            Some("0"),
        );
    }

    #[rstest]
    fn login_ack_rejects_non_code_json_types() {
        let raw = r#"{"event":"login","code":true,"msg":""}"#;

        assert!(BitgetWsMessage::parse_text(raw).is_err());
    }

    #[rstest]
    fn data_push_parses_as_data_message() {
        let raw = r#"{
            "action":"snapshot",
            "arg":{"instType":"usdt-futures","topic":"ticker","symbol":"BTCUSDT"},
            "data":[{"lastPrice":"100.0"}]
        }"#;

        let msg = BitgetWsMessage::parse_text(raw).unwrap();

        assert!(matches!(msg, BitgetWsMessage::Data(_)));
    }

    #[rstest]
    fn error_ack_parses_as_error_message() {
        let raw = r#"{
            "event":"error",
            "code":"30001",
            "msg":"topic does not exist",
            "arg":{"instType":"spot","topic":"bad","symbol":"BTCUSDT"}
        }"#;

        let msg = BitgetWsMessage::parse_text(raw).unwrap();

        assert!(matches!(msg, BitgetWsMessage::Error(_)));
        assert_eq!(
            msg.error_message().as_deref(),
            Some("30001: topic does not exist"),
        );
    }

    #[rstest]
    fn ticker_data_accepts_uta_prices() {
        let raw = r#"{
            "symbol":"BTCUSDT",
            "lastPrice":"100.1",
            "bid1Price":"100.0",
            "bid1Size":"1.5",
            "ask1Price":"100.4",
            "ask1Size":"2.5",
            "markPrice":"100.2",
            "indexPrice":"100.3",
            "fundingRate":"0.0001",
            "nextFundingTime":"1700003600000"
        }"#;

        let ticker: BitgetTickerData = serde_json::from_str(raw).unwrap();

        assert_eq!(ticker.symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(ticker.last_price.as_deref(), Some("100.1"));
        assert_eq!(ticker.bid1_price.as_deref(), Some("100.0"));
        assert_eq!(ticker.bid1_size.as_deref(), Some("1.5"));
        assert_eq!(ticker.ask1_price.as_deref(), Some("100.4"));
        assert_eq!(ticker.ask1_size.as_deref(), Some("2.5"));
        assert_eq!(ticker.mark_price.as_deref(), Some("100.2"));
        assert_eq!(ticker.index_price.as_deref(), Some("100.3"));
        assert_eq!(ticker.funding_rate.as_deref(), Some("0.0001"));
    }

    #[rstest]
    fn private_order_data_accepts_uta_order_payload() {
        let raw = r#"{
            "symbol":"BTCUSDT",
            "category":"USDT-FUTURES",
            "orderId":"123",
            "clientOid":"abc",
            "side":"buy",
            "tradeSide":"open",
            "timeInForce":"gtc",
            "orderStatus":"live",
            "avgPrice":"100.1",
            "qty":"0.01",
            "cumExecQty":"0.005",
            "reduceOnly":"no",
            "createdTime":"1700000000000",
            "updatedTime":"1700000000100"
        }"#;

        let order: BitgetWsOrderData = serde_json::from_str(raw).unwrap();

        assert_eq!(order.symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(order.order_id.as_deref(), Some("123"));
        assert_eq!(order.client_oid.as_deref(), Some("abc"));
        assert_eq!(order.time_in_force.as_deref(), Some("gtc"));
        assert_eq!(order.avg_price.as_deref(), Some("100.1"));
        assert_eq!(order.size.as_deref(), Some("0.01"));
        assert_eq!(order.filled_size.as_deref(), Some("0.005"));
    }

    #[rstest]
    fn private_fill_account_and_position_data_parse_optional_fields() {
        let fill: BitgetWsFillData = serde_json::from_str(
            r#"{
                "symbol":"BTCUSDT",
                "execId":"fill-1",
                "orderId":"order-1",
                "clientOid":"client-1",
                "execPrice":"100.2",
                "execQty":"0.01",
                "feeDetail":[{"feeCoin":"USDT","fee":"-0.01"}],
                "execTime":"1700000000000"
            }"#,
        )
        .unwrap();
        let account: BitgetWsAccountData = serde_json::from_str(
            r#"{
                "totalEquity":"1200",
                "unrealisedPnL":"10",
                "imr":"20",
                "mmr":"5",
                "coin":[{
                    "coin":"USDT",
                    "available":"1000",
                    "locked":"1",
                    "equity":"1200"
                }],
                "updatedTime":"1700000000000"
            }"#,
        )
        .unwrap();
        let position: BitgetWsPositionData = serde_json::from_str(
            r#"{
                "symbol":"BTCUSDT",
                "posId":"pos-1",
                "posSide":"long",
                "size":"0.5",
                "avgPrice":"100.0",
                "markPrice":"101.0",
                "unrealisedPnl":"0.5"
            }"#,
        )
        .unwrap();

        assert_eq!(fill.fill_id.as_deref(), Some("fill-1"));
        assert!(fill.fee_detail.is_some());
        assert_eq!(account.account_equity.as_deref(), Some("1200"));
        assert_eq!(account.assets[0].coin.as_deref(), Some("USDT"));
        assert_eq!(account.assets[0].available.as_deref(), Some("1000"));
        assert_eq!(position.symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(position.average_open_price.as_deref(), Some("100.0"));
    }
}
