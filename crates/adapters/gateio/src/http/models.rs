use std::fmt;

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, Visitor},
};

pub(crate) fn string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringVisitor;
    impl<'de> Visitor<'de> for StringVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a string, number, or null")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value)
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(String::new())
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(String::new())
        }
    }
    deserializer.deserialize_any(StringVisitor)
}

pub(crate) fn optional_string_or_number<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(serde_json::Value::String(value)) => Some(value),
        Some(serde_json::Value::Number(value)) => Some(value.to_string()),
        Some(serde_json::Value::Bool(value)) => Some(value.to_string()),
        Some(serde_json::Value::Null) | None => None,
        Some(value) => Some(value.to_string()),
    })
}

pub(crate) fn optional_i64_or_number<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(serde_json::Value::Number(value)) => value.as_i64(),
        Some(serde_json::Value::String(value)) => value.parse::<i64>().ok(),
        Some(serde_json::Value::Bool(value)) => Some(i64::from(value)),
        Some(serde_json::Value::Null) | None => None,
        Some(value) => value.to_string().parse::<i64>().ok(),
    })
}

pub(crate) fn optional_u64_or_number<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(serde_json::Value::Number(value)) => value.as_u64(),
        Some(serde_json::Value::String(value)) => value.parse::<u64>().ok(),
        Some(serde_json::Value::Null) | None => None,
        Some(value) => value.to_string().parse::<u64>().ok(),
    })
}

/// Parses Gate.io timestamps, accepting either seconds or milliseconds.
///
/// Gate.io uses both integer and fractional seconds in futures responses, while
/// some spot responses already return milliseconds. Keeping this normalization at
/// the serde boundary prevents each adapter path from guessing the unit again.
pub(crate) fn optional_timestamp_millis<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };

    let raw = match value {
        serde_json::Value::String(value) => value,
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Null => return Ok(None),
        other => other.to_string(),
    };
    let parsed = raw
        .parse::<f64>()
        .map_err(|error| de::Error::custom(format!("invalid timestamp {raw:?}: {error}")))?;
    if !parsed.is_finite() || parsed < 0.0 {
        return Err(de::Error::custom(format!(
            "timestamp must be finite and non-negative, was {raw:?}"
        )));
    }

    let millis = if parsed < 100_000_000_000.0 {
        parsed * 1_000.0
    } else {
        parsed
    };
    if millis > i64::MAX as f64 {
        return Err(de::Error::custom(format!(
            "timestamp is too large, was {raw:?}"
        )));
    }
    Ok(Some(millis.round() as i64))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GateioSpotPair {
    pub id: String,
    #[serde(default)]
    pub base: Option<String>,
    #[serde(default)]
    pub base_name: Option<String>,
    #[serde(default)]
    pub quote: Option<String>,
    #[serde(default)]
    pub quote_name: Option<String>,
    #[serde(default)]
    pub trade_quotes: Option<Vec<String>>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub fee: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub min_base_amount: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub max_base_amount: Option<String>,
    #[serde(default)]
    pub precision: Option<u32>,
    #[serde(default)]
    pub amount_precision: Option<u32>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub amount_point: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub min_quote_amount: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub max_quote_amount: Option<String>,
    #[serde(default)]
    pub trade_status: Option<String>,
    #[serde(default)]
    pub sell_start: Option<i64>,
    #[serde(default)]
    pub buy_start: Option<i64>,
    #[serde(default)]
    pub delisting_time: Option<i64>,
    #[serde(default, rename = "type")]
    pub type_: Option<String>,
    #[serde(default)]
    pub trade_url: Option<String>,
    #[serde(default)]
    pub st_tag: Option<bool>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub up_rate: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub down_rate: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub slippage: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub market_order_max_stock: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub market_order_max_money: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GateioContract {
    pub name: String,
    #[serde(default, rename = "type")]
    pub type_: Option<String>,
    #[serde(default)]
    pub contract_type: Option<String>,
    #[serde(default, deserialize_with = "string_or_number")]
    pub quanto_multiplier: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub order_price_round: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub order_size_min: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub order_size_max: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub maker_fee_rate: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub taker_fee_rate: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub funding_interval: Option<i64>,
    #[serde(default, deserialize_with = "optional_i64_or_number")]
    pub funding_next_apply: Option<i64>,
    #[serde(default)]
    pub create_time: Option<i64>,
    #[serde(default)]
    pub trade_id: Option<i64>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub trade_size: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub position_size: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub maintenance_rate: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub mark_price: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub index_price: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub last_price: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub funding_rate: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub risk_limit_max: Option<String>,
    #[serde(default)]
    pub enable_decimal: Option<bool>,
    #[serde(default)]
    pub in_delisting: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum GateioLevel {
    Spot([String; 2]),
    Futures {
        #[serde(
            default,
            alias = "p",
            alias = "price",
            deserialize_with = "string_or_number"
        )]
        price: String,
        #[serde(
            default,
            alias = "s",
            alias = "amount",
            alias = "size",
            deserialize_with = "string_or_number"
        )]
        amount: String,
    },
}

impl<'de> Deserialize<'de> for GateioLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Array(values) if values.len() == 2 => {
                let price = scalar_to_string(&values[0]).map_err(de::Error::custom)?;
                let amount = scalar_to_string(&values[1]).map_err(de::Error::custom)?;
                Ok(Self::Spot([price, amount]))
            }
            serde_json::Value::Object(values) => {
                let price = values
                    .get("p")
                    .or_else(|| values.get("price"))
                    .ok_or_else(|| de::Error::custom("Gate.io book level has no price"))?;
                let amount = values
                    .get("s")
                    .or_else(|| values.get("amount"))
                    .or_else(|| values.get("size"))
                    .ok_or_else(|| de::Error::custom("Gate.io book level has no amount"))?;
                Ok(Self::Futures {
                    price: scalar_to_string(price).map_err(de::Error::custom)?,
                    amount: scalar_to_string(amount).map_err(de::Error::custom)?,
                })
            }
            _ => Err(de::Error::custom(
                "Gate.io book level must be a two-item array or an object",
            )),
        }
    }
}

fn scalar_to_string(value: &serde_json::Value) -> Result<String, &'static str> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Null => Ok(String::new()),
        _ => Err("Gate.io book level value must be a scalar"),
    }
}

impl GateioLevel {
    pub fn price(&self) -> &str {
        match self {
            Self::Spot(level) => &level[0],
            Self::Futures { price, .. } => price,
        }
    }

    pub fn amount(&self) -> &str {
        match self {
            Self::Spot(level) => &level[1],
            Self::Futures { amount, .. } => amount,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GateioOrderBook {
    #[serde(default, alias = "lastUpdateId")]
    pub id: Option<u64>,
    #[serde(default)]
    pub full: bool,
    #[serde(default, alias = "t", deserialize_with = "optional_timestamp_millis")]
    pub current: Option<i64>,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub update: Option<i64>,
    #[serde(default, alias = "u", deserialize_with = "optional_u64_or_number")]
    pub sequence: Option<u64>,
    #[serde(
        default,
        alias = "U",
        alias = "first_update_id",
        deserialize_with = "optional_u64_or_number"
    )]
    pub first_sequence: Option<u64>,
    #[serde(default)]
    pub currency_pair: Option<String>,
    #[serde(default)]
    pub contract: Option<String>,
    #[serde(default)]
    pub bids: Vec<GateioLevel>,
    #[serde(default)]
    pub asks: Vec<GateioLevel>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GateioTrade {
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub id: Option<String>,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub create_time_ms: Option<i64>,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub create_time: Option<i64>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub price: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub amount: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub size: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
}

impl GateioTrade {
    pub fn price(&self) -> anyhow::Result<&str> {
        self.price
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("Gate.io trade has no price"))
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct GateioCandle {
    pub timestamp_ms: i64,
    pub volume: String,
    pub close: String,
    pub high: String,
    pub low: String,
    pub open: String,
}

impl<'de> Deserialize<'de> for GateioCandle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let row = match value {
            serde_json::Value::Array(row) => row,
            serde_json::Value::Object(object) => vec![
                object.get("t").cloned().unwrap_or(serde_json::Value::Null),
                object
                    .get("v")
                    .cloned()
                    .or_else(|| object.get("sum").cloned())
                    .unwrap_or(serde_json::Value::Null),
                object.get("c").cloned().unwrap_or(serde_json::Value::Null),
                object.get("h").cloned().unwrap_or(serde_json::Value::Null),
                object.get("l").cloned().unwrap_or(serde_json::Value::Null),
                object.get("o").cloned().unwrap_or(serde_json::Value::Null),
            ],
            _ => {
                return Err(de::Error::custom(
                    "Gate.io candle must be an array or object",
                ));
            }
        };
        let get = |index: usize| -> Result<String, D::Error> {
            row.get(index)
                .cloned()
                .ok_or_else(|| de::Error::custom(format!("missing candle index {index}")))
                .map(|value| value.to_string().trim_matches('"').to_string())
        };
        let timestamp = get(0)?
            .parse::<f64>()
            .map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(Self {
            timestamp_ms: if timestamp < 100_000_000_000.0 {
                (timestamp * 1_000.0) as i64
            } else {
                timestamp as i64
            },
            volume: get(1)?,
            close: get(2)?,
            high: get(3)?,
            low: get(4)?,
            open: get(5)?,
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GateioTicker {
    #[serde(default, alias = "s")]
    pub contract: Option<String>,
    #[serde(default)]
    pub currency_pair: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub last: Option<String>,
    #[serde(default, alias = "b", deserialize_with = "optional_string_or_number")]
    pub highest_bid: Option<String>,
    #[serde(default, alias = "a", deserialize_with = "optional_string_or_number")]
    pub lowest_ask: Option<String>,
    #[serde(default, alias = "B", deserialize_with = "optional_string_or_number")]
    pub highest_size: Option<String>,
    #[serde(default, alias = "A", deserialize_with = "optional_string_or_number")]
    pub lowest_size: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub mark_price: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub index_price: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub funding_rate: Option<String>,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub funding_next_apply: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GateioFundingRate {
    #[serde(default, alias = "r", deserialize_with = "string_or_number")]
    pub rate: String,
    #[serde(default, alias = "t", deserialize_with = "optional_timestamp_millis")]
    pub timestamp_ms: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GateioAccount {
    #[serde(default)]
    pub currency: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub available: String,
    #[serde(default, alias = "freeze", deserialize_with = "string_or_number")]
    pub locked: String,
    #[serde(default, alias = "balance", deserialize_with = "string_or_number")]
    pub equity: String,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub total: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub unrealised_pnl: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub position_margin: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub order_margin: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub maintenance_margin: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GateioPosition {
    #[serde(default)]
    pub contract: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub size: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub value: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub entry_price: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub mark_price: String,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub update_time: Option<i64>,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub create_time: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GateioOrder {
    #[serde(default, deserialize_with = "string_or_number")]
    pub id: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub currency_pair: Option<String>,
    #[serde(default)]
    pub contract: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default, deserialize_with = "string_or_number")]
    pub amount: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub size: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub left: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub price: String,
    #[serde(default, rename = "type")]
    pub type_: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub tif: Option<String>,
    #[serde(default)]
    pub time_in_force: Option<String>,
    #[serde(default)]
    pub finish_as: Option<String>,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub create_time_ms: Option<i64>,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub update_time_ms: Option<i64>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub fill_price: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub filled_total: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub filled_amount: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub filled_size: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub avg_deal_price: Option<String>,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub fee: Option<String>,
    #[serde(default)]
    pub fee_currency: Option<String>,
    #[serde(default)]
    pub reduce_only: Option<bool>,
    #[serde(default)]
    pub is_reduce_only: Option<bool>,
    #[serde(default)]
    pub is_liq: Option<bool>,
    #[serde(default)]
    pub auto_size: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GateioUserTrade {
    #[serde(default, deserialize_with = "string_or_number")]
    pub id: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub order_id: String,
    #[serde(default)]
    pub currency_pair: Option<String>,
    #[serde(default)]
    pub contract: Option<String>,
    #[serde(default, deserialize_with = "string_or_number")]
    pub price: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub amount: String,
    #[serde(default, deserialize_with = "string_or_number")]
    pub size: String,
    #[serde(default, deserialize_with = "optional_string_or_number")]
    pub fee: Option<String>,
    #[serde(default)]
    pub fee_currency: Option<String>,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub create_time_ms: Option<i64>,
    #[serde(default, deserialize_with = "optional_timestamp_millis")]
    pub create_time: Option<i64>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GateioErrorResponse {
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub message: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct GateioOrderRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency_pair: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub side: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub amount: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tif: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct GateioOrderAmendRequest {
    pub amount: Option<String>,
    pub size: Option<String>,
    pub price: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct GateioCancelAllRequest {
    pub currency_pair: Option<String>,
    pub contract: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize)]
    struct ScalarRow {
        #[serde(deserialize_with = "string_or_number")]
        value: String,
    }

    #[derive(Debug, Deserialize)]
    struct OptionalTimestampRow {
        #[serde(deserialize_with = "optional_timestamp_millis")]
        value: Option<i64>,
    }

    #[test]
    fn deserializes_strings_numbers_and_empty_optional_values() {
        assert_eq!(
            serde_json::from_value::<ScalarRow>(serde_json::json!({"value": 12.5}))
                .unwrap()
                .value,
            "12.5"
        );
        assert_eq!(
            serde_json::from_value::<ScalarRow>(serde_json::json!({"value": null}))
                .unwrap()
                .value,
            ""
        );
        assert_eq!(
            serde_json::from_value::<GateioOrder>(serde_json::json!({
                "id": 123,
                "amount": 1.25,
                "size": -3,
                "left": 0,
                "price": 50000.0
            }))
            .unwrap()
            .size,
            "-3"
        );
    }

    #[test]
    fn deserializes_futures_book_ticker_aliases() {
        let ticker = serde_json::from_value::<GateioTicker>(serde_json::json!({
            "s": "BTC_USDT",
            "b": "79471.8",
            "B": 32548,
            "a": 79471.9,
            "A": "1707"
        }))
        .unwrap();
        assert_eq!(ticker.contract.as_deref(), Some("BTC_USDT"));
        assert_eq!(ticker.highest_bid.as_deref(), Some("79471.8"));
        assert_eq!(ticker.highest_size.as_deref(), Some("32548"));
        assert_eq!(ticker.lowest_ask.as_deref(), Some("79471.9"));
        assert_eq!(ticker.lowest_size.as_deref(), Some("1707"));
    }

    #[test]
    fn normalizes_second_and_millisecond_timestamps() {
        assert_eq!(
            serde_json::from_value::<OptionalTimestampRow>(serde_json::json!({
                "value": 1700000000
            }))
            .unwrap()
            .value,
            Some(1_700_000_000_000)
        );
        assert_eq!(
            serde_json::from_value::<OptionalTimestampRow>(serde_json::json!({
                "value": "1700000000.5"
            }))
            .unwrap()
            .value,
            Some(1_700_000_000_500)
        );
        assert_eq!(
            serde_json::from_value::<OptionalTimestampRow>(serde_json::json!({
                "value": null
            }))
            .unwrap()
            .value,
            None
        );
    }

    #[test]
    fn deserializes_spot_and_futures_book_levels() {
        let spot = serde_json::from_value::<GateioOrderBook>(serde_json::json!({
            "id": 100,
            "bids": [["50000.0", "0.25"]],
            "asks": [[50001.0, 0.5]]
        }))
        .unwrap();
        assert_eq!(spot.bids[0].price(), "50000.0");
        assert_eq!(spot.asks[0].amount(), "0.5");

        let futures = serde_json::from_value::<GateioOrderBook>(serde_json::json!({
            "u": 102,
            "U": 101,
            "bids": [{"p": "50000.0", "s": "3"}],
            "asks": [{"price": 50001.0, "size": -2}]
        }))
        .unwrap();
        assert_eq!(futures.sequence, Some(102));
        assert_eq!(futures.first_sequence, Some(101));
        assert_eq!(futures.bids[0].amount(), "3");
        assert_eq!(futures.asks[0].amount(), "-2");
    }

    #[test]
    fn deserializes_array_and_object_candles() {
        let array = serde_json::from_value::<GateioCandle>(serde_json::json!([
            1700000000, "100", "50001", "50002", "49999", "50000"
        ]))
        .unwrap();
        assert_eq!(array.timestamp_ms, 1_700_000_000_000);
        assert_eq!(array.close, "50001");

        let object = serde_json::from_value::<GateioCandle>(serde_json::json!({
            "t": 1700000000000i64,
            "sum": "10",
            "c": 50001,
            "h": "50002",
            "l": "49999",
            "o": "50000"
        }))
        .unwrap();
        assert_eq!(object.timestamp_ms, 1_700_000_000_000);
        assert_eq!(object.volume, "10");
    }

    #[test]
    fn serializes_spot_and_futures_order_requests() {
        let spot = GateioOrderRequest {
            currency_pair: Some("BTC_USDT".to_string()),
            type_: Some("limit".to_string()),
            account: Some("spot".to_string()),
            side: "buy".to_string(),
            amount: "0.1".to_string(),
            price: Some("50000".to_string()),
            time_in_force: Some("gtc".to_string()),
            ..Default::default()
        };
        let spot_json = serde_json::to_value(spot).unwrap();
        assert_eq!(spot_json["currency_pair"], "BTC_USDT");
        assert_eq!(spot_json["amount"], "0.1");
        assert!(spot_json.get("size").is_none());

        let futures = GateioOrderRequest {
            contract: Some("BTC_USDT".to_string()),
            size: "-3".to_string(),
            price: Some("0".to_string()),
            tif: Some("ioc".to_string()),
            reduce_only: Some(true),
            ..Default::default()
        };
        let futures_json = serde_json::to_value(futures).unwrap();
        assert_eq!(futures_json["contract"], "BTC_USDT");
        assert_eq!(futures_json["size"], "-3");
        assert_eq!(futures_json["price"], "0");
        assert_eq!(futures_json["tif"], "ioc");
        assert_eq!(futures_json["reduce_only"], true);
        assert!(futures_json.get("side").is_none());

        let amend = serde_json::to_value(GateioOrderAmendRequest {
            amount: None,
            size: Some("-2".to_string()),
            price: Some("49900".to_string()),
        })
        .unwrap();
        assert_eq!(amend["size"], "-2");
        assert_eq!(amend["price"], "49900");
        assert!(amend.get("amount").is_some_and(serde_json::Value::is_null));
    }
}
