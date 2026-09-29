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

//! Classic WebSocket protocol codec. The rest of the adapter consumes normalized events.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    common::enums::BitgetProductType,
    websocket::{
        error::BitgetWsResult,
        messages::{BitgetWsArg, BitgetWsMessage},
    },
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClassicArg {
    inst_type: String,
    channel: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inst_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    coin: Option<String>,
}

pub(crate) fn command(
    op: &str,
    args: &[BitgetWsArg],
    product: BitgetProductType,
) -> BitgetWsResult<String> {
    let args: Vec<_> = args
        .iter()
        .map(|arg| {
            let channel = match arg.topic.as_str() {
                "order" => "orders".to_string(),
                "position" => "positions".to_string(),
                "strategy-order" => "orders-algo".to_string(),
                "publicTrade" => "trade".to_string(),
                "kline" => format!("candle{}", arg.interval.as_deref().unwrap_or("1m")),
                "books50" => "books".to_string(),
                other => other.to_string(),
            };
            let account = channel == "account";
            ClassicArg {
                inst_type: product.as_api_str().to_string(),
                channel,
                inst_id: if account {
                    None
                } else {
                    Some(arg.symbol.clone().unwrap_or_else(|| "default".to_string()))
                },
                coin: account.then(|| arg.coin.clone().unwrap_or_else(|| "default".to_string())),
            }
        })
        .collect();
    Ok(serde_json::to_string(&json!({"op":op,"args":args}))?)
}

fn rename(value: &mut Value, from: &str, to: &str) {
    if let Some(map) = value.as_object_mut()
        && let Some(field) = map.remove(from)
    {
        map.insert(to.to_string(), field);
    }
}

fn string_field(value: &mut Value, name: &str) {
    if let Some(field) = value.get_mut(name)
        && field.is_number()
    {
        *field = Value::String(field.to_string());
    }
}

pub(crate) fn parse(text: &str) -> BitgetWsResult<BitgetWsMessage> {
    if text == "pong" || text == nautilus_network::RECONNECTED {
        return BitgetWsMessage::parse_text(text);
    }
    let mut event: Value = serde_json::from_str(text)?;
    let Some(raw_arg) = event.get("arg").cloned() else {
        return BitgetWsMessage::parse_text(text);
    };
    let arg: ClassicArg = serde_json::from_value(raw_arg)?;
    let topic = match arg.channel.as_str() {
        "orders" => "order",
        "orders-algo" => "strategy-order",
        "positions" => "position",
        "trade" => "publicTrade",
        "books15" => "books50",
        other if other.starts_with("candle") => "kline",
        other => other,
    };
    let product = BitgetProductType::from_api_str(&arg.inst_type);
    let mut normalized_arg = BitgetWsArg::new(
        product.unwrap_or(BitgetProductType::Spot),
        topic,
        arg.inst_id.clone(),
    );
    if topic == "kline" {
        normalized_arg.interval = Some(arg.channel.trim_start_matches("candle").to_string());
    }
    normalized_arg.coin = arg.coin;
    event["arg"] = serde_json::to_value(normalized_arg)?;
    let mut timestamp = event.get("ts").cloned();
    if let Some(rows) = event.get_mut("data").and_then(Value::as_array_mut) {
        for row in rows {
            if timestamp.is_none() {
                timestamp = row.get("ts").cloned();
            }
            if product == Some(BitgetProductType::UsdtFutures) {
                for key in ["symbol", "instId"] {
                    if let Some(Value::String(symbol)) = row.get_mut(key) {
                        symbol.make_ascii_uppercase();
                    }
                }
            }
            if let Some(detail) = row.get_mut("feeDetail") {
                super::models::normalize_fee_detail(detail)?;
            }
            match topic {
                "order" | "strategy-order" => {
                    for (from, to) in [
                        ("instId", "symbol"),
                        ("status", "orderStatus"),
                        ("planStatus", "orderStatus"),
                        ("size", "qty"),
                        ("accBaseVolume", "cumExecQty"),
                        ("priceAvg", "avgPrice"),
                        ("force", "timeInForce"),
                        ("cTime", "createdTime"),
                        ("uTime", "updatedTime"),
                    ] {
                        rename(row, from, to);
                    }
                    if topic == "strategy-order" {
                        if let Some(id) = row
                            .get("executeOrderId")
                            .and_then(Value::as_str)
                            .filter(|s| !s.is_empty() && *s != "0")
                            .map(str::to_owned)
                        {
                            row["orderId"] = json!(id);
                        }
                        if row.get("timeInForce").is_none() {
                            row["timeInForce"] = json!("gtc");
                        }
                        match row.get("orderStatus").and_then(Value::as_str) {
                            Some("executed") => row["orderStatus"] = json!("triggered"),
                            Some("fail_execute") => row["orderStatus"] = json!("canceled"),
                            _ => {}
                        }
                    }
                    if let Some(product) = product {
                        row["category"] = json!(product.as_api_str());
                    }
                }
                "fill" => {
                    for (from, to) in [
                        ("instId", "symbol"),
                        ("tradeId", "execId"),
                        ("price", "execPrice"),
                        ("baseVolume", "execQty"),
                        ("quoteVolume", "execValue"),
                        ("cTime", "execTime"),
                        ("ts", "execTime"),
                    ] {
                        rename(row, from, to);
                    }
                    if row.get("execTime").is_none() {
                        rename(row, "fillTime", "execTime");
                    }
                    if row.get("execPrice").is_none() {
                        rename(row, "fillPrice", "execPrice");
                    }
                    if row.get("fee").is_none() {
                        rename(row, "fillFee", "fee");
                    }
                    if row.get("feeCoin").is_none() {
                        rename(row, "fillFeeCoin", "feeCoin");
                    }
                    if row.get("execQty").is_none() {
                        rename(row, "size", "execQty");
                    }
                    if row.get("execId").is_none() {
                        rename(row, "fillId", "execId");
                    }
                    if let Some(product) = product {
                        row["category"] = json!(product.as_api_str());
                    }
                }
                "position" => {
                    for (from, to) in [
                        ("instId", "symbol"),
                        ("holdSide", "posSide"),
                        ("posMode", "holdMode"),
                        ("total", "size"),
                        ("openPriceAvg", "avgPrice"),
                        ("achievedProfits", "curRealisedPnl"),
                        ("unrealizedPL", "unrealisedPnl"),
                        ("cTime", "createdTime"),
                        ("uTime", "updatedTime"),
                    ] {
                        rename(row, from, to);
                    }
                    if row.get("avgPrice").is_none() {
                        rename(row, "averageOpenPrice", "avgPrice");
                    }
                }
                "account" => {
                    for (from, to) in [
                        ("frozen", "locked"),
                        ("equity", "totalEquity"),
                        ("unrealizedPL", "unrealisedPnL"),
                        ("cTime", "createdTime"),
                        ("uTime", "updatedTime"),
                    ] {
                        rename(row, from, to);
                    }
                }
                "publicTrade" => {
                    for (from, to) in [
                        ("tradeId", "i"),
                        ("price", "p"),
                        ("size", "v"),
                        ("side", "S"),
                        ("ts", "T"),
                    ] {
                        rename(row, from, to);
                    }
                    string_field(row, "i");
                    string_field(row, "T");
                }
                "ticker" => {
                    for (from, to) in [
                        ("instId", "symbol"),
                        ("lastPr", "lastPrice"),
                        ("bidPr", "bid1Price"),
                        ("askPr", "ask1Price"),
                        ("bidSz", "bid1Size"),
                        ("askSz", "ask1Size"),
                    ] {
                        rename(row, from, to);
                    }
                }
                _ => {}
            }
        }
    }
    if let Some(timestamp) = timestamp {
        event["ts"] = timestamp;
    }
    BitgetWsMessage::parse_text(&serde_json::to_string(&event)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::websocket::messages::{
        BitgetPublicTradeData, BitgetTickerData, BitgetWsAccountData, BitgetWsFillData,
        BitgetWsOrderData, BitgetWsPositionData,
    };

    fn row(channel: &str, product: &str, data: Value) -> Value {
        let raw = json!({"arg":{"instType":product,"channel":channel,"instId":"BTCUSDT"},"action":"snapshot","data":[data],"ts":1700000000000_i64});
        let message = parse(&raw.to_string()).unwrap();
        message.event().unwrap().data[0].clone()
    }

    #[test]
    fn classic_subscriptions_use_product_channel_and_scope() {
        for product in [BitgetProductType::Spot, BitgetProductType::UsdtFutures] {
            let payload = command(
                "subscribe",
                &[BitgetWsArg::private("order", None), BitgetWsArg::account()],
                product,
            )
            .unwrap();
            let value: Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(value["args"][0]["instType"], product.as_api_str());
            assert_eq!(value["args"][0]["channel"], "orders");
            assert_eq!(value["args"][0]["instId"], "default");
            assert_eq!(value["args"][1]["channel"], "account");
            assert_eq!(value["args"][1]["coin"], "default");
            assert!(value["args"][0].get("topic").is_none());
        }
        let payload = command(
            "unsubscribe",
            &[BitgetWsArg::kline(BitgetProductType::Spot, "BTCUSDT", "1m")],
            BitgetProductType::Spot,
        )
        .unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["args"][0]["channel"], "candle1m");
        assert!(value["args"][0].get("interval").is_none());
    }

    #[test]
    fn classic_private_events_preserve_identifiers_quantities_and_fees() {
        let order: BitgetWsOrderData=serde_json::from_value(row("orders","SPOT",json!({"instId":"BTCUSDT","orderId":"123","status":"partially_filled","size":"1","accBaseVolume":"0.2","priceAvg":"100","force":"gtc","cTime":"1700000000000","uTime":"1700000000001"}))).unwrap();
        assert_eq!(order.symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(order.status.as_deref(), Some("partially_filled"));
        assert_eq!(order.filled_size.as_deref(), Some("0.2"));
        assert_eq!(order.avg_price.as_deref(), Some("100"));
        assert_eq!(order.u_time.as_deref(), Some("1700000000001"));
        let fill: BitgetWsFillData=serde_json::from_value(row("fill","USDT-FUTURES",json!({"symbol":"BTCUSDT","orderId":"123","tradeId":"456","price":"100","baseVolume":"0.2","quoteVolume":"20","cTime":"1700000000000","feeDetail":[{"feeCoin":"USDT","totalFee":"-0.01"}]}))).unwrap();
        assert_eq!(fill.fill_id.as_deref(), Some("456"));
        assert_eq!(fill.size.as_deref(), Some("0.2"));
        assert_eq!(fill.c_time.as_deref(), Some("1700000000000"));
        let fees = serde_json::to_value(fill.fee_detail).unwrap();
        assert_eq!(fees[0]["fee"], "-0.01");
        let position: BitgetWsPositionData=serde_json::from_value(row("positions","USDT-FUTURES",json!({"instId":"BTCUSDT","holdSide":"short","total":"2","openPriceAvg":"100","unrealizedPL":"3","cTime":"1700000000000"}))).unwrap();
        assert_eq!(position.hold_side.as_deref(), Some("short"));
        assert_eq!(position.total.as_deref(), Some("2"));
        assert_eq!(position.average_open_price.as_deref(), Some("100"));
        assert_eq!(position.unrealized_pnl.as_deref(), Some("3"));
        let account: BitgetWsAccountData = serde_json::from_value(row(
            "account",
            "SPOT",
            json!({"coin":"USDT","available":"10","frozen":"2","uTime":"1700000000000"}),
        ))
        .unwrap();
        assert_eq!(account.coin.as_deref(), Some("USDT"));
        assert_eq!(account.locked.as_deref(), Some("2"));
        assert_eq!(account.available_balance.as_deref(), Some("10"));
    }

    #[test]
    fn classic_public_events_reach_shared_market_models() {
        let ticker: BitgetTickerData=serde_json::from_value(row("ticker","SPOT",json!({"instId":"BTCUSDT","lastPr":"100","bidPr":"99","askPr":"101","bidSz":"1","askSz":"2","ts":"1700000000000"}))).unwrap();
        assert_eq!(ticker.bid1_price.as_deref(), Some("99"));
        assert_eq!(ticker.ask1_size.as_deref(), Some("2"));
        let trade: BitgetPublicTradeData = serde_json::from_value(row(
            "trade",
            "SPOT",
            json!({"tradeId":"123","price":"100","size":"1","side":"buy","ts":"1700000000000"}),
        ))
        .unwrap();
        assert_eq!(trade.trade_id, "123");
        assert_eq!(trade.ts, "1700000000000");
        let parsed=parse(r#"{"arg":{"instType":"SPOT","channel":"candle1m","instId":"BTCUSDT"},"data":[["1700000000000","1","2","1","2","10","20"]],"action":"snapshot"}"#).unwrap();
        assert_eq!(
            parsed
                .event()
                .unwrap()
                .arg
                .as_ref()
                .unwrap()
                .interval
                .as_deref(),
            Some("1m")
        );
    }
}
