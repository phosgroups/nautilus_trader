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
    http::models::BitgetFill,
    websocket::{
        error::BitgetWsResult,
        messages::{BitgetWsArg, BitgetWsMessage, BitgetWsOrderData},
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

/// Classic sends its latest execution alongside the order's cumulative state.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetClassicOrderData {
    #[serde(flatten)]
    pub order: BitgetWsOrderData,
    #[serde(default)]
    trade_id: Option<String>,
    #[serde(default)]
    fill_price: Option<String>,
    #[serde(default)]
    base_volume: Option<String>,
    #[serde(default)]
    fill_time: Option<String>,
    #[serde(default)]
    fill_fee: Option<String>,
    #[serde(default)]
    fill_fee_coin: Option<String>,
    #[serde(default)]
    trade_scope: Option<String>,
}

impl BitgetClassicOrderData {
    pub(crate) fn latest_fill(&self) -> Option<BitgetFill> {
        use rust_decimal::Decimal;

        let trade_id = self.trade_id.as_deref()?.trim();
        if trade_id.is_empty() || trade_id == "0" {
            return None;
        }
        // New/cancel pushes can have empty or zero execution placeholders.
        if self.base_volume.as_deref().is_none_or(|quantity| {
            quantity.trim().is_empty()
                || quantity
                    .trim()
                    .parse::<Decimal>()
                    .is_ok_and(|q| q.is_zero())
        }) {
            return None;
        }
        Some(BitgetFill {
            symbol: self.order.symbol.clone(),
            product_type: self.order.product_type.clone(),
            order_id: self.order.order_id.clone(),
            client_oid: self.order.client_oid.clone(),
            trade_id: Some(trade_id.to_string()),
            side: self.order.side.clone(),
            trade_side: self.order.trade_side.clone(),
            margin_coin: self.order.margin_coin.clone(),
            price: self.fill_price.clone(),
            size: self.base_volume.clone(),
            fee: self.fill_fee.clone(),
            fee_coin: self.fill_fee_coin.clone(),
            trade_scope: self.trade_scope.clone(),
            c_time: self.fill_time.clone(),
            // feeDetail is cumulative for the order; use the latest fillFee above.
            ..Default::default()
        })
    }
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

fn rename_first(value: &mut Value, to: &str, aliases: &[&str]) {
    fn present(value: &Value) -> bool {
        !value.is_null() && value.as_str().is_none_or(|s| !s.trim().is_empty())
    }

    let Some(map) = value.as_object_mut() else {
        return;
    };
    if map.get(to).is_some_and(present) {
        return;
    }
    for alias in aliases {
        if map.get(*alias).is_some_and(present) {
            let field = map.remove(*alias).expect("alias checked above");
            map.insert(to.to_string(), field);
            return;
        }
    }
}

fn normalize_fill(row: &mut Value) {
    for (to, aliases) in [
        ("symbol", &["instId"][..]),
        ("clientOid", &["clientOId"][..]),
        ("execId", &["tradeId", "fillId"][..]),
        ("execPrice", &["fillPrice", "price", "priceAvg"][..]),
        ("execQty", &["baseVolume", "size"][..]),
        ("execValue", &["quoteVolume", "amount"][..]),
        ("execTime", &["fillTime", "cTime", "ts"][..]),
        ("fee", &["fillFee"][..]),
        ("feeCoin", &["fillFeeCoin"][..]),
    ] {
        rename_first(row, to, aliases);
    }
    // The Classic Spot fill example uses "marker" for the documented maker role.
    if row.get("tradeScope").and_then(Value::as_str) == Some("marker") {
        row["tradeScope"] = json!("maker");
    }
}

fn string_field(value: &mut Value, name: &str) {
    if let Some(field) = value.get_mut(name)
        && field.is_number()
    {
        *field = Value::String(field.to_string());
    }
}

fn integer_field(value: &mut Value, name: &str) {
    if let Some(field) = value.get_mut(name)
        && let Some(raw) = field.as_str()
        && let Ok(number) = raw.parse::<i64>()
    {
        *field = json!(number);
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
                "books" | "books1" | "books5" | "books50" => {
                    rename_first(row, "a", &["asks"]);
                    rename_first(row, "b", &["bids"]);
                    string_field(row, "ts");
                    for key in ["seq", "pseq"] {
                        integer_field(row, key);
                    }
                }
                "order" | "strategy-order" => {
                    // Spot's newSize uses base units for limit orders, whereas size
                    // on a buy-side push can be denominated in the quote currency.
                    rename_first(row, "qty", &["newSize", "size"]);
                    rename_first(row, "orderType", &["ordType"]);
                    rename_first(row, "clientOid", &["clientOId"]);
                    for (from, to) in [
                        ("instId", "symbol"),
                        ("status", "orderStatus"),
                        ("planStatus", "orderStatus"),
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
                            Some("executing" | "executed") => {
                                row["orderStatus"] = json!("triggered");
                            }
                            Some("fail_execute") => row["orderStatus"] = json!("canceled"),
                            _ => {}
                        }
                    }
                    if let Some(product) = product {
                        row["category"] = json!(product.as_api_str());
                    }
                }
                "fill" => {
                    normalize_fill(row);
                    if let Some(product) = product {
                        row["category"] = json!(product.as_api_str());
                    }
                }
                "position" => {
                    // Classic documents leverage as a number in position pushes.
                    string_field(row, "leverage");
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
    use rstest::rstest;

    use super::*;
    use crate::websocket::messages::{
        BitgetBookData, BitgetPublicTradeData, BitgetTickerData, BitgetWsAccountData,
        BitgetWsFillData, BitgetWsOrderData, BitgetWsPositionData,
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
        assert_eq!(account.frozen.as_deref(), Some("2"));
        assert_eq!(account.available_balance.as_deref(), Some("10"));
    }

    #[rstest]
    #[case(json!({"priceAvg":"100", "size":"0.2", "amount":"20", "cTime":"1700000000000"}))]
    #[case(json!({"fillPrice":"100", "baseVolume":"0.2", "quoteVolume":"20", "fillTime":"1700000000000"}))]
    #[case(json!({"price":null, "fillPrice":"100", "baseVolume":"0.2", "cTime":"1700000000001", "fillTime":"1700000000000", "amount":"20"}))]
    #[case(json!({"execPrice":" ", "priceAvg":"100", "execQty":null, "size":"0.2", "execValue":"", "amount":"20", "execTime":null, "cTime":"1700000000000"}))]
    fn classic_spot_fills_normalize_price_quantity_notional_and_time(#[case] data: Value) {
        let fill: BitgetWsFillData = serde_json::from_value(row("fill", "SPOT", data)).unwrap();

        assert_eq!(fill.price.as_deref(), Some("100"));
        assert_eq!(fill.size.as_deref(), Some("0.2"));
        assert_eq!(fill.quote_size.as_deref(), Some("20"));
        assert_eq!(fill.c_time.as_deref(), Some("1700000000000"));
    }

    #[test]
    fn classic_fill_aliases_preserve_canonical_fields_and_prefer_execution_details() {
        let fill: BitgetWsFillData = serde_json::from_value(row(
            "fill",
            "SPOT",
            json!({
                "execPrice":"101", "fillPrice":"100", "price":"99", "priceAvg":"98",
                "execQty":"0.2", "baseVolume":"0.3", "size":"1",
                "execValue":"20.2", "quoteVolume":"30", "amount":"99",
                "execTime":"1700000000000", "fillTime":"1700000000001",
                "cTime":"1700000000002", "ts":"1700000000003"
            }),
        ))
        .unwrap();
        assert_eq!(fill.price.as_deref(), Some("101"));
        assert_eq!(fill.size.as_deref(), Some("0.2"));
        assert_eq!(fill.quote_size.as_deref(), Some("20.2"));
        assert_eq!(fill.c_time.as_deref(), Some("1700000000000"));

        let fill: BitgetWsFillData = serde_json::from_value(row(
            "fill",
            "SPOT",
            json!({"fillPrice":"101", "price":"100", "priceAvg":"99", "cTime":"1700000000000", "ts":"1700000000001"}),
        ))
        .unwrap();
        assert_eq!(fill.price.as_deref(), Some("101"));
        assert_eq!(fill.c_time.as_deref(), Some("1700000000000"));
    }

    #[test]
    fn classic_orders_preserve_documented_quantity_type_and_client_id() {
        let order: BitgetWsOrderData = serde_json::from_value(row(
            "orders",
            "SPOT",
            json!({"newSize":"0.2", "size":"20", "ordType":"limit", "clientOId":"C-1"}),
        ))
        .unwrap();
        assert_eq!(order.size.as_deref(), Some("0.2"));
        assert_eq!(order.order_type.as_deref(), Some("limit"));
        assert_eq!(order.client_oid.as_deref(), Some("C-1"));
    }

    #[test]
    fn classic_position_push_accepts_documented_numeric_leverage() {
        let position: BitgetWsPositionData = serde_json::from_value(row(
            "positions",
            "USDT-FUTURES",
            json!({"instId":"BTCUSDT", "holdSide":"short", "total":"0.1", "openPriceAvg":"1900", "leverage":20}),
        ))
        .unwrap();
        assert_eq!(position.leverage.as_deref(), Some("20"));
        assert_eq!(position.total.as_deref(), Some("0.1"));
        assert_eq!(position.average_open_price.as_deref(), Some("1900"));
    }

    #[rstest]
    #[case("books")]
    #[case("books1")]
    #[case("books5")]
    #[case("books15")]
    fn classic_orderbook_push_preserves_levels_and_sequence(#[case] channel: &str) {
        let book: BitgetBookData = serde_json::from_value(row(
            channel,
            "SPOT",
            json!({
                "asks":[["100.20", "0.4"]], "bids":[["100.10", "0.5"]],
                "seq":123, "pseq":"122", "ts":1700000000000_i64
            }),
        ))
        .unwrap();
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.asks[0].0, "100.20");
        assert_eq!(book.bids[0].1, "0.5");
        assert_eq!(book.seq, Some(123));
        assert_eq!(book.pseq, Some(122));
        assert_eq!(book.ts.as_deref(), Some("1700000000000"));
    }

    #[rstest]
    #[case("executing")]
    #[case("executed")]
    fn classic_trigger_order_status_accepts_execution_states(#[case] status: &str) {
        let order: BitgetWsOrderData = serde_json::from_value(row(
            "orders-algo",
            "USDT-FUTURES",
            json!({"instId":"BTCUSDT", "status":status, "size":"0.02", "triggerPrice":"27000"}),
        ))
        .unwrap();
        assert_eq!(order.status.as_deref(), Some("triggered"));

        let rest: super::super::models::BitgetOrderStatus = serde_json::from_value(json!({
            "symbol":"BTCUSDT", "planStatus":status, "size":"0.02", "triggerPrice":"27000"
        }))
        .unwrap();
        let shared: crate::http::models::BitgetOrderStatus = rest.into();
        assert_eq!(shared.status.as_deref(), Some("triggered"));
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
