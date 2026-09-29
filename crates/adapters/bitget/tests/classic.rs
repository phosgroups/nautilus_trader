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

//! Contract tests for explicit Classic selection, using documented v2 wire shapes.

use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
};
use nautilus_bitget::{
    common::{
        enums::{BitgetAccountMode, BitgetProductType},
        order::*,
    },
    config::{BitgetDataClientConfig, BitgetExecClientConfig},
    http::{client::BitgetRawHttpClient, models::*},
};
use serde_json::{Value, json};

#[derive(Clone, Default)]
struct Fixture {
    response: Arc<Mutex<Value>>,
    plan_response: Arc<Mutex<Option<Value>>>,
    error: Arc<Mutex<Option<String>>>,
    requests: Arc<Mutex<Vec<(String, String, Value)>>>,
}

async fn handle(State(fixture): State<Fixture>, request: Request) -> Json<Value> {
    let uri = request.uri().to_string();
    let method = request.method().to_string();
    if !uri.contains("/market/") && !uri.contains("/public/") {
        assert!(request.headers().contains_key("ACCESS-SIGN"));
    }
    let bytes = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    let is_plan_query = uri.contains("plan-order?") || uri.contains("orders-plan-");
    let response = if is_plan_query {
        fixture
            .plan_response
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| json!({"orderList":[],"nextFlag":false,"entrustedList":[]}))
    } else {
        fixture.response.lock().unwrap().clone()
    };
    fixture.requests.lock().unwrap().push((method, uri, body));
    let code = fixture
        .error
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| "00000".to_string());
    Json(json!({"code":code,"msg":"fixture response","data":response}))
}

async fn setup() -> (BitgetRawHttpClient, Fixture, tokio::task::JoinHandle<()>) {
    let state = Fixture::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(handle).with_state(state.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = BitgetRawHttpClient::with_credentials(
        "key".into(),
        "secret".into(),
        "pass".into(),
        Some(format!("http://{address}")),
        5,
        None,
    )
    .unwrap()
    .with_account_mode(BitgetAccountMode::Classic);
    (client, state, task)
}

impl Fixture {
    fn respond(&self, value: Value) {
        *self.response.lock().unwrap() = value;
    }
    fn last(&self) -> (String, String, Value) {
        self.requests.lock().unwrap().last().unwrap().clone()
    }
    fn assert_path(&self, path: &str) {
        assert_eq!(self.last().1.split('?').next().unwrap(), path);
    }
}

#[test]
fn old_configs_remain_uta_and_classic_is_explicit() {
    use nautilus_bitget::{
        common::enums::BitgetEnvironment, websocket::client::BitgetWebSocketClient,
    };
    use nautilus_network::websocket::TransportBackend;

    let old: BitgetExecClientConfig = serde_json::from_value(json!({})).unwrap();
    assert_eq!(old.account_mode, BitgetAccountMode::Uta);
    assert!(old.ws_private_url().contains("/v3/"));
    let classic: BitgetExecClientConfig =
        serde_json::from_value(json!({"account_mode":"classic"})).unwrap();
    assert_eq!(
        classic.ws_private_url(),
        "wss://ws.bitget.com/v2/ws/private"
    );
    let data: BitgetDataClientConfig = serde_json::from_value(
        json!({"account_mode":"classic", "base_url_ws_public":"ws://localhost/mock"}),
    )
    .unwrap();
    assert_eq!(data.ws_public_url(), "ws://localhost/mock");
    assert!(
        serde_json::from_value::<BitgetExecClientConfig>(json!({"account_mode":"auto"})).is_err()
    );
    let custom = "wss://ws.bitget.com/v3/ws/public";
    let ws = BitgetWebSocketClient::new_public(
        BitgetProductType::Spot,
        BitgetEnvironment::Mainnet,
        Some(custom.to_string()),
        30,
        TransportBackend::default(),
        None,
    )
    .with_account_mode(BitgetAccountMode::Classic);
    assert_eq!(ws.url(), custom);
    let ws = BitgetWebSocketClient::new_public(
        BitgetProductType::Spot,
        BitgetEnvironment::Mainnet,
        None,
        30,
        TransportBackend::default(),
        None,
    )
    .with_account_mode(BitgetAccountMode::Classic);
    assert_eq!(ws.url(), "wss://ws.bitget.com/v2/ws/public");
    assert_eq!(ws.with_account_mode(BitgetAccountMode::Uta).url(), custom);
}

#[tokio::test]
async fn classic_market_and_account_responses_use_classic_shapes() {
    let (client, fixture, task) = setup().await;
    fixture.respond(json!([{"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","minTradeAmount":"0.0001","minTradeUSDT":"5","pricePrecision":"2","quantityPrecision":"4","status":"online"}]));
    let symbols = client.request_spot_symbols().await.unwrap();
    assert_eq!(symbols[0].min_trade_amount.as_deref(), Some("0.0001"));
    assert_eq!(symbols[0].min_trade_usdt.as_deref(), Some("5"));
    fixture.assert_path("/api/v2/spot/public/symbols");

    fixture.respond(json!([{"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","symbolType":"perpetual","pricePlace":"1","priceEndStep":"1","volumePlace":"3","sizeMultiplier":"0.001","minTradeNum":"0.001","minTradeUSDT":"5","symbolStatus":"normal"}]));
    let contracts = client.request_usdt_futures_contracts().await.unwrap();
    assert_eq!(contracts[0].price_end_step.as_deref(), Some("1"));
    assert_eq!(contracts[0].contract_type.as_deref(), Some("perpetual"));
    fixture.assert_path("/api/v2/mix/market/contracts");
    assert!(fixture.last().1.contains("productType=USDT-FUTURES"));
    assert!(!fixture.last().1.contains("category"));

    fixture.respond(json!({"bids":[["100","1"]],"asks":[["101","2"]],"ts":"1700000000000"}));
    for (product, path) in [
        (BitgetProductType::Spot, "/api/v2/spot/market/orderbook"),
        (
            BitgetProductType::UsdtFutures,
            "/api/v2/mix/market/merge-depth",
        ),
    ] {
        let book = client
            .request_orderbook(product, "BTCUSDT", Some(10))
            .await
            .unwrap();
        assert_eq!(book.bids.len(), 1);
        fixture.assert_path(path);
    }
    fixture.respond(
        json!([{"tradeId":"123","price":"100","size":"1","side":"buy","ts":"1700000000000"}]),
    );
    assert_eq!(
        client
            .request_market_trades(BitgetProductType::Spot, "BTCUSDT", None, None, None)
            .await
            .unwrap()[0]
            .trade_id,
        "123"
    );
    fixture.assert_path("/api/v2/spot/market/fills");
    fixture.respond(json!([[
        "1700000000000",
        "100",
        "101",
        "99",
        "100",
        "1",
        "100"
    ]]));
    assert_eq!(
        client
            .request_candles(BitgetProductType::Spot, "BTCUSDT", "1m", None, None, None)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(fixture.last().1.contains("granularity=1min"));
    fixture.respond(
        json!([{"symbol":"BTCUSDT","fundingRate":"0.0001","fundingTime":"1700000000000"}]),
    );
    assert_eq!(
        client
            .request_funding_rates("BTCUSDT", None, None, None)
            .await
            .unwrap()[0]
            .funding_time,
        "1700000000000"
    );

    fixture.respond(
        json!([{"coin":"USDT","available":"10","frozen":"2","locked":"1","uTime":"1700000000000"}]),
    );
    let assets = client.request_spot_account_assets(None).await.unwrap();
    assert_eq!(assets[0].frozen.as_deref(), Some("2"));
    assert_eq!(assets[0].u_time.as_deref(), Some("1700000000000"));
    fixture.assert_path("/api/v2/spot/account/assets");
    fixture.respond(json!([{"marginCoin":"USDT","available":"10","accountEquity":"15","crossedMargin":"3","unrealizedPL":"2"}]));
    let accounts = client
        .request_mix_accounts(BitgetProductType::UsdtFutures)
        .await
        .unwrap();
    assert_eq!(accounts[0].crossed_margin.as_deref(), Some("3"));
    assert_eq!(accounts[0].unrealized_pnl.as_deref(), Some("2"));
    fixture.assert_path("/api/v2/mix/account/accounts");
    fixture.respond(json!([{"symbol":"BTCUSDT","holdSide":"long","total":"0.01","openPriceAvg":"100","unrealizedPL":"2"}]));
    let positions = client
        .request_mix_positions(BitgetProductType::UsdtFutures, Some("BTCUSDT"))
        .await
        .unwrap();
    assert_eq!(positions[0].total.as_deref(), Some("0.01"));
    assert_eq!(positions[0].open_price_avg.as_deref(), Some("100"));
    fixture.assert_path("/api/v2/mix/position/single-position");
    task.abort();
}

#[tokio::test]
async fn classic_order_commands_preserve_scope_and_wire_parameters() {
    let (client, fixture, task) = setup().await;
    fixture.respond(json!({"orderId":"123","clientOid":"O-123"}));
    let spot = BitgetSpotPlaceOrderRequest {
        category: "SPOT".into(),
        symbol: "BTCUSDT".into(),
        side: "sell".into(),
        order_type: "limit".into(),
        price: Some("100".into()),
        size: "0.1".into(),
        force: Some("gtc".into()),
        ..Default::default()
    };
    client
        .submit_order(&BitgetSubmitOrderRequest::Spot(spot))
        .await
        .unwrap();
    fixture.assert_path("/api/v2/spot/trade/place-order");
    let body = fixture.last().2;
    assert_eq!(body["size"], "0.1");
    assert_eq!(body["force"], "gtc");
    assert!(body.get("category").is_none());
    assert!(body.get("qty").is_none());
    let mix = BitgetMixPlaceOrderRequest {
        symbol: "BTCUSDT".into(),
        product_type: "USDT-FUTURES".into(),
        margin_coin: "USDT".into(),
        margin_mode: "crossed".into(),
        size: "0.1".into(),
        order_type: "market".into(),
        side: "sell".into(),
        reduce_only: Some("yes".into()),
        ..Default::default()
    };
    client
        .submit_order(&BitgetSubmitOrderRequest::Mix(mix))
        .await
        .unwrap();
    fixture.assert_path("/api/v2/mix/order/place-order");
    assert_eq!(fixture.last().2["marginCoin"], "USDT");
    assert_eq!(fixture.last().2["reduceOnly"], "YES");
    let spot_plan = BitgetSpotPlanOrderRequest {
        symbol: "BTCUSDT".into(),
        size: "0.1".into(),
        trigger_type: "market".into(),
        ..Default::default()
    };
    client
        .submit_order(&BitgetSubmitOrderRequest::SpotPlan(spot_plan))
        .await
        .unwrap();
    assert_eq!(fixture.last().2["triggerType"], "fill_price");
    assert_eq!(fixture.last().2["planType"], "amount");
    let plan = BitgetMixPlanOrderRequest {
        symbol: "BTCUSDT".into(),
        product_type: "USDT-FUTURES".into(),
        margin_coin: "USDT".into(),
        trigger_type: "mark".into(),
        execute_price: Some("100".into()),
        ..Default::default()
    };
    client
        .submit_order(&BitgetSubmitOrderRequest::MixPlan(plan))
        .await
        .unwrap();
    fixture.assert_path("/api/v2/mix/order/place-plan-order");
    assert_eq!(fixture.last().2["triggerType"], "mark_price");
    assert_eq!(fixture.last().2["planType"], "normal_plan");
    assert_eq!(fixture.last().2["price"], "100");
    client
        .modify_order(&BitgetModifyOrderRequest::MixPlan(
            BitgetMixModifyPlanOrderRequest {
                order_id: "123".into(),
                product_type: "USDT-FUTURES".into(),
                size: Some("2".into()),
                execute_price: Some("101".into()),
                trigger_price: Some("100".into()),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    let body = fixture.last().2;
    assert_eq!(body["newSize"], "2");
    assert_eq!(body["newPrice"], "101");
    assert_eq!(body["newTriggerPrice"], "100");

    fixture.respond(json!({"successList":[],"failureList":[{"orderId":"123","errorCode":"43001","errorMsg":"not found"}]}));
    let result = client
        .cancel_all_orders(&BitgetCancelAllOrdersRequest::Mix(
            BitgetMixBatchCancelOrdersRequest {
                product_type: "USDT-FUTURES".into(),
                symbol: Some("BTCUSDT".into()),
                margin_coin: Some("USDT".into()),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    fixture.assert_path("/api/v2/mix/order/batch-cancel-orders");
    assert_eq!(fixture.last().2["symbol"], "BTCUSDT");
    assert!(fixture.last().2.get("orderIdList").is_none());
    assert_eq!(result.failure_list[0].code.as_deref(), Some("43001"));
    assert_eq!(
        result.failure_list[0].error_msg.as_deref(),
        Some("not found")
    );
    task.abort();
}

#[tokio::test]
async fn classic_reports_decode_quantities_fees_and_pagination() {
    let (client, fixture, task) = setup().await;
    fixture.respond(json!([{"symbol":"BTCUSDT","orderId":"123","status":"partially_filled","size":"1","baseVolume":"0.2","priceAvg":"100","cTime":"1700000000000"}]));
    let row = client
        .request_order_status(BitgetProductType::Spot, "BTCUSDT", Some("123"), None)
        .await
        .unwrap();
    assert_eq!(row.status.as_deref(), Some("partially_filled"));
    assert_eq!(row.filled_size.as_deref(), Some("0.2"));
    assert_eq!(row.price_avg.as_deref(), Some("100"));
    fixture.assert_path("/api/v2/spot/trade/orderInfo");
    fixture.respond(json!([{"symbol":"BTCUSDT","tradeId":"456","orderId":"123","priceAvg":"100","size":"0.2","amount":"20","feeDetail":{"feeCoin":"BTC","totalFee":"-0.0001"},"cTime":"1700000000000"}]));
    let fills = client
        .request_fills(
            BitgetProductType::Spot,
            Some("BTCUSDT"),
            None,
            None,
            Some(1),
        )
        .await
        .unwrap();
    assert_eq!(fills[0].price.as_deref(), Some("100"));
    assert_eq!(fills[0].size.as_deref(), Some("0.2"));
    match fills[0].fee_detail.as_ref().unwrap() {
        BitgetFillFeeDetail::Entry(fee) => assert_eq!(fee.total_fee.as_deref(), Some("-0.0001")),
        _ => panic!("missing fee"),
    }
    // Repeated-cursor protection ends this fixture's pagination, without silently sending UTA cursor.
    assert!(fixture.last().1.contains("idLessThan=456"));
    assert!(!fixture.last().1.contains("cursor="));
    fixture.respond(json!({"entrustedList":[{"symbol":"BTCUSDT","orderId":"789","status":"live","size":"1"}],"endId":"789"}));
    let orders = client
        .request_order_statuses(
            BitgetProductType::UsdtFutures,
            None,
            None,
            None,
            true,
            Some(1),
        )
        .await
        .unwrap();
    assert!(!orders.is_empty());
    assert!(
        fixture
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(_, uri, _)| uri.contains("idLessThan=789"))
    );
    task.abort();
}

#[derive(Clone, Default)]
struct WsFixture {
    subscriptions: Arc<Mutex<Vec<Value>>>,
    logins: Arc<std::sync::atomic::AtomicUsize>,
}

async fn upgrade_ws(
    State(state): State<WsFixture>,
    upgrade: axum::extract::ws::WebSocketUpgrade,
) -> axum::response::Response {
    upgrade.on_upgrade(move |mut socket| async move {
        use axum::extract::ws::Message;
        use std::sync::atomic::Ordering;
        while let Some(Ok(message)) = socket.recv().await {
            let Message::Text(text) = message else { continue };
            if text == "ping" { socket.send(Message::Text("pong".into())).await.unwrap(); continue; }
            let value: Value = serde_json::from_str(&text).unwrap();
            match value["op"].as_str().unwrap() {
                "login" => {
                    let timestamp = value["args"][0]["timestamp"].as_str().unwrap();
                    assert_eq!(timestamp.len(), 10, "Classic login timestamp is in seconds");
                    let credential = nautilus_bitget::common::credential::Credential::new("key", "secret", "pass");
                    assert_eq!(value["args"][0]["sign"], credential.sign_websocket_login(timestamp));
                    state.logins.fetch_add(1,Ordering::SeqCst);
                    socket.send(Message::Text(json!({"event":"login","code":0}).to_string().into())).await.unwrap();
                }
                "subscribe" => {
                    state.subscriptions.lock().unwrap().push(value.clone());
                    for arg in value["args"].as_array().unwrap() {
                        socket.send(Message::Text(json!({"event":"subscribe","arg":arg}).to_string().into())).await.unwrap();
                        if arg["channel"] == "orders" {
                            socket.send(Message::Text(json!({"action":"update","arg":arg,"data":[{"instId":"BTCUSDT","orderId":"123","status":"live","size":"1"}]}).to_string().into())).await.unwrap();
                        }
                    }
                    if state.logins.load(Ordering::SeqCst)==1 {
                        socket.send(Message::Close(None)).await.unwrap();
                        break;
                    }
                }
                _ => {}
            }
        }
    })
}

#[tokio::test]
async fn classic_ws_reconnect_reauthenticates_and_replays_classic_subscription() {
    use nautilus_bitget::{
        common::enums::BitgetEnvironment,
        websocket::{client::BitgetWebSocketClient, messages::BitgetWsMessage},
    };
    use nautilus_network::websocket::TransportBackend;
    use std::{sync::atomic::Ordering, time::Duration};
    let state = WsFixture::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/ws", axum::routing::get(upgrade_ws))
        .with_state(state.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut client = BitgetWebSocketClient::new_private(
        BitgetProductType::Spot,
        BitgetEnvironment::Mainnet,
        Some("key".into()),
        Some("secret".into()),
        Some("pass".into()),
        Some(format!("ws://{addr}/ws")),
        30,
        TransportBackend::default(),
        None,
    )
    .with_account_mode(BitgetAccountMode::Classic);
    client.connect().await.unwrap();
    client.subscribe_orders().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut data_count = 0;
        while let Some(message) = client.next_event().await {
            if let BitgetWsMessage::Data(event) = message {
                assert_eq!(event.data[0]["symbol"], "BTCUSDT");
                assert_eq!(event.data[0]["orderStatus"], "live");
                data_count += 1;
                if data_count == 2 {
                    break;
                }
            }
        }
        assert_eq!(data_count, 2);
    })
    .await
    .unwrap();
    assert_eq!(state.logins.load(Ordering::SeqCst), 2);
    let subscriptions = state.subscriptions.lock().unwrap().clone();
    assert_eq!(subscriptions.len(), 2);
    for request in subscriptions {
        assert_eq!(
            request["args"][0],
            json!({"instType":"SPOT","channel":"orders","instId":"default"})
        );
    }
    client.disconnect().await.unwrap();
    task.abort();
}

#[tokio::test]
async fn classic_reconciliation_includes_plan_orders_with_distinct_price_fields() {
    let (client, fixture, task) = setup().await;
    fixture.respond(json!({"entrustedList":[]}));
    *fixture.plan_response.lock().unwrap() = Some(json!({"entrustedList":[{
        "symbol":"btcusdt", "orderId":"plan-1", "executeOrderId":"child-1",
        "clientOid":"O-1", "planStatus":"executed", "price":"100", "executePrice":"100",
        "triggerPrice":"99", "size":"1", "side":"buy", "orderType":"limit",
        "cTime":"1700000000000", "uTime":""
    }]}));
    let rows = client
        .request_order_statuses(
            BitgetProductType::UsdtFutures,
            None,
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(rows[0].status.as_deref(), Some("triggered"));
    assert_eq!(rows[0].order_id.as_deref(), Some("child-1"));
    assert_eq!(rows[0].price.as_deref(), Some("100"));
    assert_eq!(rows[0].u_time, None);
    fixture.respond(json!([]));
    *fixture.plan_response.lock().unwrap() = Some(json!({"nextFlag":false,"orderList":[{
        "symbol":"BTCUSDT","orderId":"plan-2","status":"not_trigger","executePrice":"100","triggerPrice":"99"
    }]}));
    let rows = client
        .request_order_statuses(BitgetProductType::Spot, None, None, None, true, None)
        .await
        .unwrap();
    assert_eq!(rows[0].order_id.as_deref(), Some("plan-2"));
    task.abort();
}

#[tokio::test]
async fn classic_regular_modify_requires_a_new_identity_and_both_size_and_price() {
    let (client, fixture, task) = setup().await;
    fixture.respond(json!({"orderId":"old","clientOid":"new"}));
    let mut request = BitgetMixModifyOrderRequest {
        symbol: "BTCUSDT".into(),
        product_type: "USDT-FUTURES".into(),
        order_id: Some("old".into()),
        new_size: Some("2".into()),
        new_price: Some("100".into()),
        ..Default::default()
    };
    assert!(
        client
            .modify_order(&BitgetModifyOrderRequest::Mix(request.clone()))
            .await
            .is_err()
    );
    assert!(fixture.requests.lock().unwrap().is_empty());
    request.new_client_oid = Some("new".into());
    client
        .modify_order(&BitgetModifyOrderRequest::Mix(request))
        .await
        .unwrap();
    fixture.assert_path("/api/v2/mix/order/modify-order");
    assert_eq!(fixture.last().2["newClientOid"], "new");
    assert_eq!(fixture.last().2["newSize"], "2");
    assert_eq!(fixture.last().2["newPrice"], "100");
    task.abort();
}

#[tokio::test]
async fn classic_authentication_error_does_not_probe_or_fall_back_to_uta() {
    let (client, fixture, task) = setup().await;
    *fixture.error.lock().unwrap() = Some("30005".into());
    assert!(client.request_spot_account_assets(None).await.is_err());
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].1.starts_with("/api/v2/spot/account/assets"));
    task.abort();
}
