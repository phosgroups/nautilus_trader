use std::{cell::RefCell, rc::Rc};

use axum::{
    Json, Router,
    extract::Query,
    http::StatusCode,
    routing::{get, post},
};
use nautilus_common::{
    cache::Cache,
    messages::{ExecutionEvent, execution::ExecutionReport},
};
use nautilus_model::{
    enums::AccountType, events::OrderEventAny, orders::builder::OrderTestBuilder,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc};

use super::*;
use crate::common::consts::GATEIO_CLIENT_ID;

const ORDER_TEXT: &str = "t-ABC-123";
const ORDERS: &str = "/api/v4/spot/orders";

fn open_order() -> Value {
    json!({
        "id": "701", "text": ORDER_TEXT, "currency_pair": "BTC_USDT",
        "account": "spot", "type": "limit", "side": "buy", "status": "open",
        "amount": "1", "left": "1", "price": "50000", "time_in_force": "gtc"
    })
}

struct SubmittedOrder {
    client: GateioExecutionClient,
    order: OrderAny,
    events: mpsc::UnboundedReceiver<ExecutionEvent>,
}

fn report_command(order: &OrderAny) -> GenerateOrderStatusReport {
    GenerateOrderStatusReport::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(order.instrument_id()),
        Some(order.client_order_id()),
        None,
        None,
        None,
    )
}

fn missing_order() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"label": "ORDER_NOT_FOUND"})),
    )
}

async fn finish_order_tasks(client: &GateioExecutionClient) {
    let tasks = std::mem::take(&mut *client.pending_tasks.lock().unwrap());
    for task in tasks {
        task.await.unwrap();
    }
}

async fn client_for_orders(
    app: Router,
    cache: Rc<RefCell<Cache>>,
    order: &OrderAny,
) -> (
    GateioExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url_http = Some(format!("http://{}", listener.local_addr().unwrap()));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let config = GateioExecClientConfig {
        api_key: Some("test-key".into()),
        api_secret: Some("test-secret".into()),
        base_url_http,
        ..Default::default()
    };
    let core = ExecutionClientCore::new(
        order.trader_id(),
        *GATEIO_CLIENT_ID,
        *GATEIO_VENUE,
        OmsType::Netting,
        config.account_id,
        AccountType::Cash,
        None,
        cache,
    );
    let mut client = GateioExecutionClient::new(core, config).unwrap();
    let (sender, events) = mpsc::unbounded_channel();
    client.emitter.set_sender(sender);
    client.http_client.cache_instruments(&[spot_instrument()]);

    (client, events)
}

async fn submit(status: StatusCode, body: &'static str, query_routes: Router) -> SubmittedOrder {
    let posted_texts = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&posted_texts);
    let app = query_routes.route(
        ORDERS,
        post(move |Json(request): Json<Value>| async move {
            captured.lock().unwrap().push(request["text"].clone());
            (status, body)
        }),
    );
    let instrument = spot_instrument();
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .client_order_id(ClientOrderId::from("ABC-123"))
        .quantity(Quantity::from("1"))
        .price(Price::from("50000"))
        .build();
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    let (client, mut events) = client_for_orders(app, cache, &order).await;

    let cmd = SubmitOrder::from_order(
        &order,
        order.trader_id(),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    );
    client.submit_order(cmd).unwrap();
    finish_order_tasks(&client).await;
    assert_eq!(posted_texts.lock().unwrap().as_slice(), [json!(ORDER_TEXT)]);
    assert!(matches!(
        events.try_recv().unwrap(),
        ExecutionEvent::Order(OrderEventAny::Submitted(_))
    ));
    SubmittedOrder {
        client,
        order,
        events,
    }
}

#[tokio::test]
async fn gateway_error_is_reconciled_by_client_order_text() {
    let queries = Router::new()
        .route(
            "/api/v4/spot/orders/t-ABC-123",
            get(|Query(params): Query<HashMap<String, String>>| async move {
                assert_eq!(params["currency_pair"], "BTC_USDT");
                Json(open_order())
            }),
        )
        .route(
            "/api/v4/spot/orders/701",
            get(|| async {
                let mut canceled = open_order();
                canceled["status"] = json!("cancelled");
                Json(canceled)
            }),
        );
    let SubmittedOrder {
        client,
        order,
        mut events,
    } = submit(StatusCode::BAD_GATEWAY, "upstream unavailable", queries).await;
    assert!(events.try_recv().is_err());

    client
        .query_order(QueryOrder::new(
            order.trader_id(),
            None,
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    finish_order_tasks(&client).await;
    let ExecutionEvent::Report(ExecutionReport::Order(report)) = events.try_recv().unwrap() else {
        panic!("expected order status report");
    };
    assert_eq!(report.client_order_id, Some(order.client_order_id()));
    assert_eq!(report.venue_order_id, VenueOrderId::from("701"));
    assert_eq!(report.order_status, OrderStatus::Accepted);
    let updated = client
        .generate_order_status_report(&report_command(&order))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.venue_order_id, report.venue_order_id);
    assert_eq!(updated.order_status, OrderStatus::Canceled);
}

#[tokio::test]
async fn malformed_ack_is_reconciled_from_finished_spot_orders() {
    let queries = Router::new()
        .route(
            "/api/v4/spot/orders/t-ABC-123",
            get(|| async { missing_order() }),
        )
        .route(
            ORDERS,
            get(|Query(params): Query<HashMap<String, String>>| async move {
                assert_eq!(params["currency_pair"], "BTC_USDT");
                assert_eq!(params["status"], "finished");
                let mut canceled = open_order();
                canceled["status"] = json!("cancelled");
                let mut other = canceled.clone();
                other["id"] = json!("700");
                other["account"] = json!("unified");
                Json(json!([other, canceled]))
            }),
        );
    let SubmittedOrder {
        client,
        order,
        mut events,
    } = submit(StatusCode::OK, "{", queries).await;
    assert!(events.try_recv().is_err());

    let report = client
        .generate_order_status_report(&report_command(&order))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.client_order_id, Some(order.client_order_id()));
    assert_eq!(report.venue_order_id, VenueOrderId::from("701"));
    assert_eq!(report.order_status, OrderStatus::Canceled);
}

#[rstest::rstest]
#[case(vec![])]
#[case(vec!["701", "702"])]
#[tokio::test]
async fn inconclusive_history_leaves_order_unresolved(#[case] matching_ids: Vec<&'static str>) {
    let queries = Router::new()
        .route(
            "/api/v4/spot/orders/t-ABC-123",
            get(|| async { missing_order() }),
        )
        .route(
            ORDERS,
            get(move || async move {
                let rows = matching_ids
                    .into_iter()
                    .map(|id| {
                        let mut order = open_order();
                        order["id"] = json!(id);
                        order["status"] = json!("cancelled");
                        order
                    })
                    .collect::<Vec<_>>();
                Json(rows)
            }),
        );
    let SubmittedOrder {
        client,
        order,
        mut events,
    } = submit(StatusCode::OK, "{}", queries).await;
    let result = client
        .generate_order_status_report(&report_command(&order))
        .await;
    assert!(result.is_err(), "unexpected report: {result:?}");
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn insufficient_balance_emits_order_rejected() {
    let result = submit(
        StatusCode::BAD_REQUEST,
        r#"{"label":"BALANCE_NOT_ENOUGH","message":"insufficient balance"}"#,
        Router::new(),
    )
    .await;
    let mut events = result.events;
    let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = events.try_recv().unwrap()
    else {
        panic!("expected order rejection");
    };
    assert_eq!(rejected.client_order_id, result.order.client_order_id());
    assert!(rejected.reason.as_str().contains("BALANCE_NOT_ENOUGH"));
}

#[rstest::rstest]
#[case(StatusCode::OK, false, false)]
#[case(StatusCode::OK, true, false)]
#[case(StatusCode::BAD_GATEWAY, true, false)]
#[case(StatusCode::BAD_REQUEST, false, true)]
#[tokio::test]
async fn cancel_all_during_submit(
    #[case] status: StatusCode,
    #[case] finish_post_first: bool,
    #[case] project_submitted: bool,
) {
    use axum::{extract::Path, routing::delete};
    use tokio::sync::Notify;

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let post_entered = entered.clone();
    let post_release = release.clone();
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let seen = deletes.clone();
    let app = Router::new()
        .route(
            ORDERS,
            post(move || async move {
                post_entered.notify_one();
                post_release.notified().await;
                let body = if status == StatusCode::OK {
                    open_order()
                } else {
                    json!({"label": "BALANCE_NOT_ENOUGH"})
                };
                (status, Json(body))
            }),
        )
        .route(
            "/api/v4/spot/orders/{id}",
            delete(move |Path(id): Path<String>| async move {
                seen.lock().unwrap().push(id);
                let mut row = open_order();
                row["status"] = json!("cancelled");
                Json(row)
            }),
        );
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(spot_instrument().id())
        .client_order_id(ClientOrderId::from("ABC-123"))
        .quantity(Quantity::from("1"))
        .price(Price::from("50000"))
        .build();
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    let (client, mut events) = client_for_orders(app, cache.clone(), &order).await;
    let ts = UnixNanos::default();
    client
        .submit_order(SubmitOrder::from_order(
            &order,
            order.trader_id(),
            None,
            None,
            UUID4::new(),
            ts,
        ))
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    if project_submitted {
        let ExecutionEvent::Order(submitted) = events.try_recv().unwrap() else {
            panic!("expected order submitted");
        };
        cache.borrow_mut().update_order(&submitted).unwrap();
    }
    if finish_post_first {
        release.notify_one();
        finish_order_tasks(&client).await;
    }
    client
        .cancel_all_orders(CancelAllOrders::new(
            order.trader_id(),
            None,
            order.strategy_id(),
            order.instrument_id(),
            OrderSide::NoOrderSide,
            UUID4::new(),
            ts,
            None,
            None,
        ))
        .unwrap();
    if !finish_post_first {
        release.notify_one();
    }
    finish_order_tasks(&client).await;
    let expected: &[&str] = match status {
        StatusCode::OK => &["701"],
        StatusCode::BAD_GATEWAY => &[ORDER_TEXT],
        _ => &[],
    };
    assert_eq!(deletes.lock().unwrap().as_slice(), expected);
}

#[tokio::test]
async fn cancel_all_targets_only_owned_orders_and_wallet() {
    use axum::{extract::Path, routing::delete};
    use nautilus_model::{
        events::{OrderAccepted, OrderSubmitted},
        identifiers::StrategyId,
    };

    let instrument_id = spot_instrument().id();
    let account_id = AccountId::from("GATEIO-001");
    let strategy_id = StrategyId::from("GRID-001");
    let mut cache = Cache::default();
    for (venue_id, strategy, account, symbol, side) in [
        (
            "701",
            strategy_id,
            account_id,
            instrument_id,
            OrderSide::Buy,
        ),
        (
            "702",
            StrategyId::from("GRID-002"),
            account_id,
            instrument_id,
            OrderSide::Buy,
        ),
        (
            "703",
            strategy_id,
            AccountId::from("GATEIO-002"),
            instrument_id,
            OrderSide::Buy,
        ),
        (
            "704",
            strategy_id,
            account_id,
            InstrumentId::from("ETH_USDT.GATEIO"),
            OrderSide::Buy,
        ),
        (
            "705",
            strategy_id,
            account_id,
            instrument_id,
            OrderSide::Sell,
        ),
    ] {
        let id = ClientOrderId::from(venue_id);
        let order = OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(id)
            .quantity(Quantity::from("1"))
            .strategy_id(strategy)
            .instrument_id(symbol)
            .price(Price::from("50000"))
            .side(side)
            .build();
        cache.add_order(order.clone(), None, None, false).unwrap();
        let ts = UnixNanos::default();
        cache
            .update_order(&OrderEventAny::Submitted(OrderSubmitted::new(
                order.trader_id(),
                strategy,
                symbol,
                id,
                account,
                UUID4::new(),
                ts,
                ts,
            )))
            .unwrap();
        cache
            .update_order(&OrderEventAny::Accepted(OrderAccepted::new(
                order.trader_id(),
                strategy,
                symbol,
                id,
                VenueOrderId::from(venue_id),
                account,
                UUID4::new(),
                ts,
                ts,
                false,
            )))
            .unwrap();
    }
    let order = cache.try_order_owned(&ClientOrderId::from("701")).unwrap();
    let canceled = Arc::new(Mutex::new(Vec::new()));
    let captured = canceled.clone();
    let app = Router::new().route("/api/v4/spot/orders/{id}", delete(move |Path(id): Path<String>, Query(params): Query<HashMap<String, String>>| async move {
        assert_eq!(params["account"], "spot");
        assert_eq!(params["currency_pair"], "BTC_USDT");
        captured.lock().unwrap().push(id.clone());
        let mut row = open_order();
        row["status"] = json!("cancelled");
        row["left"] = json!("0.6");
        row["filled_amount"] = json!("0.4");
        row["text"] = json!(format!("t-{id}"));
        row["id"] = json!(id);
        Json(row)
    })).route("/api/v4/spot/my_trades", get(|Query(params): Query<HashMap<String, String>>| async move {
        assert_eq!(params["order_id"], "701");
        Json(json!([{"order_id":"701", "currency_pair":"BTC_USDT","id":"87","price":"49000","amount":"0.4","fee":"0.04","fee_currency":"USDT","side":"buy","role":"maker"}]))
    }));
    let (client, mut events) = client_for_orders(app, Rc::new(RefCell::new(cache)), &order).await;
    client
        .cancel_all_orders(CancelAllOrders::new(
            order.trader_id(),
            None,
            strategy_id,
            instrument_id,
            OrderSide::Buy,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    finish_order_tasks(&client).await;
    assert_eq!(*canceled.lock().unwrap(), ["701"]);
    let ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills)) =
        events.try_recv().unwrap()
    else {
        panic!("expected the canceled order and its partial fill");
    };
    assert_eq!(report.order_status, OrderStatus::Canceled);
    assert_eq!(report.venue_order_id.as_str(), "701");
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].trade_id.as_str(), "87");
    assert_eq!(fills[0].last_qty, Quantity::from("0.4"));
    assert_eq!(fills[0].commission.to_string(), "0.04000000 USDT");
    assert!(events.try_recv().is_err());
}

#[rstest::rstest]
#[case("0.4")]
#[case("0.7")]
#[tokio::test]
async fn status_only_query_waits_for_real_executions(#[case] trade_qty: &'static str) {
    let queries = Router::new()
        .route("/api/v4/spot/orders/t-ABC-123", get(|| async {
            let mut order = open_order();
            order["status"] = json!("cancelled");
            order["left"] = json!("0.3");
            order["filled_amount"] = json!("0.7");
            // A forty-day order needs two explicit windows.
            order["create_time"] = json!(1_700_000_000);
            order["update_time"] = json!(1_703_456_000);
            Json(order)
        }))
        .route("/api/v4/spot/my_trades", get(move |Query(params): Query<HashMap<String, String>>| async move {
            assert_eq!(params["currency_pair"], "BTC_USDT");
            assert_eq!(params["order_id"], "701");
            let from = params["from"].parse::<i64>().unwrap();
            let to = params["to"].parse::<i64>().unwrap();
            let boundary = 1_700_000_000 + 30 * 86_400;
            if from == boundary {
                assert_eq!(to, 1_703_456_001);
                let later = json!({"order_id":"701","currency_pair":"BTC_USDT","id":"78","price":"49100","amount":"0.1","fee":"0.01","fee_currency":"USDT","side":"buy","role":"maker"});
                return Json(json!([later]));
            }
            assert_eq!((from, to), (1_700_000_000, boundary));
            let fill = json!({"order_id":"701", "currency_pair":"BTC_USDT","id":"77","price":"49000","amount":if trade_qty == "0.7" { "0.6" } else { "0.3" },"fee":"0.04","fee_currency":"USDT","side":"buy","role":"maker"});
            Json(json!([fill, fill]))
        }));
    let SubmittedOrder {
        client,
        order,
        mut events,
    } = submit(StatusCode::OK, "{}", queries).await;
    assert!(
        client
            .generate_order_status_report(&report_command(&order))
            .await
            .is_err()
    );
    if trade_qty == "0.4" {
        assert!(events.try_recv().is_err());
    } else {
        let ExecutionEvent::Report(ExecutionReport::OrderWithFills(order, fills)) =
            events.try_recv().unwrap()
        else {
            panic!("expected bundled terminal status and executions");
        };
        assert_eq!(order.order_status, OrderStatus::Canceled);
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].trade_id.as_str(), "77");
        assert_eq!(fills[0].last_px, Price::from("49000"));
        assert_eq!(fills[0].commission.to_string(), "0.04000000 USDT");
    }
}

#[tokio::test]
async fn submit_list_uses_each_childs_instrument() {
    use nautilus_model::{identifiers::OrderListId, orders::OrderList};
    let pairs = Arc::new(Mutex::new(Vec::new()));
    let captured = pairs.clone();
    let app = Router::new().route(
        ORDERS,
        post(move |Json(req): Json<Value>| async move {
            captured.lock().unwrap().push((
                req["text"].as_str().unwrap().to_owned(),
                req["currency_pair"].as_str().unwrap().to_owned(),
            ));
            Json(json!({"id": if req["text"] == "t-B" { "701" } else { "702" }}))
        }),
    );
    let list_id = OrderListId::from("L-001");
    let mut orders = Vec::new();
    let mut cache = Cache::default();
    for (id, symbol) in [("B", "BTC_USDT.GATEIO"), ("E", "ETH_USDT.GATEIO")] {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .order_list_id(list_id)
            .client_order_id(ClientOrderId::from(id))
            .quantity(Quantity::from("1"))
            .instrument_id(InstrumentId::from(symbol))
            .price(Price::from("10"))
            .build();
        cache.add_order(order.clone(), None, None, false).unwrap();
        orders.push(order);
    }
    let (client, _events) = client_for_orders(app, Rc::new(RefCell::new(cache)), &orders[0]).await;
    let list = SubmitOrderList::new(
        orders[0].trader_id(),
        None,
        orders[0].strategy_id(),
        OrderList::from_orders(&orders, UnixNanos::default()),
        orders.iter().map(|o| o.init_event().clone()).collect(),
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    let mut wrong_product = list.clone();
    wrong_product.order_inits[1].instrument_id = InstrumentId::from("ETH_USDT-PERP.GATEIO");
    assert!(client.submit_order_list(wrong_product).is_err());
    client.submit_order_list(list).unwrap();
    finish_order_tasks(&client).await;
    let mut pairs = pairs.lock().unwrap();
    pairs.sort();
    assert_eq!(
        *pairs,
        [
            ("t-B".into(), "BTC_USDT".into()),
            ("t-E".into(), "ETH_USDT".into())
        ]
    );
}
