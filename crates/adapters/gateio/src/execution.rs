//! Gate.io execution clients.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::{get_runtime, runner::get_exec_event_sender},
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateFillReportsBuilder, GenerateOrderStatusReport, GenerateOrderStatusReports,
        GenerateOrderStatusReportsBuilder, GeneratePositionStatusReports,
        GeneratePositionStatusReportsBuilder, ModifyOrder, QueryAccount, QueryOrder, SubmitOrder,
        SubmitOrderList,
    },
};
use nautilus_core::{
    UUID4, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{OmsType, OrderSide, OrderStatus, OrderType, TimeInForce},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Price, Quantity},
};
use tokio::task::JoinHandle;

use crate::{
    common::{
        consts::{
            GATEIO_FUTURES_BALANCES_WS_CHANNEL, GATEIO_FUTURES_ORDERS_WS_CHANNEL,
            GATEIO_FUTURES_POSITIONS_WS_CHANNEL, GATEIO_FUTURES_USER_TRADES_WS_CHANNEL,
            GATEIO_SPOT_BALANCES_WS_CHANNEL, GATEIO_SPOT_ORDERS_WS_CHANNEL,
            GATEIO_SPOT_USER_TRADES_WS_CHANNEL, GATEIO_VENUE,
        },
        enums::GateioProductType,
        parse::{parse_position_status_report, parse_user_trade},
        symbol::{GateioSymbol, raw_symbol},
    },
    config::GateioExecClientConfig,
    http::{
        client::GateioHttpClient,
        models::{GateioOrder, GateioOrderAmendRequest, GateioOrderRequest},
    },
    websocket::{GateioWebSocketClient, GateioWsMessage},
};

/// Live execution client for Gate.io.
#[derive(Debug)]
pub struct GateioExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: GateioExecClientConfig,
    emitter: ExecutionEventEmitter,
    http_client: GateioHttpClient,
    ws_client: GateioWebSocketClient,
    ws_task: Option<JoinHandle<()>>,
    is_connected: AtomicBool,
}

struct GateioExecutionWsDispatchContext {
    product_type: GateioProductType,
    account_id: AccountId,
    clock: &'static AtomicTime,
    emitter: ExecutionEventEmitter,
    instruments_by_raw: Arc<HashMap<String, InstrumentAny>>,
    seen_orders: Arc<Mutex<HashSet<String>>>,
    seen_trades: Arc<Mutex<HashSet<String>>>,
}

/// Gate.io Spot execution client type alias.
pub type GateioSpotExecutionClient = GateioExecutionClient;

/// Gate.io USDT perpetual execution client type alias.
pub type GateioFuturesExecutionClient = GateioExecutionClient;

impl GateioExecutionClient {
    /// Creates a Gate.io execution client.
    pub fn new(core: ExecutionClientCore, config: GateioExecClientConfig) -> anyhow::Result<Self> {
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );
        let http_client = GateioHttpClient::new_with_credentials_and_retry(
            config.api_key.clone(),
            config.api_secret.clone(),
            Some(config.http_base_url()),
            config.http_timeout_secs,
            config.proxy_url.clone(),
            config.max_retries,
        )?;
        let ws_client = GateioWebSocketClient::new_private(&config);

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            http_client,
            ws_client,
            ws_task: None,
            is_connected: AtomicBool::new(false),
        })
    }

    /// Returns the configured product type.
    #[must_use]
    pub const fn product_type(&self) -> GateioProductType {
        self.config.product_type
    }

    fn ensure_product(&self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        anyhow::ensure!(
            GateioProductType::from_symbol(instrument_id.symbol.as_str())
                == self.config.product_type,
            "Gate.io execution client is configured for {:?}, cannot use {}",
            self.config.product_type,
            instrument_id
        );
        Ok(())
    }

    fn start_ws_dispatch(
        &mut self,
        instruments_by_raw: Arc<HashMap<String, InstrumentAny>>,
    ) -> anyhow::Result<()> {
        if self.ws_task.is_some() {
            return Ok(());
        }
        let mut receiver = self
            .ws_client
            .take_event_receiver()
            .context("Gate.io private WebSocket event receiver was already taken")?;
        let context = GateioExecutionWsDispatchContext {
            product_type: self.config.product_type,
            account_id: self.core.account_id,
            clock: self.clock,
            emitter: self.emitter.clone(),
            instruments_by_raw,
            seen_orders: Arc::new(Mutex::new(HashSet::new())),
            seen_trades: Arc::new(Mutex::new(HashSet::new())),
        };
        self.ws_task = Some(get_runtime().spawn(async move {
            while let Some(message) = receiver.recv().await {
                dispatch_private_message(message, &context);
            }
        }));
        Ok(())
    }

    fn abort_ws_task(&mut self) {
        if let Some(task) = self.ws_task.take() {
            task.abort();
        }
    }

    async fn resolve_report_instrument(
        &self,
        fallback: Option<InstrumentId>,
        raw: Option<&str>,
    ) -> anyhow::Result<InstrumentAny> {
        if let Some(instrument_id) = fallback {
            if let Some(instrument) = self.core.cache().instrument(&instrument_id) {
                return Ok(instrument.clone());
            }
        }

        let raw = raw
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("Gate.io report has no instrument symbol")?;
        let instrument_id = match self.config.product_type {
            GateioProductType::Spot => GateioSymbol::spot(raw)?.instrument_id(),
            GateioProductType::UsdtPerpetual => GateioSymbol::usdt_perpetual(raw)?.instrument_id(),
        };

        if let Some(instrument) = self.core.cache().instrument(&instrument_id) {
            return Ok(instrument.clone());
        }
        self.http_client
            .instrument(
                instrument_id,
                self.config.product_type,
                self.clock.get_time_ns(),
            )
            .await
    }

    fn order_from_cache(&self, client_order_id: ClientOrderId) -> anyhow::Result<OrderAny> {
        self.core
            .get_order(&client_order_id)
            .with_context(|| format!("Gate.io order is not cached: {client_order_id}"))
    }

    fn submit_request(&self, order: &OrderInitializedView) -> anyhow::Result<GateioOrderRequest> {
        build_submit_request(self.config.product_type, order)
    }
}

fn build_submit_request(
    product_type: GateioProductType,
    order: &OrderInitializedView,
) -> anyhow::Result<GateioOrderRequest> {
    let side = match order.order_side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
        OrderSide::NoOrderSide => anyhow::bail!("Gate.io order side cannot be NoOrderSide"),
    };
    let order_type = match order.order_type {
        OrderType::Market => "market",
        OrderType::Limit => "limit",
        _ => anyhow::bail!(
            "Gate.io adapter does not yet support {:?}",
            order.order_type
        ),
    };
    let tif = match order.time_in_force {
        TimeInForce::Gtc | TimeInForce::Day => "gtc",
        TimeInForce::Ioc => "ioc",
        TimeInForce::Fok => "fok",
        TimeInForce::Gtd => "gtc",
        TimeInForce::AtTheOpen | TimeInForce::AtTheClose => {
            anyhow::bail!(
                "Gate.io does not support {:?} time in force",
                order.time_in_force
            )
        }
    };
    let client_text = Some(gateio_client_order_text(order.client_order_id));

    if product_type == GateioProductType::Spot {
        Ok(GateioOrderRequest {
            currency_pair: Some(raw_symbol(order.instrument_id)),
            contract: None,
            type_: Some(order_type.to_string()),
            account: Some("spot".to_string()),
            side: side.to_string(),
            amount: order.quantity.to_string(),
            size: String::new(),
            price: order.price.map(|value| value.to_string()),
            time_in_force: Some(tif.to_string()),
            tif: None,
            text: client_text,
            reduce_only: None,
        })
    } else {
        let signed_size = match order.order_side {
            OrderSide::Buy => order.quantity.to_string(),
            OrderSide::Sell => format!("-{}", order.quantity),
            OrderSide::NoOrderSide => {
                anyhow::bail!("Gate.io order side cannot be NoOrderSide")
            }
        };
        Ok(GateioOrderRequest {
            currency_pair: None,
            contract: Some(raw_symbol(order.instrument_id)),
            type_: None,
            account: None,
            side: String::new(),
            amount: String::new(),
            size: signed_size,
            price: if order.order_type == OrderType::Market {
                Some("0".to_string())
            } else {
                order.price.map(|value| value.to_string())
            },
            time_in_force: None,
            tif: Some(if order.order_type == OrderType::Market {
                "ioc".to_string()
            } else {
                tif.to_string()
            }),
            text: client_text,
            reduce_only: Some(order.reduce_only),
        })
    }
}

impl GateioExecutionClient {
    fn spawn_order_task<F>(&self, name: &'static str, future: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        get_runtime().spawn(async move {
            if let Err(error) = future.await {
                log::error!("Gate.io {name} task failed: {error:?}");
            }
        });
    }
}

struct OrderInitializedView {
    client_order_id: ClientOrderId,
    instrument_id: InstrumentId,
    order_side: OrderSide,
    order_type: OrderType,
    quantity: Quantity,
    time_in_force: TimeInForce,
    price: Option<Price>,
    reduce_only: bool,
}

fn gateio_client_order_text(client_order_id: ClientOrderId) -> String {
    const MAX_CUSTOM_TEXT_BYTES: usize = 28;
    let mut custom_text = String::new();
    for byte in client_order_id.to_string().bytes() {
        if custom_text.len() >= MAX_CUSTOM_TEXT_BYTES {
            break;
        }
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
            custom_text.push(byte as char);
        }
    }
    if custom_text.is_empty() {
        custom_text.push('x');
    }
    format!("t-{custom_text}")
}

fn private_rows(value: &serde_json::Value) -> Vec<serde_json::Value> {
    match value {
        serde_json::Value::Array(rows) => rows.clone(),
        serde_json::Value::Null => Vec::new(),
        row => vec![row.clone()],
    }
}

fn private_raw_symbol(
    value: &serde_json::Value,
    product_type: GateioProductType,
) -> Option<String> {
    let key = if product_type == GateioProductType::Spot {
        "currency_pair"
    } else {
        "contract"
    };
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("s")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        })
}

fn remember_once(cache: &Mutex<HashSet<String>>, key: String) -> bool {
    let Ok(mut cache) = cache.lock() else {
        return true;
    };
    if cache.len() >= 10_000 {
        cache.clear();
    }
    !cache.insert(key)
}

fn client_order_id_from_text(text: &str) -> Option<ClientOrderId> {
    let value = text.trim();
    if value.is_empty() {
        return None;
    }
    let value = value.strip_prefix("t-").unwrap_or(value);
    ClientOrderId::new_checked(value).ok()
}

fn dispatch_private_message(message: GateioWsMessage, context: &GateioExecutionWsDispatchContext) {
    if message.event != "update" {
        return;
    }

    if message.channel.ends_with(".orders") {
        for value in private_rows(&message.result) {
            let Ok(mut order) = serde_json::from_value::<GateioOrder>(value.clone()) else {
                log::debug!("Failed to decode Gate.io private order event");
                continue;
            };
            let Some(raw) = private_raw_symbol(&value, context.product_type) else {
                log::debug!("Gate.io private order event has no symbol");
                continue;
            };
            let Some(instrument) = context.instruments_by_raw.get(&raw) else {
                log::debug!("Gate.io private order symbol is not cached: {raw}");
                continue;
            };
            let dedup_key = format!(
                "{}:{}:{}:{}",
                order.id,
                order.update_time_ms.unwrap_or_default(),
                order.status.as_deref().unwrap_or_default(),
                order.left
            );
            if remember_once(&context.seen_orders, dedup_key) {
                continue;
            }
            if let Some(client_order_id) = client_order_id_from_text(&order.text) {
                order.text = client_order_id.to_string();
            }
            match report_from_order(
                &order,
                instrument,
                context.account_id,
                context.clock.get_time_ns(),
            ) {
                Ok(report) => context.emitter.send_order_status_report(report),
                Err(error) => log::warn!("Failed to parse Gate.io private order event: {error}"),
            }
        }
    } else if message.channel.ends_with(".usertrades") {
        for value in private_rows(&message.result) {
            let Ok(trade) =
                serde_json::from_value::<crate::http::models::GateioUserTrade>(value.clone())
            else {
                log::debug!("Failed to decode Gate.io private user trade event");
                continue;
            };
            let Some(raw) = private_raw_symbol(&value, context.product_type) else {
                log::debug!("Gate.io private user trade event has no symbol");
                continue;
            };
            let Some(instrument) = context.instruments_by_raw.get(&raw) else {
                log::debug!("Gate.io private user trade symbol is not cached: {raw}");
                continue;
            };
            if remember_once(&context.seen_trades, trade.id.clone()) {
                continue;
            }
            match parse_user_trade(
                &trade,
                instrument,
                context.account_id,
                context.clock.get_time_ns(),
            ) {
                Ok(report) => context.emitter.send_fill_report(report),
                Err(error) => log::warn!("Failed to parse Gate.io private fill event: {error}"),
            }
        }
    } else if message.channel.ends_with(".balances") {
        let rows = private_rows(&message.result)
            .into_iter()
            .filter_map(|value| serde_json::from_value(value).ok())
            .collect::<Vec<crate::http::models::GateioAccount>>();
        match crate::common::parse::parse_account_state(
            &rows,
            context.product_type,
            context.account_id,
            context.clock.get_time_ns(),
            context.clock.get_time_ns(),
        ) {
            Ok(state) => context.emitter.send_account_state(state),
            Err(error) => log::warn!("Failed to parse Gate.io private balance event: {error}"),
        }
    } else if message.channel.ends_with(".positions")
        && context.product_type == GateioProductType::UsdtPerpetual
    {
        for value in private_rows(&message.result) {
            let Ok(position) =
                serde_json::from_value::<crate::http::models::GateioPosition>(value.clone())
            else {
                log::debug!("Failed to decode Gate.io private position event");
                continue;
            };
            let Some(raw) = private_raw_symbol(&value, context.product_type) else {
                continue;
            };
            let Some(instrument) = context.instruments_by_raw.get(&raw) else {
                continue;
            };
            match parse_position_status_report(
                &position,
                instrument,
                context.account_id,
                context.clock.get_time_ns(),
            ) {
                Ok(report) => context.emitter.send_position_report(report),
                Err(error) => log::warn!("Failed to parse Gate.io private position event: {error}"),
            }
        }
    }
}

fn parse_order_status(value: &GateioOrder) -> OrderStatus {
    let has_fills = value
        .filled_amount
        .as_deref()
        .or(value.filled_size.as_deref())
        .or(value.filled_total.as_deref())
        .and_then(|raw| crate::common::parse::decimal(raw, "order.filled").ok())
        .is_some_and(|value| !value.is_zero());
    let left_is_zero =
        crate::common::parse::decimal(&value.left, "order.left").is_ok_and(|value| value.is_zero());
    if let Some(finish_as) = value.finish_as.as_deref() {
        match finish_as.to_ascii_lowercase().as_str() {
            "filled" => return OrderStatus::Filled,
            "cancelled"
            | "canceled"
            | "expired"
            | "liquidated"
            | "liquidate_cancelled"
            | "ioc"
            | "poc"
            | "fok"
            | "auto_deleveraged"
            | "reduce_only"
            | "position_closed"
            | "reduce_out"
            | "stp"
            | "small"
            | "depth_not_enough"
            | "trader_not_enough" => return OrderStatus::Canceled,
            _ => {}
        }
    }

    match value
        .status
        .as_deref()
        .unwrap_or("open")
        .to_ascii_lowercase()
        .as_str()
    {
        "open" | "new" | "wait" | "put" => {
            if has_fills {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Accepted
            }
        }
        "closed" | "filled" | "finished" if left_is_zero => OrderStatus::Filled,
        "cancelled" | "canceled" | "expired" => OrderStatus::Canceled,
        "rejected" | "reject" => OrderStatus::Rejected,
        _ if left_is_zero => OrderStatus::Filled,
        _ => OrderStatus::Accepted,
    }
}

fn report_from_order(
    value: &GateioOrder,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let side = if value.contract.is_some() {
        let size = crate::common::parse::decimal(&value.size, "order.size")?;
        if size.is_zero() {
            match value.auto_size.as_deref() {
                Some("close_long") => OrderSide::Sell,
                Some("close_short") => OrderSide::Buy,
                _ => OrderSide::Buy,
            }
        } else if size.is_sign_negative() {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        }
    } else {
        match value
            .side
            .as_deref()
            .unwrap_or("buy")
            .to_ascii_lowercase()
            .as_str()
        {
            "sell" => OrderSide::Sell,
            _ => OrderSide::Buy,
        }
    };
    let order_type = if value.contract.is_some()
        && value.price.trim() == "0"
        && value
            .tif
            .as_deref()
            .unwrap_or_default()
            .eq_ignore_ascii_case("ioc")
    {
        OrderType::Market
    } else {
        match value
            .type_
            .as_deref()
            .unwrap_or("limit")
            .to_ascii_lowercase()
            .as_str()
        {
            "market" => OrderType::Market,
            _ => OrderType::Limit,
        }
    };
    let tif = match value
        .tif
        .as_deref()
        .or(value.time_in_force.as_deref())
        .unwrap_or("gtc")
        .to_ascii_lowercase()
        .as_str()
    {
        "ioc" => TimeInForce::Ioc,
        "fok" => TimeInForce::Fok,
        _ => TimeInForce::Gtc,
    };
    let quantity_raw = if value.contract.is_some() {
        &value.size
    } else {
        &value.amount
    };
    let quantity = Quantity::from_decimal_dp(
        crate::common::parse::decimal(quantity_raw.trim_start_matches('-'), "order.quantity")?,
        instrument.size_precision(),
    )
    .context("invalid Gate.io order quantity")?;
    let filled_raw = if value.contract.is_some() {
        value
            .filled_size
            .as_deref()
            .or(value.filled_amount.as_deref())
            .map(|value| value.trim_start_matches('-').to_string())
            .unwrap_or_else(|| {
                let total = crate::common::parse::decimal(&value.size, "order.size")
                    .unwrap_or_default()
                    .abs();
                let left = crate::common::parse::decimal(&value.left, "order.left")
                    .unwrap_or_default()
                    .abs();
                (total - left).max(rust_decimal::Decimal::ZERO).to_string()
            })
    } else {
        value
            .filled_amount
            .as_deref()
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                let total = crate::common::parse::decimal(&value.amount, "order.amount")
                    .unwrap_or_default();
                let left =
                    crate::common::parse::decimal(&value.left, "order.left").unwrap_or_default();
                (total - left).max(rust_decimal::Decimal::ZERO).to_string()
            })
    };
    let filled_qty = Quantity::from_decimal_dp(
        crate::common::parse::decimal(&filled_raw, "order.filled_quantity")?,
        instrument.size_precision(),
    )
    .context("invalid Gate.io filled quantity")?;
    let ts_accepted = value
        .create_time_ms
        .and_then(|value| crate::common::parse::timestamp_nanos(value).ok())
        .unwrap_or(ts_init);
    let ts_last = value
        .update_time_ms
        .and_then(|value| crate::common::parse::timestamp_nanos(value).ok())
        .unwrap_or(ts_init);
    let venue_order_id = VenueOrderId::from(value.id.as_str());
    let report = OrderStatusReport::new(
        account_id,
        instrument.id(),
        client_order_id_from_text(&value.text),
        venue_order_id,
        side,
        order_type,
        tif,
        parse_order_status(value),
        quantity,
        filled_qty,
        ts_accepted,
        ts_last,
        ts_init,
        Some(UUID4::new()),
    );
    let report = if value.price.trim().is_empty() || value.price == "0" {
        report
    } else {
        report.with_price(crate::common::parse::price(
            &value.price,
            instrument.price_precision(),
            "order.price",
        )?)
    };
    let report = if let Some(avg_px) = value
        .avg_deal_price
        .as_deref()
        .or(value.fill_price.as_deref())
        .filter(|value| !value.is_empty() && *value != "0")
    {
        let avg_px = crate::common::parse::decimal(avg_px, "order.avg_deal_price")?;
        report.with_avg_px(
            avg_px
                .to_string()
                .parse::<f64>()
                .context("invalid Gate.io order average price")?,
        )?
    } else {
        report
    };
    Ok(report.with_reduce_only(value.reduce_only.or(value.is_reduce_only).unwrap_or(false)))
}

#[async_trait(?Send)]
impl ExecutionClient for GateioExecutionClient {
    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *GATEIO_VENUE
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<nautilus_model::types::AccountBalance>,
        margins: Vec<nautilus_model::types::MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event);
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.abort_ws_task();
        self.core.set_stopped();
        self.core.set_disconnected();
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        let ts = self.clock.get_time_ns();
        let instruments = self
            .http_client
            .instruments(self.config.product_type, ts)
            .await
            .context("failed to load Gate.io execution instruments")?;
        let instruments_by_raw = Arc::new(
            instruments
                .into_iter()
                .map(|instrument| (instrument.raw_symbol().to_string(), instrument))
                .collect::<HashMap<_, _>>(),
        );
        let account_state = self
            .http_client
            .account_state(self.config.product_type, self.core.account_id, ts)
            .await
            .context("failed to load Gate.io account state")?;
        self.emitter.send_account_state(account_state);

        self.ws_client
            .connect()
            .await
            .context("failed to connect Gate.io private WebSocket")?;
        self.start_ws_dispatch(instruments_by_raw)?;
        let private_channels = if self.config.product_type == GateioProductType::Spot {
            vec![
                GATEIO_SPOT_ORDERS_WS_CHANNEL,
                GATEIO_SPOT_USER_TRADES_WS_CHANNEL,
                GATEIO_SPOT_BALANCES_WS_CHANNEL,
            ]
        } else {
            vec![
                GATEIO_FUTURES_ORDERS_WS_CHANNEL,
                GATEIO_FUTURES_USER_TRADES_WS_CHANNEL,
                GATEIO_FUTURES_BALANCES_WS_CHANNEL,
                GATEIO_FUTURES_POSITIONS_WS_CHANNEL,
            ]
        };
        for channel in private_channels {
            self.ws_client.subscribe(channel, vec![], true).await?;
        }

        self.core.set_connected();
        self.is_connected.store(true, Ordering::Release);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.ws_client.disconnect().await?;
        self.abort_ws_task();
        self.core.set_disconnected();
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let order = self.order_from_cache(cmd.client_order_id)?;
        let view = OrderInitializedView {
            client_order_id: cmd.client_order_id,
            instrument_id: cmd.instrument_id,
            order_side: cmd.order_init.order_side,
            order_type: cmd.order_init.order_type,
            quantity: cmd.order_init.quantity,
            time_in_force: cmd.order_init.time_in_force,
            price: cmd.order_init.price,
            reduce_only: cmd.order_init.reduce_only,
        };
        let request = match self.submit_request(&view) {
            Ok(value) => value,
            Err(error) => {
                self.emitter.emit_order_denied(&order, &error.to_string());
                return Ok(());
            }
        };
        self.emitter.emit_order_submitted(&order);
        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        self.spawn_order_task("submit_order", async move {
            match http.submit_order(&request).await {
                Ok(ack) => {
                    if !ack.id.is_empty() {
                        emitter.emit_order_accepted(
                            &order,
                            VenueOrderId::from(ack.id.as_str()),
                            clock.get_time_ns(),
                        );
                    }
                }
                Err(error) => emitter.emit_order_rejected(
                    &order,
                    &format!("Gate.io submit failed: {error}"),
                    clock.get_time_ns(),
                    false,
                ),
            }
            Ok(())
        });
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        for order_init in cmd.order_inits {
            self.submit_order(SubmitOrder {
                trader_id: cmd.trader_id,
                client_id: cmd.client_id,
                strategy_id: cmd.strategy_id,
                instrument_id: cmd.instrument_id,
                client_order_id: order_init.client_order_id,
                order_init,
                exec_algorithm_id: cmd.exec_algorithm_id,
                position_id: cmd.position_id,
                params: cmd.params.clone(),
                command_id: cmd.command_id,
                ts_init: cmd.ts_init,
                correlation_id: cmd.correlation_id,
                causation_id: cmd.causation_id,
            })?;
        }
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let Some(venue_order_id) = cmd.venue_order_id else {
            anyhow::bail!("Gate.io modify requires a venue order ID");
        };
        let cached_order = self.order_from_cache(cmd.client_order_id)?;
        let request = if self.config.product_type == GateioProductType::Spot {
            GateioOrderAmendRequest {
                amount: cmd.quantity.map(|value| value.to_string()),
                size: None,
                price: cmd.price.map(|value| value.to_string()),
            }
        } else {
            let size = cmd.quantity.map(|value| {
                if cached_order.order_side() == OrderSide::Sell {
                    format!("-{value}")
                } else {
                    value.to_string()
                }
            });
            GateioOrderAmendRequest {
                amount: None,
                size,
                price: cmd.price.map(|value| value.to_string()),
            }
        };
        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;
        let clock = self.clock;
        self.spawn_order_task("modify_order", async move {
            if let Err(error) = http
                .amend_order(
                    GateioProductType::from_symbol(instrument_id.symbol.as_str()),
                    &venue_order_id.to_string(),
                    &request,
                    &raw_symbol(instrument_id),
                )
                .await
            {
                emitter.emit_order_modify_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    Some(venue_order_id),
                    &format!("Gate.io modify failed: {error}"),
                    clock.get_time_ns(),
                );
            }
            Ok(())
        });
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let Some(venue_order_id) = cmd.venue_order_id else {
            anyhow::bail!("Gate.io cancel requires a venue order ID");
        };
        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let order = self.order_from_cache(cmd.client_order_id)?;
        let clock = self.clock;
        self.spawn_order_task("cancel_order", async move {
            match http
                .cancel_order(
                    GateioProductType::from_symbol(cmd.instrument_id.symbol.as_str()),
                    &venue_order_id.to_string(),
                    &raw_symbol(cmd.instrument_id),
                )
                .await
            {
                Ok(_) => {
                    emitter.emit_order_canceled(&order, Some(venue_order_id), clock.get_time_ns())
                }
                Err(error) => emitter.emit_order_cancel_rejected(
                    &order,
                    Some(venue_order_id),
                    &format!("Gate.io cancel failed: {error}"),
                    clock.get_time_ns(),
                ),
            }
            Ok(())
        });
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let http = self.http_client.clone();
        let symbol = raw_symbol(cmd.instrument_id);
        self.spawn_order_task("cancel_all_orders", async move {
            http.cancel_all(
                GateioProductType::from_symbol(cmd.instrument_id.symbol.as_str()),
                Some(&symbol),
            )
            .await
            .map(|_| ())
            .map_err(|error| anyhow::anyhow!("Gate.io cancel all failed: {error}"))
        });
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        for cancel in cmd.cancels {
            self.cancel_order(cancel)?;
        }
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let http = self.http_client.clone();
        let product_type = self.config.product_type;
        let account_id = self.core.account_id;
        let emitter = self.emitter.clone();
        let clock = self.clock;
        self.spawn_order_task("query_account", async move {
            let state = http
                .account_state(product_type, account_id, clock.get_time_ns())
                .await?;
            emitter.send_account_state(state);
            Ok(())
        });
        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let Some(venue_order_id) = cmd.venue_order_id else {
            anyhow::bail!("Gate.io query order requires a venue order ID");
        };
        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let instrument_id = cmd.instrument_id;
        let account_id = self.core.account_id;
        let clock = self.clock;
        self.spawn_order_task("query_order", async move {
            let result = http
                .order(
                    GateioProductType::from_symbol(instrument_id.symbol.as_str()),
                    &venue_order_id.to_string(),
                    &raw_symbol(instrument_id),
                )
                .await?;
            let instrument = http
                .instrument(
                    instrument_id,
                    GateioProductType::from_symbol(instrument_id.symbol.as_str()),
                    clock.get_time_ns(),
                )
                .await?;
            emitter.send_order_status_report(report_from_order(
                &result,
                &instrument,
                account_id,
                clock.get_time_ns(),
            )?);
            Ok(())
        });
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let Some(instrument_id) = cmd.instrument_id else {
            return Ok(None);
        };
        self.ensure_product(instrument_id)?;
        let Some(venue_order_id) = cmd.venue_order_id else {
            return Ok(None);
        };
        let result = self
            .http_client
            .order(
                self.config.product_type,
                &venue_order_id.to_string(),
                &raw_symbol(instrument_id),
            )
            .await?;
        let instrument = self
            .http_client
            .instrument(
                instrument_id,
                self.config.product_type,
                self.clock.get_time_ns(),
            )
            .await?;
        Ok(Some(report_from_order(
            &result,
            &instrument,
            self.core.account_id,
            self.clock.get_time_ns(),
        )?))
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let instrument_id = cmd.instrument_id;
        if let Some(instrument_id) = instrument_id {
            self.ensure_product(instrument_id)?;
        }

        let symbol = instrument_id.map(raw_symbol);
        let mut orders = self
            .http_client
            .open_orders(self.config.product_type, symbol.as_deref())
            .await?;

        if !cmd.open_only {
            let finished = self
                .http_client
                .orders(self.config.product_type, "finished", symbol.as_deref())
                .await?;
            orders.extend(finished);
        }

        let mut instruments = HashMap::new();
        if let Some(instrument_id) = instrument_id {
            instruments.insert(
                raw_symbol(instrument_id),
                self.http_client
                    .instrument(
                        instrument_id,
                        self.config.product_type,
                        self.clock.get_time_ns(),
                    )
                    .await?,
            );
        }

        let mut seen = HashSet::new();
        let mut reports = Vec::with_capacity(orders.len());
        for order in orders {
            if !seen.insert(order.id.clone()) {
                continue;
            }
            let raw = order
                .contract
                .as_deref()
                .or(order.currency_pair.as_deref())
                .context("Gate.io order report has no instrument symbol")?;
            let instrument = if let Some(instrument) = instruments.get(raw) {
                instrument.clone()
            } else {
                let instrument = self
                    .resolve_report_instrument(instrument_id, Some(raw))
                    .await?;
                instruments.insert(raw.to_string(), instrument.clone());
                instrument
            };
            let report = report_from_order(&order, &instrument, self.core.account_id, cmd.ts_init)?;
            if let Some(start) = cmd.start
                && report.ts_last < start
            {
                continue;
            }
            if let Some(end) = cmd.end
                && report.ts_last > end
            {
                continue;
            }
            reports.push(report);
        }
        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        if let Some(instrument_id) = cmd.instrument_id {
            self.ensure_product(instrument_id)?;
        }
        let raw = cmd.instrument_id.map(raw_symbol);
        let rows = self
            .http_client
            .user_trades(self.config.product_type, raw.as_deref())
            .await?;
        let mut reports = Vec::with_capacity(rows.len());
        let mut seen_trade_ids = HashSet::new();
        for row in rows {
            if !seen_trade_ids.insert(row.id.clone()) {
                continue;
            }
            let raw = row.contract.as_deref().or(row.currency_pair.as_deref());
            let instrument = self
                .resolve_report_instrument(cmd.instrument_id, raw)
                .await?;
            let mut report =
                parse_user_trade(&row, &instrument, self.core.account_id, cmd.ts_init)?;
            if report.client_order_id.is_none() {
                report.client_order_id = self
                    .core
                    .cache()
                    .client_order_id(&report.venue_order_id)
                    .copied();
            }
            if let Some(start) = cmd.start
                && report.ts_event < start
            {
                continue;
            }
            if let Some(end) = cmd.end
                && report.ts_event > end
            {
                continue;
            }
            if let Some(venue_order_id) = cmd.venue_order_id
                && report.venue_order_id != venue_order_id
            {
                continue;
            }
            if let Some(instrument_id) = cmd.instrument_id
                && report.instrument_id != instrument_id
            {
                continue;
            }
            reports.push(report);
        }
        Ok(reports)
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        if self.config.product_type == GateioProductType::Spot {
            return Ok(Vec::new());
        }
        if let Some(instrument_id) = cmd.instrument_id {
            self.ensure_product(instrument_id)?;
        }
        let raw = cmd.instrument_id.map(raw_symbol);
        let rows = self
            .http_client
            .positions(self.config.product_type, raw.as_deref())
            .await?;
        let mut reports = Vec::with_capacity(rows.len());
        for row in rows {
            let instrument = match self
                .resolve_report_instrument(cmd.instrument_id, Some(row.contract.as_str()))
                .await
            {
                Ok(instrument) => instrument,
                Err(error) if cmd.instrument_id.is_none() => {
                    log::warn!("Skipping Gate.io position without a cached instrument: {error}");
                    continue;
                }
                Err(error) => return Err(error),
            };
            let report =
                parse_position_status_report(&row, &instrument, self.core.account_id, cmd.ts_init)?;
            if let Some(start) = cmd.start
                && report.ts_last < start
            {
                continue;
            }
            if let Some(end) = cmd.end
                && report.ts_last > end
            {
                continue;
            }
            reports.push(report);
        }
        Ok(reports)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_now = self.clock.get_time_ns();
        let start = lookback_mins.map(|mins| {
            let lookback_ns = mins.saturating_mul(60).saturating_mul(1_000_000_000);
            UnixNanos::from(ts_now.as_u64().saturating_sub(lookback_ns))
        });

        let order_cmd = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_now)
            .open_only(false)
            .start(start)
            .build()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let fill_cmd = GenerateFillReportsBuilder::default()
            .ts_init(ts_now)
            .start(start)
            .build()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let position_cmd = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_now)
            .start(start)
            .build()
            .map_err(|error| anyhow::anyhow!("{error}"))?;

        let (order_reports, fill_reports, position_reports) = tokio::try_join!(
            self.generate_order_status_reports(&order_cmd),
            self.generate_fill_reports(fill_cmd),
            self.generate_position_status_reports(&position_cmd),
        )?;

        let mut mass_status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            *GATEIO_VENUE,
            ts_now,
            None,
        );
        mass_status.add_order_reports(order_reports);
        mass_status.add_fill_reports(fill_reports);
        mass_status.add_position_reports(position_reports);
        Ok(Some(mass_status))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::parse::parse_perpetual_instrument;
    use crate::http::models::GateioContract;

    fn futures_instrument() -> InstrumentAny {
        let definition: GateioContract = serde_json::from_value(serde_json::json!({
            "name": "BTC_USDT",
            "quanto_multiplier": "0.0001",
            "order_price_round": "0.1",
            "order_size_min": 1,
            "order_size_max": 100000,
            "maker_fee_rate": "-0.0001",
            "taker_fee_rate": "0.00075"
        }))
        .unwrap();
        parse_perpetual_instrument(
            &definition,
            UnixNanos::from(1_000_000_000),
            UnixNanos::from(1_000_000_000),
        )
        .unwrap()
    }

    #[test]
    fn maps_futures_order_status_and_signed_size() {
        let instrument = futures_instrument();
        let order = GateioOrder {
            id: "venue-order-1".to_string(),
            text: "t-CLIENT-1".to_string(),
            currency_pair: None,
            contract: Some("BTC_USDT".to_string()),
            side: None,
            amount: String::new(),
            size: "-3".to_string(),
            left: "0.0".to_string(),
            price: "0".to_string(),
            type_: None,
            status: Some("finished".to_string()),
            tif: Some("ioc".to_string()),
            time_in_force: None,
            finish_as: Some("filled".to_string()),
            create_time_ms: Some(1_700_000_000_000),
            update_time_ms: Some(1_700_000_000_100),
            fill_price: Some("50000".to_string()),
            filled_total: None,
            filled_amount: None,
            filled_size: Some("-3".to_string()),
            avg_deal_price: Some("50000".to_string()),
            fee: None,
            fee_currency: None,
            reduce_only: Some(true),
            is_reduce_only: None,
            is_liq: None,
            auto_size: None,
        };

        let report = report_from_order(
            &order,
            &instrument,
            AccountId::from("GATEIO-001"),
            UnixNanos::from(2),
        )
        .unwrap();
        assert_eq!(report.order_side, OrderSide::Sell);
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.order_type, OrderType::Market);
        assert_eq!(report.time_in_force, TimeInForce::Ioc);
        assert_eq!(report.quantity, Quantity::from("3"));
        assert_eq!(report.filled_qty, Quantity::from("3"));
        assert!(report.reduce_only);
        assert_eq!(
            report.client_order_id,
            Some(ClientOrderId::from("CLIENT-1"))
        );
    }

    #[test]
    fn builds_spot_and_futures_order_requests() {
        let spot_order = OrderInitializedView {
            client_order_id: ClientOrderId::from("SPOT-CLIENT-1"),
            instrument_id: InstrumentId::from("BTC_USDT.GATEIO"),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
            quantity: Quantity::from("0.1"),
            time_in_force: TimeInForce::Gtc,
            price: Some(Price::from("50000")),
            reduce_only: false,
        };
        let spot = build_submit_request(GateioProductType::Spot, &spot_order).unwrap();
        let spot_json = serde_json::to_value(spot).unwrap();
        assert_eq!(spot_json["currency_pair"], "BTC_USDT");
        assert_eq!(spot_json["type"], "limit");
        assert_eq!(spot_json["side"], "buy");
        assert_eq!(spot_json["amount"], "0.1");
        assert_eq!(spot_json["time_in_force"], "gtc");
        assert!(spot_json.get("size").is_none());

        let futures_order = OrderInitializedView {
            client_order_id: ClientOrderId::from("FUTURES-CLIENT-1"),
            instrument_id: InstrumentId::from("BTC_USDT-PERP.GATEIO"),
            order_side: OrderSide::Sell,
            order_type: OrderType::Market,
            quantity: Quantity::from("3"),
            time_in_force: TimeInForce::Gtc,
            price: None,
            reduce_only: true,
        };
        let futures =
            build_submit_request(GateioProductType::UsdtPerpetual, &futures_order).unwrap();
        let futures_json = serde_json::to_value(futures).unwrap();
        assert_eq!(futures_json["contract"], "BTC_USDT");
        assert_eq!(futures_json["size"], "-3");
        assert_eq!(futures_json["price"], "0");
        assert_eq!(futures_json["tif"], "ioc");
        assert_eq!(futures_json["reduce_only"], true);
        assert!(futures_json.get("side").is_none());
    }

    #[test]
    fn limits_gateio_client_order_text_without_breaking_ascii() {
        let id = ClientOrderId::from("CLIENT-ORDER-123456789012345678901234567890");
        let text = gateio_client_order_text(id);
        assert!(text.starts_with("t-"));
        assert!(text.len() <= 30);
        assert!(text.is_ascii());
    }
}
