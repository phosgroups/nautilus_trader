//! Gate.io execution clients.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
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
            GATEIO_FUTURES_AUTO_DELEVERAGES_WS_CHANNEL, GATEIO_FUTURES_BALANCES_WS_CHANNEL,
            GATEIO_FUTURES_LIQUIDATES_WS_CHANNEL, GATEIO_FUTURES_ORDERS_WS_CHANNEL,
            GATEIO_FUTURES_POSITION_CLOSES_WS_CHANNEL, GATEIO_FUTURES_POSITIONS_WS_CHANNEL,
            GATEIO_FUTURES_USER_TRADES_WS_CHANNEL, GATEIO_SPOT_BALANCES_WS_CHANNEL,
            GATEIO_SPOT_ORDERS_WS_CHANNEL, GATEIO_SPOT_USER_TRADES_WS_CHANNEL, GATEIO_VENUE,
        },
        enums::GateioProductType,
        parse::{parse_position_status_report, parse_user_trade, timestamp_nanos},
        symbol::{GateioSymbol, raw_symbol},
    },
    config::GateioExecClientConfig,
    http::{
        client::GateioHttpClient,
        models::{GateioBalanceUpdate, GateioOrder, GateioOrderAmendRequest, GateioOrderRequest},
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
    pending_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    account_refresh_lock: Arc<tokio::sync::Mutex<()>>,
    latest_account_event: Arc<AtomicU64>,
    reconnect_reconciliation_lookback_mins: Option<u64>,
    reconnect_reconciliation_in_flight: Arc<AtomicBool>,
    client_order_ids: Arc<ClientOrderIdMap>,
    seen_orders: Arc<Mutex<DedupCache>>,
    seen_trades: Arc<Mutex<DedupCache>>,
    seen_balances: Arc<Mutex<DedupCache>>,
    seen_positions: Arc<Mutex<DedupCache>>,
    is_connected: AtomicBool,
}

#[derive(Clone)]
struct GateioExecutionWsDispatchContext {
    product_type: GateioProductType,
    account_id: AccountId,
    clock: &'static AtomicTime,
    emitter: ExecutionEventEmitter,
    http_client: GateioHttpClient,
    reconnect_reconciliation_lookback_mins: Option<u64>,
    reconnect_reconciliation_in_flight: Arc<AtomicBool>,
    instruments_by_raw: Arc<HashMap<String, InstrumentAny>>,
    account_refresh_lock: Arc<tokio::sync::Mutex<()>>,
    latest_account_event: Arc<AtomicU64>,
    client_order_ids: Arc<ClientOrderIdMap>,
    seen_orders: Arc<Mutex<DedupCache>>,
    seen_trades: Arc<Mutex<DedupCache>>,
    seen_balances: Arc<Mutex<DedupCache>>,
    seen_positions: Arc<Mutex<DedupCache>>,
    pending_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

const MAX_DEDUP_ENTRIES: usize = 100_000;

#[derive(Debug, Default)]
struct DedupCache {
    keys: HashSet<String>,
    order: VecDeque<String>,
}

#[derive(Debug, Default)]
struct ClientOrderIdMap {
    by_text: Mutex<HashMap<String, ClientOrderId>>,
    by_venue_order_id: Mutex<HashMap<String, ClientOrderId>>,
    by_client_order_id: Mutex<HashMap<String, String>>,
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
        let reconnect_reconciliation_lookback_mins = config.reconnect_reconciliation_lookback_mins;

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            http_client,
            ws_client,
            ws_task: None,
            pending_tasks: Arc::new(Mutex::new(Vec::new())),
            account_refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
            latest_account_event: Arc::new(AtomicU64::new(0)),
            reconnect_reconciliation_lookback_mins,
            reconnect_reconciliation_in_flight: Arc::new(AtomicBool::new(false)),
            client_order_ids: Arc::new(ClientOrderIdMap::default()),
            seen_orders: Arc::new(Mutex::new(DedupCache::default())),
            seen_trades: Arc::new(Mutex::new(DedupCache::default())),
            seen_balances: Arc::new(Mutex::new(DedupCache::default())),
            seen_positions: Arc::new(Mutex::new(DedupCache::default())),
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
        let mut receiver = self.ws_client.take_event_receiver();
        let context = GateioExecutionWsDispatchContext {
            product_type: self.config.product_type,
            account_id: self.core.account_id,
            clock: self.clock,
            emitter: self.emitter.clone(),
            http_client: self.http_client.clone(),
            reconnect_reconciliation_lookback_mins: self.reconnect_reconciliation_lookback_mins,
            reconnect_reconciliation_in_flight: Arc::clone(
                &self.reconnect_reconciliation_in_flight,
            ),
            instruments_by_raw,
            account_refresh_lock: Arc::clone(&self.account_refresh_lock),
            latest_account_event: Arc::clone(&self.latest_account_event),
            client_order_ids: Arc::clone(&self.client_order_ids),
            seen_orders: Arc::clone(&self.seen_orders),
            seen_trades: Arc::clone(&self.seen_trades),
            seen_balances: Arc::clone(&self.seen_balances),
            seen_positions: Arc::clone(&self.seen_positions),
            pending_tasks: Arc::clone(&self.pending_tasks),
        };
        self.ws_task = Some(get_runtime().spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(message) => dispatch_private_message(message, &context),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        log::warn!(
                            "Gate.io execution WebSocket receiver lagged by {count} messages"
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
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
        let client_text = self.client_order_ids.register(order.client_order_id)?;
        build_submit_request(self.config.product_type, order, Some(client_text))
    }
}

fn build_submit_request(
    product_type: GateioProductType,
    order: &OrderInitializedView,
    client_text: Option<String>,
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
        TimeInForce::Gtd => anyhow::bail!("Gate.io does not support GTD time in force"),
        TimeInForce::AtTheOpen | TimeInForce::AtTheClose => {
            anyhow::bail!(
                "Gate.io does not support {:?} time in force",
                order.time_in_force
            )
        }
    };
    anyhow::ensure!(
        order.quantity.is_positive(),
        "Gate.io order quantity must be positive"
    );
    anyhow::ensure!(
        !(order.post_only && order.order_type == OrderType::Market),
        "Gate.io market orders cannot be post-only"
    );
    let client_text = Some(client_text.unwrap_or(gateio_client_order_text(order.client_order_id)?));

    if product_type == GateioProductType::Spot {
        anyhow::ensure!(
            !order.reduce_only,
            "Gate.io Spot orders do not support reduce_only"
        );
        if order.order_type == OrderType::Limit {
            anyhow::ensure!(
                order.price.is_some(),
                "Gate.io limit orders require a price"
            );
        }
        if order.order_type == OrderType::Market {
            let expects_quote_quantity = order.order_side == OrderSide::Buy;
            anyhow::ensure!(
                order.quote_quantity == expects_quote_quantity,
                "Gate.io Spot market {} orders require quote_quantity={}",
                if expects_quote_quantity {
                    "buy"
                } else {
                    "sell"
                },
                expects_quote_quantity
            );
        } else {
            anyhow::ensure!(
                !order.quote_quantity,
                "Gate.io Spot limit orders use base-asset quantity; quote_quantity is unsupported"
            );
        }
        let spot_tif = if order.order_type == OrderType::Market {
            if order.time_in_force == TimeInForce::Fok {
                "fok"
            } else {
                "ioc"
            }
        } else if order.post_only {
            "poc"
        } else {
            tif
        };
        Ok(GateioOrderRequest {
            currency_pair: Some(raw_symbol(order.instrument_id)),
            contract: None,
            type_: Some(order_type.to_string()),
            account: Some("spot".to_string()),
            side: side.to_string(),
            amount: order.quantity.to_string(),
            size: String::new(),
            price: order.price.map(|value| value.to_string()),
            time_in_force: Some(spot_tif.to_string()),
            tif: None,
            text: client_text,
            reduce_only: None,
        })
    } else {
        anyhow::ensure!(
            !order.quote_quantity,
            "Gate.io USDT perpetual orders use contract quantity; quote_quantity is unsupported"
        );
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
            } else if order.post_only {
                "poc".to_string()
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
        self.reap_pending_tasks();
        let task = get_runtime().spawn(async move {
            if let Err(error) = future.await {
                log::error!("Gate.io {name} task failed: {error:?}");
            }
        });
        self.track_task(task);
    }

    fn track_task(&self, task: JoinHandle<()>) {
        if let Ok(mut tasks) = self.pending_tasks.lock() {
            tasks.push(task);
        } else {
            log::error!("Gate.io pending task lock poisoned; aborting task");
            task.abort();
        }
    }

    fn reap_pending_tasks(&self) {
        if let Ok(mut tasks) = self.pending_tasks.lock() {
            tasks.retain(|task| !task.is_finished());
        }
    }

    fn abort_pending_tasks(&self) {
        if let Ok(mut tasks) = self.pending_tasks.lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
        } else {
            log::error!("Gate.io pending task lock poisoned while stopping");
        }
    }
}

fn spawn_tracked_execution_task<F>(tasks: &Arc<Mutex<Vec<JoinHandle<()>>>>, future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let task = get_runtime().spawn(future);
    if let Ok(mut pending) = tasks.lock() {
        pending.retain(|task| !task.is_finished());
        pending.push(task);
    } else {
        log::error!("Gate.io pending task lock poisoned; aborting task");
        task.abort();
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
    post_only: bool,
    reduce_only: bool,
    quote_quantity: bool,
}

fn gateio_client_order_text(client_order_id: ClientOrderId) -> anyhow::Result<String> {
    const MAX_CUSTOM_TEXT_BYTES: usize = 28;
    let custom_text = client_order_id.to_string();
    anyhow::ensure!(
        !custom_text.is_empty(),
        "Gate.io client order ID cannot be empty"
    );
    anyhow::ensure!(
        custom_text.len() <= MAX_CUSTOM_TEXT_BYTES,
        "Gate.io client order ID exceeds the 28-byte text limit: {} bytes",
        custom_text.len()
    );
    anyhow::ensure!(
        custom_text.is_ascii()
            && custom_text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "Gate.io client order ID contains unsupported characters: {custom_text:?}"
    );
    Ok(format!("t-{custom_text}"))
}

impl ClientOrderIdMap {
    fn register(&self, client_order_id: ClientOrderId) -> anyhow::Result<String> {
        let text = gateio_client_order_text(client_order_id)?;
        let mut by_text = self
            .by_text
            .lock()
            .map_err(|_| anyhow::anyhow!("Gate.io client order ID map lock poisoned"))?;
        if let Some(existing) = by_text.get(&text)
            && existing != &client_order_id
        {
            anyhow::bail!("Gate.io client order ID mapping collision for {text}");
        }
        by_text.insert(text.clone(), client_order_id);
        Ok(text)
    }

    fn remember_venue_order(
        &self,
        gate_text: &str,
        venue_order_id: &str,
        client_order_id: ClientOrderId,
    ) {
        if let Ok(mut by_text) = self.by_text.lock() {
            by_text.insert(gate_text.to_string(), client_order_id);
        }
        if let Ok(mut by_venue_order_id) = self.by_venue_order_id.lock() {
            by_venue_order_id.insert(venue_order_id.to_string(), client_order_id);
        }
        if let Ok(mut by_client_order_id) = self.by_client_order_id.lock() {
            by_client_order_id.insert(client_order_id.to_string(), venue_order_id.to_string());
        }
    }

    fn resolve(&self, gate_text: &str, venue_order_id: &str) -> Option<ClientOrderId> {
        self.by_text
            .lock()
            .ok()
            .and_then(|map| map.get(gate_text).copied())
            .or_else(|| {
                self.by_venue_order_id
                    .lock()
                    .ok()
                    .and_then(|map| map.get(venue_order_id).copied())
            })
    }

    fn venue_order_id_for_client(&self, client_order_id: ClientOrderId) -> Option<VenueOrderId> {
        self.by_client_order_id
            .lock()
            .ok()
            .and_then(|map| map.get(&client_order_id.to_string()).cloned())
            .map(|venue_order_id| VenueOrderId::from(venue_order_id.as_str()))
    }
}

const MAX_PRIVATE_SYMBOLS_PER_SUBSCRIPTION: usize = 50;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GateioPrivateChannel {
    SpotOrders,
    SpotUserTrades,
    SpotBalances,
    FuturesOrders,
    FuturesUserTrades,
    FuturesBalances,
    FuturesPositions,
    FuturesLiquidates,
    FuturesAutoDeleverages,
    FuturesPositionCloses,
}

impl GateioPrivateChannel {
    fn parse(channel: &str) -> Option<Self> {
        match channel {
            GATEIO_SPOT_ORDERS_WS_CHANNEL => Some(Self::SpotOrders),
            GATEIO_SPOT_USER_TRADES_WS_CHANNEL => Some(Self::SpotUserTrades),
            GATEIO_SPOT_BALANCES_WS_CHANNEL => Some(Self::SpotBalances),
            GATEIO_FUTURES_ORDERS_WS_CHANNEL => Some(Self::FuturesOrders),
            GATEIO_FUTURES_USER_TRADES_WS_CHANNEL => Some(Self::FuturesUserTrades),
            GATEIO_FUTURES_BALANCES_WS_CHANNEL => Some(Self::FuturesBalances),
            GATEIO_FUTURES_POSITIONS_WS_CHANNEL => Some(Self::FuturesPositions),
            GATEIO_FUTURES_LIQUIDATES_WS_CHANNEL => Some(Self::FuturesLiquidates),
            GATEIO_FUTURES_AUTO_DELEVERAGES_WS_CHANNEL => Some(Self::FuturesAutoDeleverages),
            GATEIO_FUTURES_POSITION_CLOSES_WS_CHANNEL => Some(Self::FuturesPositionCloses),
            _ => None,
        }
    }
}

fn private_subscription_payloads(
    product_type: GateioProductType,
    channel: &str,
    user_id: &str,
    symbols: &[String],
) -> Vec<Vec<String>> {
    match product_type {
        GateioProductType::Spot => match channel {
            GATEIO_SPOT_BALANCES_WS_CHANNEL => vec![Vec::new()],
            _ => symbols
                .chunks(MAX_PRIVATE_SYMBOLS_PER_SUBSCRIPTION)
                .map(ToOwned::to_owned)
                .collect(),
        },
        GateioProductType::UsdtPerpetual => match channel {
            GATEIO_FUTURES_BALANCES_WS_CHANNEL => vec![vec![user_id.to_string()]],
            _ => symbols
                .chunks(MAX_PRIVATE_SYMBOLS_PER_SUBSCRIPTION)
                .map(|chunk| {
                    let mut payload = Vec::with_capacity(chunk.len() + 1);
                    payload.push(user_id.to_string());
                    payload.extend(chunk.iter().cloned());
                    payload
                })
                .collect(),
        },
    }
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

fn reconnect_reconciliation_start(lookback_mins: Option<u64>) -> Option<DateTime<Utc>> {
    lookback_mins.map(|mins| {
        let max_mins = 10 * 365 * 24 * 60;
        Utc::now() - Duration::minutes(mins.min(max_mins) as i64)
    })
}

fn unix_nanos_to_datetime(value: UnixNanos, field: &str) -> anyhow::Result<DateTime<Utc>> {
    let nanos = value.as_u64();
    let seconds = i64::try_from(nanos / 1_000_000_000)
        .with_context(|| format!("Gate.io {field} timestamp is too large"))?;
    let subsec_nanos = u32::try_from(nanos % 1_000_000_000)
        .expect("subsecond nanoseconds are always below one second");
    DateTime::from_timestamp(seconds, subsec_nanos)
        .with_context(|| format!("Gate.io {field} timestamp is out of range"))
}

fn reconcile_order(
    order: GateioOrder,
    context: &GateioExecutionWsDispatchContext,
    ts_init: UnixNanos,
) -> anyhow::Result<bool> {
    let raw = order
        .contract
        .as_deref()
        .or(order.currency_pair.as_deref())
        .context("Gate.io reconciled order has no instrument symbol")?;
    let instrument = context
        .instruments_by_raw
        .get(raw)
        .with_context(|| format!("Gate.io reconciled order symbol is not cached: {raw}"))?;
    let dedup_key = order_dedup_key(raw, &order);
    if dedup_contains(&context.seen_orders, &dedup_key) {
        return Ok(false);
    }
    let report = report_from_order(
        &order,
        instrument,
        context.account_id,
        ts_init,
        Some(&context.client_order_ids),
    )?;
    if remember_once(&context.seen_orders, dedup_key) {
        return Ok(false);
    }
    context.emitter.send_order_status_report(report);
    Ok(true)
}

fn reconcile_trade(
    trade: crate::http::models::GateioUserTrade,
    context: &GateioExecutionWsDispatchContext,
    ts_init: UnixNanos,
) -> anyhow::Result<bool> {
    let raw = trade
        .contract
        .as_deref()
        .or(trade.currency_pair.as_deref())
        .context("Gate.io reconciled trade has no instrument symbol")?;
    let instrument = context
        .instruments_by_raw
        .get(raw)
        .with_context(|| format!("Gate.io reconciled trade symbol is not cached: {raw}"))?;
    let dedup_key = user_trade_dedup_key(&trade, raw);
    if dedup_contains(&context.seen_trades, &dedup_key) {
        return Ok(false);
    }
    let mut report = parse_user_trade(&trade, instrument, context.account_id, ts_init)?;
    if report.client_order_id.is_none() {
        report.client_order_id = context
            .client_order_ids
            .resolve(trade.text.as_deref().unwrap_or_default(), &trade.order_id);
    }
    if remember_once(&context.seen_trades, dedup_key) {
        return Ok(false);
    }
    context.emitter.send_fill_report(report);
    Ok(true)
}

async fn reconcile_after_reconnect(context: GateioExecutionWsDispatchContext) {
    let ts_init = context.clock.get_time_ns();
    let start = reconnect_reconciliation_start(context.reconnect_reconciliation_lookback_mins);
    let end = Utc::now();
    let mut account_states = 0;
    let mut order_reports = 0;
    let mut fill_reports = 0;
    let mut position_reports = 0;
    let mut errors = 0;

    match context
        .http_client
        .account_state_with_timestamps(context.product_type, context.account_id, ts_init, ts_init)
        .await
    {
        Ok(state) => {
            context.emitter.send_account_state(state);
            account_states += 1;
        }
        Err(error) => {
            errors += 1;
            log::error!("Gate.io reconnect account reconciliation failed: {error:?}");
        }
    }

    match context
        .http_client
        .open_orders(context.product_type, None)
        .await
    {
        Ok(orders) => {
            for order in orders {
                match reconcile_order(order, &context, ts_init) {
                    Ok(true) => order_reports += 1,
                    Ok(false) => {}
                    Err(error) => {
                        errors += 1;
                        log::error!(
                            "Gate.io reconnect open-order reconciliation failed: {error:?}"
                        );
                    }
                }
            }
        }
        Err(error) => {
            errors += 1;
            log::error!("Gate.io reconnect open-orders request failed: {error:?}");
        }
    }

    if let Some(start) = start {
        match context
            .http_client
            .orders_in_time_range(context.product_type, None, Some(start), Some(end))
            .await
        {
            Ok(orders) => {
                for order in orders {
                    match reconcile_order(order, &context, ts_init) {
                        Ok(true) => order_reports += 1,
                        Ok(false) => {}
                        Err(error) => {
                            errors += 1;
                            log::error!(
                                "Gate.io reconnect historical-order reconciliation failed: {error:?}"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                errors += 1;
                log::error!("Gate.io reconnect historical-orders request failed: {error:?}");
            }
        }

        match context
            .http_client
            .user_trades_in_time_range(context.product_type, None, Some(start), Some(end))
            .await
        {
            Ok(trades) => {
                for trade in trades {
                    match reconcile_trade(trade, &context, ts_init) {
                        Ok(true) => fill_reports += 1,
                        Ok(false) => {}
                        Err(error) => {
                            errors += 1;
                            log::error!("Gate.io reconnect trade reconciliation failed: {error:?}");
                        }
                    }
                }
            }
            Err(error) => {
                errors += 1;
                log::error!("Gate.io reconnect trades request failed: {error:?}");
            }
        }
    } else {
        log::debug!(
            "Skipping Gate.io reconnect historical order/fill reconciliation because lookback is disabled"
        );
    }

    if context.product_type == GateioProductType::UsdtPerpetual {
        match context
            .http_client
            .positions(context.product_type, None)
            .await
        {
            Ok(positions) => {
                for position in positions {
                    if remember_once(
                        &context.seen_positions,
                        position_dedup_key(&position.contract, &position),
                    ) {
                        continue;
                    }
                    let Some(instrument) = context.instruments_by_raw.get(&position.contract)
                    else {
                        errors += 1;
                        log::error!(
                            "Gate.io reconnect position symbol is not cached: {}",
                            position.contract
                        );
                        continue;
                    };
                    match parse_position_status_report(
                        &position,
                        instrument,
                        context.account_id,
                        ts_init,
                    ) {
                        Ok(report) => {
                            context.emitter.send_position_report(report);
                            position_reports += 1;
                        }
                        Err(error) => {
                            errors += 1;
                            log::error!(
                                "Gate.io reconnect position reconciliation failed: {error:?}"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                errors += 1;
                log::error!("Gate.io reconnect positions request failed: {error:?}");
            }
        }
    }

    log::info!(
        "Gate.io reconnect reconciliation completed: account_states={account_states}, \
         order_reports={order_reports}, fill_reports={fill_reports}, \
         position_reports={position_reports}, errors={errors}"
    );
}

fn user_trade_dedup_key(trade: &crate::http::models::GateioUserTrade, raw_symbol: &str) -> String {
    if !trade.id.trim().is_empty() {
        return format!("{raw_symbol}:{}", trade.id);
    }
    format!(
        "{raw_symbol}:{}:{}:{}:{}:{}:{}:{}",
        trade.order_id,
        trade.price,
        trade.amount,
        trade.size,
        trade
            .create_time_ms
            .or(trade.create_time)
            .unwrap_or_default(),
        trade.side.as_deref().unwrap_or_default(),
        trade.fee.as_deref().unwrap_or_default()
    )
}

fn order_dedup_key(raw_symbol: &str, order: &GateioOrder) -> String {
    let state = serde_json::json!({
        "id": if order.id.is_empty() { order.text.as_str() } else { order.id.as_str() },
        "text": order.text,
        "side": order.side,
        "amount": order.amount,
        "size": order.size,
        "left": order.left,
        "price": order.price,
        "type": order.type_,
        "status": order.status,
        "tif": order.tif,
        "time_in_force": order.time_in_force,
        "finish_as": order.finish_as,
        "filled_amount": order.filled_amount,
        "filled_size": order.filled_size,
        "filled_total": order.filled_total,
        "fill_price": order.fill_price,
        "avg_deal_price": order.avg_deal_price,
        "create_time_ms": order.create_time_ms,
        "update_time_ms": order.update_time_ms,
        "finish_time_ms": order.finish_time_ms,
        "update_id": order.update_id,
        "reduce_only": order.reduce_only.or(order.is_reduce_only),
        "is_liq": order.is_liq,
        "auto_size": order.auto_size,
        "close": order.close.or(order.is_close),
    });
    format!("{raw_symbol}:{}", state)
}

fn balance_dedup_key(channel: &str, rows: &[serde_json::Value]) -> String {
    let mut serialized = rows.iter().map(ToString::to_string).collect::<Vec<_>>();
    serialized.sort_unstable();
    format!("{channel}:{}", serialized.join("|"))
}

fn position_dedup_key(raw_symbol: &str, position: &crate::http::models::GateioPosition) -> String {
    if let Some(update_id) = position
        .update_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return format!("{raw_symbol}:update_id:{update_id}");
    }

    let state = serde_json::json!({
        "contract": position.contract,
        "size": position.size,
        "value": position.value,
        "entry_price": position.entry_price,
        "mark_price": position.mark_price,
        "update_time": position.update_time,
        "create_time": position.create_time,
    });
    format!("{raw_symbol}:state:{state}")
}

fn remember_once(cache: &Mutex<DedupCache>, key: String) -> bool {
    let Ok(mut cache) = cache.lock() else {
        return true;
    };
    if cache.keys.contains(&key) {
        return true;
    }
    cache.keys.insert(key.clone());
    cache.order.push_back(key);
    while cache.order.len() > MAX_DEDUP_ENTRIES {
        if let Some(oldest) = cache.order.pop_front() {
            cache.keys.remove(&oldest);
        }
    }
    false
}

fn dedup_contains(cache: &Mutex<DedupCache>, key: &str) -> bool {
    cache
        .lock()
        .map(|cache| cache.keys.contains(key))
        .unwrap_or(true)
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
    if message.channel == crate::websocket::GATEIO_INTERNAL_RECONNECTED_CHANNEL {
        log::info!(
            "Gate.io private WebSocket subscriptions restored; starting REST reconciliation"
        );
        if context
            .reconnect_reconciliation_in_flight
            .swap(true, Ordering::AcqRel)
        {
            log::debug!("Skipping Gate.io reconnect reconciliation already in flight");
            return;
        }
        let context = context.clone();
        let pending_tasks = Arc::clone(&context.pending_tasks);
        spawn_tracked_execution_task(&pending_tasks, async move {
            reconcile_after_reconnect(context.clone()).await;
            context
                .reconnect_reconciliation_in_flight
                .store(false, Ordering::Release);
        });
        return;
    }

    if message.event != "update" {
        return;
    }

    let Some(channel) = GateioPrivateChannel::parse(&message.channel) else {
        return;
    };

    match channel {
        GateioPrivateChannel::SpotOrders | GateioPrivateChannel::FuturesOrders => {
            for value in private_rows(&message.result) {
                let Ok(order) = serde_json::from_value::<GateioOrder>(value.clone()) else {
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
                let dedup_key = order_dedup_key(&raw, &order);
                if remember_once(&context.seen_orders, dedup_key) {
                    continue;
                }
                match report_from_order(
                    &order,
                    instrument,
                    context.account_id,
                    context.clock.get_time_ns(),
                    Some(&context.client_order_ids),
                ) {
                    Ok(report) => context.emitter.send_order_status_report(report),
                    Err(error) => {
                        log::warn!("Failed to parse Gate.io private order event: {error}")
                    }
                }
            }
        }
        GateioPrivateChannel::SpotUserTrades | GateioPrivateChannel::FuturesUserTrades => {
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
                if remember_once(&context.seen_trades, user_trade_dedup_key(&trade, &raw)) {
                    continue;
                }
                match parse_user_trade(
                    &trade,
                    instrument,
                    context.account_id,
                    context.clock.get_time_ns(),
                ) {
                    Ok(mut report) => {
                        if report.client_order_id.is_none() {
                            report.client_order_id = context.client_order_ids.resolve(
                                trade.text.as_deref().unwrap_or_default(),
                                &trade.order_id,
                            );
                        }
                        context.emitter.send_fill_report(report);
                    }
                    Err(error) => {
                        log::warn!("Failed to parse Gate.io private fill event: {error}")
                    }
                }
            }
        }
        GateioPrivateChannel::SpotBalances | GateioPrivateChannel::FuturesBalances => {
            let rows = private_rows(&message.result);
            let mut valid_rows = Vec::new();
            let mut event_time = None;
            for value in rows {
                let Ok(update) = serde_json::from_value::<GateioBalanceUpdate>(value.clone())
                else {
                    log::debug!("Failed to decode Gate.io private balance event");
                    continue;
                };
                if update.currency.trim().is_empty() {
                    log::debug!("Gate.io private balance event has no currency");
                    continue;
                }
                event_time = event_time.max(update.time_ms.or(update.time));
                valid_rows.push(value);
            }
            if valid_rows.is_empty() {
                return;
            }
            if remember_once(
                &context.seen_balances,
                balance_dedup_key(&message.channel, &valid_rows),
            ) {
                return;
            }

            let ts_init = context.clock.get_time_ns();
            let ts_event = event_time
                .and_then(|value| timestamp_nanos(value).ok())
                .unwrap_or(ts_init);
            let http = context.http_client.clone();
            let emitter = context.emitter.clone();
            let product_type = context.product_type;
            let account_id = context.account_id;
            let account_refresh_lock = Arc::clone(&context.account_refresh_lock);
            let latest_account_event = Arc::clone(&context.latest_account_event);
            let event_ns = ts_event.as_u64();
            let previous = latest_account_event.fetch_max(event_ns, Ordering::AcqRel);
            if event_ns < previous {
                return;
            }
            spawn_tracked_execution_task(&context.pending_tasks, async move {
                let _guard = account_refresh_lock.lock().await;
                if latest_account_event.load(Ordering::Acquire) > event_ns {
                    return;
                }
                match http
                    .account_state_with_timestamps(product_type, account_id, ts_event, ts_init)
                    .await
                {
                    Ok(state) => emitter.send_account_state(state),
                    Err(error) => {
                        log::warn!("Failed to refresh Gate.io account after balance event: {error}")
                    }
                }
            });
        }
        GateioPrivateChannel::FuturesPositions
            if context.product_type == GateioProductType::UsdtPerpetual =>
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
                if remember_once(&context.seen_positions, position_dedup_key(&raw, &position)) {
                    continue;
                }
                match parse_position_status_report(
                    &position,
                    instrument,
                    context.account_id,
                    context.clock.get_time_ns(),
                ) {
                    Ok(report) => context.emitter.send_position_report(report),
                    Err(error) => {
                        log::warn!("Failed to parse Gate.io private position event: {error}")
                    }
                }
            }
        }
        GateioPrivateChannel::FuturesLiquidates
        | GateioPrivateChannel::FuturesAutoDeleverages
        | GateioPrivateChannel::FuturesPositionCloses
            if context.product_type == GateioProductType::UsdtPerpetual =>
        {
            let http = context.http_client.clone();
            let emitter = context.emitter.clone();
            let instruments = Arc::clone(&context.instruments_by_raw);
            let seen_positions = Arc::clone(&context.seen_positions);
            let account_id = context.account_id;
            let ts_init = context.clock.get_time_ns();
            spawn_tracked_execution_task(&context.pending_tasks, async move {
                match http.positions(GateioProductType::UsdtPerpetual, None).await {
                    Ok(rows) => {
                        for row in rows {
                            let Some(instrument) = instruments.get(&row.contract) else {
                                continue;
                            };
                            if remember_once(
                                &seen_positions,
                                position_dedup_key(&row.contract, &row),
                            ) {
                                continue;
                            }
                            match parse_position_status_report(
                                &row, instrument, account_id, ts_init,
                            ) {
                                Ok(report) => emitter.send_position_report(report),
                                Err(error) => {
                                    log::warn!(
                                        "Failed to reconcile Gate.io position event: {error}"
                                    )
                                }
                            }
                        }
                    }
                    Err(error) => {
                        log::warn!("Failed to reconcile Gate.io special futures event: {error}")
                    }
                }
            });
        }
        _ => {}
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
        "closed" | "finished" => OrderStatus::Canceled,
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
    client_order_ids: Option<&ClientOrderIdMap>,
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
    let is_futures = value.contract.is_some();
    let is_spot_market_buy =
        !is_futures && order_type == OrderType::Market && side == OrderSide::Buy;
    let order_status = parse_order_status(value);
    let filled_decimal = if is_futures {
        value
            .filled_size
            .as_deref()
            .or(value.filled_amount.as_deref())
            .map(|value| {
                crate::common::parse::decimal(value.trim_start_matches('-'), "order.filled_size")
                    .map(|value| value.abs())
            })
            .transpose()?
            .unwrap_or_else(|| {
                let total = crate::common::parse::decimal(&value.size, "order.size")
                    .unwrap_or_default()
                    .abs();
                let left = crate::common::parse::decimal(&value.left, "order.left")
                    .unwrap_or_default()
                    .abs();
                (total - left).max(rust_decimal::Decimal::ZERO)
            })
    } else {
        spot_filled_base_quantity(value)?
    };

    // Gate's Spot market-buy `amount` is a quote-currency budget, while fills
    // are reported in base units. Keep the budget as quote quantity until a
    // fill gives us an execution price. Once a fill exists, convert the order
    // budget to an estimated base quantity so reconciliation can apply base
    // fills to the local order. A terminal Filled report uses the actual filled
    // base quantity because the quote budget has been fully consumed.
    let (quantity_decimal, is_quote_quantity) = if is_spot_market_buy {
        if filled_decimal.is_zero() {
            (
                crate::common::parse::decimal(&value.amount, "order.amount")?.abs(),
                true,
            )
        } else if order_status == OrderStatus::Filled {
            (filled_decimal, false)
        } else if let Some(avg_px) = spot_average_fill_price(value)? {
            let quote_amount = crate::common::parse::decimal(&value.amount, "order.amount")?.abs();
            ((quote_amount / avg_px).max(filled_decimal), false)
        } else {
            // There is no safe way to infer the requested base quantity without
            // an execution price. Using the observed fill as the quantity keeps
            // the report internally consistent and lets the terminal status be
            // reconciled without fabricating a larger position.
            (filled_decimal, false)
        }
    } else if is_futures {
        (
            crate::common::parse::decimal(&value.size, "order.size")?.abs(),
            false,
        )
    } else {
        (
            crate::common::parse::decimal(&value.amount, "order.amount")?.abs(),
            false,
        )
    };
    let quantity = Quantity::from_decimal_dp(quantity_decimal, instrument.size_precision())
        .context("invalid Gate.io order quantity")?;
    let filled_qty = Quantity::from_decimal_dp(filled_decimal, instrument.size_precision())
        .context("invalid Gate.io filled quantity")?;
    let ts_accepted = value
        .create_time_ms
        .and_then(|value| crate::common::parse::timestamp_nanos(value).ok())
        .unwrap_or(ts_init);
    let ts_last = value
        .update_time_ms
        .and_then(|value| crate::common::parse::timestamp_nanos(value).ok())
        .unwrap_or(ts_init);
    let client_order_id = client_order_ids
        .and_then(|map| map.resolve(&value.text, &value.id))
        .or_else(|| client_order_id_from_text(&value.text));
    let venue_order_id = VenueOrderId::from(value.id.as_str());
    let report = OrderStatusReport::new(
        account_id,
        instrument.id(),
        client_order_id,
        venue_order_id,
        side,
        order_type,
        tif,
        order_status,
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
    let report = if let Some(avg_px) = spot_average_fill_price(value)? {
        report.with_avg_px(
            avg_px
                .to_string()
                .parse::<f64>()
                .context("invalid Gate.io order average price")?,
        )?
    } else {
        report
    };
    let post_only = value
        .tif
        .as_deref()
        .or(value.time_in_force.as_deref())
        .is_some_and(|tif| tif.eq_ignore_ascii_case("poc"));
    Ok(report
        .with_is_quote_quantity(is_quote_quantity)
        .with_post_only(post_only)
        .with_reduce_only(value.reduce_only.or(value.is_reduce_only).unwrap_or(false)))
}

fn spot_average_fill_price(value: &GateioOrder) -> anyhow::Result<Option<rust_decimal::Decimal>> {
    if let Some(avg_px) = value
        .avg_deal_price
        .as_deref()
        .filter(|value| !value.trim().is_empty() && *value != "0")
    {
        return Ok(Some(crate::common::parse::decimal(
            avg_px,
            "order.avg_deal_price",
        )?));
    }

    if let (Some(filled_total), Some(filled_amount)) = (
        value
            .filled_total
            .as_deref()
            .filter(|value| !value.trim().is_empty()),
        value
            .filled_amount
            .as_deref()
            .filter(|value| !value.trim().is_empty()),
    ) {
        let filled_total = crate::common::parse::decimal(filled_total, "order.filled_total")?;
        let filled_amount = crate::common::parse::decimal(filled_amount, "order.filled_amount")?;
        if !filled_amount.is_zero() {
            return Ok(Some((filled_total / filled_amount).abs()));
        }
    }

    Ok(value
        .fill_price
        .as_deref()
        .filter(|value| !value.trim().is_empty() && *value != "0")
        .map(|value| crate::common::parse::decimal(value, "order.fill_price"))
        .transpose()?)
}

fn spot_filled_base_quantity(value: &GateioOrder) -> anyhow::Result<rust_decimal::Decimal> {
    if let Some(filled_amount) = value
        .filled_amount
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return crate::common::parse::decimal(filled_amount, "order.filled_amount")
            .map(|value| value.abs());
    }

    let filled_total = value
        .filled_total
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| crate::common::parse::decimal(value, "order.filled_total"))
        .transpose()?;
    let average_price = spot_average_fill_price(value)?;
    if let (Some(filled_total), Some(average_price)) = (filled_total, average_price) {
        anyhow::ensure!(
            average_price.is_sign_positive(),
            "Gate.io order average price must be positive when filled_total is present"
        );
        return Ok((filled_total / average_price).abs());
    }

    // For a Spot limit or sell order, amount and left are both base units. A
    // market buy is deliberately excluded because those fields are quote
    // units and cannot be converted without an execution price.
    let is_market_buy = value
        .type_
        .as_deref()
        .is_some_and(|kind| kind.eq_ignore_ascii_case("market"))
        && value
            .side
            .as_deref()
            .is_some_and(|side| side.eq_ignore_ascii_case("buy"));
    if is_market_buy {
        return Ok(rust_decimal::Decimal::ZERO);
    }
    let amount = crate::common::parse::decimal(&value.amount, "order.amount")?;
    let left = crate::common::parse::decimal(&value.left, "order.left")?;
    Ok((amount - left).max(rust_decimal::Decimal::ZERO))
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
        self.http_client.cancel_all_requests();
        self.abort_pending_tasks();
        self.ws_client.stop();
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
            .instruments_for(
                self.config.product_type,
                self.config.instrument_ids.as_deref(),
                ts,
            )
            .await
            .context("failed to load Gate.io execution instruments")?;
        let instruments_by_raw = Arc::new(
            instruments
                .into_iter()
                .map(|instrument| (instrument.raw_symbol().to_string(), instrument))
                .collect::<HashMap<_, _>>(),
        );
        let mut private_payload = instruments_by_raw.keys().cloned().collect::<Vec<_>>();
        private_payload.sort_unstable();
        let user_id = self
            .http_client
            .user_id()
            .await
            .context("failed to load Gate.io account user_id")?;
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
                GATEIO_FUTURES_LIQUIDATES_WS_CHANNEL,
                GATEIO_FUTURES_AUTO_DELEVERAGES_WS_CHANNEL,
                GATEIO_FUTURES_POSITION_CLOSES_WS_CHANNEL,
            ]
        };
        for channel in private_channels {
            let payloads = private_subscription_payloads(
                self.config.product_type,
                channel,
                &user_id,
                &private_payload,
            );
            for payload in payloads {
                self.ws_client
                    .subscribe(channel, payload.clone(), true)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to subscribe Gate.io private channel {channel} with payload {payload:?}",
                        )
                    })?;
            }
        }

        self.core.set_connected();
        self.is_connected.store(true, Ordering::Release);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.http_client.cancel_all_requests();
        self.abort_pending_tasks();
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
            post_only: cmd.order_init.post_only,
            reduce_only: cmd.order_init.reduce_only,
            quote_quantity: cmd.order_init.quote_quantity,
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
        let client_order_ids = Arc::clone(&self.client_order_ids);
        let client_order_id = order.client_order_id();
        self.spawn_order_task("submit_order", async move {
            match http.submit_order(&request).await {
                Ok(ack) => {
                    if !ack.id.is_empty() {
                        if let Some(text) = request.text.as_deref() {
                            client_order_ids.remember_venue_order(text, &ack.id, client_order_id);
                        }
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
        let venue_order_id = cmd.venue_order_id.or_else(|| {
            self.client_order_ids
                .venue_order_id_for_client(cmd.client_order_id)
        });
        let Some(venue_order_id) = venue_order_id else {
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
        let order_side = cmd.order_side;
        self.spawn_order_task("cancel_all_orders", async move {
            http.cancel_all(
                GateioProductType::from_symbol(cmd.instrument_id.symbol.as_str()),
                Some(&symbol),
                order_side,
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
        let client_order_ids = Arc::clone(&self.client_order_ids);
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
                Some(&client_order_ids),
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
            Some(&self.client_order_ids),
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
            let start = cmd
                .start
                .map(|value| unix_nanos_to_datetime(value, "order start"))
                .transpose()?;
            let end = cmd
                .end
                .map(|value| unix_nanos_to_datetime(value, "order end"))
                .transpose()?;
            let finished = if start.is_some() || end.is_some() {
                self.http_client
                    .orders_in_time_range(self.config.product_type, symbol.as_deref(), start, end)
                    .await?
            } else {
                self.http_client
                    .orders(self.config.product_type, "finished", symbol.as_deref())
                    .await?
            };
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
            let report = report_from_order(
                &order,
                &instrument,
                self.core.account_id,
                cmd.ts_init,
                Some(&self.client_order_ids),
            )?;
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
        let start = cmd
            .start
            .map(|value| unix_nanos_to_datetime(value, "fill start"))
            .transpose()?;
        let end = cmd
            .end
            .map(|value| unix_nanos_to_datetime(value, "fill end"))
            .transpose()?;
        let rows = if start.is_some() || end.is_some() {
            self.http_client
                .user_trades_in_time_range(self.config.product_type, raw.as_deref(), start, end)
                .await?
        } else {
            self.http_client
                .user_trades(self.config.product_type, raw.as_deref())
                .await?
        };
        let mut reports = Vec::with_capacity(rows.len());
        let mut seen_trade_ids = HashSet::new();
        for row in rows {
            let raw = row.contract.as_deref().or(row.currency_pair.as_deref());
            let dedup_key = user_trade_dedup_key(&row, raw.unwrap_or_default());
            if !seen_trade_ids.insert(dedup_key.clone()) {
                continue;
            }
            let instrument = self
                .resolve_report_instrument(cmd.instrument_id, raw)
                .await?;
            let mut report =
                parse_user_trade(&row, &instrument, self.core.account_id, cmd.ts_init)?;
            if report.client_order_id.is_none() {
                report.client_order_id = self
                    .client_order_ids
                    .resolve(row.text.as_deref().unwrap_or_default(), &row.order_id);
            }
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
    use crate::common::parse::{parse_perpetual_instrument, parse_spot_instrument};
    use crate::http::models::{GateioContract, GateioSpotPair};

    fn spot_instrument() -> InstrumentAny {
        let definition: GateioSpotPair = serde_json::from_value(serde_json::json!({
            "id": "BTC_USDT",
            "base": "BTC",
            "quote": "USDT",
            "precision": 2,
            "amount_precision": 3,
            "amount_point": "0.001",
            "min_base_amount": "0.001",
            "fee": "0.2"
        }))
        .unwrap();
        parse_spot_instrument(
            &definition,
            UnixNanos::from(1_000_000_000),
            UnixNanos::from(1_000_000_000),
        )
        .unwrap()
    }

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
            finish_time_ms: None,
            update_id: None,
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
            close: None,
            is_close: None,
        };

        let report = report_from_order(
            &order,
            &instrument,
            AccountId::from("GATEIO-001"),
            UnixNanos::from(2),
            None,
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
    fn maps_spot_order_quantities_in_the_correct_units() {
        let instrument = spot_instrument();
        let account_id = AccountId::from("GATEIO-001");
        let ts_init = UnixNanos::from(2);

        let fully_filled_market_buy: GateioOrder = serde_json::from_value(serde_json::json!({
            "id": "spot-market-buy-1",
            "text": "t-MARKET-BUY-1",
            "currency_pair": "BTC_USDT",
            "side": "buy",
            "type": "market",
            "amount": "100",
            "left": "0",
            "filled_amount": "0.002",
            "filled_total": "100",
            "avg_deal_price": "50000",
            "price": "0",
            "time_in_force": "ioc",
            "status": "closed",
            "finish_as": "filled"
        }))
        .unwrap();
        let report = report_from_order(
            &fully_filled_market_buy,
            &instrument,
            account_id,
            ts_init,
            None,
        )
        .unwrap();
        assert_eq!(report.quantity, Quantity::from("0.002"));
        assert_eq!(report.filled_qty, Quantity::from("0.002"));
        assert!(!report.is_quote_quantity);
        assert_eq!(report.order_type, OrderType::Market);
        assert_eq!(report.order_status, OrderStatus::Filled);

        let partial_market_buy: GateioOrder = serde_json::from_value(serde_json::json!({
            "id": "spot-market-buy-2",
            "text": "t-MARKET-BUY-2",
            "currency_pair": "BTC_USDT",
            "side": "buy",
            "type": "market",
            "amount": "100",
            "left": "50",
            "filled_amount": "0.001",
            "filled_total": "50",
            "avg_deal_price": "50000",
            "price": "0",
            "time_in_force": "ioc",
            "status": "cancelled",
            "finish_as": "ioc"
        }))
        .unwrap();
        let report =
            report_from_order(&partial_market_buy, &instrument, account_id, ts_init, None).unwrap();
        assert_eq!(report.quantity, Quantity::from("0.002"));
        assert_eq!(report.filled_qty, Quantity::from("0.001"));
        assert!(!report.is_quote_quantity);
        assert_eq!(report.order_status, OrderStatus::Canceled);

        let unfilled_market_buy: GateioOrder = serde_json::from_value(serde_json::json!({
            "id": "spot-market-buy-3",
            "text": "t-MARKET-BUY-3",
            "currency_pair": "BTC_USDT",
            "side": "buy",
            "type": "market",
            "amount": "100",
            "left": "100",
            "price": "0",
            "time_in_force": "fok",
            "status": "cancelled",
            "finish_as": "fok"
        }))
        .unwrap();
        let report =
            report_from_order(&unfilled_market_buy, &instrument, account_id, ts_init, None)
                .unwrap();
        assert_eq!(report.quantity, Quantity::from("100"));
        assert_eq!(report.filled_qty, Quantity::zero(3));
        assert!(report.is_quote_quantity);
        assert_eq!(report.order_status, OrderStatus::Canceled);

        let market_sell: GateioOrder = serde_json::from_value(serde_json::json!({
            "id": "spot-market-sell-1",
            "text": "t-MARKET-SELL-1",
            "currency_pair": "BTC_USDT",
            "side": "sell",
            "type": "market",
            "amount": "0.003",
            "left": "0.001",
            "filled_amount": "0.002",
            "filled_total": "100",
            "avg_deal_price": "50000",
            "price": "0",
            "time_in_force": "ioc",
            "status": "cancelled",
            "finish_as": "ioc"
        }))
        .unwrap();
        let report =
            report_from_order(&market_sell, &instrument, account_id, ts_init, None).unwrap();
        assert_eq!(report.quantity, Quantity::from("0.003"));
        assert_eq!(report.filled_qty, Quantity::from("0.002"));
        assert!(!report.is_quote_quantity);

        let limit_order_without_filled_amount: GateioOrder =
            serde_json::from_value(serde_json::json!({
                "id": "spot-limit-1",
                "text": "t-LIMIT-1",
                "currency_pair": "BTC_USDT",
                "side": "buy",
                "type": "limit",
                "amount": "0.003",
                "left": "0.001",
                "price": "50000",
                "time_in_force": "gtc",
                "status": "open"
            }))
            .unwrap();
        let report = report_from_order(
            &limit_order_without_filled_amount,
            &instrument,
            account_id,
            ts_init,
            None,
        )
        .unwrap();
        assert_eq!(report.quantity, Quantity::from("0.003"));
        assert_eq!(report.filled_qty, Quantity::from("0.002"));
    }

    #[test]
    fn restores_post_only_from_gate_poc_time_in_force() {
        let instrument = spot_instrument();
        let order: GateioOrder = serde_json::from_value(serde_json::json!({
            "id": "spot-post-only-1",
            "text": "t-POST-ONLY-1",
            "currency_pair": "BTC_USDT",
            "side": "sell",
            "type": "limit",
            "amount": "0.003",
            "left": "0.003",
            "price": "50000",
            "time_in_force": "poc",
            "status": "open"
        }))
        .unwrap();
        let report = report_from_order(
            &order,
            &instrument,
            AccountId::from("GATEIO-001"),
            UnixNanos::from(2),
            None,
        )
        .unwrap();
        assert!(report.post_only);
        assert_eq!(report.time_in_force, TimeInForce::Gtc);
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
            post_only: false,
            reduce_only: false,
            quote_quantity: false,
        };
        let spot = build_submit_request(GateioProductType::Spot, &spot_order, None).unwrap();
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
            post_only: false,
            reduce_only: true,
            quote_quantity: false,
        };
        let futures =
            build_submit_request(GateioProductType::UsdtPerpetual, &futures_order, None).unwrap();
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
        let id = ClientOrderId::from("CLIENT-ORDER-123456789012345");
        let text = gateio_client_order_text(id).unwrap();
        assert!(text.starts_with("t-"));
        assert_eq!(text, "t-CLIENT-ORDER-123456789012345");
        assert!(text.is_ascii());
    }

    #[test]
    fn rejects_unsafe_or_overlong_gateio_client_order_text() {
        assert!(gateio_client_order_text(ClientOrderId::from("CLIENT ORDER")).is_err());
        assert!(
            gateio_client_order_text(ClientOrderId::from("CLIENT-ORDER-12345678901234567890"))
                .is_err()
        );
    }

    #[test]
    fn builds_product_specific_private_subscription_payloads() {
        let symbols = vec!["BTC_USDT".to_string(), "ETH_USDT".to_string()];
        assert_eq!(
            private_subscription_payloads(
                GateioProductType::Spot,
                GATEIO_SPOT_ORDERS_WS_CHANNEL,
                "123",
                &symbols,
            ),
            vec![symbols.clone()]
        );
        assert_eq!(
            private_subscription_payloads(
                GateioProductType::Spot,
                GATEIO_SPOT_BALANCES_WS_CHANNEL,
                "123",
                &symbols,
            ),
            vec![Vec::<String>::new()]
        );
        assert_eq!(
            private_subscription_payloads(
                GateioProductType::UsdtPerpetual,
                GATEIO_FUTURES_ORDERS_WS_CHANNEL,
                "123",
                &symbols,
            ),
            vec![vec!["123", "BTC_USDT", "ETH_USDT"]]
        );
        assert_eq!(
            private_subscription_payloads(
                GateioProductType::UsdtPerpetual,
                GATEIO_FUTURES_BALANCES_WS_CHANNEL,
                "123",
                &symbols,
            ),
            vec![vec!["123"]]
        );
    }

    #[test]
    fn splits_spot_private_subscriptions_into_bounded_batches() {
        let symbols = (0..=MAX_PRIVATE_SYMBOLS_PER_SUBSCRIPTION)
            .map(|index| format!("ASSET{index}_USDT"))
            .collect::<Vec<_>>();
        let payloads = private_subscription_payloads(
            GateioProductType::Spot,
            GATEIO_SPOT_ORDERS_WS_CHANNEL,
            "123",
            &symbols,
        );
        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0].len(), MAX_PRIVATE_SYMBOLS_PER_SUBSCRIPTION);
        assert_eq!(payloads[1].len(), 1);
    }

    #[test]
    fn futures_private_payloads_include_user_id_in_each_batch() {
        let symbols = (0..=MAX_PRIVATE_SYMBOLS_PER_SUBSCRIPTION)
            .map(|index| format!("ASSET{index}_USDT"))
            .collect::<Vec<_>>();
        let payloads = private_subscription_payloads(
            GateioProductType::UsdtPerpetual,
            GATEIO_FUTURES_POSITIONS_WS_CHANNEL,
            "123",
            &symbols,
        );
        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0][0], "123");
        assert_eq!(payloads[1][0], "123");
        assert_eq!(payloads[0].len(), MAX_PRIVATE_SYMBOLS_PER_SUBSCRIPTION + 1);
        assert_eq!(payloads[1].len(), 2);
    }

    #[test]
    fn parses_private_channels_without_suffix_matching() {
        assert_eq!(
            GateioPrivateChannel::parse(GATEIO_SPOT_ORDERS_WS_CHANNEL),
            Some(GateioPrivateChannel::SpotOrders)
        );
        assert_eq!(
            GateioPrivateChannel::parse(GATEIO_SPOT_USER_TRADES_WS_CHANNEL),
            Some(GateioPrivateChannel::SpotUserTrades)
        );
        assert_eq!(
            GateioPrivateChannel::parse(GATEIO_FUTURES_POSITIONS_WS_CHANNEL),
            Some(GateioPrivateChannel::FuturesPositions)
        );
        assert_eq!(
            GateioPrivateChannel::parse(GATEIO_FUTURES_POSITION_CLOSES_WS_CHANNEL),
            Some(GateioPrivateChannel::FuturesPositionCloses)
        );
        assert_eq!(GateioPrivateChannel::parse("futures.orders.extra"), None);
    }

    #[test]
    fn rejects_long_client_order_ids_in_the_mapping() {
        let map = ClientOrderIdMap::default();
        let result = map.register(ClientOrderId::from("CLIENT-ORDER-12345678901234567890"));
        assert!(result.is_err());
    }

    #[test]
    fn remembers_venue_order_id_by_client_order_id() {
        let map = ClientOrderIdMap::default();
        let client_order_id = ClientOrderId::from("CLIENT-ORDER-1");

        map.remember_venue_order("t-CLIENT-ORDER-1", "123456789", client_order_id);

        assert_eq!(
            map.venue_order_id_for_client(client_order_id),
            Some(VenueOrderId::from("123456789"))
        );
    }

    #[test]
    fn updates_remembered_venue_order_id_for_client_order_id() {
        let map = ClientOrderIdMap::default();
        let client_order_id = ClientOrderId::from("CLIENT-ORDER-1");

        map.remember_venue_order("t-CLIENT-ORDER-1", "123456789", client_order_id);
        map.remember_venue_order("t-CLIENT-ORDER-1", "987654321", client_order_id);

        assert_eq!(
            map.venue_order_id_for_client(client_order_id),
            Some(VenueOrderId::from("987654321"))
        );
    }

    #[test]
    fn order_dedup_key_keeps_same_timestamp_price_amend() {
        let mut first = GateioOrder {
            id: "venue-order-1".to_string(),
            price: "50000".to_string(),
            update_time_ms: Some(1_700_000_000_000),
            ..Default::default()
        };
        let second = GateioOrder {
            price: "50001".to_string(),
            ..first.clone()
        };
        assert_ne!(
            order_dedup_key("BTC_USDT", &first),
            order_dedup_key("BTC_USDT", &second)
        );
        first.update_id = Some("same-update-id".to_string());
        let duplicate = first.clone();
        assert_eq!(
            order_dedup_key("BTC_USDT", &first),
            order_dedup_key("BTC_USDT", &duplicate)
        );
    }

    #[test]
    fn position_dedup_key_prefers_gate_update_id() {
        let first = crate::http::models::GateioPosition {
            contract: "BTC_USDT".to_string(),
            size: "3".to_string(),
            value: "150000".to_string(),
            entry_price: "50000".to_string(),
            mark_price: "50001".to_string(),
            update_id: Some("42".to_string()),
            update_time: Some(1_700_000_000_000),
            create_time: Some(1_700_000_000_000),
        };
        let same_version = crate::http::models::GateioPosition {
            mark_price: "50002".to_string(),
            update_time: Some(1_700_000_000_100),
            ..first.clone()
        };
        let next_version = crate::http::models::GateioPosition {
            update_id: Some("43".to_string()),
            ..first.clone()
        };

        assert_eq!(
            position_dedup_key("BTC_USDT", &first),
            position_dedup_key("BTC_USDT", &same_version)
        );
        assert_ne!(
            position_dedup_key("BTC_USDT", &first),
            position_dedup_key("BTC_USDT", &next_version)
        );
    }

    #[test]
    fn position_dedup_key_falls_back_to_complete_state() {
        let first = crate::http::models::GateioPosition {
            contract: "BTC_USDT".to_string(),
            size: "3".to_string(),
            value: "150000".to_string(),
            entry_price: "50000".to_string(),
            mark_price: "50001".to_string(),
            ..Default::default()
        };
        let same = first.clone();
        let changed = crate::http::models::GateioPosition {
            mark_price: "50002".to_string(),
            ..first.clone()
        };

        assert_eq!(
            position_dedup_key("BTC_USDT", &first),
            position_dedup_key("BTC_USDT", &same)
        );
        assert_ne!(
            position_dedup_key("BTC_USDT", &first),
            position_dedup_key("BTC_USDT", &changed)
        );
    }

    #[test]
    fn user_trade_dedup_key_is_scoped_to_symbol() {
        let trade = crate::http::models::GateioUserTrade {
            id: "trade-1".to_string(),
            ..Default::default()
        };
        assert_ne!(
            user_trade_dedup_key(&trade, "BTC_USDT"),
            user_trade_dedup_key(&trade, "ETH_USDT")
        );
    }
}
