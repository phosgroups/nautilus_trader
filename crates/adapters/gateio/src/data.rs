//! Gate.io market data clients.

use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{runner::try_get_data_event_sender, runtime::get_runtime},
    messages::{
        DataEvent,
        data::{
            BarsResponse, BookResponse, DataResponse, FundingRatesResponse, InstrumentResponse,
            InstrumentsResponse, RequestBars, RequestBookDeltas, RequestBookDepth,
            RequestBookSnapshot, RequestFundingRates, RequestInstrument, RequestInstruments,
            RequestQuotes, RequestTrades, SubscribeBars, SubscribeBookDeltas, SubscribeBookDepth10,
            SubscribeFundingRates, SubscribeIndexPrices, SubscribeMarkPrices, SubscribeQuotes,
            SubscribeTrades, UnsubscribeBars, UnsubscribeBookDeltas, UnsubscribeBookDepth10,
            UnsubscribeFundingRates, UnsubscribeIndexPrices, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    AtomicMap,
    datetime::datetime_to_unix_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::{
        BarSpecification, BarType, BookOrder, DEPTH10_LEN, Data, FundingRateUpdate,
        OrderBookDeltas, OrderBookDepth10,
    },
    enums::{
        AggregationSource, BarAggregation, BookAction, BookType, OrderSide, PriceType, RecordFlag,
    },
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    orderbook::OrderBook,
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        consts::{
            GATEIO_FUTURES_BOOK_TICKER_WS_CHANNEL, GATEIO_FUTURES_CANDLES_WS_CHANNEL,
            GATEIO_FUTURES_ORDER_BOOK_UPDATE_WS_CHANNEL, GATEIO_FUTURES_ORDER_BOOK_WS_CHANNEL,
            GATEIO_FUTURES_TICKER_WS_CHANNEL, GATEIO_FUTURES_TRADES_WS_CHANNEL,
            GATEIO_SPOT_BOOK_TICKER_WS_CHANNEL, GATEIO_SPOT_CANDLES_WS_CHANNEL,
            GATEIO_SPOT_ORDER_BOOK_UPDATE_WS_CHANNEL, GATEIO_SPOT_ORDER_BOOK_WS_CHANNEL,
            GATEIO_SPOT_TICKER_WS_CHANNEL, GATEIO_SPOT_TRADES_WS_CHANNEL, GATEIO_VENUE,
        },
        enums::GateioProductType,
        parse::{
            parse_book, parse_candle, parse_index_price, parse_mark_price, parse_quote,
            parse_trade, product_for_instrument,
        },
        symbol::raw_symbol,
    },
    config::GateioDataClientConfig,
    http::{
        client::GateioHttpClient,
        models::{GateioCandle, GateioOrderBook, GateioTicker, GateioTrade},
    },
    websocket::{GATEIO_INTERNAL_RECONNECTED_CHANNEL, GateioWebSocketClient, GateioWsMessage},
};

/// Live market data client for one Gate.io product family.
#[derive(Debug)]
pub struct GateioDataClient {
    client_id: ClientId,
    config: GateioDataClientConfig,
    http_client: GateioHttpClient,
    ws_client: GateioWebSocketClient,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    bar_types: Arc<AtomicMap<String, BarType>>,
    book_states: Arc<Mutex<HashMap<String, GateioBookState>>>,
    book_generation: Arc<AtomicU64>,
    ticker_subscriptions: Arc<Mutex<HashMap<String, usize>>>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    ws_task: Option<JoinHandle<()>>,
    cancellation_token: CancellationToken,
    clock: &'static AtomicTime,
    is_connected: AtomicBool,
}

/// Gate.io Spot data client type alias.
pub type GateioSpotDataClient = GateioDataClient;

/// Gate.io USDT perpetual data client type alias.
pub type GateioFuturesDataClient = GateioDataClient;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GateioDataChannel {
    Trades,
    Tickers,
    OrderBook,
    OrderBookUpdate,
    Candlesticks,
}

impl GateioDataChannel {
    fn parse(channel: &str) -> Option<Self> {
        match channel {
            GATEIO_SPOT_TRADES_WS_CHANNEL | GATEIO_FUTURES_TRADES_WS_CHANNEL => Some(Self::Trades),
            GATEIO_SPOT_TICKER_WS_CHANNEL
            | GATEIO_SPOT_BOOK_TICKER_WS_CHANNEL
            | GATEIO_FUTURES_TICKER_WS_CHANNEL
            | GATEIO_FUTURES_BOOK_TICKER_WS_CHANNEL => Some(Self::Tickers),
            GATEIO_SPOT_ORDER_BOOK_WS_CHANNEL | GATEIO_FUTURES_ORDER_BOOK_WS_CHANNEL => {
                Some(Self::OrderBook)
            }
            GATEIO_SPOT_ORDER_BOOK_UPDATE_WS_CHANNEL
            | GATEIO_FUTURES_ORDER_BOOK_UPDATE_WS_CHANNEL => Some(Self::OrderBookUpdate),
            GATEIO_SPOT_CANDLES_WS_CHANNEL | GATEIO_FUTURES_CANDLES_WS_CHANNEL => {
                Some(Self::Candlesticks)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GateioWsEvent {
    Update,
    All,
    Other,
}

impl GateioWsEvent {
    fn parse(event: &str) -> Self {
        match event {
            "update" => Self::Update,
            "all" => Self::All,
            _ => Self::Other,
        }
    }
}

impl GateioDataClient {
    /// Creates a Gate.io data client.
    pub fn new(client_id: ClientId, config: GateioDataClientConfig) -> anyhow::Result<Self> {
        let http_client = GateioHttpClient::new(&config)?;
        let data_sender = try_get_data_event_sender().unwrap_or_else(|| {
            log::warn!(
                "GateioDataClient created before the live runner initialized a data sender; events will be dropped"
            );
            let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
            sender
        });

        Ok(Self {
            client_id,
            ws_client: GateioWebSocketClient::new_public(&config),
            config,
            http_client,
            data_sender,
            instruments: Arc::new(AtomicMap::new()),
            bar_types: Arc::new(AtomicMap::new()),
            book_states: Arc::new(Mutex::new(HashMap::new())),
            book_generation: Arc::new(AtomicU64::new(0)),
            ticker_subscriptions: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(Vec::new())),
            ws_task: None,
            cancellation_token: CancellationToken::new(),
            clock: get_atomic_clock_realtime(),
            is_connected: AtomicBool::new(false),
        })
    }

    /// Returns the configured Gate.io product family.
    #[must_use]
    pub const fn product_type(&self) -> GateioProductType {
        self.config.product_type
    }

    fn ensure_product(&self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let actual = product_for_instrument(instrument_id);
        anyhow::ensure!(
            actual == self.config.product_type,
            "Gate.io data client is configured for {:?}, cannot use {}",
            self.config.product_type,
            instrument_id
        );
        Ok(())
    }

    fn abort_tasks(&mut self) {
        if let Ok(mut tasks) = self.tasks.lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
        } else {
            log::error!("Gate.io data task lock poisoned while stopping");
        }
        if let Some(task) = self.ws_task.take() {
            task.abort();
        }
    }

    fn cache_instrument(&self, instrument: InstrumentAny) {
        self.instruments.insert(instrument.id(), instrument);
    }

    fn queue<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        spawn_tracked(&self.tasks, future);
    }

    fn spawn_task<F>(&self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        spawn_tracked(&self.tasks, future);
    }

    fn queue_subscription(
        &mut self,
        channel: &'static str,
        instrument_id: InstrumentId,
        extra: Vec<String>,
    ) {
        let payload = std::iter::once(raw_symbol(instrument_id))
            .chain(extra)
            .collect();
        self.queue_subscription_payload(channel, payload);
    }

    fn queue_subscription_payload(&mut self, channel: &'static str, payload: Vec<String>) {
        let ws = self.ws_client.clone();
        self.queue(async move {
            if let Err(error) = ws.subscribe(channel, payload, false).await {
                log::warn!("Gate.io subscription failed for {channel}: {error}");
            }
        });
    }

    fn acquire_ticker_subscription(&self, instrument_id: InstrumentId) -> anyhow::Result<bool> {
        let raw = raw_symbol(instrument_id);
        let mut subscriptions = self
            .ticker_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("Gate.io ticker subscription lock poisoned"))?;
        let count = subscriptions.entry(raw).or_default();
        let first = *count == 0;
        *count += 1;
        Ok(first)
    }

    fn release_ticker_subscription(&self, instrument_id: InstrumentId) -> anyhow::Result<bool> {
        let raw = raw_symbol(instrument_id);
        let mut subscriptions = self
            .ticker_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("Gate.io ticker subscription lock poisoned"))?;
        let Some(count) = subscriptions.get_mut(&raw) else {
            return Ok(false);
        };
        if *count <= 1 {
            subscriptions.remove(&raw);
            Ok(true)
        } else {
            *count -= 1;
            Ok(false)
        }
    }

    fn start_ws_dispatch(&mut self) -> anyhow::Result<()> {
        if self.ws_task.is_some() {
            return Ok(());
        }
        let mut receiver = self.ws_client.take_event_receiver();
        let sender = self.data_sender.clone();
        let instruments = Arc::clone(&self.instruments);
        let bar_types = Arc::clone(&self.bar_types);
        let book_states = Arc::clone(&self.book_states);
        let book_generation = Arc::clone(&self.book_generation);
        let http_client = self.http_client.clone();
        let tasks = Arc::clone(&self.tasks);
        let product_type = self.config.product_type;
        let clock = self.clock;

        self.ws_task = Some(get_runtime().spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(message) => dispatch_ws_message(
                        message,
                        &sender,
                        &instruments,
                        &bar_types,
                        &book_states,
                        &book_generation,
                        &http_client,
                        &tasks,
                        product_type,
                        clock,
                    ),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        log::warn!(
                            "Gate.io market-data WebSocket receiver lagged by {count} messages"
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }));
        Ok(())
    }

    fn send(&self, event: DataEvent) {
        if let Err(error) = self.data_sender.send(event) {
            log::warn!("Failed to publish Gate.io data event: {error}");
        }
    }

    fn book_channel(&self) -> &'static str {
        match self.config.product_type {
            GateioProductType::Spot => GATEIO_SPOT_ORDER_BOOK_UPDATE_WS_CHANNEL,
            GateioProductType::UsdtPerpetual => GATEIO_FUTURES_ORDER_BOOK_UPDATE_WS_CHANNEL,
        }
    }

    fn subscribe_book(&mut self, instrument_id: InstrumentId, depth10: bool) -> anyhow::Result<()> {
        self.ensure_product(instrument_id)?;
        let raw = raw_symbol(instrument_id);
        let product_type = self.config.product_type;
        let channel = self.book_channel();
        let payload = order_book_update_subscription_payload(product_type, instrument_id);

        let (generation, should_start) = {
            let mut states = self
                .book_states
                .lock()
                .map_err(|_| anyhow::anyhow!("Gate.io order-book state lock poisoned"))?;
            let state = states.entry(raw.clone()).or_default();
            let generation = if state.has_subscribers() {
                state.generation
            } else {
                next_book_generation(&self.book_generation)
            };
            let was_empty = state.add_subscriber(depth10, generation);
            (state.generation, was_empty)
        };

        if !should_start {
            return Ok(());
        }

        let ws = self.ws_client.clone();
        let sender = self.data_sender.clone();
        let instruments = Arc::clone(&self.instruments);
        let book_states = Arc::clone(&self.book_states);
        let http_client = self.http_client.clone();
        let clock = self.clock;
        self.queue(async move {
            if let Err(error) = ws.subscribe(channel, payload, false).await {
                log::warn!("Gate.io order-book update subscription failed: {error}");
                if let Ok(mut states) = book_states.lock() {
                    let should_remove = states.get_mut(&raw).is_some_and(|state| {
                        if state.generation == generation {
                            state.mark_sync_failed();
                            true
                        } else {
                            false
                        }
                    });
                    if should_remove {
                        states.remove(&raw);
                    }
                }
                return;
            }
            synchronize_book(
                &raw,
                generation,
                product_type,
                &sender,
                &instruments,
                &book_states,
                &http_client,
                clock,
            )
            .await;
        });
        Ok(())
    }

    fn unsubscribe_book(
        &mut self,
        instrument_id: InstrumentId,
        depth10: bool,
    ) -> anyhow::Result<()> {
        self.ensure_product(instrument_id)?;
        let raw = raw_symbol(instrument_id);
        let should_stop = {
            let mut states = self
                .book_states
                .lock()
                .map_err(|_| anyhow::anyhow!("Gate.io order-book state lock poisoned"))?;
            let Some(state) = states.get_mut(&raw) else {
                return Ok(());
            };
            if state.remove_subscriber(depth10) {
                states.remove(&raw);
                true
            } else {
                false
            }
        };

        if should_stop {
            let ws = self.ws_client.clone();
            let payload =
                order_book_update_subscription_payload(self.config.product_type, instrument_id);
            let channel = self.book_channel();
            self.queue(async move {
                if let Err(error) = ws.unsubscribe(channel, payload, false).await {
                    log::debug!("Gate.io order-book update unsubscription failed: {error}");
                }
            });
        }
        Ok(())
    }
}

fn instrument_for_raw(
    instruments: &AtomicMap<InstrumentId, InstrumentAny>,
    raw: &str,
) -> Option<InstrumentAny> {
    instruments
        .load()
        .values()
        .find(|instrument| instrument.raw_symbol().as_str() == raw)
        .cloned()
}

fn raw_from_value(value: &serde_json::Value, product_type: GateioProductType) -> Option<String> {
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
        .or_else(|| {
            value
                .get("n")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.split_once('_'))
                .map(|(_, symbol)| symbol.to_string())
        })
}

fn candle_metadata(
    value: &serde_json::Value,
    product_type: GateioProductType,
) -> Option<(String, String)> {
    let name = value.get("n").and_then(serde_json::Value::as_str);
    if let Some((interval, raw_symbol)) = name.and_then(|value| value.split_once('_')) {
        return Some((interval.to_string(), raw_symbol.to_string()));
    }
    let interval = name?.to_string();
    let raw_symbol = raw_from_value(value, product_type)?;
    Some((interval, raw_symbol))
}

fn bar_key(raw_symbol: &str, interval: &str) -> String {
    format!("{raw_symbol}:{interval}")
}

fn bar_spec_from_interval(interval: &str) -> Option<BarSpecification> {
    let (step, suffix) = interval.split_at(interval.len().saturating_sub(1));
    let step = step.parse::<usize>().ok()?;
    let aggregation = match suffix {
        "m" => BarAggregation::Minute,
        "h" => BarAggregation::Hour,
        "d" => BarAggregation::Day,
        _ => return None,
    };
    Some(BarSpecification::new(step, aggregation, PriceType::Last))
}

fn candle_subscription_payload(
    product_type: GateioProductType,
    raw_symbol: String,
    interval: String,
) -> Vec<String> {
    match product_type {
        // Gate.io Spot and Futures both use [interval, symbol].
        GateioProductType::Spot => vec![interval, raw_symbol],
        GateioProductType::UsdtPerpetual => vec![interval, raw_symbol],
    }
}

fn order_book_update_subscription_payload(
    product_type: GateioProductType,
    instrument_id: InstrumentId,
) -> Vec<String> {
    let symbol = raw_symbol(instrument_id);
    match product_type {
        // Spot v4 uses [currency_pair, frequency].
        GateioProductType::Spot => vec![symbol, "100ms".to_string()],
        // Futures v4 uses [contract, frequency, depth].
        GateioProductType::UsdtPerpetual => {
            vec![symbol, "100ms".to_string(), "100".to_string()]
        }
    }
}

const MAX_BOOK_BUFFERED_UPDATES: usize = 4096;

fn next_book_generation(counter: &AtomicU64) -> u64 {
    let previous = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(if current == u64::MAX { 1 } else { current + 1 })
        })
        .unwrap_or(0);
    if previous == u64::MAX {
        1
    } else {
        previous + 1
    }
}

#[derive(Debug, Default)]
struct GateioBookState {
    initialized: bool,
    syncing: bool,
    generation: u64,
    last_sequence: u64,
    buffered: Vec<GateioOrderBook>,
    delta_subscribers: usize,
    depth10_subscribers: usize,
    bids: BTreeMap<Decimal, (String, String)>,
    asks: BTreeMap<Decimal, (String, String)>,
}

impl GateioBookState {
    fn begin_sync(&mut self, generation: u64) {
        self.initialized = false;
        self.syncing = true;
        self.generation = generation;
        self.last_sequence = 0;
        self.buffered.clear();
        self.clear_levels();
    }

    fn mark_sync_failed(&mut self) {
        self.initialized = false;
        self.syncing = false;
        self.last_sequence = 0;
        self.buffered.clear();
        self.clear_levels();
    }

    fn wants_deltas(&self) -> bool {
        self.delta_subscribers > 0
    }

    fn wants_depth10(&self) -> bool {
        self.depth10_subscribers > 0
    }

    fn has_subscribers(&self) -> bool {
        self.wants_deltas() || self.wants_depth10()
    }

    fn add_subscriber(&mut self, depth10: bool, generation: u64) -> bool {
        let was_empty = !self.has_subscribers();
        if depth10 {
            self.depth10_subscribers = self.depth10_subscribers.saturating_add(1);
        } else {
            self.delta_subscribers = self.delta_subscribers.saturating_add(1);
        }
        if was_empty {
            self.begin_sync(generation);
        }
        was_empty
    }

    fn remove_subscriber(&mut self, depth10: bool) -> bool {
        let count = if depth10 {
            &mut self.depth10_subscribers
        } else {
            &mut self.delta_subscribers
        };
        if *count == 0 {
            return false;
        }
        *count -= 1;
        !self.has_subscribers()
    }

    fn clear_levels(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    fn apply_deltas(&mut self, deltas: &OrderBookDeltas) {
        for delta in &deltas.deltas {
            match delta.action {
                BookAction::Clear => self.clear_levels(),
                BookAction::Add | BookAction::Update | BookAction::Delete => {
                    let levels = match delta.order.side {
                        OrderSide::Buy => &mut self.bids,
                        OrderSide::Sell => &mut self.asks,
                        OrderSide::NoOrderSide => continue,
                    };
                    let price = delta.order.price.to_string();
                    let size = delta.order.size.to_string();
                    let Ok(price_key) = Decimal::from_str(&price) else {
                        continue;
                    };
                    if matches!(delta.action, BookAction::Delete) || delta.order.size.is_zero() {
                        levels.remove(&price_key);
                    } else {
                        levels.insert(price_key, (price, size));
                    }
                }
            }
        }
    }

    fn top_levels(&self) -> (Vec<(String, String)>, Vec<(String, String)>) {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(DEPTH10_LEN)
            .map(|(_, level)| level.clone())
            .collect();
        let asks = self
            .asks
            .iter()
            .take(DEPTH10_LEN)
            .map(|(_, level)| level.clone())
            .collect();
        (bids, asks)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BookUpdateRange {
    first: u64,
    last: u64,
}

#[derive(Debug, PartialEq, Eq)]
enum BookUpdateAction {
    Apply,
    Stale,
    Gap,
}

#[derive(Debug)]
struct BookReplayPlan {
    updates: Vec<GateioOrderBook>,
    last_sequence: u64,
}

#[derive(Debug)]
struct BookReplayGap {
    remaining: Vec<GateioOrderBook>,
}

fn book_update_range(book: &GateioOrderBook) -> Option<BookUpdateRange> {
    let first = book.first_sequence?;
    let last = book.sequence?;
    if first == 0 || last == 0 || first > last {
        return None;
    }
    Some(BookUpdateRange { first, last })
}

fn classify_book_update(last_sequence: u64, range: BookUpdateRange) -> BookUpdateAction {
    let next_sequence = last_sequence.saturating_add(1);
    if range.last < next_sequence {
        BookUpdateAction::Stale
    } else if range.first > next_sequence {
        BookUpdateAction::Gap
    } else {
        BookUpdateAction::Apply
    }
}

fn sort_book_updates(updates: &mut [GateioOrderBook]) {
    updates.sort_by_key(|update| {
        book_update_range(update).map_or((u64::MAX, u64::MAX), |range| (range.first, range.last))
    });
}

fn prepare_book_replay(
    snapshot_sequence: u64,
    mut buffered: Vec<GateioOrderBook>,
) -> Result<BookReplayPlan, BookReplayGap> {
    sort_book_updates(&mut buffered);
    let mut replay = Vec::new();
    let mut last_sequence = snapshot_sequence;

    for (index, update) in buffered.iter().cloned().enumerate() {
        let Some(range) = book_update_range(&update) else {
            return Err(BookReplayGap {
                remaining: buffered[index..].to_vec(),
            });
        };

        match classify_book_update(last_sequence, range) {
            BookUpdateAction::Stale => {}
            BookUpdateAction::Gap => {
                return Err(BookReplayGap {
                    remaining: buffered[index..].to_vec(),
                });
            }
            BookUpdateAction::Apply => {
                last_sequence = range.last;
                replay.push(update);
            }
        }
    }

    Ok(BookReplayPlan {
        updates: replay,
        last_sequence,
    })
}

fn parse_book_replay_updates(
    updates: &[GateioOrderBook],
    instrument: &InstrumentAny,
    ts_init: nautilus_core::UnixNanos,
) -> anyhow::Result<Vec<OrderBookDeltas>> {
    updates
        .iter()
        .map(|update| crate::common::parse::parse_book_update(update, instrument, ts_init))
        .collect()
}

fn send_book_deltas(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    deltas: nautilus_model::data::OrderBookDeltas,
) {
    let _ = sender.send(DataEvent::Data(Data::Deltas(
        nautilus_model::data::OrderBookDeltas_API::new(deltas),
    )));
}

fn build_depth10(
    state: &GateioBookState,
    instrument: &InstrumentAny,
    sequence: u64,
    ts_event: nautilus_core::UnixNanos,
    ts_init: nautilus_core::UnixNanos,
) -> anyhow::Result<OrderBookDepth10> {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    let empty_bid = BookOrder::new(
        OrderSide::Buy,
        Price::zero(price_precision),
        Quantity::zero(size_precision),
        0,
    );
    let empty_ask = BookOrder::new(
        OrderSide::Sell,
        Price::zero(price_precision),
        Quantity::zero(size_precision),
        0,
    );
    let mut bids = [empty_bid; DEPTH10_LEN];
    let mut asks = [empty_ask; DEPTH10_LEN];
    let mut bid_counts = [0_u32; DEPTH10_LEN];
    let mut ask_counts = [0_u32; DEPTH10_LEN];
    let (raw_bids, raw_asks) = state.top_levels();

    for (index, (price_raw, size_raw)) in raw_bids.iter().enumerate() {
        let price = Decimal::from_str(price_raw)
            .with_context(|| format!("invalid Gate.io depth10 bid price {price_raw:?}"))?;
        let size = Decimal::from_str(size_raw)
            .with_context(|| format!("invalid Gate.io depth10 bid size {size_raw:?}"))?;
        bids[index] = BookOrder::new(
            OrderSide::Buy,
            Price::from_decimal_dp(price, price_precision)
                .context("invalid Gate.io depth10 bid price precision")?,
            Quantity::from_decimal_dp(size, size_precision)
                .context("invalid Gate.io depth10 bid size precision")?,
            0,
        );
        bid_counts[index] = 1;
    }

    for (index, (price_raw, size_raw)) in raw_asks.iter().enumerate() {
        let price = Decimal::from_str(price_raw)
            .with_context(|| format!("invalid Gate.io depth10 ask price {price_raw:?}"))?;
        let size = Decimal::from_str(size_raw)
            .with_context(|| format!("invalid Gate.io depth10 ask size {size_raw:?}"))?;
        asks[index] = BookOrder::new(
            OrderSide::Sell,
            Price::from_decimal_dp(price, price_precision)
                .context("invalid Gate.io depth10 ask price precision")?,
            Quantity::from_decimal_dp(size, size_precision)
                .context("invalid Gate.io depth10 ask size precision")?,
            0,
        );
        ask_counts[index] = 1;
    }

    Ok(OrderBookDepth10::new(
        instrument.id(),
        bids,
        asks,
        bid_counts,
        ask_counts,
        RecordFlag::F_SNAPSHOT as u8,
        sequence,
        ts_event,
        ts_init,
    ))
}

fn send_depth10(sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>, depth10: OrderBookDepth10) {
    let _ = sender.send(DataEvent::Data(Data::Depth10(Box::new(depth10))));
}

fn spawn_tracked<F>(tasks: &Arc<Mutex<Vec<JoinHandle<()>>>>, future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let task = get_runtime().spawn(future);
    if let Ok(mut pending) = tasks.lock() {
        pending.retain(|task| !task.is_finished());
        pending.push(task);
    } else {
        log::error!("Gate.io data task lock poisoned; aborting task");
        task.abort();
    }
}

fn schedule_book_sync(
    raw: String,
    generation: u64,
    product_type: GateioProductType,
    sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    book_states: Arc<Mutex<HashMap<String, GateioBookState>>>,
    http_client: GateioHttpClient,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    clock: &'static AtomicTime,
) {
    spawn_tracked(&tasks, async move {
        synchronize_book(
            &raw,
            generation,
            product_type,
            &sender,
            &instruments,
            book_states.as_ref(),
            &http_client,
            clock,
        )
        .await;
    });
}

async fn synchronize_book(
    raw: &str,
    generation: u64,
    product_type: GateioProductType,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: &AtomicMap<InstrumentId, InstrumentAny>,
    book_states: &Mutex<HashMap<String, GateioBookState>>,
    http_client: &GateioHttpClient,
    clock: &'static AtomicTime,
) {
    let Some(instrument) = instrument_for_raw(instruments, raw) else {
        log::warn!("Gate.io order-book sync has no cached instrument for {raw}");
        if let Ok(mut states) = book_states.lock()
            && let Some(state) = states.get_mut(raw)
        {
            state.syncing = false;
        }
        return;
    };

    for attempt in 0..5 {
        if let Ok(states) = book_states.lock()
            && states
                .get(raw)
                .is_none_or(|state| state.generation != generation)
        {
            return;
        }
        let (snapshot_deltas, snapshot_sequence) = match http_client
            .order_book_with_sequence(&instrument, product_type, Some(100), clock.get_time_ns())
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                log::warn!(
                    "Gate.io order-book snapshot failed for {raw} (attempt {}): {error}",
                    attempt + 1
                );
                continue;
            }
        };
        if snapshot_sequence == 0 {
            log::warn!("Gate.io order-book snapshot for {raw} did not include an update id");
            continue;
        }

        let Ok(mut states) = book_states.lock() else {
            log::warn!("Gate.io order-book state lock poisoned for {raw}");
            return;
        };
        let Some(state) = states.get_mut(raw) else {
            return;
        };
        if state.generation != generation {
            return;
        }
        let buffered = std::mem::take(&mut state.buffered);
        let replay = match prepare_book_replay(snapshot_sequence, buffered) {
            Ok(replay) => replay,
            Err(gap) => {
                state.initialized = false;
                state.buffered = if gap
                    .remaining
                    .iter()
                    .any(|update| book_update_range(update).is_none())
                {
                    Vec::new()
                } else {
                    gap.remaining
                };
                continue;
            }
        };

        let replayed_deltas =
            match parse_book_replay_updates(&replay.updates, &instrument, clock.get_time_ns()) {
                Ok(deltas) => deltas,
                Err(error) => {
                    log::warn!("Failed to replay Gate.io order-book update for {raw}: {error}");
                    state.initialized = false;
                    state.buffered.clear();
                    continue;
                }
            };

        let (emit_deltas, depth10) = {
            let Some(state) = states.get_mut(raw) else {
                return;
            };
            if state.generation != generation {
                return;
            }
            state.clear_levels();
            state.apply_deltas(&snapshot_deltas);
            for deltas in &replayed_deltas {
                state.apply_deltas(deltas);
            }
            state.initialized = true;
            state.syncing = false;
            state.last_sequence = replay.last_sequence;
            let depth10 = if state.wants_depth10() {
                Some(build_depth10(
                    state,
                    &instrument,
                    replay.last_sequence,
                    snapshot_deltas.ts_event,
                    snapshot_deltas.ts_init,
                ))
            } else {
                None
            };
            (state.wants_deltas(), depth10)
        };
        drop(states);

        if emit_deltas {
            send_book_deltas(sender, snapshot_deltas);
            for deltas in replayed_deltas {
                send_book_deltas(sender, deltas);
            }
        }
        if let Some(depth10) = depth10 {
            match depth10 {
                Ok(depth10) => send_depth10(sender, depth10),
                Err(error) => {
                    log::warn!("Failed to build Gate.io depth10 snapshot for {raw}: {error}")
                }
            }
        }
        return;
    }

    log::error!("Gate.io order-book sync could not establish a contiguous stream for {raw}");
    if let Ok(mut states) = book_states.lock()
        && let Some(state) = states.get_mut(raw)
    {
        state.mark_sync_failed();
    }
}

fn schedule_reconnect_book_syncs(
    product_type: GateioProductType,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: &Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    book_states: &Arc<Mutex<HashMap<String, GateioBookState>>>,
    book_generation: &Arc<AtomicU64>,
    http_client: &GateioHttpClient,
    tasks: &Arc<Mutex<Vec<JoinHandle<()>>>>,
    clock: &'static AtomicTime,
) {
    let Ok(mut states) = book_states.lock() else {
        log::warn!("Gate.io order-book state lock poisoned during WebSocket recovery");
        return;
    };
    let generations = states
        .iter_mut()
        .map(|(raw, state)| {
            state.begin_sync(next_book_generation(book_generation));
            (raw.clone(), state.generation)
        })
        .collect::<Vec<_>>();
    drop(states);

    for (raw, generation) in generations {
        schedule_book_sync(
            raw,
            generation,
            product_type,
            sender.clone(),
            Arc::clone(instruments),
            Arc::clone(book_states),
            http_client.clone(),
            Arc::clone(tasks),
            clock,
        );
    }
}

fn timestamp_from_ws(
    message: &GateioWsMessage,
    clock: &'static AtomicTime,
) -> nautilus_core::UnixNanos {
    message
        .time_ms
        .filter(|value| *value > 0)
        .and_then(|millis| crate::common::parse::timestamp_nanos(millis).ok())
        .or_else(|| {
            message
                .time
                .filter(|value| *value > 0)
                .and_then(|seconds| seconds.checked_mul(1_000))
                .and_then(|millis| crate::common::parse::timestamp_nanos(millis).ok())
        })
        .unwrap_or_else(|| clock.get_time_ns())
}

fn dispatch_ws_message(
    message: GateioWsMessage,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: &Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    bar_types: &AtomicMap<String, BarType>,
    book_states: &Arc<Mutex<HashMap<String, GateioBookState>>>,
    book_generation: &Arc<AtomicU64>,
    http_client: &GateioHttpClient,
    tasks: &Arc<Mutex<Vec<JoinHandle<()>>>>,
    product_type: GateioProductType,
    clock: &'static AtomicTime,
) {
    if message.channel == GATEIO_INTERNAL_RECONNECTED_CHANNEL {
        log::info!("Gate.io market-data WebSocket subscriptions restored; resyncing order books");
        schedule_reconnect_book_syncs(
            product_type,
            sender,
            instruments,
            book_states,
            book_generation,
            http_client,
            tasks,
            clock,
        );
        return;
    }

    let Some(channel) = GateioDataChannel::parse(&message.channel) else {
        return;
    };
    let event = GateioWsEvent::parse(&message.event);
    let ts_init = clock.get_time_ns();
    let ts_event = timestamp_from_ws(&message, clock);

    match channel {
        GateioDataChannel::Trades => {
            if event != GateioWsEvent::Update {
                return;
            }
            let rows = match message.result.as_array() {
                Some(rows) => rows.as_slice(),
                None if message.result.is_object() => std::slice::from_ref(&message.result),
                None => return,
            };
            for row in rows {
                let Ok(trade) = serde_json::from_value::<GateioTrade>(row.clone()) else {
                    continue;
                };
                let Some(raw) = raw_from_value(row, product_type) else {
                    continue;
                };
                let Some(instrument) = instrument_for_raw(instruments, &raw) else {
                    continue;
                };
                match parse_trade(&trade, &instrument, ts_init) {
                    Ok(trade) => {
                        let _ = sender.send(DataEvent::Data(Data::Trade(trade)));
                    }
                    Err(error) => log::debug!("Failed to parse Gate.io trade update: {error}"),
                }
            }
        }
        GateioDataChannel::Tickers => {
            if event != GateioWsEvent::Update {
                return;
            }
            let rows = message
                .result
                .as_array()
                .map_or_else(|| vec![&message.result], |rows| rows.iter().collect());
            for value in rows {
                let Ok(ticker) = serde_json::from_value::<GateioTicker>(value.clone()) else {
                    continue;
                };
                let raw = raw_from_value(value, product_type)
                    .or_else(|| ticker.currency_pair.clone())
                    .or_else(|| ticker.contract.clone());
                let Some(instrument) = raw
                    .as_deref()
                    .and_then(|value| instrument_for_raw(instruments, value))
                else {
                    continue;
                };

                if let (Some(bid), Some(ask)) =
                    (ticker.highest_bid.as_deref(), ticker.lowest_ask.as_deref())
                {
                    match parse_quote(
                        bid,
                        ask,
                        ticker.highest_size.as_deref().unwrap_or("0"),
                        ticker.lowest_size.as_deref().unwrap_or("0"),
                        &instrument,
                        ts_event,
                        ts_init,
                    ) {
                        Ok(quote) => {
                            let _ = sender.send(DataEvent::Data(Data::Quote(quote)));
                        }
                        Err(error) => {
                            log::debug!("Failed to parse Gate.io quote update: {error}")
                        }
                    }
                }

                if product_type.is_derivative() {
                    if let Some(mark_price) = ticker.mark_price.as_deref()
                        && let Ok(update) =
                            parse_mark_price(mark_price, &instrument, ts_event, ts_init)
                    {
                        let _ = sender.send(DataEvent::Data(Data::MarkPriceUpdate(update)));
                    }
                    if let Some(index_price) = ticker.index_price.as_deref()
                        && let Ok(update) =
                            parse_index_price(index_price, &instrument, ts_event, ts_init)
                    {
                        let _ = sender.send(DataEvent::Data(Data::IndexPriceUpdate(update)));
                    }
                    if let Some(funding_rate) = ticker.funding_rate.as_deref()
                        && let Ok(rate) = rust_decimal::Decimal::from_str(funding_rate)
                    {
                        let next_funding_ns = ticker
                            .funding_next_apply
                            .map(crate::common::parse::timestamp_nanos)
                            .transpose()
                            .ok()
                            .flatten();
                        let update = FundingRateUpdate::new(
                            instrument.id(),
                            rate,
                            None,
                            next_funding_ns,
                            ts_event,
                            ts_init,
                        );
                        let _ = sender.send(DataEvent::FundingRate(update));
                    }
                }
            }
        }
        GateioDataChannel::OrderBook => {
            if !matches!(event, GateioWsEvent::Update | GateioWsEvent::All) {
                return;
            }
            let value = message.result.clone();
            let Ok(book) = serde_json::from_value::<GateioOrderBook>(value.clone()) else {
                return;
            };
            let Some(raw) = raw_from_value(&value, product_type) else {
                return;
            };
            let Some(instrument) = instrument_for_raw(instruments, &raw) else {
                return;
            };
            match parse_book(&book, &instrument, ts_init) {
                Ok(deltas) => {
                    let _ = sender.send(DataEvent::Data(Data::Deltas(
                        nautilus_model::data::OrderBookDeltas_API::new(deltas),
                    )));
                }
                Err(error) => log::debug!("Failed to parse Gate.io full order book: {error}"),
            }
        }
        GateioDataChannel::OrderBookUpdate => {
            if event != GateioWsEvent::Update {
                return;
            }
            let value = message.result.clone();
            let Ok(book) = serde_json::from_value::<GateioOrderBook>(value.clone()) else {
                return;
            };
            let Some(raw) = raw_from_value(&value, product_type) else {
                return;
            };
            let Some(instrument) = instrument_for_raw(instruments, &raw) else {
                return;
            };

            let range = book_update_range(&book);
            let mut resync_generation = None;
            let mut publication = None;
            if let Ok(mut states) = book_states.lock() {
                let Some(state) = states.get_mut(&raw) else {
                    return;
                };
                if !state.initialized || state.syncing {
                    if range.is_none() {
                        let generation = next_book_generation(book_generation);
                        state.begin_sync(generation);
                        resync_generation = Some(generation);
                    } else {
                        if !state.syncing {
                            let generation = next_book_generation(book_generation);
                            state.begin_sync(generation);
                            resync_generation = Some(generation);
                        }
                        if state.buffered.len() >= MAX_BOOK_BUFFERED_UPDATES {
                            state.buffered.clear();
                            log::warn!(
                                "Gate.io order-book buffer overflow for {raw}; waiting for a fresh snapshot"
                            );
                        }
                        state.buffered.push(book);
                    }
                } else {
                    match range {
                        None => {
                            let generation = next_book_generation(book_generation);
                            state.begin_sync(generation);
                            resync_generation = Some(generation);
                        }
                        Some(range) => match classify_book_update(state.last_sequence, range) {
                            BookUpdateAction::Stale => return,
                            BookUpdateAction::Gap => {
                                let generation = next_book_generation(book_generation);
                                state.begin_sync(generation);
                                if state.buffered.len() >= MAX_BOOK_BUFFERED_UPDATES {
                                    state.buffered.clear();
                                }
                                state.buffered.push(book.clone());
                                resync_generation = Some(generation);
                            }
                            BookUpdateAction::Apply => {
                                match crate::common::parse::parse_book_update(
                                    &book,
                                    &instrument,
                                    ts_init,
                                ) {
                                    Ok(parsed_deltas) => {
                                        state.apply_deltas(&parsed_deltas);
                                        state.last_sequence = range.last;
                                        let depth10 = if state.wants_depth10() {
                                            Some(build_depth10(
                                                state,
                                                &instrument,
                                                range.last,
                                                parsed_deltas.ts_event,
                                                parsed_deltas.ts_init,
                                            ))
                                        } else {
                                            None
                                        };
                                        publication =
                                            Some((parsed_deltas, state.wants_deltas(), depth10));
                                    }
                                    Err(error) => {
                                        log::debug!(
                                            "Failed to parse Gate.io order-book update: {error}"
                                        );
                                        let generation = next_book_generation(book_generation);
                                        state.begin_sync(generation);
                                        resync_generation = Some(generation);
                                    }
                                }
                            }
                        },
                    }
                };
            } else {
                log::warn!("Gate.io order-book state lock poisoned for {raw}");
                return;
            }

            if let Some(generation) = resync_generation {
                schedule_book_sync(
                    raw,
                    generation,
                    product_type,
                    sender.clone(),
                    Arc::clone(instruments),
                    Arc::clone(book_states),
                    http_client.clone(),
                    Arc::clone(tasks),
                    clock,
                );
            } else if let Some((parsed_deltas, emit_deltas, depth10)) = publication {
                if emit_deltas {
                    send_book_deltas(sender, parsed_deltas);
                }
                if let Some(depth10) = depth10 {
                    match depth10 {
                        Ok(depth10) => send_depth10(sender, depth10),
                        Err(error) => {
                            log::debug!("Failed to build Gate.io depth10 update: {error}")
                        }
                    }
                }
            }
        }
        GateioDataChannel::Candlesticks => {
            if event != GateioWsEvent::Update {
                return;
            }
            let rows = message
                .result
                .as_array()
                .map_or_else(|| vec![&message.result], |rows| rows.iter().collect());
            for value in rows {
                let Ok(candle) = serde_json::from_value::<GateioCandle>(value.clone()) else {
                    continue;
                };
                let Some((interval, raw)) = candle_metadata(value, product_type) else {
                    continue;
                };
                let Some(instrument) = instrument_for_raw(instruments, &raw) else {
                    continue;
                };
                let Some(bar_type) =
                    bar_types.get_cloned(&bar_key(&raw, &interval)).or_else(|| {
                        bar_spec_from_interval(&interval).map(|spec| {
                            BarType::new(instrument.id(), spec, AggregationSource::External)
                        })
                    })
                else {
                    continue;
                };
                match parse_candle(&candle, &instrument, bar_type, ts_init) {
                    Ok(bar) => {
                        let _ = sender.send(DataEvent::Data(Data::Bar(bar)));
                    }
                    Err(error) => log::debug!("Failed to parse Gate.io candle update: {error}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::parse::parse_spot_instrument;
    use crate::http::models::{GateioLevel, GateioSpotPair};

    fn spot_instrument() -> InstrumentAny {
        let definition: GateioSpotPair = serde_json::from_value(serde_json::json!({
            "id": "BTC_USDT",
            "base": "BTC",
            "quote": "USDT",
            "precision": 2,
            "amount_precision": 3,
            "amount_point": "0.001"
        }))
        .unwrap();
        parse_spot_instrument(
            &definition,
            nautilus_core::UnixNanos::from(1_000_000_000),
            nautilus_core::UnixNanos::from(1_000_000_000),
        )
        .unwrap()
    }

    fn book_update(first: Option<u64>, last: Option<u64>) -> GateioOrderBook {
        GateioOrderBook {
            update: Some(1_700_000_000_000),
            first_sequence: first,
            sequence: last,
            ..Default::default()
        }
    }

    #[test]
    fn parses_supported_market_data_channels() {
        assert_eq!(
            GateioDataChannel::parse(GATEIO_SPOT_TRADES_WS_CHANNEL),
            Some(GateioDataChannel::Trades)
        );
        assert_eq!(
            GateioDataChannel::parse(GATEIO_FUTURES_TICKER_WS_CHANNEL),
            Some(GateioDataChannel::Tickers)
        );
        assert_eq!(
            GateioDataChannel::parse(GATEIO_FUTURES_BOOK_TICKER_WS_CHANNEL),
            Some(GateioDataChannel::Tickers)
        );
        assert_eq!(
            GateioDataChannel::parse(GATEIO_SPOT_ORDER_BOOK_WS_CHANNEL),
            Some(GateioDataChannel::OrderBook)
        );
        assert_eq!(
            GateioDataChannel::parse(GATEIO_FUTURES_ORDER_BOOK_WS_CHANNEL),
            Some(GateioDataChannel::OrderBook)
        );
        assert_eq!(
            GateioDataChannel::parse(GATEIO_FUTURES_CANDLES_WS_CHANNEL),
            Some(GateioDataChannel::Candlesticks)
        );
    }

    #[test]
    fn ignores_unknown_market_data_channels() {
        assert_eq!(GateioDataChannel::parse("spot.unknown"), None);
    }

    #[test]
    fn parses_supported_websocket_events() {
        assert_eq!(GateioWsEvent::parse("update"), GateioWsEvent::Update);
        assert_eq!(GateioWsEvent::parse("all"), GateioWsEvent::All);
        assert_eq!(GateioWsEvent::parse("subscribe"), GateioWsEvent::Other);
    }

    #[test]
    fn builds_order_book_update_subscription_payloads() {
        let spot = InstrumentId::from("BTC_USDT.GATEIO");
        let perpetual = InstrumentId::from("BTC_USDT-PERP.GATEIO");
        assert_eq!(
            order_book_update_subscription_payload(GateioProductType::Spot, spot),
            vec!["BTC_USDT", "100ms"]
        );
        assert_eq!(
            order_book_update_subscription_payload(GateioProductType::UsdtPerpetual, perpetual),
            vec!["BTC_USDT", "100ms", "100"]
        );
    }

    #[test]
    fn builds_product_specific_candle_subscription_payloads() {
        assert_eq!(
            candle_subscription_payload(
                GateioProductType::Spot,
                "BTC_USDT".to_string(),
                "1m".to_string(),
            ),
            vec!["1m", "BTC_USDT"]
        );
        assert_eq!(
            candle_subscription_payload(
                GateioProductType::UsdtPerpetual,
                "BTC_USDT".to_string(),
                "1m".to_string(),
            ),
            vec!["1m", "BTC_USDT"]
        );
    }

    #[test]
    fn extracts_only_gateio_order_book_update_range_fields() {
        let update = book_update(Some(101), Some(102));
        assert_eq!(
            book_update_range(&update),
            Some(BookUpdateRange {
                first: 101,
                last: 102
            })
        );

        assert_eq!(book_update_range(&book_update(None, Some(102))), None);
        assert_eq!(book_update_range(&book_update(Some(101), None)), None);
        assert_eq!(book_update_range(&book_update(Some(0), Some(102))), None);
        assert_eq!(book_update_range(&book_update(Some(103), Some(102))), None);
    }

    #[test]
    fn classifies_gateio_order_book_update_windows() {
        assert_eq!(
            classify_book_update(
                100,
                BookUpdateRange {
                    first: 101,
                    last: 101
                }
            ),
            BookUpdateAction::Apply
        );
        assert_eq!(
            classify_book_update(
                100,
                BookUpdateRange {
                    first: 99,
                    last: 101
                }
            ),
            BookUpdateAction::Apply
        );
        assert_eq!(
            classify_book_update(
                100,
                BookUpdateRange {
                    first: 99,
                    last: 100
                }
            ),
            BookUpdateAction::Stale
        );
        assert_eq!(
            classify_book_update(
                100,
                BookUpdateRange {
                    first: 102,
                    last: 103
                }
            ),
            BookUpdateAction::Gap
        );
    }

    #[test]
    fn replays_cached_order_book_updates_from_snapshot_boundary() {
        let plan = prepare_book_replay(
            100,
            vec![
                book_update(Some(102), Some(102)),
                book_update(Some(99), Some(100)),
                book_update(Some(99), Some(101)),
            ],
        )
        .unwrap();

        assert_eq!(plan.updates.len(), 2);
        assert_eq!(plan.updates[0].first_sequence, Some(99));
        assert_eq!(plan.updates[0].sequence, Some(101));
        assert_eq!(plan.updates[1].first_sequence, Some(102));
        assert_eq!(plan.updates[1].sequence, Some(102));
        assert_eq!(plan.last_sequence, 102);
    }

    #[test]
    fn rejects_cached_order_book_gap_after_snapshot() {
        let gap = prepare_book_replay(
            100,
            vec![
                book_update(Some(99), Some(100)),
                book_update(Some(102), Some(103)),
            ],
        )
        .unwrap_err();

        assert_eq!(gap.remaining.len(), 1);
        assert_eq!(gap.remaining[0].first_sequence, Some(102));
        assert_eq!(gap.remaining[0].sequence, Some(103));
    }

    #[test]
    fn rejects_cached_order_book_update_without_sequence() {
        let gap = prepare_book_replay(100, vec![book_update(None, None)]).unwrap_err();

        assert_eq!(gap.remaining.len(), 1);
        assert_eq!(gap.remaining[0].update, Some(1_700_000_000_000));
        assert_eq!(book_update_range(&gap.remaining[0]), None);
    }

    #[test]
    fn rejects_entire_book_replay_when_any_update_cannot_parse() {
        let instrument = spot_instrument();
        let valid = GateioOrderBook {
            current: Some(1_700_000_000_100),
            sequence: Some(101),
            first_sequence: Some(101),
            currency_pair: Some("BTC_USDT".to_string()),
            bids: vec![GateioLevel::Spot(["50000".to_string(), "1".to_string()])],
            ..Default::default()
        };
        let invalid = GateioOrderBook {
            current: Some(1_700_000_000_200),
            sequence: Some(102),
            first_sequence: Some(102),
            currency_pair: Some("BTC_USDT".to_string()),
            ..Default::default()
        };

        let error = parse_book_replay_updates(
            &[valid, invalid],
            &instrument,
            nautilus_core::UnixNanos::from(2_000_000_000),
        )
        .unwrap_err();

        assert!(error.to_string().contains("contained no levels"));
    }

    #[test]
    fn allocates_nonzero_monotonic_book_generations() {
        let counter = AtomicU64::new(0);
        assert_eq!(next_book_generation(&counter), 1);
        assert_eq!(next_book_generation(&counter), 2);

        counter.store(u64::MAX, Ordering::Relaxed);
        assert_eq!(next_book_generation(&counter), 1);
    }

    #[test]
    fn first_book_subscriber_starts_a_new_sync_generation() {
        let mut state = GateioBookState::default();
        assert!(state.add_subscriber(false, 11));
        assert_eq!(state.generation, 11);
        assert!(state.syncing);
        assert!(!state.initialized);

        state.initialized = true;
        state.syncing = false;
        state.last_sequence = 100;
        assert!(state.remove_subscriber(false));
        assert!(state.add_subscriber(true, 12));
        assert_eq!(state.generation, 12);
        assert!(state.syncing);
        assert!(!state.initialized);
        assert_eq!(state.last_sequence, 0);
    }

    #[test]
    fn failed_book_sync_clears_stale_state() {
        let mut state = GateioBookState {
            initialized: true,
            syncing: true,
            generation: 7,
            last_sequence: 100,
            buffered: vec![book_update(Some(101), Some(101))],
            delta_subscribers: 1,
            ..Default::default()
        };

        state.mark_sync_failed();

        assert!(!state.initialized);
        assert!(!state.syncing);
        assert_eq!(state.last_sequence, 0);
        assert!(state.buffered.is_empty());
        assert!(state.has_subscribers());
    }

    #[test]
    fn keeps_depth10_and_delta_subscriptions_independent() {
        let mut state = GateioBookState::default();
        assert!(state.add_subscriber(false, 1));
        assert_eq!(state.delta_subscribers, 1);
        assert_eq!(state.depth10_subscribers, 0);
        assert!(!state.add_subscriber(true, 1));
        assert_eq!(state.delta_subscribers, 1);
        assert_eq!(state.depth10_subscribers, 1);

        assert!(!state.remove_subscriber(false));
        assert_eq!(state.depth10_subscribers, 1);
        assert!(state.remove_subscriber(true));
        assert!(!state.has_subscribers());
        assert!(!state.remove_subscriber(true));
    }

    #[test]
    fn applies_snapshot_updates_and_deletes_before_building_depth10() {
        let instrument = spot_instrument();
        let snapshot = GateioOrderBook {
            id: Some(100),
            current: Some(1_700_000_000_000),
            currency_pair: Some("BTC_USDT".to_string()),
            bids: vec![
                GateioLevel::Spot(["50000".to_string(), "2".to_string()]),
                GateioLevel::Spot(["49999".to_string(), "3".to_string()]),
            ],
            asks: vec![GateioLevel::Spot(["50001".to_string(), "1".to_string()])],
            ..Default::default()
        };
        let mut state = GateioBookState::default();
        state.apply_deltas(
            &crate::common::parse::parse_book(
                &snapshot,
                &instrument,
                nautilus_core::UnixNanos::from(2_000_000_000),
            )
            .unwrap(),
        );

        let update = GateioOrderBook {
            current: Some(1_700_000_000_100),
            sequence: Some(101),
            first_sequence: Some(101),
            currency_pair: Some("BTC_USDT".to_string()),
            bids: vec![
                GateioLevel::Spot(["50000".to_string(), "4".to_string()]),
                GateioLevel::Spot(["49999".to_string(), "0".to_string()]),
            ],
            ..Default::default()
        };
        let deltas = crate::common::parse::parse_book_update(
            &update,
            &instrument,
            nautilus_core::UnixNanos::from(2_000_000_000),
        )
        .unwrap();
        state.apply_deltas(&deltas);
        let (bids, asks) = state.top_levels();
        assert_eq!(bids, vec![("50000.00".to_string(), "4.000".to_string())]);
        assert_eq!(asks, vec![("50001.00".to_string(), "1.000".to_string())]);

        state.depth10_subscribers = 1;
        let depth10 =
            build_depth10(&state, &instrument, 101, deltas.ts_event, deltas.ts_init).unwrap();
        assert_eq!(depth10.sequence, 101);
        assert_eq!(depth10.bids[0].price, Price::from("50000"));
        assert_eq!(depth10.bids[0].size, Quantity::from("4"));
        assert_eq!(depth10.asks[0].price, Price::from("50001"));
        assert_eq!(depth10.asks[0].size, Quantity::from("1"));
        assert_eq!(depth10.bid_counts[0], 1);
        assert_eq!(depth10.ask_counts[0], 1);
    }

    #[test]
    fn does_not_use_timestamp_as_book_sequence() {
        let update = GateioOrderBook {
            update: Some(1_700_000_000_000),
            current: Some(1_700_000_000_000),
            first_sequence: None,
            sequence: None,
            ..Default::default()
        };
        assert_eq!(book_update_range(&update), None);
        assert_eq!(
            classify_book_update(
                1_700_000_000_000,
                BookUpdateRange {
                    first: 1_700_000_000_001,
                    last: 1_700_000_000_001,
                },
            ),
            BookUpdateAction::Apply
        );
    }
}

#[async_trait(?Send)]
impl DataClient for GateioDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*GATEIO_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.http_client.cancel_all_requests();
        self.ws_client.stop();
        self.abort_tasks();
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.instruments.store(Default::default());
        self.bar_types.store(Default::default());
        self.book_states
            .lock()
            .map_err(|_| anyhow::anyhow!("Gate.io order-book state lock poisoned"))?
            .clear();
        self.ticker_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("Gate.io ticker subscription lock poisoned"))?
            .clear();
        self.cancellation_token = CancellationToken::new();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
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
            .context("failed to load Gate.io instruments")?;
        for instrument in instruments {
            self.cache_instrument(instrument.clone());
            self.send(DataEvent::Instrument(instrument));
        }

        self.ws_client
            .connect()
            .await
            .context("failed to connect Gate.io public WebSocket")?;
        self.start_ws_dispatch()?;
        self.is_connected.store(true, Ordering::Release);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.http_client.cancel_all_requests();
        self.ws_client.disconnect().await?;
        self.abort_tasks();
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let channel = match self.config.product_type {
            GateioProductType::Spot => GATEIO_SPOT_BOOK_TICKER_WS_CHANNEL,
            GateioProductType::UsdtPerpetual => GATEIO_FUTURES_BOOK_TICKER_WS_CHANNEL,
        };
        self.queue_subscription(channel, cmd.instrument_id, vec![]);
        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let channel = if self.config.product_type == GateioProductType::Spot {
            GATEIO_SPOT_TRADES_WS_CHANNEL
        } else {
            GATEIO_FUTURES_TRADES_WS_CHANNEL
        };
        self.queue_subscription(channel, cmd.instrument_id, vec![]);
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.book_type == BookType::L2_MBP,
            "Gate.io supports L2_MBP order book data only"
        );
        self.subscribe_book(cmd.instrument_id, false)
    }

    fn subscribe_book_depth10(&mut self, cmd: SubscribeBookDepth10) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.book_type == BookType::L2_MBP,
            "Gate.io supports L2_MBP order book data only"
        );
        self.subscribe_book(cmd.instrument_id, true)
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> anyhow::Result<()> {
        self.ensure_product(cmd.bar_type.instrument_id())?;
        let spec = cmd.bar_type.spec();
        let interval = match spec.aggregation {
            BarAggregation::Minute => format!("{}m", spec.step),
            BarAggregation::Hour => format!("{}h", spec.step),
            BarAggregation::Day => format!("{}d", spec.step),
            _ => anyhow::bail!("Gate.io supports minute, hour, and day bars"),
        };
        let channel = if self.config.product_type == GateioProductType::Spot {
            GATEIO_SPOT_CANDLES_WS_CHANNEL
        } else {
            crate::common::consts::GATEIO_FUTURES_CANDLES_WS_CHANNEL
        };
        let instrument_id = cmd.bar_type.instrument_id();
        let raw = raw_symbol(instrument_id);
        self.bar_types
            .insert(bar_key(&raw, &interval), cmd.bar_type);
        self.queue_subscription_payload(
            channel,
            candle_subscription_payload(self.config.product_type, raw, interval),
        );
        Ok(())
    }

    fn subscribe_mark_prices(&mut self, cmd: SubscribeMarkPrices) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.config.product_type.is_derivative(),
            "Gate.io mark prices are only available for perpetual instruments"
        );
        self.ensure_product(cmd.instrument_id)?;
        if self.acquire_ticker_subscription(cmd.instrument_id)? {
            self.queue_subscription(GATEIO_FUTURES_TICKER_WS_CHANNEL, cmd.instrument_id, vec![]);
        }
        Ok(())
    }

    fn subscribe_index_prices(&mut self, cmd: SubscribeIndexPrices) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.config.product_type.is_derivative(),
            "Gate.io index prices are only available for perpetual instruments"
        );
        self.ensure_product(cmd.instrument_id)?;
        if self.acquire_ticker_subscription(cmd.instrument_id)? {
            self.queue_subscription(GATEIO_FUTURES_TICKER_WS_CHANNEL, cmd.instrument_id, vec![]);
        }
        Ok(())
    }

    fn subscribe_funding_rates(&mut self, cmd: SubscribeFundingRates) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.config.product_type.is_derivative(),
            "Gate.io funding rates are only available for perpetual instruments"
        );
        self.ensure_product(cmd.instrument_id)?;
        if self.acquire_ticker_subscription(cmd.instrument_id)? {
            self.queue_subscription(GATEIO_FUTURES_TICKER_WS_CHANNEL, cmd.instrument_id, vec![]);
        }
        Ok(())
    }

    fn unsubscribe_quotes(
        &mut self,
        cmd: &nautilus_common::messages::data::UnsubscribeQuotes,
    ) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let ws = self.ws_client.clone();
        let raw = raw_symbol(cmd.instrument_id);
        let channel = match self.config.product_type {
            GateioProductType::Spot => GATEIO_SPOT_BOOK_TICKER_WS_CHANNEL,
            GateioProductType::UsdtPerpetual => GATEIO_FUTURES_BOOK_TICKER_WS_CHANNEL,
        };
        self.queue(async move {
            let _ = ws.unsubscribe(channel, vec![raw], false).await;
        });
        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let ws = self.ws_client.clone();
        let raw = raw_symbol(cmd.instrument_id);
        let channel = if self.config.product_type == GateioProductType::Spot {
            GATEIO_SPOT_TRADES_WS_CHANNEL
        } else {
            GATEIO_FUTURES_TRADES_WS_CHANNEL
        };
        self.queue(async move {
            let _ = ws.unsubscribe(channel, vec![raw], false).await;
        });
        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.unsubscribe_book(cmd.instrument_id, false)
    }

    fn unsubscribe_book_depth10(&mut self, cmd: &UnsubscribeBookDepth10) -> anyhow::Result<()> {
        self.unsubscribe_book(cmd.instrument_id, true)
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        self.ensure_product(cmd.bar_type.instrument_id())?;
        let spec = cmd.bar_type.spec();
        let interval = match spec.aggregation {
            BarAggregation::Minute => format!("{}m", spec.step),
            BarAggregation::Hour => format!("{}h", spec.step),
            BarAggregation::Day => format!("{}d", spec.step),
            _ => anyhow::bail!("Gate.io supports minute, hour, and day bars"),
        };
        let ws = self.ws_client.clone();
        let raw = raw_symbol(cmd.bar_type.instrument_id());
        self.bar_types.remove(&bar_key(&raw, &interval));
        let channel = if self.config.product_type == GateioProductType::Spot {
            GATEIO_SPOT_CANDLES_WS_CHANNEL
        } else {
            crate::common::consts::GATEIO_FUTURES_CANDLES_WS_CHANNEL
        };
        let payload =
            candle_subscription_payload(self.config.product_type, raw.clone(), interval.clone());
        self.queue(async move {
            let _ = ws.unsubscribe(channel, payload, false).await;
        });
        Ok(())
    }

    fn unsubscribe_mark_prices(
        &mut self,
        cmd: &nautilus_common::messages::data::UnsubscribeMarkPrices,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.config.product_type.is_derivative(),
            "Gate.io mark prices are only available for perpetual instruments"
        );
        self.ensure_product(cmd.instrument_id)?;
        if self.release_ticker_subscription(cmd.instrument_id)? {
            let ws = self.ws_client.clone();
            let raw = raw_symbol(cmd.instrument_id);
            self.queue(async move {
                let _ = ws
                    .unsubscribe(GATEIO_FUTURES_TICKER_WS_CHANNEL, vec![raw], false)
                    .await;
            });
        }
        Ok(())
    }

    fn unsubscribe_index_prices(&mut self, cmd: &UnsubscribeIndexPrices) -> anyhow::Result<()> {
        self.unsubscribe_mark_prices(&nautilus_common::messages::data::UnsubscribeMarkPrices {
            instrument_id: cmd.instrument_id,
            client_id: cmd.client_id,
            venue: cmd.venue,
            command_id: cmd.command_id,
            ts_init: cmd.ts_init,
            correlation_id: cmd.correlation_id,
            params: cmd.params.clone(),
        })
    }

    fn unsubscribe_funding_rates(&mut self, cmd: &UnsubscribeFundingRates) -> anyhow::Result<()> {
        self.unsubscribe_mark_prices(&nautilus_common::messages::data::UnsubscribeMarkPrices {
            instrument_id: cmd.instrument_id,
            client_id: cmd.client_id,
            venue: cmd.venue,
            command_id: cmd.command_id,
            ts_init: cmd.ts_init,
            correlation_id: cmd.correlation_id,
            params: cmd.params.clone(),
        })
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let product_type = self.config.product_type;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start = datetime_to_unix_nanos(request.start);
        let end = datetime_to_unix_nanos(request.end);
        let params = request.params;
        let request_id = request.request_id;
        let clock = self.clock;
        self.spawn_task(async move {
            match http.instruments(product_type, clock.get_time_ns()).await {
                Ok(data) => {
                    let response = DataResponse::Instruments(InstrumentsResponse::new(
                        request_id,
                        client_id,
                        *GATEIO_VENUE,
                        data,
                        start,
                        end,
                        clock.get_time_ns(),
                        params,
                    ));
                    let _ = sender.send(DataEvent::Response(response));
                }
                Err(error) => log::warn!("Gate.io instruments request failed: {error}"),
            }
        });
        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        self.ensure_product(request.instrument_id)?;
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let instrument_id = request.instrument_id;
        let start = datetime_to_unix_nanos(request.start);
        let end = datetime_to_unix_nanos(request.end);
        let params = request.params;
        let product_type = self.config.product_type;
        let instruments = Arc::clone(&self.instruments);
        let clock = self.clock;
        self.spawn_task(async move {
            match http
                .instrument(instrument_id, product_type, clock.get_time_ns())
                .await
            {
                Ok(data) => {
                    instruments.insert(instrument_id, data.clone());
                    let response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
                        request_id,
                        client_id,
                        instrument_id,
                        data,
                        start,
                        end,
                        clock.get_time_ns(),
                        params,
                    )));
                    let _ = sender.send(DataEvent::Response(response));
                }
                Err(error) => log::warn!("Gate.io instrument request failed: {error}"),
            }
        });
        Ok(())
    }

    fn request_book_snapshot(&self, request: RequestBookSnapshot) -> anyhow::Result<()> {
        self.ensure_product(request.instrument_id)?;
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments = Arc::clone(&self.instruments);
        let product_type = self.config.product_type;
        let instrument_id = request.instrument_id;
        let depth = request.depth.map(|value| value.get() as u32);
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let params = request.params;
        let clock = self.clock;
        self.spawn_task(async move {
            let result = async {
                let instrument = match instruments.get_cloned(&instrument_id) {
                    Some(value) => value,
                    None => {
                        let value = http
                            .instrument(instrument_id, product_type, clock.get_time_ns())
                            .await?;
                        instruments.insert(instrument_id, value.clone());
                        value
                    }
                };
                let deltas = http
                    .order_book(&instrument, product_type, depth, clock.get_time_ns())
                    .await?;
                let mut book = OrderBook::new(instrument_id, BookType::L2_MBP);
                book.apply_deltas(&deltas)?;
                anyhow::Ok(book)
            }
            .await;
            match result {
                Ok(book) => {
                    let response = DataResponse::Book(BookResponse::new(
                        request_id,
                        client_id,
                        instrument_id,
                        book,
                        None,
                        None,
                        clock.get_time_ns(),
                        params,
                    ));
                    let _ = sender.send(DataEvent::Response(response));
                }
                Err(error) => log::warn!("Gate.io book request failed: {error}"),
            }
        });
        Ok(())
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        self.ensure_product(request.instrument_id)?;
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments = Arc::clone(&self.instruments);
        let product_type = self.config.product_type;
        let instrument_id = request.instrument_id;
        let limit = request.limit.map(|value| value.get() as u32);
        let start = request.start;
        let end = request.end;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);
        let params = request.params;
        let clock = self.clock;
        self.spawn_task(async move {
            let result = async {
                let instrument = match instruments.get_cloned(&instrument_id) {
                    Some(value) => value,
                    None => {
                        let value = http
                            .instrument(instrument_id, product_type, clock.get_time_ns())
                            .await?;
                        instruments.insert(instrument_id, value.clone());
                        value
                    }
                };
                http.trades(
                    &instrument,
                    product_type,
                    limit,
                    start,
                    end,
                    clock.get_time_ns(),
                )
                .await
            }
            .await;
            match result {
                Ok(data) => {
                    let response =
                        DataResponse::Trades(nautilus_common::messages::data::TradesResponse::new(
                            request_id,
                            client_id,
                            instrument_id,
                            data,
                            start_nanos,
                            end_nanos,
                            clock.get_time_ns(),
                            params,
                        ));
                    let _ = sender.send(DataEvent::Response(response));
                }
                Err(error) => log::warn!("Gate.io trades request failed: {error}"),
            }
        });
        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let bar_type = request.bar_type;
        let instrument_id = bar_type.instrument_id();
        self.ensure_product(instrument_id)?;
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments = Arc::clone(&self.instruments);
        let product_type = self.config.product_type;
        let limit = request.limit.map(|value| value.get() as u32);
        let start = request.start;
        let end = request.end;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);
        let params = request.params;
        let clock = self.clock;
        self.spawn_task(async move {
            let result = async {
                let instrument = match instruments.get_cloned(&instrument_id) {
                    Some(value) => value,
                    None => {
                        let value = http
                            .instrument(instrument_id, product_type, clock.get_time_ns())
                            .await?;
                        instruments.insert(instrument_id, value.clone());
                        value
                    }
                };
                http.bars(
                    &instrument,
                    product_type,
                    bar_type,
                    limit,
                    start,
                    end,
                    clock.get_time_ns(),
                )
                .await
            }
            .await;
            match result {
                Ok(data) => {
                    let response = DataResponse::Bars(BarsResponse::new(
                        request_id,
                        client_id,
                        bar_type,
                        data,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));
                    let _ = sender.send(DataEvent::Response(response));
                }
                Err(error) => log::warn!("Gate.io bars request failed: {error}"),
            }
        });
        Ok(())
    }

    fn request_funding_rates(&self, request: RequestFundingRates) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.config.product_type.is_derivative(),
            "Gate.io funding rates are only available for perpetual instruments"
        );
        let http = self.http_client.clone();
        let instruments = Arc::clone(&self.instruments);
        let client_id = request.client_id.unwrap_or(self.client_id);
        let sender = self.data_sender.clone();
        let request_id = request.request_id;
        let instrument_id = request.instrument_id;
        let limit = request.limit.map(|value| value.get() as u32);
        let start = request.start;
        let end = request.end;
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);
        let params = request.params;
        let product_type = self.config.product_type;
        let clock = self.clock;
        self.spawn_task(async move {
            let result = async {
                let instrument = match instruments.get_cloned(&instrument_id) {
                    Some(value) => value,
                    None => {
                        let value = http
                            .instrument(instrument_id, product_type, clock.get_time_ns())
                            .await?;
                        instruments.insert(instrument_id, value.clone());
                        value
                    }
                };
                http.funding_rates(&instrument, limit, clock.get_time_ns())
                    .await
            }
            .await;
            match result {
                Ok(data) => {
                    let response = DataResponse::FundingRates(FundingRatesResponse::new(
                        request_id,
                        client_id,
                        instrument_id,
                        data,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));
                    let _ = sender.send(DataEvent::Response(response));
                }
                Err(error) => log::warn!("Gate.io funding rates request failed: {error}"),
            }
        });
        Ok(())
    }

    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        self.ensure_product(request.instrument_id)?;
        log::debug!(
            "Gate.io quote history is not available through the REST facade; use quote subscription"
        );
        Ok(())
    }

    fn request_book_depth(&self, request: RequestBookDepth) -> anyhow::Result<()> {
        self.request_book_snapshot(RequestBookSnapshot {
            instrument_id: request.instrument_id,
            depth: request.depth,
            client_id: request.client_id,
            request_id: request.request_id,
            ts_init: request.ts_init,
            params: request.params,
        })
    }

    fn request_book_deltas(&self, request: RequestBookDeltas) -> anyhow::Result<()> {
        self.request_book_snapshot(RequestBookSnapshot {
            instrument_id: request.instrument_id,
            depth: None,
            client_id: request.client_id,
            request_id: request.request_id,
            ts_init: request.ts_init,
            params: request.params,
        })
    }
}
