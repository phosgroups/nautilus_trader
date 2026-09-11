//! Gate.io market data clients.

use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
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
    data::{BarSpecification, BarType, Data, FundingRateUpdate},
    enums::{AggregationSource, BarAggregation, BookType, PriceType},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    orderbook::OrderBook,
};
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
    book_subscriptions: Arc<Mutex<HashMap<InstrumentId, Vec<String>>>>,
    book_states: Arc<Mutex<HashMap<String, GateioBookState>>>,
    ticker_subscriptions: Arc<Mutex<HashMap<String, usize>>>,
    tasks: Vec<JoinHandle<()>>,
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
            book_subscriptions: Arc::new(Mutex::new(HashMap::new())),
            book_states: Arc::new(Mutex::new(HashMap::new())),
            ticker_subscriptions: Arc::new(Mutex::new(HashMap::new())),
            tasks: Vec::new(),
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
        for task in self.tasks.drain(..) {
            task.abort();
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
        self.tasks.push(get_runtime().spawn(future));
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
        let http_client = self.http_client.clone();
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
                        &http_client,
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

#[derive(Debug, Default)]
struct GateioBookState {
    initialized: bool,
    syncing: bool,
    generation: u64,
    last_sequence: u64,
    buffered: Vec<GateioOrderBook>,
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

fn send_book_deltas(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    deltas: nautilus_model::data::OrderBookDeltas,
) {
    let _ = sender.send(DataEvent::Data(Data::Deltas(
        nautilus_model::data::OrderBookDeltas_API::new(deltas),
    )));
}

fn schedule_book_sync(
    raw: String,
    generation: u64,
    product_type: GateioProductType,
    sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    book_states: Arc<Mutex<HashMap<String, GateioBookState>>>,
    http_client: GateioHttpClient,
    clock: &'static AtomicTime,
) {
    get_runtime().spawn(async move {
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
                state.buffered = gap.remaining;
                continue;
            }
        };
        state.initialized = true;
        state.syncing = false;
        state.last_sequence = replay.last_sequence;

        send_book_deltas(sender, snapshot_deltas);
        for update in replay.updates {
            match crate::common::parse::parse_book_update(&update, &instrument, clock.get_time_ns())
            {
                Ok(deltas) => send_book_deltas(sender, deltas),
                Err(error) => {
                    log::warn!("Failed to replay Gate.io order-book update for {raw}: {error}")
                }
            }
        }
        drop(states);
        return;
    }

    log::error!("Gate.io order-book sync could not establish a contiguous stream for {raw}");
    if let Ok(mut states) = book_states.lock()
        && let Some(state) = states.get_mut(raw)
    {
        state.syncing = false;
    }
}

fn schedule_reconnect_book_syncs(
    product_type: GateioProductType,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: &Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    book_states: &Arc<Mutex<HashMap<String, GateioBookState>>>,
    http_client: &GateioHttpClient,
    clock: &'static AtomicTime,
) {
    let Ok(mut states) = book_states.lock() else {
        log::warn!("Gate.io order-book state lock poisoned during WebSocket recovery");
        return;
    };
    let generations = states
        .iter_mut()
        .map(|(raw, state)| {
            state.initialized = false;
            state.syncing = true;
            state.buffered.clear();
            state.generation = state.generation.saturating_add(1);
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
    http_client: &GateioHttpClient,
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
            http_client,
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

            let mut resync_generation = None;
            let mut should_publish = false;
            if let Ok(mut states) = book_states.lock() {
                let Some(state) = states.get_mut(&raw) else {
                    return;
                };
                if !state.initialized || state.syncing {
                    if state.buffered.len() >= MAX_BOOK_BUFFERED_UPDATES {
                        state.buffered.clear();
                        log::warn!(
                            "Gate.io order-book buffer overflow for {raw}; waiting for a fresh snapshot"
                        );
                    }
                    state.buffered.push(book);
                    return;
                }
                match book_update_range(&book) {
                    None => {
                        state.initialized = false;
                        state.syncing = true;
                        state.generation = state.generation.saturating_add(1);
                        if state.buffered.len() >= MAX_BOOK_BUFFERED_UPDATES {
                            state.buffered.clear();
                        }
                        state.buffered.push(book.clone());
                        resync_generation = Some(state.generation);
                    }
                    Some(range) => match classify_book_update(state.last_sequence, range) {
                        BookUpdateAction::Stale => return,
                        BookUpdateAction::Gap => {
                            state.initialized = false;
                            state.syncing = true;
                            state.generation = state.generation.saturating_add(1);
                            if state.buffered.len() >= MAX_BOOK_BUFFERED_UPDATES {
                                state.buffered.clear();
                            }
                            state.buffered.push(book.clone());
                            resync_generation = Some(state.generation);
                        }
                        BookUpdateAction::Apply => {
                            state.last_sequence = range.last;
                            should_publish = true;
                        }
                    },
                }
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
                    clock,
                );
            } else if should_publish {
                match crate::common::parse::parse_book_update(&book, &instrument, ts_init) {
                    Ok(deltas) => send_book_deltas(sender, deltas),
                    Err(error) => log::debug!("Failed to parse Gate.io order-book update: {error}"),
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
        self.ws_client.stop();
        self.abort_tasks();
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.instruments.store(Default::default());
        self.bar_types.store(Default::default());
        self.book_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("Gate.io order-book subscription lock poisoned"))?
            .clear();
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
        self.ensure_product(cmd.instrument_id)?;
        let product_type = self.config.product_type;
        let instrument_id = cmd.instrument_id;
        let payload = order_book_update_subscription_payload(product_type, instrument_id);
        self.book_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("Gate.io order-book subscription lock poisoned"))?
            .insert(instrument_id, payload.clone());
        let raw = raw_symbol(instrument_id);
        self.book_states
            .lock()
            .map_err(|_| anyhow::anyhow!("Gate.io order-book state lock poisoned"))?
            .insert(
                raw.clone(),
                GateioBookState {
                    syncing: true,
                    ..Default::default()
                },
            );
        let channel = if product_type == GateioProductType::Spot {
            GATEIO_SPOT_ORDER_BOOK_UPDATE_WS_CHANNEL
        } else {
            GATEIO_FUTURES_ORDER_BOOK_UPDATE_WS_CHANNEL
        };
        let ws = self.ws_client.clone();
        let sender = self.data_sender.clone();
        let instruments = Arc::clone(&self.instruments);
        let book_states = Arc::clone(&self.book_states);
        let http_client = self.http_client.clone();
        let clock = self.clock;
        self.queue(async move {
            if let Err(error) = ws.subscribe(channel, payload, false).await {
                log::warn!("Gate.io order-book update subscription failed: {error}");
                if let Ok(mut states) = book_states.lock()
                    && let Some(state) = states.get_mut(&raw)
                {
                    state.syncing = false;
                }
                return;
            }
            synchronize_book(
                &raw,
                0,
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

    fn subscribe_book_depth10(&mut self, cmd: SubscribeBookDepth10) -> anyhow::Result<()> {
        self.subscribe_book_deltas(SubscribeBookDeltas {
            instrument_id: cmd.instrument_id,
            book_type: cmd.book_type,
            client_id: cmd.client_id,
            venue: cmd.venue,
            command_id: cmd.command_id,
            ts_init: cmd.ts_init,
            depth: cmd.depth,
            managed: cmd.managed,
            correlation_id: cmd.correlation_id,
            params: cmd.params.clone(),
        })
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
        self.ensure_product(cmd.instrument_id)?;
        let ws = self.ws_client.clone();
        let payload = self
            .book_subscriptions
            .lock()
            .ok()
            .and_then(|mut subscriptions| subscriptions.remove(&cmd.instrument_id))
            .unwrap_or_else(|| {
                order_book_update_subscription_payload(self.config.product_type, cmd.instrument_id)
            });
        if let Ok(mut states) = self.book_states.lock() {
            states.remove(raw_symbol(cmd.instrument_id).as_str());
        }
        let channel = if self.config.product_type == GateioProductType::Spot {
            GATEIO_SPOT_ORDER_BOOK_UPDATE_WS_CHANNEL
        } else {
            GATEIO_FUTURES_ORDER_BOOK_UPDATE_WS_CHANNEL
        };
        self.queue(async move {
            let _ = ws.unsubscribe(channel, payload, false).await;
        });
        Ok(())
    }

    fn unsubscribe_book_depth10(&mut self, cmd: &UnsubscribeBookDepth10) -> anyhow::Result<()> {
        self.unsubscribe_book_deltas(&UnsubscribeBookDeltas {
            instrument_id: cmd.instrument_id,
            client_id: cmd.client_id,
            venue: cmd.venue,
            command_id: cmd.command_id,
            ts_init: cmd.ts_init,
            correlation_id: cmd.correlation_id,
            params: cmd.params.clone(),
        })
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
        get_runtime().spawn(async move {
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
        get_runtime().spawn(async move {
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
        get_runtime().spawn(async move {
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
        get_runtime().spawn(async move {
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
        get_runtime().spawn(async move {
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
        get_runtime().spawn(async move {
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
