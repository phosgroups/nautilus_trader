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
            GATEIO_FUTURES_ORDER_BOOK_WS_CHANNEL, GATEIO_FUTURES_TICKER_WS_CHANNEL,
            GATEIO_FUTURES_TRADES_WS_CHANNEL, GATEIO_SPOT_CANDLES_WS_CHANNEL,
            GATEIO_SPOT_ORDER_BOOK_WS_CHANNEL, GATEIO_SPOT_TICKER_WS_CHANNEL,
            GATEIO_SPOT_TRADES_WS_CHANNEL, GATEIO_VENUE,
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
    websocket::{GateioWebSocketClient, GateioWsMessage},
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
    Candlesticks,
}

impl GateioDataChannel {
    fn parse(channel: &str) -> Option<Self> {
        match channel {
            GATEIO_SPOT_TRADES_WS_CHANNEL | GATEIO_FUTURES_TRADES_WS_CHANNEL => Some(Self::Trades),
            GATEIO_SPOT_TICKER_WS_CHANNEL
            | GATEIO_FUTURES_TICKER_WS_CHANNEL
            | GATEIO_FUTURES_BOOK_TICKER_WS_CHANNEL => Some(Self::Tickers),
            GATEIO_SPOT_ORDER_BOOK_WS_CHANNEL | GATEIO_FUTURES_ORDER_BOOK_WS_CHANNEL => {
                Some(Self::OrderBook)
            }
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
        let ws = self.ws_client.clone();
        self.queue(async move {
            if let Err(error) = ws.subscribe(channel, payload, false).await {
                log::warn!("Gate.io subscription failed for {channel}: {error}");
            }
        });
    }

    fn start_ws_dispatch(&mut self) -> anyhow::Result<()> {
        if self.ws_task.is_some() {
            return Ok(());
        }
        let mut receiver = self
            .ws_client
            .take_event_receiver()
            .context("Gate.io WebSocket event receiver was already taken")?;
        let sender = self.data_sender.clone();
        let instruments = Arc::clone(&self.instruments);
        let bar_types = Arc::clone(&self.bar_types);
        let product_type = self.config.product_type;
        let clock = self.clock;

        self.ws_task = Some(get_runtime().spawn(async move {
            while let Some(message) = receiver.recv().await {
                dispatch_ws_message(
                    message,
                    &sender,
                    &instruments,
                    &bar_types,
                    product_type,
                    clock,
                );
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

fn order_book_subscription_payload(
    product_type: GateioProductType,
    instrument_id: InstrumentId,
    depth: Option<u32>,
) -> Vec<String> {
    let raw = raw_symbol(instrument_id);
    let level = match depth.unwrap_or(20) {
        0..=5 => "5",
        6..=10 => "10",
        11..=20 => "20",
        21..=50 => "50",
        _ => "100",
    };
    match product_type {
        GateioProductType::Spot => vec![raw, level.to_string(), "100ms".to_string()],
        GateioProductType::UsdtPerpetual => vec![raw, level.to_string(), "0".to_string()],
    }
}

fn timestamp_from_ws(
    message: &GateioWsMessage,
    clock: &'static AtomicTime,
) -> nautilus_core::UnixNanos {
    message
        .time_ms
        .and_then(|millis| crate::common::parse::timestamp_nanos(millis).ok())
        .or_else(|| {
            message
                .time
                .and_then(|seconds| seconds.checked_mul(1_000))
                .and_then(|millis| crate::common::parse::timestamp_nanos(millis).ok())
        })
        .unwrap_or_else(|| clock.get_time_ns())
}

fn dispatch_ws_message(
    message: GateioWsMessage,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: &AtomicMap<InstrumentId, InstrumentAny>,
    bar_types: &AtomicMap<String, BarType>,
    product_type: GateioProductType,
    clock: &'static AtomicTime,
) {
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
            let value = message
                .result
                .as_array()
                .and_then(|rows| rows.first())
                .unwrap_or(&message.result);
            let Ok(ticker) = serde_json::from_value::<GateioTicker>(value.clone()) else {
                return;
            };
            let raw = raw_from_value(value, product_type)
                .or_else(|| ticker.currency_pair.clone())
                .or_else(|| ticker.contract.clone());
            let Some(instrument) = raw
                .as_deref()
                .and_then(|value| instrument_for_raw(instruments, value))
            else {
                return;
            };
            let Some(bid) = ticker.highest_bid.as_deref() else {
                return;
            };
            let Some(ask) = ticker.lowest_ask.as_deref() else {
                return;
            };
            let quote = match parse_quote(
                bid,
                ask,
                ticker.highest_size.as_deref().unwrap_or("0"),
                ticker.lowest_size.as_deref().unwrap_or("0"),
                &instrument,
                ts_event,
                ts_init,
            ) {
                Ok(quote) => quote,
                Err(error) => {
                    log::debug!("Failed to parse Gate.io quote update: {error}");
                    return;
                }
            };
            let _ = sender.send(DataEvent::Data(Data::Quote(quote)));

            if product_type.is_derivative() {
                if let Some(mark_price) = ticker.mark_price.as_deref()
                    && let Ok(update) = parse_mark_price(mark_price, &instrument, ts_event, ts_init)
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
        GateioDataChannel::Candlesticks => {
            if event != GateioWsEvent::Update {
                return;
            }
            let value = message.result.clone();
            let Ok(candle) = serde_json::from_value::<GateioCandle>(value.clone()) else {
                return;
            };
            let Some(raw) = raw_from_value(&value, product_type) else {
                return;
            };
            let Some(instrument) = instrument_for_raw(instruments, &raw) else {
                return;
            };
            let Some(interval) = value.get("n").and_then(serde_json::Value::as_str) else {
                return;
            };
            let Some(bar_type) = bar_types.get_cloned(&bar_key(&raw, interval)).or_else(|| {
                bar_spec_from_interval(interval)
                    .map(|spec| BarType::new(instrument.id(), spec, AggregationSource::External))
            }) else {
                return;
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn builds_full_book_subscription_payloads() {
        let spot = InstrumentId::from("BTC_USDT.GATEIO");
        let perpetual = InstrumentId::from("BTC_USDT-PERP.GATEIO");
        assert_eq!(
            order_book_subscription_payload(GateioProductType::Spot, spot, Some(10)),
            vec!["BTC_USDT", "10", "100ms"]
        );
        assert_eq!(
            order_book_subscription_payload(GateioProductType::UsdtPerpetual, perpetual, Some(20)),
            vec!["BTC_USDT", "20", "0"]
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
            .instruments(self.config.product_type, ts)
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
            GateioProductType::Spot => GATEIO_SPOT_TICKER_WS_CHANNEL,
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
        let depth = cmd.depth.map(|value| value.get() as u32);
        let payload = order_book_subscription_payload(product_type, instrument_id, depth);
        self.book_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("Gate.io order-book subscription lock poisoned"))?
            .insert(instrument_id, payload.clone());
        let channel = if product_type == GateioProductType::Spot {
            GATEIO_SPOT_ORDER_BOOK_WS_CHANNEL
        } else {
            GATEIO_FUTURES_ORDER_BOOK_WS_CHANNEL
        };
        let ws = self.ws_client.clone();
        self.queue(async move {
            if let Err(error) = ws.subscribe(channel, payload, false).await {
                log::warn!("Gate.io full order-book subscription failed: {error}");
            }
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
        self.bar_types
            .insert(bar_key(&raw_symbol(instrument_id), &interval), cmd.bar_type);
        self.queue_subscription(channel, instrument_id, vec![interval]);
        Ok(())
    }

    fn subscribe_mark_prices(&mut self, cmd: SubscribeMarkPrices) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.config.product_type.is_derivative(),
            "Gate.io mark prices are only available for perpetual instruments"
        );
        self.subscribe_quotes(SubscribeQuotes {
            instrument_id: cmd.instrument_id,
            client_id: cmd.client_id,
            venue: cmd.venue,
            command_id: cmd.command_id,
            ts_init: cmd.ts_init,
            correlation_id: cmd.correlation_id,
            params: cmd.params.clone(),
        })
    }

    fn subscribe_index_prices(&mut self, cmd: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscribe_mark_prices(SubscribeMarkPrices {
            instrument_id: cmd.instrument_id,
            client_id: cmd.client_id,
            venue: cmd.venue,
            command_id: cmd.command_id,
            ts_init: cmd.ts_init,
            correlation_id: cmd.correlation_id,
            params: cmd.params.clone(),
        })
    }

    fn subscribe_funding_rates(&mut self, cmd: SubscribeFundingRates) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.config.product_type.is_derivative(),
            "Gate.io funding rates are only available for perpetual instruments"
        );
        self.subscribe_quotes(SubscribeQuotes {
            instrument_id: cmd.instrument_id,
            client_id: cmd.client_id,
            venue: cmd.venue,
            command_id: cmd.command_id,
            ts_init: cmd.ts_init,
            correlation_id: cmd.correlation_id,
            params: cmd.params.clone(),
        })
    }

    fn unsubscribe_quotes(
        &mut self,
        cmd: &nautilus_common::messages::data::UnsubscribeQuotes,
    ) -> anyhow::Result<()> {
        self.ensure_product(cmd.instrument_id)?;
        let ws = self.ws_client.clone();
        let raw = raw_symbol(cmd.instrument_id);
        let channel = GateioWebSocketClient::ticker_channel(self.config.product_type);
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
                order_book_subscription_payload(self.config.product_type, cmd.instrument_id, None)
            });
        let channel = if self.config.product_type == GateioProductType::Spot {
            GATEIO_SPOT_ORDER_BOOK_WS_CHANNEL
        } else {
            GATEIO_FUTURES_ORDER_BOOK_WS_CHANNEL
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
        self.queue(async move {
            let _ = ws.unsubscribe(channel, vec![raw, interval], false).await;
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
        self.unsubscribe_quotes(&nautilus_common::messages::data::UnsubscribeQuotes {
            instrument_id: cmd.instrument_id,
            client_id: cmd.client_id,
            venue: cmd.venue,
            command_id: cmd.command_id,
            ts_init: cmd.ts_init,
            correlation_id: cmd.correlation_id,
            params: cmd.params.clone(),
        })
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
