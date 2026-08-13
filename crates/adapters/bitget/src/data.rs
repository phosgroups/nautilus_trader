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

//! Live market data client implementation for the Bitget adapter.

use std::{
    collections::BTreeMap,
    future::Future,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{runner::try_get_data_event_sender, runtime::get_runtime},
    messages::{
        DataEvent,
        data::{
            BarsResponse, BookResponse, DataResponse, FundingRatesResponse, InstrumentResponse,
            InstrumentsResponse, RequestBars, RequestBookSnapshot, RequestFundingRates,
            RequestInstrument, RequestInstruments, RequestTrades, SubscribeBars,
            SubscribeBookDeltas, SubscribeBookDepth10, SubscribeFundingRates, SubscribeIndexPrices,
            SubscribeInstrumentClose, SubscribeInstrumentStatus, SubscribeMarkPrices,
            SubscribeQuotes, SubscribeTrades, TradesResponse, UnsubscribeBars,
            UnsubscribeBookDeltas, UnsubscribeBookDepth10, UnsubscribeFundingRates,
            UnsubscribeIndexPrices, UnsubscribeInstrumentClose, UnsubscribeInstrumentStatus,
            UnsubscribeMarkPrices, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    AtomicMap, AtomicSet,
    datetime::datetime_to_unix_nanos,
    nanos::UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::{BarType, Data, InstrumentStatus, OrderBookDeltas_API},
    enums::{BookType, MarketStatusAction},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    orderbook::book::OrderBook,
};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        consts::BITGET_VENUE,
        enums::BitgetProductType,
        parse::{
            bar_spec_to_bitget_interval_for_product, parse_candle_bar, parse_market_trade,
            parse_orderbook_depth10_snapshot, parse_orderbook_snapshot, parse_ws_funding_rate,
            parse_ws_index_price, parse_ws_mark_price, parse_ws_orderbook_deltas,
            parse_ws_quote_tick,
        },
        symbol::{BitgetSymbol, extract_raw_symbol},
    },
    config::BitgetDataClientConfig,
    http::{
        client::BitgetHttpClient,
        models::{BitgetCandle, BitgetDecimalValue, BitgetOrderBookSnapshot},
    },
    websocket::{
        client::BitgetWebSocketClient,
        messages::{
            BitgetBookData, BitgetBookLevel, BitgetPublicTradeData, BitgetTickerData, BitgetWsArg,
            BitgetWsMessage,
        },
    },
};

pub(crate) const TICKER_SUB_MARK: &str = "mark";
pub(crate) const TICKER_SUB_INDEX: &str = "index";
pub(crate) const TICKER_SUB_FUNDING: &str = "funding";
pub(crate) const TICKER_SUB_QUOTE: &str = "quote";
pub(crate) const BOOK_SUB_DELTAS: &str = "deltas";
pub(crate) const BOOK_SUB_DEPTH10: &str = "depth10";
const BITGET_BOOK_CHECKSUM_DEPTH: usize = 25;
pub(crate) const BITGET_DEPTH10_DEPTH: u32 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookSyncDecision {
    Apply,
    Recover,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BookChecksumDecision {
    Valid,
    Recover,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BitgetBookChecksumState {
    bids: BTreeMap<Decimal, (String, String)>,
    asks: BTreeMap<Decimal, (String, String)>,
}

impl BitgetBookChecksumState {
    fn from_snapshot(snapshot: &BitgetOrderBookSnapshot) -> anyhow::Result<Self> {
        let mut state = Self::default();
        for level in &snapshot.bids {
            state.apply_snapshot_level(true, level)?;
        }
        for level in &snapshot.asks {
            state.apply_snapshot_level(false, level)?;
        }
        Ok(state)
    }

    fn apply_book(&mut self, book: &BitgetBookData, action: Option<&str>) -> anyhow::Result<()> {
        if action.is_some_and(|value| value.eq_ignore_ascii_case("snapshot")) {
            self.bids.clear();
            self.asks.clear();
        }

        for level in &book.bids {
            self.apply_ws_level(true, level)?;
        }
        for level in &book.asks {
            self.apply_ws_level(false, level)?;
        }
        Ok(())
    }

    fn apply_snapshot_level(
        &mut self,
        is_bid: bool,
        level: &[BitgetDecimalValue],
    ) -> anyhow::Result<()> {
        let price = level
            .first()
            .map(BitgetDecimalValue::as_decimal_str)
            .context("Bitget snapshot book level missing price")?;
        let size = level
            .get(1)
            .map(BitgetDecimalValue::as_decimal_str)
            .context("Bitget snapshot book level missing size")?;
        self.apply_raw_level(is_bid, price, size)
    }

    fn apply_ws_level(&mut self, is_bid: bool, level: &BitgetBookLevel) -> anyhow::Result<()> {
        self.apply_raw_level(is_bid, level.0.clone(), level.1.clone())
    }

    fn apply_raw_level(
        &mut self,
        is_bid: bool,
        price_raw: String,
        size_raw: String,
    ) -> anyhow::Result<()> {
        let price = Decimal::from_str(price_raw.trim())
            .with_context(|| format!("invalid Bitget checksum price: {price_raw:?}"))?;
        let size = Decimal::from_str(size_raw.trim())
            .with_context(|| format!("invalid Bitget checksum size: {size_raw:?}"))?;
        let levels = if is_bid {
            &mut self.bids
        } else {
            &mut self.asks
        };

        if size.is_zero() {
            levels.remove(&price);
        } else {
            levels.insert(price, (price_raw, size_raw));
        }
        Ok(())
    }

    fn checksum_string(&self) -> String {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(BITGET_BOOK_CHECKSUM_DEPTH)
            .collect::<Vec<_>>();
        let asks = self
            .asks
            .iter()
            .take(BITGET_BOOK_CHECKSUM_DEPTH)
            .collect::<Vec<_>>();
        let max_len = bids.len().max(asks.len());
        let mut raw = String::new();

        for index in 0..max_len {
            if let Some((_, (price, size))) = bids.get(index) {
                append_checksum_level(&mut raw, price, size);
            }
            if let Some((_, (price, size))) = asks.get(index) {
                append_checksum_level(&mut raw, price, size);
            }
        }

        raw
    }

    fn checksum(&self) -> u32 {
        crc32_ieee(self.checksum_string().as_bytes())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BitgetBookDepth10State {
    bids: BTreeMap<Decimal, (String, String)>,
    asks: BTreeMap<Decimal, (String, String)>,
}

impl BitgetBookDepth10State {
    fn from_snapshot(snapshot: &BitgetOrderBookSnapshot) -> anyhow::Result<Self> {
        let mut state = Self::default();
        for level in &snapshot.bids {
            state.apply_snapshot_level(true, level)?;
        }
        for level in &snapshot.asks {
            state.apply_snapshot_level(false, level)?;
        }
        Ok(state)
    }

    fn apply_snapshot(&mut self, book: &BitgetBookData) -> anyhow::Result<()> {
        self.bids.clear();
        self.asks.clear();
        self.apply_update(book)
    }

    fn apply_update(&mut self, book: &BitgetBookData) -> anyhow::Result<()> {
        for level in &book.bids {
            self.apply_ws_level(true, level)?;
        }
        for level in &book.asks {
            self.apply_ws_level(false, level)?;
        }
        Ok(())
    }

    fn apply_snapshot_level(
        &mut self,
        is_bid: bool,
        level: &[BitgetDecimalValue],
    ) -> anyhow::Result<()> {
        let price = level
            .first()
            .ok_or_else(|| anyhow::anyhow!("missing price in Bitget depth10 level"))?;
        let size = level
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("missing size in Bitget depth10 level"))?;
        self.apply_raw_level(
            is_bid,
            book_decimal_value_as_string(price, "depth10.price")?,
            book_decimal_value_as_string(size, "depth10.size")?,
        )
    }

    fn apply_ws_level(&mut self, is_bid: bool, level: &BitgetBookLevel) -> anyhow::Result<()> {
        self.apply_raw_level(is_bid, level.0.clone(), level.1.clone())
    }

    fn apply_raw_level(
        &mut self,
        is_bid: bool,
        price_raw: String,
        size_raw: String,
    ) -> anyhow::Result<()> {
        let price = Decimal::from_str(&price_raw)
            .with_context(|| format!("invalid Bitget depth10 price: {price_raw:?}"))?;
        let size = Decimal::from_str(&size_raw)
            .with_context(|| format!("invalid Bitget depth10 size: {size_raw:?}"))?;
        let levels = if is_bid {
            &mut self.bids
        } else {
            &mut self.asks
        };

        if size.is_zero() {
            levels.remove(&price);
        } else {
            levels.insert(price, (price_raw, size_raw));
        }
        Ok(())
    }

    fn top_levels(&self) -> (Vec<(String, String)>, Vec<(String, String)>) {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(10)
            .map(|(_, level)| level.clone())
            .collect();
        let asks = self
            .asks
            .iter()
            .take(10)
            .map(|(_, level)| level.clone())
            .collect();
        (bids, asks)
    }
}

fn parse_bitget_millis_timestamp(raw: &str, field: &str) -> anyhow::Result<UnixNanos> {
    let millis = raw
        .trim()
        .parse::<u64>()
        .with_context(|| format!("invalid Bitget {field} timestamp: {raw:?}"))?;
    Ok(UnixNanos::from_millis(millis))
}

fn raw_book_ts_event(snapshot: &BitgetOrderBookSnapshot, ts_init: UnixNanos) -> UnixNanos {
    snapshot
        .ts
        .as_deref()
        .and_then(|ts| parse_bitget_millis_timestamp(ts, "orderbook.ts").ok())
        .unwrap_or(ts_init)
}

fn raw_book_sequence(snapshot: &BitgetOrderBookSnapshot, ts_event: UnixNanos) -> u64 {
    snapshot
        .seq
        .as_deref()
        .and_then(|seq| seq.trim().parse::<u64>().ok())
        .unwrap_or_else(|| ts_event.as_u64())
}

fn ws_book_ts_event(book: &BitgetBookData, ts_init: UnixNanos) -> UnixNanos {
    book.ts
        .as_deref()
        .and_then(|ts| parse_bitget_millis_timestamp(ts, "book.ts").ok())
        .unwrap_or(ts_init)
}

fn ws_book_sequence(book: &BitgetBookData, ts_event: UnixNanos) -> u64 {
    book.seq
        .and_then(|seq| u64::try_from(seq).ok())
        .unwrap_or_else(|| ts_event.as_u64())
}

fn recovery_book_depth(
    book_depths: &Arc<AtomicMap<InstrumentId, Option<u32>>>,
    instrument_id: InstrumentId,
    wants_deltas: bool,
    wants_depth10: bool,
) -> Option<u32> {
    if wants_deltas {
        book_depths.get_cloned(&instrument_id).flatten()
    } else if wants_depth10 {
        Some(BITGET_DEPTH10_DEPTH)
    } else {
        None
    }
}

fn build_depth10_from_state(
    state: &BitgetBookDepth10State,
    instrument: &InstrumentAny,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<nautilus_model::data::OrderBookDepth10> {
    let (bids, asks) = state.top_levels();
    parse_orderbook_depth10_snapshot(&bids, &asks, instrument, sequence, ts_event, ts_init)
}

pub(crate) fn store_depth10_snapshot(
    instrument: &InstrumentAny,
    snapshot: &BitgetOrderBookSnapshot,
    book_depth10_states: &Arc<AtomicMap<InstrumentId, BitgetBookDepth10State>>,
) -> anyhow::Result<BitgetBookDepth10State> {
    let state = BitgetBookDepth10State::from_snapshot(snapshot)?;
    book_depth10_states.insert(instrument.id(), state.clone());
    Ok(state)
}

fn emit_depth10_from_state(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    state: &BitgetBookDepth10State,
    instrument: &InstrumentAny,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<()> {
    let depth10 = build_depth10_from_state(state, instrument, sequence, ts_event, ts_init)?;
    send_data(sender, Data::Depth10(Box::new(depth10)));
    Ok(())
}

pub(crate) fn emit_depth10_snapshot(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instrument: &InstrumentAny,
    snapshot: &BitgetOrderBookSnapshot,
    book_depth10_states: &Arc<AtomicMap<InstrumentId, BitgetBookDepth10State>>,
    ts_init: UnixNanos,
) -> anyhow::Result<()> {
    let state = store_depth10_snapshot(instrument, snapshot, book_depth10_states)?;
    let ts_event = raw_book_ts_event(snapshot, ts_init);
    let sequence = raw_book_sequence(snapshot, ts_event);
    emit_depth10_from_state(sender, &state, instrument, sequence, ts_event, ts_init)
}

fn append_checksum_level(raw: &mut String, price: &str, size: &str) {
    if !raw.is_empty() {
        raw.push(':');
    }
    raw.push_str(price);
    raw.push(':');
    raw.push_str(size);
}

fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn checksum_matches(local: u32, remote: i64) -> bool {
    i64::from(local) == remote || i64::from(local as i32) == remote
}

pub(crate) fn upsert_instrument(
    instruments: &Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    instrument: InstrumentAny,
) {
    instruments.insert(instrument.id(), instrument);
}

pub(crate) async fn get_or_fetch_instrument(
    http: BitgetHttpClient,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    product_type: BitgetProductType,
    instrument_id: InstrumentId,
    ts_init: nautilus_core::UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    if let Some(instrument) = instruments.get_cloned(&instrument_id) {
        return Ok(instrument);
    }

    let fetched = http
        .request_instruments(product_type, ts_init)
        .await
        .context("fetch Bitget instruments from REST")?;
    let mut matched = None;

    for instrument in fetched {
        if instrument.id() == instrument_id {
            matched = Some(instrument.clone());
        }
        upsert_instrument(&instruments, instrument);
    }

    matched.ok_or_else(|| anyhow::anyhow!("Bitget instrument not found: {instrument_id}"))
}

pub(crate) fn send_data(sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>, data: Data) {
    if let Err(e) = sender.send(DataEvent::Data(data)) {
        log::error!("Failed to send Bitget data event: {e}");
    }
}

pub(crate) fn raw_symbol_for_instrument(instrument_id: InstrumentId) -> String {
    extract_raw_symbol(instrument_id.symbol.as_str()).to_string()
}

fn book_decimal_value_as_string(value: &BitgetDecimalValue, field: &str) -> anyhow::Result<String> {
    let value = value.as_decimal_str();
    anyhow::ensure!(
        !value.trim().is_empty(),
        "missing decimal value for {field}"
    );
    Ok(value)
}

fn instrument_id_from_ws_arg(arg: &BitgetWsArg) -> anyhow::Result<InstrumentId> {
    let raw_symbol = arg
        .symbol
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Bitget WebSocket data arg missing symbol"))?;
    let product_type = BitgetProductType::from_api_str(&arg.inst_type).ok_or_else(|| {
        anyhow::anyhow!("unsupported Bitget WebSocket instType: {}", arg.inst_type)
    })?;
    let symbol = match product_type {
        BitgetProductType::Spot => BitgetSymbol::spot(raw_symbol)?,
        BitgetProductType::UsdtFutures => BitgetSymbol::usdt_perp(raw_symbol)?,
    };
    Ok(symbol.to_instrument_id())
}

fn book_sync_decision(
    last_seq: Option<i64>,
    book: &BitgetBookData,
    action: Option<&str>,
) -> BookSyncDecision {
    if action.is_some_and(|value| value.eq_ignore_ascii_case("snapshot")) {
        return BookSyncDecision::Apply;
    }

    let Some(seq) = book.seq else {
        return BookSyncDecision::Apply;
    };

    let Some(last_seq) = last_seq else {
        return if book.pseq.is_some() {
            BookSyncDecision::Recover
        } else {
            BookSyncDecision::Apply
        };
    };

    if seq <= last_seq {
        return BookSyncDecision::Drop;
    }

    match book.pseq {
        Some(pseq) if pseq == last_seq => BookSyncDecision::Apply,
        Some(_) => BookSyncDecision::Recover,
        None => BookSyncDecision::Apply,
    }
}

fn record_book_sequence(
    book_sequences: &Arc<AtomicMap<InstrumentId, i64>>,
    instrument_id: InstrumentId,
    sequence: i64,
) {
    book_sequences.insert(instrument_id, sequence);
}

pub(crate) fn emit_instrument_status(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instrument_id: InstrumentId,
    action: MarketStatusAction,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) {
    let is_trading = Some(matches!(action, MarketStatusAction::Trading));
    let status = InstrumentStatus::new(
        instrument_id,
        action,
        ts_event,
        ts_init,
        None,
        None,
        is_trading,
        None,
        None,
    );

    if let Err(e) = sender.send(DataEvent::InstrumentStatus(status)) {
        log::error!("Failed to send Bitget instrument status: {e}");
    }
}

fn diff_and_emit_instrument_statuses(
    new_statuses: &AHashMap<InstrumentId, MarketStatusAction>,
    cached_statuses: &mut AHashMap<InstrumentId, MarketStatusAction>,
    subscriptions: &AHashSet<InstrumentId>,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) {
    for (instrument_id, &new_action) in new_statuses {
        let changed = cached_statuses
            .get(instrument_id)
            .is_none_or(|&prev| prev != new_action);

        if changed {
            cached_statuses.insert(*instrument_id, new_action);
            if subscriptions.contains(instrument_id) {
                emit_instrument_status(sender, *instrument_id, new_action, ts_event, ts_init);
            }
        }
    }

    let removed: Vec<InstrumentId> = cached_statuses
        .keys()
        .filter(|id| !new_statuses.contains_key(id))
        .copied()
        .collect();

    for instrument_id in removed {
        cached_statuses.remove(&instrument_id);
        if subscriptions.contains(&instrument_id) {
            emit_instrument_status(
                sender,
                instrument_id,
                MarketStatusAction::NotAvailableForTrading,
                ts_event,
                ts_init,
            );
        }
    }
}

pub(crate) async fn request_orderbook_snapshot_raw(
    http: &BitgetHttpClient,
    product_type: BitgetProductType,
    instrument: &InstrumentAny,
    depth: Option<u32>,
    ts_init: nautilus_core::UnixNanos,
) -> anyhow::Result<(
    BitgetOrderBookSnapshot,
    nautilus_model::data::OrderBookDeltas,
)> {
    let raw_symbol = raw_symbol_for_instrument(instrument.id());
    let snapshot = http
        .raw()
        .request_orderbook(product_type, &raw_symbol, depth)
        .await
        .context("request raw Bitget order book snapshot")?;
    let deltas = parse_orderbook_snapshot(&snapshot, instrument, Some(ts_init))?;
    Ok((snapshot, deltas))
}

pub(crate) fn store_spot_book_checksum_snapshot(
    product_type: BitgetProductType,
    instrument_id: InstrumentId,
    snapshot: &BitgetOrderBookSnapshot,
    book_checksum_states: &Arc<AtomicMap<InstrumentId, BitgetBookChecksumState>>,
) -> anyhow::Result<()> {
    if product_type != BitgetProductType::Spot {
        return Ok(());
    }

    let state = BitgetBookChecksumState::from_snapshot(snapshot)?;
    book_checksum_states.insert(instrument_id, state);
    Ok(())
}

fn apply_and_validate_spot_book_checksum(
    product_type: BitgetProductType,
    instrument_id: InstrumentId,
    book: &BitgetBookData,
    action: Option<&str>,
    book_checksum_states: &Arc<AtomicMap<InstrumentId, BitgetBookChecksumState>>,
) -> anyhow::Result<BookChecksumDecision> {
    if product_type != BitgetProductType::Spot {
        return Ok(BookChecksumDecision::Valid);
    }

    let remote_checksum = book.checksum;
    let snapshot = action.is_some_and(|value| value.eq_ignore_ascii_case("snapshot"));
    let mut state = if snapshot {
        BitgetBookChecksumState::default()
    } else if let Some(state) = book_checksum_states.get_cloned(&instrument_id) {
        state
    } else if remote_checksum.is_some() {
        return Ok(BookChecksumDecision::Recover);
    } else {
        return Ok(BookChecksumDecision::Valid);
    };

    state.apply_book(book, action)?;
    let local_checksum = remote_checksum.map(|_| state.checksum());
    book_checksum_states.insert(instrument_id, state);

    if let (Some(local), Some(remote)) = (local_checksum, remote_checksum)
        && !checksum_matches(local, remote)
    {
        log::warn!(
            "Bitget Spot book checksum mismatch for {instrument_id}: local={local}, remote={remote}"
        );
        return Ok(BookChecksumDecision::Recover);
    }

    Ok(BookChecksumDecision::Valid)
}

pub(crate) async fn recover_book_snapshot(
    http: BitgetHttpClient,
    product_type: BitgetProductType,
    instrument: &InstrumentAny,
    depth: Option<u32>,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    book_sequences: &Arc<AtomicMap<InstrumentId, i64>>,
    book_checksum_states: &Arc<AtomicMap<InstrumentId, BitgetBookChecksumState>>,
    book_depth10_states: &Arc<AtomicMap<InstrumentId, BitgetBookDepth10State>>,
    emit_deltas: bool,
    emit_depth10: bool,
    ts_init: nautilus_core::UnixNanos,
) -> anyhow::Result<()> {
    let (snapshot, deltas) =
        request_orderbook_snapshot_raw(&http, product_type, instrument, depth, ts_init)
            .await
            .context("recover Bitget order book snapshot")?;
    store_spot_book_checksum_snapshot(
        product_type,
        instrument.id(),
        &snapshot,
        book_checksum_states,
    )?;

    if let Ok(sequence) = i64::try_from(deltas.sequence) {
        record_book_sequence(book_sequences, instrument.id(), sequence);
    }

    if emit_depth10 {
        emit_depth10_snapshot(sender, instrument, &snapshot, book_depth10_states, ts_init)
            .context("emit recovered Bitget order book depth10 snapshot")?;
    }

    if emit_deltas {
        send_data(sender, Data::Deltas(OrderBookDeltas_API::new(deltas)));
    }

    Ok(())
}

pub(crate) async fn handle_bitget_ws_message(
    message: BitgetWsMessage,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    http: &BitgetHttpClient,
    product_type: BitgetProductType,
    instruments: &Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    bar_types: &Arc<AtomicMap<String, BarType>>,
    book_sequences: &Arc<AtomicMap<InstrumentId, i64>>,
    book_depths: &Arc<AtomicMap<InstrumentId, Option<u32>>>,
    book_checksum_states: &Arc<AtomicMap<InstrumentId, BitgetBookChecksumState>>,
    book_depth10_states: &Arc<AtomicMap<InstrumentId, BitgetBookDepth10State>>,
    book_subs: &Arc<AtomicMap<InstrumentId, AHashSet<&'static str>>>,
    ticker_subs: &Arc<AtomicMap<InstrumentId, AHashSet<&'static str>>>,
    clock: &AtomicTime,
) {
    let BitgetWsMessage::Data(event) = message else {
        return;
    };

    let Some(arg) = event.arg.as_ref() else {
        log::debug!("Skipping Bitget WebSocket data without topic arg");
        return;
    };

    let Ok(instrument_id) = instrument_id_from_ws_arg(arg) else {
        log::warn!("Skipping Bitget WebSocket data with invalid arg: {arg:?}");
        return;
    };

    let Some(instrument) = instruments.get_cloned(&instrument_id) else {
        log::warn!("Skipping Bitget WebSocket data for unknown instrument: {instrument_id}");
        return;
    };

    let ts_init = clock.get_time_ns();

    match arg.topic.as_str() {
        "publicTrade" | "trade" => {
            for value in event.data {
                match serde_json::from_value::<BitgetPublicTradeData>(value)
                    .map_err(anyhow::Error::from)
                    .map(Into::into)
                    .and_then(|trade| parse_market_trade(&trade, &instrument, Some(ts_init)))
                {
                    Ok(trade) => send_data(sender, Data::Trade(trade)),
                    Err(e) => log::error!("Failed to parse Bitget WebSocket trade: {e:?}"),
                }
            }
        }
        "books" => {
            let (wants_deltas, wants_depth10) =
                book_subs
                    .load()
                    .get(&instrument_id)
                    .map_or((false, false), |subs| {
                        (
                            subs.contains(BOOK_SUB_DELTAS),
                            subs.contains(BOOK_SUB_DEPTH10),
                        )
                    });

            if !(wants_deltas || wants_depth10) {
                return;
            }

            for value in event.data {
                let book = match serde_json::from_value::<BitgetBookData>(value) {
                    Ok(book) => book,
                    Err(e) => {
                        log::error!("Failed to decode Bitget WebSocket book update: {e:?}");
                        continue;
                    }
                };

                match book_sync_decision(
                    book_sequences.get_cloned(&instrument_id),
                    &book,
                    event.action.as_deref(),
                ) {
                    BookSyncDecision::Apply => {
                        let deltas = if wants_deltas {
                            match parse_ws_orderbook_deltas(
                                &book,
                                &instrument,
                                event.action.as_deref(),
                                Some(ts_init),
                            ) {
                                Ok(deltas) => Some(deltas),
                                Err(e) => {
                                    log::error!(
                                        "Failed to parse Bitget WebSocket book deltas: {e:?}"
                                    );
                                    continue;
                                }
                            }
                        } else {
                            None
                        };

                        match apply_and_validate_spot_book_checksum(
                            product_type,
                            instrument_id,
                            &book,
                            event.action.as_deref(),
                            book_checksum_states,
                        ) {
                            Ok(BookChecksumDecision::Valid) => {}
                            Ok(BookChecksumDecision::Recover) => {
                                let depth = recovery_book_depth(
                                    book_depths,
                                    instrument_id,
                                    wants_deltas,
                                    wants_depth10,
                                );
                                if let Err(e) = recover_book_snapshot(
                                    http.clone(),
                                    product_type,
                                    &instrument,
                                    depth,
                                    sender,
                                    book_sequences,
                                    book_checksum_states,
                                    book_depth10_states,
                                    wants_deltas,
                                    wants_depth10,
                                    ts_init,
                                )
                                .await
                                {
                                    log::error!(
                                        "Failed to recover Bitget order book snapshot for checksum mismatch on {instrument_id}: {e:?}"
                                    );
                                }
                                continue;
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed to validate Bitget Spot book checksum for {instrument_id}: {e:?}"
                                );
                                let depth = recovery_book_depth(
                                    book_depths,
                                    instrument_id,
                                    wants_deltas,
                                    wants_depth10,
                                );
                                if let Err(e) = recover_book_snapshot(
                                    http.clone(),
                                    product_type,
                                    &instrument,
                                    depth,
                                    sender,
                                    book_sequences,
                                    book_checksum_states,
                                    book_depth10_states,
                                    wants_deltas,
                                    wants_depth10,
                                    ts_init,
                                )
                                .await
                                {
                                    log::error!(
                                        "Failed to recover Bitget order book snapshot for checksum validation error on {instrument_id}: {e:?}"
                                    );
                                }
                                continue;
                            }
                        }

                        if wants_depth10 {
                            let mut state = if event
                                .action
                                .as_deref()
                                .is_some_and(|value| value.eq_ignore_ascii_case("snapshot"))
                            {
                                BitgetBookDepth10State::default()
                            } else {
                                book_depth10_states
                                    .get_cloned(&instrument_id)
                                    .unwrap_or_default()
                            };

                            let result = if event
                                .action
                                .as_deref()
                                .is_some_and(|value| value.eq_ignore_ascii_case("snapshot"))
                            {
                                state.apply_snapshot(&book)
                            } else {
                                state.apply_update(&book)
                            };

                            match result {
                                Ok(()) => {
                                    let ts_event = ws_book_ts_event(&book, ts_init);
                                    let sequence = ws_book_sequence(&book, ts_event);
                                    book_depth10_states.insert(instrument_id, state.clone());
                                    if let Err(e) = emit_depth10_from_state(
                                        sender,
                                        &state,
                                        &instrument,
                                        sequence,
                                        ts_event,
                                        ts_init,
                                    ) {
                                        log::error!(
                                            "Failed to emit Bitget order book depth10 for {instrument_id}: {e:?}"
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to update Bitget order book depth10 state for {instrument_id}: {e:?}"
                                    );
                                }
                            }
                        }

                        if let Some(seq) = book.seq {
                            record_book_sequence(book_sequences, instrument_id, seq);
                        }

                        if let Some(deltas) = deltas {
                            send_data(sender, Data::Deltas(OrderBookDeltas_API::new(deltas)));
                        }
                    }
                    BookSyncDecision::Recover => {
                        log::warn!(
                            "Bitget book sequence gap for {instrument_id}: last_seq={:?}, pseq={:?}, seq={:?}; recovering snapshot",
                            book_sequences.get_cloned(&instrument_id),
                            book.pseq,
                            book.seq,
                        );
                        let depth = recovery_book_depth(
                            book_depths,
                            instrument_id,
                            wants_deltas,
                            wants_depth10,
                        );
                        if let Err(e) = recover_book_snapshot(
                            http.clone(),
                            product_type,
                            &instrument,
                            depth,
                            sender,
                            book_sequences,
                            book_checksum_states,
                            book_depth10_states,
                            wants_deltas,
                            wants_depth10,
                            ts_init,
                        )
                        .await
                        {
                            log::error!(
                                "Failed to recover Bitget order book snapshot for {instrument_id}: {e:?}"
                            );
                        }
                    }
                    BookSyncDecision::Drop => {
                        log::debug!(
                            "Dropping stale Bitget book update for {instrument_id}: last_seq={:?}, seq={:?}",
                            book_sequences.get_cloned(&instrument_id),
                            book.seq,
                        );
                    }
                }
            }
        }
        "kline" => {
            let topic_key = arg.topic_key();
            let Some(bar_type) = bar_types.get_cloned(&topic_key) else {
                log::warn!("No Bitget bar type cached for WebSocket topic: {topic_key}");
                return;
            };

            for value in event.data {
                match serde_json::from_value::<BitgetCandle>(value)
                    .map_err(anyhow::Error::from)
                    .and_then(|candle| {
                        parse_candle_bar(&candle, &instrument, bar_type, true, Some(ts_init))
                    }) {
                    Ok(bar) => send_data(sender, Data::Bar(bar)),
                    Err(e) => log::error!("Failed to parse Bitget WebSocket candle: {e:?}"),
                }
            }
        }
        "ticker" => {
            let (wants_quote, wants_mark, wants_index, wants_funding) = ticker_subs
                .load()
                .get(&instrument_id)
                .map_or((false, false, false, false), |subs| {
                    (
                        subs.contains(TICKER_SUB_QUOTE),
                        subs.contains(TICKER_SUB_MARK),
                        subs.contains(TICKER_SUB_INDEX),
                        subs.contains(TICKER_SUB_FUNDING),
                    )
                });

            if !(wants_quote || wants_mark || wants_index || wants_funding) {
                return;
            }

            for value in event.data {
                let ticker = match serde_json::from_value::<BitgetTickerData>(value) {
                    Ok(ticker) => ticker,
                    Err(e) => {
                        log::error!("Failed to decode Bitget WebSocket ticker update: {e:?}");
                        continue;
                    }
                };
                let ticker_ts = event.ts.as_deref();

                if wants_quote {
                    match parse_ws_quote_tick(&ticker, &instrument, ticker_ts, Some(ts_init)) {
                        Ok(quote) => send_data(sender, Data::Quote(quote)),
                        Err(e) => log::debug!("Skipping Bitget quote ticker update: {e:?}"),
                    }
                }

                if wants_mark {
                    match parse_ws_mark_price(&ticker, &instrument, ticker_ts, Some(ts_init)) {
                        Ok(update) => send_data(sender, Data::MarkPriceUpdate(update)),
                        Err(e) => log::debug!("Skipping Bitget mark price ticker update: {e:?}"),
                    }
                }

                if wants_index {
                    match parse_ws_index_price(&ticker, &instrument, ticker_ts, Some(ts_init)) {
                        Ok(update) => send_data(sender, Data::IndexPriceUpdate(update)),
                        Err(e) => log::debug!("Skipping Bitget index price ticker update: {e:?}"),
                    }
                }

                if wants_funding {
                    match parse_ws_funding_rate(&ticker, &instrument, ticker_ts, Some(ts_init)) {
                        Ok(update) => {
                            if let Err(e) = sender.send(DataEvent::FundingRate(update)) {
                                log::error!("Failed to send Bitget funding rate event: {e}");
                            }
                        }
                        Err(e) => {
                            log::debug!("Skipping Bitget funding rate ticker update: {e:?}");
                        }
                    }
                }
            }
        }
        topic => {
            log::debug!("Ignoring unsupported Bitget WebSocket data topic: {topic}");
        }
    }
}

/// Live market data client for Bitget.
#[derive(Debug)]
pub struct BitgetDataClient {
    client_id: ClientId,
    config: BitgetDataClientConfig,
    http_client: BitgetHttpClient,
    ws_client: BitgetWebSocketClient,
    ws_task: Option<tokio::task::JoinHandle<()>>,
    is_connected: AtomicBool,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    bar_types: Arc<AtomicMap<String, BarType>>,
    book_sequences: Arc<AtomicMap<InstrumentId, i64>>,
    book_depths: Arc<AtomicMap<InstrumentId, Option<u32>>>,
    book_checksum_states: Arc<AtomicMap<InstrumentId, BitgetBookChecksumState>>,
    book_depth10_states: Arc<AtomicMap<InstrumentId, BitgetBookDepth10State>>,
    book_subs: Arc<AtomicMap<InstrumentId, AHashSet<&'static str>>>,
    ticker_subs: Arc<AtomicMap<InstrumentId, AHashSet<&'static str>>>,
    instrument_status_subs: Arc<AtomicSet<InstrumentId>>,
    status_cache: Arc<AtomicMap<InstrumentId, MarketStatusAction>>,
    tasks: Vec<JoinHandle<()>>,
    cancellation_token: CancellationToken,
    clock: &'static AtomicTime,
}

impl BitgetDataClient {
    /// Creates a new [`BitgetDataClient`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    pub fn new(client_id: ClientId, config: BitgetDataClientConfig) -> anyhow::Result<Self> {
        let http_client = BitgetHttpClient::new_with_env_for_environment(
            config.environment,
            config.api_key.clone(),
            config.api_secret.clone(),
            config.api_passphrase.clone(),
            Some(config.http_base_url()),
            config.http_timeout_secs,
            config.proxy_url.clone(),
        )?;
        let data_sender = try_get_data_event_sender().unwrap_or_else(|| {
            log::warn!(
                "BitgetDataClient created before live runner initialized a data sender; events will be dropped"
            );
            let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel();
            sender
        });
        let ws_client = BitgetWebSocketClient::new_public(
            config.product_type,
            config.environment,
            Some(config.ws_public_url()),
            config.heartbeat_interval_secs,
            config.transport_backend,
            config.proxy_url.clone(),
        );

        Ok(Self {
            client_id,
            config,
            http_client,
            ws_client,
            ws_task: None,
            is_connected: AtomicBool::new(false),
            data_sender,
            instruments: Arc::new(AtomicMap::new()),
            bar_types: Arc::new(AtomicMap::new()),
            book_sequences: Arc::new(AtomicMap::new()),
            book_depths: Arc::new(AtomicMap::new()),
            book_checksum_states: Arc::new(AtomicMap::new()),
            book_depth10_states: Arc::new(AtomicMap::new()),
            book_subs: Arc::new(AtomicMap::new()),
            ticker_subs: Arc::new(AtomicMap::new()),
            instrument_status_subs: Arc::new(AtomicSet::new()),
            status_cache: Arc::new(AtomicMap::new()),
            tasks: Vec::new(),
            cancellation_token: CancellationToken::new(),
            clock: get_atomic_clock_realtime(),
        })
    }

    /// Returns this client's configured product type.
    #[must_use]
    pub const fn product_type(&self) -> crate::common::enums::BitgetProductType {
        self.config.product_type
    }

    fn configured_product_type_for(
        &self,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<BitgetProductType> {
        let inferred = BitgetProductType::from_symbol(instrument_id.symbol.as_str());
        anyhow::ensure!(
            inferred == self.config.product_type,
            "Bitget data client is configured for {:?}, cannot request {}",
            self.config.product_type,
            instrument_id,
        );
        Ok(inferred)
    }

    fn abort_ws_task(&mut self) {
        if let Some(handle) = self.ws_task.take() {
            handle.abort();
        }
    }

    fn abort_tasks(&mut self) {
        for handle in self.tasks.drain(..) {
            handle.abort();
        }
    }

    fn start_ws_dispatch(&mut self) -> anyhow::Result<()> {
        if self.ws_task.is_some() {
            return Ok(());
        }

        let mut event_rx = self
            .ws_client
            .take_event_receiver()
            .ok_or_else(|| anyhow::anyhow!("Bitget WebSocket event receiver was already taken"))?;
        let sender = self.data_sender.clone();
        let http = self.http_client.clone();
        let product_type = self.config.product_type;
        let instruments = Arc::clone(&self.instruments);
        let bar_types = Arc::clone(&self.bar_types);
        let book_sequences = Arc::clone(&self.book_sequences);
        let book_depths = Arc::clone(&self.book_depths);
        let book_checksum_states = Arc::clone(&self.book_checksum_states);
        let book_depth10_states = Arc::clone(&self.book_depth10_states);
        let book_subs = Arc::clone(&self.book_subs);
        let ticker_subs = Arc::clone(&self.ticker_subs);
        let clock = self.clock;

        self.ws_task = Some(get_runtime().spawn(async move {
            while let Some(message) = event_rx.recv().await {
                handle_bitget_ws_message(
                    message,
                    &sender,
                    &http,
                    product_type,
                    &instruments,
                    &bar_types,
                    &book_sequences,
                    &book_depths,
                    &book_checksum_states,
                    &book_depth10_states,
                    &book_subs,
                    &ticker_subs,
                    clock,
                )
                .await;
            }
            log::debug!("Bitget data WebSocket dispatch task exited");
        }));

        Ok(())
    }

    fn ensure_futures_ticker_subscription(
        &self,
        instrument_id: InstrumentId,
        data_kind: &str,
    ) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(instrument_id)?;
        anyhow::ensure!(
            product_type == BitgetProductType::UsdtFutures,
            "Bitget {data_kind} subscriptions are only available for USDT-FUTURES instruments"
        );

        if let Some(instrument) = self.instruments.get_cloned(&instrument_id) {
            anyhow::ensure!(
                matches!(instrument, InstrumentAny::CryptoPerpetual(_)),
                "Bitget {data_kind} subscriptions are only available for perpetual instruments"
            );
        }

        Ok(())
    }

    fn ensure_ticker_subscription(
        &self,
        instrument_id: InstrumentId,
        data_kind: &str,
    ) -> anyhow::Result<()> {
        self.configured_product_type_for(instrument_id)
            .with_context(|| format!("Bitget {data_kind} subscription"))?;
        Ok(())
    }

    fn add_ticker_sub(&self, instrument_id: InstrumentId, sub: &'static str) -> bool {
        let mut should_subscribe = false;
        self.ticker_subs.rcu(|m| {
            let entry = m.entry(instrument_id).or_default();
            should_subscribe = entry.is_empty();
            entry.insert(sub);
        });
        should_subscribe
    }

    fn remove_ticker_sub(&self, instrument_id: InstrumentId, sub: &'static str) -> bool {
        let mut should_unsubscribe = false;
        self.ticker_subs.rcu(|m| {
            if let Some(entry) = m.get_mut(&instrument_id) {
                entry.remove(sub);
                should_unsubscribe = entry.is_empty();
                if should_unsubscribe {
                    m.remove(&instrument_id);
                }
            }
        });
        should_unsubscribe
    }

    fn add_book_sub(&self, instrument_id: InstrumentId, sub: &'static str) -> bool {
        let mut should_subscribe = false;
        self.book_subs.rcu(|m| {
            let entry = m.entry(instrument_id).or_default();
            should_subscribe = entry.is_empty();
            entry.insert(sub);
        });
        should_subscribe
    }

    fn remove_book_sub(&self, instrument_id: InstrumentId, sub: &'static str) -> bool {
        let mut should_unsubscribe = false;
        self.book_subs.rcu(|m| {
            if let Some(entry) = m.get_mut(&instrument_id) {
                entry.remove(sub);
                should_unsubscribe = entry.is_empty();
                if should_unsubscribe {
                    m.remove(&instrument_id);
                }
            }
        });
        should_unsubscribe
    }

    fn subscribe_ticker_derived(
        &mut self,
        instrument_id: InstrumentId,
        sub: &'static str,
        data_kind: &'static str,
    ) -> anyhow::Result<()> {
        self.ensure_futures_ticker_subscription(instrument_id, data_kind)?;

        if self.add_ticker_sub(instrument_id, sub) {
            let raw_symbol = raw_symbol_for_instrument(instrument_id);
            let ws = self.ws_client.clone();
            self.spawn_ws(
                async move {
                    ws.subscribe_ticker(raw_symbol)
                        .await
                        .with_context(|| format!("ticker subscription for {data_kind}"))
                },
                data_kind,
            );
        }

        Ok(())
    }

    fn subscribe_ticker_quote(&mut self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        self.ensure_ticker_subscription(instrument_id, "quote")?;

        if self.add_ticker_sub(instrument_id, TICKER_SUB_QUOTE) {
            let raw_symbol = raw_symbol_for_instrument(instrument_id);
            let ws = self.ws_client.clone();
            self.spawn_ws(
                async move {
                    ws.subscribe_ticker(raw_symbol)
                        .await
                        .context("ticker subscription for quote")
                },
                "quote",
            );
        }

        Ok(())
    }

    fn unsubscribe_ticker_derived(
        &mut self,
        instrument_id: InstrumentId,
        sub: &'static str,
        data_kind: &'static str,
    ) -> anyhow::Result<()> {
        self.ensure_futures_ticker_subscription(instrument_id, data_kind)?;

        if self.remove_ticker_sub(instrument_id, sub) {
            let raw_symbol = raw_symbol_for_instrument(instrument_id);
            let ws = self.ws_client.clone();
            self.spawn_ws(
                async move {
                    ws.unsubscribe_ticker(raw_symbol)
                        .await
                        .with_context(|| format!("ticker unsubscription for {data_kind}"))
                },
                data_kind,
            );
        }

        Ok(())
    }

    fn unsubscribe_ticker_quote(&mut self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        self.ensure_ticker_subscription(instrument_id, "quote")?;

        if self.remove_ticker_sub(instrument_id, TICKER_SUB_QUOTE) {
            let raw_symbol = raw_symbol_for_instrument(instrument_id);
            let ws = self.ws_client.clone();
            self.spawn_ws(
                async move {
                    ws.unsubscribe_ticker(raw_symbol)
                        .await
                        .context("ticker unsubscription for quote")
                },
                "quote",
            );
        }

        Ok(())
    }

    fn spawn_ws<F>(&self, future: F, action: &'static str)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        get_runtime().spawn(async move {
            if let Err(e) = future.await {
                log::error!("Bitget WebSocket {action} failed: {e:?}");
            }
        });
    }

    fn spawn_instrument_status_polling(&mut self, interval_secs: u64) {
        let http = self.http_client.clone();
        let product_type = self.config.product_type;
        let status_cache = Arc::clone(&self.status_cache);
        let status_subs = Arc::clone(&self.instrument_status_subs);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let cancel = self.cancellation_token.clone();

        let handle = get_runtime().spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            interval.tick().await;

            loop {
                tokio::select! {
                    () = cancel.cancelled() => {
                        log::debug!("Bitget instrument status polling task cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if status_subs.load().is_empty() {
                            continue;
                        }

                        let ts_init = clock.get_time_ns();
                        match http.request_instrument_statuses(product_type).await {
                            Ok(new_statuses) => {
                                let mut cache = (**status_cache.load()).clone();
                                let subs = status_subs.load();
                                diff_and_emit_instrument_statuses(
                                    &new_statuses,
                                    &mut cache,
                                    &subs,
                                    &sender,
                                    ts_init,
                                    ts_init,
                                );
                                status_cache.store(cache);
                            }
                            Err(e) => {
                                log::warn!("Bitget instrument status poll failed: {e:?}");
                            }
                        }
                    }
                }
            }
        });

        self.tasks.push(handle);
    }
}

#[async_trait(?Send)]
impl DataClient for BitgetDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*BITGET_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.abort_tasks();
        self.abort_ws_task();
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.abort_tasks();
        self.abort_ws_task();
        self.book_sequences.store(Default::default());
        self.book_depths.store(Default::default());
        self.book_checksum_states.store(Default::default());
        self.book_depth10_states.store(Default::default());
        self.book_subs.store(Default::default());
        self.ticker_subs.store(Default::default());
        self.instrument_status_subs.store(Default::default());
        self.status_cache.store(Default::default());
        self.cancellation_token = CancellationToken::new();
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!("Bitget only supports L2_MBP order book deltas");
        }

        let product_type = self.configured_product_type_for(cmd.instrument_id)?;
        let instrument_id = cmd.instrument_id;
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let depth = cmd.depth.map(|depth| depth.get() as u32);
        self.book_depths.insert(instrument_id, depth);
        let should_subscribe = self.add_book_sub(instrument_id, BOOK_SUB_DELTAS);
        let http = self.http_client.clone();
        let instruments = Arc::clone(&self.instruments);
        let book_sequences = Arc::clone(&self.book_sequences);
        let book_checksum_states = Arc::clone(&self.book_checksum_states);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let ws = self.ws_client.clone();

        self.spawn_ws(
            async move {
                let ts_init = clock.get_time_ns();
                let instrument = get_or_fetch_instrument(
                    http.clone(),
                    instruments,
                    product_type,
                    instrument_id,
                    ts_init,
                )
                .await?;
                let (raw_snapshot, snapshot) = request_orderbook_snapshot_raw(
                    &http,
                    product_type,
                    &instrument,
                    depth,
                    ts_init,
                )
                .await
                .context("REST order book snapshot")?;
                store_spot_book_checksum_snapshot(
                    product_type,
                    instrument_id,
                    &raw_snapshot,
                    &book_checksum_states,
                )?;
                if let Ok(sequence) = i64::try_from(snapshot.sequence) {
                    record_book_sequence(&book_sequences, instrument_id, sequence);
                }
                send_data(&sender, Data::Deltas(OrderBookDeltas_API::new(snapshot)));
                if should_subscribe {
                    ws.subscribe_books(raw_symbol)
                        .await
                        .context("books subscription")?;
                }
                Ok(())
            },
            "books subscription",
        );

        Ok(())
    }

    fn subscribe_book_depth10(&mut self, cmd: SubscribeBookDepth10) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!("Bitget only supports L2_MBP order book depth10");
        }

        let product_type = self.configured_product_type_for(cmd.instrument_id)?;
        let instrument_id = cmd.instrument_id;
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let should_subscribe = self.add_book_sub(instrument_id, BOOK_SUB_DEPTH10);
        let http = self.http_client.clone();
        let instruments = Arc::clone(&self.instruments);
        let book_sequences = Arc::clone(&self.book_sequences);
        let book_checksum_states = Arc::clone(&self.book_checksum_states);
        let book_depth10_states = Arc::clone(&self.book_depth10_states);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let ws = self.ws_client.clone();

        self.spawn_ws(
            async move {
                let ts_init = clock.get_time_ns();
                let instrument = get_or_fetch_instrument(
                    http.clone(),
                    instruments,
                    product_type,
                    instrument_id,
                    ts_init,
                )
                .await?;
                let (raw_snapshot, snapshot) = request_orderbook_snapshot_raw(
                    &http,
                    product_type,
                    &instrument,
                    Some(BITGET_DEPTH10_DEPTH),
                    ts_init,
                )
                .await
                .context("REST order book depth10 snapshot")?;
                store_spot_book_checksum_snapshot(
                    product_type,
                    instrument_id,
                    &raw_snapshot,
                    &book_checksum_states,
                )?;
                if let Ok(sequence) = i64::try_from(snapshot.sequence) {
                    record_book_sequence(&book_sequences, instrument_id, sequence);
                }
                emit_depth10_snapshot(
                    &sender,
                    &instrument,
                    &raw_snapshot,
                    &book_depth10_states,
                    ts_init,
                )?;
                if should_subscribe {
                    ws.subscribe_books(raw_symbol)
                        .await
                        .context("books subscription for depth10")?;
                }
                Ok(())
            },
            "books depth10 subscription",
        );

        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        self.configured_product_type_for(cmd.instrument_id)?;
        let raw_symbol = raw_symbol_for_instrument(cmd.instrument_id);
        let ws = self.ws_client.clone();

        self.spawn_ws(
            async move {
                ws.subscribe_trades(raw_symbol)
                    .await
                    .context("trade subscription")
            },
            "trade subscription",
        );

        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        self.subscribe_ticker_quote(cmd.instrument_id)
    }

    fn subscribe_mark_prices(&mut self, cmd: SubscribeMarkPrices) -> anyhow::Result<()> {
        self.subscribe_ticker_derived(cmd.instrument_id, TICKER_SUB_MARK, "mark price")
    }

    fn subscribe_index_prices(&mut self, cmd: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscribe_ticker_derived(cmd.instrument_id, TICKER_SUB_INDEX, "index price")
    }

    fn subscribe_funding_rates(&mut self, cmd: SubscribeFundingRates) -> anyhow::Result<()> {
        self.subscribe_ticker_derived(cmd.instrument_id, TICKER_SUB_FUNDING, "funding rate")
    }

    fn subscribe_instrument_status(
        &mut self,
        cmd: SubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        self.configured_product_type_for(cmd.instrument_id)?;
        let instrument_id = cmd.instrument_id;
        self.instrument_status_subs.insert(instrument_id);

        if let Some(action) = self.status_cache.get_cloned(&instrument_id) {
            let ts_init = self.clock.get_time_ns();
            emit_instrument_status(&self.data_sender, instrument_id, action, ts_init, ts_init);
        }

        Ok(())
    }

    fn subscribe_instrument_close(&mut self, cmd: SubscribeInstrumentClose) -> anyhow::Result<()> {
        self.configured_product_type_for(cmd.instrument_id)?;
        log::warn!(
            "Bitget instrument close subscriptions are not supported: v3 UTA instruments expose status but not an instrument close price"
        );
        Ok(())
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> anyhow::Result<()> {
        let bar_type = cmd.bar_type;
        let instrument_id = bar_type.instrument_id();
        let product_type = self.configured_product_type_for(instrument_id)?;
        let spec = bar_type.spec();
        let interval = bar_spec_to_bitget_interval_for_product(
            product_type,
            spec.aggregation,
            spec.step.get() as u64,
        )?;
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let topic_key = BitgetWsArg::kline(product_type, raw_symbol.clone(), interval).topic_key();
        self.bar_types.insert(topic_key, bar_type);

        let ws = self.ws_client.clone();
        self.spawn_ws(
            async move {
                ws.subscribe_candles(raw_symbol, interval)
                    .await
                    .context("candle subscription")
            },
            "candle subscription",
        );

        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.configured_product_type_for(cmd.instrument_id)?;
        let should_unsubscribe = self.remove_book_sub(cmd.instrument_id, BOOK_SUB_DELTAS);
        if should_unsubscribe {
            self.book_sequences.remove(&cmd.instrument_id);
            self.book_checksum_states.remove(&cmd.instrument_id);
            self.book_depth10_states.remove(&cmd.instrument_id);
        }
        self.book_depths.remove(&cmd.instrument_id);
        let raw_symbol = raw_symbol_for_instrument(cmd.instrument_id);
        let ws = self.ws_client.clone();

        self.spawn_ws(
            async move {
                if should_unsubscribe {
                    ws.unsubscribe_books(raw_symbol)
                        .await
                        .context("books unsubscription")?;
                }
                Ok(())
            },
            "books unsubscription",
        );

        Ok(())
    }

    fn unsubscribe_book_depth10(&mut self, cmd: &UnsubscribeBookDepth10) -> anyhow::Result<()> {
        self.configured_product_type_for(cmd.instrument_id)?;
        let should_unsubscribe = self.remove_book_sub(cmd.instrument_id, BOOK_SUB_DEPTH10);
        if should_unsubscribe {
            self.book_sequences.remove(&cmd.instrument_id);
            self.book_checksum_states.remove(&cmd.instrument_id);
            self.book_depth10_states.remove(&cmd.instrument_id);
            self.book_depths.remove(&cmd.instrument_id);
        } else {
            self.book_depth10_states.remove(&cmd.instrument_id);
        }
        let raw_symbol = raw_symbol_for_instrument(cmd.instrument_id);
        let ws = self.ws_client.clone();

        self.spawn_ws(
            async move {
                if should_unsubscribe {
                    ws.unsubscribe_books(raw_symbol)
                        .await
                        .context("books depth10 unsubscription")?;
                }
                Ok(())
            },
            "books depth10 unsubscription",
        );

        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.configured_product_type_for(cmd.instrument_id)?;
        let raw_symbol = raw_symbol_for_instrument(cmd.instrument_id);
        let ws = self.ws_client.clone();

        self.spawn_ws(
            async move {
                ws.unsubscribe_trades(raw_symbol)
                    .await
                    .context("trade unsubscription")
            },
            "trade unsubscription",
        );

        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.unsubscribe_ticker_quote(cmd.instrument_id)
    }

    fn unsubscribe_mark_prices(&mut self, cmd: &UnsubscribeMarkPrices) -> anyhow::Result<()> {
        self.unsubscribe_ticker_derived(cmd.instrument_id, TICKER_SUB_MARK, "mark price")
    }

    fn unsubscribe_index_prices(&mut self, cmd: &UnsubscribeIndexPrices) -> anyhow::Result<()> {
        self.unsubscribe_ticker_derived(cmd.instrument_id, TICKER_SUB_INDEX, "index price")
    }

    fn unsubscribe_funding_rates(&mut self, cmd: &UnsubscribeFundingRates) -> anyhow::Result<()> {
        self.unsubscribe_ticker_derived(cmd.instrument_id, TICKER_SUB_FUNDING, "funding rate")
    }

    fn unsubscribe_instrument_status(
        &mut self,
        cmd: &UnsubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        self.configured_product_type_for(cmd.instrument_id)?;
        self.instrument_status_subs.remove(&cmd.instrument_id);
        Ok(())
    }

    fn unsubscribe_instrument_close(
        &mut self,
        cmd: &UnsubscribeInstrumentClose,
    ) -> anyhow::Result<()> {
        self.configured_product_type_for(cmd.instrument_id)?;
        Ok(())
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        let bar_type = cmd.bar_type;
        let instrument_id = bar_type.instrument_id();
        let product_type = self.configured_product_type_for(instrument_id)?;
        let spec = bar_type.spec();
        let interval = bar_spec_to_bitget_interval_for_product(
            product_type,
            spec.aggregation,
            spec.step.get() as u64,
        )?;
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let topic_key = BitgetWsArg::kline(product_type, raw_symbol.clone(), interval).topic_key();
        self.bar_types.remove(&topic_key);

        let ws = self.ws_client.clone();
        self.spawn_ws(
            async move {
                ws.unsubscribe_candles(raw_symbol, interval)
                    .await
                    .context("candle unsubscription")
            },
            "candle unsubscription",
        );

        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.cancellation_token.is_cancelled() {
            self.cancellation_token = CancellationToken::new();
        }

        let ts_init = self.clock.get_time_ns();
        let instruments = self
            .http_client
            .request_instruments(self.config.product_type, ts_init)
            .await?;

        match self
            .http_client
            .request_instrument_statuses(self.config.product_type)
            .await
        {
            Ok(statuses) => self.status_cache.store(statuses),
            Err(e) => log::warn!("Failed to seed Bitget instrument status cache: {e:?}"),
        }

        for instrument in instruments {
            upsert_instrument(&self.instruments, instrument.clone());
            if let Err(e) = self.data_sender.send(DataEvent::Instrument(instrument)) {
                log::error!("Failed to send Bitget instrument event: {e}");
            }
        }

        self.ws_client
            .connect()
            .await
            .context("connect Bitget public WebSocket")?;
        self.start_ws_dispatch()?;

        if let Some(interval_secs) = self.config.instrument_poll_interval_secs
            && interval_secs > 0
        {
            self.spawn_instrument_status_polling(interval_secs);
        }

        self.is_connected.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        if let Err(e) = self.ws_client.disconnect().await {
            log::warn!("Error disconnecting Bitget WebSocket: {e:?}");
        }
        self.abort_tasks();
        self.abort_ws_task();
        self.bar_types.store(Default::default());
        self.book_sequences.store(Default::default());
        self.book_depths.store(Default::default());
        self.book_checksum_states.store(Default::default());
        self.book_depth10_states.store(Default::default());
        self.book_subs.store(Default::default());
        self.ticker_subs.store(Default::default());
        self.instrument_status_subs.store(Default::default());
        self.status_cache.store(Default::default());
        self.cancellation_token = CancellationToken::new();
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments_cache = self.instruments.clone();
        let product_type = self.config.product_type;
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let params = request.params;
        let clock = self.clock;

        get_runtime().spawn(async move {
            match http
                .request_instruments(product_type, clock.get_time_ns())
                .await
            {
                Ok(instruments) => {
                    for instrument in &instruments {
                        upsert_instrument(&instruments_cache, instrument.clone());
                    }

                    let response = DataResponse::Instruments(InstrumentsResponse::new(
                        request_id,
                        client_id,
                        *BITGET_VENUE,
                        instruments,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send Bitget instruments response: {e}");
                    }
                }
                Err(e) => log::error!("Bitget instruments request failed: {e:?}"),
            }
        });

        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(request.instrument_id)?;
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments = self.instruments.clone();
        let instrument_id = request.instrument_id;
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let params = request.params;
        let clock = self.clock;

        get_runtime().spawn(async move {
            match get_or_fetch_instrument(
                http,
                instruments,
                product_type,
                instrument_id,
                clock.get_time_ns(),
            )
            .await
            {
                Ok(instrument) => {
                    let response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
                        request_id,
                        client_id,
                        instrument.id(),
                        instrument,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    )));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send Bitget instrument response: {e}");
                    }
                }
                Err(e) => {
                    log::error!("Bitget instrument request failed for {instrument_id}: {e:?}")
                }
            }
        });

        Ok(())
    }

    fn request_book_snapshot(&self, request: RequestBookSnapshot) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(request.instrument_id)?;
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments = self.instruments.clone();
        let instrument_id = request.instrument_id;
        let depth = request.depth.map(|depth| depth.get() as u32);
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let params = request.params;
        let clock = self.clock;

        get_runtime().spawn(async move {
            let ts_init = clock.get_time_ns();
            let result = async {
                let instrument = get_or_fetch_instrument(
                    http.clone(),
                    instruments,
                    product_type,
                    instrument_id,
                    ts_init,
                )
                .await?;
                let deltas = http
                    .request_orderbook_snapshot(product_type, &instrument, depth, ts_init)
                    .await?;
                let mut book = OrderBook::new(instrument_id, BookType::L2_MBP);
                book.apply_deltas(&deltas)
                    .context("apply Bitget order book snapshot deltas")?;
                Ok::<_, anyhow::Error>(book)
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

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send Bitget book snapshot response: {e}");
                    }
                }
                Err(e) => {
                    log::error!("Bitget book snapshot request failed for {instrument_id}: {e:?}");
                }
            }
        });

        Ok(())
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(request.instrument_id)?;
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments = self.instruments.clone();
        let instrument_id = request.instrument_id;
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(|limit| limit.get() as u32);
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);
        let params = request.params;
        let clock = self.clock;

        get_runtime().spawn(async move {
            let ts_init = clock.get_time_ns();
            let result = async {
                let instrument = get_or_fetch_instrument(
                    http.clone(),
                    instruments,
                    product_type,
                    instrument_id,
                    ts_init,
                )
                .await?;
                http.request_trades(product_type, &instrument, start, end, limit)
                    .await
            }
            .await;

            match result {
                Ok(trades) => {
                    let response = DataResponse::Trades(TradesResponse::new(
                        request_id,
                        client_id,
                        instrument_id,
                        trades,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send Bitget trades response: {e}");
                    }
                }
                Err(e) => log::error!("Bitget trades request failed for {instrument_id}: {e:?}"),
            }
        });

        Ok(())
    }

    fn request_funding_rates(&self, request: RequestFundingRates) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(request.instrument_id)?;
        anyhow::ensure!(
            product_type == BitgetProductType::UsdtFutures,
            "Bitget funding rates are only available for USDT-FUTURES instruments"
        );

        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments = self.instruments.clone();
        let instrument_id = request.instrument_id;
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(|limit| limit.get() as u32);
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);
        let params = request.params;
        let clock = self.clock;

        get_runtime().spawn(async move {
            let ts_init = clock.get_time_ns();
            let result = async {
                let instrument = get_or_fetch_instrument(
                    http.clone(),
                    instruments,
                    product_type,
                    instrument_id,
                    ts_init,
                )
                .await?;
                http.request_funding_rates(product_type, &instrument, start, end, limit)
                    .await
            }
            .await;

            match result {
                Ok(funding_rates) => {
                    let response = DataResponse::FundingRates(FundingRatesResponse::new(
                        request_id,
                        client_id,
                        instrument_id,
                        funding_rates,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send Bitget funding rates response: {e}");
                    }
                }
                Err(e) => {
                    log::error!("Bitget funding rates request failed for {instrument_id}: {e:?}");
                }
            }
        });

        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let bar_type = request.bar_type;
        let instrument_id = bar_type.instrument_id();
        let product_type = self.configured_product_type_for(instrument_id)?;
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments = self.instruments.clone();
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(|limit| limit.get() as u32);
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);
        let params = request.params;
        let clock = self.clock;

        get_runtime().spawn(async move {
            let ts_init = clock.get_time_ns();
            let result = async {
                let instrument = get_or_fetch_instrument(
                    http.clone(),
                    instruments,
                    product_type,
                    instrument_id,
                    ts_init,
                )
                .await?;
                http.request_bars(product_type, &instrument, bar_type, start, end, limit, true)
                    .await
            }
            .await;

            match result {
                Ok(bars) => {
                    let response = DataResponse::Bars(BarsResponse::new(
                        request_id,
                        client_id,
                        bar_type,
                        bars,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send Bitget bars response: {e}");
                    }
                }
                Err(e) => log::error!("Bitget bars request failed for {bar_type}: {e:?}"),
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::SocketAddr,
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        Json, Router,
        extract::{
            Query, State,
            ws::{Message, WebSocket, WebSocketUpgrade},
        },
        response::{IntoResponse, Response},
        routing::get,
    };
    use futures_util::{SinkExt, StreamExt};
    use nautilus_common::testing::wait_until_async;
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::enums::BookAction;
    use nautilus_network::websocket::{TEXT_PING, TEXT_PONG};
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        common::parse::{parse_spot_instrument, parse_usdt_perp_instrument},
        http::models::{BitgetMixContract, BitgetSpotSymbol},
    };

    fn book(seq: Option<i64>, pseq: Option<i64>) -> BitgetBookData {
        BitgetBookData {
            seq,
            pseq,
            ts: Some("1700000000000".to_string()),
            ..Default::default()
        }
    }

    fn client(product_type: BitgetProductType) -> BitgetDataClient {
        BitgetDataClient::new(
            ClientId::from("BITGET"),
            BitgetDataClientConfig {
                product_type,
                base_url_http: Some("http://localhost".to_string()),
                base_url_ws_public: Some("ws://localhost/public".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn btcusdt_perp() -> InstrumentAny {
        parse_usdt_perp_instrument(
            &BitgetMixContract {
                symbol: "BTCUSDT".to_string(),
                base_coin: "BTC".to_string(),
                quote_coin: "USDT".to_string(),
                product_type: Some("USDT-FUTURES".to_string()),
                symbol_type: Some("perpetual".to_string()),
                contract_type: None,
                margin_coin: Some("USDT".to_string()),
                maker_fee_rate: Some("0.0002".to_string()),
                taker_fee_rate: Some("0.0006".to_string()),
                min_trade_num: Some("0.001".to_string()),
                min_trade_usdt: Some("5".to_string()),
                max_order_qty: Some("1000".to_string()),
                size_multiplier: Some("0.001".to_string()),
                price_place: Some("1".to_string()),
                volume_place: Some("3".to_string()),
                price_end_step: Some("1".to_string()),
                max_lever: Some("125".to_string()),
                min_lever: Some("1".to_string()),
                fund_interval: Some("8".to_string()),
                symbol_status: Some("normal".to_string()),
            },
            nautilus_core::UnixNanos::new(1),
            nautilus_core::UnixNanos::new(1),
        )
        .unwrap()
    }

    fn btcusdt_spot() -> InstrumentAny {
        parse_spot_instrument(
            &BitgetSpotSymbol {
                symbol: "BTCUSDT".to_string(),
                base_coin: "BTC".to_string(),
                quote_coin: "USDT".to_string(),
                min_trade_amount: Some("0.00001".to_string()),
                max_trade_amount: Some("1000".to_string()),
                min_trade_usdt: Some("5".to_string()),
                maker_fee_rate: Some("0.001".to_string()),
                taker_fee_rate: Some("0.001".to_string()),
                price_precision: Some("2".to_string()),
                quantity_precision: Some("6".to_string()),
                quote_precision: Some("2".to_string()),
                status: Some("online".to_string()),
            },
            nautilus_core::UnixNanos::new(1),
            nautilus_core::UnixNanos::new(1),
        )
        .unwrap()
    }

    #[derive(Clone, Default)]
    struct BookFixtureState {
        requests: Arc<AtomicUsize>,
    }

    #[derive(Clone, Default)]
    struct PublicWsFixtureState {
        received_messages: Arc<tokio::sync::Mutex<Vec<Value>>>,
        send_gap_after_books_subscribe: Arc<AtomicBool>,
        send_checksum_mismatch_after_books_subscribe: Arc<AtomicBool>,
    }

    impl PublicWsFixtureState {
        async fn received_subscribe_args(&self) -> Vec<Value> {
            self.received_messages
                .lock()
                .await
                .iter()
                .filter(|value| value.get("op").and_then(Value::as_str) == Some("subscribe"))
                .flat_map(|value| {
                    value
                        .get("args")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                })
                .collect()
        }
    }

    async fn start_book_fixture_server(state: BookFixtureState) -> SocketAddr {
        let router = Router::new()
            .route("/api/v3/market/orderbook", get(handle_spot_book))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router.into_make_service())
                .await
                .unwrap();
        });

        addr
    }

    async fn handle_spot_book(
        State(state): State<BookFixtureState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Response {
        state.requests.fetch_add(1, Ordering::Relaxed);
        assert_eq!(query.get("category").map(String::as_str), Some("SPOT"));
        assert_eq!(query.get("symbol").map(String::as_str), Some("BTCUSDT"));

        Json(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": {
                "b": [["100.00", "1.000000"]],
                "a": [["101.00", "2.000000"]],
                "ts": "1700000000000",
                "seq": "42"
            }
        }))
        .into_response()
    }

    async fn handle_public_ws_upgrade(
        ws: WebSocketUpgrade,
        State(state): State<PublicWsFixtureState>,
    ) -> Response {
        ws.on_upgrade(move |socket| handle_public_ws_socket(socket, state))
    }

    async fn handle_public_ws_socket(socket: WebSocket, state: PublicWsFixtureState) {
        let (mut sink, mut stream) = socket.split();

        while let Some(message) = stream.next().await {
            let Ok(message) = message else { break };

            match message {
                Message::Text(text) if text.as_str() == TEXT_PING => {
                    let _ = sink.send(Message::Text(TEXT_PONG.to_string().into())).await;
                }
                Message::Text(text) => {
                    let payload: Value = match serde_json::from_str(&text) {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    state.received_messages.lock().await.push(payload.clone());

                    if payload.get("op").and_then(Value::as_str) != Some("subscribe") {
                        continue;
                    }

                    let args = payload
                        .get("args")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();

                    for arg in args {
                        let ack = json!({
                            "event": "subscribe",
                            "arg": arg,
                        });
                        let _ = sink.send(Message::Text(ack.to_string().into())).await;

                        let is_books = arg.get("topic").and_then(Value::as_str) == Some("books");
                        if is_books
                            && state
                                .send_gap_after_books_subscribe
                                .swap(false, Ordering::SeqCst)
                        {
                            let update = json!({
                                "action": "update",
                                "arg": arg,
                                "data": [{
                                    "b": [["100.00", "2.000000"]],
                                    "a": [],
                                    "seq": 44,
                                    "pseq": 43,
                                    "ts": "1700000000001"
                                }]
                            });
                            let _ = sink.send(Message::Text(update.to_string().into())).await;
                        } else if is_books
                            && state
                                .send_checksum_mismatch_after_books_subscribe
                                .swap(false, Ordering::SeqCst)
                        {
                            let update = json!({
                                "action": "update",
                                "arg": arg,
                                "data": [{
                                    "b": [["100.00", "2.000000"]],
                                    "a": [],
                                    "seq": 43,
                                    "pseq": 42,
                                    "checksum": 1,
                                    "ts": "1700000000001"
                                }]
                            });
                            let _ = sink.send(Message::Text(update.to_string().into())).await;
                        }
                    }
                }
                Message::Ping(payload) => {
                    let _ = sink.send(Message::Pong(payload)).await;
                }
                Message::Close(_) => break,
                Message::Binary(_) | Message::Pong(_) => {}
            }
        }
    }

    async fn start_public_ws_fixture_server(state: PublicWsFixtureState) -> SocketAddr {
        let router = Router::new()
            .route("/ws", get(handle_public_ws_upgrade))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, router.into_make_service())
                .await
                .unwrap();
        });

        wait_until_async(
            || async { tokio::net::TcpStream::connect(addr).await.is_ok() },
            Duration::from_secs(5),
        )
        .await;

        addr
    }

    #[rstest]
    fn bitget_book_checksum_string_interleaves_bids_and_asks() {
        let snapshot = BitgetOrderBookSnapshot {
            bids: vec![
                vec![
                    BitgetDecimalValue::String("100.00".to_string()),
                    BitgetDecimalValue::String("1.000000".to_string()),
                ],
                vec![
                    BitgetDecimalValue::String("99.00".to_string()),
                    BitgetDecimalValue::String("3.000000".to_string()),
                ],
            ],
            asks: vec![vec![
                BitgetDecimalValue::String("101.00".to_string()),
                BitgetDecimalValue::String("2.000000".to_string()),
            ]],
            ..Default::default()
        };
        let state = BitgetBookChecksumState::from_snapshot(&snapshot).unwrap();

        assert_eq!(
            state.checksum_string(),
            "100.00:1.000000:101.00:2.000000:99.00:3.000000"
        );
    }

    #[rstest]
    fn bitget_book_crc32_uses_ieee_polynomial() {
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF4_3926);
    }

    #[rstest]
    fn book_sync_decision_allows_snapshots_without_local_sequence() {
        assert_eq!(
            book_sync_decision(None, &book(Some(10), None), Some("snapshot")),
            BookSyncDecision::Apply,
        );
    }

    #[rstest]
    fn book_sync_decision_applies_contiguous_update() {
        assert_eq!(
            book_sync_decision(Some(10), &book(Some(11), Some(10)), Some("update")),
            BookSyncDecision::Apply,
        );
    }

    #[rstest]
    fn book_sync_decision_recovers_gap() {
        assert_eq!(
            book_sync_decision(Some(10), &book(Some(13), Some(12)), Some("update")),
            BookSyncDecision::Recover,
        );
    }

    #[rstest]
    fn book_sync_decision_recovers_when_update_has_pseq_but_no_local_sequence() {
        assert_eq!(
            book_sync_decision(None, &book(Some(11), Some(10)), Some("update")),
            BookSyncDecision::Recover,
        );
    }

    #[rstest]
    fn book_sync_decision_drops_stale_update() {
        assert_eq!(
            book_sync_decision(Some(10), &book(Some(10), Some(9)), Some("update")),
            BookSyncDecision::Drop,
        );
    }

    #[rstest]
    fn ticker_sub_state_shares_underlying_exchange_subscription() {
        let client = client(BitgetProductType::UsdtFutures);
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");

        assert!(client.add_ticker_sub(instrument_id, TICKER_SUB_MARK));
        assert!(!client.add_ticker_sub(instrument_id, TICKER_SUB_INDEX));
        assert!(!client.remove_ticker_sub(instrument_id, TICKER_SUB_MARK));
        assert!(client.ticker_subs.contains_key(&instrument_id));
        assert!(client.remove_ticker_sub(instrument_id, TICKER_SUB_INDEX));
        assert!(!client.ticker_subs.contains_key(&instrument_id));
    }

    #[rstest]
    fn book_sub_state_shares_underlying_exchange_subscription() {
        let client = client(BitgetProductType::Spot);
        let instrument_id = InstrumentId::from("BTCUSDT.BITGET");

        assert!(client.add_book_sub(instrument_id, BOOK_SUB_DELTAS));
        assert!(!client.add_book_sub(instrument_id, BOOK_SUB_DEPTH10));
        assert!(!client.remove_book_sub(instrument_id, BOOK_SUB_DELTAS));
        assert!(client.book_subs.contains_key(&instrument_id));
        assert!(client.remove_book_sub(instrument_id, BOOK_SUB_DEPTH10));
        assert!(!client.book_subs.contains_key(&instrument_id));
    }

    #[rstest]
    fn instrument_status_diff_emits_only_subscribed_changes() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let subscribed_id = InstrumentId::from("BTCUSDT.BITGET");
        let unsubscribed_id = InstrumentId::from("ETHUSDT.BITGET");
        let mut cached = AHashMap::new();
        cached.insert(subscribed_id, MarketStatusAction::Trading);
        cached.insert(unsubscribed_id, MarketStatusAction::Trading);
        let mut new_statuses = AHashMap::new();
        new_statuses.insert(subscribed_id, MarketStatusAction::Halt);
        new_statuses.insert(unsubscribed_id, MarketStatusAction::Halt);
        let subscriptions = [subscribed_id].into_iter().collect();

        diff_and_emit_instrument_statuses(
            &new_statuses,
            &mut cached,
            &subscriptions,
            &sender,
            UnixNanos::new(10),
            UnixNanos::new(11),
        );

        match receiver.try_recv().unwrap() {
            DataEvent::InstrumentStatus(status) => {
                assert_eq!(status.instrument_id, subscribed_id);
                assert_eq!(status.action, MarketStatusAction::Halt);
                assert_eq!(status.is_trading, Some(false));
            }
            event => panic!("expected instrument status, got {event:?}"),
        }
        assert!(receiver.try_recv().is_err());
        assert_eq!(cached.get(&subscribed_id), Some(&MarketStatusAction::Halt));
        assert_eq!(
            cached.get(&unsubscribed_id),
            Some(&MarketStatusAction::Halt)
        );
    }

    #[rstest]
    fn futures_ticker_subscription_rejects_spot_product() {
        let client = client(BitgetProductType::Spot);
        let err = client
            .ensure_futures_ticker_subscription(InstrumentId::from("BTCUSDT.BITGET"), "mark price")
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("only available for USDT-FUTURES instruments")
        );
    }

    #[tokio::test]
    async fn spot_book_checksum_mismatch_recovers_snapshot() {
        let fixture = BookFixtureState::default();
        let addr = start_book_fixture_server(fixture.clone()).await;
        let http = BitgetHttpClient::new_with_env(
            None,
            None,
            None,
            Some(format!("http://{addr}")),
            1,
            None,
        )
        .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let instruments = Arc::new(AtomicMap::new());
        let bar_types = Arc::new(AtomicMap::new());
        let book_sequences = Arc::new(AtomicMap::new());
        let book_depths = Arc::new(AtomicMap::new());
        let book_checksum_states = Arc::new(AtomicMap::new());
        let book_depth10_states = Arc::new(AtomicMap::new());
        let book_subs = Arc::new(AtomicMap::new());
        let ticker_subs = Arc::new(AtomicMap::new());
        let instrument = btcusdt_spot();
        let instrument_id = instrument.id();

        instruments.insert(instrument_id, instrument);
        book_sequences.insert(instrument_id, 10);
        book_depths.insert(instrument_id, Some(50));
        book_subs.insert(instrument_id, [BOOK_SUB_DELTAS].into_iter().collect());

        let message = BitgetWsMessage::Data(crate::websocket::messages::BitgetWsEvent {
            event: None,
            action: Some("update".to_string()),
            arg: Some(BitgetWsArg::new(
                BitgetProductType::Spot,
                "books",
                Some("BTCUSDT".to_string()),
            )),
            data: vec![json!({
                "b": [["100.00", "2.000000"]],
                "a": [],
                "seq": 11,
                "pseq": 10,
                "checksum": 1,
                "ts": "1700000000000"
            })],
            ts: None,
            code: None,
            msg: None,
        });

        handle_bitget_ws_message(
            message,
            &sender,
            &http,
            BitgetProductType::Spot,
            &instruments,
            &bar_types,
            &book_sequences,
            &book_depths,
            &book_checksum_states,
            &book_depth10_states,
            &book_subs,
            &ticker_subs,
            get_atomic_clock_realtime(),
        )
        .await;

        assert_eq!(fixture.requests.load(Ordering::Relaxed), 1);
        assert_eq!(book_sequences.get_cloned(&instrument_id), Some(42));
        assert!(book_checksum_states.contains_key(&instrument_id));

        match receiver.try_recv().unwrap() {
            DataEvent::Data(Data::Deltas(deltas)) => {
                assert_eq!(deltas.sequence, 42);
                assert_eq!(deltas.deltas[0].action, BookAction::Clear);
            }
            event => panic!("expected recovered snapshot deltas, got {event:?}"),
        }
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn books_ws_dispatch_emits_depth10_when_requested() {
        let http = BitgetHttpClient::new_with_env(
            None,
            None,
            None,
            Some("http://localhost".to_string()),
            1,
            None,
        )
        .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let instruments = Arc::new(AtomicMap::new());
        let bar_types = Arc::new(AtomicMap::new());
        let book_sequences = Arc::new(AtomicMap::new());
        let book_depths = Arc::new(AtomicMap::new());
        let book_checksum_states = Arc::new(AtomicMap::new());
        let book_depth10_states = Arc::new(AtomicMap::new());
        let book_subs = Arc::new(AtomicMap::new());
        let ticker_subs = Arc::new(AtomicMap::new());
        let instrument = btcusdt_spot();
        let instrument_id = instrument.id();

        instruments.insert(instrument_id, instrument);
        book_subs.insert(instrument_id, [BOOK_SUB_DEPTH10].into_iter().collect());

        let message = BitgetWsMessage::Data(crate::websocket::messages::BitgetWsEvent {
            event: None,
            action: Some("snapshot".to_string()),
            arg: Some(BitgetWsArg::new(
                BitgetProductType::Spot,
                "books",
                Some("BTCUSDT".to_string()),
            )),
            data: vec![json!({
                "b": [["100.00", "1.000000"], ["99.00", "3.000000"]],
                "a": [["101.00", "2.000000"]],
                "seq": 42,
                "pseq": 0,
                "ts": "1700000000000"
            })],
            ts: None,
            code: None,
            msg: None,
        });

        handle_bitget_ws_message(
            message,
            &sender,
            &http,
            BitgetProductType::Spot,
            &instruments,
            &bar_types,
            &book_sequences,
            &book_depths,
            &book_checksum_states,
            &book_depth10_states,
            &book_subs,
            &ticker_subs,
            get_atomic_clock_realtime(),
        )
        .await;

        match receiver.try_recv().unwrap() {
            DataEvent::Data(Data::Depth10(depth)) => {
                assert_eq!(depth.instrument_id, instrument_id);
                assert_eq!(depth.sequence, 42);
                assert_eq!(depth.ts_event, UnixNanos::from_millis(1_700_000_000_000));
                assert_eq!(depth.bids[0].price.to_string(), "100.00");
                assert_eq!(depth.bids[0].size.to_string(), "1.000000");
                assert_eq!(depth.asks[0].price.to_string(), "101.00");
                assert_eq!(depth.asks[0].size.to_string(), "2.000000");
                assert_eq!(depth.bid_counts[0], 1);
                assert_eq!(depth.ask_counts[0], 1);
            }
            event => panic!("expected order book depth10, got {event:?}"),
        }
        assert!(book_depth10_states.contains_key(&instrument_id));
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn public_ws_fixture_book_gap_recovers_via_http_snapshot() {
        let book_fixture = BookFixtureState::default();
        let http_addr = start_book_fixture_server(book_fixture.clone()).await;
        let ws_fixture = PublicWsFixtureState::default();
        ws_fixture
            .send_gap_after_books_subscribe
            .store(true, Ordering::SeqCst);
        let ws_addr = start_public_ws_fixture_server(ws_fixture.clone()).await;
        let mut client = BitgetDataClient::new(
            ClientId::from("BITGET"),
            BitgetDataClientConfig {
                product_type: BitgetProductType::Spot,
                base_url_http: Some(format!("http://{http_addr}")),
                base_url_ws_public: Some(format!("ws://{ws_addr}/ws")),
                ..Default::default()
            },
        )
        .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.data_sender = sender;

        let instrument = btcusdt_spot();
        let instrument_id = instrument.id();
        client.instruments.insert(instrument_id, instrument);
        client.ws_client.connect().await.unwrap();
        client.start_ws_dispatch().unwrap();

        client
            .subscribe_book_deltas(SubscribeBookDeltas::new(
                instrument_id,
                BookType::L2_MBP,
                Some(ClientId::from("BITGET")),
                None,
                UUID4::new(),
                UnixNanos::new(1_700_000_000_000_000_000),
                NonZeroUsize::new(50),
                false,
                None,
                None,
            ))
            .unwrap();

        wait_until_async(
            || {
                let book_fixture = book_fixture.clone();
                async move { book_fixture.requests.load(Ordering::Relaxed) >= 2 }
            },
            Duration::from_secs(10),
        )
        .await;

        let mut sequences = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline && sequences.len() < 2 {
            match tokio::time::timeout(Duration::from_millis(250), receiver.recv()).await {
                Ok(Some(DataEvent::Data(Data::Deltas(deltas)))) => {
                    sequences.push(deltas.sequence);
                }
                Ok(Some(_)) | Err(_) => {}
                Ok(None) => break,
            }
        }

        assert_eq!(sequences, vec![42, 42]);
        assert_eq!(client.book_sequences.get_cloned(&instrument_id), Some(42));
        assert!(client.book_checksum_states.contains_key(&instrument_id));
        assert!(
            ws_fixture
                .received_subscribe_args()
                .await
                .iter()
                .any(|arg| arg.get("topic").and_then(Value::as_str) == Some("books"))
        );

        client.ws_client.disconnect().await.unwrap();
        client.abort_ws_task();
    }

    #[tokio::test]
    async fn public_ws_fixture_book_checksum_mismatch_recovers_via_http_snapshot() {
        let book_fixture = BookFixtureState::default();
        let http_addr = start_book_fixture_server(book_fixture.clone()).await;
        let ws_fixture = PublicWsFixtureState::default();
        ws_fixture
            .send_checksum_mismatch_after_books_subscribe
            .store(true, Ordering::SeqCst);
        let ws_addr = start_public_ws_fixture_server(ws_fixture.clone()).await;
        let mut client = BitgetDataClient::new(
            ClientId::from("BITGET"),
            BitgetDataClientConfig {
                product_type: BitgetProductType::Spot,
                base_url_http: Some(format!("http://{http_addr}")),
                base_url_ws_public: Some(format!("ws://{ws_addr}/ws")),
                ..Default::default()
            },
        )
        .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        client.data_sender = sender;

        let instrument = btcusdt_spot();
        let instrument_id = instrument.id();
        client.instruments.insert(instrument_id, instrument);
        client.ws_client.connect().await.unwrap();
        client.start_ws_dispatch().unwrap();

        client
            .subscribe_book_deltas(SubscribeBookDeltas::new(
                instrument_id,
                BookType::L2_MBP,
                Some(ClientId::from("BITGET")),
                None,
                UUID4::new(),
                UnixNanos::new(1_700_000_000_000_000_000),
                NonZeroUsize::new(50),
                false,
                None,
                None,
            ))
            .unwrap();

        wait_until_async(
            || {
                let book_fixture = book_fixture.clone();
                async move { book_fixture.requests.load(Ordering::Relaxed) >= 2 }
            },
            Duration::from_secs(10),
        )
        .await;

        let mut sequences = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline && sequences.len() < 2 {
            match tokio::time::timeout(Duration::from_millis(250), receiver.recv()).await {
                Ok(Some(DataEvent::Data(Data::Deltas(deltas)))) => {
                    sequences.push(deltas.sequence);
                }
                Ok(Some(_)) | Err(_) => {}
                Ok(None) => break,
            }
        }

        assert_eq!(sequences, vec![42, 42]);
        assert_eq!(client.book_sequences.get_cloned(&instrument_id), Some(42));
        assert!(client.book_checksum_states.contains_key(&instrument_id));
        assert!(
            ws_fixture
                .received_subscribe_args()
                .await
                .iter()
                .any(|arg| arg.get("topic").and_then(Value::as_str) == Some("books"))
        );

        client.ws_client.disconnect().await.unwrap();
        client.abort_ws_task();
    }

    #[tokio::test]
    async fn ticker_ws_dispatch_emits_requested_quote_mark_index_and_funding_updates() {
        let http = BitgetHttpClient::new_with_env(
            None,
            None,
            None,
            Some("http://localhost".to_string()),
            1,
            None,
        )
        .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let instruments = Arc::new(AtomicMap::new());
        let bar_types = Arc::new(AtomicMap::new());
        let book_sequences = Arc::new(AtomicMap::new());
        let book_depths = Arc::new(AtomicMap::new());
        let book_checksum_states = Arc::new(AtomicMap::new());
        let book_depth10_states = Arc::new(AtomicMap::new());
        let book_subs = Arc::new(AtomicMap::new());
        let ticker_subs = Arc::new(AtomicMap::new());
        let instrument = btcusdt_perp();
        let instrument_id = instrument.id();

        instruments.insert(instrument_id, instrument);
        ticker_subs.insert(
            instrument_id,
            [
                TICKER_SUB_QUOTE,
                TICKER_SUB_MARK,
                TICKER_SUB_INDEX,
                TICKER_SUB_FUNDING,
            ]
            .into_iter()
            .collect(),
        );

        let message = BitgetWsMessage::Data(crate::websocket::messages::BitgetWsEvent {
            event: None,
            action: Some("snapshot".to_string()),
            arg: Some(BitgetWsArg::new(
                BitgetProductType::UsdtFutures,
                "ticker",
                Some("BTCUSDT".to_string()),
            )),
            data: vec![json!({
                "symbol": "BTCUSDT",
                "lastPrice": "100.1",
                "bid1Price": "100.0",
                "bid1Size": "1.5",
                "ask1Price": "100.4",
                "ask1Size": "2.5",
                "markPrice": "100.2",
                "indexPrice": "100.3",
                "fundingRate": "0.0001",
                "nextFundingTime": "1700003600000"
            })],
            ts: Some("1700000000000".to_string()),
            code: None,
            msg: None,
        });

        handle_bitget_ws_message(
            message,
            &sender,
            &http,
            BitgetProductType::UsdtFutures,
            &instruments,
            &bar_types,
            &book_sequences,
            &book_depths,
            &book_checksum_states,
            &book_depth10_states,
            &book_subs,
            &ticker_subs,
            get_atomic_clock_realtime(),
        )
        .await;

        match receiver.try_recv().unwrap() {
            DataEvent::Data(Data::Quote(quote)) => {
                assert_eq!(quote.instrument_id, instrument_id);
                assert_eq!(quote.bid_price.to_string(), "100.0");
                assert_eq!(quote.ask_price.to_string(), "100.4");
                assert_eq!(quote.bid_size.to_string(), "1.500");
                assert_eq!(quote.ask_size.to_string(), "2.500");
            }
            event => panic!("expected quote tick, got {event:?}"),
        }

        match receiver.try_recv().unwrap() {
            DataEvent::Data(Data::MarkPriceUpdate(update)) => {
                assert_eq!(update.instrument_id, instrument_id);
                assert_eq!(update.value.to_string(), "100.2");
            }
            event => panic!("expected mark price update, got {event:?}"),
        }

        match receiver.try_recv().unwrap() {
            DataEvent::Data(Data::IndexPriceUpdate(update)) => {
                assert_eq!(update.instrument_id, instrument_id);
                assert_eq!(update.value.to_string(), "100.3");
            }
            event => panic!("expected index price update, got {event:?}"),
        }

        match receiver.try_recv().unwrap() {
            DataEvent::FundingRate(update) => {
                assert_eq!(update.instrument_id, instrument_id);
                assert_eq!(update.rate.to_string(), "0.0001");
            }
            event => panic!("expected funding rate update, got {event:?}"),
        }

        assert!(receiver.try_recv().is_err());
    }
}
