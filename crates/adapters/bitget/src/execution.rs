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

//! Live execution client implementation for the Bitget adapter.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use nautilus_common::{
    clients::ExecutionClient,
    live::{runner::get_exec_event_sender, runtime::get_runtime},
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder,
    },
};
use nautilus_core::time::{AtomicTime, get_atomic_clock_realtime};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{OmsType, OrderSide},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, Venue, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    reports::{FillReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance},
};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    common::{
        consts::BITGET_VENUE,
        enums::BitgetProductType,
        order::{
            map_batch_cancel_orders, map_cancel_all_orders, map_cancel_order, map_modify_order,
            map_submit_order,
        },
        parse::{
            parse_fill_report, parse_mix_account_state, parse_order_status_report,
            parse_position_status_report, parse_spot_account_state,
        },
        symbol::{BitgetSymbol, extract_raw_symbol},
    },
    config::BitgetExecClientConfig,
    http::{
        client::BitgetHttpClient,
        error::BitgetHttpError,
        models::{
            BitgetCancelBatchResponse, BitgetCancelBatchResult, BitgetFill, BitgetMixAccount,
            BitgetMixPosition, BitgetOrderStatus, BitgetSpotAsset, BitgetUtaAccount,
        },
    },
    websocket::{
        client::BitgetWebSocketClient,
        messages::{
            BitgetWsAccountData, BitgetWsFillData, BitgetWsMessage, BitgetWsOrderData,
            BitgetWsPositionData,
        },
    },
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct BitgetExecutionWsDispatchSummary {
    orders: usize,
    fills: usize,
    accounts: usize,
    positions: usize,
    reconnects: usize,
    errors: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct BitgetReconnectReconciliationSummary {
    account_states: usize,
    order_reports: usize,
    fill_reports: usize,
    position_reports: usize,
    errors: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitgetCommandFailureKind {
    StructuredVenueRejection,
    LocalValidation,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BitgetCancelRejectionContext {
    strategy_id: StrategyId,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
    client_order_id_raw: String,
    venue_order_id_raw: Option<String>,
}

impl BitgetCancelRejectionContext {
    fn new(
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
    ) -> Self {
        Self {
            strategy_id,
            instrument_id,
            client_order_id,
            venue_order_id,
            client_order_id_raw: client_order_id.to_string(),
            venue_order_id_raw: venue_order_id.map(|id| id.to_string()),
        }
    }

    fn from_cancel(cancel: &CancelOrder) -> Self {
        Self::new(
            cancel.strategy_id,
            cancel.instrument_id,
            cancel.client_order_id,
            cancel.venue_order_id,
        )
    }
}

#[derive(Debug, Clone)]
struct BitgetExecutionWsDispatchContext {
    product_type: BitgetProductType,
    account_id: AccountId,
    clock: &'static AtomicTime,
    emitter: ExecutionEventEmitter,
    http_client: BitgetHttpClient,
    reconnect_reconciliation_lookback_mins: Option<u64>,
    reconnect_reconciliation_in_flight: Arc<AtomicBool>,
    instruments_by_id: HashMap<InstrumentId, InstrumentAny>,
    instrument_ids_by_raw_symbol: HashMap<String, InstrumentId>,
}

impl BitgetExecutionWsDispatchContext {
    fn new(
        product_type: BitgetProductType,
        account_id: AccountId,
        clock: &'static AtomicTime,
        emitter: ExecutionEventEmitter,
        http_client: BitgetHttpClient,
        reconnect_reconciliation_lookback_mins: Option<u64>,
        instruments: Vec<InstrumentAny>,
    ) -> Self {
        let mut instruments_by_id = HashMap::with_capacity(instruments.len());
        let mut instrument_ids_by_raw_symbol = HashMap::with_capacity(instruments.len());

        for instrument in instruments {
            let instrument_id = instrument.id();
            instrument_ids_by_raw_symbol.insert(instrument.raw_symbol().to_string(), instrument_id);
            instrument_ids_by_raw_symbol.insert(
                extract_raw_symbol(instrument_id.symbol.as_str()).to_string(),
                instrument_id,
            );
            instruments_by_id.insert(instrument_id, instrument);
        }

        Self {
            product_type,
            account_id,
            clock,
            emitter,
            http_client,
            reconnect_reconciliation_lookback_mins,
            reconnect_reconciliation_in_flight: Arc::new(AtomicBool::new(false)),
            instruments_by_id,
            instrument_ids_by_raw_symbol,
        }
    }

    fn instrument_for_raw_symbol(
        &self,
        raw_symbol: Option<&str>,
    ) -> anyhow::Result<&InstrumentAny> {
        let raw_symbol = raw_symbol
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("Bitget private WebSocket row missing symbol")?;
        let instrument_id = self
            .instrument_ids_by_raw_symbol
            .get(raw_symbol)
            .with_context(|| format!("Bitget private WebSocket symbol not cached: {raw_symbol}"))?;

        self.instruments_by_id
            .get(instrument_id)
            .with_context(|| format!("Bitget instrument not cached: {instrument_id}"))
    }
}

/// Live execution client for Bitget.
#[derive(Debug)]
pub struct BitgetExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: BitgetExecClientConfig,
    emitter: ExecutionEventEmitter,
    http_client: BitgetHttpClient,
    ws_client: BitgetWebSocketClient,
    ws_task: Option<tokio::task::JoinHandle<()>>,
    is_connected: AtomicBool,
}

impl BitgetExecutionClient {
    /// Creates a new [`BitgetExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    pub fn new(core: ExecutionClientCore, config: BitgetExecClientConfig) -> anyhow::Result<Self> {
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );
        let http_client = BitgetHttpClient::new_with_env_for_environment(
            config.environment,
            config.api_key.clone(),
            config.api_secret.clone(),
            config.api_passphrase.clone(),
            Some(config.http_base_url()),
            config.http_timeout_secs,
            config.proxy_url.clone(),
        )?;
        let ws_client = BitgetWebSocketClient::new_private(
            config.product_type,
            config.environment,
            config.api_key.clone(),
            config.api_secret.clone(),
            config.api_passphrase.clone(),
            Some(config.ws_private_url()),
            config.heartbeat_interval_secs,
            config.transport_backend,
            config.proxy_url.clone(),
        );

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
            "Bitget execution client is configured for {:?}, cannot route {}",
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

    fn resolve_report_instrument_id(
        &self,
        instrument_id: Option<InstrumentId>,
        client_order_id: Option<ClientOrderId>,
    ) -> Option<InstrumentId> {
        instrument_id.or_else(|| {
            client_order_id.and_then(|id| {
                self.core
                    .get_order(&id)
                    .map(|order| order.instrument_id())
                    .map_err(|e| {
                        log::warn!("Bitget could not infer report instrument from {id}: {e:?}");
                        e
                    })
                    .ok()
            })
        })
    }

    fn cached_instrument(&self, instrument_id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        self.core
            .cache()
            .instrument(&instrument_id)
            .cloned()
            .with_context(|| format!("Bitget instrument not cached: {instrument_id}"))
    }

    fn instrument_id_for_order_status(
        &self,
        order: &BitgetOrderStatus,
        fallback_instrument_id: Option<InstrumentId>,
    ) -> anyhow::Result<InstrumentId> {
        if let Some(instrument_id) = fallback_instrument_id {
            return Ok(instrument_id);
        }

        let raw_symbol = order
            .symbol
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .context("Bitget order status missing symbol")?;
        let symbol = match self.config.product_type {
            BitgetProductType::Spot => BitgetSymbol::spot(raw_symbol)?,
            BitgetProductType::UsdtFutures => BitgetSymbol::usdt_perp(raw_symbol)?,
        };
        Ok(symbol.to_instrument_id())
    }

    fn instrument_id_for_fill(
        &self,
        fill: &BitgetFill,
        fallback_instrument_id: Option<InstrumentId>,
    ) -> anyhow::Result<InstrumentId> {
        if let Some(instrument_id) = fallback_instrument_id {
            return Ok(instrument_id);
        }

        let raw_symbol = fill
            .symbol
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .context("Bitget fill missing symbol")?;
        let symbol = match self.config.product_type {
            BitgetProductType::Spot => BitgetSymbol::spot(raw_symbol)?,
            BitgetProductType::UsdtFutures => BitgetSymbol::usdt_perp(raw_symbol)?,
        };
        Ok(symbol.to_instrument_id())
    }

    fn instrument_id_for_position(
        &self,
        position: &BitgetMixPosition,
        fallback_instrument_id: Option<InstrumentId>,
    ) -> anyhow::Result<InstrumentId> {
        if let Some(instrument_id) = fallback_instrument_id {
            return Ok(instrument_id);
        }

        let raw_symbol = position
            .symbol
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .context("Bitget position missing symbol")?;
        let symbol = BitgetSymbol::usdt_perp(raw_symbol)?;
        Ok(symbol.to_instrument_id())
    }

    fn start_ws_dispatch(&mut self) -> anyhow::Result<()> {
        if self.ws_task.is_some() {
            return Ok(());
        }

        let mut event_rx = self.ws_client.take_event_receiver().ok_or_else(|| {
            anyhow::anyhow!("Bitget private WebSocket receiver was already taken")
        })?;
        let venue = *BITGET_VENUE;
        let instruments = self
            .core
            .cache()
            .instruments(&venue, None)
            .into_iter()
            .cloned()
            .collect();
        let dispatch_ctx = BitgetExecutionWsDispatchContext::new(
            self.config.product_type,
            self.core.account_id,
            self.clock,
            self.emitter.clone(),
            self.http_client.clone(),
            self.config.reconnect_reconciliation_lookback_mins,
            instruments,
        );

        self.ws_task = Some(get_runtime().spawn(async move {
            while let Some(message) = event_rx.recv().await {
                let _ =
                    handle_bitget_execution_ws_message_with_context(message, Some(&dispatch_ctx));
            }
            log::debug!("Bitget execution WebSocket dispatch task exited");
        }));

        Ok(())
    }
}

fn classify_bitget_http_failure(error: &BitgetHttpError) -> BitgetCommandFailureKind {
    match error {
        BitgetHttpError::BitgetError { .. } => BitgetCommandFailureKind::StructuredVenueRejection,
        BitgetHttpError::MissingCredentials
        | BitgetHttpError::ValidationError(_)
        | BitgetHttpError::JsonError(_) => BitgetCommandFailureKind::LocalValidation,
        BitgetHttpError::NetworkError(_) | BitgetHttpError::UnexpectedStatus { .. } => {
            BitgetCommandFailureKind::Ambiguous
        }
    }
}

fn checked_client_order_id(raw: &str) -> Option<ClientOrderId> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    match ClientOrderId::new_checked(raw) {
        Ok(id) => Some(id),
        Err(e) => {
            log::warn!("Ignoring invalid Bitget client order ID in cancel response: {e:?}");
            None
        }
    }
}

fn checked_venue_order_id(raw: &str) -> Option<VenueOrderId> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    match VenueOrderId::new_checked(raw) {
        Ok(id) => Some(id),
        Err(e) => {
            log::warn!("Ignoring invalid Bitget venue order ID in cancel response: {e:?}");
            None
        }
    }
}

fn emit_cancel_rejected_for_context(
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    context: &BitgetCancelRejectionContext,
    reason: &str,
) {
    emitter.emit_order_cancel_rejected_event(
        context.strategy_id,
        context.instrument_id,
        context.client_order_id,
        context.venue_order_id,
        reason,
        clock.get_time_ns(),
    );
}

fn emit_cancel_rejected_for_contexts(
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    contexts: &[BitgetCancelRejectionContext],
    reason: &str,
) {
    for context in contexts {
        emit_cancel_rejected_for_context(emitter, clock, context, reason);
    }
}

fn cancel_failure_context<'a>(
    failure: &BitgetCancelBatchResult,
    contexts: &'a [BitgetCancelRejectionContext],
) -> Option<&'a BitgetCancelRejectionContext> {
    let client_oid = failure
        .client_oid
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let order_id = failure
        .order_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    contexts.iter().find(|context| {
        client_oid.is_some_and(|client_oid| context.client_order_id_raw == client_oid)
            || order_id.is_some_and(|order_id| {
                context
                    .venue_order_id_raw
                    .as_deref()
                    .is_some_and(|venue_order_id| venue_order_id == order_id)
            })
    })
}

fn emit_cancel_failure_list_rejections(
    response: &BitgetCancelBatchResponse,
    contexts: &[BitgetCancelRejectionContext],
    fallback_strategy_id: StrategyId,
    fallback_instrument_id: InstrumentId,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
) {
    for failure in &response.failure_list {
        let reason = failure
            .error_msg
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("Bitget cancel order rejected");
        let reason = format!("Bitget cancel order rejected: {reason}");

        if let Some(context) = cancel_failure_context(failure, contexts) {
            emit_cancel_rejected_for_context(emitter, clock, context, &reason);
            continue;
        }

        let Some(client_order_id) = failure
            .client_oid
            .as_deref()
            .and_then(checked_client_order_id)
        else {
            log::warn!(
                "Bitget cancel failure could not be mapped to a Nautilus order: {failure:?}"
            );
            continue;
        };
        let venue_order_id = failure.order_id.as_deref().and_then(checked_venue_order_id);

        emitter.emit_order_cancel_rejected_event(
            fallback_strategy_id,
            fallback_instrument_id,
            client_order_id,
            venue_order_id,
            &reason,
            clock.get_time_ns(),
        );
    }
}

fn decode_private_rows<T>(channel: &str, data: Vec<Value>) -> (Vec<T>, usize)
where
    T: DeserializeOwned,
{
    let mut rows = Vec::with_capacity(data.len());
    let mut errors = 0;

    for value in data {
        match serde_json::from_value::<T>(value) {
            Ok(row) => rows.push(row),
            Err(e) => {
                errors += 1;
                log::error!("Failed to decode Bitget private WebSocket {channel} row: {e:?}");
            }
        }
    }

    (rows, errors)
}

fn ws_order_to_status(row: BitgetWsOrderData) -> BitgetOrderStatus {
    BitgetOrderStatus {
        symbol: row.symbol,
        product_type: row.product_type,
        order_id: row.order_id,
        client_oid: row.client_oid,
        price: row.price,
        avg_price: row.avg_price,
        size: row.size,
        filled_size: row.filled_size,
        quote_size: row.quote_size,
        side: row.side,
        trade_side: row.trade_side,
        order_type: row.order_type,
        force: row.time_in_force,
        status: row.status,
        trigger_price: row.trigger_price,
        trigger_type: row.trigger_type,
        reduce_only: row.reduce_only,
        margin_coin: row.margin_coin,
        c_time: row.c_time,
        u_time: row.u_time,
        ..Default::default()
    }
}

fn ws_fill_to_fill(row: BitgetWsFillData) -> BitgetFill {
    BitgetFill {
        symbol: row.symbol,
        product_type: row.product_type,
        order_id: row.order_id,
        client_oid: row.client_oid,
        trade_id: row.fill_id,
        side: row.side,
        trade_side: row.trade_side,
        price: row.price,
        size: row.size,
        quote_size: row.quote_size,
        fee: row.fee,
        fee_coin: row.fee_currency,
        fee_detail: row.fee_detail,
        margin_coin: row.margin_coin,
        trade_scope: row.role,
        c_time: row.c_time,
        ..Default::default()
    }
}

fn ws_account_to_spot_asset(row: BitgetWsAccountData) -> BitgetSpotAsset {
    BitgetSpotAsset {
        coin: row.coin.or(row.margin_coin),
        available: row.available_balance,
        frozen: row.locked,
        u_time: row.u_time.or(row.c_time),
        ..Default::default()
    }
}

fn ws_account_to_mix_account(row: BitgetWsAccountData) -> BitgetMixAccount {
    BitgetMixAccount {
        margin_coin: row.margin_coin.or(row.coin),
        locked: row.locked,
        available: row.available_balance,
        account_equity: row.account_equity,
        usdt_equity: row.usdt_equity,
        unrealized_pnl: row.unrealized_pnl,
        u_time: row.u_time.or(row.c_time),
        ..Default::default()
    }
}

fn ws_account_to_uta_account(row: BitgetWsAccountData) -> BitgetUtaAccount {
    BitgetUtaAccount {
        account_equity: row.account_equity,
        usdt_equity: row.usdt_equity,
        eff_equity: row.available_balance,
        imr: row.imr,
        mmr: row.mmr,
        unrealised_pnl: row.unrealized_pnl,
        assets: row.assets,
    }
}

fn ws_position_to_position(row: BitgetWsPositionData) -> BitgetMixPosition {
    BitgetMixPosition {
        symbol: row.symbol,
        pos_id: row.pos_id,
        margin_coin: row.margin_coin,
        hold_side: row.hold_side,
        pos_mode: row.pos_mode,
        margin_mode: row.margin_mode,
        total: row.total,
        available: row.available,
        average_open_price: row.average_open_price,
        mark_price: row.mark_price,
        liquidation_price: row.liquidation_price,
        leverage: row.leverage,
        realized_pnl: row.realized_pnl,
        unrealized_pnl: row.unrealized_pnl,
        c_time: row.c_time,
        u_time: row.u_time,
        ..Default::default()
    }
}

fn dispatch_ws_order(
    row: BitgetWsOrderData,
    ctx: &BitgetExecutionWsDispatchContext,
) -> anyhow::Result<()> {
    let status = ws_order_to_status(row);
    emit_order_status_report(status, ctx)
}

fn emit_order_status_report(
    status: BitgetOrderStatus,
    ctx: &BitgetExecutionWsDispatchContext,
) -> anyhow::Result<()> {
    let instrument = ctx.instrument_for_raw_symbol(status.symbol.as_deref())?;
    let report =
        parse_order_status_report(&status, instrument, ctx.account_id, ctx.clock.get_time_ns())?;
    ctx.emitter.send_order_status_report(report);
    Ok(())
}

fn dispatch_ws_fill(
    row: BitgetWsFillData,
    ctx: &BitgetExecutionWsDispatchContext,
) -> anyhow::Result<()> {
    let fill = ws_fill_to_fill(row);
    emit_fill_report(fill, ctx)
}

fn emit_fill_report(
    fill: BitgetFill,
    ctx: &BitgetExecutionWsDispatchContext,
) -> anyhow::Result<()> {
    let instrument = ctx.instrument_for_raw_symbol(fill.symbol.as_deref())?;
    let report = parse_fill_report(&fill, instrument, ctx.account_id, ctx.clock.get_time_ns())?;
    ctx.emitter.send_fill_report(report);
    Ok(())
}

fn dispatch_ws_account(
    rows: Vec<BitgetWsAccountData>,
    ctx: &BitgetExecutionWsDispatchContext,
) -> anyhow::Result<()> {
    let ts_init = ctx.clock.get_time_ns();
    let account_state = match ctx.product_type {
        BitgetProductType::Spot => {
            let assets = rows
                .into_iter()
                .flat_map(|row| {
                    if row.assets.is_empty() {
                        vec![ws_account_to_spot_asset(row)]
                    } else {
                        ws_account_to_uta_account(row).into_spot_assets(None)
                    }
                })
                .collect::<Vec<_>>();
            parse_spot_account_state(&assets, ctx.account_id, ts_init)?
        }
        BitgetProductType::UsdtFutures => {
            let accounts = rows
                .into_iter()
                .flat_map(|row| {
                    if row.assets.is_empty() {
                        vec![ws_account_to_mix_account(row)]
                    } else {
                        ws_account_to_uta_account(row).into_usdt_futures_accounts()
                    }
                })
                .collect::<Vec<_>>();
            parse_mix_account_state(&accounts, ctx.account_id, ts_init)?
        }
    };

    ctx.emitter.send_account_state(account_state);
    Ok(())
}

fn dispatch_ws_position(
    row: BitgetWsPositionData,
    ctx: &BitgetExecutionWsDispatchContext,
) -> anyhow::Result<()> {
    if ctx.product_type == BitgetProductType::Spot {
        return Ok(());
    }

    let position = ws_position_to_position(row);
    let instrument = ctx.instrument_for_raw_symbol(position.symbol.as_deref())?;
    let report = parse_position_status_report(
        &position,
        instrument,
        ctx.account_id,
        ctx.clock.get_time_ns(),
    )?;
    ctx.emitter.send_position_report(report);
    Ok(())
}

fn reconnect_reconciliation_start(lookback_mins: Option<u64>) -> Option<DateTime<Utc>> {
    lookback_mins.map(|mins| {
        let max_mins = 10 * 365 * 24 * 60;
        let clamped_mins = mins.min(max_mins) as i64;
        Utc::now() - Duration::minutes(clamped_mins)
    })
}

fn non_empty_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn order_seen_key(order: &BitgetOrderStatus) -> Option<String> {
    non_empty_string(order.order_id.as_deref())
        .or_else(|| non_empty_string(order.client_oid.as_deref()))
}

fn fill_seen_key(fill: &BitgetFill) -> Option<String> {
    non_empty_string(fill.trade_id.as_deref())
}

async fn reconcile_after_reconnect(
    ctx: BitgetExecutionWsDispatchContext,
) -> BitgetReconnectReconciliationSummary {
    let mut summary = BitgetReconnectReconciliationSummary::default();
    let start = reconnect_reconciliation_start(ctx.reconnect_reconciliation_lookback_mins);

    match ctx
        .http_client
        .request_account_state(ctx.product_type, ctx.account_id, ctx.clock.get_time_ns())
        .await
    {
        Ok(account_state) => {
            ctx.emitter.send_account_state(account_state);
            summary.account_states = 1;
        }
        Err(e) => {
            summary.errors += 1;
            log::error!("Bitget reconnect account reconciliation failed: {e:?}");
        }
    }

    let mut seen_orders = HashSet::new();
    match ctx
        .http_client
        .request_order_statuses(ctx.product_type, None, None, None, true, Some(100))
        .await
    {
        Ok(orders) => {
            for order in orders {
                let seen_key = order_seen_key(&order);
                if seen_key
                    .as_ref()
                    .is_some_and(|key| seen_orders.contains(key))
                {
                    continue;
                }

                match emit_order_status_report(order, &ctx) {
                    Ok(()) => {
                        if let Some(key) = seen_key {
                            seen_orders.insert(key);
                        }
                        summary.order_reports += 1;
                    }
                    Err(e) => {
                        summary.errors += 1;
                        log::error!("Bitget reconnect open order reconciliation failed: {e:?}");
                    }
                }
            }
        }
        Err(e) => {
            summary.errors += 1;
            log::error!("Bitget reconnect open orders request failed: {e:?}");
        }
    }

    if let Some(start) = start {
        match ctx
            .http_client
            .request_order_statuses(ctx.product_type, None, Some(start), None, false, Some(100))
            .await
        {
            Ok(orders) => {
                for order in orders {
                    let seen_key = order_seen_key(&order);
                    if seen_key
                        .as_ref()
                        .is_some_and(|key| seen_orders.contains(key))
                    {
                        continue;
                    }

                    match emit_order_status_report(order, &ctx) {
                        Ok(()) => {
                            if let Some(key) = seen_key {
                                seen_orders.insert(key);
                            }
                            summary.order_reports += 1;
                        }
                        Err(e) => {
                            summary.errors += 1;
                            log::error!(
                                "Bitget reconnect historical order reconciliation failed: {e:?}"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                summary.errors += 1;
                log::error!("Bitget reconnect historical orders request failed: {e:?}");
            }
        }

        let mut seen_fills = HashSet::new();
        match ctx
            .http_client
            .request_fills(ctx.product_type, None, Some(start), None, Some(100))
            .await
        {
            Ok(fills) => {
                for fill in fills {
                    let seen_key = fill_seen_key(&fill);
                    if seen_key
                        .as_ref()
                        .is_some_and(|key| seen_fills.contains(key))
                    {
                        continue;
                    }

                    match emit_fill_report(fill, &ctx) {
                        Ok(()) => {
                            if let Some(key) = seen_key {
                                seen_fills.insert(key);
                            }
                            summary.fill_reports += 1;
                        }
                        Err(e) => {
                            summary.errors += 1;
                            log::error!("Bitget reconnect fill reconciliation failed: {e:?}");
                        }
                    }
                }
            }
            Err(e) => {
                summary.errors += 1;
                log::error!("Bitget reconnect fills request failed: {e:?}");
            }
        }
    } else {
        log::debug!(
            "Skipping Bitget reconnect historical order/fill reconciliation because lookback is disabled"
        );
    }

    if ctx.product_type == BitgetProductType::UsdtFutures {
        match ctx
            .http_client
            .request_positions(ctx.product_type, None)
            .await
        {
            Ok(positions) => {
                for position in positions {
                    let instrument = match ctx.instrument_for_raw_symbol(position.symbol.as_deref())
                    {
                        Ok(instrument) => instrument,
                        Err(e) => {
                            summary.errors += 1;
                            log::error!(
                                "Bitget reconnect position instrument resolution failed: {e:?}"
                            );
                            continue;
                        }
                    };

                    match parse_position_status_report(
                        &position,
                        instrument,
                        ctx.account_id,
                        ctx.clock.get_time_ns(),
                    ) {
                        Ok(report) => {
                            ctx.emitter.send_position_report(report);
                            summary.position_reports += 1;
                        }
                        Err(e) => {
                            summary.errors += 1;
                            log::error!("Bitget reconnect position reconciliation failed: {e:?}");
                        }
                    }
                }
            }
            Err(e) => {
                summary.errors += 1;
                log::error!("Bitget reconnect positions request failed: {e:?}");
            }
        }
    }

    log::info!(
        "Bitget reconnect reconciliation completed: account_states={}, order_reports={}, fill_reports={}, position_reports={}, errors={}",
        summary.account_states,
        summary.order_reports,
        summary.fill_reports,
        summary.position_reports,
        summary.errors,
    );

    summary
}

#[cfg(test)]
fn handle_bitget_execution_ws_message(
    message: BitgetWsMessage,
) -> BitgetExecutionWsDispatchSummary {
    handle_bitget_execution_ws_message_with_context(message, None)
}

fn handle_bitget_execution_ws_message_with_context(
    message: BitgetWsMessage,
    ctx: Option<&BitgetExecutionWsDispatchContext>,
) -> BitgetExecutionWsDispatchSummary {
    let mut summary = BitgetExecutionWsDispatchSummary::default();

    match message {
        BitgetWsMessage::Data(event) => {
            let topic = event
                .arg
                .as_ref()
                .map(|arg| arg.topic.as_str())
                .unwrap_or("<unknown>");

            match topic {
                "order" | "orders" | "strategy-order" => {
                    let (rows, errors) =
                        decode_private_rows::<BitgetWsOrderData>(topic, event.data);
                    summary.orders = rows.len();
                    summary.errors += errors;
                    if let Some(ctx) = ctx {
                        for row in rows {
                            if let Err(e) = dispatch_ws_order(row, ctx) {
                                summary.errors += 1;
                                log::error!("Failed to dispatch Bitget private order row: {e:?}");
                            }
                        }
                    }
                    log::debug!("Decoded {} Bitget private order rows", summary.orders);
                }
                "fill" | "fills" => {
                    let (rows, errors) = decode_private_rows::<BitgetWsFillData>(topic, event.data);
                    summary.fills = rows.len();
                    summary.errors += errors;
                    if let Some(ctx) = ctx {
                        for row in rows {
                            if let Err(e) = dispatch_ws_fill(row, ctx) {
                                summary.errors += 1;
                                log::error!("Failed to dispatch Bitget private fill row: {e:?}");
                            }
                        }
                    }
                    log::debug!("Decoded {} Bitget private fill rows", summary.fills);
                }
                "account" => {
                    let (rows, errors) =
                        decode_private_rows::<BitgetWsAccountData>(topic, event.data);
                    summary.accounts = rows.len();
                    summary.errors += errors;
                    if let Some(ctx) = ctx
                        && !rows.is_empty()
                        && let Err(e) = dispatch_ws_account(rows, ctx)
                    {
                        summary.errors += 1;
                        log::error!("Failed to dispatch Bitget private account rows: {e:?}");
                    }
                    log::debug!("Decoded {} Bitget private account rows", summary.accounts);
                }
                "position" | "positions" => {
                    let (rows, errors) =
                        decode_private_rows::<BitgetWsPositionData>(topic, event.data);
                    summary.positions = rows.len();
                    summary.errors += errors;
                    if let Some(ctx) = ctx {
                        for row in rows {
                            if let Err(e) = dispatch_ws_position(row, ctx) {
                                summary.errors += 1;
                                log::error!(
                                    "Failed to dispatch Bitget private position row: {e:?}"
                                );
                            }
                        }
                    }
                    log::debug!("Decoded {} Bitget private position rows", summary.positions);
                }
                _ => {
                    log::debug!(
                        "Ignoring unsupported Bitget private WebSocket data topic: {topic}"
                    );
                }
            }
        }
        BitgetWsMessage::Error(event) => {
            summary.errors = 1;
            log::warn!(
                "Bitget private WebSocket error: code={:?}, msg={:?}, arg={:?}",
                event.code,
                event.msg,
                event.arg,
            );
        }
        BitgetWsMessage::Login(_) => {
            log::debug!("Bitget private WebSocket login acknowledged");
        }
        BitgetWsMessage::Subscribe(event) => {
            log::debug!(
                "Bitget private WebSocket subscription acknowledged: {:?}",
                event.arg
            );
        }
        BitgetWsMessage::Unsubscribe(event) => {
            log::debug!(
                "Bitget private WebSocket unsubscription acknowledged: {:?}",
                event.arg
            );
        }
        BitgetWsMessage::Reconnected => {
            summary.reconnects = 1;
            log::info!("Bitget private WebSocket reconnected; starting REST reconciliation");
            if let Some(ctx) = ctx {
                if ctx
                    .reconnect_reconciliation_in_flight
                    .swap(true, Ordering::AcqRel)
                {
                    log::debug!("Skipping Bitget reconnect reconciliation already in flight");
                    return summary;
                }

                let ctx = ctx.clone();
                get_runtime().spawn(async move {
                    let _ = reconcile_after_reconnect(ctx.clone()).await;
                    ctx.reconnect_reconciliation_in_flight
                        .store(false, Ordering::Release);
                });
            }
        }
        BitgetWsMessage::Pong => {}
    }

    summary
}

#[async_trait(?Send)]
impl ExecutionClient for BitgetExecutionClient {
    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *BITGET_VENUE
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        None
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: nautilus_core::UnixNanos,
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
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.abort_ws_task();
        self.core.set_stopped();
        self.core.set_disconnected();
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.ws_client
            .connect()
            .await
            .context("connect Bitget private WebSocket")?;
        self.start_ws_dispatch()?;

        self.ws_client
            .subscribe_account(None)
            .await
            .context("subscribe Bitget account WebSocket channel")?;
        self.ws_client
            .subscribe_orders()
            .await
            .context("subscribe Bitget orders WebSocket channel")?;
        self.ws_client
            .subscribe_strategy_orders()
            .await
            .context("subscribe Bitget strategy orders WebSocket channel")?;

        if self.config.product_type == BitgetProductType::UsdtFutures {
            self.ws_client
                .subscribe_fills()
                .await
                .context("subscribe Bitget fill WebSocket channel")?;
            self.ws_client
                .subscribe_positions()
                .await
                .context("subscribe Bitget positions WebSocket channel")?;
        }

        self.core.set_connected();
        self.is_connected.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Err(e) = self.ws_client.disconnect().await {
            log::warn!("Error disconnecting Bitget private WebSocket: {e:?}");
        }
        self.abort_ws_task();
        self.core.set_disconnected();
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(cmd.instrument_id)?;
        let client_order_id = cmd.client_order_id;
        let order = match self.core.get_order(&client_order_id) {
            Ok(order) => Some(order),
            Err(e) => {
                log::warn!(
                    "Bitget submit order could not load cached order for {client_order_id}: {e:?}"
                );
                None
            }
        };
        let request = match map_submit_order(product_type, &cmd.order_init, cmd.params.as_ref()) {
            Ok(request) => request,
            Err(e) => {
                let reason = e.to_string();
                if let Some(order) = order.as_ref() {
                    self.emitter.emit_order_denied(order, &reason);
                } else {
                    log::warn!("Bitget submit order denied for {client_order_id}: {reason}");
                }
                return Ok(());
            }
        };

        if let Some(order) = order.as_ref() {
            self.emitter.emit_order_submitted(order);
        }

        let emitter = self.emitter.clone();
        let clock = self.clock;
        let http = self.http_client.clone();

        get_runtime().spawn(async move {
            match http.submit_order(&request).await {
                Ok(ack) => {
                    log::debug!(
                        "Bitget submit order ack: client_order_id={}, venue_order_id={:?}, ack_client_oid={:?}",
                        client_order_id,
                        ack.order_id,
                        ack.client_oid,
                    );
                    if let (Some(order), Some(order_id)) = (order.as_ref(), ack.order_id.as_deref())
                    {
                        emitter.emit_order_accepted(
                            order,
                            VenueOrderId::from(order_id),
                            clock.get_time_ns(),
                        );
                    }
                }
                Err(e) => {
                    log::error!("Bitget submit order failed for {client_order_id}: {e:?}");
                    if let Some(order) = order.as_ref() {
                        emitter.emit_order_rejected(
                            order,
                            &format!("Bitget submit order failed: {e}"),
                            clock.get_time_ns(),
                            false,
                        );
                    }
                }
            }
        });

        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(cmd.instrument_id)?;
        let client_order_id = cmd.client_order_id;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let venue_order_id = cmd.venue_order_id;
        let request = match map_modify_order(
            product_type,
            instrument_id,
            client_order_id,
            venue_order_id,
            cmd.quantity,
            cmd.price,
            cmd.trigger_price,
            cmd.params.as_ref(),
        ) {
            Ok(request) => request,
            Err(e) => {
                let reason = e.to_string();
                self.emitter.emit_order_modify_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    venue_order_id,
                    &reason,
                    self.clock.get_time_ns(),
                );
                return Ok(());
            }
        };
        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        get_runtime().spawn(async move {
            match http.modify_order(&request).await {
                Ok(ack) => {
                    log::debug!(
                        "Bitget modify order ack: client_order_id={}, venue_order_id={:?}, ack_client_oid={:?}",
                        client_order_id,
                        ack.order_id,
                        ack.client_oid,
                    );
                }
                Err(e) => {
                    match classify_bitget_http_failure(&e) {
                        BitgetCommandFailureKind::StructuredVenueRejection
                        | BitgetCommandFailureKind::LocalValidation => {
                            emitter.emit_order_modify_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                venue_order_id,
                                &format!("Bitget modify order failed: {e}"),
                                clock.get_time_ns(),
                            );
                        }
                        BitgetCommandFailureKind::Ambiguous => {
                            log::warn!(
                                "Bitget modify order outcome unknown for {client_order_id}, awaiting reconciliation: {e:?}"
                            );
                        }
                    }
                }
            }
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(cmd.instrument_id)?;
        let client_order_id = cmd.client_order_id;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let venue_order_id = cmd.venue_order_id;
        let request = match map_cancel_order(
            product_type,
            instrument_id,
            client_order_id,
            venue_order_id,
            cmd.params.as_ref(),
        ) {
            Ok(request) => request,
            Err(e) => {
                let reason = e.to_string();
                self.emitter.emit_order_cancel_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    venue_order_id,
                    &reason,
                    self.clock.get_time_ns(),
                );
                return Ok(());
            }
        };
        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        get_runtime().spawn(async move {
            match http.cancel_order(&request).await {
                Ok(ack) => {
                    log::debug!(
                        "Bitget cancel order ack: client_order_id={}, venue_order_id={:?}, ack_client_oid={:?}",
                        client_order_id,
                        ack.order_id,
                        ack.client_oid,
                    );
                }
                Err(e) => {
                    match classify_bitget_http_failure(&e) {
                        BitgetCommandFailureKind::StructuredVenueRejection
                        | BitgetCommandFailureKind::LocalValidation => {
                            emitter.emit_order_cancel_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                venue_order_id,
                                &format!("Bitget cancel order failed: {e}"),
                                clock.get_time_ns(),
                            );
                        }
                        BitgetCommandFailureKind::Ambiguous => {
                            log::warn!(
                                "Bitget cancel order outcome unknown for {client_order_id}, awaiting reconciliation: {e:?}"
                            );
                        }
                    }
                }
            }
        });

        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(cmd.instrument_id)?;
        if cmd.order_side != OrderSide::NoOrderSide {
            log::warn!(
                "Bitget does not support order_side filtering for cancel all orders; \
                 ignoring order_side={:?} and canceling all orders for {}",
                cmd.order_side,
                cmd.instrument_id,
            );
        }

        let instrument_id = cmd.instrument_id;
        let strategy_id = cmd.strategy_id;
        let cancel_contexts = self
            .core
            .cache()
            .orders_open(
                Some(&*BITGET_VENUE),
                Some(&instrument_id),
                Some(&strategy_id),
                Some(&self.core.account_id),
                None,
            )
            .into_iter()
            .map(|order| {
                BitgetCancelRejectionContext::new(
                    order.strategy_id(),
                    order.instrument_id(),
                    order.client_order_id(),
                    order.venue_order_id(),
                )
            })
            .collect::<Vec<_>>();

        let request = match map_cancel_all_orders(product_type, instrument_id, cmd.params.as_ref())
        {
            Ok(request) => request,
            Err(e) => {
                let reason = e.to_string();
                emit_cancel_rejected_for_contexts(
                    &self.emitter,
                    self.clock,
                    &cancel_contexts,
                    &reason,
                );
                if cancel_contexts.is_empty() {
                    log::warn!("Bitget cancel all rejected for {instrument_id}: {reason}");
                }
                return Ok(());
            }
        };

        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        get_runtime().spawn(async move {
            match http.cancel_all_orders(&request).await {
                Ok(response) => {
                    log::debug!(
                        "Bitget cancel all ack for {instrument_id}: successes={}, failures={}",
                        response.success_list.len(),
                        response.failure_list.len(),
                    );
                    emit_cancel_failure_list_rejections(
                        &response,
                        &cancel_contexts,
                        strategy_id,
                        instrument_id,
                        &emitter,
                        clock,
                    );
                }
                Err(e) => match classify_bitget_http_failure(&e) {
                    BitgetCommandFailureKind::StructuredVenueRejection
                    | BitgetCommandFailureKind::LocalValidation => {
                        let reason = format!("Bitget cancel all orders failed: {e}");
                        emit_cancel_rejected_for_contexts(
                            &emitter,
                            clock,
                            &cancel_contexts,
                            &reason,
                        );
                        if cancel_contexts.is_empty() {
                            log::warn!("Bitget cancel all rejected for {instrument_id}: {e:?}");
                        }
                    }
                    BitgetCommandFailureKind::Ambiguous => {
                        log::warn!(
                            "Bitget cancel all outcome unknown for {instrument_id}, awaiting reconciliation: {e:?}"
                        );
                    }
                },
            }
        });

        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        if cmd.cancels.is_empty() {
            return Ok(());
        }

        let product_type = self.configured_product_type_for(cmd.instrument_id)?;
        let instrument_id = cmd.instrument_id;
        let strategy_id = cmd.strategy_id;
        let cancel_contexts = cmd
            .cancels
            .iter()
            .map(BitgetCancelRejectionContext::from_cancel)
            .collect::<Vec<_>>();
        let request = match map_batch_cancel_orders(
            product_type,
            instrument_id,
            &cmd.cancels,
            cmd.params.as_ref(),
        ) {
            Ok(request) => request,
            Err(e) => {
                let reason = e.to_string();
                emit_cancel_rejected_for_contexts(
                    &self.emitter,
                    self.clock,
                    &cancel_contexts,
                    &reason,
                );
                return Ok(());
            }
        };

        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        get_runtime().spawn(async move {
            match http.batch_cancel_orders(&request).await {
                Ok(response) => {
                    log::debug!(
                        "Bitget batch cancel ack for {instrument_id}: successes={}, failures={}",
                        response.success_list.len(),
                        response.failure_list.len(),
                    );
                    emit_cancel_failure_list_rejections(
                        &response,
                        &cancel_contexts,
                        strategy_id,
                        instrument_id,
                        &emitter,
                        clock,
                    );
                }
                Err(e) => match classify_bitget_http_failure(&e) {
                    BitgetCommandFailureKind::StructuredVenueRejection
                    | BitgetCommandFailureKind::LocalValidation => {
                        emit_cancel_rejected_for_contexts(
                            &emitter,
                            clock,
                            &cancel_contexts,
                            &format!("Bitget batch cancel orders failed: {e}"),
                        );
                    }
                    BitgetCommandFailureKind::Ambiguous => {
                        log::warn!(
                            "Bitget batch cancel outcome unknown for {} orders on {instrument_id}, awaiting reconciliation: {e:?}",
                            cancel_contexts.len(),
                        );
                    }
                },
            }
        });

        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let product_type = self.config.product_type;
        let account_id = self.core.account_id;
        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let ts_init = self.clock.get_time_ns();

        get_runtime().spawn(async move {
            match http
                .request_account_state(product_type, account_id, ts_init)
                .await
            {
                Ok(account_state) => emitter.send_account_state(account_state),
                Err(e) => log::error!("Bitget query account failed: {e:?}"),
            }
        });

        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let product_type = self.configured_product_type_for(cmd.instrument_id)?;
        let instrument = self.cached_instrument(cmd.instrument_id)?;
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;
        let venue_order_id = cmd.venue_order_id.map(|id| id.to_string());
        let client_order_id_str = client_order_id.to_string();
        let http = self.http_client.clone();
        let emitter = self.emitter.clone();
        let account_id = self.core.account_id;
        let clock = self.clock;

        get_runtime().spawn(async move {
            match http
                .request_order_status(
                    product_type,
                    instrument_id,
                    venue_order_id.as_deref(),
                    Some(&client_order_id_str),
                )
                .await
            {
                Ok(status) => {
                    log::debug!("Bitget order status for {client_order_id}: {status:?}");
                    match parse_order_status_report(
                        &status,
                        &instrument,
                        account_id,
                        clock.get_time_ns(),
                    ) {
                        Ok(report) => emitter.send_order_status_report(report),
                        Err(e) => log::error!(
                            "Bitget order status report parse failed for {client_order_id}: {e:?}"
                        ),
                    }
                }
                Err(e) => {
                    log::error!("Bitget query order failed for {client_order_id}: {e:?}");
                }
            }
        });

        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<nautilus_model::reports::OrderStatusReport>> {
        if cmd.venue_order_id.is_none() && cmd.client_order_id.is_none() {
            log::warn!(
                "Bitget generate_order_status_report requires venue_order_id or client_order_id"
            );
            return Ok(None);
        }

        let Some(instrument_id) =
            self.resolve_report_instrument_id(cmd.instrument_id, cmd.client_order_id)
        else {
            log::warn!(
                "Bitget generate_order_status_report requires instrument_id when local order cache cannot infer it"
            );
            return Ok(None);
        };

        let product_type = self.configured_product_type_for(instrument_id)?;
        let instrument = self.cached_instrument(instrument_id)?;
        let venue_order_id = cmd.venue_order_id.map(|id| id.to_string());
        let client_order_id = cmd.client_order_id.map(|id| id.to_string());
        let status = self
            .http_client
            .request_order_status(
                product_type,
                instrument_id,
                venue_order_id.as_deref(),
                client_order_id.as_deref(),
            )
            .await?;
        let report = parse_order_status_report(
            &status,
            &instrument,
            self.core.account_id,
            self.clock.get_time_ns(),
        )?;

        Ok(Some(report))
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<nautilus_model::reports::OrderStatusReport>> {
        if let Some(instrument_id) = cmd.instrument_id {
            self.configured_product_type_for(instrument_id)?;
        }

        let start = cmd.start.map(DateTime::<Utc>::from);
        let end = cmd.end.map(DateTime::<Utc>::from);
        let orders = self
            .http_client
            .request_order_statuses(
                self.config.product_type,
                cmd.instrument_id,
                start,
                end,
                cmd.open_only,
                Some(100),
            )
            .await?;
        let ts_init = self.clock.get_time_ns();
        let mut reports = Vec::with_capacity(orders.len());

        for order in orders {
            let instrument_id = self.instrument_id_for_order_status(&order, cmd.instrument_id)?;
            let instrument = self.cached_instrument(instrument_id)?;
            reports.push(parse_order_status_report(
                &order,
                &instrument,
                self.core.account_id,
                ts_init,
            )?);
        }

        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        if let Some(instrument_id) = cmd.instrument_id {
            self.configured_product_type_for(instrument_id)?;
        }

        let start = cmd.start.map(DateTime::<Utc>::from);
        let end = cmd.end.map(DateTime::<Utc>::from);
        let fills = self
            .http_client
            .request_fills(
                self.config.product_type,
                cmd.instrument_id,
                start,
                end,
                Some(100),
            )
            .await?;
        let ts_init = self.clock.get_time_ns();
        let mut reports = Vec::with_capacity(fills.len());

        for fill in fills {
            if let Some(venue_order_id) = cmd.venue_order_id
                && fill.order_id.as_deref() != Some(venue_order_id.as_str())
            {
                continue;
            }

            let instrument_id = self.instrument_id_for_fill(&fill, cmd.instrument_id)?;
            let instrument = self.cached_instrument(instrument_id)?;
            reports.push(parse_fill_report(
                &fill,
                &instrument,
                self.core.account_id,
                ts_init,
            )?);
        }

        Ok(reports)
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        if self.config.product_type == BitgetProductType::Spot {
            return Ok(Vec::new());
        }

        if let Some(instrument_id) = cmd.instrument_id {
            self.configured_product_type_for(instrument_id)?;
        }

        let positions = self
            .http_client
            .request_positions(self.config.product_type, cmd.instrument_id)
            .await?;
        let ts_init = self.clock.get_time_ns();
        let mut reports = Vec::with_capacity(positions.len());

        for position in positions {
            let instrument_id = self.instrument_id_for_position(&position, cmd.instrument_id)?;
            let instrument = match self.cached_instrument(instrument_id) {
                Ok(instrument) => instrument,
                Err(e) if cmd.instrument_id.is_none() => {
                    log::warn!(
                        "Skipping Bitget position report for uncached instrument {instrument_id}: {e:?}"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };
            reports.push(parse_position_status_report(
                &position,
                &instrument,
                self.core.account_id,
                ts_init,
            )?);
        }

        Ok(reports)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration as StdDuration, Instant},
    };

    use axum::{
        Json, Router,
        extract::{
            Query, State,
            ws::{Message, WebSocket, WebSocketUpgrade},
        },
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::get,
    };
    use futures_util::{SinkExt, StreamExt};
    use nautilus_common::messages::{ExecutionEvent, ExecutionReport};
    use nautilus_common::testing::wait_until_async;
    use nautilus_core::UnixNanos;
    use nautilus_model::{enums::AccountType, events::OrderEventAny, identifiers::TraderId};
    use nautilus_network::websocket::{TEXT_PING, TEXT_PONG, TransportBackend};
    use rstest::rstest;
    use serde_json::{Value, json};

    use crate::{
        common::{enums::BitgetEnvironment, parse::parse_usdt_perp_instrument},
        http::models::BitgetMixContract,
        websocket::messages::{BitgetWsArg, BitgetWsEvent},
    };

    use super::*;

    const TEST_TS: UnixNanos = UnixNanos::new(1_700_000_000_000_000_000);

    #[derive(Clone, Default)]
    struct ReconnectFixtureState {
        account_requests: Arc<AtomicUsize>,
        pending_order_queries: Arc<tokio::sync::Mutex<Vec<HashMap<String, String>>>>,
        history_order_queries: Arc<tokio::sync::Mutex<Vec<HashMap<String, String>>>>,
        fill_queries: Arc<tokio::sync::Mutex<Vec<HashMap<String, String>>>>,
        position_queries: Arc<tokio::sync::Mutex<Vec<HashMap<String, String>>>>,
        fail_fills: Arc<AtomicBool>,
    }

    #[derive(Clone, Default)]
    struct PrivateWsFixtureState {
        connection_count: Arc<AtomicUsize>,
        login_count: Arc<AtomicUsize>,
        subscribe_arg_count: Arc<AtomicUsize>,
        close_after_subscribe_args: Arc<AtomicUsize>,
        send_order_after_subscribe: Arc<AtomicBool>,
        received_messages: Arc<tokio::sync::Mutex<Vec<Value>>>,
    }

    impl PrivateWsFixtureState {
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

    fn private_data_message(topic: &str, data: Vec<Value>) -> BitgetWsMessage {
        BitgetWsMessage::Data(BitgetWsEvent {
            event: None,
            action: Some("snapshot".to_string()),
            arg: Some(BitgetWsArg::private(topic, None)),
            data,
            ts: None,
            code: None,
            msg: None,
        })
    }

    fn usdt_perp_instrument() -> InstrumentAny {
        let definition = BitgetMixContract {
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
        };

        parse_usdt_perp_instrument(&definition, TEST_TS, TEST_TS).unwrap()
    }

    fn test_emitter() -> (
        ExecutionEventEmitter,
        tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) {
        let clock = get_atomic_clock_realtime();
        let mut emitter = ExecutionEventEmitter::new(
            clock,
            TraderId::from("TESTER-001"),
            AccountId::from("BITGET-001"),
            AccountType::Margin,
            None,
        );
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(tx);
        (emitter, rx)
    }

    fn drain_execution_events(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) -> Vec<ExecutionEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    async fn collect_execution_events_until<F>(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
        timeout: StdDuration,
        mut predicate: F,
    ) -> Vec<ExecutionEvent>
    where
        F: FnMut(&[ExecutionEvent]) -> bool,
    {
        let mut events = Vec::new();
        let deadline = Instant::now() + timeout;

        loop {
            while let Ok(event) = rx.try_recv() {
                events.push(event);
            }

            if predicate(&events) || Instant::now() >= deadline {
                return events;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining.min(StdDuration::from_millis(250)), rx.recv())
                .await
            {
                Ok(Some(event)) => events.push(event),
                Ok(None) => return events,
                Err(_) => {}
            }
        }
    }

    #[rstest]
    #[case(
        BitgetHttpError::BitgetError {
            code: "40010".to_string(),
            message: "order not found".to_string()
        },
        BitgetCommandFailureKind::StructuredVenueRejection
    )]
    #[case(
        BitgetHttpError::MissingCredentials,
        BitgetCommandFailureKind::LocalValidation
    )]
    #[case(
        BitgetHttpError::NetworkError("timeout".to_string()),
        BitgetCommandFailureKind::Ambiguous
    )]
    fn classifies_bitget_http_failures(
        #[case] error: BitgetHttpError,
        #[case] expected: BitgetCommandFailureKind,
    ) {
        assert_eq!(classify_bitget_http_failure(&error), expected);
    }

    #[rstest]
    fn cancel_failure_context_matches_client_or_venue_id() {
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
        let contexts = vec![
            BitgetCancelRejectionContext::new(
                StrategyId::from("S-001"),
                instrument_id,
                ClientOrderId::from("C-1"),
                Some(VenueOrderId::from("V-1")),
            ),
            BitgetCancelRejectionContext::new(
                StrategyId::from("S-001"),
                instrument_id,
                ClientOrderId::from("C-2"),
                Some(VenueOrderId::from("V-2")),
            ),
        ];

        let by_client = BitgetCancelBatchResult {
            order_id: None,
            client_oid: Some("C-2".to_string()),
            code: None,
            error_msg: Some("order not found".to_string()),
        };
        let by_venue = BitgetCancelBatchResult {
            order_id: Some("V-1".to_string()),
            client_oid: None,
            code: None,
            error_msg: Some("order not found".to_string()),
        };

        assert_eq!(
            cancel_failure_context(&by_client, &contexts)
                .unwrap()
                .client_order_id,
            ClientOrderId::from("C-2")
        );
        assert_eq!(
            cancel_failure_context(&by_venue, &contexts)
                .unwrap()
                .client_order_id,
            ClientOrderId::from("C-1")
        );
    }

    #[rstest]
    fn emit_cancel_failure_list_rejections_emits_cancel_rejected_events() {
        let (emitter, mut event_rx) = test_emitter();
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BITGET");
        let contexts = vec![BitgetCancelRejectionContext::new(
            StrategyId::from("S-001"),
            instrument_id,
            ClientOrderId::from("C-1"),
            Some(VenueOrderId::from("V-1")),
        )];
        let response = BitgetCancelBatchResponse {
            success_list: vec![],
            failure_list: vec![BitgetCancelBatchResult {
                order_id: Some("V-1".to_string()),
                client_oid: None,
                code: None,
                error_msg: Some("order not found".to_string()),
            }],
        };

        emit_cancel_failure_list_rejections(
            &response,
            &contexts,
            StrategyId::from("S-001"),
            instrument_id,
            &emitter,
            get_atomic_clock_realtime(),
        );

        let events = drain_execution_events(&mut event_rx);
        assert_eq!(events.len(), 1);
        match &events[0] {
            ExecutionEvent::Order(OrderEventAny::CancelRejected(event)) => {
                assert_eq!(event.client_order_id, ClientOrderId::from("C-1"));
                assert_eq!(event.venue_order_id, Some(VenueOrderId::from("V-1")));
                assert!(event.reason.as_str().contains("order not found"));
            }
            event => panic!("expected cancel rejected event, was {event:?}"),
        }
    }

    fn reconnect_context(
        base_url: String,
        lookback_mins: Option<u64>,
        emitter: ExecutionEventEmitter,
    ) -> BitgetExecutionWsDispatchContext {
        let http_client = BitgetHttpClient::with_credentials(
            "test-key".to_string(),
            "test-secret".to_string(),
            "test-passphrase".to_string(),
            Some(base_url),
            5,
            None,
        )
        .unwrap();

        BitgetExecutionWsDispatchContext::new(
            BitgetProductType::UsdtFutures,
            AccountId::from("BITGET-001"),
            get_atomic_clock_realtime(),
            emitter,
            http_client,
            lookback_mins,
            vec![usdt_perp_instrument()],
        )
    }

    fn bitget_success(data: serde_json::Value) -> serde_json::Value {
        json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": data,
        })
    }

    fn order_row(order_id: &str, client_oid: &str, status: &str) -> serde_json::Value {
        json!({
            "symbol": "BTCUSDT",
            "category": "USDT-FUTURES",
            "orderId": order_id,
            "clientOid": client_oid,
            "price": "100.0",
            "avgPrice": "100.1",
            "qty": "0.010",
            "cumExecQty": "0.004",
            "side": "buy",
            "orderType": "limit",
            "timeInForce": "gtc",
            "orderStatus": status,
            "reduceOnly": "YES",
            "createdTime": "1700000000000",
            "updatedTime": "1700000001000",
        })
    }

    async fn start_reconnect_fixture_server(state: ReconnectFixtureState) -> SocketAddr {
        let router = Router::new()
            .route("/api/v3/account/assets", get(handle_reconnect_accounts))
            .route(
                "/api/v3/trade/unfilled-orders",
                get(handle_reconnect_pending_orders),
            )
            .route(
                "/api/v3/trade/history-orders",
                get(handle_reconnect_history_orders),
            )
            .route("/api/v3/trade/fills", get(handle_reconnect_fills))
            .route(
                "/api/v3/position/current-position",
                get(handle_reconnect_positions),
            )
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

    async fn handle_private_ws_upgrade(
        ws: WebSocketUpgrade,
        State(state): State<PrivateWsFixtureState>,
    ) -> Response {
        ws.on_upgrade(move |socket| handle_private_ws_socket(socket, state))
    }

    async fn handle_private_ws_socket(socket: WebSocket, state: PrivateWsFixtureState) {
        state.connection_count.fetch_add(1, Ordering::SeqCst);
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

                    match payload.get("op").and_then(Value::as_str) {
                        Some("login") => {
                            state.login_count.fetch_add(1, Ordering::SeqCst);
                            let ack = json!({
                                "event": "login",
                                "code": "0",
                                "msg": "success",
                            });
                            let _ = sink.send(Message::Text(ack.to_string().into())).await;
                        }
                        Some("subscribe") => {
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

                                let is_order_channel =
                                    arg.get("topic").and_then(Value::as_str) == Some("order");
                                if is_order_channel
                                    && state
                                        .send_order_after_subscribe
                                        .swap(false, Ordering::SeqCst)
                                {
                                    let data = json!({
                                        "action": "snapshot",
                                        "arg": arg,
                                        "data": [order_row("O-WS", "C-WS", "live")],
                                    });
                                    let _ = sink.send(Message::Text(data.to_string().into())).await;
                                }

                                let seen =
                                    state.subscribe_arg_count.fetch_add(1, Ordering::SeqCst) + 1;
                                let close_after =
                                    state.close_after_subscribe_args.load(Ordering::SeqCst);
                                if close_after != 0 && seen == close_after {
                                    let _ = sink.send(Message::Close(None)).await;
                                    return;
                                }
                            }
                        }
                        _ => {}
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

    async fn start_private_ws_fixture_server(state: PrivateWsFixtureState) -> SocketAddr {
        let router = Router::new()
            .route("/ws", get(handle_private_ws_upgrade))
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
            StdDuration::from_secs(5),
        )
        .await;

        addr
    }

    fn ws_url(addr: SocketAddr) -> String {
        format!("ws://{addr}/ws")
    }

    async fn handle_reconnect_accounts(
        State(state): State<ReconnectFixtureState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Response {
        assert!(query.is_empty());
        state.account_requests.fetch_add(1, Ordering::Relaxed);
        Json(bitget_success(json!({
            "accountEquity": "123",
            "usdtEquity": "123",
            "effEquity": "100",
            "imr": "10",
            "mmr": "4",
            "assets": [{
                "coin": "USDT",
                "equity": "123",
                "available": "100",
                "locked": "1"
            }]
        })))
        .into_response()
    }

    async fn handle_reconnect_pending_orders(
        State(state): State<ReconnectFixtureState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Response {
        state.pending_order_queries.lock().await.push(query);
        Json(bitget_success(json!({
            "list": [order_row("O-1", "C-1", "live")],
            "cursor": "",
        })))
        .into_response()
    }

    async fn handle_reconnect_history_orders(
        State(state): State<ReconnectFixtureState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Response {
        state.history_order_queries.lock().await.push(query);
        Json(bitget_success(json!({
            "list": [
                order_row("O-1", "C-1", "live"),
                order_row("O-2", "C-2", "filled")
            ],
            "cursor": "",
        })))
        .into_response()
    }

    async fn handle_reconnect_fills(
        State(state): State<ReconnectFixtureState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Response {
        state.fill_queries.lock().await.push(query);
        if state.fail_fills.load(Ordering::Relaxed) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "code": "50000",
                    "msg": "fixture fill failure",
                    "data": {}
                })),
            )
                .into_response();
        }

        Json(bitget_success(json!({
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "category": "USDT-FUTURES",
                    "orderId": "O-2",
                    "clientOid": "C-2",
                    "execId": "T-1",
                    "side": "buy",
                    "execPrice": "100.1",
                    "execQty": "0.004",
                    "feeDetail": [{"feeCoin": "USDT", "fee": "-0.001"}],
                    "tradeScope": "taker",
                    "createdTime": "1700000001000"
                },
                {
                    "symbol": "BTCUSDT",
                    "category": "USDT-FUTURES",
                    "orderId": "O-2",
                    "clientOid": "C-2",
                    "execId": "T-1",
                    "side": "buy",
                    "execPrice": "100.1",
                    "execQty": "0.004",
                    "feeDetail": [{"feeCoin": "USDT", "fee": "-0.001"}],
                    "tradeScope": "taker",
                    "createdTime": "1700000001000"
                }
            ],
            "cursor": "",
        })))
        .into_response()
    }

    async fn handle_reconnect_positions(
        State(state): State<ReconnectFixtureState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Response {
        state.position_queries.lock().await.push(query);
        Json(bitget_success(json!({
            "list": [{
            "symbol": "BTCUSDT",
            "category": "USDT-FUTURES",
            "posId": "P-1",
            "marginCoin": "USDT",
            "posSide": "long",
            "size": "0.004",
            "avgPrice": "100.1",
            "updatedTime": "1700000001000",
        }]
        })))
        .into_response()
    }

    #[rstest]
    #[case("order", json!({"symbol":"BTCUSDT","orderId":"1"}), "orders")]
    #[case("fill", json!({"symbol":"BTCUSDT","execId":"1"}), "fills")]
    #[case("account", json!({"marginCoin":"USDT","available":"100"}), "accounts")]
    #[case("position", json!({"symbol":"BTCUSDT","posId":"1"}), "positions")]
    fn private_ws_dispatch_decodes_supported_topics(
        #[case] topic: &str,
        #[case] row: Value,
        #[case] expected_counter: &str,
    ) {
        let summary = handle_bitget_execution_ws_message(private_data_message(topic, vec![row]));

        assert_eq!(summary.errors, 0);
        match expected_counter {
            "orders" => assert_eq!(summary.orders, 1),
            "fills" => assert_eq!(summary.fills, 1),
            "accounts" => assert_eq!(summary.accounts, 1),
            "positions" => assert_eq!(summary.positions, 1),
            _ => unreachable!(),
        }
    }

    #[rstest]
    fn private_ws_dispatch_counts_decode_errors() {
        let summary = handle_bitget_execution_ws_message(private_data_message(
            "order",
            vec![json!("not an object")],
        ));

        assert_eq!(summary.orders, 0);
        assert_eq!(summary.errors, 1);
    }

    #[rstest]
    fn private_ws_dispatch_counts_reconnects() {
        let summary = handle_bitget_execution_ws_message(BitgetWsMessage::Reconnected);

        assert_eq!(summary.reconnects, 1);
        assert_eq!(summary.errors, 0);
    }

    #[tokio::test]
    async fn reconnect_reconciliation_emits_rest_reports_and_records_queries() {
        let state = ReconnectFixtureState::default();
        let addr = start_reconnect_fixture_server(state.clone()).await;
        let (emitter, mut event_rx) = test_emitter();
        let ctx = reconnect_context(format!("http://{addr}"), Some(60), emitter);

        let summary = reconcile_after_reconnect(ctx).await;
        let events = drain_execution_events(&mut event_rx);

        assert_eq!(summary.account_states, 1);
        assert_eq!(summary.order_reports, 2);
        assert_eq!(summary.fill_reports, 1);
        assert_eq!(summary.position_reports, 1);
        assert_eq!(summary.errors, 0);

        let account_events = events
            .iter()
            .filter(|event| matches!(event, ExecutionEvent::Account(_)))
            .count();
        let order_reports = events
            .iter()
            .filter(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Order(_))))
            .count();
        let fill_reports = events
            .iter()
            .filter(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Fill(_))))
            .count();
        let position_reports = events
            .iter()
            .filter(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Position(_))))
            .count();

        assert_eq!(account_events, 1);
        assert_eq!(order_reports, 2);
        assert_eq!(fill_reports, 1);
        assert_eq!(position_reports, 1);

        let order_ids = events
            .iter()
            .filter_map(|event| match event {
                ExecutionEvent::Report(ExecutionReport::Order(report)) => {
                    Some(report.venue_order_id.to_string())
                }
                _ => None,
            })
            .collect::<HashSet<_>>();
        let trade_ids = events
            .iter()
            .filter_map(|event| match event {
                ExecutionEvent::Report(ExecutionReport::Fill(report)) => {
                    Some(report.trade_id.to_string())
                }
                _ => None,
            })
            .collect::<HashSet<_>>();

        assert_eq!(
            order_ids,
            HashSet::from(["O-1".to_string(), "O-2".to_string()])
        );
        assert_eq!(trade_ids, HashSet::from(["T-1".to_string()]));

        let pending_queries = state.pending_order_queries.lock().await;
        assert_eq!(pending_queries.len(), 1);
        assert_eq!(
            pending_queries[0].get("category").map(String::as_str),
            Some("USDT-FUTURES")
        );
        assert!(!pending_queries[0].contains_key("startTime"));

        let history_queries = state.history_order_queries.lock().await;
        assert_eq!(history_queries.len(), 1);
        assert!(history_queries[0].contains_key("startTime"));

        let fill_queries = state.fill_queries.lock().await;
        assert_eq!(fill_queries.len(), 1);
        assert!(fill_queries[0].contains_key("startTime"));

        let position_queries = state.position_queries.lock().await;
        assert_eq!(position_queries.len(), 1);
        assert_eq!(
            position_queries[0].get("category").map(String::as_str),
            Some("USDT-FUTURES")
        );
        assert!(!position_queries[0].contains_key("marginCoin"));
    }

    #[tokio::test]
    async fn reconnect_reconciliation_continues_when_fills_fail() {
        let state = ReconnectFixtureState::default();
        state.fail_fills.store(true, Ordering::Relaxed);
        let addr = start_reconnect_fixture_server(state.clone()).await;
        let (emitter, mut event_rx) = test_emitter();
        let ctx = reconnect_context(format!("http://{addr}"), Some(60), emitter);

        let summary = reconcile_after_reconnect(ctx).await;
        let events = drain_execution_events(&mut event_rx);

        assert_eq!(summary.account_states, 1);
        assert_eq!(summary.order_reports, 2);
        assert_eq!(summary.fill_reports, 0);
        assert_eq!(summary.position_reports, 1);
        assert_eq!(summary.errors, 1);

        assert!(
            events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::Account(_)))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Order(_))))
        );
        assert!(events.iter().any(|event| {
            matches!(event, ExecutionEvent::Report(ExecutionReport::Position(_)))
        }));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Fill(_))))
        );
    }

    #[tokio::test]
    async fn private_ws_reconnect_skips_reconciliation_already_in_flight() {
        let state = ReconnectFixtureState::default();
        let addr = start_reconnect_fixture_server(state.clone()).await;
        let (emitter, _event_rx) = test_emitter();
        let ctx = reconnect_context(format!("http://{addr}"), Some(60), emitter);
        ctx.reconnect_reconciliation_in_flight
            .store(true, Ordering::Release);

        let summary = handle_bitget_execution_ws_message_with_context(
            BitgetWsMessage::Reconnected,
            Some(&ctx),
        );
        tokio::time::sleep(StdDuration::from_millis(25)).await;

        assert_eq!(summary.reconnects, 1);
        assert_eq!(summary.errors, 0);
        assert_eq!(state.account_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn private_ws_fixture_reconnect_replays_subscriptions_and_reconciles_rest() {
        let rest_state = ReconnectFixtureState::default();
        let rest_addr = start_reconnect_fixture_server(rest_state.clone()).await;
        let ws_state = PrivateWsFixtureState::default();
        ws_state
            .close_after_subscribe_args
            .store(5, Ordering::SeqCst);
        ws_state
            .send_order_after_subscribe
            .store(true, Ordering::SeqCst);
        let ws_addr = start_private_ws_fixture_server(ws_state.clone()).await;
        let (emitter, mut event_rx) = test_emitter();
        let ctx = reconnect_context(format!("http://{rest_addr}"), Some(60), emitter);
        let mut ws_client = BitgetWebSocketClient::new_private(
            BitgetProductType::UsdtFutures,
            BitgetEnvironment::Mainnet,
            Some("key".to_string()),
            Some("secret".to_string()),
            Some("passphrase".to_string()),
            Some(ws_url(ws_addr)),
            30,
            TransportBackend::default(),
            None,
        );

        ws_client.connect().await.unwrap();
        let mut ws_event_rx = ws_client.take_event_receiver().unwrap();
        let dispatch_ctx = ctx.clone();
        let dispatch_handle = tokio::spawn(async move {
            while let Some(message) = ws_event_rx.recv().await {
                let _ =
                    handle_bitget_execution_ws_message_with_context(message, Some(&dispatch_ctx));
            }
        });

        ws_client.subscribe_account(None).await.unwrap();
        ws_client.subscribe_orders().await.unwrap();
        ws_client.subscribe_strategy_orders().await.unwrap();
        ws_client.subscribe_fills().await.unwrap();
        ws_client.subscribe_positions().await.unwrap();

        wait_until_async(
            || {
                let ws_state = ws_state.clone();
                async move {
                    ws_state.connection_count.load(Ordering::SeqCst) >= 2
                        && ws_state.login_count.load(Ordering::SeqCst) >= 2
                        && ws_state.received_subscribe_args().await.len() >= 8
                }
            },
            StdDuration::from_secs(15),
        )
        .await;

        wait_until_async(
            || {
                let rest_state = rest_state.clone();
                async move {
                    rest_state.account_requests.load(Ordering::Relaxed) >= 1
                        && !rest_state.pending_order_queries.lock().await.is_empty()
                        && !rest_state.history_order_queries.lock().await.is_empty()
                        && !rest_state.fill_queries.lock().await.is_empty()
                        && !rest_state.position_queries.lock().await.is_empty()
                }
            },
            StdDuration::from_secs(10),
        )
        .await;

        let events =
            collect_execution_events_until(&mut event_rx, StdDuration::from_secs(5), |events| {
                let has_account = events
                    .iter()
                    .any(|event| matches!(event, ExecutionEvent::Account(_)));
                let order_count = events
                    .iter()
                    .filter(|event| {
                        matches!(event, ExecutionEvent::Report(ExecutionReport::Order(_)))
                    })
                    .count();
                let has_fill = events
                    .iter()
                    .any(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Fill(_))));
                let has_position = events.iter().any(|event| {
                    matches!(event, ExecutionEvent::Report(ExecutionReport::Position(_)))
                });

                has_account && order_count >= 3 && has_fill && has_position
            })
            .await;

        assert_eq!(ws_state.connection_count.load(Ordering::SeqCst), 2);
        assert_eq!(ws_state.login_count.load(Ordering::SeqCst), 2);

        let subscribe_topics = ws_state
            .received_subscribe_args()
            .await
            .into_iter()
            .filter_map(|arg| {
                arg.get("topic")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect::<Vec<_>>();
        for topic in ["account", "order", "strategy-order", "fill", "position"] {
            assert!(
                subscribe_topics
                    .iter()
                    .filter(|value| value.as_str() == topic)
                    .count()
                    >= 2,
                "subscription {topic} was not replayed: {subscribe_topics:?}",
            );
        }

        let order_ids = events
            .iter()
            .filter_map(|event| match event {
                ExecutionEvent::Report(ExecutionReport::Order(report)) => {
                    Some(report.venue_order_id.to_string())
                }
                _ => None,
            })
            .collect::<HashSet<_>>();
        assert!(order_ids.contains("O-WS"));
        assert!(order_ids.contains("O-1"));
        assert!(order_ids.contains("O-2"));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::Account(_)))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ExecutionEvent::Report(ExecutionReport::Fill(_))))
        );
        assert!(events.iter().any(|event| {
            matches!(event, ExecutionEvent::Report(ExecutionReport::Position(_)))
        }));

        ws_client.disconnect().await.unwrap();
        dispatch_handle.abort();
    }
}
