//! Pair Gate's separate order and user-trade channels before reconciling cumulative quantities.

use std::time::Duration;

use nautilus_model::identifiers::TradeId;
use rust_decimal::Decimal;

use super::*;

type OrderKey = (InstrumentId, VenueOrderId);

#[derive(Default)]
pub(super) struct OrderExecutions {
    orders: HashMap<OrderKey, Executions>,
    closed: VecDeque<OrderKey>,
}

#[derive(Default)]
struct Executions {
    latest: Option<OrderStatusReport>,
    pending_status: bool,
    trades: HashSet<TradeId>,
    pending_fills: Vec<FillReport>,
    total: Decimal,
    recovering: bool,
    retired: bool,
}

impl Executions {
    fn on_order(&mut self, report: OrderStatusReport) {
        if self.latest.as_ref().is_some_and(|last| {
            last.filled_qty > report.filled_qty
                || (last.order_status.is_closed() && !report.order_status.is_closed())
                || (last.filled_qty == report.filled_qty && last.ts_last > report.ts_last)
        }) {
            return;
        }
        self.latest = Some(report);
        self.pending_status = true;
    }

    fn on_fill(&mut self, fill: FillReport) {
        if self.trades.insert(fill.trade_id) {
            self.total += fill.last_qty.as_decimal();
            self.pending_fills.push(fill);
        }
    }

    fn flush(&mut self, emitter: &ExecutionEventEmitter) {
        let Some(report) = &self.latest else { return };
        if !self.pending_status || self.total != report.filled_qty.as_decimal() {
            return;
        }
        if self.pending_fills.is_empty() {
            emitter.send_order_status_report(report.clone());
        } else {
            self.pending_fills.sort_by_key(|f| (f.ts_event, f.trade_id));
            emitter.send_order_with_fills(report.clone(), std::mem::take(&mut self.pending_fills));
        }
        self.pending_status = false;
    }

    fn pending(&self) -> bool {
        self.pending_status || !self.pending_fills.is_empty()
    }
}

impl OrderExecutions {
    // Keep active and incomplete orders until they reconcile. Only terminal
    // orders enter the bounded replay cache.
    fn flush(&mut self, key: OrderKey, emitter: &ExecutionEventEmitter) {
        let state = self
            .orders
            .get_mut(&key)
            .expect("updated order must be present");
        state.flush(emitter);
        if !state.pending()
            && !state.retired
            && state
                .latest
                .as_ref()
                .is_some_and(|r| r.order_status.is_closed())
        {
            state.retired = true;
            self.closed.push_back(key);
            if self.closed.len() > MAX_DEDUP_ENTRIES
                && let Some(oldest) = self.closed.pop_front()
                && let Some(old) = self.orders.get_mut(&oldest)
            {
                if old.pending() {
                    old.retired = false;
                } else {
                    self.orders.remove(&oldest);
                }
            }
        }
    }
}

pub(super) fn publish_order(report: OrderStatusReport, ctx: &GateioExecutionWsDispatchContext) {
    update(
        (report.instrument_id, report.venue_order_id),
        ctx,
        |entry| entry.on_order(report),
    );
}

pub(super) fn publish_fill(fill: FillReport, ctx: &GateioExecutionWsDispatchContext) {
    update((fill.instrument_id, fill.venue_order_id), ctx, |entry| {
        entry.on_fill(fill)
    });
}

fn update(
    key: OrderKey,
    ctx: &GateioExecutionWsDispatchContext,
    apply: impl FnOnce(&mut Executions),
) {
    let mut orders = ctx
        .executions
        .lock()
        .expect("Gate.io executions lock poisoned");
    apply(orders.orders.entry(key).or_default());
    orders.flush(key, &ctx.emitter);
    let entry = orders
        .orders
        .get_mut(&key)
        .expect("updated order must be present");
    if !entry.pending() || entry.recovering {
        return;
    }
    entry.recovering = true;
    drop(orders);
    let context = ctx.clone();
    spawn_tracked_execution_task(&ctx.pending_tasks, async move {
        let mut delay = Duration::from_millis(250);
        loop {
            // WS usually supplies both messages. Recover only when one is missing.
            tokio::time::sleep(delay).await;
            {
                let mut orders = context
                    .executions
                    .lock()
                    .expect("Gate.io executions lock poisoned");
                let Some(entry) = orders.orders.get_mut(&key) else {
                    break;
                };
                if !entry.pending() {
                    entry.recovering = false;
                    break;
                }
            }
            if let Err(error) = recover(key, &context).await {
                log::warn!(
                    "Gate.io executions for {} on {} await reconciliation: {error}",
                    key.1,
                    key.0
                );
            }
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    });
}

async fn recover(key: OrderKey, ctx: &GateioExecutionWsDispatchContext) -> anyhow::Result<()> {
    let (instrument_id, venue_id) = key;
    let symbol = raw_symbol(instrument_id);
    let instrument = ctx
        .instruments_by_raw
        .get(&symbol)
        .context("Unknown Gate.io order instrument")?;
    let row = ctx
        .http_client
        .order(ctx.product_type, venue_id.as_str(), &symbol)
        .await?;
    anyhow::ensure!(
        row.id == venue_id.as_str()
            && row.contract.as_deref().or(row.currency_pair.as_deref()) == Some(symbol.as_str()),
        "Unexpected Gate.io order identity"
    );
    let report = report_from_order(
        &row,
        instrument,
        ctx.account_id,
        ctx.clock.get_time_ns(),
        Some(&ctx.client_order_ids),
    )?;
    let fills = load(&ctx.http_client, &report).await?;
    let mut orders = ctx
        .executions
        .lock()
        .expect("Gate.io executions lock poisoned");
    if let Some(entry) = orders.orders.get_mut(&key)
        && entry.pending()
    {
        entry.on_order(report);
        for fill in fills {
            entry.on_fill(fill);
        }
        orders.flush(key, &ctx.emitter);
    }
    Ok(())
}

pub(super) async fn load(
    http: &GateioHttpClient,
    report: &OrderStatusReport,
) -> anyhow::Result<Vec<FillReport>> {
    if report.filled_qty.is_zero() {
        return Ok(Vec::new());
    }
    let id = report.instrument_id;
    let symbol = raw_symbol(id);
    let product = GateioProductType::from_symbol(id.symbol.as_str());
    let instrument = http.instrument(id, product, report.ts_init).await?;
    let venue_id = Some(report.venue_order_id.as_str());
    let rows = if product == GateioProductType::Spot {
        // An order ID does not extend the default seven-day spot trade history.
        // Overlap whole-second bounds so subsecond executions cannot fall between windows.
        let mut from = report.ts_accepted.to_datetime_utc().timestamp();
        let end = report.ts_last.to_datetime_utc().timestamp().max(from) + 1;
        let mut rows = Vec::new();
        loop {
            let to = (from + crate::http::client::MAX_SPOT_TRADE_RANGE_SECS).min(end);
            rows.extend(
                http.raw()
                    .user_trades_for_order(product, Some(&symbol), venue_id, Some((from, to)))
                    .await?,
            );
            if to == end {
                break;
            }
            from = to;
        }
        rows
    } else {
        http.raw()
            .user_trades_for_order(product, Some(&symbol), venue_id, None)
            .await?
    };
    let mut fills = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        anyhow::ensure!(
            row.order_id == report.venue_order_id.as_str()
                && row.currency_pair.as_deref().or(row.contract.as_deref())
                    == Some(symbol.as_str()),
            "Gate.io returned an execution for a different order"
        );
        let mut fill = parse_user_trade(&row, &instrument, report.account_id, report.ts_init)?;
        if seen.insert(fill.trade_id) {
            anyhow::ensure!(
                fill.order_side == report.order_side,
                "Gate.io execution side conflicts with order"
            );
            fill.client_order_id = report.client_order_id;
            fills.push(fill);
        }
    }
    fills.sort_by_key(|f| (f.ts_event, f.trade_id));
    anyhow::ensure!(
        covered(report, fills.iter()),
        "Awaiting real Gate.io executions for {} (cumulative quantity {})",
        report.venue_order_id,
        report.filled_qty
    );
    Ok(fills)
}

pub(super) fn covered<'a>(
    report: &OrderStatusReport,
    fills: impl Iterator<Item = &'a FillReport>,
) -> bool {
    let total: Decimal = fills.map(|fill| fill.last_qty.as_decimal()).sum();
    total == report.filled_qty.as_decimal()
}

pub(super) async fn publish_rest(
    http: &GateioHttpClient,
    emitter: &ExecutionEventEmitter,
    report: OrderStatusReport,
) -> anyhow::Result<()> {
    if report.filled_qty.is_zero() {
        emitter.send_order_status_report(report);
    } else {
        let fills = load(http, &report).await?;
        emitter.send_order_with_fills(report, fills);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nautilus_common::messages::{ExecutionEvent, execution::ExecutionReport};
    use nautilus_model::{enums::AccountType, identifiers::TraderId};
    use serde_json::json;

    use super::*;

    #[rstest::rstest]
    #[case(true)]
    #[case(false)]
    fn real_executions_precede_partial_and_terminal_status(#[case] fill_first: bool) {
        let instrument = super::super::tests::spot_instrument();
        let account = AccountId::from("GATEIO-001");
        let mut emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            TraderId::from("TRADER-001"),
            account,
            AccountType::Cash,
            None,
        );
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);
        let order = serde_json::from_value(json!({"id":"701","text":"t-ABC","currency_pair":"BTC_USDT","side":"buy","type":"limit","amount":"1","left":"0.4","filled_amount":"0.6","price":"50000","status":"open"})).unwrap();
        let partial =
            report_from_order(&order, &instrument, account, UnixNanos::from(1), None).unwrap();
        assert_eq!(partial.order_status, OrderStatus::PartiallyFilled);
        let trade = |id, qty, fee| {
            parse_user_trade(&serde_json::from_value(json!({"id":id, "order_id":"701", "currency_pair":"BTC_USDT", "side":"buy", "price":"50000", "amount":qty,"fee":fee,"fee_currency":"USDT","role":"maker"})).unwrap(), &instrument, account, UnixNanos::from(1)).unwrap()
        };
        let first = trade("11", "0.2", "0.01");
        let second = trade("12", "0.4", "0.03");
        let mut state = Executions::default();
        if !fill_first {
            state.on_order(partial.clone());
            state.flush(&emitter);
            assert!(receiver.try_recv().is_err());
        }
        state.on_fill(first.clone());
        state.on_fill(first.clone());
        state.flush(&emitter);
        assert!(receiver.try_recv().is_err());
        if fill_first {
            state.on_order(partial.clone());
            state.flush(&emitter);
            assert!(receiver.try_recv().is_err());
        }
        state.on_fill(second.clone());
        state.flush(&emitter);
        let ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills)) =
            receiver.try_recv().unwrap()
        else {
            panic!("expected real executions with partial status")
        };
        assert_eq!(*report, partial);
        assert_eq!(fills, vec![first.clone(), second.clone()]);

        let mut canceled = partial.clone();
        canceled.order_status = OrderStatus::Canceled;
        state.on_order(canceled);
        state.flush(&emitter);
        assert!(
            matches!(receiver.try_recv().unwrap(), ExecutionEvent::Report(ExecutionReport::Order(report)) if report.order_status == OrderStatus::Canceled)
        );
        state.on_order(partial);
        state.on_fill(second);
        state.flush(&emitter);
        assert!(receiver.try_recv().is_err());
    }
}
