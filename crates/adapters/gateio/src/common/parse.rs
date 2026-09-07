use std::str::FromStr;

use anyhow::Context;
use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_MILLISECOND};
use nautilus_model::{
    data::{
        Bar, BarType, BookOrder, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate,
        OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick,
    },
    enums::{
        AccountType, AggressorSide, BookAction, LiquiditySide, OrderSide, PositionSideSpecified,
        RecordFlag,
    },
    events::AccountState,
    identifiers::{AccountId, ClientOrderId, InstrumentId, Symbol, TradeId, VenueOrderId},
    instruments::{
        Instrument, InstrumentAny, crypto_perpetual::CryptoPerpetual, currency_pair::CurrencyPair,
    },
    reports::{FillReport, PositionStatusReport},
    types::{
        AccountBalance, Currency, MarginBalance, Money, Price, Quantity, fixed::FIXED_PRECISION,
    },
};
use rust_decimal::Decimal;

use crate::{
    common::{enums::GateioProductType, symbol::GateioSymbol},
    http::models::{
        GateioCandle, GateioContract, GateioFundingRate, GateioOrderBook, GateioPosition,
        GateioSpotPair, GateioTrade, GateioUserTrade,
    },
};

#[must_use]
pub fn currency(code: &str) -> Currency {
    Currency::get_or_create_crypto(code)
}

pub fn decimal(value: impl AsRef<str>, field: &str) -> anyhow::Result<Decimal> {
    let value = value.as_ref().trim();
    anyhow::ensure!(!value.is_empty(), "missing Gate.io {field}");
    Decimal::from_str(value).with_context(|| format!("invalid Gate.io {field}: {value:?}"))
}

pub fn price(value: impl AsRef<str>, precision: u8, field: &str) -> anyhow::Result<Price> {
    ensure_fixed_precision(precision, field)?;
    let value = value.as_ref();
    let parsed = value
        .parse::<f64>()
        .with_context(|| format!("invalid Gate.io {field}: {value:?}"))?;
    anyhow::ensure!(parsed > 0.0, "Gate.io {field} must be positive");
    Price::new_checked(parsed, precision).map_err(|e| anyhow::anyhow!("{field}: {e}"))
}

pub fn quantity(value: impl AsRef<str>, precision: u8, field: &str) -> anyhow::Result<Quantity> {
    ensure_fixed_precision(precision, field)?;
    let value = value.as_ref();
    let parsed = value
        .parse::<f64>()
        .with_context(|| format!("invalid Gate.io {field}: {value:?}"))?;
    anyhow::ensure!(parsed >= 0.0, "Gate.io {field} must be non-negative");
    Quantity::new_checked(parsed, precision).map_err(|e| anyhow::anyhow!("{field}: {e}"))
}

pub fn timestamp_nanos(value: i64) -> anyhow::Result<UnixNanos> {
    let millis = u64::try_from(value).context("negative Gate.io timestamp")?;
    Ok(UnixNanos::from(
        millis
            .checked_mul(NANOSECONDS_IN_MILLISECOND)
            .context("Gate.io timestamp overflow")?,
    ))
}

fn precision(value: &str) -> u8 {
    value
        .split('.')
        .nth(1)
        .map_or(0, |digits| digits.trim_end_matches('0').len() as u8)
}

fn ensure_fixed_precision(precision: u8, field: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        precision <= FIXED_PRECISION,
        "Gate.io {field} precision {precision} exceeds Nautilus maximum {FIXED_PRECISION}",
    );
    Ok(())
}

fn decimal_increment_string(dp: u32) -> String {
    Decimal::new(1, dp).to_string()
}

fn optional_decimal(value: Option<&str>, field: &str) -> anyhow::Result<Option<Decimal>> {
    value
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            Decimal::from_str(value).with_context(|| format!("invalid Gate.io {field}: {value:?}"))
        })
        .transpose()
}

pub fn parse_spot_instrument(
    definition: &GateioSpotPair,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let symbol = GateioSymbol::spot(&definition.id)?;
    let base = definition
        .base
        .as_deref()
        .context("spot pair missing base")?;
    let quote = definition
        .quote
        .as_deref()
        .context("spot pair missing quote")?;
    let price_precision = u8::try_from(definition.precision.unwrap_or(8))
        .context("spot price precision does not fit in u8")?;
    let size_precision = u8::try_from(definition.amount_precision.unwrap_or(8))
        .context("spot size precision does not fit in u8")?;
    ensure_fixed_precision(price_precision, "spot price")?;
    ensure_fixed_precision(size_precision, "spot size")?;
    let price_increment = price(
        decimal_increment_string(u32::from(price_precision)),
        price_precision,
        "spot price increment",
    )?;
    let size_increment = if let Some(amount_point) = definition
        .amount_point
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        quantity(amount_point, size_precision, "amount_point")?
    } else {
        quantity(
            decimal_increment_string(u32::from(size_precision)),
            size_precision,
            "spot size increment",
        )?
    };
    let quote_currency = currency(quote);
    let instrument = CurrencyPair::new_checked(
        symbol.instrument_id(),
        Symbol::new(symbol.raw_symbol()),
        currency(base),
        quote_currency,
        price_precision,
        size_precision,
        price_increment,
        size_increment,
        None,
        None,
        definition
            .max_base_amount
            .as_deref()
            .and_then(|value| quantity(value, size_precision, "max_base_amount").ok()),
        definition
            .min_base_amount
            .as_deref()
            .and_then(|value| quantity(value, size_precision, "min_base_amount").ok()),
        definition
            .max_quote_amount
            .as_deref()
            .and_then(|value| value.parse::<f64>().ok())
            .map(|value| Money::new(value, quote_currency)),
        definition
            .min_quote_amount
            .as_deref()
            .and_then(|value| value.parse::<f64>().ok())
            .map(|value| Money::new(value, quote_currency)),
        None,
        None,
        None,
        None,
        optional_decimal(definition.fee.as_deref(), "spot fee")?,
        optional_decimal(definition.fee.as_deref(), "spot fee")?,
        None,
        None,
        ts_event,
        ts_init,
    )?;
    Ok(InstrumentAny::CurrencyPair(instrument))
}

pub fn parse_perpetual_instrument(
    definition: &GateioContract,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    anyhow::ensure!(
        definition.name.ends_with("_USDT"),
        "Gate.io contract is not USDT settled: {}",
        definition.name
    );
    let symbol = GateioSymbol::usdt_perpetual(&definition.name)?;
    let base = definition
        .name
        .split('_')
        .next()
        .context("invalid contract")?;
    let quote_currency = currency("USDT");
    let price_precision = precision(&definition.order_price_round);
    let size_precision = precision(&definition.order_size_min);
    let multiplier_precision = precision(&definition.quanto_multiplier);
    ensure_fixed_precision(price_precision, "perpetual price")?;
    ensure_fixed_precision(size_precision, "perpetual size")?;
    ensure_fixed_precision(multiplier_precision, "perpetual multiplier")?;
    let price_increment = price(
        &definition.order_price_round,
        price_precision,
        "order_price_round",
    )?;
    let size_increment = quantity(&definition.order_size_min, size_precision, "order_size_min")?;
    let multiplier = quantity(
        &definition.quanto_multiplier,
        multiplier_precision,
        "quanto_multiplier",
    )?;
    let instrument = CryptoPerpetual::new_checked(
        symbol.instrument_id(),
        Symbol::new(symbol.raw_symbol()),
        currency(base),
        quote_currency,
        quote_currency,
        false,
        price_precision,
        size_precision,
        price_increment,
        size_increment,
        Some(multiplier),
        Some(size_increment),
        Some(quantity(
            &definition.order_size_max,
            size_precision,
            "order_size_max",
        )?),
        Some(size_increment),
        None,
        None,
        None,
        None,
        None,
        optional_decimal(definition.maintenance_rate.as_deref(), "maintenance_rate")?,
        optional_decimal(Some(&definition.maker_fee_rate), "maker_fee_rate")?,
        optional_decimal(Some(&definition.taker_fee_rate), "taker_fee_rate")?,
        None,
        None,
        ts_event,
        ts_init,
    )?;
    Ok(InstrumentAny::CryptoPerpetual(instrument))
}

pub fn parse_trade(
    trade: &GateioTrade,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let price_raw = trade
        .price
        .as_deref()
        .context("Gate.io trade missing price")?;
    let price = price(price_raw, instrument.price_precision(), "trade.price")?;
    let size_raw = trade
        .amount
        .as_deref()
        .or(trade.size.as_deref())
        .context("trade missing amount")?;
    let (side_hint, size_raw) = if let Some(stripped) = size_raw.strip_prefix('-') {
        (Some(OrderSide::Sell), stripped)
    } else {
        (None, size_raw)
    };
    let size = quantity(size_raw, instrument.size_precision(), "trade.size")?;
    anyhow::ensure!(!size.is_zero(), "Gate.io trade size must be positive");
    let aggressor_side = match trade.side.as_deref().or(match side_hint {
        Some(OrderSide::Buy) => Some("buy"),
        Some(OrderSide::Sell) => Some("sell"),
        _ => None,
    }) {
        Some("buy") => AggressorSide::Buyer,
        Some("sell") => AggressorSide::Seller,
        _ => AggressorSide::NoAggressor,
    };
    let timestamp_ms = trade.create_time_ms.or(trade.create_time).unwrap_or(0);
    let ts_event = timestamp_nanos(timestamp_ms)?;
    let id = trade
        .id
        .clone()
        .unwrap_or_else(|| format!("{}-{}", instrument.id(), ts_event.as_u64()));
    TradeTick::new_checked(
        instrument.id(),
        price,
        size,
        aggressor_side,
        TradeId::new_checked(&id)?,
        ts_event,
        ts_init,
    )
    .context("failed to construct Gate.io TradeTick")
}

/// Converts a Gate.io private user-trade row into Nautilus' reconciliation format.
///
/// Gate.io spot trades report a positive base-asset amount plus a side field. Futures
/// trades report a signed contract size, so the sign is the source of truth for the
/// order side and the absolute value becomes the Nautilus quantity.
pub fn parse_user_trade(
    trade: &GateioUserTrade,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<FillReport> {
    let venue_order_id = VenueOrderId::new_checked(trade.order_id.trim())
        .context("Gate.io user trade has an invalid order ID")?;
    let trade_id = TradeId::new_checked(trade.id.trim())
        .context("Gate.io user trade has an invalid trade ID")?;

    let is_futures = trade.contract.is_some() || instrument.id().symbol.as_str().ends_with("-PERP");
    let (order_side, quantity_decimal) = if is_futures {
        let signed_size = decimal(&trade.size, "user_trade.size")?;
        anyhow::ensure!(
            !signed_size.is_zero(),
            "Gate.io futures user trade size must be non-zero"
        );
        let side = if signed_size.is_sign_negative() {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };
        (side, signed_size.abs())
    } else {
        let side = match trade
            .side
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            other => anyhow::bail!("unsupported Gate.io spot trade side: {other:?}"),
        };
        (side, decimal(&trade.amount, "user_trade.amount")?.abs())
    };

    let last_qty = Quantity::from_decimal_dp(quantity_decimal, instrument.size_precision())
        .context("invalid Gate.io user trade quantity")?;
    anyhow::ensure!(
        last_qty.is_positive(),
        "Gate.io user trade quantity must be positive"
    );
    let last_px = Price::from_decimal_dp(
        decimal(&trade.price, "user_trade.price")?,
        instrument.price_precision(),
    )
    .context("invalid Gate.io user trade price")?;

    let fee = trade
        .fee
        .as_deref()
        .map(|value| decimal(value, "user_trade.fee"))
        .transpose()?
        .unwrap_or_default();
    let fee_currency = trade
        .fee_currency
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(currency)
        .unwrap_or_else(|| instrument.quote_currency());
    // Gate reports a positive fee as a debit. Keep rebates positive by negating the
    // venue value once, instead of losing the sign when constructing Money.
    let commission =
        Money::from_decimal(-fee, fee_currency).context("invalid Gate.io user trade commission")?;
    let liquidity_side = match trade
        .role
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "maker" => LiquiditySide::Maker,
        "taker" => LiquiditySide::Taker,
        _ => LiquiditySide::NoLiquiditySide,
    };
    let ts_event = trade
        .create_time_ms
        .or(trade.create_time)
        .map(timestamp_nanos)
        .transpose()?
        .unwrap_or(ts_init);
    let client_order_id = trade
        .text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.strip_prefix("t-").unwrap_or(value).trim())
        .and_then(|value| ClientOrderId::new_checked(value).ok());

    Ok(FillReport::new(
        account_id,
        instrument.id(),
        venue_order_id,
        trade_id,
        order_side,
        last_qty,
        last_px,
        commission,
        liquidity_side,
        client_order_id,
        None,
        ts_event,
        ts_init,
        None,
    ))
}

/// Converts a Gate.io USDT-perpetual position into a Nautilus status report.
pub fn parse_position_status_report(
    position: &GateioPosition,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<PositionStatusReport> {
    let signed_size = decimal(&position.size, "position.size")?;
    let position_side = if signed_size.is_zero() {
        PositionSideSpecified::Flat
    } else if signed_size.is_sign_negative() {
        PositionSideSpecified::Short
    } else {
        PositionSideSpecified::Long
    };
    let quantity = Quantity::from_decimal_dp(signed_size.abs(), instrument.size_precision())
        .context("invalid Gate.io position quantity")?;
    let avg_px_open = match position.entry_price.trim() {
        "" | "0" => None,
        value => Some(decimal(value, "position.entry_price")?),
    };
    let ts_last = position
        .update_time
        .or(position.create_time)
        .map(timestamp_nanos)
        .transpose()?
        .unwrap_or(ts_init);

    Ok(PositionStatusReport::new(
        account_id,
        instrument.id(),
        position_side,
        quantity,
        ts_last,
        ts_init,
        None,
        None,
        avg_px_open,
    ))
}

/// Converts a complete Gate.io account response, including private balance updates,
/// into the account event consumed by the execution manager.
pub fn parse_account_state(
    rows: &[crate::http::models::GateioAccount],
    product_type: GateioProductType,
    account_id: AccountId,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<AccountState> {
    let mut balances = Vec::with_capacity(rows.len());
    let mut margins = Vec::new();
    for row in rows {
        let currency = currency(&row.currency);
        let available = decimal(&row.available, "account.available")?;
        if product_type == GateioProductType::Spot {
            let locked = decimal(&row.locked, "account.locked")?;
            let total = available + locked;
            balances.push(
                AccountBalance::from_total_and_locked(total, locked, currency)
                    .context("invalid Gate.io spot account balance")?,
            );
            continue;
        }

        let position_margin = row
            .position_margin
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| decimal(value, "account.position_margin"))
            .transpose()?
            .unwrap_or_default();
        let order_margin = row
            .order_margin
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| decimal(value, "account.order_margin"))
            .transpose()?
            .unwrap_or_default();
        let maintenance_margin = row
            .maintenance_margin
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| decimal(value, "account.maintenance_margin"))
            .transpose()?
            .unwrap_or_default();
        let total = row
            .total
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| decimal(value, "account.total"))
            .transpose()?
            .unwrap_or(available + position_margin + order_margin);

        balances.push(
            AccountBalance::from_total_and_free(total, available, currency)
                .context("invalid Gate.io futures account balance")?,
        );

        let initial_margin = position_margin + order_margin;
        if !initial_margin.is_zero() || !maintenance_margin.is_zero() {
            margins.push(MarginBalance::new(
                Money::from_decimal(initial_margin, currency)?,
                Money::from_decimal(maintenance_margin, currency)?,
                None,
            ));
        }
    }
    let account_type = if product_type == GateioProductType::Spot {
        AccountType::Cash
    } else {
        AccountType::Margin
    };
    Ok(AccountState::new(
        account_id,
        account_type,
        balances,
        margins,
        true,
        nautilus_core::UUID4::new(),
        ts_event,
        ts_init,
        None,
    ))
}

pub fn parse_book(
    book: &GateioOrderBook,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    let sequence = book
        .sequence
        .or(book.id)
        .or_else(|| book.update.and_then(|value| u64::try_from(value).ok()))
        .unwrap_or(0);
    let ts_event = timestamp_nanos(book.current.or(book.update).unwrap_or(0))?;
    let total_levels = book.bids.len() + book.asks.len();
    let mut deltas = vec![OrderBookDelta::clear(
        instrument.id(),
        sequence,
        ts_event,
        ts_init,
    )];
    let mut processed = 0;
    for (side, levels) in [(OrderSide::Buy, &book.bids), (OrderSide::Sell, &book.asks)] {
        for level in levels {
            let p = level.price();
            let q = level.amount();
            processed += 1;
            let is_last = processed == total_levels;
            let flags = if is_last {
                RecordFlag::F_MBP as u8 | RecordFlag::F_LAST as u8
            } else {
                RecordFlag::F_MBP as u8
            };
            deltas.push(OrderBookDelta::new_checked(
                instrument.id(),
                BookAction::Add,
                BookOrder::new(
                    side,
                    price(p, instrument.price_precision(), "book.price")?,
                    book_quantity(q, instrument)?,
                    0,
                ),
                flags,
                sequence,
                ts_event,
                ts_init,
            )?);
        }
    }
    OrderBookDeltas::new_checked(instrument.id(), deltas).context("failed to construct book deltas")
}

/// Returns the venue sequence used to order Gate.io book snapshots and updates.
#[must_use]
pub fn book_sequence(book: &GateioOrderBook) -> u64 {
    book.sequence
        .or(book.id)
        .or_else(|| book.update.and_then(|value| u64::try_from(value).ok()))
        .unwrap_or(0)
}

/// Parses a Gate.io order-book update without clearing the existing book.
///
/// Gate.io sends a zero quantity when a price level must be removed. Non-zero
/// quantities replace the current level at that price.
pub fn parse_book_update(
    book: &GateioOrderBook,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    if book.full {
        return parse_book(book, instrument, ts_init);
    }

    let sequence = book_sequence(book);
    let ts_event = timestamp_nanos(book.current.or(book.update).unwrap_or(0))?;
    let total_levels = book.bids.len() + book.asks.len();
    anyhow::ensure!(
        total_levels > 0,
        "Gate.io order-book update contained no levels"
    );

    let mut deltas = Vec::with_capacity(total_levels);
    let mut processed = 0;
    for (side, levels) in [(OrderSide::Buy, &book.bids), (OrderSide::Sell, &book.asks)] {
        for level in levels {
            let price = price(level.price(), instrument.price_precision(), "book.price")?;
            let size = book_quantity(level.amount(), instrument)?;
            let action = if size.is_zero() {
                BookAction::Delete
            } else {
                BookAction::Update
            };
            processed += 1;
            let mut flags = RecordFlag::F_MBP as u8;
            if processed == total_levels {
                flags |= RecordFlag::F_LAST as u8;
            }
            deltas.push(OrderBookDelta::new_checked(
                instrument.id(),
                action,
                BookOrder::new(side, price, size, 0),
                flags,
                sequence,
                ts_event,
                ts_init,
            )?);
        }
    }
    OrderBookDeltas::new_checked(instrument.id(), deltas)
        .context("failed to construct Gate.io order-book update")
}

fn book_quantity(value: &str, instrument: &InstrumentAny) -> anyhow::Result<Quantity> {
    let value = if instrument.id().symbol.as_str().ends_with("-PERP") {
        decimal(value, "book.size")?.abs().to_string()
    } else {
        value.to_string()
    };
    quantity(value, instrument.size_precision(), "book.size")
}

pub fn parse_mark_price(
    value: &str,
    instrument: &InstrumentAny,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<MarkPriceUpdate> {
    Ok(MarkPriceUpdate::new(
        instrument.id(),
        price(value, instrument.price_precision(), "mark_price")?,
        ts_event,
        ts_init,
    ))
}

pub fn parse_index_price(
    value: &str,
    instrument: &InstrumentAny,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<IndexPriceUpdate> {
    Ok(IndexPriceUpdate::new(
        instrument.id(),
        price(value, instrument.price_precision(), "index_price")?,
        ts_event,
        ts_init,
    ))
}

pub fn parse_funding_rate(
    value: &GateioFundingRate,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<FundingRateUpdate> {
    let rate = Decimal::from_str(value.rate.trim())
        .with_context(|| format!("invalid Gate.io funding rate: {:?}", value.rate))?;
    let ts_event = value
        .timestamp_ms
        .map(timestamp_nanos)
        .transpose()?
        .unwrap_or(ts_init);
    Ok(FundingRateUpdate::new(
        instrument.id(),
        rate,
        None,
        None,
        ts_event,
        ts_init,
    ))
}

pub fn parse_quote(
    bid: &str,
    ask: &str,
    bid_size: &str,
    ask_size: &str,
    instrument: &InstrumentAny,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    QuoteTick::new_checked(
        instrument.id(),
        price(bid, instrument.price_precision(), "quote.bid")?,
        price(ask, instrument.price_precision(), "quote.ask")?,
        quantity(bid_size, instrument.size_precision(), "quote.bid_size")?,
        quantity(ask_size, instrument.size_precision(), "quote.ask_size")?,
        ts_event,
        ts_init,
    )
}

pub fn parse_candle(
    candle: &GateioCandle,
    instrument: &InstrumentAny,
    bar_type: BarType,
    ts_init: UnixNanos,
) -> anyhow::Result<Bar> {
    Bar::new_checked(
        bar_type,
        price(&candle.open, instrument.price_precision(), "candle.open")?,
        price(&candle.high, instrument.price_precision(), "candle.high")?,
        price(&candle.low, instrument.price_precision(), "candle.low")?,
        price(&candle.close, instrument.price_precision(), "candle.close")?,
        quantity(&candle.volume, instrument.size_precision(), "candle.volume")?,
        timestamp_nanos(candle.timestamp_ms)?,
        ts_init,
    )
    .context("failed to construct Gate.io Bar")
}

pub fn product_for_instrument(id: InstrumentId) -> GateioProductType {
    GateioProductType::from_symbol(id.symbol.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::models::{GateioLevel, GateioPosition, GateioSpotPair};

    fn spot_instrument() -> InstrumentAny {
        let definition: GateioSpotPair = serde_json::from_value(serde_json::json!({
            "id": "BTC_USDT",
            "base": "BTC",
            "quote": "USDT",
            "precision": 2,
            "amount_precision": 3,
            "amount_point": "0.001",
            "min_base_amount": "0.005",
            "max_base_amount": "100",
            "min_quote_amount": "1",
            "max_quote_amount": "100000",
            "fee": "0.001"
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
            "taker_fee_rate": "0.00075",
            "maintenance_rate": "0.003",
            "status": "trading"
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
    fn parses_spot_instrument_rules() {
        let instrument = spot_instrument();
        let InstrumentAny::CurrencyPair(instrument) = instrument else {
            panic!("expected a spot currency pair");
        };

        assert_eq!(instrument.id.to_string(), "BTC_USDT.GATEIO");
        assert_eq!(instrument.base_currency.code.as_str(), "BTC");
        assert_eq!(instrument.quote_currency.code.as_str(), "USDT");
        assert_eq!(instrument.price_increment, Price::from("0.01"));
        assert_eq!(instrument.size_increment, Quantity::from("0.001"));
        assert_eq!(instrument.min_quantity, Some(Quantity::from("0.005")));
        assert_eq!(instrument.max_quantity, Some(Quantity::from("100")));
        assert_eq!(
            instrument.min_notional.unwrap().as_decimal(),
            Decimal::from_str("1").unwrap()
        );
        assert_eq!(instrument.maker_fee, Decimal::from_str("0.001").unwrap());
        assert_eq!(instrument.taker_fee, Decimal::from_str("0.001").unwrap());
    }

    #[test]
    fn parses_usdt_perpetual_rules() {
        let instrument = futures_instrument();
        let InstrumentAny::CryptoPerpetual(instrument) = instrument else {
            panic!("expected a perpetual instrument");
        };

        assert_eq!(instrument.id.to_string(), "BTC_USDT-PERP.GATEIO");
        assert_eq!(instrument.base_currency.code.as_str(), "BTC");
        assert_eq!(instrument.quote_currency.code.as_str(), "USDT");
        assert_eq!(instrument.settlement_currency.code.as_str(), "USDT");
        assert!(!instrument.is_inverse);
        assert_eq!(instrument.price_increment, Price::from("0.1"));
        assert_eq!(instrument.size_increment, Quantity::from("1"));
        assert_eq!(instrument.multiplier, Quantity::from("0.0001"));
        assert_eq!(instrument.min_quantity, Some(Quantity::from("1")));
        assert_eq!(instrument.max_quantity, Some(Quantity::from("100000")));
        assert_eq!(instrument.maker_fee, Decimal::from_str("-0.0001").unwrap());
        assert_eq!(instrument.taker_fee, Decimal::from_str("0.00075").unwrap());
    }

    #[test]
    fn rejects_spot_price_precision_above_nautilus_limit_without_panicking() {
        let definition: GateioSpotPair = serde_json::from_value(serde_json::json!({
            "id": "PEIPEI_USDT",
            "base": "PEIPEI",
            "quote": "USDT",
            "precision": FIXED_PRECISION as u32 + 1,
            "amount_precision": 6,
            "amount_point": "0.000001"
        }))
        .unwrap();

        let result = std::panic::catch_unwind(|| {
            parse_spot_instrument(
                &definition,
                UnixNanos::from(1_000_000_000),
                UnixNanos::from(1_000_000_000),
            )
        });

        assert!(result.is_ok(), "invalid Gate metadata must not panic");
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn rejects_spot_size_precision_above_nautilus_limit_without_panicking() {
        let definition: GateioSpotPair = serde_json::from_value(serde_json::json!({
            "id": "BTC_USDT",
            "base": "BTC",
            "quote": "USDT",
            "precision": 2,
            "amount_precision": FIXED_PRECISION as u32 + 1,
            "amount_point": "0.0000000000000001"
        }))
        .unwrap();

        let result = std::panic::catch_unwind(|| {
            parse_spot_instrument(
                &definition,
                UnixNanos::from(1_000_000_000),
                UnixNanos::from(1_000_000_000),
            )
        });

        assert!(result.is_ok(), "invalid Gate metadata must not panic");
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn rejects_perpetual_price_precision_above_nautilus_limit_without_panicking() {
        let over_precision_decimal = format!("0.{}1", "0".repeat(FIXED_PRECISION as usize));
        let definition: GateioContract = serde_json::from_value(serde_json::json!({
            "name": "CHEEMS_USDT",
            "quanto_multiplier": "1",
            "order_price_round": over_precision_decimal,
            "order_size_min": 1,
            "order_size_max": 100000,
            "maker_fee_rate": "-0.0001",
            "taker_fee_rate": "0.00075"
        }))
        .unwrap();

        let result = std::panic::catch_unwind(|| {
            parse_perpetual_instrument(
                &definition,
                UnixNanos::from(1_000_000_000),
                UnixNanos::from(1_000_000_000),
            )
        });

        assert!(result.is_ok(), "invalid Gate metadata must not panic");
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn rejects_perpetual_multiplier_precision_above_nautilus_limit_without_panicking() {
        let over_precision_decimal = format!("0.{}1", "0".repeat(FIXED_PRECISION as usize));
        let definition: GateioContract = serde_json::from_value(serde_json::json!({
            "name": "BTC_USDT",
            "quanto_multiplier": over_precision_decimal,
            "order_price_round": "0.1",
            "order_size_min": 1,
            "order_size_max": 100000,
            "maker_fee_rate": "-0.0001",
            "taker_fee_rate": "0.00075"
        }))
        .unwrap();

        let result = std::panic::catch_unwind(|| {
            parse_perpetual_instrument(
                &definition,
                UnixNanos::from(1_000_000_000),
                UnixNanos::from(1_000_000_000),
            )
        });

        assert!(result.is_ok(), "invalid Gate metadata must not panic");
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn maps_spot_and_futures_user_trades() {
        let spot = spot_instrument();
        let spot_trade = GateioUserTrade {
            id: "spot-trade-1".to_string(),
            order_id: "spot-order-1".to_string(),
            currency_pair: Some("BTC_USDT".to_string()),
            contract: None,
            price: "50000".to_string(),
            amount: "0.250".to_string(),
            size: String::new(),
            fee: Some("0.00025".to_string()),
            fee_currency: Some("USDT".to_string()),
            create_time_ms: Some(1_700_000_000_000),
            create_time: None,
            side: Some("sell".to_string()),
            role: Some("maker".to_string()),
            text: Some("t-CLIENT-1".to_string()),
        };
        let spot_report = parse_user_trade(
            &spot_trade,
            &spot,
            AccountId::from("GATEIO-001"),
            UnixNanos::from(2_000_000_000),
        )
        .unwrap();
        assert_eq!(spot_report.order_side, OrderSide::Sell);
        assert_eq!(spot_report.last_qty, Quantity::from("0.250"));
        assert_eq!(
            spot_report.commission.as_decimal(),
            Decimal::from_str("-0.00025").unwrap()
        );
        assert_eq!(spot_report.liquidity_side, LiquiditySide::Maker);
        assert_eq!(
            spot_report.client_order_id,
            Some(ClientOrderId::from("CLIENT-1"))
        );

        let futures = futures_instrument();
        let futures_trade = GateioUserTrade {
            id: "futures-trade-1".to_string(),
            order_id: "futures-order-1".to_string(),
            currency_pair: None,
            contract: Some("BTC_USDT".to_string()),
            price: "50000".to_string(),
            amount: String::new(),
            size: "-3".to_string(),
            fee: Some("0.75".to_string()),
            fee_currency: Some("USDT".to_string()),
            create_time_ms: None,
            create_time: Some(1_700_000_000),
            side: None,
            role: Some("taker".to_string()),
            text: None,
        };
        let futures_report = parse_user_trade(
            &futures_trade,
            &futures,
            AccountId::from("GATEIO-001"),
            UnixNanos::from(2_000_000_000),
        )
        .unwrap();
        assert_eq!(futures_report.order_side, OrderSide::Sell);
        assert_eq!(futures_report.last_qty, Quantity::from("3"));
        assert_eq!(
            futures_report.commission.as_decimal(),
            Decimal::from_str("-0.75").unwrap()
        );
        assert_eq!(futures_report.liquidity_side, LiquiditySide::Taker);
    }

    #[test]
    fn maps_signed_position_to_long_short_and_flat() {
        let instrument = futures_instrument();
        for (raw_size, expected_side) in [
            ("3", PositionSideSpecified::Long),
            ("-3", PositionSideSpecified::Short),
            ("0", PositionSideSpecified::Flat),
        ] {
            let position = GateioPosition {
                contract: "BTC_USDT".to_string(),
                size: raw_size.to_string(),
                value: "0".to_string(),
                entry_price: "50000".to_string(),
                mark_price: "50000".to_string(),
                update_time: Some(1_700_000_000_000),
                create_time: None,
            };
            let report = parse_position_status_report(
                &position,
                &instrument,
                AccountId::from("GATEIO-001"),
                UnixNanos::from(2_000_000_000),
            )
            .unwrap();
            assert_eq!(report.position_side, expected_side);
            assert_eq!(
                report.quantity,
                Quantity::from(raw_size.trim_start_matches('-'))
            );
        }
    }

    #[test]
    fn parses_snapshot_and_updates_with_futures_signed_book_sizes() {
        let instrument = futures_instrument();
        let snapshot = GateioOrderBook {
            id: Some(10),
            full: true,
            current: Some(1_700_000_000_000),
            update: None,
            sequence: None,
            first_sequence: None,
            currency_pair: None,
            contract: Some("BTC_USDT".to_string()),
            bids: vec![GateioLevel::Futures {
                price: "50000".to_string(),
                amount: "3".to_string(),
            }],
            asks: vec![GateioLevel::Futures {
                price: "50001".to_string(),
                amount: "-2".to_string(),
            }],
        };
        let snapshot_deltas =
            parse_book(&snapshot, &instrument, UnixNanos::from(2_000_000_000)).unwrap();
        assert_eq!(snapshot_deltas.deltas.len(), 3);
        assert_eq!(snapshot_deltas.deltas[1].action, BookAction::Add);
        assert_eq!(snapshot_deltas.deltas[2].order.size, Quantity::from("2"));

        let update = GateioOrderBook {
            id: None,
            full: false,
            current: Some(1_700_000_000_100),
            update: None,
            sequence: Some(11),
            first_sequence: Some(11),
            currency_pair: None,
            contract: Some("BTC_USDT".to_string()),
            bids: vec![GateioLevel::Futures {
                price: "50000".to_string(),
                amount: "4".to_string(),
            }],
            asks: vec![GateioLevel::Futures {
                price: "50001".to_string(),
                amount: "0".to_string(),
            }],
        };
        let update_deltas =
            parse_book_update(&update, &instrument, UnixNanos::from(2_000_000_000)).unwrap();
        assert_eq!(update_deltas.deltas[0].action, BookAction::Update);
        assert_eq!(update_deltas.deltas[0].order.size, Quantity::from("4"));
        assert_eq!(update_deltas.deltas[1].action, BookAction::Delete);
        assert_eq!(update_deltas.deltas[1].order.size, Quantity::from("0"));
    }
}
