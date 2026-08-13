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

//! Conversion functions that translate Bitget API schemas into Nautilus types.

use std::str::FromStr;

use anyhow::Context;
use nautilus_core::{UUID4, datetime::NANOSECONDS_IN_MILLISECOND, nanos::UnixNanos};
use nautilus_model::{
    data::{
        Bar, BarType, BookOrder, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate,
        OrderBookDelta, OrderBookDeltas, OrderBookDepth10, QuoteTick, TradeTick,
        depth::DEPTH10_LEN,
    },
    enums::{
        AccountType, AggressorSide, BarAggregation, BookAction, LiquiditySide, MarketStatusAction,
        OrderSide, OrderStatus, OrderType, PositionSideSpecified, RecordFlag, TimeInForce,
    },
    events::AccountState,
    identifiers::{AccountId, ClientOrderId, PositionId, Symbol, TradeId, VenueOrderId},
    instruments::{
        Instrument, any::InstrumentAny, crypto_perpetual::CryptoPerpetual,
        currency_pair::CurrencyPair,
    },
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, QUANTITY_MAX, Quantity},
};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use crate::{
    common::{enums::BitgetProductType, symbol::BitgetSymbol},
    http::models::{
        BitgetCandle, BitgetDecimalValue, BitgetFill, BitgetFillFee, BitgetFillFeeDetail,
        BitgetFundingRate, BitgetMarketTrade, BitgetMixAccount, BitgetMixContract,
        BitgetMixPosition, BitgetOrderBookSnapshot, BitgetOrderStatus, BitgetSpotAsset,
        BitgetSpotSymbol,
    },
    websocket::messages::{BitgetBookData, BitgetBookLevel, BitgetTickerData},
};

fn default_margin() -> Decimal {
    Decimal::new(1, 1)
}

/// Returns a currency from the internal map or creates a new crypto currency.
#[must_use]
pub fn get_currency(code: &str) -> Currency {
    Currency::get_or_create_crypto(code)
}

fn parse_decimal(raw: Option<&str>, field: &str) -> anyhow::Result<Decimal> {
    match raw.map(str::trim).filter(|s| !s.is_empty()) {
        Some(value) => Decimal::from_str(value)
            .with_context(|| format!("invalid decimal for {field}: {value:?}")),
        None => Ok(Decimal::ZERO),
    }
}

fn parse_price(raw: &str, field: &str) -> anyhow::Result<Price> {
    Price::from_str(raw).map_err(|e| anyhow::anyhow!("invalid price for {field}: {raw:?}: {e}"))
}

fn parse_quantity(raw: &str, field: &str) -> anyhow::Result<Quantity> {
    Quantity::from_str(raw)
        .map_err(|e| anyhow::anyhow!("invalid quantity for {field}: {raw:?}: {e}"))
}

fn parse_price_with_precision(raw: &str, precision: u8, field: &str) -> anyhow::Result<Price> {
    let decimal = Decimal::from_str(raw)
        .with_context(|| format!("invalid decimal price for {field}: {raw:?}"))?;
    let value = decimal
        .to_f64()
        .with_context(|| format!("price out of f64 range for {field}: {raw:?}"))?;
    anyhow::ensure!(value > 0.0, "price must be positive for {field}: {raw:?}");
    Price::new_checked(value, precision)
        .map_err(|e| anyhow::anyhow!("invalid price for {field}: {raw:?}: {e}"))
}

fn parse_quantity_with_precision(
    raw: &str,
    precision: u8,
    field: &str,
) -> anyhow::Result<Quantity> {
    let decimal = Decimal::from_str(raw)
        .with_context(|| format!("invalid decimal quantity for {field}: {raw:?}"))?;
    let value = decimal
        .to_f64()
        .with_context(|| format!("quantity out of f64 range for {field}: {raw:?}"))?;
    anyhow::ensure!(
        value >= 0.0,
        "quantity must be non-negative for {field}: {raw:?}"
    );
    Quantity::new_checked(value, precision)
        .map_err(|e| anyhow::anyhow!("invalid quantity for {field}: {raw:?}: {e}"))
}

fn empty_book_order(side: OrderSide, price_precision: u8, size_precision: u8) -> BookOrder {
    BookOrder::new(
        side,
        Price::zero(price_precision),
        Quantity::zero(size_precision),
        0,
    )
}

fn parse_millis_timestamp(raw: &str, field: &str) -> anyhow::Result<UnixNanos> {
    let millis = raw
        .trim()
        .parse::<u64>()
        .with_context(|| format!("invalid millisecond timestamp for {field}: {raw:?}"))?;
    let nanos = millis
        .checked_mul(NANOSECONDS_IN_MILLISECOND)
        .with_context(|| format!("timestamp overflow for {field}: {raw:?}"))?;
    Ok(UnixNanos::from(nanos))
}

fn optional_millis_timestamp(raw: Option<&str>, field: &str) -> anyhow::Result<Option<UnixNanos>> {
    raw.filter(|value| !value.trim().is_empty())
        .map(|value| parse_millis_timestamp(value, field))
        .transpose()
}

fn decimal_value_as_string(value: &BitgetDecimalValue, field: &str) -> anyhow::Result<String> {
    let value = value.as_decimal_str();
    anyhow::ensure!(
        !value.trim().is_empty(),
        "missing decimal value for {field}"
    );
    Ok(value)
}

fn parse_optional_quantity(raw: Option<&str>, field: &str) -> anyhow::Result<Option<Quantity>> {
    let Some(value) = raw.map(str::trim).filter(|s| !s.is_empty() && *s != "0") else {
        return Ok(None);
    };
    Ok(Some(parse_quantity(value, field)?))
}

fn parse_optional_max_quantity(raw: Option<&str>, field: &str) -> anyhow::Result<Option<Quantity>> {
    let Some(value) = raw.map(str::trim).filter(|s| !s.is_empty() && *s != "0") else {
        return Ok(None);
    };

    let decimal = Decimal::from_str(value)
        .with_context(|| format!("invalid decimal quantity for {field}: {value:?}"))?;
    anyhow::ensure!(
        decimal >= Decimal::ZERO,
        "quantity must be non-negative for {field}: {value:?}"
    );

    let quantity_max =
        Decimal::from_f64(QUANTITY_MAX).expect("QUANTITY_MAX should be representable as Decimal");
    if decimal > quantity_max {
        return Ok(None);
    }

    Ok(Some(parse_quantity(value, field)?))
}

fn parse_optional_money(
    raw: Option<&str>,
    currency: Currency,
    field: &str,
) -> anyhow::Result<Option<Money>> {
    let Some(value) = raw.map(str::trim).filter(|s| !s.is_empty() && *s != "0") else {
        return Ok(None);
    };
    let amount: f64 = value
        .parse()
        .with_context(|| format!("invalid money amount for {field}: {value:?}"))?;
    if !amount.is_finite() || amount <= 0.0 {
        return Ok(None);
    }
    Ok(Some(Money::new(amount, currency)))
}

fn optional_str<'a>(values: impl IntoIterator<Item = Option<&'a String>>) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(String::as_str)
        .find(|value| !value.trim().is_empty())
}

fn normalize_bitget_token(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['-', ' '], "_")
}

fn parse_bitget_bool(raw: Option<&str>) -> bool {
    raw.is_some_and(|value| {
        matches!(
            normalize_bitget_token(value).as_str(),
            "true" | "yes" | "y" | "1"
        )
    })
}

fn parse_order_side(raw: &str) -> anyhow::Result<OrderSide> {
    match normalize_bitget_token(raw).as_str() {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        other => anyhow::bail!("unsupported Bitget order side: {other:?}"),
    }
}

fn parse_order_type(raw: Option<&str>, trigger_price: Option<&str>) -> anyhow::Result<OrderType> {
    let Some(raw) = raw else {
        return Ok(if trigger_price.is_some() {
            OrderType::StopMarket
        } else {
            OrderType::Limit
        });
    };

    match normalize_bitget_token(raw).as_str() {
        "market" => Ok(if trigger_price.is_some() {
            OrderType::StopMarket
        } else {
            OrderType::Market
        }),
        "limit" => Ok(if trigger_price.is_some() {
            OrderType::StopLimit
        } else {
            OrderType::Limit
        }),
        "stop_market" | "trigger_market" => Ok(OrderType::StopMarket),
        "stop_limit" | "trigger_limit" => Ok(OrderType::StopLimit),
        "mit" | "market_if_touched" => Ok(OrderType::MarketIfTouched),
        "lit" | "limit_if_touched" => Ok(OrderType::LimitIfTouched),
        other => anyhow::bail!("unsupported Bitget order type: {other:?}"),
    }
}

fn parse_time_in_force(raw: Option<&str>) -> anyhow::Result<(TimeInForce, bool)> {
    let Some(raw) = raw else {
        return Ok((TimeInForce::Gtc, false));
    };

    match normalize_bitget_token(raw).as_str() {
        "normal" | "gtc" => Ok((TimeInForce::Gtc, false)),
        "post_only" | "postonly" => Ok((TimeInForce::Gtc, true)),
        "ioc" => Ok((TimeInForce::Ioc, false)),
        "fok" => Ok((TimeInForce::Fok, false)),
        other => anyhow::bail!("unsupported Bitget time in force: {other:?}"),
    }
}

fn parse_order_status(raw: &str, filled_qty: Quantity) -> anyhow::Result<OrderStatus> {
    match normalize_bitget_token(raw).as_str() {
        "new" | "init" | "live" | "not_trigger" | "not_triggered" | "untriggered" => {
            Ok(OrderStatus::Accepted)
        }
        "triggered" => Ok(OrderStatus::Triggered),
        "partial_fill" | "partially_filled" | "part_filled" => Ok(OrderStatus::PartiallyFilled),
        "filled" | "full_fill" | "full_filled" => Ok(OrderStatus::Filled),
        "cancelled" | "canceled" | "cancel" | "cancelled_oco" | "partially_filled_canceled" => {
            Ok(OrderStatus::Canceled)
        }
        "expired" => Ok(OrderStatus::Expired),
        "rejected" | "fail" | "failed" => {
            if filled_qty.is_positive() {
                Ok(OrderStatus::Canceled)
            } else {
                Ok(OrderStatus::Rejected)
            }
        }
        other => anyhow::bail!("unsupported Bitget order status: {other:?}"),
    }
}

fn fill_fee_entry_amount_and_coin(entry: &BitgetFillFee) -> Option<(String, Option<String>)> {
    let amount = entry
        .total_fee
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let coin = entry
        .fee_coin
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);

    Some((amount.to_string(), coin))
}

fn fill_fee_detail_amount_and_coin(
    detail: &BitgetFillFeeDetail,
) -> Option<(String, Option<String>)> {
    match detail {
        BitgetFillFeeDetail::List(entries) => {
            entries.iter().find_map(fill_fee_entry_amount_and_coin)
        }
        BitgetFillFeeDetail::Entry(entry) => fill_fee_entry_amount_and_coin(entry),
        BitgetFillFeeDetail::Raw(raw) => {
            let raw = raw.trim();
            if raw.is_empty() {
                return None;
            }

            if raw.starts_with('[') || raw.starts_with('{') {
                serde_json::from_str::<BitgetFillFeeDetail>(raw)
                    .ok()
                    .and_then(|parsed| fill_fee_detail_amount_and_coin(&parsed))
            } else {
                Some((raw.to_string(), None))
            }
        }
    }
}

fn fill_fee_amount_and_coin(fill: &BitgetFill) -> (String, Option<String>) {
    fill.fee_detail
        .as_ref()
        .and_then(fill_fee_detail_amount_and_coin)
        .or_else(|| {
            let amount = optional_str([fill.fee.as_ref()])?.to_string();
            let coin = optional_str([fill.fee_coin.as_ref(), fill.margin_coin.as_ref()])
                .map(ToString::to_string);
            Some((amount, coin))
        })
        .unwrap_or_else(|| {
            (
                "0".to_string(),
                optional_str([fill.fee_coin.as_ref(), fill.margin_coin.as_ref()])
                    .map(ToString::to_string),
            )
        })
}

fn parse_fill_commission(fill: &BitgetFill, instrument: &InstrumentAny) -> anyhow::Result<Money> {
    let (fee_raw, fee_coin) = fill_fee_amount_and_coin(fill);
    let fee_decimal = Decimal::from_str(fee_raw.trim())
        .with_context(|| format!("invalid decimal fill fee: {fee_raw:?}"))?;
    let commission_decimal = if fee_decimal < Decimal::ZERO {
        -fee_decimal
    } else {
        fee_decimal
    };
    let fee_currency = fee_coin
        .as_deref()
        .map(str::trim)
        .filter(|coin| !coin.is_empty())
        .map(get_currency)
        .unwrap_or_else(|| instrument.settlement_currency());

    Money::from_decimal(commission_decimal, fee_currency)
        .with_context(|| format!("invalid Bitget fill commission: {fee_raw:?}"))
}

fn parse_liquidity_side(
    is_maker: Option<bool>,
    raw: Option<&str>,
) -> anyhow::Result<LiquiditySide> {
    if let Some(is_maker) = is_maker {
        return Ok(if is_maker {
            LiquiditySide::Maker
        } else {
            LiquiditySide::Taker
        });
    }

    let Some(raw) = raw else {
        return Ok(LiquiditySide::NoLiquiditySide);
    };

    match normalize_bitget_token(raw).as_str() {
        "maker" | "make" | "m" | "add_liquidity" => Ok(LiquiditySide::Maker),
        "taker" | "take" | "t" | "remove_liquidity" => Ok(LiquiditySide::Taker),
        "" => Ok(LiquiditySide::NoLiquiditySide),
        other => anyhow::bail!("unsupported Bitget liquidity side: {other:?}"),
    }
}

fn precision_to_increment(
    precision: Option<&str>,
    default_precision: u8,
) -> anyhow::Result<String> {
    let precision = match precision.map(str::trim).filter(|s| !s.is_empty()) {
        Some(raw) => raw
            .parse::<u8>()
            .with_context(|| format!("invalid precision value: {raw:?}"))?,
        None => default_precision,
    };

    Ok(if precision == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(usize::from(precision - 1)))
    })
}

fn futures_price_increment(contract: &BitgetMixContract) -> anyhow::Result<String> {
    let price_place = contract
        .price_place
        .as_deref()
        .unwrap_or("0")
        .trim()
        .parse::<u8>()
        .context("invalid pricePlace")?;
    let end_step_raw = contract.price_end_step.as_deref().unwrap_or("1").trim();

    if end_step_raw.contains('.') {
        return Ok(end_step_raw.to_string());
    }

    let end_step = end_step_raw
        .parse::<u64>()
        .context("invalid priceEndStep")?;

    if price_place == 0 {
        Ok(end_step.to_string())
    } else {
        let mut digits = end_step.to_string();
        let place = usize::from(price_place);
        if digits.len() <= place {
            digits = format!("{}{}", "0".repeat(place + 1 - digits.len()), digits);
        }
        let split = digits.len() - place;
        Ok(format!("{}.{}", &digits[..split], &digits[split..]))
    }
}

/// Parses a Bitget Spot symbol definition into a Nautilus [`CurrencyPair`].
pub fn parse_spot_instrument(
    definition: &BitgetSpotSymbol,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    anyhow::ensure!(
        !definition.symbol.is_empty(),
        "Bitget spot symbol cannot be empty"
    );
    anyhow::ensure!(
        !definition.base_coin.is_empty(),
        "baseCoin is empty for symbol '{}'",
        definition.symbol
    );
    anyhow::ensure!(
        !definition.quote_coin.is_empty(),
        "quoteCoin is empty for symbol '{}'",
        definition.symbol
    );

    let base_currency = get_currency(&definition.base_coin);
    let quote_currency = get_currency(&definition.quote_coin);
    let symbol = BitgetSymbol::spot(&definition.symbol)?;
    let instrument_id = symbol.to_instrument_id();
    let raw_symbol = Symbol::new(symbol.raw_symbol());

    let price_increment = parse_price(
        &precision_to_increment(definition.price_precision.as_deref(), 8)?,
        "pricePrecision",
    )?;
    let size_increment = parse_quantity(
        &precision_to_increment(definition.quantity_precision.as_deref(), 8)?,
        "quantityPrecision",
    )?;

    let maker_fee = parse_decimal(definition.maker_fee_rate.as_deref(), "makerFeeRate")?;
    let taker_fee = parse_decimal(definition.taker_fee_rate.as_deref(), "takerFeeRate")?;

    let instrument = CurrencyPair::new(
        instrument_id,
        raw_symbol,
        base_currency,
        quote_currency,
        price_increment.precision,
        size_increment.precision,
        price_increment,
        size_increment,
        None,
        Some(size_increment),
        parse_optional_max_quantity(definition.max_trade_amount.as_deref(), "maxOrderQty")?,
        parse_optional_quantity(definition.min_trade_amount.as_deref(), "minOrderQty")?,
        None,
        parse_optional_money(
            definition.min_trade_usdt.as_deref(),
            quote_currency,
            "minOrderAmount",
        )?,
        None,
        None,
        Some(default_margin()),
        Some(default_margin()),
        Some(maker_fee),
        Some(taker_fee),
        None,
        None,
        ts_event,
        ts_init,
    );

    Ok(InstrumentAny::CurrencyPair(instrument))
}

/// Parses a Bitget USDT-FUTURES perpetual definition into a Nautilus [`CryptoPerpetual`].
pub fn parse_usdt_perp_instrument(
    definition: &BitgetMixContract,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    anyhow::ensure!(
        definition
            .product_type
            .as_deref()
            .is_none_or(|p| p.eq_ignore_ascii_case("USDT-FUTURES")),
        "unsupported Bitget category for '{}': {:?}",
        definition.symbol,
        definition.product_type,
    );
    let contract_type = definition.contract_type.as_deref();
    anyhow::ensure!(
        contract_type.is_none_or(|s| s.eq_ignore_ascii_case("perpetual")),
        "unsupported Bitget contract type for '{}': {:?}",
        definition.symbol,
        contract_type,
    );
    anyhow::ensure!(
        !definition.symbol.is_empty(),
        "Bitget futures symbol cannot be empty"
    );

    let base_currency = get_currency(&definition.base_coin);
    let quote_currency = get_currency(&definition.quote_coin);
    let settlement_currency = get_currency(definition.margin_coin.as_deref().unwrap_or("USDT"));
    let symbol = BitgetSymbol::usdt_perp(&definition.symbol)?;
    let instrument_id = symbol.to_instrument_id();
    let raw_symbol = Symbol::new(symbol.raw_symbol());

    let price_increment = parse_price(&futures_price_increment(definition)?, "priceMultiplier")?;
    let size_increment_raw = match definition
        .size_multiplier
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(raw) => raw.to_string(),
        None => precision_to_increment(definition.volume_place.as_deref(), 8)?,
    };
    let size_increment = parse_quantity(&size_increment_raw, "quantityMultiplier")?;
    let maker_fee = parse_decimal(definition.maker_fee_rate.as_deref(), "makerFeeRate")?;
    let taker_fee = parse_decimal(definition.taker_fee_rate.as_deref(), "takerFeeRate")?;

    let instrument = CryptoPerpetual::new(
        instrument_id,
        raw_symbol,
        base_currency,
        quote_currency,
        settlement_currency,
        false,
        price_increment.precision,
        size_increment.precision,
        price_increment,
        size_increment,
        None,
        Some(size_increment),
        parse_optional_max_quantity(definition.max_order_qty.as_deref(), "maxOrderQty")?,
        parse_optional_quantity(definition.min_trade_num.as_deref(), "minOrderQty")?,
        None,
        parse_optional_money(
            definition.min_trade_usdt.as_deref(),
            quote_currency,
            "minOrderAmount",
        )?,
        None,
        None,
        Some(default_margin()),
        Some(default_margin()),
        Some(maker_fee),
        Some(taker_fee),
        None,
        None,
        ts_event,
        ts_init,
    );

    Ok(InstrumentAny::CryptoPerpetual(instrument))
}

/// Maps a Bitget UTA instrument status token to a Nautilus market status action.
#[must_use]
pub fn bitget_symbol_status_action(status: Option<&str>) -> MarketStatusAction {
    match status
        .map(normalize_bitget_token)
        .unwrap_or_else(|| "unknown".to_string())
        .as_str()
    {
        "online" | "normal" | "trading" => MarketStatusAction::Trading,
        "gray" | "pre_online" | "preonline" | "pre_launch" | "prelaunch" => {
            MarketStatusAction::PreOpen
        }
        "limit_open" | "limitopen" | "restricted" => MarketStatusAction::Pause,
        "maintain" | "maintenance" | "halt" | "halted" | "suspend" | "suspended" => {
            MarketStatusAction::Halt
        }
        "offline" | "delisted" | "closed" | "close" => MarketStatusAction::Close,
        _ => MarketStatusAction::NotAvailableForTrading,
    }
}

/// Parses a Bitget REST order book snapshot into Nautilus order book deltas.
pub fn parse_orderbook_snapshot(
    snapshot: &BitgetOrderBookSnapshot,
    instrument: &InstrumentAny,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<OrderBookDeltas> {
    let ts_event = match snapshot
        .ts
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        Some(ts) => parse_millis_timestamp(ts, "orderbook.ts")?,
        None => ts_init.context("Bitget order book snapshot did not include ts")?,
    };
    let ts_init = ts_init.unwrap_or(ts_event);
    let sequence = match snapshot
        .seq
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        Some(seq) => seq
            .parse::<u64>()
            .with_context(|| format!("invalid orderbook.seq: {seq:?}"))?,
        None => ts_event.as_u64(),
    };

    let instrument_id = instrument.id();
    let total_levels = snapshot.bids.len() + snapshot.asks.len();
    let mut deltas = Vec::with_capacity(total_levels + 1);
    let mut clear = OrderBookDelta::clear(instrument_id, sequence, ts_event, ts_init);

    if total_levels == 0 {
        clear.flags |= RecordFlag::F_LAST as u8;
    }
    deltas.push(clear);

    let mut processed = 0_usize;
    let mut push_level = |level: &[BitgetDecimalValue], side: OrderSide| -> anyhow::Result<()> {
        let price = level
            .first()
            .ok_or_else(|| anyhow::anyhow!("missing price in Bitget order book level"))?;
        let size = level
            .get(1)
            .ok_or_else(|| anyhow::anyhow!("missing size in Bitget order book level"))?;
        let price = parse_price_with_precision(
            &decimal_value_as_string(price, "orderbook.price")?,
            instrument.price_precision(),
            "orderbook.price",
        )?;
        let size = parse_quantity_with_precision(
            &decimal_value_as_string(size, "orderbook.size")?,
            instrument.size_precision(),
            "orderbook.size",
        )?;

        processed += 1;
        let mut flags = RecordFlag::F_MBP as u8;
        if processed == total_levels {
            flags |= RecordFlag::F_LAST as u8;
        }

        let order = BookOrder::new(side, price, size, 0);
        let delta = OrderBookDelta::new_checked(
            instrument_id,
            BookAction::Add,
            order,
            flags,
            sequence,
            ts_event,
            ts_init,
        )
        .context("failed to construct OrderBookDelta from Bitget book level")?;
        deltas.push(delta);
        Ok(())
    };

    for level in &snapshot.bids {
        push_level(level, OrderSide::Buy)?;
    }

    for level in &snapshot.asks {
        push_level(level, OrderSide::Sell)?;
    }

    OrderBookDeltas::new_checked(instrument_id, deltas)
        .context("failed to assemble OrderBookDeltas from Bitget snapshot")
}

/// Parses a Bitget REST/WebSocket book snapshot into Nautilus top-10 depth.
pub fn parse_orderbook_depth10_snapshot(
    bids_raw: &[(String, String)],
    asks_raw: &[(String, String)],
    instrument: &InstrumentAny,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDepth10> {
    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    let mut bids = [empty_book_order(OrderSide::Buy, price_precision, size_precision); DEPTH10_LEN];
    let mut asks =
        [empty_book_order(OrderSide::Sell, price_precision, size_precision); DEPTH10_LEN];
    let mut bid_counts = [0_u32; DEPTH10_LEN];
    let mut ask_counts = [0_u32; DEPTH10_LEN];

    for (idx, (price_raw, size_raw)) in bids_raw.iter().take(DEPTH10_LEN).enumerate() {
        let price = parse_price_with_precision(price_raw, price_precision, "depth10.bid.price")?;
        let size = parse_quantity_with_precision(size_raw, size_precision, "depth10.bid.size")?;
        bids[idx] = BookOrder::new(OrderSide::Buy, price, size, 0);
        bid_counts[idx] = 1;
    }

    for (idx, (price_raw, size_raw)) in asks_raw.iter().take(DEPTH10_LEN).enumerate() {
        let price = parse_price_with_precision(price_raw, price_precision, "depth10.ask.price")?;
        let size = parse_quantity_with_precision(size_raw, size_precision, "depth10.ask.size")?;
        asks[idx] = BookOrder::new(OrderSide::Sell, price, size, 0);
        ask_counts[idx] = 1;
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

/// Parses a Bitget WebSocket order book push into Nautilus order book deltas.
pub fn parse_ws_orderbook_deltas(
    data: &BitgetBookData,
    instrument: &InstrumentAny,
    action: Option<&str>,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<OrderBookDeltas> {
    let ts_event = match data.ts.as_deref().filter(|value| !value.trim().is_empty()) {
        Some(ts) => parse_millis_timestamp(ts, "ws.orderbook.ts")?,
        None => ts_init.context("Bitget WebSocket order book update did not include ts")?,
    };
    let ts_init = ts_init.unwrap_or(ts_event);
    let sequence = match data.seq {
        Some(seq) => u64::try_from(seq).context("ws.orderbook.seq must be non-negative")?,
        None => ts_event.as_u64(),
    };
    let is_snapshot = action.is_some_and(|value| value.eq_ignore_ascii_case("snapshot"));
    let total_levels = data.bids.len() + data.asks.len();

    anyhow::ensure!(
        is_snapshot || total_levels > 0,
        "Bitget WebSocket order book update contained no deltas"
    );

    let instrument_id = instrument.id();
    let mut deltas = Vec::with_capacity(total_levels + usize::from(is_snapshot));

    if is_snapshot {
        let mut clear = OrderBookDelta::clear(instrument_id, sequence, ts_event, ts_init);
        if total_levels == 0 {
            clear.flags |= RecordFlag::F_LAST as u8;
        }
        deltas.push(clear);
    }

    let mut processed = 0_usize;
    let mut push_level = |level: &BitgetBookLevel, side: OrderSide| -> anyhow::Result<()> {
        let price = parse_price_with_precision(
            &level.0,
            instrument.price_precision(),
            "ws.orderbook.price",
        )?;
        let size = parse_quantity_with_precision(
            &level.1,
            instrument.size_precision(),
            "ws.orderbook.size",
        )?;
        let action = if is_snapshot {
            BookAction::Add
        } else if size.as_f64() == 0.0 {
            BookAction::Delete
        } else {
            BookAction::Update
        };

        processed += 1;
        let mut flags = RecordFlag::F_MBP as u8;
        if processed == total_levels {
            flags |= RecordFlag::F_LAST as u8;
        }

        let order = BookOrder::new(side, price, size, 0);
        let delta = OrderBookDelta::new_checked(
            instrument_id,
            action,
            order,
            flags,
            sequence,
            ts_event,
            ts_init,
        )
        .context("failed to construct OrderBookDelta from Bitget WebSocket book level")?;
        deltas.push(delta);
        Ok(())
    };

    for level in &data.bids {
        push_level(level, OrderSide::Buy)?;
    }

    for level in &data.asks {
        push_level(level, OrderSide::Sell)?;
    }

    OrderBookDeltas::new_checked(instrument_id, deltas)
        .context("failed to assemble OrderBookDeltas from Bitget WebSocket update")
}

/// Parses a Bitget REST market trade into a Nautilus [`TradeTick`].
pub fn parse_market_trade(
    trade: &BitgetMarketTrade,
    instrument: &InstrumentAny,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<TradeTick> {
    let price =
        parse_price_with_precision(&trade.price, instrument.price_precision(), "trade.price")?;
    let size =
        parse_quantity_with_precision(&trade.size, instrument.size_precision(), "trade.size")?;
    anyhow::ensure!(size.as_f64() > 0.0, "trade.size must be positive");

    let aggressor_side = match trade.side.trim().to_ascii_lowercase().as_str() {
        "buy" => AggressorSide::Buyer,
        "sell" => AggressorSide::Seller,
        _ => AggressorSide::NoAggressor,
    };

    let ts_event = parse_millis_timestamp(&trade.ts, "trade.ts")?;
    let ts_init = ts_init.unwrap_or(ts_event);
    let trade_id = if trade.trade_id.trim().is_empty() {
        format!(
            "{}-{}-{}-{}-{}",
            instrument.id(),
            ts_event.as_u64(),
            trade.side,
            trade.price,
            trade.size
        )
    } else {
        trade.trade_id.clone()
    };
    let trade_id =
        TradeId::new_checked(trade_id.as_str()).context("invalid Bitget market trade id")?;

    TradeTick::new_checked(
        instrument.id(),
        price,
        size,
        aggressor_side,
        trade_id,
        ts_event,
        ts_init,
    )
    .context("failed to construct TradeTick from Bitget market trade")
}

/// Parses a Bitget order detail into a Nautilus [`OrderStatusReport`].
pub fn parse_order_status_report(
    order: &BitgetOrderStatus,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let venue_order_id = optional_str([order.order_id.as_ref()])
        .context("Bitget order status did not include orderId")
        .and_then(|value| {
            VenueOrderId::new_checked(value).context("invalid Bitget venue order ID")
        })?;
    let client_order_id = optional_str([order.client_oid.as_ref()])
        .map(ClientOrderId::new_checked)
        .transpose()
        .context("invalid Bitget client order ID")?;
    let side = optional_str([order.side.as_ref()]).context("Bitget order status missing side")?;
    let trigger_price_raw = optional_str([order.trigger_price.as_ref()]);
    let order_side = parse_order_side(side)?;
    let order_type = parse_order_type(order.order_type.as_deref(), trigger_price_raw)?;
    let (time_in_force, tif_post_only) = parse_time_in_force(order.force.as_deref())?;

    let quantity_raw =
        optional_str([order.size.as_ref()]).context("Bitget order status missing qty")?;
    let quantity =
        parse_quantity_with_precision(quantity_raw, instrument.size_precision(), "order.size")?;
    let filled_qty_raw = optional_str([
        order.filled_size.as_ref(),
        order.filled_qty.as_ref(),
        order.cumulative_filled_qty.as_ref(),
    ]);
    let mut filled_qty = match filled_qty_raw {
        Some(value) => {
            parse_quantity_with_precision(value, instrument.size_precision(), "order.cumExecQty")?
        }
        None => Quantity::zero(instrument.size_precision()),
    };

    let status_raw =
        optional_str([order.status.as_ref()]).context("Bitget order status missing status")?;
    let order_status = parse_order_status(status_raw, filled_qty)?;
    if order_status == OrderStatus::Filled && filled_qty.is_zero() {
        filled_qty = quantity;
    }

    let ts_updated = optional_millis_timestamp(order.u_time.as_deref(), "order.updatedTime")?;
    let ts_accepted = optional_millis_timestamp(order.c_time.as_deref(), "order.createdTime")?
        .or(ts_updated)
        .unwrap_or(ts_init);
    let ts_last = ts_updated.unwrap_or(ts_accepted);

    let mut report = OrderStatusReport::new(
        account_id,
        instrument.id(),
        client_order_id,
        venue_order_id,
        order_side,
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
        ts_accepted,
        ts_last,
        ts_init,
        Some(UUID4::new()),
    );

    if let Some(price) = optional_str([order.price.as_ref()]).filter(|value| *value != "0") {
        report = report.with_price(parse_price_with_precision(
            price,
            instrument.price_precision(),
            "order.price",
        )?);
    }

    if let Some(avg_price) = optional_str([order.avg_price.as_ref(), order.price_avg.as_ref()])
        .filter(|value| *value != "0")
    {
        let avg_px = Decimal::from_str(avg_price)
            .with_context(|| format!("invalid decimal order.avgPrice: {avg_price:?}"))?
            .to_f64()
            .with_context(|| format!("order.avgPrice out of f64 range: {avg_price:?}"))?;
        report = report.with_avg_px(avg_px)?;
    }

    if let Some(trigger_price) = trigger_price_raw.filter(|value| *value != "0") {
        report = report.with_trigger_price(parse_price_with_precision(
            trigger_price,
            instrument.price_precision(),
            "order.triggerPrice",
        )?);
    }

    Ok(report
        .with_post_only(tif_post_only)
        .with_reduce_only(parse_bitget_bool(order.reduce_only.as_deref())))
}

/// Parses a Bitget private fill row into a Nautilus [`FillReport`].
pub fn parse_fill_report(
    fill: &BitgetFill,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<FillReport> {
    let venue_order_id = optional_str([fill.order_id.as_ref()])
        .context("Bitget fill did not include orderId")
        .and_then(|value| {
            VenueOrderId::new_checked(value).context("invalid Bitget venue order ID")
        })?;
    let trade_id = optional_str([fill.trade_id.as_ref()])
        .context("Bitget fill did not include execId")
        .and_then(|value| TradeId::new_checked(value).context("invalid Bitget trade ID"))?;
    let client_order_id = optional_str([fill.client_oid.as_ref()])
        .map(ClientOrderId::new_checked)
        .transpose()
        .context("invalid Bitget client order ID")?;
    let side = optional_str([fill.side.as_ref()]).context("Bitget fill missing side")?;
    let order_side = parse_order_side(side)?;
    let price_raw = optional_str([fill.price.as_ref()]).context("Bitget fill missing price")?;
    let size_raw = optional_str([fill.size.as_ref()]).context("Bitget fill missing size")?;
    let last_px =
        parse_price_with_precision(price_raw, instrument.price_precision(), "fill.price")?;
    let last_qty =
        parse_quantity_with_precision(size_raw, instrument.size_precision(), "fill.size")?;
    anyhow::ensure!(last_qty.is_positive(), "fill.size must be positive");

    let commission = parse_fill_commission(fill, instrument)?;
    let liquidity_side = parse_liquidity_side(fill.is_maker, fill.trade_scope.as_deref())?;
    let ts_event =
        optional_millis_timestamp(fill.c_time.as_deref(), "fill.createdTime")?.unwrap_or(ts_init);

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
        Some(UUID4::new()),
    ))
}

/// Parses Bitget Spot asset rows into a Nautilus [`AccountState`].
pub fn parse_spot_account_state(
    assets: &[BitgetSpotAsset],
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<AccountState> {
    let mut balances = Vec::new();
    let mut ts_event = ts_init;

    for asset in assets {
        let coin = optional_str([asset.coin.as_ref()]).context("Bitget spot asset missing coin")?;
        let currency = get_currency(coin);
        let available = parse_decimal(asset.available.as_deref(), "asset.available")?;
        let frozen = parse_decimal(asset.frozen.as_deref(), "asset.frozen")?;
        let locked = parse_decimal(asset.locked.as_deref(), "asset.locked")?;
        let locked_total = frozen + locked;
        let total = available + locked_total;

        if total == Decimal::ZERO && locked_total == Decimal::ZERO {
            continue;
        }

        balances.push(AccountBalance::from_total_and_locked(
            total,
            locked_total,
            currency,
        )?);

        if let Some(timestamp) =
            optional_millis_timestamp(asset.u_time.as_deref(), "asset.updatedTime")?
        {
            ts_event = UnixNanos::from(ts_event.as_u64().max(timestamp.as_u64()));
        }
    }

    Ok(AccountState::new(
        account_id,
        AccountType::Cash,
        balances,
        Vec::new(),
        true,
        UUID4::new(),
        ts_event,
        ts_init,
        None,
    ))
}

/// Parses Bitget Mix account rows into a Nautilus [`AccountState`].
pub fn parse_mix_account_state(
    accounts: &[BitgetMixAccount],
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<AccountState> {
    let mut balances = Vec::new();
    let mut margins = Vec::new();
    let mut ts_event = ts_init;

    for account in accounts {
        let coin = optional_str([account.margin_coin.as_ref()])
            .context("Bitget mix account missing marginCoin")?;
        let currency = get_currency(coin);
        let total = parse_decimal(
            optional_str([
                account.account_equity.as_ref(),
                account.usdt_equity.as_ref(),
            ]),
            "account.accountEquity",
        )?;
        let free = parse_decimal(account.available.as_deref(), "account.available")?;

        if total != Decimal::ZERO || free != Decimal::ZERO {
            balances.push(AccountBalance::from_total_and_free(total, free, currency)?);
        }

        let crossed_margin =
            parse_decimal(account.crossed_margin.as_deref(), "account.crossedMargin")?;
        let isolated_margin =
            parse_decimal(account.isolated_margin.as_deref(), "account.isolatedMargin")?;
        let locked = parse_decimal(account.locked.as_deref(), "account.locked")?;
        let initial_margin = crossed_margin + isolated_margin + locked;
        let maintenance_margin = parse_decimal(account.union_mm.as_deref(), "account.unionMm")?;

        if initial_margin != Decimal::ZERO || maintenance_margin != Decimal::ZERO {
            margins.push(MarginBalance::new(
                Money::from_decimal(initial_margin, currency)?,
                Money::from_decimal(maintenance_margin, currency)?,
                None,
            ));
        }

        if let Some(timestamp) =
            optional_millis_timestamp(account.u_time.as_deref(), "account.updatedTime")?
        {
            ts_event = UnixNanos::from(ts_event.as_u64().max(timestamp.as_u64()));
        }
    }

    Ok(AccountState::new(
        account_id,
        AccountType::Margin,
        balances,
        margins,
        true,
        UUID4::new(),
        ts_event,
        ts_init,
        None,
    ))
}

fn parse_position_side(
    raw: Option<&str>,
    quantity: Quantity,
) -> anyhow::Result<PositionSideSpecified> {
    if quantity.is_zero() {
        return Ok(PositionSideSpecified::Flat);
    }

    let Some(raw) = raw else {
        return Ok(PositionSideSpecified::Long);
    };

    match normalize_bitget_token(raw).as_str() {
        "long" | "buy" | "open_long" => Ok(PositionSideSpecified::Long),
        "short" | "sell" | "open_short" => Ok(PositionSideSpecified::Short),
        "net" | "both" | "" => Ok(PositionSideSpecified::Long),
        other => anyhow::bail!("unsupported Bitget position side: {other:?}"),
    }
}

fn parse_position_quantity(raw: &str, precision: u8, field: &str) -> anyhow::Result<Quantity> {
    let decimal = Decimal::from_str(raw)
        .with_context(|| format!("invalid decimal quantity for {field}: {raw:?}"))?;
    let abs_decimal = if decimal < Decimal::ZERO {
        -decimal
    } else {
        decimal
    };
    let value = abs_decimal
        .to_f64()
        .with_context(|| format!("quantity out of f64 range for {field}: {raw:?}"))?;
    Quantity::new_checked(value, precision)
        .map_err(|e| anyhow::anyhow!("invalid quantity for {field}: {raw:?}: {e}"))
}

/// Parses a Bitget Mix position row into a Nautilus [`PositionStatusReport`].
pub fn parse_position_status_report(
    position: &BitgetMixPosition,
    instrument: &InstrumentAny,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> anyhow::Result<PositionStatusReport> {
    let size_raw = optional_str([position.total.as_ref()]).unwrap_or("0");
    let quantity = parse_position_quantity(size_raw, instrument.size_precision(), "position.size")?;
    let position_side = parse_position_side(position.hold_side.as_deref(), quantity)?;
    let avg_px_open = optional_str([
        position.open_price_avg.as_ref(),
        position.average_open_price.as_ref(),
    ])
    .filter(|value| *value != "0")
    .map(Decimal::from_str)
    .transpose()
    .context("invalid Bitget position average open price")?;
    let ts_last = optional_millis_timestamp(position.u_time.as_deref(), "position.updatedTime")?
        .or(optional_millis_timestamp(
            position.c_time.as_deref(),
            "position.createdTime",
        )?)
        .unwrap_or(ts_init);
    let venue_position_id = optional_str([position.pos_id.as_ref()])
        .map(PositionId::new_checked)
        .transpose()
        .context("invalid Bitget position ID")?;

    Ok(PositionStatusReport::new(
        account_id,
        instrument.id(),
        position_side,
        quantity,
        ts_last,
        ts_init,
        Some(UUID4::new()),
        venue_position_id,
        avg_px_open,
    ))
}

/// Parses a Bitget historical funding rate into a Nautilus [`FundingRateUpdate`].
pub fn parse_funding_rate(
    funding: &BitgetFundingRate,
    instrument: &InstrumentAny,
    interval_millis: Option<i64>,
) -> anyhow::Result<FundingRateUpdate> {
    let rate = Decimal::from_str(&funding.funding_rate).with_context(|| {
        format!(
            "invalid decimal funding rate for funding.fundingRate: {:?}",
            funding.funding_rate
        )
    })?;
    let ts_event = parse_millis_timestamp(&funding.funding_time, "funding.fundingTime")?;
    let interval = interval_millis
        .map(|millis| {
            anyhow::ensure!(
                millis > 0,
                "funding interval millis must be positive: {millis}"
            );
            u16::try_from(millis / 60_000).context("funding interval minutes out of bounds")
        })
        .transpose()?;

    Ok(FundingRateUpdate::new(
        instrument.id(),
        rate,
        interval,
        None,
        ts_event,
        ts_event,
    ))
}

fn parse_ticker_ts(
    ticker_ts: Option<&str>,
    ts_init: Option<UnixNanos>,
    field: &str,
) -> anyhow::Result<UnixNanos> {
    match ticker_ts.filter(|value| !value.trim().is_empty()) {
        Some(ts) => parse_millis_timestamp(ts, field),
        None => ts_init.context("Bitget ticker update did not include ts"),
    }
}

/// Parses a Bitget WebSocket ticker into a Nautilus [`QuoteTick`].
pub fn parse_ws_quote_tick(
    ticker: &BitgetTickerData,
    instrument: &InstrumentAny,
    ticker_ts: Option<&str>,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<QuoteTick> {
    let bid_price_raw = ticker
        .bid1_price
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("Bitget ticker update did not include bid1Price")?;
    let ask_price_raw = ticker
        .ask1_price
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("Bitget ticker update did not include ask1Price")?;
    let bid_size_raw = ticker
        .bid1_size
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("Bitget ticker update did not include bid1Size")?;
    let ask_size_raw = ticker
        .ask1_size
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("Bitget ticker update did not include ask1Size")?;

    let price_precision = instrument.price_precision();
    let size_precision = instrument.size_precision();
    let bid_price = parse_price_with_precision(bid_price_raw, price_precision, "ticker.bid1Price")?;
    let ask_price = parse_price_with_precision(ask_price_raw, price_precision, "ticker.ask1Price")?;
    let bid_size = parse_quantity_with_precision(bid_size_raw, size_precision, "ticker.bid1Size")?;
    let ask_size = parse_quantity_with_precision(ask_size_raw, size_precision, "ticker.ask1Size")?;
    let ts_event = parse_ticker_ts(ticker_ts, ts_init, "ticker.ts")?;
    let ts_init = ts_init.unwrap_or(ts_event);

    Ok(QuoteTick::new(
        instrument.id(),
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        ts_event,
        ts_init,
    ))
}

/// Parses a Bitget WebSocket ticker into a Nautilus [`MarkPriceUpdate`].
pub fn parse_ws_mark_price(
    ticker: &BitgetTickerData,
    instrument: &InstrumentAny,
    ticker_ts: Option<&str>,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<MarkPriceUpdate> {
    let raw = ticker
        .mark_price
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("Bitget ticker update did not include markPrice")?;
    let price = parse_price_with_precision(raw, instrument.price_precision(), "ticker.markPrice")?;
    let ts_event = parse_ticker_ts(ticker_ts, ts_init, "ticker.ts")?;
    let ts_init = ts_init.unwrap_or(ts_event);

    Ok(MarkPriceUpdate::new(
        instrument.id(),
        price,
        ts_event,
        ts_init,
    ))
}

/// Parses a Bitget WebSocket ticker into a Nautilus [`IndexPriceUpdate`].
pub fn parse_ws_index_price(
    ticker: &BitgetTickerData,
    instrument: &InstrumentAny,
    ticker_ts: Option<&str>,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<IndexPriceUpdate> {
    let raw = ticker
        .index_price
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("Bitget ticker update did not include indexPrice")?;
    let price = parse_price_with_precision(raw, instrument.price_precision(), "ticker.indexPrice")?;
    let ts_event = parse_ticker_ts(ticker_ts, ts_init, "ticker.ts")?;
    let ts_init = ts_init.unwrap_or(ts_event);

    Ok(IndexPriceUpdate::new(
        instrument.id(),
        price,
        ts_event,
        ts_init,
    ))
}

/// Parses a Bitget WebSocket ticker into a Nautilus [`FundingRateUpdate`].
pub fn parse_ws_funding_rate(
    ticker: &BitgetTickerData,
    instrument: &InstrumentAny,
    ticker_ts: Option<&str>,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<FundingRateUpdate> {
    let raw = ticker
        .funding_rate
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .context("Bitget ticker update did not include fundingRate")?;
    let rate = Decimal::from_str(raw)
        .with_context(|| format!("invalid ticker fundingRate decimal: {raw:?}"))?;
    let next_funding_ns = ticker
        .next_funding_time
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| parse_millis_timestamp(value, "ticker.nextFundingTime"))
        .transpose()?;
    let ts_event = parse_ticker_ts(ticker_ts, ts_init, "ticker.ts")?;
    let ts_init = ts_init.unwrap_or(ts_event);

    Ok(FundingRateUpdate::new(
        instrument.id(),
        rate,
        None,
        next_funding_ns,
        ts_event,
        ts_init,
    ))
}

/// Parses a Bitget candle row into a Nautilus [`Bar`].
pub fn parse_candle_bar(
    candle: &BitgetCandle,
    instrument: &InstrumentAny,
    bar_type: BarType,
    timestamp_on_close: bool,
    ts_init: Option<UnixNanos>,
) -> anyhow::Result<Bar> {
    let value = |index: usize, field: &str| -> anyhow::Result<String> {
        let value = candle
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("missing {field} in Bitget candle row"))?;
        decimal_value_as_string(value, field)
    };

    let open = parse_price_with_precision(
        &value(1, "candle.open")?,
        instrument.price_precision(),
        "candle.open",
    )?;
    let high = parse_price_with_precision(
        &value(2, "candle.high")?,
        instrument.price_precision(),
        "candle.high",
    )?;
    let low = parse_price_with_precision(
        &value(3, "candle.low")?,
        instrument.price_precision(),
        "candle.low",
    )?;
    let close = parse_price_with_precision(
        &value(4, "candle.close")?,
        instrument.price_precision(),
        "candle.close",
    )?;
    let volume = parse_quantity_with_precision(
        &value(5, "candle.volume")?,
        instrument.size_precision(),
        "candle.volume",
    )?;

    let mut ts_event = parse_millis_timestamp(&value(0, "candle.ts")?, "candle.ts")?;

    if timestamp_on_close {
        let interval_ns = bar_type
            .spec()
            .timedelta()
            .num_nanoseconds()
            .context("bar specification produced non-integer interval")?;
        let interval_ns = u64::try_from(interval_ns)
            .context("bar interval overflowed the u64 range for nanoseconds")?;
        ts_event = UnixNanos::from(
            ts_event
                .as_u64()
                .checked_add(interval_ns)
                .context("bar timestamp overflowed when adjusting to close time")?,
        );
    }

    let ts_init = ts_init.unwrap_or(ts_event);

    Bar::new_checked(bar_type, open, high, low, close, volume, ts_event, ts_init)
        .context("failed to construct Bar from Bitget candle row")
}

/// Converts a Nautilus bar aggregation and step to a Bitget candle interval string.
pub fn bar_spec_to_bitget_interval(
    aggregation: BarAggregation,
    step: u64,
) -> anyhow::Result<&'static str> {
    match aggregation {
        BarAggregation::Minute => match step {
            1 => Ok("1min"),
            5 => Ok("5min"),
            15 => Ok("15min"),
            30 => Ok("30min"),
            _ => anyhow::bail!("Bitget only supports minute intervals 1, 5, 15, 30"),
        },
        BarAggregation::Hour => match step {
            1 => Ok("1h"),
            4 => Ok("4h"),
            6 => Ok("6h"),
            12 => Ok("12h"),
            _ => anyhow::bail!("Bitget only supports hour intervals 1, 4, 6, 12"),
        },
        BarAggregation::Day if step == 1 => Ok("1day"),
        BarAggregation::Week if step == 1 => Ok("1week"),
        _ => anyhow::bail!("Bitget does not support {aggregation:?} bars with step {step}"),
    }
}

/// Converts a Nautilus bar aggregation and step to a Bitget interval for a product type.
pub fn bar_spec_to_bitget_interval_for_product(
    product_type: BitgetProductType,
    aggregation: BarAggregation,
    step: u64,
) -> anyhow::Result<&'static str> {
    if product_type == BitgetProductType::Spot {
        return bar_spec_to_bitget_interval(aggregation, step);
    }

    match aggregation {
        BarAggregation::Minute => match step {
            1 => Ok("1m"),
            5 => Ok("5m"),
            15 => Ok("15m"),
            30 => Ok("30m"),
            _ => anyhow::bail!("Bitget futures only supports minute intervals 1, 5, 15, 30"),
        },
        BarAggregation::Hour => match step {
            1 => Ok("1H"),
            4 => Ok("4H"),
            6 => Ok("6H"),
            12 => Ok("12H"),
            _ => anyhow::bail!("Bitget futures only supports hour intervals 1, 4, 6, 12"),
        },
        BarAggregation::Day if step == 1 => Ok("1D"),
        BarAggregation::Week if step == 1 => Ok("1W"),
        _ => anyhow::bail!("Bitget futures does not support {aggregation:?} bars with step {step}"),
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::BarType,
        enums::{AggressorSide, BookAction, RecordFlag},
        instruments::Instrument,
    };
    use rstest::rstest;

    use super::*;

    const TS: UnixNanos = UnixNanos::new(1_700_000_000_000_000_000);

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

        parse_usdt_perp_instrument(&definition, TS, TS).unwrap()
    }

    #[rstest]
    fn parse_spot_instrument_builds_currency_pair() {
        let definition = BitgetSpotSymbol {
            symbol: "BTCUSDT".to_string(),
            base_coin: "BTC".to_string(),
            quote_coin: "USDT".to_string(),
            min_trade_amount: Some("0.00001".to_string()),
            max_trade_amount: Some("100".to_string()),
            min_trade_usdt: Some("5".to_string()),
            maker_fee_rate: Some("0.001".to_string()),
            taker_fee_rate: Some("0.001".to_string()),
            price_precision: Some("2".to_string()),
            quantity_precision: Some("6".to_string()),
            quote_precision: Some("2".to_string()),
            status: Some("online".to_string()),
        };

        let parsed = parse_spot_instrument(&definition, TS, TS).unwrap();

        assert_eq!(parsed.id().to_string(), "BTCUSDT.BITGET");
        assert_eq!(parsed.raw_symbol().to_string(), "BTCUSDT");
        assert_eq!(parsed.price_precision(), 2);
        assert_eq!(parsed.size_precision(), 6);
    }

    #[rstest]
    fn parse_spot_instrument_ignores_overflowing_max_trade_amount() {
        let definition = BitgetSpotSymbol {
            symbol: "BTCUSDT".to_string(),
            base_coin: "BTC".to_string(),
            quote_coin: "USDT".to_string(),
            min_trade_amount: Some("0.00001".to_string()),
            max_trade_amount: Some("900000000000000000000".to_string()),
            min_trade_usdt: Some("5".to_string()),
            maker_fee_rate: Some("0.001".to_string()),
            taker_fee_rate: Some("0.001".to_string()),
            price_precision: Some("2".to_string()),
            quantity_precision: Some("6".to_string()),
            quote_precision: Some("2".to_string()),
            status: Some("online".to_string()),
        };

        let parsed = parse_spot_instrument(&definition, TS, TS).unwrap();
        let InstrumentAny::CurrencyPair(pair) = parsed else {
            panic!("expected CurrencyPair");
        };

        assert_eq!(pair.max_quantity, None);
        assert_eq!(pair.min_quantity, Some(Quantity::from("0.00001")));
    }

    #[rstest]
    fn parse_usdt_perp_instrument_builds_perpetual() {
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

        let parsed = parse_usdt_perp_instrument(&definition, TS, TS).unwrap();

        assert_eq!(parsed.id().to_string(), "BTCUSDT-PERP.BITGET");
        assert_eq!(parsed.raw_symbol().to_string(), "BTCUSDT");
        assert_eq!(parsed.price_precision(), 1);
        assert_eq!(parsed.size_precision(), 3);
    }

    #[rstest]
    fn parse_usdt_perp_instrument_ignores_overflowing_max_order_qty() {
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
            max_order_qty: Some("900000000000000000000".to_string()),
            size_multiplier: Some("0.001".to_string()),
            price_place: Some("1".to_string()),
            volume_place: Some("3".to_string()),
            price_end_step: Some("1".to_string()),
            max_lever: Some("125".to_string()),
            min_lever: Some("1".to_string()),
            fund_interval: Some("8".to_string()),
            symbol_status: Some("normal".to_string()),
        };

        let parsed = parse_usdt_perp_instrument(&definition, TS, TS).unwrap();
        let InstrumentAny::CryptoPerpetual(perp) = parsed else {
            panic!("expected CryptoPerpetual");
        };

        assert_eq!(perp.max_quantity, None);
        assert_eq!(perp.min_quantity, Some(Quantity::from("0.001")));
    }

    #[rstest]
    fn parse_orderbook_snapshot_builds_snapshot_deltas() {
        let definition = BitgetSpotSymbol {
            symbol: "BTCUSDT".to_string(),
            base_coin: "BTC".to_string(),
            quote_coin: "USDT".to_string(),
            min_trade_amount: Some("0.00001".to_string()),
            max_trade_amount: Some("100".to_string()),
            min_trade_usdt: Some("5".to_string()),
            maker_fee_rate: Some("0.001".to_string()),
            taker_fee_rate: Some("0.001".to_string()),
            price_precision: Some("2".to_string()),
            quantity_precision: Some("6".to_string()),
            quote_precision: Some("2".to_string()),
            status: Some("online".to_string()),
        };
        let instrument = parse_spot_instrument(&definition, TS, TS).unwrap();
        let snapshot = BitgetOrderBookSnapshot {
            bids: vec![vec![
                BitgetDecimalValue::String("100.10".to_string()),
                BitgetDecimalValue::String("0.500000".to_string()),
            ]],
            asks: vec![vec![
                BitgetDecimalValue::String("100.20".to_string()),
                BitgetDecimalValue::String("0.400000".to_string()),
            ]],
            ts: Some("1700000000123".to_string()),
            seq: Some("42".to_string()),
        };

        let deltas = parse_orderbook_snapshot(&snapshot, &instrument, Some(TS)).unwrap();

        assert_eq!(deltas.instrument_id.to_string(), "BTCUSDT.BITGET");
        assert_eq!(deltas.sequence, 42);
        assert_eq!(deltas.deltas.len(), 3);
        assert_ne!(deltas.deltas[2].flags & RecordFlag::F_LAST as u8, 0);
    }

    #[rstest]
    fn parse_ws_orderbook_update_builds_update_and_delete_deltas() {
        let definition = BitgetSpotSymbol {
            symbol: "BTCUSDT".to_string(),
            base_coin: "BTC".to_string(),
            quote_coin: "USDT".to_string(),
            min_trade_amount: Some("0.00001".to_string()),
            max_trade_amount: Some("100".to_string()),
            min_trade_usdt: Some("5".to_string()),
            maker_fee_rate: Some("0.001".to_string()),
            taker_fee_rate: Some("0.001".to_string()),
            price_precision: Some("2".to_string()),
            quantity_precision: Some("6".to_string()),
            quote_precision: Some("2".to_string()),
            status: Some("online".to_string()),
        };
        let instrument = parse_spot_instrument(&definition, TS, TS).unwrap();
        let update = BitgetBookData {
            bids: vec![BitgetBookLevel(
                "100.10".to_string(),
                "0.500000".to_string(),
            )],
            asks: vec![BitgetBookLevel("100.20".to_string(), "0".to_string())],
            seq: Some(43),
            pseq: Some(42),
            checksum: None,
            ts: Some("1700000000123".to_string()),
        };

        let deltas =
            parse_ws_orderbook_deltas(&update, &instrument, Some("update"), Some(TS)).unwrap();

        assert_eq!(deltas.sequence, 43);
        assert_eq!(deltas.deltas.len(), 2);
        assert_eq!(deltas.deltas[0].action, BookAction::Update);
        assert_eq!(deltas.deltas[1].action, BookAction::Delete);
        assert_ne!(deltas.deltas[1].flags & RecordFlag::F_LAST as u8, 0);
    }

    #[rstest]
    fn parse_market_trade_builds_trade_tick() {
        let definition = BitgetSpotSymbol {
            symbol: "BTCUSDT".to_string(),
            base_coin: "BTC".to_string(),
            quote_coin: "USDT".to_string(),
            min_trade_amount: Some("0.00001".to_string()),
            max_trade_amount: Some("100".to_string()),
            min_trade_usdt: Some("5".to_string()),
            maker_fee_rate: Some("0.001".to_string()),
            taker_fee_rate: Some("0.001".to_string()),
            price_precision: Some("2".to_string()),
            quantity_precision: Some("6".to_string()),
            quote_precision: Some("2".to_string()),
            status: Some("online".to_string()),
        };
        let instrument = parse_spot_instrument(&definition, TS, TS).unwrap();
        let trade = BitgetMarketTrade {
            trade_id: "12345".to_string(),
            price: "100.10".to_string(),
            size: "0.500000".to_string(),
            side: "buy".to_string(),
            ts: "1700000000123".to_string(),
            ..Default::default()
        };

        let tick = parse_market_trade(&trade, &instrument, Some(TS)).unwrap();

        assert_eq!(tick.instrument_id.to_string(), "BTCUSDT.BITGET");
        assert_eq!(tick.aggressor_side, AggressorSide::Buyer);
        assert_eq!(tick.trade_id.to_string(), "12345");
        assert_eq!(tick.price.precision, 2);
        assert_eq!(tick.size.precision, 6);
    }

    #[rstest]
    fn parse_funding_rate_builds_update() {
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
        let instrument = parse_usdt_perp_instrument(&definition, TS, TS).unwrap();
        let funding = BitgetFundingRate {
            symbol: Some("BTCUSDT".to_string()),
            funding_rate: "0.0001".to_string(),
            funding_time: "1700000000000".to_string(),
        };

        let update = parse_funding_rate(&funding, &instrument, Some(28_800_000)).unwrap();

        assert_eq!(update.instrument_id.to_string(), "BTCUSDT-PERP.BITGET");
        assert_eq!(update.interval, Some(480));
        assert_eq!(update.rate, Decimal::from_str("0.0001").unwrap());
    }

    #[rstest]
    fn parse_order_status_report_builds_partially_filled_report() {
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
        let instrument = parse_usdt_perp_instrument(&definition, TS, TS).unwrap();
        let status = BitgetOrderStatus {
            symbol: Some("BTCUSDT".to_string()),
            order_id: Some("123".to_string()),
            client_oid: Some("O-123".to_string()),
            price: Some("100.0".to_string()),
            price_avg: Some("100.1".to_string()),
            size: Some("0.010".to_string()),
            filled_qty: Some("0.004".to_string()),
            side: Some("buy".to_string()),
            order_type: Some("limit".to_string()),
            force: Some("post_only".to_string()),
            status: Some("partially_filled".to_string()),
            reduce_only: Some("true".to_string()),
            c_time: Some("1700000000000".to_string()),
            u_time: Some("1700000001000".to_string()),
            ..Default::default()
        };

        let report =
            parse_order_status_report(&status, &instrument, AccountId::from("BITGET-001"), TS)
                .unwrap();

        assert_eq!(report.instrument_id.to_string(), "BTCUSDT-PERP.BITGET");
        assert_eq!(report.client_order_id.unwrap().to_string(), "O-123");
        assert_eq!(report.venue_order_id.to_string(), "123");
        assert_eq!(report.order_status, OrderStatus::PartiallyFilled);
        assert_eq!(report.order_side, OrderSide::Buy);
        assert_eq!(report.order_type, OrderType::Limit);
        assert_eq!(report.time_in_force, TimeInForce::Gtc);
        assert_eq!(report.quantity.to_string(), "0.010");
        assert_eq!(report.filled_qty.to_string(), "0.004");
        assert_eq!(report.price.unwrap().to_string(), "100.0");
        assert_eq!(report.avg_px.unwrap().to_string(), "100.1");
        assert!(report.post_only);
        assert!(report.reduce_only);
    }

    #[rstest]
    fn parse_fill_report_builds_report_from_fee_detail() {
        let instrument = usdt_perp_instrument();
        let fill = BitgetFill {
            symbol: Some("BTCUSDT".to_string()),
            product_type: Some("USDT-FUTURES".to_string()),
            order_id: Some("123".to_string()),
            client_oid: Some("O-123".to_string()),
            trade_id: Some("T-1".to_string()),
            side: Some("sell".to_string()),
            price: Some("100.1".to_string()),
            size: Some("0.004".to_string()),
            fee_detail: Some(BitgetFillFeeDetail::List(vec![BitgetFillFee {
                fee_coin: Some("USDT".to_string()),
                total_fee: Some("-0.001".to_string()),
            }])),
            trade_scope: Some("maker".to_string()),
            c_time: Some("1700000000000".to_string()),
            ..Default::default()
        };

        let report =
            parse_fill_report(&fill, &instrument, AccountId::from("BITGET-001"), TS).unwrap();

        assert_eq!(report.instrument_id.to_string(), "BTCUSDT-PERP.BITGET");
        assert_eq!(report.client_order_id.unwrap().to_string(), "O-123");
        assert_eq!(report.venue_order_id.to_string(), "123");
        assert_eq!(report.trade_id.to_string(), "T-1");
        assert_eq!(report.order_side, OrderSide::Sell);
        assert_eq!(report.last_qty.to_string(), "0.004");
        assert_eq!(report.last_px.to_string(), "100.1");
        assert_eq!(
            report.commission.as_decimal(),
            Decimal::from_str("0.001").unwrap()
        );
        assert_eq!(report.commission.currency.code.as_str(), "USDT");
        assert_eq!(report.liquidity_side, LiquiditySide::Maker);
    }

    #[rstest]
    fn parse_spot_account_state_builds_balances() {
        let assets = vec![BitgetSpotAsset {
            coin: Some("USDT".to_string()),
            available: Some("100".to_string()),
            frozen: Some("2".to_string()),
            locked: Some("3".to_string()),
            u_time: Some("1700000001000".to_string()),
            ..Default::default()
        }];

        let state = parse_spot_account_state(&assets, AccountId::from("BITGET-001"), TS).unwrap();

        assert_eq!(state.account_type, AccountType::Cash);
        assert_eq!(state.balances.len(), 1);
        assert_eq!(
            state.balances[0].total.as_decimal(),
            Decimal::from_str("105").unwrap()
        );
        assert_eq!(
            state.balances[0].locked.as_decimal(),
            Decimal::from_str("5").unwrap()
        );
        assert_eq!(
            state.balances[0].free.as_decimal(),
            Decimal::from_str("100").unwrap()
        );
        assert!(state.margins.is_empty());
    }

    #[rstest]
    fn parse_mix_account_state_builds_balances_and_margins() {
        let accounts = vec![BitgetMixAccount {
            margin_coin: Some("USDT".to_string()),
            locked: Some("1".to_string()),
            available: Some("100".to_string()),
            crossed_margin: Some("10".to_string()),
            isolated_margin: Some("2".to_string()),
            account_equity: Some("123".to_string()),
            union_mm: Some("4".to_string()),
            u_time: Some("1700000001000".to_string()),
            ..Default::default()
        }];

        let state = parse_mix_account_state(&accounts, AccountId::from("BITGET-001"), TS).unwrap();

        assert_eq!(state.account_type, AccountType::Margin);
        assert_eq!(state.balances.len(), 1);
        assert_eq!(
            state.balances[0].total.as_decimal(),
            Decimal::from_str("123").unwrap()
        );
        assert_eq!(
            state.balances[0].free.as_decimal(),
            Decimal::from_str("100").unwrap()
        );
        assert_eq!(state.margins.len(), 1);
        assert_eq!(
            state.margins[0].initial.as_decimal(),
            Decimal::from_str("13").unwrap()
        );
        assert_eq!(
            state.margins[0].maintenance.as_decimal(),
            Decimal::from_str("4").unwrap()
        );
    }

    #[rstest]
    fn parse_position_status_report_builds_short_report() {
        let instrument = usdt_perp_instrument();
        let position = BitgetMixPosition {
            symbol: Some("BTCUSDT".to_string()),
            product_type: Some("USDT-FUTURES".to_string()),
            margin_coin: Some("USDT".to_string()),
            hold_side: Some("short".to_string()),
            total: Some("0.004".to_string()),
            open_price_avg: Some("100.1".to_string()),
            u_time: Some("1700000001000".to_string()),
            ..Default::default()
        };

        let report =
            parse_position_status_report(&position, &instrument, AccountId::from("BITGET-001"), TS)
                .unwrap();

        assert_eq!(report.instrument_id.to_string(), "BTCUSDT-PERP.BITGET");
        assert_eq!(report.position_side, PositionSideSpecified::Short);
        assert_eq!(report.quantity.to_string(), "0.004");
        assert_eq!(
            report.avg_px_open.unwrap(),
            Decimal::from_str("100.1").unwrap()
        );
    }

    #[rstest]
    fn parse_order_status_report_rejects_unknown_status() {
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
        let instrument = parse_usdt_perp_instrument(&definition, TS, TS).unwrap();
        let status = BitgetOrderStatus {
            order_id: Some("123".to_string()),
            size: Some("0.010".to_string()),
            side: Some("buy".to_string()),
            order_type: Some("limit".to_string()),
            status: Some("mystery".to_string()),
            ..Default::default()
        };

        let err =
            parse_order_status_report(&status, &instrument, AccountId::from("BITGET-001"), TS)
                .unwrap_err();

        assert!(err.to_string().contains("unsupported Bitget order status"));
    }

    #[rstest]
    fn parse_ws_ticker_builds_mark_index_and_funding_updates() {
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
        let instrument = parse_usdt_perp_instrument(&definition, TS, TS).unwrap();
        let ticker = BitgetTickerData {
            symbol: Some("BTCUSDT".to_string()),
            last_price: Some("100.1".to_string()),
            bid1_price: Some("100.0".to_string()),
            bid1_size: Some("1.5".to_string()),
            ask1_price: Some("100.4".to_string()),
            ask1_size: Some("2.5".to_string()),
            mark_price: Some("100.2".to_string()),
            index_price: Some("100.3".to_string()),
            funding_rate: Some("0.0001".to_string()),
            next_funding_time: Some("1700003600000".to_string()),
        };

        let quote =
            parse_ws_quote_tick(&ticker, &instrument, Some("1700000000000"), Some(TS)).unwrap();
        let mark =
            parse_ws_mark_price(&ticker, &instrument, Some("1700000000000"), Some(TS)).unwrap();
        let index =
            parse_ws_index_price(&ticker, &instrument, Some("1700000000000"), Some(TS)).unwrap();
        let funding =
            parse_ws_funding_rate(&ticker, &instrument, Some("1700000000000"), Some(TS)).unwrap();

        assert_eq!(mark.instrument_id.to_string(), "BTCUSDT-PERP.BITGET");
        assert_eq!(quote.bid_price.to_string(), "100.0");
        assert_eq!(quote.ask_price.to_string(), "100.4");
        assert_eq!(quote.bid_size.to_string(), "1.500");
        assert_eq!(quote.ask_size.to_string(), "2.500");
        assert_eq!(mark.value.precision, 1);
        assert_eq!(index.value.precision, 1);
        assert_eq!(funding.rate, Decimal::from_str("0.0001").unwrap());
        assert!(funding.next_funding_ns.is_some());
    }

    #[rstest]
    fn parse_candle_bar_builds_close_timestamp_bar() {
        let definition = BitgetSpotSymbol {
            symbol: "BTCUSDT".to_string(),
            base_coin: "BTC".to_string(),
            quote_coin: "USDT".to_string(),
            min_trade_amount: Some("0.00001".to_string()),
            max_trade_amount: Some("100".to_string()),
            min_trade_usdt: Some("5".to_string()),
            maker_fee_rate: Some("0.001".to_string()),
            taker_fee_rate: Some("0.001".to_string()),
            price_precision: Some("2".to_string()),
            quantity_precision: Some("6".to_string()),
            quote_precision: Some("2".to_string()),
            status: Some("online".to_string()),
        };
        let instrument = parse_spot_instrument(&definition, TS, TS).unwrap();
        let bar_type = BarType::from("BTCUSDT.BITGET-1-MINUTE-LAST-EXTERNAL");
        let candle = vec![
            BitgetDecimalValue::String("1700000000000".to_string()),
            BitgetDecimalValue::String("100.00".to_string()),
            BitgetDecimalValue::String("101.00".to_string()),
            BitgetDecimalValue::String("99.00".to_string()),
            BitgetDecimalValue::String("100.50".to_string()),
            BitgetDecimalValue::String("12.500000".to_string()),
        ];

        let bar = parse_candle_bar(&candle, &instrument, bar_type, true, Some(TS)).unwrap();

        assert_eq!(bar.bar_type, bar_type);
        assert_eq!(bar.ts_event, UnixNanos::from(1_700_000_060_000_000_000));
        assert_eq!(bar.close.precision, 2);
        assert_eq!(bar.volume.precision, 6);
    }

    #[rstest]
    fn bar_interval_mapping_is_product_specific() {
        assert_eq!(
            bar_spec_to_bitget_interval_for_product(
                BitgetProductType::Spot,
                BarAggregation::Minute,
                1
            )
            .unwrap(),
            "1min"
        );
        assert_eq!(
            bar_spec_to_bitget_interval_for_product(
                BitgetProductType::UsdtFutures,
                BarAggregation::Minute,
                1
            )
            .unwrap(),
            "1m"
        );
        assert_eq!(
            bar_spec_to_bitget_interval_for_product(
                BitgetProductType::UsdtFutures,
                BarAggregation::Hour,
                1
            )
            .unwrap(),
            "1H"
        );
    }
}
