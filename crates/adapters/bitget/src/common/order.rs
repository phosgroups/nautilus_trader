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

//! Order mapping helpers for Bitget execution REST requests.

use anyhow::Context;
use nautilus_common::messages::execution::CancelOrder;
use nautilus_core::Params;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce, TriggerType},
    events::OrderInitialized,
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    types::{Price, Quantity},
};

use crate::{
    common::{enums::BitgetProductType, symbol::extract_raw_symbol},
    http::models::{
        BitgetCancelBatchOrderItem, BitgetMixBatchCancelOrdersRequest, BitgetMixCancelOrderRequest,
        BitgetMixCancelPlanOrderRequest, BitgetMixModifyOrderRequest,
        BitgetMixModifyPlanOrderRequest, BitgetMixPlaceOrderRequest, BitgetMixPlanOrderRequest,
        BitgetSpotBatchCancelOrderRequest, BitgetSpotCancelOrderRequest,
        BitgetSpotCancelSymbolOrderRequest, BitgetSpotPlaceOrderRequest,
        BitgetSpotPlanOrderRequest,
    },
};

/// Optional command/order params key for Bitget futures margin mode.
pub const PARAM_MARGIN_MODE: &str = "margin_mode";
/// Optional command/order params key for Bitget margin coin.
pub const PARAM_MARGIN_COIN: &str = "margin_coin";
/// Optional command/order params key for Bitget hedge-mode trade side.
pub const PARAM_TRADE_SIDE: &str = "trade_side";
/// Optional command/order params key for Bitget hedge-mode position side.
pub const PARAM_POS_SIDE: &str = "pos_side";
/// Optional command/order params key for Bitget trigger type.
pub const PARAM_TRIGGER_TYPE: &str = "trigger_type";
/// Optional command/order params key for Bitget self-trade prevention mode.
pub const PARAM_STP_MODE: &str = "stp_mode";
/// Optional command/order params key for Bitget futures TP price.
pub const PARAM_PRESET_TAKE_PROFIT_PRICE: &str = "preset_take_profit_price";
/// Optional command/order params key for Bitget futures SL price.
pub const PARAM_PRESET_STOP_LOSS_PRICE: &str = "preset_stop_loss_price";
/// Optional command/order params key for Bitget plan order type.
pub const PARAM_PLAN_TYPE: &str = "plan_type";
/// Optional command/order params key for Bitget trailing callback ratio.
pub const PARAM_CALLBACK_RATIO: &str = "callback_ratio";

const DEFAULT_MARGIN_MODE: &str = "crossed";
const DEFAULT_MARGIN_COIN: &str = "USDT";

/// Bitget submit order request, split by product and regular/plan order route.
#[derive(Clone, Debug, PartialEq)]
pub enum BitgetSubmitOrderRequest {
    /// Spot regular order.
    Spot(BitgetSpotPlaceOrderRequest),
    /// Spot plan/trigger order.
    SpotPlan(BitgetSpotPlanOrderRequest),
    /// USDT futures regular order.
    Mix(BitgetMixPlaceOrderRequest),
    /// USDT futures plan/trigger order.
    MixPlan(BitgetMixPlanOrderRequest),
}

/// Bitget modify order request.
#[derive(Clone, Debug, PartialEq)]
pub enum BitgetModifyOrderRequest {
    /// USDT futures regular order modify.
    Mix(BitgetMixModifyOrderRequest),
    /// USDT futures plan order modify.
    MixPlan(BitgetMixModifyPlanOrderRequest),
}

/// Bitget cancel order request.
#[derive(Clone, Debug, PartialEq)]
pub enum BitgetCancelOrderRequest {
    /// Spot regular order cancel.
    Spot(BitgetSpotCancelOrderRequest),
    /// USDT futures regular order cancel.
    Mix(BitgetMixCancelOrderRequest),
    /// USDT futures plan order cancel.
    MixPlan(BitgetMixCancelPlanOrderRequest),
}

/// Bitget batch cancel orders request.
#[derive(Clone, Debug, PartialEq)]
pub enum BitgetBatchCancelOrdersRequest {
    /// Spot regular order batch cancel.
    Spot(BitgetSpotBatchCancelOrderRequest),
    /// USDT futures regular order batch cancel.
    Mix(BitgetMixBatchCancelOrdersRequest),
}

/// Bitget cancel all orders request.
#[derive(Clone, Debug, PartialEq)]
pub enum BitgetCancelAllOrdersRequest {
    /// Spot cancel all orders for a symbol.
    Spot(BitgetSpotCancelSymbolOrderRequest),
    /// USDT futures cancel all regular orders for a symbol.
    Mix(BitgetMixBatchCancelOrdersRequest),
}

fn param_str<'a>(params: Option<&'a Params>, key: &str) -> Option<&'a str> {
    params.and_then(|params| params.get_str(key))
}

fn raw_symbol(instrument_id: InstrumentId) -> String {
    extract_raw_symbol(instrument_id.symbol.as_str()).to_string()
}

fn price_to_string(price: Price) -> String {
    price.to_string()
}

fn quantity_to_string(quantity: Quantity) -> String {
    quantity.to_string()
}

fn side_to_bitget(side: OrderSide) -> anyhow::Result<&'static str> {
    match side {
        OrderSide::Buy => Ok("buy"),
        OrderSide::Sell => Ok("sell"),
        OrderSide::NoOrderSide => anyhow::bail!("Bitget order side must be Buy or Sell"),
    }
}

fn order_type_to_bitget(order_type: OrderType) -> anyhow::Result<&'static str> {
    match order_type {
        OrderType::Market | OrderType::StopMarket | OrderType::MarketIfTouched => Ok("market"),
        OrderType::Limit | OrderType::StopLimit | OrderType::LimitIfTouched => Ok("limit"),
        OrderType::TrailingStopMarket => Ok("market"),
        OrderType::TrailingStopLimit => Ok("limit"),
        OrderType::MarketToLimit => {
            anyhow::bail!("Bitget does not support Nautilus MarketToLimit orders")
        }
    }
}

fn force_from_tif(tif: TimeInForce, post_only: bool) -> anyhow::Result<String> {
    if post_only {
        return Ok("post_only".to_string());
    }

    let force = match tif {
        TimeInForce::Gtc => "gtc",
        TimeInForce::Ioc => "ioc",
        TimeInForce::Fok => "fok",
        TimeInForce::Gtd | TimeInForce::Day | TimeInForce::AtTheOpen | TimeInForce::AtTheClose => {
            anyhow::bail!("Bitget does not support Nautilus time in force {tif:?}")
        }
    };

    Ok(force.to_string())
}

fn trigger_type_to_bitget(
    trigger_type: Option<TriggerType>,
    params: Option<&Params>,
) -> anyhow::Result<String> {
    if let Some(raw) = param_str(params, PARAM_TRIGGER_TYPE) {
        return Ok(raw.to_string());
    }

    let trigger = match trigger_type {
        Some(TriggerType::MarkPrice) => "mark",
        Some(TriggerType::LastPrice | TriggerType::Default) | None => "market",
        Some(other) => anyhow::bail!("Bitget does not support Nautilus trigger type {other:?}"),
    };

    Ok(trigger.to_string())
}

fn is_plan_order(order_type: OrderType) -> bool {
    matches!(
        order_type,
        OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::MarketIfTouched
            | OrderType::LimitIfTouched
            | OrderType::TrailingStopMarket
            | OrderType::TrailingStopLimit
    )
}

fn is_limit_execution(order_type: OrderType) -> bool {
    matches!(
        order_type,
        OrderType::Limit | OrderType::StopLimit | OrderType::LimitIfTouched
    )
}

fn required_price(order: &OrderInitialized) -> anyhow::Result<String> {
    order
        .price
        .map(price_to_string)
        .context("Bitget limit order requires price")
}

fn required_trigger_price(order: &OrderInitialized) -> anyhow::Result<String> {
    order
        .trigger_price
        .map(price_to_string)
        .context("Bitget plan order requires trigger_price")
}

fn common_margin_mode(params: Option<&Params>) -> String {
    param_str(params, PARAM_MARGIN_MODE)
        .unwrap_or(DEFAULT_MARGIN_MODE)
        .to_string()
}

fn common_margin_coin(params: Option<&Params>) -> String {
    param_str(params, PARAM_MARGIN_COIN)
        .unwrap_or(DEFAULT_MARGIN_COIN)
        .to_string()
}

fn reject_quote_quantity(order: &OrderInitialized) -> anyhow::Result<()> {
    anyhow::ensure!(
        !order.quote_quantity,
        "Bitget adapter does not yet map Nautilus quote_quantity orders"
    );
    Ok(())
}

/// Maps a Nautilus order initialization to a Bitget REST submit order request.
pub fn map_submit_order(
    product_type: BitgetProductType,
    order: &OrderInitialized,
    params: Option<&Params>,
) -> anyhow::Result<BitgetSubmitOrderRequest> {
    reject_quote_quantity(order)?;

    match product_type {
        BitgetProductType::Spot => map_spot_submit_order(order, params),
        BitgetProductType::UsdtFutures => map_mix_submit_order(order, params),
    }
}

fn map_spot_submit_order(
    order: &OrderInitialized,
    params: Option<&Params>,
) -> anyhow::Result<BitgetSubmitOrderRequest> {
    anyhow::ensure!(
        !order.reduce_only,
        "Bitget Spot does not support reduce_only orders"
    );
    anyhow::ensure!(
        !matches!(
            order.order_type,
            OrderType::TrailingStopMarket | OrderType::TrailingStopLimit
        ),
        "Bitget Spot trailing stop orders are not mapped by this adapter"
    );
    anyhow::ensure!(
        !order.post_only || matches!(order.order_type, OrderType::Limit),
        "Bitget Spot post_only is only valid for limit orders"
    );

    let symbol = raw_symbol(order.instrument_id);
    let side = side_to_bitget(order.order_side)?.to_string();
    let order_type = order_type_to_bitget(order.order_type)?.to_string();
    let stp_mode = param_str(params, PARAM_STP_MODE).map(str::to_string);
    let client_oid = Some(order.client_order_id.to_string());

    if is_plan_order(order.order_type) {
        let execute_price = if is_limit_execution(order.order_type) {
            Some(required_price(order)?)
        } else {
            None
        };

        return Ok(BitgetSubmitOrderRequest::SpotPlan(
            BitgetSpotPlanOrderRequest {
                category: BitgetProductType::Spot.as_api_str().to_string(),
                symbol,
                side,
                order_type,
                trigger_price: required_trigger_price(order)?,
                execute_price,
                size: quantity_to_string(order.quantity),
                trigger_type: trigger_type_to_bitget(order.trigger_type, params)?,
                plan_type: "trigger".to_string(),
                client_oid,
                stp_mode,
            },
        ));
    }

    let price = if is_limit_execution(order.order_type) {
        Some(required_price(order)?)
    } else {
        None
    };
    let force = if is_limit_execution(order.order_type) || order.post_only {
        Some(force_from_tif(order.time_in_force, order.post_only)?)
    } else {
        None
    };

    Ok(BitgetSubmitOrderRequest::Spot(
        BitgetSpotPlaceOrderRequest {
            category: BitgetProductType::Spot.as_api_str().to_string(),
            symbol,
            side,
            order_type,
            force,
            price,
            size: quantity_to_string(order.quantity),
            client_oid,
            stp_mode,
        },
    ))
}

fn map_mix_submit_order(
    order: &OrderInitialized,
    params: Option<&Params>,
) -> anyhow::Result<BitgetSubmitOrderRequest> {
    anyhow::ensure!(
        !order.post_only || matches!(order.order_type, OrderType::Limit),
        "Bitget futures post_only is only valid for regular limit orders"
    );
    anyhow::ensure!(
        !matches!(
            order.order_type,
            OrderType::TrailingStopMarket | OrderType::TrailingStopLimit
        ),
        "Bitget UTA futures trailing stop orders are not mapped by this adapter"
    );

    let symbol = raw_symbol(order.instrument_id);
    let side = side_to_bitget(order.order_side)?.to_string();
    let margin_mode = common_margin_mode(params);
    let margin_coin = common_margin_coin(params);
    let trade_side = param_str(params, PARAM_TRADE_SIDE).map(str::to_string);
    let pos_side = param_str(params, PARAM_POS_SIDE).map(str::to_string);
    let stp_mode = param_str(params, PARAM_STP_MODE).map(str::to_string);
    let client_oid = Some(order.client_order_id.to_string());
    let reduce_only = order.reduce_only.then(|| "yes".to_string());

    if is_plan_order(order.order_type) {
        let execute_price = if is_limit_execution(order.order_type) {
            Some(required_price(order)?)
        } else {
            None
        };

        return Ok(BitgetSubmitOrderRequest::MixPlan(
            BitgetMixPlanOrderRequest {
                symbol,
                product_type: BitgetProductType::UsdtFutures.as_api_str().to_string(),
                margin_mode,
                margin_coin,
                size: quantity_to_string(order.quantity),
                side,
                trade_side,
                pos_side,
                order_type: order_type_to_bitget(order.order_type)?.to_string(),
                execute_price,
                trigger_price: required_trigger_price(order)?,
                trigger_type: trigger_type_to_bitget(order.trigger_type, params)?,
                plan_type: "trigger".to_string(),
                callback_ratio: None,
                client_oid,
                reduce_only,
                stp_mode,
            },
        ));
    }

    let price = if is_limit_execution(order.order_type) {
        Some(required_price(order)?)
    } else {
        None
    };
    let force = if is_limit_execution(order.order_type) || order.post_only {
        Some(force_from_tif(order.time_in_force, order.post_only)?)
    } else {
        None
    };

    Ok(BitgetSubmitOrderRequest::Mix(BitgetMixPlaceOrderRequest {
        symbol,
        product_type: BitgetProductType::UsdtFutures.as_api_str().to_string(),
        margin_mode,
        margin_coin,
        size: quantity_to_string(order.quantity),
        price,
        side,
        trade_side,
        pos_side,
        order_type: order_type_to_bitget(order.order_type)?.to_string(),
        force,
        client_oid,
        reduce_only,
        preset_stop_surplus_price: param_str(params, PARAM_PRESET_TAKE_PROFIT_PRICE)
            .map(str::to_string),
        preset_stop_loss_price: param_str(params, PARAM_PRESET_STOP_LOSS_PRICE).map(str::to_string),
        stp_mode,
    }))
}

/// Maps a Nautilus cancel command identity to a Bitget REST cancel request.
pub fn map_cancel_order(
    product_type: BitgetProductType,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
    params: Option<&Params>,
) -> anyhow::Result<BitgetCancelOrderRequest> {
    let symbol = raw_symbol(instrument_id);

    match product_type {
        BitgetProductType::Spot => Ok(BitgetCancelOrderRequest::Spot(
            BitgetSpotCancelOrderRequest {
                category: Some(BitgetProductType::Spot.as_api_str().to_string()),
                symbol,
                order_id: venue_order_id.map(|id| id.to_string()),
                client_oid: Some(client_order_id.to_string()),
            },
        )),
        BitgetProductType::UsdtFutures => {
            if let Some(plan_type) = param_str(params, PARAM_PLAN_TYPE) {
                let order_id = venue_order_id
                    .map(|id| id.to_string())
                    .context("Bitget plan order cancel requires venue_order_id")?;
                return Ok(BitgetCancelOrderRequest::MixPlan(
                    BitgetMixCancelPlanOrderRequest {
                        order_id,
                        symbol,
                        product_type: BitgetProductType::UsdtFutures.as_api_str().to_string(),
                        margin_coin: common_margin_coin(params),
                        plan_type: plan_type.to_string(),
                    },
                ));
            }

            Ok(BitgetCancelOrderRequest::Mix(BitgetMixCancelOrderRequest {
                symbol,
                product_type: BitgetProductType::UsdtFutures.as_api_str().to_string(),
                margin_coin: Some(common_margin_coin(params)),
                order_id: venue_order_id.map(|id| id.to_string()),
                client_oid: Some(client_order_id.to_string()),
            }))
        }
    }
}

/// Maps a Nautilus batch cancel command to a Bitget REST batch cancel request.
pub fn map_batch_cancel_orders(
    product_type: BitgetProductType,
    instrument_id: InstrumentId,
    cancels: &[CancelOrder],
    params: Option<&Params>,
) -> anyhow::Result<BitgetBatchCancelOrdersRequest> {
    anyhow::ensure!(
        param_str(params, PARAM_PLAN_TYPE).is_none(),
        "Bitget batch cancel for plan orders is not mapped by this adapter"
    );
    anyhow::ensure!(
        !cancels.is_empty(),
        "Bitget batch cancel requires at least one cancel"
    );

    let symbol = raw_symbol(instrument_id);
    let mut items = Vec::with_capacity(cancels.len());
    for cancel in cancels {
        anyhow::ensure!(
            cancel.instrument_id == instrument_id,
            "Bitget batch cancel requires all cancels to use the command instrument_id"
        );
        items.push(BitgetCancelBatchOrderItem {
            category: None,
            symbol: None,
            order_id: cancel.venue_order_id.map(|id| id.to_string()),
            client_oid: Some(cancel.client_order_id.to_string()),
        });
    }

    match product_type {
        BitgetProductType::Spot => Ok(BitgetBatchCancelOrdersRequest::Spot(
            BitgetSpotBatchCancelOrderRequest {
                category: BitgetProductType::Spot.as_api_str().to_string(),
                symbol: symbol.clone(),
                batch_mode: Some("single".to_string()),
                order_list: items
                    .into_iter()
                    .map(|mut item| {
                        item.category = Some(BitgetProductType::Spot.as_api_str().to_string());
                        item.symbol = Some(symbol.clone());
                        item
                    })
                    .collect(),
            },
        )),
        BitgetProductType::UsdtFutures => Ok(BitgetBatchCancelOrdersRequest::Mix(
            BitgetMixBatchCancelOrdersRequest {
                order_id_list: items
                    .into_iter()
                    .map(|mut item| {
                        item.category =
                            Some(BitgetProductType::UsdtFutures.as_api_str().to_string());
                        item.symbol = Some(symbol.clone());
                        item
                    })
                    .collect(),
                symbol: Some(symbol),
                product_type: BitgetProductType::UsdtFutures.as_api_str().to_string(),
                margin_coin: Some(common_margin_coin(params)),
            },
        )),
    }
}

/// Maps a Nautilus cancel-all command identity to a Bitget REST cancel-all request.
pub fn map_cancel_all_orders(
    product_type: BitgetProductType,
    instrument_id: InstrumentId,
    params: Option<&Params>,
) -> anyhow::Result<BitgetCancelAllOrdersRequest> {
    anyhow::ensure!(
        param_str(params, PARAM_PLAN_TYPE).is_none(),
        "Bitget cancel all for plan orders is not mapped by this adapter"
    );

    let symbol = raw_symbol(instrument_id);
    match product_type {
        BitgetProductType::Spot => Ok(BitgetCancelAllOrdersRequest::Spot(
            BitgetSpotCancelSymbolOrderRequest {
                category: BitgetProductType::Spot.as_api_str().to_string(),
                symbol: Some(symbol),
            },
        )),
        BitgetProductType::UsdtFutures => Ok(BitgetCancelAllOrdersRequest::Mix(
            BitgetMixBatchCancelOrdersRequest {
                order_id_list: Vec::new(),
                symbol: Some(symbol),
                product_type: BitgetProductType::UsdtFutures.as_api_str().to_string(),
                margin_coin: Some(common_margin_coin(params)),
            },
        )),
    }
}

/// Maps a Nautilus modify command identity to a Bitget REST modify request.
pub fn map_modify_order(
    product_type: BitgetProductType,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
    quantity: Option<Quantity>,
    price: Option<Price>,
    trigger_price: Option<Price>,
    params: Option<&Params>,
) -> anyhow::Result<BitgetModifyOrderRequest> {
    anyhow::ensure!(
        product_type == BitgetProductType::UsdtFutures,
        "Bitget Spot order modify is not mapped; cancel and replace instead"
    );

    let symbol = raw_symbol(instrument_id);
    if let Some(plan_type) = param_str(params, PARAM_PLAN_TYPE) {
        let order_id = venue_order_id
            .map(|id| id.to_string())
            .context("Bitget plan order modify requires venue_order_id")?;
        anyhow::ensure!(
            plan_type == "normal_plan" || plan_type == "profit_plan" || plan_type == "loss_plan",
            "Bitget modify-plan-order does not support plan_type={plan_type:?}"
        );

        return Ok(BitgetModifyOrderRequest::MixPlan(
            BitgetMixModifyPlanOrderRequest {
                order_id,
                symbol,
                product_type: BitgetProductType::UsdtFutures.as_api_str().to_string(),
                margin_coin: common_margin_coin(params),
                trigger_price: trigger_price.map(price_to_string),
                execute_price: price.map(price_to_string),
                size: quantity.map(quantity_to_string),
            },
        ));
    }

    Ok(BitgetModifyOrderRequest::Mix(BitgetMixModifyOrderRequest {
        symbol,
        product_type: BitgetProductType::UsdtFutures.as_api_str().to_string(),
        order_id: venue_order_id.map(|id| id.to_string()),
        client_oid: Some(client_order_id.to_string()),
        new_client_oid: None,
        new_size: quantity.map(quantity_to_string),
        new_price: price.map(price_to_string),
    }))
}

#[cfg(test)]
mod tests {
    use nautilus_core::{UnixNanos, uuid::UUID4};
    use nautilus_model::{
        enums::{ContingencyType, OrderSide, OrderType, TimeInForce, TriggerType},
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId},
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    const TS: UnixNanos = UnixNanos::new(1_700_000_000_000_000_000);

    fn order_init(
        instrument_id: &str,
        order_type: OrderType,
        price: Option<&str>,
        trigger_price: Option<&str>,
    ) -> OrderInitialized {
        OrderInitialized::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("S-001"),
            InstrumentId::from(instrument_id),
            ClientOrderId::from("O-001"),
            OrderSide::Buy,
            order_type,
            Quantity::from("0.001"),
            TimeInForce::Gtc,
            false,
            false,
            false,
            false,
            UUID4::new(),
            TS,
            TS,
            price.map(Price::from),
            trigger_price.map(Price::from),
            Some(TriggerType::LastPrice),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(ContingencyType::NoContingency),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn cancel_cmd(
        instrument_id: &str,
        client_order_id: &str,
        venue_order_id: Option<&str>,
    ) -> CancelOrder {
        CancelOrder::new(
            TraderId::from("TRADER-001"),
            Some(crate::common::consts::BITGET_CLIENT_ID.to_owned()),
            StrategyId::from("S-001"),
            InstrumentId::from(instrument_id),
            ClientOrderId::from(client_order_id),
            venue_order_id.map(VenueOrderId::from),
            UUID4::new(),
            TS,
            None,
            None,
        )
    }

    #[rstest]
    fn maps_spot_limit_order() {
        let order = order_init("BTCUSDT.BITGET", OrderType::Limit, Some("100.1"), None);

        let mapped = map_submit_order(BitgetProductType::Spot, &order, None).unwrap();

        let BitgetSubmitOrderRequest::Spot(request) = mapped else {
            panic!("expected spot regular order")
        };
        assert_eq!(request.category, "SPOT");
        assert_eq!(request.symbol, "BTCUSDT");
        assert_eq!(request.side, "buy");
        assert_eq!(request.order_type, "limit");
        assert_eq!(request.force.as_deref(), Some("gtc"));
        assert_eq!(request.price.as_deref(), Some("100.1"));
        assert_eq!(request.client_oid.as_deref(), Some("O-001"));
    }

    #[rstest]
    fn maps_mix_market_order() {
        let order = order_init("BTCUSDT-PERP.BITGET", OrderType::Market, None, None);

        let mapped = map_submit_order(BitgetProductType::UsdtFutures, &order, None).unwrap();

        let BitgetSubmitOrderRequest::Mix(request) = mapped else {
            panic!("expected mix regular order")
        };
        assert_eq!(request.symbol, "BTCUSDT");
        assert_eq!(request.product_type, "USDT-FUTURES");
        assert_eq!(request.margin_mode, "crossed");
        assert_eq!(request.margin_coin, "USDT");
        assert_eq!(request.order_type, "market");
        assert!(request.force.is_none());
    }

    #[rstest]
    fn maps_mix_stop_limit_to_plan_order() {
        let order = order_init(
            "BTCUSDT-PERP.BITGET",
            OrderType::StopLimit,
            Some("100.1"),
            Some("99.9"),
        );

        let mapped = map_submit_order(BitgetProductType::UsdtFutures, &order, None).unwrap();

        let BitgetSubmitOrderRequest::MixPlan(request) = mapped else {
            panic!("expected mix plan order")
        };
        assert_eq!(request.plan_type, "trigger");
        assert_eq!(request.order_type, "limit");
        assert_eq!(request.execute_price.as_deref(), Some("100.1"));
        assert_eq!(request.trigger_price, "99.9");
        assert_eq!(request.trigger_type, "market");
    }

    #[rstest]
    fn rejects_mix_trailing_stop_market_for_uta() {
        let order = order_init(
            "BTCUSDT-PERP.BITGET",
            OrderType::TrailingStopMarket,
            None,
            Some("99.9"),
        );

        let err = map_submit_order(BitgetProductType::UsdtFutures, &order, None).unwrap_err();

        assert!(err.to_string().contains("UTA futures trailing stop"));
    }

    #[rstest]
    fn maps_mix_cancel_by_client_order_id() {
        let mapped = map_cancel_order(
            BitgetProductType::UsdtFutures,
            InstrumentId::from("BTCUSDT-PERP.BITGET"),
            ClientOrderId::from("O-001"),
            None,
            None,
        )
        .unwrap();

        let BitgetCancelOrderRequest::Mix(request) = mapped else {
            panic!("expected mix cancel")
        };
        assert_eq!(request.symbol, "BTCUSDT");
        assert_eq!(request.client_oid.as_deref(), Some("O-001"));
        assert_eq!(request.margin_coin.as_deref(), Some("USDT"));
    }

    #[rstest]
    fn maps_spot_batch_cancel_orders() {
        let cancels = vec![
            cancel_cmd("BTCUSDT.BITGET", "O-001", Some("1")),
            cancel_cmd("BTCUSDT.BITGET", "O-002", None),
        ];

        let mapped = map_batch_cancel_orders(
            BitgetProductType::Spot,
            InstrumentId::from("BTCUSDT.BITGET"),
            &cancels,
            None,
        )
        .unwrap();

        let BitgetBatchCancelOrdersRequest::Spot(request) = mapped else {
            panic!("expected spot batch cancel")
        };
        assert_eq!(request.symbol, "BTCUSDT");
        assert_eq!(request.batch_mode.as_deref(), Some("single"));
        assert_eq!(request.order_list.len(), 2);
        assert_eq!(request.order_list[0].category.as_deref(), Some("SPOT"));
        assert_eq!(request.order_list[0].symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(request.order_list[0].order_id.as_deref(), Some("1"));
        assert_eq!(request.order_list[0].client_oid.as_deref(), Some("O-001"));
        assert_eq!(request.order_list[1].client_oid.as_deref(), Some("O-002"));
    }

    #[rstest]
    fn maps_mix_cancel_all_orders() {
        let mapped = map_cancel_all_orders(
            BitgetProductType::UsdtFutures,
            InstrumentId::from("BTCUSDT-PERP.BITGET"),
            None,
        )
        .unwrap();

        let BitgetCancelAllOrdersRequest::Mix(request) = mapped else {
            panic!("expected mix cancel all")
        };
        assert!(request.order_id_list.is_empty());
        assert_eq!(request.symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(request.product_type, "USDT-FUTURES");
        assert_eq!(request.margin_coin.as_deref(), Some("USDT"));
    }

    #[rstest]
    fn rejects_batch_cancel_plan_orders() {
        let cancels = vec![cancel_cmd("BTCUSDT-PERP.BITGET", "O-001", Some("1"))];
        let mut params = Params::new();
        params.insert(
            PARAM_PLAN_TYPE.to_string(),
            serde_json::json!("normal_plan"),
        );

        let err = map_batch_cancel_orders(
            BitgetProductType::UsdtFutures,
            InstrumentId::from("BTCUSDT-PERP.BITGET"),
            &cancels,
            Some(&params),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Bitget batch cancel for plan orders is not mapped")
        );
    }

    #[rstest]
    fn maps_mix_modify_regular_order() {
        let mapped = map_modify_order(
            BitgetProductType::UsdtFutures,
            InstrumentId::from("BTCUSDT-PERP.BITGET"),
            ClientOrderId::from("O-001"),
            Some(VenueOrderId::from("123")),
            Some(Quantity::from("0.002")),
            Some(Price::from("101.0")),
            None,
            None,
        )
        .unwrap();

        let BitgetModifyOrderRequest::Mix(request) = mapped else {
            panic!("expected mix modify")
        };
        assert_eq!(request.order_id.as_deref(), Some("123"));
        assert_eq!(request.new_size.as_deref(), Some("0.002"));
        assert_eq!(request.new_price.as_deref(), Some("101.0"));
    }
}
