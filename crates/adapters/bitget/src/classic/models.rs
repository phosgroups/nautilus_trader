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

//! Classic REST wire models and conversion to shared adapter records.

use serde::{Deserialize, Serialize};

use crate::http::models::{self as shared, BitgetDecimalValue, BitgetFillFeeDetail};

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetSpotPlaceOrderRequest {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub order_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(default)]
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stp_mode: Option<String>,
}

impl From<&shared::BitgetSpotPlaceOrderRequest> for BitgetSpotPlaceOrderRequest {
    fn from(value: &shared::BitgetSpotPlaceOrderRequest) -> Self {
        Self {
            symbol: value.symbol.clone(),
            side: value.side.clone(),
            order_type: value.order_type.clone(),
            force: value.force.clone(),
            price: value.price.clone(),
            size: value.size.clone(),
            client_oid: value.client_oid.clone(),
            stp_mode: value.stp_mode.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetSpotPlanOrderRequest {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub order_type: String,
    #[serde(default)]
    pub trigger_price: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execute_price: Option<String>,
    #[serde(default)]
    pub size: String,
    #[serde(default)]
    pub trigger_type: String,
    #[serde(default)]
    pub plan_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stp_mode: Option<String>,
}

impl From<&shared::BitgetSpotPlanOrderRequest> for BitgetSpotPlanOrderRequest {
    fn from(value: &shared::BitgetSpotPlanOrderRequest) -> Self {
        Self {
            symbol: value.symbol.clone(),
            side: value.side.clone(),
            order_type: value.order_type.clone(),
            trigger_price: value.trigger_price.clone(),
            execute_price: value.execute_price.clone(),
            size: value.size.clone(),
            trigger_type: match value.trigger_type.as_str() {
                "mark" | "mark_price" => "mark_price",
                "market" | "fill_price" => "fill_price",
                other => other,
            }
            .to_string(),
            plan_type: "amount".to_string(),
            client_oid: value.client_oid.clone(),
            stp_mode: value.stp_mode.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetSpotCancelOrderRequest {
    #[serde(default)]
    pub symbol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
}

impl From<&shared::BitgetSpotCancelOrderRequest> for BitgetSpotCancelOrderRequest {
    fn from(value: &shared::BitgetSpotCancelOrderRequest) -> Self {
        Self {
            symbol: value.symbol.clone(),
            order_id: value.order_id.clone(),
            client_oid: value.client_oid.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetCancelBatchOrderItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
}

impl From<&shared::BitgetCancelBatchOrderItem> for BitgetCancelBatchOrderItem {
    fn from(value: &shared::BitgetCancelBatchOrderItem) -> Self {
        Self {
            order_id: value.order_id.clone(),
            client_oid: value.client_oid.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetSpotBatchCancelOrderRequest {
    #[serde(default)]
    pub symbol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_mode: Option<String>,
    #[serde(default)]
    pub order_list: Vec<BitgetCancelBatchOrderItem>,
}

impl From<&shared::BitgetSpotBatchCancelOrderRequest> for BitgetSpotBatchCancelOrderRequest {
    fn from(value: &shared::BitgetSpotBatchCancelOrderRequest) -> Self {
        Self {
            symbol: value.symbol.clone(),
            batch_mode: value.batch_mode.clone(),
            order_list: value.order_list.iter().map(Into::into).collect(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetSpotCancelSymbolOrderRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

impl From<&shared::BitgetSpotCancelSymbolOrderRequest> for BitgetSpotCancelSymbolOrderRequest {
    fn from(value: &shared::BitgetSpotCancelSymbolOrderRequest) -> Self {
        Self {
            symbol: value.symbol.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMixPlaceOrderRequest {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub product_type: String,
    #[serde(default)]
    pub margin_mode: String,
    #[serde(default)]
    pub margin_coin: String,
    #[serde(default)]
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(default)]
    pub side: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_side: Option<String>,
    #[serde(default)]
    pub order_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preset_stop_surplus_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preset_stop_loss_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stp_mode: Option<String>,
}

impl From<&shared::BitgetMixPlaceOrderRequest> for BitgetMixPlaceOrderRequest {
    fn from(value: &shared::BitgetMixPlaceOrderRequest) -> Self {
        Self {
            symbol: value.symbol.clone(),
            product_type: value.product_type.clone(),
            margin_mode: if value.margin_mode == "cross" {
                "crossed".to_string()
            } else {
                value.margin_mode.clone()
            },
            margin_coin: value.margin_coin.clone(),
            size: value.size.clone(),
            price: value.price.clone(),
            side: value.side.clone(),
            trade_side: value.trade_side.clone(),
            order_type: value.order_type.clone(),
            force: value.force.clone(),
            client_oid: value.client_oid.clone(),
            reduce_only: value.reduce_only.as_ref().map(|v| v.to_ascii_uppercase()),
            preset_stop_surplus_price: value.preset_stop_surplus_price.clone(),
            preset_stop_loss_price: value.preset_stop_loss_price.clone(),
            stp_mode: value.stp_mode.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMixPlanOrderRequest {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub product_type: String,
    #[serde(default)]
    pub margin_mode: String,
    #[serde(default)]
    pub margin_coin: String,
    #[serde(default)]
    pub size: String,
    #[serde(default)]
    pub side: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_side: Option<String>,
    #[serde(default)]
    pub order_type: String,
    #[serde(default, rename = "price", skip_serializing_if = "Option::is_none")]
    pub execute_price: Option<String>,
    #[serde(default)]
    pub trigger_price: String,
    #[serde(default)]
    pub trigger_type: String,
    #[serde(default)]
    pub plan_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_ratio: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stp_mode: Option<String>,
}

impl From<&shared::BitgetMixPlanOrderRequest> for BitgetMixPlanOrderRequest {
    fn from(value: &shared::BitgetMixPlanOrderRequest) -> Self {
        Self {
            symbol: value.symbol.clone(),
            product_type: value.product_type.clone(),
            margin_mode: if value.margin_mode == "cross" {
                "crossed".to_string()
            } else {
                value.margin_mode.clone()
            },
            margin_coin: value.margin_coin.clone(),
            size: value.size.clone(),
            side: value.side.clone(),
            trade_side: value.trade_side.clone(),
            order_type: value.order_type.clone(),
            execute_price: value.execute_price.clone(),
            trigger_price: value.trigger_price.clone(),
            trigger_type: match value.trigger_type.as_str() {
                "mark" | "mark_price" => "mark_price",
                "market" | "fill_price" => "fill_price",
                other => other,
            }
            .to_string(),
            plan_type: "normal_plan".to_string(),
            callback_ratio: value.callback_ratio.clone(),
            client_oid: value.client_oid.clone(),
            reduce_only: value.reduce_only.as_ref().map(|v| v.to_ascii_uppercase()),
            stp_mode: value.stp_mode.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMixModifyOrderRequest {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub product_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_client_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_price: Option<String>,
}

impl From<&shared::BitgetMixModifyOrderRequest> for BitgetMixModifyOrderRequest {
    fn from(value: &shared::BitgetMixModifyOrderRequest) -> Self {
        Self {
            symbol: value.symbol.clone(),
            product_type: value.product_type.clone(),
            order_id: value.order_id.clone(),
            client_oid: value.client_oid.clone(),
            new_client_oid: value.new_client_oid.clone(),
            new_size: value.new_size.clone(),
            new_price: value.new_price.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMixModifyPlanOrderRequest {
    #[serde(default)]
    pub order_id: String,
    #[serde(default)]
    pub product_type: String,
    #[serde(
        default,
        rename = "newTriggerPrice",
        skip_serializing_if = "Option::is_none"
    )]
    pub trigger_price: Option<String>,
    #[serde(default, rename = "newPrice", skip_serializing_if = "Option::is_none")]
    pub execute_price: Option<String>,
    #[serde(default, rename = "newSize", skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
}

impl From<&shared::BitgetMixModifyPlanOrderRequest> for BitgetMixModifyPlanOrderRequest {
    fn from(value: &shared::BitgetMixModifyPlanOrderRequest) -> Self {
        Self {
            order_id: value.order_id.clone(),
            product_type: value.product_type.clone(),
            trigger_price: value.trigger_price.clone(),
            execute_price: value.execute_price.clone(),
            size: value.size.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMixCancelOrderRequest {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub product_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin_coin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
}

impl From<&shared::BitgetMixCancelOrderRequest> for BitgetMixCancelOrderRequest {
    fn from(value: &shared::BitgetMixCancelOrderRequest) -> Self {
        Self {
            symbol: value.symbol.clone(),
            product_type: value.product_type.clone(),
            margin_coin: value.margin_coin.clone(),
            order_id: value.order_id.clone(),
            client_oid: value.client_oid.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMixBatchCancelOrdersRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_id_list: Vec<BitgetCancelBatchOrderItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(default)]
    pub product_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin_coin: Option<String>,
}

impl From<&shared::BitgetMixBatchCancelOrdersRequest> for BitgetMixBatchCancelOrdersRequest {
    fn from(value: &shared::BitgetMixBatchCancelOrdersRequest) -> Self {
        Self {
            order_id_list: value.order_id_list.iter().map(Into::into).collect(),
            symbol: value.symbol.clone(),
            product_type: value.product_type.clone(),
            margin_coin: value.margin_coin.clone(),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetSpotSymbol {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub base_coin: String,
    #[serde(default)]
    pub quote_coin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_trade_amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_trade_amount: Option<String>,
    #[serde(
        default,
        rename = "minTradeUSDT",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_trade_usdt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maker_fee_rate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub taker_fee_rate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_precision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity_precision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_precision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

impl From<BitgetSpotSymbol> for shared::BitgetSpotSymbol {
    fn from(value: BitgetSpotSymbol) -> Self {
        Self {
            symbol: value.symbol,
            base_coin: value.base_coin,
            quote_coin: value.quote_coin,
            min_trade_amount: value.min_trade_amount,
            max_trade_amount: value.max_trade_amount,
            min_trade_usdt: value.min_trade_usdt,
            maker_fee_rate: value.maker_fee_rate,
            taker_fee_rate: value.taker_fee_rate,
            price_precision: value.price_precision,
            quantity_precision: value.quantity_precision,
            quote_precision: value.quote_precision,
            status: value.status,
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMixContract {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub base_coin: String,
    #[serde(default)]
    pub quote_coin: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_type: Option<String>,
    #[serde(skip)]
    pub symbol_type: Option<String>,
    #[serde(
        default,
        rename = "symbolType",
        skip_serializing_if = "Option::is_none"
    )]
    pub contract_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin_coin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maker_fee_rate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub taker_fee_rate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_trade_num: Option<String>,
    #[serde(
        default,
        rename = "minTradeUSDT",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_trade_usdt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_order_qty: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_multiplier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_place: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_place: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_end_step: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_lever: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_lever: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fund_interval: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_status: Option<String>,
}

impl From<BitgetMixContract> for shared::BitgetMixContract {
    fn from(value: BitgetMixContract) -> Self {
        Self {
            symbol: value.symbol,
            base_coin: value.base_coin,
            quote_coin: value.quote_coin,
            product_type: value.product_type,
            symbol_type: value.symbol_type,
            contract_type: value.contract_type,
            margin_coin: value.margin_coin,
            maker_fee_rate: value.maker_fee_rate,
            taker_fee_rate: value.taker_fee_rate,
            min_trade_num: value.min_trade_num,
            min_trade_usdt: value.min_trade_usdt,
            max_order_qty: value.max_order_qty,
            size_multiplier: value.size_multiplier,
            price_place: value.price_place,
            volume_place: value.volume_place,
            price_end_step: value.price_end_step,
            max_lever: value.max_lever,
            min_lever: value.min_lever,
            fund_interval: value.fund_interval,
            symbol_status: value.symbol_status,
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetOrderBookSnapshot {
    #[serde(default)]
    pub bids: Vec<Vec<BitgetDecimalValue>>,
    #[serde(default)]
    pub asks: Vec<Vec<BitgetDecimalValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<String>,
}

impl From<BitgetOrderBookSnapshot> for shared::BitgetOrderBookSnapshot {
    fn from(value: BitgetOrderBookSnapshot) -> Self {
        Self {
            bids: value.bids,
            asks: value.asks,
            ts: value.ts,
            seq: value.seq,
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMarketTrade {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(default)]
    pub trade_id: String,
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub size: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub ts: String,
}

impl From<BitgetMarketTrade> for shared::BitgetMarketTrade {
    fn from(value: BitgetMarketTrade) -> Self {
        Self {
            symbol: value.symbol,
            trade_id: value.trade_id,
            price: value.price,
            size: value.size,
            side: value.side,
            ts: value.ts,
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetFundingRate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(default)]
    pub funding_rate: String,
    #[serde(default)]
    pub funding_time: String,
}

impl From<BitgetFundingRate> for shared::BitgetFundingRate {
    fn from(value: BitgetFundingRate) -> Self {
        Self {
            symbol: value.symbol,
            funding_rate: value.funding_rate,
            funding_time: value.funding_time,
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetSpotAsset {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locked: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_available: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub u_time: Option<String>,
}

impl From<BitgetSpotAsset> for shared::BitgetSpotAsset {
    fn from(value: BitgetSpotAsset) -> Self {
        Self {
            coin: value.coin,
            available: value.available,
            frozen: value.frozen,
            locked: value.locked,
            limit_available: value.limit_available,
            u_time: value.u_time,
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMixAccount {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin_coin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locked: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crossed_margin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolated_margin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_equity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usdt_equity: Option<String>,
    #[serde(
        default,
        rename = "unrealizedPL",
        skip_serializing_if = "Option::is_none"
    )]
    pub unrealized_pnl: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub union_mm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub u_time: Option<String>,
}

impl From<BitgetMixAccount> for shared::BitgetMixAccount {
    fn from(value: BitgetMixAccount) -> Self {
        Self {
            margin_coin: value.margin_coin,
            locked: value.locked,
            available: value.available,
            crossed_margin: value.crossed_margin,
            isolated_margin: value.isolated_margin,
            account_equity: value.account_equity,
            usdt_equity: value.usdt_equity,
            unrealized_pnl: value.unrealized_pnl,
            union_mm: value.union_mm,
            u_time: value.u_time,
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetMixPosition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pos_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin_coin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_side: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pos_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available: Option<String>,
    #[serde(skip)]
    pub average_open_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_price_avg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mark_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liquidation_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leverage: Option<String>,
    #[serde(
        default,
        rename = "achievedProfits",
        skip_serializing_if = "Option::is_none"
    )]
    pub realized_pnl: Option<String>,
    #[serde(
        default,
        rename = "unrealizedPL",
        skip_serializing_if = "Option::is_none"
    )]
    pub unrealized_pnl: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub c_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub u_time: Option<String>,
}

impl From<BitgetMixPosition> for shared::BitgetMixPosition {
    fn from(value: BitgetMixPosition) -> Self {
        Self {
            symbol: value.symbol.map(|s| s.to_ascii_uppercase()),
            product_type: value.product_type,
            pos_id: value.pos_id,
            margin_coin: value.margin_coin,
            hold_side: value.hold_side,
            pos_mode: value.pos_mode,
            margin_mode: value.margin_mode,
            total: value.total,
            available: value.available,
            average_open_price: value.average_open_price,
            open_price_avg: value.open_price_avg,
            mark_price: value.mark_price,
            liquidation_price: value.liquidation_price,
            leverage: value.leverage,
            realized_pnl: value.realized_pnl,
            unrealized_pnl: value.unrealized_pnl,
            c_time: value.c_time,
            u_time: value.u_time,
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetOrderStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(default)]
    pub execute_price: Option<String>,
    #[serde(default)]
    pub execute_order_id: Option<String>,
    #[serde(skip)]
    pub avg_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_avg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    #[serde(
        default,
        rename = "baseVolume",
        skip_serializing_if = "Option::is_none"
    )]
    pub filled_size: Option<String>,
    #[serde(skip)]
    pub filled_qty: Option<String>,
    #[serde(skip)]
    pub cumulative_filled_qty: Option<String>,
    #[serde(
        default,
        rename = "quoteVolume",
        skip_serializing_if = "Option::is_none"
    )]
    pub quote_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_side: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<String>,
    #[serde(
        default,
        alias = "state",
        alias = "planStatus",
        skip_serializing_if = "Option::is_none"
    )]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin_coin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub c_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub u_time: Option<String>,
}

impl BitgetOrderStatus {
    pub(crate) fn into_futures(mut self) -> shared::BitgetOrderStatus {
        if let Some(symbol) = self.symbol.as_mut() {
            symbol.make_ascii_uppercase();
        }
        self.product_type = Some("USDT-FUTURES".to_string());
        self.into()
    }
}

impl From<BitgetOrderStatus> for shared::BitgetOrderStatus {
    fn from(value: BitgetOrderStatus) -> Self {
        let is_quote_quantity = value
            .product_type
            .as_deref()
            .is_none_or(|product| product.eq_ignore_ascii_case("SPOT"))
            && value
                .order_type
                .as_deref()
                .is_some_and(|kind| kind.eq_ignore_ascii_case("market"))
            && value
                .side
                .as_deref()
                .is_some_and(|side| side.eq_ignore_ascii_case("buy"));
        Self {
            symbol: value.symbol,
            product_type: value.product_type,
            order_id: value
                .execute_order_id
                .filter(|s| !s.is_empty() && s != "0")
                .or(value.order_id),
            client_oid: value.client_oid,
            price: value
                .price
                .filter(|p| !p.is_empty())
                .or(value.execute_price),
            avg_price: value.avg_price,
            price_avg: value.price_avg,
            size: value.size,
            is_quote_quantity,
            filled_size: value.filled_size,
            filled_qty: value.filled_qty,
            cumulative_filled_qty: value.cumulative_filled_qty,
            quote_size: value.quote_size,
            side: value.side,
            trade_side: value.trade_side,
            order_type: value.order_type,
            force: value
                .force
                .or_else(|| value.trigger_price.as_ref().map(|_| "gtc".to_string())),
            status: value.status.map(|s| match s.as_str() {
                "executing" | "executed" => "triggered".to_string(),
                "fail_execute" => "canceled".to_string(),
                _ => s,
            }),
            trigger_price: value.trigger_price,
            trigger_type: value.trigger_type,
            reduce_only: value.reduce_only,
            margin_coin: value.margin_coin,
            c_time: value.c_time,
            u_time: value.u_time.filter(|s| !s.is_empty()),
        }
    }
}

/// Classic wire representation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetFill {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_side: Option<String>,
    #[serde(default, alias = "priceAvg", skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(
        default,
        rename = "baseVolume",
        skip_serializing_if = "Option::is_none",
        alias = "size"
    )]
    pub size: Option<String>,
    #[serde(
        default,
        rename = "quoteVolume",
        alias = "amount",
        skip_serializing_if = "Option::is_none"
    )]
    pub quote_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_coin: Option<String>,
    #[serde(default, deserialize_with = "deserialize_fee_detail")]
    pub fee_detail: Option<BitgetFillFeeDetail>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub margin_coin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_maker: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub c_time: Option<String>,
}

impl BitgetFill {
    pub(crate) fn into_futures(mut self) -> shared::BitgetFill {
        if let Some(symbol) = self.symbol.as_mut() {
            symbol.make_ascii_uppercase();
        }
        self.product_type = Some("USDT-FUTURES".to_string());
        self.into()
    }
}

impl From<BitgetFill> for shared::BitgetFill {
    fn from(value: BitgetFill) -> Self {
        Self {
            symbol: value.symbol,
            product_type: value.product_type,
            order_id: value.order_id,
            client_oid: value.client_oid,
            trade_id: value.trade_id,
            side: value.side,
            trade_side: value.trade_side,
            price: value.price,
            size: value.size,
            quote_size: value.quote_size,
            fee: value.fee,
            fee_coin: value.fee_coin,
            fee_detail: value.fee_detail,
            margin_coin: value.margin_coin,
            trade_scope: value.trade_scope,
            is_maker: value.is_maker,
            c_time: value.c_time,
        }
    }
}

/// Classic per-order cancel failures use errorCode/errorMsg.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetCancelBatchResult {
    pub order_id: Option<String>,
    pub client_oid: Option<String>,
    #[serde(alias = "code")]
    pub error_code: Option<String>,
    #[serde(alias = "msg")]
    pub error_msg: Option<String>,
}

impl From<BitgetCancelBatchResult> for shared::BitgetCancelBatchResult {
    fn from(row: BitgetCancelBatchResult) -> Self {
        Self {
            order_id: row.order_id,
            client_oid: row.client_oid,
            code: row.error_code,
            error_msg: row.error_msg,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BitgetCancelBatchResponse {
    #[serde(default)]
    pub success_list: Vec<BitgetCancelBatchResult>,
    #[serde(default)]
    pub failure_list: Vec<BitgetCancelBatchResult>,
}

impl From<BitgetCancelBatchResponse> for shared::BitgetCancelBatchResponse {
    fn from(row: BitgetCancelBatchResponse) -> Self {
        Self {
            success_list: row.success_list.into_iter().map(Into::into).collect(),
            failure_list: row.failure_list.into_iter().map(Into::into).collect(),
        }
    }
}

/// Converts only Classic fee names, preserving the sign used by the shared commission parser.
pub(crate) fn normalize_fee_detail(value: &mut serde_json::Value) -> serde_json::Result<()> {
    if let Some(text) = value.as_str() {
        if text.trim().is_empty() {
            *value = serde_json::Value::Null;
            return Ok(());
        }
        *value = serde_json::from_str(text)?;
    }
    match value {
        serde_json::Value::Array(rows) => {
            for row in rows {
                normalize_fee_detail(row)?;
            }
        }
        serde_json::Value::Object(row) => {
            if let Some(fee) = row.remove("totalFee") {
                row.insert(
                    "fee".to_string(),
                    if fee.is_number() {
                        serde_json::Value::String(fee.to_string())
                    } else {
                        fee
                    },
                );
            }
        }
        _ => {}
    }
    Ok(())
}

fn deserialize_fee_detail<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<BitgetFillFeeDetail>, D::Error> {
    let mut value = serde_json::Value::deserialize(deserializer)?;
    normalize_fee_detail(&mut value).map_err(serde::de::Error::custom)?;
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}
