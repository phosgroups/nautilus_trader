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

//! Data transfer objects for Bitget REST responses.

use serde::{Deserialize, Serialize};

/// Bitget decimal fields are normally returned as strings, but some market-data payloads have
/// historically used JSON numbers. Keep the raw value lossless enough for model parsing.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum BitgetDecimalValue {
    /// Decimal encoded as a string.
    String(String),
    /// Decimal encoded as a JSON number.
    Number(serde_json::Number),
}

impl Default for BitgetDecimalValue {
    fn default() -> Self {
        Self::String(String::new())
    }
}

impl BitgetDecimalValue {
    /// Returns the value as a decimal string.
    #[must_use]
    pub fn as_decimal_str(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Number(value) => value.to_string(),
        }
    }
}

/// Bitget REST response envelope.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetResponse<T> {
    /// Bitget response code (`00000` indicates success).
    pub code: String,
    /// Response message.
    pub msg: String,
    /// Server request time in milliseconds.
    #[serde(default)]
    pub request_time: Option<i64>,
    /// Response payload.
    #[serde(default)]
    pub data: T,
}

impl<T> BitgetResponse<T> {
    /// Returns `true` when Bitget returned a successful envelope.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.code == "00000"
    }
}

/// Bitget UTA Spot instrument definition.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSpotSymbol {
    /// Raw Bitget symbol.
    pub symbol: String,
    /// Base asset.
    pub base_coin: String,
    /// Quote asset.
    pub quote_coin: String,
    /// Minimum base quantity.
    #[serde(default, rename = "minOrderQty")]
    pub min_trade_amount: Option<String>,
    /// Maximum base quantity.
    #[serde(default, rename = "maxOrderQty")]
    pub max_trade_amount: Option<String>,
    /// Minimum quote notional.
    #[serde(default, rename = "minOrderAmount")]
    pub min_trade_usdt: Option<String>,
    /// Maker fee rate.
    #[serde(default)]
    pub maker_fee_rate: Option<String>,
    /// Taker fee rate.
    #[serde(default)]
    pub taker_fee_rate: Option<String>,
    /// Price decimal places.
    #[serde(default)]
    pub price_precision: Option<String>,
    /// Base quantity decimal places.
    #[serde(default)]
    pub quantity_precision: Option<String>,
    /// Quote decimal places.
    #[serde(default)]
    pub quote_precision: Option<String>,
    /// Symbol status.
    #[serde(default)]
    pub status: Option<String>,
}

/// Bitget UTA USDT-FUTURES instrument definition.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixContract {
    /// Raw Bitget symbol.
    pub symbol: String,
    /// Base asset.
    pub base_coin: String,
    /// Quote asset.
    pub quote_coin: String,
    /// Product type, expected `USDT-FUTURES`.
    #[serde(default, rename = "category")]
    pub product_type: Option<String>,
    /// Symbol type/classification.
    #[serde(default)]
    pub symbol_type: Option<String>,
    /// UTA contract type, expected perpetual for this adapter.
    #[serde(default, rename = "type")]
    pub contract_type: Option<String>,
    /// Margin/settlement coin.
    #[serde(default)]
    pub margin_coin: Option<String>,
    /// Maker fee rate.
    #[serde(default)]
    pub maker_fee_rate: Option<String>,
    /// Taker fee rate.
    #[serde(default)]
    pub taker_fee_rate: Option<String>,
    /// Minimum order quantity.
    #[serde(default, rename = "minOrderQty")]
    pub min_trade_num: Option<String>,
    /// Minimum order notional.
    #[serde(default, rename = "minOrderAmount")]
    pub min_trade_usdt: Option<String>,
    /// Maximum per-order quantity.
    #[serde(default, rename = "maxOrderQty")]
    pub max_order_qty: Option<String>,
    /// Contract size/multiplier.
    #[serde(default, rename = "quantityMultiplier")]
    pub size_multiplier: Option<String>,
    /// Price precision as decimal places.
    #[serde(default, rename = "pricePrecision")]
    pub price_place: Option<String>,
    /// Quantity precision as decimal places.
    #[serde(default, rename = "quantityPrecision")]
    pub volume_place: Option<String>,
    /// Price end step, used by Bitget as a price increment hint.
    #[serde(default, rename = "priceMultiplier")]
    pub price_end_step: Option<String>,
    /// Max leverage.
    #[serde(default, rename = "maxLeverage")]
    pub max_lever: Option<String>,
    /// Min leverage.
    #[serde(default, rename = "minLeverage")]
    pub min_lever: Option<String>,
    /// Funding interval in hours.
    #[serde(default)]
    pub fund_interval: Option<String>,
    /// Symbol status.
    #[serde(default, rename = "status")]
    pub symbol_status: Option<String>,
}

/// Bitget order book snapshot payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetOrderBookSnapshot {
    /// Bid levels as `[price, size]`.
    #[serde(default, rename = "b")]
    pub bids: Vec<Vec<BitgetDecimalValue>>,
    /// Ask levels as `[price, size]`.
    #[serde(default, rename = "a")]
    pub asks: Vec<Vec<BitgetDecimalValue>>,
    /// Exchange timestamp in milliseconds.
    #[serde(default)]
    pub ts: Option<String>,
    /// Optional sequence number returned by some Bitget book endpoints.
    #[serde(default)]
    pub seq: Option<String>,
}

/// Bitget public market trade payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMarketTrade {
    /// Raw Bitget symbol, when present.
    #[serde(default)]
    pub symbol: Option<String>,
    /// Exchange trade ID.
    #[serde(default, rename = "execId")]
    pub trade_id: String,
    /// Trade price.
    #[serde(default)]
    pub price: String,
    /// Trade size.
    #[serde(default)]
    pub size: String,
    /// Aggressor side (`buy` or `sell`).
    #[serde(default)]
    pub side: String,
    /// Exchange timestamp in milliseconds.
    #[serde(default)]
    pub ts: String,
}

/// Bitget historical funding rate payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetFundingRate {
    /// Raw Bitget symbol, when present.
    #[serde(default)]
    pub symbol: Option<String>,
    /// Funding rate as a decimal string.
    #[serde(default)]
    pub funding_rate: String,
    /// Funding timestamp in milliseconds.
    #[serde(default, rename = "fundingRateTimestamp")]
    pub funding_time: String,
}

/// Bitget UTA historical funding rates payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BitgetFundingRatesData {
    #[serde(default)]
    pub result_list: Vec<BitgetFundingRate>,
}

/// Bitget REST order acknowledgement payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetOrderAck {
    /// Venue order ID.
    #[serde(default)]
    pub order_id: Option<String>,
    /// Client order ID.
    #[serde(default)]
    pub client_oid: Option<String>,
    /// Per-item success flag returned by some batch-like endpoints.
    #[serde(default)]
    pub success: Option<bool>,
    /// Optional venue message for per-item responses.
    #[serde(default)]
    pub msg: Option<String>,
}

/// Bitget Spot place order request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSpotPlaceOrderRequest {
    pub category: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    #[serde(rename = "timeInForce", skip_serializing_if = "Option::is_none")]
    pub force: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(rename = "qty")]
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stp_mode: Option<String>,
}

/// Bitget Spot trigger/plan order request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSpotPlanOrderRequest {
    pub category: String,
    pub symbol: String,
    pub side: String,
    #[serde(rename = "triggerOrderType")]
    pub order_type: String,
    #[serde(rename = "triggerPrice")]
    pub trigger_price: String,
    #[serde(rename = "triggerOrderPrice", skip_serializing_if = "Option::is_none")]
    pub execute_price: Option<String>,
    #[serde(rename = "qty")]
    pub size: String,
    #[serde(rename = "triggerBy")]
    pub trigger_type: String,
    #[serde(rename = "type")]
    pub plan_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stp_mode: Option<String>,
}

/// Bitget Spot cancel order request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSpotCancelOrderRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
}

/// Bitget batch cancel order identity item.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetCancelBatchOrderItem {
    /// Product category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// Raw Bitget symbol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// Venue order ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    /// Client order ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
}

/// Bitget Spot batch cancel orders request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSpotBatchCancelOrderRequest {
    pub category: String,
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_list: Vec<BitgetCancelBatchOrderItem>,
}

/// Bitget Spot cancel all orders for a symbol request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSpotCancelSymbolOrderRequest {
    pub category: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

/// Bitget Mix place order request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixPlaceOrderRequest {
    pub symbol: String,
    #[serde(rename = "category")]
    pub product_type: String,
    pub margin_mode: String,
    #[serde(skip_serializing)]
    pub margin_coin: String,
    #[serde(rename = "qty")]
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    pub side: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_side: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pos_side: Option<String>,
    pub order_type: String,
    #[serde(rename = "timeInForce", skip_serializing_if = "Option::is_none")]
    pub force: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<String>,
    #[serde(rename = "takeProfit", skip_serializing_if = "Option::is_none")]
    pub preset_stop_surplus_price: Option<String>,
    #[serde(rename = "stopLoss", skip_serializing_if = "Option::is_none")]
    pub preset_stop_loss_price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stp_mode: Option<String>,
}

/// Bitget Mix trigger/plan order request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixPlanOrderRequest {
    pub symbol: String,
    #[serde(rename = "category")]
    pub product_type: String,
    pub margin_mode: String,
    #[serde(skip_serializing)]
    pub margin_coin: String,
    #[serde(rename = "qty")]
    pub size: String,
    pub side: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_side: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pos_side: Option<String>,
    #[serde(rename = "triggerOrderType")]
    pub order_type: String,
    #[serde(rename = "triggerOrderPrice", skip_serializing_if = "Option::is_none")]
    pub execute_price: Option<String>,
    #[serde(rename = "triggerPrice")]
    pub trigger_price: String,
    #[serde(rename = "triggerBy")]
    pub trigger_type: String,
    #[serde(rename = "type")]
    pub plan_type: String,
    #[serde(skip_serializing)]
    pub callback_ratio: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stp_mode: Option<String>,
}

/// Bitget Mix modify order request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixModifyOrderRequest {
    pub symbol: String,
    #[serde(rename = "category")]
    pub product_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
    #[serde(skip_serializing)]
    pub new_client_oid: Option<String>,
    #[serde(rename = "qty", skip_serializing_if = "Option::is_none")]
    pub new_size: Option<String>,
    #[serde(rename = "price", skip_serializing_if = "Option::is_none")]
    pub new_price: Option<String>,
}

/// Bitget Mix modify plan order request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixModifyPlanOrderRequest {
    pub order_id: String,
    pub symbol: String,
    #[serde(rename = "category")]
    pub product_type: String,
    #[serde(skip_serializing)]
    pub margin_coin: String,
    #[serde(rename = "triggerPrice", skip_serializing_if = "Option::is_none")]
    pub trigger_price: Option<String>,
    #[serde(rename = "triggerOrderPrice", skip_serializing_if = "Option::is_none")]
    pub execute_price: Option<String>,
    #[serde(rename = "qty", skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
}

/// Bitget Mix cancel order request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixCancelOrderRequest {
    pub symbol: String,
    #[serde(rename = "category")]
    pub product_type: String,
    #[serde(skip_serializing)]
    pub margin_coin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_oid: Option<String>,
}

/// Bitget Mix batch/cancel-all orders request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixBatchCancelOrdersRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_id_list: Vec<BitgetCancelBatchOrderItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(rename = "category")]
    pub product_type: String,
    #[serde(skip_serializing)]
    pub margin_coin: Option<String>,
}

/// Bitget Mix cancel plan order request.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixCancelPlanOrderRequest {
    pub order_id: String,
    pub symbol: String,
    #[serde(rename = "category")]
    pub product_type: String,
    #[serde(skip_serializing)]
    pub margin_coin: String,
    #[serde(skip_serializing)]
    pub plan_type: String,
}

/// Bitget per-item batch cancel result.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetCancelBatchResult {
    /// Venue order ID.
    #[serde(default)]
    pub order_id: Option<String>,
    /// Client order ID.
    #[serde(default)]
    pub client_oid: Option<String>,
    /// Per-item venue error code for failed cancels.
    #[serde(default)]
    pub code: Option<String>,
    /// Per-item venue error message for failed cancels.
    #[serde(default, rename = "msg")]
    pub error_msg: Option<String>,
}

/// Bitget batch cancel response payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetCancelBatchResponse {
    #[serde(default)]
    pub success_list: Vec<BitgetCancelBatchResult>,
    #[serde(default)]
    pub failure_list: Vec<BitgetCancelBatchResult>,
}

impl BitgetCancelBatchResponse {
    /// Converts UTA per-order results into the Classic success/failure shape used internally.
    #[must_use]
    pub fn from_uta_results(results: Vec<BitgetCancelBatchResult>) -> Self {
        let mut success_list = Vec::new();
        let mut failure_list = Vec::new();

        for result in results {
            let failed = result
                .code
                .as_deref()
                .map(str::trim)
                .is_some_and(|code| !code.is_empty() && code != "00000");
            if failed {
                failure_list.push(result);
            } else {
                success_list.push(result);
            }
        }

        Self {
            success_list,
            failure_list,
        }
    }
}

/// Bitget UTA cancel-all response payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetCancelAllResponse {
    #[serde(default)]
    pub list: Vec<BitgetCancelBatchResult>,
}

/// Bitget fill fee detail entry.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetFillFee {
    #[serde(default)]
    pub fee_coin: Option<String>,
    #[serde(default, rename = "fee")]
    pub total_fee: Option<String>,
}

/// Bitget fill fee detail, which can be returned as a list, object, or JSON string.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum BitgetFillFeeDetail {
    List(Vec<BitgetFillFee>),
    Entry(BitgetFillFee),
    Raw(String),
}

/// Bitget private fill payload. The fields are intentionally broad across Spot/Mix.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetFill {
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default, rename = "category")]
    pub product_type: Option<String>,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub client_oid: Option<String>,
    #[serde(default, rename = "execId")]
    pub trade_id: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub trade_side: Option<String>,
    #[serde(default, rename = "execPrice")]
    pub price: Option<String>,
    #[serde(default, rename = "execQty")]
    pub size: Option<String>,
    #[serde(default, rename = "execValue")]
    pub quote_size: Option<String>,
    #[serde(default)]
    pub fee: Option<String>,
    #[serde(default)]
    pub fee_coin: Option<String>,
    #[serde(default)]
    pub fee_detail: Option<BitgetFillFeeDetail>,
    #[serde(default)]
    pub margin_coin: Option<String>,
    #[serde(default)]
    pub trade_scope: Option<String>,
    #[serde(default)]
    pub is_maker: Option<bool>,
    #[serde(default, rename = "createdTime")]
    pub c_time: Option<String>,
}

/// Bitget paginated fill list payload used by Mix private fill endpoints.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetFillList {
    #[serde(default, rename = "list")]
    pub fill_list: Vec<BitgetFill>,
    #[serde(default, rename = "cursor")]
    pub end_id: Option<String>,
}

/// Bitget Spot account asset payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetSpotAsset {
    #[serde(default)]
    pub coin: Option<String>,
    #[serde(default)]
    pub available: Option<String>,
    #[serde(default)]
    pub frozen: Option<String>,
    #[serde(default)]
    pub locked: Option<String>,
    #[serde(default)]
    pub limit_available: Option<String>,
    #[serde(default, rename = "updatedTime")]
    pub u_time: Option<String>,
}

/// Bitget Mix account payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixAccount {
    #[serde(default)]
    pub margin_coin: Option<String>,
    #[serde(default)]
    pub locked: Option<String>,
    #[serde(default)]
    pub available: Option<String>,
    #[serde(default, rename = "imr")]
    pub crossed_margin: Option<String>,
    #[serde(default)]
    pub isolated_margin: Option<String>,
    #[serde(default)]
    pub account_equity: Option<String>,
    #[serde(default)]
    pub usdt_equity: Option<String>,
    #[serde(default, rename = "unrealisedPnl")]
    pub unrealized_pnl: Option<String>,
    #[serde(default, rename = "mmr")]
    pub union_mm: Option<String>,
    #[serde(default, rename = "updatedTime")]
    pub u_time: Option<String>,
}

/// Bitget Mix position payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixPosition {
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default, rename = "category")]
    pub product_type: Option<String>,
    #[serde(default)]
    pub pos_id: Option<String>,
    #[serde(default)]
    pub margin_coin: Option<String>,
    #[serde(default, rename = "posSide")]
    pub hold_side: Option<String>,
    #[serde(default, rename = "holdMode")]
    pub pos_mode: Option<String>,
    #[serde(default)]
    pub margin_mode: Option<String>,
    #[serde(default, rename = "size")]
    pub total: Option<String>,
    #[serde(default)]
    pub available: Option<String>,
    #[serde(default, rename = "avgPrice")]
    pub average_open_price: Option<String>,
    #[serde(default, skip)]
    pub open_price_avg: Option<String>,
    #[serde(default)]
    pub mark_price: Option<String>,
    #[serde(default)]
    pub liquidation_price: Option<String>,
    #[serde(default)]
    pub leverage: Option<String>,
    #[serde(default, rename = "curRealisedPnl")]
    pub realized_pnl: Option<String>,
    #[serde(default, rename = "unrealisedPnl")]
    pub unrealized_pnl: Option<String>,
    #[serde(default, rename = "createdTime")]
    pub c_time: Option<String>,
    #[serde(default, rename = "updatedTime")]
    pub u_time: Option<String>,
}

/// Bitget order detail/status payload. The fields are intentionally broad across Spot/Mix.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetOrderStatus {
    #[serde(default)]
    pub symbol: Option<String>,
    #[serde(default, rename = "category")]
    pub product_type: Option<String>,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub client_oid: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default, rename = "avgPrice")]
    pub avg_price: Option<String>,
    #[serde(default, skip)]
    pub price_avg: Option<String>,
    #[serde(default, rename = "qty")]
    pub size: Option<String>,
    #[serde(default, skip)]
    pub filled_size: Option<String>,
    #[serde(default, skip)]
    pub filled_qty: Option<String>,
    #[serde(default, rename = "cumExecQty")]
    pub cumulative_filled_qty: Option<String>,
    #[serde(default, rename = "cumExecValue")]
    pub quote_size: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub trade_side: Option<String>,
    #[serde(default)]
    pub order_type: Option<String>,
    #[serde(default, rename = "timeInForce")]
    pub force: Option<String>,
    #[serde(default, rename = "orderStatus")]
    pub status: Option<String>,
    #[serde(default)]
    pub trigger_price: Option<String>,
    #[serde(default)]
    pub trigger_type: Option<String>,
    #[serde(default)]
    pub reduce_only: Option<String>,
    #[serde(default)]
    pub margin_coin: Option<String>,
    #[serde(default, rename = "createdTime")]
    pub c_time: Option<String>,
    #[serde(default, rename = "updatedTime")]
    pub u_time: Option<String>,
}

/// Bitget paginated order list payload used by several private order endpoints.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetOrderStatusList {
    #[serde(default, rename = "list")]
    pub entrusted_list: Vec<BitgetOrderStatus>,
    #[serde(default, rename = "cursor")]
    pub end_id: Option<String>,
}

/// Bitget UTA account asset row.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetUtaAsset {
    #[serde(default)]
    pub coin: Option<String>,
    #[serde(default)]
    pub equity: Option<String>,
    #[serde(default)]
    pub balance: Option<String>,
    #[serde(default)]
    pub available: Option<String>,
    #[serde(default)]
    pub locked: Option<String>,
    #[serde(default)]
    pub debt: Option<String>,
    #[serde(default)]
    pub bonus: Option<String>,
    #[serde(default)]
    pub usd_value: Option<String>,
}

/// Bitget UTA account assets payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetUtaAccount {
    #[serde(default)]
    pub account_equity: Option<String>,
    #[serde(default)]
    pub usdt_equity: Option<String>,
    #[serde(default)]
    pub eff_equity: Option<String>,
    #[serde(default)]
    pub imr: Option<String>,
    #[serde(default)]
    pub mmr: Option<String>,
    #[serde(default)]
    pub unrealised_pnl: Option<String>,
    #[serde(default)]
    pub assets: Vec<BitgetUtaAsset>,
}

impl BitgetUtaAccount {
    /// Converts UTA assets into the Spot account row shape used internally.
    #[must_use]
    pub fn into_spot_assets(self, coin_filter: Option<&str>) -> Vec<BitgetSpotAsset> {
        self.assets
            .into_iter()
            .filter(|asset| {
                coin_filter.is_none_or(|coin_filter| {
                    asset
                        .coin
                        .as_deref()
                        .is_some_and(|coin| coin.eq_ignore_ascii_case(coin_filter))
                })
            })
            .map(|asset| BitgetSpotAsset {
                coin: asset.coin,
                available: asset.available.or(asset.balance).or(asset.equity),
                frozen: Some("0".to_string()),
                locked: asset.locked,
                limit_available: None,
                u_time: None,
            })
            .collect()
    }

    /// Converts a UTA account payload into the USDT futures account row shape used internally.
    #[must_use]
    pub fn into_usdt_futures_accounts(self) -> Vec<BitgetMixAccount> {
        let usdt_asset = self
            .assets
            .into_iter()
            .find(|asset| asset.coin.as_deref() == Some("USDT"));

        let available = usdt_asset
            .as_ref()
            .and_then(|asset| asset.available.clone())
            .or_else(|| self.eff_equity.clone())
            .or_else(|| self.usdt_equity.clone())
            .or_else(|| self.account_equity.clone());
        let locked = usdt_asset
            .as_ref()
            .and_then(|asset| asset.locked.clone())
            .unwrap_or_else(|| "0".to_string());

        vec![BitgetMixAccount {
            margin_coin: Some("USDT".to_string()),
            locked: Some(locked),
            available,
            crossed_margin: self.imr,
            isolated_margin: Some("0".to_string()),
            account_equity: self.usdt_equity.or(self.account_equity),
            usdt_equity: None,
            unrealized_pnl: self.unrealised_pnl,
            union_mm: self.mmr,
            u_time: None,
        }]
    }
}

/// Bitget UTA position list payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BitgetMixPositionList {
    #[serde(default)]
    pub list: Vec<BitgetMixPosition>,
}

/// Bitget candle row, typically `[ts, open, high, low, close, volume, ...]`.
pub type BitgetCandle = Vec<BitgetDecimalValue>;

/// Alias for Spot symbols endpoint payload.
pub type BitgetSpotSymbolsResponse = BitgetResponse<Vec<BitgetSpotSymbol>>;

/// Alias for Mix contracts endpoint payload.
pub type BitgetMixContractsResponse = BitgetResponse<Vec<BitgetMixContract>>;

/// Alias for order book endpoint payload.
pub type BitgetOrderBookResponse = BitgetResponse<BitgetOrderBookSnapshot>;

/// Alias for public market trades endpoint payload.
pub type BitgetMarketTradesResponse = BitgetResponse<Vec<BitgetMarketTrade>>;

/// Alias for candles endpoint payload.
pub type BitgetCandlesResponse = BitgetResponse<Vec<BitgetCandle>>;

/// Alias for historical funding rates endpoint payload.
pub type BitgetFundingRatesResponse = BitgetResponse<BitgetFundingRatesData>;

/// Alias for order acknowledgement endpoint payload.
pub type BitgetOrderAckResponse = BitgetResponse<BitgetOrderAck>;

/// Alias for batch cancel endpoint payload.
pub type BitgetCancelBatchResponseEnvelope = BitgetResponse<BitgetCancelBatchResponse>;

/// Alias for UTA batch cancel endpoint payload.
pub type BitgetUtaCancelBatchResponseEnvelope = BitgetResponse<Vec<BitgetCancelBatchResult>>;

/// Alias for UTA cancel-all endpoint payload.
pub type BitgetCancelAllResponseEnvelope = BitgetResponse<BitgetCancelAllResponse>;

/// Alias for order status endpoint payload.
pub type BitgetOrderStatusResponse = BitgetResponse<BitgetOrderStatus>;

/// Alias for order status list endpoint payload.
pub type BitgetOrderStatusListResponse = BitgetResponse<BitgetOrderStatusList>;

/// Alias for Spot fill endpoint payload.
pub type BitgetFillsResponse = BitgetResponse<Vec<BitgetFill>>;

/// Alias for fill list endpoint payload.
pub type BitgetFillListResponse = BitgetResponse<BitgetFillList>;

/// Alias for Spot account assets endpoint payload.
pub type BitgetSpotAssetsResponse = BitgetResponse<Vec<BitgetSpotAsset>>;

/// Alias for Mix account list endpoint payload.
pub type BitgetMixAccountsResponse = BitgetResponse<Vec<BitgetMixAccount>>;

/// Alias for UTA account assets endpoint payload.
pub type BitgetUtaAccountResponse = BitgetResponse<BitgetUtaAccount>;

/// Alias for Mix positions endpoint payload.
pub type BitgetMixPositionsResponse = BitgetResponse<BitgetMixPositionList>;

/// Alias for UTA positions endpoint payload.
pub type BitgetMixPositionListResponse = BitgetResponse<BitgetMixPositionList>;

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn parses_spot_symbols_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "requestTime":1700000000000,
            "data":[{
                "symbol":"BTCUSDT",
                "baseCoin":"BTC",
                "quoteCoin":"USDT",
                "minOrderQty":"0.00001",
                "maxOrderQty":"100",
                "minOrderAmount":"5",
                "makerFeeRate":"0.001",
                "takerFeeRate":"0.001",
                "pricePrecision":"2",
                "quantityPrecision":"6",
                "status":"online"
            }]
        }"#;

        let response: BitgetSpotSymbolsResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data[0].symbol, "BTCUSDT");
        assert_eq!(response.data[0].price_precision.as_deref(), Some("2"));
    }

    #[rstest]
    fn parses_mix_contracts_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":[{
                "symbol":"BTCUSDT",
                "category":"USDT-FUTURES",
                "baseCoin":"BTC",
                "quoteCoin":"USDT",
                "type":"perpetual",
                "marginCoin":"USDT",
                "makerFeeRate":"0.0002",
                "takerFeeRate":"0.0006",
                "minOrderQty":"0.001",
                "minOrderAmount":"5",
                "quantityMultiplier":"0.001",
                "pricePrecision":"1",
                "quantityPrecision":"3",
                "priceMultiplier":"0.1",
                "maxLeverage":"125",
                "fundInterval":"8"
            }]
        }"#;

        let response: BitgetMixContractsResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data[0].contract_type.as_deref(), Some("perpetual"));
        assert_eq!(
            response.data[0].product_type.as_deref(),
            Some("USDT-FUTURES")
        );
    }

    #[rstest]
    fn parses_order_book_envelope_with_numeric_levels() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":{
                "b":[["100.1","0.5"],[99.9,1.25]],
                "a":[["100.2","0.4"]],
                "ts":"1700000000123",
                "seq":"42"
            }
        }"#;

        let response: BitgetOrderBookResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data.bids.len(), 2);
        assert_eq!(response.data.bids[1][0].as_decimal_str(), "99.9");
        assert_eq!(response.data.seq.as_deref(), Some("42"));
    }

    #[rstest]
    fn parses_market_trades_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":[{
                "execId":"123",
                "price":"100.1",
                "size":"0.5",
                "side":"buy",
                "ts":"1700000000123"
            }]
        }"#;

        let response: BitgetMarketTradesResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data[0].trade_id, "123");
        assert_eq!(response.data[0].side, "buy");
    }

    #[rstest]
    fn parses_candles_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":[["1700000000000","100","101","99","100.5","12.5"]]
        }"#;

        let response: BitgetCandlesResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data[0][4].as_decimal_str(), "100.5");
    }

    #[rstest]
    fn parses_funding_rates_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":{
                "resultList":[{
                    "symbol":"BTCUSDT",
                    "fundingRate":"0.0001",
                    "fundingRateTimestamp":"1700000000000"
                }]
            }
        }"#;

        let response: BitgetFundingRatesResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data.result_list[0].funding_rate, "0.0001");
        assert_eq!(response.data.result_list[0].funding_time, "1700000000000");
    }

    #[rstest]
    fn serializes_mix_place_order_request() {
        let request = BitgetMixPlaceOrderRequest {
            symbol: "BTCUSDT".to_string(),
            product_type: "USDT-FUTURES".to_string(),
            margin_mode: "crossed".to_string(),
            margin_coin: "USDT".to_string(),
            size: "0.001".to_string(),
            price: Some("100.0".to_string()),
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            force: Some("gtc".to_string()),
            client_oid: Some("C-1".to_string()),
            reduce_only: Some("NO".to_string()),
            ..Default::default()
        };

        let raw = serde_json::to_string(&request).unwrap();

        assert!(raw.contains(r#""category":"USDT-FUTURES""#));
        assert!(raw.contains(r#""marginMode":"crossed""#));
        assert!(raw.contains(r#""qty":"0.001""#));
        assert!(raw.contains(r#""timeInForce":"gtc""#));
        assert!(raw.contains(r#""clientOid":"C-1""#));
        assert!(!raw.contains("marginCoin"));
    }

    #[rstest]
    fn parses_order_ack_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":{"orderId":"123","clientOid":"C-1"}
        }"#;

        let response: BitgetOrderAckResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data.order_id.as_deref(), Some("123"));
        assert_eq!(response.data.client_oid.as_deref(), Some("C-1"));
    }

    #[rstest]
    fn parses_uta_cancel_batch_response_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":[
                {"orderId":"123","clientOid":"C-1"},
                {"orderId":"124","clientOid":"C-2","code":"24056","msg":"order not found"}
            ]
        }"#;

        let response: BitgetUtaCancelBatchResponseEnvelope = serde_json::from_str(raw).unwrap();
        let response = BitgetCancelBatchResponse::from_uta_results(response.data);

        assert_eq!(response.success_list.len(), 1);
        assert_eq!(response.success_list[0].client_oid.as_deref(), Some("C-1"));
        assert_eq!(response.failure_list.len(), 1);
        assert_eq!(
            response.failure_list[0].error_msg.as_deref(),
            Some("order not found")
        );
    }

    #[rstest]
    fn serializes_spot_batch_cancel_order_request() {
        let request = BitgetSpotBatchCancelOrderRequest {
            category: "SPOT".to_string(),
            symbol: "BTCUSDT".to_string(),
            batch_mode: Some("single".to_string()),
            order_list: vec![BitgetCancelBatchOrderItem {
                category: Some("SPOT".to_string()),
                symbol: Some("BTCUSDT".to_string()),
                order_id: None,
                client_oid: Some("C-1".to_string()),
            }],
        };

        let raw = serde_json::to_string(&request).unwrap();

        assert!(raw.contains(r#""symbol":"BTCUSDT""#));
        assert!(raw.contains(r#""batchMode":"single""#));
        assert!(raw.contains(r#""clientOid":"C-1""#));
    }

    #[rstest]
    fn parses_uta_order_status_list_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":{
                "list":[{
                    "symbol":"BTCUSDT",
                    "category":"USDT-FUTURES",
                    "orderId":"123",
                    "clientOid":"C-1",
                    "avgPrice":"100.1",
                    "cumExecQty":"0.01",
                    "orderStatus":"partially_filled",
                    "timeInForce":"gtc",
                    "createdTime":"1700000000000",
                    "updatedTime":"1700000001000"
                }],
                "cursor":"CUR-1"
            }
        }"#;

        let response: BitgetOrderStatusListResponse = serde_json::from_str(raw).unwrap();

        assert_eq!(response.data.entrusted_list.len(), 1);
        assert_eq!(response.data.end_id.as_deref(), Some("CUR-1"));
        assert_eq!(
            response.data.entrusted_list[0].product_type.as_deref(),
            Some("USDT-FUTURES")
        );
        assert_eq!(
            response.data.entrusted_list[0].avg_price.as_deref(),
            Some("100.1")
        );
        assert_eq!(
            response.data.entrusted_list[0].status.as_deref(),
            Some("partially_filled")
        );
    }

    #[rstest]
    fn parses_spot_fills_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":[{
                "symbol":"BTCUSDT",
                "category":"SPOT",
                "orderId":"123",
                "clientOid":"C-1",
                "execId":"T-1",
                "side":"buy",
                "execPrice":"100.1",
                "execQty":"0.01",
                "feeDetail":[{"feeCoin":"USDT","fee":"-0.001"}],
                "tradeScope":"maker",
                "createdTime":"1700000000000"
            }]
        }"#;

        let response: BitgetFillsResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data[0].trade_id.as_deref(), Some("T-1"));
        assert!(matches!(
            response.data[0].fee_detail,
            Some(BitgetFillFeeDetail::List(_))
        ));
        assert_eq!(response.data[0].trade_scope.as_deref(), Some("maker"));
    }

    #[rstest]
    fn parses_mix_fill_list_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":{
                "list":[{
                    "symbol":"BTCUSDT",
                    "category":"USDT-FUTURES",
                    "orderId":"123",
                    "execId":"T-1",
                    "side":"sell",
                    "execPrice":"100.1",
                    "execQty":"0.01",
                    "feeDetail":[{"feeCoin":"USDT","fee":"-0.001"}],
                    "tradeScope":"taker",
                    "createdTime":"1700000000000"
                }],
                "cursor":"T-1"
            }
        }"#;

        let response: BitgetFillListResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data.fill_list.len(), 1);
        assert_eq!(response.data.end_id.as_deref(), Some("T-1"));
        assert!(matches!(
            response.data.fill_list[0].fee_detail,
            Some(BitgetFillFeeDetail::List(_))
        ));
    }

    #[rstest]
    fn parses_spot_assets_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":[{
                "coin":"USDT",
                "available":"100.0",
                "frozen":"2.0",
                "locked":"3.0",
                "updatedTime":"1700000000000"
            }]
        }"#;

        let response: BitgetSpotAssetsResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data[0].coin.as_deref(), Some("USDT"));
        assert_eq!(response.data[0].available.as_deref(), Some("100.0"));
        assert_eq!(response.data[0].frozen.as_deref(), Some("2.0"));
    }

    #[rstest]
    fn parses_mix_accounts_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":[{
                "marginCoin":"USDT",
                "locked":"1",
                "available":"100",
                "imr":"10",
                "isolatedMargin":"2",
                "accountEquity":"123",
                "mmr":"4",
                "updatedTime":"1700000000000"
            }]
        }"#;

        let response: BitgetMixAccountsResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data[0].margin_coin.as_deref(), Some("USDT"));
        assert_eq!(response.data[0].account_equity.as_deref(), Some("123"));
        assert_eq!(response.data[0].union_mm.as_deref(), Some("4"));
    }

    #[rstest]
    fn parses_mix_positions_envelope() {
        let raw = r#"{
            "code":"00000",
            "msg":"success",
            "data":{"list":[{
                "symbol":"BTCUSDT",
                "category":"USDT-FUTURES",
                "marginCoin":"USDT",
                "posSide":"long",
                "size":"0.004",
                "avgPrice":"100.1",
                "updatedTime":"1700000000000"
            }]}
        }"#;

        let response: BitgetMixPositionsResponse = serde_json::from_str(raw).unwrap();

        assert!(response.is_success());
        assert_eq!(response.data.list[0].symbol.as_deref(), Some("BTCUSDT"));
        assert_eq!(response.data.list[0].hold_side.as_deref(), Some("long"));
        assert_eq!(
            response.data.list[0].average_open_price.as_deref(),
            Some("100.1")
        );
    }
}
