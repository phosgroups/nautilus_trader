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

//! Classic REST routing. Wire types stay separate from UTA; results use shared records.

use chrono::{DateTime, Utc};
use nautilus_network::http::Method;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;

use super::models as wire;
use crate::{
    common::{enums::BitgetProductType, order::*},
    http::{
        client::{BitgetFillPage, BitgetOrderStatusPage, BitgetRawHttpClient},
        error::BitgetHttpError,
        models as shared,
    },
};

type Result<T> = std::result::Result<T, BitgetHttpError>;

fn query(params: Vec<(&str, String)>) -> Result<String> {
    let mut params = params;
    params.sort_by(|a, b| a.0.cmp(b.0));
    Ok(format!(
        "?{}",
        serde_urlencoded::to_string(params)
            .map_err(|e| BitgetHttpError::ValidationError(e.to_string()))?
    ))
}

fn product_params(product: BitgetProductType) -> Vec<(&'static str, String)> {
    if product.is_derivative() {
        vec![("productType", product.as_api_str().to_string())]
    } else {
        vec![]
    }
}

impl BitgetRawHttpClient {
    async fn classic_get<T: DeserializeOwned + Default>(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
        private: bool,
    ) -> Result<T> {
        let query = query(params)?;
        if private {
            self.send_private::<T, serde_json::Value>(Method::GET, path, Some(&query), None)
                .await
        } else {
            self.get_public(path, Some(&query)).await
        }
    }

    async fn classic_post<T: DeserializeOwned + Default, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        self.send_private(Method::POST, path, None, Some(body))
            .await
    }

    async fn classic_batch_post<B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<shared::BitgetCancelBatchResponse> {
        let response: wire::BitgetCancelBatchResponse = self.classic_post(path, body).await?;
        Ok(response.into())
    }

    pub(crate) async fn classic_spot_symbols(&self) -> Result<Vec<shared::BitgetSpotSymbol>> {
        let rows: Vec<wire::BitgetSpotSymbol> = self
            .classic_get("/api/v2/spot/public/symbols", vec![], false)
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn classic_contracts(&self) -> Result<Vec<shared::BitgetMixContract>> {
        let rows: Vec<wire::BitgetMixContract> = self
            .classic_get(
                "/api/v2/mix/market/contracts",
                product_params(BitgetProductType::UsdtFutures),
                false,
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn classic_orderbook(
        &self,
        product: BitgetProductType,
        symbol: &str,
        limit: Option<u32>,
    ) -> Result<shared::BitgetOrderBookSnapshot> {
        let mut params = product_params(product);
        params.push(("symbol", symbol.to_string()));
        params.push(("precision", "scale0".to_string()));
        if let Some(limit) = limit {
            params.push((
                "limit",
                if product.is_derivative() {
                    match limit {
                        0..=1 => "1",
                        2..=5 => "5",
                        6..=15 => "15",
                        16..=50 => "50",
                        _ => "max",
                    }
                    .to_string()
                } else {
                    limit.min(150).to_string()
                },
            ));
        }
        let path = if product.is_derivative() {
            "/api/v2/mix/market/merge-depth"
        } else {
            "/api/v2/spot/market/orderbook"
        };
        let row: wire::BitgetOrderBookSnapshot = self.classic_get(path, params, false).await?;
        Ok(row.into())
    }

    pub(crate) async fn classic_trades(
        &self,
        product: BitgetProductType,
        symbol: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<shared::BitgetMarketTrade>> {
        let history = start.is_some() || end.is_some();
        let mut params = product_params(product);
        params.push(("symbol", symbol.to_string()));
        add_window(
            &mut params,
            start.as_ref(),
            end.as_ref(),
            limit.unwrap_or(100).clamp(1, 1000),
            None,
        );
        let path = match (product, history) {
            (BitgetProductType::Spot, false) => "/api/v2/spot/market/fills",
            (BitgetProductType::Spot, true) => "/api/v2/spot/market/fills-history",
            (_, false) => "/api/v2/mix/market/fills",
            (_, true) => "/api/v2/mix/market/fills-history",
        };
        let rows: Vec<wire::BitgetMarketTrade> = self.classic_get(path, params, false).await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn classic_candles(
        &self,
        product: BitgetProductType,
        symbol: &str,
        interval: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<shared::BitgetCandle>> {
        let mut params = product_params(product);
        params.push(("symbol", symbol.to_string()));
        let granularity = if product == BitgetProductType::Spot {
            match interval {
                "1m" => "1min",
                "3m" => "3min",
                "5m" => "5min",
                "15m" => "15min",
                "30m" => "30min",
                "1H" => "1h",
                "4H" => "4h",
                "6H" => "6h",
                "12H" => "12h",
                "1D" => "1day",
                "1W" => "1week",
                "1M" => "1M",
                other => other,
            }
        } else {
            interval
        };
        params.push(("granularity", granularity.to_string()));
        add_window(
            &mut params,
            start.as_ref(),
            end.as_ref(),
            limit.unwrap_or(100).clamp(1, 1000),
            None,
        );
        let path = if product.is_derivative() {
            "/api/v2/mix/market/candles"
        } else {
            "/api/v2/spot/market/candles"
        };
        self.classic_get(path, params, false).await
    }

    pub(crate) async fn classic_funding(
        &self,
        symbol: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<shared::BitgetFundingRate>> {
        let limit = limit.unwrap_or(100).clamp(1, 100) as usize;
        let mut result = Vec::new();
        let mut page = 1;
        loop {
            let mut params = product_params(BitgetProductType::UsdtFutures);
            params.extend([
                ("symbol", symbol.to_string()),
                ("pageSize", "100".to_string()),
                ("pageNo", page.to_string()),
            ]);
            let rows: Vec<wire::BitgetFundingRate> = self
                .classic_get("/api/v2/mix/market/history-fund-rate", params, false)
                .await?;
            let len = rows.len();
            let mut past_start = false;
            for row in rows {
                let time = row
                    .funding_time
                    .parse::<i64>()
                    .map_err(|e| BitgetHttpError::ValidationError(e.to_string()))?;
                if start.is_some_and(|s| time < s.timestamp_millis()) {
                    past_start = true;
                    continue;
                }
                if end.is_none_or(|e| time <= e.timestamp_millis()) {
                    result.push(row.into());
                }
            }
            if result.len() >= limit || len < 100 || past_start {
                break;
            }
            page += 1;
        }
        result.truncate(limit);
        Ok(result)
    }

    pub(crate) async fn classic_spot_assets(
        &self,
        coin: Option<&str>,
    ) -> Result<Vec<shared::BitgetSpotAsset>> {
        let params = coin
            .map(|c| vec![("coin", c.to_string())])
            .unwrap_or_default();
        let rows: Vec<wire::BitgetSpotAsset> = self
            .classic_get("/api/v2/spot/account/assets", params, true)
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn classic_mix_accounts(&self) -> Result<Vec<shared::BitgetMixAccount>> {
        let rows: Vec<wire::BitgetMixAccount> = self
            .classic_get(
                "/api/v2/mix/account/accounts",
                product_params(BitgetProductType::UsdtFutures),
                true,
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn classic_positions(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<shared::BitgetMixPosition>> {
        let mut params = product_params(BitgetProductType::UsdtFutures);
        params.push(("marginCoin", "USDT".to_string()));
        let path = if let Some(symbol) = symbol {
            params.push(("symbol", symbol.to_string()));
            "/api/v2/mix/position/single-position"
        } else {
            "/api/v2/mix/position/all-position"
        };
        let rows: Vec<wire::BitgetMixPosition> = self.classic_get(path, params, true).await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub(crate) async fn classic_submit(
        &self,
        request: &BitgetSubmitOrderRequest,
    ) -> Result<shared::BitgetOrderAck> {
        let ack: shared::BitgetOrderAck = match request {
            BitgetSubmitOrderRequest::Spot(r) => {
                self.classic_post(
                    "/api/v2/spot/trade/place-order",
                    &wire::BitgetSpotPlaceOrderRequest::from(r),
                )
                .await
            }
            BitgetSubmitOrderRequest::SpotPlan(r) => {
                self.classic_post(
                    "/api/v2/spot/trade/place-plan-order",
                    &wire::BitgetSpotPlanOrderRequest::from(r),
                )
                .await
            }
            BitgetSubmitOrderRequest::Mix(r) => {
                self.classic_post(
                    "/api/v2/mix/order/place-order",
                    &wire::BitgetMixPlaceOrderRequest::from(r),
                )
                .await
            }
            BitgetSubmitOrderRequest::MixPlan(r) => {
                self.classic_post(
                    "/api/v2/mix/order/place-plan-order",
                    &wire::BitgetMixPlanOrderRequest::from(r),
                )
                .await
            }
        }?;
        let plan_client = match request {
            BitgetSubmitOrderRequest::SpotPlan(row) => row.client_oid.as_ref(),
            BitgetSubmitOrderRequest::MixPlan(row) => row.client_oid.as_ref(),
            _ => None,
        };
        if let (Some(id), Some(client)) = (ack.order_id.as_ref(), plan_client) {
            self.classic_plans
                .lock()
                .expect("plan lock poisoned")
                .insert(id.clone(), client.clone());
        }
        Ok(ack)
    }

    pub(crate) async fn classic_modify(
        &self,
        request: &BitgetModifyOrderRequest,
    ) -> Result<shared::BitgetOrderAck> {
        match request {
            BitgetModifyOrderRequest::Mix(r) => {
                if r.new_client_oid.as_deref().is_none_or(str::is_empty)
                    || r.new_price.is_none()
                    || r.new_size.is_none()
                {
                    return Err(BitgetHttpError::ValidationError(
                        "Classic futures modify requires newClientOid, newPrice and newSize"
                            .to_string(),
                    ));
                }
                self.classic_post(
                    "/api/v2/mix/order/modify-order",
                    &wire::BitgetMixModifyOrderRequest::from(r),
                )
                .await
            }
            BitgetModifyOrderRequest::MixPlan(r) => {
                self.classic_post(
                    "/api/v2/mix/order/modify-plan-order",
                    &wire::BitgetMixModifyPlanOrderRequest::from(r),
                )
                .await
            }
        }
    }

    pub(crate) async fn classic_cancel(
        &self,
        request: &BitgetCancelOrderRequest,
    ) -> Result<shared::BitgetOrderAck> {
        match request {
            BitgetCancelOrderRequest::Spot(r) => {
                self.classic_post(
                    "/api/v2/spot/trade/cancel-order",
                    &wire::BitgetSpotCancelOrderRequest::from(r),
                )
                .await
            }
            BitgetCancelOrderRequest::SpotPlan(r) => {
                self.classic_post(
                    "/api/v2/spot/trade/cancel-plan-order",
                    &json!({"orderId":r.order_id,"clientOid":r.client_oid}),
                )
                .await
            }
            BitgetCancelOrderRequest::Mix(r) => {
                let (order_id, client_oid) = self
                    .classic_replacements
                    .lock()
                    .expect("replacement lock poisoned")
                    .resolve(r.client_oid.as_deref(), r.order_id.as_deref())
                    .map_err(|e| BitgetHttpError::ValidationError(e.to_string()))?;
                let mut body = wire::BitgetMixCancelOrderRequest::from(r);
                body.order_id = order_id;
                body.client_oid = client_oid;
                self.classic_post("/api/v2/mix/order/cancel-order", &body)
                    .await
            }
            BitgetCancelOrderRequest::MixPlan(r) => {
                let response: wire::BitgetCancelBatchResponse = self.classic_post("/api/v2/mix/order/cancel-plan-order", &json!({"symbol":r.symbol,"productType":r.product_type,"marginCoin":r.margin_coin,"planType":r.plan_type,"orderIdList":[{"orderId":r.order_id}]})).await?;
                if let Some(failure) = response.failure_list.first() {
                    return Err(BitgetHttpError::BitgetError {
                        code: failure.error_code.clone().unwrap_or_default(),
                        message: failure.error_msg.clone().unwrap_or_default(),
                    });
                }
                Ok(shared::BitgetOrderAck {
                    order_id: Some(r.order_id.clone()),
                    ..Default::default()
                })
            }
        }
    }

    pub(crate) async fn classic_batch_cancel(
        &self,
        request: &BitgetBatchCancelOrdersRequest,
    ) -> Result<shared::BitgetCancelBatchResponse> {
        match request {
            BitgetBatchCancelOrdersRequest::Spot(r) => {
                self.classic_batch_post(
                    "/api/v2/spot/trade/batch-cancel-order",
                    &wire::BitgetSpotBatchCancelOrderRequest::from(r),
                )
                .await
            }
            BitgetBatchCancelOrdersRequest::Mix(r) => {
                let mut body = wire::BitgetMixBatchCancelOrdersRequest::from(r);
                for item in &mut body.order_id_list {
                    let (order_id, client_oid) = self
                        .classic_replacements
                        .lock()
                        .expect("replacement lock poisoned")
                        .resolve(item.client_oid.as_deref(), item.order_id.as_deref())
                        .map_err(|e| BitgetHttpError::ValidationError(e.to_string()))?;
                    item.order_id = order_id;
                    item.client_oid = client_oid;
                }
                self.classic_batch_post("/api/v2/mix/order/batch-cancel-orders", &body)
                    .await
            }
        }
    }

    pub(crate) async fn classic_cancel_all(
        &self,
        request: &BitgetCancelAllOrdersRequest,
    ) -> Result<shared::BitgetCancelBatchResponse> {
        match request {
            BitgetCancelAllOrdersRequest::Spot(r) => {
                let _: serde_json::Value = self
                    .classic_post(
                        "/api/v2/spot/trade/cancel-symbol-order",
                        &wire::BitgetSpotCancelSymbolOrderRequest::from(r),
                    )
                    .await?;
                Ok(Default::default())
            }
            BitgetCancelAllOrdersRequest::Mix(r) => {
                self.classic_batch_post(
                    "/api/v2/mix/order/batch-cancel-orders",
                    &wire::BitgetMixBatchCancelOrdersRequest::from(r),
                )
                .await
            }
        }
    }

    /// Classic Spot exposes pending/history trigger orders through REST, not a private WS topic.
    pub(crate) async fn classic_spot_plan_orders(
        &self,
        mut since: DateTime<Utc>,
    ) -> Result<Vec<shared::BitgetOrderStatus>> {
        let end = Utc::now();
        since = since.max(end - chrono::Duration::days(90));
        let mut result = Vec::new();
        for history in [false, true] {
            let mut cursor: Option<String> = None;
            loop {
                let mut params = vec![("limit", "100".to_string())];
                if history {
                    params.push(("startTime", since.timestamp_millis().to_string()));
                    params.push(("endTime", end.timestamp_millis().to_string()));
                }
                if let Some(cursor) = &cursor {
                    params.push(("idLessThan", cursor.clone()));
                }
                let path = if history {
                    "/api/v2/spot/trade/history-plan-order"
                } else {
                    "/api/v2/spot/trade/current-plan-order"
                };
                let page: PlanPage = self.classic_get(path, params, true).await?;
                result.extend(page.order_list.into_iter().map(|row| {
                    if let Some(created) = row
                        .c_time
                        .as_deref()
                        .and_then(|s| s.parse::<i64>().ok())
                        .and_then(DateTime::from_timestamp_millis)
                    {
                        since = since.min(created).max(end - chrono::Duration::days(90));
                    }
                    let mut row: shared::BitgetOrderStatus = row.into();
                    row.product_type = Some("SPOT".to_string());
                    row.force = Some("gtc".to_string());
                    row
                }));
                if !page.next_flag {
                    break;
                }
                let next = page.id_less_than.filter(|id| !id.is_empty());
                if next.is_none() || next == cursor {
                    return Err(BitgetHttpError::ValidationError(
                        "Classic plan pagination did not advance".to_string(),
                    ));
                }
                cursor = next;
            }
        }
        Ok(result)
    }

    pub(crate) async fn classic_plan_orders(
        &self,
        product: BitgetProductType,
        symbol: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        open_only: bool,
    ) -> Result<Vec<shared::BitgetOrderStatus>> {
        if product == BitgetProductType::Spot {
            let rows = self
                .classic_spot_plan_orders(
                    start.unwrap_or_else(|| Utc::now() - chrono::Duration::days(90)),
                )
                .await?;
            return Ok(rows
                .into_iter()
                .filter(|row| {
                    symbol.is_none_or(|s| row.symbol.as_deref() == Some(s))
                        && (!open_only
                            || matches!(row.status.as_deref(), Some("not_trigger" | "live")))
                        && end.is_none_or(|end| {
                            row.c_time
                                .as_deref()
                                .and_then(|s| s.parse::<i64>().ok())
                                .is_none_or(|ts| ts <= end.timestamp_millis())
                        })
                })
                .collect());
        }
        let mut result = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut params = product_params(product);
            params.push(("planType", "normal_plan".to_string()));
            if let Some(symbol) = symbol {
                params.push(("symbol", symbol.to_owned()));
            }
            add_window(
                &mut params,
                start.as_ref(),
                end.as_ref(),
                100,
                cursor.as_deref(),
            );
            let path = if open_only {
                "/api/v2/mix/order/orders-plan-pending"
            } else {
                "/api/v2/mix/order/orders-plan-history"
            };
            let page: OrderPage = self.classic_get(path, params, true).await?;
            let count = page.entrusted_list.len();
            result.extend(
                page.entrusted_list
                    .into_iter()
                    .map(wire::BitgetOrderStatus::into_futures),
            );
            if count < 100 {
                break;
            }
            let next = page.end_id.filter(|s| !s.is_empty());
            if next.is_none() || next == cursor {
                return Err(BitgetHttpError::ValidationError(
                    "Classic futures plan pagination did not advance".to_string(),
                ));
            }
            cursor = next;
        }
        Ok(result)
    }

    pub(crate) async fn classic_order_status(
        &self,
        product: BitgetProductType,
        symbol: &str,
        order_id: Option<&str>,
        client_oid: Option<&str>,
    ) -> Result<shared::BitgetOrderStatus> {
        let plan_client = {
            let plans = self.classic_plans.lock().expect("plan lock poisoned");
            order_id.and_then(|id| plans.get(id).cloned()).or_else(|| {
                client_oid
                    .filter(|id| plans.values().any(|v| v == id))
                    .map(str::to_owned)
            })
        };
        if let Some(client) = plan_client {
            for open_only in [true, false] {
                let rows = self
                    .classic_plan_orders(product, Some(symbol), None, None, open_only)
                    .await?;
                if let Some(mut row) = rows.into_iter().find(|row| {
                    row.client_oid.as_deref() == Some(&client)
                        || row.order_id.as_deref() == order_id && order_id.is_some()
                }) {
                    if row.status.as_deref() == Some("triggered") {
                        let mut child = self
                            .classic_regular_order_status(
                                product,
                                symbol,
                                row.order_id.as_deref(),
                                None,
                            )
                            .await?;
                        child.client_oid = Some(client);
                        child.trigger_price = row.trigger_price.take();
                        child.trigger_type = row.trigger_type.take();
                        return Ok(child);
                    }
                    return Ok(row);
                }
            }
            return Err(BitgetHttpError::ValidationError(
                "Classic plan order not found in pending/history queries".to_string(),
            ));
        }
        self.classic_regular_order_status(product, symbol, order_id, client_oid)
            .await
    }

    async fn classic_regular_order_status(
        &self,
        product: BitgetProductType,
        symbol: &str,
        order_id: Option<&str>,
        client_oid: Option<&str>,
    ) -> Result<shared::BitgetOrderStatus> {
        let (order_id, client_oid) = self
            .classic_replacements
            .lock()
            .expect("replacement lock poisoned")
            .query_identity(client_oid, order_id);
        let mut params = product_params(product);
        params.push(("symbol", symbol.to_string()));
        if let Some(id) = order_id {
            params.push(("orderId", id));
        }
        if let Some(id) = client_oid {
            params.push(("clientOid", id));
        }
        if product == BitgetProductType::Spot {
            let rows: Vec<wire::BitgetOrderStatus> = self
                .classic_get("/api/v2/spot/trade/orderInfo", params, true)
                .await?;
            rows.into_iter().next().map(Into::into).ok_or_else(|| {
                BitgetHttpError::ValidationError(
                    "Classic order query returned no order".to_string(),
                )
            })
        } else {
            let row: wire::BitgetOrderStatus = self
                .classic_get("/api/v2/mix/order/detail", params, true)
                .await?;
            Ok(row.into_futures())
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors the shared paginated order query"
    )]
    pub(crate) async fn classic_order_page(
        &self,
        product: BitgetProductType,
        symbol: Option<&str>,
        start: Option<&DateTime<Utc>>,
        end: Option<&DateTime<Utc>>,
        open: bool,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<BitgetOrderStatusPage> {
        let mut params = product_params(product);
        if let Some(symbol) = symbol {
            params.push(("symbol", symbol.to_string()));
        }
        add_window(&mut params, start, end, limit, cursor);
        let path = match (product, open) {
            (BitgetProductType::Spot, true) => "/api/v2/spot/trade/unfilled-orders",
            (BitgetProductType::Spot, false) => "/api/v2/spot/trade/history-orders",
            (_, true) => "/api/v2/mix/order/orders-pending",
            (_, false) => "/api/v2/mix/order/orders-history",
        };
        if product == BitgetProductType::Spot {
            let rows: Vec<wire::BitgetOrderStatus> = self.classic_get(path, params, true).await?;
            let next_cursor = rows.last().and_then(|r| r.order_id.clone());
            Ok(BitgetOrderStatusPage {
                orders: rows.into_iter().map(Into::into).collect(),
                next_cursor,
            })
        } else {
            let page: OrderPage = self.classic_get(path, params, true).await?;
            Ok(BitgetOrderStatusPage {
                orders: page
                    .entrusted_list
                    .into_iter()
                    .map(wire::BitgetOrderStatus::into_futures)
                    .collect(),
                next_cursor: page.end_id,
            })
        }
    }

    pub(crate) async fn classic_fill_page(
        &self,
        product: BitgetProductType,
        symbol: Option<&str>,
        start: Option<&DateTime<Utc>>,
        end: Option<&DateTime<Utc>>,
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<BitgetFillPage> {
        let mut params = product_params(product);
        if let Some(symbol) = symbol {
            params.push(("symbol", symbol.to_string()));
        }
        add_window(&mut params, start, end, limit, cursor);
        if product == BitgetProductType::Spot {
            let rows: Vec<wire::BitgetFill> = self
                .classic_get("/api/v2/spot/trade/fills", params, true)
                .await?;
            let next_cursor = rows.last().and_then(|r| r.trade_id.clone());
            Ok(BitgetFillPage {
                fills: rows.into_iter().map(Into::into).collect(),
                next_cursor,
            })
        } else {
            let page: FillPage = self
                .classic_get("/api/v2/mix/order/fills", params, true)
                .await?;
            Ok(BitgetFillPage {
                fills: page
                    .fill_list
                    .into_iter()
                    .map(wire::BitgetFill::into_futures)
                    .collect(),
                next_cursor: page.end_id,
            })
        }
    }
}

fn add_window(
    params: &mut Vec<(&'static str, String)>,
    start: Option<&DateTime<Utc>>,
    end: Option<&DateTime<Utc>>,
    limit: u32,
    cursor: Option<&str>,
) {
    if let Some(start) = start {
        params.push(("startTime", start.timestamp_millis().to_string()));
    }
    if let Some(end) = end {
        params.push(("endTime", end.timestamp_millis().to_string()));
    }
    params.push(("limit", limit.to_string()));
    if let Some(cursor) = cursor {
        params.push(("idLessThan", cursor.to_string()));
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderPage {
    #[serde(default)]
    entrusted_list: Vec<wire::BitgetOrderStatus>,
    end_id: Option<String>,
}

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FillPage {
    #[serde(default)]
    fill_list: Vec<wire::BitgetFill>,
    end_id: Option<String>,
}

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlanPage {
    #[serde(default)]
    order_list: Vec<wire::BitgetOrderStatus>,
    #[serde(default)]
    next_flag: bool,
    id_less_than: Option<String>,
}
