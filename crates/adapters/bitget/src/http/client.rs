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

//! Provides the HTTP client integration for the Bitget REST API.

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::AHashMap;
use anyhow::Context;
use chrono::{DateTime, Utc};
#[cfg(feature = "python")]
use nautilus_common::cache::InstrumentLookupError;
use nautilus_core::{AtomicMap, UnixNanos, consts::NAUTILUS_USER_AGENT};
use nautilus_model::{
    data::{Bar, BarType, FundingRateUpdate, OrderBookDeltas, TradeTick},
    enums::MarketStatusAction,
    events::AccountState,
    identifiers::{AccountId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{
    http::{HttpClient, Method, USER_AGENT},
    ratelimiter::quota::Quota,
};
use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use super::{error::BitgetHttpError, models::BitgetResponse};
use crate::common::{
    consts::{
        BITGET_ACCESS_KEY_HEADER, BITGET_ACCESS_PASSPHRASE_HEADER, BITGET_ACCESS_SIGN_HEADER,
        BITGET_ACCESS_TIMESTAMP_HEADER, BITGET_LOCALE_HEADER, BITGET_MARKET_CANDLES_ENDPOINT,
        BITGET_MARKET_FILLS_ENDPOINT, BITGET_MARKET_FUNDING_HISTORY_ENDPOINT,
        BITGET_MARKET_INSTRUMENTS_ENDPOINT, BITGET_MARKET_ORDERBOOK_ENDPOINT,
        BITGET_MIX_ACCOUNT_LIST_ENDPOINT, BITGET_MIX_ALL_POSITIONS_ENDPOINT,
        BITGET_MIX_CANCEL_ORDER_ENDPOINT, BITGET_MIX_CANCEL_PLAN_ORDER_ENDPOINT,
        BITGET_MIX_MODIFY_ORDER_ENDPOINT, BITGET_MIX_MODIFY_PLAN_ORDER_ENDPOINT,
        BITGET_MIX_ORDER_DETAIL_ENDPOINT, BITGET_MIX_ORDER_FILLS_ENDPOINT,
        BITGET_MIX_ORDERS_HISTORY_ENDPOINT, BITGET_MIX_ORDERS_PENDING_ENDPOINT,
        BITGET_MIX_PLACE_ORDER_ENDPOINT, BITGET_MIX_PLACE_PLAN_ORDER_ENDPOINT,
        BITGET_MIX_SINGLE_POSITION_ENDPOINT, BITGET_PAPTRADING_HEADER, BITGET_REST_QUOTA,
        BITGET_SPOT_ACCOUNT_ASSETS_ENDPOINT, BITGET_SPOT_BATCH_CANCEL_ORDER_ENDPOINT,
        BITGET_SPOT_CANCEL_ORDER_ENDPOINT, BITGET_SPOT_CANCEL_SYMBOL_ORDER_ENDPOINT,
        BITGET_SPOT_FILLS_ENDPOINT, BITGET_SPOT_HISTORY_ORDERS_ENDPOINT,
        BITGET_SPOT_ORDER_INFO_ENDPOINT, BITGET_SPOT_PLACE_ORDER_ENDPOINT,
        BITGET_SPOT_PLACE_PLAN_ORDER_ENDPOINT, BITGET_SPOT_UNFILLED_ORDERS_ENDPOINT,
    },
    credential::Credential,
    enums::{BitgetEnvironment, BitgetProductType},
    order::{
        BitgetBatchCancelOrdersRequest, BitgetCancelAllOrdersRequest, BitgetCancelOrderRequest,
        BitgetModifyOrderRequest, BitgetSubmitOrderRequest,
    },
    parse::{
        bar_spec_to_bitget_interval_for_product, bitget_symbol_status_action, parse_candle_bar,
        parse_funding_rate, parse_market_trade, parse_mix_account_state, parse_orderbook_snapshot,
        parse_spot_account_state, parse_spot_instrument, parse_usdt_perp_instrument,
    },
    symbol::extract_raw_symbol,
    urls::bitget_http_base_url,
};

const BITGET_GLOBAL_RATE_KEY: &str = "bitget:global";

fn query_string(mut params: Vec<(&str, String)>) -> Result<String, BitgetHttpError> {
    params.sort_by(|left, right| left.0.cmp(right.0).then(left.1.cmp(&right.1)));
    let query = serde_urlencoded::to_string(params)
        .map_err(|e| BitgetHttpError::ValidationError(e.to_string()))?;
    Ok(format!("?{query}"))
}

fn spot_depth_limit(limit: Option<u32>) -> Option<String> {
    limit.map(|value| value.min(150).to_string())
}

fn mix_depth_limit(limit: Option<u32>) -> Option<String> {
    limit.map(|value| {
        if value <= 1 {
            "1".to_string()
        } else if value <= 5 {
            "5".to_string()
        } else if value <= 15 {
            "15".to_string()
        } else if value <= 50 {
            "50".to_string()
        } else {
            "max".to_string()
        }
    })
}

#[derive(Debug, Default)]
struct BitgetOrderStatusPage {
    orders: Vec<super::models::BitgetOrderStatus>,
    next_cursor: Option<String>,
}

#[derive(Debug, Default)]
struct BitgetFillPage {
    fills: Vec<super::models::BitgetFill>,
    next_cursor: Option<String>,
}

/// Raw HTTP client for low-level Bitget API operations.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
#[derive(Clone)]
pub struct BitgetRawHttpClient {
    base_url: String,
    environment: BitgetEnvironment,
    client: HttpClient,
    credential: Option<Credential>,
    cancellation_token: Arc<std::sync::Mutex<CancellationToken>>,
}

impl Default for BitgetRawHttpClient {
    fn default() -> Self {
        Self::new(None, 60, None).expect("Failed to create default BitgetRawHttpClient")
    }
}

impl Debug for BitgetRawHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BitgetRawHttpClient))
            .field("base_url", &self.base_url)
            .field("environment", &self.environment)
            .field("has_credentials", &self.credential.is_some())
            .finish()
    }
}

impl BitgetRawHttpClient {
    /// Creates a new public [`BitgetRawHttpClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created.
    pub fn new(
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Self::new_with_environment(
            BitgetEnvironment::Mainnet,
            base_url,
            timeout_secs,
            proxy_url,
        )
    }

    /// Creates a new public [`BitgetRawHttpClient`] for an environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created.
    pub fn new_with_environment(
        environment: BitgetEnvironment,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Ok(Self {
            base_url: base_url.unwrap_or_else(|| bitget_http_base_url(environment).to_string()),
            environment,
            client: HttpClient::new(
                Self::default_headers(),
                vec![],
                Self::rate_limiter_quotas(),
                Some(*BITGET_REST_QUOTA),
                Some(timeout_secs),
                proxy_url,
            )
            .map_err(|e| {
                BitgetHttpError::NetworkError(format!("Failed to create HTTP client: {e}"))
            })?,
            credential: None,
            cancellation_token: Arc::new(std::sync::Mutex::new(CancellationToken::new())),
        })
    }

    /// Creates a new authenticated [`BitgetRawHttpClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created.
    #[expect(clippy::too_many_arguments)]
    pub fn with_credentials(
        api_key: String,
        api_secret: String,
        api_passphrase: String,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Self::with_credentials_for_environment(
            BitgetEnvironment::Mainnet,
            api_key,
            api_secret,
            api_passphrase,
            base_url,
            timeout_secs,
            proxy_url,
        )
    }

    /// Creates a new authenticated [`BitgetRawHttpClient`] for an environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created.
    #[expect(clippy::too_many_arguments)]
    pub fn with_credentials_for_environment(
        environment: BitgetEnvironment,
        api_key: String,
        api_secret: String,
        api_passphrase: String,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        let mut client =
            Self::new_with_environment(environment, base_url, timeout_secs, proxy_url)?;
        client.credential = Some(Credential::new(api_key, api_secret, api_passphrase));
        Ok(client)
    }

    /// Creates a new [`BitgetRawHttpClient`] with environment variable credential resolution.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created.
    pub fn new_with_env(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Self::new_with_env_for_environment(
            BitgetEnvironment::Mainnet,
            api_key,
            api_secret,
            api_passphrase,
            base_url,
            timeout_secs,
            proxy_url,
        )
    }

    /// Creates a new [`BitgetRawHttpClient`] for an environment with credential resolution.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created.
    pub fn new_with_env_for_environment(
        environment: BitgetEnvironment,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        match Credential::resolve(api_key, api_secret, api_passphrase) {
            Some(credential) => {
                let mut client =
                    Self::new_with_environment(environment, base_url, timeout_secs, proxy_url)?;
                client.credential = Some(credential);
                Ok(client)
            }
            None => Self::new_with_environment(environment, base_url, timeout_secs, proxy_url),
        }
    }

    /// Cancels all pending HTTP requests.
    #[expect(clippy::missing_panics_doc, reason = "mutex poisoning is not expected")]
    pub fn cancel_all_requests(&self) {
        self.cancellation_token
            .lock()
            .expect("cancellation token lock poisoned")
            .cancel();
    }

    fn default_headers() -> HashMap<String, String> {
        HashMap::from([
            (USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
            (BITGET_LOCALE_HEADER.to_string(), "en-US".to_string()),
        ])
    }

    fn environment_headers(&self) -> HashMap<String, String> {
        if self.environment.is_demo() {
            HashMap::from([(BITGET_PAPTRADING_HEADER.to_string(), "1".to_string())])
        } else {
            HashMap::new()
        }
    }

    fn rate_limiter_quotas() -> Vec<(String, Quota)> {
        vec![(BITGET_GLOBAL_RATE_KEY.to_string(), *BITGET_REST_QUOTA)]
    }

    fn rate_limit_keys(endpoint: &str) -> Vec<String> {
        let normalized = endpoint.split('?').next().unwrap_or(endpoint);
        vec![
            BITGET_GLOBAL_RATE_KEY.to_string(),
            format!("bitget:{normalized}"),
        ]
    }

    fn sign_request(
        &self,
        timestamp: &str,
        method: Method,
        endpoint: &str,
        query: Option<&str>,
        body: Option<&str>,
    ) -> Result<HashMap<String, String>, BitgetHttpError> {
        let credential = self
            .credential
            .as_ref()
            .ok_or(BitgetHttpError::MissingCredentials)?;
        let method = method.as_str().to_ascii_uppercase();
        let signature = credential.sign(timestamp, &method, endpoint, query, body);

        Ok(HashMap::from([
            (
                BITGET_ACCESS_KEY_HEADER.to_string(),
                credential.api_key().to_string(),
            ),
            (BITGET_ACCESS_SIGN_HEADER.to_string(), signature),
            (
                BITGET_ACCESS_TIMESTAMP_HEADER.to_string(),
                timestamp.to_string(),
            ),
            (
                BITGET_ACCESS_PASSPHRASE_HEADER.to_string(),
                credential.api_passphrase().to_string(),
            ),
        ]))
    }

    async fn send<T: DeserializeOwned + Default>(
        &self,
        method: Method,
        endpoint: &str,
        query: Option<&str>,
        body: Option<String>,
        authenticated: bool,
    ) -> Result<T, BitgetHttpError> {
        let mut url = format!("{}{}", self.base_url, endpoint);
        if let Some(query) = query {
            url.push_str(query);
        }

        let body_bytes = body.as_ref().map(|body| body.as_bytes().to_vec());
        let mut headers = self.environment_headers();
        if authenticated {
            let timestamp = chrono::Utc::now().timestamp_millis().to_string();
            headers.extend(self.sign_request(
                &timestamp,
                method.clone(),
                endpoint,
                query,
                body.as_deref(),
            )?);
        }

        let response = self
            .client
            .request(
                method,
                url,
                None,
                Some(headers),
                body_bytes,
                None,
                Some(Self::rate_limit_keys(endpoint)),
            )
            .await?;

        if !response.status.is_success() {
            return Err(BitgetHttpError::UnexpectedStatus {
                status: response.status.as_u16(),
                body: String::from_utf8_lossy(response.body.as_ref()).to_string(),
            });
        }

        let envelope: BitgetResponse<T> = serde_json::from_slice(response.body.as_ref())?;
        if !envelope.is_success() {
            return Err(BitgetHttpError::BitgetError {
                code: envelope.code,
                message: envelope.msg,
            });
        }

        Ok(envelope.data)
    }

    /// Sends a public GET request and returns the typed Bitget `data` payload.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the Bitget envelope is not successful.
    pub async fn get_public<T: DeserializeOwned + Default>(
        &self,
        endpoint: &str,
        query: Option<&str>,
    ) -> Result<T, BitgetHttpError> {
        self.send(Method::GET, endpoint, query, None, false).await
    }

    /// Sends an authenticated request and returns the typed Bitget `data` payload.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the envelope fails.
    pub async fn send_private<T: DeserializeOwned + Default, B: Serialize>(
        &self,
        method: Method,
        endpoint: &str,
        query: Option<&str>,
        body: Option<&B>,
    ) -> Result<T, BitgetHttpError> {
        let body = body.map(serde_json::to_string).transpose()?;
        self.send(method, endpoint, query, body, true).await
    }

    /// Requests Bitget Spot symbol definitions.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_spot_symbols(
        &self,
    ) -> Result<Vec<super::models::BitgetSpotSymbol>, BitgetHttpError> {
        self.get_public::<Vec<super::models::BitgetSpotSymbol>>(
            BITGET_MARKET_INSTRUMENTS_ENDPOINT,
            Some("?category=SPOT"),
        )
        .await
    }

    /// Requests Bitget USDT-FUTURES contract definitions.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_usdt_futures_contracts(
        &self,
    ) -> Result<Vec<super::models::BitgetMixContract>, BitgetHttpError> {
        self.get_public::<Vec<super::models::BitgetMixContract>>(
            BITGET_MARKET_INSTRUMENTS_ENDPOINT,
            Some("?category=USDT-FUTURES"),
        )
        .await
    }

    /// Requests a Bitget order book snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_orderbook(
        &self,
        product_type: BitgetProductType,
        raw_symbol: &str,
        limit: Option<u32>,
    ) -> Result<super::models::BitgetOrderBookSnapshot, BitgetHttpError> {
        let mut params = vec![
            ("category", product_type.as_api_str().to_string()),
            ("symbol", raw_symbol.to_string()),
        ];
        let limit = match product_type {
            BitgetProductType::Spot => spot_depth_limit(limit),
            BitgetProductType::UsdtFutures => mix_depth_limit(limit),
        };
        if let Some(limit) = limit {
            params.push(("limit", limit));
        }
        let query = query_string(params)?;

        self.get_public::<super::models::BitgetOrderBookSnapshot>(
            BITGET_MARKET_ORDERBOOK_ENDPOINT,
            Some(&query),
        )
        .await
    }

    /// Requests Bitget public market trades.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_market_trades(
        &self,
        product_type: BitgetProductType,
        raw_symbol: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<super::models::BitgetMarketTrade>, BitgetHttpError> {
        let mut params = vec![
            ("category", product_type.as_api_str().to_string()),
            ("symbol", raw_symbol.to_string()),
        ];
        if let Some(limit) = limit {
            params.push(("limit", limit.min(1_000).to_string()));
        }

        if let Some(start) = start {
            params.push(("startTime", start.timestamp_millis().to_string()));
        }
        if let Some(end) = end {
            params.push(("endTime", end.timestamp_millis().to_string()));
        }

        let query = query_string(params)?;

        self.get_public::<Vec<super::models::BitgetMarketTrade>>(
            BITGET_MARKET_FILLS_ENDPOINT,
            Some(&query),
        )
        .await
    }

    /// Requests Bitget candle rows.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_candles(
        &self,
        product_type: BitgetProductType,
        raw_symbol: &str,
        interval: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<super::models::BitgetCandle>, BitgetHttpError> {
        let mut params = vec![
            ("category", product_type.as_api_str().to_string()),
            ("symbol", raw_symbol.to_string()),
            ("interval", interval.to_string()),
        ];

        if let Some(start) = start {
            params.push(("startTime", start.timestamp_millis().to_string()));
        }
        if let Some(end) = end {
            params.push(("endTime", end.timestamp_millis().to_string()));
        }
        if let Some(limit) = limit {
            params.push(("limit", limit.min(1_000).to_string()));
        }

        let query = query_string(params)?;

        self.get_public::<Vec<super::models::BitgetCandle>>(
            BITGET_MARKET_CANDLES_ENDPOINT,
            Some(&query),
        )
        .await
    }

    /// Requests Bitget USDT-FUTURES historical funding rates.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_funding_rates(
        &self,
        raw_symbol: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<super::models::BitgetFundingRate>, BitgetHttpError> {
        let mut params = vec![
            (
                "category",
                BitgetProductType::UsdtFutures.as_api_str().to_string(),
            ),
            ("symbol", raw_symbol.to_string()),
        ];

        if let Some(start) = start {
            params.push(("startTime", start.timestamp_millis().to_string()));
        }
        if let Some(end) = end {
            params.push(("endTime", end.timestamp_millis().to_string()));
        }
        if let Some(limit) = limit {
            params.push(("limit", limit.min(100).to_string()));
        }

        let query = query_string(params)?;

        Ok(self
            .get_public::<super::models::BitgetFundingRatesData>(
                BITGET_MARKET_FUNDING_HISTORY_ENDPOINT,
                Some(&query),
            )
            .await?
            .result_list)
    }

    /// Requests Bitget Spot account asset rows.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_spot_account_assets(
        &self,
        coin: Option<&str>,
    ) -> Result<Vec<super::models::BitgetSpotAsset>, BitgetHttpError> {
        let account = self
            .send_private::<super::models::BitgetUtaAccount, serde_json::Value>(
                Method::GET,
                BITGET_SPOT_ACCOUNT_ASSETS_ENDPOINT,
                None,
                None,
            )
            .await?;

        Ok(account.into_spot_assets(coin))
    }

    /// Requests Bitget UTA account assets.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_uta_account(
        &self,
    ) -> Result<super::models::BitgetUtaAccount, BitgetHttpError> {
        self.send_private::<super::models::BitgetUtaAccount, serde_json::Value>(
            Method::GET,
            BITGET_SPOT_ACCOUNT_ASSETS_ENDPOINT,
            None,
            None,
        )
        .await
    }

    /// Requests Bitget Mix account rows.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_mix_accounts(
        &self,
        product_type: BitgetProductType,
    ) -> Result<Vec<super::models::BitgetMixAccount>, BitgetHttpError> {
        if product_type != BitgetProductType::UsdtFutures {
            return Ok(Vec::new());
        }

        let account = self
            .send_private::<super::models::BitgetUtaAccount, serde_json::Value>(
                Method::GET,
                BITGET_MIX_ACCOUNT_LIST_ENDPOINT,
                None,
                None,
            )
            .await?;

        Ok(account.into_usdt_futures_accounts())
    }

    /// Requests Bitget Mix position rows.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_mix_positions(
        &self,
        product_type: BitgetProductType,
        raw_symbol: Option<&str>,
    ) -> Result<Vec<super::models::BitgetMixPosition>, BitgetHttpError> {
        let mut params = vec![("category", product_type.as_api_str().to_string())];
        let endpoint = if let Some(raw_symbol) = raw_symbol {
            params.push(("symbol", raw_symbol.to_string()));
            BITGET_MIX_SINGLE_POSITION_ENDPOINT
        } else {
            BITGET_MIX_ALL_POSITIONS_ENDPOINT
        };
        let query = query_string(params)?;

        let payload = self
            .send_private::<super::models::BitgetMixPositionList, serde_json::Value>(
                Method::GET,
                endpoint,
                Some(&query),
                None,
            )
            .await?;

        Ok(payload.list)
    }

    async fn send_uta_batch_cancel(
        &self,
        orders: &[super::models::BitgetCancelBatchOrderItem],
    ) -> Result<super::models::BitgetCancelBatchResponse, BitgetHttpError> {
        let orders = orders.to_vec();
        let results = self
            .send_private::<Vec<super::models::BitgetCancelBatchResult>, _>(
                Method::POST,
                BITGET_SPOT_BATCH_CANCEL_ORDER_ENDPOINT,
                None,
                Some(&orders),
            )
            .await?;

        Ok(super::models::BitgetCancelBatchResponse::from_uta_results(
            results,
        ))
    }

    async fn send_uta_cancel_all<T: Serialize + Debug>(
        &self,
        request: &T,
    ) -> Result<super::models::BitgetCancelBatchResponse, BitgetHttpError> {
        let payload = self
            .send_private::<super::models::BitgetCancelAllResponse, _>(
                Method::POST,
                BITGET_SPOT_CANCEL_SYMBOL_ORDER_ENDPOINT,
                None,
                Some(request),
            )
            .await?;

        Ok(super::models::BitgetCancelBatchResponse::from_uta_results(
            payload.list,
        ))
    }

    async fn send_uta_order_status_page(
        &self,
        endpoint: &str,
        query: &str,
    ) -> Result<BitgetOrderStatusPage, BitgetHttpError> {
        let payload = self
            .send_private::<super::models::BitgetOrderStatusList, serde_json::Value>(
                Method::GET,
                endpoint,
                Some(query),
                None,
            )
            .await?;

        Ok(BitgetOrderStatusPage {
            orders: payload.entrusted_list,
            next_cursor: payload.end_id,
        })
    }

    async fn send_uta_fill_page(
        &self,
        endpoint: &str,
        query: &str,
    ) -> Result<BitgetFillPage, BitgetHttpError> {
        let payload = self
            .send_private::<super::models::BitgetFillList, serde_json::Value>(
                Method::GET,
                endpoint,
                Some(query),
                None,
            )
            .await?;

        Ok(BitgetFillPage {
            fills: payload.fill_list,
            next_cursor: payload.end_id,
        })
    }

    /// Submits a mapped Bitget order request.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn submit_order(
        &self,
        request: &BitgetSubmitOrderRequest,
    ) -> Result<super::models::BitgetOrderAck, BitgetHttpError> {
        match request {
            BitgetSubmitOrderRequest::Spot(request) => {
                self.send_private(
                    Method::POST,
                    BITGET_SPOT_PLACE_ORDER_ENDPOINT,
                    None,
                    Some(request),
                )
                .await
            }
            BitgetSubmitOrderRequest::SpotPlan(request) => {
                self.send_private(
                    Method::POST,
                    BITGET_SPOT_PLACE_PLAN_ORDER_ENDPOINT,
                    None,
                    Some(request),
                )
                .await
            }
            BitgetSubmitOrderRequest::Mix(request) => {
                self.send_private(
                    Method::POST,
                    BITGET_MIX_PLACE_ORDER_ENDPOINT,
                    None,
                    Some(request),
                )
                .await
            }
            BitgetSubmitOrderRequest::MixPlan(request) => {
                self.send_private(
                    Method::POST,
                    BITGET_MIX_PLACE_PLAN_ORDER_ENDPOINT,
                    None,
                    Some(request),
                )
                .await
            }
        }
    }

    /// Modifies a mapped Bitget order request.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn modify_order(
        &self,
        request: &BitgetModifyOrderRequest,
    ) -> Result<super::models::BitgetOrderAck, BitgetHttpError> {
        match request {
            BitgetModifyOrderRequest::Mix(request) => {
                self.send_private(
                    Method::POST,
                    BITGET_MIX_MODIFY_ORDER_ENDPOINT,
                    None,
                    Some(request),
                )
                .await
            }
            BitgetModifyOrderRequest::MixPlan(request) => {
                self.send_private(
                    Method::POST,
                    BITGET_MIX_MODIFY_PLAN_ORDER_ENDPOINT,
                    None,
                    Some(request),
                )
                .await
            }
        }
    }

    /// Cancels a mapped Bitget order request.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn cancel_order(
        &self,
        request: &BitgetCancelOrderRequest,
    ) -> Result<super::models::BitgetOrderAck, BitgetHttpError> {
        match request {
            BitgetCancelOrderRequest::Spot(request) => {
                self.send_private(
                    Method::POST,
                    BITGET_SPOT_CANCEL_ORDER_ENDPOINT,
                    None,
                    Some(request),
                )
                .await
            }
            BitgetCancelOrderRequest::Mix(request) => {
                self.send_private(
                    Method::POST,
                    BITGET_MIX_CANCEL_ORDER_ENDPOINT,
                    None,
                    Some(request),
                )
                .await
            }
            BitgetCancelOrderRequest::MixPlan(request) => {
                self.send_private(
                    Method::POST,
                    BITGET_MIX_CANCEL_PLAN_ORDER_ENDPOINT,
                    None,
                    Some(request),
                )
                .await
            }
        }
    }

    /// Cancels mapped Bitget orders in a batch.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn batch_cancel_orders(
        &self,
        request: &BitgetBatchCancelOrdersRequest,
    ) -> Result<super::models::BitgetCancelBatchResponse, BitgetHttpError> {
        match request {
            BitgetBatchCancelOrdersRequest::Spot(request) => {
                self.send_uta_batch_cancel(&request.order_list).await
            }
            BitgetBatchCancelOrdersRequest::Mix(request) => {
                self.send_uta_batch_cancel(&request.order_id_list).await
            }
        }
    }

    /// Cancels all mapped Bitget orders for a symbol/product route.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn cancel_all_orders(
        &self,
        request: &BitgetCancelAllOrdersRequest,
    ) -> Result<super::models::BitgetCancelBatchResponse, BitgetHttpError> {
        match request {
            BitgetCancelAllOrdersRequest::Spot(request) => self.send_uta_cancel_all(request).await,
            BitgetCancelAllOrdersRequest::Mix(request) => self.send_uta_cancel_all(request).await,
        }
    }

    /// Requests Bitget order detail by venue or client order ID.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_order_status(
        &self,
        product_type: BitgetProductType,
        raw_symbol: &str,
        venue_order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<super::models::BitgetOrderStatus, BitgetHttpError> {
        let mut params = vec![
            ("category", product_type.as_api_str().to_string()),
            ("symbol", raw_symbol.to_string()),
        ];
        let endpoint = match product_type {
            BitgetProductType::Spot => BITGET_SPOT_ORDER_INFO_ENDPOINT,
            BitgetProductType::UsdtFutures => BITGET_MIX_ORDER_DETAIL_ENDPOINT,
        };

        if let Some(order_id) = venue_order_id {
            params.push(("orderId", order_id.to_string()));
        }
        if let Some(client_oid) = client_order_id {
            params.push(("clientOid", client_oid.to_string()));
        }

        let query = query_string(params)?;

        self.send_private::<super::models::BitgetOrderStatus, serde_json::Value>(
            Method::GET,
            endpoint,
            Some(&query),
            None,
        )
        .await
    }

    /// Requests Bitget order status rows for reconciliation.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    #[expect(clippy::too_many_arguments)]
    async fn request_order_statuses_page(
        &self,
        product_type: BitgetProductType,
        raw_symbol: Option<&str>,
        start: Option<&DateTime<Utc>>,
        end: Option<&DateTime<Utc>>,
        open_only: bool,
        limit: u32,
        id_less_than: Option<&str>,
    ) -> Result<BitgetOrderStatusPage, BitgetHttpError> {
        let mut params: Vec<(&str, String)> =
            vec![("category", product_type.as_api_str().to_string())];
        let endpoint = match (product_type, open_only) {
            (BitgetProductType::Spot, true) => BITGET_SPOT_UNFILLED_ORDERS_ENDPOINT,
            (BitgetProductType::Spot, false) => BITGET_SPOT_HISTORY_ORDERS_ENDPOINT,
            (BitgetProductType::UsdtFutures, true) => BITGET_MIX_ORDERS_PENDING_ENDPOINT,
            (BitgetProductType::UsdtFutures, false) => BITGET_MIX_ORDERS_HISTORY_ENDPOINT,
        };

        if let Some(raw_symbol) = raw_symbol {
            params.push(("symbol", raw_symbol.to_string()));
        }
        if let Some(start) = start {
            params.push(("startTime", start.timestamp_millis().to_string()));
        }
        if let Some(end) = end {
            params.push(("endTime", end.timestamp_millis().to_string()));
        }
        params.push(("limit", limit.clamp(1, 100).to_string()));
        if let Some(id_less_than) = id_less_than {
            params.push(("cursor", id_less_than.to_string()));
        }

        let query = query_string(params)?;

        self.send_uta_order_status_page(endpoint, &query).await
    }

    /// Requests Bitget order status rows for reconciliation.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_order_statuses(
        &self,
        product_type: BitgetProductType,
        raw_symbol: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        open_only: bool,
        limit: Option<u32>,
    ) -> Result<Vec<super::models::BitgetOrderStatus>, BitgetHttpError> {
        let page_limit = limit.unwrap_or(100).clamp(1, 100);
        let mut cursor: Option<String> = None;
        let mut orders = Vec::new();

        loop {
            let page = self
                .request_order_statuses_page(
                    product_type,
                    raw_symbol,
                    start.as_ref(),
                    end.as_ref(),
                    open_only,
                    page_limit,
                    cursor.as_deref(),
                )
                .await?;
            let page_len = page.orders.len();
            orders.extend(page.orders);

            if page_len < page_limit as usize {
                break;
            }

            let Some(next_cursor) = page
                .next_cursor
                .map(|cursor| cursor.trim().to_string())
                .filter(|cursor| !cursor.is_empty())
            else {
                break;
            };

            if cursor.as_deref() == Some(next_cursor.as_str()) {
                log::warn!(
                    "Bitget order status pagination returned repeated cursor {next_cursor}; stopping"
                );
                break;
            }

            cursor = Some(next_cursor);
        }

        Ok(orders)
    }

    /// Requests one page of Bitget private fill rows.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    #[expect(clippy::too_many_arguments)]
    async fn request_fills_page(
        &self,
        product_type: BitgetProductType,
        raw_symbol: Option<&str>,
        start: Option<&DateTime<Utc>>,
        end: Option<&DateTime<Utc>>,
        limit: u32,
        id_less_than: Option<&str>,
    ) -> Result<BitgetFillPage, BitgetHttpError> {
        let mut params: Vec<(&str, String)> =
            vec![("category", product_type.as_api_str().to_string())];
        let endpoint = match product_type {
            BitgetProductType::Spot => BITGET_SPOT_FILLS_ENDPOINT,
            BitgetProductType::UsdtFutures => BITGET_MIX_ORDER_FILLS_ENDPOINT,
        };

        if let Some(raw_symbol) = raw_symbol {
            params.push(("symbol", raw_symbol.to_string()));
        }
        if let Some(start) = start {
            params.push(("startTime", start.timestamp_millis().to_string()));
        }
        if let Some(end) = end {
            params.push(("endTime", end.timestamp_millis().to_string()));
        }
        params.push(("limit", limit.clamp(1, 100).to_string()));
        if let Some(id_less_than) = id_less_than {
            params.push(("cursor", id_less_than.to_string()));
        }

        let query = query_string(params)?;

        self.send_uta_fill_page(endpoint, &query).await
    }

    /// Requests Bitget private fill rows for reconciliation.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_fills(
        &self,
        product_type: BitgetProductType,
        raw_symbol: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<super::models::BitgetFill>, BitgetHttpError> {
        let page_limit = limit.unwrap_or(100).clamp(1, 100);
        let mut cursor: Option<String> = None;
        let mut fills = Vec::new();

        loop {
            let page = self
                .request_fills_page(
                    product_type,
                    raw_symbol,
                    start.as_ref(),
                    end.as_ref(),
                    page_limit,
                    cursor.as_deref(),
                )
                .await?;
            let page_len = page.fills.len();
            fills.extend(page.fills);

            if page_len < page_limit as usize {
                break;
            }

            let Some(next_cursor) = page
                .next_cursor
                .map(|cursor| cursor.trim().to_string())
                .filter(|cursor| !cursor.is_empty())
            else {
                break;
            };

            if cursor.as_deref() == Some(next_cursor.as_str()) {
                log::warn!(
                    "Bitget fill pagination returned repeated cursor {next_cursor}; stopping"
                );
                break;
            }

            cursor = Some(next_cursor);
        }

        Ok(fills)
    }
}

/// Higher-level Bitget HTTP client which converts raw payloads to Nautilus model types.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
#[derive(Debug)]
pub struct BitgetHttpClient {
    raw: BitgetRawHttpClient,
    instruments_cache: Arc<AtomicMap<Ustr, InstrumentAny>>,
    cache_initialized: AtomicBool,
}

impl Clone for BitgetHttpClient {
    fn clone(&self) -> Self {
        let cache_initialized = AtomicBool::new(self.cache_initialized.load(Ordering::Acquire));

        Self {
            raw: self.raw.clone(),
            instruments_cache: Arc::clone(&self.instruments_cache),
            cache_initialized,
        }
    }
}

impl Default for BitgetHttpClient {
    fn default() -> Self {
        Self {
            raw: BitgetRawHttpClient::default(),
            instruments_cache: Arc::new(AtomicMap::new()),
            cache_initialized: AtomicBool::new(false),
        }
    }
}

impl BitgetHttpClient {
    /// Creates a new public [`BitgetHttpClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created.
    pub fn new(
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Self::new_with_environment(
            BitgetEnvironment::Mainnet,
            base_url,
            timeout_secs,
            proxy_url,
        )
    }

    /// Creates a new public [`BitgetHttpClient`] for an environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created.
    pub fn new_with_environment(
        environment: BitgetEnvironment,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Ok(Self {
            raw: BitgetRawHttpClient::new_with_environment(
                environment,
                base_url,
                timeout_secs,
                proxy_url,
            )?,
            instruments_cache: Arc::new(AtomicMap::new()),
            cache_initialized: AtomicBool::new(false),
        })
    }

    /// Creates a new authenticated [`BitgetHttpClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created.
    #[expect(clippy::too_many_arguments)]
    pub fn with_credentials(
        api_key: String,
        api_secret: String,
        api_passphrase: String,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Self::with_credentials_for_environment(
            BitgetEnvironment::Mainnet,
            api_key,
            api_secret,
            api_passphrase,
            base_url,
            timeout_secs,
            proxy_url,
        )
    }

    /// Creates a new authenticated [`BitgetHttpClient`] for an environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created.
    #[expect(clippy::too_many_arguments)]
    pub fn with_credentials_for_environment(
        environment: BitgetEnvironment,
        api_key: String,
        api_secret: String,
        api_passphrase: String,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Ok(Self {
            raw: BitgetRawHttpClient::with_credentials_for_environment(
                environment,
                api_key,
                api_secret,
                api_passphrase,
                base_url,
                timeout_secs,
                proxy_url,
            )?,
            instruments_cache: Arc::new(AtomicMap::new()),
            cache_initialized: AtomicBool::new(false),
        })
    }

    /// Creates a new [`BitgetHttpClient`] resolving credentials from environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created.
    pub fn new_with_env(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Self::new_with_env_for_environment(
            BitgetEnvironment::Mainnet,
            api_key,
            api_secret,
            api_passphrase,
            base_url,
            timeout_secs,
            proxy_url,
        )
    }

    /// Creates a new [`BitgetHttpClient`] for an environment resolving credentials from
    /// environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created.
    pub fn new_with_env_for_environment(
        environment: BitgetEnvironment,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, BitgetHttpError> {
        Ok(Self {
            raw: BitgetRawHttpClient::new_with_env_for_environment(
                environment,
                api_key,
                api_secret,
                api_passphrase,
                base_url,
                timeout_secs,
                proxy_url,
            )?,
            instruments_cache: Arc::new(AtomicMap::new()),
            cache_initialized: AtomicBool::new(false),
        })
    }

    /// Returns the raw HTTP client.
    #[must_use]
    pub const fn raw(&self) -> &BitgetRawHttpClient {
        &self.raw
    }

    /// Cancels all pending HTTP requests.
    pub fn cancel_all_requests(&self) {
        self.raw.cancel_all_requests();
    }

    /// Checks if the client has cached instrument definitions.
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        self.cache_initialized.load(Ordering::Acquire)
    }

    /// Returns a snapshot of all instrument symbols currently held in the cache.
    #[must_use]
    pub fn get_cached_symbols(&self) -> Vec<String> {
        self.instruments_cache
            .load()
            .keys()
            .map(ToString::to_string)
            .collect()
    }

    /// Caches a single instrument by both Nautilus and venue raw symbols.
    pub fn cache_instrument(&self, instrument: InstrumentAny) {
        self.instruments_cache.rcu(|cache| {
            cache.insert(instrument.id().symbol.inner(), instrument.clone());
            cache.insert(instrument.raw_symbol().inner(), instrument.clone());
        });
        self.cache_initialized.store(true, Ordering::Release);
    }

    /// Caches multiple instruments by both Nautilus and venue raw symbols.
    pub fn cache_instruments(&self, instruments: &[InstrumentAny]) {
        self.instruments_cache.rcu(|cache| {
            for instrument in instruments {
                cache.insert(instrument.id().symbol.inner(), instrument.clone());
                cache.insert(instrument.raw_symbol().inner(), instrument.clone());
            }
        });
        self.cache_initialized.store(true, Ordering::Release);
    }

    #[cfg(feature = "python")]
    pub(crate) fn instrument_from_cache_by_id(
        &self,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<InstrumentAny> {
        self.instruments_cache
            .get_cloned(&instrument_id.symbol.inner())
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id).into())
    }

    /// Requests instruments for the configured Bitget product type.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or an instrument cannot be parsed.
    pub async fn request_instruments(
        &self,
        product_type: BitgetProductType,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        let instruments = match product_type {
            BitgetProductType::Spot => self
                .raw
                .request_spot_symbols()
                .await?
                .iter()
                .map(|definition| parse_spot_instrument(definition, ts_init, ts_init))
                .collect::<anyhow::Result<Vec<_>>>()?,
            BitgetProductType::UsdtFutures => self
                .raw
                .request_usdt_futures_contracts()
                .await?
                .iter()
                .filter(|definition| {
                    definition
                        .contract_type
                        .as_deref()
                        .is_none_or(|s| s.eq_ignore_ascii_case("perpetual"))
                })
                .map(|definition| parse_usdt_perp_instrument(definition, ts_init, ts_init))
                .collect::<anyhow::Result<Vec<_>>>()?,
        };

        Ok(instruments)
    }

    /// Requests instrument statuses for the configured Bitget product type.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails.
    pub async fn request_instrument_statuses(
        &self,
        product_type: BitgetProductType,
    ) -> anyhow::Result<AHashMap<InstrumentId, MarketStatusAction>> {
        let mut statuses = AHashMap::new();

        match product_type {
            BitgetProductType::Spot => {
                for definition in self.raw.request_spot_symbols().await? {
                    let instrument_id =
                        crate::common::symbol::BitgetSymbol::spot(&definition.symbol)?
                            .to_instrument_id();
                    statuses.insert(
                        instrument_id,
                        bitget_symbol_status_action(definition.status.as_deref()),
                    );
                }
            }
            BitgetProductType::UsdtFutures => {
                for definition in self.raw.request_usdt_futures_contracts().await? {
                    if definition
                        .contract_type
                        .as_deref()
                        .is_some_and(|s| !s.eq_ignore_ascii_case("perpetual"))
                    {
                        continue;
                    }

                    let instrument_id =
                        crate::common::symbol::BitgetSymbol::usdt_perp(&definition.symbol)?
                            .to_instrument_id();
                    statuses.insert(
                        instrument_id,
                        bitget_symbol_status_action(definition.symbol_status.as_deref()),
                    );
                }
            }
        }

        Ok(statuses)
    }

    /// Requests an order book snapshot and converts it to Nautilus deltas.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_orderbook_snapshot(
        &self,
        product_type: BitgetProductType,
        instrument: &InstrumentAny,
        limit: Option<u32>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderBookDeltas> {
        let instrument_id = instrument.id();
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str()).to_string();
        let snapshot = self
            .raw
            .request_orderbook(product_type, &raw_symbol, limit)
            .await?;

        parse_orderbook_snapshot(&snapshot, instrument, Some(ts_init))
    }

    /// Requests public market trades and converts them to Nautilus trade ticks.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_trades(
        &self,
        product_type: BitgetProductType,
        instrument: &InstrumentAny,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> anyhow::Result<Vec<TradeTick>> {
        let instrument_id = instrument.id();
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str()).to_string();
        let raw_trades = self
            .raw
            .request_market_trades(product_type, &raw_symbol, start, end, limit)
            .await?;

        let mut trades = Vec::with_capacity(raw_trades.len());
        for trade in &raw_trades {
            trades.push(parse_market_trade(trade, instrument, None)?);
        }
        trades.sort_by_key(|trade| trade.ts_event);

        Ok(trades)
    }

    /// Requests candle history and converts it to Nautilus bars.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_bars(
        &self,
        product_type: BitgetProductType,
        instrument: &InstrumentAny,
        bar_type: BarType,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
        timestamp_on_close: bool,
    ) -> anyhow::Result<Vec<Bar>> {
        let instrument_id = instrument.id();
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str()).to_string();
        let interval = bar_spec_to_bitget_interval_for_product(
            product_type,
            bar_type.spec().aggregation,
            bar_type.spec().step.get() as u64,
        )?;
        let raw_bars = self
            .raw
            .request_candles(product_type, &raw_symbol, interval, start, end, limit)
            .await?;

        let mut bars = Vec::with_capacity(raw_bars.len());
        for candle in &raw_bars {
            bars.push(parse_candle_bar(
                candle,
                instrument,
                bar_type,
                timestamp_on_close,
                None,
            )?);
        }
        bars.sort_by_key(|bar| bar.ts_event);

        Ok(bars)
    }

    /// Requests historical funding rates and converts them to Nautilus funding updates.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_funding_rates(
        &self,
        product_type: BitgetProductType,
        instrument: &InstrumentAny,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> anyhow::Result<Vec<FundingRateUpdate>> {
        anyhow::ensure!(
            product_type == BitgetProductType::UsdtFutures,
            "Bitget funding rates are only available for USDT-FUTURES instruments"
        );

        let instrument_id = instrument.id();
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str()).to_string();
        let raw_rates = self
            .raw
            .request_funding_rates(&raw_symbol, start, end, limit)
            .await?;

        let mut raw_with_ts = Vec::with_capacity(raw_rates.len());
        for funding in raw_rates {
            let timestamp = funding.funding_time.parse::<i64>().with_context(|| {
                format!("invalid Bitget fundingTime: {:?}", funding.funding_time)
            })?;
            raw_with_ts.push((timestamp, funding));
        }
        raw_with_ts.sort_by_key(|(timestamp, _)| *timestamp);

        let mut rates = Vec::with_capacity(raw_with_ts.len());
        for index in 0..raw_with_ts.len() {
            let (timestamp, funding) = &raw_with_ts[index];
            let interval_millis = raw_with_ts
                .get(index + 1)
                .map(|(next_timestamp, _)| next_timestamp - timestamp);
            rates.push(parse_funding_rate(funding, instrument, interval_millis)?);
        }

        Ok(rates)
    }

    /// Requests the current Bitget account state.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or payload parsing fails.
    pub async fn request_account_state(
        &self,
        product_type: BitgetProductType,
        account_id: AccountId,
        ts_init: UnixNanos,
    ) -> anyhow::Result<AccountState> {
        match product_type {
            BitgetProductType::Spot => {
                let assets = self.raw.request_spot_account_assets(None).await?;
                parse_spot_account_state(&assets, account_id, ts_init)
            }
            BitgetProductType::UsdtFutures => {
                let accounts = self.raw.request_mix_accounts(product_type).await?;
                parse_mix_account_state(&accounts, account_id, ts_init)
            }
        }
    }

    /// Submits a mapped Bitget order request.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn submit_order(
        &self,
        request: &BitgetSubmitOrderRequest,
    ) -> Result<super::models::BitgetOrderAck, BitgetHttpError> {
        self.raw.submit_order(request).await
    }

    /// Modifies a mapped Bitget order request.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn modify_order(
        &self,
        request: &BitgetModifyOrderRequest,
    ) -> Result<super::models::BitgetOrderAck, BitgetHttpError> {
        self.raw.modify_order(request).await
    }

    /// Cancels a mapped Bitget order request.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn cancel_order(
        &self,
        request: &BitgetCancelOrderRequest,
    ) -> Result<super::models::BitgetOrderAck, BitgetHttpError> {
        self.raw.cancel_order(request).await
    }

    /// Cancels mapped Bitget orders in a batch.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn batch_cancel_orders(
        &self,
        request: &BitgetBatchCancelOrdersRequest,
    ) -> Result<super::models::BitgetCancelBatchResponse, BitgetHttpError> {
        self.raw.batch_cancel_orders(request).await
    }

    /// Cancels all mapped Bitget orders for a symbol/product route.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn cancel_all_orders(
        &self,
        request: &BitgetCancelAllOrdersRequest,
    ) -> Result<super::models::BitgetCancelBatchResponse, BitgetHttpError> {
        self.raw.cancel_all_orders(request).await
    }

    /// Requests Bitget order detail by venue or client order ID.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_order_status(
        &self,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        venue_order_id: Option<&str>,
        client_order_id: Option<&str>,
    ) -> Result<super::models::BitgetOrderStatus, BitgetHttpError> {
        let raw_symbol = extract_raw_symbol(instrument_id.symbol.as_str()).to_string();
        self.raw
            .request_order_status(product_type, &raw_symbol, venue_order_id, client_order_id)
            .await
    }

    /// Requests Bitget order status rows for reconciliation.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_order_statuses(
        &self,
        product_type: BitgetProductType,
        instrument_id: Option<InstrumentId>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        open_only: bool,
        limit: Option<u32>,
    ) -> Result<Vec<super::models::BitgetOrderStatus>, BitgetHttpError> {
        let raw_symbol = instrument_id.map(|id| extract_raw_symbol(id.symbol.as_str()).to_string());
        self.raw
            .request_order_statuses(
                product_type,
                raw_symbol.as_deref(),
                start,
                end,
                open_only,
                limit,
            )
            .await
    }

    /// Requests Bitget private fill rows for reconciliation.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_fills(
        &self,
        product_type: BitgetProductType,
        instrument_id: Option<InstrumentId>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<super::models::BitgetFill>, BitgetHttpError> {
        let raw_symbol = instrument_id.map(|id| extract_raw_symbol(id.symbol.as_str()).to_string());
        self.raw
            .request_fills(product_type, raw_symbol.as_deref(), start, end, limit)
            .await
    }

    /// Requests Bitget position rows for reconciliation.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials are missing, the request fails, or the payload fails.
    pub async fn request_positions(
        &self,
        product_type: BitgetProductType,
        instrument_id: Option<InstrumentId>,
    ) -> Result<Vec<super::models::BitgetMixPosition>, BitgetHttpError> {
        if product_type == BitgetProductType::Spot {
            return Ok(Vec::new());
        }

        let raw_symbol = instrument_id.map(|id| extract_raw_symbol(id.symbol.as_str()).to_string());
        self.raw
            .request_mix_positions(product_type, raw_symbol.as_deref())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use axum::{
        Json, Router,
        extract::State,
        http::HeaderMap,
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        common::order::{
            BitgetBatchCancelOrdersRequest, BitgetCancelAllOrdersRequest, BitgetCancelOrderRequest,
            BitgetModifyOrderRequest, BitgetSubmitOrderRequest,
        },
        http::models::{
            BitgetCancelBatchOrderItem, BitgetMixBatchCancelOrdersRequest,
            BitgetMixCancelOrderRequest, BitgetMixContractsResponse, BitgetMixModifyOrderRequest,
            BitgetSpotBatchCancelOrderRequest, BitgetSpotPlaceOrderRequest,
            BitgetSpotSymbolsResponse,
        },
    };

    #[derive(Clone, Default)]
    struct OrderFixtureState {
        requests: Arc<tokio::sync::Mutex<Vec<(String, Value)>>>,
        request_headers: Arc<tokio::sync::Mutex<Vec<(String, HeaderMap)>>>,
    }

    async fn start_order_fixture_server(state: OrderFixtureState) -> SocketAddr {
        let router = Router::new()
            .route("/api/v3/market/instruments", get(handle_instruments))
            .route("/api/v3/trade/place-order", post(handle_spot_place_order))
            .route("/api/v3/trade/modify-order", post(handle_mix_modify_order))
            .route("/api/v3/trade/cancel-order", post(handle_mix_cancel_order))
            .route("/api/v3/trade/cancel-batch", post(handle_batch_cancel))
            .route("/api/v3/trade/cancel-symbol-order", post(handle_cancel_all))
            .route("/api/v3/trade/fills", get(handle_null_fills))
            .route(
                "/api/v3/position/current-position",
                get(handle_null_positions),
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

    async fn handle_instruments(
        State(state): State<OrderFixtureState>,
        headers: HeaderMap,
    ) -> Response {
        state
            .request_headers
            .lock()
            .await
            .push(("instruments".to_string(), headers));
        Json(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": []
        }))
        .into_response()
    }

    async fn handle_spot_place_order(
        State(state): State<OrderFixtureState>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        state
            .request_headers
            .lock()
            .await
            .push(("spot_place_order".to_string(), headers));
        state
            .requests
            .lock()
            .await
            .push(("spot_place_order".to_string(), body.clone()));
        Json(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": {"orderId": "100", "clientOid": body["clientOid"]}
        }))
        .into_response()
    }

    async fn handle_mix_modify_order(
        State(state): State<OrderFixtureState>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        state
            .request_headers
            .lock()
            .await
            .push(("mix_modify_order".to_string(), headers));
        state
            .requests
            .lock()
            .await
            .push(("mix_modify_order".to_string(), body.clone()));
        Json(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": {"orderId": body["orderId"], "clientOid": body["clientOid"]}
        }))
        .into_response()
    }

    async fn handle_mix_cancel_order(
        State(state): State<OrderFixtureState>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        state
            .request_headers
            .lock()
            .await
            .push(("mix_cancel_order".to_string(), headers));
        state
            .requests
            .lock()
            .await
            .push(("mix_cancel_order".to_string(), body.clone()));
        Json(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": {"orderId": body["orderId"], "clientOid": body["clientOid"]}
        }))
        .into_response()
    }

    async fn handle_batch_cancel(
        State(state): State<OrderFixtureState>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        state
            .request_headers
            .lock()
            .await
            .push(("batch_cancel".to_string(), headers));
        state
            .requests
            .lock()
            .await
            .push(("batch_cancel".to_string(), body));
        Json(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": [
                {"orderId": "1", "clientOid": "C-1"},
                {"orderId": "2", "clientOid": "C-2", "code": "24056", "msg": "order not found"}
            ]
        }))
        .into_response()
    }

    async fn handle_cancel_all(
        State(state): State<OrderFixtureState>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        state
            .request_headers
            .lock()
            .await
            .push(("cancel_all".to_string(), headers));
        state
            .requests
            .lock()
            .await
            .push(("cancel_all".to_string(), body));
        Json(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": {
                "list": [{"orderId": "10", "clientOid": "C-10"}]
            }
        }))
        .into_response()
    }

    async fn handle_null_fills(
        State(state): State<OrderFixtureState>,
        headers: HeaderMap,
    ) -> Response {
        state
            .request_headers
            .lock()
            .await
            .push(("fills".to_string(), headers));
        Json(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": null
        }))
        .into_response()
    }

    async fn handle_null_positions(
        State(state): State<OrderFixtureState>,
        headers: HeaderMap,
    ) -> Response {
        state
            .request_headers
            .lock()
            .await
            .push(("positions".to_string(), headers));
        Json(json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1700000000000i64,
            "data": null
        }))
        .into_response()
    }

    fn authenticated_fixture_client(addr: SocketAddr) -> BitgetHttpClient {
        BitgetHttpClient::with_credentials(
            "key".to_string(),
            "secret".to_string(),
            "passphrase".to_string(),
            Some(format!("http://{addr}")),
            5,
            None,
        )
        .unwrap()
    }

    fn demo_authenticated_fixture_client(addr: SocketAddr) -> BitgetHttpClient {
        BitgetHttpClient::with_credentials_for_environment(
            BitgetEnvironment::Demo,
            "key".to_string(),
            "secret".to_string(),
            "passphrase".to_string(),
            Some(format!("http://{addr}")),
            5,
            None,
        )
        .unwrap()
    }

    #[rstest]
    fn raw_client_default_uses_mainnet_url() {
        let client = BitgetRawHttpClient::default();

        assert!(format!("{client:?}").contains("https://api.bitget.com"));
    }

    #[tokio::test]
    async fn demo_environment_adds_paptrading_header_to_public_rest_requests() {
        let state = OrderFixtureState::default();
        let addr = start_order_fixture_server(state.clone()).await;
        let client = BitgetHttpClient::new_with_environment(
            BitgetEnvironment::Demo,
            Some(format!("http://{addr}")),
            5,
            None,
        )
        .unwrap();

        let instruments = client
            .request_instruments(
                BitgetProductType::Spot,
                UnixNanos::new(1_700_000_000_000_000_000),
            )
            .await
            .unwrap();

        assert!(instruments.is_empty());
        let request_headers = state.request_headers.lock().await;
        let headers = &request_headers[0].1;
        assert_eq!(
            headers
                .get(BITGET_PAPTRADING_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
    }

    #[tokio::test]
    async fn demo_environment_adds_paptrading_header_to_private_rest_requests() {
        let state = OrderFixtureState::default();
        let addr = start_order_fixture_server(state.clone()).await;
        let client = demo_authenticated_fixture_client(addr);
        let request = BitgetSubmitOrderRequest::Spot(BitgetSpotPlaceOrderRequest {
            category: "SPOT".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            force: Some("gtc".to_string()),
            price: Some("100.0".to_string()),
            size: "0.01".to_string(),
            client_oid: Some("C-DEMO".to_string()),
            stp_mode: None,
        });

        client.submit_order(&request).await.unwrap();

        let request_headers = state.request_headers.lock().await;
        let headers = request_headers
            .iter()
            .find(|(name, _)| name == "spot_place_order")
            .map(|(_, headers)| headers)
            .unwrap();
        assert_eq!(
            headers
                .get(BITGET_PAPTRADING_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        assert!(headers.get(BITGET_ACCESS_SIGN_HEADER).is_some());
    }

    #[tokio::test]
    async fn mainnet_environment_does_not_add_paptrading_header() {
        let state = OrderFixtureState::default();
        let addr = start_order_fixture_server(state.clone()).await;
        let client = authenticated_fixture_client(addr);
        let request = BitgetSubmitOrderRequest::Spot(BitgetSpotPlaceOrderRequest {
            category: "SPOT".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            force: Some("gtc".to_string()),
            price: Some("100.0".to_string()),
            size: "0.01".to_string(),
            client_oid: Some("C-MAINNET".to_string()),
            stp_mode: None,
        });

        client.submit_order(&request).await.unwrap();

        let request_headers = state.request_headers.lock().await;
        let headers = request_headers
            .iter()
            .find(|(name, _)| name == "spot_place_order")
            .map(|(_, headers)| headers)
            .unwrap();
        assert!(headers.get(BITGET_PAPTRADING_HEADER).is_none());
    }

    #[tokio::test]
    async fn http_fixture_null_private_list_payloads_parse_as_empty() {
        let state = OrderFixtureState::default();
        let addr = start_order_fixture_server(state).await;
        let client = authenticated_fixture_client(addr);

        let fills = client
            .raw()
            .request_fills(
                BitgetProductType::UsdtFutures,
                Some("BTCUSDT"),
                None,
                None,
                Some(100),
            )
            .await
            .unwrap();
        let positions = client
            .raw()
            .request_mix_positions(BitgetProductType::UsdtFutures, Some("BTCUSDT"))
            .await
            .unwrap();

        assert!(fills.is_empty());
        assert!(positions.is_empty());
    }

    #[rstest]
    fn query_string_sorts_parameters_for_bitget_signing() {
        let query = query_string(vec![
            ("category", "USDT-FUTURES".to_string()),
            ("symbol", "BTCUSDT".to_string()),
            ("limit", "100".to_string()),
        ])
        .unwrap();

        assert_eq!(query, "?category=USDT-FUTURES&limit=100&symbol=BTCUSDT");
    }

    #[rstest]
    fn sign_headers_include_bitget_access_headers() {
        let client = BitgetRawHttpClient::with_credentials(
            "key".to_string(),
            "secret".to_string(),
            "passphrase".to_string(),
            None,
            60,
            None,
        )
        .unwrap();

        let headers = client
            .sign_request(
                "1700000000000",
                Method::GET,
                BITGET_MARKET_INSTRUMENTS_ENDPOINT,
                Some("?category=USDT-FUTURES"),
                None,
            )
            .unwrap();

        assert_eq!(headers.get(BITGET_ACCESS_KEY_HEADER).unwrap(), "key");
        assert_eq!(
            headers.get(BITGET_ACCESS_PASSPHRASE_HEADER).unwrap(),
            "passphrase"
        );
        assert_eq!(
            headers.get(BITGET_ACCESS_SIGN_HEADER).unwrap(),
            "rzhObUOoLh7FK+WLJfYrk/BfldkvNvDB1mEeiADbwT0="
        );
    }

    #[rstest]
    fn sign_headers_allow_empty_passphrase() {
        let client = BitgetRawHttpClient::with_credentials(
            "key".to_string(),
            "secret".to_string(),
            String::new(),
            None,
            60,
            None,
        )
        .unwrap();

        let headers = client
            .sign_request(
                "1700000000000",
                Method::GET,
                BITGET_MARKET_INSTRUMENTS_ENDPOINT,
                Some("?category=USDT-FUTURES"),
                None,
            )
            .unwrap();

        assert_eq!(headers.get(BITGET_ACCESS_KEY_HEADER).unwrap(), "key");
        assert_eq!(headers.get(BITGET_ACCESS_PASSPHRASE_HEADER).unwrap(), "");
        assert_eq!(
            headers.get(BITGET_ACCESS_SIGN_HEADER).unwrap(),
            "rzhObUOoLh7FK+WLJfYrk/BfldkvNvDB1mEeiADbwT0="
        );
    }

    #[rstest]
    fn response_aliases_deserialize_as_expected() {
        let spot: BitgetSpotSymbolsResponse =
            serde_json::from_str(r#"{"code":"00000","msg":"success","data":[]}"#).unwrap();
        let mix: BitgetMixContractsResponse =
            serde_json::from_str(r#"{"code":"00000","msg":"success","data":[]}"#).unwrap();

        assert!(spot.is_success());
        assert!(mix.is_success());
    }

    #[tokio::test]
    async fn http_fixture_spot_batch_cancel_posts_expected_body_and_parses_failures() {
        let state = OrderFixtureState::default();
        let addr = start_order_fixture_server(state.clone()).await;
        let client = authenticated_fixture_client(addr);
        let request = BitgetBatchCancelOrdersRequest::Spot(BitgetSpotBatchCancelOrderRequest {
            category: "SPOT".to_string(),
            symbol: "BTCUSDT".to_string(),
            batch_mode: Some("single".to_string()),
            order_list: vec![
                BitgetCancelBatchOrderItem {
                    category: Some("SPOT".to_string()),
                    symbol: Some("BTCUSDT".to_string()),
                    order_id: Some("1".to_string()),
                    client_oid: Some("C-1".to_string()),
                },
                BitgetCancelBatchOrderItem {
                    category: Some("SPOT".to_string()),
                    symbol: Some("BTCUSDT".to_string()),
                    order_id: Some("2".to_string()),
                    client_oid: Some("C-2".to_string()),
                },
            ],
        });

        let response = client.batch_cancel_orders(&request).await.unwrap();

        assert_eq!(response.success_list.len(), 1);
        assert_eq!(response.failure_list.len(), 1);
        assert_eq!(
            response.failure_list[0].error_msg.as_deref(),
            Some("order not found")
        );

        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "batch_cancel");
        let body = &requests[0].1;
        assert_eq!(body[0]["category"], "SPOT");
        assert_eq!(body[0]["symbol"], "BTCUSDT");
        assert_eq!(body[0]["orderId"], "1");
        assert_eq!(body[1]["clientOid"], "C-2");
    }

    #[tokio::test]
    async fn http_fixture_submit_modify_cancel_posts_expected_bodies() {
        let state = OrderFixtureState::default();
        let addr = start_order_fixture_server(state.clone()).await;
        let client = authenticated_fixture_client(addr);

        let submit = BitgetSubmitOrderRequest::Spot(BitgetSpotPlaceOrderRequest {
            category: "SPOT".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "buy".to_string(),
            order_type: "limit".to_string(),
            force: Some("gtc".to_string()),
            price: Some("100.00".to_string()),
            size: "0.001".to_string(),
            client_oid: Some("C-1".to_string()),
            stp_mode: None,
        });
        let modify = BitgetModifyOrderRequest::Mix(BitgetMixModifyOrderRequest {
            symbol: "BTCUSDT".to_string(),
            product_type: "USDT-FUTURES".to_string(),
            order_id: Some("100".to_string()),
            client_oid: Some("C-1".to_string()),
            new_client_oid: None,
            new_size: Some("0.002".to_string()),
            new_price: Some("101.00".to_string()),
        });
        let cancel = BitgetCancelOrderRequest::Mix(BitgetMixCancelOrderRequest {
            symbol: "BTCUSDT".to_string(),
            product_type: "USDT-FUTURES".to_string(),
            margin_coin: Some("USDT".to_string()),
            order_id: Some("100".to_string()),
            client_oid: Some("C-1".to_string()),
        });

        let submit_ack = client.submit_order(&submit).await.unwrap();
        let modify_ack = client.modify_order(&modify).await.unwrap();
        let cancel_ack = client.cancel_order(&cancel).await.unwrap();

        assert_eq!(submit_ack.order_id.as_deref(), Some("100"));
        assert_eq!(modify_ack.client_oid.as_deref(), Some("C-1"));
        assert_eq!(cancel_ack.order_id.as_deref(), Some("100"));

        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].0, "spot_place_order");
        assert_eq!(requests[0].1["symbol"], "BTCUSDT");
        assert_eq!(requests[0].1["category"], "SPOT");
        assert_eq!(requests[0].1["orderType"], "limit");
        assert_eq!(requests[0].1["timeInForce"], "gtc");
        assert_eq!(requests[0].1["qty"], "0.001");
        assert_eq!(requests[0].1["clientOid"], "C-1");

        assert_eq!(requests[1].0, "mix_modify_order");
        assert_eq!(requests[1].1["category"], "USDT-FUTURES");
        assert_eq!(requests[1].1["qty"], "0.002");
        assert_eq!(requests[1].1["price"], "101.00");

        assert_eq!(requests[2].0, "mix_cancel_order");
        assert_eq!(requests[2].1["category"], "USDT-FUTURES");
        assert!(requests[2].1.get("marginCoin").is_none());
        assert_eq!(requests[2].1["orderId"], "100");
    }

    #[tokio::test]
    async fn http_fixture_mix_cancel_all_posts_product_scope_body() {
        let state = OrderFixtureState::default();
        let addr = start_order_fixture_server(state.clone()).await;
        let client = authenticated_fixture_client(addr);
        let request = BitgetCancelAllOrdersRequest::Mix(BitgetMixBatchCancelOrdersRequest {
            order_id_list: Vec::new(),
            symbol: Some("BTCUSDT".to_string()),
            product_type: "USDT-FUTURES".to_string(),
            margin_coin: Some("USDT".to_string()),
        });

        let response = client.cancel_all_orders(&request).await.unwrap();

        assert_eq!(response.success_list.len(), 1);
        assert!(response.failure_list.is_empty());

        let requests = state.requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "cancel_all");
        let body = &requests[0].1;
        assert_eq!(body["symbol"], "BTCUSDT");
        assert_eq!(body["category"], "USDT-FUTURES");
        assert!(body.get("marginCoin").is_none());
        assert!(body.get("orderIdList").is_none());
    }
}
