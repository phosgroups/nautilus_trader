use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use chrono::{DateTime, Utc};
use nautilus_core::{AtomicMap, UnixNanos, consts::NAUTILUS_USER_AGENT};
use nautilus_model::{
    data::{Bar, BarType, OrderBookDeltas, TradeTick},
    enums::BarAggregation,
    events::AccountState,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::http::{HttpClient, Method, USER_AGENT};
use nautilus_network::retry::{RetryConfig, RetryManager};
use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use crate::{
    common::{
        consts::*,
        credential::Credential,
        enums::GateioProductType,
        parse::{
            parse_book, parse_candle, parse_perpetual_instrument, parse_spot_instrument,
            parse_trade,
        },
        symbol::raw_symbol,
    },
    config,
    http::{
        error::GateioHttpError,
        models::{
            GateioAccount, GateioAccountDetail, GateioCandle, GateioContract, GateioErrorResponse,
            GateioFundingRate, GateioOrder, GateioOrderAmendRequest, GateioOrderBook,
            GateioOrderRequest, GateioPosition, GateioSpotOpenOrders, GateioSpotPair, GateioTrade,
            GateioUserTrade,
        },
    },
};

const RATE_KEY: &str = "gateio:global";
const PAGINATION_LIMIT: u32 = 1000;
const SPOT_OPEN_ORDER_LIMIT: u32 = 100;
const SPOT_CANDLE_MAX_LIMIT: u32 = 1000;
const FUTURES_CANDLE_MAX_LIMIT: u32 = 2000;
const MAX_SPOT_TRADE_RANGE_SECS: i64 = 30 * 24 * 60 * 60;
const MAX_PAGINATION_PAGES: u32 = 10_000;

#[derive(Clone)]
pub struct GateioRawHttpClient {
    base_url: String,
    client: HttpClient,
    credential: Option<Credential>,
    cancellation_token: Arc<std::sync::Mutex<CancellationToken>>,
    retry_manager: RetryManager<GateioHttpError>,
}

impl std::fmt::Debug for GateioRawHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GateioRawHttpClient")
            .field("base_url", &self.base_url)
            .field("has_credentials", &self.credential.is_some())
            .finish()
    }
}

impl GateioRawHttpClient {
    pub fn new(
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, GateioHttpError> {
        Ok(Self {
            base_url: crate::common::urls::http_base_url(base_url.as_deref()),
            client: HttpClient::new(
                HashMap::from([
                    (USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string()),
                    ("Content-Type".to_string(), "application/json".to_string()),
                ]),
                vec![],
                vec![(RATE_KEY.to_string(), *GATEIO_REST_QUOTA)],
                Some(*GATEIO_REST_QUOTA),
                Some(timeout_secs),
                proxy_url,
            )?,
            credential: None,
            cancellation_token: Arc::new(std::sync::Mutex::new(CancellationToken::new())),
            retry_manager: retry_manager(3),
        })
    }

    pub fn new_with_credentials(
        api_key: Option<String>,
        api_secret: Option<String>,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, GateioHttpError> {
        Self::new_with_credentials_and_retry(
            api_key,
            api_secret,
            base_url,
            timeout_secs,
            proxy_url,
            3,
        )
    }

    pub fn new_with_credentials_and_retry(
        api_key: Option<String>,
        api_secret: Option<String>,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
        max_retries: u32,
    ) -> Result<Self, GateioHttpError> {
        let mut client = Self::new(base_url, timeout_secs, proxy_url)?;
        client.credential = Credential::resolve(api_key, api_secret);
        client.retry_manager = retry_manager(max_retries);
        Ok(client)
    }

    pub fn cancel_all_requests(&self) {
        if let Ok(mut token) = self.cancellation_token.lock() {
            token.cancel();
            *token = CancellationToken::new();
        }
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token
            .lock()
            .map_or_else(|_| CancellationToken::new(), |token| token.clone())
    }

    fn endpoint_path(endpoint: &str) -> String {
        format!("/api/v4{endpoint}")
    }

    fn query_string(params: &[(String, String)]) -> Result<String, GateioHttpError> {
        if params.is_empty() {
            return Ok(String::new());
        }
        let mut params = params.to_vec();
        params.sort();
        Ok(serde_urlencoded::to_string(params)
            .map_err(|e| GateioHttpError::Validation(e.to_string()))?)
    }

    async fn send<T: DeserializeOwned + 'static>(
        &self,
        method: Method,
        endpoint: &str,
        params: &[(String, String)],
        body: Option<String>,
        authenticated: bool,
    ) -> Result<T, GateioHttpError> {
        let retry_method = method.clone();
        let endpoint = endpoint.to_string();
        let params = params.to_vec();
        let token = self.cancellation_token();
        self.retry_manager
            .execute_with_retry_with_cancel(
                "gateio_http_request",
                || {
                    self.send_once(
                        method.clone(),
                        &endpoint,
                        &params,
                        body.clone(),
                        authenticated,
                    )
                },
                |error| should_retry(&retry_method, error),
                GateioHttpError::Network,
                &token,
            )
            .await
    }

    async fn send_once<T: DeserializeOwned>(
        &self,
        method: Method,
        endpoint: &str,
        params: &[(String, String)],
        body: Option<String>,
        authenticated: bool,
    ) -> Result<T, GateioHttpError> {
        let query = Self::query_string(params)?;
        let path = Self::endpoint_path(endpoint);
        let mut url = format!("{}{}", self.base_url.trim_end_matches('/'), endpoint);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&query);
        }
        let mut headers = HashMap::new();
        if authenticated {
            let credential = self
                .credential
                .as_ref()
                .ok_or(GateioHttpError::MissingCredentials)?;
            let timestamp = Utc::now().timestamp().to_string();
            headers.insert(
                GATEIO_KEY_HEADER.to_string(),
                credential.api_key().to_string(),
            );
            headers.insert(GATEIO_TIMESTAMP_HEADER.to_string(), timestamp.clone());
            headers.insert(
                GATEIO_SIGN_HEADER.to_string(),
                credential.sign_rest(
                    method.as_str(),
                    &path,
                    &query,
                    body.as_deref().unwrap_or(""),
                    &timestamp,
                ),
            );
        }
        let response = self
            .client
            .request(
                method,
                url,
                None,
                Some(headers),
                body.map(String::into_bytes),
                None,
                Some(vec![RATE_KEY.to_string()]),
            )
            .await?;
        if !response.status.is_success() {
            return Err(GateioHttpError::UnexpectedStatus {
                status: response.status.as_u16(),
                body: String::from_utf8_lossy(response.body.as_ref()).to_string(),
            });
        }
        let value = serde_json::from_slice::<serde_json::Value>(response.body.as_ref())?;
        if let Ok(error) = serde_json::from_value::<GateioErrorResponse>(value.clone())
            && (!error.label.is_empty() || !error.message.is_empty())
        {
            return Err(GateioHttpError::GateioError {
                label: error.label,
                message: error.message,
            });
        }
        Ok(serde_json::from_value(value)?)
    }

    pub async fn spot_pairs(&self) -> Result<Vec<GateioSpotPair>, GateioHttpError> {
        self.send(Method::GET, SPOT_CURRENCY_PAIRS, &[], None, false)
            .await
    }

    pub async fn spot_pair(&self, symbol: &str) -> Result<GateioSpotPair, GateioHttpError> {
        self.send(
            Method::GET,
            &format!("{SPOT_CURRENCY_PAIRS}/{symbol}"),
            &[],
            None,
            false,
        )
        .await
    }

    pub async fn account_detail(&self) -> Result<GateioAccountDetail, GateioHttpError> {
        self.send(Method::GET, ACCOUNT_DETAIL, &[], None, true)
            .await
    }

    pub async fn contracts(&self) -> Result<Vec<GateioContract>, GateioHttpError> {
        self.send(Method::GET, FUTURES_CONTRACTS, &[], None, false)
            .await
    }

    pub async fn contract(&self, symbol: &str) -> Result<GateioContract, GateioHttpError> {
        self.send(
            Method::GET,
            &format!("{FUTURES_CONTRACTS}/{symbol}"),
            &[],
            None,
            false,
        )
        .await
    }

    pub async fn spot_order_book(
        &self,
        symbol: &str,
        limit: Option<u32>,
    ) -> Result<GateioOrderBook, GateioHttpError> {
        let mut params = vec![
            ("currency_pair".to_string(), symbol.to_string()),
            ("with_id".to_string(), "true".to_string()),
        ];
        if let Some(limit) = limit {
            params.push((
                "limit".to_string(),
                bounded_limit(limit, PAGINATION_LIMIT, "spot order book")?.to_string(),
            ));
        }
        self.send(Method::GET, SPOT_ORDER_BOOK, &params, None, false)
            .await
    }

    pub async fn futures_order_book(
        &self,
        contract: &str,
        limit: Option<u32>,
    ) -> Result<GateioOrderBook, GateioHttpError> {
        let mut params = vec![
            ("contract".to_string(), contract.to_string()),
            ("with_id".to_string(), "true".to_string()),
        ];
        if let Some(limit) = limit {
            params.push((
                "limit".to_string(),
                bounded_limit(limit, PAGINATION_LIMIT, "futures order book")?.to_string(),
            ));
        }
        self.send(Method::GET, FUTURES_ORDER_BOOK, &params, None, false)
            .await
    }

    pub async fn spot_trades(
        &self,
        symbol: &str,
        limit: Option<u32>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<GateioTrade>, GateioHttpError> {
        let mut params = vec![("currency_pair".to_string(), symbol.to_string())];
        if let Some(limit) = limit {
            params.push((
                "limit".to_string(),
                bounded_limit(limit, PAGINATION_LIMIT, "spot trades")?.to_string(),
            ));
        }
        validate_time_range(start, end, Some(MAX_SPOT_TRADE_RANGE_SECS), "spot trades")?;
        append_time_range(&mut params, start, end)?;
        self.send(Method::GET, SPOT_TRADES, &params, None, false)
            .await
    }

    pub async fn futures_trades(
        &self,
        contract: &str,
        limit: Option<u32>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<GateioTrade>, GateioHttpError> {
        let mut params = vec![("contract".to_string(), contract.to_string())];
        if let Some(limit) = limit {
            params.push((
                "limit".to_string(),
                bounded_limit(limit, PAGINATION_LIMIT, "futures trades")?.to_string(),
            ));
        }
        validate_time_range(start, end, None, "futures trades")?;
        append_time_range(&mut params, start, end)?;
        self.send(Method::GET, FUTURES_TRADES, &params, None, false)
            .await
    }

    pub async fn futures_funding_rates(
        &self,
        contract: &str,
        limit: Option<u32>,
    ) -> Result<Vec<GateioFundingRate>, GateioHttpError> {
        let mut params = vec![("contract".to_string(), contract.to_string())];
        if let Some(limit) = limit {
            params.push((
                "limit".to_string(),
                bounded_limit(limit, PAGINATION_LIMIT, "funding rates")?.to_string(),
            ));
        }
        self.send(Method::GET, FUTURES_FUNDING_RATE, &params, None, false)
            .await
    }

    pub async fn candles(
        &self,
        product_type: GateioProductType,
        symbol: &str,
        interval: &str,
        limit: Option<u32>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<GateioCandle>, GateioHttpError> {
        let endpoint = if product_type == GateioProductType::Spot {
            SPOT_CANDLESTICKS
        } else {
            FUTURES_CANDLESTICKS
        };
        let symbol_key = if product_type == GateioProductType::Spot {
            "currency_pair"
        } else {
            "contract"
        };
        let mut params = vec![
            (symbol_key.to_string(), symbol.to_string()),
            ("interval".to_string(), interval.to_string()),
        ];
        validate_time_range(start, end, None, "candlesticks")?;
        match (limit, start, end) {
            (Some(limit), None, None) => {
                let maximum = if product_type == GateioProductType::Spot {
                    SPOT_CANDLE_MAX_LIMIT
                } else {
                    FUTURES_CANDLE_MAX_LIMIT
                };
                params.push((
                    "limit".to_string(),
                    bounded_limit(limit, maximum, "candlesticks")?.to_string(),
                ));
            }
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                return Err(GateioHttpError::Validation(
                    "Gate.io candlestick limit cannot be combined with from/to".to_string(),
                ));
            }
            (None, Some(_), _) | (None, _, Some(_)) => {
                append_time_range(&mut params, start, end)?;
            }
            (None, None, None) => {}
        }
        self.send(Method::GET, endpoint, &params, None, false).await
    }

    pub async fn spot_accounts(&self) -> Result<Vec<GateioAccount>, GateioHttpError> {
        self.send(Method::GET, SPOT_ACCOUNTS, &[], None, true).await
    }

    pub async fn futures_accounts(&self) -> Result<GateioAccount, GateioHttpError> {
        self.send(Method::GET, FUTURES_ACCOUNTS, &[], None, true)
            .await
    }

    pub async fn futures_positions(&self) -> Result<Vec<GateioPosition>, GateioHttpError> {
        self.send(Method::GET, FUTURES_POSITIONS, &[], None, true)
            .await
    }

    pub async fn futures_positions_for(
        &self,
        contract: Option<&str>,
    ) -> Result<Vec<GateioPosition>, GateioHttpError> {
        let params = contract
            .map(|value| vec![("contract".to_string(), value.to_string())])
            .unwrap_or_default();
        self.send(Method::GET, FUTURES_POSITIONS, &params, None, true)
            .await
    }

    pub async fn order(
        &self,
        product_type: GateioProductType,
        order_id: &str,
        symbol: &str,
    ) -> Result<GateioOrder, GateioHttpError> {
        let endpoint = if product_type == GateioProductType::Spot {
            format!("{SPOT_ORDERS}/{order_id}")
        } else {
            format!("{FUTURES_ORDERS}/{order_id}")
        };
        let params = if product_type == GateioProductType::Spot {
            vec![("currency_pair".to_string(), symbol.to_string())]
        } else {
            // The futures single-order endpoint only accepts settle and order_id.
            // The contract is present in the response and is not a query parameter.
            Vec::new()
        };
        self.send(Method::GET, &endpoint, &params, None, true).await
    }

    pub async fn open_orders(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioOrder>, GateioHttpError> {
        if product_type == GateioProductType::Spot {
            let mut result = Vec::new();
            let mut page_fingerprints = HashSet::new();
            let mut order_fingerprints = HashSet::new();
            for page in 1..=MAX_PAGINATION_PAGES {
                let params = vec![
                    ("page".to_string(), page.to_string()),
                    ("limit".to_string(), SPOT_OPEN_ORDER_LIMIT.to_string()),
                ];
                let groups: Vec<GateioSpotOpenOrders> = self
                    .send(Method::GET, SPOT_OPEN_ORDERS, &params, None, true)
                    .await?;
                if groups.is_empty() {
                    break;
                }

                let page_fingerprint = serde_json::to_string(&groups)?;
                if !page_fingerprints.insert(page_fingerprint) {
                    return Err(GateioHttpError::Validation(
                        "Gate.io spot open-orders pagination returned a repeated page".to_string(),
                    ));
                }
                let mut page_is_complete = true;
                let mut new_orders = 0;
                for group in groups {
                    if group.orders.len() as u32 >= SPOT_OPEN_ORDER_LIMIT {
                        page_is_complete = false;
                    }
                    if symbol.is_some_and(|value| value != group.currency_pair.as_str()) {
                        continue;
                    }
                    for mut order in group.orders {
                        order.currency_pair = Some(group.currency_pair.clone());
                        let fingerprint = serde_json::to_string(&order)?;
                        if order_fingerprints.insert(fingerprint) {
                            result.push(order);
                            new_orders += 1;
                        }
                    }
                }
                if !page_is_complete && new_orders == 0 {
                    return Err(GateioHttpError::Validation(
                        "Gate.io spot open-orders pagination made no progress".to_string(),
                    ));
                }

                if page_is_complete {
                    break;
                }
            }
            return Ok(result);
        }
        self.orders(product_type, "open", symbol).await
    }

    pub async fn orders(
        &self,
        product_type: GateioProductType,
        status: &str,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioOrder>, GateioHttpError> {
        let endpoint = if product_type == GateioProductType::Spot {
            SPOT_ORDERS
        } else {
            FUTURES_ORDERS
        };
        let symbol_key = if product_type == GateioProductType::Spot {
            "currency_pair"
        } else {
            "contract"
        };
        let page_limit =
            if product_type == GateioProductType::Spot && status.eq_ignore_ascii_case("open") {
                SPOT_OPEN_ORDER_LIMIT
            } else {
                PAGINATION_LIMIT
            };
        if product_type == GateioProductType::Spot
            && status.eq_ignore_ascii_case("open")
            && symbol.is_none()
        {
            return Err(GateioHttpError::Validation(
                "Gate.io spot open-order queries require currency_pair; use open_orders(None) for all pairs"
                    .to_string(),
            ));
        }
        let mut result = Vec::new();
        let mut tracker = PaginationTracker::default();
        for page in 1..=MAX_PAGINATION_PAGES {
            let mut params = vec![
                ("status".to_string(), status.to_string()),
                ("limit".to_string(), page_limit.to_string()),
            ];
            if product_type == GateioProductType::Spot {
                params.push(("page".to_string(), page.to_string()));
            } else {
                params.push((
                    "offset".to_string(),
                    pagination_offset(page, page_limit)?.to_string(),
                ));
            }
            if let Some(symbol) = symbol {
                params.push((symbol_key.to_string(), symbol.to_string()));
            }
            let rows: Vec<GateioOrder> = self
                .send(Method::GET, endpoint, &params, None, true)
                .await?;
            let page_is_complete = rows.len() < page_limit as usize;
            let new_rows = tracker.append_unique(&rows, "orders", &mut result)?;
            if !page_is_complete && new_rows == 0 {
                return Err(GateioHttpError::Validation(
                    "Gate.io order pagination made no progress".to_string(),
                ));
            }
            if page_is_complete {
                break;
            }
        }
        Ok(result)
    }

    pub async fn orders_in_time_range(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<GateioOrder>, GateioHttpError> {
        validate_time_range(start, end, None, "orders")?;
        let mut result = Vec::new();
        let mut tracker = PaginationTracker::default();
        for page in 1..=MAX_PAGINATION_PAGES {
            let (endpoint, mut params) = if product_type == GateioProductType::Spot {
                (
                    SPOT_ORDERS,
                    vec![
                        ("status".to_string(), "finished".to_string()),
                        ("page".to_string(), page.to_string()),
                        ("limit".to_string(), PAGINATION_LIMIT.to_string()),
                    ],
                )
            } else {
                (
                    FUTURES_ORDERS_TIMERANGE,
                    vec![
                        (
                            "offset".to_string(),
                            pagination_offset(page, PAGINATION_LIMIT)?.to_string(),
                        ),
                        ("limit".to_string(), PAGINATION_LIMIT.to_string()),
                    ],
                )
            };
            let symbol_key = if product_type == GateioProductType::Spot {
                "currency_pair"
            } else {
                "contract"
            };
            if let Some(symbol) = symbol {
                params.push((symbol_key.to_string(), symbol.to_string()));
            }
            append_time_range(&mut params, start, end)?;
            let rows: Vec<GateioOrder> = self
                .send(Method::GET, endpoint, &params, None, true)
                .await?;
            let page_is_complete = rows.len() < PAGINATION_LIMIT as usize;
            let new_rows = tracker.append_unique(&rows, "orders in time range", &mut result)?;
            if !page_is_complete && new_rows == 0 {
                return Err(GateioHttpError::Validation(
                    "Gate.io historical-order pagination made no progress".to_string(),
                ));
            }
            if page_is_complete {
                break;
            }
        }
        Ok(result)
    }

    pub async fn user_trades(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioUserTrade>, GateioHttpError> {
        let mut result = Vec::new();
        let mut tracker = PaginationTracker::default();
        for page in 1..=MAX_PAGINATION_PAGES {
            let (endpoint, mut params) = if product_type == GateioProductType::Spot {
                (
                    SPOT_MY_TRADES,
                    vec![
                        ("page".to_string(), page.to_string()),
                        ("limit".to_string(), PAGINATION_LIMIT.to_string()),
                    ],
                )
            } else {
                (
                    FUTURES_MY_TRADES,
                    vec![
                        (
                            "offset".to_string(),
                            pagination_offset(page, PAGINATION_LIMIT)?.to_string(),
                        ),
                        ("limit".to_string(), PAGINATION_LIMIT.to_string()),
                    ],
                )
            };
            let symbol_key = if product_type == GateioProductType::Spot {
                "currency_pair"
            } else {
                "contract"
            };
            if let Some(symbol) = symbol {
                params.push((symbol_key.to_string(), symbol.to_string()));
            }
            let rows: Vec<GateioUserTrade> = self
                .send(Method::GET, endpoint, &params, None, true)
                .await?;
            let page_is_complete = rows.len() < PAGINATION_LIMIT as usize;
            let new_rows = tracker.append_unique(&rows, "user trades", &mut result)?;
            if !page_is_complete && new_rows == 0 {
                return Err(GateioHttpError::Validation(
                    "Gate.io user-trade pagination made no progress".to_string(),
                ));
            }
            if page_is_complete {
                break;
            }
        }
        Ok(result)
    }

    pub async fn user_trades_in_time_range(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<GateioUserTrade>, GateioHttpError> {
        validate_time_range(
            start,
            end,
            (product_type == GateioProductType::Spot).then_some(MAX_SPOT_TRADE_RANGE_SECS),
            "user trades",
        )?;
        let mut result = Vec::new();
        let mut tracker = PaginationTracker::default();
        for page in 1..=MAX_PAGINATION_PAGES {
            let (endpoint, mut params) = if product_type == GateioProductType::Spot {
                (
                    SPOT_MY_TRADES,
                    vec![
                        ("page".to_string(), page.to_string()),
                        ("limit".to_string(), PAGINATION_LIMIT.to_string()),
                    ],
                )
            } else {
                (
                    FUTURES_MY_TRADES_TIMERANGE,
                    vec![
                        (
                            "offset".to_string(),
                            pagination_offset(page, PAGINATION_LIMIT)?.to_string(),
                        ),
                        ("limit".to_string(), PAGINATION_LIMIT.to_string()),
                    ],
                )
            };
            let symbol_key = if product_type == GateioProductType::Spot {
                "currency_pair"
            } else {
                "contract"
            };
            if let Some(symbol) = symbol {
                params.push((symbol_key.to_string(), symbol.to_string()));
            }
            append_time_range(&mut params, start, end)?;
            let rows: Vec<GateioUserTrade> = self
                .send(Method::GET, endpoint, &params, None, true)
                .await?;
            let page_is_complete = rows.len() < PAGINATION_LIMIT as usize;
            let new_rows =
                tracker.append_unique(&rows, "user trades in time range", &mut result)?;
            if !page_is_complete && new_rows == 0 {
                return Err(GateioHttpError::Validation(
                    "Gate.io historical user-trade pagination made no progress".to_string(),
                ));
            }
            if page_is_complete {
                break;
            }
        }
        Ok(result)
    }

    pub async fn submit_order(
        &self,
        request: &GateioOrderRequest,
    ) -> Result<GateioOrder, GateioHttpError> {
        let (endpoint, body) = if request.contract.is_some() {
            (FUTURES_ORDERS, serde_json::to_string(request)?)
        } else {
            (SPOT_ORDERS, serde_json::to_string(request)?)
        };
        self.send(Method::POST, endpoint, &[], Some(body), true)
            .await
    }

    pub async fn amend_order(
        &self,
        product_type: GateioProductType,
        order_id: &str,
        request: &GateioOrderAmendRequest,
        symbol: &str,
    ) -> Result<GateioOrder, GateioHttpError> {
        let endpoint = if product_type == GateioProductType::Spot {
            format!("{SPOT_ORDERS}/{order_id}")
        } else {
            format!("{FUTURES_ORDERS}/{order_id}")
        };
        let mut params = Vec::new();
        if product_type == GateioProductType::Spot {
            params.push(("currency_pair".to_string(), symbol.to_string()));
        } else {
            params.push(("contract".to_string(), symbol.to_string()));
        }
        let method = if product_type == GateioProductType::Spot {
            Method::PATCH
        } else {
            Method::PUT
        };
        self.send(
            method,
            &endpoint,
            &params,
            Some(serde_json::to_string(request)?),
            true,
        )
        .await
    }

    pub async fn cancel_order(
        &self,
        product_type: GateioProductType,
        order_id: &str,
        symbol: &str,
    ) -> Result<GateioOrder, GateioHttpError> {
        let endpoint = if product_type == GateioProductType::Spot {
            format!("{SPOT_ORDERS}/{order_id}")
        } else {
            format!("{FUTURES_ORDERS}/{order_id}")
        };
        let key = if product_type == GateioProductType::Spot {
            "currency_pair"
        } else {
            "contract"
        };
        self.send(
            Method::DELETE,
            &endpoint,
            &[(key.to_string(), symbol.to_string())],
            None,
            true,
        )
        .await
    }

    pub async fn cancel_all(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioOrder>, GateioHttpError> {
        let endpoint = if product_type == GateioProductType::Spot {
            SPOT_ORDERS
        } else {
            FUTURES_ORDERS
        };
        let params = symbol
            .map(|value| {
                vec![(
                    if product_type == GateioProductType::Spot {
                        "currency_pair"
                    } else {
                        "contract"
                    }
                    .to_string(),
                    value.to_string(),
                )]
            })
            .unwrap_or_default();
        self.send(Method::DELETE, endpoint, &params, None, true)
            .await
    }
}

fn append_time_range(
    params: &mut Vec<(String, String)>,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Result<(), GateioHttpError> {
    if let Some(start) = start {
        params.push(("from".to_string(), start.timestamp().to_string()));
    }
    if let Some(end) = end {
        params.push(("to".to_string(), end.timestamp().to_string()));
    }
    Ok(())
}

fn bounded_limit(value: u32, maximum: u32, resource: &str) -> Result<u32, GateioHttpError> {
    if value == 0 {
        return Err(GateioHttpError::Validation(format!(
            "Gate.io {resource} limit must be greater than zero"
        )));
    }
    Ok(value.min(maximum))
}

fn pagination_offset(page: u32, page_limit: u32) -> Result<u32, GateioHttpError> {
    page.checked_sub(1)
        .and_then(|page_index| page_index.checked_mul(page_limit))
        .ok_or_else(|| {
            GateioHttpError::Validation("Gate.io pagination offset overflow".to_string())
        })
}

fn validate_time_range(
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    max_range_secs: Option<i64>,
    resource: &str,
) -> Result<(), GateioHttpError> {
    if let (Some(start), Some(end)) = (start, end) {
        if start >= end {
            return Err(GateioHttpError::Validation(format!(
                "Gate.io {resource} requires start to be before end"
            )));
        }
        if let Some(max_range_secs) = max_range_secs
            && (end - start).num_seconds() > max_range_secs
        {
            return Err(GateioHttpError::Validation(format!(
                "Gate.io {resource} time range cannot exceed {max_range_secs} seconds"
            )));
        }
    } else if let (Some(start), Some(max_range_secs)) = (start, max_range_secs)
        && (Utc::now() - start).num_seconds() > max_range_secs
    {
        return Err(GateioHttpError::Validation(format!(
            "Gate.io {resource} time range starting at {start} cannot exceed {max_range_secs} seconds"
        )));
    }
    Ok(())
}

#[derive(Default)]
struct PaginationTracker {
    page_fingerprints: HashSet<String>,
    record_fingerprints: HashSet<String>,
}

impl PaginationTracker {
    fn append_unique<T: Serialize + Clone>(
        &mut self,
        rows: &[T],
        resource: &str,
        output: &mut Vec<T>,
    ) -> Result<usize, GateioHttpError> {
        let page_fingerprint = serde_json::to_string(rows)?;
        if !self.page_fingerprints.insert(page_fingerprint) {
            return Err(GateioHttpError::Validation(format!(
                "Gate.io {resource} pagination returned a repeated page"
            )));
        }
        let mut new_rows = 0;
        for row in rows {
            let fingerprint = serde_json::to_string(row)?;
            if self.record_fingerprints.insert(fingerprint) {
                output.push(row.clone());
                new_rows += 1;
            }
        }
        Ok(new_rows)
    }
}

fn retry_manager(max_retries: u32) -> RetryManager<GateioHttpError> {
    RetryManager::new(RetryConfig {
        max_retries,
        initial_delay_ms: 250,
        max_delay_ms: 3_000,
        backoff_factor: 2.0,
        jitter_ms: 0,
        operation_timeout_ms: None,
        immediate_first: false,
        max_elapsed_ms: None,
    })
}

fn should_retry(method: &Method, error: &GateioHttpError) -> bool {
    if !matches!(
        *method,
        Method::GET | Method::PATCH | Method::PUT | Method::DELETE
    ) {
        return false;
    }
    match error {
        GateioHttpError::Network(_) => true,
        GateioHttpError::UnexpectedStatus { status, .. } => *status == 429 || *status >= 500,
        _ => false,
    }
}

#[derive(Clone, Debug)]
pub struct GateioHttpClient {
    raw: GateioRawHttpClient,
    instruments: Arc<AtomicMap<Ustr, InstrumentAny>>,
    initialized: Arc<AtomicBool>,
}

impl GateioHttpClient {
    pub fn new(config: &config::GateioDataClientConfig) -> Result<Self, GateioHttpError> {
        Self::new_with_credentials_and_retry(
            config.api_key.clone(),
            config.api_secret.clone(),
            Some(config.http_base_url()),
            config.http_timeout_secs,
            config.proxy_url.clone(),
            config.max_retries,
        )
    }

    pub fn new_with_credentials(
        api_key: Option<String>,
        api_secret: Option<String>,
        base_url_http: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> Result<Self, GateioHttpError> {
        Self::new_with_credentials_and_retry(
            api_key,
            api_secret,
            base_url_http,
            timeout_secs,
            proxy_url,
            3,
        )
    }

    pub fn new_with_credentials_and_retry(
        api_key: Option<String>,
        api_secret: Option<String>,
        base_url_http: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
        max_retries: u32,
    ) -> Result<Self, GateioHttpError> {
        Ok(Self {
            raw: GateioRawHttpClient::new_with_credentials_and_retry(
                api_key,
                api_secret,
                base_url_http,
                timeout_secs,
                proxy_url,
                max_retries,
            )?,
            instruments: Arc::new(AtomicMap::new()),
            initialized: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn raw(&self) -> &GateioRawHttpClient {
        &self.raw
    }

    pub fn cancel_all_requests(&self) {
        self.raw.cancel_all_requests();
    }

    pub fn cache_instruments(&self, values: &[InstrumentAny]) {
        self.instruments.rcu(|cache| {
            for instrument in values {
                cache.insert(instrument.id().symbol.inner(), instrument.clone());
                cache.insert(instrument.raw_symbol().inner(), instrument.clone());
            }
        });
        self.initialized.store(true, Ordering::Release);
    }

    pub fn cached(&self, id: InstrumentId) -> Option<InstrumentAny> {
        self.instruments.get_cloned(&id.symbol.inner())
    }

    pub async fn instruments(
        &self,
        product_type: GateioProductType,
        ts: UnixNanos,
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        let result = match product_type {
            GateioProductType::Spot => self
                .raw
                .spot_pairs()
                .await?
                .into_iter()
                .filter(|item| {
                    item.trade_status
                        .as_deref()
                        .is_none_or(|status| status == "tradable")
                })
                .filter_map(|item| match parse_spot_instrument(&item, ts, ts) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        log::warn!("Skipping invalid Gate.io spot pair {}: {error}", item.id);
                        None
                    }
                })
                .collect::<Vec<InstrumentAny>>(),
            GateioProductType::UsdtPerpetual => self
                .raw
                .contracts()
                .await?
                .into_iter()
                .filter(|item| {
                    item.name.ends_with("_USDT")
                        && item
                            .status
                            .as_deref()
                            .is_none_or(|status| status.eq_ignore_ascii_case("trading"))
                        && !item.in_delisting.unwrap_or(false)
                })
                .filter_map(|item| match parse_perpetual_instrument(&item, ts, ts) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        log::warn!("Skipping invalid Gate.io contract {}: {error}", item.name);
                        None
                    }
                })
                .collect::<Vec<InstrumentAny>>(),
        };
        self.cache_instruments(&result);
        Ok(result)
    }

    pub async fn instrument(
        &self,
        id: InstrumentId,
        product_type: GateioProductType,
        ts: UnixNanos,
    ) -> anyhow::Result<InstrumentAny> {
        if let Some(value) = self.cached(id) {
            return Ok(value);
        }
        anyhow::ensure!(
            GateioProductType::from_symbol(id.symbol.as_str()) == product_type,
            "Gate.io client is configured for {product_type:?}, cannot load {id}"
        );
        let raw = raw_symbol(id);
        let instrument = match product_type {
            GateioProductType::Spot => {
                let pair = self.raw.spot_pair(&raw).await?;
                parse_spot_instrument(&pair, ts, ts)?
            }
            GateioProductType::UsdtPerpetual => {
                let contract = self.raw.contract(&raw).await?;
                parse_perpetual_instrument(&contract, ts, ts)?
            }
        };
        self.cache_instruments(std::slice::from_ref(&instrument));
        Ok(instrument)
    }

    pub async fn instruments_for(
        &self,
        product_type: GateioProductType,
        instrument_ids: Option<&[InstrumentId]>,
        ts: UnixNanos,
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        let Some(instrument_ids) = instrument_ids.filter(|values| !values.is_empty()) else {
            return self.instruments(product_type, ts).await;
        };

        let mut result = Vec::with_capacity(instrument_ids.len());
        for instrument_id in instrument_ids {
            result.push(self.instrument(*instrument_id, product_type, ts).await?);
        }
        self.cache_instruments(&result);
        Ok(result)
    }

    pub async fn order_book(
        &self,
        instrument: &InstrumentAny,
        product_type: GateioProductType,
        limit: Option<u32>,
        ts: UnixNanos,
    ) -> anyhow::Result<OrderBookDeltas> {
        Ok(self
            .order_book_with_sequence(instrument, product_type, limit, ts)
            .await?
            .0)
    }

    pub async fn order_book_with_sequence(
        &self,
        instrument: &InstrumentAny,
        product_type: GateioProductType,
        limit: Option<u32>,
        ts: UnixNanos,
    ) -> anyhow::Result<(OrderBookDeltas, u64)> {
        let symbol = raw_symbol(instrument.id());
        let book = if product_type == GateioProductType::Spot {
            self.raw.spot_order_book(&symbol, limit).await?
        } else {
            self.raw.futures_order_book(&symbol, limit).await?
        };
        let sequence = crate::common::parse::book_snapshot_sequence(&book);
        anyhow::ensure!(
            sequence > 0,
            "Gate.io order-book snapshot missing update id"
        );
        Ok((parse_book(&book, instrument, ts)?, sequence))
    }

    pub async fn trades(
        &self,
        instrument: &InstrumentAny,
        product_type: GateioProductType,
        limit: Option<u32>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        ts: UnixNanos,
    ) -> anyhow::Result<Vec<TradeTick>> {
        let symbol = raw_symbol(instrument.id());
        let rows = if product_type == GateioProductType::Spot {
            self.raw.spot_trades(&symbol, limit, start, end).await?
        } else {
            self.raw.futures_trades(&symbol, limit, start, end).await?
        };
        rows.iter()
            .map(|row| parse_trade(row, instrument, ts))
            .collect()
    }

    pub async fn bars(
        &self,
        instrument: &InstrumentAny,
        product_type: GateioProductType,
        bar_type: BarType,
        limit: Option<u32>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        ts: UnixNanos,
    ) -> anyhow::Result<Vec<Bar>> {
        let interval = match bar_type.spec().aggregation {
            BarAggregation::Minute => format!("{}m", bar_type.spec().step),
            BarAggregation::Hour => format!("{}h", bar_type.spec().step),
            BarAggregation::Day => format!("{}d", bar_type.spec().step),
            _ => anyhow::bail!("Gate.io adapter supports minute, hour, and day bars"),
        };
        let rows = self
            .raw
            .candles(
                product_type,
                &raw_symbol(instrument.id()),
                &interval,
                limit,
                start,
                end,
            )
            .await?;
        rows.iter()
            .map(|row| parse_candle(row, instrument, bar_type, ts))
            .collect()
    }

    pub async fn account_state(
        &self,
        product_type: GateioProductType,
        account_id: nautilus_model::identifiers::AccountId,
        ts: UnixNanos,
    ) -> anyhow::Result<AccountState> {
        self.account_state_with_timestamps(product_type, account_id, ts, ts)
            .await
    }

    pub async fn user_id(&self) -> anyhow::Result<String> {
        let detail = self.raw.account_detail().await?;
        anyhow::ensure!(
            !detail.user_id.trim().is_empty(),
            "Gate.io account detail did not return a user_id"
        );
        Ok(detail.user_id)
    }

    pub async fn account_state_with_timestamps(
        &self,
        product_type: GateioProductType,
        account_id: nautilus_model::identifiers::AccountId,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> anyhow::Result<AccountState> {
        let rows = match product_type {
            GateioProductType::Spot => self.raw.spot_accounts().await?,
            GateioProductType::UsdtPerpetual => vec![self.raw.futures_accounts().await?],
        };
        crate::common::parse::parse_account_state(
            &rows,
            product_type,
            account_id,
            ts_event,
            ts_init,
        )
    }

    pub async fn submit_order(
        &self,
        request: &GateioOrderRequest,
    ) -> Result<GateioOrder, GateioHttpError> {
        self.raw.submit_order(request).await
    }

    pub async fn amend_order(
        &self,
        product_type: GateioProductType,
        order_id: &str,
        request: &GateioOrderAmendRequest,
        symbol: &str,
    ) -> Result<GateioOrder, GateioHttpError> {
        self.raw
            .amend_order(product_type, order_id, request, symbol)
            .await
    }

    pub async fn cancel_order(
        &self,
        product_type: GateioProductType,
        order_id: &str,
        symbol: &str,
    ) -> Result<GateioOrder, GateioHttpError> {
        self.raw.cancel_order(product_type, order_id, symbol).await
    }

    pub async fn cancel_all(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioOrder>, GateioHttpError> {
        self.raw.cancel_all(product_type, symbol).await
    }

    pub async fn order(
        &self,
        product_type: GateioProductType,
        order_id: &str,
        symbol: &str,
    ) -> Result<GateioOrder, GateioHttpError> {
        self.raw.order(product_type, order_id, symbol).await
    }

    pub async fn open_orders(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioOrder>, GateioHttpError> {
        self.raw.open_orders(product_type, symbol).await
    }

    pub async fn orders(
        &self,
        product_type: GateioProductType,
        status: &str,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioOrder>, GateioHttpError> {
        self.raw.orders(product_type, status, symbol).await
    }

    pub async fn orders_in_time_range(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<GateioOrder>, GateioHttpError> {
        self.raw
            .orders_in_time_range(product_type, symbol, start, end)
            .await
    }

    pub async fn user_trades(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioUserTrade>, GateioHttpError> {
        self.raw.user_trades(product_type, symbol).await
    }

    pub async fn user_trades_in_time_range(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<GateioUserTrade>, GateioHttpError> {
        self.raw
            .user_trades_in_time_range(product_type, symbol, start, end)
            .await
    }

    pub async fn positions(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioPosition>, GateioHttpError> {
        if product_type != GateioProductType::UsdtPerpetual {
            return Ok(Vec::new());
        }
        self.raw.futures_positions_for(symbol).await
    }

    pub async fn funding_rates(
        &self,
        instrument: &InstrumentAny,
        limit: Option<u32>,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<nautilus_model::data::FundingRateUpdate>> {
        anyhow::ensure!(
            instrument.id().symbol.as_str().ends_with("-PERP"),
            "Gate.io funding rates require a perpetual instrument"
        );
        let rows = self
            .raw
            .futures_funding_rates(&raw_symbol(instrument.id()), limit)
            .await?;
        rows.iter()
            .map(|row| crate::common::parse::parse_funding_rate(row, instrument, ts_init))
            .collect()
    }
}
