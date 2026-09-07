use std::{
    collections::HashMap,
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
use serde::de::DeserializeOwned;
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
            GateioAccount, GateioCandle, GateioContract, GateioErrorResponse, GateioFundingRate,
            GateioOrder, GateioOrderAmendRequest, GateioOrderBook, GateioOrderRequest,
            GateioPosition, GateioSpotPair, GateioTrade, GateioUserTrade,
        },
    },
};

const RATE_KEY: &str = "gateio:global";

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

    pub async fn contracts(&self) -> Result<Vec<GateioContract>, GateioHttpError> {
        self.send(Method::GET, FUTURES_CONTRACTS, &[], None, false)
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
            params.push(("limit".to_string(), limit.min(1000).to_string()));
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
            params.push(("limit".to_string(), limit.min(1000).to_string()));
        }
        self.send(Method::GET, FUTURES_ORDER_BOOK, &params, None, false)
            .await
    }

    pub async fn spot_trades(
        &self,
        symbol: &str,
        limit: Option<u32>,
    ) -> Result<Vec<GateioTrade>, GateioHttpError> {
        let mut params = vec![("currency_pair".to_string(), symbol.to_string())];
        if let Some(limit) = limit {
            params.push(("limit".to_string(), limit.min(1000).to_string()));
        }
        self.send(Method::GET, SPOT_TRADES, &params, None, false)
            .await
    }

    pub async fn futures_trades(
        &self,
        contract: &str,
        limit: Option<u32>,
    ) -> Result<Vec<GateioTrade>, GateioHttpError> {
        let mut params = vec![("contract".to_string(), contract.to_string())];
        if let Some(limit) = limit {
            params.push(("limit".to_string(), limit.min(1000).to_string()));
        }
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
            params.push(("limit".to_string(), limit.min(1000).to_string()));
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
        if let Some(limit) = limit {
            params.push(("limit".to_string(), limit.min(1000).to_string()));
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
        let key = if product_type == GateioProductType::Spot {
            "currency_pair"
        } else {
            "contract"
        };
        self.send(
            Method::GET,
            &endpoint,
            &[(key.to_string(), symbol.to_string())],
            None,
            true,
        )
        .await
    }

    pub async fn open_orders(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioOrder>, GateioHttpError> {
        if product_type == GateioProductType::Spot {
            let params = symbol
                .map(|value| vec![("currency_pair".to_string(), value.to_string())])
                .unwrap_or_default();
            return self
                .send(Method::GET, SPOT_OPEN_ORDERS, &params, None, true)
                .await;
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
        let mut params = vec![("status".to_string(), status.to_string())];
        if let Some(symbol) = symbol {
            params.push((symbol_key.to_string(), symbol.to_string()));
        }
        self.send(Method::GET, endpoint, &params, None, true).await
    }

    pub async fn user_trades(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioUserTrade>, GateioHttpError> {
        let endpoint = if product_type == GateioProductType::Spot {
            SPOT_MY_TRADES
        } else {
            FUTURES_MY_TRADES
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
        self.send(Method::GET, endpoint, &params, None, true).await
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
    if !matches!(*method, Method::GET | Method::PATCH | Method::DELETE) {
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
                .filter(|item| item.name.ends_with("_USDT"))
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
        self.instruments(product_type, ts)
            .await?
            .into_iter()
            .find(|value| value.id() == id)
            .ok_or_else(|| anyhow::anyhow!("Gate.io instrument not found: {id}"))
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
        let sequence = crate::common::parse::book_sequence(&book);
        Ok((parse_book(&book, instrument, ts)?, sequence))
    }

    pub async fn trades(
        &self,
        instrument: &InstrumentAny,
        product_type: GateioProductType,
        limit: Option<u32>,
        _start: Option<DateTime<Utc>>,
        _end: Option<DateTime<Utc>>,
        ts: UnixNanos,
    ) -> anyhow::Result<Vec<TradeTick>> {
        let symbol = raw_symbol(instrument.id());
        let rows = if product_type == GateioProductType::Spot {
            self.raw.spot_trades(&symbol, limit).await?
        } else {
            self.raw.futures_trades(&symbol, limit).await?
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
            .candles(product_type, &raw_symbol(instrument.id()), &interval, limit)
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
        let rows = match product_type {
            GateioProductType::Spot => self.raw.spot_accounts().await?,
            GateioProductType::UsdtPerpetual => vec![self.raw.futures_accounts().await?],
        };
        crate::common::parse::parse_account_state(&rows, product_type, account_id, ts, ts)
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

    pub async fn user_trades(
        &self,
        product_type: GateioProductType,
        symbol: Option<&str>,
    ) -> Result<Vec<GateioUserTrade>, GateioHttpError> {
        self.raw.user_trades(product_type, symbol).await
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
