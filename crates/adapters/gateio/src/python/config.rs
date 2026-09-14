//! Python constructors for Gate.io configuration types.

use nautilus_model::identifiers::{AccountId, InstrumentId, TraderId};
use pyo3::prelude::*;

use crate::{
    common::enums::{GateioEnvironment, GateioProductType},
    config::{GateioDataClientConfig, GateioExecClientConfig},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl GateioDataClientConfig {
    #[new]
    #[pyo3(signature = (
        product_type = None,
        environment = None,
        api_key = None,
        api_secret = None,
        instrument_ids = None,
        base_url_http = None,
        base_url_ws = None,
        proxy_url = None,
        http_timeout_secs = None,
        max_retries = None,
        heartbeat_interval_secs = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        product_type: Option<GateioProductType>,
        environment: Option<GateioEnvironment>,
        api_key: Option<String>,
        api_secret: Option<String>,
        instrument_ids: Option<Vec<InstrumentId>>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        heartbeat_interval_secs: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            product_type: product_type.unwrap_or(defaults.product_type),
            environment: environment.unwrap_or(defaults.environment),
            api_key,
            api_secret,
            instrument_ids,
            base_url_http,
            base_url_ws,
            proxy_url,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            max_retries: max_retries.unwrap_or(defaults.max_retries),
            heartbeat_interval_secs: heartbeat_interval_secs
                .unwrap_or(defaults.heartbeat_interval_secs),
            transport_backend: defaults.transport_backend,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    #[getter]
    fn instrument_ids(&self) -> Option<Vec<InstrumentId>> {
        self.instrument_ids.clone()
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl GateioExecClientConfig {
    #[new]
    #[pyo3(signature = (
        trader_id,
        account_id,
        product_type = None,
        environment = None,
        api_key = None,
        api_secret = None,
        instrument_ids = None,
        base_url_http = None,
        base_url_ws = None,
        proxy_url = None,
        http_timeout_secs = None,
        max_retries = None,
        heartbeat_interval_secs = None,
        reconnect_reconciliation_lookback_mins = 60,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        trader_id: TraderId,
        account_id: AccountId,
        product_type: Option<GateioProductType>,
        environment: Option<GateioEnvironment>,
        api_key: Option<String>,
        api_secret: Option<String>,
        instrument_ids: Option<Vec<InstrumentId>>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        heartbeat_interval_secs: Option<u64>,
        reconnect_reconciliation_lookback_mins: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            trader_id,
            account_id,
            product_type: product_type.unwrap_or(defaults.product_type),
            environment: environment.unwrap_or(defaults.environment),
            api_key,
            api_secret,
            instrument_ids,
            base_url_http,
            base_url_ws,
            proxy_url,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            max_retries: max_retries.unwrap_or(defaults.max_retries),
            heartbeat_interval_secs: heartbeat_interval_secs
                .unwrap_or(defaults.heartbeat_interval_secs),
            reconnect_reconciliation_lookback_mins,
            transport_backend: defaults.transport_backend,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    #[getter]
    fn instrument_ids(&self) -> Option<Vec<InstrumentId>> {
        self.instrument_ids.clone()
    }
}
