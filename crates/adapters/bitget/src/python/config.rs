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

//! Python bindings for Bitget configuration.

use nautilus_model::identifiers::AccountId;
use pyo3::pymethods;

use crate::{
    common::enums::{BitgetEnvironment, BitgetProductType},
    config::{BitgetDataClientConfig, BitgetExecClientConfig, BitgetInstrumentProviderConfig},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BitgetInstrumentProviderConfig {
    /// Configuration for Bitget instrument loading.
    #[new]
    #[pyo3(signature = (product_type = None, include_inactive = None))]
    fn py_new(product_type: Option<BitgetProductType>, include_inactive: Option<bool>) -> Self {
        let defaults = Self::default();
        Self {
            product_type: product_type.unwrap_or(defaults.product_type),
            include_inactive: include_inactive.unwrap_or(defaults.include_inactive),
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BitgetDataClientConfig {
    /// Configuration for the Bitget live data client.
    #[new]
    #[pyo3(signature = (
        product_type = None,
        environment = None,
        api_key = None,
        api_secret = None,
        api_passphrase = None,
        base_url_http = None,
        base_url_ws_public = None,
        base_url_ws_private = None,
        proxy_url = None,
        http_timeout_secs = None,
        max_retries = None,
        retry_delay_initial_ms = None,
        retry_delay_max_ms = None,
        heartbeat_interval_secs = None,
        update_instruments_interval_mins = None,
        instrument_poll_interval_secs = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        product_type: Option<BitgetProductType>,
        environment: Option<BitgetEnvironment>,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url_http: Option<String>,
        base_url_ws_public: Option<String>,
        base_url_ws_private: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        heartbeat_interval_secs: Option<u64>,
        update_instruments_interval_mins: Option<u64>,
        instrument_poll_interval_secs: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            api_key,
            api_secret,
            api_passphrase,
            product_type: product_type.unwrap_or(defaults.product_type),
            environment: environment.unwrap_or(defaults.environment),
            base_url_http,
            base_url_ws_public,
            base_url_ws_private,
            proxy_url,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            max_retries: max_retries.unwrap_or(defaults.max_retries),
            retry_delay_initial_ms: retry_delay_initial_ms
                .unwrap_or(defaults.retry_delay_initial_ms),
            retry_delay_max_ms: retry_delay_max_ms.unwrap_or(defaults.retry_delay_max_ms),
            heartbeat_interval_secs: heartbeat_interval_secs
                .unwrap_or(defaults.heartbeat_interval_secs),
            update_instruments_interval_mins: update_instruments_interval_mins
                .or(defaults.update_instruments_interval_mins),
            instrument_poll_interval_secs: instrument_poll_interval_secs
                .or(defaults.instrument_poll_interval_secs),
            transport_backend: defaults.transport_backend,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BitgetExecClientConfig {
    /// Configuration for the Bitget live execution client.
    #[new]
    #[pyo3(signature = (
        product_type = None,
        environment = None,
        api_key = None,
        api_secret = None,
        api_passphrase = None,
        base_url_http = None,
        base_url_ws_private = None,
        proxy_url = None,
        http_timeout_secs = None,
        max_retries = None,
        retry_delay_initial_ms = None,
        retry_delay_max_ms = None,
        heartbeat_interval_secs = None,
        account_id = None,
        ignore_uncached_instrument_executions = None,
        reconnect_reconciliation_lookback_mins = 60,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        product_type: Option<BitgetProductType>,
        environment: Option<BitgetEnvironment>,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url_http: Option<String>,
        base_url_ws_private: Option<String>,
        proxy_url: Option<String>,
        http_timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_initial_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
        heartbeat_interval_secs: Option<u64>,
        account_id: Option<AccountId>,
        ignore_uncached_instrument_executions: Option<bool>,
        reconnect_reconciliation_lookback_mins: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            api_key,
            api_secret,
            api_passphrase,
            product_type: product_type.unwrap_or(defaults.product_type),
            environment: environment.unwrap_or(defaults.environment),
            base_url_http,
            base_url_ws_private,
            proxy_url,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            max_retries: max_retries.unwrap_or(defaults.max_retries),
            retry_delay_initial_ms: retry_delay_initial_ms
                .unwrap_or(defaults.retry_delay_initial_ms),
            retry_delay_max_ms: retry_delay_max_ms.unwrap_or(defaults.retry_delay_max_ms),
            heartbeat_interval_secs: heartbeat_interval_secs
                .unwrap_or(defaults.heartbeat_interval_secs),
            account_id,
            ignore_uncached_instrument_executions: ignore_uncached_instrument_executions
                .unwrap_or(defaults.ignore_uncached_instrument_executions),
            reconnect_reconciliation_lookback_mins,
            transport_backend: defaults.transport_backend,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
