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

//! Configuration structures for the Bitget adapter.

use nautilus_model::identifiers::AccountId;
use nautilus_network::websocket::TransportBackend;
use serde::{Deserialize, Serialize};

use crate::common::{
    enums::{BitgetEnvironment, BitgetProductType},
    urls::{bitget_http_base_url, bitget_ws_private_url, bitget_ws_public_url},
};

/// Configuration for the Bitget instrument provider.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetInstrumentProviderConfig {
    /// Product type to load instruments for.
    #[builder(default = BitgetProductType::UsdtFutures)]
    pub product_type: BitgetProductType,
    /// Whether inactive instruments should be included when the endpoint returns them.
    #[builder(default)]
    pub include_inactive: bool,
}

impl Default for BitgetInstrumentProviderConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Configuration for the Bitget live data client.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetDataClientConfig {
    /// Optional API key for authenticated REST/WebSocket requests.
    pub api_key: Option<String>,
    /// Optional API secret for authenticated REST/WebSocket requests.
    pub api_secret: Option<String>,
    /// Optional API passphrase for authenticated REST/WebSocket requests.
    pub api_passphrase: Option<String>,
    /// Product type for this data client.
    #[builder(default = BitgetProductType::UsdtFutures)]
    pub product_type: BitgetProductType,
    /// Environment selection.
    #[builder(default = BitgetEnvironment::Mainnet)]
    pub environment: BitgetEnvironment,
    /// Optional override for the REST base URL.
    pub base_url_http: Option<String>,
    /// Optional override for the public WebSocket URL.
    pub base_url_ws_public: Option<String>,
    /// Optional override for the private WebSocket URL.
    pub base_url_ws_private: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// REST timeout in seconds.
    #[builder(default = 60)]
    pub http_timeout_secs: u64,
    /// Maximum retry attempts for REST requests.
    #[builder(default = 3)]
    pub max_retries: u32,
    /// Initial retry backoff in milliseconds.
    #[builder(default = 1_000)]
    pub retry_delay_initial_ms: u64,
    /// Maximum retry backoff in milliseconds.
    #[builder(default = 10_000)]
    pub retry_delay_max_ms: u64,
    /// Heartbeat interval in seconds for WebSocket clients.
    #[builder(default = 30)]
    pub heartbeat_interval_secs: u64,
    /// Interval in minutes for instrument refresh from REST.
    pub update_instruments_interval_mins: Option<u64>,
    /// Interval in seconds for polling instrument definitions and status changes from REST.
    pub instrument_poll_interval_secs: Option<u64>,
    /// WebSocket transport backend.
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for BitgetDataClientConfig {
    fn default() -> Self {
        Self {
            update_instruments_interval_mins: Some(60),
            instrument_poll_interval_secs: Some(60),
            ..Self::builder().build()
        }
    }
}

impl BitgetDataClientConfig {
    /// Creates a configuration with default values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if all Bitget API credential fields are available.
    #[must_use]
    pub fn has_api_credentials(&self) -> bool {
        self.api_key.is_some() && self.api_secret.is_some() && self.api_passphrase.is_some()
    }

    /// Returns the REST base URL, considering overrides and environment.
    #[must_use]
    pub fn http_base_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| bitget_http_base_url(self.environment).to_string())
    }

    /// Returns the public WebSocket URL, considering overrides and environment.
    #[must_use]
    pub fn ws_public_url(&self) -> String {
        self.base_url_ws_public
            .clone()
            .unwrap_or_else(|| bitget_ws_public_url(self.environment).to_string())
    }

    /// Returns the private WebSocket URL, considering overrides and environment.
    #[must_use]
    pub fn ws_private_url(&self) -> String {
        self.base_url_ws_private
            .clone()
            .unwrap_or_else(|| bitget_ws_private_url(self.environment).to_string())
    }
}

/// Configuration for the Bitget live execution client.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetExecClientConfig {
    /// Optional API key for authenticated requests.
    pub api_key: Option<String>,
    /// Optional API secret for authenticated requests.
    pub api_secret: Option<String>,
    /// Optional API passphrase for authenticated requests.
    pub api_passphrase: Option<String>,
    /// Product type for this execution client.
    #[builder(default = BitgetProductType::UsdtFutures)]
    pub product_type: BitgetProductType,
    /// Environment selection.
    #[builder(default = BitgetEnvironment::Mainnet)]
    pub environment: BitgetEnvironment,
    /// Optional override for the REST base URL.
    pub base_url_http: Option<String>,
    /// Optional override for the private WebSocket URL.
    pub base_url_ws_private: Option<String>,
    /// Optional proxy URL for HTTP and WebSocket transports.
    pub proxy_url: Option<String>,
    /// REST timeout in seconds.
    #[builder(default = 60)]
    pub http_timeout_secs: u64,
    /// Maximum retry attempts for REST requests.
    #[builder(default = 3)]
    pub max_retries: u32,
    /// Initial retry backoff in milliseconds.
    #[builder(default = 1_000)]
    pub retry_delay_initial_ms: u64,
    /// Maximum retry backoff in milliseconds.
    #[builder(default = 10_000)]
    pub retry_delay_max_ms: u64,
    /// Heartbeat interval in seconds for WebSocket clients.
    #[builder(default = 30)]
    pub heartbeat_interval_secs: u64,
    /// Optional account identifier to associate with the execution client.
    pub account_id: Option<AccountId>,
    /// Whether uncached instrument executions should be ignored.
    #[builder(default)]
    pub ignore_uncached_instrument_executions: bool,
    /// Lookback window in minutes for private WebSocket reconnect REST reconciliation.
    ///
    /// Open orders and current positions are always fetched without a lookback. This window applies
    /// to historical orders and fills to recover events missed while the WebSocket was disconnected.
    /// Set to `None` to skip historical order/fill reconciliation on reconnect.
    pub reconnect_reconciliation_lookback_mins: Option<u64>,
    /// WebSocket transport backend.
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for BitgetExecClientConfig {
    fn default() -> Self {
        Self {
            reconnect_reconciliation_lookback_mins: Some(60),
            ..Self::builder().build()
        }
    }
}

impl BitgetExecClientConfig {
    /// Creates a configuration with default values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if all Bitget API credential fields are available.
    #[must_use]
    pub fn has_api_credentials(&self) -> bool {
        self.api_key.is_some() && self.api_secret.is_some() && self.api_passphrase.is_some()
    }

    /// Returns the REST base URL, considering overrides and environment.
    #[must_use]
    pub fn http_base_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| bitget_http_base_url(self.environment).to_string())
    }

    /// Returns the private WebSocket URL, considering overrides and environment.
    #[must_use]
    pub fn ws_private_url(&self) -> String {
        self.base_url_ws_private
            .clone()
            .unwrap_or_else(|| bitget_ws_private_url(self.environment).to_string())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn data_config_default() {
        let config = BitgetDataClientConfig::default();

        assert!(!config.has_api_credentials());
        assert_eq!(config.product_type, BitgetProductType::UsdtFutures);
        assert_eq!(config.http_base_url(), "https://api.bitget.com");
        assert_eq!(config.ws_public_url(), "wss://ws.bitget.com/v3/ws/public");
        assert_eq!(config.heartbeat_interval_secs, 30);
    }

    #[rstest]
    fn exec_config_default() {
        let config = BitgetExecClientConfig::default();

        assert!(!config.has_api_credentials());
        assert_eq!(config.product_type, BitgetProductType::UsdtFutures);
        assert_eq!(config.ws_private_url(), "wss://ws.bitget.com/v3/ws/private");
        assert_eq!(config.reconnect_reconciliation_lookback_mins, Some(60));
    }

    #[rstest]
    fn demo_environment_uses_pap_websocket_urls() {
        let data_config = BitgetDataClientConfig {
            environment: BitgetEnvironment::Demo,
            ..Default::default()
        };
        let exec_config = BitgetExecClientConfig {
            environment: BitgetEnvironment::Demo,
            ..Default::default()
        };

        assert_eq!(data_config.http_base_url(), "https://api.bitget.com");
        assert_eq!(
            data_config.ws_public_url(),
            "wss://wspap.bitget.com/v3/ws/public"
        );
        assert_eq!(
            data_config.ws_private_url(),
            "wss://wspap.bitget.com/v3/ws/private"
        );
        assert_eq!(
            exec_config.ws_private_url(),
            "wss://wspap.bitget.com/v3/ws/private"
        );
    }

    #[rstest]
    fn config_toml_round_trips_product_type() {
        let config: BitgetDataClientConfig = toml::from_str(
            r#"
product_type = "SPOT"
http_timeout_secs = 45
"#,
        )
        .unwrap();

        assert_eq!(config.product_type, BitgetProductType::Spot);
        assert_eq!(config.http_timeout_secs, 45);
    }
}
