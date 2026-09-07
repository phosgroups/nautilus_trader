use nautilus_model::identifiers::{AccountId, TraderId};
use nautilus_network::websocket::TransportBackend;
use serde::{Deserialize, Serialize};

use crate::common::{enums::GateioProductType, urls};

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.gateio", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.gateio")
)]
pub struct GateioDataClientConfig {
    #[builder(default = GateioProductType::Spot)]
    pub product_type: GateioProductType,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    pub proxy_url: Option<String>,
    #[builder(default = 60)]
    pub http_timeout_secs: u64,
    #[builder(default = 3)]
    pub max_retries: u32,
    #[builder(default = 10)]
    pub heartbeat_interval_secs: u64,
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for GateioDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl GateioDataClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn has_api_credentials(&self) -> bool {
        crate::common::credential::Credential::resolve(
            self.api_key.clone(),
            self.api_secret.clone(),
        )
        .is_some()
    }

    #[must_use]
    pub fn http_base_url(&self) -> String {
        urls::http_base_url(self.base_url_http.as_deref())
    }

    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| urls::ws_url(self.product_type).to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.gateio", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.gateio")
)]
pub struct GateioExecClientConfig {
    #[builder(default = TraderId::from("TRADER-001"))]
    pub trader_id: TraderId,
    #[builder(default = AccountId::from("GATEIO-001"))]
    pub account_id: AccountId,
    #[builder(default = GateioProductType::Spot)]
    pub product_type: GateioProductType,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    pub proxy_url: Option<String>,
    #[builder(default = 60)]
    pub http_timeout_secs: u64,
    #[builder(default = 3)]
    pub max_retries: u32,
    #[builder(default = 10)]
    pub heartbeat_interval_secs: u64,
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for GateioExecClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl GateioExecClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn has_api_credentials(&self) -> bool {
        crate::common::credential::Credential::resolve(
            self.api_key.clone(),
            self.api_secret.clone(),
        )
        .is_some()
    }

    #[must_use]
    pub fn http_base_url(&self) -> String {
        urls::http_base_url(self.base_url_http.as_deref())
    }

    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| urls::ws_url(self.product_type).to_string())
    }
}
