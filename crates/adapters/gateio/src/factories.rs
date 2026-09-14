//! Gate.io client factories.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::ClientId,
};

use crate::{
    common::consts::{GATEIO, GATEIO_VENUE},
    common::enums::GateioProductType,
    config::{GateioDataClientConfig, GateioExecClientConfig},
    data::GateioDataClient,
    execution::GateioExecutionClient,
};

impl ClientConfig for GateioDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for GateioExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Gate.io market data clients.
#[derive(Debug, Clone, Copy, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.gateio", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.gateio")
)]
pub struct GateioDataClientFactory;

impl GateioDataClientFactory {
    /// Creates a new Gate.io data client factory.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for GateioDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<GateioDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for GateioDataClientFactory; expected GateioDataClientConfig"
                )
            })?
            .clone();
        Ok(Box::new(GateioDataClient::new(
            ClientId::from(name),
            config,
        )?))
    }

    fn name(&self) -> &'static str {
        GATEIO
    }

    fn config_type(&self) -> &'static str {
        stringify!(GateioDataClientConfig)
    }
}

/// Factory for creating Gate.io execution clients.
#[derive(Debug, Clone, Copy, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.gateio", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.gateio")
)]
pub struct GateioExecutionClientFactory {}

impl GateioExecutionClientFactory {
    /// Creates a new Gate.io execution client factory.
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

impl ExecutionClientFactory for GateioExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<GateioExecClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for GateioExecutionClientFactory; expected GateioExecClientConfig"
                )
            })?
            .clone();
        let (account_type, oms_type) = match config.product_type {
            GateioProductType::Spot => (AccountType::Cash, OmsType::Hedging),
            GateioProductType::UsdtPerpetual => (AccountType::Margin, OmsType::Netting),
        };
        let core = ExecutionClientCore::new(
            config.trader_id,
            ClientId::from(name),
            *GATEIO_VENUE,
            oms_type,
            config.account_id,
            account_type,
            None,
            cache,
        );
        Ok(Box::new(GateioExecutionClient::new(core, config)?))
    }

    fn name(&self) -> &'static str {
        GATEIO
    }

    fn config_type(&self) -> &'static str {
        stringify!(GateioExecClientConfig)
    }
}
