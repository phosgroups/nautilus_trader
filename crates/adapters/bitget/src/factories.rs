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

//! Factory functions for creating Bitget clients and components.

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
    identifiers::{AccountId, ClientId, TraderId},
};

use crate::{
    common::{
        consts::{BITGET, BITGET_VENUE},
        enums::BitgetProductType,
    },
    config::{BitgetDataClientConfig, BitgetExecClientConfig},
    data::BitgetDataClient,
    execution::BitgetExecutionClient,
};

impl ClientConfig for BitgetDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for BitgetExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Bitget data clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetDataClientFactory;

impl BitgetDataClientFactory {
    /// Creates a new [`BitgetDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for BitgetDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for BitgetDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let bitget_config = config
            .as_any()
            .downcast_ref::<BitgetDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for BitgetDataClientFactory. Expected BitgetDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client = BitgetDataClient::new(ClientId::from(name), bitget_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        BITGET
    }

    fn config_type(&self) -> &'static str {
        stringify!(BitgetDataClientConfig)
    }
}

/// Factory for creating Bitget execution clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.bitget", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.bitget")
)]
pub struct BitgetExecutionClientFactory {
    trader_id: TraderId,
    account_id: AccountId,
}

impl BitgetExecutionClientFactory {
    /// Creates a new [`BitgetExecutionClientFactory`] instance.
    #[must_use]
    pub const fn new(trader_id: TraderId, account_id: AccountId) -> Self {
        Self {
            trader_id,
            account_id,
        }
    }
}

impl ExecutionClientFactory for BitgetExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let bitget_config = config
            .as_any()
            .downcast_ref::<BitgetExecClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for BitgetExecutionClientFactory. Expected BitgetExecClientConfig, was {config:?}",
                )
            })?
            .clone();

        let (account_type, oms_type) = match bitget_config.product_type {
            BitgetProductType::Spot => (AccountType::Cash, OmsType::Hedging),
            BitgetProductType::UsdtFutures => (AccountType::Margin, OmsType::Netting),
        };

        let account_id = bitget_config.account_id.unwrap_or(self.account_id);

        let core = ExecutionClientCore::new(
            self.trader_id,
            ClientId::from(name),
            *BITGET_VENUE,
            oms_type,
            account_id,
            account_type,
            None,
            cache,
        );

        let client = BitgetExecutionClient::new(core, bitget_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        BITGET
    }

    fn config_type(&self) -> &'static str {
        stringify!(BitgetExecClientConfig)
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::{cache::Cache, factories::ExecutionClientFactory};
    use rstest::rstest;
    use std::{cell::RefCell, rc::Rc};

    use super::*;

    #[rstest]
    fn data_factory_metadata() {
        let factory = BitgetDataClientFactory::new();

        assert_eq!(factory.name(), "BITGET");
        assert_eq!(factory.config_type(), "BitgetDataClientConfig");
    }

    #[rstest]
    fn execution_factory_creates_spot_cash_hedging_client() {
        let cache = Rc::new(RefCell::new(Cache::default())).into();
        let factory = BitgetExecutionClientFactory::new(
            TraderId::from("TRADER-001"),
            AccountId::from("BITGET-001"),
        );
        let config = BitgetExecClientConfig {
            product_type: BitgetProductType::Spot,
            ..Default::default()
        };

        let client = factory.create("BITGET", &config, cache).unwrap();

        assert_eq!(client.account_id(), AccountId::from("BITGET-001"));
        assert_eq!(client.venue(), *BITGET_VENUE);
        assert_eq!(client.oms_type(), OmsType::Hedging);
    }

    #[rstest]
    fn execution_factory_creates_futures_margin_netting_client() {
        let cache = Rc::new(RefCell::new(Cache::default())).into();
        let factory = BitgetExecutionClientFactory::new(
            TraderId::from("TRADER-001"),
            AccountId::from("BITGET-001"),
        );
        let config = BitgetExecClientConfig::default();

        let client = factory.create("BITGET", &config, cache).unwrap();

        assert_eq!(client.account_id(), AccountId::from("BITGET-001"));
        assert_eq!(client.venue(), *BITGET_VENUE);
        assert_eq!(client.oms_type(), OmsType::Netting);
    }
}
