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

#![cfg(feature = "python")]

use std::{cell::RefCell, rc::Rc};

use nautilus_bitget::{
    common::{
        consts::BITGET,
        enums::{BitgetEnvironment, BitgetProductType},
    },
    config::{BitgetDataClientConfig, BitgetExecClientConfig},
    factories::{BitgetDataClientFactory, BitgetExecutionClientFactory},
    python,
};
use nautilus_common::{cache::Cache, clock::TestClock};
use nautilus_model::{
    enums::OmsType,
    identifiers::{AccountId, ClientId, TraderId},
};
use nautilus_system::get_global_pyo3_registry;
use pyo3::{Py, Python, types::PyModule};
use rstest::rstest;

#[rstest]
fn bitget_python_factories_extract_from_registry() {
    Python::initialize();

    Python::attach(|py| {
        register_bitget_python_module(py);
        assert_data_factory_extracts_from_python_object(py);
        assert_exec_factory_extracts_from_python_object(py);
    });
}

fn register_bitget_python_module(py: Python<'_>) {
    let module = PyModule::new(py, "bitget").expect("Bitget module should be created");
    python::bitget(py, &module).expect("Bitget Python module should register");
}

fn assert_data_factory_extracts_from_python_object(py: Python<'_>) {
    let factory = Py::new(py, BitgetDataClientFactory::new())
        .expect("factory should convert to Python object")
        .into_any();
    let config = Py::new(
        py,
        BitgetDataClientConfig {
            product_type: BitgetProductType::Spot,
            environment: BitgetEnvironment::Mainnet,
            ..BitgetDataClientConfig::default()
        },
    )
    .expect("config should convert to Python object")
    .into_any();
    let registry = get_global_pyo3_registry();

    let extracted_factory = registry
        .extract_factory(py, factory)
        .expect("data factory should extract");
    let extracted_config = registry
        .extract_config(py, config)
        .expect("data config should extract");
    let bitget_config = extracted_config
        .as_any()
        .downcast_ref::<BitgetDataClientConfig>()
        .expect("data config should downcast");
    let cache = Rc::new(RefCell::new(Cache::default()));
    let clock = Rc::new(RefCell::new(TestClock::new()));
    let client = extracted_factory
        .create(
            "BITGET-DATA-EXTRACTED",
            extracted_config.as_ref(),
            cache.into(),
            clock,
        )
        .expect("extracted factory should create data client");

    assert_eq!(extracted_factory.name(), BITGET);
    assert_eq!(extracted_factory.config_type(), "BitgetDataClientConfig");
    assert_eq!(bitget_config.product_type, BitgetProductType::Spot);
    assert_eq!(client.client_id(), ClientId::from("BITGET-DATA-EXTRACTED"));
}

fn assert_exec_factory_extracts_from_python_object(py: Python<'_>) {
    let trader_id = TraderId::from("TRADER-001");
    let account_id = AccountId::from("BITGET-001");
    let factory = Py::new(py, BitgetExecutionClientFactory::new(trader_id, account_id))
        .expect("factory should convert to Python object")
        .into_any();
    let config = Py::new(
        py,
        BitgetExecClientConfig {
            account_id: Some(account_id),
            product_type: BitgetProductType::UsdtFutures,
            environment: BitgetEnvironment::Mainnet,
            api_key: Some("test_key".to_string()),
            api_secret: Some("test_secret".to_string()),
            api_passphrase: Some("test_passphrase".to_string()),
            ..BitgetExecClientConfig::default()
        },
    )
    .expect("config should convert to Python object")
    .into_any();
    let registry = get_global_pyo3_registry();

    let extracted_factory = registry
        .extract_exec_factory(py, factory)
        .expect("exec factory should extract");
    let extracted_config = registry
        .extract_config(py, config)
        .expect("exec config should extract");
    let bitget_config = extracted_config
        .as_any()
        .downcast_ref::<BitgetExecClientConfig>()
        .expect("exec config should downcast");
    let cache = Rc::new(RefCell::new(Cache::default()));
    let client = extracted_factory
        .create(
            "BITGET-EXEC-EXTRACTED",
            extracted_config.as_ref(),
            cache.into(),
        )
        .expect("extracted factory should create exec client");

    assert_eq!(extracted_factory.name(), BITGET);
    assert_eq!(extracted_factory.config_type(), "BitgetExecClientConfig");
    assert_eq!(bitget_config.account_id, Some(account_id));
    assert_eq!(bitget_config.product_type, BitgetProductType::UsdtFutures);
    assert_eq!(client.client_id(), ClientId::from("BITGET-EXEC-EXTRACTED"));
    assert_eq!(client.account_id(), account_id);
    assert_eq!(client.oms_type(), OmsType::Netting);
}
