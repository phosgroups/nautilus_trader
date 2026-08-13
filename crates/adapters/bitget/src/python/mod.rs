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

//! Python bindings from `pyo3`.

pub mod config;
pub mod enums;
pub mod factories;
pub mod http;
pub mod urls;
pub mod websocket;

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_model::enums::BarAggregation;
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::{
        consts::BITGET,
        parse::bar_spec_to_bitget_interval,
        symbol::{BitgetSymbol, extract_raw_symbol},
    },
    config::{BitgetDataClientConfig, BitgetExecClientConfig, BitgetInstrumentProviderConfig},
    factories::{BitgetDataClientFactory, BitgetExecutionClientFactory},
};

/// Extracts the raw symbol from a Bitget Nautilus symbol.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.bitget")]
#[pyo3(name = "bitget_extract_raw_symbol")]
fn py_bitget_extract_raw_symbol(symbol: &str) -> &str {
    extract_raw_symbol(symbol)
}

/// Extracts the Bitget product type from a Nautilus symbol.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.bitget")]
#[pyo3(name = "bitget_product_type_from_symbol")]
fn py_bitget_product_type_from_symbol(
    symbol: &str,
) -> PyResult<crate::common::enums::BitgetProductType> {
    let symbol = BitgetSymbol::new(symbol).map_err(to_pyvalue_err)?;
    Ok(symbol.product_type())
}

/// Converts a Nautilus bar aggregation and step to a Bitget candle interval string.
#[pyfunction]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.bitget")]
#[pyo3(name = "bitget_bar_spec_to_interval")]
fn py_bitget_bar_spec_to_interval(aggregation: u8, step: u64) -> PyResult<String> {
    let aggregation = BarAggregation::from_repr(aggregation as usize)
        .ok_or_else(|| to_pyvalue_err(format!("Invalid BarAggregation value: {aggregation}")))?;
    let interval = bar_spec_to_bitget_interval(aggregation, step).map_err(to_pyvalue_err)?;
    Ok(interval.to_string())
}

#[expect(clippy::needless_pass_by_value)]
fn extract_bitget_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<BitgetDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract BitgetDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_bitget_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    match factory.extract::<BitgetExecutionClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract BitgetExecutionClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_bitget_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<BitgetDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract BitgetDataClientConfig: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_bitget_exec_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<BitgetExecClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract BitgetExecClientConfig: {e}"
        ))),
    }
}

/// Loaded as `nautilus_pyo3.bitget`.
///
/// # Errors
///
/// Returns an error if any bindings fail to register with the Python module.
#[pymodule]
pub fn bitget(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<crate::common::enums::BitgetEnvironment>()?;
    m.add_class::<crate::common::enums::BitgetProductType>()?;
    m.add_class::<crate::http::client::BitgetRawHttpClient>()?;
    m.add_class::<crate::http::client::BitgetHttpClient>()?;
    m.add_class::<BitgetInstrumentProviderConfig>()?;
    m.add_class::<BitgetDataClientConfig>()?;
    m.add_class::<BitgetExecClientConfig>()?;
    m.add_class::<BitgetDataClientFactory>()?;
    m.add_class::<BitgetExecutionClientFactory>()?;
    m.add_class::<websocket::PyBitgetWebSocketClient>()?;
    m.add_function(wrap_pyfunction!(urls::py_get_bitget_http_base_url, m)?)?;
    m.add_function(wrap_pyfunction!(urls::py_get_bitget_ws_url_public, m)?)?;
    m.add_function(wrap_pyfunction!(urls::py_get_bitget_ws_url_private, m)?)?;
    m.add_function(wrap_pyfunction!(py_bitget_extract_raw_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(py_bitget_product_type_from_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(py_bitget_bar_spec_to_interval, m)?)?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor(BITGET.to_string(), extract_bitget_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Bitget data factory extractor: {e}"
        )));
    }

    if let Err(e) =
        registry.register_exec_factory_extractor(BITGET.to_string(), extract_bitget_exec_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Bitget exec factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "BitgetDataClientConfig".to_string(),
        extract_bitget_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Bitget data config extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "BitgetExecClientConfig".to_string(),
        extract_bitget_exec_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Bitget exec config extractor: {e}"
        )));
    }

    Ok(())
}
