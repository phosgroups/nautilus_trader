//! Python bindings for the Gate.io adapter.

#![expect(
    clippy::missing_errors_doc,
    reason = "errors are documented on the underlying Rust methods"
)]

pub mod config;
pub mod factories;
pub mod urls;

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::{consts::GATEIO, enums::GateioProductType, symbol::extract_raw_symbol},
    config::{GateioDataClientConfig, GateioExecClientConfig},
    factories::{GateioDataClientFactory, GateioExecutionClientFactory},
};

/// Extracts the raw Gate.io symbol from a Nautilus symbol.
#[pyfunction]
#[pyo3(name = "gateio_extract_raw_symbol")]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.gateio")]
fn py_gateio_extract_raw_symbol(symbol: &str) -> &str {
    extract_raw_symbol(symbol)
}

/// Determines the Gate.io product family from a Nautilus symbol.
#[pyfunction]
#[pyo3(name = "gateio_product_type_from_symbol")]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.gateio")]
fn py_gateio_product_type_from_symbol(symbol: &str) -> GateioProductType {
    GateioProductType::from_symbol(symbol)
}

#[expect(clippy::needless_pass_by_value)]
fn extract_gateio_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    factory
        .extract::<GateioDataClientFactory>(py)
        .map(|factory| Box::new(factory) as Box<dyn DataClientFactory>)
        .map_err(|error| {
            to_pyvalue_err(format!(
                "Failed to extract GateioDataClientFactory: {error}"
            ))
        })
}

#[expect(clippy::needless_pass_by_value)]
fn extract_gateio_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    factory
        .extract::<GateioExecutionClientFactory>(py)
        .map(|factory| Box::new(factory) as Box<dyn ExecutionClientFactory>)
        .map_err(|error| {
            to_pyvalue_err(format!(
                "Failed to extract GateioExecutionClientFactory: {error}"
            ))
        })
}

#[expect(clippy::needless_pass_by_value)]
fn extract_gateio_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    config
        .extract::<GateioDataClientConfig>(py)
        .map(|config| Box::new(config) as Box<dyn ClientConfig>)
        .map_err(|error| {
            to_pyvalue_err(format!("Failed to extract GateioDataClientConfig: {error}"))
        })
}

#[expect(clippy::needless_pass_by_value)]
fn extract_gateio_exec_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    config
        .extract::<GateioExecClientConfig>(py)
        .map(|config| Box::new(config) as Box<dyn ClientConfig>)
        .map_err(|error| {
            to_pyvalue_err(format!("Failed to extract GateioExecClientConfig: {error}"))
        })
}

/// Loaded as `nautilus_trader._libnautilus.gateio`.
#[pymodule]
pub fn gateio(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(GATEIO, GATEIO)?;
    m.add_class::<GateioProductType>()?;
    m.add_class::<GateioDataClientConfig>()?;
    m.add_class::<GateioExecClientConfig>()?;
    m.add_class::<GateioDataClientFactory>()?;
    m.add_class::<GateioExecutionClientFactory>()?;
    m.add_function(wrap_pyfunction!(py_gateio_extract_raw_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(py_gateio_product_type_from_symbol, m)?)?;
    m.add_function(wrap_pyfunction!(urls::get_gateio_http_base_url, m)?)?;
    m.add_function(wrap_pyfunction!(urls::get_gateio_ws_url, m)?)?;

    let registry = get_global_pyo3_registry();
    registry
        .register_factory_extractor(GATEIO.to_string(), extract_gateio_data_factory)
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register Gate.io data factory extractor: {error}"
            ))
        })?;
    registry
        .register_exec_factory_extractor(GATEIO.to_string(), extract_gateio_exec_factory)
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register Gate.io execution factory extractor: {error}"
            ))
        })?;
    registry
        .register_config_extractor(
            "GateioDataClientConfig".to_string(),
            extract_gateio_data_config,
        )
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register Gate.io data config extractor: {error}"
            ))
        })?;
    registry
        .register_config_extractor(
            "GateioExecClientConfig".to_string(),
            extract_gateio_exec_config,
        )
        .map_err(|error| {
            to_pyruntime_err(format!(
                "Failed to register Gate.io execution config extractor: {error}"
            ))
        })?;
    Ok(())
}
