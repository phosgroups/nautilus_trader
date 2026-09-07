//! Python wrappers for Gate.io URL helpers.

use pyo3::prelude::*;

use crate::{common::enums::GateioProductType, common::urls};

#[pyfunction]
#[pyo3(signature = (base_url = None))]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.gateio")]
pub fn get_gateio_http_base_url(base_url: Option<String>) -> String {
    urls::http_base_url(base_url.as_deref())
}

#[pyfunction]
#[pyo3(signature = (product_type = None, base_url = None))]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.gateio")]
pub fn get_gateio_ws_url(
    product_type: Option<GateioProductType>,
    base_url: Option<String>,
) -> String {
    base_url.unwrap_or_else(|| {
        urls::ws_url(product_type.unwrap_or(GateioProductType::Spot)).to_string()
    })
}
