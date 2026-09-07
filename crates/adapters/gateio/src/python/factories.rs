//! Python constructors for Gate.io factory types.

use pyo3::prelude::*;

use crate::{
    common::consts::GATEIO,
    factories::{GateioDataClientFactory, GateioExecutionClientFactory},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl GateioDataClientFactory {
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        GATEIO
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl GateioExecutionClientFactory {
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        GATEIO
    }
}
