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

//! Python bindings for the Bitget HTTP client.

use chrono::{DateTime, Utc};
use nautilus_core::{UnixNanos, python::to_pyvalue_err};
use nautilus_model::{
    identifiers::AccountId,
    instruments::Instrument,
    python::instruments::{instrument_any_to_pyobject, pyobject_to_instrument_any},
};
use pyo3::{conversion::IntoPyObjectExt, prelude::*, types::PyList};

use crate::{
    common::{
        enums::BitgetProductType,
        parse::{parse_fill_report, parse_order_status_report, parse_position_status_report},
    },
    http::client::{BitgetHttpClient, BitgetRawHttpClient},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BitgetRawHttpClient {
    /// Raw HTTP client for low-level Bitget API operations.
    #[new]
    #[pyo3(signature = (
        api_key = None,
        api_secret = None,
        api_passphrase = None,
        base_url = None,
        timeout_secs = 60,
        proxy_url = None,
    ))]
    fn py_new(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> PyResult<Self> {
        Self::new_with_env(
            api_key,
            api_secret,
            api_passphrase,
            base_url,
            timeout_secs,
            proxy_url,
        )
        .map_err(to_pyvalue_err)
    }

    /// Cancels all pending HTTP requests.
    #[pyo3(name = "cancel_all_requests")]
    fn py_cancel_all_requests(&self) {
        self.cancel_all_requests();
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BitgetHttpClient {
    /// Higher-level Bitget HTTP client.
    #[new]
    #[pyo3(signature = (
        api_key = None,
        api_secret = None,
        api_passphrase = None,
        base_url = None,
        timeout_secs = 60,
        proxy_url = None,
    ))]
    fn py_new(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> PyResult<Self> {
        Self::new_with_env(
            api_key,
            api_secret,
            api_passphrase,
            base_url,
            timeout_secs,
            proxy_url,
        )
        .map_err(to_pyvalue_err)
    }

    /// Requests instruments for a Bitget product type.
    #[pyo3(name = "request_instruments")]
    #[pyo3(signature = (product_type, ts_init_ns = None))]
    fn py_request_instruments<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let instruments = client
                .request_instruments(product_type, ts_init)
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let py_instruments: Vec<Py<PyAny>> = instruments
                    .into_iter()
                    .map(|instrument| instrument_any_to_pyobject(py, instrument))
                    .collect::<PyResult<Vec<_>>>()?;
                Ok(py_instruments.into_py_any(py)?)
            })
        })
    }

    /// Requests an order book snapshot and returns Nautilus order book deltas.
    #[pyo3(name = "request_orderbook_snapshot")]
    #[pyo3(signature = (product_type, instrument, limit = None, ts_init_ns = None))]
    fn py_request_orderbook_snapshot<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        instrument: Py<PyAny>,
        limit: Option<u32>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = pyobject_to_instrument_any(py, instrument)?;
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let deltas = client
                .request_orderbook_snapshot(product_type, &instrument, limit, ts_init)
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| deltas.into_py_any(py))
        })
    }

    /// Requests public market trades and returns Nautilus trade ticks.
    #[pyo3(name = "request_trades")]
    #[pyo3(signature = (product_type, instrument, start = None, end = None, limit = None))]
    fn py_request_trades<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        instrument: Py<PyAny>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = pyobject_to_instrument_any(py, instrument)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let trades = client
                .request_trades(product_type, &instrument, start, end, limit)
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let py_trades: PyResult<Vec<_>> = trades
                    .into_iter()
                    .map(|trade| trade.into_py_any(py))
                    .collect();
                Ok(PyList::new(py, py_trades?)?.into_any().unbind())
            })
        })
    }

    /// Requests historical funding rates and returns Nautilus funding updates.
    #[pyo3(name = "request_funding_rates")]
    #[pyo3(signature = (product_type, instrument, start = None, end = None, limit = None))]
    fn py_request_funding_rates<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        instrument: Py<PyAny>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = pyobject_to_instrument_any(py, instrument)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rates = client
                .request_funding_rates(product_type, &instrument, start, end, limit)
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let py_rates: PyResult<Vec<_>> =
                    rates.into_iter().map(|rate| rate.into_py_any(py)).collect();
                Ok(PyList::new(py, py_rates?)?.into_any().unbind())
            })
        })
    }

    /// Requests the current account state.
    #[pyo3(name = "request_account_state")]
    #[pyo3(signature = (product_type, account_id, ts_init_ns = None))]
    fn py_request_account_state<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        account_id: AccountId,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let account_state = client
                .request_account_state(product_type, account_id, ts_init)
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| account_state.into_py_any(py))
        })
    }

    /// Requests one order status report by venue or client order ID.
    #[pyo3(name = "request_order_status_report")]
    #[pyo3(signature = (
        account_id,
        product_type,
        instrument,
        venue_order_id = None,
        client_order_id = None,
        ts_init_ns = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_request_order_status_report<'py>(
        &self,
        py: Python<'py>,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument: Py<PyAny>,
        venue_order_id: Option<String>,
        client_order_id: Option<String>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = pyobject_to_instrument_any(py, instrument)?;
        let instrument_id = instrument.id();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let status = client
                .request_order_status(
                    product_type,
                    instrument_id,
                    venue_order_id.as_deref(),
                    client_order_id.as_deref(),
                )
                .await
                .map_err(to_pyvalue_err)?;
            let report = parse_order_status_report(&status, &instrument, account_id, ts_init)
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| report.into_py_any(py))
        })
    }

    /// Requests order status reports for one instrument.
    #[pyo3(name = "request_order_status_reports")]
    #[pyo3(signature = (
        account_id,
        product_type,
        instrument,
        open_only = false,
        start = None,
        end = None,
        limit = None,
        ts_init_ns = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_request_order_status_reports<'py>(
        &self,
        py: Python<'py>,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument: Py<PyAny>,
        open_only: bool,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = pyobject_to_instrument_any(py, instrument)?;
        let instrument_id = instrument.id();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = client
                .request_order_statuses(
                    product_type,
                    Some(instrument_id),
                    start,
                    end,
                    open_only,
                    limit,
                )
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let py_reports: PyResult<Vec<_>> = rows
                    .iter()
                    .map(|status| {
                        parse_order_status_report(status, &instrument, account_id, ts_init)
                            .map_err(to_pyvalue_err)?
                            .into_py_any(py)
                    })
                    .collect();
                Ok(PyList::new(py, py_reports?)?.into_any().unbind())
            })
        })
    }

    /// Requests private fill reports for one instrument.
    #[pyo3(name = "request_fill_reports")]
    #[pyo3(signature = (
        account_id,
        product_type,
        instrument,
        start = None,
        end = None,
        limit = None,
        ts_init_ns = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_request_fill_reports<'py>(
        &self,
        py: Python<'py>,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument: Py<PyAny>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = pyobject_to_instrument_any(py, instrument)?;
        let instrument_id = instrument.id();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = client
                .request_fills(product_type, Some(instrument_id), start, end, limit)
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let py_reports: PyResult<Vec<_>> = rows
                    .iter()
                    .map(|fill| {
                        parse_fill_report(fill, &instrument, account_id, ts_init)
                            .map_err(to_pyvalue_err)?
                            .into_py_any(py)
                    })
                    .collect();
                Ok(PyList::new(py, py_reports?)?.into_any().unbind())
            })
        })
    }

    /// Requests position status reports for one instrument.
    #[pyo3(name = "request_position_status_reports")]
    #[pyo3(signature = (account_id, product_type, instrument, ts_init_ns = None))]
    fn py_request_position_status_reports<'py>(
        &self,
        py: Python<'py>,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument: Py<PyAny>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = pyobject_to_instrument_any(py, instrument)?;
        let instrument_id = instrument.id();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = client
                .request_positions(product_type, Some(instrument_id))
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let py_reports: PyResult<Vec<_>> = rows
                    .iter()
                    .map(|position| {
                        parse_position_status_report(position, &instrument, account_id, ts_init)
                            .map_err(to_pyvalue_err)?
                            .into_py_any(py)
                    })
                    .collect();
                Ok(PyList::new(py, py_reports?)?.into_any().unbind())
            })
        })
    }
}
