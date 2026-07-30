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
use nautilus_core::{
    UUID4, UnixNanos,
    python::{params::pydict_to_params, to_pyvalue_err},
};
use nautilus_model::{
    data::BarType,
    enums::{OrderSide, OrderType, TimeInForce, TriggerType},
    events::OrderInitialized,
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId},
    instruments::Instrument,
    python::instruments::{instrument_any_to_pyobject, pyobject_to_instrument_any},
    types::{Price, Quantity},
};
use pyo3::{
    conversion::IntoPyObjectExt,
    prelude::*,
    types::{PyDict, PyList},
};

use crate::{
    common::{
        enums::{BitgetEnvironment, BitgetProductType},
        order::{map_cancel_order, map_submit_order},
        parse::{parse_fill_report, parse_order_status_report, parse_position_status_report},
    },
    http::client::{BitgetHttpClient, BitgetRawHttpClient},
};

fn bitget_order_ack_to_pydict(
    py: Python<'_>,
    ack: crate::http::models::BitgetOrderAck,
) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);

    if let Some(order_id) = ack.order_id {
        dict.set_item("order_id", order_id)?;
    }

    if let Some(client_oid) = ack.client_oid {
        dict.set_item("client_oid", client_oid)?;
    }

    if let Some(success) = ack.success {
        dict.set_item("success", success)?;
    }

    if let Some(msg) = ack.msg {
        dict.set_item("msg", msg)?;
    }

    dict.into_py_any(py)
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BitgetRawHttpClient {
    /// Raw HTTP client for low-level Bitget API operations.
    #[new]
    #[pyo3(signature = (
        api_key = None,
        api_secret = None,
        api_passphrase = None,
        environment = BitgetEnvironment::Mainnet,
        base_url = None,
        timeout_secs = 60,
        proxy_url = None,
    ))]
    fn py_new(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        environment: BitgetEnvironment,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> PyResult<Self> {
        Self::new_with_env_for_environment(
            environment,
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
        environment = BitgetEnvironment::Mainnet,
        base_url = None,
        timeout_secs = 60,
        proxy_url = None,
    ))]
    fn py_new(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        environment: BitgetEnvironment,
        base_url: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> PyResult<Self> {
        Self::new_with_env_for_environment(
            environment,
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

    /// Checks if the HTTP client has cached instrument definitions.
    #[pyo3(name = "is_initialized")]
    #[must_use]
    fn py_is_initialized(&self) -> bool {
        self.is_initialized()
    }

    /// Returns symbols currently cached inside the HTTP client.
    #[pyo3(name = "get_cached_symbols")]
    #[must_use]
    fn py_get_cached_symbols(&self) -> Vec<String> {
        self.get_cached_symbols()
    }

    /// Caches a single pyo3 instrument for later instrument-id based requests.
    #[pyo3(name = "cache_instrument")]
    fn py_cache_instrument(&self, py: Python<'_>, instrument: Py<PyAny>) -> PyResult<()> {
        let instrument = pyobject_to_instrument_any(py, instrument)?;
        self.cache_instrument(instrument);
        Ok(())
    }

    /// Caches pyo3 instruments for later instrument-id based requests.
    #[pyo3(name = "cache_instruments")]
    fn py_cache_instruments(&self, py: Python<'_>, instruments: Vec<Py<PyAny>>) -> PyResult<()> {
        let instruments = instruments
            .into_iter()
            .map(|instrument| pyobject_to_instrument_any(py, instrument))
            .collect::<PyResult<Vec<_>>>()?;
        self.cache_instruments(&instruments);
        Ok(())
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
            client.cache_instruments(&instruments);

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
    #[pyo3(signature = (product_type, instrument_id, limit = None, ts_init_ns = None))]
    fn py_request_orderbook_snapshot<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        limit: Option<u32>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = client
            .instrument_from_cache_by_id(instrument_id)
            .map_err(to_pyvalue_err)?;
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
    #[pyo3(signature = (product_type, instrument_id, start = None, end = None, limit = None))]
    fn py_request_trades<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = client
            .instrument_from_cache_by_id(instrument_id)
            .map_err(to_pyvalue_err)?;

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
    #[pyo3(signature = (product_type, instrument_id, start = None, end = None, limit = None))]
    fn py_request_funding_rates<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = client
            .instrument_from_cache_by_id(instrument_id)
            .map_err(to_pyvalue_err)?;

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

    /// Requests historical bars and returns Nautilus bars.
    #[pyo3(name = "request_bars")]
    #[pyo3(signature = (
        product_type,
        instrument_id,
        bar_type,
        start = None,
        end = None,
        limit = None,
        timestamp_on_close = false,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_request_bars<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        bar_type: BarType,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
        timestamp_on_close: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = client
            .instrument_from_cache_by_id(instrument_id)
            .map_err(to_pyvalue_err)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bars = client
                .request_bars(
                    product_type,
                    &instrument,
                    bar_type,
                    start,
                    end,
                    limit,
                    timestamp_on_close,
                )
                .await
                .map_err(to_pyvalue_err)?;

            Python::attach(|py| {
                let py_bars: PyResult<Vec<_>> =
                    bars.into_iter().map(|bar| bar.into_py_any(py)).collect();
                Ok(PyList::new(py, py_bars?)?.into_any().unbind())
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
        instrument_id,
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
        instrument_id: InstrumentId,
        venue_order_id: Option<String>,
        client_order_id: Option<String>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = client
            .instrument_from_cache_by_id(instrument_id)
            .map_err(to_pyvalue_err)?;
        let cached_instrument_id = instrument.id();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let status = client
                .request_order_status(
                    product_type,
                    cached_instrument_id,
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
        instrument_id,
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
        instrument_id: InstrumentId,
        open_only: bool,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = client
            .instrument_from_cache_by_id(instrument_id)
            .map_err(to_pyvalue_err)?;
        let cached_instrument_id = instrument.id();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = client
                .request_order_statuses(
                    product_type,
                    Some(cached_instrument_id),
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
        instrument_id,
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
        instrument_id: InstrumentId,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u32>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = client
            .instrument_from_cache_by_id(instrument_id)
            .map_err(to_pyvalue_err)?;
        let cached_instrument_id = instrument.id();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = client
                .request_fills(product_type, Some(cached_instrument_id), start, end, limit)
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
    #[pyo3(signature = (account_id, product_type, instrument_id, ts_init_ns = None))]
    fn py_request_position_status_reports<'py>(
        &self,
        py: Python<'py>,
        account_id: AccountId,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();
        let instrument = client
            .instrument_from_cache_by_id(instrument_id)
            .map_err(to_pyvalue_err)?;
        let cached_instrument_id = instrument.id();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or_default();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = client
                .request_positions(product_type, Some(cached_instrument_id))
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

    /// Submits an order through Bitget REST using Nautilus domain types.
    #[pyo3(name = "submit_order")]
    #[pyo3(signature = (
        product_type,
        trader_id,
        strategy_id,
        instrument_id,
        client_order_id,
        order_side,
        order_type,
        quantity,
        time_in_force,
        price = None,
        trigger_price = None,
        trigger_type = None,
        post_only = false,
        reduce_only = false,
        quote_quantity = false,
        params = None,
        ts_event_ns = None,
        ts_init_ns = None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_submit_order<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        trader_id: TraderId,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        order_side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        time_in_force: TimeInForce,
        price: Option<Price>,
        trigger_price: Option<Price>,
        trigger_type: Option<TriggerType>,
        post_only: bool,
        reduce_only: bool,
        quote_quantity: bool,
        params: Option<Py<PyDict>>,
        ts_event_ns: Option<u64>,
        ts_init_ns: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = match params.as_ref() {
            Some(dict) => pydict_to_params(py, dict)?,
            None => None,
        };
        let client = self.clone();
        let ts_event = ts_event_ns.map(UnixNanos::from).unwrap_or_default();
        let ts_init = ts_init_ns.map(UnixNanos::from).unwrap_or(ts_event);
        let order_init = OrderInitialized::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            order_side,
            order_type,
            quantity,
            time_in_force,
            post_only,
            reduce_only,
            quote_quantity,
            false,
            UUID4::new(),
            ts_event,
            ts_init,
            price,
            trigger_price,
            trigger_type,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let request =
            map_submit_order(product_type, &order_init, params.as_ref()).map_err(to_pyvalue_err)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ack = client
                .submit_order(&request)
                .await
                .map_err(to_pyvalue_err)?;
            Python::attach(|py| bitget_order_ack_to_pydict(py, ack))
        })
    }

    /// Cancels an order through Bitget REST using Nautilus domain identifiers.
    #[pyo3(name = "cancel_order")]
    #[pyo3(signature = (
        product_type,
        instrument_id,
        client_order_id,
        venue_order_id = None,
        params = None,
    ))]
    fn py_cancel_order<'py>(
        &self,
        py: Python<'py>,
        product_type: BitgetProductType,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: Option<VenueOrderId>,
        params: Option<Py<PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = match params.as_ref() {
            Some(dict) => pydict_to_params(py, dict)?,
            None => None,
        };
        let client = self.clone();
        let request = map_cancel_order(
            product_type,
            instrument_id,
            client_order_id,
            venue_order_id,
            params.as_ref(),
        )
        .map_err(to_pyvalue_err)?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ack = client
                .cancel_order(&request)
                .await
                .map_err(to_pyvalue_err)?;
            Python::attach(|py| bitget_order_ack_to_pydict(py, ack))
        })
    }
}
