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

//! Python bindings for the Bitget WebSocket client.

use std::sync::{Arc, Mutex};

use ahash::AHashSet;
use nautilus_common::{live::get_runtime, messages::DataEvent};
use nautilus_core::{
    AtomicMap,
    python::{call_python_threadsafe, clone_py_object, params::value_to_pyobject, to_pyvalue_err},
    time::get_atomic_clock_realtime,
};
use nautilus_model::{
    data::{BarType, Data, OrderBookDeltas_API},
    identifiers::InstrumentId,
    instruments::InstrumentAny,
    python::{
        data::data_to_pycapsule,
        instruments::{instrument_any_to_pyobject, pyobject_to_instrument_any},
    },
};
use nautilus_network::websocket::TransportBackend;
use pyo3::{IntoPyObjectExt, prelude::*};
use serde_json::{Value, json};

use crate::{
    common::{
        enums::{BitgetEnvironment, BitgetProductType},
        parse::bar_spec_to_bitget_interval_for_product,
    },
    data::{
        BOOK_SUB_DELTAS, BOOK_SUB_DEPTH10, BitgetBookChecksumState, BitgetBookDepth10State,
        TICKER_SUB_FUNDING, TICKER_SUB_INDEX, TICKER_SUB_MARK, TICKER_SUB_QUOTE,
        emit_depth10_snapshot, get_or_fetch_instrument, handle_bitget_ws_message,
        raw_symbol_for_instrument, send_data, store_spot_book_checksum_snapshot, upsert_instrument,
    },
    http::client::BitgetHttpClient,
    websocket::{
        client::BitgetWebSocketClient,
        messages::{BitgetWsArg, BitgetWsEvent, BitgetWsMessage},
    },
};

/// Python wrapper for the Bitget public/private WebSocket protocol client.
#[pyclass(
    name = "BitgetWebSocketClient",
    module = "nautilus_trader.core.nautilus_pyo3.bitget",
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyBitgetWebSocketClient {
    inner: Arc<tokio::sync::Mutex<BitgetWebSocketClient>>,
    http: BitgetHttpClient,
    product_type: BitgetProductType,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    bar_types: Arc<AtomicMap<String, BarType>>,
    book_sequences: Arc<AtomicMap<InstrumentId, i64>>,
    book_depths: Arc<AtomicMap<InstrumentId, Option<u32>>>,
    book_checksum_states: Arc<AtomicMap<InstrumentId, BitgetBookChecksumState>>,
    book_depth10_states: Arc<AtomicMap<InstrumentId, BitgetBookDepth10State>>,
    book_subs: Arc<AtomicMap<InstrumentId, AHashSet<&'static str>>>,
    ticker_subs: Arc<AtomicMap<InstrumentId, AHashSet<&'static str>>>,
    data_sender: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedSender<DataEvent>>>>,
    url: String,
}

impl PyBitgetWebSocketClient {
    fn wrap(
        client: BitgetWebSocketClient,
        http: BitgetHttpClient,
        product_type: BitgetProductType,
    ) -> Self {
        let url = client.url().to_string();
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(client)),
            http,
            product_type,
            instruments: Arc::new(AtomicMap::new()),
            bar_types: Arc::new(AtomicMap::new()),
            book_sequences: Arc::new(AtomicMap::new()),
            book_depths: Arc::new(AtomicMap::new()),
            book_checksum_states: Arc::new(AtomicMap::new()),
            book_depth10_states: Arc::new(AtomicMap::new()),
            book_subs: Arc::new(AtomicMap::new()),
            ticker_subs: Arc::new(AtomicMap::new()),
            data_sender: Arc::new(Mutex::new(None)),
            url,
        }
    }

    fn configured_product_type_for(&self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let inferred = BitgetProductType::from_symbol(instrument_id.symbol.as_str());
        anyhow::ensure!(
            inferred == self.product_type,
            "Bitget WebSocket client is configured for {:?}, cannot request {}",
            self.product_type,
            instrument_id,
        );
        Ok(())
    }

    fn ensure_futures_ticker_subscription(
        &self,
        instrument_id: InstrumentId,
        data_kind: &str,
    ) -> anyhow::Result<()> {
        self.configured_product_type_for(instrument_id)?;
        anyhow::ensure!(
            self.product_type == BitgetProductType::UsdtFutures,
            "Bitget {data_kind} subscriptions are only available for USDT-FUTURES instruments"
        );

        if let Some(instrument) = self.instruments.get_cloned(&instrument_id) {
            anyhow::ensure!(
                matches!(instrument, InstrumentAny::CryptoPerpetual(_)),
                "Bitget {data_kind} subscriptions are only available for perpetual instruments"
            );
        }

        Ok(())
    }

    fn ensure_ticker_subscription(
        &self,
        instrument_id: InstrumentId,
        data_kind: &str,
    ) -> anyhow::Result<()> {
        self.configured_product_type_for(instrument_id)
            .map_err(|e| anyhow::anyhow!("Bitget {data_kind} subscription: {e}"))?;
        Ok(())
    }

    fn add_ticker_sub(&self, instrument_id: InstrumentId, sub: &'static str) -> bool {
        let mut should_subscribe = false;
        self.ticker_subs.rcu(|m| {
            let entry = m.entry(instrument_id).or_default();
            should_subscribe = entry.is_empty();
            entry.insert(sub);
        });
        should_subscribe
    }

    fn remove_ticker_sub(&self, instrument_id: InstrumentId, sub: &'static str) -> bool {
        let mut should_unsubscribe = false;
        self.ticker_subs.rcu(|m| {
            if let Some(entry) = m.get_mut(&instrument_id) {
                entry.remove(sub);
                should_unsubscribe = entry.is_empty();
                if should_unsubscribe {
                    m.remove(&instrument_id);
                }
            }
        });
        should_unsubscribe
    }

    fn add_book_sub(&self, instrument_id: InstrumentId, sub: &'static str) -> bool {
        let mut should_subscribe = false;
        self.book_subs.rcu(|m| {
            let entry = m.entry(instrument_id).or_default();
            should_subscribe = entry.is_empty();
            entry.insert(sub);
        });
        should_subscribe
    }

    fn remove_book_sub(&self, instrument_id: InstrumentId, sub: &'static str) -> bool {
        let mut should_unsubscribe = false;
        self.book_subs.rcu(|m| {
            if let Some(entry) = m.get_mut(&instrument_id) {
                entry.remove(sub);
                should_unsubscribe = entry.is_empty();
                if should_unsubscribe {
                    m.remove(&instrument_id);
                }
            }
        });
        should_unsubscribe
    }

    fn data_sender(&self) -> Option<tokio::sync::mpsc::UnboundedSender<DataEvent>> {
        self.data_sender.lock().ok().and_then(|guard| guard.clone())
    }

    fn start_python_dispatch(
        &self,
        mut event_rx: tokio::sync::mpsc::UnboundedReceiver<BitgetWsMessage>,
        data_tx: tokio::sync::mpsc::UnboundedSender<DataEvent>,
        mut data_rx: tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
        call_soon: Py<PyAny>,
        callback: Py<PyAny>,
    ) {
        let event_call_soon = clone_py_object(&call_soon);
        let event_callback = clone_py_object(&callback);
        let http = self.http.clone();
        let product_type = self.product_type;
        let instruments = Arc::clone(&self.instruments);
        let bar_types = Arc::clone(&self.bar_types);
        let book_sequences = Arc::clone(&self.book_sequences);
        let book_depths = Arc::clone(&self.book_depths);
        let book_checksum_states = Arc::clone(&self.book_checksum_states);
        let book_depth10_states = Arc::clone(&self.book_depth10_states);
        let book_subs = Arc::clone(&self.book_subs);
        let ticker_subs = Arc::clone(&self.ticker_subs);
        let clock = get_atomic_clock_realtime();

        get_runtime().spawn(async move {
            while let Some(event) = data_rx.recv().await {
                dispatch_data_event_to_python(event, &call_soon, &callback);
            }
            log::debug!("Bitget Python data dispatch task exited");
        });

        get_runtime().spawn(async move {
            while let Some(message) = event_rx.recv().await {
                match message {
                    BitgetWsMessage::Data(_) => {
                        handle_bitget_ws_message(
                            message,
                            &data_tx,
                            &http,
                            product_type,
                            &instruments,
                            &bar_types,
                            &book_sequences,
                            &book_depths,
                            &book_checksum_states,
                            &book_depth10_states,
                            &book_subs,
                            &ticker_subs,
                            clock,
                        )
                        .await;
                    }
                    other => {
                        send_json_to_python(
                            ws_message_to_value(other),
                            &event_call_soon,
                            &event_callback,
                        );
                    }
                }
            }
            log::debug!("Bitget Python WebSocket event dispatch task exited");
        });
    }
}

fn ws_event_to_value(message_type: &str, event: BitgetWsEvent<Value>) -> Value {
    let mut value = serde_json::to_value(event).unwrap_or_else(|e| {
        json!({
            "msg": format!("Failed to serialize Bitget WebSocket event: {e}"),
        })
    });

    if let Value::Object(object) = &mut value {
        object.insert("type".to_string(), json!(message_type));
    }

    value
}

fn ws_message_to_value(message: BitgetWsMessage) -> Value {
    match message {
        BitgetWsMessage::Reconnected => json!({"type": "reconnected"}),
        BitgetWsMessage::Pong => json!({"type": "pong"}),
        BitgetWsMessage::Login(event) => ws_event_to_value("login", event),
        BitgetWsMessage::Subscribe(event) => ws_event_to_value("subscribe", event),
        BitgetWsMessage::Unsubscribe(event) => ws_event_to_value("unsubscribe", event),
        BitgetWsMessage::Error(event) => ws_event_to_value("error", event),
        BitgetWsMessage::Data(event) => ws_event_to_value("data", event),
    }
}

fn send_data_to_python(data: Data, call_soon: &Py<PyAny>, callback: &Py<PyAny>) {
    Python::attach(|py| {
        let py_obj = data_to_pycapsule(py, data);
        call_python_threadsafe(py, call_soon, callback, py_obj);
    });
}

fn send_to_python<T: for<'py> IntoPyObjectExt<'py>>(
    value: T,
    call_soon: &Py<PyAny>,
    callback: &Py<PyAny>,
) {
    Python::attach(|py| match value.into_py_any(py) {
        Ok(py_obj) => call_python_threadsafe(py, call_soon, callback, py_obj),
        Err(e) => log::error!("Failed to convert Bitget event to Python object: {e}"),
    });
}

fn send_json_to_python(value: Value, call_soon: &Py<PyAny>, callback: &Py<PyAny>) {
    Python::attach(|py| match value_to_pyobject(py, &value) {
        Ok(py_obj) => call_python_threadsafe(py, call_soon, callback, py_obj),
        Err(e) => log::error!("Failed to convert Bitget WebSocket event to Python dict: {e}"),
    });
}

fn dispatch_data_event_to_python(event: DataEvent, call_soon: &Py<PyAny>, callback: &Py<PyAny>) {
    match event {
        DataEvent::Data(data) => send_data_to_python(data, call_soon, callback),
        DataEvent::FundingRate(update) => send_to_python(update, call_soon, callback),
        DataEvent::Instrument(instrument) => {
            Python::attach(|py| match instrument_any_to_pyobject(py, instrument) {
                Ok(py_obj) => call_python_threadsafe(py, call_soon, callback, py_obj),
                Err(e) => log::error!("Failed to convert Bitget instrument to Python: {e}"),
            })
        }
        DataEvent::InstrumentStatus(status) => send_to_python(status, call_soon, callback),
        DataEvent::OptionGreeks(greeks) => send_to_python(greeks, call_soon, callback),
        _ => {}
    }
}

#[pymethods]
impl PyBitgetWebSocketClient {
    /// Bitget WebSocket client.
    #[new]
    #[pyo3(signature = (
        product_type = BitgetProductType::UsdtFutures,
        environment = BitgetEnvironment::Mainnet,
        api_key = None,
        api_secret = None,
        api_passphrase = None,
        url = None,
        private = false,
        heartbeat_secs = 30,
        proxy_url = None,
        http_url = None,
        http_timeout_secs = 60,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        product_type: BitgetProductType,
        environment: BitgetEnvironment,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        url: Option<String>,
        private: bool,
        heartbeat_secs: u64,
        proxy_url: Option<String>,
        http_url: Option<String>,
        http_timeout_secs: u64,
    ) -> PyResult<Self> {
        if private {
            Self::new_private(
                product_type,
                environment,
                api_key,
                api_secret,
                api_passphrase,
                url,
                heartbeat_secs,
                proxy_url,
                http_url,
                http_timeout_secs,
            )
        } else {
            Self::new_public(
                product_type,
                environment,
                url,
                heartbeat_secs,
                proxy_url,
                http_url,
                http_timeout_secs,
            )
        }
    }

    /// Creates a public Bitget WebSocket client.
    #[staticmethod]
    #[pyo3(name = "new_public")]
    #[pyo3(signature = (
        product_type = BitgetProductType::UsdtFutures,
        environment = BitgetEnvironment::Mainnet,
        url = None,
        heartbeat_secs = 30,
        proxy_url = None,
        http_url = None,
        http_timeout_secs = 60,
    ))]
    fn new_public(
        product_type: BitgetProductType,
        environment: BitgetEnvironment,
        url: Option<String>,
        heartbeat_secs: u64,
        proxy_url: Option<String>,
        http_url: Option<String>,
        http_timeout_secs: u64,
    ) -> PyResult<Self> {
        let http = BitgetHttpClient::new_with_env_for_environment(
            environment,
            None,
            None,
            None,
            http_url,
            http_timeout_secs,
            proxy_url.clone(),
        )
        .map_err(to_pyvalue_err)?;

        Ok(Self::wrap(
            BitgetWebSocketClient::new_public(
                product_type,
                environment,
                url,
                heartbeat_secs,
                TransportBackend::default(),
                proxy_url,
            ),
            http,
            product_type,
        ))
    }

    /// Creates a private Bitget WebSocket client.
    #[staticmethod]
    #[pyo3(name = "new_private")]
    #[pyo3(signature = (
        product_type = BitgetProductType::UsdtFutures,
        environment = BitgetEnvironment::Mainnet,
        api_key = None,
        api_secret = None,
        api_passphrase = None,
        url = None,
        heartbeat_secs = 30,
        proxy_url = None,
        http_url = None,
        http_timeout_secs = 60,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn new_private(
        product_type: BitgetProductType,
        environment: BitgetEnvironment,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        url: Option<String>,
        heartbeat_secs: u64,
        proxy_url: Option<String>,
        http_url: Option<String>,
        http_timeout_secs: u64,
    ) -> PyResult<Self> {
        let http = BitgetHttpClient::new_with_env_for_environment(
            environment,
            api_key.clone(),
            api_secret.clone(),
            api_passphrase.clone(),
            http_url,
            http_timeout_secs,
            proxy_url.clone(),
        )
        .map_err(to_pyvalue_err)?;

        Ok(Self::wrap(
            BitgetWebSocketClient::new_private(
                product_type,
                environment,
                api_key,
                api_secret,
                api_passphrase,
                url,
                heartbeat_secs,
                TransportBackend::default(),
                proxy_url,
            ),
            http,
            product_type,
        ))
    }

    /// Returns the resolved WebSocket URL.
    #[getter]
    #[pyo3(name = "url")]
    fn py_url(&self) -> String {
        self.url.clone()
    }

    /// Returns `true` when the WebSocket connection is active.
    #[pyo3(name = "is_active")]
    fn py_is_active(&self) -> bool {
        self.inner
            .try_lock()
            .map(|client| client.is_active())
            .unwrap_or(false)
    }

    /// Returns `true` when the WebSocket connection is closed.
    #[pyo3(name = "is_closed")]
    fn py_is_closed(&self) -> bool {
        self.inner
            .try_lock()
            .map(|client| client.is_closed())
            .unwrap_or(false)
    }

    /// Establishes the WebSocket connection.
    #[pyo3(name = "connect")]
    #[pyo3(signature = (loop_=None, callback=None))]
    fn py_connect<'py>(
        &self,
        py: Python<'py>,
        loop_: Option<Py<PyAny>>,
        callback: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let call_soon = match (&loop_, &callback) {
            (Some(loop_), Some(_)) => Some(loop_.getattr(py, "call_soon_threadsafe")?),
            (None, None) => None,
            _ => {
                return Err(to_pyvalue_err(
                    "Bitget WebSocket connect requires both loop_ and callback, or neither",
                ));
            }
        };
        let inner = Arc::clone(&self.inner);
        let this = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let event_rx = {
                let mut client = inner.lock().await;
                client.connect().await.map_err(to_pyvalue_err)?;
                if callback.is_some() {
                    client.take_event_receiver()
                } else {
                    None
                }
            };

            if let (Some(event_rx), Some(call_soon), Some(callback)) =
                (event_rx, call_soon, callback)
            {
                let (data_tx, data_rx) = tokio::sync::mpsc::unbounded_channel();
                if let Ok(mut guard) = this.data_sender.lock() {
                    *guard = Some(data_tx.clone());
                }
                this.start_python_dispatch(event_rx, data_tx, data_rx, call_soon, callback);
            }

            Ok(())
        })
    }

    /// Waits until the WebSocket connection becomes active.
    #[pyo3(name = "wait_until_active")]
    #[pyo3(signature = (timeout_secs = 5.0))]
    fn py_wait_until_active<'py>(
        &self,
        py: Python<'py>,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .wait_until_active(timeout_secs)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Disconnects the WebSocket connection.
    #[pyo3(name = "disconnect")]
    fn py_disconnect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let data_sender = Arc::clone(&self.data_sender);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Ok(mut guard) = data_sender.lock() {
                *guard = None;
            }
            inner
                .lock()
                .await
                .disconnect()
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Alias for `disconnect`.
    #[pyo3(name = "close")]
    fn py_close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.py_disconnect(py)
    }

    /// Returns the number of tracked subscriptions.
    #[pyo3(name = "subscription_count")]
    fn py_subscription_count<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(inner.lock().await.subscription_count().await)
        })
    }

    /// Adds instruments to the shared Rust-native parsing cache.
    #[pyo3(name = "cache_instruments")]
    fn py_cache_instruments(&self, py: Python<'_>, instruments: Vec<Py<PyAny>>) -> PyResult<()> {
        for instrument in instruments {
            let instrument_any = pyobject_to_instrument_any(py, instrument)?;
            upsert_instrument(&self.instruments, instrument_any);
        }
        Ok(())
    }

    /// Adds an instrument to the shared Rust-native parsing cache.
    #[pyo3(name = "cache_instrument")]
    fn py_cache_instrument(&self, py: Python<'_>, instrument: Py<PyAny>) -> PyResult<()> {
        let instrument_any = pyobject_to_instrument_any(py, instrument)?;
        upsert_instrument(&self.instruments, instrument_any);
        Ok(())
    }

    /// Sends a raw text frame.
    #[pyo3(name = "send_text")]
    fn py_send_text<'py>(&self, py: Python<'py>, payload: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .send_text(payload)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Receives the next parsed Bitget WebSocket event.
    #[pyo3(name = "next_event")]
    fn py_next_event<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let event = inner.lock().await.next_event().await;

            Python::attach(|py| match event {
                Some(message) => {
                    let value = ws_message_to_value(message);
                    value_to_pyobject(py, &value)
                }
                None => Ok(py.None()),
            })
        })
    }

    /// Subscribes to ticker updates for a raw Bitget symbol.
    #[pyo3(name = "subscribe_ticker")]
    fn py_subscribe_ticker<'py>(
        &self,
        py: Python<'py>,
        raw_symbol: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_ticker(raw_symbol)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to trades for a raw Bitget symbol.
    #[pyo3(name = "subscribe_trades")]
    fn py_subscribe_trades<'py>(
        &self,
        py: Python<'py>,
        raw_symbol: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_trades(raw_symbol)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to order book deltas for a raw Bitget symbol.
    #[pyo3(name = "subscribe_books")]
    fn py_subscribe_books<'py>(
        &self,
        py: Python<'py>,
        raw_symbol: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_books(raw_symbol)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to order book deltas with Rust-native snapshot seeding.
    #[pyo3(name = "subscribe_book_deltas")]
    #[pyo3(signature = (instrument_id, depth = None))]
    fn py_subscribe_book_deltas<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
        depth: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.configured_product_type_for(instrument_id)
            .map_err(to_pyvalue_err)?;
        self.book_depths.insert(instrument_id, depth);
        let should_subscribe = self.add_book_sub(instrument_id, BOOK_SUB_DELTAS);

        let inner = Arc::clone(&self.inner);
        let http = self.http.clone();
        let instruments = Arc::clone(&self.instruments);
        let book_sequences = Arc::clone(&self.book_sequences);
        let book_checksum_states = Arc::clone(&self.book_checksum_states);
        let product_type = self.product_type;
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let sender = self.data_sender();
        let clock = get_atomic_clock_realtime();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ts_init = clock.get_time_ns();
            let instrument = get_or_fetch_instrument(
                http.clone(),
                instruments,
                product_type,
                instrument_id,
                ts_init,
            )
            .await
            .map_err(to_pyvalue_err)?;
            let (raw_snapshot, snapshot) = crate::data::request_orderbook_snapshot_raw(
                &http,
                product_type,
                &instrument,
                depth,
                ts_init,
            )
            .await
            .map_err(to_pyvalue_err)?;
            store_spot_book_checksum_snapshot(
                product_type,
                instrument_id,
                &raw_snapshot,
                &book_checksum_states,
            )
            .map_err(to_pyvalue_err)?;

            if let Ok(sequence) = i64::try_from(snapshot.sequence) {
                book_sequences.insert(instrument_id, sequence);
            }

            if let Some(sender) = sender.as_ref() {
                send_data(sender, Data::Deltas(OrderBookDeltas_API::new(snapshot)));
            }

            if should_subscribe {
                inner
                    .lock()
                    .await
                    .subscribe_books(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }

            Ok(())
        })
    }

    /// Subscribes to top 10 order book depth with Rust-native snapshot seeding.
    #[pyo3(name = "subscribe_book_depth10")]
    fn py_subscribe_book_depth10<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.configured_product_type_for(instrument_id)
            .map_err(to_pyvalue_err)?;
        let should_subscribe = self.add_book_sub(instrument_id, BOOK_SUB_DEPTH10);

        let inner = Arc::clone(&self.inner);
        let http = self.http.clone();
        let instruments = Arc::clone(&self.instruments);
        let book_sequences = Arc::clone(&self.book_sequences);
        let book_checksum_states = Arc::clone(&self.book_checksum_states);
        let book_depth10_states = Arc::clone(&self.book_depth10_states);
        let product_type = self.product_type;
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let sender = self.data_sender();
        let clock = get_atomic_clock_realtime();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ts_init = clock.get_time_ns();
            let instrument = get_or_fetch_instrument(
                http.clone(),
                instruments,
                product_type,
                instrument_id,
                ts_init,
            )
            .await
            .map_err(to_pyvalue_err)?;
            let (raw_snapshot, snapshot) = crate::data::request_orderbook_snapshot_raw(
                &http,
                product_type,
                &instrument,
                Some(crate::data::BITGET_DEPTH10_DEPTH),
                ts_init,
            )
            .await
            .map_err(to_pyvalue_err)?;
            store_spot_book_checksum_snapshot(
                product_type,
                instrument_id,
                &raw_snapshot,
                &book_checksum_states,
            )
            .map_err(to_pyvalue_err)?;

            if let Ok(sequence) = i64::try_from(snapshot.sequence) {
                book_sequences.insert(instrument_id, sequence);
            }

            if let Some(sender) = sender.as_ref() {
                emit_depth10_snapshot(
                    sender,
                    &instrument,
                    &raw_snapshot,
                    &book_depth10_states,
                    ts_init,
                )
                .map_err(to_pyvalue_err)?;
            }

            if should_subscribe {
                inner
                    .lock()
                    .await
                    .subscribe_books(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }

            Ok(())
        })
    }

    /// Subscribes to candles for a raw Bitget symbol and interval.
    #[pyo3(name = "subscribe_candles")]
    fn py_subscribe_candles<'py>(
        &self,
        py: Python<'py>,
        raw_symbol: String,
        interval: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_candles(raw_symbol, interval)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to candles for a Nautilus bar type using Rust-native interval mapping.
    #[pyo3(name = "subscribe_bars")]
    fn py_subscribe_bars<'py>(
        &self,
        py: Python<'py>,
        bar_type: BarType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let instrument_id = bar_type.instrument_id();
        self.configured_product_type_for(instrument_id)
            .map_err(to_pyvalue_err)?;
        let spec = bar_type.spec();
        let interval = bar_spec_to_bitget_interval_for_product(
            self.product_type,
            spec.aggregation,
            spec.step.get() as u64,
        )
        .map_err(to_pyvalue_err)?;
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let topic_key =
            BitgetWsArg::kline(self.product_type, raw_symbol.clone(), interval).topic_key();
        self.bar_types.insert(topic_key, bar_type);

        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_candles(raw_symbol, interval)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to best bid/ask quote ticks through the Bitget ticker channel.
    #[pyo3(name = "subscribe_quotes")]
    fn py_subscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_ticker_subscription(instrument_id, "quote")
            .map_err(to_pyvalue_err)?;
        let should_subscribe = self.add_ticker_sub(instrument_id, TICKER_SUB_QUOTE);
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_subscribe {
                inner
                    .lock()
                    .await
                    .subscribe_ticker(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Subscribes to mark price updates through the Bitget ticker channel.
    #[pyo3(name = "subscribe_mark_prices")]
    fn py_subscribe_mark_prices<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_futures_ticker_subscription(instrument_id, "mark price")
            .map_err(to_pyvalue_err)?;
        let should_subscribe = self.add_ticker_sub(instrument_id, TICKER_SUB_MARK);
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_subscribe {
                inner
                    .lock()
                    .await
                    .subscribe_ticker(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Subscribes to index price updates through the Bitget ticker channel.
    #[pyo3(name = "subscribe_index_prices")]
    fn py_subscribe_index_prices<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_futures_ticker_subscription(instrument_id, "index price")
            .map_err(to_pyvalue_err)?;
        let should_subscribe = self.add_ticker_sub(instrument_id, TICKER_SUB_INDEX);
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_subscribe {
                inner
                    .lock()
                    .await
                    .subscribe_ticker(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Subscribes to funding rate updates through the Bitget ticker channel.
    #[pyo3(name = "subscribe_funding_rates")]
    fn py_subscribe_funding_rates<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_futures_ticker_subscription(instrument_id, "funding rate")
            .map_err(to_pyvalue_err)?;
        let should_subscribe = self.add_ticker_sub(instrument_id, TICKER_SUB_FUNDING);
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_subscribe {
                inner
                    .lock()
                    .await
                    .subscribe_ticker(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Unsubscribes from order book deltas and clears Rust-native book state.
    #[pyo3(name = "unsubscribe_book_deltas")]
    fn py_unsubscribe_book_deltas<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.configured_product_type_for(instrument_id)
            .map_err(to_pyvalue_err)?;
        let should_unsubscribe = self.remove_book_sub(instrument_id, BOOK_SUB_DELTAS);
        if should_unsubscribe {
            self.book_sequences.remove(&instrument_id);
            self.book_checksum_states.remove(&instrument_id);
            self.book_depth10_states.remove(&instrument_id);
        }
        self.book_depths.remove(&instrument_id);

        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_unsubscribe {
                inner
                    .lock()
                    .await
                    .unsubscribe_books(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Unsubscribes from top 10 order book depth and clears Rust-native depth10 state.
    #[pyo3(name = "unsubscribe_book_depth10")]
    fn py_unsubscribe_book_depth10<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.configured_product_type_for(instrument_id)
            .map_err(to_pyvalue_err)?;
        let should_unsubscribe = self.remove_book_sub(instrument_id, BOOK_SUB_DEPTH10);
        if should_unsubscribe {
            self.book_sequences.remove(&instrument_id);
            self.book_depths.remove(&instrument_id);
            self.book_checksum_states.remove(&instrument_id);
        }
        self.book_depth10_states.remove(&instrument_id);

        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_unsubscribe {
                inner
                    .lock()
                    .await
                    .unsubscribe_books(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Unsubscribes from trade updates.
    #[pyo3(name = "unsubscribe_trade_ticks")]
    fn py_unsubscribe_trade_ticks<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.configured_product_type_for(instrument_id)
            .map_err(to_pyvalue_err)?;
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .unsubscribe_trades(raw_symbol)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Unsubscribes from candles for a Nautilus bar type.
    #[pyo3(name = "unsubscribe_bars")]
    fn py_unsubscribe_bars<'py>(
        &self,
        py: Python<'py>,
        bar_type: BarType,
    ) -> PyResult<Bound<'py, PyAny>> {
        let instrument_id = bar_type.instrument_id();
        self.configured_product_type_for(instrument_id)
            .map_err(to_pyvalue_err)?;
        let spec = bar_type.spec();
        let interval = bar_spec_to_bitget_interval_for_product(
            self.product_type,
            spec.aggregation,
            spec.step.get() as u64,
        )
        .map_err(to_pyvalue_err)?;
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let topic_key =
            BitgetWsArg::kline(self.product_type, raw_symbol.clone(), interval).topic_key();
        self.bar_types.remove(&topic_key);

        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .unsubscribe_candles(raw_symbol, interval)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Unsubscribes from best bid/ask quote ticks.
    #[pyo3(name = "unsubscribe_quotes")]
    fn py_unsubscribe_quotes<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_ticker_subscription(instrument_id, "quote")
            .map_err(to_pyvalue_err)?;
        let should_unsubscribe = self.remove_ticker_sub(instrument_id, TICKER_SUB_QUOTE);
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_unsubscribe {
                inner
                    .lock()
                    .await
                    .unsubscribe_ticker(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Unsubscribes from mark price updates.
    #[pyo3(name = "unsubscribe_mark_prices")]
    fn py_unsubscribe_mark_prices<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_futures_ticker_subscription(instrument_id, "mark price")
            .map_err(to_pyvalue_err)?;
        let should_unsubscribe = self.remove_ticker_sub(instrument_id, TICKER_SUB_MARK);
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_unsubscribe {
                inner
                    .lock()
                    .await
                    .unsubscribe_ticker(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Unsubscribes from index price updates.
    #[pyo3(name = "unsubscribe_index_prices")]
    fn py_unsubscribe_index_prices<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_futures_ticker_subscription(instrument_id, "index price")
            .map_err(to_pyvalue_err)?;
        let should_unsubscribe = self.remove_ticker_sub(instrument_id, TICKER_SUB_INDEX);
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_unsubscribe {
                inner
                    .lock()
                    .await
                    .unsubscribe_ticker(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Unsubscribes from funding rate updates.
    #[pyo3(name = "unsubscribe_funding_rates")]
    fn py_unsubscribe_funding_rates<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_futures_ticker_subscription(instrument_id, "funding rate")
            .map_err(to_pyvalue_err)?;
        let should_unsubscribe = self.remove_ticker_sub(instrument_id, TICKER_SUB_FUNDING);
        let raw_symbol = raw_symbol_for_instrument(instrument_id);
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if should_unsubscribe {
                inner
                    .lock()
                    .await
                    .unsubscribe_ticker(raw_symbol)
                    .await
                    .map_err(to_pyvalue_err)?;
            }
            Ok(())
        })
    }

    /// Subscribes to account updates.
    #[pyo3(name = "subscribe_account")]
    #[pyo3(signature = (coin = None))]
    fn py_subscribe_account<'py>(
        &self,
        py: Python<'py>,
        coin: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_account(coin)
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to private order updates.
    #[pyo3(name = "subscribe_orders")]
    fn py_subscribe_orders<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_orders()
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to private fill updates.
    #[pyo3(name = "subscribe_fills")]
    fn py_subscribe_fills<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_fills()
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to private position updates.
    #[pyo3(name = "subscribe_positions")]
    fn py_subscribe_positions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_positions()
                .await
                .map_err(to_pyvalue_err)
        })
    }

    /// Subscribes to private strategy order updates.
    #[pyo3(name = "subscribe_strategy_orders")]
    fn py_subscribe_strategy_orders<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .lock()
                .await
                .subscribe_strategy_orders()
                .await
                .map_err(to_pyvalue_err)
        })
    }
}
