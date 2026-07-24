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

use std::sync::Arc;

use nautilus_core::python::{params::value_to_pyobject, to_pyvalue_err};
use nautilus_network::websocket::TransportBackend;
use pyo3::prelude::*;
use serde_json::{Value, json};

use crate::{
    common::enums::{BitgetEnvironment, BitgetProductType},
    websocket::{
        client::BitgetWebSocketClient,
        messages::{BitgetWsEvent, BitgetWsMessage},
    },
};

/// Python wrapper for the Bitget public/private WebSocket protocol client.
#[pyclass(
    name = "BitgetWebSocketClient",
    module = "nautilus_trader.core.nautilus_pyo3.bitget",
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub struct PyBitgetWebSocketClient {
    inner: Arc<tokio::sync::Mutex<BitgetWebSocketClient>>,
    url: String,
}

impl PyBitgetWebSocketClient {
    fn wrap(client: BitgetWebSocketClient) -> Self {
        let url = client.url().to_string();
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(client)),
            url,
        }
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
    ) -> Self {
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
            )
        } else {
            Self::new_public(product_type, environment, url, heartbeat_secs, proxy_url)
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
    ))]
    fn new_public(
        product_type: BitgetProductType,
        environment: BitgetEnvironment,
        url: Option<String>,
        heartbeat_secs: u64,
        proxy_url: Option<String>,
    ) -> Self {
        Self::wrap(BitgetWebSocketClient::new_public(
            product_type,
            environment,
            url,
            heartbeat_secs,
            TransportBackend::default(),
            proxy_url,
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
    ) -> Self {
        Self::wrap(BitgetWebSocketClient::new_private(
            product_type,
            environment,
            api_key,
            api_secret,
            api_passphrase,
            url,
            heartbeat_secs,
            TransportBackend::default(),
            proxy_url,
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
    fn py_connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.connect().await.map_err(to_pyvalue_err)
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
