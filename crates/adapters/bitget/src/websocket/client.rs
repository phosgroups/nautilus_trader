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

//! Outer Bitget WebSocket client.

use std::{
    collections::BTreeMap,
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use chrono::Utc;
use nautilus_common::live::get_runtime;
use nautilus_network::{
    mode::ConnectionMode,
    websocket::{
        AUTHENTICATION_TIMEOUT_SECS, AuthTracker, TEXT_PING, TransportBackend, WebSocketClient,
        WebSocketConfig, channel_message_handler,
    },
};

use crate::{
    common::{
        credential::Credential,
        enums::{BitgetEnvironment, BitgetProductType},
        urls::{bitget_ws_private_url, bitget_ws_public_url},
    },
    websocket::{
        error::{BitgetWsError, BitgetWsResult},
        handler::{BitgetWsFeedHandler, HandlerCommand},
        messages::{BitgetWsArg, BitgetWsCommand, BitgetWsLoginArg, BitgetWsMessage},
    },
};

const RECONNECT_TIMEOUT_MS: u64 = 15_000;
const RECONNECT_DELAY_INITIAL_MS: u64 = 500;
const RECONNECT_DELAY_MAX_MS: u64 = 5_000;
const RECONNECT_BACKOFF_FACTOR: f64 = 1.5;
const RECONNECT_JITTER_MS: u64 = 250;
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Bitget public/private WebSocket client.
pub struct BitgetWebSocketClient {
    url: String,
    product_type: BitgetProductType,
    credential: Option<Credential>,
    requires_auth: bool,
    auth_tracker: AuthTracker,
    heartbeat: Option<u64>,
    connection_mode: Arc<ArcSwap<AtomicU8>>,
    cmd_tx: Arc<tokio::sync::RwLock<tokio::sync::mpsc::UnboundedSender<HandlerCommand>>>,
    out_rx: Option<tokio::sync::mpsc::UnboundedReceiver<BitgetWsMessage>>,
    signal: Arc<AtomicBool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    subscriptions: Arc<tokio::sync::RwLock<BTreeMap<String, BitgetWsArg>>>,
    transport_backend: TransportBackend,
    proxy_url: Option<String>,
}

impl Debug for BitgetWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BitgetWebSocketClient))
            .field("url", &self.url)
            .field("product_type", &self.product_type)
            .field("requires_auth", &self.requires_auth)
            .field("heartbeat", &self.heartbeat)
            .field("is_active", &self.is_active())
            .field("transport_backend", &self.transport_backend)
            .field("proxy_url", &self.proxy_url)
            .finish_non_exhaustive()
    }
}

impl Clone for BitgetWebSocketClient {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            product_type: self.product_type,
            credential: self.credential.clone(),
            requires_auth: self.requires_auth,
            auth_tracker: self.auth_tracker.clone(),
            heartbeat: self.heartbeat,
            connection_mode: Arc::clone(&self.connection_mode),
            cmd_tx: Arc::clone(&self.cmd_tx),
            out_rx: None,
            signal: Arc::clone(&self.signal),
            task_handle: None,
            subscriptions: Arc::clone(&self.subscriptions),
            transport_backend: self.transport_backend,
            proxy_url: self.proxy_url.clone(),
        }
    }
}

impl BitgetWebSocketClient {
    /// Creates a public WebSocket client for the selected product type.
    #[must_use]
    pub fn new_public(
        product_type: BitgetProductType,
        environment: BitgetEnvironment,
        url: Option<String>,
        heartbeat_secs: u64,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        Self::new(
            product_type,
            url.unwrap_or_else(|| bitget_ws_public_url(environment).to_string()),
            None,
            false,
            heartbeat_secs,
            transport_backend,
            proxy_url,
        )
    }

    /// Creates a private WebSocket client for the selected product type.
    ///
    /// Missing credential values are resolved from `BITGET_API_KEY`, `BITGET_API_SECRET`, and
    /// `BITGET_API_PASSPHRASE`.
    #[must_use]
    pub fn new_private(
        product_type: BitgetProductType,
        environment: BitgetEnvironment,
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
        url: Option<String>,
        heartbeat_secs: u64,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        let credential = Credential::resolve(api_key, api_secret, api_passphrase);

        Self::new(
            product_type,
            url.unwrap_or_else(|| bitget_ws_private_url(environment).to_string()),
            credential,
            true,
            heartbeat_secs,
            transport_backend,
            proxy_url,
        )
    }

    fn new(
        product_type: BitgetProductType,
        url: String,
        credential: Option<Credential>,
        requires_auth: bool,
        heartbeat_secs: u64,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        let (placeholder_tx, _) = tokio::sync::mpsc::unbounded_channel();

        Self {
            url,
            product_type,
            credential,
            requires_auth,
            auth_tracker: AuthTracker::new(),
            heartbeat: Some(heartbeat_secs),
            connection_mode: Arc::new(ArcSwap::from_pointee(AtomicU8::new(
                ConnectionMode::Closed.as_u8(),
            ))),
            cmd_tx: Arc::new(tokio::sync::RwLock::new(placeholder_tx)),
            out_rx: None,
            signal: Arc::new(AtomicBool::new(false)),
            task_handle: None,
            subscriptions: Arc::new(tokio::sync::RwLock::new(BTreeMap::new())),
            transport_backend,
            proxy_url,
        }
    }

    /// Returns the resolved WebSocket URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns `true` when the underlying connection is active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.connection_mode.load().load(Ordering::SeqCst) == ConnectionMode::Active.as_u8()
            && !self.signal.load(Ordering::Relaxed)
    }

    /// Returns `true` when the client is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.connection_mode.load().load(Ordering::SeqCst) == ConnectionMode::Closed.as_u8()
            || self.signal.load(Ordering::Relaxed)
    }

    /// Waits until the WebSocket becomes active.
    ///
    /// # Errors
    ///
    /// Returns an error if the client does not become active within `timeout_secs`.
    pub async fn wait_until_active(&self, timeout_secs: f64) -> BitgetWsResult<()> {
        let timeout = Duration::from_secs_f64(timeout_secs);

        tokio::time::timeout(timeout, async {
            while !self.is_active() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            BitgetWsError::Client(format!(
                "Bitget WebSocket connection timeout after {timeout_secs} seconds"
            ))
        })
    }

    /// Establishes the WebSocket connection and spawns the feed-handler task.
    ///
    /// # Errors
    ///
    /// Returns an error if the network connection cannot be established or private login fails.
    pub async fn connect(&mut self) -> BitgetWsResult<()> {
        if self.is_active() {
            log::warn!("Bitget WebSocket already connected: {}", self.url);
            return Ok(());
        }

        self.signal.store(false, Ordering::Relaxed);

        let (message_handler, raw_rx) = channel_message_handler();
        let config = WebSocketConfig {
            url: self.url.clone(),
            headers: vec![],
            heartbeat: self.heartbeat,
            heartbeat_msg: Some(TEXT_PING.to_string()),
            reconnect_timeout_ms: Some(RECONNECT_TIMEOUT_MS),
            reconnect_delay_initial_ms: Some(RECONNECT_DELAY_INITIAL_MS),
            reconnect_delay_max_ms: Some(RECONNECT_DELAY_MAX_MS),
            reconnect_backoff_factor: Some(RECONNECT_BACKOFF_FACTOR),
            reconnect_jitter_ms: Some(RECONNECT_JITTER_MS),
            reconnect_max_attempts: None,
            idle_timeout_ms: self.heartbeat.map(|secs| (secs + 10) * 1_000),
            backend: self.transport_backend,
            proxy_url: self.proxy_url.clone(),
        };

        let client =
            WebSocketClient::connect(config, Some(message_handler), None, None, vec![], None)
                .await?;
        let connection_mode = client.connection_mode_atomic();
        client.set_auth_tracker(self.auth_tracker.clone(), self.requires_auth);

        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<HandlerCommand>();
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<BitgetWsMessage>();

        cmd_tx
            .send(HandlerCommand::SetClient(client))
            .map_err(|e| BitgetWsError::Send(format!("Failed to initialize handler: {e:?}")))?;

        *self.cmd_tx.write().await = cmd_tx.clone();
        self.out_rx = Some(out_rx);
        self.connection_mode.store(connection_mode);

        let signal = Arc::clone(&self.signal);
        let auth_tracker = self.auth_tracker.clone();
        let subscriptions = Arc::clone(&self.subscriptions);
        let credential = self.credential.clone();
        let requires_auth = self.requires_auth;
        let cmd_tx_for_reconnect = cmd_tx.clone();

        let task = get_runtime().spawn(async move {
            let mut handler = BitgetWsFeedHandler::new(
                Arc::clone(&signal),
                cmd_rx,
                raw_rx,
                out_tx,
                auth_tracker.clone(),
            );

            loop {
                match handler.next().await {
                    Some(BitgetWsMessage::Reconnected) => {
                        log::info!("Bitget WebSocket reconnected");
                        auth_tracker.invalidate();

                        if requires_auth {
                            if let Some(credential) = &credential {
                                let _rx = auth_tracker.begin();
                                match login_payload(credential) {
                                    Ok(payload) => {
                                        if let Err(e) = cmd_tx_for_reconnect
                                            .send(HandlerCommand::Login { payload })
                                        {
                                            log::error!(
                                                "Failed to queue Bitget WebSocket re-login: {e:?}"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        auth_tracker.fail(e.to_string());
                                        log::error!(
                                            "Failed to create Bitget re-login payload: {e}"
                                        );
                                    }
                                }
                            } else {
                                auth_tracker.fail("Bitget credentials are missing");
                                log::error!(
                                    "Cannot re-authenticate Bitget WebSocket: missing credentials"
                                );
                            }
                        } else if let Err(e) =
                            replay_subscriptions(&subscriptions, &cmd_tx_for_reconnect).await
                        {
                            log::error!("Failed to replay Bitget subscriptions: {e}");
                        }

                        if handler.send(BitgetWsMessage::Reconnected).is_err() {
                            if handler.is_stopped() {
                                log::debug!("Bitget WebSocket receiver dropped during shutdown");
                            } else {
                                log::error!("Bitget WebSocket receiver dropped after reconnect");
                            }
                            break;
                        }
                    }
                    Some(msg @ BitgetWsMessage::Login(_)) => {
                        if msg.is_login_success()
                            && let Err(e) =
                                replay_subscriptions(&subscriptions, &cmd_tx_for_reconnect).await
                        {
                            log::error!("Failed to replay Bitget subscriptions after login: {e}");
                        }

                        if handler.send(msg).is_err() {
                            if handler.is_stopped() {
                                log::debug!("Bitget WebSocket receiver dropped during shutdown");
                            } else {
                                log::error!("Bitget WebSocket receiver dropped");
                            }
                            break;
                        }
                    }
                    Some(msg) => {
                        if handler.send(msg).is_err() {
                            if handler.is_stopped() {
                                log::debug!("Bitget WebSocket receiver dropped during shutdown");
                            } else {
                                log::error!("Bitget WebSocket receiver dropped");
                            }
                            break;
                        }
                    }
                    None => {
                        if handler.is_stopped() {
                            log::debug!("Bitget WebSocket handler stopped");
                        } else {
                            log::warn!("Bitget WebSocket stream ended unexpectedly");
                        }
                        break;
                    }
                }
            }
        });

        self.task_handle = Some(task);

        if self.requires_auth {
            self.authenticate_if_required().await?;
        }

        Ok(())
    }

    /// Disconnects the WebSocket client and stops the handler task.
    ///
    /// # Errors
    ///
    /// This function currently completes best-effort shutdown and returns `Ok(())`.
    pub async fn disconnect(&mut self) -> BitgetWsResult<()> {
        self.signal.store(true, Ordering::Release);

        if let Err(e) = self.cmd_tx.read().await.send(HandlerCommand::Disconnect) {
            log::debug!("Failed to queue Bitget WebSocket disconnect: {e:?}");
        }

        if let Some(handle) = self.task_handle.take() {
            let abort_handle = handle.abort_handle();
            tokio::select! {
                result = handle => match result {
                    Ok(()) => log::debug!("Bitget WebSocket handler task completed"),
                    Err(e) if e.is_cancelled() => log::debug!("Bitget WebSocket handler task cancelled"),
                    Err(e) => log::error!("Bitget WebSocket handler task error: {e:?}"),
                },
                () = tokio::time::sleep(DISCONNECT_TIMEOUT) => {
                    log::warn!("Timeout waiting for Bitget WebSocket handler task, aborting");
                    abort_handle.abort();
                }
            }
        }

        self.connection_mode
            .store(Arc::new(AtomicU8::new(ConnectionMode::Closed.as_u8())));
        self.auth_tracker.invalidate();
        Ok(())
    }

    /// Receives the next parsed Bitget WebSocket message.
    pub async fn next_event(&mut self) -> Option<BitgetWsMessage> {
        if let Some(rx) = self.out_rx.as_mut() {
            rx.recv().await
        } else {
            None
        }
    }

    /// Takes the event receiver, leaving the client usable for command sends.
    #[must_use]
    pub fn take_event_receiver(
        &mut self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<BitgetWsMessage>> {
        self.out_rx.take()
    }

    /// Returns the number of tracked subscriptions that will be replayed on reconnect.
    pub async fn subscription_count(&self) -> usize {
        self.subscriptions.read().await.len()
    }

    /// Sends an arbitrary text frame to Bitget.
    ///
    /// # Errors
    ///
    /// Returns an error if the handler command cannot be queued.
    pub async fn send_text(&self, payload: String) -> BitgetWsResult<()> {
        self.send_cmd(HandlerCommand::SendText { payload }).await
    }

    /// Subscribes to one or more Bitget topic arguments.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be serialized or sent.
    pub async fn subscribe(&self, args: Vec<BitgetWsArg>) -> BitgetWsResult<()> {
        self.wait_for_auth_if_required().await?;

        let mut to_send = Vec::new();
        {
            let mut subscriptions = self.subscriptions.write().await;
            for arg in args {
                let key = arg.topic_key();
                if let std::collections::btree_map::Entry::Vacant(entry) = subscriptions.entry(key)
                {
                    entry.insert(arg.clone());
                    to_send.push(arg);
                }
            }
        }

        if to_send.is_empty() {
            return Ok(());
        }

        let payload = subscribe_payload(to_send)?;
        self.send_cmd(HandlerCommand::Subscribe { payload }).await
    }

    /// Unsubscribes from one or more Bitget channel arguments.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be serialized or sent.
    pub async fn unsubscribe(&self, args: Vec<BitgetWsArg>) -> BitgetWsResult<()> {
        let mut to_send = Vec::new();
        {
            let mut subscriptions = self.subscriptions.write().await;
            for arg in args {
                let key = arg.topic_key();
                if subscriptions.remove(&key).is_some() {
                    to_send.push(arg);
                }
            }
        }

        if to_send.is_empty() {
            return Ok(());
        }

        let command = BitgetWsCommand::unsubscribe(to_send)?;
        let payload = serde_json::to_string(&command)?;
        self.send_cmd(HandlerCommand::Unsubscribe { payload }).await
    }

    /// Subscribes to the ticker channel for a raw Bitget symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent.
    pub async fn subscribe_ticker(&self, raw_symbol: impl Into<String>) -> BitgetWsResult<()> {
        self.subscribe(vec![self.arg("ticker", Some(raw_symbol.into()))])
            .await
    }

    /// Unsubscribes from the ticker channel for a raw Bitget symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscribe command cannot be sent.
    pub async fn unsubscribe_ticker(&self, raw_symbol: impl Into<String>) -> BitgetWsResult<()> {
        self.unsubscribe(vec![self.arg("ticker", Some(raw_symbol.into()))])
            .await
    }

    /// Subscribes to the public trade topic for a raw Bitget symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent.
    pub async fn subscribe_trades(&self, raw_symbol: impl Into<String>) -> BitgetWsResult<()> {
        self.subscribe(vec![self.arg("publicTrade", Some(raw_symbol.into()))])
            .await
    }

    /// Unsubscribes from the public trade topic for a raw Bitget symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscribe command cannot be sent.
    pub async fn unsubscribe_trades(&self, raw_symbol: impl Into<String>) -> BitgetWsResult<()> {
        self.unsubscribe(vec![self.arg("publicTrade", Some(raw_symbol.into()))])
            .await
    }

    /// Subscribes to the order book channel for a raw Bitget symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent.
    pub async fn subscribe_books(&self, raw_symbol: impl Into<String>) -> BitgetWsResult<()> {
        self.subscribe(vec![self.arg("books", Some(raw_symbol.into()))])
            .await
    }

    /// Unsubscribes from the order book channel for a raw Bitget symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscribe command cannot be sent.
    pub async fn unsubscribe_books(&self, raw_symbol: impl Into<String>) -> BitgetWsResult<()> {
        self.unsubscribe(vec![self.arg("books", Some(raw_symbol.into()))])
            .await
    }

    /// Subscribes to the v3 UTA kline topic for a raw Bitget symbol.
    ///
    /// Pass intervals such as `"1m"` or `"1H"`.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent.
    pub async fn subscribe_candles(
        &self,
        raw_symbol: impl Into<String>,
        interval: impl AsRef<str>,
    ) -> BitgetWsResult<()> {
        self.subscribe(vec![BitgetWsArg::kline(
            self.product_type,
            raw_symbol.into(),
            interval.as_ref().to_string(),
        )])
        .await
    }

    /// Unsubscribes from a candlestick channel for a raw Bitget symbol.
    ///
    /// # Errors
    ///
    /// Returns an error if the unsubscribe command cannot be sent.
    pub async fn unsubscribe_candles(
        &self,
        raw_symbol: impl Into<String>,
        interval: impl AsRef<str>,
    ) -> BitgetWsResult<()> {
        self.unsubscribe(vec![BitgetWsArg::kline(
            self.product_type,
            raw_symbol.into(),
            interval.as_ref().to_string(),
        )])
        .await
    }

    /// Subscribes to a private UTA topic.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent.
    pub async fn subscribe_private_channel(
        &self,
        topic: impl Into<String>,
        symbol: Option<String>,
    ) -> BitgetWsResult<()> {
        self.subscribe(vec![BitgetWsArg::private(topic, symbol)])
            .await
    }

    /// Subscribes to the private account channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent or the private session is not
    /// authenticated.
    pub async fn subscribe_account(&self, _coin: Option<String>) -> BitgetWsResult<()> {
        self.subscribe(vec![BitgetWsArg::account()]).await
    }

    /// Subscribes to the private order channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent or the private session is not
    /// authenticated.
    pub async fn subscribe_orders(&self) -> BitgetWsResult<()> {
        self.subscribe_private_channel("order", None).await
    }

    /// Subscribes to the private fill channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent or the private session is not
    /// authenticated.
    pub async fn subscribe_fills(&self) -> BitgetWsResult<()> {
        self.subscribe_private_channel("fill", None).await
    }

    /// Subscribes to the private positions channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent or the private session is not
    /// authenticated.
    pub async fn subscribe_positions(&self) -> BitgetWsResult<()> {
        self.subscribe_private_channel("position", None).await
    }

    /// Subscribes to the private strategy order channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscribe command cannot be sent or the private session is not
    /// authenticated.
    pub async fn subscribe_strategy_orders(&self) -> BitgetWsResult<()> {
        self.subscribe_private_channel("strategy-order", Some("default".to_string()))
            .await
    }

    async fn authenticate_if_required(&self) -> BitgetWsResult<()> {
        if !self.requires_auth {
            return Ok(());
        }

        let credential = self
            .credential
            .as_ref()
            .ok_or(BitgetWsError::MissingCredentials)?;
        let receiver = self.auth_tracker.begin();
        let payload = login_payload(credential)?;

        self.send_cmd(HandlerCommand::Login { payload }).await?;
        self.auth_tracker
            .wait_for_result(Duration::from_secs(AUTHENTICATION_TIMEOUT_SECS), receiver)
            .await
    }

    async fn wait_for_auth_if_required(&self) -> BitgetWsResult<()> {
        if self.requires_auth
            && !self
                .auth_tracker
                .wait_for_authenticated(Duration::from_secs(AUTHENTICATION_TIMEOUT_SECS))
                .await
        {
            return Err(BitgetWsError::Authentication(
                "Bitget WebSocket is not authenticated".to_string(),
            ));
        }

        Ok(())
    }

    async fn send_cmd(&self, cmd: HandlerCommand) -> BitgetWsResult<()> {
        self.cmd_tx
            .read()
            .await
            .send(cmd)
            .map_err(|e| BitgetWsError::Send(format!("Failed to send handler command: {e:?}")))
    }

    fn arg(&self, topic: impl Into<String>, symbol: Option<String>) -> BitgetWsArg {
        BitgetWsArg::new(self.product_type, topic, symbol)
    }
}

fn login_payload(credential: &Credential) -> BitgetWsResult<String> {
    let timestamp = Utc::now().timestamp_millis().to_string();
    login_payload_for_timestamp(credential, timestamp)
}

fn login_payload_for_timestamp(
    credential: &Credential,
    timestamp: impl Into<String>,
) -> BitgetWsResult<String> {
    let timestamp = timestamp.into();
    let sign = credential.sign_websocket_login(&timestamp);
    let arg = BitgetWsLoginArg {
        api_key: credential.api_key().to_string(),
        passphrase: credential.api_passphrase().to_string(),
        timestamp,
        sign,
    };
    let command = BitgetWsCommand::login(arg)?;
    Ok(serde_json::to_string(&command)?)
}

fn subscribe_payload(args: Vec<BitgetWsArg>) -> BitgetWsResult<String> {
    let command = BitgetWsCommand::subscribe(args)?;
    Ok(serde_json::to_string(&command)?)
}

async fn replay_subscriptions(
    subscriptions: &Arc<tokio::sync::RwLock<BTreeMap<String, BitgetWsArg>>>,
    cmd_tx: &tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
) -> BitgetWsResult<()> {
    let args: Vec<BitgetWsArg> = subscriptions.read().await.values().cloned().collect();
    if args.is_empty() {
        return Ok(());
    }

    let payload = subscribe_payload(args)?;
    cmd_tx
        .send(HandlerCommand::Subscribe { payload })
        .map_err(|e| BitgetWsError::Send(format!("Failed to queue subscription replay: {e:?}")))
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use axum::{
        Router,
        extract::{
            State,
            ws::{Message, WebSocket, WebSocketUpgrade},
        },
        response::Response,
        routing::get,
    };
    use futures_util::{SinkExt, StreamExt};
    use nautilus_common::testing::wait_until_async;
    use nautilus_network::websocket::TEXT_PONG;
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;

    #[derive(Clone, Default)]
    struct WsFixtureState {
        connection_count: Arc<AtomicUsize>,
        login_count: Arc<AtomicUsize>,
        received_messages: Arc<tokio::sync::Mutex<Vec<Value>>>,
        drop_after_subscribe: Arc<tokio::sync::Mutex<bool>>,
        send_order_after_subscribe: Arc<AtomicBool>,
    }

    impl WsFixtureState {
        async fn received_subscribes(&self) -> Vec<Value> {
            self.received_messages
                .lock()
                .await
                .iter()
                .filter(|value| value.get("op").and_then(Value::as_str) == Some("subscribe"))
                .cloned()
                .collect()
        }
    }

    async fn handle_ws_upgrade(
        ws: WebSocketUpgrade,
        State(state): State<WsFixtureState>,
    ) -> Response {
        ws.on_upgrade(move |socket| handle_ws_socket(socket, state))
    }

    async fn handle_ws_socket(socket: WebSocket, state: WsFixtureState) {
        state.connection_count.fetch_add(1, Ordering::SeqCst);
        let (mut sink, mut stream) = socket.split();

        while let Some(message) = stream.next().await {
            let Ok(message) = message else { break };

            match message {
                Message::Text(text) if text.as_str() == TEXT_PING => {
                    let _ = sink.send(Message::Text(TEXT_PONG.to_string().into())).await;
                }
                Message::Text(text) => {
                    let payload: Value = match serde_json::from_str(&text) {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    state.received_messages.lock().await.push(payload.clone());

                    match payload.get("op").and_then(Value::as_str) {
                        Some("login") => {
                            state.login_count.fetch_add(1, Ordering::SeqCst);
                            let ack = json!({
                                "event": "login",
                                "code": "0",
                                "msg": "success",
                            });
                            let _ = sink.send(Message::Text(ack.to_string().into())).await;
                        }
                        Some("subscribe") => {
                            let ack = json!({
                                "event": "subscribe",
                                "arg": payload
                                    .get("args")
                                    .and_then(Value::as_array)
                                    .and_then(|args| args.first())
                                    .cloned()
                                    .unwrap_or(Value::Null),
                            });
                            let _ = sink.send(Message::Text(ack.to_string().into())).await;

                            let is_order_subscribe = payload
                                .get("args")
                                .and_then(Value::as_array)
                                .and_then(|args| args.first())
                                .and_then(|arg| arg.get("topic"))
                                .and_then(Value::as_str)
                                == Some("order");
                            if is_order_subscribe
                                && state
                                    .send_order_after_subscribe
                                    .swap(false, Ordering::SeqCst)
                            {
                                let data = json!({
                                    "action": "snapshot",
                                    "arg": payload
                                        .get("args")
                                        .and_then(Value::as_array)
                                        .and_then(|args| args.first())
                                        .cloned()
                                        .unwrap_or(Value::Null),
                                    "data": [{
                                        "symbol": "BTCUSDT",
                                        "category": "USDT-FUTURES",
                                        "orderId": "O-WS",
                                        "clientOid": "C-WS",
                                        "status": "live"
                                    }],
                                });
                                let _ = sink.send(Message::Text(data.to_string().into())).await;
                            }

                            let mut drop_after_subscribe = state.drop_after_subscribe.lock().await;
                            if *drop_after_subscribe {
                                *drop_after_subscribe = false;
                                let _ = sink.send(Message::Close(None)).await;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                Message::Ping(payload) => {
                    let _ = sink.send(Message::Pong(payload)).await;
                }
                Message::Close(_) => break,
                Message::Binary(_) | Message::Pong(_) => {}
            }
        }
    }

    async fn start_mock_ws_server(state: WsFixtureState) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = Router::new()
            .route("/ws", get(handle_ws_upgrade))
            .with_state(state);

        tokio::spawn(async move {
            axum::serve(listener, router.into_make_service())
                .await
                .unwrap();
        });

        wait_until_async(
            || async { tokio::net::TcpStream::connect(addr).await.is_ok() },
            Duration::from_secs(5),
        )
        .await;

        addr
    }

    fn ws_url(addr: SocketAddr) -> String {
        format!("ws://{addr}/ws")
    }

    #[rstest]
    fn login_payload_uses_expected_shape_and_signature() {
        let credential = Credential::new("key", "secret", "passphrase");

        let payload = login_payload_for_timestamp(&credential, "1700000000000").unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(value["op"], "login");
        assert_eq!(value["args"][0]["apiKey"], "key");
        assert_eq!(value["args"][0]["passphrase"], "passphrase");
        assert_eq!(value["args"][0]["timestamp"], "1700000000000");
        assert_eq!(
            value["args"][0]["sign"],
            credential.sign_websocket_login("1700000000000"),
        );
    }

    #[rstest]
    fn login_payload_allows_empty_passphrase() {
        let credential = Credential::new("key", "secret", "");

        let payload = login_payload_for_timestamp(&credential, "1700000000000").unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(value["op"], "login");
        assert_eq!(value["args"][0]["apiKey"], "key");
        assert_eq!(value["args"][0]["passphrase"], "");
        assert_eq!(value["args"][0]["timestamp"], "1700000000000");
        assert_eq!(
            value["args"][0]["sign"],
            credential.sign_websocket_login("1700000000000"),
        );
    }

    #[rstest]
    fn subscribe_payload_batches_args() {
        let payload = subscribe_payload(vec![
            BitgetWsArg::new(
                BitgetProductType::Spot,
                "trade",
                Some("BTCUSDT".to_string()),
            ),
            BitgetWsArg::new(
                BitgetProductType::UsdtFutures,
                "books",
                Some("ETHUSDT".to_string()),
            ),
        ])
        .unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(value["op"], "subscribe");
        assert_eq!(value["args"].as_array().unwrap().len(), 2);
        assert_eq!(value["args"][0]["instType"], "spot");
        assert_eq!(value["args"][1]["instType"], "usdt-futures");
    }

    #[rstest]
    fn subscribe_payload_uses_v3_kline_arg_shape() {
        let payload = subscribe_payload(vec![BitgetWsArg::kline(
            BitgetProductType::UsdtFutures,
            "BTCUSDT",
            "1m",
        )])
        .unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(value["op"], "subscribe");
        assert_eq!(value["args"][0]["instType"], "usdt-futures");
        assert_eq!(value["args"][0]["topic"], "kline");
        assert_eq!(value["args"][0]["symbol"], "BTCUSDT");
        assert_eq!(value["args"][0]["interval"], "1m");
    }

    #[rstest]
    fn private_fill_payload_omits_legacy_default_symbol() {
        let payload = subscribe_payload(vec![BitgetWsArg::private("fill", None)]).unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();

        assert_eq!(value["args"][0]["instType"], "UTA");
        assert_eq!(value["args"][0]["topic"], "fill");
        assert!(value["args"][0].get("symbol").is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn ws_client_replays_subscriptions_and_emits_reconnected_after_drop() {
        let state = WsFixtureState::default();
        *state.drop_after_subscribe.lock().await = true;
        let addr = start_mock_ws_server(state.clone()).await;

        let mut client = BitgetWebSocketClient::new_public(
            BitgetProductType::UsdtFutures,
            BitgetEnvironment::Mainnet,
            Some(ws_url(addr)),
            30,
            TransportBackend::default(),
            None,
        );
        client.connect().await.unwrap();
        client.subscribe_trades("BTCUSDT").await.unwrap();

        let reconnected = tokio::time::timeout(
            Duration::from_secs(15),
            wait_until_async(
                || {
                    let state = state.clone();
                    async move { state.connection_count.load(Ordering::SeqCst) >= 2 }
                },
                Duration::from_secs(15),
            ),
        )
        .await
        .is_ok();
        assert!(
            reconnected,
            "client did not reconnect; connection_count={}",
            state.connection_count.load(Ordering::SeqCst),
        );

        let replayed = tokio::time::timeout(
            Duration::from_secs(10),
            wait_until_async(
                || {
                    let state = state.clone();
                    async move { state.received_subscribes().await.len() >= 2 }
                },
                Duration::from_secs(10),
            ),
        )
        .await
        .is_ok();
        assert!(
            replayed,
            "client did not replay subscriptions; messages={:?}",
            state.received_messages.lock().await,
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut saw_reconnected = false;
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), client.next_event()).await {
                Ok(Some(BitgetWsMessage::Reconnected)) => {
                    saw_reconnected = true;
                    break;
                }
                Ok(Some(_)) | Err(_) => {}
                Ok(None) => break,
            }
        }
        assert!(saw_reconnected, "client did not emit Reconnected");

        let subscribes = state.received_subscribes().await;
        assert_eq!(subscribes.len(), 2);
        for subscribe in subscribes {
            assert_eq!(subscribe["args"][0]["instType"], "usdt-futures");
            assert_eq!(subscribe["args"][0]["topic"], "publicTrade");
            assert_eq!(subscribe["args"][0]["symbol"], "BTCUSDT");
        }

        client.disconnect().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn private_ws_client_authenticates_and_emits_private_data_from_fixture() {
        let state = WsFixtureState::default();
        state
            .send_order_after_subscribe
            .store(true, Ordering::SeqCst);
        let addr = start_mock_ws_server(state.clone()).await;

        let mut client = BitgetWebSocketClient::new_private(
            BitgetProductType::UsdtFutures,
            BitgetEnvironment::Mainnet,
            Some("key".to_string()),
            Some("secret".to_string()),
            Some("passphrase".to_string()),
            Some(ws_url(addr)),
            30,
            TransportBackend::default(),
            None,
        );
        client.connect().await.unwrap();
        client.subscribe_orders().await.unwrap();

        let mut saw_subscribe = false;
        let mut saw_order_data = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), client.next_event()).await {
                Ok(Some(BitgetWsMessage::Subscribe(event))) => {
                    saw_subscribe = event.arg.as_ref().is_some_and(|arg| arg.topic == "order");
                }
                Ok(Some(BitgetWsMessage::Data(event))) => {
                    saw_order_data = event.arg.as_ref().is_some_and(|arg| arg.topic == "order")
                        && event.data.first().is_some_and(|row| {
                            row.get("orderId").and_then(Value::as_str) == Some("O-WS")
                        });
                    if saw_order_data {
                        break;
                    }
                }
                Ok(Some(_)) | Err(_) => {}
                Ok(None) => break,
            }
        }

        assert_eq!(state.login_count.load(Ordering::SeqCst), 1);
        assert!(saw_subscribe, "client did not receive subscribe ack");
        assert!(saw_order_data, "client did not receive private order data");

        client.disconnect().await.unwrap();
    }
}
