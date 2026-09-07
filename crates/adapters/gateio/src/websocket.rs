//! Gate.io JSON WebSocket transport.
//!
//! The exchange exposes one JSON protocol for public channels and a closely related
//! authenticated protocol for private channels.  This module intentionally keeps the
//! transport independent from Nautilus data types: the data and execution clients
//! receive [`GateioWsMessage`] values and perform the venue-specific decoding there.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    common::{consts::*, credential::Credential, enums::GateioProductType},
    config::{GateioDataClientConfig, GateioExecClientConfig},
};

const RECONNECT_DELAY_INITIAL: Duration = Duration::from_millis(250);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(5);
const INITIAL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);

/// Errors reported by the Gate.io WebSocket protocol.
#[derive(Debug, Error)]
pub enum GateioWsError {
    /// The transport could not be established or used.
    #[error("Gate.io WebSocket transport error: {0}")]
    Transport(String),
    /// The protocol message could not be decoded.
    #[error("Gate.io WebSocket JSON error: {0}")]
    Json(String),
    /// The operation was invalid for the current client state.
    #[error("Gate.io WebSocket client error: {0}")]
    Client(String),
}

/// A decoded Gate.io WebSocket envelope.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GateioWsMessage {
    #[serde(default)]
    pub time: Option<i64>,
    #[serde(default)]
    pub time_ms: Option<i64>,
    pub channel: String,
    pub event: String,
    #[serde(default)]
    pub result: Value,
    #[serde(default)]
    pub error: Option<Value>,
}

#[derive(Clone, Debug)]
struct Subscription {
    channel: String,
    payload: Vec<String>,
    private: bool,
}

#[derive(Debug)]
enum Command {
    Subscribe(Subscription),
    Unsubscribe(Subscription),
    Send(Value),
    Disconnect,
}

#[derive(Debug)]
struct Inner {
    url: String,
    credential: Option<Credential>,
    requires_auth: bool,
    heartbeat_interval: Duration,
    ping_channel: &'static str,
    subscriptions: RwLock<BTreeMap<String, Subscription>>,
    command_tx: RwLock<Option<mpsc::UnboundedSender<Command>>>,
    event_rx: Mutex<Option<mpsc::UnboundedReceiver<GateioWsMessage>>>,
    active: AtomicBool,
    stopping: AtomicBool,
}

/// A reconnecting Gate.io JSON WebSocket client.
#[derive(Clone, Debug)]
pub struct GateioWebSocketClient {
    inner: Arc<Inner>,
}

impl GateioWebSocketClient {
    /// Creates a public client for the selected Gate.io product.
    #[must_use]
    pub fn new_public(config: &GateioDataClientConfig) -> Self {
        Self::new(
            config.ws_url(),
            None,
            false,
            config.product_type,
            config.heartbeat_interval_secs,
        )
    }

    /// Creates a private client for the selected Gate.io product.
    #[must_use]
    pub fn new_private(config: &GateioExecClientConfig) -> Self {
        Self::new(
            config.ws_url(),
            Credential::resolve(config.api_key.clone(), config.api_secret.clone()),
            true,
            config.product_type,
            config.heartbeat_interval_secs,
        )
    }

    fn new(
        url: String,
        credential: Option<Credential>,
        requires_auth: bool,
        product_type: GateioProductType,
        heartbeat_interval_secs: u64,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                url,
                credential,
                requires_auth,
                heartbeat_interval: Duration::from_secs(heartbeat_interval_secs.max(1)),
                ping_channel: ping_channel(product_type),
                subscriptions: RwLock::new(BTreeMap::new()),
                command_tx: RwLock::new(None),
                event_rx: Mutex::new(None),
                active: AtomicBool::new(false),
                stopping: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the configured WebSocket URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.inner.url
    }

    /// Returns whether the socket currently has an active connection.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.inner.active.load(Ordering::Acquire)
    }

    /// Takes the event receiver.  It can only be taken once.
    pub fn take_event_receiver(&self) -> Option<mpsc::UnboundedReceiver<GateioWsMessage>> {
        self.inner.event_rx.lock().ok()?.take()
    }

    /// Connects the transport and starts its reconnect loop.
    pub async fn connect(&self) -> anyhow::Result<()> {
        if self.is_active() {
            return Ok(());
        }

        self.inner.stopping.store(false, Ordering::Release);
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        {
            let mut tx = self.inner.command_tx.write().await;
            *tx = Some(command_tx);
        }
        {
            let mut receiver =
                self.inner.event_rx.lock().map_err(|_| {
                    anyhow::anyhow!("Gate.io WebSocket event receiver lock poisoned")
                })?;
            if receiver.is_none() {
                *receiver = Some(event_rx);
            }
        }

        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let mut delay = RECONNECT_DELAY_INITIAL;

            loop {
                if inner.stopping.load(Ordering::Acquire) {
                    break;
                }

                match connect_async(&inner.url).await {
                    Ok((mut socket, _)) => {
                        inner.active.store(true, Ordering::Release);
                        delay = RECONNECT_DELAY_INITIAL;
                        let mut heartbeat = tokio::time::interval(inner.heartbeat_interval);
                        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        heartbeat.tick().await;

                        let subscriptions = inner.subscriptions.read().await;
                        for subscription in subscriptions.values() {
                            if let Err(error) =
                                send_subscription(&mut socket, &inner, subscription, true).await
                            {
                                log::warn!(
                                    "Failed to restore Gate.io WebSocket subscription: {error}"
                                );
                            }
                        }
                        drop(subscriptions);

                        loop {
                            tokio::select! {
                                command = command_rx.recv() => {
                                    match command {
                                        Some(Command::Subscribe(subscription)) => {
                                            if let Err(error) = send_subscription(&mut socket, &inner, &subscription, true).await {
                                                log::warn!("Failed to subscribe to Gate.io WebSocket channel {}: {error}", subscription.channel);
                                            }
                                        }
                                        Some(Command::Unsubscribe(subscription)) => {
                                            if let Err(error) = send_subscription(&mut socket, &inner, &subscription, false).await {
                                                log::warn!("Failed to unsubscribe from Gate.io WebSocket channel {}: {error}", subscription.channel);
                                            }
                                        }
                                        Some(Command::Send(value)) => {
                                            if let Err(error) = socket.send(Message::Text(value.to_string().into())).await {
                                                log::warn!("Failed to send Gate.io WebSocket message: {error}");
                                                break;
                                            }
                                        }
                                        Some(Command::Disconnect) | None => {
                                            inner.stopping.store(true, Ordering::Release);
                                            break;
                                        }
                                    }
                                }
                                _ = heartbeat.tick() => {
                                    if let Err(error) = socket.send(Message::Ping(Vec::new().into())).await {
                                        log::debug!("Failed to send Gate.io protocol heartbeat: {error}");
                                        break;
                                    }
                                    let timestamp = Utc::now().timestamp();
                                    let message = json!({
                                        "time": timestamp,
                                        "channel": inner.ping_channel,
                                        "event": "",
                                        "payload": [],
                                    });
                                    if let Err(error) = socket.send(Message::Text(message.to_string().into())).await {
                                        log::debug!("Failed to send Gate.io heartbeat: {error}");
                                        break;
                                    }
                                }
                                message = socket.next() => {
                                    match message {
                                        Some(Ok(Message::Text(text))) => {
                                            match serde_json::from_str::<GateioWsMessage>(text.as_ref()) {
                                                Ok(message) => {
                                                    let _ = event_tx.send(message);
                                                }
                                                Err(error) => {
                                                    log::debug!("Ignoring non-envelope Gate.io WebSocket message: {error}");
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Binary(bytes))) => {
                                            match serde_json::from_slice::<GateioWsMessage>(&bytes) {
                                                Ok(message) => {
                                                    let _ = event_tx.send(message);
                                                }
                                                Err(error) => {
                                                    log::debug!("Ignoring invalid Gate.io binary message: {error}");
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(payload))) => {
                                            if let Err(error) = socket.send(Message::Pong(payload)).await {
                                                log::debug!("Failed to answer Gate.io WebSocket ping: {error}");
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Pong(_))) => {}
                                        Some(Ok(Message::Close(_))) | None => break,
                                        Some(Err(error)) => {
                                            log::warn!("Gate.io WebSocket receive error: {error}");
                                            break;
                                        }
                                        Some(Ok(Message::Frame(_))) => {}
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        log::warn!("Gate.io WebSocket connection failed: {error}");
                    }
                }

                inner.active.store(false, Ordering::Release);
                if inner.stopping.load(Ordering::Acquire) {
                    break;
                }
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay.saturating_mul(2), RECONNECT_DELAY_MAX);
            }

            inner.active.store(false, Ordering::Release);
        });

        let connected = tokio::time::timeout(INITIAL_CONNECTION_TIMEOUT, async {
            while !self.is_active() && !self.inner.stopping.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            self.is_active()
        })
        .await
        .unwrap_or(false);

        if connected {
            return Ok(());
        }

        Err(anyhow::anyhow!(
            "Gate.io WebSocket connection timeout: {}",
            self.url()
        ))
    }

    /// Disconnects the socket and stops reconnect attempts.
    pub async fn disconnect(&self) -> anyhow::Result<()> {
        self.inner.stopping.store(true, Ordering::Release);
        if let Some(sender) = self.inner.command_tx.read().await.as_ref() {
            let _ = sender.send(Command::Disconnect);
        }
        self.inner.active.store(false, Ordering::Release);
        Ok(())
    }

    /// Subscribes to a public or private channel.
    pub async fn subscribe(
        &self,
        channel: impl Into<String>,
        payload: Vec<String>,
        private: bool,
    ) -> anyhow::Result<()> {
        if private && self.inner.credential.is_none() {
            anyhow::bail!("Gate.io credentials are required for private WebSocket channels");
        }
        if private && !self.inner.requires_auth {
            anyhow::bail!("Gate.io WebSocket client was created without private authentication");
        }

        let subscription = Subscription {
            channel: channel.into(),
            payload,
            private,
        };
        let key = subscription_key(&subscription);
        self.inner
            .subscriptions
            .write()
            .await
            .insert(key, subscription.clone());

        if let Some(sender) = self.inner.command_tx.read().await.as_ref() {
            sender
                .send(Command::Subscribe(subscription))
                .map_err(|error| {
                    anyhow::anyhow!("failed to queue Gate.io subscription: {error}")
                })?;
        }
        Ok(())
    }

    /// Unsubscribes from a public or private channel.
    pub async fn unsubscribe(
        &self,
        channel: impl Into<String>,
        payload: Vec<String>,
        private: bool,
    ) -> anyhow::Result<()> {
        let subscription = Subscription {
            channel: channel.into(),
            payload,
            private,
        };
        self.inner
            .subscriptions
            .write()
            .await
            .remove(&subscription_key(&subscription));
        if let Some(sender) = self.inner.command_tx.read().await.as_ref() {
            sender
                .send(Command::Unsubscribe(subscription))
                .map_err(|error| {
                    anyhow::anyhow!("failed to queue Gate.io unsubscription: {error}")
                })?;
        }
        Ok(())
    }

    /// Sends an already encoded protocol message.
    pub async fn send_json(&self, message: Value) -> anyhow::Result<()> {
        let sender = self
            .inner
            .command_tx
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Gate.io WebSocket is not connected"))?;
        sender
            .send(Command::Send(message))
            .map_err(|error| anyhow::anyhow!("failed to queue Gate.io WebSocket message: {error}"))
    }

    /// Returns a convenient channel name for the selected product.
    #[must_use]
    pub const fn ticker_channel(product_type: GateioProductType) -> &'static str {
        match product_type {
            GateioProductType::Spot => GATEIO_SPOT_TICKER_WS_CHANNEL,
            GateioProductType::UsdtPerpetual => GATEIO_FUTURES_BOOK_TICKER_WS_CHANNEL,
        }
    }
}

fn subscription_key(subscription: &Subscription) -> String {
    format!(
        "{}:{}:{}",
        subscription.channel,
        subscription.private,
        subscription.payload.join("\u{1f}")
    )
}

const fn ping_channel(product_type: GateioProductType) -> &'static str {
    match product_type {
        GateioProductType::Spot => GATEIO_SPOT_PING_WS_CHANNEL,
        GateioProductType::UsdtPerpetual => GATEIO_FUTURES_PING_WS_CHANNEL,
    }
}

async fn send_subscription<S>(
    socket: &mut S,
    inner: &Inner,
    subscription: &Subscription,
    subscribe: bool,
) -> anyhow::Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let event = if subscribe {
        "subscribe"
    } else {
        "unsubscribe"
    };
    let timestamp = Utc::now().timestamp();
    let mut message = json!({
        "time": timestamp,
        "channel": subscription.channel,
        "event": event,
        "payload": subscription.payload,
    });
    if subscription.private {
        let credential = inner.credential.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Gate.io credentials are required for private channels")
        })?;
        let sign = credential.sign_ws(&subscription.channel, event, &timestamp.to_string());
        message["auth"] = json!({
            "method": "api_key",
            "KEY": credential.api_key(),
            "SIGN": sign,
        });
    }
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .map_err(|error| anyhow::anyhow!("WebSocket send failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GateioDataClientConfig;

    #[test]
    fn public_client_uses_configured_url() {
        let config = GateioDataClientConfig {
            base_url_ws: Some("wss://example.test/ws".to_string()),
            ..Default::default()
        };
        let client = GateioWebSocketClient::new_public(&config);
        assert_eq!(client.url(), "wss://example.test/ws");
    }

    #[test]
    fn subscription_key_is_stable() {
        let subscription = Subscription {
            channel: "spot.trades".to_string(),
            payload: vec!["BTC_USDT".to_string()],
            private: false,
        };
        assert_eq!(
            subscription_key(&subscription),
            "spot.trades:false:BTC_USDT"
        );
    }

    #[test]
    fn uses_product_specific_heartbeat_channel() {
        assert_eq!(
            ping_channel(GateioProductType::Spot),
            GATEIO_SPOT_PING_WS_CHANNEL
        );
        assert_eq!(
            ping_channel(GateioProductType::UsdtPerpetual),
            GATEIO_FUTURES_PING_WS_CHANNEL
        );
    }
}
