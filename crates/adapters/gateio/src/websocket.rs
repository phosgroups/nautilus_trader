//! Gate.io JSON WebSocket transport.
//!
//! The exchange exposes one JSON protocol for public channels and a closely related
//! authenticated protocol for private channels.  This module intentionally keeps the
//! transport independent from Nautilus data types: the data and execution clients
//! receive [`GateioWsMessage`] values and perform the venue-specific decoding there.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    sync::{RwLock, broadcast, mpsc, oneshot},
    time::Instant as TokioInstant,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};

use crate::{
    common::{consts::*, credential::Credential, enums::GateioProductType},
    config::{GateioDataClientConfig, GateioExecClientConfig},
};

const RECONNECT_DELAY_INITIAL: Duration = Duration::from_millis(250);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(5);
const INITIAL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);
const SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const GATEIO_INTERNAL_RECONNECTED_CHANNEL: &str = "__gateio.reconnected";

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
#[derive(Clone, Debug, Serialize)]
pub struct GateioWsMessage {
    #[serde(default)]
    pub time: Option<i64>,
    #[serde(default)]
    pub time_ms: Option<i64>,
    #[serde(default)]
    pub id: Option<i64>,
    pub channel: String,
    pub event: String,
    #[serde(default)]
    pub result: Value,
    #[serde(default)]
    pub error: Option<Value>,
}

impl GateioWsMessage {
    pub(crate) fn reconnected() -> Self {
        Self {
            time: None,
            time_ms: None,
            id: None,
            channel: GATEIO_INTERNAL_RECONNECTED_CHANNEL.to_string(),
            event: "reconnected".to_string(),
            result: Value::Null,
            error: None,
        }
    }
}

impl<'de> Deserialize<'de> for GateioWsMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| de::Error::custom("Gate.io WebSocket message must be an object"))?;
        let header = object.get("header").and_then(Value::as_object);
        let data = object.get("data").and_then(Value::as_object);

        let channel = object
            .get("channel")
            .and_then(Value::as_str)
            .or_else(|| {
                header
                    .and_then(|header| header.get("channel"))
                    .and_then(Value::as_str)
            })
            .ok_or_else(|| de::Error::custom("Gate.io WebSocket message has no channel"))?
            .to_string();
        let event = object
            .get("event")
            .and_then(Value::as_str)
            .or_else(|| {
                header
                    .and_then(|header| header.get("event"))
                    .and_then(Value::as_str)
            })
            .unwrap_or_default()
            .to_string();
        let result = object
            .get("result")
            .cloned()
            .or_else(|| data.and_then(|data| data.get("result")).cloned())
            .or_else(|| data.and_then(|data| data.get("errs")).cloned())
            .unwrap_or(Value::Null);
        let error = object
            .get("error")
            .filter(|value| !value.is_null())
            .cloned()
            .or_else(|| {
                data.and_then(|data| data.get("errs"))
                    .filter(|value| !value.is_null())
                    .cloned()
            });

        Ok(Self {
            time: scalar_i64(object.get("time")),
            time_ms: scalar_i64(object.get("time_ms")),
            id: scalar_i64(object.get("id")),
            channel,
            event,
            result,
            error,
        })
    }
}

fn scalar_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(value) => value.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

#[derive(Clone, Debug)]
struct Subscription {
    channel: String,
    payload: Vec<String>,
    private: bool,
}

#[derive(Debug)]
enum Command {
    Subscribe {
        subscription: Subscription,
        ack: Option<oneshot::Sender<anyhow::Result<()>>>,
    },
    Unsubscribe {
        subscription: Subscription,
        ack: Option<oneshot::Sender<anyhow::Result<()>>>,
    },
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
    event_tx: broadcast::Sender<GateioWsMessage>,
    subscription_lock: tokio::sync::Mutex<()>,
    next_request_id: AtomicI64,
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
                event_tx: broadcast::channel(4096).0,
                subscription_lock: tokio::sync::Mutex::new(()),
                next_request_id: AtomicI64::new(1),
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

    /// Creates an event receiver for the current logical client.
    ///
    /// A broadcast receiver is intentionally created on demand so a data or
    /// execution client can stop and start its dispatch task without losing
    /// the WebSocket transport's event channel.
    pub fn take_event_receiver(&self) -> broadcast::Receiver<GateioWsMessage> {
        self.inner.event_tx.subscribe()
    }

    /// Connects the transport and starts its reconnect loop.
    pub async fn connect(&self) -> anyhow::Result<()> {
        if self.is_active() {
            return Ok(());
        }

        self.inner.stopping.store(false, Ordering::Release);
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let event_tx = self.inner.event_tx.clone();

        {
            let mut tx = self.inner.command_tx.write().await;
            *tx = Some(command_tx);
        }

        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let mut delay = RECONNECT_DELAY_INITIAL;
            let mut has_connected_once = false;

            loop {
                if inner.stopping.load(Ordering::Acquire) {
                    break;
                }

                let mut request = match inner.url.clone().into_client_request() {
                    Ok(request) => request,
                    Err(error) => {
                        log::warn!("Invalid Gate.io WebSocket URL {}: {error}", inner.url);
                        inner.active.store(false, Ordering::Release);
                        tokio::time::sleep(delay).await;
                        delay = std::cmp::min(delay.saturating_mul(2), RECONNECT_DELAY_MAX);
                        continue;
                    }
                };
                request
                    .headers_mut()
                    .insert("X-Gate-Size-Decimal", HeaderValue::from_static("1"));

                match connect_async(request).await {
                    Ok((mut socket, _)) => {
                        let is_reconnect = has_connected_once;
                        has_connected_once = true;
                        inner.active.store(true, Ordering::Release);
                        delay = RECONNECT_DELAY_INITIAL;
                        let mut heartbeat = tokio::time::interval(inner.heartbeat_interval);
                        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        heartbeat.tick().await;

                        let mut pending_acks: BTreeMap<i64, PendingAck> = BTreeMap::new();
                        let mut restore_pending = std::collections::HashSet::new();
                        let subscriptions = inner
                            .subscriptions
                            .read()
                            .await
                            .values()
                            .cloned()
                            .collect::<Vec<_>>();
                        let restore_deadline = TokioInstant::now() + SUBSCRIPTION_ACK_TIMEOUT;
                        let mut restore_failed = false;
                        for subscription in &subscriptions {
                            let request_id = next_request_id(&inner);
                            let (ack_tx, _ack_rx) = oneshot::channel();
                            pending_acks.insert(
                                request_id,
                                PendingAck {
                                    event: "subscribe",
                                    sender: ack_tx,
                                },
                            );
                            if is_reconnect {
                                restore_pending.insert(request_id);
                            }
                            if let Err(error) = send_subscription(
                                &mut socket,
                                &inner,
                                subscription,
                                true,
                                request_id,
                            )
                            .await
                            {
                                resolve_one_subscription_ack(
                                    &mut pending_acks,
                                    request_id,
                                    Err(error.to_string()),
                                );
                                restore_pending.remove(&request_id);
                                restore_failed = true;
                                log::warn!(
                                    "Failed to restore Gate.io WebSocket subscription {}: {error}",
                                    subscription.channel
                                );
                            }
                        }
                        if is_reconnect && restore_failed {
                            fail_pending_acks(
                                &mut pending_acks,
                                "Gate.io WebSocket subscription restoration failed",
                            );
                            inner.active.store(false, Ordering::Release);
                            if inner.stopping.load(Ordering::Acquire) {
                                break;
                            }
                            tokio::time::sleep(delay).await;
                            delay = std::cmp::min(delay.saturating_mul(2), RECONNECT_DELAY_MAX);
                            continue;
                        }
                        let mut reconnect_notified = !is_reconnect;
                        if is_reconnect && restore_pending.is_empty() {
                            let _ = event_tx.send(GateioWsMessage::reconnected());
                            reconnect_notified = true;
                        }

                        loop {
                            tokio::select! {
                                command = command_rx.recv() => {
                                    match command {
                                        Some(Command::Subscribe { subscription, ack }) => {
                                            let request_id = next_request_id(&inner);
                                            if let Some(ack) = ack {
                                                pending_acks.insert(
                                                    request_id,
                                                    PendingAck {
                                                        event: "subscribe",
                                                        sender: ack,
                                                    },
                                                );
                                            }
                                            if let Err(error) = send_subscription(&mut socket, &inner, &subscription, true, request_id).await {
                                                resolve_one_subscription_ack(
                                                    &mut pending_acks,
                                                    request_id,
                                                    Err(error.to_string()),
                                                );
                                                log::warn!("Failed to subscribe to Gate.io WebSocket channel {}: {error}", subscription.channel);
                                            }
                                        }
                                        Some(Command::Unsubscribe { subscription, ack }) => {
                                            let request_id = next_request_id(&inner);
                                            if let Some(ack) = ack {
                                                pending_acks.insert(
                                                    request_id,
                                                    PendingAck {
                                                        event: "unsubscribe",
                                                        sender: ack,
                                                    },
                                                );
                                            }
                                            if let Err(error) = send_subscription(&mut socket, &inner, &subscription, false, request_id).await {
                                                resolve_one_subscription_ack(
                                                    &mut pending_acks,
                                                    request_id,
                                                    Err(error.to_string()),
                                                );
                                                log::warn!("Failed to unsubscribe from Gate.io WebSocket channel {}: {error}", subscription.channel);
                                            }
                                        }
                                        Some(Command::Send(value)) => {
                                            if let Err(error) = socket.send(Message::Text(value.to_string().into())).await {
                                                log::warn!("Failed to send Gate.io WebSocket message: {error}");
                                                fail_pending_acks(&mut pending_acks, error.to_string());
                                                break;
                                            }
                                        }
                                        Some(Command::Disconnect) | None => {
                                            inner.stopping.store(true, Ordering::Release);
                                            fail_pending_acks(&mut pending_acks, "Gate.io WebSocket disconnected");
                                            break;
                                        }
                                    }
                                }
                                _ = heartbeat.tick() => {
                                    if let Err(error) = socket.send(Message::Ping(Vec::new().into())).await {
                                        log::debug!("Failed to send Gate.io protocol heartbeat: {error}");
                                        fail_pending_acks(&mut pending_acks, error.to_string());
                                        break;
                                    }
                                    let timestamp = Utc::now().timestamp();
                                    let message = json!({
                                        "time": timestamp,
                                        "channel": inner.ping_channel,
                                    });
                                    if let Err(error) = socket.send(Message::Text(message.to_string().into())).await {
                                        log::debug!("Failed to send Gate.io heartbeat: {error}");
                                        fail_pending_acks(&mut pending_acks, error.to_string());
                                        break;
                                    }
                                }
                                _ = tokio::time::sleep_until(restore_deadline), if is_reconnect && !restore_pending.is_empty() => {
                                    log::warn!(
                                        "Gate.io WebSocket subscription restoration timed out with {} pending ACKs",
                                        restore_pending.len()
                                    );
                                    fail_pending_acks(
                                        &mut pending_acks,
                                        "Gate.io WebSocket subscription restoration timed out",
                                    );
                                    break;
                                }
                                message = socket.next() => {
                                    match message {
                                        Some(Ok(Message::Text(text))) => {
                                            match serde_json::from_str::<GateioWsMessage>(text.as_ref()) {
                                                Ok(message) => {
                                                    let restore_id = message
                                                        .id
                                                        .filter(|id| restore_pending.contains(id));
                                                    let ack_result =
                                                        resolve_subscription_ack(&mut pending_acks, &message);
                                                    if let Some(request_id) = restore_id {
                                                        restore_pending.remove(&request_id);
                                                        if !matches!(ack_result, Some(Ok(()))) {
                                                            log::warn!(
                                                                "Gate.io WebSocket restored subscription was not accepted: channel={}, id={request_id}",
                                                                message.channel
                                                            );
                                                            fail_pending_acks(
                                                                &mut pending_acks,
                                                                "Gate.io restored subscription was rejected",
                                                            );
                                                            break;
                                                        }
                                                        if restore_pending.is_empty()
                                                            && is_reconnect
                                                            && !reconnect_notified
                                                        {
                                                            let _ = event_tx
                                                                .send(GateioWsMessage::reconnected());
                                                            reconnect_notified = true;
                                                        }
                                                    }
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
                                                    let restore_id = message
                                                        .id
                                                        .filter(|id| restore_pending.contains(id));
                                                    let ack_result =
                                                        resolve_subscription_ack(&mut pending_acks, &message);
                                                    if let Some(request_id) = restore_id {
                                                        restore_pending.remove(&request_id);
                                                        if !matches!(ack_result, Some(Ok(()))) {
                                                            log::warn!(
                                                                "Gate.io WebSocket restored subscription was not accepted: channel={}, id={request_id}",
                                                                message.channel
                                                            );
                                                            fail_pending_acks(
                                                                &mut pending_acks,
                                                                "Gate.io restored subscription was rejected",
                                                            );
                                                            break;
                                                        }
                                                        if restore_pending.is_empty()
                                                            && is_reconnect
                                                            && !reconnect_notified
                                                        {
                                                            let _ = event_tx
                                                                .send(GateioWsMessage::reconnected());
                                                            reconnect_notified = true;
                                                        }
                                                    }
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
                                                fail_pending_acks(&mut pending_acks, error.to_string());
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Pong(_))) => {}
                                        Some(Ok(Message::Close(_))) | None => {
                                            fail_pending_acks(&mut pending_acks, "Gate.io WebSocket closed");
                                            break;
                                        }
                                        Some(Err(error)) => {
                                            log::warn!("Gate.io WebSocket receive error: {error}");
                                            fail_pending_acks(&mut pending_acks, error.to_string());
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
        self.stop();
        self.inner.subscriptions.write().await.clear();
        Ok(())
    }

    /// Stops the socket synchronously for lifecycle methods that cannot await.
    pub fn stop(&self) {
        self.inner.stopping.store(true, Ordering::Release);
        if let Ok(sender) = self.inner.command_tx.try_read()
            && let Some(sender) = sender.as_ref()
        {
            let _ = sender.send(Command::Disconnect);
        }
        if let Ok(mut subscriptions) = self.inner.subscriptions.try_write() {
            subscriptions.clear();
        }
        self.inner.active.store(false, Ordering::Release);
    }

    /// Subscribes to a public or private channel.
    pub async fn subscribe(
        &self,
        channel: impl Into<String>,
        payload: Vec<String>,
        private: bool,
    ) -> anyhow::Result<()> {
        let _operation_guard = self.inner.subscription_lock.lock().await;
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
        let sender = self
            .inner
            .command_tx
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Gate.io WebSocket is not connected"))?;
        let (ack_tx, ack_rx) = oneshot::channel();
        sender
            .send(Command::Subscribe {
                subscription: subscription.clone(),
                ack: Some(ack_tx),
            })
            .map_err(|error| anyhow::anyhow!("failed to queue Gate.io subscription: {error}"))?;
        let result = tokio::time::timeout(SUBSCRIPTION_ACK_TIMEOUT, ack_rx)
            .await
            .map_err(|_| anyhow::anyhow!("Gate.io subscription acknowledgement timed out"))?
            .map_err(|_| anyhow::anyhow!("Gate.io subscription acknowledgement channel closed"))?;
        if result.is_ok() {
            self.inner
                .subscriptions
                .write()
                .await
                .insert(subscription_key(&subscription), subscription);
        }
        result
    }

    /// Unsubscribes from a public or private channel.
    pub async fn unsubscribe(
        &self,
        channel: impl Into<String>,
        payload: Vec<String>,
        private: bool,
    ) -> anyhow::Result<()> {
        let _operation_guard = self.inner.subscription_lock.lock().await;
        let subscription = Subscription {
            channel: channel.into(),
            payload,
            private,
        };
        let sender = self
            .inner
            .command_tx
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Gate.io WebSocket is not connected"))?;
        let (ack_tx, ack_rx) = oneshot::channel();
        sender
            .send(Command::Unsubscribe {
                subscription: subscription.clone(),
                ack: Some(ack_tx),
            })
            .map_err(|error| anyhow::anyhow!("failed to queue Gate.io unsubscription: {error}"))?;
        let result = tokio::time::timeout(SUBSCRIPTION_ACK_TIMEOUT, ack_rx)
            .await
            .map_err(|_| anyhow::anyhow!("Gate.io unsubscription acknowledgement timed out"))?
            .map_err(|_| {
                anyhow::anyhow!("Gate.io unsubscription acknowledgement channel closed")
            })?;
        if result.is_ok() {
            self.inner
                .subscriptions
                .write()
                .await
                .remove(&subscription_key(&subscription));
        }
        result
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
            GateioProductType::Spot => GATEIO_SPOT_BOOK_TICKER_WS_CHANNEL,
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

struct PendingAck {
    event: &'static str,
    sender: oneshot::Sender<anyhow::Result<()>>,
}

fn next_request_id(inner: &Inner) -> i64 {
    let id = inner
        .next_request_id
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(if current >= i64::MAX { 1 } else { current + 1 })
        })
        .unwrap_or(1);
    if id > 0 { id } else { 1 }
}

fn resolve_subscription_ack(
    pending: &mut BTreeMap<i64, PendingAck>,
    message: &GateioWsMessage,
) -> Option<Result<(), String>> {
    let event = match message.event.as_str() {
        "subscribe" | "unsubscribe" => message.event.as_str(),
        _ => return None,
    };
    let Some(request_id) = message.id else {
        log::warn!(
            "Gate.io WebSocket {event} acknowledgement for {} has no request id",
            message.channel
        );
        return None;
    };
    let Some(expected) = pending.get(&request_id).map(|ack| ack.event) else {
        return None;
    };
    if expected != event {
        let result = Err(format!(
            "Gate.io WebSocket acknowledgement event mismatch: expected {expected}, got {event}"
        ));
        resolve_one_subscription_ack(pending, request_id, result.clone());
        return Some(result);
    }
    let result = if let Some(error) = &message.error
        && !error.is_null()
    {
        Err(format!(
            "Gate.io WebSocket {event} rejected for {}: {error}",
            message.channel
        ))
    } else if let Some(status) = message.result.get("status").and_then(Value::as_str)
        && !status.eq_ignore_ascii_case("success")
    {
        Err(format!(
            "Gate.io WebSocket {event} returned status {status:?} for {}",
            message.channel
        ))
    } else {
        Ok(())
    };
    resolve_one_subscription_ack(pending, request_id, result.clone());
    Some(result)
}

fn resolve_one_subscription_ack(
    pending: &mut BTreeMap<i64, PendingAck>,
    request_id: i64,
    result: Result<(), String>,
) {
    let Some(waiter) = pending.remove(&request_id) else {
        return;
    };
    let _ = waiter.sender.send(result.map_err(anyhow::Error::msg));
}

fn fail_pending_acks(pending: &mut BTreeMap<i64, PendingAck>, message: impl Into<String>) {
    let message = message.into();
    for (_, waiter) in std::mem::take(pending) {
        let _ = waiter.sender.send(Err(anyhow::anyhow!(message.clone())));
    }
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
    request_id: i64,
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
        "id": request_id,
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

    #[tokio::test]
    async fn resolves_subscription_ack_by_request_id() {
        let (first_tx, mut first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        let mut pending = BTreeMap::from([
            (
                101,
                PendingAck {
                    event: "subscribe",
                    sender: first_tx,
                },
            ),
            (
                102,
                PendingAck {
                    event: "subscribe",
                    sender: second_tx,
                },
            ),
        ]);

        let message = serde_json::from_value::<GateioWsMessage>(serde_json::json!({
            "id": 102,
            "channel": "spot.trades",
            "event": "subscribe",
            "result": {"status": "success"}
        }))
        .unwrap();
        resolve_subscription_ack(&mut pending, &message);

        assert!(matches!(
            first_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(second_rx.await.unwrap().unwrap(), ());
        assert!(pending.contains_key(&101));
    }

    #[tokio::test]
    async fn rejects_subscription_ack_with_error_or_wrong_event() {
        let (error_tx, error_rx) = oneshot::channel();
        let (mismatch_tx, mismatch_rx) = oneshot::channel();
        let mut pending = BTreeMap::from([
            (
                201,
                PendingAck {
                    event: "subscribe",
                    sender: error_tx,
                },
            ),
            (
                202,
                PendingAck {
                    event: "subscribe",
                    sender: mismatch_tx,
                },
            ),
        ]);

        let error_message = serde_json::from_value::<GateioWsMessage>(serde_json::json!({
            "id": 201,
            "channel": "spot.orders",
            "event": "subscribe",
            "error": {"label": "INVALID", "message": "bad payload"},
            "result": {}
        }))
        .unwrap();
        resolve_subscription_ack(&mut pending, &error_message);
        assert!(error_rx.await.unwrap().is_err());

        let mismatch_message = serde_json::from_value::<GateioWsMessage>(serde_json::json!({
            "id": 202,
            "channel": "spot.orders",
            "event": "unsubscribe",
            "result": {"status": "success"}
        }))
        .unwrap();
        resolve_subscription_ack(&mut pending, &mismatch_message);
        let error = mismatch_rx.await.unwrap().unwrap_err().to_string();
        assert!(error.contains("event mismatch"));
    }

    #[test]
    fn request_ids_are_positive_and_monotonic() {
        let config = GateioDataClientConfig::default();
        let client = GateioWebSocketClient::new_public(&config);
        let first = next_request_id(&client.inner);
        let second = next_request_id(&client.inner);
        assert!(first > 0);
        assert_eq!(second, first + 1);
    }
}
