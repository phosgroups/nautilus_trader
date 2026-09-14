//! Gate.io JSON WebSocket transport.
//!
//! Gate.io uses a JSON envelope for both public and authenticated channels. This
//! module owns the venue protocol while delegating connection establishment,
//! proxy support, transport selection, heartbeat frames, and reconnection to
//! Nautilus' shared [`WebSocketClient`].

use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use nautilus_common::live::get_runtime;
use nautilus_network::{
    RECONNECTED,
    websocket::{TransportBackend, WebSocketClient, WebSocketConfig, channel_message_handler},
};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    sync::{RwLock, broadcast, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;

use crate::{
    common::{consts::*, credential::Credential, enums::GateioProductType},
    config::{GateioDataClientConfig, GateioExecClientConfig},
};

const INITIAL_CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);
const SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const COMMAND_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
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
    transport_backend: TransportBackend,
    proxy_url: Option<String>,
    subscriptions: RwLock<BTreeMap<String, Subscription>>,
    command_tx: RwLock<Option<mpsc::UnboundedSender<Command>>>,
    command_task: RwLock<Option<JoinHandle<()>>>,
    event_tx: broadcast::Sender<GateioWsMessage>,
    subscription_lock: tokio::sync::Mutex<()>,
    next_request_id: AtomicI64,
    active: AtomicBool,
    stopping: AtomicBool,
}

/// A Gate.io protocol client using Nautilus' shared WebSocket transport.
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
            config.transport_backend,
            config.proxy_url.clone(),
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
            config.transport_backend,
            config.proxy_url.clone(),
        )
    }

    fn new(
        url: String,
        credential: Option<Credential>,
        requires_auth: bool,
        product_type: GateioProductType,
        heartbeat_interval_secs: u64,
        transport_backend: TransportBackend,
        proxy_url: Option<String>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                url,
                credential,
                requires_auth,
                heartbeat_interval: Duration::from_secs(heartbeat_interval_secs.max(1)),
                ping_channel: ping_channel(product_type),
                transport_backend,
                proxy_url,
                subscriptions: RwLock::new(BTreeMap::new()),
                command_tx: RwLock::new(None),
                command_task: RwLock::new(None),
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

    /// Returns whether the logical Gate.io client is active.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.inner.active.load(Ordering::Acquire)
    }

    /// Creates an event receiver for the current logical client.
    pub fn take_event_receiver(&self) -> broadcast::Receiver<GateioWsMessage> {
        self.inner.event_tx.subscribe()
    }

    async fn shutdown_command_loop(&self, clear_subscriptions: bool) {
        self.inner.stopping.store(true, Ordering::Release);
        let sender = self.inner.command_tx.write().await.take();
        if let Some(sender) = sender {
            let _ = sender.send(Command::Disconnect);
        }
        if clear_subscriptions {
            self.inner.subscriptions.write().await.clear();
        }
        let task = self.inner.command_task.write().await.take();
        if let Some(mut task) = task {
            tokio::select! {
                result = &mut task => {
                    if let Err(error) = result
                        && !error.is_cancelled()
                    {
                        log::warn!("Gate.io WebSocket command task failed: {error}");
                    }
                }
                () = tokio::time::sleep(COMMAND_TASK_SHUTDOWN_TIMEOUT) => {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
        self.inner.active.store(false, Ordering::Release);
    }

    /// Connects using Nautilus' shared WebSocket client.
    pub async fn connect(&self) -> anyhow::Result<()> {
        if self.is_active() {
            return Ok(());
        }

        self.shutdown_command_loop(false).await;
        self.inner.stopping.store(false, Ordering::Release);
        let (message_handler, mut raw_rx) = channel_message_handler();
        let config = WebSocketConfig {
            url: self.inner.url.clone(),
            headers: vec![("X-Gate-Size-Decimal".to_string(), "1".to_string())],
            heartbeat: Some(self.inner.heartbeat_interval.as_secs().max(1)),
            heartbeat_msg: None,
            reconnect_timeout_ms: Some(5_000),
            reconnect_delay_initial_ms: Some(250),
            reconnect_delay_max_ms: Some(5_000),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(250),
            reconnect_max_attempts: None,
            idle_timeout_ms: Some(
                self.inner
                    .heartbeat_interval
                    .as_millis()
                    .saturating_mul(3)
                    .try_into()
                    .unwrap_or(u64::MAX),
            ),
            backend: self.inner.transport_backend,
            proxy_url: self.inner.proxy_url.clone(),
        };
        let client = tokio::time::timeout(
            INITIAL_CONNECTION_TIMEOUT,
            WebSocketClient::connect(config, Some(message_handler), None, None, vec![], None),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Gate.io WebSocket connection timed out: {}", self.url()))??;

        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let event_tx = self.inner.event_tx.clone();
        let inner = Arc::clone(&self.inner);
        let command_task = get_runtime().spawn(async move {
            let mut pending_acks = BTreeMap::new();
            let mut restore_pending = HashSet::new();
            let mut restore_started = None;
            let mut heartbeat = tokio::time::interval(inner.heartbeat_interval);
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            heartbeat.tick().await;
            inner.active.store(true, Ordering::Release);

            loop {
                tokio::select! {
                    command = command_rx.recv() => {
                        match command {
                            Some(Command::Subscribe { subscription, ack }) => {
                                let request_id = next_request_id(&inner);
                                if let Some(ack) = ack {
                                    pending_acks.insert(
                                        request_id,
                                        PendingAck { event: "subscribe", sender: ack },
                                    );
                                }
                                if let Err(error) = send_subscription(
                                    &client,
                                    &inner,
                                    &subscription,
                                    true,
                                    request_id,
                                ).await {
                                    resolve_one_subscription_ack(
                                        &mut pending_acks,
                                        request_id,
                                        Err(error.to_string()),
                                    );
                                    log::warn!(
                                        "Failed to subscribe to Gate.io WebSocket channel {}: {error}",
                                        subscription.channel
                                    );
                                }
                            }
                            Some(Command::Unsubscribe { subscription, ack }) => {
                                let request_id = next_request_id(&inner);
                                if let Some(ack) = ack {
                                    pending_acks.insert(
                                        request_id,
                                        PendingAck { event: "unsubscribe", sender: ack },
                                    );
                                }
                                if let Err(error) = send_subscription(
                                    &client,
                                    &inner,
                                    &subscription,
                                    false,
                                    request_id,
                                ).await {
                                    resolve_one_subscription_ack(
                                        &mut pending_acks,
                                        request_id,
                                        Err(error.to_string()),
                                    );
                                    log::warn!(
                                        "Failed to unsubscribe from Gate.io WebSocket channel {}: {error}",
                                        subscription.channel
                                    );
                                }
                            }
                            Some(Command::Send(value)) => {
                                if let Err(error) = client.send_text(value.to_string(), None).await {
                                    log::warn!("Failed to send Gate.io WebSocket message: {error}");
                                    fail_pending_acks(&mut pending_acks, error.to_string());
                                }
                            }
                            Some(Command::Disconnect) | None => {
                                inner.stopping.store(true, Ordering::Release);
                                fail_pending_acks(
                                    &mut pending_acks,
                                    "Gate.io WebSocket disconnected",
                                );
                                client.disconnect().await;
                                break;
                            }
                        }
                    }
                    _ = heartbeat.tick() => {
                        let message = json!({
                            "time": Utc::now().timestamp(),
                            "channel": inner.ping_channel,
                        });
                        if let Err(error) = client.send_text(message.to_string(), None).await {
                            log::debug!("Failed to send Gate.io heartbeat: {error}");
                        }
                    }
                    message = raw_rx.recv() => {
                        let Some(message) = message else {
                            fail_pending_acks(
                                &mut pending_acks,
                                "Gate.io WebSocket message handler closed",
                            );
                            break;
                        };

                        let payload = match message {
                            Message::Text(text) => {
                                if text.as_str() == RECONNECTED {
                                    let subscriptions = inner
                                        .subscriptions
                                        .read()
                                        .await
                                        .values()
                                        .cloned()
                                        .collect::<Vec<_>>();
                                    fail_pending_acks(
                                        &mut pending_acks,
                                        "Gate.io WebSocket connection was re-established",
                                    );
                                    restore_pending.clear();
                                    restore_started = Some(tokio::time::Instant::now());
                                    for subscription in subscriptions {
                                        let request_id = next_request_id(&inner);
                                        let (ack_tx, _ack_rx) = oneshot::channel();
                                        pending_acks.insert(
                                            request_id,
                                            PendingAck {
                                                event: "subscribe",
                                                sender: ack_tx,
                                            },
                                        );
                                        restore_pending.insert(request_id);
                                        if let Err(error) = send_subscription(
                                            &client,
                                            &inner,
                                            &subscription,
                                            true,
                                            request_id,
                                        ).await {
                                            resolve_one_subscription_ack(
                                                &mut pending_acks,
                                                request_id,
                                                Err(error.to_string()),
                                            );
                                            restore_pending.remove(&request_id);
                                            log::warn!(
                                                "Failed to restore Gate.io WebSocket subscription {}: {error}",
                                                subscription.channel
                                            );
                                        }
                                    }
                                    if restore_pending.is_empty() {
                                        restore_started = None;
                                        let _ = event_tx.send(GateioWsMessage::reconnected());
                                    }
                                    continue;
                                }
                                serde_json::from_str::<GateioWsMessage>(text.as_ref())
                            }
                            Message::Binary(bytes) => {
                                serde_json::from_slice::<GateioWsMessage>(&bytes)
                            }
                            Message::Ping(_) | Message::Pong(_) | Message::Close(_) => continue,
                            Message::Frame(_) => continue,
                        };

                        match payload {
                            Ok(message) => {
                                let restore_id =
                                    message.id.filter(|id| restore_pending.contains(id));
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
                                        restore_pending.clear();
                                        restore_started = None;
                                    } else if restore_pending.is_empty() {
                                        restore_started = None;
                                        let _ = event_tx.send(GateioWsMessage::reconnected());
                                    }
                                }
                                let _ = event_tx.send(message);
                            }
                            Err(error) => {
                                log::debug!(
                                    "Ignoring invalid Gate.io WebSocket message: {error}"
                                );
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(
                        restore_started
                            .unwrap_or_else(|| tokio::time::Instant::now() + SUBSCRIPTION_ACK_TIMEOUT),
                    ), if !restore_pending.is_empty() => {
                        log::warn!(
                            "Gate.io WebSocket subscription restoration timed out with {} pending ACKs",
                            restore_pending.len()
                        );
                        fail_pending_acks(
                            &mut pending_acks,
                            "Gate.io WebSocket subscription restoration timed out",
                        );
                        restore_pending.clear();
                        restore_started = None;
                    }
                }
            }

            inner.active.store(false, Ordering::Release);
        });

        {
            let mut tx = self.inner.command_tx.write().await;
            *tx = Some(command_tx);
        }
        {
            let mut task = self.inner.command_task.write().await;
            *task = Some(command_task);
        }
        self.inner.active.store(true, Ordering::Release);
        Ok(())
    }

    /// Disconnects the socket and stops reconnect attempts.
    pub async fn disconnect(&self) -> anyhow::Result<()> {
        self.shutdown_command_loop(true).await;
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

    /// Subscribes to a public or private channel and waits for Gate.io's ACK.
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

    /// Unsubscribes from a public or private channel and waits for Gate.io's ACK.
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

async fn send_subscription(
    client: &WebSocketClient,
    inner: &Inner,
    subscription: &Subscription,
    subscribe: bool,
    request_id: i64,
) -> anyhow::Result<()> {
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
    client
        .send_text(message.to_string(), None)
        .await
        .map_err(|error| anyhow::anyhow!("WebSocket send failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GateioDataClientConfig;

    #[test]
    fn public_client_uses_configured_url_and_network_options() {
        let config = GateioDataClientConfig {
            base_url_ws: Some("wss://example.test/ws".to_string()),
            proxy_url: Some("http://proxy.test:8080".to_string()),
            transport_backend: TransportBackend::Tungstenite,
            ..Default::default()
        };
        let client = GateioWebSocketClient::new_public(&config);
        assert_eq!(client.url(), "wss://example.test/ws");
        assert_eq!(
            client.inner.proxy_url.as_deref(),
            Some("http://proxy.test:8080")
        );
        assert_eq!(
            client.inner.transport_backend,
            TransportBackend::Tungstenite
        );
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
