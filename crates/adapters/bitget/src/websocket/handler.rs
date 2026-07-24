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

//! Inner Bitget WebSocket feed handler.

use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use nautilus_network::websocket::{AuthTracker, TEXT_PING, TEXT_PONG, WebSocketClient};
use tokio_tungstenite::tungstenite::Message;

use crate::websocket::{
    error::BitgetWsError,
    messages::{BitgetWsMessage, BitgetWsOp},
};

/// Commands sent from the outer client to the inner feed handler.
pub(super) enum HandlerCommand {
    /// Hand the live `WebSocketClient` to the handler after network connect.
    SetClient(WebSocketClient),
    /// Disconnect and stop the handler.
    Disconnect,
    /// Send a login payload.
    Login { payload: String },
    /// Send a subscription payload.
    Subscribe { payload: String },
    /// Send an unsubscription payload.
    Unsubscribe { payload: String },
    /// Send an arbitrary text payload.
    SendText { payload: String },
}

impl Debug for HandlerCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SetClient(_) => f.write_str("SetClient(<WebSocketClient>)"),
            Self::Disconnect => f.write_str("Disconnect"),
            Self::Login { .. } => f.write_str("Login(<redacted>)"),
            Self::Subscribe { payload } => f.debug_tuple("Subscribe").field(payload).finish(),
            Self::Unsubscribe { payload } => f.debug_tuple("Unsubscribe").field(payload).finish(),
            Self::SendText { payload } => f.debug_tuple("SendText").field(payload).finish(),
        }
    }
}

pub(super) struct BitgetWsFeedHandler {
    signal: Arc<AtomicBool>,
    inner: Option<WebSocketClient>,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    out_tx: tokio::sync::mpsc::UnboundedSender<BitgetWsMessage>,
    auth_tracker: AuthTracker,
}

impl BitgetWsFeedHandler {
    /// Creates a new handler instance.
    pub(super) fn new(
        signal: Arc<AtomicBool>,
        cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
        out_tx: tokio::sync::mpsc::UnboundedSender<BitgetWsMessage>,
        auth_tracker: AuthTracker,
    ) -> Self {
        Self {
            signal,
            inner: None,
            cmd_rx,
            raw_rx,
            out_tx,
            auth_tracker,
        }
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.signal.load(Ordering::Relaxed)
    }

    pub(super) fn send(&self, msg: BitgetWsMessage) -> Result<(), String> {
        self.out_tx
            .send(msg)
            .map_err(|e| format!("Failed to send WebSocket message: {e}"))
    }

    async fn send_with_retry(&self, payload: String) -> Result<(), BitgetWsError> {
        if let Some(client) = &self.inner {
            client.send_text(payload, None).await?;
            Ok(())
        } else {
            Err(BitgetWsError::NotConnected)
        }
    }

    fn update_auth_state(&self, msg: &BitgetWsMessage) {
        match msg {
            BitgetWsMessage::Login(event) if event.code.as_deref().unwrap_or("0") == "0" => {
                self.auth_tracker.succeed();
                log::debug!("Bitget WebSocket authenticated");
            }
            BitgetWsMessage::Login(event) => {
                let message = event
                    .msg
                    .clone()
                    .unwrap_or_else(|| "Bitget WebSocket authentication failed".to_string());
                self.auth_tracker.fail(message.clone());
                log::error!("Bitget WebSocket authentication failed: {message}");
            }
            BitgetWsMessage::Error(event) if !self.auth_tracker.is_authenticated() => {
                let message = event
                    .msg
                    .clone()
                    .or_else(|| event.code.clone())
                    .unwrap_or_else(|| "Bitget WebSocket error before authentication".to_string());
                self.auth_tracker.fail(message);
            }
            _ => {}
        }
    }

    async fn parse_raw_message(&self, msg: Message) -> Option<BitgetWsMessage> {
        match msg {
            Message::Text(text) => {
                let text = text.as_str();

                if text == TEXT_PING {
                    if let Err(e) = self.send_with_retry(TEXT_PONG.to_string()).await {
                        log::warn!("Failed to send text pong to Bitget: {e}");
                    }
                    return None;
                }

                match BitgetWsMessage::parse_text(text) {
                    Ok(msg) => Some(msg),
                    Err(e) => {
                        log::warn!("Failed to parse Bitget WebSocket text frame: {e}");
                        None
                    }
                }
            }
            Message::Binary(data) => match std::str::from_utf8(data.as_ref()) {
                Ok(text) => match BitgetWsMessage::parse_text(text) {
                    Ok(msg) => Some(msg),
                    Err(e) => {
                        log::warn!("Failed to parse Bitget WebSocket binary text frame: {e}");
                        None
                    }
                },
                Err(e) => {
                    log::warn!("Dropping non-UTF8 Bitget WebSocket binary frame: {e}");
                    None
                }
            },
            Message::Ping(data) => {
                log::trace!("Received Bitget ping frame with {} bytes", data.len());

                if let Some(client) = &self.inner
                    && let Err(e) = client.send_pong(data.to_vec()).await
                {
                    log::warn!("Failed to send WebSocket pong frame to Bitget: {e}");
                }
                None
            }
            Message::Pong(_) => Some(BitgetWsMessage::Pong),
            Message::Close(frame) => {
                log::debug!("Bitget WebSocket close frame received: {frame:?}");
                None
            }
            Message::Frame(_) => None,
        }
    }

    pub(super) async fn next(&mut self) -> Option<BitgetWsMessage> {
        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        HandlerCommand::SetClient(client) => {
                            self.inner = Some(client);
                        }
                        HandlerCommand::Disconnect => {
                            if let Some(client) = self.inner.take() {
                                client.disconnect().await;
                            }
                            return None;
                        }
                        HandlerCommand::Login { payload }
                        | HandlerCommand::Subscribe { payload }
                        | HandlerCommand::Unsubscribe { payload }
                        | HandlerCommand::SendText { payload } => {
                            if let Err(e) = self.send_with_retry(payload).await {
                                log::error!("Failed to send Bitget WebSocket command: {e}");
                            }
                        }
                    }
                }

                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    if self.signal.load(Ordering::Relaxed) {
                        return None;
                    }
                }

                raw = self.raw_rx.recv() => {
                    let raw = raw?;
                    let Some(msg) = self.parse_raw_message(raw).await else {
                        continue;
                    };

                    if let Some(event) = msg.event()
                        && let Some(arg) = &event.arg
                    {
                        match &msg {
                            BitgetWsMessage::Subscribe(_) => {
                                log::debug!(
                                    "Bitget WebSocket subscribed: op={:?}, arg={:?}",
                                    BitgetWsOp::Subscribe,
                                    arg,
                                );
                            }
                            BitgetWsMessage::Unsubscribe(_) => {
                                log::debug!(
                                    "Bitget WebSocket unsubscribed: op={:?}, arg={:?}",
                                    BitgetWsOp::Unsubscribe,
                                    arg,
                                );
                            }
                            _ => {}
                        }
                    }

                    self.update_auth_state(&msg);
                    return Some(msg);
                }
            }
        }
    }
}
