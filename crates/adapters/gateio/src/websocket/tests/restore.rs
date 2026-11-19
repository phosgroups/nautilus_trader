use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{WebSocketStream, accept_async};

use super::*;

async fn accept(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    accept_async(listener.accept().await.unwrap().0)
        .await
        .unwrap()
}

async fn subscription(ws: &mut WebSocketStream<TcpStream>) -> Value {
    let frame = ws.next().await.unwrap().unwrap();
    let request: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
    assert_eq!(request["event"], "subscribe");
    request
}

async fn acknowledge(ws: &mut WebSocketStream<TcpStream>, request: &Value, accepted: bool) {
    let mut ack = json!({
        "id": request["id"],
        "channel": request["channel"],
        "event": "subscribe",
        "result": {"status": "success"},
    });
    if !accepted {
        ack["result"] = Value::Null;
        ack["error"] = json!({"code": 2, "message": "subscription unavailable"});
    }
    ws.send(Message::Text(ack.to_string().into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn reconnect_retries_failed_restore_before_reporting_ready() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = GateioWebSocketClient::new_public(&GateioDataClientConfig {
            base_url_ws: Some(format!("ws://{}", listener.local_addr().unwrap())),
            transport_backend: TransportBackend::Tungstenite,
            heartbeat_interval_secs: 60,
            ..Default::default()
        });
        let (connected, mut initial) = tokio::join!(client.connect(), accept(&listener));
        connected.unwrap();
        for channel in ["spot.trades", "spot.tickers"] {
            let (result, ()) = tokio::join!(
                client.subscribe(channel, vec!["BTC_USDT".into()], false),
                async {
                    let request = subscription(&mut initial).await;
                    assert_eq!(request["channel"], channel);
                    acknowledge(&mut initial, &request, true).await;
                },
            );
            result.unwrap();
        }

        let mut events = client.take_event_receiver();
        drop(initial);
        let mut failed = accept(&listener).await;
        let first = subscription(&mut failed).await;
        let second = subscription(&mut failed).await;
        assert_ne!(first["channel"], second["channel"]);
        acknowledge(&mut failed, &first, true).await;
        assert_eq!(events.recv().await.unwrap().id, first["id"].as_i64());
        assert!(!client.is_active());
        acknowledge(&mut failed, &second, false).await;
        assert!(events.recv().await.unwrap().error.is_some());
        assert!(!client.is_active());

        // Keep the rejected socket open. Recovery must be initiated by the client.
        let mut recovered = accept(&listener).await;
        let retry_first = subscription(&mut recovered).await;
        let retry_second = subscription(&mut recovered).await;
        assert_eq!(retry_first["channel"], first["channel"]);
        assert_eq!(retry_second["channel"], second["channel"]);
        acknowledge(&mut recovered, &retry_first, true).await;
        assert_eq!(events.recv().await.unwrap().id, retry_first["id"].as_i64());
        assert!(!client.is_active());
        acknowledge(&mut recovered, &retry_second, true).await;
        assert_eq!(
            events.recv().await.unwrap().channel,
            GATEIO_INTERNAL_RECONNECTED_CHANNEL
        );
        assert!(client.is_active());
        client.disconnect().await.unwrap();
    })
    .await
    .expect("timed out restoring Gate.io subscriptions");
}
