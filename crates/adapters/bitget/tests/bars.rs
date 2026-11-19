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

//! Exercises the Nautilus bar request against both Bitget spot candle protocols.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{Json, Router, http::Uri, routing::get};
use chrono::{DateTime, Duration, Utc};
use nautilus_bitget::{
    common::{
        enums::{BitgetAccountMode, BitgetProductType},
        parse::parse_spot_instrument,
    },
    http::client::BitgetHttpClient,
};
use nautilus_model::data::Bar;
use rstest::rstest;
use serde_json::{Value, json};

const MINUTE_MS: i64 = 60_000;

struct CandleServer {
    client: BitgetHttpClient,
    requests: Arc<Mutex<Vec<Uri>>>,
}

impl CandleServer {
    async fn new(mode: BitgetAccountMode, pages: Vec<Vec<Value>>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        let pages = Arc::new(pages);
        let app = Router::new().fallback(get(move |uri: Uri| {
            let mut requests = log.lock().unwrap();
            let page = pages.get(requests.len()).cloned().unwrap_or_default();
            requests.push(uri);
            async move { Json(json!({"code": "00000", "msg": "success", "data": page})) }
        }));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = BitgetHttpClient::new(Some(base_url), 5, None)
            .unwrap()
            .with_account_mode(mode);
        Self { client, requests }
    }

    async fn bars(
        &self,
        start: Option<i64>,
        end: Option<i64>,
        limit: Option<u32>,
        on_close: bool,
    ) -> Vec<Bar> {
        let symbol = serde_json::from_value(json!({
            "symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT",
            "pricePrecision": "2", "quantityPrecision": "6"
        }))
        .unwrap();
        let instrument = parse_spot_instrument(&symbol, 0.into(), 0.into()).unwrap();
        self.client
            .request_bars(
                BitgetProductType::Spot,
                &instrument,
                "BTCUSDT.BITGET-1-MINUTE-LAST-EXTERNAL".parse().unwrap(),
                start.map(|ms| DateTime::from_timestamp_millis(ms).unwrap()),
                end.map(|ms| DateTime::from_timestamp_millis(ms).unwrap()),
                limit,
                on_close,
            )
            .await
            .unwrap()
    }
}

fn minute_aligned_days_ago(days: i64) -> i64 {
    (Utc::now() - Duration::days(days)).timestamp_millis() / MINUTE_MS * MINUTE_MS
}

fn candles(base: i64, offsets: impl IntoIterator<Item = i64>) -> Vec<Value> {
    offsets
        .into_iter()
        .map(|offset| {
            json!([
                (base + offset * MINUTE_MS).to_string(),
                "100.00",
                "102.00",
                "99.00",
                "101.00",
                "1.000000",
                "101.00"
            ])
        })
        .collect()
}

fn event_times(bars: &[Bar]) -> Vec<i64> {
    bars.iter()
        .map(|bar| i64::try_from(bar.ts_event.as_millis()).unwrap())
        .collect()
}

#[rstest]
#[case::uta(BitgetAccountMode::Uta, "/api/v3/market/candles")]
#[case::classic(BitgetAccountMode::Classic, "/api/v2/spot/market/candles")]
#[tokio::test]
async fn recent_bars_keep_latest_unique_in_range(
    #[case] mode: BitgetAccountMode,
    #[case] path: &str,
) {
    let base = minute_aligned_days_ago(1);
    let server = CandleServer::new(mode, vec![candles(base, [3, 0, 2, 2, 4, -1, 1])]).await;
    let bars = server
        .bars(Some(base), Some(base + 3 * MINUTE_MS), Some(2), true)
        .await;
    // The window is expressed in open times. The returned events are stamped at the close.
    assert_eq!(
        event_times(&bars),
        [base + 3 * MINUTE_MS, base + 4 * MINUTE_MS]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path(), path);
}

#[rstest]
#[case::uta(BitgetAccountMode::Uta, "/api/v3/market/history-candles", 100)]
#[case::classic(BitgetAccountMode::Classic, "/api/v2/spot/market/history-candles", 200)]
#[tokio::test]
async fn archived_bars_page_backwards_and_merge_overlapping_rows(
    #[case] mode: BitgetAccountMode,
    #[case] path: &str,
    #[case] cap: i64,
    #[values(false, true)] limited: bool,
) {
    let base = minute_aligned_days_ago(45);
    let last = cap + 2;
    let mut first_page = candles(base, 3..=last);
    first_page.rotate_left(2);
    let server = CandleServer::new(mode, vec![first_page, candles(base, [3, 2, 0, -1, 1])]).await;
    let limit = limited.then(|| u32::try_from(cap + 2).unwrap());
    let bars = server
        .bars(Some(base), Some(base + last * MINUTE_MS), limit, false)
        .await;
    let first = if limited { 1 } else { 0 };
    let expected: Vec<_> = (first..=last).map(|i| base + i * MINUTE_MS).collect();
    assert_eq!(event_times(&bars), expected);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let mut cursors = Vec::new();
    for request in requests.iter() {
        assert_eq!(request.path(), path);
        let params: HashMap<String, String> =
            serde_urlencoded::from_str(request.query().unwrap()).unwrap();
        assert_eq!(params["symbol"], "BTCUSDT");
        if mode == BitgetAccountMode::Classic {
            assert_eq!(params["granularity"], "1min");
            assert!(!params.contains_key("startTime"));
        } else {
            assert_eq!(params["category"], "SPOT");
            assert_eq!(params["interval"], "1m");
        }
        if let Some(limit) = params.get("limit") {
            assert!((1..=cap).contains(&limit.parse::<i64>().unwrap()));
        }
        cursors.push(params["endTime"].parse::<i64>().unwrap());
    }
    assert!(cursors[0] > base + last * MINUTE_MS);
    assert!((base + 2 * MINUTE_MS + 1..=base + 3 * MINUTE_MS).contains(&cursors[1]));
}

#[rstest]
#[case::uta(BitgetAccountMode::Uta)]
#[case::classic(BitgetAccountMode::Classic)]
#[tokio::test]
async fn unbounded_request_fetches_a_single_recent_page(#[case] mode: BitgetAccountMode) {
    let base = minute_aligned_days_ago(1);
    let server = CandleServer::new(mode, vec![candles(base, 0..1_000)]).await;
    let bars = server.bars(None, None, None, false).await;
    assert_eq!(bars.len(), 1_000);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].path().ends_with("/market/candles"));
}

#[rstest]
#[case::uta(BitgetAccountMode::Uta, "/api/v3/market")]
#[case::classic(BitgetAccountMode::Classic, "/api/v2/spot/market")]
#[tokio::test]
async fn stalled_recent_cursor_continues_in_archive_until_no_older_rows(
    #[case] mode: BitgetAccountMode,
    #[case] prefix: &str,
) {
    let base = minute_aligned_days_ago(1);
    let pages = vec![
        candles(base, [4, 3]), // Recent returns fewer than the requested six.
        candles(base, [3]),    // Recent repeats its oldest row.
        candles(base, [2, 3]), // Archive overlaps recent data and resumes progress.
        candles(base, [2]),    // Archive also exhausts its rows before the limit.
    ];
    let server = CandleServer::new(mode, pages).await;
    let bars = server
        .bars(None, Some(base + 4 * MINUTE_MS), Some(6), false)
        .await;
    assert_eq!(
        event_times(&bars),
        [
            base + 2 * MINUTE_MS,
            base + 3 * MINUTE_MS,
            base + 4 * MINUTE_MS
        ]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    for (uri, (route, end)) in requests.iter().zip([
        ("candles", base + 4 * MINUTE_MS),
        ("candles", base + 3 * MINUTE_MS - 1),
        ("history-candles", base + 3 * MINUTE_MS),
        ("history-candles", base + 2 * MINUTE_MS),
    ]) {
        assert_eq!(uri.path(), format!("{prefix}/{route}"));
        let params: HashMap<String, String> =
            serde_urlencoded::from_str(uri.query().unwrap()).unwrap();
        assert_eq!(params["endTime"], end.to_string());
    }
}
