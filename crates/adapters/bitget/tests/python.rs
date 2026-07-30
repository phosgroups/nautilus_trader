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

#![cfg(feature = "python")]

use std::{cell::RefCell, ffi::CString, net::SocketAddr, rc::Rc};

use axum::{
    Json, Router,
    response::{IntoResponse, Response},
    routing::get,
};
use nautilus_bitget::{
    common::{
        consts::BITGET,
        enums::{BitgetEnvironment, BitgetProductType},
        parse::parse_usdt_perp_instrument,
    },
    config::{BitgetDataClientConfig, BitgetExecClientConfig},
    factories::{BitgetDataClientFactory, BitgetExecutionClientFactory},
    http::{client::BitgetHttpClient, models::BitgetMixContract},
    python,
};
use nautilus_common::{cache::Cache, clock::TestClock};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::OmsType,
    identifiers::{AccountId, ClientId, InstrumentId, TraderId},
    instruments::InstrumentAny,
};
use nautilus_system::get_global_pyo3_registry;
use pyo3::{
    Py, PyAny, PyResult, Python,
    types::{PyAnyMethods, PyModule},
};
use rstest::rstest;
use serde_json::json;

#[rstest]
fn bitget_python_factories_extract_from_registry() {
    Python::initialize();

    Python::attach(|py| {
        register_bitget_python_module(py);
        assert_data_factory_extracts_from_python_object(py);
        assert_exec_factory_extracts_from_python_object(py);
    });
}

#[rstest]
fn bitget_http_client_exposes_common_python_methods() {
    Python::initialize();

    Python::attach(|py| {
        let client = Py::new(
            py,
            nautilus_bitget::http::client::BitgetHttpClient::default(),
        )
        .expect("BitgetHttpClient should convert to Python object")
        .into_any();
        let client = client.bind(py);

        for method in [
            "cancel_all_requests",
            "is_initialized",
            "get_cached_symbols",
            "cache_instrument",
            "cache_instruments",
            "request_orderbook_snapshot",
            "request_trades",
            "request_funding_rates",
            "request_bars",
            "request_account_state",
            "request_order_status_report",
            "request_order_status_reports",
            "request_fill_reports",
            "request_position_status_reports",
            "submit_order",
            "cancel_order",
        ] {
            assert!(
                client
                    .hasattr(method)
                    .expect("attribute lookup should succeed"),
                "BitgetHttpClient missing Python method {method}",
            );
        }

        client
            .call_method0("cancel_all_requests")
            .expect("cancel_all_requests should be callable");
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn bitget_python_report_methods_parse_null_payloads_as_empty_lists() {
    Python::initialize();

    let addr = start_null_payload_fixture_server().await;
    let client = BitgetHttpClient::with_credentials(
        "key".to_string(),
        "secret".to_string(),
        "passphrase".to_string(),
        Some(format!("http://{addr}")),
        5,
        None,
    )
    .expect("BitgetHttpClient should be created");
    client.cache_instrument(usdt_perp_instrument());

    Python::attach(|py| {
        let client = Py::new(py, client)
            .expect("BitgetHttpClient should convert to Python object")
            .into_any();
        let fills_len = run_report_method(py, &client, "request_fill_reports")
            .expect("request_fill_reports should parse null payload");
        let positions_len = run_report_method(py, &client, "request_position_status_reports")
            .expect("request_position_status_reports should parse null payload");
        let orders_len = run_report_method(py, &client, "request_order_status_reports")
            .expect("request_order_status_reports should parse null payload");

        assert_eq!(fills_len, 0);
        assert_eq!(positions_len, 0);
        assert_eq!(orders_len, 0);
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn bitget_python_report_methods_parse_null_list_payloads_as_empty_lists() {
    Python::initialize();

    let addr = start_null_list_payload_fixture_server().await;
    let client = BitgetHttpClient::with_credentials(
        "key".to_string(),
        "secret".to_string(),
        "passphrase".to_string(),
        Some(format!("http://{addr}")),
        5,
        None,
    )
    .expect("BitgetHttpClient should be created");
    client.cache_instrument(usdt_perp_instrument());

    Python::attach(|py| {
        let client = Py::new(py, client)
            .expect("BitgetHttpClient should convert to Python object")
            .into_any();
        let fills_len = run_report_method(py, &client, "request_fill_reports")
            .expect("request_fill_reports should parse null list payload");
        let positions_len = run_report_method(py, &client, "request_position_status_reports")
            .expect("request_position_status_reports should parse null list payload");
        let orders_len = run_report_method(py, &client, "request_order_status_reports")
            .expect("request_order_status_reports should parse null list payload");

        assert_eq!(fills_len, 0);
        assert_eq!(positions_len, 0);
        assert_eq!(orders_len, 0);
    });
}

fn register_bitget_python_module(py: Python<'_>) {
    let module = PyModule::new(py, "bitget").expect("Bitget module should be created");
    python::bitget(py, &module).expect("Bitget Python module should register");
}

async fn start_null_payload_fixture_server() -> SocketAddr {
    let router = Router::new()
        .route("/api/v3/trade/fills", get(handle_null_payload))
        .route("/api/v3/trade/history-orders", get(handle_null_payload))
        .route(
            "/api/v3/position/current-position",
            get(handle_null_payload),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fixture server should bind");
    let addr = listener
        .local_addr()
        .expect("fixture server should have addr");

    tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
            .await
            .expect("fixture server should run");
    });

    addr
}

async fn start_null_list_payload_fixture_server() -> SocketAddr {
    let router = Router::new()
        .route("/api/v3/trade/fills", get(handle_null_list_payload))
        .route(
            "/api/v3/trade/history-orders",
            get(handle_null_list_payload),
        )
        .route(
            "/api/v3/position/current-position",
            get(handle_null_list_payload),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fixture server should bind");
    let addr = listener
        .local_addr()
        .expect("fixture server should have addr");

    tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
            .await
            .expect("fixture server should run");
    });

    addr
}

async fn handle_null_payload() -> Response {
    Json(json!({
        "code": "00000",
        "msg": "success",
        "requestTime": 1700000000000i64,
        "data": null
    }))
    .into_response()
}

async fn handle_null_list_payload() -> Response {
    Json(json!({
        "code": "00000",
        "msg": "success",
        "requestTime": 1700000000000i64,
        "data": {
            "list": null,
            "cursor": null,
        }
    }))
    .into_response()
}

fn run_report_method(py: Python<'_>, client: &Py<PyAny>, method: &str) -> PyResult<usize> {
    let asyncio = py.import("asyncio")?;
    let event_loop = asyncio.call_method0("new_event_loop")?;
    let module = PyModule::from_code(
        py,
        &CString::new(
            r#"
async def call_report(client, method, account_id, product_type, instrument_id):
    return await getattr(client, method)(account_id, product_type, instrument_id)
"#,
        )
        .expect("Python code should not contain nul bytes"),
        &CString::new("bitget_report_test.py").expect("file name should not contain nul bytes"),
        &CString::new("bitget_report_test").expect("module name should not contain nul bytes"),
    )?;
    let account_id = Py::new(py, AccountId::from("BITGET-001"))?.into_any();
    let product_type = Py::new(py, BitgetProductType::UsdtFutures)?.into_any();
    let instrument_id = Py::new(py, InstrumentId::from("BTCUSDT-PERP.BITGET"))?.into_any();
    let awaitable = module.getattr("call_report")?.call1((
        client.bind(py),
        method,
        account_id,
        product_type,
        instrument_id,
    ))?;

    let result = event_loop.call_method1("run_until_complete", (awaitable,))?;
    let len = result.len()?;
    event_loop.call_method0("close")?;

    Ok(len)
}

fn usdt_perp_instrument() -> InstrumentAny {
    let definition = BitgetMixContract {
        symbol: "BTCUSDT".to_string(),
        base_coin: "BTC".to_string(),
        quote_coin: "USDT".to_string(),
        product_type: Some("USDT-FUTURES".to_string()),
        symbol_type: Some("perpetual".to_string()),
        contract_type: Some("perpetual".to_string()),
        margin_coin: Some("USDT".to_string()),
        maker_fee_rate: Some("0.0002".to_string()),
        taker_fee_rate: Some("0.0006".to_string()),
        min_trade_num: Some("0.001".to_string()),
        min_trade_usdt: Some("5".to_string()),
        max_order_qty: Some("1000".to_string()),
        size_multiplier: Some("0.001".to_string()),
        price_place: Some("1".to_string()),
        volume_place: Some("3".to_string()),
        price_end_step: Some("1".to_string()),
        max_lever: Some("125".to_string()),
        min_lever: Some("1".to_string()),
        fund_interval: Some("8".to_string()),
        symbol_status: Some("normal".to_string()),
    };

    parse_usdt_perp_instrument(&definition, UnixNanos::default(), UnixNanos::default())
        .expect("Bitget test instrument should parse")
}

fn assert_data_factory_extracts_from_python_object(py: Python<'_>) {
    let factory = Py::new(py, BitgetDataClientFactory::new())
        .expect("factory should convert to Python object")
        .into_any();
    let config = Py::new(
        py,
        BitgetDataClientConfig {
            product_type: BitgetProductType::Spot,
            environment: BitgetEnvironment::Mainnet,
            ..BitgetDataClientConfig::default()
        },
    )
    .expect("config should convert to Python object")
    .into_any();
    let registry = get_global_pyo3_registry();

    let extracted_factory = registry
        .extract_factory(py, factory)
        .expect("data factory should extract");
    let extracted_config = registry
        .extract_config(py, config)
        .expect("data config should extract");
    let bitget_config = extracted_config
        .as_any()
        .downcast_ref::<BitgetDataClientConfig>()
        .expect("data config should downcast");
    let cache = Rc::new(RefCell::new(Cache::default()));
    let clock = Rc::new(RefCell::new(TestClock::new()));
    let client = extracted_factory
        .create(
            "BITGET-DATA-EXTRACTED",
            extracted_config.as_ref(),
            cache.into(),
            clock,
        )
        .expect("extracted factory should create data client");

    assert_eq!(extracted_factory.name(), BITGET);
    assert_eq!(extracted_factory.config_type(), "BitgetDataClientConfig");
    assert_eq!(bitget_config.product_type, BitgetProductType::Spot);
    assert_eq!(client.client_id(), ClientId::from("BITGET-DATA-EXTRACTED"));
}

fn assert_exec_factory_extracts_from_python_object(py: Python<'_>) {
    let trader_id = TraderId::from("TRADER-001");
    let account_id = AccountId::from("BITGET-001");
    let factory = Py::new(py, BitgetExecutionClientFactory::new(trader_id, account_id))
        .expect("factory should convert to Python object")
        .into_any();
    let config = Py::new(
        py,
        BitgetExecClientConfig {
            account_id: Some(account_id),
            product_type: BitgetProductType::UsdtFutures,
            environment: BitgetEnvironment::Mainnet,
            api_key: Some("test_key".to_string()),
            api_secret: Some("test_secret".to_string()),
            api_passphrase: Some("test_passphrase".to_string()),
            ..BitgetExecClientConfig::default()
        },
    )
    .expect("config should convert to Python object")
    .into_any();
    let registry = get_global_pyo3_registry();

    let extracted_factory = registry
        .extract_exec_factory(py, factory)
        .expect("exec factory should extract");
    let extracted_config = registry
        .extract_config(py, config)
        .expect("exec config should extract");
    let bitget_config = extracted_config
        .as_any()
        .downcast_ref::<BitgetExecClientConfig>()
        .expect("exec config should downcast");
    let cache = Rc::new(RefCell::new(Cache::default()));
    let client = extracted_factory
        .create(
            "BITGET-EXEC-EXTRACTED",
            extracted_config.as_ref(),
            cache.into(),
        )
        .expect("extracted factory should create exec client");

    assert_eq!(extracted_factory.name(), BITGET);
    assert_eq!(extracted_factory.config_type(), "BitgetExecClientConfig");
    assert_eq!(bitget_config.account_id, Some(account_id));
    assert_eq!(bitget_config.product_type, BitgetProductType::UsdtFutures);
    assert_eq!(client.client_id(), ClientId::from("BITGET-EXEC-EXTRACTED"));
    assert_eq!(client.account_id(), account_id);
    assert_eq!(client.oms_type(), OmsType::Netting);
}
