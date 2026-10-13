# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from nautilus_trader.adapters.bitget.config import BitgetExecClientConfig
from nautilus_trader.adapters.bitget.execution import BitgetExecutionClient
from nautilus_trader.adapters.bitget.providers import BitgetInstrumentProvider
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.trading.strategy import Strategy


pytestmark = pytest.mark.live_components


def push(topic, *rows):
    return {
        "type": "data",
        "arg": {"instType": "usdt-futures", "topic": topic, "symbol": "default"},
        "data": list(rows),
    }


def order_row(
    *, filled="0.010", status="filled", venue_id="O-REAL", trade_id=None, latest_qty=None
):
    row = {
        "symbol": "BTCUSDT",
        "orderId": venue_id,
        "clientOid": "C-REAL",
        "side": "buy",
        "orderType": "market",
        "orderStatus": status,
        "qty": "0.010",
        "cumExecQty": filled,
        "avgPrice": "100.1",
        "createdTime": "1700000000000",
        "updatedTime": "1700000001000",
    }
    if trade_id is not None:
        row.update(
            {
                "tradeId": trade_id,
                "baseVolume": latest_qty or filled,
                "fillPrice": "100.1",
                "fillFee": "-0.001",
                "fillFeeCoin": "USDT",
                "tradeScope": "T",
                "fillTime": "1700000001000",
            }
        )
    return row


def fill_row(*, trade_id="T-REAL", quantity="0.010", timestamp="1700000001000", venue_id="O-REAL"):
    return {
        "symbol": "BTCUSDT",
        "orderId": venue_id,
        "clientOid": "C-REAL",
        "execId": trade_id,
        "side": "buy",
        "execPrice": "100.1",
        "execQty": quantity,
        "feeDetail": [{"feeCoin": "USDT", "fee": "-0.001"}],
        "tradeScope": "taker",
        "createdTime": timestamp,
    }


@pytest.fixture
def exec_engine(event_loop, msgbus, cache, clock):
    engine = LiveExecutionEngine(event_loop, msgbus, cache, clock)
    yield engine
    engine.dispose()


@pytest_asyncio.fixture
async def bridge_client(
    request,
    event_loop,
    bitget_http_client,
    msgbus,
    cache,
    clock,
    account_id,
    portfolio,
    trader_id,
    exec_engine,
):
    product_type = getattr(request, "param", nautilus_pyo3.BitgetProductType.USDT_FUTURES)
    provider = BitgetInstrumentProvider(bitget_http_client, product_type=product_type)
    await provider.load_all_async()
    classic_http = nautilus_pyo3.BitgetHttpClient(
        account_mode=nautilus_pyo3.BitgetAccountMode.CLASSIC
    )
    classic_http.cache_instruments(provider.instruments_pyo3())
    http = MagicMock()
    http.parse_execution_ws_event.side_effect = classic_http.parse_execution_ws_event
    http.request_fill_reports = AsyncMock(return_value=[])
    http.request_order_status_report = AsyncMock(return_value=None)
    client = BitgetExecutionClient(
        loop=event_loop,
        client=http,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=BitgetExecClientConfig(product_type=product_type),
        name=None,
    )
    client._set_account_id(account_id)
    client.pyo3_account_id = nautilus_pyo3.AccountId(account_id.value)
    client._cache_instruments()
    exec_engine.register_client(client)
    instrument_id = next(iter(provider.get_all()))
    strategy = Strategy(
        config=StrategyConfig(
            strategy_id="CAPTURE",
            order_id_tag="001",
            external_order_claims=[instrument_id],
        )
    )
    strategy.register(
        trader_id=trader_id, portfolio=portfolio, msgbus=msgbus, cache=cache, clock=clock
    )
    exec_engine.register_external_order_claims(strategy)
    events = []
    msgbus.subscribe(f"events.order.{strategy.id}", events.append)
    return client, http, classic_http, events


def parse_fill(http, row, client):
    return http.parse_execution_ws_event(
        push("fill", row),
        nautilus_pyo3.BitgetProductType.USDT_FUTURES,
        nautilus_pyo3.TraderId(client.trader_id.value),
        client.pyo3_account_id,
    )[0]


@pytest.mark.asyncio
async def test_classic_market_push_preserves_claimed_real_fill_and_duplicate_is_ignored(
    bridge_client, cache
):
    client, http, _, events = bridge_client
    event = push("order", order_row(trade_id="T-REAL"))
    await client._handle_ws_event(event)
    await client._handle_ws_event(event)

    fills = [event for event in events if isinstance(event, OrderFilled)]
    assert len(fills) == 1
    assert fills[0].trade_id.value == "T-REAL"
    assert fills[0].strategy_id == StrategyId("CAPTURE-001")
    assert fills[0].last_qty.as_decimal() == Decimal("0.010")
    assert fills[0].commission.as_decimal() == Decimal("0.001")
    assert fills[0].commission.currency.code == "USDT"
    order = cache.order(cache.client_order_id(VenueOrderId("O-REAL")))
    assert order.status == OrderStatus.FILLED
    assert len(order.trade_ids) == 1
    http.request_fill_reports.assert_not_awaited()


@pytest.mark.asyncio
async def test_uta_filled_status_queries_true_fill_before_reconciliation(bridge_client):
    client, http, classic_http, events = bridge_client
    # Switch just the parser to the UTA mode: separate order/fill channels.
    uta_http = nautilus_pyo3.BitgetHttpClient()
    uta_http.cache_instruments(client.instrument_provider.instruments_pyo3())
    http.parse_execution_ws_event.side_effect = uta_http.parse_execution_ws_event
    http.request_fill_reports.return_value = [parse_fill(uta_http, fill_row(), client)]
    await client._handle_ws_event(push("order", order_row()))

    fills = [event for event in events if isinstance(event, OrderFilled)]
    assert [fill.trade_id.value for fill in fills] == ["T-REAL"]
    assert fills[0].commission.as_decimal() == Decimal("0.001")
    http.request_fill_reports.assert_awaited_once()


@pytest.mark.asyncio
async def test_fill_before_external_order_fetches_status_and_preserves_execution(bridge_client):
    client, http, classic_http, events = bridge_client
    status = classic_http.parse_execution_ws_event(
        push("order", order_row()),
        nautilus_pyo3.BitgetProductType.USDT_FUTURES,
        nautilus_pyo3.TraderId(client.trader_id.value),
        client.pyo3_account_id,
    )[0]
    http.request_order_status_report.return_value = status
    await client._handle_ws_event(push("fill", fill_row()))

    assert [event.trade_id.value for event in events if isinstance(event, OrderFilled)] == [
        "T-REAL"
    ]
    http.request_order_status_report.assert_awaited_once()
    http.request_fill_reports.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_real_fill_defers_status_until_the_fill_push_arrives(bridge_client):
    client, http, _, events = bridge_client
    await client._handle_ws_event(push("order", order_row()))
    assert not [event for event in events if isinstance(event, OrderFilled)]
    assert VenueOrderId("O-REAL") in client._pending_order_reports

    await client._handle_ws_event(push("fill", fill_row()))
    assert [event.trade_id.value for event in events if isinstance(event, OrderFilled)] == [
        "T-REAL"
    ]
    assert not client._pending_order_reports
    assert not client._pending_fill_reports
    http.request_fill_reports.assert_awaited_once()


@pytest.mark.asyncio
async def test_first_classic_push_backfills_earlier_trades_without_synthetic_ids(bridge_client):
    client, http, classic_http, events = bridge_client
    http.request_fill_reports.return_value = [
        parse_fill(
            classic_http,
            fill_row(trade_id="T-1", quantity="0.006", timestamp="1700000000500"),
            client,
        ),
        parse_fill(classic_http, fill_row(trade_id="T-2", quantity="0.004"), client),
    ]
    await client._handle_ws_event(push("order", order_row(trade_id="T-2", latest_qty="0.004")))

    fills = [event for event in events if isinstance(event, OrderFilled)]
    assert [fill.trade_id.value for fill in fills] == ["T-1", "T-2"]
    assert sum((fill.last_qty.as_decimal() for fill in fills), Decimal(0)) == Decimal("0.010")


@pytest.mark.asyncio
async def test_out_of_order_real_fills_keep_both_trade_ids(bridge_client):
    client, _, _, events = bridge_client
    await client._handle_ws_event(push("order", order_row(filled="0", status="live")))
    await client._handle_ws_event(push("fill", fill_row(trade_id="T-2", quantity="0.004")))
    await client._handle_ws_event(
        push("fill", fill_row(trade_id="T-1", quantity="0.006", timestamp="1700000000500"))
    )
    fills = [event for event in events if isinstance(event, OrderFilled)]
    assert [fill.trade_id.value for fill in fills] == ["T-2", "T-1"]


@pytest.mark.asyncio
async def test_malformed_row_keeps_other_real_orders_in_same_push(bridge_client, cache):
    client, _, _, events = bridge_client
    good = order_row(trade_id="T-REAL")
    invalid = {**order_row(venue_id="BAD"), "qty": "bad-number"}
    await client._handle_ws_event(push("order", invalid, good))
    assert [event.trade_id.value for event in events if isinstance(event, OrderFilled)] == [
        "T-REAL"
    ]
    assert cache.client_order_id(VenueOrderId("BAD")) is None


@pytest.mark.asyncio
async def test_private_account_push_refreshes_full_account_state(bridge_client):
    client, _, _, _ = bridge_client
    client._update_account_state = AsyncMock()
    await client._handle_ws_event(push("account", {"coin": "USDT", "available": "10"}))
    client._update_account_state.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_fill_history_request_retries_pending_order_without_inference(bridge_client):
    client, http, classic_http, events = bridge_client
    http.request_fill_reports.side_effect = [
        RuntimeError("temporary HTTP failure"),
        [parse_fill(classic_http, fill_row(), client)],
    ]
    await client._handle_ws_event(push("order", order_row()))
    assert not [event for event in events if isinstance(event, OrderFilled)]
    assert VenueOrderId("O-REAL") in client._pending_order_reports
    await client._retry_pending_ws_reports()
    assert [event.trade_id.value for event in events if isinstance(event, OrderFilled)] == [
        "T-REAL"
    ]
    assert not client._pending_order_reports


@pytest.mark.asyncio
async def test_failed_fill_first_lookup_is_recovered_without_another_push(bridge_client):
    client, http, classic_http, events = bridge_client
    status = classic_http.parse_execution_ws_event(
        push("order", order_row()),
        nautilus_pyo3.BitgetProductType.USDT_FUTURES,
        nautilus_pyo3.TraderId(client.trader_id.value),
        client.pyo3_account_id,
    )[0]
    http.request_order_status_report.side_effect = [RuntimeError("temporary HTTP failure"), status]
    await client._handle_ws_event(push("fill", fill_row()))
    assert not [event for event in events if isinstance(event, OrderFilled)]
    await client._retry_pending_ws_reports()
    assert [event.trade_id.value for event in events if isinstance(event, OrderFilled)] == [
        "T-REAL"
    ]
    assert not client._pending_fill_reports


@pytest.mark.asyncio
async def test_reconnect_rest_history_keeps_real_fill_id(bridge_client):
    client, http, classic_http, events = bridge_client
    status = classic_http.parse_execution_ws_event(
        push("order", order_row()),
        nautilus_pyo3.BitgetProductType.USDT_FUTURES,
        nautilus_pyo3.TraderId(client.trader_id.value),
        client.pyo3_account_id,
    )[0]
    client._update_account_state = AsyncMock()
    http.request_order_status_reports = AsyncMock(return_value=[status])
    http.request_fill_reports.return_value = [parse_fill(classic_http, fill_row(), client)]
    await client._handle_ws_event({"type": "reconnected"})
    assert [event.trade_id.value for event in events if isinstance(event, OrderFilled)] == [
        "T-REAL"
    ]
    client._update_account_state.assert_awaited_once()
    assert http.request_order_status_reports.call_args.kwargs["start"] is not None


@pytest.mark.asyncio
async def test_connect_finishes_subscriptions_before_receiver_and_disconnect_cancels_tasks(
    bridge_client,
):
    client, http, _, _ = bridge_client
    calls = []
    websocket = MagicMock()
    websocket.url = "ws://fixture.invalid/private"
    websocket.is_closed.return_value = False
    for name in (
        "connect",
        "wait_until_active",
        "subscribe_account",
        "subscribe_orders",
        "subscribe_fills",
        "subscribe_positions",
    ):

        async def record(*args, _name=name, **kwargs):
            calls.append(_name)

        setattr(websocket, name, AsyncMock(side_effect=record))

    async def next_event():
        calls.append("next_event")
        await asyncio.Future()

    websocket.next_event = AsyncMock(side_effect=next_event)
    websocket.close = AsyncMock()
    client._ws_client = websocket
    client._update_account_state = AsyncMock()
    client._await_account_registered = AsyncMock()
    client.instrument_provider.initialize = AsyncMock()
    await client._connect()
    await asyncio.sleep(0)
    tasks = [client._ws_task, client._ws_recovery_task]
    assert calls.index("subscribe_positions") < calls.index("next_event")
    await client._disconnect()
    assert all(task.done() for task in tasks)
    assert client._ws_task is None
    assert client._ws_recovery_task is None
    websocket.close.assert_awaited_once()
    http.cancel_all_requests.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("bridge_client", [nautilus_pyo3.BitgetProductType.SPOT], indirect=True)
async def test_classic_spot_market_buy_switches_quote_budget_to_real_base_fill(
    bridge_client, cache
):
    client, http, _, events = bridge_client
    accepted = order_row(filled="0", status="live")
    accepted["qty"] = "100"
    await client._handle_ws_event(push("order", accepted))
    cached_order = cache.order(cache.client_order_id(VenueOrderId("O-REAL")))
    assert cached_order.is_quote_quantity
    assert cached_order.quantity.as_decimal() == Decimal(100)

    filled = order_row(filled="0.5", trade_id="SPOT-T-1")
    filled.update(
        {
            "qty": "100",
            "avgPrice": "200",
            "fillPrice": "200",
            "fillFee": "-0.0001",
            "fillFeeCoin": "BTC",
        }
    )
    await client._handle_ws_event(push("order", filled))
    await client._handle_ws_event(push("order", filled))

    fills = [event for event in events if isinstance(event, OrderFilled)]
    assert [fill.trade_id.value for fill in fills] == ["SPOT-T-1"]
    assert fills[0].last_qty.as_decimal() == Decimal("0.5")
    assert fills[0].commission.as_decimal() == Decimal("0.0001")
    assert fills[0].commission.currency.code == "BTC"
    assert cached_order.status == OrderStatus.FILLED
    assert not cached_order.is_quote_quantity
    assert cached_order.quantity.as_decimal() == Decimal("0.5")
    http.request_fill_reports.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("bridge_client", [nautilus_pyo3.BitgetProductType.SPOT], indirect=True)
async def test_spot_partial_market_buy_missing_average_price_waits_for_complete_status(
    bridge_client, cache
):
    client, http, classic_http, events = bridge_client
    accepted = order_row(filled="0", status="live")
    accepted["qty"] = "100"
    await client._handle_ws_event(push("order", accepted))
    cached_order = cache.order(cache.client_order_id(VenueOrderId("O-REAL")))
    partial = order_row(filled="0.5", status="partially_filled", trade_id="SPOT-T-1")
    partial.update(
        {
            "qty": "100",
            "avgPrice": None,
            "fillPrice": "100",
            "fillFee": "-0.0001",
            "fillFeeCoin": "BTC",
        }
    )
    await client._handle_ws_event(push("order", partial))
    assert not cached_order.is_closed
    assert not [event for event in events if isinstance(event, OrderFilled)]
    assert VenueOrderId("O-REAL") in client._pending_order_reports

    complete = {**partial, "avgPrice": "100"}
    status = classic_http.parse_execution_ws_event(
        push("order", complete),
        nautilus_pyo3.BitgetProductType.SPOT,
        nautilus_pyo3.TraderId(client.trader_id.value),
        client.pyo3_account_id,
    )[0]["order"]
    http.request_order_status_report.return_value = status
    await client._retry_pending_ws_reports()
    assert cached_order.is_open
    assert cached_order.filled_qty.as_decimal() == Decimal("0.5")
    assert cached_order.quantity.as_decimal() == Decimal(1)
    assert [event.trade_id.value for event in events if isinstance(event, OrderFilled)] == [
        "SPOT-T-1"
    ]
    assert not client._pending_order_reports
