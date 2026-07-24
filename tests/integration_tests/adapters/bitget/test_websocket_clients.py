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

import pytest

from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BitgetEnvironment
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType


async def _next_event_of_type(client, event_type: str, timeout_secs: float = 2.0):
    deadline = asyncio.get_running_loop().time() + timeout_secs
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for Bitget WS event type {event_type!r}")

        event = await asyncio.wait_for(client.next_event(), timeout=remaining)
        if event is not None and event.get("type") == event_type:
            return event


@pytest.mark.asyncio
async def test_public_ws_fixture_runs_ping_subscribe_and_data_path(bitget_ws_server):
    client = nautilus_pyo3.BitgetWebSocketClient.new_public(
        product_type=BitgetProductType.USDT_FUTURES,
        environment=BitgetEnvironment.MAINNET,
        url=f"ws://{bitget_ws_server.host}:{bitget_ws_server.port}/ws",
        heartbeat_secs=30,
    )

    await client.connect()
    await client.wait_until_active(timeout_secs=2.0)
    await client.send_text("ping")
    pong = await _next_event_of_type(client, "pong")

    await client.subscribe_trades("BTCUSDT")
    subscribe = await _next_event_of_type(client, "subscribe")
    data = await _next_event_of_type(client, "data")

    assert pong == {"type": "pong"}
    assert subscribe["arg"]["instType"] == "usdt-futures"
    assert subscribe["arg"]["topic"] == "publicTrade"
    assert subscribe["arg"]["symbol"] == "BTCUSDT"
    assert data["arg"]["topic"] == "publicTrade"
    assert data["data"][0]["i"] == "T-MKT-1"
    assert await client.subscription_count() == 1

    await client.disconnect()
    assert client.is_closed()


@pytest.mark.asyncio
async def test_private_ws_fixture_runs_login_subscribe_and_order_push(bitget_ws_server):
    client = nautilus_pyo3.BitgetWebSocketClient.new_private(
        product_type=BitgetProductType.USDT_FUTURES,
        environment=BitgetEnvironment.MAINNET,
        api_key="test-key",
        api_secret="test-secret",
        api_passphrase="test-passphrase",
        url=f"ws://{bitget_ws_server.host}:{bitget_ws_server.port}/ws",
        heartbeat_secs=30,
    )

    await client.connect()
    login = await _next_event_of_type(client, "login")

    await client.subscribe_orders()
    subscribe = await _next_event_of_type(client, "subscribe")
    data = await _next_event_of_type(client, "data")

    assert login["event"] == "login"
    assert login["code"] == "0"
    assert subscribe["arg"]["topic"] == "order"
    assert subscribe["arg"]["instType"] == "UTA"
    assert data["arg"]["topic"] == "order"
    assert data["data"][0]["orderId"] == "O-1"
    assert await client.subscription_count() == 1

    await client.disconnect()
    assert any(message.get("op") == "login" for message in bitget_ws_server.app["messages"])
    assert any(message.get("op") == "subscribe" for message in bitget_ws_server.app["messages"])
