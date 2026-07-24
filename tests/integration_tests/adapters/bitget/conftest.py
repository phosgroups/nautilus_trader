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

from __future__ import annotations

import json
import weakref
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import WSCloseCode
from aiohttp import WSMsgType
from aiohttp import web
from aiohttp.test_utils import TestServer

from nautilus_trader.adapters.bitget.common.constants import BITGET_VENUE
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.currencies import BTC
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import MarginBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


REQUEST_TIME_MS = 1_700_000_000_000

SPOT_SYMBOL = {
    "symbol": "BTCUSDT",
    "category": "SPOT",
    "baseCoin": "BTC",
    "quoteCoin": "USDT",
    "minOrderQty": "0.00001",
    "maxOrderQty": "100",
    "minOrderAmount": "5",
    "makerFeeRate": "0.001",
    "takerFeeRate": "0.001",
    "pricePrecision": "2",
    "quantityPrecision": "6",
    "quotePrecision": "2",
    "status": "online",
}

USDT_FUTURES_CONTRACT = {
    "symbol": "BTCUSDT",
    "category": "USDT-FUTURES",
    "baseCoin": "BTC",
    "quoteCoin": "USDT",
    "type": "perpetual",
    "marginCoin": "USDT",
    "makerFeeRate": "0.0002",
    "takerFeeRate": "0.0006",
    "minOrderQty": "0.001",
    "minOrderAmount": "5",
    "maxOrderQty": "1000",
    "quantityMultiplier": "0.001",
    "pricePrecision": "1",
    "quantityPrecision": "3",
    "priceMultiplier": "0.1",
    "maxLeverage": "125",
    "minLeverage": "1",
    "fundInterval": "8",
    "status": "online",
    "symbolType": "crypto",
}

ORDERBOOK_SNAPSHOT = {
    "b": [["100.0", "0.010"], ["99.9", "0.020"]],
    "a": [["100.1", "0.015"], ["100.2", "0.030"]],
    "ts": str(REQUEST_TIME_MS),
    "seq": "42",
}

MARKET_TRADE = {
    "symbol": "BTCUSDT",
    "execId": "T-MKT-1",
    "price": "100.0",
    "size": "0.010",
    "side": "buy",
    "ts": str(REQUEST_TIME_MS),
}

WS_MARKET_TRADE = {
    "symbol": "BTCUSDT",
    "i": "T-MKT-1",
    "p": "100.0",
    "v": "0.010",
    "S": "buy",
    "T": str(REQUEST_TIME_MS),
}

ORDER_STATUS = {
    "symbol": "BTCUSDT",
    "category": "USDT-FUTURES",
    "orderId": "O-1",
    "clientOid": "C-1",
    "price": "100.0",
    "avgPrice": "100.1",
    "qty": "0.010",
    "cumExecQty": "0.004",
    "side": "buy",
    "orderType": "limit",
    "timeInForce": "post_only",
    "orderStatus": "partially_filled",
    "reduceOnly": "YES",
    "createdTime": str(REQUEST_TIME_MS),
    "updatedTime": str(REQUEST_TIME_MS + 1_000),
}

FILL = {
    "symbol": "BTCUSDT",
    "category": "USDT-FUTURES",
    "orderId": "O-1",
    "clientOid": "C-1",
    "execId": "F-1",
    "side": "sell",
    "execPrice": "100.1",
    "execQty": "0.004",
    "feeDetail": [{"feeCoin": "USDT", "fee": "-0.001"}],
    "tradeScope": "maker",
    "createdTime": str(REQUEST_TIME_MS),
}

POSITION = {
    "symbol": "BTCUSDT",
    "category": "USDT-FUTURES",
    "marginCoin": "USDT",
    "posSide": "short",
    "size": "0.004",
    "avgPrice": "100.1",
    "updatedTime": str(REQUEST_TIME_MS + 1_000),
}

UTA_ACCOUNT = {
    "accountEquity": "123",
    "usdtEquity": "123",
    "effEquity": "100",
    "imr": "10",
    "mmr": "4",
    "assets": [
        {
            "coin": "USDT",
            "equity": "123",
            "balance": "101",
            "available": "100",
            "locked": "3",
        },
    ],
}


def bitget_ok(data: Any) -> dict[str, Any]:
    return {
        "code": "00000",
        "msg": "success",
        "requestTime": REQUEST_TIME_MS,
        "data": data,
    }


def bitget_http_url(server: TestServer) -> str:
    return f"http://{server.host}:{server.port}"


def bitget_ws_url(server: TestServer) -> str:
    return f"ws://{server.host}:{server.port}/ws"


@pytest.fixture
def venue() -> Venue:
    return BITGET_VENUE


@pytest.fixture
def account_id() -> AccountId:
    return AccountId("BITGET-001")


@pytest.fixture
def instrument() -> CryptoPerpetual:
    return CryptoPerpetual(
        instrument_id=InstrumentId(Symbol("BTCUSDT-PERP"), BITGET_VENUE),
        raw_symbol=Symbol("BTCUSDT"),
        base_currency=BTC,
        quote_currency=USDT,
        settlement_currency=USDT,
        is_inverse=False,
        price_precision=1,
        size_precision=3,
        price_increment=Price.from_str("0.1"),
        size_increment=Quantity.from_str("0.001"),
        max_quantity=Quantity.from_str("1000"),
        min_quantity=Quantity.from_str("0.001"),
        max_notional=None,
        min_notional=Money(5, USDT),
        max_price=None,
        min_price=None,
        margin_init=Decimal("0.008"),
        margin_maint=Decimal("0.004"),
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0006"),
        ts_event=0,
        ts_init=0,
    )


@pytest.fixture
def account_state(account_id: AccountId, instrument: CryptoPerpetual) -> AccountState:
    return AccountState(
        account_id=account_id,
        account_type=AccountType.MARGIN,
        base_currency=None,
        reported=True,
        balances=[
            AccountBalance(
                total=Money(100_000, USDT),
                locked=Money(1_000, USDT),
                free=Money(99_000, USDT),
            ),
        ],
        margins=[
            MarginBalance(
                initial=Money(100, USDT),
                maintenance=Money(50, USDT),
                instrument_id=instrument.id,
            ),
        ],
        info={},
        event_id=TestIdStubs.uuid(),
        ts_event=0,
        ts_init=0,
    )


@pytest.fixture
def instrument_provider():
    return None


@pytest.fixture
def data_client():
    return None


@pytest.fixture
def exec_client():
    return None


@pytest_asyncio.fixture(name="bitget_http_server")
async def fixture_bitget_http_server(event_loop):  # noqa: C901
    async def handle_instruments(request):
        category = request.query.get("category")
        if category == "SPOT":
            return web.json_response(bitget_ok([SPOT_SYMBOL]))
        if category == "USDT-FUTURES":
            return web.json_response(bitget_ok([USDT_FUTURES_CONTRACT]))
        return web.json_response(bitget_ok([]))

    async def handle_orderbook(request):
        assert request.query.get("category") in {"SPOT", "USDT-FUTURES"}
        assert request.query.get("symbol") == "BTCUSDT"
        return web.json_response(bitget_ok(ORDERBOOK_SNAPSHOT))

    async def handle_market_trades(request):
        assert request.query.get("category") in {"SPOT", "USDT-FUTURES"}
        assert request.query.get("symbol") == "BTCUSDT"
        return web.json_response(bitget_ok([MARKET_TRADE]))

    async def handle_candles(request):
        assert request.query.get("category") in {"SPOT", "USDT-FUTURES"}
        assert request.query.get("interval")
        return web.json_response(
            bitget_ok(
                [
                    [
                        str(REQUEST_TIME_MS),
                        "100.0",
                        "101.0",
                        "99.0",
                        "100.5",
                        "1.23",
                    ],
                ],
            ),
        )

    async def handle_funding(request):
        assert request.query.get("category") == "USDT-FUTURES"
        return web.json_response(
            bitget_ok(
                {
                    "resultList": [
                        {
                            "symbol": "BTCUSDT",
                            "fundingRate": "0.0001",
                            "fundingRateTimestamp": str(REQUEST_TIME_MS),
                        },
                        {
                            "symbol": "BTCUSDT",
                            "fundingRate": "0.0002",
                            "fundingRateTimestamp": str(REQUEST_TIME_MS + 28_800_000),
                        },
                    ],
                },
            ),
        )

    async def handle_uta_account(request):
        assert not request.query
        return web.json_response(bitget_ok(UTA_ACCOUNT))

    async def handle_uta_order_detail(request):
        assert request.query.get("category") == "USDT-FUTURES"
        assert request.query.get("symbol") == "BTCUSDT"
        return web.json_response(bitget_ok(ORDER_STATUS))

    async def handle_uta_orders_pending(request):
        assert request.query.get("category") == "USDT-FUTURES"
        return web.json_response(bitget_ok({"list": [ORDER_STATUS], "cursor": ""}))

    async def handle_uta_fills(request):
        assert request.query.get("category") == "USDT-FUTURES"
        return web.json_response(bitget_ok({"list": [FILL], "cursor": ""}))

    async def handle_uta_positions(request):
        assert request.query.get("category") == "USDT-FUTURES"
        return web.json_response(bitget_ok({"list": [POSITION]}))

    app = web.Application()
    app.add_routes(
        [
            web.get("/api/v3/market/instruments", handle_instruments),
            web.get("/api/v3/market/orderbook", handle_orderbook),
            web.get("/api/v3/market/fills", handle_market_trades),
            web.get("/api/v3/market/candles", handle_candles),
            web.get("/api/v3/market/history-fund-rate", handle_funding),
            web.get("/api/v3/account/assets", handle_uta_account),
            web.get("/api/v3/trade/order-info", handle_uta_order_detail),
            web.get("/api/v3/trade/unfilled-orders", handle_uta_orders_pending),
            web.get("/api/v3/trade/history-orders", handle_uta_orders_pending),
            web.get("/api/v3/trade/fills", handle_uta_fills),
            web.get("/api/v3/position/current-position", handle_uta_positions),
        ],
    )

    server = TestServer(app)
    await server.start_server(loop=event_loop)
    yield server
    await app.shutdown()
    await app.cleanup()
    await server.close()


@pytest_asyncio.fixture(name="bitget_ws_server")
async def fixture_bitget_ws_server(event_loop):  # noqa: C901
    async def handle_ws(request):  # noqa: C901
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        request.app["websockets"].add(ws)

        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue

            if msg.data == "ping":
                await ws.send_str("pong")
                continue

            payload = json.loads(msg.data)
            request.app["messages"].append(payload)

            op = payload.get("op")
            args = payload.get("args") or []
            if op == "login":
                await ws.send_json({"event": "login", "code": "0", "msg": "success"})
                continue

            if op == "subscribe":
                for arg in args:
                    await ws.send_json({"event": "subscribe", "arg": arg})
                    topic = arg.get("topic")
                    if topic == "publicTrade":
                        await ws.send_json(
                            {
                                "action": "snapshot",
                                "arg": arg,
                                "data": [WS_MARKET_TRADE],
                            },
                        )
                    elif topic == "order":
                        await ws.send_json(
                            {
                                "action": "snapshot",
                                "arg": arg,
                                "data": [ORDER_STATUS],
                            },
                        )
                continue

            if op == "unsubscribe":
                for arg in args:
                    await ws.send_json({"event": "unsubscribe", "arg": arg})

        return ws

    app = web.Application()
    app["messages"] = []
    app["websockets"] = weakref.WeakSet()
    app.add_routes([web.get("/ws", handle_ws)])

    async def on_shutdown(app):
        for ws in set(app["websockets"]):
            await ws.close(code=WSCloseCode.GOING_AWAY, message=b"Server shutdown")

    app.on_shutdown.append(on_shutdown)

    server = TestServer(app)
    await server.start_server(loop=event_loop)
    yield server
    await app.shutdown()
    await app.cleanup()
    await server.close()


@pytest.fixture
def bitget_http_client(bitget_http_server) -> nautilus_pyo3.BitgetHttpClient:
    return nautilus_pyo3.BitgetHttpClient(
        api_key="test-key",
        api_secret="test-secret",
        api_passphrase="test-passphrase",
        base_url=bitget_http_url(bitget_http_server),
        timeout_secs=5,
        proxy_url=None,
    )
