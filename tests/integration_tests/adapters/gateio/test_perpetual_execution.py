"""Gate v1 USDT perpetual execution commands and reconciliation."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from nautilus_trader.adapters.gateio.config import GateioExecClientConfig
from nautilus_trader.adapters.gateio.config import GateioProductType
from nautilus_trader.adapters.gateio.perpetual_execution import GateioPerpetualExecutionClient
from nautilus_trader.adapters.gateio.providers import GateioInstrumentProvider
from nautilus_trader.adapters.gateio.providers import parse_perpetual
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.test_kit.stubs.component import TestComponentStubs


CONTRACT = {
    "name": "BTC_USDT",
    "order_price_round": "0.1",
    "order_size_min": "0.1",
    "order_size_max": "1000",
    "quanto_multiplier": "0.001",
    "maker_fee_rate": "0.0002",
    "taker_fee_rate": "0.0005",
}
ORDER = {
    "contract": "BTC_USDT",
    "id": 87,
    "text": "t-ntStest",
    "size": "-2.5",
    "price": "64000",
    "left": "-1.5",
    "tif": "ioc",
    "status": "finished",
    "finish_as": "ioc",
    "fill_price": "64001",
    "is_reduce_only": True,
    "create_time_ms": 100,
    "finish_time_ms": 105,
}
FILL = {
    "contract": "BTC_USDT",
    "order_id": 87,
    "id": 23,
    "size": "-1",
    "role": "taker",
    "fee": "0.162",
    "price": "64001",
    "create_time_ms": 102,
}
POSITION = {"contract": "BTC_USDT", "size": "-1", "entry_price": "64001", "mode": "single"}
ACCOUNT = {
    "currency": "USDT",
    "available": "100",
    "total": "104",
    "order_margin": "1",
    "position_margin": "3",
    "maintenance_margin": "0.5",
}


class Client(GateioPerpetualExecutionClient):
    def generate_account_state(self, **kwargs):
        self.events.append(kwargs)

    def _send_order_status_report(self, report):
        self.events.append(report)

    def _send_mass_status_report(self, report):
        self.events.append(report)

    def generate_order_rejected(self, **kwargs):
        self.events.append(kwargs)

    def generate_order_denied(self, **kwargs):
        self.events.append(kwargs)

    def generate_order_submitted(self, **kwargs):
        self.events.append(kwargs)


def client_for(routes):
    async def request(method, path, params=None, body=None, private=False, before_send=None):
        assert private
        if before_send:
            before_send()
        if path.endswith("/my_trades"):
            assert params["order"] == "87"
            assert params["contract"] == "BTC_USDT"
        return routes[(method, path.removeprefix("/futures/usdt"))]

    clock = TestClock()
    http = SimpleNamespace(request=AsyncMock(side_effect=request))
    product = GateioProductType.USDT_PERPETUAL
    provider = GateioInstrumentProvider(http, clock, InstrumentProviderConfig(), product)
    instrument = parse_perpetual(CONTRACT, 0)
    provider.add(instrument)
    client = Client(
        asyncio.get_running_loop(),
        http,
        TestComponentStubs.msgbus(),
        TestComponentStubs.cache(),
        clock,
        provider,
        GateioExecClientConfig(product_type=product),
    )
    client.events = []
    return client, instrument


def test_perpetual_mass_status_pairs_real_contract_fills_and_margin():
    async def run():
        client, instrument = client_for(
            {
                ("GET", "/accounts"): ACCOUNT,
                ("GET", "/orders"): [],
                ("GET", "/orders_timerange"): [ORDER],
                ("GET", "/positions"): [POSITION],
                ("GET", "/my_trades_timerange"): [],
                ("GET", "/my_trades"): [FILL],
            }
        )
        await client._reconcile()
        state, mass = client.events
        assert client.account_type == AccountType.MARGIN
        assert state["balances"][0].locked.as_decimal() == 4
        assert state["margins"][0].initial.as_decimal() == 4
        assert state["margins"][0].maintenance.as_decimal() == Decimal("0.5")
        order = mass.order_reports[VenueOrderId("87")]
        assert order.instrument_id == instrument.id
        assert order.reduce_only
        assert order.order_status == OrderStatus.CANCELED
        assert order.order_side == OrderSide.SELL
        assert order.filled_qty.as_decimal() == 1
        (fill,) = mass.fill_reports[order.venue_order_id]
        assert fill.last_qty.as_decimal() == 1
        assert fill.last_px.as_decimal() == 64001
        assert str(fill.trade_id) == "23"
        assert fill.commission.as_decimal() == Decimal("0.162")
        (position,) = mass.position_reports[instrument.id]
        assert position.position_side == PositionSide.SHORT
        assert position.quantity.as_decimal() == 1

    asyncio.run(run())


@pytest.mark.parametrize("market", [True, False])
def test_perpetual_order_submission_and_amend_cancel_acknowledgements(market):
    async def run():
        ack = dict(
            ORDER,
            tif="ioc" if market else "gtc",
            price="0" if market else "64000",
            left="-2.5",
            status="open",
            finish_as="_new",
            finish_time_ms=0,
        )
        client, instrument = client_for(
            {
                ("POST", "/orders"): ack,
                ("PUT", "/orders/87"): dict(ack, price="63000"),
                ("DELETE", "/orders/87"): ORDER,
                ("GET", "/my_trades"): [FILL],
            }
        )
        client.ws.is_ready = Mock(return_value=True)
        factory = TestComponentStubs.order_factory()
        common = {
            "instrument_id": instrument.id,
            "order_side": OrderSide.SELL,
            "quantity": instrument.make_qty(2.5),
            "client_order_id": ClientOrderId("test"),
            "reduce_only": True,
        }
        order = (
            factory.market(**common)
            if market
            else factory.limit(**common, price=instrument.make_price(64000))
        )
        client._cache.add_order(order, None, None)
        await client._submit_order(SimpleNamespace(order=order))
        payload = client.http.request.await_args_list[0].kwargs["body"]
        assert payload["size"] == "-2.5"
        assert payload["reduce_only"]
        assert payload["price"] == ("0" if market else str(order.price))
        assert payload["tif"] == ("ioc" if market else "gtc")
        assert client.events[-1].order_status == OrderStatus.ACCEPTED
        cmd = SimpleNamespace(
            client_order_id=order.client_order_id,
            venue_order_id=VenueOrderId("87"),
            instrument_id=instrument.id,
            strategy_id=order.strategy_id,
            trigger_price=None,
            price=instrument.make_price(63000),
            quantity=instrument.make_qty(2),
        )
        if not market:
            await client._modify_order(cmd)
            assert client.events[-1].price.as_decimal() == 63000
            assert Decimal(client.http.request.await_args.kwargs["body"]["size"]) == -2
        await client._cancel_order(cmd)
        assert client.events[-1].order_reports[cmd.venue_order_id].filled_qty.as_decimal() == 1

    asyncio.run(run())


def test_perpetual_reconciliation_declines_incomplete_fills_and_dual_position_mode():
    async def run():
        client, _ = client_for(
            {
                ("GET", "/positions"): [POSITION],
                ("GET", "/orders"): [],
                ("GET", "/orders_timerange"): [ORDER],
                ("GET", "/my_trades_timerange"): [],
                ("GET", "/my_trades"): [],
                ("GET", "/accounts"): {**ACCOUNT, "position_mode": "dual"},
            }
        )
        assert await client.generate_mass_status() is None
        with pytest.raises(ValueError, match="one-way"):
            await client._query_account(None)
        assert not client.events

    asyncio.run(run())


def test_perpetual_history_trade_id_and_ws_order_millisecond_timestamp():
    async def run():
        old_fill = {k: v for k, v in FILL.items() if k != "id"}
        old_fill["trade_id"] = "23"
        client, inst = client_for(
            {
                ("GET", "/orders"): [],
                ("GET", "/orders_timerange"): [ORDER],
                ("GET", "/my_trades"): [],
                ("GET", "/positions"): [],
            }
        )
        request = client.http.request

        async def history(method, path, params=None, **kwargs):
            if path.endswith("/my_trades_timerange"):
                return [old_fill] if params["from"] == 0 else []
            return await request(method, path, params=params, **kwargs)

        client.http.request = AsyncMock(side_effect=history)
        client._clock.set_time(300 * 86400 * 1_000_000_000)
        mass = await client.generate_mass_status(60)
        assert str(mass.fill_reports[VenueOrderId("87")][0].trade_id) == "23"
        assert (
            client._order({**ORDER, "update_time": 1_800_000_000_123}).ts_last
            == 1_800_000_000_123_000_000
        )
        with pytest.raises(ValueError, match="point-card"):
            client._fill({**old_fill, "point_fee": "8"})

    asyncio.run(run())


def test_perpetual_evolved_classic_margin_fields():
    async def run():
        client, _ = client_for(
            {
                ("GET", "/accounts"): {
                    **ACCOUNT,
                    "enable_evolved_classic": True,
                    "cross_initial_margin": "7",
                    "isolated_position_margin": "9",
                    "cross_maintenance_margin": "0.1",
                    "cross_unrealised_pnl": "5",
                    "order_margin": "1",
                    "maintenance_margin": "0.4",
                }
            }
        )
        await client._query_account(None)
        (state,) = client.events
        assert state["margins"][0].initial.as_decimal() == 17
        assert state["margins"][0].maintenance.as_decimal() == Decimal("0.4")
        assert state["balances"][0].total.as_decimal() == 109
        client.http.request = AsyncMock(return_value={**ACCOUNT, "margin_mode": 3})
        with pytest.raises(ValueError, match="classic"):
            await client._query_account(None)

    asyncio.run(run())


def test_perpetual_cancel_keeps_acknowledged_write_when_trade_lookup_fails():
    from nautilus_trader.adapters.gateio.http import GateioHttpError

    async def run():
        client, instrument = client_for({("DELETE", "/orders/87"): ORDER})
        request = client.http.request

        async def history(method, path, *args, **kwargs):
            if method == "GET":
                raise GateioHttpError(403, "FORBIDDEN")
            return await request(method, path, **kwargs)

        client.http.request = AsyncMock(side_effect=history)
        cmd = SimpleNamespace(
            instrument_id=instrument.id,
            client_order_id=ClientOrderId("test"),
            venue_order_id=VenueOrderId("87"),
        )
        with pytest.raises(GateioHttpError, match="FORBIDDEN"):
            await client._cancel_order(cmd)
        assert not client.events

    asyncio.run(run())


@pytest.mark.parametrize("rejected", [True, False])
def test_perpetual_cancel_all_waits_for_submission(rejected):
    from nautilus_trader.adapters.gateio.http import GateioHttpError

    async def run():
        client, instrument = client_for({})
        order = TestComponentStubs.order_factory().limit(
            instrument.id,
            OrderSide.SELL,
            instrument.make_qty(2.5),
            instrument.make_price(64000),
            client_order_id=ClientOrderId("test"),
        )
        client._cache.add_order(order, None, None)
        cmd = SimpleNamespace(
            instrument_id=instrument.id,
            strategy_id=order.strategy_id,
            trader_id=order.trader_id,
            order_side=OrderSide.SELL,
        )
        pending, release = asyncio.Event(), asyncio.Event()
        deleted = []

        async def request(method, path, **kw):
            if method == "POST":
                kw["before_send"]()
                pending.set()
                await release.wait()
                if rejected:
                    raise GateioHttpError(400, "BALANCE_NOT_ENOUGH")
                return {**ORDER, "status": "open", "finish_as": "_new", "left": "-2.5"}
            assert method == "DELETE"
            deleted.append(path)
            return {**ORDER, "left": "-2.5", "finish_as": "cancelled"}

        client.ws.is_ready = Mock(return_value=True)
        client.http.request = AsyncMock(side_effect=request)
        sending = asyncio.create_task(client._submit(order))
        await pending.wait()
        cancel = asyncio.create_task(client._cancel_all_orders(cmd))
        await asyncio.sleep(0)
        assert not deleted
        release.set()
        await asyncio.gather(sending, cancel)
        assert deleted == ([] if rejected else ["/futures/usdt/orders/87"])

    asyncio.run(run())


def test_external_close_all_uses_actual_executions_for_a_zero_size_order():
    async def run():
        filled = {**ORDER, "size": "0", "left": "0", "is_close": True, "finish_as": "filled"}
        cancelled = {**filled, "id": "88", "finish_as": "cancelled"}
        client, _ = client_for(
            {
                ("GET", "/orders"): [],
                ("GET", "/orders_timerange"): [filled, cancelled],
                ("GET", "/my_trades_timerange"): [FILL],
                ("GET", "/positions"): [],
            }
        )
        request = client.http.request

        async def read(method, path, params=None, **kw):
            if path.endswith("/my_trades"):
                return [FILL] if params["order"] == "87" else []
            return await request(method, path, params, **kw)

        client.http.request = AsyncMock(side_effect=read)
        mass = await client.generate_mass_status()
        (report,) = mass.order_reports.values()
        assert report.quantity.as_decimal() == 1
        assert report.order_side == OrderSide.SELL
        assert report.reduce_only

    asyncio.run(run())


@pytest.mark.parametrize("invalid", ["day", "linked", "contingent", "strategy", "instrument"])
def test_perpetual_list_is_validated_before_dispatch(invalid):
    async def run():
        client, instrument = client_for({})
        factory = TestComponentStubs.order_factory()
        first = factory.limit(
            instrument.id, OrderSide.BUY, instrument.make_qty(1), instrument.make_price(64000)
        )
        last = Mock(
            wraps=first,
            client_order_id=ClientOrderId("second"),
            strategy_id=first.strategy_id,
            is_closed=False,
            instrument_id=first.instrument_id,
            contingency_type=ContingencyType.NO_CONTINGENCY,
            parent_order_id=None,
            linked_order_ids=None,
            time_in_force=TimeInForce.GTC,
            order_type=first.order_type,
            is_quote_quantity=False,
        )
        if invalid == "day":
            last = factory.limit(
                instrument.id,
                OrderSide.BUY,
                instrument.make_qty(1),
                instrument.make_price(64000),
                time_in_force=TimeInForce.DAY,
            )
        elif invalid == "linked":
            last.linked_order_ids = [first.client_order_id]
        elif invalid == "contingent":
            last.contingency_type = ContingencyType.OTO
        elif invalid == "strategy":
            last.strategy_id = "OTHER"
        else:
            last.instrument_id = type(first.instrument_id).from_str("OTHER-PERP.GATEIO")
        client.ws.is_ready = Mock(return_value=True)
        await client._submit_order_list(
            SimpleNamespace(
                strategy_id=first.strategy_id, order_list=SimpleNamespace(orders=[first, last])
            )
        )
        assert [event["client_order_id"] for event in client.events] == [
            first.client_order_id,
            last.client_order_id,
        ]
        client.http.request.assert_not_awaited()

    asyncio.run(run())


def trading_node_scenario():  # noqa: C901 (isolated TradingNode and local exchange)
    import hashlib
    import hmac

    from aiohttp import web

    from nautilus_trader.adapters.gateio.factories import GateioLiveExecClientFactory
    from nautilus_trader.config import LiveExecEngineConfig
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.config import TradingNodeConfig
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.identifiers import InstrumentId
    from nautilus_trader.trading.strategy import Strategy

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    iid = InstrumentId.from_str("BTC_USDT-PERP.GATEIO")
    state = {"submitted": None, "channels": set()}

    class SubmitPerpetual(Strategy):
        def on_start(self):
            inst = self.cache.instrument(iid)
            self.submit_order(
                self.order_factory.market(
                    iid,
                    OrderSide.SELL,
                    inst.make_qty(1),
                    client_order_id=ClientOrderId("test"),
                )
            )

    async def ws_handler(req):
        assert req.headers.get("X-Gate-Size-Decimal") == "1"
        ws = web.WebSocketResponse()
        await ws.prepare(req)
        async for frame in ws:
            if frame.type != web.WSMsgType.TEXT:
                continue
            msg = frame.json()
            assert msg["payload"][0] == "27"
            assert msg["auth"]["KEY"] == "test-key"
            signed = f"channel={msg['channel']}&event=subscribe&time={msg['time']}"
            assert (
                msg["auth"]["SIGN"]
                == hmac.new(b"test-secret", signed.encode(), hashlib.sha512).hexdigest()
            )
            state["channels"].add(msg["channel"])
            await ws.send_json(
                {
                    "channel": msg["channel"],
                    "event": "subscribe",
                    "id": msg["id"],
                    "result": {"status": "success"},
                }
            )
        return ws

    async def handler(req):
        path = req.match_info["tail"]
        if path == "futures/usdt/contracts/BTC_USDT":
            return web.json_response(CONTRACT)
        assert req.headers["KEY"] == "test-key"
        if path == "account/detail":
            return web.json_response({"user_id": 27})
        assert req.headers.get("X-Gate-Size-Decimal") == "1"
        route = path.removeprefix("futures/usdt/")
        if route == "accounts":
            return web.json_response(ACCOUNT)
        if route == "positions":
            return web.json_response([POSITION] if state["submitted"] else [])
        if route == "orders" and req.method == "POST":
            submitted = await req.json()
            assert state["submitted"] is None
            state["submitted"] = submitted
            return web.json_response(
                {**ORDER, **submitted, "left": "0", "status": "finished", "finish_as": "filled"}
            )
        if route == "my_trades":
            return web.json_response([FILL])
        if route in {"orders", "orders_timerange", "my_trades_timerange"}:
            return web.json_response([])
        raise AssertionError((req.method, path))

    async def run():
        app = web.Application()
        app.router.add_get("/ws", ws_handler)
        app.router.add_route("*", "/api/v4/{tail:.*}", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        cfg = GateioExecClientConfig(
            product_type=GateioProductType.USDT_PERPETUAL,
            api_key="test-key",
            api_secret="test-secret",
            instrument_provider=InstrumentProviderConfig(load_ids=frozenset({iid})),
            base_url_http=f"http://127.0.0.1:{port}/api/v4",
            base_url_ws=f"ws://127.0.0.1:{port}/ws",
        )
        node = TradingNode(
            TradingNodeConfig(
                exec_clients={"GATEIO": cfg},
                logging=LoggingConfig(log_level="ERROR"),
                timeout_post_stop=0.1,
                exec_engine=LiveExecEngineConfig(
                    open_check_interval_secs=0, position_check_interval_secs=0
                ),
            ),
            loop=loop,
        )
        strategy = SubmitPerpetual()
        node.trader.add_strategy(strategy)
        node.add_exec_client_factory("GATEIO", GateioLiveExecClientFactory)
        node.build()
        events = []
        node.kernel.msgbus.subscribe(f"events.order.{strategy.id}", events.append)
        task = loop.create_task(node.run_async())
        try:
            async with asyncio.timeout(12):
                while not any(type(e).__name__ == "OrderFilled" for e in events):
                    await asyncio.sleep(0.03)
            (filled,) = [e for e in events if type(e).__name__ == "OrderFilled"]
            assert str(filled.trade_id) == "23"
            assert filled.last_qty.as_decimal() == 1
            assert filled.commission.as_decimal() == Decimal("0.162")
            assert node.kernel.cache.positions_open(instrument_id=iid)[0].is_short
            assert state["submitted"]["contract"] == "BTC_USDT"
            assert Decimal(state["submitted"]["size"]) == -1
            assert {
                "futures.balances",
                "futures.orders",
                "futures.usertrades",
                "futures.positions",
            } <= state["channels"]
        finally:
            await node.stop_async()
            await task
            await runner.cleanup()

    loop.run_until_complete(run())


def test_trading_node_submits_and_receives_perpetual_fill():
    import subprocess
    import sys

    script = f"import runpy; runpy.run_path({__file__!r})['trading_node_scenario']()"
    result = subprocess.run(  # noqa: S603 (fixed local test script)
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=45, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
