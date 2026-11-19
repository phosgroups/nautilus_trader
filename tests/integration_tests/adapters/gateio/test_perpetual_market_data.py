import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

from nautilus_trader.adapters.gateio.config import GateioDataClientConfig
from nautilus_trader.adapters.gateio.config import GateioEnvironment
from nautilus_trader.adapters.gateio.config import GateioProductType
from nautilus_trader.adapters.gateio.config import endpoints
from nautilus_trader.adapters.gateio.perpetual_data import GateioPerpetualDataClient
from nautilus_trader.adapters.gateio.providers import GateioInstrumentProvider
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import FundingRateUpdate
from nautilus_trader.model.data import IndexPriceUpdate
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.test_kit.stubs.component import TestComponentStubs


CONTRACT = {
    "name": "BTC_USDT",
    "type": "direct",
    "order_price_round": "0.01",
    "order_size_min": "0.1",
    "order_size_max": "1000",
    "quanto_multiplier": "0.001",
    "maker_fee_rate": "-0.0002",
    "taker_fee_rate": "0.0005",
    "funding_interval": 28800,
}


class Recorder(GateioPerpetualDataClient):
    def _handle_data(self, data):
        self.received.append(data)

    def _handle_data_response(self, *, data, **kwargs):
        self.received.extend(data)

    def _handle_bars(self, kind, bars, *args):
        self.received.extend(bars)

    def _handle_trade_ticks(self, instrument_id, ticks, *args):
        self.received.extend(ticks)

    def _handle_funding_rates(self, instrument_id, rates, *args):
        self.received.extend(rates)


async def market():
    product = GateioProductType.USDT_PERPETUAL
    http = SimpleNamespace(request=AsyncMock(return_value=CONTRACT))
    clock = TestClock()
    clock.set_time(240_000_000_000)
    provider = GateioInstrumentProvider(http, clock, InstrumentProviderConfig(), product)
    iid = InstrumentId.from_str("BTC_USDT-PERP.GATEIO")
    await provider.load_async(iid)
    http.request.assert_awaited_once_with("GET", "/futures/usdt/contracts/BTC_USDT")
    contract = provider.find(iid)
    assert contract.multiplier.as_decimal() == Decimal("0.001")
    assert contract.size_increment.as_decimal() == Decimal("0.1")
    assert contract.maker_fee == Decimal("-0.0002")
    config = GateioDataClientConfig(product_type=product)
    assert endpoints(GateioEnvironment.TESTNET, product_type=product)[1] == (
        "wss://ws-testnet.gate.com/v4/ws/futures/usdt"
    )
    client = Recorder(
        asyncio.get_running_loop(),
        http,
        TestComponentStubs.msgbus(),
        TestComponentStubs.cache(),
        clock,
        provider,
        config,
    )
    client.received = []
    assert client.ws.headers == [("X-Gate-Size-Decimal", "1")]
    client.ws = AsyncMock()
    return client, iid


def test_shared_futures_tickers_signed_trades_and_closed_candles():
    async def run():
        client, iid = await market()
        cmd = SimpleNamespace(instrument_id=iid)
        await client._subscribe_mark_prices(cmd)
        await client._subscribe_index_prices(cmd)
        await client._subscribe_funding_rates(cmd)
        client.ws.subscribe.assert_awaited_once_with("futures.tickers", ["BTC_USDT"])
        await client._message(
            {
                "channel": "futures.tickers",
                "time_ms": 123,
                "result": [
                    {
                        "contract": "BTC_USDT",
                        "mark_price": "100",
                        "index_price": "99",
                        "funding_rate": "0.001",
                        "funding_next_apply": 456,
                    }
                ],
            }
        )
        data = {type(value): value for value in client.received}
        assert {MarkPriceUpdate, IndexPriceUpdate, FundingRateUpdate} == data.keys()
        assert data[FundingRateUpdate].next_funding_ns == 456_000_000_000
        assert data[FundingRateUpdate].interval == 480
        await client._unsubscribe_mark_prices(cmd)
        await client._unsubscribe_index_prices(cmd)
        client.ws.unsubscribe.assert_not_awaited()
        await client._unsubscribe_funding_rates(cmd)
        client.ws.unsubscribe.assert_awaited_once_with("futures.tickers", ["BTC_USDT"])
        await client._subscribe_trade_ticks(cmd)
        client.ws.subscribe.assert_awaited_with("futures.trades", ["BTC_USDT"])
        await client._message(
            {
                "channel": "futures.trades",
                "result": [
                    {"contract": "BTC_USDT", "size": "0", "price": "100", "id": 6},
                    {
                        "contract": "BTC_USDT",
                        "size": "-2.5",
                        "price": "100",
                        "id": 7,
                        "create_time_ms": 123,
                    },
                ],
            }
        )
        tick = client.received[-1]
        assert tick.instrument_id == iid
        assert tick.size.as_decimal() == Decimal("2.5")
        assert tick.aggressor_side == AggressorSide.SELLER
        bars = SimpleNamespace(
            bar_type=BarType.from_str("BTC_USDT-PERP.GATEIO-1-MINUTE-LAST-EXTERNAL")
        )
        await client._subscribe_bars(bars)
        client.ws.subscribe.assert_awaited_with("futures.candlesticks", ["1m", "BTC_USDT"])
        row = {
            "n": "1m_BTC_USDT",
            "v": "2.5",
            "a": "250",
            "h": "101",
            "l": "97",
            "o": "99",
            "c": "100",
            "t": 120,
            "w": True,
        }
        await client._message(
            {"channel": "futures.candlesticks", "result": [dict(row, w=False), row, row]}
        )
        assert len(client.received) == 5
        assert client.received[-1].volume.as_decimal() == Decimal("2.5")
        await client._unsubscribe_bars(bars)
        client.ws.unsubscribe.assert_awaited_with("futures.candlesticks", ["1m", "BTC_USDT"])

    asyncio.run(run())


def test_futures_rest_market_history_and_subsecond_book_time():
    async def run():
        client, iid = await market()
        cmd = SimpleNamespace(
            instrument_id=iid,
            limit=4,
            start=None,
            end=None,
            params={},
            id=None,
            bar_type=BarType.from_str("BTC_USDT-PERP.GATEIO-1-MINUTE-LAST-EXTERNAL"),
        )
        client.http.request.side_effect = [
            {
                "id": 71,
                "update": 123.456789,
                "asks": [{"p": "101", "s": "3.2"}],
                "bids": [{"p": "100", "s": "1"}],
            },
            [
                {"t": 120, "o": "100", "h": "101", "l": "99", "c": "100", "v": "5.2"},
                {"t": 240, "o": "100", "h": "101", "l": "99", "c": "100", "v": "1.5"},
            ],
            [
                {
                    "contract": "BTC_USDT",
                    "id": 55,
                    "create_time_ms": 131333,
                    "size": "-1.3",
                    "price": "102",
                }
            ],
            [{"t": 120, "r": "-0.001"}],
        ]
        await client._request_order_book_snapshot(cmd)
        snap = client.received.pop()
        assert snap.ts_event == 123456789000
        assert snap.sequence == 71
        await client._request_bars(cmd)
        await client._request_trade_ticks(cmd)
        await client._request_funding_rates(cmd)
        bar, trade, rate = client.received
        assert bar.volume.as_decimal() == Decimal("5.2")
        assert trade.aggressor_side == AggressorSide.SELLER
        assert trade.ts_event == 131333000000
        assert rate.rate == Decimal("-0.001")

    asyncio.run(run())


def perpetual_market_node():  # noqa: C901 (local exchange and actor share the test event loop)
    import runpy
    from pathlib import Path

    from aiohttp import web

    from nautilus_trader.adapters.gateio import GateioLiveDataClientFactory
    from nautilus_trader.common.actor import Actor
    from nautilus_trader.config import LiveExecEngineConfig
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.config import TradingNodeConfig
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.data import TradeTick

    iid = InstrumentId.from_str("BTC_USDT-PERP.GATEIO")
    received = {}
    local_gate = runpy.run_path(str(Path(__file__).with_name("protocol_server.py")))["LocalGate"]

    class DerivativesGate(local_gate):
        async def http(self, request):
            assert request.match_info["tail"] == "futures/usdt/contracts/BTC_USDT"
            assert request.headers["X-Gate-Size-Decimal"] == "1"
            return web.json_response(CONTRACT)

        async def websocket(self, request):
            assert request.headers["X-Gate-Size-Decimal"] == "1"
            return await super().websocket(request)

        async def push_books(self):
            while True:
                await asyncio.sleep(0.1)
                self.sequence += 1
                await self.broadcast(
                    "futures.trades",
                    [
                        {
                            "contract": "BTC_USDT",
                            "price": "100",
                            "size": "-2.5",
                            "id": self.sequence,
                            "create_time_ms": 1_700_000_000_123,
                        }
                    ],
                )
                await self.broadcast(
                    "futures.tickers",
                    [
                        {
                            "contract": "BTC_USDT",
                            "mark_price": "100.01",
                            "index_price": "99.99",
                            "funding_rate": "-0.001",
                            "funding_next_apply": 1_700_028_800,
                        }
                    ],
                )

    class Collector(Actor):
        def on_start(self):
            self.subscribe_trade_ticks(iid)
            self.subscribe_mark_prices(iid)
            self.subscribe_index_prices(iid)
            self.subscribe_funding_rates(iid)

        def on_trade_tick(self, tick):
            received[TradeTick] = tick

        def on_mark_price(self, mark):
            received[MarkPriceUpdate] = mark

        def on_index_price(self, index):
            received[IndexPriceUpdate] = index

        def on_funding_rate(self, funding):
            received[FundingRateUpdate] = funding

    loop = asyncio.new_event_loop()
    nodes = []

    async def run():
        server = DerivativesGate()
        await server.start()
        node = TradingNode(
            TradingNodeConfig(
                logging=LoggingConfig(log_level="ERROR"),
                exec_engine=LiveExecEngineConfig(reconciliation=False),
                data_clients={
                    "GATEIO": GateioDataClientConfig(
                        product_type=GateioProductType.USDT_PERPETUAL,
                        base_url_http=server.url,
                        base_url_ws=server.ws_url,
                        instrument_provider=InstrumentProviderConfig(load_ids=frozenset({iid})),
                    )
                },
                timeout_post_stop=0.1,
            ),
            loop=loop,
        )
        nodes.append(node)
        node.add_data_client_factory("GATEIO", GateioLiveDataClientFactory)
        node.trader.add_actor(Collector())
        node.build()
        task = asyncio.create_task(node.run_async())
        try:
            async with asyncio.timeout(15):
                while (
                    not {TradeTick, MarkPriceUpdate, IndexPriceUpdate, FundingRateUpdate}
                    <= received.keys()
                ):
                    if task.done():
                        await task
                        raise AssertionError(
                            "TradingNode exited before receiving the futures feeds"
                        )
                    await asyncio.sleep(0.05)
            assert all(item.instrument_id == iid for item in received.values())
            assert node.kernel.cache.instrument(iid).multiplier.as_decimal() == Decimal("0.001")
            trade = received[TradeTick]
            assert trade.size.as_decimal() == Decimal("2.5")
            assert trade.aggressor_side == AggressorSide.SELLER
            assert trade.ts_event == 1_700_000_000_123_000_000
            assert received[MarkPriceUpdate].value.as_decimal() == Decimal("100.01")
            assert received[IndexPriceUpdate].value.as_decimal() == Decimal("99.99")
            assert received[FundingRateUpdate].rate == Decimal("-0.001")
            assert received[FundingRateUpdate].next_funding_ns == 1_700_028_800_000_000_000
        finally:
            await node.stop_async()
            await asyncio.wait_for(task, 15)
            await server.stop()

    try:
        loop.run_until_complete(run())
    finally:
        for node in nodes:
            node.dispose()


def test_perpetual_market_data_through_trading_node():
    import subprocess
    import sys

    script = f"import runpy; runpy.run_path({__file__!r})['perpetual_market_node']()"
    result = subprocess.run(  # noqa: S603 (local test module in an isolated TradingNode process)
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
