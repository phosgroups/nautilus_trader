"""Route historical and live Gate market data through the v1 actor interface."""

import asyncio
import runpy
import subprocess
import sys
from datetime import UTC
from datetime import datetime
from pathlib import Path

from aiohttp import web


def market_data_node():
    from nautilus_trader.adapters.gateio import GateioDataClientConfig
    from nautilus_trader.adapters.gateio import GateioLiveDataClientFactory
    from nautilus_trader.common.actor import Actor
    from nautilus_trader.config import InstrumentProviderConfig
    from nautilus_trader.config import LiveExecEngineConfig
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.config import TradingNodeConfig
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.data import Bar
    from nautilus_trader.model.data import BarType
    from nautilus_trader.model.data import OrderBookDeltas
    from nautilus_trader.model.data import OrderBookDepth10

    bar_type = BarType.from_str("BTC_USDT.GATEIO-1-MINUTE-LAST-EXTERNAL")
    iid = bar_type.instrument_id
    opened = 1_700_000_040
    local_gate = runpy.run_path(str(Path(__file__).with_name("protocol_server.py")))["LocalGate"]
    received, completed, requests = {}, set(), set()

    class MarketGate(local_gate):
        async def http(self, request):
            if request.match_info["tail"] == "spot/candlesticks":
                assert request.query["currency_pair"] == "BTC_USDT"
                assert request.query["interval"] == "1m"
                return web.json_response([[opened, "155", "101", "103", "98", "100", "7", "true"]])
            return await super().http(request)

    class Consumer(Actor):
        def on_start(self):
            requests.add(self.request_bars(
                bar_type, start=datetime.fromtimestamp(opened - 60, UTC),
                end=datetime.fromtimestamp(opened + 60, UTC), callback=completed.add,
            ))
            requests.add(self.request_order_book_snapshot(iid, limit=2, callback=completed.add))
            self.subscribe_order_book_depth(iid, depth=10)

        def on_historical_data(self, data):
            received[type(data)] = data

        def on_order_book_depth(self, depth):
            received[OrderBookDepth10] = depth

    loop = asyncio.new_event_loop()
    nodes = []

    async def run():
        server = MarketGate()
        await server.start()
        node = TradingNode(TradingNodeConfig(
            logging=LoggingConfig(log_level="ERROR"), exec_engine=LiveExecEngineConfig(reconciliation=False),
            data_clients={"GATEIO": GateioDataClientConfig(
                base_url_http=server.url, base_url_ws=server.ws_url,
                instrument_provider=InstrumentProviderConfig(load_ids=frozenset({iid})),
            )}, timeout_post_stop=0.1,
        ), loop=loop)
        nodes.append(node)
        node.add_data_client_factory("GATEIO", GateioLiveDataClientFactory)
        node.trader.add_actor(Consumer())
        node.build()
        task = asyncio.create_task(node.run_async())
        try:
            async with asyncio.timeout(15):
                while not ({Bar, OrderBookDeltas, OrderBookDepth10} <= received.keys() and requests == completed):
                    if task.done():
                        await task
                        raise AssertionError("TradingNode stopped before market data arrived")
                    await asyncio.sleep(0.05)
            assert len(completed) == 2
            assert received[Bar].bar_type == bar_type
            assert received[Bar].ts_event == opened * 10**9
            assert int(received[Bar].volume) == 7
            assert received[OrderBookDeltas].sequence >= 100
            depth = received[OrderBookDepth10]
            assert depth.instrument_id == iid
            assert int(depth.bids[0].price) == 99990
            assert int(depth.asks[0].price) == 100010
        finally:
            await node.stop_async()
            await asyncio.wait_for(task, 15)
            await server.stop()

    try:
        loop.run_until_complete(run())
    finally:
        for node in nodes:
            node.dispose()


def test_history_snapshot_and_live_depth_reach_actor_callbacks():
    script = f"import runpy; runpy.run_path({__file__!r})['market_data_node']()"
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=45, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
