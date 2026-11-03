"""Opt-in read-only public Gate feed through all three native data subscriptions."""

import asyncio
import os
import subprocess
import sys

import pytest


def public_node():
    from nautilus_trader.adapters.gateio import GateioDataClientConfig
    from nautilus_trader.adapters.gateio import GateioEnvironment
    from nautilus_trader.adapters.gateio import GateioLiveDataClientFactory
    from nautilus_trader.common.actor import Actor
    from nautilus_trader.config import InstrumentProviderConfig
    from nautilus_trader.config import LiveExecEngineConfig
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.config import TradingNodeConfig
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.enums import BookType
    from nautilus_trader.model.identifiers import InstrumentId

    instrument_id = InstrumentId.from_str("BTC_USDT.GATEIO")
    counts = {"books": 0, "trades": 0, "quotes": 0}

    class Collector(Actor):
        def on_start(self):
            self.subscribe_order_book_deltas(instrument_id, BookType.L2_MBP, managed=True)
            self.subscribe_trade_ticks(instrument_id)
            self.subscribe_quote_ticks(instrument_id)

        def on_order_book_deltas(self, data):
            counts["books"] += 1

        def on_trade_tick(self, data):
            assert data.price.as_double() > 0
            assert data.ts_event > 0
            counts["trades"] += 1

        def on_quote_tick(self, data):
            assert data.bid_price < data.ask_price
            counts["quotes"] += 1

    loop = asyncio.new_event_loop()
    node = TradingNode(
        TradingNodeConfig(
            logging=LoggingConfig(log_level="INFO"),
            exec_engine=LiveExecEngineConfig(reconciliation=False),
            timeout_post_stop=0.1,
            data_clients={
                "GATEIO": GateioDataClientConfig(
                    environment=GateioEnvironment.LIVE,
                    proxy_url=os.environ.get("GATE_PUBLIC_PROXY_URL") or None,
                    instrument_provider=InstrumentProviderConfig(
                        load_ids=frozenset({instrument_id})
                    ),
                )
            },
        ),
        loop=loop,
    )
    node.add_data_client_factory("GATEIO", GateioLiveDataClientFactory)
    node.trader.add_actor(Collector())
    node.build()

    async def run():
        task = asyncio.create_task(node.run_async())
        try:
            async with asyncio.timeout(45):
                while not all(counts.values()):
                    assert not task.done(), "Native public node exited early"
                    await asyncio.sleep(0.1)
            print(f"Gate native public subscriptions: {counts}")
        finally:
            print(f"Gate public counts at shutdown: {counts}")
            await node.stop_async()
            await asyncio.wait_for(task, 15)

    try:
        loop.run_until_complete(run())
    finally:
        node.dispose()


@pytest.mark.skipif(
    os.environ.get("GATE_PUBLIC_SMOKE") != "1", reason="Opt-in external network test"
)
def test_all_public_channels_through_native_node():
    script = f"import runpy; runpy.run_path({__file__!r})['public_node']()"
    result = subprocess.run(  # noqa: S603 (fixed local test script)
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
