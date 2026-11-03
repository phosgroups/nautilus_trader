"""Already-filled external Gate orders must retain their real executions and fees."""

import asyncio
import runpy
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path


def cold_scenario():
    from nautilus_trader.adapters.gateio import GateioDataClientConfig
    from nautilus_trader.adapters.gateio import GateioExecClientConfig
    from nautilus_trader.adapters.gateio import GateioLiveDataClientFactory
    from nautilus_trader.adapters.gateio import GateioLiveExecClientFactory
    from nautilus_trader.config import InstrumentProviderConfig
    from nautilus_trader.config import LiveExecEngineConfig
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.config import StrategyConfig
    from nautilus_trader.config import TradingNodeConfig
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.events import OrderFilled
    from nautilus_trader.model.identifiers import InstrumentId
    from nautilus_trader.trading.strategy import Strategy

    server_type = runpy.run_path(str(Path(__file__).with_name("protocol_server.py")))["LocalGate"]
    instrument_id = InstrumentId.from_str("BTC_USDT.GATEIO")
    loop = asyncio.new_event_loop()
    nodes = []

    async def run():
        server = server_type()
        await server.start()
        server.orders["123"] = {
            "id": "123",
            "text": "t-external-cold",
            "currency_pair": "BTC_USDT",
            "amount": "0.0001",
            "left": "0.0001",
            "price": "99900",
            "side": "buy",
            "status": "open",
            "time_in_force": "gtc",
            "create_time_ms": str(int(time.time() * 1000)),
        }
        await server.partial_fill("123")
        options = {
            "instrument_provider": InstrumentProviderConfig(load_ids=frozenset({instrument_id})),
            "base_url_http": server.url,
            "base_url_ws": server.ws_url,
        }
        node = TradingNode(
            TradingNodeConfig(
                logging=LoggingConfig(log_level="ERROR"),
                timeout_post_stop=0.1,
                exec_engine=LiveExecEngineConfig(
                    reconciliation=True,
                    open_check_interval_secs=0,
                    position_check_interval_secs=0,
                ),
                data_clients={"GATEIO": GateioDataClientConfig(**options)},
                exec_clients={
                    "GATEIO": GateioExecClientConfig(
                        **options,
                        api_key="test-key",
                        api_secret="test-secret",
                    )
                },
            ),
            loop=loop,
        )
        nodes.append(node)
        observer = Strategy(
            StrategyConfig(
                strategy_id="COLD-OBSERVER",
                order_id_tag="001",
                external_order_claims=[instrument_id],
            )
        )
        node.trader.add_strategy(observer)
        node.add_data_client_factory("GATEIO", GateioLiveDataClientFactory)
        node.add_exec_client_factory("GATEIO", GateioLiveExecClientFactory)
        node.build()
        events = []
        node.kernel.msgbus.subscribe(f"events.order.{observer.id}", events.append)
        task = asyncio.create_task(node.run_async())
        try:
            async with asyncio.timeout(15):
                while not any(isinstance(e, OrderFilled) for e in events):
                    await asyncio.sleep(0.05)
            fills = [e for e in events if isinstance(e, OrderFilled)]
            assert len(fills) == 1
            fill = fills[0]
            assert str(fill.trade_id) == "fill-1"
            assert fill.last_px.as_decimal() == Decimal(99900)
            assert fill.commission.as_decimal() == Decimal("0.00000001")
            assert str(fill.commission.currency) == "BTC"
            assert server.post_count == 0
            assert server.cancel_ids == []
        finally:
            await node.stop_async()
            await asyncio.wait_for(task, 15)
            await server.stop()

    try:
        loop.run_until_complete(run())
    finally:
        for node in nodes:
            node.dispose()


def test_cold_partial_fill_is_a_real_trade_not_an_inferred_execution():
    script = f"import runpy; runpy.run_path({__file__!r})['cold_scenario']()"
    result = subprocess.run(  # noqa: S603 (fixed local test script)
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
