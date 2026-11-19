import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.model.data import BarType


def test_external_bars_subscribe_multiple_windows_and_publish_only_the_confirmed_close():
    client = SimpleNamespace(
        ws=AsyncMock(), _candles={}, _history_instrument=AsyncMock(
            return_value=SimpleNamespace(raw_symbol=SimpleNamespace(value="BTC_USDT")),
        ),
        _subscription_lock=lambda *_: asyncio.Lock(), _clock=SimpleNamespace(timestamp_ns=lambda: 42),
        _handle_data=Mock(), _log=Mock(),
    )
    client._candle_update = lambda row: GateioDataClient._candle_update(client, row)
    async def run():
        commands = [SimpleNamespace(bar_type=BarType.from_str(
            f"BTC_USDT.GATEIO-{n}-MINUTE-LAST-EXTERNAL"
        )) for n in (1, 5)]
        for cmd in commands:
            await GateioDataClient._subscribe_bars(client, cmd)
        for closed in (False, True, True):
            await GateioDataClient._message(client, {"channel": "spot.candlesticks", "result": {
                "w": closed, "t": "600", "n": "5m_BTC_USDT", "v": "231", "a": "2",
                "o": "100", "h": "103", "l": "98", "c": "102",
            }})
        assert client.ws.subscribe.await_count == 2
        client._handle_data.assert_called_once()
        bar = client._handle_data.call_args.args[0]
        assert bar.bar_type == commands[1].bar_type
        assert str(bar.volume) == "2"
        assert bar.ts_event == 600 * 10**9
        await GateioDataClient._unsubscribe_bars(client, commands[1])
        client.ws.unsubscribe.assert_awaited_once_with("spot.candlesticks", ["5m", "BTC_USDT"])
        assert list(client._candles) == ["1m_BTC_USDT"]
    asyncio.run(run())
