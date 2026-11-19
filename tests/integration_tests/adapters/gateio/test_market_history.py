import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pandas as pd

from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.model.data import BarType


BAR_TYPE = BarType.from_str("BTC_USDT.GATEIO-1-MINUTE-LAST-EXTERNAL")
INSTRUMENT = parse_instrument(
    {"id": "BTC_USDT", "base": "BTC", "quote": "USDT", "precision": 2, "amount_precision": 4}, 0,
)


def test_history_bars_split_gate_windows_filter_final_and_preserve_base_units():
    end = pd.Timestamp(1001 * 60, unit="s", tz="UTC")
    start = pd.Timestamp(0, unit="s", tz="UTC")
    request = SimpleNamespace(bar_type=BAR_TYPE, start=start, end=end, limit=0, id="id", params={})
    pages = [
        [[60060, "111", "2", "3", "1", "2", "42", "false"],
         [60000, "222", "2", "3", "1", "2", "5", "true"]],
        [[0, "333", "2", "3", "1", "2", "7", "true"]],
    ]
    client = SimpleNamespace(
        http=SimpleNamespace(request=AsyncMock(side_effect=pages)),
        _clock=SimpleNamespace(timestamp_ns=lambda: end.value),
        _history_instrument=AsyncMock(return_value=INSTRUMENT), _handle_bars=Mock(),
    )
    asyncio.run(GateioDataClient._request_bars(client, request))
    series, bars, rid, begin, until, params = client._handle_bars.call_args.args
    assert (series, rid, begin, until, params) == (BAR_TYPE, "id", start, end, {})
    assert [(b.ts_event, int(b.volume)) for b in bars] == [(0, 7), (60000 * 10**9, 5)]
    params = [call.args[2] for call in client.http.request.await_args_list]
    assert len(params) == 2
    assert all("limit" not in p and p["to"] - p["from"] < 60000 for p in params)
    assert params[0]["from"] == params[1]["to"] + 1


def test_history_trades_page_sort_deduplicate_and_filter_exact_bounds(monkeypatch):
    monkeypatch.setattr("nautilus_trader.adapters.gateio.history.PAGE_SIZE", 2)
    start = pd.Timestamp("2020-01-01T00:00:00.010Z")
    end = start + pd.Timedelta(seconds=1)
    request = SimpleNamespace(instrument_id=INSTRUMENT.id, start=start, end=end, limit=0, id="id", params={})

    def row(i, offset):
        return {"currency_pair": "BTC_USDT", "price": "1", "amount": "1", "side": "sell",
                "create_time_ms": str(start.value // 10**6 + offset), "id": str(i)}

    client = SimpleNamespace(
        http=SimpleNamespace(request=AsyncMock(side_effect=[
            [row(3, 500), row(2, 200)], [row(2, 200), row(1, -3)], [],
        ])), _clock=SimpleNamespace(timestamp_ns=lambda: end.value),
        _history_instrument=AsyncMock(return_value=INSTRUMENT), _handle_trade_ticks=Mock(),
    )
    asyncio.run(GateioDataClient._request_trade_ticks(client, request))
    iid, ticks, rid, begin, until, params = client._handle_trade_ticks.call_args.args
    assert (iid, rid, begin, until, params) == (INSTRUMENT.id, "id", start, end, {})
    assert [str(t.trade_id) for t in ticks] == ["2", "3"]
    assert [call.args[2]["page"] for call in client.http.request.await_args_list] == [1, 2, 3]
