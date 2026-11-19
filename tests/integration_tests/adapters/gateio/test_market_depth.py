import asyncio
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import OrderBookDepth10
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId


ID = InstrumentId.from_str("BTC_USDT.GATEIO")


def test_depth_and_deltas_share_a_single_wire_book_and_unsubscribe_independently():
    async def run():
        client = SimpleNamespace(
            books={}, resyncs={}, config=SimpleNamespace(book_depth=1, max_book_buffer=100),
            _clock=SimpleNamespace(timestamp_ns=lambda: 5), _handle_data=Mock(),
            _instrument_provider=SimpleNamespace(find=lambda _: object()), _schedule_resync=Mock(),
            _subscription_lock=lambda *_: asyncio.Lock(), ws=AsyncMock(),
        )
        for method in ("_emit", "_book_update", "_subscribe_book", "_unsubscribe_book"):
            setattr(client, method, partial(getattr(GateioDataClient, method), client))
        cmd = SimpleNamespace(instrument_id=ID, book_type=BookType.L2_MBP, depth=10)
        await GateioDataClient._subscribe_order_book_depth(client, cmd)
        client._handle_data.assert_not_called()
        msg = {"channel": "spot.order_book_update", "result": {
            "s": "BTC_USDT", "full": True, "l": 100, "u": 101, "t": 5,
            "b": [["98", "2"], ["99", "1"]], "a": [["101", "3"]],
        }}
        await GateioDataClient._message(client, msg)
        depth = client._handle_data.call_args.args[0]
        assert isinstance(depth, OrderBookDepth10)
        assert depth.sequence == 101
        assert [int(x.price) for x in depth.bids[:2]] == [99, 98]
        assert int(depth.asks[0].size) == 3
        assert int(depth.asks[1].size) == 0

        await GateioDataClient._subscribe_order_book_deltas(client, cmd)
        assert isinstance(client._handle_data.call_args.args[0], OrderBookDeltas)
        client.ws.subscribe.assert_awaited_once()
        client._handle_data.reset_mock()
        await GateioDataClient._unsubscribe_order_book_deltas(client, cmd)
        client.ws.unsubscribe.assert_not_awaited()
        await GateioDataClient._message(client, msg)
        assert isinstance(client._handle_data.call_args.args[0], OrderBookDepth10)
        await GateioDataClient._unsubscribe_order_book_depth(client, cmd)
        client.ws.unsubscribe.assert_awaited_once()
        assert not client.books

    asyncio.run(run())


def test_snapshot_request_returns_correlated_historical_deltas_without_touching_live_book():
    request = SimpleNamespace(instrument_id=ID, limit=1, id="id", start=None, end=None, params={})
    client = SimpleNamespace(
        books={}, _clock=SimpleNamespace(timestamp_ns=lambda: 9_000_000),
        _history_instrument=AsyncMock(return_value=SimpleNamespace(id=ID)),
        _fetch_book=AsyncMock(return_value={"id": 42, "update": 8, "bids": [["99", "2"]], "asks": []}),
        _handle_data_response=Mock(),
    )
    asyncio.run(GateioDataClient._request_order_book_snapshot(client, request))
    client._fetch_book.assert_awaited_once_with(ID, 1)
    response = client._handle_data_response.call_args.kwargs
    assert response["correlation_id"] == "id"
    deltas, = response["data"]
    assert isinstance(deltas, OrderBookDeltas)
    assert deltas.sequence == 42
    assert deltas.ts_event == 8_000_000
    assert not client.books
