"""Opt-in public REST/WS smoke test. Never authenticates or sends order requests."""

import asyncio
import os

import pytest

from nautilus_trader.adapters.gateio.config import GateioDataClientConfig
from nautilus_trader.adapters.gateio.config import GateioEnvironment
from nautilus_trader.adapters.gateio.config import endpoints
from nautilus_trader.adapters.gateio.data import BookGap
from nautilus_trader.adapters.gateio.data import GateBook
from nautilus_trader.adapters.gateio.http import GateioHttpClient
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.common.component import LiveClock


@pytest.mark.skipif(
    os.environ.get("GATE_PUBLIC_SMOKE") != "1", reason="Opt-in external network test"
)
def test_public_rest_and_sequence_bridged_ws():
    async def run():
        clock = LiveClock()
        http_url, ws_url = endpoints(GateioEnvironment.LIVE)
        client = GateioHttpClient(clock, http_url, GateioDataClientConfig())
        pair = await client.request("GET", "/spot/currency_pairs/BTC_USDT")
        assert str(parse_instrument(pair, clock.timestamp_ns()).id) == "BTC_USDT.GATEIO"
        book = GateBook(100)
        received = asyncio.Event()

        async def update(message):
            if message.get("channel") == "spot.order_book_update" and book.update(
                message["result"]
            ):
                received.set()

        async def reconnect():
            raise RuntimeError("Public smoke test disconnected before completing")

        ws = GateioWebSocket(asyncio.get_running_loop(), clock, ws_url, update, reconnect)
        try:
            await ws.connect()
            await ws.subscribe("spot.order_book_update", ["BTC_USDT", "100ms"])
            attempts = []
            for attempt in range(4):
                snapshot = await client.request(
                    "GET",
                    "/spot/order_book",
                    {"currency_pair": "BTC_USDT", "limit": 100, "with_id": "true"},
                )
                book.snapshot(snapshot)
                pending, book.buffer = book.buffer, []
                attempts.append(
                    {
                        "snapshot_id": snapshot["id"],
                        "snapshot_current": snapshot.get("current"),
                        "first_ws": pending[0]["U"] if pending else None,
                        "last_ws": pending[-1]["u"] if pending else None,
                        "local_ms": clock.timestamp_ns() // 1_000_000,
                    }
                )
                try:
                    for row in pending:
                        if book.update(row):
                            received.set()
                    break
                except BookGap:
                    book.sequence = None
                    book.buffer.extend(pending)
                    if attempt < 3:
                        await asyncio.sleep(min(0.25 * 2**attempt, 2))
            else:
                raise AssertionError(
                    f"Gate public REST snapshot did not bridge the WS feed: {attempts}"
                )
            await asyncio.wait_for(received.wait(), 15)
            assert book.ready
            assert max(book.bids) < min(book.asks)
            assert ws.error is None
            print(
                f"Gate public sequence={book.sequence}, bids={len(book.bids)}, asks={len(book.asks)}"
            )
        finally:
            await ws.disconnect()

    asyncio.run(run())
