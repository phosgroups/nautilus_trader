"""Slow balance reads must not delay Gate order reports or lose account updates."""

import asyncio
import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from nautilus_trader.adapters.gateio.config import GateioExecClientConfig
from nautilus_trader.adapters.gateio.execution import GateioExecutionClient
from nautilus_trader.adapters.gateio.providers import GateioInstrumentProvider
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.test_kit.stubs.component import TestComponentStubs


PAIR = {
    "id": "BTC_USDT",
    "base": "BTC",
    "quote": "USDT",
    "precision": 2,
    "amount_precision": 6,
    "min_base_amount": "0.000001",
    "min_quote_amount": "1",
    "trade_status": "tradable",
}


def balances(available="1000", locked="0"):
    return [{"currency": "USDT", "available": available, "locked": locked}]


def canceled_order():
    return {
        "id": "42",
        "currency_pair": "BTC_USDT",
        "amount": "0.0001",
        "left": "0.0001",
        "price": "99000",
        "side": "buy",
        "status": "cancelled",
        "time_in_force": "poc",
        "create_time_ms": "1793620800123",
    }


class RecordingClient(GateioExecutionClient):
    def generate_account_state(self, **kwargs):
        self.states.append(kwargs)

    def _send_order_status_report(self, report):
        self.reports.append(report)


def client_for(request, interval=10, **config):
    clock = TestClock()

    async def dispatch(*args, **kwargs):
        before_send = kwargs.pop("before_send", None)
        if before_send is not None:
            before_send()
        result = request(*args, **kwargs)
        return await result if inspect.isawaitable(result) else result

    http = SimpleNamespace(request=AsyncMock(side_effect=dispatch), pages=AsyncMock(return_value=[]))
    provider = GateioInstrumentProvider(http, clock, InstrumentProviderConfig())
    provider.add(parse_instrument(PAIR, 0))
    client = RecordingClient(
        asyncio.get_running_loop(),
        http,
        TestComponentStubs.msgbus(),
        TestComponentStubs.cache(),
        clock,
        provider,
        GateioExecClientConfig(account_refresh_interval_ms=interval, **config),
    )
    client.states, client.reports = [], []
    client._account_paused = False
    return client


async def wait_for(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.001)


def account_idle(client):
    return (
        not client._account_dirty
        and not client._account_requested.is_set()
        and client._account_read_task is not None
        and client._account_read_task.done()
        and not client._account_read_task.cancelled()
        and client._account_read_task.exception() is None
    )


def test_balance_burst_is_coalesced_and_slow_rest_does_not_block_ws_cancel():
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def request(*args, **kwargs):
            entered.set()
            await release.wait()
            return balances()

        client = client_for(request)
        ws = client.ws
        ws.worker = asyncio.create_task(ws._consume())
        try:
            for _ in range(200):
                ws.queue.put_nowait({"channel": "spot.balances", "result": []})
            await entered.wait()
            ws.queue.put_nowait({"channel": "spot.orders", "result": [canceled_order()]})
            await asyncio.wait_for(ws.queue.join(), 0.2)
            assert client.reports[0].order_status == OrderStatus.CANCELED
            assert not client._report_lock.locked()
            assert client.http.request.await_count == 1
            release.set()
            await wait_for(lambda: account_idle(client))
            # The order notification during the first read requires one final read.
            assert client.http.request.await_count == 2
            assert len(client.states) == 2
        finally:
            await ws.disconnect()
            await client._cancel_account_tasks()

    asyncio.run(run())


def test_notification_during_read_is_not_lost_and_uses_latest_full_balances():
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()
        count = 0

        async def request(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 1:
                entered.set()
                await release.wait()
                return balances("900", "100")
            return balances("1000", "0")

        client = client_for(request)
        try:
            client._request_account_refresh()
            await entered.wait()
            for _ in range(100):
                client._request_account_refresh()
            release.set()
            await wait_for(lambda: account_idle(client))
            assert count == 2
            latest = {b.currency.code: b for b in client.states[-1]["balances"]}
            assert latest["USDT"].free.as_decimal() == 1000
            assert latest["USDT"].locked.as_decimal() == 0
            assert latest["BTC"].total.as_decimal() == 0
        finally:
            await client._cancel_account_tasks()

    asyncio.run(run())


def test_explicit_queries_share_one_read_and_canceling_one_waiter_is_safe():
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def request(*args, **kwargs):
            entered.set()
            await release.wait()
            return balances()

        client = client_for(request)
        first = asyncio.create_task(client._query_account(None))
        second = asyncio.create_task(client._query_account(None))
        await entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not client._account_read_task.cancelled()
        release.set()
        await second
        assert client.http.request.await_count == 1
        assert len(client.states) == 1
        await client._cancel_account_tasks()

    asyncio.run(run())


@pytest.mark.parametrize("failure", [TimeoutError(), ValueError("invalid account response")])
def test_failed_refresh_retries_without_another_ws_notification(failure):
    async def run():
        replies = iter([failure, balances()])

        async def request(*args, **kwargs):
            reply = next(replies)
            if isinstance(reply, Exception):
                raise reply
            return reply

        client = client_for(request)
        client._request_account_refresh()
        await wait_for(lambda: len(client.states) == 1)
        await wait_for(lambda: account_idle(client))
        assert client.http.request.await_count == 2
        assert not client._account_dirty
        await client._cancel_account_tasks()

    asyncio.run(run())


def test_invalid_snapshot_is_not_published_and_retry_recovers():
    async def run():
        replies = iter([balances("-1"), balances()])
        client = client_for(lambda *args, **kwargs: next(replies))
        client._request_account_refresh()
        await wait_for(lambda: account_idle(client))
        assert client.http.request.await_count == 2
        assert len(client.states) == 1
        await client._cancel_account_tasks()

    asyncio.run(run())


def test_reconnect_cancels_old_read_and_old_response_cannot_overwrite_recovery():
    async def run():
        entered = asyncio.Event()

        async def request(*args, **kwargs):
            if not entered.is_set():
                entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    # Even a transport which returns after cancellation is fenced out.
                    return balances("1")
            return balances("1000")

        client = client_for(request)
        client._request_account_refresh()
        await entered.wait()
        await client._reconcile()
        await wait_for(lambda: account_idle(client))
        assert client._account_is_fresh()
        assert not client._account_paused
        assert all(
            b.free.as_decimal() == 1000
            for state in client.states
            for b in state["balances"]
            if b.currency.code == "USDT"
        )
        await client._cancel_account_tasks()

    asyncio.run(run())


def test_account_query_during_recovery_does_not_start_competing_snapshot():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        client._account_paused = True
        with pytest.raises(RuntimeError, match="not ready"):
            await client._query_account(None)
        assert client.http.request.await_count == 0

    asyncio.run(run())


def test_cancel_confirmation_does_not_wait_for_pending_balance_refresh():
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def request(method, path, *args, **kwargs):
            if path == "/spot/accounts":
                entered.set()
                await release.wait()
                return balances()
            assert method == "DELETE"
            return canceled_order()

        client = client_for(request)
        client._request_account_refresh()
        await entered.wait()
        command = SimpleNamespace(
            client_order_id=ClientOrderId("cancel-with-slow-balances"),
            venue_order_id="42",
            instrument_id=next(iter(client._instrument_provider.get_all())),
        )
        try:
            await asyncio.wait_for(client._cancel_order(command), 0.2)
            assert client.reports[0].order_status == OrderStatus.CANCELED
        finally:
            release.set()
            await client._cancel_account_tasks()

    asyncio.run(run())


def test_disconnect_cancels_old_refresh_and_publishes_final_unlocked_balances():
    async def run():
        entered = asyncio.Event()

        async def request(*args, **kwargs):
            if not entered.is_set():
                entered.set()
                await asyncio.Event().wait()
            return balances()

        client = client_for(request)
        client._request_account_refresh()
        await entered.wait()
        await client._disconnect()
        assert len(client.states) == 1
        assert all(b.locked.as_decimal() == 0 for b in client.states[0]["balances"])
        assert client._account_read_task is None
        assert client._account_refresh_task is None
        client._request_account_refresh()
        assert client._account_refresh_task is None

    asyncio.run(run())


def test_disconnect_timeout_also_cancels_the_shielded_http_read():
    async def run():
        canceled = asyncio.Event()

        async def request(*args, **kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                canceled.set()

        client = client_for(request, http_timeout_secs=1)
        await asyncio.wait_for(client._disconnect(), 2)
        assert canceled.is_set()
        assert client._account_read_task is None
        assert client._account_refresh_task is None
        assert client.states == []

    asyncio.run(run())


def test_failed_ws_connection_does_not_leave_consumer_task():
    async def run():
        ws = GateioWebSocket(
            asyncio.get_running_loop(), TestClock(), "ws://local", AsyncMock(), AsyncMock()
        )
        with (
            patch(
                "nautilus_trader.adapters.gateio.websocket.WebSocketClient.connect",
                new=AsyncMock(side_effect=ConnectionError()),
            ),
            pytest.raises(ConnectionError),
        ):
            await ws.connect()
        assert ws.worker is None

    asyncio.run(run())


@pytest.mark.parametrize("interval", [0, -1])
def test_account_refresh_interval_must_be_positive(interval):
    async def run():
        with pytest.raises(ValueError, match="must be positive"):
            client_for(lambda *args, **kwargs: balances(), interval=interval)

    asyncio.run(run())


def test_ws_disconnect_error_still_cancels_consumer_and_account_worker():
    async def run():
        entered = asyncio.Event()

        async def request(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()

        client = client_for(request)
        client._request_account_refresh()
        await entered.wait()
        worker = asyncio.create_task(client.ws._consume())
        client.ws.worker = worker
        client.ws.client = SimpleNamespace(disconnect=AsyncMock(side_effect=ConnectionError()))
        with pytest.raises(ConnectionError):
            await client._disconnect()
        assert worker.cancelled()
        assert client.ws.worker is None
        assert client._account_read_task is None
        assert client._account_refresh_task is None

    asyncio.run(run())
