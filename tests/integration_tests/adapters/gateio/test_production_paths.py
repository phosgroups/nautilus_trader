"""Production entry points: throttled writes, periodic checks and paired recovery."""

import asyncio
import json
import runpy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from nautilus_trader.adapters.gateio.config import GateioDataClientConfig
from nautilus_trader.adapters.gateio.http import GateioHttpClient
from nautilus_trader.adapters.gateio.parsing import fill_report
from nautilus_trader.adapters.gateio.parsing import order_report
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.common.component import TestClock
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


helpers = runpy.run_path(str(Path(__file__).with_name("test_recovery_edges.py")))
client_for, cleanup = helpers["client_for"], helpers["cleanup"]
partial, fill, balances = helpers["partial"], helpers["fill"], helpers["balances"]
submission, wait_for = helpers["submission"], helpers["wait_for"]


async def bounded(coroutine):
    async with asyncio.timeout(5):
        return await coroutine


def query(client):
    return SimpleNamespace(
        instrument_id=next(iter(client._instrument_provider.get_all())),
        start=None, end=None, open_only=True,
    )


def test_private_signature_is_generated_after_waiting_for_a_rate_slot():
    async def run():
        clock = TestClock()
        client = GateioHttpClient(clock, "http://local/api/v4", GateioDataClientConfig(), "k", "s")
        entered, release = asyncio.Event(), asyncio.Event()

        async def queued(*_):
            entered.set()
            await release.wait()

        client._throttle = queued
        client.transport = SimpleNamespace(request=AsyncMock(
            return_value=SimpleNamespace(status=200, body=b"[]")
        ))
        task = asyncio.create_task(client.request("GET", "/spot/accounts", private=True))
        await entered.wait()
        clock.set_time(90_000_000_000)
        release.set()
        await task
        assert client.transport.request.call_args.kwargs["headers"]["Timestamp"] == "90"

    asyncio.run(bounded(run()))


def test_cancels_and_account_reads_are_not_starved_by_a_queued_ladder():
    async def run():
        client = GateioHttpClient(
            TestClock(), "http://local/api/v4", GateioDataClientConfig(http_rate_per_second=100)
        )
        arrivals = []

        async def transport(**kwargs):
            arrivals.append(kwargs["url"])
            return SimpleNamespace(status=200, body=b"[]")

        client.transport = SimpleNamespace(request=AsyncMock(side_effect=transport))
        client._next_request_at = asyncio.get_running_loop().time() + 0.03
        await asyncio.gather(
            *(client.request("POST", f"/spot/orders?test={i}") for i in range(20)),
            client.request("GET", "/spot/accounts"),
            client.request("DELETE", "/spot/orders/urgent"),
        )
        assert arrivals[0].endswith("/spot/orders/urgent")
        assert arrivals[1].endswith("/spot/accounts")
        assert client._waiting_requests == [0, 0, 0]

    asyncio.run(bounded(run()))


def test_cancel_during_http_quota_wait_prevents_a_late_post():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        client.ws.is_ready = lambda: True
        client._account_last_success = client._loop.time()
        command = submission(client)
        client.generate_order_canceled = Mock()
        http = GateioHttpClient(client._clock, "http://local/api/v4", GateioDataClientConfig())
        entered, release = asyncio.Event(), asyncio.Event()

        async def queued(*_):
            entered.set()
            await release.wait()

        http._throttle = queued
        http.transport = SimpleNamespace(request=AsyncMock())
        client.http = http
        try:
            task = asyncio.create_task(client._submit_order(command))
            await entered.wait()
            await client._cancel_order(SimpleNamespace(
                client_order_id=command.order.client_order_id,
                strategy_id=command.order.strategy_id,
                instrument_id=command.order.instrument_id,
                venue_order_id=None,
            ))
            release.set()
            await task
            http.transport.request.assert_not_awaited()
            client.generate_order_submitted.assert_not_called()
            client.generate_order_canceled.assert_called_once()
            client.generate_order_denied.assert_not_called()
            assert not client._waiting_submissions
            assert not client._inflight_submissions
        finally:
            release.set()
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_account_expiry_in_http_queue_is_a_local_denial_without_a_post():
    async def run():
        client = client_for(lambda *args, **kwargs: balances(), submission_ready_timeout_secs=0.05)
        client.ws.is_ready = lambda: True
        client._account_last_success = client._loop.time()
        command = submission(client)
        http = GateioHttpClient(client._clock, "http://local/api/v4", GateioDataClientConfig())

        async def expired(*_):
            client._account_last_success = float("-inf")

        http._throttle = expired
        http.transport = SimpleNamespace(request=AsyncMock())
        client.http = http
        try:
            await client._submit_order(command)
            client.generate_order_denied.assert_called_once()
            client.generate_order_submitted.assert_not_called()
            http.transport.request.assert_not_awaited()
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_brief_private_disconnect_during_quota_wait_defers_instead_of_denying():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        ready = True
        client.ws.is_ready = lambda: ready
        client._account_last_success = client._loop.time()
        command = submission(client)
        http = GateioHttpClient(
            client._clock, "http://local/api/v4", GateioDataClientConfig(), "k", "s"
        )
        submissions = 0
        recovery_tasks = []

        async def recover():
            nonlocal ready
            await asyncio.sleep(0.02)
            ready = True

        async def queued(priority):
            nonlocal ready, submissions
            if priority == 2:
                submissions += 1
                if submissions == 1:
                    ready = False
                    recovery_tasks.append(asyncio.create_task(recover()))

        async def transport(**kwargs):
            row = balances() if kwargs["url"].endswith("accounts") else helpers["canceled_order"]()
            return SimpleNamespace(status=200, body=json.dumps(row).encode())

        http._throttle = queued
        http.transport = SimpleNamespace(request=AsyncMock(side_effect=transport))
        client.http = http
        try:
            await client._submit_order(command)
            await asyncio.gather(*recovery_tasks)
            assert submissions == 2
            client.generate_order_submitted.assert_called_once()
            client.generate_order_denied.assert_not_called()
            posts = [c for c in http.transport.request.call_args_list if c.kwargs["url"].endswith("orders")]
            assert len(posts) == 1
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_cancel_without_venue_id_waits_for_the_inflight_submit_response():
    async def run():
        client = client_for(lambda *args, **kwargs: helpers["canceled_order"]())
        order = submission(client).order
        finished = asyncio.Event()
        client._inflight_submissions[order.client_order_id] = finished
        try:
            task = asyncio.create_task(client._cancel_order(SimpleNamespace(
                client_order_id=order.client_order_id, venue_order_id=None,
                instrument_id=order.instrument_id, strategy_id=order.strategy_id,
            )))
            await asyncio.sleep(0)
            client.http.request.assert_not_awaited()
            finished.set()
            await task
            assert client.http.request.call_args.args[0] == "DELETE"
            assert client.reports[-1].order_status.name == "CANCELED"
        finally:
            finished.set()
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_periodic_query_does_not_expose_filled_totals_without_real_executions():
    async def run():
        client = client_for(lambda *args, **kwargs: partial())
        client.http.pages = AsyncMock(return_value=[partial()])
        client._schedule_execution_recovery = Mock()
        try:
            with pytest.raises(RuntimeError, match="matching real executions"):
                await client.generate_order_status_reports(query(client))
            assert not client.reports
            assert ("BTC_USDT", "41") in client._pending_orders
            client._schedule_execution_recovery.assert_called()
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_bulk_recovery_reuses_complete_history_instead_of_querying_each_order():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())

        async def pages(path, params):
            if path == "/spot/my_trades":
                assert "order_id" not in params
                return [fill(), fill()]
            return [partial()] if params["status"] == "open" else []

        client.http.pages = AsyncMock(side_effect=pages)
        try:
            mass = await client.generate_mass_status(5)
            assert mass is not None
            assert len(mass.order_reports) == 1
            assert sum(len(v) for v in mass.fill_reports.values()) == 1
            client.http.request.assert_not_awaited()  # No N+1 reads or account query.
            assert not client.reconciliation_active
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_recent_fill_of_an_old_order_gets_a_status_even_outside_creation_window():
    async def run():
        client = client_for(lambda *args, **kwargs: partial())
        client.http.pages = AsyncMock(side_effect=lambda path, _: [fill()] if "trades" in path else [])
        try:
            mass = await client.generate_mass_status(5)
            assert mass is not None
            assert len(mass.order_reports) == 1
            assert sum(len(v) for v in mass.fill_reports.values()) == 1
            assert client.http.request.call_args.args[1] == "/spot/orders/41"
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_incomplete_mass_status_is_not_returned_and_reconciliation_flag_is_reset():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        client.http.pages = AsyncMock(side_effect=lambda path, params: (
            [partial()] if path == "/spot/orders" and params["status"] == "open" else []
        ))
        try:
            assert await client.generate_mass_status() is None
            assert not client.reconciliation_active
            client.http.pages = AsyncMock(side_effect=asyncio.CancelledError)
            with pytest.raises(asyncio.CancelledError):
                await client.generate_mass_status()
            assert not client.reconciliation_active
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_recovery_rejects_history_for_a_different_order_even_if_its_quantity_matches():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        client.http.pages = AsyncMock(side_effect=lambda path, params: (
            [partial()] if path == "/spot/orders" and params["status"] == "open"
            else [fill(oid="wrong-order")] if "order_id" in params else []
        ))
        try:
            with pytest.raises(ValueError, match="another order"):
                await client._collect_reconciliation()
            assert not client.reports
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_bulk_fill_cannot_be_erased_to_match_a_lagging_order_status():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        old = partial()
        old["left"] = old["amount"]
        client.http.pages = AsyncMock(side_effect=lambda path, params: (
            [old] if path == "/spot/orders" and params["status"] == "open"
            else [fill()] if path == "/spot/my_trades" and "order_id" not in params else []
        ))
        try:
            assert await client.generate_mass_status() is None
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_late_or_uncorrelated_ack_does_not_confirm_a_new_subscription():
    async def run():
        ws = GateioWebSocket(asyncio.get_running_loop(), TestClock(), "ws://local", AsyncMock(), AsyncMock())
        sent = []

        async def send(raw):
            sent.append(json.loads(raw))

        ws.client = SimpleNamespace(send_text=AsyncMock(side_effect=send), disconnect=AsyncMock())
        first = asyncio.create_task(ws.subscribe("spot.orders", ["BTC_USDT"]))
        await wait_for(lambda: len(sent) == 1)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(ws.subscribe("spot.orders", ["ETH_USDT"]))
        try:
            await wait_for(lambda: len(sent) == 2)
            ack = {"channel": "spot.orders", "event": "subscribe", "result": {"status": "success"}}
            ws._receive(json.dumps(dict(ack, id=sent[0]["id"])))
            ws._receive(json.dumps(ack))
            assert not second.done()
            ws._receive(json.dumps(dict(ack, id=sent[1]["id"])))
            await second
            assert ws.subscriptions == [("spot.orders", ("ETH_USDT",))]
            assert not ws.pending
            assert not ws._pending_request_ids
        finally:
            await ws.disconnect()

    asyncio.run(bounded(run()))


@pytest.mark.parametrize(("field", "value"), [(f, v) for f in ("amount", "price") for v in ("NaN", "Infinity", "-1", "0")])
def test_invalid_execution_numbers_fail_before_native_model_construction(field, value):
    instrument = helpers["parse_instrument"](helpers["PAIR"], 0)
    with pytest.raises(ValueError, match="execution price or amount"):
        fill_report(fill(**{field: value}), instrument, AccountId("GATEIO-master"), None, 0)


def test_adapter_dedup_memory_is_bounded_and_old_cached_trades_still_deduplicate():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        for i in range(100001):
            client._remember_fill(("BTC_USDT", str(i)))
        assert len(client._seen_fills) == 100000
        assert ("BTC_USDT", "0") not in client._seen_fills
        client._cached_order = lambda _: SimpleNamespace(trade_ids=["0"])
        client._send_fill_report = Mock()
        try:
            await client._publish_fills([fill(id="0")])
            client._send_fill_report.assert_not_called()
            assert not client._pending_fills
            assert not hasattr(client, "_client_ids")
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))


def test_exchange_execution_precision_is_not_rounded_to_current_instrument_rules():
    instrument = helpers["parse_instrument"](helpers["PAIR"], 0)
    row = fill(amount="0.00000001", price="99000.12345678")
    report = fill_report(row, instrument, AccountId("GATEIO-master"), None, 0)
    assert report.last_qty.as_decimal() == Decimal("0.00000001")
    assert report.last_px.as_decimal() == Decimal("99000.12345678")
    row = partial()
    row.update(amount="0.00000003", left="0.00000001", price="99000.12345678")
    report = order_report(row, instrument, AccountId("GATEIO-master"), None, 0)
    assert report.quantity.as_decimal() == Decimal("0.00000003")
    assert report.filled_qty.as_decimal() == Decimal("0.00000002")
    assert report.price.as_decimal() == Decimal("99000.12345678")


@pytest.mark.parametrize(("key", "value"), [("account", "margin"), ("account", "unified"), ("currency_pair", "ETH_USDT")])
def test_cash_spot_reports_do_not_accept_other_wallets_or_instruments(key, value):
    instrument = helpers["parse_instrument"](helpers["PAIR"], 0)
    with pytest.raises(ValueError):
        fill_report(fill(**{key: value}), instrument, AccountId("GATEIO-master"), None, 0)


@pytest.mark.parametrize("already_accepted", [False, True])
def test_pending_cancel_keeps_first_ack_but_drops_a_duplicate_acceptance(already_accepted):
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        row = helpers["canceled_order"]()
        row["status"] = "open"
        cached = SimpleNamespace(
            is_closed=False, status=OrderStatus.PENDING_CANCEL, ts_accepted=int(already_accepted),
            filled_qty=Quantity.from_str("0"), quantity=Quantity.from_str(row["amount"]),
            price=Price.from_str(row["price"]), venue_order_id=VenueOrderId(row["id"]),
        )
        client._cached_order = lambda _: cached
        try:
            await client._publish_order(row)
            assert len(client.reports) == (0 if already_accepted else 1)
        finally:
            await cleanup(client)

    asyncio.run(bounded(run()))
