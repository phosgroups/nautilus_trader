"""Gate protocol edge cases: full images, delayed facts and recoverable outages."""

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
from nautilus_trader.adapters.gateio.data import BookGap
from nautilus_trader.adapters.gateio.data import GateBook
from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.adapters.gateio.execution import order_text
from nautilus_trader.adapters.gateio.execution import original_order_id
from nautilus_trader.adapters.gateio.parsing import fill_report
from nautilus_trader.adapters.gateio.parsing import order_report
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.common.component import TestClock
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.component import TestComponentStubs


helpers = runpy.run_path(str(Path(__file__).with_name("test_account_refresh.py")))
client_for, wait_for = helpers["client_for"], helpers["wait_for"]
balances, canceled_order = helpers["balances"], helpers["canceled_order"]
PAIR = helpers["PAIR"]


def full(sequence=102, bids=None):
    return {
        "full": True,
        "l": "100",
        "u": sequence,
        "t": 1793700000000,
        "b": bids if bids is not None else [["97", "1"], ["96", "1"]],
        "a": [["101", "1"], ["102", "1"]],
    }


def base_book():
    book = GateBook(2)
    book.snapshot(
        {"id": 100, "bids": [["99", "1"], ["98", "1"]], "asks": [["101", "1"], ["102", "1"]]}
    )
    return book


def fill(oid="41", **overrides):
    return dict(
        {
            "id": "real-trade-1",
            "order_id": oid,
            "currency_pair": "BTC_USDT",
            "amount": "0.00004",
            "price": "99000",
            "side": "buy",
            "fee": "0.00000001",
            "fee_currency": "BTC",
            "role": "maker",
            "create_time_ms": "1793700000123.456",
        },
        **overrides,
    )


def partial():
    return dict(
        canceled_order(), id="41", status="open", left="0.00006", update_time_ms="1793700000200"
    )


def submission(client):
    client.generate_order_denied = Mock()
    client.generate_order_submitted = Mock()
    return SimpleNamespace(
        order=SimpleNamespace(
            client_order_id=ClientOrderId("wait-for-account"),
            instrument_id=next(iter(client._instrument_provider.get_all())),
            strategy_id="PMM-001",
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            side=OrderSide.BUY,
            is_post_only=True,
            is_quote_quantity=False,
            is_reduce_only=False,
            is_closed=False,
            quantity=Quantity.from_str("0.0001"),
            price=Price.from_str("99000"),
        )
    )


async def cleanup(client):
    client._account_stopping = True
    await client.ws.disconnect()
    await client._cancel_account_tasks()
    await client._cancel_execution_recovery()


def test_full_image_clears_old_prices_in_native_managed_book_even_across_gap():
    instrument = parse_instrument(PAIR, 0)
    local = base_book()
    native = OrderBook(instrument.id, BookType.L2_MBP)
    client = SimpleNamespace(
        _instrument_provider=SimpleNamespace(find=lambda _: instrument),
        _clock=TestClock(),
        _handle_data=native.apply_deltas,
        books={instrument.id: local},
    )
    snapshot, changes = local.update({"U": 101, "u": 101, "b": [], "a": []})
    GateioDataClient._emit(client, instrument.id, snapshot, changes, 101, 1)
    assert native.best_bid_price().as_decimal() == 99
    snapshot, changes = local.update(full(200))  # Full image does not require U or a bridge.
    GateioDataClient._emit(client, instrument.id, snapshot, changes, 200, 2)
    assert snapshot
    assert native.best_bid_price().as_decimal() == 97
    assert len(native.bids()) == 2
    assert local.update(full(199)) is None
    snapshot, changes = local.update(full(200, []))
    GateioDataClient._emit(client, instrument.id, snapshot, changes, 200, 3)
    assert native.best_bid_price() is None


def test_visible_window_restores_a_price_that_temporarily_moves_outside_it():
    book = base_book()
    book.update({"U": 101, "u": 101, "b": [["100", "1"]], "a": []})
    assert list(book.visible()[0]) == [Decimal(100), Decimal(99)]
    _, changes = book.update({"U": 102, "u": 102, "b": [["100", "0"]], "a": []})
    assert list(book.visible()[0]) == [Decimal(99), Decimal(98)]
    assert (OrderSide.BUY, Decimal(98), Decimal(1)) in changes


def test_exhausted_snapshot_boundary_requires_resync_instead_of_a_false_ready_book():
    book = GateBook(2, cache_depth=2)
    book.snapshot(
        {"id": 100, "bids": [["99", "1"], ["98", "1"]], "asks": [["101", "1"], ["102", "1"]]}
    )
    with pytest.raises(BookGap, match="coverage"):
        book.update({"U": 101, "u": 101, "b": [["99", "0"], ["98", "0"]], "a": []})


def test_late_rest_snapshot_cannot_overwrite_newer_ws_full_image():
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()
        book = GateBook(2)
        instrument = parse_instrument(PAIR, 0)

        async def request(*args, **kwargs):
            entered.set()
            await release.wait()
            return {"id": 100, "bids": [["99", "1"]], "asks": [["101", "1"]]}

        client = SimpleNamespace(
            books={instrument.id: book},
            config=SimpleNamespace(max_retries=1),
            http=SimpleNamespace(request=AsyncMock(side_effect=request)),
            _emit=Mock(),
        )
        task = asyncio.create_task(GateioDataClient._resync(client, instrument.id))
        await entered.wait()
        book.update(full(200))
        release.set()
        await task
        assert book.sequence == 200
        assert max(book.bids) == 97
        client._emit.assert_not_called()
        assert client.http.request.call_args.args[2]["limit"] == 100

    asyncio.run(run())


def test_slow_fill_history_never_blocks_unrelated_cancel_reports():
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def request(method, path, *args, **kwargs):
            return partial() if path.startswith("/spot/orders/") else balances()

        async def history(*args, **kwargs):
            entered.set()
            await release.wait()
            return [fill(), fill()]  # Duplicate REST rows must not double the fill.

        client = client_for(request)
        client.http.pages = AsyncMock(side_effect=history)
        client._publish_facts = Mock()
        client.ws.worker = asyncio.create_task(client.ws._consume())
        try:
            client.ws.queue.put_nowait({"channel": "spot.orders", "result": [partial()]})
            await entered.wait()
            client.ws.queue.put_nowait({"channel": "spot.orders", "result": [canceled_order()]})
            await asyncio.wait_for(client.ws.queue.join(), 0.2)
            assert client.reports[-1].order_status.name == "CANCELED"
            assert not client._report_lock.locked()
            release.set()
            await wait_for(lambda: not client._pending_orders)
            reports, rows = client._publish_facts.call_args.args
            assert reports[0].filled_qty.as_decimal() == Decimal("0.00004")
            assert [r["id"] for r in rows] == ["real-trade-1"]
        finally:
            release.set()
            await cleanup(client)

    asyncio.run(run())


def test_delayed_trade_history_retries_and_later_ws_fill_completes_pending_order():
    async def run():
        client = client_for(
            lambda method, path, *args, **kwargs: (
                partial() if path.startswith("/spot/orders/") else balances()
            )
        )
        client.http.pages = AsyncMock(return_value=[])
        client._publish_facts = Mock()
        client.ws.client = SimpleNamespace(is_active=lambda: True, disconnect=AsyncMock())
        client.ws.ready = True
        try:
            await client._message({"channel": "spot.orders", "result": [partial()]})
            await wait_for(lambda: client.http.pages.await_count >= 2)
            assert client.ws.is_ready()
            assert client._pending_orders
            client._publish_facts.assert_not_called()
            await client._message({"channel": "spot.usertrades", "result": [fill(), fill()]})
            assert not client._pending_orders
            assert client._publish_facts.call_count == 1
        finally:
            await cleanup(client)

    asyncio.run(run())


def test_failed_reconnect_retries_without_another_transport_disconnect():
    async def run():
        reconcile = AsyncMock(
            side_effect=[TimeoutError(), ValueError("temporarily incomplete"), None]
        )
        ws = GateioWebSocket(
            asyncio.get_running_loop(), TestClock(), "ws://unused", AsyncMock(), reconcile
        )
        ws.client = SimpleNamespace(is_active=lambda: True, disconnect=AsyncMock())
        ws._subscription = AsyncMock()
        ws.subscriptions = [("spot.orders", ("BTC_USDT",))]
        ws._reconnected()
        task = ws.recovery
        await asyncio.wait_for(task, 2)
        assert ws.is_ready()
        assert reconcile.await_count == 3
        assert ws._subscription.await_count == 3
        assert ws.recovery is None
        await ws.disconnect()

    asyncio.run(run())


def test_queued_old_connection_messages_are_fenced_out_during_recovery():
    async def run():
        received = []
        entered, release = asyncio.Event(), asyncio.Event()

        async def reconcile():
            entered.set()
            await release.wait()

        ws = GateioWebSocket(
            asyncio.get_running_loop(),
            TestClock(),
            "ws://unused",
            AsyncMock(side_effect=received.append),
            reconcile,
        )
        ws.client = SimpleNamespace(is_active=lambda: True, disconnect=AsyncMock())
        ws._receive(
            json.dumps({"channel": "spot.orders", "event": "update", "result": ["old"]}).encode()
        )
        ws._reconnected()
        ws.worker = asyncio.create_task(ws._consume())
        await entered.wait()
        ws._receive(
            json.dumps({"channel": "spot.orders", "event": "update", "result": ["new"]}).encode()
        )
        assert not received
        release.set()
        await asyncio.wait_for(ws.queue.join(), 1)
        assert [r["result"] for r in received] == [["new"]]
        await ws.disconnect()

    asyncio.run(run())


def test_disconnect_cancels_retrying_recovery():
    async def run():
        recover = AsyncMock(side_effect=TimeoutError())
        ws = GateioWebSocket(
            asyncio.get_running_loop(), TestClock(), "ws://unused", AsyncMock(), recover
        )
        ws.client = SimpleNamespace(is_active=lambda: True, disconnect=AsyncMock())
        ws._reconnected()
        task = ws.recovery
        await wait_for(lambda: recover.await_count == 1)
        await ws.disconnect()
        assert task.cancelled()
        assert ws.recovery is None
        assert not ws.is_ready()

    asyncio.run(run())


def test_stale_balance_defers_submit_until_fresh_snapshot_then_submits_once():
    async def run():
        release, entered = asyncio.Event(), asyncio.Event()

        async def request(method, path, *args, **kwargs):
            if path == "/spot/accounts":
                entered.set()
                await release.wait()
                return balances()
            return dict(canceled_order(), status="open")

        client = client_for(request, account_stale_after_ms=100, submission_ready_timeout_secs=0.5)
        client.ws.client = SimpleNamespace(is_active=lambda: True, disconnect=AsyncMock())
        client.ws.ready = True
        try:
            task = asyncio.create_task(client._submit_order(submission(client)))
            await entered.wait()
            assert not any(c.args[0] == "POST" for c in client.http.request.call_args_list)
            release.set()
            await task
            assert sum(c.args[0] == "POST" for c in client.http.request.call_args_list) == 1
            client.generate_order_denied.assert_not_called()
        finally:
            release.set()
            await cleanup(client)

    asyncio.run(run())


def test_expired_balance_refuses_new_submit_but_still_allows_cancellation():
    async def run():
        async def request(method, path, *args, **kwargs):
            if path == "/spot/accounts":
                raise TimeoutError
            assert method == "DELETE"
            return canceled_order()

        client = client_for(request, account_stale_after_ms=100, submission_ready_timeout_secs=0.08)
        client.ws.client = SimpleNamespace(is_active=lambda: True, disconnect=AsyncMock())
        client.ws.ready = True
        try:
            command = submission(client)
            await client._submit_order(command)
            client.generate_order_denied.assert_called_once()
            assert not any(c.args[0] == "POST" for c in client.http.request.call_args_list)
            await client._cancel_order(
                SimpleNamespace(
                    client_order_id=command.order.client_order_id,
                    venue_order_id="42",
                    instrument_id=command.order.instrument_id,
                )
            )
            assert client.reports[-1].order_status.name == "CANCELED"
        finally:
            await cleanup(client)

    asyncio.run(run())


def test_quiet_account_is_refreshed_without_ws_notifications():
    async def run():
        client = client_for(lambda *args, **kwargs: balances(), account_stale_after_ms=100)
        try:
            client._request_account_refresh()
            await wait_for(lambda: client.http.request.await_count >= 3)
            assert client._account_is_fresh()
        finally:
            await cleanup(client)

    asyncio.run(run())


@pytest.mark.parametrize(
    "cid",
    ["88be7664-0061-4f36-bf15-e00000000000", "88be7664-0061-4f36-bf15-e00000000001", "short-id-12"],
)
def test_client_order_id_round_trip_does_not_need_process_local_state(cid):
    value = ClientOrderId(cid)
    assert original_order_id(order_text(value)) == value
    assert len(order_text(value).encode()) <= 30


@pytest.mark.parametrize(
    "cid", ["x" * 26, "x" * 28 + "different-tail", "非ASCII", "contains/slash"]
)
def test_unrepresentable_order_ids_are_rejected_before_submission(cid):
    with pytest.raises(ValueError):
        order_text(cid)
    assert original_order_id("t-ntS" + cid) is None


def test_trade_rules_and_price_protection_terminal_state_are_preserved():
    instrument = parse_instrument(dict(PAIR, max_base_amount="0.01", max_quote_amount="100"), 0)
    assert instrument.max_quantity.as_decimal() == Decimal("0.01")
    assert instrument.max_notional.as_decimal() == 100
    report = order_report(
        dict(canceled_order(), status="closed", finish_as="price_protect_cancelled"),
        instrument,
        AccountId("GATEIO-master"),
        None,
        0,
    )
    assert report.order_status.name == "CANCELED"


@pytest.mark.parametrize(
    ("fee", "rebate", "expected"),
    [("0.75", "0", "0.75"), ("0", "0.1", "-0.1"), ("0.75", "0.1", "0.65")],
)
def test_fee_debits_and_same_currency_rebates_have_correct_commission_sign(fee, rebate, expected):
    report = fill_report(
        fill(fee=fee, fee_currency="USDT", rebated_fee=rebate, rebated_fee_currency="USDT"),
        parse_instrument(PAIR, 0),
        AccountId("GATEIO-master"),
        None,
        0,
    )
    assert report.commission.as_decimal() == Decimal(expected)


def test_partial_trade_history_is_not_returned_as_a_status_nt_could_use_to_infer_fills():
    async def run():
        client = client_for(lambda *args, **kwargs: partial())
        client._account_paused = True  # Inspect the deferred state before background recovery.
        report = await client.generate_order_status_report(
            SimpleNamespace(
                venue_order_id="41",
                client_order_id=ClientOrderId("delayed"),
                instrument_id=next(iter(client._instrument_provider.get_all())),
            )
        )
        assert report is None
        assert ("BTC_USDT", "41") in client._pending_orders
        assert not client.reports
        await cleanup(client)

    asyncio.run(run())


def test_price_only_update_with_same_order_status_is_not_dropped():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        client._account_paused = True
        first = dict(canceled_order(), status="open", update_time_ms="1793700000000")
        second = dict(first, price="99001", update_time_ms="1793700000123.456")
        await client._message({"channel": "spot.orders", "result": [first, second]})
        assert [r.price.as_decimal() for r in client.reports] == [Decimal(99000), Decimal(99001)]
        assert client.reports[1].ts_last == 1793700000123456000
        await cleanup(client)

    asyncio.run(run())


def test_duplicate_pagination_is_detected_and_rows_are_deduplicated():
    async def run():
        from nautilus_trader.adapters.gateio.http import GateioHttpClient

        client = GateioHttpClient(TestClock(), "http://unused", GateioDataClientConfig())
        page = [{"id": str(n)} for n in range(100)]
        client.request = AsyncMock(side_effect=[page, page])
        with pytest.raises(RuntimeError, match="without progress"):
            await client.pages("/spot/orders", {})
        client.request = AsyncMock(side_effect=[page, [page[-1], {"id": "new"}]])
        rows = await client.pages("/spot/orders", {})
        assert len(rows) == 101

    asyncio.run(run())


def test_deeper_rest_depth_is_not_misrepresented_as_a_supported_ws_depth():
    async def run():
        client = client_for(lambda *args, **kwargs: [])
        with pytest.raises(ValueError, match="1-100"):
            GateioDataClient(
                asyncio.get_running_loop(),
                client.http,
                client._msgbus,
                client._cache,
                client._clock,
                client._instrument_provider,
                GateioDataClientConfig(book_depth=1000),
            )
        await cleanup(client)

    asyncio.run(run())


def test_private_symbols_are_split_into_acknowledged_batches_of_at_most_50():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        for index in range(50):
            client._instrument_provider.add(
                parse_instrument(dict(PAIR, id=f"ASSET{index}_USDT", base=f"ASSET{index}"), 0)
            )
        client._await_account_registered = AsyncMock()
        client._reconcile = AsyncMock()
        client.ws.connect = AsyncMock()
        client.ws.subscribe = AsyncMock()
        await client._connect()
        calls = client.ws.subscribe.call_args_list
        assert [len(call.args[1]) for call in calls if call.args[0] == "spot.orders"] == [50, 1]
        assert [len(call.args[1]) for call in calls if call.args[0] == "spot.usertrades"] == [50, 1]
        assert calls[-1].args == ("spot.balances",)
        await cleanup(client)

    asyncio.run(run())


def test_malformed_subscription_confirmation_cannot_mark_channel_ready():
    async def run():
        loop = asyncio.get_running_loop()
        ws = GateioWebSocket(loop, TestClock(), "ws://unused", AsyncMock(), AsyncMock())
        future = loop.create_future()
        ws.pending[("spot.orders", "subscribe")] = future
        ws._receive(b'{"channel":"spot.orders","event":"subscribe","result":[]}')
        with pytest.raises(RuntimeError, match="rejected"):
            await future
        assert not ws.is_ready()
        await ws.disconnect()

    asyncio.run(run())


def test_reconnect_cancels_slow_execution_recovery_and_fences_late_response():
    async def run():
        entered = asyncio.Event()

        async def request(method, path, *args, **kwargs):
            if path.startswith("/spot/orders/"):
                entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    return partial()
            return balances()

        client = client_for(request)
        client._publish_facts = Mock()
        try:
            await client._message({"channel": "spot.orders", "result": [partial()]})
            await entered.wait()
            await client._reconcile()
            assert not client._pending_orders
            assert not client._pending_fills
            assert client._execution_recovery_task is None
            assert client._account_is_fresh()
        finally:
            await cleanup(client)

    asyncio.run(run())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("order_type", OrderType.MARKET),
        ("time_in_force", TimeInForce.IOC),
        ("is_quote_quantity", True),
        ("is_post_only", False),
        ("is_reduce_only", True),
    ],
)
def test_unsupported_order_semantics_are_denied_locally_without_a_post(field, value):
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        command = submission(client)
        setattr(command.order, field, value)
        await client._submit_order(command)
        client.generate_order_denied.assert_called_once()
        assert not client.http.request.called
        await cleanup(client)

    asyncio.run(run())


def test_slow_reconnect_account_request_does_not_hold_the_order_report_lock():
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
        try:
            task = asyncio.create_task(client._reconcile())
            await entered.wait()
            assert not client._report_lock.locked()
            await asyncio.wait_for(
                client._cancel_order(
                    SimpleNamespace(
                        client_order_id=ClientOrderId("cancel-during-reconnect"),
                        venue_order_id="42",
                        instrument_id=next(iter(client._instrument_provider.get_all())),
                    )
                ),
                0.2,
            )
            assert client.reports[-1].order_status.name == "CANCELED"
            release.set()
            await task
        finally:
            release.set()
            await cleanup(client)

    asyncio.run(run())


def test_recovery_does_not_replay_an_open_status_after_a_concurrent_cancel():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        client._cached_order = Mock(return_value=SimpleNamespace(is_closed=True))
        client._send_mass_status_report = Mock()
        client._send_fill_report = Mock()
        report = order_report(
            dict(canceled_order(), id="41", status="open"),
            parse_instrument(PAIR, 0),
            AccountId("GATEIO-master"),
            None,
            0,
        )
        client._publish_facts([report], [fill()])
        mass = client._send_mass_status_report.call_args.args[0]
        assert not mass.order_reports
        client._send_fill_report.assert_called_once()
        assert client._send_fill_report.call_args.args[0].trade_id.value == "real-trade-1"
        await cleanup(client)

    asyncio.run(run())


def test_cancel_while_waiting_for_fresh_balance_prevents_a_late_post():
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def request(*args, **kwargs):
            entered.set()
            await release.wait()
            return balances()

        client = client_for(request, submission_ready_timeout_secs=0.5)
        client.ws.client = SimpleNamespace(is_active=lambda: True, disconnect=AsyncMock())
        client.ws.ready = True
        client.generate_order_canceled = Mock()
        command = submission(client)
        task = asyncio.create_task(client._submit_order(command))
        try:
            await entered.wait()
            await client._cancel_order(
                SimpleNamespace(
                    client_order_id=command.order.client_order_id,
                    instrument_id=command.order.instrument_id,
                    strategy_id=command.order.strategy_id,
                    venue_order_id=None,
                )
            )
            release.set()
            await task
            client.generate_order_canceled.assert_called_once()
            client.generate_order_denied.assert_not_called()
            assert not any(c.args[0] == "POST" for c in client.http.request.call_args_list)
            assert not client._waiting_submissions
        finally:
            release.set()
            await cleanup(client)

    asyncio.run(run())


def test_unsent_local_cancel_is_a_valid_native_order_transition_without_a_venue_id():
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def request(*args, **kwargs):
            entered.set()
            await release.wait()
            return balances()

        client = client_for(request, submission_ready_timeout_secs=0.5)
        client.ws.client = SimpleNamespace(is_active=lambda: True, disconnect=AsyncMock())
        client.ws.ready = True
        order = TestComponentStubs.order_factory().limit(
            instrument_id=next(iter(client._instrument_provider.get_all())),
            order_side=OrderSide.BUY,
            quantity=Quantity.from_str("0.0001"),
            price=Price.from_str("99000"),
            post_only=True,
            client_order_id=ClientOrderId("88be7664-0061-4f36-bf15-e00000000000"),
        )
        client._cache.add_order(order)
        client._msgbus.register("ExecEngine.process", order.apply)
        task = asyncio.create_task(client._submit_order(SimpleNamespace(order=order)))
        try:
            await entered.wait()
            await client._cancel_order(
                SimpleNamespace(
                    client_order_id=order.client_order_id,
                    instrument_id=order.instrument_id,
                    strategy_id=order.strategy_id,
                    venue_order_id=None,
                )
            )
            release.set()
            await task
            assert order.status.name == "CANCELED"
            assert order.venue_order_id is None
            assert not any(c.args[0] == "POST" for c in client.http.request.call_args_list)
        finally:
            release.set()
            await cleanup(client)

    asyncio.run(run())


def test_fill_batch_orders_business_seconds_and_milliseconds_consistently():
    async def run():
        client = client_for(lambda *args, **kwargs: balances())
        client._account_paused = True
        client._flush_order = Mock()
        later = fill("1", id="later", create_time_ms="3000")
        earlier = fill("2", id="earlier", create_time="1")
        earlier.pop("create_time_ms")
        await client._publish_fills([later, earlier])
        assert [c.args[0] for c in client._flush_order.call_args_list] == [
            ("BTC_USDT", "2"),
            ("BTC_USDT", "1"),
        ]
        await cleanup(client)

    asyncio.run(run())
