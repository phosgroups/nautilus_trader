"""Cross-adapter audit cases for scope, precision and subscription lifecycle."""

import asyncio
import runpy
from decimal import Decimal
from decimal import localcontext
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.adapters.gateio.execution import GateioExecutionClient
from nautilus_trader.adapters.gateio.execution import order_text
from nautilus_trader.adapters.gateio.parsing import fill_report
from nautilus_trader.adapters.gateio.parsing import public_quote_tick
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.common.component import TestClock
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import VenueOrderId


helpers = runpy.run_path(str(Path(__file__).with_name("test_recovery_edges.py")))
client_for, cleanup = helpers["client_for"], helpers["cleanup"]


@pytest.mark.parametrize("field", ["id", "order_id"])
@pytest.mark.parametrize("value", [None, "", True, [], {}])
def test_null_or_malformed_execution_ids_cannot_become_native_trade_or_order_ids(field, value):
    row = helpers["fill"](**{field: value})
    with pytest.raises(ValueError, match="identifiers"):
        fill_report(row, parse_instrument(helpers["PAIR"], 0), AccountId("GATEIO-master"), None, 0)


@pytest.mark.parametrize("conflict", ["account", "instrument", "venue_order", "venue"])
def test_decoded_client_id_cannot_claim_another_cached_account_instrument_or_order(conflict):
    cid = ClientOrderId("owned")
    cached = SimpleNamespace(
        instrument_id=InstrumentId.from_str("LIGHT_USDT.GATEIO" if conflict == "instrument" else "BTC_USDT.GATEIO"),
        account_id=AccountId("OTHER-master" if conflict == "account" else "GATEIO-master"),
        venue_order_id=VenueOrderId("other" if conflict == "venue_order" else "41"),
    )
    if conflict == "venue":
        cached.instrument_id = InstrumentId.from_str("BTC_USDT.BINANCE")
        cached.account_id = None
    client = SimpleNamespace(account_id=AccountId("GATEIO-master"),
                             _cache=SimpleNamespace(order=lambda _: cached, client_order_id=lambda _: None))
    client._checked_identity = lambda *args: GateioExecutionClient._checked_identity(client, *args)
    with pytest.raises(ValueError, match="cached"):
        GateioExecutionClient._resolve(client, {"text": order_text(cid), "id": "41", "currency_pair": "BTC_USDT"})


def data_client():
    instrument = parse_instrument(helpers["PAIR"], 0)
    client = SimpleNamespace(
        books={}, resyncs={}, _subscription_locks={},
        config=SimpleNamespace(book_depth=10, max_book_buffer=100),
        _instrument_provider=SimpleNamespace(find=lambda _: instrument),
        _clock=TestClock(), _emit=Mock(), _schedule_resync=Mock(), ws=AsyncMock(),
    )
    client._reset = partial(GateioDataClient._reset, client)
    client._subscribe_book = partial(GateioDataClient._subscribe_book, client)
    client._unsubscribe_book = partial(GateioDataClient._unsubscribe_book, client)
    client._subscription_lock = lambda channel, iid: client._subscription_locks.setdefault(
        (channel, iid), asyncio.Lock(),
    )
    return client, SimpleNamespace(instrument_id=instrument.id, book_type=BookType.L2_MBP)


@pytest.mark.parametrize("failure", [RuntimeError("rejected"), asyncio.CancelledError()])
def test_failed_book_subscription_rolls_back_target_without_starting_rest_recovery(failure):
    async def run():
        client, command = data_client()
        client.ws.subscribe.side_effect = failure
        with pytest.raises(type(failure)):
            await GateioDataClient._subscribe_order_book_deltas(client, command)
        assert not client.books
        client._schedule_resync.assert_not_called()
    asyncio.run(run())


def test_concurrent_duplicate_book_subscription_creates_one_subscription_and_snapshot():
    async def run():
        client, command = data_client()
        await asyncio.gather(*(
            GateioDataClient._subscribe_order_book_deltas(client, command) for _ in range(3)
        ))
        client.ws.subscribe.assert_awaited_once()
        client._schedule_resync.assert_called_once()
        await GateioDataClient._unsubscribe_order_book_deltas(client, command)
        await GateioDataClient._unsubscribe_order_book_deltas(client, command)
        client.ws.unsubscribe.assert_awaited_once()
    asyncio.run(run())


def test_book_unsubscribe_waits_for_its_pending_subscribe_and_leaves_no_target():
    async def run():
        client, command = data_client()
        started, release = asyncio.Event(), asyncio.Event()
        async def subscribe(*_):
            started.set()
            await release.wait()
        client.ws.subscribe.side_effect = subscribe
        first = asyncio.create_task(GateioDataClient._subscribe_order_book_deltas(client, command))
        await started.wait()
        second = asyncio.create_task(GateioDataClient._unsubscribe_order_book_deltas(client, command))
        await asyncio.sleep(0)
        client.ws.unsubscribe.assert_not_awaited()
        release.set()
        await asyncio.gather(first, second)
        assert not client.books
    asyncio.run(run())


def test_reconnect_invalidates_both_books_before_a_failing_snapshot_request():
    async def run():
        instruments = [InstrumentId.from_str(p + ".GATEIO") for p in ["BTC_USDT", "LIGHT_USDT"]]
        client = SimpleNamespace(books=dict.fromkeys(instruments, object()), resyncs={},
                                 _reset=Mock(), _resync=AsyncMock(side_effect=ConnectionError))
        with pytest.raises(ConnectionError):
            await GateioDataClient._reconnect(client)
        assert [c.args[0] for c in client._reset.call_args_list] == instruments
    asyncio.run(run())


def test_queue_failure_immediately_invalidates_books_and_drops_a_pre_failure_held_message():
    async def run():
        invalidated, handler = Mock(), AsyncMock()
        ws = GateioWebSocket(asyncio.get_running_loop(), TestClock(), "ws://local",
                             handler, AsyncMock(), invalidate=invalidated)
        ws._resume.clear()
        ws.worker = asyncio.create_task(ws._consume())
        ws.queue.put_nowait((ws._message_epoch, {"old": True}))
        await asyncio.sleep(0)
        ws._failed(ValueError("invalid frame"))
        invalidated.assert_called_once()
        ws._resume.set()
        await ws.queue.join()
        handler.assert_not_awaited()
        await ws.disconnect()
    asyncio.run(run())


def test_subscription_timeout_cleans_pending_state_and_later_acknowledged_retry_restores_readiness():
    async def run():
        ws = GateioWebSocket(asyncio.get_running_loop(), TestClock(), "ws://local", AsyncMock(), AsyncMock())
        ws.ready = True
        ws.client = SimpleNamespace(send_text=AsyncMock(side_effect=TimeoutError))
        with pytest.raises(TimeoutError):
            await ws.subscribe("spot.book_ticker", ["LIGHT_USDT"])
        assert not ws.ready
        assert not ws.pending
        assert not ws.subscriptions
        ws._send_subscription = AsyncMock()
        await ws.subscribe("spot.book_ticker", ["LIGHT_USDT"])
        assert ws.ready
    asyncio.run(run())


def test_explicit_connection_reopen_restores_both_confirmed_topics_before_becoming_ready():
    async def run():
        ws = GateioWebSocket(asyncio.get_running_loop(), TestClock(), "ws://local", AsyncMock(), AsyncMock())
        ws.subscriptions = [("spot.book_ticker", ("BTC_USDT",)), ("spot.book_ticker", ("LIGHT_USDT",))]
        ws._send_subscription = AsyncMock()
        transport = SimpleNamespace(disconnect=AsyncMock())
        with patch("nautilus_trader.adapters.gateio.websocket.WebSocketClient.connect",
                   AsyncMock(return_value=transport)):
            await ws.connect()
        assert ws._send_subscription.await_count == 2
        ws.reconnect.assert_awaited_once()
        assert ws.ready
        await ws.disconnect()
    asyncio.run(run())


def test_disconnect_while_transport_is_opening_cannot_resurrect_a_socket_or_consumer():
    async def run():
        ws = GateioWebSocket(asyncio.get_running_loop(), TestClock(), "ws://local", AsyncMock(), AsyncMock())
        entered, release = asyncio.Event(), asyncio.Event()
        transport = SimpleNamespace(disconnect=AsyncMock())
        async def opening(**kwargs):
            entered.set()
            await release.wait()
            return transport
        with patch("nautilus_trader.adapters.gateio.websocket.WebSocketClient.connect", opening):
            task = asyncio.create_task(ws.connect())
            await entered.wait()
            await ws.disconnect()
            release.set()
            with pytest.raises(ConnectionError):
                await task
        transport.disconnect.assert_awaited_once()
        assert ws.client is None
        assert ws.worker is None
        assert not ws.ready
    asyncio.run(run())


def test_concurrent_connect_cannot_overwrite_the_first_socket_or_consumer():
    async def run():
        ws = GateioWebSocket(asyncio.get_running_loop(), TestClock(), "ws://local", AsyncMock(), AsyncMock())
        entered, release = asyncio.Event(), asyncio.Event()
        transport = SimpleNamespace(disconnect=AsyncMock())
        async def opening(**kwargs):
            entered.set()
            await release.wait()
            return transport
        with patch("nautilus_trader.adapters.gateio.websocket.WebSocketClient.connect", opening):
            first = asyncio.create_task(ws.connect())
            await entered.wait()
            with pytest.raises(RuntimeError):
                await ws.connect()
            release.set()
            await first
        assert ws.client is transport
        assert ws.ready
        await ws.disconnect()
        transport.disconnect.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize("instrument", ["BTC_USDT.BINANCE", "ETH_USDT.GATEIO"])
def test_unconfigured_submission_cannot_reach_any_gate_order_endpoint(instrument):
    async def run():
        client = client_for(lambda *args, **kwargs: helpers["balances"]())
        command = helpers["submission"](client)
        command.order.instrument_id = InstrumentId.from_str(instrument)
        try:
            await client._submit_order(command)
            client.generate_order_denied.assert_called_once()
            assert not any(c.args[0] == "POST" for c in client.http.request.call_args_list)
        finally:
            await cleanup(client)
    asyncio.run(run())


@pytest.mark.parametrize(("symbol", "pp", "qp", "bid", "ask"), [
    ("BTC_USDT", 2, 6, "99999.901", "100000.012"),
    ("LIGHT_USDT", 4, 1, "0.123456789", "0.123456799"),
    ("LIGHT_USDT", 0, 0, "100", "101"),
    ("LIGHT_USDT", 16, 16, "0.1234567890123456", "0.1234567890123457"),
])
def test_quote_values_do_not_depend_on_process_decimal_context(symbol, pp, qp, bid, ask):
    instrument = parse_instrument({"id": symbol, "base": symbol.split("_")[0],
                                   "quote": "USDT", "precision": pp, "amount_precision": qp}, 0)
    with localcontext() as context:
        context.prec = 6
        tick = public_quote_tick({"s": symbol, "b": bid, "a": ask,
                                  "B": "123.456789", "A": "123.456790", "t": 1}, instrument, 0)
    assert tick.bid_price.as_decimal() == Decimal(bid)
    assert tick.ask_price.as_decimal() == Decimal(ask)
    assert tick.bid_size.as_decimal() == Decimal("123.456789")


@pytest.mark.parametrize("wrong", ["pair", "order"])
def test_single_order_query_rejects_a_response_for_another_pair_or_order(wrong):
    async def run():
        row = helpers["partial"]()
        row.update(currency_pair="LIGHT_USDT" if wrong == "pair" else "BTC_USDT",
                   id="other" if wrong == "order" else "41")
        client = client_for(lambda *args, **kwargs: row)
        client._instrument_provider.add(parse_instrument(
            dict(helpers["PAIR"], id="LIGHT_USDT", base="LIGHT"), 0,
        ))
        try:
            with pytest.raises(ValueError, match="requested"):
                await client.generate_order_status_report(SimpleNamespace(
                    instrument_id=InstrumentId.from_str("BTC_USDT.GATEIO"),
                    venue_order_id=VenueOrderId("41"), client_order_id=ClientOrderId("query"),
                ))
            assert not client.reports
        finally:
            await cleanup(client)
    asyncio.run(run())


def test_order_specific_fill_query_rejects_other_order_history():
    async def run():
        client = client_for(lambda *args, **kwargs: helpers["balances"]())
        client.http.pages.return_value = [helpers["fill"](oid="another-order")]
        try:
            with pytest.raises(ValueError, match="another order"):
                await client.generate_fill_reports(SimpleNamespace(
                    instrument_id=InstrumentId.from_str("BTC_USDT.GATEIO"),
                    venue_order_id=VenueOrderId("41"), start=None, end=None,
                ))
        finally:
            await cleanup(client)
    asyncio.run(run())


def test_cancel_all_excludes_other_accounts_and_includes_owned_submitted_orders():
    async def run():
        client = SimpleNamespace(account_id=AccountId("GATEIO-master"),
                                 _waiting_submissions={}, _clock=TestClock(), _cancel_order=AsyncMock())
        strategy = StrategyId("MAKER-001")
        instrument = InstrumentId.from_str("BTC_USDT.GATEIO")
        def order(cid, account):
            return SimpleNamespace(client_order_id=ClientOrderId(cid), instrument_id=instrument,
                                   account_id=account, strategy_id=strategy, side=OrderSide.BUY,
                                   venue_order_id=VenueOrderId(cid))
        ours = order("ours", client.account_id)
        other = order("other", AccountId("OTHER-master"))
        submitted = order("submitted", client.account_id)
        client._cache = SimpleNamespace(
            orders_open=lambda **kwargs: [ours, other],
            orders_inflight=lambda **kwargs: [submitted],
        )
        await GateioExecutionClient._cancel_all_orders(client, SimpleNamespace(
                instrument_id=instrument, strategy_id=strategy, order_side=OrderSide.NO_ORDER_SIDE,
                trader_id=TraderId("TRADER-001"),
        ))
        assert {call.args[0].client_order_id for call in client._cancel_order.call_args_list} == {
            ours.client_order_id, submitted.client_order_id,
        }
    asyncio.run(run())
