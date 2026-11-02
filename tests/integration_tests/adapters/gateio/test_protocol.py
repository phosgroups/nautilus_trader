"""Gate protocol contracts against the installed NT v1.230 native models."""

import asyncio
import hashlib
import hmac
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from nautilus_trader.adapters.gateio.config import GateioDataClientConfig
from nautilus_trader.adapters.gateio.config import GateioEnvironment
from nautilus_trader.adapters.gateio.config import endpoints
from nautilus_trader.adapters.gateio.data import BookGap
from nautilus_trader.adapters.gateio.data import GateBook
from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.adapters.gateio.execution import GateioExecutionClient
from nautilus_trader.adapters.gateio.execution import order_text
from nautilus_trader.adapters.gateio.execution import submit_payload
from nautilus_trader.adapters.gateio.http import GateioHttpClient
from nautilus_trader.adapters.gateio.http import GateioHttpError
from nautilus_trader.adapters.gateio.http import sign_rest
from nautilus_trader.adapters.gateio.parsing import fill_report
from nautilus_trader.adapters.gateio.parsing import order_report
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.adapters.gateio.websocket import subscription
from nautilus_trader.common.component import TestClock
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


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


def order(**overrides):
    return dict(
        id="123",
        currency_pair="BTC_USDT",
        amount="0.0001",
        left="0.0001",
        price="100000",
        side="buy",
        status="open",
        time_in_force="poc",
        create_time_ms="1793620800123.456",
        **overrides,
    )


def trade(**overrides):
    row = {
        "id": "1",
        "order_id": "123",
        "currency_pair": "BTC_USDT",
        "amount": "0.00004",
        "price": "100000",
        "side": "buy",
        "fee": "0.000000012",
        "fee_currency": "BTC",
        "role": "maker",
        "create_time_ms": "1793620800123.456",
    }
    row.update(overrides)
    return row


def test_signatures_use_exact_wire_query_path_body():
    body = b'{"currency_pair":"BTC_USDT","amount":"0.001"}'
    query = "currency_pair=BTC_USDT&limit=100"
    expected = hmac.new(
        b"test-secret",
        (
            "POST\n/api/v4/spot/orders\n"
            + query
            + "\n"
            + hashlib.sha512(body).hexdigest()
            + "\n1793620800"
        ).encode(),
        hashlib.sha512,
    ).hexdigest()
    assert (
        sign_rest("test-secret", "POST", "/api/v4/spot/orders", query, body, 1793620800) == expected
    )
    assert sign_rest("test-secret", "POST", "/api/v4/spot/orders", "", body, 1793620800) != expected
    message = subscription("spot.orders", ["BTC_USDT"], 1793620800, "key", "secret")
    assert (
        message["auth"]["SIGN"]
        == hmac.new(
            b"secret",
            b"channel=spot.orders&event=subscribe&time=1793620800",
            hashlib.sha512,
        ).hexdigest()
    )
    assert "auth" not in subscription("spot.order_book_update", ["BTC_USDT", "100ms"], 0)


def test_endpoints_are_explicit_and_unknown_environment_fails():
    assert "testnet" in endpoints(GateioEnvironment.TESTNET)[0]
    assert "testnet" not in endpoints(GateioEnvironment.LIVE)[0]
    assert endpoints(GateioEnvironment.LIVE, "http://local/api/v4", "ws://local/ws") == (
        "http://local/api/v4",
        "ws://local/ws",
    )
    with pytest.raises(ValueError):
        endpoints("invalid")


def test_instrument_and_timestamp_native_conversion():
    instrument = parse_instrument(PAIR, 100)
    assert str(instrument.id) == "BTC_USDT.GATEIO"
    assert instrument.price_increment.as_decimal() == Decimal("0.01")
    report = order_report(order(), instrument, AccountId("GATEIO-master"), None, 200)
    assert report.order_status == OrderStatus.ACCEPTED
    assert report.post_only
    assert report.ts_accepted == 1793620800123456000
    partial = order()
    partial["left"] = "0.00006"
    assert (
        order_report(partial, instrument, AccountId("GATEIO-master"), None, 200).order_status
        == OrderStatus.PARTIALLY_FILLED
    )
    canceled = dict(partial, status="cancelled")
    assert order_report(
        canceled, instrument, AccountId("GATEIO-master"), None, 200
    ).filled_qty.as_decimal() == Decimal("0.00004")
    fill = fill_report(trade(), instrument, AccountId("GATEIO-master"), None, 200)
    assert str(fill.commission.currency) == "BTC"
    assert fill.last_qty.as_decimal() == Decimal("0.00004")
    with pytest.raises(ValueError, match="GT/points"):
        fill_report(trade(gt_fee="0.01"), instrument, AccountId("GATEIO-master"), None, 200)


def test_snapshot_waits_for_bridge_and_updates_are_absolute():
    book = GateBook(2)
    assert book.update({"U": 101, "u": 102, "b": [["99", "3"]], "a": []}) is None
    book.snapshot(
        {"id": 100, "bids": [["99", "1"], ["98", "2"]], "asks": [["101", "1"], ["102", "2"]]}
    )
    pending = book.buffer.pop()
    snapshot, changes = book.update(pending)
    assert snapshot
    assert book.ready
    assert book.sequence == 102
    assert book.bids[Decimal(99)] == Decimal(3)
    assert len(changes) == 4
    assert book.update(pending) is None
    snapshot, changes = book.update({"U": 103, "u": 103, "b": [["99", "0"], ["97", "4"]], "a": []})
    assert not snapshot
    assert Decimal(99) not in book.bids
    assert (OrderSide.BUY, Decimal(99), Decimal(0)) in changes
    with pytest.raises(BookGap):
        book.update({"U": 105, "u": 105, "b": [], "a": []})


def test_gap_buffer_overflow_and_crossed_book_fail_closed():
    book = GateBook(1, 1)
    row = {"U": 1, "u": 1, "b": [], "a": []}
    book.update(row)
    with pytest.raises(BookGap):
        book.update(row)
    with pytest.raises(BookGap):
        GateBook(1).snapshot({"id": 1, "bids": [["102", "1"]], "asks": [["101", "1"]]})
    book = GateBook(2)
    book.snapshot({"id": 10, "bids": [["99", "1"]], "asks": [["101", "1"]]})
    with pytest.raises(BookGap):
        book.update({"U": 12, "u": 12, "b": [], "a": []})


def test_visible_window_does_not_accumulate_outer_prices():
    book = GateBook(2)
    book.snapshot(
        {"id": 10, "bids": [["99", "1"], ["98", "1"]], "asks": [["101", "1"], ["102", "1"]]}
    )
    _, changes = book.update({"U": 11, "u": 11, "b": [["99.5", "1"]], "a": []})
    assert len(book.bids) == 2
    assert Decimal(98) not in book.bids
    assert all(q >= 0 for _, _, q in changes)


def test_native_uuid_order_ids_are_safe_and_stable():
    cid = ClientOrderId("88be7664-0061-4f36-bf15-e00000000000")
    text = order_text(cid)
    assert len(text) == 30
    assert text.startswith("t-")
    assert order_text(cid) == text
    order_value = SimpleNamespace(
        client_order_id=cid,
        instrument_id=parse_instrument(PAIR, 0).id,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        is_post_only=True,
        is_quote_quantity=False,
        is_reduce_only=False,
        quantity=Quantity.from_str("0.0001"),
        price=Price.from_str("100000.00"),
    )
    assert submit_payload(order_value)["time_in_force"] == "poc"
    order_value.is_post_only = False
    with pytest.raises(ValueError):
        submit_payload(order_value)


def test_uncertain_writes_are_never_retried():
    async def run():
        client = GateioHttpClient(
            TestClock(), "http://local/api/v4", GateioDataClientConfig(), "key", "secret"
        )
        client.transport = SimpleNamespace(request=AsyncMock(side_effect=TimeoutError))
        with pytest.raises(TimeoutError):
            await client.request("POST", "/spot/orders", body={}, private=True)
        assert client.transport.request.await_count == 1
        client.transport = SimpleNamespace(
            request=AsyncMock(
                return_value=SimpleNamespace(
                    status=503,
                    body=b'{"label":"SERVER_ERROR"}',
                )
            )
        )
        with pytest.raises(GateioHttpError) as error:
            await client.request("DELETE", "/spot/orders/1", private=True)
        assert not error.value.definite_rejection
        assert client.transport.request.await_count == 1

    asyncio.run(run())


def test_wire_signature_matches_transport_and_empty_query_has_no_question_mark():
    async def run():
        clock = TestClock()
        clock.set_time(1793620800000000000)
        client = GateioHttpClient(
            clock, "http://local/api/v4", GateioDataClientConfig(), "key", "secret"
        )
        transport = SimpleNamespace(
            request=AsyncMock(return_value=SimpleNamespace(status=200, body=b"[]"))
        )
        client.transport = transport
        await client.request("GET", "/spot/accounts", private=True)
        args = transport.request.call_args.kwargs
        assert args["url"] == "http://local/api/v4/spot/accounts"
        assert args["headers"]["SIGN"] == sign_rest(
            "secret",
            "GET",
            "/api/v4/spot/accounts",
            "",
            b"",
            "1793620800",
        )

    asyncio.run(run())


def test_subscription_rejection_and_malformed_data_disable_readiness():
    async def run():
        loop = asyncio.get_running_loop()
        ws = GateioWebSocket(loop, TestClock(), "ws://local", AsyncMock(), AsyncMock())
        future = loop.create_future()
        ws.pending[("spot.orders", "subscribe")] = future
        ws.ready = True
        ws._receive(b'{"channel":"spot.orders","event":"subscribe","error":{"code":2}}')
        with pytest.raises(RuntimeError):
            await future
        assert not ws.ready
        ws._receive(b"invalid")
        assert ws.error is not None

    asyncio.run(run())


@pytest.mark.parametrize(
    ("event", "finish", "left", "expected"),
    [
        ("put", "open", "0.0001", OrderStatus.ACCEPTED),
        ("update", "open", "0.00006", OrderStatus.PARTIALLY_FILLED),
        ("finish", "filled", "0", OrderStatus.FILLED),
        ("finish", "cancelled", "0.00006", OrderStatus.CANCELED),
        ("finish", "stp", "0.0001", OrderStatus.CANCELED),
        ("finish", "poc", "0.0001", OrderStatus.REJECTED),
    ],
)
def test_official_ws_order_shape_has_no_rest_status(event, finish, left, expected):
    row = order()
    del row["status"]
    row.update(event=event, finish_as=finish, left=left)
    report = order_report(row, parse_instrument(PAIR, 0), AccountId("GATEIO-master"), None, 200)
    assert report.order_status == expected


def test_unified_asset_routing_and_missing_fees_are_not_silently_accepted():
    instrument = parse_instrument(PAIR, 0)
    with pytest.raises(ValueError, match="regular spot"):
        fill_report(trade(money="USDC"), instrument, AccountId("GATEIO-master"), None, 200)
    row = trade()
    del row["fee_currency"]
    with pytest.raises(KeyError):
        fill_report(row, instrument, AccountId("GATEIO-master"), None, 200)
    # Both REST and WS expose the global id. Optional id_market must not change it.
    fill = fill_report(
        trade(id_market="different-local-id"), instrument, AccountId("GATEIO-master"), None, 200
    )
    assert str(fill.trade_id) == "1"


def test_read_retries_and_full_pagination_are_bounded():
    async def run():
        client = GateioHttpClient(
            TestClock(),
            "http://local/api/v4",
            GateioDataClientConfig(),
            "key",
            "secret",
        )
        transport = SimpleNamespace(
            request=AsyncMock(
                side_effect=[
                    SimpleNamespace(status=429, body=b'{"label":"TOO_MANY_REQUESTS"}'),
                    SimpleNamespace(status=200, body=b"[]"),
                ]
            )
        )
        client.transport = transport
        assert await client.request("GET", "/spot/accounts", private=True) == []
        assert transport.request.await_count == 2
        client.request = AsyncMock(
            side_effect=[[{"id": str(i)} for i in range(100)], [{"id": "last"}]]
        )
        rows = await client.pages("/spot/orders", {"currency_pair": "BTC_USDT"})
        assert len(rows) == 101
        assert client.request.call_args_list[1].args[2]["page"] == 2
        client.request = AsyncMock(return_value={"invalid": "shape"})
        with pytest.raises(ValueError, match="must be a list"):
            await client.pages("/spot/orders", {})

    asyncio.run(run())


def test_lagging_rest_snapshot_keeps_the_ws_interval_for_the_next_attempt():
    async def run():
        instrument_id = parse_instrument(PAIR, 0).id
        book = GateBook(2)
        book.update({"U": 103, "u": 103, "t": 1793620800000, "b": [["99", "2"]], "a": []})
        books = {instrument_id: book}
        client = SimpleNamespace(
            books=books,
            config=GateioDataClientConfig(book_depth=2, max_retries=1),
            http=SimpleNamespace(
                request=AsyncMock(
                    side_effect=[
                        {"id": seq, "bids": [["99", "1"]], "asks": [["101", "1"]]}
                        for seq in (100, 102)
                    ]
                )
            ),
            _emit=Mock(),
            _reset=lambda iid: books.update({iid: GateBook(2)}),
        )
        await GateioDataClient._resync(client, instrument_id)
        assert client.http.request.await_count == 2
        assert books[instrument_id].ready
        assert books[instrument_id].sequence == 103
        assert books[instrument_id].bids[Decimal(99)] == 2

    asyncio.run(run())


def test_timed_out_submission_can_be_queried_after_it_has_already_closed():
    async def run():
        instrument = parse_instrument(PAIR, 0)
        cid = ClientOrderId("timed-out-order")
        closed = order()
        closed.update(status="closed", left="0", text=order_text(cid), finish_as="filled")
        fill_check = AsyncMock()
        client = SimpleNamespace(
            http=SimpleNamespace(
                request=AsyncMock(side_effect=GateioHttpError(404, "ORDER_NOT_FOUND")),
                pages=AsyncMock(return_value=[closed]),
            ),
            config=SimpleNamespace(reconciliation_lookback_mins=60),
            _clock=TestClock(),
            _instrument=lambda _: instrument,
            _resolve=lambda _: cid,
            _load_order_fills=fill_check,
            _report_lock=asyncio.Lock(),
            account_id=AccountId("GATEIO-master"),
        )
        command = SimpleNamespace(
            client_order_id=cid,
            venue_order_id=None,
            instrument_id=instrument.id,
        )
        report = await GateioExecutionClient.generate_order_status_report(client, command)
        assert report.order_status == OrderStatus.FILLED
        assert report.client_order_id == cid
        assert client.http.pages.call_args.args[1]["status"] == "finished"
        fill_check.assert_awaited_once()

    asyncio.run(run())


def test_read_transport_timeouts_and_non_json_server_errors_are_retried():
    async def run():
        client = GateioHttpClient(
            TestClock(),
            "http://local/api/v4",
            GateioDataClientConfig(),
            "key",
            "secret",
        )
        client.transport = SimpleNamespace(
            request=AsyncMock(
                side_effect=[
                    TimeoutError("read timed out"),
                    SimpleNamespace(status=503, body=b"<html>Gateway unavailable</html>"),
                    SimpleNamespace(status=200, body=b"[]"),
                ]
            )
        )
        assert await client.request("GET", "/spot/accounts", private=True) == []
        assert client.transport.request.await_count == 3

    asyncio.run(run())
