"""Public Gate trade and best-price subscriptions alongside managed L2 books."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.adapters.gateio.parsing import public_quote_tick
from nautilus_trader.adapters.gateio.parsing import public_trade_tick
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.common.component import TestClock
from nautilus_trader.model.enums import AggressorSide


@pytest.fixture
def instrument():
    return parse_instrument(
        {
            "id": "BTC_USDT",
            "base": "BTC",
            "quote": "USDT",
            "precision": 2,
            "amount_precision": 6,
            "min_quote_amount": "1",
            "trade_status": "tradable",
        },
        0,
    )


def trade_row(**overrides):
    return dict(
        {
            "id": "9223372036854775808",
            "currency_pair": "BTC_USDT",
            "price": "100000.12",
            "amount": "0.000123",
            "side": "buy",
            "create_time_ms": "1793700000123.456",
        },
        **overrides,
    )


def quote_row(**overrides):
    return dict(
        {
            "s": "BTC_USDT",
            "t": 1793700000123,
            "b": "99999.99",
            "a": "100000.01",
            "B": "1.000001",
            "A": "2.000002",
        },
        **overrides,
    )


@pytest.mark.parametrize(
    ("side", "expected"), [("buy", AggressorSide.BUYER), ("sell", AggressorSide.SELLER)]
)
def test_public_trade_preserves_id_side_and_fractional_exchange_time(instrument, side, expected):
    tick = public_trade_tick(trade_row(side=side), instrument, 1793700000124000000)
    assert str(tick.trade_id) == "9223372036854775808"
    assert tick.aggressor_side == expected
    assert tick.price.as_decimal() == Decimal("100000.12")
    assert tick.size.as_decimal() == Decimal("0.000123")
    assert tick.ts_event == 1793700000123456000
    assert tick.ts_init == 1793700000124000000


def test_public_quote_uses_best_sizes_and_exchange_time(instrument):
    tick = public_quote_tick(quote_row(), instrument, 1793700000124000000)
    assert tick.bid_size.as_decimal() == Decimal("1.000001")
    assert tick.ask_size.as_decimal() == Decimal("2.000002")
    assert tick.ts_event == 1793700000123000000


@pytest.mark.parametrize(
    ("symbol", "precision", "amount_precision", "bid", "ask", "bid_size", "ask_size"),
    [
        ("BTC_USDT", 2, 6, "99999.9", "100000.01", "1", "2.000002"),
        ("LIGHT_USDT", 5, 2, "0.123", "0.12345", "100", "101.25"),
    ],
)
def test_quote_normalizes_both_price_and_size_precision_without_changing_values(
    symbol, precision, amount_precision, bid, ask, bid_size, ask_size,
):
    instrument = parse_instrument(
        {"id": symbol, "base": symbol.split("_")[0], "quote": "USDT",
         "precision": precision, "amount_precision": amount_precision},
        0,
    )
    tick = public_quote_tick(
        quote_row(s=symbol, b=bid, a=ask, B=bid_size, A=ask_size), instrument, 42,
    )
    assert tick.bid_price.precision == tick.ask_price.precision == precision
    assert tick.bid_size.precision == tick.ask_size.precision == amount_precision
    assert tick.bid_price.as_decimal() == Decimal(bid)
    assert tick.ask_price.as_decimal() == Decimal(ask)
    assert tick.bid_size.as_decimal() == Decimal(bid_size)
    assert tick.ask_size.as_decimal() == Decimal(ask_size)
    assert tick.ts_init == 42


def test_quote_preserves_values_finer_than_current_instrument_rules(instrument):
    tick = public_quote_tick(
        quote_row(b="99999.901", a="100000.012", B="1.0000001", A="2"), instrument, 0,
    )
    assert tick.bid_price.precision == tick.ask_price.precision == 3
    assert tick.bid_size.precision == tick.ask_size.precision == 7
    assert tick.bid_price.as_decimal() == Decimal("99999.901")
    assert tick.bid_size.as_decimal() == Decimal("1.0000001")


@pytest.mark.parametrize("bad", [None, [], {}, {"s": "BTC_USDT", "b": "NaN"}])
def test_bad_independent_quote_does_not_disrupt_next_quote_or_book_subscription(instrument, bad):
    client = SimpleNamespace(
        _instrument_provider=SimpleNamespace(find=lambda _: instrument),
        public_subscriptions={"spot.book_ticker": {instrument.id}},
        _clock=SimpleNamespace(timestamp_ns=lambda: 42),
        _handle_data=Mock(), _log=Mock(),
    )
    GateioDataClient._public_tick(client, "spot.book_ticker", bad)
    assert client.public_subscriptions["spot.book_ticker"] == {instrument.id}
    client._handle_data.assert_not_called()
    GateioDataClient._public_tick(client, "spot.book_ticker", quote_row(B="1", A="2.01"))
    client._handle_data.assert_called_once()


def test_btc_and_light_subscriptions_share_a_channel_without_conflicting_acknowledgements():
    import json

    async def run():
        ws = GateioWebSocket(
            asyncio.get_running_loop(), TestClock(), "ws://local", AsyncMock(), AsyncMock(),
        )

        async def send(raw):
            message = json.loads(raw)
            await asyncio.sleep(0.01)
            ws._receive(json.dumps({
                "id": message["id"], "channel": message["channel"], "event": "subscribe",
                "result": {"status": "success"},
            }).encode())

        ws.client = SimpleNamespace(send_text=AsyncMock(side_effect=send))
        await asyncio.gather(
            ws.subscribe("spot.book_ticker", ["BTC_USDT"]),
            ws.subscribe("spot.book_ticker", ["LIGHT_USDT"]),
        )
        assert ws.subscriptions == [
            ("spot.book_ticker", ("BTC_USDT",)), ("spot.book_ticker", ("LIGHT_USDT",)),
        ]
        assert not ws.pending
        ids = [json.loads(call.args[0])["id"] for call in ws.client.send_text.call_args_list]
        assert len(set(ids)) == 2

    asyncio.run(run())


def test_canceled_btc_subscription_releases_light_and_late_btc_ack_cannot_confirm_light():
    import json

    async def run():
        ws = GateioWebSocket(asyncio.get_running_loop(), TestClock(), "ws://local", AsyncMock(), AsyncMock())
        ws.client = SimpleNamespace(send_text=AsyncMock())
        btc = asyncio.create_task(ws.subscribe("spot.book_ticker", ["BTC_USDT"]))
        await asyncio.sleep(0)
        first_id = json.loads(ws.client.send_text.call_args.args[0])["id"]
        light = asyncio.create_task(ws.subscribe("spot.book_ticker", ["LIGHT_USDT"]))
        btc.cancel()
        with pytest.raises(asyncio.CancelledError):
            await btc
        await asyncio.sleep(0)
        second_id = json.loads(ws.client.send_text.call_args.args[0])["id"]
        for request_id in [first_id, second_id]:
            ws._receive(json.dumps({"id": request_id, "channel": "spot.book_ticker",
                                    "event": "subscribe", "result": {"status": "success"}}).encode())
            if request_id == first_id:
                assert not light.done()
        await light
        assert ws.subscriptions == [("spot.book_ticker", ("LIGHT_USDT",))]
        assert not ws.pending
        assert ws.ready
    asyncio.run(run())


@pytest.mark.parametrize("field", ["b", "a", "B", "A"])
def test_empty_best_side_does_not_create_a_stale_or_zero_quote(instrument, field):
    assert public_quote_tick(quote_row(**{field: ""}), instrument, 0) is None


@pytest.mark.parametrize("price", ["0", "-1", "NaN", "Infinity"])
def test_invalid_trade_values_fail_closed(instrument, price):
    with pytest.raises(ValueError, match="Invalid Gate public trade"):
        public_trade_tick(trade_row(price=price), instrument, 0)


def test_crossed_quotes_and_wrong_pair_fail_closed(instrument):
    with pytest.raises(ValueError, match="Crossed"):
        public_quote_tick(quote_row(b="100001"), instrument, 0)
    with pytest.raises(ValueError, match="mismatch"):
        public_trade_tick(trade_row(currency_pair="ETH_USDT"), instrument, 0)


@pytest.mark.parametrize("channel", ["spot.trades", "spot.book_ticker"])
def test_subscription_is_idempotent_and_failed_ack_rolls_back(instrument, channel):
    async def run():
        client = SimpleNamespace(
            _instrument_provider=SimpleNamespace(find=lambda _: instrument),
            public_subscriptions={channel: set()},
            _subscription_locks={},
            ws=AsyncMock(),
        )
        client._subscription_lock = lambda ch, iid: client._subscription_locks.setdefault(
            (ch, iid), asyncio.Lock(),
        )
        client._change_public_subscription = lambda *args: GateioDataClient._change_public_subscription(
            client, *args,
        )
        subscribe = GateioDataClient._public_subscription
        await subscribe(client, channel, instrument.id, True)
        await subscribe(client, channel, instrument.id, True)
        client.ws.subscribe.assert_awaited_once_with(channel, ["BTC_USDT"])
        await subscribe(client, channel, instrument.id, False)
        client.ws.unsubscribe.assert_awaited_once_with(channel, ["BTC_USDT"])
        assert not client.public_subscriptions[channel]
        client.ws.subscribe.side_effect = RuntimeError("subscription rejected")
        with pytest.raises(RuntimeError, match="rejected"):
            await subscribe(client, channel, instrument.id, True)
        assert not client.public_subscriptions[channel]

    asyncio.run(run())
