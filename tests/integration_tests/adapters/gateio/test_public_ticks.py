"""Public Gate trade and best-price subscriptions alongside managed L2 books."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.adapters.gateio.parsing import public_quote_tick
from nautilus_trader.adapters.gateio.parsing import public_trade_tick
from nautilus_trader.adapters.gateio.providers import parse_instrument
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
            ws=AsyncMock(),
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
