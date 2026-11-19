"""Spot v1 order command and execution report parity."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from nautilus_trader.adapters.gateio.execution import GateioExecutionClient
from nautilus_trader.adapters.gateio.execution import submit_payload
from nautilus_trader.adapters.gateio.http import GateioHttpError
from nautilus_trader.adapters.gateio.parsing import order_report
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId

from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.component import TestComponentStubs

from test_protocol import PAIR
from test_protocol import order


@pytest.mark.parametrize(
    ("tif", "left", "finish", "status"),
    [
        ("ioc", "0.00006", "ioc", OrderStatus.CANCELED),
        ("fok", "0.0001", "fok", OrderStatus.CANCELED),
        ("fok", "0", "filled", OrderStatus.FILLED),
    ],
)
def test_limit_order_terminal_reports(tif, left, finish, status):
    row = dict(order(), time_in_force=tif, status="closed", finish_as=finish, left=left)
    report = order_report(row, parse_instrument(PAIR, 0), AccountId("GATEIO-test"), None, 1)
    assert report.order_status == status
    assert report.filled_qty.as_decimal() == Decimal(row["amount"]) - Decimal(left)
    assert not report.post_only


@pytest.mark.parametrize(("side", "quote", "tif", "expected"), [
    (OrderSide.BUY, True, TimeInForce.GTC, "ioc"),
    (OrderSide.SELL, False, TimeInForce.FOK, "fok"),
    (OrderSide.BUY, False, TimeInForce.IOC, None),
    (OrderSide.SELL, True, TimeInForce.IOC, None),
])
def test_submit_native_market_order(side, quote, tif, expected):
    native = TestComponentStubs.order_factory().market(
        parse_instrument(PAIR, 0).id, side, Quantity.from_int(3), time_in_force=tif,
        quote_quantity=quote, client_order_id=ClientOrderId("spot-market"),
    )
    if expected is None:
        with pytest.raises(ValueError, match="quote quantity"):
            submit_payload(native)
        return
    payload = submit_payload(native)
    assert payload["type"] == "market"
    assert payload["time_in_force"] == expected
    assert payload["amount"] == "3"
    assert "price" not in payload


@pytest.mark.parametrize(("fields", "quantity", "filled", "quote", "status"), [
    ({"left": "10"}, "10", "0", True, OrderStatus.ACCEPTED),
    ({"left": "6", "filled_amount": "0.04", "avg_deal_price": "100", "status": "cancelled"},
     "0.1", "0.04", False, OrderStatus.CANCELED),
    ({"left": "0.1", "filled_total": "9.9", "avg_deal_price": "100", "finish_as": "filled",
      "status": "closed"}, "0.099", "0.099", False, OrderStatus.FILLED),
    ({"side": "sell", "left": "4", "status": "cancelled"}, "10", "6", False, OrderStatus.CANCELED),
])
def test_market_report_units(fields, quantity, filled, quote, status):
    row = dict(order(), type="market", amount="10", price="0", time_in_force="ioc")
    row.update(fields)
    report = order_report(row, parse_instrument(PAIR, 0), AccountId("GATEIO-test"), None, 1)
    assert (report.quantity.as_decimal(), report.filled_qty.as_decimal()) == (
        Decimal(quantity), Decimal(filled),
    )
    assert report.order_status == status
    assert report.order_type == OrderType.MARKET
    assert report.is_quote_quantity is quote
    assert report.price is None


@pytest.mark.parametrize("outcome", ["rejected", "timeout"])
def test_amendment_reconciles_an_uncertain_patch_without_resending(outcome):
    async def run():
        instrument = parse_instrument(PAIR, 0)
        request = SimpleNamespace(
            venue_order_id=VenueOrderId("123"), client_order_id=ClientOrderId("co1"),
            instrument_id=instrument.id, strategy_id="S-001", quantity=None,
            price=Price.from_str("110000"), trigger_price=None,
        )
        cached = SimpleNamespace(
            venue_order_id=request.venue_order_id, instrument_id=instrument.id, is_closed=False,
            order_type=OrderType.LIMIT, strategy_id=request.strategy_id, account_id=AccountId("GATEIO-test"),
        )
        error = GateioHttpError(400, "INVALID_PARAM_VALUE") if outcome == "rejected" else TimeoutError()
        updated = dict(order(), price="110000")
        transport = AsyncMock(side_effect=[error, updated])
        client = SimpleNamespace(
            _cache=SimpleNamespace(order=lambda cid: cached), account_id=cached.account_id,
            _clock=SimpleNamespace(timestamp_ns=lambda: 0), _publish_order=AsyncMock(),
            _report_lock=asyncio.Lock(), generate_order_modify_rejected=Mock(),
            http=SimpleNamespace(request=transport),
        )
        await GateioExecutionClient._modify_order(client, request)
        assert transport.await_args_list[0].args == ("PATCH", "/spot/orders/123")
        assert transport.await_args_list[0].kwargs["body"] == {
            "price": "110000", "currency_pair": "BTC_USDT", "account": "spot",
        }
        if outcome == "timeout":
            assert transport.await_count == 2
            assert transport.await_args_list[1].args[0] == "GET"
            client._publish_order.assert_awaited_once_with(updated)
            client.generate_order_modify_rejected.assert_not_called()
        else:
            assert transport.await_count == 1
            client.generate_order_modify_rejected.assert_called_once()

    asyncio.run(run())
