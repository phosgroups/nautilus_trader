"""Spot v1 order command and execution report parity."""

from decimal import Decimal

import pytest

from nautilus_trader.adapters.gateio.parsing import order_report
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import AccountId

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
