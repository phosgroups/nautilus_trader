# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import pytest

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.execution import TestExecStubs


def _report(**kwargs):
    return OrderStatusReport(
        account_id=AccountId("BITGET-001"),
        instrument_id=TestInstrumentProvider.btcusdt_binance().id,
        venue_order_id=VenueOrderId("QUOTE-001"),
        order_side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.GTC,
        order_status=OrderStatus.ACCEPTED,
        quantity=Quantity.from_str("100.00"),
        filled_qty=Quantity.from_str("0.000000"),
        report_id=UUID4(),
        ts_accepted=1,
        ts_last=2,
        ts_init=3,
        **kwargs,
    )


@pytest.mark.parametrize("is_quote_quantity", [False, True])
def test_order_report_quantity_unit_survives_python_rust_round_trip(is_quote_quantity):
    report = _report(is_quote_quantity=is_quote_quantity)

    rust_report = report.to_pyo3()
    restored = OrderStatusReport.from_pyo3(rust_report)

    assert rust_report.is_quote_quantity is is_quote_quantity
    assert restored.is_quote_quantity is is_quote_quantity
    assert restored.quantity == report.quantity
    assert restored.filled_qty == report.filled_qty
    assert restored.id == report.id


@pytest.mark.parametrize("is_quote_quantity", [False, True])
def test_order_report_quantity_unit_survives_dict_round_trip(is_quote_quantity):
    report = _report(is_quote_quantity=is_quote_quantity)

    restored = OrderStatusReport.from_dict(report.to_dict())

    assert restored.is_quote_quantity is is_quote_quantity
    assert restored.quantity == report.quantity
    assert restored.filled_qty == report.filled_qty


def test_order_report_legacy_dict_and_constructor_default_to_base_units():
    report = _report()
    values = report.to_dict()
    del values["is_quote_quantity"]

    assert report.is_quote_quantity is False
    assert OrderStatusReport.from_dict(values).is_quote_quantity is False
    assert report.to_pyo3().is_quote_quantity is False


def test_order_report_detects_quantity_unit_change_without_numerical_change():
    report = _report()
    order = TestExecStubs.market_order(quantity=report.quantity)
    order.set_quote_quantity(True)

    assert report.is_order_updated(order)
