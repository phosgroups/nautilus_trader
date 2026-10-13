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

from decimal import Decimal

import pytest

from nautilus_trader.common import Environment
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.live.data_engine import LiveDataEngine
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.live.risk_engine import LiveRiskEngine
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderUpdated
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Quantity
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.trading.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.trader import Trader


@pytest.fixture
def reconciliation_engine(event_loop, cache, trader_id):
    clock = LiveClock()
    msgbus = MessageBus(trader_id=trader_id, clock=clock)
    engine = LiveExecutionEngine(loop=event_loop, msgbus=msgbus, cache=cache, clock=clock)
    return engine, msgbus, clock, event_loop


def _claim(instrument, reconciliation_engine, cache, trader_id):
    engine, msgbus, clock, loop = reconciliation_engine
    strategy = Strategy(StrategyConfig(external_order_claims=[instrument.id.value]))
    portfolio = Portfolio(msgbus=msgbus, cache=cache, clock=clock)
    trader = Trader(
        trader_id=trader_id,
        instance_id=UUID4(),
        msgbus=msgbus,
        cache=cache,
        portfolio=portfolio,
        data_engine=LiveDataEngine(loop=loop, msgbus=msgbus, cache=cache, clock=clock),
        risk_engine=LiveRiskEngine(
            loop=loop,
            portfolio=portfolio,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        ),
        exec_engine=engine,
        clock=clock,
        environment=Environment.LIVE,
    )
    trader.add_strategy(strategy)
    assert engine.get_external_order_claim(instrument.id) == strategy.id
    received = []
    msgbus.subscribe(f"events.order.{strategy.id}", received.append)
    return strategy, received


def _status(instrument, account_id, status, quantity, filled_qty, **kwargs):
    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("EXTERNAL-QUOTE-001"),
        venue_order_id=VenueOrderId("QUOTE-001"),
        order_side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.GTC,
        order_status=status,
        quantity=quantity,
        filled_qty=instrument.make_qty(filled_qty),
        report_id=UUID4(),
        ts_accepted=1,
        ts_last=3 if status == OrderStatus.FILLED else 2,
        ts_init=4,
        **kwargs,
    )


def _fill(instrument, account_id, trade_id, quantity, timestamp):
    return FillReport(
        account_id=account_id,
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("EXTERNAL-QUOTE-001"),
        venue_order_id=VenueOrderId("QUOTE-001"),
        trade_id=TradeId(trade_id),
        order_side=OrderSide.BUY,
        last_qty=instrument.make_qty(quantity),
        last_px=instrument.make_price(10_000),
        commission=Money(0.07, instrument.quote_currency),
        liquidity_side=LiquiditySide.TAKER,
        report_id=UUID4(),
        ts_event=timestamp,
        ts_init=timestamp,
    )


def _reconcile_bundle(engine, report, fills):
    mass_status = ExecutionMassStatus(
        client_id=ClientId("BITGET"),
        account_id=report.account_id,
        venue=report.instrument_id.venue,
        report_id=UUID4(),
        ts_init=report.ts_init,
    )
    mass_status.add_order_reports([report])
    mass_status.add_fill_reports(fills)
    return engine._reconcile_execution_mass_status(mass_status)


def test_quote_external_market_buy_converts_before_real_partial_and_full_fills(
    reconciliation_engine,
    cache,
    trader_id,
    account_id,
):
    instrument = TestInstrumentProvider.btcusdt_binance()
    cache.add_instrument(instrument)
    engine, _, _, _ = reconciliation_engine
    strategy, received = _claim(instrument, reconciliation_engine, cache, trader_id)
    accepted = _status(
        instrument,
        account_id,
        OrderStatus.ACCEPTED,
        Quantity.from_str("100.00"),
        0,
        is_quote_quantity=True,
    )

    assert engine.reconcile_execution_report(accepted)
    order = cache.order(accepted.client_order_id)
    assert order.strategy_id == strategy.id
    assert order.is_quote_quantity
    assert order.quantity == Quantity.from_str("100.00")
    assert order.filled_qty == instrument.make_qty(0)

    first_fill = _fill(instrument, account_id, "REAL-TRADE-1", 0.004, 2)
    partial = _status(
        instrument,
        account_id,
        OrderStatus.PARTIALLY_FILLED,
        instrument.make_qty(0.01),
        0.004,
        avg_px=Decimal(10000),
    )

    assert _reconcile_bundle(engine, partial, [first_fill])
    assert not order.is_quote_quantity
    assert order.quantity == instrument.make_qty(0.01)
    assert order.filled_qty == instrument.make_qty(0.004)
    assert order.leaves_qty == instrument.make_qty(0.006)
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert (
        next(event for event in received if isinstance(event, OrderUpdated)).is_quote_quantity
        is False
    )

    last_fill = _fill(instrument, account_id, "REAL-TRADE-2", 0.006, 3)
    filled = _status(
        instrument,
        account_id,
        OrderStatus.FILLED,
        instrument.make_qty(0.01),
        0.01,
        avg_px=Decimal(10000),
    )

    assert _reconcile_bundle(engine, filled, [first_fill, last_fill])
    assert _reconcile_bundle(engine, filled, [first_fill, last_fill])
    assert order.status == OrderStatus.FILLED
    assert not order.is_quote_quantity
    assert order.filled_qty == instrument.make_qty(0.01)
    assert order.leaves_qty == instrument.make_qty(0)
    fills = [event for event in received if isinstance(event, OrderFilled)]
    assert [event.trade_id for event in fills] == [first_fill.trade_id, last_fill.trade_id]
    assert [event.last_qty for event in fills] == [first_fill.last_qty, last_fill.last_qty]
    assert [event.commission for event in fills] == [first_fill.commission, last_fill.commission]


def test_quote_to_base_update_with_equal_numerical_quantities(
    reconciliation_engine,
    cache,
    account_id,
):
    instrument = TestInstrumentProvider.btcusdt_binance()
    cache.add_instrument(instrument)
    engine, _, _, _ = reconciliation_engine
    accepted = _status(
        instrument,
        account_id,
        OrderStatus.ACCEPTED,
        instrument.make_qty(1),
        0,
        is_quote_quantity=True,
    )
    assert engine.reconcile_execution_report(accepted)

    base_report = _status(
        instrument,
        account_id,
        OrderStatus.ACCEPTED,
        instrument.make_qty(1),
        0,
    )
    assert engine.reconcile_execution_report(base_report)

    order = cache.order(accepted.client_order_id)
    assert order.quantity == base_report.quantity
    assert not order.is_quote_quantity
    assert any(isinstance(event, OrderUpdated) for event in order.events)


def test_out_of_order_real_fills_keep_trade_ids_without_an_inferred_cutoff(
    reconciliation_engine,
    cache,
    account_id,
):
    instrument = TestInstrumentProvider.btcusdt_binance()
    cache.add_instrument(instrument)
    engine, _, _, _ = reconciliation_engine
    accepted = _status(
        instrument,
        account_id,
        OrderStatus.ACCEPTED,
        instrument.make_qty(0.01),
        0,
    )
    assert engine.reconcile_execution_report(accepted)

    later_fill = _fill(instrument, account_id, "REAL-LATER", 0.004, 3)
    earlier_fill = _fill(instrument, account_id, "REAL-EARLIER", 0.003, 2)

    assert engine.reconcile_execution_report(later_fill)
    assert engine.reconcile_execution_report(earlier_fill)
    assert engine.reconcile_execution_report(earlier_fill)

    order = cache.order(accepted.client_order_id)
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.filled_qty == instrument.make_qty(0.007)
    assert order.leaves_qty == instrument.make_qty(0.003)
    assert order.trade_ids == [later_fill.trade_id, earlier_fill.trade_id]
    assert order.client_order_id not in engine._inferred_fill_ts


def test_inferred_fill_still_blocks_historical_fills_already_accounted_for(
    reconciliation_engine,
    cache,
    account_id,
):
    instrument = TestInstrumentProvider.btcusdt_binance()
    cache.add_instrument(instrument)
    engine, _, _, _ = reconciliation_engine
    partial = _status(
        instrument,
        account_id,
        OrderStatus.PARTIALLY_FILLED,
        instrument.make_qty(0.01),
        0.004,
        avg_px=Decimal(10000),
    )

    assert engine.reconcile_execution_report(partial)
    order = cache.order(partial.client_order_id)
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert engine._inferred_fill_ts[order.client_order_id] == partial.ts_last
    inferred_trade_ids = order.trade_ids

    historical = _fill(instrument, account_id, "HISTORICAL-REAL", 0.004, 1)
    assert engine.reconcile_execution_report(historical)

    assert order.filled_qty == partial.filled_qty
    assert order.trade_ids == inferred_trade_ids
    assert historical.trade_id not in order.trade_ids


@pytest.mark.parametrize(
    "instrument",
    [TestInstrumentProvider.btcusdt_binance(), TestInstrumentProvider.btcusdt_perp_binance()],
    ids=["spot-base", "perpetual"],
)
def test_base_external_orders_keep_quantity_units_and_real_fill_identity(
    instrument,
    reconciliation_engine,
    cache,
    account_id,
):
    cache.add_instrument(instrument)
    engine, _, _, _ = reconciliation_engine
    report = _status(
        instrument,
        account_id,
        OrderStatus.FILLED,
        instrument.make_qty(1),
        1,
        avg_px=Decimal(10000),
    )
    fill = _fill(instrument, account_id, "REAL-BASE-TRADE", 1, 3)

    assert _reconcile_bundle(engine, report, [fill])

    order = cache.order(report.client_order_id)
    assert not order.is_quote_quantity
    assert order.quantity == report.quantity
    assert order.filled_qty == report.filled_qty
    assert order.trade_ids == [fill.trade_id]
    assert order.status == OrderStatus.FILLED
    assert not any(isinstance(event, OrderUpdated) for event in order.events)
