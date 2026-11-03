"""Convert exchange facts to NT v1 reports without synthesizing fills from order totals."""

from decimal import Decimal

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def event_ns(row, prefix, fallback):
    if row.get(prefix + "_ms") is not None:
        return int(Decimal(str(row[prefix + "_ms"])) * 1_000_000)
    if row.get(prefix) is not None:
        return int(Decimal(str(row[prefix])) * 1_000_000_000)
    return fallback


def public_trade_tick(row, instrument, now):
    """Gate spot.trades reports the taker's side and a potentially fractional ms time."""
    if row["currency_pair"] != instrument.raw_symbol.value:
        raise ValueError("Gate trade instrument mismatch")
    price, size = Decimal(row["price"]), Decimal(row["amount"])
    if any(not value.is_finite() or value <= 0 for value in (price, size)):
        raise ValueError("Invalid Gate public trade price or size")
    side = {"buy": AggressorSide.BUYER, "sell": AggressorSide.SELLER}[row["side"]]
    return TradeTick(
        instrument.id,
        Price.from_str(f"{price:.{instrument.price_precision}f}"),
        Quantity.from_str(f"{size:.{instrument.size_precision}f}"),
        side,
        TradeId(str(row["id"])),
        event_ns(row, "create_time", now),
        now,
    )


def public_quote_tick(row, instrument, now):
    """Return no quote while either side of Gate's best-price channel is empty."""
    if row["s"] != instrument.raw_symbol.value:
        raise ValueError("Gate quote instrument mismatch")
    values = [Decimal(row.get(key) or "0") for key in ("b", "a", "B", "A")]
    if any(not value.is_finite() or value < 0 for value in values):
        raise ValueError("Invalid Gate public quote price or size")
    bid, ask, bid_size, ask_size = values
    if not all(values):
        return None
    if bid >= ask:
        raise ValueError("Crossed Gate public quote")
    return QuoteTick(
        instrument.id,
        Price.from_str(f"{bid:.{instrument.price_precision}f}"),
        Price.from_str(f"{ask:.{instrument.price_precision}f}"),
        Quantity.from_str(f"{bid_size:.{instrument.size_precision}f}"),
        Quantity.from_str(f"{ask_size:.{instrument.size_precision}f}"),
        int(Decimal(str(row["t"])) * 1_000_000),
        now,
    )


def order_report(row, instrument, account_id, client_order_id, now):
    amount, left = Decimal(row["amount"]), Decimal(row.get("left", row["amount"]))
    if not amount.is_finite() or not left.is_finite() or amount <= 0 or not 0 <= left <= amount:
        raise ValueError("Invalid Gate order quantities")
    validate_assets(row, instrument)
    status = row.get("status")
    finish = row.get("finish_as")
    if status is None:
        # spot.orders WS reports use event/finish_as, not REST's status.
        if row.get("event") in {"put", "update"} and finish in {None, "open"}:
            status = "open"
        elif row.get("event") == "finish" and finish not in {None, "open", "-"}:
            status = "closed"
    if finish in {"poc", "small", "unified_check_failed"} and left == amount:
        status = OrderStatus.REJECTED
    elif status == "open":
        status = OrderStatus.PARTIALLY_FILLED if left < amount else OrderStatus.ACCEPTED
    elif status == "closed" and left == 0:
        status = OrderStatus.FILLED
    elif status in {"cancelled", "canceled"} or (
        status == "closed"
        and finish
        in {
            "cancelled",
            "ioc",
            "poc",
            "fok",
            "stp",
            "trader_not_enough",
            "depth_not_enough",
            "small",
            "liquidate_cancelled",
            "unified_check_failed",
            "price_protect_cancelled",
        }
    ):
        status = OrderStatus.CANCELED
    else:
        raise ValueError("Unknown Gate order terminal state")
    if row.get("type", "limit") != "limit":
        raise ValueError("Gate PMM order reports currently support base-sized limit orders")
    side = {"buy": OrderSide.BUY, "sell": OrderSide.SELL}[row["side"]]
    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(str(row["id"])),
        client_order_id=client_order_id,
        order_side=side,
        order_type=OrderType.LIMIT,
        time_in_force={
            "gtc": TimeInForce.GTC,
            "poc": TimeInForce.GTC,
            "ioc": TimeInForce.IOC,
            "fok": TimeInForce.FOK,
        }[row.get("time_in_force", "gtc")],
        order_status=status,
        quantity=Quantity.from_str(f"{amount:.{instrument.size_precision}f}"),
        filled_qty=Quantity.from_str(f"{amount - left:.{instrument.size_precision}f}"),
        price=Price.from_str(f"{Decimal(row['price']):.{instrument.price_precision}f}"),
        avg_px=Decimal(str(row["avg_deal_price"])) if row.get("avg_deal_price") else None,
        post_only=row.get("time_in_force") == "poc",
        report_id=UUID4(),
        ts_accepted=event_ns(row, "create_time", now),
        ts_last=event_ns(row, "update_time", now),
        ts_init=now,
    )


def fill_report(row, instrument, account_id, client_order_id, now):
    validate_assets(row, instrument)
    # GT/points discounts are additional dimensions, not zero quote fees.
    if Decimal(str(row.get("gt_fee", "0"))) or Decimal(str(row.get("point_fee", "0"))):
        raise ValueError("Gate GT/points fee deductions are not supported by this PMM profile")
    fee = Decimal(str(row["fee"]))
    rebate = Decimal(str(row.get("rebated_fee", "0")))
    if not fee.is_finite() or not rebate.is_finite() or min(fee, rebate) < 0:
        raise ValueError("Gate fee and rebate must be finite nonnegative amounts")
    fee_currency = Currency.from_str(row["fee_currency"])
    if rebate:
        rebate_currency = Currency.from_str(row["rebated_fee_currency"])
        if fee and fee_currency != rebate_currency:
            raise ValueError(
                "A Gate fill with fee and rebate in different currencies cannot be represented by one NT commission"
            )
        fee_currency = rebate_currency
    commission = Money.from_decimal(fee - rebate, fee_currency)
    return FillReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(str(row["order_id"])),
        client_order_id=client_order_id,
        trade_id=TradeId(str(row["id"])),
        order_side={"buy": OrderSide.BUY, "sell": OrderSide.SELL}[row["side"]],
        last_qty=Quantity.from_str(f"{Decimal(row['amount']):.{instrument.size_precision}f}"),
        last_px=Price.from_str(f"{Decimal(row['price']):.{instrument.price_precision}f}"),
        commission=commission,
        liquidity_side={"maker": LiquiditySide.MAKER, "taker": LiquiditySide.TAKER}[row["role"]],
        report_id=UUID4(),
        ts_event=event_ns(row, "create_time", now),
        ts_init=now,
    )


def validate_assets(row, instrument):
    if (
        int(row.get("trade_mode", 0)) != 0
        or row.get("stock", instrument.base_currency.code) != instrument.base_currency.code
        or row.get("money", instrument.quote_currency.code) != instrument.quote_currency.code
    ):
        raise ValueError("Gate PMM supports regular spot assets, not unified-asset routing")
