"""Gate USDT-settled perpetual execution for the v1 live execution engine."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.adapters.gateio.config import GATEIO
from nautilus_trader.adapters.gateio.execution import order_text
from nautilus_trader.adapters.gateio.execution import original_order_id
from nautilus_trader.adapters.gateio.http import GateioHttpError
from nautilus_trader.adapters.gateio.parsing import event_ns
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import MarginBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


FUTURES = "/futures/usdt"
USDT = Currency.from_str("USDT")
TIF = {
    "gtc": TimeInForce.GTC,
    "poc": TimeInForce.GTC,
    "ioc": TimeInForce.IOC,
    "fok": TimeInForce.FOK,
}


def _decimal(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Gate futures number must be finite")
    return result


def _quantity(value):
    return Quantity.from_str(format(value, "f"))


def futures_payload(order, contract):
    if order.order_type not in (OrderType.MARKET, OrderType.LIMIT) or order.is_quote_quantity:
        raise ValueError("Gate perpetuals require limit or market orders sized in contracts")
    if order.time_in_force not in (
        TimeInForce.GTC,
        TimeInForce.IOC,
        TimeInForce.FOK,
    ):
        raise ValueError("Unsupported Gate perpetual order validity")
    size = order.quantity.as_decimal()
    if size <= 0:
        raise ValueError("Gate perpetual order quantity must be positive")
    if order.order_type == OrderType.MARKET:
        if order.is_post_only or order.time_in_force == TimeInForce.FOK:
            raise ValueError("Gate perpetual market orders require IOC")
        price, tif = "0", "ioc"
    else:
        price = str(order.price)
        if order.is_post_only and order.time_in_force != TimeInForce.GTC:
            raise ValueError("Gate post-only order conflicts with immediate execution")
        tif = (
            "poc"
            if order.is_post_only
            else {TimeInForce.IOC: "ioc", TimeInForce.FOK: "fok"}.get(order.time_in_force, "gtc")
        )
    payload = {
        "contract": contract,
        "size": format(size if order.side == OrderSide.BUY else -size, "f"),
        "price": price,
        "tif": tif,
        "text": order_text(order.client_order_id),
        "reduce_only": order.is_reduce_only,
    }
    display = getattr(order, "display_qty", None)
    if display is not None:
        if not 0 < display <= order.quantity:
            raise ValueError(
                "Gate perpetual iceberg size must be positive and at most the order quantity"
            )
        payload["iceberg"] = str(display)
    return payload


def futures_order(row, instrument, account, client_id, now):
    size = _decimal(row["size"])
    left = abs(_decimal(row["left"]))
    total = abs(size)
    if size == 0 or left > total:
        raise ValueError("Invalid Gate perpetual order quantities")
    filled = total - left
    tif = row["tif"]
    price = _decimal(row["price"])
    status = row["status"]
    finish = row.get("finish_as")
    if status == "open":
        state = OrderStatus.PARTIALLY_FILLED if filled else OrderStatus.ACCEPTED
    elif status == "finished":
        if finish == "filled" and left:
            raise ValueError("Gate filled perpetual order still has leaves")
        state = OrderStatus.FILLED if not left else OrderStatus.CANCELED
    else:
        raise ValueError(f"Unknown Gate perpetual order status {status}")
    display = _decimal(row.get("iceberg", 0))
    accepted = event_ns(row, "create_time", now)
    updated = event_ns(row, "update_time", 0)
    # futures.orders uses epoch milliseconds under update_time, REST uses seconds.
    if "update_time_ms" not in row and _decimal(row.get("update_time") or 0) >= 1_000_000_000_000:
        updated = int(_decimal(row["update_time"]) * 1_000_000)
    return OrderStatusReport(
        account_id=account,
        client_order_id=client_id,
        venue_order_id=VenueOrderId(str(row["id"])),
        instrument_id=instrument.id,
        order_side=OrderSide.BUY if size > 0 else OrderSide.SELL,
        order_type=OrderType.LIMIT if price else OrderType.MARKET,
        time_in_force=TIF[tif],
        order_status=state,
        quantity=_quantity(total),
        filled_qty=_quantity(filled),
        price=Price.from_str(format(price, "f")) if price else None,
        display_qty=_quantity(display) if display else None,
        avg_px=_decimal(row.get("fill_price", 0)) or None,
        post_only=tif == "poc",
        reduce_only=bool(row.get("is_reduce_only", row.get("reduce_only", False))),
        report_id=UUID4(),
        ts_accepted=accepted,
        ts_last=updated or event_ns(row, "finish_time", 0) or accepted,
        ts_init=now,
    )


def _trade_id(row):
    # Time-range REST calls use trade_id. Live usertrades and GET my_trades use id.
    return str(row["trade_id"] if "trade_id" in row else row["id"])


def futures_fill(row, instrument, account, client_id, now):
    size = _decimal(row["size"])
    if not size or _decimal(row["price"]) <= 0:
        raise ValueError("Invalid Gate perpetual execution price or size")
    if _decimal(row.get("point_fee", 0)):
        raise ValueError("Gate perpetual point-card fees cannot be represented in USDT commission")
    return FillReport(
        account_id=account,
        client_order_id=client_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(str(row["order_id"])),
        trade_id=TradeId(_trade_id(row)),
        order_side=OrderSide.BUY if size > 0 else OrderSide.SELL,
        last_qty=_quantity(abs(size)),
        last_px=Price.from_str(str(row["price"])),
        commission=Money.from_decimal(_decimal(row["fee"]), USDT),
        liquidity_side={"maker": LiquiditySide.MAKER, "taker": LiquiditySide.TAKER}[row["role"]],
        report_id=UUID4(),
        ts_event=event_ns(row, "create_time", now),
        ts_init=now,
    )


def futures_position(row, instrument, account, now):
    if row.get("mode", "single") != "single":
        raise ValueError("Gate USDT perpetual execution requires one-way position mode")
    size = _decimal(row["size"])
    return PositionStatusReport(
        account_id=account,
        instrument_id=instrument.id,
        quantity=_quantity(abs(size)),
        position_side=PositionSide.LONG
        if size > 0
        else PositionSide.SHORT
        if size < 0
        else PositionSide.FLAT,
        avg_px_open=_decimal(row.get("entry_price", 0)) or None,
        report_id=UUID4(),
        ts_last=event_ns(row, "update_time", event_ns(row, "time", now)),
        ts_init=now,
    )


class GateioPerpetualExecutionClient(LiveExecutionClient):
    def __init__(self, loop, http, msgbus, cache, clock, provider, config, name=None):
        super().__init__(
            loop=loop,
            client_id=ClientId(name or "GATEIO"),
            venue=GATEIO,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=USDT,
            instrument_provider=provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )
        self._set_account_id(AccountId(f"{name or 'GATEIO'}-USDT"))
        self.http, self.config = http, config
        self._report_lock = asyncio.Lock()
        self._reconciling = asyncio.Lock()
        self._submitting = {}
        self.ws = GateioWebSocket(
            loop,
            clock,
            config.base_url_ws,
            self._message,
            self._reconcile,
            config.proxy_url,
            config.api_key,
            config.api_secret,
            error_handler=self._log.error,
            headers=[("X-Gate-Size-Decimal", "1")],
        )

    def _instrument(self, instrument_id):
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None or not instrument.id.symbol.value.endswith("-PERP"):
            raise ValueError(f"Unknown Gate perpetual instrument {instrument_id}")
        return instrument

    def _from_contract(self, contract):
        return self._instrument(InstrumentId.from_str(f"{contract}-PERP.GATEIO"))

    def _client_order_id(self, row, instrument):
        venue_id = VenueOrderId(str(row["order_id"] if "order_id" in row else row["id"]))
        cached = self._cache.client_order_id(venue_id)
        decoded = original_order_id(row.get("text"))
        if cached and decoded and cached != decoded:
            raise ValueError("Gate perpetual client and venue order identities conflict")
        client_id = decoded or cached
        order = self._cache.order(client_id) if client_id else None
        if order and (
            order.instrument_id != instrument.id
            or order.account_id not in (self.account_id, None)
            or order.venue_order_id not in (venue_id, None)
        ):
            raise ValueError("Gate perpetual report conflicts with the cached order")
        return client_id

    def _order(self, row):
        instrument = self._from_contract(row["contract"])
        return futures_order(
            row,
            instrument,
            self.account_id,
            self._client_order_id(row, instrument),
            self._clock.timestamp_ns(),
        )

    def _fill(self, row):
        instrument = self._from_contract(row["contract"])
        return futures_fill(
            row,
            instrument,
            self.account_id,
            self._client_order_id(row, instrument),
            self._clock.timestamp_ns(),
        )

    async def _connect(self):
        await self._instrument_provider.initialize()
        instruments = self._instrument_provider.get_all()
        if not instruments:
            raise ValueError("Configure Gate perpetual instruments before connecting")
        for instrument in instruments.values():
            self._cache.add_instrument(instrument)
        detail = await self.http.request("GET", "/account/detail", private=True)
        uid = str(detail["user_id"])
        await self._query_account(None)
        await self._await_account_registered()
        try:
            await self.ws.connect()
            self.ws.ready = False
            self.ws._resume.clear()
            await self.ws.subscribe("futures.balances", [uid])
            for instrument in instruments.values():
                for channel in (
                    "futures.orders",
                    "futures.usertrades",
                    "futures.positions",
                    "futures.liquidates",
                    "futures.auto_deleverages",
                    "futures.position_closes",
                ):
                    await self.ws.subscribe(channel, [uid, instrument.raw_symbol.value])
            await self._reconcile()
            if self.ws.recovery and not self.ws.recovery.done():
                await self.ws.recovery
            else:
                self.ws.ready = True
                self.ws._resume.set()
        except BaseException:
            await self._disconnect()
            raise

    async def _disconnect(self):
        await self.ws.disconnect()
        self.ws.subscriptions.clear()

    async def _query_account(self, command):
        row = await self.http.request("GET", FUTURES + "/accounts", private=True)
        if (
            row.get("currency") != "USDT"
            or int(row.get("margin_mode", 0)) != 0
            or row.get("enable_credit")
            or row.get("in_dual_mode")
            or row.get("position_mode", "single") not in ("single", "single_mode")
        ):
            raise ValueError("Gate execution requires a one-way classic USDT perpetual account")
        available, total = _decimal(row["available"]), _decimal(row["total"])
        if "cross_initial_margin" in row and (
            row.get("enable_evolved_classic")
            or not row.get("position_margin")
            or _decimal(row["cross_initial_margin"])
        ):
            initial = (
                _decimal(row["cross_initial_margin"])
                + _decimal(row.get("isolated_position_margin", 0))
                + _decimal(row.get("order_margin", row.get("cross_order_margin", 0)))
            )
            # The cash total excludes UPL, which new classic accounts can use as cross collateral.
            total += _decimal(row.get("cross_unrealised_pnl", 0))
        else:
            initial = _decimal(row.get("position_margin", 0)) + _decimal(row.get("order_margin", 0))
        maintenance = _decimal(
            row.get("maintenance_margin", row.get("cross_maintenance_margin", 0))
        )
        balance = AccountBalance(
            *(Money.from_decimal(v, USDT) for v in (total, total - available, available))
        )
        margin = MarginBalance(
            Money.from_decimal(initial, USDT), Money.from_decimal(maintenance, USDT)
        )
        self.generate_account_state(
            balances=[balance], margins=[margin], reported=True, ts_event=self._clock.timestamp_ns()
        )

    async def _pages(self, endpoint, symbol, **params):
        rows = {}
        limit = 100
        for offset in range(0, 1_000_000, limit):
            batch = await self.http.request(
                "GET",
                FUTURES + endpoint,
                {
                    **params,
                    "contract": symbol,
                    "limit": limit,
                    "offset": offset,
                },
                private=True,
            )
            before = len(rows)
            for row in batch:
                if row["contract"] != symbol:
                    raise ValueError("Gate perpetual history returned a different contract")
                key = _trade_id(row) if "order_id" in row else str(row["id"])
                rows[key] = row
            if len(batch) < limit:
                return list(rows.values())
            if len(rows) == before:
                raise ValueError("Gate perpetual pagination made no progress")
        raise ValueError("Gate perpetual history exceeds pagination limit")

    async def _get_order(self, target, symbol):
        row = await self.http.request("GET", f"{FUTURES}/orders/{target}", private=True)
        if row["contract"] != symbol or not (
            str(row["id"]) == str(target) or row.get("text") == str(target)
        ):
            raise ValueError("Gate perpetual order lookup identity mismatch")
        return row

    def _mass(self):
        return ExecutionMassStatus(
            self.id, self.account_id, GATEIO, UUID4(), self._clock.timestamp_ns()
        )

    async def _order_fills(self, report, symbol):
        if not report.filled_qty.as_decimal():
            return []
        rows = await self._pages("/my_trades", symbol, order=str(report.venue_order_id))
        if sum(abs(_decimal(row["size"])) for row in rows) < report.filled_qty.as_decimal():
            # Order-filtered /my_trades only covers the recent six months. Older live
            # orders can have executions outside both that period and our mass lookback.
            by_id = {_trade_id(row): row for row in rows}
            start = report.ts_accepted // 1_000_000_000
            end = max(report.ts_last // 1_000_000_000, start) + 1
            while start < end:
                until = min(start + 7 * 86400, end)
                batch = await self._pages(
                    "/my_trades_timerange", symbol, **{"from": start, "to": until}
                )
                for trade in batch:
                    if str(trade["order_id"]) == str(report.venue_order_id):
                        by_id[_trade_id(trade)] = trade
                start = until
            rows = list(by_id.values())
        fills = [self._fill(row) for row in rows]
        if any(
            f.venue_order_id != report.venue_order_id or f.order_side != report.order_side
            for f in fills
        ):
            raise ValueError("Gate perpetual trades conflict with the order identity or side")
        if sum(f.last_qty.as_decimal() for f in fills) != report.filled_qty.as_decimal():
            raise ValueError("Gate perpetual execution history has not reached its order snapshot")
        return sorted(fills, key=lambda f: (f.ts_event, str(f.trade_id)))

    async def _resolve_close_order(self, row):
        # A close-all command can carry size=0, even though it actually traded
        # contracts. Use signed venue executions for a completed external close.
        if _decimal(row["size"]) or not row.get("is_close", row.get("close", False)):
            return row
        if row["status"] != "finished":
            return None
        trades = await self._pages("/my_trades", row["contract"], order=str(row["id"]))
        if any(str(trade["order_id"]) != str(row["id"]) for trade in trades):
            raise ValueError("Gate close executions belong to a different order")
        sizes = [_decimal(trade["size"]) for trade in trades]
        signed = sum(sizes)
        if not sizes and row.get("finish_as") != "filled":
            return None
        if not signed or any((size > 0) != (signed > 0) for size in sizes):
            raise ValueError("Gate close order awaits its signed executions")
        if row.get("finish_as") != "filled":
            raise ValueError(
                "Gate canceled close order has fills but its requested size is unknown"
            )
        return {**row, "size": format(signed, "f"), "left": "0", "is_reduce_only": True}

    async def _publish(self, row):
        row = await self._resolve_close_order(row)
        if row is None:
            return
        report = self._order(row)
        cached = self._cache.order(report.client_order_id) if report.client_order_id else None
        if cached and (
            cached.filled_qty > report.filled_qty or (cached.is_closed and report.is_open)
        ):
            return
        if report.filled_qty.as_decimal():
            fills = await self._order_fills(report, row["contract"])
            mass = self._mass()
            mass.add_order_reports([report])
            mass.add_fill_reports(fills)
            self._send_mass_status_report(mass)
        else:
            self._send_order_status_report(report)

    async def _publish_trade(self, row):
        fill = self._fill(row)
        cached = self._cache.order(fill.client_order_id) if fill.client_order_id else None
        if (
            cached is not None
            and cached.ts_accepted
            and cached.venue_order_id == fill.venue_order_id
        ):
            if fill.trade_id not in cached.trade_ids:
                self._send_fill_report(fill)
            return
        order_row = await self._get_order(str(fill.venue_order_id), row["contract"])
        order_row = await self._resolve_close_order(order_row)
        if order_row is None or self._order(order_row).filled_qty < fill.last_qty:
            raise RuntimeError("Gate perpetual order snapshot precedes the received execution")
        await self._publish(order_row)

    async def _message(self, message):
        channel = message["channel"]
        rows = message["result"]
        if not isinstance(rows, list):
            rows = [rows]
        async with self._report_lock:
            if channel == "futures.orders":
                for row in rows:
                    await self._publish(row)
            elif channel == "futures.usertrades":
                for row in rows:
                    await self._publish_trade(row)
            elif channel == "futures.positions":
                for row in rows:
                    self._send_position_status_report(
                        futures_position(
                            row,
                            self._from_contract(row["contract"]),
                            self.account_id,
                            self._clock.timestamp_ns(),
                        )
                    )
            elif channel == "futures.balances":
                await self._query_account(None)
            else:
                # Liquidations, ADLs and closes can change both margin and positions.
                await self._recover_state()

    async def _reconcile(self):
        async with self._report_lock:
            await self._recover_state()

    async def _recover_state(self):
        mass = await self.generate_mass_status()
        if mass is None:
            raise ValueError("Gate perpetual reconciliation incomplete")
        await self._query_account(None)
        self._send_mass_status_report(mass)

    async def _submit_order(self, command):
        await self._submit(command.order)

    async def _submit(self, order):  # noqa: C901 (distinguish queued, dispatched and acknowledged writes)
        if order.is_closed:
            return
        ids = {
            "strategy_id": order.strategy_id,
            "instrument_id": order.instrument_id,
            "client_order_id": order.client_order_id,
        }
        try:
            payload = futures_payload(order, self._instrument(order.instrument_id).raw_symbol.value)
            if not self.ws.is_ready():
                raise ValueError("Gate perpetual private stream is not ready")
        except ValueError as error:
            self.generate_order_denied(
                **ids, reason=str(error), ts_event=self._clock.timestamp_ns()
            )
            return
        completed = asyncio.get_running_loop().create_future()
        self._submitting[order.client_order_id] = completed
        target = payload["text"]
        dispatched = False
        acknowledged = False
        try:

            def dispatch():
                nonlocal dispatched
                if not self.ws.is_ready():
                    raise ValueError("Gate perpetual private stream unavailable before submit")
                self.generate_order_submitted(**ids, ts_event=self._clock.timestamp_ns())
                dispatched = True

            row = await self.http.request(
                "POST", FUTURES + "/orders", body=payload, private=True, before_send=dispatch
            )
            if row["contract"] != payload["contract"] or row.get("text") != payload["text"]:
                raise RuntimeError("Gate perpetual submit returned a different order")
            target = str(row["id"])
            acknowledged = True
            async with self._report_lock:
                await self._publish(row)
        except GateioHttpError as error:
            if error.definite_rejection and not acknowledged:
                target = None
                self.generate_order_rejected(
                    **ids,
                    reason=str(error),
                    ts_event=self._clock.timestamp_ns(),
                    due_post_only=error.label == "POC_FILL_IMMEDIATELY",
                )
            else:
                self._log.warning(f"Gate perpetual submit awaits reconciliation: {error}")
        except Exception as error:
            if not dispatched:
                target = None
                self.generate_order_denied(
                    **ids, reason=str(error), ts_event=self._clock.timestamp_ns()
                )
            else:
                self._log.warning(
                    f"Gate perpetual submit awaits reconciliation: {type(error).__name__}"
                )
        finally:
            completed.set_result(target)
            self._submitting.pop(order.client_order_id, None)

    async def _submit_order_list(self, command):
        orders = [order for order in command.order_list.orders if not order.is_closed]
        try:
            for order in orders:
                if (
                    order.contingency_type != ContingencyType.NO_CONTINGENCY
                    or order.parent_order_id
                    or order.linked_order_ids
                ):
                    raise ValueError("Gate perpetual lists require independent orders")
                if order.strategy_id != command.strategy_id:
                    raise ValueError("Gate perpetual list contains an order from another strategy")
                instrument = self._instrument(order.instrument_id)
                futures_payload(order, instrument.raw_symbol.value)
        except ValueError as error:
            for order in orders:
                self.generate_order_denied(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=str(error),
                    ts_event=self._clock.timestamp_ns(),
                )
            return
        for order in orders:
            await self._submit(order)

    async def _target(self, command):
        order = self._cache.order(command.client_order_id) if command.client_order_id else None
        if order and (
            order.instrument_id != command.instrument_id
            or order.account_id not in (self.account_id, None)
        ):
            raise ValueError(
                "Gate perpetual command conflicts with the cached account or instrument"
            )
        if (
            command.venue_order_id
            and order
            and order.venue_order_id
            and command.venue_order_id != order.venue_order_id
        ):
            raise ValueError("Gate perpetual command conflicts with the cached venue order ID")
        venue_id = command.venue_order_id or (order.venue_order_id if order else None)
        if venue_id:
            return str(venue_id)
        pending = self._submitting.get(command.client_order_id)
        if pending:
            return await asyncio.shield(pending)
        if command.client_order_id:
            return order_text(command.client_order_id)
        raise ValueError("Gate perpetual command requires an order ID")

    async def _amend_or_cancel(self, command, method, body=None):
        target = await self._target(command)
        if target is None:
            return
        try:
            row = await self.http.request(
                method, f"{FUTURES}/orders/{target}", body=body, private=True
            )
        except GateioHttpError as error:
            if error.definite_rejection and error.status != 404:
                rejected = (
                    self.generate_order_cancel_rejected
                    if method == "DELETE"
                    else self.generate_order_modify_rejected
                )
                rejected(
                    strategy_id=command.strategy_id,
                    instrument_id=command.instrument_id,
                    client_order_id=command.client_order_id,
                    venue_order_id=command.venue_order_id,
                    reason=str(error),
                    ts_event=self._clock.timestamp_ns(),
                )
            else:
                await self._query_order(command)
            return
        except (OSError, TimeoutError, ConnectionError, RuntimeError):
            await self._query_order(command)
            return
        if row["contract"] != self._instrument(command.instrument_id).raw_symbol.value or (
            str(row["id"]) != target and row.get("text") != target
        ):
            raise ValueError("Gate perpetual write acknowledgement identity mismatch")
        async with self._report_lock:
            await self._publish(row)

    async def _cancel_order(self, command):
        order = self._cache.order(command.client_order_id)
        if order is None or not order.is_closed:
            await self._amend_or_cancel(command, "DELETE")

    async def _modify_order(self, command):
        order = self._cache.order(command.client_order_id)
        try:
            if (
                command.trigger_price is not None
                or order is None
                or order.order_type != OrderType.LIMIT
            ):
                raise ValueError("Gate perpetual amendment requires a cached limit order")
            body = {}
            if command.quantity is not None:
                quantity = command.quantity.as_decimal()
                if quantity <= 0 or command.quantity < order.filled_qty:
                    raise ValueError("Gate perpetual amended quantity is below executed size")
                body["size"] = format(quantity if order.side == OrderSide.BUY else -quantity, "f")
            if command.price is not None:
                if command.price.as_decimal() <= 0:
                    raise ValueError("Gate perpetual amended price must be positive")
                body["price"] = str(command.price)
            if not body:
                raise ValueError("Gate perpetual amendment requires a quantity or price")
        except ValueError as error:
            self.generate_order_modify_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=command.venue_order_id,
                reason=str(error),
                ts_event=self._clock.timestamp_ns(),
            )
            return
        await self._amend_or_cancel(command, "PUT", body)

    async def _cancel_all_orders(self, command):
        orders = {
            o.client_order_id: o
            for o in (
                self._cache.orders_open(instrument_id=command.instrument_id)
                + self._cache.orders_inflight(instrument_id=command.instrument_id)
            )
            if o.account_id == self.account_id
        }
        for cid in tuple(self._submitting):
            order = self._cache.order(cid)
            if order and order.instrument_id == command.instrument_id:
                orders[cid] = order
        for order in orders.values():
            if order.strategy_id == command.strategy_id and command.order_side in (
                OrderSide.NO_ORDER_SIDE,
                order.side,
            ):
                await self._cancel_order(
                    CancelOrder(
                        trader_id=command.trader_id,
                        strategy_id=order.strategy_id,
                        instrument_id=order.instrument_id,
                        client_order_id=order.client_order_id,
                        venue_order_id=order.venue_order_id,
                        command_id=UUID4(),
                        ts_init=self._clock.timestamp_ns(),
                    )
                )

    async def _batch_cancel_orders(self, command):
        for cancel in command.cancels:
            await self._cancel_order(cancel)

    def _symbols(self, command):
        if command.instrument_id is not None:
            return [self._instrument(command.instrument_id).raw_symbol.value]
        return [inst.raw_symbol.value for inst in self._instrument_provider.get_all().values()]

    def _bounds(self, command):
        end = (
            int(command.end.timestamp())
            if command.end is not None
            else self._clock.timestamp_ns() // 1_000_000_000
        )
        start = (
            int(command.start.timestamp())
            if command.start is not None
            else max(0, end - self.config.reconciliation_lookback_mins * 60)
        )
        if start > end:
            raise ValueError("Gate perpetual history start exceeds end")
        return {"from": start, "to": end}

    async def _find_order(self, command):
        symbol = self._instrument(command.instrument_id).raw_symbol.value
        target = await self._target(command)
        if not target:
            return None
        try:
            return await self._get_order(target, symbol)
        except GateioHttpError as error:
            if error.status != 404:
                raise
        now = self._clock.timestamp_ns() // 1_000_000_000
        rows = await self._pages(
            "/orders_timerange",
            symbol,
            **{"from": max(0, now - self.config.reconciliation_lookback_mins * 60), "to": now},
        )
        matches = [row for row in rows if target in (str(row["id"]), row.get("text"))]
        if len(matches) > 1:
            raise ValueError("Gate perpetual order identity is ambiguous")
        return matches[0] if matches else None

    async def _query_order(self, command):
        row = await self._find_order(command)
        if row is not None:
            async with self._report_lock:
                await self._publish(row)

    async def _checked_status(self, row):
        row = await self._resolve_close_order(row)
        if row is None:
            return None
        report = self._order(row)
        cached = self._cache.order(report.client_order_id) if report.client_order_id else None
        if cached and (
            cached.filled_qty > report.filled_qty or (cached.is_closed and report.is_open)
        ):
            raise RuntimeError("Gate perpetual order snapshot predates cached execution")
        if report.filled_qty.as_decimal() and (
            cached is None or cached.filled_qty != report.filled_qty
        ):
            await self._publish(row)
            raise RuntimeError("Gate perpetual order awaits application of real executions")
        return report

    async def generate_order_status_report(self, command):
        row = await self._find_order(command)
        return await self._checked_status(row) if row else None

    async def _order_rows(self, symbol, open_only, bounds):
        rows = await self._pages("/orders", symbol, status="open")
        if not open_only:
            rows.extend(await self._pages("/orders_timerange", symbol, **bounds))
        return {str(row["id"]): row for row in rows}

    async def generate_order_status_reports(self, command):
        reports = []
        for symbol in self._symbols(command):
            rows = await self._order_rows(symbol, command.open_only, self._bounds(command))
            for row in rows.values():
                report = await self._checked_status(row)
                if report is not None:
                    reports.append(report)
        return reports

    async def generate_fill_reports(self, command):
        fills = []
        for symbol in self._symbols(command):
            rows = await self._pages("/my_trades_timerange", symbol, **self._bounds(command))
            fills.extend(
                self._fill(row)
                for row in rows
                if command.venue_order_id is None
                or str(row["order_id"]) == str(command.venue_order_id)
            )
        return sorted(fills, key=lambda f: (f.ts_event, str(f.trade_id)))

    async def generate_position_status_reports(self, command):
        reports = []
        symbols = self._symbols(command)
        rows = await self.http.request("GET", FUTURES + "/positions", private=True)
        for row in rows:
            if row["contract"] in symbols:
                reports.append(
                    futures_position(
                        row,
                        self._from_contract(row["contract"]),
                        self.account_id,
                        self._clock.timestamp_ns(),
                    )
                )
        # Gate may omit a flat contract. Include flat status for a requested instrument
        # or a cached position that went flat while the private stream was unavailable.
        reported = {r.instrument_id for r in reports}
        expected = (
            {command.instrument_id}
            if command.instrument_id
            else {
                position.instrument_id
                for position in self._cache.positions_open(venue=GATEIO)
                if position.account_id == self.account_id
                and position.instrument_id in self._instrument_provider.get_all()
            }
        )
        for instrument_id in expected - reported:
            instrument = self._instrument(instrument_id)
            reports.append(
                PositionStatusReport.create_flat(
                    self.account_id,
                    instrument.id,
                    instrument.size_precision,
                    self._clock.timestamp_ns(),
                )
            )
        start, end = getattr(command, "start", None), getattr(command, "end", None)
        return [
            report
            for report in reports
            if (start is None or report.ts_last >= int(start.timestamp() * 1_000_000_000))
            and (end is None or report.ts_last <= int(end.timestamp() * 1_000_000_000))
        ]

    async def generate_mass_status(self, lookback_mins=None):
        async with self._reconciling:
            return await self._collect_mass_status(lookback_mins)

    async def _collect_instrument(self, mass, symbol, bounds):
        rows = await self._order_rows(symbol, False, bounds)
        trades = await self._pages("/my_trades_timerange", symbol, **bounds)
        by_order = {}
        for trade in trades:
            oid = str(trade["order_id"])
            if oid not in rows:
                rows[oid] = await self._get_order(oid, symbol)
            by_order.setdefault(oid, []).append(self._fill(trade))
        for oid, row in rows.items():
            row = await self._resolve_close_order(row)
            if row is None:
                if by_order.get(oid):
                    raise ValueError("Gate close order awaits all executions")
                continue
            report = self._order(row)
            fills = by_order.get(oid, [])
            if any(fill.order_side != report.order_side for fill in fills):
                raise ValueError("Gate perpetual history contains conflicting order and fill sides")
            if fills and not report.filled_qty.as_decimal():
                raise ValueError(
                    "Gate perpetual history contains fills newer than its order snapshot"
                )
            if sum(f.last_qty.as_decimal() for f in fills) != report.filled_qty.as_decimal():
                fills = await self._order_fills(report, symbol)
            mass.add_order_reports([report])
            mass.add_fill_reports(fills)

    async def _collect_mass_status(self, lookback_mins):
        self.reconciliation_active = True
        try:
            mass = self._mass()
            minutes = (
                self.config.reconciliation_lookback_mins if lookback_mins is None else lookback_mins
            )
            end = self._clock.timestamp_ns() // 1_000_000_000
            bounds = {"from": max(0, end - minutes * 60), "to": end}
            positions = await self.generate_position_status_reports(
                SimpleNamespace(instrument_id=None)
            )
            mass.add_position_reports(positions)
            for instrument in self._instrument_provider.get_all().values():
                await self._collect_instrument(mass, instrument.raw_symbol.value, bounds)
            return mass
        except Exception as error:
            self._log.warning(f"Gate perpetual mass reconciliation incomplete: {error}")
            return None
        finally:
            self.reconciliation_active = False
