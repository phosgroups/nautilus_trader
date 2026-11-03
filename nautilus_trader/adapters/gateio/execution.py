"""Gate spot execution and reconciliation for PMM on NT v1."""

import asyncio
import hashlib
from decimal import Decimal

from nautilus_trader.adapters.gateio.config import GATEIO
from nautilus_trader.adapters.gateio.http import GateioHttpError
from nautilus_trader.adapters.gateio.parsing import event_ns
from nautilus_trader.adapters.gateio.parsing import fill_report
from nautilus_trader.adapters.gateio.parsing import order_report
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money


def order_text(client_order_id):
    # Gate accepts at most 28 custom bytes after "t-"; NT UUID IDs need mapping.
    return "t-" + hashlib.sha256(str(client_order_id).encode()).hexdigest()[:28]


def submit_payload(order):
    if order.order_type != OrderType.LIMIT or order.time_in_force != TimeInForce.GTC:
        raise ValueError("Gate PMM supports GTC limit orders only")
    if not order.is_post_only or order.is_quote_quantity or order.is_reduce_only:
        raise ValueError("Gate PMM requires post-only, base-sized spot orders")
    return {
        "currency_pair": order.instrument_id.symbol.value,
        "account": "spot",
        "type": "limit",
        "side": "buy" if order.side == OrderSide.BUY else "sell",
        "amount": str(order.quantity.as_decimal()),
        "price": str(order.price.as_decimal()),
        "time_in_force": "poc",
        "text": order_text(order.client_order_id),
    }


class GateioExecutionClient(LiveExecutionClient):
    def __init__(self, loop, client, msgbus, cache, clock, instrument_provider, config, name=None):
        super().__init__(
            loop=loop,
            client_id=ClientId(name or "GATEIO"),
            venue=GATEIO,
            oms_type=OmsType.HEDGING,
            account_type=AccountType.CASH,
            base_currency=None,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )
        self._set_account_id(AccountId(f"{name or 'GATEIO'}-master"))
        self.http, self.config = client, config
        self._client_ids = {}
        self._seen_fills = set()
        self._report_lock = asyncio.Lock()
        if config.account_refresh_interval_ms <= 0:
            raise ValueError("Gate account refresh interval must be positive")
        self._account_refresh_task = None
        self._account_read_task = None
        self._account_dirty = False
        self._account_paused = True
        self._account_stopping = False
        self._account_generation = 0
        self._account_last_request = float("-inf")
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
        )

    async def _connect(self):
        self._account_stopping = False
        await self._instrument_provider.initialize()
        if not self._instrument_provider.get_all():
            raise ValueError("Configure Gate instrument_provider.load_ids before connecting")
        for instrument in self._instrument_provider.get_all().values():
            self._cache.add_instrument(instrument)
        await self._update_account()
        await self._await_account_registered()
        try:
            await self.ws.connect()
            symbols = [i.symbol.value for i in self._instrument_provider.get_all()]
            await self.ws.subscribe("spot.orders", symbols)
            await self.ws.subscribe("spot.usertrades", symbols)
            await self.ws.subscribe("spot.balances")
            await self._reconcile()
        except BaseException:
            self._account_paused = True
            await self.ws.disconnect()
            await self._cancel_account_tasks()
            raise

    async def _disconnect(self):
        self._account_stopping = True
        self._account_paused = True
        try:
            await self.ws.disconnect()
        finally:
            await self._cancel_account_tasks()
        # Order shutdown has already drained. Keep the final Cache balances
        # authoritative even if the last coalesced refresh was still pending.
        try:
            async with asyncio.timeout(min(self.config.http_timeout_secs, 5)):
                await self._update_account()
        except Exception as error:
            self._log.warning(f"Gate final account refresh failed: {type(error).__name__}")
        finally:
            # The shielded HTTP read also needs cancellation if the bounded
            # final refresh timed out. No task may publish after disconnect.
            await self._cancel_account_tasks()
            self._account_paused = True
            self._account_dirty = False

    async def _cancel_account_tasks(self):
        # Responses from the previous connection must never overwrite recovery.
        self._account_generation += 1
        tasks = [
            task
            for task in (self._account_refresh_task, self._account_read_task)
            if task is not None and task is not asyncio.current_task()
        ]
        self._account_refresh_task = self._account_read_task = None
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _instrument(self, symbol):
        instrument = next(
            (
                i
                for i in self._instrument_provider.get_all().values()
                if i.raw_symbol.value == symbol
            ),
            None,
        )
        if instrument is None:
            raise ValueError("Gate report instrument is not configured")
        return instrument

    def _resolve(self, row):
        result = self._client_ids.get(row.get("text"))
        if result is not None:
            return result
        venue_id = str(row.get("order_id", row.get("id", "")))
        for order in self._cache.orders(venue=GATEIO):
            if str(order.venue_order_id) == venue_id or order_text(
                order.client_order_id
            ) == row.get("text"):
                return order.client_order_id
        return None  # External orders must not be claimed by PMM.

    async def _update_account(self):
        # Explicit queries and the background worker share one in-flight read.
        # Shielding lets one canceled waiter leave other account queries intact.
        task = self._account_read_task
        if task is None or task.done():
            task = self.create_task(self._read_account(self._account_generation))
            self._account_read_task = task
        await asyncio.shield(task)

    async def _read_account(self, generation):
        self._account_last_request = self._loop.time()
        rows = await self.http.request("GET", "/spot/accounts", private=True)
        if generation == self._account_generation:
            self._publish_account(rows)

    def _request_account_refresh(self):
        if self._account_stopping:
            return
        self._account_dirty = True
        if self._account_paused:
            return
        task = self._account_refresh_task
        if task is None or task.done():
            self._account_refresh_task = self.create_task(self._refresh_account())

    async def _refresh_account(self):
        failures = 0
        try:
            while self._account_dirty and not self._account_stopping and not self._account_paused:
                interval = self.config.account_refresh_interval_ms / 1000
                await asyncio.sleep(
                    max(0, self._account_last_request + interval - self._loop.time())
                )
                # Fold notifications during the delay into this read. Updates
                # arriving during the read remain dirty and get a follow-up.
                self._account_dirty = False
                try:
                    await self._update_account()
                    failures = 0
                except Exception as error:
                    self._account_dirty = True
                    failures += 1
                    self._log.warning(
                        f"Gate account refresh failed; retrying: {type(error).__name__}"
                    )
                    await asyncio.sleep(min(0.25 * 2 ** min(failures - 1, 3), 2))
        finally:
            if self._account_refresh_task is asyncio.current_task():
                self._account_refresh_task = None

    def _publish_account(self, rows):
        # Gate omits currencies with zero balances. Publish explicit zeroes
        # for configured instruments and previously observed currencies so NT
        # does not treat an empty wallet as unready or retain a spent balance.
        rows = list(rows)
        currencies = {
            currency.code
            for instrument in self._instrument_provider.get_all().values()
            for currency in (instrument.base_currency, instrument.quote_currency)
        }
        account = self._cache.account(self.account_id)
        if account is not None:
            currencies.update(currency.code for currency in account.balances())
        currencies.difference_update(row["currency"] for row in rows)
        rows.extend(
            {"currency": code, "available": "0", "locked": "0"} for code in sorted(currencies)
        )
        balances = []
        for row in rows:
            currency = Currency.from_str(row["currency"])
            free, locked = Decimal(row["available"]), Decimal(row["locked"])
            if min(free, locked) < 0 or not free.is_finite() or not locked.is_finite():
                raise ValueError("Invalid Gate account balance")
            balances.append(
                AccountBalance(
                    Money.from_decimal(free + locked, currency),
                    Money.from_decimal(locked, currency),
                    Money.from_decimal(free, currency),
                )
            )
        if not balances:
            raise ValueError("Gate returned no spot balances")
        self.generate_account_state(
            balances=balances, margins=[], reported=True, ts_event=self._clock.timestamp_ns()
        )

    async def _publish_fills(self, rows):
        for row in sorted(rows, key=lambda r: Decimal(str(r.get("create_time_ms", "0")))):
            key = (row["currency_pair"], str(row["id"]))
            if key not in self._seen_fills:
                self._send_fill_report(
                    fill_report(
                        row,
                        self._instrument(row["currency_pair"]),
                        self.account_id,
                        self._resolve(row),
                        self._clock.timestamp_ns(),
                    )
                )
                self._seen_fills.add(key)

    async def _publish_order(self, row):
        instrument = self._instrument(row["currency_pair"])
        report = order_report(
            row, instrument, self.account_id, self._resolve(row), self._clock.timestamp_ns()
        )
        if not await self._load_order_fills(row, report):
            self._send_order_status_report(report)
        return report

    def _publish_facts(self, reports, rows):
        # Reconcile status and executions atomically, as the Bitget adapter
        # does. Separate reports can otherwise create inferred zero-fee fills
        # when an external order is first discovered already partially filled.
        mass = ExecutionMassStatus(
            client_id=self.id,
            account_id=self.account_id,
            venue=GATEIO,
            report_id=UUID4(),
            ts_init=self._clock.timestamp_ns(),
        )
        mass.add_order_reports(reports)
        unique = {(r["currency_pair"], str(r["id"])): r for r in rows}
        mass.add_fill_reports(
            [
                fill_report(
                    row,
                    self._instrument(row["currency_pair"]),
                    self.account_id,
                    self._resolve(row),
                    self._clock.timestamp_ns(),
                )
                for row in sorted(unique.values(), key=lambda r: event_ns(r, "create_time", 0))
            ]
        )
        self._send_mass_status_report(mass)
        self._seen_fills.update(unique)

    async def _load_order_fills(self, row, report):
        if report.filled_qty.as_decimal() > 0:
            fills = await self.http.pages(
                "/spot/my_trades",
                {
                    "currency_pair": row["currency_pair"],
                    "order_id": row["id"],
                },
            )
            # Do not let aggregate order reports invent trades with unknown fees.
            known = sum((Decimal(r["amount"]) for r in fills), Decimal(0))
            if known < report.filled_qty.as_decimal():
                raise RuntimeError("Gate filled quantity is not explained by trade records")
            self._publish_facts([report], fills)
            return True
        return False

    async def _message(self, message):
        channel, rows = message["channel"], message["result"]
        if channel == "spot.balances":
            # Balance pushes are partial currency updates. Like the Bitget
            # account path, retain full REST reconciliation, but never wait for
            # it in the ordered order/fill consumer (or under its report lock).
            self._request_account_refresh()
            return
        async with self._report_lock:
            if channel == "spot.orders":
                for row in rows:
                    await self._publish_order(row)
            elif channel == "spot.usertrades":
                await self._publish_fills(rows)
        if channel in {"spot.orders", "spot.usertrades"}:
            self._request_account_refresh()

    async def _reconcile(self):
        self._account_paused = True
        self.ws.ready = False
        await self._cancel_account_tasks()
        async with self._report_lock:
            now = self._clock.timestamp_ns() // 1_000_000_000
            since = now - self.config.reconciliation_lookback_mins * 60
            fills, reports = [], []
            for instrument in self._instrument_provider.get_all().values():
                symbol = instrument.raw_symbol.value
                fills.extend(
                    await self.http.pages(
                        "/spot/my_trades", {"currency_pair": symbol, "from": since}
                    )
                )
                for status in ("open", "finished"):
                    for row in await self.http.pages(
                        "/spot/orders",
                        {
                            "currency_pair": symbol,
                            "status": status,
                            **({"from": since} if status == "finished" else {}),
                        },
                    ):
                        report = order_report(
                            row,
                            instrument,
                            self.account_id,
                            self._resolve(row),
                            self._clock.timestamp_ns(),
                        )
                        if report.filled_qty.as_decimal() > 0:
                            trades = await self.http.pages(
                                "/spot/my_trades", {"currency_pair": symbol, "order_id": row["id"]}
                            )
                            known = sum((Decimal(r["amount"]) for r in trades), Decimal(0))
                            if known < report.filled_qty.as_decimal():
                                raise RuntimeError(
                                    "Gate reconciliation trade history is incomplete"
                                )
                            fills.extend(trades)
                        reports.append(report)
            balances = await self.http.request("GET", "/spot/accounts", private=True)
            # Recovery facts are collected before publishing any callbacks. New
            # PMM replacements triggered by those callbacks can use the restored
            # private stream, instead of receiving a spurious reconnect denial.
            self._publish_facts(reports, fills)
            self._publish_account(balances)
            self.ws.ready = True
        self._account_paused = False
        if self._account_dirty:
            self._request_account_refresh()

    async def _submit_order(self, command):
        order = command.order
        if order.is_closed:
            return
        try:
            payload = submit_payload(order)
            if not self.ws.is_ready():
                raise ValueError("Gate private stream is not ready")
        except ValueError as error:
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=str(error),
                ts_event=self._clock.timestamp_ns(),
            )
            return
        self._client_ids[payload["text"]] = order.client_order_id
        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )
        try:
            row = await self.http.request("POST", "/spot/orders", body=payload, private=True)
            async with self._report_lock:
                await self._publish_order(row)
            self._request_account_refresh()
        except GateioHttpError as error:
            if error.definite_rejection:
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=str(error),
                    ts_event=self._clock.timestamp_ns(),
                )
            else:
                self._log.warning("Gate submission outcome unknown; query before any replacement")
        except Exception:
            self._log.warning("Gate submission outcome unknown; request is not retried")

    async def _cancel_order(self, command):
        order = self._cache.order(command.client_order_id)
        if order is not None and order.is_closed:
            return
        venue_id = command.venue_order_id or (order.venue_order_id if order is not None else None)
        target = str(venue_id) if venue_id is not None else order_text(command.client_order_id)
        try:
            row = await self.http.request(
                "DELETE",
                "/spot/orders/" + target,
                {"currency_pair": command.instrument_id.symbol.value},
                private=True,
            )
            async with self._report_lock:
                await self._publish_order(row)
            self._request_account_refresh()
        except GateioHttpError as error:
            if error.definite_rejection:
                # Cancellation can queue behind rate-limited requests while a
                # private event or another cancellation closes the order.
                current = self._cache.order(command.client_order_id)
                if current is not None and current.is_closed:
                    return
                if error.status == 404 and error.label == "ORDER_NOT_FOUND":
                    try:
                        row = await self.http.request(
                            "GET",
                            "/spot/orders/" + target,
                            {"currency_pair": command.instrument_id.symbol.value},
                            private=True,
                        )
                        async with self._report_lock:
                            report = await self._publish_order(row)
                        self._request_account_refresh()
                        if report.order_status in {
                            OrderStatus.CANCELED,
                            OrderStatus.FILLED,
                            OrderStatus.REJECTED,
                            OrderStatus.EXPIRED,
                        }:
                            return
                    except Exception:
                        self._log.warning("Gate cancel target could not be reconciled")
                # A missing or still-open target is not cancellation proof.
                self.generate_order_cancel_rejected(
                    strategy_id=command.strategy_id,
                    instrument_id=command.instrument_id,
                    client_order_id=command.client_order_id,
                    venue_order_id=venue_id,
                    reason=str(error),
                    ts_event=self._clock.timestamp_ns(),
                )
            else:
                self._log.warning("Gate cancel outcome unknown; order remains unresolved")
        except Exception:
            self._log.warning("Gate cancel outcome unknown; query before any replacement")

    async def _cancel_all_orders(self, command):
        # Never issue venue-wide cancel-all: another program may share this account.
        for order in self._cache.orders_open(instrument_id=command.instrument_id):
            if order.strategy_id != command.strategy_id:
                continue
            if command.order_side not in (OrderSide.NO_ORDER_SIDE, order.side):
                continue
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

    async def _modify_order(self, command):
        self.generate_order_modify_rejected(
            strategy_id=command.strategy_id,
            instrument_id=command.instrument_id,
            client_order_id=command.client_order_id,
            venue_order_id=command.venue_order_id,
            reason="Gate PMM uses cancel-confirm-create, not in-place modification",
            ts_event=self._clock.timestamp_ns(),
        )

    async def _query_account(self, command):
        if self._account_paused or self._account_stopping:
            raise RuntimeError("Gate account reconciliation is not ready")
        await self._update_account()

    async def generate_order_status_report(self, command):
        target = (
            str(command.venue_order_id)
            if command.venue_order_id
            else order_text(command.client_order_id)
        )
        try:
            row = await self.http.request(
                "GET",
                "/spot/orders/" + target,
                {"currency_pair": command.instrument_id.symbol.value},
                private=True,
            )
        except GateioHttpError as error:
            if error.status == 404 and error.label == "ORDER_NOT_FOUND":
                if command.venue_order_id is not None:
                    return None
                # Gate custom text lookup covers open orders only. A timed-out
                # submit may already have filled or closed; inspect history
                # before treating a custom-ID 404 as absence.
                since = (
                    self._clock.timestamp_ns() // 1_000_000_000
                    - self.config.reconciliation_lookback_mins * 60
                )
                rows = await self.http.pages(
                    "/spot/orders",
                    {
                        "currency_pair": command.instrument_id.symbol.value,
                        "status": "finished",
                        "from": since,
                    },
                )
                row = next((r for r in rows if r.get("text") == target), None)
                if row is None:
                    return None
            else:
                raise
        report = order_report(
            row,
            self._instrument(row["currency_pair"]),
            self.account_id,
            self._resolve(row),
            self._clock.timestamp_ns(),
        )
        async with self._report_lock:
            await self._load_order_fills(row, report)
        return report

    def _symbols(self, command):
        instrument_id = getattr(command, "instrument_id", None)
        if instrument_id is not None:
            return [self._instrument(instrument_id.symbol.value)]
        return list(self._instrument_provider.get_all().values())

    async def generate_order_status_reports(self, command):
        reports = []
        for instrument in self._symbols(command):
            for status in ("open",) if command.open_only else ("open", "finished"):
                params = {"currency_pair": instrument.raw_symbol.value, "status": status}
                if command.start is not None:
                    params["from"] = int(command.start.timestamp())
                if command.end is not None:
                    params["to"] = int(command.end.timestamp())
                for row in await self.http.pages("/spot/orders", params):
                    reports.append(
                        order_report(
                            row,
                            instrument,
                            self.account_id,
                            self._resolve(row),
                            self._clock.timestamp_ns(),
                        )
                    )
        return reports

    async def generate_fill_reports(self, command):
        reports = []
        for instrument in self._symbols(command):
            params = {
                "currency_pair": instrument.raw_symbol.value,
                "order_id": str(command.venue_order_id) if command.venue_order_id else None,
            }
            if command.start is not None:
                params["from"] = int(command.start.timestamp())
            if command.end is not None:
                params["to"] = int(command.end.timestamp())
            for row in await self.http.pages("/spot/my_trades", params):
                reports.append(
                    fill_report(
                        row,
                        instrument,
                        self.account_id,
                        self._resolve(row),
                        self._clock.timestamp_ns(),
                    )
                )
        return reports

    async def generate_position_status_reports(self, command):
        return []  # Cash spot has balances, not derivative positions.
