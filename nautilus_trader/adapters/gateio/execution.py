"""Gate spot execution and reconciliation for PMM on NT v1."""

import asyncio
import base64
import hashlib
import math
import re
from collections import OrderedDict
from contextlib import suppress
from decimal import Decimal
from uuid import UUID

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
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money


def order_text(client_order_id):
    # Preserve the complete UUID in 22 URL-safe bytes, including across restarts.
    value = str(client_order_id)
    try:
        parsed = UUID(value)
        if str(parsed) == value:
            return "t-ntU" + base64.urlsafe_b64encode(parsed.bytes).decode().rstrip("=")
    except ValueError:
        pass
    if re.fullmatch(r"[A-Za-z0-9_.-]{1,25}", value):
        return "t-ntS" + value
    raise ValueError(
        "Gate requires a canonical UUID or an ASCII client order ID of at most 25 bytes"
    )


def original_order_id(text):
    if not isinstance(text, str):
        return None
    try:
        if text.startswith("t-ntU") and len(text) == 27:
            value = str(
                UUID(bytes=base64.b64decode(text[5:] + "==", altchars=b"-_", validate=True))
            )
        elif text.startswith("t-ntS"):
            value = text[5:]
        else:
            return None
        # Validate protocol text before entering native identifier constructors.
        return ClientOrderId(value) if order_text(value) == text else None
    except (ValueError, TypeError):
        return None


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


class _SubmissionDeferred(ValueError):
    """The request has not been sent; wait for private execution readiness again."""


def _check_order_response(row, instrument_id, venue_order_id=None, client_order_id=None):
    if row["currency_pair"] != instrument_id.symbol.value or (
        venue_order_id is not None and str(row["id"]) != str(venue_order_id)
    ):
        raise ValueError("Gate response does not match the requested instrument/order")
    if venue_order_id is None and client_order_id is not None and (
        row.get("text") != order_text(client_order_id)
    ):
        raise ValueError("Gate response does not match the requested client order")


def _order_not_found(error):
    return error.status == 404 and error.label in {"ORDER_NOT_FOUND", "CLIENT_ID_NOT_FOUND"}


def _cached_query_venue_id(client, command, venue_id):
    cache = getattr(client, "_cache", None)
    cached = cache.order(command.client_order_id) if cache and command.client_order_id else None
    if cached is None:
        return None
    if cached.instrument_id != command.instrument_id or cached.account_id not in (
        None,
        client.account_id,
    ):
        raise ValueError("Gate query conflicts with the cached account/instrument")
    if venue_id is not None and cached.venue_order_id not in (None, venue_id):
        raise ValueError("Gate query conflicts with the cached venue order identity")
    return cached.venue_order_id


async def _find_finished_order(client, command, venue_id):
    # Custom IDs do not reliably identify finished spot orders. An empty
    # history response is still unknown (history can lag), not canceled.
    since = (
        client._clock.timestamp_ns() // 1_000_000_000
        - client.config.reconciliation_lookback_mins * 60
    )
    rows = await client.http.pages(
        "/spot/orders",
        {
            "currency_pair": command.instrument_id.symbol.value,
            "status": "finished",
            "from": since,
        },
    )
    matches = [
        item
        for item in rows
        if (
            str(item["id"]) == str(venue_id)
            if venue_id is not None
            else item.get("text") == order_text(command.client_order_id)
        )
    ]
    if len({str(item["id"]) for item in matches}) > 1:
        raise ValueError("Gate history has ambiguous client order identity")
    return matches[0] if matches else None


async def _fetch_order(client, command):
    """Resolve a lookup miss using identity and history, never a synthetic terminal event."""
    cached_id = _cached_query_venue_id(client, command, command.venue_order_id)
    venue_id = command.venue_order_id or cached_id
    if venue_id is None and command.client_order_id in getattr(client, "_waiting_submissions", {}):
        # This POST has not crossed the transport boundary. REST queries cannot
        # resolve it and would starve queued submissions of their rate budget.
        return None
    if venue_id is None and command.client_order_id is None:
        raise ValueError("Gate order query requires a client or venue order ID")
    target = str(venue_id) if venue_id is not None else order_text(command.client_order_id)
    row = None
    try:
        row = await client.http.request(
            "GET",
            "/spot/orders/" + target,
            {"currency_pair": command.instrument_id.symbol.value},
            private=True,
        )
    except GateioHttpError as error:
        if not _order_not_found(error):
            raise
        # Acceptance can arrive while the custom-ID request is in flight.
        known_id = _cached_query_venue_id(client, command, venue_id)
        if known_id is not None and str(known_id) != target:
            venue_id = known_id
            try:
                row = await client.http.request(
                    "GET",
                    "/spot/orders/" + str(venue_id),
                    {"currency_pair": command.instrument_id.symbol.value},
                    private=True,
                )
            except GateioHttpError as retry_error:
                if not _order_not_found(retry_error):
                    raise
        if row is None:
            row = await _find_finished_order(client, command, venue_id)
    if row is not None:
        _check_order_response(row, command.instrument_id, venue_id, command.client_order_id)
    return row


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
        self._seen_fills = OrderedDict()
        self._report_lock = asyncio.Lock()
        self._recovery_lock = asyncio.Lock()
        if config.account_refresh_interval_ms <= 0:
            raise ValueError("Gate account refresh interval must be positive")
        if (
            config.account_stale_after_ms <= config.account_refresh_interval_ms
            or config.submission_ready_timeout_secs <= 0
            or not math.isfinite(config.submission_ready_timeout_secs)
        ):
            raise ValueError(
                "Gate account stale limit must exceed the refresh interval and submission timeout must be positive"
            )
        self._account_refresh_task = None
        self._account_read_task = None
        self._account_dirty = False
        self._account_paused = True
        self._account_stopping = False
        self._account_generation = 0
        self._account_last_request = float("-inf")
        self._account_last_success = float("-inf")
        self._account_requested = asyncio.Event()
        self._pending_orders = {}
        self._pending_fills = {}
        self._execution_recovery_task = None
        self._execution_generation = 0
        self._waiting_submissions = {}
        self._inflight_submissions = {}
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
        try:
            await self._instrument_provider.initialize()
            if not self._instrument_provider.get_all():
                raise ValueError("Configure Gate instrument_provider.load_ids before connecting")
            for instrument in self._instrument_provider.get_all().values():
                self._cache.add_instrument(instrument)
            await self._update_account()
            await self._await_account_registered()
            await self.ws.connect()
            self.ws.ready = False
            self.ws._resume.clear()
            symbols = [i.symbol.value for i in self._instrument_provider.get_all()]
            for start in range(0, len(symbols), 50):
                await self.ws.subscribe("spot.orders", symbols[start : start + 50])
                await self.ws.subscribe("spot.usertrades", symbols[start : start + 50])
            await self.ws.subscribe("spot.balances")
            await self._reconcile()
            if self.ws.recovery is not None and not self.ws.recovery.done():
                await self.ws.recovery
            else:
                self.ws.ready = True
                self.ws._resume.set()
        except BaseException:
            self._account_stopping = True
            self._account_paused = True
            try:
                await self.ws.disconnect()
            finally:
                await self._cancel_account_tasks()
                await self._cancel_execution_recovery()
            raise

    async def _disconnect(self):
        self._account_stopping = True
        self._account_paused = True
        try:
            await self.ws.disconnect()
        finally:
            await self._cancel_account_tasks()
            await self._cancel_execution_recovery()
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
        decoded = original_order_id(row.get("text"))
        venue_id = str(row.get("order_id", row.get("id", "")))
        cached_id = None
        if venue_id:
            cached_id = self._cache.client_order_id(VenueOrderId(venue_id))
        if decoded is not None and cached_id is not None and decoded != cached_id:
            raise ValueError("Gate client and venue order identities conflict")
        if decoded is not None or cached_id is not None:
            return self._checked_identity(decoded or cached_id, row, venue_id)
        if not re.fullmatch(r"t-[0-9a-f]{28}", row.get("text") or ""):
            return None
        for order in self._cache.orders(venue=GATEIO):
            # Retain recovery of pre-upgrade hashes when the NT Cache is restored.
            legacy = "t-" + hashlib.sha256(str(order.client_order_id).encode()).hexdigest()[:28]
            if str(order.venue_order_id) == venue_id or legacy == row.get("text"):
                return self._checked_identity(order.client_order_id, row, venue_id)
        # Recover identity only. NT's external-order ownership rules still apply.
        return None

    def _checked_identity(self, client_order_id, row, venue_id):
        cached = self._cache.order(client_order_id)
        if cached is not None and (
            cached.instrument_id.venue != GATEIO
            or cached.instrument_id.symbol.value != row["currency_pair"]
            or cached.account_id not in (None, self.account_id)
            or (cached.venue_order_id is not None and str(cached.venue_order_id) != venue_id)
        ):
            raise ValueError("Gate report conflicts with the cached account/instrument/order")
        return client_order_id

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
            self._account_last_success = self._loop.time()

    def _request_account_refresh(self):
        if self._account_stopping:
            return
        self._account_dirty = True
        self._account_requested.set()
        if self._account_paused:
            return
        task = self._account_refresh_task
        if task is None or task.done():
            self._account_refresh_task = self.create_task(self._refresh_account())

    async def _refresh_account(self):
        failures = 0
        try:
            while not self._account_stopping and not self._account_paused:
                if not self._account_dirty:
                    # Refresh quiet accounts before their state expires.
                    with suppress(TimeoutError):
                        await asyncio.wait_for(
                            self._account_requested.wait(),
                            self.config.account_stale_after_ms / 2000,
                        )
                interval = self.config.account_refresh_interval_ms / 1000
                await asyncio.sleep(
                    max(0, self._account_last_request + interval - self._loop.time())
                )
                # Fold notifications during the delay into this read. Updates
                # arriving during the read remain dirty and get a follow-up.
                self._account_dirty = False
                self._account_requested.clear()
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
        for row in sorted(rows, key=lambda r: event_ns(r, "create_time", 0)):
            key = (row["currency_pair"], str(row["id"]))
            cached = self._cached_order(row)
            if cached is not None and str(row["id"]) in {str(t) for t in cached.trade_ids}:
                self._remember_fill(key)
                continue
            if key not in self._seen_fills:
                # Validate fees/assets before buffering. Unknown orders need a
                # matching status; sending the fill first can make NT drop it.
                parsed = fill_report(
                    row,
                    self._instrument(row["currency_pair"]),
                    self.account_id,
                    self._resolve(row),
                    self._clock.timestamp_ns(),
                )
                order_key = (row["currency_pair"], str(row["order_id"]))
                self._pending_fills.setdefault(order_key, {})[str(row["id"])] = row
                cached = self._cached_order(row)
                if cached is not None and order_key not in self._pending_orders:
                    self._send_fill_report(parsed)
                    self._remember_fill(key)
                    self._pending_fills[order_key].pop(str(row["id"]), None)
                    if not self._pending_fills[order_key]:
                        self._pending_fills.pop(order_key, None)
                else:
                    self._flush_order(order_key)
        if self._pending_orders or self._pending_fills:
            self._schedule_execution_recovery()

    def _remember_fill(self, key):
        # Cache trade IDs remain authoritative after this bounded fast path evicts.
        self._seen_fills[key] = None
        self._seen_fills.move_to_end(key)
        if len(self._seen_fills) > 100000:
            self._seen_fills.popitem(last=False)

    def _cached_order(self, row):
        cid = self._resolve(row)
        if cid is None:
            oid = str(row.get("order_id", row.get("id", "")))
            cid = self._cache.client_order_id(VenueOrderId(oid))
        return self._cache.order(cid) if cid is not None else None

    @staticmethod
    def _duplicate_pending_acceptance(cached, report):
        # REST and WS may acknowledge the same submit after a local cancel.
        # Replaying it into NT would overwrite PENDING_CANCEL and emit another
        # OrderAccepted. Preserve first acknowledgements and genuine amendments.
        return (
            report.order_status == OrderStatus.ACCEPTED
            and cached is not None
            and not cached.is_closed
            and cached.status == OrderStatus.PENDING_CANCEL
            and cached.ts_accepted > 0
            and cached.venue_order_id == report.venue_order_id
            and cached.quantity == report.quantity
            and cached.price == report.price
        )

    async def _publish_order(self, row):
        instrument = self._instrument(row["currency_pair"])
        report = order_report(
            row, instrument, self.account_id, self._resolve(row), self._clock.timestamp_ns()
        )
        cached = self._cached_order(row)
        if self._duplicate_pending_acceptance(cached, report):
            return report
        if cached is not None and (
            cached.filled_qty.as_decimal() > report.filled_qty.as_decimal()
            or (
                cached.is_closed
                and report.order_status in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}
            )
        ):
            return report
        pending = self._pending_orders.get((row["currency_pair"], str(row["id"])))
        if pending is not None and (
            pending[1].ts_last > report.ts_last or pending[1].filled_qty > report.filled_qty
        ):
            return report
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
        current, stale_open = [], set()
        for report in reports:
            cached = self._cached_order(
                {
                    "currency_pair": report.instrument_id.symbol.value,
                    "id": str(report.venue_order_id),
                }
            )
            if (
                cached is not None
                and (
                    (cached.is_closed and report.order_status in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED})
                    or self._duplicate_pending_acceptance(cached, report)
                )
            ):
                stale_open.add((report.instrument_id.symbol.value, str(report.venue_order_id)))
            else:
                current.append(report)
        mass.add_order_reports(current)
        unique = {(r["currency_pair"], str(r["id"])): r for r in rows}
        late_fills = [
            row
            for row in unique.values()
            if (row["currency_pair"], str(row["order_id"])) in stale_open
        ]
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
                if (row["currency_pair"], str(row["order_id"])) not in stale_open
            ]
        )
        self._send_mass_status_report(mass)
        # A cancel response may close the Cache order while REST recovery is
        # in flight. Preserve real late fills without replaying stale ACCEPTED.
        for row in late_fills:
            if (row["currency_pair"], str(row["id"])) not in self._seen_fills:
                self._send_fill_report(
                    fill_report(
                        row,
                        self._instrument(row["currency_pair"]),
                        self.account_id,
                        self._resolve(row),
                        self._clock.timestamp_ns(),
                    )
                )
        for key in unique:
            self._remember_fill(key)

    async def _load_order_fills(self, row, report):
        if report.filled_qty.as_decimal() == 0:
            return False
        key = (row["currency_pair"], str(row["id"]))
        previous = self._pending_orders.get(key)
        if previous is None or report.ts_last >= previous[1].ts_last:
            self._pending_orders[key] = (dict(row), report)
        if not self._flush_order(key):
            self._schedule_execution_recovery()
        return True

    def _flush_order(self, key):
        pending = self._pending_orders.get(key)
        if pending is None:
            return False
        row, report = pending
        cached = self._cached_order(row)
        known = cached.filled_qty.as_decimal() if cached is not None else Decimal(0)
        known_ids = {str(t) for t in cached.trade_ids} if cached is not None else set()
        fills = [r for tid, r in self._pending_fills.get(key, {}).items() if tid not in known_ids]
        available = known + sum((Decimal(r["amount"]) for r in fills), Decimal(0))
        target = report.filled_qty.as_decimal()
        if known > target:
            self._pending_orders.pop(key, None)  # Ignore a status older than cached executions.
            return True
        if available != target:
            return False  # Wait for real fills/status; never infer trades or fees.
        if (
            cached is not None
            and cached.is_closed
            and report.order_status in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}
        ):
            self._pending_orders.pop(key, None)
            self._pending_fills.pop(key, None)
            return True
        if fills:
            self._publish_facts([report], fills)
        else:
            self._send_order_status_report(report)
        self._pending_orders.pop(key, None)
        self._pending_fills.pop(key, None)
        return True

    def _schedule_execution_recovery(self):
        if self._account_stopping or self._account_paused:
            return
        task = self._execution_recovery_task
        if task is None or task.done():
            self._execution_recovery_task = self.create_task(self._recover_executions())

    async def _cancel_execution_recovery(self):
        self._execution_generation += 1
        task, self._execution_recovery_task = self._execution_recovery_task, None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _recover_executions(self):
        generation = self._execution_generation
        failures = 0
        try:
            while (
                (self._pending_orders or self._pending_fills)
                and not self._account_stopping
                and not self._account_paused
            ):
                for key in list(self._pending_orders.keys() | self._pending_fills.keys()):
                    try:
                        await self._recover_execution_order(key, generation)
                        failures = 0
                    except Exception as error:
                        failures += 1
                        detail = str(error) if isinstance(error, GateioHttpError) else type(error).__name__
                        self._log.warning(
                            f"Gate execution recovery deferred: {detail}"
                        )
                if self._pending_orders or self._pending_fills:
                    await asyncio.sleep(min(0.25 * 2 ** min(failures, 3), 2))
        finally:
            if self._execution_recovery_task is asyncio.current_task():
                self._execution_recovery_task = None

    async def _recover_execution_order(self, key, generation):
        async with self._report_lock:
            if self._flush_order(key):
                return
        # No network operation holds the order/fill report lock.
        row = await self.http.request(
            "GET", "/spot/orders/" + key[1], {"currency_pair": key[0]}, private=True
        )
        if generation != self._execution_generation:
            return
        async with self._report_lock:
            await self._publish_order(row)
            if key not in self._pending_orders and key not in self._pending_fills:
                return
        fills = await self.http.pages(
            "/spot/my_trades", {"currency_pair": key[0], "order_id": key[1]}
        )
        if generation != self._execution_generation:
            return
        # Validate every execution before publishing any of the batch.
        for fill in fills:
            if fill["currency_pair"] != key[0] or str(fill["order_id"]) != key[1]:
                raise ValueError("Gate returned executions for another order")
            fill_report(
                fill,
                self._instrument(key[0]),
                self.account_id,
                self._resolve(fill),
                self._clock.timestamp_ns(),
            )
        async with self._report_lock:
            buffered = self._pending_fills.setdefault(key, {})
            buffered.update((str(fill["id"]), fill) for fill in fills)
            self._flush_order(key)

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
        async with self._recovery_lock:
            generation = self.ws._generation
            self._account_paused = True
            self.ws.ready = False
            await self._cancel_account_tasks()
            await self._cancel_execution_recovery()
            reports, fills, balances = await self._collect_reconciliation()
            if generation != self.ws._generation or self._account_stopping:
                return
            async with self._report_lock:
                self._publish_facts(reports, fills)
                self._publish_account(balances)
                self._account_last_success = self._loop.time()
                self._pending_orders.clear()
                self._pending_fills.clear()
            self._account_paused = False
            self._request_account_refresh()

    async def _collect_reconciliation(self, lookback_mins=None, include_account=True):
        now = self._clock.timestamp_ns() // 1_000_000_000
        minutes = self.config.reconciliation_lookback_mins if lookback_mins is None else lookback_mins
        if minutes < 0:
            raise ValueError("Gate reconciliation lookback must be nonnegative")
        since = max(0, now - minutes * 60)
        fills, reports = [], []
        for instrument in self._instrument_provider.get_all().values():
            paired_orders, paired_fills = await self._collect_instrument_reconciliation(instrument, since)
            reports.extend(paired_orders)
            fills.extend(paired_fills)
        balances = None
        if include_account:
            self._account_last_request = self._loop.time()
            balances = await self.http.request("GET", "/spot/accounts", private=True)
        return reports, fills, balances

    async def _reconciliation_orders(self, instrument, since, needed):
        symbol, orders = instrument.raw_symbol.value, {}
        for status in ("open", "finished"):
            params = {"currency_pair": symbol, "status": status}
            if status == "finished":
                params["from"] = since
            rows = (
                await self.http.open_orders(symbol)
                if status == "open"
                else await self.http.pages("/spot/orders", params)
            )
            for row in rows:
                if row["currency_pair"] != symbol:
                    raise ValueError("Gate returned an order for another instrument")
                orders[str(row["id"])] = row
        active = self._cache.orders_open(instrument_id=instrument.id)
        active += self._cache.orders_inflight(instrument_id=instrument.id)
        needed = set(needed) | {str(o.venue_order_id) for o in active if o.venue_order_id is not None}
        for oid in needed - orders.keys():
            row = await self.http.request(
                "GET", "/spot/orders/" + oid, {"currency_pair": symbol}, private=True
            )
            if row["currency_pair"] != symbol or str(row["id"]) != oid:
                raise ValueError("Gate returned a status for another order")
            orders[oid] = row
        return orders

    async def _collect_instrument_reconciliation(self, instrument, since):
        symbol, by_order = instrument.raw_symbol.value, {}
        for fill in await self.http.pages("/spot/my_trades", {"currency_pair": symbol, "from": since}):
            self._validate_execution(fill, instrument)
            by_order.setdefault(str(fill["order_id"]), {})[str(fill["id"])] = fill
        # Pair recent fills and cached active orders even when their creation
        # time lies outside Gate's finished-order filter.
        orders = await self._reconciliation_orders(instrument, since, by_order)
        fills, reports = {}, []
        for oid, row in orders.items():
            report = order_report(
                row, instrument, self.account_id, self._resolve(row), self._clock.timestamp_ns()
            )
            trades = dict(by_order.get(oid, {}))
            known = sum((Decimal(r["amount"]) for r in trades.values()), Decimal(0))
            if known != report.filled_qty.as_decimal():
                # Reuse bulk history; query individual partial lifecycles only.
                for fill in await self.http.pages(
                    "/spot/my_trades", {"currency_pair": symbol, "order_id": oid}
                ):
                    self._validate_execution(fill, instrument, oid)
                    trades[str(fill["id"])] = fill
                known = sum((Decimal(r["amount"]) for r in trades.values()), Decimal(0))
            if known != report.filled_qty.as_decimal():
                raise RuntimeError("Gate reconciliation trade history is incomplete")
            fills.update(((symbol, tid), fill) for tid, fill in trades.items())
            reports.append(report)
        return reports, list(fills.values())

    def _validate_execution(self, row, instrument, order_id=None):
        if row["currency_pair"] != instrument.raw_symbol.value or (
            order_id is not None and str(row["order_id"]) != str(order_id)
        ):
            raise ValueError("Gate returned executions for another order or instrument")
        return fill_report(
            row, instrument, self.account_id, self._resolve(row), self._clock.timestamp_ns()
        )

    async def generate_mass_status(self, lookback_mins=None):
        # The base client gathers orders and fills independently. Gate history
        # can lag its statuses, so return only a complete, paired execution view.
        self.reconciliation_active = True
        try:
            reports, fills, _ = await self._collect_reconciliation(
                lookback_mins=lookback_mins, include_account=False
            )
            mass = ExecutionMassStatus(
                client_id=self.id,
                account_id=self.account_id,
                venue=GATEIO,
                report_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            )
            mass.add_order_reports([
                report for report in reports
                if not self._duplicate_pending_acceptance(self._cached_order({
                    "currency_pair": report.instrument_id.symbol.value,
                    "id": str(report.venue_order_id),
                }), report)
            ])
            mass.add_fill_reports([
                self._validate_execution(row, self._instrument(row["currency_pair"]))
                for row in sorted(fills, key=lambda r: event_ns(r, "create_time", 0))
            ])
            return mass
        except Exception as error:
            detail = str(error) if isinstance(error, GateioHttpError) else type(error).__name__
            self._log.warning(f"Gate mass reconciliation incomplete: {detail}")
            return None
        finally:
            self.reconciliation_active = False

    def _account_is_fresh(self):
        return (
            self._loop.time() - self._account_last_success
            < self.config.account_stale_after_ms / 1000
        )

    async def _await_submission_ready(self, canceled, timeout_secs=None):
        timeout = self.config.submission_ready_timeout_secs if timeout_secs is None else timeout_secs
        deadline = self._loop.time() + timeout
        while not self._account_stopping:
            if canceled.is_set():
                return
            if (
                self.ws.is_ready()
                and self._account_is_fresh()
                and not self._account_paused
                and not self._pending_orders
                and not self._pending_fills
            ):
                return
            if not self._account_paused:
                self._request_account_refresh()
                if self._pending_orders or self._pending_fills:
                    self._schedule_execution_recovery()
            if self._loop.time() >= deadline:
                break
            await asyncio.sleep(0.05)
        raise ValueError(
            "Gate private stream, execution facts or account balance is not ready before submission deadline"
        )

    def _prepare_submission(self, order, canceled, finished):
        if order.is_closed or canceled.is_set():
            raise ValueError("Gate submission canceled before transport dispatch")
        if (
            self._account_stopping or self._account_paused or not self.ws.is_ready()
            or not self._account_is_fresh() or self._pending_orders or self._pending_fills
        ):
            raise _SubmissionDeferred("Gate execution/account state expired while submission was queued")
        self._waiting_submissions.pop(order.client_order_id, None)
        self._inflight_submissions[order.client_order_id] = finished
        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

    async def _send_submission(self, order, payload, canceled, finished):
        budget = self.config.submission_ready_timeout_secs
        while True:
            started = self._loop.time()
            await self._await_submission_ready(canceled, max(0, budget))
            budget -= self._loop.time() - started
            if order.is_closed or canceled.is_set():
                return None
            try:
                return await self.http.request(
                    "POST", "/spot/orders", body=payload, private=True,
                    before_send=lambda: self._prepare_submission(order, canceled, finished),
                )
            except _SubmissionDeferred:
                # Re-queue only an explicitly unsent request. Quota waits do
                # not consume the private-readiness budget.
                continue

    async def _submit_order(self, command):
        order = command.order
        if order.is_closed:
            return
        canceled, finished = asyncio.Event(), asyncio.Event()
        self._waiting_submissions[order.client_order_id] = canceled
        try:
            payload = submit_payload(order)
            if self._instrument_provider.find(order.instrument_id) is None:
                raise ValueError("Gate submission instrument is not configured for this client")
            row = await self._send_submission(order, payload, canceled, finished)
            if order.is_closed or canceled.is_set():
                return
            _check_order_response(row, order.instrument_id, client_order_id=order.client_order_id)
            async with self._report_lock:
                await self._publish_order(row)
            self._request_account_refresh()
        except ValueError as error:
            if order.client_order_id not in self._inflight_submissions:
                if not order.is_closed and not canceled.is_set():
                    self.generate_order_denied(
                        strategy_id=order.strategy_id,
                        instrument_id=order.instrument_id,
                        client_order_id=order.client_order_id,
                        reason=str(error),
                        ts_event=self._clock.timestamp_ns(),
                    )
            else:
                self._log.warning("Gate submission response invalid; query before replacement")
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
        finally:
            self._waiting_submissions.pop(order.client_order_id, None)
            self._inflight_submissions.pop(order.client_order_id, None)
            finished.set()

    async def _cancel_order(self, command):  # noqa: C901 (local, in-flight and exchange outcomes)
        waiting = self._waiting_submissions.get(command.client_order_id)
        if waiting is not None:
            # The POST has not started: cancellation is a known local outcome,
            # not an exchange 404 which could race a delayed future submission.
            waiting.set()
            self.generate_order_canceled(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=None,
                ts_event=self._clock.timestamp_ns(),
            )
            return
        order = self._cache.order(command.client_order_id)
        if order is not None and order.is_closed:
            return
        inflight = self._inflight_submissions.get(command.client_order_id)
        if inflight is not None and not (
            command.venue_order_id or (order is not None and order.venue_order_id)
        ):
            # A custom-ID cancel must not overtake an HTTP POST still in flight.
            # No report lock is held while waiting; WS confirmations keep flowing.
            await inflight.wait()
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
            _check_order_response(
                row,
                command.instrument_id,
                venue_id,
                command.client_order_id,
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
                if _order_not_found(error):
                    try:
                        row = await _fetch_order(self, command)
                        if row is None:
                            self._log.warning(
                                "Gate cancel target remains unknown; await reconciliation"
                            )
                            return
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
                        return
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
        orders = {
            o.client_order_id: o
            for o in (
                self._cache.orders_open(instrument_id=command.instrument_id)
                + self._cache.orders_inflight(instrument_id=command.instrument_id)
            )
            if o.account_id == self.account_id
        }
        for cid in tuple(self._waiting_submissions):
            order = self._cache.order(cid)
            if order is not None and order.instrument_id == command.instrument_id:
                orders[cid] = order
        for order in orders.values():
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
        row = await _fetch_order(self, command)
        if row is None:
            return None
        report = order_report(
            row,
            self._instrument(row["currency_pair"]),
            self.account_id,
            self._resolve(row),
            self._clock.timestamp_ns(),
        )
        if report.order_status == OrderStatus.ACCEPTED and self._duplicate_pending_acceptance(
            self._cached_order(row), report
        ):
            return None
        async with self._report_lock:
            await self._load_order_fills(row, report)
            if (row["currency_pair"], str(row["id"])) in self._pending_orders:
                return None
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
                if status == "finished" and command.start is not None:
                    params["from"] = int(command.start.timestamp())
                if status == "finished" and command.end is not None:
                    params["to"] = int(command.end.timestamp())
                rows = (
                    await self.http.open_orders(instrument.raw_symbol.value)
                    if status == "open"
                    else await self.http.pages("/spot/orders", params)
                )
                for row in rows:
                    if row["currency_pair"] != instrument.raw_symbol.value:
                        raise ValueError("Gate returned an order for another instrument")
                    report = order_report(
                        row, instrument, self.account_id, self._resolve(row), self._clock.timestamp_ns()
                    )
                    async with self._report_lock:
                        await self._load_order_fills(row, report)
                        if (row["currency_pair"], str(row["id"])) in self._pending_orders:
                            # Periodic NT open-order checks reconcile with trades=[],
                            # so never expose a filled total before real fills land.
                            raise RuntimeError("Gate order query awaits matching real executions")
                    cached = self._cached_order(row)
                    if cached is not None and (
                        cached.filled_qty > report.filled_qty
                        or (cached.is_closed and report.is_open)
                        or self._duplicate_pending_acceptance(cached, report)
                    ):
                        continue
                    reports.append(report)
        return reports

    async def generate_fill_reports(self, command):
        if command.venue_order_id is not None and command.instrument_id is None:
            raise ValueError("Gate order-specific fill query requires an instrument")
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
                    self._validate_execution(row, instrument, command.venue_order_id)
                )
        return reports

    async def generate_position_status_reports(self, command):
        return []  # Cash spot has balances, not derivative positions.
