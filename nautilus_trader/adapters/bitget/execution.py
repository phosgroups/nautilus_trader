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

from __future__ import annotations

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Any

from nautilus_trader.adapters.bitget.common.constants import BITGET_VENUE
from nautilus_trader.adapters.bitget.config import BitgetExecClientConfig
from nautilus_trader.adapters.bitget.providers import BitgetInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.secure import mask_api_key
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import ensure_pydatetime_utc
from nautilus_trader.core.nautilus_pyo3 import BitgetEnvironment
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import QueryAccount
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.events import OrderModifyRejected
from nautilus_trader.model.events import OrderUpdated
from nautilus_trader.model.functions import order_side_to_pyo3
from nautilus_trader.model.functions import order_type_to_pyo3
from nautilus_trader.model.functions import time_in_force_to_pyo3
from nautilus_trader.model.functions import trigger_type_to_pyo3
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.orders import Order


class BitgetExecutionClient(LiveExecutionClient):
    """
    Provides a Python live execution client for the Bitget centralized crypto exchange.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: nautilus_pyo3.BitgetHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: BitgetInstrumentProvider,
        config: BitgetExecClientConfig,
        name: str | None,
    ) -> None:
        PyCondition.not_none(client, "client")
        PyCondition.not_none(instrument_provider, "instrument_provider")

        account_type, oms_type = self._derive_account_and_oms_type(config.product_type)

        super().__init__(
            loop=loop,
            client_id=ClientId(name or BITGET_VENUE.value),
            venue=BITGET_VENUE,
            oms_type=oms_type,
            account_type=account_type,
            base_currency=None,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

        self._config = config
        self._environment = config.environment or BitgetEnvironment.MAINNET
        self._http_client = client
        self._instrument_provider: BitgetInstrumentProvider = instrument_provider
        self._product_type = config.product_type
        self._ws_task: asyncio.Task | None = None
        self._ws_recovery_task: asyncio.Task | None = None
        self._ws_report_lock = asyncio.Lock()
        self._pending_order_reports: dict[VenueOrderId, OrderStatusReport] = {}
        self._pending_fill_reports: dict[VenueOrderId, dict[Any, FillReport]] = {}

        account_id = AccountId(f"{name or BITGET_VENUE.value}-master")
        self._set_account_id(account_id)
        self.pyo3_account_id = nautilus_pyo3.AccountId(account_id.value)

        self._ws_client = nautilus_pyo3.BitgetWebSocketClient.new_private(
            product_type=self._product_type,
            environment=self._environment,
            account_mode=config.account_mode,
            api_key=config.api_key,
            api_secret=config.api_secret,
            api_passphrase=config.api_passphrase,
            url=config.base_url_ws_private,
            heartbeat_secs=30,
            proxy_url=config.proxy_url,
        )

        self._log.info(f"product_type={self._product_type}", LogColor.BLUE)
        self._log.info(f"environment={self._environment}", LogColor.BLUE)
        self._log.info(f"base_url_http={config.base_url_http}", LogColor.BLUE)
        self._log.info(f"base_url_ws_private={config.base_url_ws_private}", LogColor.BLUE)
        self._log.info(
            f"ignore_uncached_instrument_executions={config.ignore_uncached_instrument_executions}",
            LogColor.BLUE,
        )
        self._log.info(f"proxy_url={config.proxy_url}", LogColor.BLUE)

        if config.api_key:
            self._log.info(f"REST API key {mask_api_key(config.api_key)}", LogColor.BLUE)

    @staticmethod
    def _derive_account_and_oms_type(
        product_type: BitgetProductType,
    ) -> tuple[AccountType, OmsType]:
        if product_type == BitgetProductType.SPOT:
            return AccountType.CASH, OmsType.HEDGING
        return AccountType.MARGIN, OmsType.NETTING

    @property
    def instrument_provider(self) -> BitgetInstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()
        self._cache_instruments()
        await self._update_account_state()
        await self._await_account_registered()

        await self._ws_client.connect()
        await self._ws_client.wait_until_active(timeout_secs=30.0)
        self._log.info(f"Connected to private websocket {self._ws_client.url}", LogColor.BLUE)

        await self._ws_client.subscribe_account()
        await self._ws_client.subscribe_orders()
        await self._ws_client.subscribe_fills()
        await self._ws_client.subscribe_positions()
        # next_event waits while holding the Rust client lock. Finish subscription
        # commands before starting the receiver so they cannot wait behind it.
        self._ws_task = self.create_task(
            self._consume_ws_events(),
            log_msg="bitget_private_ws_consume",
        )
        self._ws_recovery_task = self.create_task(
            self._recover_pending_ws_reports(),
            log_msg="bitget_private_ws_recovery",
        )

    async def _disconnect(self) -> None:
        tasks = [task for task in (self._ws_task, self._ws_recovery_task) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._ws_task = None
        self._ws_recovery_task = None

        if not self._ws_client.is_closed():
            await self._ws_client.close()
            self._log.info(f"Disconnected from {self._ws_client.url}", LogColor.BLUE)

        self._http_client.cancel_all_requests()

    def _cache_instruments(self) -> None:
        self._http_client.cache_instruments(self._instrument_provider.instruments_pyo3())

        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

        for instrument in self._instrument_provider.get_all().values():
            self._cache.add_instrument(instrument)

    async def _consume_ws_events(self) -> None:
        while self.is_connected or not self._ws_client.is_closed():
            event = await self._ws_client.next_event()
            if event is None:
                if self._ws_client.is_closed():
                    return
                await asyncio.sleep(0.1)
                continue

            try:
                await self._handle_ws_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._log.exception("Failed to process Bitget private websocket push", error)

    async def _handle_ws_event(self, event: Any) -> None:
        async with self._ws_report_lock:
            await self._process_ws_event(event)

    async def _process_ws_event(self, event: Any) -> None:
        if not isinstance(event, dict):
            self._log.debug(f"Ignoring Bitget websocket event {event!r}")
            return

        event_type = event.get("type")
        if event_type == "error":
            self._log.warning(f"Bitget websocket error: {event}")
            return
        if event_type == "reconnected":
            await self._reconcile_ws_reconnect()
            return
        if event_type != "data":
            self._log.debug(f"Bitget websocket event: {event}")
            return

        topic = event.get("arg", {}).get("topic")
        if topic == "account":
            # Account updates can be deltas, and Classic futures omit margin.
            # A full REST state preserves balances and margins for all currencies.
            await self._update_account_state()
            return
        if topic not in {
            "order",
            "orders",
            "strategy-order",
            "fill",
            "fills",
            "position",
            "positions",
        }:
            self._log.debug(f"Ignoring Bitget private websocket topic {topic!r}")
            return

        rows = await self._prepare_ws_rows(event)
        if not rows:
            return
        parsed = self._http_client.parse_execution_ws_event(
            event={**event, "data": rows},
            product_type=self._product_type,
            trader_id=nautilus_pyo3.TraderId(self.trader_id.value),
            account_id=self.pyo3_account_id,
        )
        await self._dispatch_parsed_ws_events(parsed)

    async def _prepare_ws_rows(self, event: dict) -> list[dict]:
        rows = []
        for row in event.get("data", []):
            symbol = row.get("symbol")
            if symbol:
                suffix = "-PERP" if self._product_type == BitgetProductType.USDT_FUTURES else ""
                instrument_id = InstrumentId.from_str(f"{symbol}{suffix}.{BITGET_VENUE.value}")
                instrument = self._cache.instrument(instrument_id)
                if instrument is None and not self._config.ignore_uncached_instrument_executions:
                    instrument = await self._ensure_instrument(instrument_id)
                if instrument is None:
                    self._log.debug(
                        f"Ignoring execution for uncached Bitget instrument {instrument_id}"
                    )
                    continue
            rows.append(row)

        return rows

    async def _dispatch_parsed_ws_events(self, parsed: list) -> None:
        for message in parsed:
            try:
                await self._dispatch_parsed_ws_event(message)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Keep processing other orders in the same frame. Failed REST
                # requests leave this order/fill in the pending maps for retry.
                self._log.exception("Failed to dispatch Bitget execution update", error)

    async def _dispatch_parsed_ws_event(self, message) -> None:
        if isinstance(message, dict):
            report = OrderStatusReport.from_pyo3(message["order"])
            fills = [FillReport.from_pyo3(fill) for fill in message["fills"]]
            await self._dispatch_order_report(report, fills)
        elif isinstance(message, nautilus_pyo3.OrderStatusReport):
            await self._dispatch_order_report(OrderStatusReport.from_pyo3(message), [])
        elif isinstance(message, nautilus_pyo3.FillReport):
            await self._dispatch_fill_report(FillReport.from_pyo3(message))
        elif isinstance(message, nautilus_pyo3.PositionStatusReport):
            # Position pushes may precede their order/fill pushes. Runtime
            # position reconciliation remains the engine's periodic check;
            # applying them here could create synthetic trades first.
            self._log.debug(f"Received Bitget position update: {message}")
        elif isinstance(message, nautilus_pyo3.OrderUpdated):
            self._send_order_event(OrderUpdated.from_dict(message.to_dict()))
        elif isinstance(message, nautilus_pyo3.OrderModifyRejected):
            self._send_order_event(OrderModifyRejected.from_dict(message.to_dict()))

    def _cached_order_for_report(self, report):
        client_order_id = self._cache.client_order_id(report.venue_order_id)
        if client_order_id is None:
            client_order_id = report.client_order_id
        return self._cache.order(client_order_id) if client_order_id is not None else None

    async def _request_ws_order_status(self, report) -> OrderStatusReport | None:
        pyo3_report = await self._http_client.request_order_status_report(
            account_id=self.pyo3_account_id,
            product_type=self._product_type,
            instrument_id=nautilus_pyo3.InstrumentId.from_str(report.instrument_id.value),
            venue_order_id=report.venue_order_id.value,
            client_order_id=report.client_order_id.value if report.client_order_id else None,
            ts_init_ns=self._clock.timestamp_ns(),
        )
        return OrderStatusReport.from_pyo3(pyo3_report) if pyo3_report is not None else None

    def _needs_market_buy_base_quantity(self, report: OrderStatusReport) -> bool:
        return (
            self._product_type == BitgetProductType.SPOT
            and report.order_type == OrderType.MARKET
            and report.order_side == OrderSide.BUY
            and report.order_status == OrderStatus.PARTIALLY_FILLED
            and report.avg_px is None
            and report.quantity == report.filled_qty
        )

    async def _resolve_market_buy_base_quantity(
        self,
        report: OrderStatusReport,
    ) -> OrderStatusReport | None:
        if not self._needs_market_buy_base_quantity(report):
            return report
        # Without an average price, the shared parser can only expose the
        # executed base quantity. Applying it as total size would close a
        # venue order which is still partially filled.
        refreshed = await self._request_ws_order_status(report)
        if refreshed is None or self._needs_market_buy_base_quantity(refreshed):
            self._log.warning(
                f"Deferring Bitget market buy {report.venue_order_id}: "
                "partial execution is missing its total base quantity",
            )
            return None
        return refreshed

    async def _dispatch_order_report(
        self,
        report: OrderStatusReport,
        fills: list[FillReport],
    ) -> None:
        pending = self._pending_fill_reports.setdefault(report.venue_order_id, {})
        pending.update((fill.trade_id, fill) for fill in fills)
        previous = self._pending_order_reports.get(report.venue_order_id)
        if previous is not None and previous.ts_last > report.ts_last:
            report = previous
        self._pending_order_reports[report.venue_order_id] = report
        resolved = await self._resolve_market_buy_base_quantity(report)
        if resolved is None:
            return
        report = resolved
        self._pending_order_reports[report.venue_order_id] = report
        order = self._cached_order_for_report(report)
        known_qty = order.filled_qty.as_decimal() if order is not None else Decimal(0)
        known_trades = set(order.trade_ids) if order is not None else set()
        pending = {
            trade_id: fill for trade_id, fill in pending.items() if trade_id not in known_trades
        }
        self._pending_fill_reports[report.venue_order_id] = pending
        if known_qty > report.filled_qty.as_decimal() and not pending:
            self._pending_fill_reports.pop(report.venue_order_id, None)
            self._pending_order_reports.pop(report.venue_order_id, None)
            return  # Ignore a replayed status older than the cached executions.

        def available_quantity() -> Decimal:
            return known_qty + sum(
                (
                    fill.last_qty.as_decimal()
                    for trade_id, fill in pending.items()
                    if trade_id not in known_trades
                ),
                Decimal(0),
            )

        if available_quantity() < report.filled_qty.as_decimal():
            # UTA has separate order/fill channels. Also recover earlier Classic
            # fills if our first push only contains the latest execution.
            pyo3_fills = await self._http_client.request_fill_reports(
                account_id=self.pyo3_account_id,
                product_type=self._product_type,
                instrument_id=nautilus_pyo3.InstrumentId.from_str(report.instrument_id.value),
                start=datetime.fromtimestamp(max(0, report.ts_accepted / 1_000_000_000 - 1), UTC),
                end=None,
                limit=None,
                ts_init_ns=self._clock.timestamp_ns(),
            )
            for fill in map(FillReport.from_pyo3, pyo3_fills):
                if fill.venue_order_id == report.venue_order_id:
                    pending[fill.trade_id] = fill

        if available_quantity() != report.filled_qty.as_decimal():
            self._log.warning(
                f"Deferring Bitget order {report.venue_order_id}: real fills total "
                f"{available_quantity()} differs from reported {report.filled_qty}; "
                "waiting for matching order/fill pushes",
            )
            return

        if pending:
            # Reconcile status and fills together. Sending the filled status
            # first would let the engine infer executions with synthetic IDs.
            mass_status = ExecutionMassStatus(
                client_id=self.id,
                account_id=self.account_id,
                venue=BITGET_VENUE,
                report_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            )
            mass_status.add_order_reports([report])
            mass_status.add_fill_reports(sorted(pending.values(), key=lambda fill: fill.ts_event))
            self._send_mass_status_report(mass_status)
        else:
            self._send_order_status_report(report)
        self._pending_fill_reports.pop(report.venue_order_id, None)
        self._pending_order_reports.pop(report.venue_order_id, None)

    async def _dispatch_fill_report(self, report: FillReport) -> None:
        order = self._cached_order_for_report(report)
        status = self._pending_order_reports.get(report.venue_order_id)
        if status is not None or order is None or order.is_quote_quantity:
            self._pending_fill_reports.setdefault(report.venue_order_id, {})[report.trade_id] = (
                report
            )
            pending_quantity = sum(
                (
                    fill.last_qty.as_decimal()
                    for fill in self._pending_fill_reports[report.venue_order_id].values()
                ),
                Decimal(0),
            )
            known_quantity = order.filled_qty.as_decimal() if order is not None else Decimal(0)
            if status is None or status.filled_qty.as_decimal() < known_quantity + pending_quantity:
                status = await self._request_ws_order_status(report)
            if status is not None:
                await self._dispatch_order_report(status, [report])
            return
        self._send_fill_report(report)

    async def _recover_pending_ws_reports(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            async with self._ws_report_lock:
                await self._retry_pending_ws_reports()

    async def _retry_pending_ws_reports(self) -> None:
        for venue_order_id in list(self._pending_fill_reports):
            status = self._pending_order_reports.get(venue_order_id)
            fills = list(self._pending_fill_reports.get(venue_order_id, {}).values())
            try:
                if fills:
                    refreshed = await self._request_ws_order_status(fills[0])
                    if refreshed is not None:
                        status = refreshed
                if status is not None:
                    await self._dispatch_order_report(status, [])
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._log.warning(
                    f"Bitget pending execution recovery failed for {venue_order_id}: {error}"
                )

    async def _reconcile_ws_reconnect(self) -> None:
        await self._update_account_state()
        start = self._clock.utc_now() - timedelta(
            minutes=self._config.reconnect_reconciliation_lookback_mins,
        )
        for instrument in self._instrument_provider.get_all().values():
            reports = await self._http_client.request_order_status_reports(
                account_id=self.pyo3_account_id,
                product_type=self._product_type,
                instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
                open_only=False,
                start=ensure_pydatetime_utc(start),
                end=None,
                limit=None,
                ts_init_ns=self._clock.timestamp_ns(),
            )
            for report in map(OrderStatusReport.from_pyo3, reports):
                await self._dispatch_order_report(report, [])
        await self._retry_pending_ws_reports()

    async def _ensure_instrument(self, instrument_id: InstrumentId) -> Instrument | None:
        instrument = self._instrument_provider.find(instrument_id) or self._cache.instrument(
            instrument_id,
        )
        if instrument is not None:
            return instrument

        await self._instrument_provider.load_ids_async([instrument_id])
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is not None:
            self._cache.add_instrument(instrument)
            self._http_client.cache_instruments(self._instrument_provider.instruments_pyo3())
        return instrument

    async def _update_account_state(self) -> None:
        pyo3_account_state = await self._http_client.request_account_state(
            self._product_type,
            self.pyo3_account_id,
            ts_init_ns=self._clock.timestamp_ns(),
        )
        account_state = AccountState.from_dict(pyo3_account_state.to_dict())

        self.generate_account_state(
            balances=account_state.balances,
            margins=account_state.margins,
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

        if account_state.balances:
            self._log.info(f"Generated account state with {len(account_state.balances)} balance(s)")

    def _instruments_for_request(self, instrument_id: InstrumentId | None) -> list[Instrument]:
        if instrument_id is not None:
            instrument = self._instrument_provider.find(instrument_id) or self._cache.instrument(
                instrument_id,
            )
            return [instrument] if instrument is not None else []

        return list(self._instrument_provider.get_all().values())

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        instrument = await self._ensure_instrument(command.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find Bitget instrument {command.instrument_id}")
            return None

        try:
            pyo3_report = await self._http_client.request_order_status_report(
                account_id=self.pyo3_account_id,
                product_type=self._product_type,
                instrument_id=nautilus_pyo3.InstrumentId.from_str(command.instrument_id.value),
                venue_order_id=command.venue_order_id.value if command.venue_order_id else None,
                client_order_id=command.client_order_id.value if command.client_order_id else None,
                ts_init_ns=self._clock.timestamp_ns(),
            )
            report = OrderStatusReport.from_pyo3(pyo3_report)
            self._log.debug(f"Received {report}", LogColor.MAGENTA)
            return report
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "OrderStatusReport")
            return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        reports: list[OrderStatusReport] = []

        try:
            for instrument in self._instruments_for_request(command.instrument_id):
                pyo3_reports = await self._http_client.request_order_status_reports(
                    account_id=self.pyo3_account_id,
                    product_type=self._product_type,
                    instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
                    open_only=command.open_only,
                    start=ensure_pydatetime_utc(command.start),
                    end=ensure_pydatetime_utc(command.end),
                    limit=None,
                    ts_init_ns=self._clock.timestamp_ns(),
                )
                reports.extend(OrderStatusReport.from_pyo3(report) for report in pyo3_reports)
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "OrderStatusReports")

        self._log_report_receipt(
            len(reports),
            "OrderStatusReport",
            command.log_receipt_level,
        )
        return reports

    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        reports: list[FillReport] = []

        try:
            for instrument in self._instruments_for_request(command.instrument_id):
                pyo3_reports = await self._http_client.request_fill_reports(
                    account_id=self.pyo3_account_id,
                    product_type=self._product_type,
                    instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
                    start=ensure_pydatetime_utc(command.start),
                    end=ensure_pydatetime_utc(command.end),
                    limit=None,
                    ts_init_ns=self._clock.timestamp_ns(),
                )
                reports.extend(FillReport.from_pyo3(report) for report in pyo3_reports)
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "FillReports")

        self._log_report_receipt(len(reports), "FillReport", LogLevel.INFO)
        return reports

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        reports: list[PositionStatusReport] = []

        try:
            for instrument in self._instruments_for_request(command.instrument_id):
                pyo3_reports = await self._http_client.request_position_status_reports(
                    account_id=self.pyo3_account_id,
                    product_type=self._product_type,
                    instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
                    ts_init_ns=self._clock.timestamp_ns(),
                )
                if pyo3_reports:
                    reports.extend(
                        PositionStatusReport.from_pyo3(report) for report in pyo3_reports
                    )
                elif command.instrument_id is not None:
                    reports.append(
                        PositionStatusReport.create_flat(
                            account_id=self.account_id,
                            instrument_id=command.instrument_id,
                            size_precision=instrument.size_precision,
                            ts_init=self._clock.timestamp_ns(),
                            report_id=UUID4(),
                        ),
                    )
        except (asyncio.CancelledError, Exception) as e:
            self._log_report_error(e, "PositionReports")

        self._log_report_receipt(
            len(reports),
            "PositionReport",
            command.log_receipt_level,
        )
        return reports

    async def _query_account(self, _command: QueryAccount) -> None:
        await self._update_account_state()

    def _submit_order_request(self, order: Order, params: dict[str, Any] | None):
        pyo3_trigger_type = None
        trigger_type = getattr(order, "trigger_type", None)
        if trigger_type is not None:
            pyo3_trigger_type = trigger_type_to_pyo3(trigger_type)

        return self._http_client.submit_order(
            product_type=self._product_type,
            trader_id=nautilus_pyo3.TraderId.from_str(order.trader_id.value),
            strategy_id=nautilus_pyo3.StrategyId.from_str(order.strategy_id.value),
            instrument_id=nautilus_pyo3.InstrumentId.from_str(order.instrument_id.value),
            client_order_id=nautilus_pyo3.ClientOrderId(order.client_order_id.value),
            order_side=order_side_to_pyo3(order.side),
            order_type=order_type_to_pyo3(order.order_type),
            quantity=nautilus_pyo3.Quantity.from_str(str(order.quantity)),
            time_in_force=time_in_force_to_pyo3(order.time_in_force),
            price=nautilus_pyo3.Price.from_str(str(order.price)) if order.has_price else None,
            trigger_price=(
                nautilus_pyo3.Price.from_str(str(order.trigger_price))
                if order.has_trigger_price
                else None
            ),
            trigger_type=pyo3_trigger_type,
            post_only=order.is_post_only,
            reduce_only=order.is_reduce_only,
            quote_quantity=order.is_quote_quantity,
            params=params,
            ts_event_ns=self._clock.timestamp_ns(),
            ts_init_ns=order.ts_init,
        )

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order

        if order.is_closed:
            self._log.warning(f"Cannot submit already closed order: {order}")
            return

        try:
            response_fut = self._submit_order_request(order, command.params)
        except Exception as e:
            self.generate_order_denied(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )
            return

        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        try:
            response = await response_fut
            self._raise_for_bitget_ack(response)
            order_id = response.get("order_id")
            if order_id is None:
                self._log.warning(
                    f"Bitget accepted {order.client_order_id!r} without returning order_id",
                )
                return

            self.generate_order_accepted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=VenueOrderId(str(order_id)),
                ts_event=self._clock.timestamp_ns(),
            )
        except Exception as e:
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        for order in command.order_list.orders:
            await self._submit_order(
                SubmitOrder(
                    trader_id=command.trader_id,
                    strategy_id=order.strategy_id,
                    order=order,
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                    position_id=command.position_id,
                    client_id=command.client_id,
                    params=command.params,
                ),
            )

    async def _modify_order(self, command: ModifyOrder) -> None:
        self.generate_order_modify_rejected(
            strategy_id=command.strategy_id,
            instrument_id=command.instrument_id,
            client_order_id=command.client_order_id,
            venue_order_id=command.venue_order_id,
            reason="Bitget Python client does not expose order modification yet",
            ts_event=self._clock.timestamp_ns(),
        )

    @staticmethod
    def _raise_for_bitget_ack(response: dict[str, Any]) -> None:
        if response.get("success") is False:
            raise ValueError(response.get("msg") or "Bitget API returned success=false")

    def _cancel_order_request(
        self,
        instrument_id: InstrumentId,
        client_order_id,
        venue_order_id,
        params: dict[str, Any] | None,
    ):
        return self._http_client.cancel_order(
            product_type=self._product_type,
            instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument_id.value),
            client_order_id=nautilus_pyo3.ClientOrderId(client_order_id.value),
            venue_order_id=(
                nautilus_pyo3.VenueOrderId(venue_order_id.value)
                if venue_order_id is not None
                else None
            ),
            params=params,
        )

    async def _cancel_order(self, command: CancelOrder) -> None:
        order: Order | None = self._cache.order(command.client_order_id)
        if order is not None and order.is_closed:
            self._log.warning(
                f"CancelOrder for {command.client_order_id!r} ignored because order is already {order.status_string()}",
            )
            return

        venue_order_id = command.venue_order_id or (
            order.venue_order_id if order is not None else None
        )

        try:
            response_fut = self._cancel_order_request(
                command.instrument_id,
                command.client_order_id,
                venue_order_id,
                command.params,
            )
            response = await response_fut
            self._raise_for_bitget_ack(response)

            ack_order_id = response.get("order_id")
            resolved_venue_order_id = venue_order_id or (
                VenueOrderId(str(ack_order_id)) if ack_order_id else None
            )
            if resolved_venue_order_id is None:
                self._log.warning(
                    f"Bitget canceled {command.client_order_id!r} without returning order_id",
                )
                return

            self.generate_order_canceled(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=resolved_venue_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
        except Exception as e:
            self.generate_order_cancel_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=venue_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        if command.order_side != OrderSide.NO_ORDER_SIDE:
            self._log.warning("Bitget cancel-all ignores order_side filtering in the Python client")

        for order in self._cache.orders_open(instrument_id=command.instrument_id):
            cancel = CancelOrder(
                trader_id=command.trader_id,
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=order.venue_order_id,
                command_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
                client_id=command.client_id,
                params=command.params,
            )
            await self._cancel_order(cancel)

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        for cancel in command.cancels:
            await self._cancel_order(cancel)
