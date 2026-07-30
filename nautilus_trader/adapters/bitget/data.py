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
from typing import Any

from nautilus_trader.adapters.bitget.common.constants import BITGET_VENUE
from nautilus_trader.adapters.bitget.config import BitgetDataClientConfig
from nautilus_trader.adapters.bitget.providers import BitgetInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.secure import mask_api_key
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import ensure_pydatetime_utc
from nautilus_trader.core.nautilus_pyo3 import BitgetEnvironment
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import RequestData
from nautilus_trader.data.messages import RequestForwardPrices
from nautilus_trader.data.messages import RequestFundingRates
from nautilus_trader.data.messages import RequestInstrument
from nautilus_trader.data.messages import RequestInstruments
from nautilus_trader.data.messages import RequestOrderBookDeltas
from nautilus_trader.data.messages import RequestOrderBookDepth
from nautilus_trader.data.messages import RequestOrderBookSnapshot
from nautilus_trader.data.messages import RequestQuoteTicks
from nautilus_trader.data.messages import RequestTradeTicks
from nautilus_trader.data.messages import SubscribeBars
from nautilus_trader.data.messages import SubscribeData
from nautilus_trader.data.messages import SubscribeFundingRates
from nautilus_trader.data.messages import SubscribeIndexPrices
from nautilus_trader.data.messages import SubscribeInstrument
from nautilus_trader.data.messages import SubscribeInstrumentClose
from nautilus_trader.data.messages import SubscribeInstrumentStatus
from nautilus_trader.data.messages import SubscribeInstruments
from nautilus_trader.data.messages import SubscribeMarkPrices
from nautilus_trader.data.messages import SubscribeOptionGreeks
from nautilus_trader.data.messages import SubscribeOrderBook
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeBars
from nautilus_trader.data.messages import UnsubscribeData
from nautilus_trader.data.messages import UnsubscribeFundingRates
from nautilus_trader.data.messages import UnsubscribeIndexPrices
from nautilus_trader.data.messages import UnsubscribeInstrument
from nautilus_trader.data.messages import UnsubscribeInstrumentClose
from nautilus_trader.data.messages import UnsubscribeInstrumentStatus
from nautilus_trader.data.messages import UnsubscribeInstruments
from nautilus_trader.data.messages import UnsubscribeMarkPrices
from nautilus_trader.data.messages import UnsubscribeOptionGreeks
from nautilus_trader.data.messages import UnsubscribeOrderBook
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import FundingRateUpdate
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.enums import book_type_to_str
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument


class BitgetDataClient(LiveMarketDataClient):
    """
    Provides a Python live market data client for the Bitget centralized crypto exchange.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: nautilus_pyo3.BitgetHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: BitgetInstrumentProvider,
        config: BitgetDataClientConfig,
        name: str | None,
    ) -> None:
        PyCondition.not_none(client, "client")
        PyCondition.not_none(instrument_provider, "instrument_provider")

        super().__init__(
            loop=loop,
            client_id=ClientId(name or BITGET_VENUE.value),
            venue=BITGET_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        self._config = config
        self._environment = config.environment or BitgetEnvironment.MAINNET
        self._http_client = client
        self._instrument_provider: BitgetInstrumentProvider = instrument_provider
        self._product_type = config.product_type
        self._ws_client = nautilus_pyo3.BitgetWebSocketClient.new_public(
            product_type=self._product_type,
            environment=self._environment,
            url=config.base_url_ws_public,
            heartbeat_secs=30,
            proxy_url=config.proxy_url,
        )

        self._log.info(f"product_type={self._product_type}", LogColor.BLUE)
        self._log.info(f"environment={self._environment}", LogColor.BLUE)
        self._log.info(f"base_url_http={config.base_url_http}", LogColor.BLUE)
        self._log.info(f"base_url_ws_public={config.base_url_ws_public}", LogColor.BLUE)
        self._log.info(f"proxy_url={config.proxy_url}", LogColor.BLUE)

        if config.api_key:
            self._log.info(f"REST API key {mask_api_key(config.api_key)}", LogColor.BLUE)

    @property
    def instrument_provider(self) -> BitgetInstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()
        self._cache_instruments()
        self._send_all_instruments_to_data_engine()

        await self._ws_client.connect()
        await self._ws_client.wait_until_active(timeout_secs=30.0)
        self._log.info(f"Connected to public websocket {self._ws_client.url}", LogColor.BLUE)
        self.create_task(self._consume_ws_events(), log_msg="bitget_public_ws_consume")

    async def _disconnect(self) -> None:
        self._http_client.cancel_all_requests()

        if not self._ws_client.is_closed():
            await self._ws_client.close()
            self._log.info(f"Disconnected from {self._ws_client.url}", LogColor.BLUE)

    def _cache_instruments(self) -> None:
        self._http_client.cache_instruments(self._instrument_provider.instruments_pyo3())

        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

        for instrument in self._instrument_provider.get_all().values():
            self._cache.add_instrument(instrument)

    def _send_all_instruments_to_data_engine(self) -> None:
        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)

    async def _consume_ws_events(self) -> None:
        while self.is_connected or not self._ws_client.is_closed():
            event = await self._ws_client.next_event()
            if event is None:
                if self._ws_client.is_closed():
                    return
                await asyncio.sleep(0.1)
                continue

            self._handle_ws_event(event)

    def _handle_ws_event(self, event: Any) -> None:
        if not isinstance(event, dict):
            self._log.debug(f"Ignoring Bitget websocket event {event!r}")
            return

        event_type = event.get("type")
        if event_type == "error":
            self._log.warning(f"Bitget websocket error: {event}")
        elif event_type in {"subscribe", "unsubscribe", "login", "pong", "reconnected"}:
            self._log.debug(f"Bitget websocket event: {event}")
        else:
            self._log.debug(f"Received Bitget websocket data: {event}")

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
        return instrument

    @staticmethod
    def _raw_symbol(instrument_id: InstrumentId) -> str:
        symbol = str(instrument_id.symbol)
        return symbol.removesuffix("-PERP")

    async def _subscribe(self, command: SubscribeData) -> None:
        self._log.warning(f"Generic Bitget data subscription not implemented: {command}")

    async def _subscribe_instruments(self, command: SubscribeInstruments) -> None:
        await self._instrument_provider.load_all_async(command.params)
        self._cache_instruments()
        self._send_all_instruments_to_data_engine()

    async def _subscribe_instrument(self, command: SubscribeInstrument) -> None:
        instrument = await self._ensure_instrument(command.instrument_id)
        if instrument is not None:
            self._handle_data(instrument)

    async def _subscribe_order_book_deltas(self, command: SubscribeOrderBook) -> None:
        if command.book_type != BookType.L2_MBP:
            self._log.warning(
                f"Book type {book_type_to_str(command.book_type)} not supported by Bitget",
            )
            return

        raw_symbol = self._raw_symbol(command.instrument_id)
        await self._ws_client.subscribe_books(raw_symbol)

    async def _subscribe_order_book_depth(self, command: SubscribeOrderBook) -> None:
        await self._subscribe_order_book_deltas(command)

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        raw_symbol = self._raw_symbol(command.instrument_id)
        await self._ws_client.subscribe_ticker(raw_symbol)

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        raw_symbol = self._raw_symbol(command.instrument_id)
        await self._ws_client.subscribe_trades(raw_symbol)

    async def _subscribe_mark_prices(self, command: SubscribeMarkPrices) -> None:
        raw_symbol = self._raw_symbol(command.instrument_id)
        await self._ws_client.subscribe_ticker(raw_symbol)

    async def _subscribe_index_prices(self, command: SubscribeIndexPrices) -> None:
        raw_symbol = self._raw_symbol(command.instrument_id)
        await self._ws_client.subscribe_ticker(raw_symbol)

    async def _subscribe_funding_rates(self, command: SubscribeFundingRates) -> None:
        raw_symbol = self._raw_symbol(command.instrument_id)
        await self._ws_client.subscribe_ticker(raw_symbol)

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        interval = command.params.get("interval") if command.params else None
        if interval is None:
            self._log.warning(
                "Bitget candle subscriptions require params['interval'] until BarType mapping is exposed",
            )
            return

        raw_symbol = self._raw_symbol(command.bar_type.instrument_id)
        await self._ws_client.subscribe_candles(raw_symbol, str(interval))

    async def _subscribe_instrument_status(self, command: SubscribeInstrumentStatus) -> None:
        pass

    async def _subscribe_instrument_close(self, command: SubscribeInstrumentClose) -> None:
        pass

    async def _subscribe_option_greeks(self, command: SubscribeOptionGreeks) -> None:
        self._log.warning("Bitget option greeks subscriptions are not supported")

    async def _unsubscribe(self, command: UnsubscribeData) -> None:
        pass

    async def _unsubscribe_instruments(self, command: UnsubscribeInstruments) -> None:
        pass

    async def _unsubscribe_instrument(self, command: UnsubscribeInstrument) -> None:
        pass

    async def _unsubscribe_order_book_deltas(self, command: UnsubscribeOrderBook) -> None:
        pass

    async def _unsubscribe_order_book_depth(self, command: UnsubscribeOrderBook) -> None:
        pass

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        pass

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        pass

    async def _unsubscribe_mark_prices(self, command: UnsubscribeMarkPrices) -> None:
        pass

    async def _unsubscribe_index_prices(self, command: UnsubscribeIndexPrices) -> None:
        pass

    async def _unsubscribe_funding_rates(self, command: UnsubscribeFundingRates) -> None:
        pass

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        pass

    async def _unsubscribe_instrument_status(self, command: UnsubscribeInstrumentStatus) -> None:
        pass

    async def _unsubscribe_instrument_close(self, command: UnsubscribeInstrumentClose) -> None:
        pass

    async def _unsubscribe_option_greeks(self, command: UnsubscribeOptionGreeks) -> None:
        pass

    async def _request(self, request: RequestData) -> None:
        self._log.warning(f"Generic Bitget data request not implemented: {request}")

    async def _request_instrument(self, request: RequestInstrument) -> None:
        instrument = await self._ensure_instrument(request.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find Bitget instrument {request.instrument_id}")
            return

        self._handle_instrument(
            instrument,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_instruments(self, request: RequestInstruments) -> None:
        await self._instrument_provider.load_all_async(request.params)
        self._cache_instruments()

        self._handle_instruments(
            request.venue,
            list(self._instrument_provider.get_all().values()),
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        self._log.warning("Cannot request historical quotes: not published by Bitget")

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None:
        instrument = await self._ensure_instrument(request.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find Bitget instrument {request.instrument_id}")
            return

        limit = request.limit if request.limit else None
        pyo3_trades = await self._http_client.request_trades(
            self._product_type,
            nautilus_pyo3.InstrumentId.from_str(request.instrument_id.value),
            start=ensure_pydatetime_utc(request.start),
            end=ensure_pydatetime_utc(request.end),
            limit=limit,
        )
        trades = TradeTick.from_pyo3_list(pyo3_trades)

        self._handle_trade_ticks(
            request.instrument_id,
            trades,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_funding_rates(self, request: RequestFundingRates) -> None:
        instrument = await self._ensure_instrument(request.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find Bitget instrument {request.instrument_id}")
            return

        limit = request.limit if request.limit else None
        pyo3_funding_rates = await self._http_client.request_funding_rates(
            self._product_type,
            nautilus_pyo3.InstrumentId.from_str(request.instrument_id.value),
            start=ensure_pydatetime_utc(request.start),
            end=ensure_pydatetime_utc(request.end),
            limit=limit,
        )
        funding_rates = FundingRateUpdate.from_pyo3_list(pyo3_funding_rates)

        self._handle_funding_rates(
            request.instrument_id,
            funding_rates,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        if request.bar_type.is_internally_aggregated():
            self._log.error(
                f"Cannot request {request.bar_type} bars: "
                "only historical bars with EXTERNAL aggregation available from Bitget",
            )
            return

        if not request.bar_type.spec.is_time_aggregated():
            self._log.error(
                f"Cannot request {request.bar_type} bars: only time bars are aggregated by Bitget",
            )
            return

        if request.bar_type.spec.price_type != PriceType.LAST:
            self._log.error(
                f"Cannot request {request.bar_type} bars: "
                "only historical bars for LAST price type available from Bitget",
            )
            return

        instrument = await self._ensure_instrument(request.bar_type.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find Bitget instrument {request.bar_type.instrument_id}")
            return

        pyo3_instrument_id = nautilus_pyo3.InstrumentId.from_str(
            request.bar_type.instrument_id.value,
        )
        pyo3_bar_type = nautilus_pyo3.BarType.from_str(str(request.bar_type))

        pyo3_bars = await self._http_client.request_bars(
            product_type=self._product_type,
            instrument_id=pyo3_instrument_id,
            bar_type=pyo3_bar_type,
            start=ensure_pydatetime_utc(request.start),
            end=ensure_pydatetime_utc(request.end),
            limit=request.limit,
            timestamp_on_close=request.params.get("timestamp_on_close", False)
            if request.params
            else False,
        )
        bars = Bar.from_pyo3_list(pyo3_bars)

        self._handle_bars(
            request.bar_type,
            bars,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_forward_prices(self, request: RequestForwardPrices) -> None:
        self._log.warning("Bitget forward prices are not supported")
        self._handle_forward_prices([], request.id, request.params or {})

    async def _request_order_book_snapshot_response(
        self,
        instrument_id: InstrumentId,
        limit: int,
        correlation_id,
        start,
        end,
        params: dict[str, object] | None,
    ) -> None:
        instrument = await self._ensure_instrument(instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find Bitget instrument {instrument_id}")
            return

        depth = limit if limit and limit > 0 else None
        pyo3_deltas = await self._http_client.request_orderbook_snapshot(
            self._product_type,
            nautilus_pyo3.InstrumentId.from_str(instrument_id.value),
            limit=depth,
            ts_init_ns=self._clock.timestamp_ns(),
        )
        deltas = OrderBookDeltas.from_pyo3(pyo3_deltas)

        data_type = DataType(
            OrderBookDeltas,
            metadata={"instrument_id": instrument_id},
        )
        self._handle_data_response(
            data_type=data_type,
            data=[deltas],
            correlation_id=correlation_id,
            start=start,
            end=end,
            params=params,
        )

    async def _request_order_book_deltas(self, request: RequestOrderBookDeltas) -> None:
        await self._request_order_book_snapshot_response(
            instrument_id=request.instrument_id,
            limit=request.limit,
            correlation_id=request.id,
            start=request.start,
            end=request.end,
            params=request.params,
        )

    async def _request_order_book_depth(self, request: RequestOrderBookDepth) -> None:
        self._log.warning("Bitget historical order book depth is not exposed")

    async def _request_order_book_snapshot(self, request: RequestOrderBookSnapshot) -> None:
        await self._request_order_book_snapshot_response(
            instrument_id=request.instrument_id,
            limit=request.limit,
            correlation_id=request.id,
            start=request.start,
            end=request.end,
            params=request.params,
        )
