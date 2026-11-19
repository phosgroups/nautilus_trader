"""Sequence-checked Gate spot L2 callbacks for NT v1 managed books."""

import asyncio
from decimal import Decimal

from nautilus_trader.adapters.gateio.config import GATEIO
from nautilus_trader.adapters.gateio.history import candle_bar
from nautilus_trader.adapters.gateio.history import candle_interval
from nautilus_trader.adapters.gateio.history import spot_bars
from nautilus_trader.adapters.gateio.history import spot_trades
from nautilus_trader.adapters.gateio.parsing import public_quote_tick
from nautilus_trader.adapters.gateio.parsing import public_trade_tick
from nautilus_trader.adapters.gateio.websocket import GateioWebSocket
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import NULL_ORDER
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import OrderBookDepth10
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class BookGap(ValueError):
    pass


class GateBook:
    """Buffer WS updates until a REST snapshot and its bridging update agree."""

    def __init__(self, depth, max_buffer=10000, cache_depth=100):
        self.depth, self.max_buffer = depth, max_buffer
        self.publish_deltas = True
        self.publish_depth = False
        self.cache_depth = cache_depth
        self.bids, self.asks = {}, {}
        self.sequence = None
        self.ready = False
        self.buffer = []
        self.boundaries = [None, None]

    @staticmethod
    def levels(rows):
        result = {}
        for price, size in rows:
            price, size = Decimal(price), Decimal(size)
            if not price.is_finite() or not size.is_finite() or price <= 0 or size < 0:
                raise ValueError("Invalid Gate book price or size")
            if price in result:
                raise ValueError("Duplicate Gate price in one update")
            result[price] = size
        return result

    def snapshot(self, raw):
        self.sequence = int(raw["id"])
        self.bids = {p: q for p, q in self.levels(raw["bids"]).items() if q}
        self.asks = {p: q for p, q in self.levels(raw["asks"]).items() if q}
        self.ready = False
        self._bound_cache()

    def _bound_cache(self):
        for index, (levels, reverse) in enumerate(((self.bids, True), (self.asks, False))):
            prices = sorted(levels, reverse=reverse)
            self.boundaries[index] = (
                prices[self.cache_depth - 1] if len(prices) >= self.cache_depth else None
            )
            keep = set(prices[: self.cache_depth])
            for price in list(levels):
                if price not in keep:
                    del levels[price]
        self._validate()

    def _validate(self):
        if self.bids and self.asks and max(self.bids) >= min(self.asks):
            raise BookGap("Crossed Gate book")

    def visible(self):
        return tuple(
            {p: levels[p] for p in sorted(levels, reverse=reverse)[: self.depth]}
            for levels, reverse in ((self.bids, True), (self.asks, False))
        )

    def update(self, raw):
        if raw.get("full") is True:
            last = int(raw["u"])
            if self.sequence is not None and last < self.sequence:
                return None
            if int(raw.get("l", self.cache_depth)) < max(self.depth, 10 if self.publish_depth else 0):
                raise BookGap("Gate channel depth is smaller than the requested visible depth")
            # A full image replaces the cache and is valid across sequence gaps.
            self.snapshot({"id": last, "bids": raw["b"], "asks": raw["a"]})
            self.ready = True
            return True, self._changes(({}, {}), True)
        if self.sequence is None:
            if len(self.buffer) >= self.max_buffer:
                raise BookGap("Gate snapshot buffer overflow")
            self.buffer.append(raw)
            return None
        first, last = int(raw["U"]), int(raw["u"])
        if first > last:
            raise BookGap("Invalid Gate update sequence range")
        if last <= self.sequence:
            return None
        if first > self.sequence + 1:
            raise BookGap("Missing Gate order book updates")
        before = self.visible()
        self._apply(raw)
        self.sequence = last
        initial, self.ready = not self.ready, True
        return initial, self._changes(before, initial)

    def _apply(self, raw):
        for index, (levels, rows) in enumerate(((self.bids, raw["b"]), (self.asks, raw["a"]))):
            for price, quantity in self.levels(rows).items():
                if quantity:
                    levels[price] = quantity
                else:
                    levels.pop(price, None)
            if self.boundaries[index] is not None and not levels:
                raise BookGap("Gate visible book exhausted its known snapshot coverage")
            if len(levels) > self.cache_depth:
                prices = sorted(levels, reverse=index == 0)
                self.boundaries[index] = prices[self.cache_depth - 1]
                for price in prices[self.cache_depth :]:
                    del levels[price]
        self._validate()

    def depth10(self, instrument_id, ts_event, ts_init):
        sides = []
        for index, (levels, side) in enumerate(((self.bids, OrderSide.BUY), (self.asks, OrderSide.SELL))):
            prices = sorted(levels, reverse=index == 0)[:10]
            boundary = self.boundaries[index]
            if boundary is not None and (
                len(prices) < 10 or (prices[-1] < boundary if index == 0 else prices[-1] > boundary)
            ):
                raise BookGap("Gate ten-level book exhausted known snapshot coverage")
            orders = [BookOrder(side, Price.from_str(f"{p:f}"), Quantity.from_str(f"{levels[p]:f}"), 0)
                      for p in prices]
            sides.append(orders + [NULL_ORDER] * (10 - len(orders)))
        return OrderBookDepth10(
            instrument_id, *sides, [0] * 10, [0] * 10, 32 | 128, self.sequence, ts_event, ts_init,
        )

    def _changes(self, before, initial):
        changes = []
        for side, old, new in (
            (OrderSide.BUY, before[0], self.visible()[0]),
            (OrderSide.SELL, before[1], self.visible()[1]),
        ):
            for price in new if initial else old.keys() | new.keys():
                quantity = new.get(price, Decimal(0))
                if initial or old.get(price) != quantity:
                    changes.append((side, price, quantity))
        return changes


def book_deltas(instrument_id, book, snapshot, changes, sequence, ts_event, now):
    rows = []
    if snapshot:
        rows.append((BookAction.CLEAR, OrderSide.NO_ORDER_SIDE, Decimal(0), Decimal(0)))
    rows.extend(
        (BookAction.UPDATE if q else BookAction.DELETE, side, p, q) for side, p, q in changes
    )
    if not rows:
        # A sequence-advancing update still proves freshness when prices
        # and sizes are unchanged. Do not silently turn a quiet book stale.
        if book.ready and book.bids:
            price = max(book.bids)
            rows.append((BookAction.UPDATE, OrderSide.BUY, price, book.bids[price]))
    if not rows:
        return
    deltas = []
    for index, (action, side, price, size) in enumerate(rows):
        flags = (32 if snapshot and changes else 0) | (128 if index == len(rows) - 1 else 0)
        deltas.append(
            OrderBookDelta(
                instrument_id,
                action,
                BookOrder(
                    side,
                    Price.from_str(f"{price:f}"),
                    Quantity.from_str(f"{size:f}"),
                    0,
                ),
                flags,
                sequence,
                ts_event,
                now,
            )
        )
    return OrderBookDeltas(instrument_id, deltas)


class GateioDataClient(LiveMarketDataClient):
    def __init__(self, loop, client, msgbus, cache, clock, instrument_provider, config, name=None):
        super().__init__(
            loop=loop,
            client_id=ClientId(name or "GATEIO"),
            venue=GATEIO,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        if not 1 <= config.book_depth <= 100 or config.max_book_buffer <= 0:
            raise ValueError(
                "Gate 100ms channel supports 1-100 visible levels and a positive buffer limit"
            )
        self.http, self.config = client, config
        self.books, self.resyncs = {}, {}
        self._subscription_locks = {}
        self._candles = {}
        self.public_subscriptions = {"spot.trades": set(), "spot.book_ticker": set()}
        self.ws = GateioWebSocket(
            loop,
            clock,
            config.base_url_ws,
            self._message,
            self._reconnect,
            config.proxy_url,
            error_handler=self._log.error,
            invalidate=self._invalidate_books,
        )

    async def _connect(self):
        await self._instrument_provider.initialize()
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)
        await self.ws.connect()

    async def _disconnect(self):
        try:
            await self.ws.disconnect()
        finally:
            for task in self.resyncs.values():
                task.cancel()
            await asyncio.gather(*self.resyncs.values(), return_exceptions=True)
            self.resyncs.clear()

    def _emit(self, instrument_id, snapshot, changes, sequence, ts_event):
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            raise ValueError("Gate instrument was not initialized")
        now = self._clock.timestamp_ns()
        book = self.books[instrument_id]
        book.ts_event = ts_event
        depth = book.depth10(instrument_id, ts_event, now) if book.publish_depth and book.ready else None
        if book.publish_deltas:
            deltas = book_deltas(instrument_id, book, snapshot, changes, sequence, ts_event, now)
            if deltas is not None:
                self._handle_data(deltas)
        if depth is not None:
            self._handle_data(depth)

    def _reset(self, instrument_id):
        old = self.books.get(instrument_id)
        book = GateBook(self.config.book_depth, self.config.max_book_buffer)
        if old is not None:
            book.publish_deltas, book.publish_depth = old.publish_deltas, old.publish_depth
        self.books[instrument_id] = book
        self._emit(instrument_id, True, [], 0, self._clock.timestamp_ns())

    def _invalidate_books(self):
        for instrument_id in list(self.books):
            self._reset(instrument_id)

    def _schedule_resync(self, instrument_id):
        task = self.resyncs.get(instrument_id)
        if task is None or task.done():
            self.resyncs[instrument_id] = self.create_task(self._resync_loop(instrument_id))

    async def _resync_loop(self, instrument_id):
        failures = 0
        while instrument_id in self.books:
            book = self.books[instrument_id]
            try:
                await self._resync(instrument_id)
                if self.books.get(instrument_id) is book:
                    return  # Snapshot is installed; wait for its bridging delta.
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._log.warning(f"Gate book recovery failed; retrying: {type(error).__name__}")
                failures += 1
                await asyncio.sleep(min(0.25 * 2 ** min(failures - 1, 4), 2))

    async def _fetch_book(self, instrument_id, depth):
        return await self.http.request("GET", "/spot/order_book", {
            "currency_pair": instrument_id.symbol.value, "limit": depth, "with_id": "true",
        })

    async def _resync(self, instrument_id):
        book = self.books[instrument_id]
        for attempt in range(self.config.max_retries + 1):
            snapshot = await self._fetch_book(instrument_id, 100)
            if self.books.get(instrument_id) is not book or book.ready:
                return
            book.snapshot(snapshot)
            pending, book.buffer = book.buffer, []
            try:
                for row in pending:
                    changes = book.update(row)
                    if changes:
                        self._emit(
                            instrument_id, *changes, book.sequence, int(row["t"]) * 1_000_000
                        )
                return
            except BookGap:
                self._reset(instrument_id)
                book = self.books[instrument_id]
                # REST may lag the WS feed. Keep the buffered interval while
                # fetching a newer snapshot so the next attempt can bridge it.
                # Dropping it can make every fresh REST snapshot lag forever.
                book.buffer.extend(pending)
                if attempt < self.config.max_retries:
                    await asyncio.sleep(min(0.25 * 2**attempt, 2))
        raise BookGap("Gate REST snapshot could not bridge WebSocket updates")

    async def _message(self, message):
        channel = message["channel"]
        if channel == "spot.candlesticks":
            self._candle_update(message["result"])
            return
        if channel in {"spot.trades", "spot.book_ticker"}:
            self._public_tick(channel, message["result"])
            return
        if message["channel"] != "spot.order_book_update":
            return
        row = message["result"]
        if not isinstance(row, dict) or not isinstance(row.get("s"), str):
            self._log.warning("Discarding Gate book update without a valid symbol")
            return
        instrument_id = next((i for i in self.books if i.symbol.value == row["s"]), None)
        if instrument_id is None:
            return
        book = self.books[instrument_id]
        try:
            result = book.update(row)
            if result:
                self._emit(instrument_id, *result, book.sequence, int(row["t"]) * 1_000_000)
            elif book.sequence is None:
                self._schedule_resync(instrument_id)
        except (ArithmeticError, BookGap, ValueError, KeyError, TypeError) as error:
            self._reset(instrument_id)
            if isinstance(error, BookGap) and not row.get("full"):
                self.books[instrument_id].buffer.append(row)
            self._schedule_resync(instrument_id)

    def _public_tick(self, channel, row):
        if not isinstance(row, dict):
            self._log.warning(f"Discarding invalid Gate {channel} update: expected an object")
            return
        symbol = row.get("currency_pair") if channel == "spot.trades" else row.get("s")
        instrument_id = next(
            (i for i in self.public_subscriptions[channel] if i.symbol.value == symbol), None
        )
        if instrument_id is None:
            return
        instrument = self._instrument_provider.find(instrument_id)
        parser = public_trade_tick if channel == "spot.trades" else public_quote_tick
        try:
            tick = parser(row, instrument, self._clock.timestamp_ns())
        except (ArithmeticError, KeyError, TypeError, ValueError) as error:
            # A malformed independent trade/quote must not reset healthy L2
            # subscriptions. Book gaps and private execution errors still recover.
            self._log.warning(
                f"Discarding invalid Gate {channel} update for {instrument_id}: "
                f"{type(error).__name__}"
            )
            return
        if tick is not None:
            self._handle_data(tick)

    async def _public_subscription(self, channel, instrument_id, subscribe):
        async with self._subscription_lock(channel, instrument_id):
            await self._change_public_subscription(channel, instrument_id, subscribe)

    def _subscription_lock(self, channel, instrument_id):
        return self._subscription_locks.setdefault((channel, instrument_id), asyncio.Lock())

    async def _change_public_subscription(self, channel, instrument_id, subscribe):
        if self._instrument_provider.find(instrument_id) is None:
            raise ValueError("Gate subscription instrument was not loaded")
        selected = self.public_subscriptions[channel]
        payload = [instrument_id.symbol.value]
        if subscribe:
            if instrument_id in selected:
                return
            selected.add(instrument_id)
            try:
                await self.ws.subscribe(channel, payload)
            except BaseException:
                selected.discard(instrument_id)
                raise
        else:
            if instrument_id not in selected:
                return
            await self.ws.unsubscribe(channel, payload)
            selected.discard(instrument_id)

    async def _subscribe_trade_ticks(self, command):
        await self._public_subscription("spot.trades", command.instrument_id, True)

    async def _unsubscribe_trade_ticks(self, command):
        await self._public_subscription("spot.trades", command.instrument_id, False)

    async def _subscribe_quote_ticks(self, command):
        await self._public_subscription("spot.book_ticker", command.instrument_id, True)

    async def _unsubscribe_quote_ticks(self, command):
        await self._public_subscription("spot.book_ticker", command.instrument_id, False)

    def _candle_update(self, row):
        if not isinstance(row, dict) or row.get("w") is not True:
            return
        name = row.get("n")
        if name not in self._candles:
            return
        bar_type, last_open = self._candles[name]
        try:
            opened = int(row["t"])
            if last_open is not None and opened <= last_open:
                return
            bar = candle_bar(
                bar_type, opened, row["o"], row["h"], row["l"], row["c"], row["a"],
                self._clock.timestamp_ns(),
            )
        except (ArithmeticError, KeyError, ValueError, TypeError) as error:
            self._log.warning(f"Discarding invalid Gate candlestick for {name}: {type(error).__name__}")
            return
        self._handle_data(bar)
        self._candles[name] = (bar_type, opened)

    async def _subscribe_bars(self, command):
        bar_type = command.bar_type
        interval = candle_interval(bar_type)
        instrument = await self._history_instrument(bar_type.instrument_id)
        raw = instrument.raw_symbol.value
        name = f"{interval}_{raw}"
        async with self._subscription_lock("spot.candlesticks", name):
            if name in self._candles:
                return
            self._candles[name] = (bar_type, None)
            try:
                await self.ws.subscribe("spot.candlesticks", [interval, raw])
            except BaseException:
                self._candles.pop(name, None)
                raise

    async def _unsubscribe_bars(self, command):
        interval = candle_interval(command.bar_type)
        raw = command.bar_type.instrument_id.symbol.value
        name = f"{interval}_{raw}"
        async with self._subscription_lock("spot.candlesticks", name):
            if name in self._candles:
                await self.ws.unsubscribe("spot.candlesticks", [interval, raw])
                self._candles.pop(name)

    async def _reconnect(self):
        instruments, tasks = list(self.books), []
        # Clear every managed book before any network wait. A failed snapshot
        # for one pair must not leave another pair's old book looking current.
        for instrument_id in instruments:
            task = self.resyncs.pop(instrument_id, None)
            if task and not task.done():
                task.cancel()
                tasks.append(task)
            self._reset(instrument_id)
        await asyncio.gather(*tasks, return_exceptions=True)
        for instrument_id in instruments:
            if instrument_id in self.books:
                await self._resync(instrument_id)

    async def _subscribe_order_book_deltas(self, command):
        async with self._subscription_lock("spot.order_book_update", command.instrument_id):
            await self._subscribe_book(command)

    async def _subscribe_order_book_depth(self, command):
        if command.depth not in (0, 10):
            raise ValueError("Gate OrderBookDepth10 requires ten levels")
        async with self._subscription_lock("spot.order_book_update", command.instrument_id):
            await self._subscribe_book(command, depth=True)

    async def _subscribe_book(self, command, depth=False):
        if command.book_type != BookType.L2_MBP:
            raise ValueError("Gate PMM requires L2_MBP")
        if self._instrument_provider.find(command.instrument_id) is None:
            raise ValueError("Gate subscription instrument was not loaded")
        book = self.books.get(command.instrument_id)
        if book is not None:
            if depth and not book.publish_depth:
                if book.ready:
                    try:
                        data = book.depth10(command.instrument_id, book.ts_event, self._clock.timestamp_ns())
                    except BookGap:
                        book.publish_depth = True
                        self._reset(command.instrument_id)
                        self._schedule_resync(command.instrument_id)
                        return
                    self._handle_data(data)
                book.publish_depth = True
            elif not depth and not book.publish_deltas:
                if book.ready:
                    self._handle_data(book_deltas(
                        command.instrument_id, book, True, book._changes(({}, {}), True),
                        book.sequence, book.ts_event, self._clock.timestamp_ns(),
                    ))
                book.publish_deltas = True
            return
        book = GateBook(self.config.book_depth, self.config.max_book_buffer)
        book.publish_depth, book.publish_deltas = depth, not depth
        self.books[command.instrument_id] = book
        self._emit(command.instrument_id, True, [], 0, self._clock.timestamp_ns())
        try:
            await self.ws.subscribe(
                "spot.order_book_update", [command.instrument_id.symbol.value, "100ms"]
            )
        except BaseException:
            self.books.pop(command.instrument_id, None)
            raise
        self._schedule_resync(command.instrument_id)

    async def _unsubscribe_order_book_deltas(self, command):
        async with self._subscription_lock("spot.order_book_update", command.instrument_id):
            await self._unsubscribe_book(command)

    async def _unsubscribe_order_book_depth(self, command):
        async with self._subscription_lock("spot.order_book_update", command.instrument_id):
            await self._unsubscribe_book(command, depth=True)

    async def _unsubscribe_book(self, command, depth=False):
        book = self.books.get(command.instrument_id)
        if book is None:
            return
        if depth and book.publish_deltas:
            book.publish_depth = False
            return
        if not depth and book.publish_depth:
            book.publish_deltas = False
            return
        await self.ws.unsubscribe(
            "spot.order_book_update", [command.instrument_id.symbol.value, "100ms"]
        )
        task = self.resyncs.pop(command.instrument_id, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.books.pop(command.instrument_id, None)

    async def _request_order_book_snapshot(self, request):
        depth = request.limit or self.config.book_depth
        if not 1 <= depth <= 1000:
            raise ValueError("Gate supports snapshot depth from 1 to 1000")
        instrument = await self._history_instrument(request.instrument_id)
        raw = await self._fetch_book(instrument.id, depth)
        now = self._clock.timestamp_ns()
        event = int(raw["update"]) * 1_000_000 if raw.get("update") is not None else now
        book = GateBook(depth, cache_depth=depth)
        book.snapshot(raw)
        if book.sequence <= 0:
            raise ValueError("Gate snapshot must include a positive update ID")
        deltas = book_deltas(
            instrument.id, book, True, book._changes(({}, {}), True), book.sequence, event, now,
        )
        self._handle_data_response(
            data_type=DataType(OrderBookDeltas, metadata={"instrument_id": instrument.id}),
            data=[deltas], correlation_id=request.id, start=request.start,
            end=request.end, params=request.params,
        )

    async def _history_instrument(self, instrument_id):
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(instrument_id)
            instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            raise ValueError(f"Unknown Gate instrument: {instrument_id}")
        return instrument

    async def _request_bars(self, request):
        instrument = await self._history_instrument(request.bar_type.instrument_id)
        bars = await spot_bars(self.http, instrument, request, self._clock.timestamp_ns())
        self._handle_bars(request.bar_type, bars, request.id, request.start, request.end, request.params)

    async def _request_trade_ticks(self, request):
        instrument = await self._history_instrument(request.instrument_id)
        trades = await spot_trades(self.http, instrument, request, self._clock.timestamp_ns())
        self._handle_trade_ticks(instrument.id, trades, request.id, request.start, request.end, request.params)

    async def _request_instrument(self, request):
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is not None:
            self._handle_instrument(
                instrument, request.id, request.start, request.end, request.params
            )

    async def _request_instruments(self, request):
        self._handle_instruments(
            list(self._instrument_provider.get_all().values()),
            request.id,
            request.start,
            request.end,
            request.params,
        )
