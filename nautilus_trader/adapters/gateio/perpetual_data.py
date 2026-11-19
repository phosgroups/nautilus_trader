"""USDT-settled perpetual market data for Nautilus v1."""

from decimal import Decimal

from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.adapters.gateio.history import SECOND
from nautilus_trader.adapters.gateio.history import bounds
from nautilus_trader.adapters.gateio.history import candle_bar
from nautilus_trader.adapters.gateio.history import candle_interval
from nautilus_trader.adapters.gateio.parsing import event_ns
from nautilus_trader.adapters.gateio.parsing import public_quote_tick
from nautilus_trader.model.data import FundingRateUpdate
from nautilus_trader.model.data import IndexPriceUpdate
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def perpetual_trade(row, instrument, now):
    if row["contract"] != instrument.raw_symbol.value:
        raise ValueError("Gate futures trade contract mismatch")
    size = Decimal(str(row["size"]))
    price = Price.from_str(str(row["price"]))
    if not size.is_finite() or not size or price.as_decimal() <= 0:
        raise ValueError("Invalid Gate futures trade price or size")
    if row.get("id") is None or not str(row["id"]).isdecimal():
        raise ValueError("Invalid Gate futures trade ID")
    if row.get("create_time") is None and row.get("create_time_ms") is None:
        raise ValueError("Missing Gate futures trade timestamp")
    return TradeTick(
        instrument.id,
        price,
        Quantity.from_str(format(abs(size), "f")),
        AggressorSide.BUYER if size > 0 else AggressorSide.SELLER,
        TradeId(str(row["id"])),
        event_ns(row, "create_time", now),
        now,
    )


def levels(rows):
    return [(row["p"], format(abs(Decimal(str(row["s"]))), "f")) for row in rows]


class GateioPerpetualDataClient(GateioDataClient):
    def __init__(self, loop, client, msgbus, cache, clock, instrument_provider, config, name=None):
        super().__init__(loop, client, msgbus, cache, clock, instrument_provider, config, name)
        self.ws.headers = [("X-Gate-Size-Decimal", "1")]
        self.public_subscriptions = {"futures.trades": set(), "futures.book_ticker": set()}
        self._ticker_fields = {}

    async def _fetch_book(self, instrument_id, depth):
        instrument = await self._history_instrument(instrument_id)
        raw = await self.http.request(
            "GET",
            "/futures/usdt/order_book",
            {
                "contract": instrument.raw_symbol.value,
                "limit": depth,
                "with_id": "true",
            },
        )
        snapshot = {"id": raw["id"], "bids": levels(raw["bids"]), "asks": levels(raw["asks"])}
        if raw.get("update") is not None:
            # Unlike spot, futures REST uses Unix seconds (possibly fractional).
            snapshot["update"] = Decimal(str(raw["update"])) * 1000
        return snapshot

    async def _message(self, message):
        channel = message["channel"]
        rows = message["result"]
        now = self._clock.timestamp_ns()
        ts = event_ns(message, "time", now)
        for row in rows if isinstance(rows, list) else [rows]:
            if channel == "futures.candlesticks":
                self._candle_update(row, volume_key="v")
                continue
            if not isinstance(row, dict):
                continue
            raw = row.get("contract") or row.get("s")
            if not raw:
                continue
            iid = InstrumentId.from_str(f"{raw}-PERP.GATEIO")
            instrument = self._instrument_provider.find(iid)
            if instrument is None:
                continue
            self._publish_row(channel, instrument, row, ts, now)

    def _publish_row(self, channel, instrument, row, ts, now):
        iid = instrument.id
        try:
            if channel == "futures.order_book_update" and iid in self.books:
                self._book_update(iid, dict(row, b=levels(row["b"]), a=levels(row["a"])))
            elif channel == "futures.tickers":
                self._publish_ticker(instrument, row, ts, now)
            elif iid in self.public_subscriptions.get(channel, ()):
                tick = (
                    perpetual_trade(row, instrument, now)
                    if channel == "futures.trades"
                    else public_quote_tick(row, instrument, now)
                )
                if tick is not None:
                    self._handle_data(tick)
        except (ArithmeticError, KeyError, TypeError, ValueError) as error:
            self._log.warning(f"Discarding invalid Gate {channel} update for {iid}: {error}")
            if channel == "futures.order_book_update" and iid in self.books:
                self._reset(iid)
                self._schedule_resync(iid)

    async def _set_ticker_field(self, iid, field, subscribe):
        instrument = await self._history_instrument(iid)
        payload = [instrument.raw_symbol.value]
        async with self._subscription_lock("futures.tickers", iid):
            fields = self._ticker_fields.get(iid, set())
            if subscribe and field not in fields:
                self._ticker_fields[iid] = fields | {field}
                if not fields:
                    try:
                        await self.ws.subscribe("futures.tickers", payload)
                    except BaseException:
                        self._ticker_fields.pop(iid, None)
                        raise
            elif not subscribe and field in fields:
                if len(fields) == 1:
                    await self.ws.unsubscribe("futures.tickers", payload)
                    self._ticker_fields.pop(iid)
                else:
                    fields.remove(field)

    async def _subscribe_mark_prices(self, command):
        await self._set_ticker_field(command.instrument_id, "mark_price", True)

    async def _unsubscribe_mark_prices(self, command):
        await self._set_ticker_field(command.instrument_id, "mark_price", False)

    async def _subscribe_index_prices(self, command):
        await self._set_ticker_field(command.instrument_id, "index_price", True)

    async def _unsubscribe_index_prices(self, command):
        await self._set_ticker_field(command.instrument_id, "index_price", False)

    async def _subscribe_funding_rates(self, command):
        await self._set_ticker_field(command.instrument_id, "funding_rate", True)

    async def _unsubscribe_funding_rates(self, command):
        await self._set_ticker_field(command.instrument_id, "funding_rate", False)

    def _publish_ticker(self, instrument, row, ts, now):
        for field in self._ticker_fields.get(instrument.id, ()):
            value = row.get(field)
            if value is None:
                continue
            if field == "funding_rate":
                next_apply = row.get("funding_next_apply")
                seconds = instrument.info.get("funding_interval")
                update = FundingRateUpdate(
                    instrument.id,
                    Decimal(str(value)),
                    ts,
                    now,
                    interval=int(seconds) // 60 if seconds else None,
                    next_funding_ns=int(next_apply) * SECOND if next_apply else None,
                )
            else:
                update_type = MarkPriceUpdate if field == "mark_price" else IndexPriceUpdate
                price = Price.from_str(str(value))
                if price.as_decimal() <= 0:
                    continue
                update = update_type(instrument.id, price, ts, now)
            self._handle_data(update)

    async def _request_bars(self, request):
        bt = request.bar_type
        instrument = await self._history_instrument(bt.instrument_id)
        now = self._clock.timestamp_ns()
        start, end = bounds(request, now)
        duration = int(bt.spec.timedelta.total_seconds())
        limit = request.limit or (100 if start is None else 0)
        last = end // SECOND
        first = start // SECOND if start is not None else max(0, last - (limit + 1) * duration)
        interval = candle_interval(bt)
        bars = {}
        while last >= first:
            begin = max(first, last - 2000 * duration + 1)
            rows = await self.http.request(
                "GET",
                "/futures/usdt/candlesticks",
                {
                    "contract": instrument.raw_symbol.value,
                    "interval": interval,
                    "from": begin,
                    "to": last,
                },
            )
            for row in rows:
                t = int(row["t"])
                if (
                    (start is None or t * SECOND >= start)
                    and t * SECOND <= end
                    and (t + duration) * SECOND <= now
                ):
                    bars[t] = candle_bar(
                        bt, t, row["o"], row["h"], row["l"], row["c"], row["v"], now
                    )
            if limit and len(bars) >= limit:
                break
            last = begin - 1
        self._handle_bars(
            bt,
            [bars[t] for t in sorted(bars)[-limit:]],
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_trade_ticks(self, request):
        instrument = await self._history_instrument(request.instrument_id)
        now = self._clock.timestamp_ns()
        start, end = bounds(request, now)
        limit = request.limit or (100 if start is None else 0)
        size = min(limit or 1000, 1000)
        trades = {}
        offset = 0
        previous = None
        while True:
            rows = await self.http.request(
                "GET",
                "/futures/usdt/trades",
                {
                    "contract": instrument.raw_symbol.value,
                    "from": start // SECOND if start is not None else None,
                    "to": end // SECOND + 1,
                    "limit": size,
                    "offset": offset,
                },
            )
            for row in rows:
                tick = perpetual_trade(row, instrument, now)
                if (start is None or tick.ts_event >= start) and tick.ts_event <= end:
                    trades[tick.trade_id] = tick
            if len(rows) < size or (limit and len(trades) >= limit):
                break
            page = tuple(str(row["id"]) for row in rows)
            if page == previous:
                raise ValueError("Gate futures trade history pagination made no progress")
            previous = page
            offset += len(rows)
        ticks = sorted(trades.values(), key=lambda t: (t.ts_event, int(t.trade_id.value)))
        self._handle_trade_ticks(
            instrument.id, ticks[-limit:], request.id, request.start, request.end, request.params
        )

    async def _request_funding_rates(self, request):
        instrument = await self._history_instrument(request.instrument_id)
        now = self._clock.timestamp_ns()
        start, end = bounds(request, now)
        rates = {}
        limit = request.limit or (100 if start is None else 0)
        size = min(limit or 1000, 1000)
        until = end // SECOND
        while start is None or until >= start // SECOND:
            rows = await self.http.request(
                "GET",
                "/futures/usdt/funding_rate",
                {
                    "contract": instrument.raw_symbol.value,
                    "from": start // SECOND if start is not None else None,
                    "to": until,
                    "limit": size,
                },
            )
            for row in rows:
                ts = int(row["t"]) * SECOND
                if (start is None or ts >= start) and ts <= end:
                    rates[ts] = FundingRateUpdate(instrument.id, Decimal(str(row["r"])), ts, now)
            if len(rows) < size or (limit and len(rates) >= limit):
                break
            oldest = min(int(row["t"]) for row in rows)
            if oldest > until:
                raise ValueError("Gate funding history pagination made no progress")
            until = oldest - 1
        self._handle_funding_rates(
            instrument.id,
            [rates[t] for t in sorted(rates)[-limit:]],
            request.id,
            request.start,
            request.end,
            request.params,
        )
