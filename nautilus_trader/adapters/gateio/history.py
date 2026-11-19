"""Historical market data from Gate's public spot endpoints."""

from nautilus_trader.adapters.gateio.parsing import public_trade_tick
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import Bar
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


SECOND = 1_000_000_000
PAGE_SIZE = 1000


def candle_interval(bar_type):
    spec = bar_type.spec
    if bar_type.is_internally_aggregated() or spec.price_type != PriceType.LAST:
        raise ValueError("Gate candlesticks require EXTERNAL LAST bars")
    units = {BarAggregation.MINUTE: "m", BarAggregation.HOUR: "h", BarAggregation.DAY: "d"}
    interval = f"{spec.step}{units.get(spec.aggregation, '')}"
    if interval not in {"1m", "5m", "15m", "30m", "1h", "4h", "8h", "1d", "7d"}:
        raise ValueError(f"Unsupported Gate candlestick interval: {spec}")
    return interval


def bounds(request, now):
    start = dt_to_unix_nanos(request.start) if request.start is not None else None
    end = dt_to_unix_nanos(request.end) if request.end is not None else now
    if start is not None and start > end:
        raise ValueError("Gate history start exceeds end")
    return start, min(now, end)


def candle_bar(bar_type, t, op, hi, lo, cl, base_volume, now):
    return Bar(
        bar_type,
        Price.from_str(str(op)), Price.from_str(str(hi)), Price.from_str(str(lo)),
        Price.from_str(str(cl)), Quantity.from_str(str(base_volume)), int(t) * SECOND, now,
    )


async def spot_bars(http, instrument, request, now):
    interval = candle_interval(request.bar_type)
    duration = int(request.bar_type.spec.timedelta.total_seconds())
    start, end = bounds(request, now)
    # An unbounded request means recent bars. Walk bounded requests backwards so
    # an explicit limit selects the latest eligible closed bars.
    limit = request.limit or (100 if start is None else 0)
    last = end // SECOND
    first = start // SECOND if start is not None else max(0, last - (limit + 1) * duration)
    params = {"currency_pair": instrument.raw_symbol.value, "interval": interval}
    bars = {}
    while last >= first:
        begin = max(first, last - PAGE_SIZE * duration + 1)
        rows = await http.request("GET", "/spot/candlesticks", {**params, "from": begin, "to": last})
        for row in rows:
            t, _, cl, hi, lo, op, volume = row[:7]
            close = row[7] if len(row) > 7 else None
            if close is None:
                closed = (int(t) + duration) * SECOND <= now
            elif str(close).lower() in {"true", "false"}:
                closed = str(close).lower() == "true"
            else:
                raise ValueError("Invalid Gate candle close flag")
            ts = int(t) * SECOND
            if closed and (start is None or ts >= start) and ts <= end:
                bars[ts] = candle_bar(request.bar_type, t, op, hi, lo, cl, volume, now)
        if limit and len(bars) >= limit:
            break
        last = begin - 1
    return [bars[ts] for ts in sorted(bars)[-limit:]]


async def spot_trades(http, instrument, request, now):
    start, end = bounds(request, now)
    limit = request.limit or (100 if start is None else 0)
    page_size = min(limit or PAGE_SIZE, PAGE_SIZE)
    # Gate accepts whole seconds while trades have fractional timestamps.
    last = end // SECOND + 1
    first = start // SECOND if start is not None else max(0, last - 30 * 86400)
    trades = {}
    while last >= first:
        begin = max(first, last - 30 * 86400)
        params = {"currency_pair": instrument.raw_symbol.value, "from": begin, "to": last,
                  "limit": page_size}
        page = 1
        seen = set()
        while True:
            if (page - 1) * page_size > 100_000:
                raise ValueError("Gate trade pagination exceeded its offset limit")
            rows = await http.request("GET", "/spot/trades", {**params, "page": page})
            previous = len(seen)
            for row in rows:
                if row.get("create_time_ms") is None and row.get("create_time") is None:
                    raise ValueError("Missing Gate historical trade time")
                tick = public_trade_tick(row, instrument, now)
                seen.add(tick.trade_id)
                if (start is None or tick.ts_event >= start) and tick.ts_event <= end:
                    trades[tick.trade_id] = tick
            if len(rows) < page_size or (limit and len(trades) >= limit):
                break
            if len(seen) == previous:
                raise ValueError("Gate historical trade pagination made no progress")
            page += 1
        if begin == first or (limit and len(trades) >= limit):
            break
        last = begin  # Overlap the boundary second and deduplicate by trade ID.
    return sorted(trades.values(), key=lambda tick: (tick.ts_event, int(str(tick.trade_id))))[-limit:]
