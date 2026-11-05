"""BTC/LIGHT public and private channels through one native NT node."""

import asyncio
import hashlib
import hmac
import runpy
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

from aiohttp import web


LocalGate = runpy.run_path(str(Path(__file__).with_name("protocol_server.py")))["LocalGate"]
PAIRS = {
    "BTC_USDT": {"id": "BTC_USDT", "base": "BTC", "quote": "USDT", "precision": 2,
                 "amount_precision": 6, "trade_status": "tradable", "min_quote_amount": "1"},
    "LIGHT_USDT": {"id": "LIGHT_USDT", "base": "LIGHT", "quote": "USDT", "precision": 5,
                   "amount_precision": 2, "trade_status": "tradable", "min_quote_amount": "1"},
}


class MultiGate(LocalGate):
    def __init__(self):
        super().__init__()
        self.light = Decimal(10000)
        self.fallback_reads = 0

    def balances(self):
        rows = []
        for code, total in [("BTC", self.base), ("LIGHT", self.light), ("USDT", self.quote)]:
            locked = sum((
                Decimal(o["left"]) * (Decimal(o["price"]) if code == "USDT" else 1)
                for o in self.orders.values() if o["status"] == "open" and (
                    (code == "USDT" and o["side"] == "buy") or
                    (o["currency_pair"] == code + "_USDT" and o["side"] == "sell")
                )
            ), Decimal(0))
            rows.append({"currency": code, "available": str(total - locked), "locked": str(locked)})
        return rows

    def authenticate(self, request):
        timestamp = request.headers["Timestamp"]
        message = "\n".join(("GET", request.path, request.query_string,
                             hashlib.sha512(b"").hexdigest(), timestamp))
        assert request.headers["KEY"] == "test-key"
        assert request.headers["SIGN"] == hmac.new(
            b"test-secret", message.encode(), hashlib.sha512,
        ).hexdigest()

    async def http(self, request):
        tail = request.match_info["tail"]
        if tail.startswith("spot/currency_pairs/"):
            return web.json_response(PAIRS[tail.rsplit("/", 1)[1]])
        if tail == "spot/order_book":
            symbol = request.query["currency_pair"]
            bid, ask = ("99990", "100010") if symbol == "BTC_USDT" else ("0.123", "0.12345")
            return web.json_response({"id": self.sequence, "bids": [[bid, "100"]],
                                      "asks": [[ask, "200.5"]]})
        if request.method == "GET" and tail in {"spot/orders", "spot/open_orders", "spot/my_trades"}:
            self.authenticate(request)
            symbol = request.query.get("currency_pair")
            if tail == "spot/my_trades":
                oid = request.query.get("order_id")
                return web.json_response([f for f in self.fills if f["currency_pair"] == symbol
                                          and (oid is None or f["order_id"] == oid)])
            if tail == "spot/open_orders":
                self.fallback_reads += 1
                return web.json_response([
                    {"currency_pair": p, "orders": [o for o in self.orders.values()
                                                     if o["currency_pair"] == p and o["status"] == "open"]}
                    for p in PAIRS
                ])
            status = request.query["status"]
            if symbol == "LIGHT_USDT" and status == "open":
                return web.json_response({"label": "INVALID_PARAM_VALUE"}, status=400)
            assert status == "finished" or not {"from", "to"}.intersection(request.query)
            return web.json_response([o for o in self.orders.values() if o["currency_pair"] == symbol
                                      and (o["status"] == "open") == (status == "open")])
        return await super().http(request)

    async def push_books(self):
        while True:
            await asyncio.sleep(0.15)
            self.sequence += 1
            for index, symbol in enumerate(PAIRS):
                bid, ask = ("99990", "100010.00") if symbol == "BTC_USDT" else ("0.123", "0.12345")
                stamp = int(time.time() * 1000)
                await self.broadcast("spot.order_book_update", {
                    "s": symbol, "full": True, "l": "100", "u": self.sequence, "t": stamp,
                    "b": [[bid, "100"]], "a": [[ask, "200.5"]],
                })
                await self.broadcast("spot.book_ticker", {"s": symbol, "t": stamp,
                                                         "b": bid, "a": ask, "B": "100", "A": "200.5"})
                await self.broadcast("spot.trades", {"currency_pair": symbol,
                    "id": str(self.sequence * 2 + index), "price": bid, "amount": "1",
                    "side": "buy", "create_time_ms": str(stamp)})

    async def fill_base(self, oid, amount):
        row = self.orders[oid]
        fee = (amount * Decimal("0.0003")).quantize(Decimal("0.00000001"))
        currency = PAIRS[row["currency_pair"]]["base"]
        if currency == "BTC":
            self.base += amount - fee
        else:
            self.light += amount - fee
        self.quote -= amount * Decimal(row["price"])
        row["left"] = str(Decimal(row["left"]) - amount)
        fill = {"id": f"fill-{len(self.fills) + 1}", "order_id": oid, "text": row["text"],
                "currency_pair": row["currency_pair"], "price": row["price"], "amount": str(amount),
                "side": "buy", "role": "maker", "fee": str(fee), "fee_currency": currency,
                "create_time_ms": str(int(time.time() * 1000))}
        self.fills.append(fill)
        await self.broadcast("spot.orders", [self.order_notification(row)])
        await self.broadcast("spot.usertrades", [fill, fill])


def scenario():  # noqa: C901 (two-pair native lifecycle and settlement assertions)
    from nautilus_trader.adapters.gateio import GateioDataClientConfig
    from nautilus_trader.adapters.gateio import GateioExecClientConfig
    from nautilus_trader.adapters.gateio import GateioLiveDataClientFactory
    from nautilus_trader.adapters.gateio import GateioLiveExecClientFactory
    from nautilus_trader.config import InstrumentProviderConfig
    from nautilus_trader.config import LiveExecEngineConfig
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.config import StrategyConfig
    from nautilus_trader.config import TradingNodeConfig
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.enums import BookType
    from nautilus_trader.model.enums import OrderSide
    from nautilus_trader.model.events import OrderFilled
    from nautilus_trader.model.identifiers import AccountId
    from nautilus_trader.model.identifiers import InstrumentId
    from nautilus_trader.model.objects import Currency
    from nautilus_trader.model.objects import Money
    from nautilus_trader.trading.strategy import Strategy

    ids = [InstrumentId.from_str(p + ".GATEIO") for p in PAIRS]
    counts = {str(i): {"books": 0, "quotes": 0, "trades": 0} for i in ids}
    class Maker(Strategy):
        def on_start(self):
            for iid in ids:
                self.subscribe_order_book_deltas(iid, BookType.L2_MBP, managed=True)
                self.subscribe_quote_ticks(iid)
                self.subscribe_trade_ticks(iid)
                instrument = self.cache.instrument(iid)
                for side in [OrderSide.BUY, OrderSide.SELL]:
                    price = (99900 if side == OrderSide.BUY else 100100) if iid == ids[0] else (
                        0.122 if side == OrderSide.BUY else 0.125
                    )
                    self.submit_order(self.order_factory.limit(
                        instrument_id=iid, order_side=side, price=instrument.make_price(price),
                        quantity=instrument.make_qty(0.0001 if iid == ids[0] else 100), post_only=True,
                    ))
        def on_quote_tick(self, tick):
            assert tick.bid_price.precision == tick.ask_price.precision
            assert tick.bid_size.precision == tick.ask_size.precision
            counts[str(tick.instrument_id)]["quotes"] += 1
        def on_trade_tick(self, tick):
            counts[str(tick.instrument_id)]["trades"] += 1
        def on_order_book_deltas(self, data):
            counts[str(data.instrument_id)]["books"] += 1

    async def wait_for(condition):
        async with asyncio.timeout(25):
            while not condition():
                await asyncio.sleep(0.02)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    nodes = []
    async def run():
        server = MultiGate()
        await server.start()
        options = {"base_url_http": server.url, "base_url_ws": server.ws_url,
                   "instrument_provider": InstrumentProviderConfig(load_ids=frozenset(ids))}
        node = TradingNode(TradingNodeConfig(
            logging=LoggingConfig(log_level="ERROR"), timeout_post_stop=0.1,
            exec_engine=LiveExecEngineConfig(open_check_interval_secs=0, position_check_interval_secs=0),
            data_clients={"GATEIO": GateioDataClientConfig(**options)},
            exec_clients={"GATEIO": GateioExecClientConfig(**options, api_key="test-key", api_secret="test-secret")},
        ), loop=loop)
        nodes.append(node)
        node.add_data_client_factory("GATEIO", GateioLiveDataClientFactory)
        node.add_exec_client_factory("GATEIO", GateioLiveExecClientFactory)
        strategy = Maker(StrategyConfig(use_uuid_client_order_ids=True))
        node.trader.add_strategy(strategy)
        node.build()
        events = []
        node.kernel.msgbus.subscribe(f"events.order.{strategy.id}", events.append)
        task = asyncio.create_task(node.run_async())
        try:
            await wait_for(lambda: len(node.kernel.cache.orders_open()) == 4 and
                           all(all(c.values()) for c in counts.values()))
            for iid in ids:
                order = next(o for o in node.kernel.cache.orders_open(instrument_id=iid)
                             if o.side == OrderSide.BUY)
                await server.fill_base(str(order.venue_order_id), Decimal("0.00004") if iid == ids[0] else Decimal("12.3"))
            await wait_for(lambda: sum(isinstance(e, OrderFilled) for e in events) == 2)
            for socket, _ in list(server.sockets):
                await socket.close()
            before = {k: v["quotes"] for k, v in counts.items()}
            await wait_for(lambda: all(v["quotes"] > before[k] + 2 for k, v in counts.items()))
            await asyncio.sleep(1)
            assert sum(isinstance(e, OrderFilled) for e in events) == 2
            assert {str(e.commission.currency) for e in events if isinstance(e, OrderFilled)} == {"BTC", "LIGHT"}
            for iid in ids:
                strategy.cancel_all_orders(iid)
            await wait_for(lambda: not node.kernel.cache.orders_open() and
                           all(o["status"] != "open" for o in server.orders.values()))
            account = node.kernel.cache.account(AccountId("GATEIO-master"))
            await wait_for(lambda: all(account.balance_locked(Currency.from_str(c)).as_decimal() == 0
                                      for c in ["BTC", "LIGHT", "USDT"]))
            for code, total in [("BTC", server.base), ("LIGHT", server.light), ("USDT", server.quote)]:
                assert account.balance_total(Currency.from_str(code)) == Money.from_decimal(total, Currency.from_str(code))
            assert server.fallback_reads >= 2
            assert not node.kernel.cache.orders_inflight()
        finally:
            await node.stop_async()
            await asyncio.wait_for(task, 20)
            await server.stop()
    try:
        loop.run_until_complete(run())
    finally:
        for node in nodes:
            node.dispose()
        loop.close()


def test_native_two_pair_quotes_partial_fills_reconnect_and_account_scoped_cancellation():
    script = f"import runpy; runpy.run_path({__file__!r})['scenario']()"
    result = subprocess.run(  # noqa: S603 (fixed authenticated local protocol server)
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
