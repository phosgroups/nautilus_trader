"""Authenticated local Gate protocol server; no exchange network access."""

import asyncio
import hashlib
import hmac
import time
from decimal import Decimal

from aiohttp import web


PAIR = {
    "id": "BTC_USDT",
    "base": "BTC",
    "quote": "USDT",
    "precision": 2,
    "amount_precision": 6,
    "min_base_amount": "0.000001",
    "min_quote_amount": "1",
    "trade_status": "tradable",
}


class LocalGate:
    def __init__(self):
        self.orders, self.fills, self.sockets = {}, [], []
        self.sequence = 100
        self.post_count = 0
        self.cancel_ids = []
        self.authenticated = 0
        self.base, self.quote = Decimal(1), Decimal(100000)
        self.ticker = None
        self.fill_on_cancel = None
        self.reject_next = False
        self.history_available = True

    async def start(self):
        app = web.Application()
        app.router.add_get("/ws", self.websocket)
        app.router.add_route("*", "/api/v4/{tail:.*}", self.http)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/api/v4"
        self.ws_url = f"ws://127.0.0.1:{port}/ws"
        self.ticker = asyncio.create_task(self.push_books())

    async def stop(self):
        self.ticker.cancel()
        await asyncio.gather(self.ticker, return_exceptions=True)
        await self.runner.cleanup()

    def balances(self):
        locked_base = sum(
            (
                Decimal(o["left"])
                for o in self.orders.values()
                if o["status"] == "open" and o["side"] == "sell"
            ),
            Decimal(0),
        )
        locked_quote = sum(
            (
                Decimal(o["left"]) * Decimal(o["price"])
                for o in self.orders.values()
                if o["status"] == "open" and o["side"] == "buy"
            ),
            Decimal(0),
        )
        return [
            {
                "currency": "BTC",
                "available": str(self.base - locked_base),
                "locked": str(locked_base),
            },
            {
                "currency": "USDT",
                "available": str(self.quote - locked_quote),
                "locked": str(locked_quote),
            },
        ]

    async def broadcast(self, channel, result):
        for socket, channels in list(self.sockets):
            if not socket.closed and channel in channels:
                await socket.send_json(
                    {
                        "channel": channel,
                        "event": "update",
                        "time_ms": int(time.time() * 1000),
                        "result": result,
                    }
                )

    async def websocket(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        channels = set()
        self.sockets.append((ws, channels))
        async for message in ws:
            if message.type != web.WSMsgType.TEXT:
                continue
            row = message.json()
            channel, event = row["channel"], row["event"]
            if channel in {"spot.orders", "spot.usertrades", "spot.balances"}:
                signature = hmac.new(
                    b"test-secret",
                    f"channel={channel}&event={event}&time={row['time']}".encode(),
                    hashlib.sha512,
                ).hexdigest()
                assert row["auth"]["KEY"] == "test-key"
                assert row["auth"]["SIGN"] == signature
            if event == "subscribe":
                channels.add(channel)
            else:
                channels.discard(channel)
            await ws.send_json(
                {"id": row.get("id"), "channel": channel, "event": event, "result": {"status": "success"}}
            )
        return ws

    async def push_books(self):
        while True:
            await asyncio.sleep(0.15)
            self.sequence += 1
            await self.broadcast(
                "spot.order_book_update",
                {
                    "s": "BTC_USDT",
                    "U": self.sequence,
                    "u": self.sequence,
                    "t": int(time.time() * 1000),
                    "b": [["99990", "1"]],
                    "a": [["100010", "1"]],
                },
            )

    async def http(self, request):  # noqa: C901 (local protocol router)
        tail = request.match_info["tail"]
        body = await request.read()
        if tail not in {"spot/currency_pairs/BTC_USDT", "spot/order_book"}:
            timestamp = request.headers.get("Timestamp", "")
            wire = "\n".join(
                (
                    request.method,
                    request.path,
                    request.query_string,
                    hashlib.sha512(body).hexdigest(),
                    timestamp,
                )
            )
            signature = hmac.new(b"test-secret", wire.encode(), hashlib.sha512).hexdigest()
            assert request.headers.get("KEY") == "test-key"
            assert request.headers.get("SIGN") == signature
            self.authenticated += 1
        if tail == "spot/currency_pairs/BTC_USDT":
            return web.json_response(PAIR)
        if tail == "spot/order_book":
            return web.json_response(
                {
                    "id": self.sequence,
                    "bids": [["99990", "1"], ["97000", "1"]],
                    "asks": [["100010", "1"], ["103000", "1"]],
                }
            )
        if tail == "spot/accounts":
            return web.json_response(self.balances())
        if tail == "spot/my_trades":
            if not self.history_available:
                return web.json_response([])
            oid = request.query.get("order_id")
            return web.json_response([f for f in self.fills if oid is None or f["order_id"] == oid])
        if tail == "spot/orders" and request.method == "POST":
            row = await request.json()
            assert row["time_in_force"] == "poc"
            assert row["account"] == "spot"
            self.post_count += 1
            oid = str(self.post_count)
            row.update(
                id=oid,
                status="open",
                left=row["amount"],
                create_time_ms=str(int(time.time() * 1000)),
            )
            self.orders[oid] = row
            if self.reject_next:
                self.reject_next = False
                row.update(status="closed", finish_as="poc")
            return web.json_response(row)
        if tail == "spot/orders":
            status = request.query["status"]
            return web.json_response(
                [o for o in self.orders.values() if (o["status"] == "open") == (status == "open")]
            )
        if tail.startswith("spot/orders/"):
            target = tail.rsplit("/", 1)[1]
            row = self.orders.get(target) or next(
                (o for o in self.orders.values() if o["text"] == target),
                None,
            )
            if row is None:
                return web.json_response({"label": "ORDER_NOT_FOUND"}, status=404)
            if request.method == "DELETE":
                if row["status"] != "open":
                    return web.json_response({"label": "ORDER_NOT_FOUND"}, status=404)
                if self.fill_on_cancel == row["id"]:
                    await self.fill(row["id"], Decimal(row["left"]), publish=False)
                    row.update(status="closed", finish_as="filled")
                    return web.json_response({"label": "ORDER_NOT_FOUND"}, status=404)
                elif row["status"] == "open":
                    row["status"] = "cancelled"
                self.cancel_ids.append(row["id"])
            return web.json_response(row)
        return web.json_response({"label": "UNKNOWN_ROUTE"}, status=404)

    async def partial_fill(self, oid):
        await self.fill(oid, Decimal("0.00004"), publish=True)

    async def fill(self, oid, quantity, publish):
        row = self.orders[oid]
        fee = (quantity * Decimal("0.0003")).quantize(Decimal("0.00000001"))
        self.base += quantity - fee
        self.quote -= quantity * Decimal(row["price"])
        row["left"] = str(Decimal(row["left"]) - quantity)
        fill = {
            "id": f"fill-{len(self.fills) + 1}",
            "order_id": oid,
            "currency_pair": "BTC_USDT",
            "text": row["text"],
            "side": "buy",
            "amount": str(quantity),
            "price": row["price"],
            "fee": str(fee),
            "fee_currency": "BTC",
            "role": "maker",
            "create_time_ms": str(int(time.time() * 1000)),
        }
        self.fills.append(fill)
        if publish:
            await self.broadcast("spot.usertrades", [fill, fill])
            await self.broadcast("spot.orders", [self.order_notification(row)])

    @staticmethod
    def order_notification(row):
        result = dict(row)
        status = result.pop("status")
        result["event"] = "update" if status == "open" else "finish"
        result["finish_as"] = "open" if status == "open" else "cancelled"
        return result

    async def manual_cancel(self, oid):
        self.orders[oid]["status"] = "cancelled"
        await self.broadcast("spot.orders", [self.order_notification(self.orders[oid])])
