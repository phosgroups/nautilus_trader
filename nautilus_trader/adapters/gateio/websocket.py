"""Authenticated subscriptions and ordered callbacks over NT's reconnecting transport."""

import asyncio
import hashlib
import hmac
import json

from nautilus_trader.core.nautilus_pyo3 import WebSocketClient
from nautilus_trader.core.nautilus_pyo3 import WebSocketConfig


def subscription(channel, payload, timestamp, key=None, secret=None, event="subscribe"):
    result = {"time": timestamp, "channel": channel, "event": event}
    if payload is not None:
        result["payload"] = list(payload)
    if key is not None:
        if not secret:
            raise ValueError("Missing Gate WebSocket signing secret")
        message = f"channel={channel}&event={event}&time={timestamp}"
        result["auth"] = {
            "method": "api_key",
            "KEY": key,
            "SIGN": hmac.new(secret.encode(), message.encode(), hashlib.sha512).hexdigest(),
        }
    return result


class GateioWebSocket:
    def __init__(
        self,
        loop,
        clock,
        url,
        handler,
        reconnect,
        proxy=None,
        key=None,
        secret=None,
        error_handler=None,
    ):
        self.loop, self.clock, self.url = loop, clock, url
        self.handler, self.reconnect = handler, reconnect
        self.proxy, self.key, self.secret = proxy, key, secret
        self.client = None
        self.subscriptions = []
        self.pending = {}
        self.queue = asyncio.Queue(maxsize=10000)
        self.worker = None
        self.recovery = None
        self.ready = False
        self.error = None
        self.error_handler = error_handler
        self._closing = False
        self._generation = 0
        self._recover_again = False
        self._resume = asyncio.Event()
        self._resume.set()

    def _failed(self, error):
        self.ready = False
        self.error = type(error).__name__
        if self.error_handler is not None:
            self.error_handler(f"Gate WebSocket is not ready: {self.error}")
        self._resume.clear()
        self._drain()
        if not self._closing and self.client is not None:
            if self.recovery is None or self.recovery.done():
                self.recovery = self.loop.create_task(self._recover())
            else:
                self._recover_again = True

    def _drain(self):
        while not self.queue.empty():
            self.queue.get_nowait()
            self.queue.task_done()

    async def connect(self):
        self._closing, self.error = False, None
        self._resume.set()
        self.client = await WebSocketClient.connect(
            loop_=self.loop,
            config=WebSocketConfig(url=self.url, headers=[], heartbeat=10, proxy_url=self.proxy),
            handler=self._receive,
            post_reconnection=self._reconnected,
        )
        self.worker = self.loop.create_task(self._consume())
        self.ready = True

    async def subscribe(self, channel, payload=None):
        item = (channel, tuple(payload) if payload is not None else None)
        await self._subscription(*item)
        if item not in self.subscriptions:
            self.subscriptions.append(item)

    async def unsubscribe(self, channel, payload=None):
        item = (channel, tuple(payload) if payload is not None else None)
        await self._subscription(*item, event="unsubscribe")
        if item in self.subscriptions:
            self.subscriptions.remove(item)

    async def _subscription(self, channel, payload, event="subscribe"):
        future = self.loop.create_future()
        key = (channel, event)
        if key in self.pending:
            raise RuntimeError("Concurrent Gate subscription for the same channel")
        self.pending[key] = future
        message = subscription(
            channel,
            payload,
            self.clock.timestamp_ns() // 1_000_000_000,
            self.key,
            self.secret,
            event,
        )
        try:
            await self.client.send_text(json.dumps(message).encode())
            await asyncio.wait_for(future, timeout=10)
        finally:
            if self.pending.get(key) is future:
                self.pending.pop(key, None)

    def _receive(self, raw):
        try:
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("Gate WebSocket message must be an object")
            key = (value.get("channel"), value.get("event"))
            future = self.pending.get(key)
            if future is not None and not future.done():
                result = value.get("result")
                if (
                    value.get("error")
                    or not isinstance(result, dict)
                    or result.get("status") != "success"
                ):
                    future.set_exception(RuntimeError("Gate subscription rejected"))
                    self.ready = False
                else:
                    future.set_result(None)
                return
            if value.get("event") == "update":
                self.queue.put_nowait((self._generation, value))
        except (ValueError, TypeError, asyncio.QueueFull) as error:
            self._failed(error)

    async def _consume(self):
        while True:
            value = await self.queue.get()
            try:
                generation = self._generation
                if isinstance(value, tuple):
                    generation, value = value
                await self._resume.wait()
                if generation != self._generation:
                    continue
                await self.handler(value)
            except Exception as error:
                self._failed(error)
            finally:
                self.queue.task_done()

    def _reconnected(self):
        if self._closing:
            return
        self._generation += 1
        self.ready = False
        self._resume.clear()
        self._drain()
        for future in self.pending.values():
            if not future.done():
                future.set_exception(ConnectionError("Gate reconnected during subscription"))
        self.pending.clear()
        if self.recovery is not None:
            self.recovery.cancel()
        self.recovery = self.loop.create_task(self._recover())

    async def _recover(self):
        generation = self._generation
        failures = 0
        try:
            while not self._closing and generation == self._generation:
                self._recover_again = False
                try:
                    if self.client is None or not self.client.is_active():
                        raise ConnectionError("Gate transport is reconnecting")
                    for channel, payload in list(self.subscriptions):
                        await self._subscription(channel, payload)
                    await self.reconnect()
                    if generation != self._generation or self._closing:
                        return
                    if self._recover_again:
                        continue  # Another overflow/error invalidated this reconciliation.
                    self.error, self.ready = None, True
                    self._resume.set()
                    return
                except Exception as error:
                    self.ready, self.error = False, type(error).__name__
                    if self.error_handler is not None:
                        self.error_handler(f"Gate recovery failed; retrying: {self.error}")
                    failures += 1
                    await asyncio.sleep(min(0.25 * 2 ** min(failures - 1, 5), 5))
        finally:
            if self.recovery is asyncio.current_task():
                self.recovery = None

    def is_ready(self):
        return (
            self.ready
            and self.error is None
            and self.client is not None
            and self.client.is_active()
        )

    async def disconnect(self):
        self._closing = True
        self._generation += 1
        self.ready = False
        self._resume.set()
        try:
            if self.client is not None:
                await self.client.disconnect()
        finally:
            tasks = [t for t in (self.worker, self.recovery) if t is not None]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for future in self.pending.values():
                if not future.done():
                    future.cancel()
            self.pending.clear()
            self.worker = self.recovery = None
            self.client = None
            self._drain()
