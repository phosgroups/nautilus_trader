"""Gate API v4 protocol using NT v1's existing Rust HTTP transport."""

import asyncio
import hashlib
import hmac
import json
from urllib.parse import urlencode
from urllib.parse import urlsplit

from nautilus_trader.core.nautilus_pyo3 import HttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod
from nautilus_trader.core.nautilus_pyo3 import Quota


class GateioHttpError(RuntimeError):
    def __init__(self, status, label):
        self.status = status
        self.label = label
        super().__init__(f"Gate HTTP {status}: {label}")

    @property
    def definite_rejection(self):
        return 400 <= self.status < 500 and self.status not in {408}


def sign_rest(secret, method, path, query, body, timestamp):
    digest = hashlib.sha512(body).hexdigest()
    message = "\n".join((method, path, query, digest, str(timestamp)))
    return hmac.new(secret.encode(), message.encode(), hashlib.sha512).hexdigest()


class GateioHttpClient:
    def __init__(self, clock, base_url, config, api_key=None, api_secret=None):
        if config.http_rate_per_second <= 0 or config.http_timeout_secs <= 0:
            raise ValueError("Gate HTTP rate and timeout must be positive")
        if config.max_retries < 0:
            raise ValueError("Gate max_retries must be nonnegative")
        self.clock = clock
        self.base_url = base_url.rstrip("/")
        self.key, self.secret = api_key, api_secret
        self.max_retries = config.max_retries
        self._request_interval = 1 / config.http_rate_per_second
        self._rate_condition = asyncio.Condition()
        self._waiting_requests = [0, 0, 0]
        self._next_request_at = 0.0
        self.transport = HttpClient(
            timeout_secs=config.http_timeout_secs,
            default_quota=Quota.rate_per_second(config.http_rate_per_second),
            proxy_url=config.proxy_url,
        )

    async def _throttle(self, priority=1):
        # Queue before signing, rather than aging signatures in native quota waits.
        # Cancels and account/recovery reads must not queue behind an entire ladder.
        async with self._rate_condition:
            self._waiting_requests[priority] += 1
            try:
                loop = asyncio.get_running_loop()
                while True:
                    if any(self._waiting_requests[:priority]):
                        await self._rate_condition.wait()
                        continue
                    delay = self._next_request_at - loop.time()
                    if delay <= 0:
                        self._next_request_at = loop.time() + self._request_interval
                        return
                    try:
                        async with asyncio.timeout(delay):
                            await self._rate_condition.wait()
                    except TimeoutError:
                        pass
            finally:
                self._waiting_requests[priority] -= 1
                self._rate_condition.notify_all()

    async def request(self, method, path, params=None, body=None, private=False, before_send=None):
        query = urlencode([(k, str(v)) for k, v in (params or {}).items() if v is not None])
        payload = json.dumps(body, separators=(",", ":")).encode() if body is not None else b""
        url = self.base_url + path + ("?" + query if query else "")
        attempts = self.max_retries + 1 if method == "GET" else 1
        for attempt in range(attempts):
            await self._throttle(0 if method == "DELETE" else 2 if method == "POST" else 1)
            if before_send is not None:
                before_send()  # Synchronous final check; no yield before transport dispatch.
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            if private:
                if not self.key or not self.secret:
                    raise ValueError("Gate private requests require api_key and api_secret")
                timestamp = str(self.clock.timestamp_ns() // 1_000_000_000)
                headers.update(
                    KEY=self.key,
                    Timestamp=timestamp,
                    SIGN=sign_rest(
                        self.secret, method, urlsplit(url).path, query, payload, timestamp
                    ),
                )
            try:
                response = await self.transport.request(
                    method=getattr(HttpMethod, method),
                    url=url,
                    headers=headers,
                    body=payload or None,
                    # NT's default quota is applied only to explicitly keyed requests.
                    # Share one budget across reads, submissions, and cancellations.
                    keys=["gateio"],
                )
            except Exception:
                if attempt + 1 >= attempts:
                    raise
                await asyncio.sleep(min(0.25 * 2**attempt, 2))
                continue
            if (
                method == "GET"
                and attempt + 1 < attempts
                and (response.status == 429 or response.status >= 500)
            ):
                await asyncio.sleep(min(0.25 * 2**attempt, 2))
                continue
            try:
                data = json.loads(response.body) if response.body else None
            except (ValueError, UnicodeError):
                raise GateioHttpError(response.status, "invalid_response") from None
            if 200 <= response.status < 300:
                return data
            label = (
                data.get("label", "request_failed") if isinstance(data, dict) else "request_failed"
            )
            # Error text may echo requests. Never log arbitrary server payloads or credentials.
            raise GateioHttpError(response.status, str(label)[:80])
        raise RuntimeError("Gate HTTP retry loop exhausted")

    async def pages(self, path, params):
        rows = []
        seen = set()
        for page in range(1, 1001):
            batch = await self.request(
                "GET",
                path,
                {**params, "page": page, "limit": 100},
                private=True,
            )
            if not isinstance(batch, list):
                raise ValueError("Gate paginated response must be a list")
            new_rows = []
            for row in batch:
                key = (row.get("currency_pair"), str(row["id"]))
                if key not in seen:
                    seen.add(key)
                    new_rows.append(row)
            if len(batch) == 100 and not new_rows:
                raise RuntimeError("Gate pagination repeated a page without progress")
            rows.extend(new_rows)
            if len(batch) < 100:
                return rows
        raise RuntimeError("Gate pagination exceeded safety bound; reconciliation incomplete")
