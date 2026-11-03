"""Explicit spot endpoints and client settings; no environment credential fallback."""

from enum import Enum

from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.model.identifiers import Venue


GATEIO = Venue("GATEIO")


class GateioEnvironment(str, Enum):
    LIVE = "LIVE"
    TESTNET = "TESTNET"


def endpoints(environment, http=None, ws=None):
    environment = GateioEnvironment(environment)
    if environment == GateioEnvironment.TESTNET:
        return (
            http or "https://api-testnet.gateapi.io/api/v4",
            ws or "wss://ws-testnet.gate.com/v4/ws/spot",
        )
    return http or "https://api.gateio.ws/api/v4", ws or "wss://api.gateio.ws/ws/v4/"


class GateioDataClientConfig(LiveDataClientConfig, frozen=True):
    venue: Venue = GATEIO
    environment: GateioEnvironment = GateioEnvironment.TESTNET
    base_url_http: str | None = None
    base_url_ws: str | None = None
    proxy_url: str | None = None
    http_timeout_secs: int = 10
    http_rate_per_second: int = 10
    max_retries: int = 3
    book_depth: int = 100
    max_book_buffer: int = 10000


class GateioExecClientConfig(LiveExecClientConfig, frozen=True):
    venue: Venue = GATEIO
    environment: GateioEnvironment = GateioEnvironment.TESTNET
    api_key: str | None = None
    api_secret: str | None = None
    base_url_http: str | None = None
    base_url_ws: str | None = None
    proxy_url: str | None = None
    http_timeout_secs: int = 10
    http_rate_per_second: int = 10
    max_retries: int = 3
    reconciliation_lookback_mins: int = 60
    account_refresh_interval_ms: int = 250
