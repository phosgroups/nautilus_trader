"""Construct NT v1 clients without loading the Rust/v2 factory registry."""

from nautilus_trader.adapters.gateio.config import GateioProductType
from nautilus_trader.adapters.gateio.config import endpoints
from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.adapters.gateio.execution import GateioExecutionClient
from nautilus_trader.adapters.gateio.http import GateioHttpClient
from nautilus_trader.adapters.gateio.providers import GateioInstrumentProvider
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


def dependencies(config, clock, private=False):
    http_url, ws_url = endpoints(config.environment, config.base_url_http, config.base_url_ws, config.product_type)
    config = type(config)(**dict(config.dict(), base_url_http=http_url, base_url_ws=ws_url))
    if private and (not config.api_key or not config.api_secret):
        raise ValueError("Gate execution requires api_key and api_secret")
    http = GateioHttpClient(
        clock,
        http_url,
        config,
        config.api_key if private else None,
        config.api_secret if private else None,
    )
    provider = GateioInstrumentProvider(http, clock, config.instrument_provider, config.product_type)
    return http, provider, config


class GateioLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        client, provider, config = dependencies(config, clock)
        if config.product_type == GateioProductType.USDT_PERPETUAL:
            from nautilus_trader.adapters.gateio.perpetual_data import GateioPerpetualDataClient

            return GateioPerpetualDataClient(loop, client, msgbus, cache, clock, provider, config, name)
        return GateioDataClient(loop, client, msgbus, cache, clock, provider, config, name)


class GateioLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop, name, config, msgbus, cache, clock):
        client, provider, config = dependencies(config, clock, private=True)
        if config.product_type == GateioProductType.USDT_PERPETUAL:
            raise ValueError("Gate perpetual execution is unavailable")
        return GateioExecutionClient(loop, client, msgbus, cache, clock, provider, config, name)


GateioDataClientFactory = GateioLiveDataClientFactory
GateioExecutionClientFactory = GateioLiveExecClientFactory
