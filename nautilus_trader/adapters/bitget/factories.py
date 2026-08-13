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

import asyncio
from functools import lru_cache

from nautilus_trader.adapters.bitget.config import BitgetDataClientConfig
from nautilus_trader.adapters.bitget.config import BitgetExecClientConfig
from nautilus_trader.adapters.bitget.data import BitgetDataClient
from nautilus_trader.adapters.bitget.execution import BitgetExecutionClient
from nautilus_trader.adapters.bitget.providers import BitgetInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BitgetEnvironment
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


@lru_cache(1)
def get_cached_bitget_http_client(
    environment: BitgetEnvironment = BitgetEnvironment.MAINNET,
    api_key: str | None = None,
    api_secret: str | None = None,
    api_passphrase: str | None = None,
    base_url: str | None = None,
    timeout_secs: int | None = None,
    proxy_url: str | None = None,
) -> nautilus_pyo3.BitgetHttpClient:
    """
    Cache and return a Bitget HTTP client.
    """
    if base_url is None:
        base_url = nautilus_pyo3.get_bitget_http_base_url(environment)

    return nautilus_pyo3.BitgetHttpClient(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        environment=environment,
        base_url=base_url,
        timeout_secs=timeout_secs or 60,
        proxy_url=proxy_url,
    )


@lru_cache(1)
def get_cached_bitget_instrument_provider(
    client: nautilus_pyo3.BitgetHttpClient,
    product_type: BitgetProductType = BitgetProductType.USDT_FUTURES,
    config: InstrumentProviderConfig | None = None,
) -> BitgetInstrumentProvider:
    """
    Cache and return a Bitget instrument provider.
    """
    return BitgetInstrumentProvider(
        client=client,
        product_type=product_type,
        config=config,
    )


class BitgetLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a Bitget Python live data client factory.
    """

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: BitgetDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> BitgetDataClient:
        environment = config.environment or BitgetEnvironment.MAINNET
        client = get_cached_bitget_http_client(
            environment=environment,
            api_key=config.api_key,
            api_secret=config.api_secret,
            api_passphrase=config.api_passphrase,
            base_url=config.base_url_http,
            proxy_url=config.proxy_url,
        )
        provider = get_cached_bitget_instrument_provider(
            client=client,
            product_type=config.product_type,
            config=config.instrument_provider,
        )
        return BitgetDataClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )


class BitgetLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides a Bitget Python live execution client factory.
    """

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: BitgetExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> BitgetExecutionClient:
        environment = config.environment or BitgetEnvironment.MAINNET
        client = get_cached_bitget_http_client(
            environment=environment,
            api_key=config.api_key,
            api_secret=config.api_secret,
            api_passphrase=config.api_passphrase,
            base_url=config.base_url_http,
            proxy_url=config.proxy_url,
        )
        provider = get_cached_bitget_instrument_provider(
            client=client,
            product_type=config.product_type,
            config=config.instrument_provider,
        )
        return BitgetExecutionClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
