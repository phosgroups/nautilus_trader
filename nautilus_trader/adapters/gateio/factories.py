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

from nautilus_trader.adapters.gateio.config import GateioDataClientConfig
from nautilus_trader.adapters.gateio.config import GateioExecClientConfig
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.data_client import LiveDataClient
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


GATEIO_V1_UNSUPPORTED = (
    "Gate.io is not supported by the NautilusTrader v1 Python runtime. "
    "Its Rust/v2 adapter is retained, but no v1 live client is implemented."
)


class GateioLiveDataClientFactory(LiveDataClientFactory):
    """Reject Gate.io data client creation in the v1 runtime."""

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: GateioDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> LiveDataClient:
        raise NotImplementedError(GATEIO_V1_UNSUPPORTED)


class GateioLiveExecClientFactory(LiveExecClientFactory):
    """Reject Gate.io execution client creation in the v1 runtime."""

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: GateioExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> LiveExecutionClient:
        raise NotImplementedError(GATEIO_V1_UNSUPPORTED)


# Preserve facade import names while routing all creation through the v1 rejection.
GateioDataClientFactory = GateioLiveDataClientFactory
GateioExecutionClientFactory = GateioLiveExecClientFactory
