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
"""
Bitget cryptocurrency exchange integration adapter.
"""

from nautilus_trader.adapters.bitget.common.constants import BITGET
from nautilus_trader.adapters.bitget.common.constants import BITGET_CLIENT_ID
from nautilus_trader.adapters.bitget.common.constants import BITGET_VENUE
from nautilus_trader.adapters.bitget.config import BitgetDataClientConfig
from nautilus_trader.adapters.bitget.config import BitgetExecClientConfig
from nautilus_trader.adapters.bitget.config import BitgetInstrumentProviderConfig
from nautilus_trader.adapters.bitget.data import BitgetDataClient
from nautilus_trader.adapters.bitget.execution import BitgetExecutionClient
from nautilus_trader.adapters.bitget.factories import BitgetLiveDataClientFactory
from nautilus_trader.adapters.bitget.factories import BitgetLiveExecClientFactory
from nautilus_trader.adapters.bitget.factories import get_cached_bitget_http_client
from nautilus_trader.adapters.bitget.factories import get_cached_bitget_instrument_provider
from nautilus_trader.adapters.bitget.providers import BitgetInstrumentProvider
from nautilus_trader.core.nautilus_pyo3 import BitgetEnvironment
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType


__all__ = [
    "BITGET",
    "BITGET_CLIENT_ID",
    "BITGET_VENUE",
    "BitgetDataClient",
    "BitgetDataClientConfig",
    "BitgetEnvironment",
    "BitgetExecutionClient",
    "BitgetExecClientConfig",
    "BitgetInstrumentProvider",
    "BitgetInstrumentProviderConfig",
    "BitgetLiveDataClientFactory",
    "BitgetLiveExecClientFactory",
    "BitgetProductType",
    "get_cached_bitget_http_client",
    "get_cached_bitget_instrument_provider",
]
