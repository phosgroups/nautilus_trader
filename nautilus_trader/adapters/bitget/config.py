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

from __future__ import annotations

from nautilus_trader.adapters.bitget.common.constants import BITGET_VENUE
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.config import PositiveInt
from nautilus_trader.core.nautilus_pyo3 import BitgetEnvironment
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType
from nautilus_trader.model.identifiers import Venue


class BitgetInstrumentProviderConfig(InstrumentProviderConfig, frozen=True):
    """
    Configuration for ``BitgetInstrumentProvider`` instances.
    """

    product_type: BitgetProductType = BitgetProductType.USDT_FUTURES
    include_inactive: bool = False


class BitgetDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for ``BitgetDataClient`` instances.
    """

    venue: Venue = BITGET_VENUE
    api_key: str | None = None
    api_secret: str | None = None
    api_passphrase: str | None = None
    product_type: BitgetProductType = BitgetProductType.USDT_FUTURES
    environment: BitgetEnvironment | None = None
    base_url_http: str | None = None
    base_url_ws_public: str | None = None
    base_url_ws_private: str | None = None
    proxy_url: str | None = None
    update_instruments_interval_mins: PositiveInt | None = 60
    instrument_poll_interval_secs: PositiveInt | None = 60


class BitgetExecClientConfig(LiveExecClientConfig, frozen=True):
    """
    Configuration for ``BitgetExecutionClient`` instances.
    """

    venue: Venue = BITGET_VENUE
    api_key: str | None = None
    api_secret: str | None = None
    api_passphrase: str | None = None
    product_type: BitgetProductType = BitgetProductType.USDT_FUTURES
    environment: BitgetEnvironment | None = None
    base_url_http: str | None = None
    base_url_ws_private: str | None = None
    proxy_url: str | None = None
    ignore_uncached_instrument_executions: bool = False
