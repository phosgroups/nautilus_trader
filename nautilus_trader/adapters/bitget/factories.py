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

from functools import lru_cache

from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BitgetEnvironment


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
        base_url=base_url,
        timeout_secs=timeout_secs or 60,
        proxy_url=proxy_url,
    )


BitgetLiveDataClientFactory = nautilus_pyo3.BitgetDataClientFactory
BitgetLiveExecClientFactory = nautilus_pyo3.BitgetExecutionClientFactory
