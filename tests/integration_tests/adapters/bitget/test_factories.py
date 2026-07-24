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

from nautilus_trader.adapters.bitget import BITGET
from nautilus_trader.adapters.bitget import BITGET_VENUE
from nautilus_trader.adapters.bitget import BitgetDataClientConfig
from nautilus_trader.adapters.bitget import BitgetExecClientConfig
from nautilus_trader.adapters.bitget import BitgetInstrumentProviderConfig
from nautilus_trader.adapters.bitget import BitgetLiveDataClientFactory
from nautilus_trader.adapters.bitget import BitgetLiveExecClientFactory
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BitgetEnvironment
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType


def test_bitget_python_facade_imports_and_config_defaults():
    provider_config = BitgetInstrumentProviderConfig()
    data_config = BitgetDataClientConfig()
    exec_config = BitgetExecClientConfig()

    assert BITGET == "BITGET"
    assert BITGET_VENUE.value == "BITGET"
    assert provider_config.product_type == BitgetProductType.USDT_FUTURES
    assert data_config.product_type == BitgetProductType.USDT_FUTURES
    assert exec_config.product_type == BitgetProductType.USDT_FUTURES


def test_bitget_python_configs_accept_spot_and_fixture_urls():
    data_config = BitgetDataClientConfig(
        product_type=BitgetProductType.SPOT,
        environment=BitgetEnvironment.MAINNET,
        base_url_http="http://127.0.0.1:9000",
        base_url_ws_public="ws://127.0.0.1:9001/ws",
    )
    exec_config = BitgetExecClientConfig(
        product_type=BitgetProductType.USDT_FUTURES,
        environment=BitgetEnvironment.MAINNET,
        base_url_http="http://127.0.0.1:9000",
        base_url_ws_private="ws://127.0.0.1:9001/ws",
    )

    assert data_config.product_type == BitgetProductType.SPOT
    assert data_config.base_url_http == "http://127.0.0.1:9000"
    assert exec_config.base_url_ws_private == "ws://127.0.0.1:9001/ws"


def test_bitget_pyo3_factory_bindings_are_registered():
    data_factory = BitgetLiveDataClientFactory()
    exec_factory = BitgetLiveExecClientFactory(
        nautilus_pyo3.TraderId("TRADER-001"),
        nautilus_pyo3.AccountId("BITGET-001"),
    )

    assert data_factory.name() == "BITGET"
    assert exec_factory.name() == "BITGET"


def test_bitget_url_and_symbol_helpers():
    assert nautilus_pyo3.get_bitget_http_base_url(BitgetEnvironment.MAINNET).startswith("https://")
    assert nautilus_pyo3.get_bitget_ws_url_public(BitgetEnvironment.MAINNET).startswith("wss://")
    assert nautilus_pyo3.get_bitget_ws_url_private(BitgetEnvironment.MAINNET).startswith("wss://")

    assert nautilus_pyo3.bitget_extract_raw_symbol("BTCUSDT-PERP") == "BTCUSDT"
    assert (
        nautilus_pyo3.bitget_product_type_from_symbol("BTCUSDT-PERP")
        == BitgetProductType.USDT_FUTURES
    )
    assert nautilus_pyo3.bitget_product_type_from_symbol("BTCUSDT") == BitgetProductType.SPOT
