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

import subprocess
import sys
from unittest.mock import patch

import pytest

from nautilus_trader.adapters.gateio import GateioDataClientConfig
from nautilus_trader.adapters.gateio import GateioDataClientFactory
from nautilus_trader.adapters.gateio import GateioExecClientConfig
from nautilus_trader.adapters.gateio import GateioExecutionClientFactory
from nautilus_trader.adapters.gateio import GateioLiveDataClientFactory
from nautilus_trader.adapters.gateio import GateioLiveExecClientFactory


@pytest.mark.parametrize(
    ("factory", "config_type"),
    [
        (GateioLiveDataClientFactory, GateioDataClientConfig),
        (GateioLiveExecClientFactory, GateioExecClientConfig),
        (GateioDataClientFactory, GateioDataClientConfig),
        (GateioExecutionClientFactory, GateioExecClientConfig),
    ],
)
def test_gateio_v1_factory_constructs_the_matching_live_client(factory, config_type):
    config = config_type()
    module = "nautilus_trader.adapters.gateio.factories"
    client_name = (
        "GateioDataClient" if config_type is GateioDataClientConfig else "GateioExecutionClient"
    )
    dependencies = (object(), object(), config)
    loop, msgbus, cache, clock = object(), object(), object(), object()
    with (
        patch(f"{module}.dependencies", return_value=dependencies),
        patch(f"{module}.{client_name}") as client,
    ):
        result = factory.create(
            loop=loop,
            name="GATEIO",
            config=config,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )
    assert result is client.return_value
    client.assert_called_once_with(
        loop, dependencies[0], msgbus, cache, clock, dependencies[1], config, "GATEIO"
    )


@pytest.mark.parametrize("mode", ["CLASSIC", "UTA"])
def test_gate_and_bitget_build_in_the_same_installed_v1_node(mode):
    script = """
import asyncio
from nautilus_trader.adapters.gateio import GateioDataClientConfig, GateioExecClientConfig, GateioLiveDataClientFactory, GateioLiveExecClientFactory
from nautilus_trader.adapters.bitget import BitgetAccountMode, BitgetProductType, BitgetDataClientConfig, BitgetExecClientConfig, BitgetLiveDataClientFactory, BitgetLiveExecClientFactory
from nautilus_trader.config import TradingNodeConfig, LoggingConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId
loop = asyncio.new_event_loop()
config = TradingNodeConfig(
    trader_id=TraderId('COEXIST-001'), logging=LoggingConfig(log_level='ERROR'),
    data_clients={'GATEIO': GateioDataClientConfig(), 'BITGET': BitgetDataClientConfig(account_mode=MODE, product_type=BitgetProductType.SPOT)},
    exec_clients={'GATEIO': GateioExecClientConfig(api_key='offline', api_secret='offline'), 'BITGET': BitgetExecClientConfig(account_mode=MODE, product_type=BitgetProductType.SPOT, api_key='offline', api_secret='offline', api_passphrase='offline')},
)
node = TradingNode(config=config, loop=loop)
try:
    node.add_data_client_factory('GATEIO', GateioLiveDataClientFactory)
    node.add_exec_client_factory('GATEIO', GateioLiveExecClientFactory)
    node.add_data_client_factory('BITGET', BitgetLiveDataClientFactory)
    node.add_exec_client_factory('BITGET', BitgetLiveExecClientFactory)
    node.build()
    assert node.is_built() and not node.is_running()
    for engine in (node.kernel.data_engine, node.kernel.exec_engine):
        assert {str(x) for x in engine.registered_clients} == {'GATEIO', 'BITGET'}
finally:
    node.dispose()
""".replace("MODE", f"BitgetAccountMode.{mode}")
    result = subprocess.run(  # noqa: S603 (fixed local test script)
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
