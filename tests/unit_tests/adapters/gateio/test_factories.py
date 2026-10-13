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
def test_gateio_v1_factory_explicitly_rejects_unsupported_usage(factory, config_type):
    # Importing and constructing configs is safe. Reject usage before touching
    # a runtime dependency, credential, or network connection.
    config = config_type()
    with pytest.raises(NotImplementedError, match=r"Gate\.io.*not supported.*v1"):
        factory.create(
            loop=None,
            name="GATEIO",
            config=config,
            msgbus=None,
            cache=None,
            clock=None,
        )
