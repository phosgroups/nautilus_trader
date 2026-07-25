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

import os

import pytest

from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BitgetEnvironment
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType


@pytest.mark.skipif(
    os.getenv("BITGET_DEMO_SMOKE") != "1",
    reason="Bitget demo smoke tests are opt-in with BITGET_DEMO_SMOKE=1",
)
@pytest.mark.asyncio
async def test_demo_public_smoke_loads_spot_and_usdt_futures_instruments():
    client = nautilus_pyo3.BitgetHttpClient(
        environment=BitgetEnvironment.DEMO,
        timeout_secs=10,
    )

    spot_instruments = await client.request_instruments(BitgetProductType.SPOT)
    futures_instruments = await client.request_instruments(BitgetProductType.USDT_FUTURES)

    assert spot_instruments
    assert futures_instruments


@pytest.mark.skipif(
    os.getenv("BITGET_DEMO_PRIVATE_SMOKE") != "1"
    or not os.getenv("BITGET_DEMO_API_KEY")
    or not os.getenv("BITGET_DEMO_API_SECRET")
    or "BITGET_DEMO_API_PASSPHRASE" not in os.environ,
    reason=(
        "Bitget private demo smoke requires BITGET_DEMO_PRIVATE_SMOKE=1 and "
        "BITGET_DEMO_API_KEY/BITGET_DEMO_API_SECRET/BITGET_DEMO_API_PASSPHRASE"
    ),
)
@pytest.mark.asyncio
async def test_demo_private_read_only_smoke_requests_usdt_futures_account_state():
    client = nautilus_pyo3.BitgetHttpClient(
        api_key=os.environ["BITGET_DEMO_API_KEY"],
        api_secret=os.environ["BITGET_DEMO_API_SECRET"],
        api_passphrase=os.environ["BITGET_DEMO_API_PASSPHRASE"],
        environment=BitgetEnvironment.DEMO,
        timeout_secs=10,
    )

    state = await client.request_account_state(
        BitgetProductType.USDT_FUTURES,
        nautilus_pyo3.AccountId("BITGET-DEMO"),
    )

    assert str(state.account_id) == "BITGET-DEMO"
