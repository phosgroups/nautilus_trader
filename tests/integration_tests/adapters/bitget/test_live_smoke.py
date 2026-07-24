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
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType


pytestmark = pytest.mark.skipif(
    os.getenv("BITGET_LIVE_SMOKE") != "1",
    reason="Bitget live smoke tests are opt-in with BITGET_LIVE_SMOKE=1",
)


@pytest.mark.asyncio
async def test_live_public_smoke_loads_spot_and_usdt_futures_instruments():
    client = nautilus_pyo3.BitgetHttpClient(timeout_secs=10)

    spot_instruments = await client.request_instruments(BitgetProductType.SPOT)
    futures_instruments = await client.request_instruments(BitgetProductType.USDT_FUTURES)

    assert spot_instruments
    assert futures_instruments


@pytest.mark.skipif(
    os.getenv("BITGET_LIVE_PRIVATE_SMOKE") != "1"
    or not os.getenv("BITGET_API_KEY")
    or not os.getenv("BITGET_API_SECRET")
    or "BITGET_API_PASSPHRASE" not in os.environ,
    reason=(
        "Bitget private live smoke requires BITGET_LIVE_PRIVATE_SMOKE=1 and "
        "BITGET_API_KEY/BITGET_API_SECRET/BITGET_API_PASSPHRASE"
    ),
)
@pytest.mark.asyncio
async def test_live_private_read_only_smoke_requests_usdt_futures_account_state():
    client = nautilus_pyo3.BitgetHttpClient(timeout_secs=10)

    state = await client.request_account_state(
        BitgetProductType.USDT_FUTURES,
        nautilus_pyo3.AccountId("BITGET-LIVE"),
    )

    assert str(state.account_id) == "BITGET-LIVE"
