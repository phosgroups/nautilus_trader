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

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.bitget.providers import BitgetInstrumentProvider
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType
from nautilus_trader.model.identifiers import InstrumentId


@pytest.mark.asyncio
async def test_provider_loads_spot_instruments_from_http_fixture(bitget_http_client):
    provider = BitgetInstrumentProvider(
        client=bitget_http_client,
        product_type=BitgetProductType.SPOT,
    )

    await provider.load_all_async()

    instrument_id = InstrumentId.from_str("BTCUSDT.BITGET")
    instrument = provider.find(instrument_id)

    assert provider.count == 1
    assert instrument is not None
    assert str(instrument.id) == "BTCUSDT.BITGET"
    assert str(instrument.raw_symbol) == "BTCUSDT"
    assert len(provider.instruments_pyo3()) == 1


@pytest.mark.asyncio
async def test_provider_loads_usdt_futures_instruments_from_http_fixture(bitget_http_client):
    provider = BitgetInstrumentProvider(
        client=bitget_http_client,
        product_type=BitgetProductType.USDT_FUTURES,
    )

    await provider.load_all_async()

    instrument_id = InstrumentId.from_str("BTCUSDT-PERP.BITGET")
    instrument = provider.find(instrument_id)

    assert provider.count == 1
    assert instrument is not None
    assert str(instrument.id) == "BTCUSDT-PERP.BITGET"
    assert str(instrument.raw_symbol) == "BTCUSDT"
    assert instrument.is_inverse is False
    assert len(provider.instruments_pyo3()) == 1


@pytest.mark.asyncio
async def test_load_ids_async_filters_loaded_instruments(monkeypatch):
    requested_id = InstrumentId.from_str("BTCUSDT-PERP.BITGET")
    other_id = InstrumentId.from_str("ETHUSDT-PERP.BITGET")

    requested_pyo3 = MagicMock(name="btc_pyo3")
    requested_pyo3.id = requested_id
    other_pyo3 = MagicMock(name="eth_pyo3")
    other_pyo3.id = other_id

    requested_instrument = MagicMock(name="btc_instrument")
    requested_instrument.id = requested_id
    other_instrument = MagicMock(name="eth_instrument")
    other_instrument.id = other_id

    client = MagicMock()
    client.request_instruments = AsyncMock(return_value=[requested_pyo3, other_pyo3])
    client.cache_instruments = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.bitget.providers.instruments_from_pyo3",
        lambda _values: [requested_instrument, other_instrument],
    )
    provider = BitgetInstrumentProvider(
        client=client,
        product_type=BitgetProductType.USDT_FUTURES,
    )

    await provider.load_ids_async([requested_id])

    assert provider.count == 1
    assert provider.get_all().get(requested_id) is requested_instrument
    assert provider.get_all().get(other_id) is None
    assert provider.instruments_pyo3() == [requested_pyo3]
