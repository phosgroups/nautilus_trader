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

from typing import Any

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import instruments_from_pyo3


class BitgetInstrumentProvider(InstrumentProvider):
    """
    Provides Nautilus instrument definitions from Bitget.
    """

    def __init__(
        self,
        client: nautilus_pyo3.BitgetHttpClient,
        product_type: BitgetProductType = BitgetProductType.USDT_FUTURES,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._client = client
        self._product_type = product_type
        self._instruments_pyo3: list[Any] = []

    @property
    def product_type(self) -> BitgetProductType:
        """
        Return the Bitget product type configured for the provider.
        """
        return self._product_type

    def instruments_pyo3(self) -> list[Any]:
        """
        Return all Bitget PyO3 instrument definitions held by the provider.
        """
        return self._instruments_pyo3

    async def load_all_async(self, filters: dict | None = None) -> None:
        filters_str = "..." if not filters else f" with filters {filters}..."
        self._log.info(f"Loading all Bitget instruments{filters_str}")

        all_pyo3_instruments = await self._client.request_instruments(self._product_type)
        self._client.cache_instruments(all_pyo3_instruments)
        self._instruments_pyo3 = all_pyo3_instruments
        instruments = instruments_from_pyo3(all_pyo3_instruments)
        for instrument in instruments:
            self.add(instrument=instrument)

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        if not instrument_ids:
            self._log.warning("No instrument IDs given for loading")
            return

        existing_instruments = dict(self._instruments)
        existing_pyo3_by_id = {
            str(instrument.id): instrument for instrument in self._instruments_pyo3
        }

        await self.load_all_async(filters=filters)

        instrument_ids_set = set(instrument_ids)
        self._instruments = {
            instrument_id: instrument
            for instrument_id, instrument in self._instruments.items()
            if instrument_id in instrument_ids_set
        }

        for instrument_id, instrument in existing_instruments.items():
            self._instruments.setdefault(instrument_id, instrument)

        loaded_pyo3_by_id = dict(existing_pyo3_by_id)
        loaded_pyo3_by_id.update(
            {str(instrument.id): instrument for instrument in self._instruments_pyo3},
        )
        self._instruments_pyo3 = [
            loaded_pyo3_by_id[instrument_id.value]
            for instrument_id in self._instruments
            if instrument_id.value in loaded_pyo3_by_id
        ]

        for instrument_id in instrument_ids:
            if self.find(instrument_id) is None:
                self._log.warning(f"Unable to find Bitget instrument {instrument_id}")
