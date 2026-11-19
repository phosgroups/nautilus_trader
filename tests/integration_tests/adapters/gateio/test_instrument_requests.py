import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

from nautilus_trader.adapters.gateio.config import GATEIO
from nautilus_trader.adapters.gateio.data import GateioDataClient
from nautilus_trader.adapters.gateio.providers import GateioInstrumentProvider
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.identifiers import InstrumentId


def test_instrument_requests_load_cache_misses_and_refresh_the_venue_catalog():
    async def run():
        pair = {
            "id": "BTC_USDT", "base": "BTC", "quote": "USDT", "precision": 2,
            "amount_precision": 4, "amount_point": "0.0025", "fee": "0.2", "trade_status": "tradable",
        }
        eth = dict(pair, id="ETH_USDT", base="ETH")
        http = SimpleNamespace(request=AsyncMock(side_effect=[pair, [eth]]))
        provider = GateioInstrumentProvider(http, TestClock(), InstrumentProviderConfig())
        client = SimpleNamespace(
            _instrument_provider=provider, _handle_instrument=Mock(), _handle_instruments=Mock(),
        )
        request = SimpleNamespace(instrument_id=InstrumentId.from_str("BTC_USDT.GATEIO"), id=11,
                                  start=None, end=None, params={}, venue=GATEIO)
        await GateioDataClient._request_instrument(client, request)
        await GateioDataClient._request_instrument(client, request)
        http.request.assert_awaited_once_with("GET", "/spot/currency_pairs/BTC_USDT")
        instrument = provider.find(request.instrument_id)
        assert instrument.size_increment.as_decimal() == Decimal("0.0025")
        assert instrument.maker_fee == instrument.taker_fee == Decimal("0.002")
        await GateioDataClient._request_instruments(client, request)
        venue, instruments, correlation, *_ = client._handle_instruments.call_args.args
        assert (venue, correlation) == (GATEIO, 11)
        assert [i.raw_symbol.value for i in instruments] == ["ETH_USDT"]
        assert provider.find(request.instrument_id) is None
    asyncio.run(run())
