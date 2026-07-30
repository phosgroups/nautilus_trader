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

from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BitgetProductType


@pytest.mark.asyncio
async def test_http_fixture_runs_public_data_key_paths(bitget_http_client):
    spot_instruments = await bitget_http_client.request_instruments(BitgetProductType.SPOT)
    futures_instruments = await bitget_http_client.request_instruments(
        BitgetProductType.USDT_FUTURES,
    )
    futures = futures_instruments[0]

    deltas = await bitget_http_client.request_orderbook_snapshot(
        BitgetProductType.USDT_FUTURES,
        futures.id,
        limit=50,
    )
    trades = await bitget_http_client.request_trades(
        BitgetProductType.USDT_FUTURES,
        futures.id,
        limit=10,
    )
    funding_rates = await bitget_http_client.request_funding_rates(
        BitgetProductType.USDT_FUTURES,
        futures.id,
        limit=10,
    )

    assert str(spot_instruments[0].id) == "BTCUSDT.BITGET"
    assert str(futures.id) == "BTCUSDT-PERP.BITGET"
    assert str(deltas.instrument_id) == "BTCUSDT-PERP.BITGET"
    assert deltas.sequence == 42
    assert len(deltas.deltas) >= 3
    assert str(trades[0].instrument_id) == "BTCUSDT-PERP.BITGET"
    assert str(trades[0].trade_id) == "T-MKT-1"
    assert str(funding_rates[0].instrument_id) == "BTCUSDT-PERP.BITGET"
    assert funding_rates[0].interval == 480


@pytest.mark.asyncio
async def test_http_fixture_runs_execution_rest_report_key_paths(bitget_http_client, account_id):
    pyo3_account_id = nautilus_pyo3.AccountId(account_id.value)
    futures_instruments = await bitget_http_client.request_instruments(
        BitgetProductType.USDT_FUTURES,
    )
    futures = futures_instruments[0]

    spot_state = await bitget_http_client.request_account_state(
        BitgetProductType.SPOT,
        pyo3_account_id,
    )
    mix_state = await bitget_http_client.request_account_state(
        BitgetProductType.USDT_FUTURES,
        pyo3_account_id,
    )
    order_report = await bitget_http_client.request_order_status_report(
        pyo3_account_id,
        BitgetProductType.USDT_FUTURES,
        futures.id,
        venue_order_id="O-1",
    )
    order_reports = await bitget_http_client.request_order_status_reports(
        pyo3_account_id,
        BitgetProductType.USDT_FUTURES,
        futures.id,
        open_only=True,
        limit=10,
    )
    fill_reports = await bitget_http_client.request_fill_reports(
        pyo3_account_id,
        BitgetProductType.USDT_FUTURES,
        futures.id,
        limit=10,
    )
    position_reports = await bitget_http_client.request_position_status_reports(
        pyo3_account_id,
        BitgetProductType.USDT_FUTURES,
        futures.id,
    )

    assert str(spot_state.account_id) == "BITGET-001"
    assert len(spot_state.balances) == 1
    assert len(spot_state.margins) == 0
    assert str(mix_state.account_id) == "BITGET-001"
    assert len(mix_state.balances) == 1
    assert len(mix_state.margins) == 1

    assert str(order_report.instrument_id) == "BTCUSDT-PERP.BITGET"
    assert str(order_report.venue_order_id) == "O-1"
    assert str(order_report.client_order_id) == "C-1"
    assert order_report.post_only is True
    assert order_report.reduce_only is True
    assert len(order_reports) == 1
    assert str(order_reports[0].venue_order_id) == "O-1"

    assert len(fill_reports) == 1
    assert str(fill_reports[0].trade_id) == "F-1"
    assert str(fill_reports[0].venue_order_id) == "O-1"

    assert len(position_reports) == 1
    assert str(position_reports[0].instrument_id) == "BTCUSDT-PERP.BITGET"
    assert position_reports[0].is_short
