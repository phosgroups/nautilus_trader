"""Gate testnet per-pair 400 compatibility without discarding account facts."""

import asyncio
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nautilus_trader.adapters.gateio.config import GateioDataClientConfig
from nautilus_trader.adapters.gateio.http import GateioHttpClient
from nautilus_trader.adapters.gateio.http import GateioHttpError
from nautilus_trader.common.component import TestClock


def http_client():
    return GateioHttpClient(
        TestClock(), "http://local/api/v4", GateioDataClientConfig(), "private-key", "private-secret",
    )


def order(number, symbol="LIGHT_USDT"):
    return {"id": str(number), "currency_pair": symbol, "status": "open"}


def test_light_per_pair_400_uses_complete_filtered_account_order_pages():
    async def run():
        client = http_client()
        client.pages = AsyncMock(side_effect=GateioHttpError(400, "INVALID_PARAM_VALUE"))
        client.request = AsyncMock(side_effect=[
            [{"currency_pair": "BTC_USDT", "orders": [order(999, "BTC_USDT")]},
             {"currency_pair": "LIGHT_USDT", "orders": [order(i) for i in range(100)]}],
            [{"currency_pair": "LIGHT_USDT", "orders": [order(100)]}],
        ])
        rows = await client.open_orders("LIGHT_USDT")
        assert [r["id"] for r in rows] == [str(i) for i in range(101)]
        assert client.request.await_count == 2
        assert client.request.call_args.args == (
            "GET", "/spot/open_orders", {"page": 2, "limit": 100, "account": "spot"},
        )

    asyncio.run(run())


def test_btc_successful_pair_query_does_not_fetch_the_account():
    async def run():
        client = http_client()
        client.pages = AsyncMock(return_value=[order(1, "BTC_USDT")])
        client.request = AsyncMock()
        assert await client.open_orders("BTC_USDT") == [order(1, "BTC_USDT")]
        client.request.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize(("status", "label"), [(401, "INVALID_KEY"), (500, "SERVER_ERROR"), (400, "OTHER")])
def test_other_errors_are_not_silenced_or_treated_as_empty_orders(status, label):
    async def run():
        client = http_client()
        client.pages = AsyncMock(side_effect=GateioHttpError(status, label))
        client.request = AsyncMock()
        with pytest.raises(GateioHttpError):
            await client.open_orders("LIGHT_USDT")
        client.request.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize("response", [
    {"error": "not a list"},
    [{"currency_pair": "LIGHT_USDT", "orders": [order(1, "BTC_USDT")]}],
    [{"currency_pair": "LIGHT_USDT", "orders": "bad"}],
])
def test_invalid_fallback_data_does_not_establish_an_empty_account(response):
    async def run():
        client = http_client()
        client.pages = AsyncMock(side_effect=GateioHttpError(400, "INVALID_PARAM_VALUE"))
        client.request = AsyncMock(return_value=response)
        with pytest.raises(ValueError):
            await client.open_orders("LIGHT_USDT")

    asyncio.run(run())


def test_fallback_failure_propagates_and_repeated_pages_fail_closed():
    async def run():
        client = http_client()
        client.pages = AsyncMock(side_effect=GateioHttpError(400, "INVALID_PARAM_VALUE"))
        client.request = AsyncMock(side_effect=GateioHttpError(503, "SERVER_ERROR"))
        with pytest.raises(GateioHttpError):
            await client.open_orders("LIGHT_USDT")
        client.request = AsyncMock(return_value=[
            {"currency_pair": "LIGHT_USDT", "orders": [order(i) for i in range(100)]},
        ])
        with pytest.raises(RuntimeError, match="without progress"):
            await client.open_orders("LIGHT_USDT")

    asyncio.run(run())


def test_http_error_has_endpoint_and_safe_parameters_without_server_text_or_secrets():
    async def run():
        client = http_client()
        client.transport = SimpleNamespace(request=AsyncMock(return_value=SimpleNamespace(
            status=400,
            body=b'{"label":"INVALID_PARAM_VALUE","message":"private-key private-secret"}',
        )))
        with pytest.raises(GateioHttpError) as caught:
            await client.request("GET", "/spot/orders", {
                "currency_pair": "LIGHT_USDT", "status": "open", "page": 1,
                "api_key": "private-key", "SIGN": "private-signature",
            }, private=True)
        error = caught.value
        assert error.path == "/spot/orders"
        assert error.method == "GET"
        assert error.params == {"currency_pair": "LIGHT_USDT", "status": "open", "page": "1"}
        for secret in ["private-key", "private-secret", "private-signature"]:
            assert secret not in str(error)
        assert "LIGHT_USDT" in str(error)
        assert client.transport.request.await_count == 1

    asyncio.run(run())


def test_periodic_open_order_query_never_sends_finished_order_time_filters():
    import runpy
    from pathlib import Path

    helpers = runpy.run_path(str(Path(__file__).with_name("test_recovery_edges.py")))

    async def run():
        client = helpers["client_for"](lambda *args, **kwargs: helpers["balances"]())
        command = SimpleNamespace(
            instrument_id=next(iter(client._instrument_provider.get_all())),
            start=datetime(2026, 11, 5, tzinfo=UTC),
            end=datetime(2026, 11, 6, tzinfo=UTC), open_only=False,
        )
        try:
            assert await client.generate_order_status_reports(command) == []
            open_params, finished_params = [c.args[1] for c in client.http.pages.call_args_list]
            assert open_params == {"currency_pair": "BTC_USDT", "status": "open"}
            assert "from" in finished_params
            assert "to" in finished_params
        finally:
            await helpers["cleanup"](client)

    asyncio.run(run())
