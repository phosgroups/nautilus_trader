"""Gate lookup misses never fabricate terminal state or cross order identities."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nautilus_trader.adapters.gateio.execution import _fetch_order
from nautilus_trader.adapters.gateio.execution import order_text
from nautilus_trader.adapters.gateio.http import GateioHttpError
from nautilus_trader.common.component import TestClock
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import VenueOrderId


def fixture(symbol="BTC_USDT", venue_id=None, cached_id=None):
    command = SimpleNamespace(
        instrument_id=InstrumentId.from_str(f"{symbol}.GATEIO"),
        client_order_id=ClientOrderId(f"query-{symbol}"),
        venue_order_id=VenueOrderId(venue_id) if venue_id else None,
    )
    cached = SimpleNamespace(
        instrument_id=command.instrument_id,
        account_id=AccountId("GATEIO-master"),
        venue_order_id=VenueOrderId(cached_id) if cached_id else None,
    )
    client = SimpleNamespace(
        _cache=SimpleNamespace(order=lambda _: cached),
        account_id=cached.account_id,
        http=SimpleNamespace(request=AsyncMock(), pages=AsyncMock(return_value=[])),
        _clock=TestClock(),
        config=SimpleNamespace(reconciliation_lookback_mins=60),
    )
    row = {
        "id": "123",
        "currency_pair": symbol,
        "text": order_text(command.client_order_id),
        "status": "cancelled",
    }
    return client, command, cached, row


@pytest.mark.parametrize("symbol", ["BTC_USDT", "LIGHT_USDT"])
@pytest.mark.parametrize("label", ["ORDER_NOT_FOUND", "CLIENT_ID_NOT_FOUND"])
@pytest.mark.parametrize("history_present", [True, False])
def test_custom_id_lookup_miss_requires_positive_history_evidence(symbol, label, history_present):
    async def run():
        client, command, _, row = fixture(symbol)
        client.http.request.side_effect = GateioHttpError(404, label)
        client.http.pages.return_value = [row] if history_present else []
        assert await _fetch_order(client, command) == (row if history_present else None)
        client.http.request.assert_awaited_once()
        assert client.http.pages.call_args.args[1]["currency_pair"] == symbol
        assert client.http.pages.call_args.args[1]["status"] == "finished"

    asyncio.run(run())


def test_query_uses_venue_identity_added_to_cache_after_command_creation():
    async def run():
        client, command, _, row = fixture(cached_id="123")
        client.http.request.return_value = row
        assert await _fetch_order(client, command) == row
        assert client.http.request.call_args.args[1] == "/spot/orders/123"
        client.http.pages.assert_not_awaited()

    asyncio.run(run())


def test_acceptance_during_custom_lookup_retries_once_with_exchange_identity():
    async def run():
        client, command, cached, row = fixture()

        async def request(method, path, *args, **kwargs):
            if path.endswith(order_text(command.client_order_id)):
                cached.venue_order_id = VenueOrderId("123")
                raise GateioHttpError(404, "CLIENT_ID_NOT_FOUND")
            return row

        client.http.request.side_effect = request
        assert await _fetch_order(client, command) == row
        assert client.http.request.await_count == 2
        assert client.http.request.call_args.args[1] == "/spot/orders/123"
        client.http.pages.assert_not_awaited()

    asyncio.run(run())


def test_numeric_lookup_miss_and_empty_history_remains_unknown():
    async def run():
        client, command, _, _ = fixture(venue_id="123", cached_id="123")
        client.http.request.side_effect = GateioHttpError(404, "ORDER_NOT_FOUND")
        assert await _fetch_order(client, command) is None
        client.http.request.assert_awaited_once()

    asyncio.run(run())


@pytest.mark.parametrize(
    ("status", "label"),
    [(403, "FORBIDDEN"), (429, "TOO_MANY_REQUESTS"), (500, "SERVER_ERROR"), (404, "UNKNOWN_ROUTE")],
)
def test_unrelated_errors_do_not_become_missing_orders(status, label):
    async def run():
        client, command, _, _ = fixture()
        client.http.request.side_effect = GateioHttpError(status, label)
        with pytest.raises(GateioHttpError):
            await _fetch_order(client, command)
        client.http.pages.assert_not_awaited()

    asyncio.run(run())


def test_ambiguous_history_or_wrong_pair_cannot_confirm_an_order():
    async def run():
        client, command, _, row = fixture()
        client.http.request.side_effect = GateioHttpError(404, "CLIENT_ID_NOT_FOUND")
        client.http.pages.return_value = [row, dict(row, id="456")]
        with pytest.raises(ValueError, match="ambiguous"):
            await _fetch_order(client, command)
        client.http.pages.return_value = [dict(row, currency_pair="LIGHT_USDT")]
        with pytest.raises(ValueError, match="instrument"):
            await _fetch_order(client, command)

    asyncio.run(run())


def test_concurrent_pair_queries_keep_their_own_history_and_identity():
    async def run():
        fixtures = [fixture(symbol) for symbol in ("BTC_USDT", "LIGHT_USDT")]
        for client, _, _, row in fixtures:
            client.http.request.side_effect = GateioHttpError(404, "CLIENT_ID_NOT_FOUND")
            client.http.pages.return_value = [row]
        results = await asyncio.gather(*(_fetch_order(c, cmd) for c, cmd, _, _ in fixtures))
        assert [r["currency_pair"] for r in results] == ["BTC_USDT", "LIGHT_USDT"]

    asyncio.run(run())


@pytest.mark.parametrize("mismatch", ["account", "instrument", "venue_id"])
def test_conflicting_cached_identity_is_rejected_before_http(mismatch):
    async def run():
        client, command, cached, _ = fixture(venue_id="123", cached_id="123")
        if mismatch == "account":
            cached.account_id = AccountId("GATEIO-other")
        elif mismatch == "instrument":
            cached.instrument_id = InstrumentId.from_str("LIGHT_USDT.GATEIO")
        else:
            cached.venue_order_id = VenueOrderId("456")
        with pytest.raises(ValueError, match="conflicts"):
            await _fetch_order(client, command)
        client.http.request.assert_not_awaited()
        client.http.pages.assert_not_awaited()

    asyncio.run(run())


def test_locally_queued_submission_is_not_queried_as_a_missing_remote_order():
    async def run():
        client, command, _, _ = fixture()
        client._waiting_submissions = {command.client_order_id: asyncio.Event()}
        assert await _fetch_order(client, command) is None
        client.http.request.assert_not_awaited()
        client.http.pages.assert_not_awaited()

    asyncio.run(run())
