"""Gate HTTP rejection reasons must survive native order event generation."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nautilus_trader.adapters.gateio.config import GateioExecClientConfig
from nautilus_trader.adapters.gateio.execution import GateioExecutionClient
from nautilus_trader.adapters.gateio.http import GateioHttpClient
from nautilus_trader.adapters.gateio.providers import GateioInstrumentProvider
from nautilus_trader.adapters.gateio.providers import parse_instrument
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import OrderSubmitted
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.test_kit.stubs.component import TestComponentStubs


async def submit_with_response(response):
    clock = TestClock()
    clock.set_time(1_000_000_000)
    config = GateioExecClientConfig(api_key="test-key", api_secret="test-secret")
    http = GateioHttpClient(clock, "http://local/api/v4", config, "test-key", "test-secret")
    provider = GateioInstrumentProvider(http, clock, InstrumentProviderConfig())
    instrument = parse_instrument(
        {
            "id": "BTC_USDT",
            "base": "BTC",
            "quote": "USDT",
            "precision": 2,
            "amount_precision": 6,
            "trade_status": "tradable",
        },
        0,
    )
    provider.add(instrument)
    client = GateioExecutionClient(
        asyncio.get_running_loop(),
        http,
        TestComponentStubs.msgbus(),
        TestComponentStubs.cache(),
        clock,
        provider,
        config,
    )
    client.ws.is_ready = lambda: True
    client._account_paused = False
    client._account_last_success = client._loop.time()
    order = TestComponentStubs.order_factory().limit(
        instrument_id=instrument.id,
        order_side=OrderSide.BUY,
        quantity=instrument.make_qty("0.0001"),
        price=instrument.make_price("99000"),
        post_only=True,
        client_order_id=ClientOrderId("gate-rejection"),
    )
    events, completions = [], []

    def receive(event):
        events.append(event)
        order.apply(event)

    async def transport(**kwargs):
        assert json.loads(kwargs["body"])["time_in_force"] == "poc"
        assert order.status == OrderStatus.SUBMITTED
        assert order.client_order_id not in client._waiting_submissions
        completions.append(client._inflight_submissions[order.client_order_id])
        if isinstance(response, Exception):
            raise response
        return response

    client._msgbus.register("ExecEngine.process", receive)
    http.transport = SimpleNamespace(request=AsyncMock(side_effect=transport))
    await client._submit_order(SimpleNamespace(order=order))

    http.transport.request.assert_awaited_once()
    assert not client._waiting_submissions
    assert not client._inflight_submissions
    assert len(completions) == 1
    assert completions[0].is_set()
    return client, order, events


@pytest.mark.parametrize(
    ("label", "due_post_only"),
    [
        ("POC_FILL_IMMEDIATELY", True),
        ("BALANCE_NOT_ENOUGH", False),
        ("INVALID_PARAM_VALUE", False),
        ("UNKNOWN_REJECTION", False),
        ("POC_FILL_IMMEDIATELY_OTHER", False),
    ],
)
def test_http_rejection_preserves_label_and_post_only_cause(label, due_post_only):
    async def run():
        client, order, events = await submit_with_response(
            SimpleNamespace(
                status=400,
                # The label, rather than free-form text or the order flag, determines the cause.
                body=json.dumps({"label": label, "message": "POC_FILL_IMMEDIATELY"}).encode(),
            ),
        )
        assert len(events) == 2
        assert isinstance(events[0], OrderSubmitted)
        rejected = events[1]
        assert isinstance(rejected, OrderRejected)
        assert rejected.due_post_only is due_post_only
        assert rejected.reason == f"Gate HTTP 400: {label} [POST /spot/orders params={{}}]"
        assert rejected.client_order_id == order.client_order_id
        assert rejected.strategy_id == order.strategy_id
        assert rejected.instrument_id == order.instrument_id
        assert rejected.account_id == client.account_id
        assert rejected.ts_event == 1_000_000_000
        assert order.status == OrderStatus.REJECTED
        assert order.is_closed

    asyncio.run(run())


@pytest.mark.parametrize("status", [408, 500, 503])
def test_ambiguous_http_response_does_not_become_a_post_only_rejection(status):
    async def run():
        _, order, events = await submit_with_response(
            SimpleNamespace(status=status, body=b'{"label":"POC_FILL_IMMEDIATELY"}'),
        )
        assert len(events) == 1
        assert isinstance(events[0], OrderSubmitted)
        assert order.status == OrderStatus.SUBMITTED
        assert not order.is_closed

    asyncio.run(run())


def test_transport_timeout_keeps_submission_unresolved_without_retry():
    async def run():
        _, order, events = await submit_with_response(TimeoutError("transport timeout"))
        assert len(events) == 1
        assert isinstance(events[0], OrderSubmitted)
        assert order.status == OrderStatus.SUBMITTED
        assert not order.is_closed

    asyncio.run(run())
