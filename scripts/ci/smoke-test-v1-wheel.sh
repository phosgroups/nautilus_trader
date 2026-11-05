#!/usr/bin/env bash
# Run from the repository root. Import the installed v1 wheel from a neutral
# directory so the Cython package and retained phos adapters are exercised.
set -euo pipefail

pkg_dir="$(pwd)"
neutral_dir="${RUNNER_TEMP:-/tmp}"

uv sync --frozen --no-default-groups --no-install-package nautilus-trader
uv pip install --no-deps "${pkg_dir}/dist/"*.whl

cd "$neutral_dir"
uv run --project "$pkg_dir" --no-sync python - << 'PY'
import asyncio
import importlib
from importlib.metadata import version

import nautilus_trader

for name in (
    "model",
    "common",
    "core",
    "live",
    "backtest",
    "test_kit",
    "adapters.binance",
    "adapters.bitget",
    "adapters.gateio",
    "adapters.okx",
    "core.nautilus_pyo3",
):
    importlib.import_module(f"nautilus_trader.{name}")

from nautilus_trader.adapters.bitget import BitgetDataClientConfig
from nautilus_trader.adapters.bitget import BitgetExecClientConfig
from nautilus_trader.adapters.bitget import BitgetLiveDataClientFactory
from nautilus_trader.adapters.bitget import BitgetLiveExecClientFactory
from nautilus_trader.adapters.gateio import GateioDataClientConfig
from nautilus_trader.adapters.gateio import GateioExecClientConfig
from nautilus_trader.adapters.gateio import GateioLiveDataClientFactory
from nautilus_trader.adapters.gateio import GateioLiveExecClientFactory
from nautilus_trader.config import StrategyConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

wheel_version = version("nautilus-trader")
assert wheel_version.startswith("1.231.0"), wheel_version
assert nautilus_trader.__version__.startswith("1.231.0"), nautilus_trader.__version__
assert callable(TradingNode.build)
assert isinstance(TradingNodeConfig(), TradingNodeConfig)
assert callable(Strategy.on_order_event)

instrument_id = InstrumentId.from_str("BTCUSDT.BITGET")
config = StrategyConfig(external_order_claims=[instrument_id])
assert config.external_order_claims == [instrument_id]

assert isinstance(BitgetDataClientConfig(), BitgetDataClientConfig)
assert isinstance(BitgetExecClientConfig(), BitgetExecClientConfig)
assert issubclass(BitgetLiveDataClientFactory, LiveDataClientFactory)
assert issubclass(BitgetLiveExecClientFactory, LiveExecClientFactory)
for name in ("BitgetHttpClient", "BitgetWebSocketClient", "BitgetAccountMode"):
    assert hasattr(nautilus_pyo3, name), name

assert issubclass(GateioLiveDataClientFactory, LiveDataClientFactory)
assert issubclass(GateioLiveExecClientFactory, LiveExecClientFactory)
loop = asyncio.new_event_loop()
node = TradingNode(
    TradingNodeConfig(
        logging=LoggingConfig(log_level="ERROR"),
        data_clients={"GATEIO": GateioDataClientConfig()},
        exec_clients={
            "GATEIO": GateioExecClientConfig(
                api_key="build-only-smoke-key",
                api_secret="build-only-smoke-secret",
            ),
        },
    ),
    loop=loop,
)
try:
    node.add_data_client_factory("GATEIO", GateioLiveDataClientFactory)
    node.add_exec_client_factory("GATEIO", GateioLiveExecClientFactory)
    node.build()
    assert node.is_built()
    assert not node.is_running()
finally:
    node.dispose()
    loop.close()

print(f"nautilus_trader {wheel_version} imported OK (v1 API + Bitget + Gate spot factories)")
PY
