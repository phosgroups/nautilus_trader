#!/usr/bin/env bash
# Smoke test a freshly built v2 wheel across platforms: install it into the synced venv,
# then import the package and key submodules from a neutral directory so the installed
# wheel is exercised rather than the in-tree source. Run from the python/ package directory.
set -euo pipefail

pkg_dir="$(pwd)"
neutral_dir="${RUNNER_TEMP:-/tmp}"

uv sync --no-install-package nautilus-trader
uv pip install "${pkg_dir}/../dist/"*.whl

cd "$neutral_dir"
uv run --project "$pkg_dir" --no-sync python - << 'PY'
import importlib

import nautilus_trader

submodules = [
    "model",
    "common",
    "core",
    "live",
    "backtest",
    "testkit",
    "adapters.binance",
    "adapters.gateio",
    "adapters.okx",
    "adapters.lighter",
    "_libnautilus.binance",
    "_libnautilus.bitget",
    "_libnautilus.gateio",
    "_libnautilus.okx",
]
for name in submodules:
    importlib.import_module(f"nautilus_trader.{name}")

from nautilus_trader._libnautilus.bitget import BitgetDataClientConfig
from nautilus_trader._libnautilus.bitget import BitgetDataClientFactory
from nautilus_trader._libnautilus.bitget import BitgetEnvironment
from nautilus_trader._libnautilus.bitget import BitgetProductType
from nautilus_trader.adapters.okx import OKXDataClientFactory
from nautilus_trader.common import CacheConfig
from nautilus_trader.common import DataActor
from nautilus_trader.live import LiveNode

assert hasattr(LiveNode, "builder"), "LiveNode.builder missing"

print(f"nautilus_trader {nautilus_trader.__version__} imported OK (v2 API + phos adapters)")
PY
