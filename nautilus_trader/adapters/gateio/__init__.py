"""Gate spot adapter for the NautilusTrader v1 Python runtime."""

from nautilus_trader.adapters.gateio.config import GateioDataClientConfig
from nautilus_trader.adapters.gateio.config import GateioEnvironment
from nautilus_trader.adapters.gateio.config import GateioExecClientConfig
from nautilus_trader.adapters.gateio.factories import GateioLiveDataClientFactory
from nautilus_trader.adapters.gateio.factories import GateioLiveExecClientFactory


GateioDataClientFactory = GateioLiveDataClientFactory
GateioExecutionClientFactory = GateioLiveExecClientFactory

__all__ = [
    "GateioDataClientConfig",
    "GateioDataClientFactory",
    "GateioEnvironment",
    "GateioExecClientConfig",
    "GateioExecutionClientFactory",
    "GateioLiveDataClientFactory",
    "GateioLiveExecClientFactory",
]
