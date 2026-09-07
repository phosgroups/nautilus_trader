from __future__ import annotations

from nautilus_trader._fixup import fixup_module_names
from nautilus_trader._libnautilus.gateio import *  # noqa: F403


fixup_module_names(globals(), __name__)
del fixup_module_names
