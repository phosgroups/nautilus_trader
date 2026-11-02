"""
Test the Python Gate adapter against a separately installed NT 1.230 wheel.

This does not replace any native NT modules. Run it from any directory with
Python 3.14, nautilus-trader==1.230.0, pytest, and (for PMM tests) aiohttp.
"""

import argparse
import os
import sys
from pathlib import Path

import pytest

import nautilus_trader
import nautilus_trader.adapters


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pmm-root", type=Path)
    parser.add_argument("pytest_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if nautilus_trader.__version__ != "1.230.0":
        parser.error("This runner requires the native nautilus-trader==1.230.0 wheel")
    root = Path(__file__).resolve().parents[1]
    nautilus_trader.adapters.__path__.insert(0, str(root / "nautilus_trader/adapters"))
    if args.pmm_root is not None:
        pmm_root = args.pmm_root.resolve()
        sys.path.insert(0, str(pmm_root / "src"))
        tests = pmm_root / "tests"
    else:
        tests = root / "tests/integration_tests/adapters/gateio"
    # `python -c` subprocesses must not import the unbuilt NT checkout via cwd.
    os.chdir(tests.parent)
    extra = args.pytest_args
    if extra[:1] == ["--"]:
        extra = extra[1:]
    print(f"Native NT: {nautilus_trader.__version__} ({nautilus_trader.__file__})")
    return pytest.main([str(tests), "--confcutdir", str(tests), "-o", "addopts=", *extra])


if __name__ == "__main__":
    raise SystemExit(main())
