#!/usr/bin/env bash
set -euo pipefail

# A fixed environment path keeps NumPy headers and PyO3 configuration stable
# across runs. uv's default isolated build uses a different temporary path.
uv venv --clear --python "$(command -v python)" .venv-build

build_requirements="${RUNNER_TEMP:-/tmp}/nautilus-v1-build-requirements.txt"
python - "$build_requirements" <<'PY'
import sys
import tomllib
from pathlib import Path

project = tomllib.loads(Path("pyproject.toml").read_text())
requirements = [*project["build-system"]["requires"], "packaging"]
Path(sys.argv[1]).write_text("\n".join(requirements) + "\n")
PY

uv pip install --python .venv-build/bin/python --requirement "$build_requirements"
echo "PYO3_PYTHON=${GITHUB_WORKSPACE}/.venv-build/bin/python" >> "$GITHUB_ENV"
