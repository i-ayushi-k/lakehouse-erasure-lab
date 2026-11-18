#!/usr/bin/env bash
# Reproduce everything from a fresh checkout: venv -> tests -> demo -> benchmark -> notebook.
set -euo pipefail
cd "$(dirname "$0")"

python3 -m venv .venv
. .venv/bin/activate
python -m pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

export PYTHONWARNINGS=ignore
python -m pytest -q
python -m src.erasure.pipeline > results/demo_output.txt
python scripts/benchmark.py
python scripts/create_notebook.py
python -m jupyter nbconvert --to notebook --execute --inplace notebooks/results.ipynb
echo "Done. See results/ and notebooks/results.ipynb"
