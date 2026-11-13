# One-command reproduction. Requires Python 3.10+.
PY ?= python

.PHONY: setup test demo benchmark notebook all clean

setup:
	$(PY) -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

test:
	$(PY) -m pytest -q

demo:
	$(PY) -m src.erasure.pipeline

benchmark:
	$(PY) scripts/benchmark.py

notebook:
	$(PY) scripts/create_notebook.py
	$(PY) -m jupyter nbconvert --to notebook --execute --inplace notebooks/results.ipynb

# Everything, in order: tests -> demo -> benchmark -> executed notebook.
all: test demo benchmark notebook

clean:
	rm -rf data/processed/*.duckdb* data/processed/snapshots data/iceberg_catalog.db \
	       data/iceberg_warehouse results/deletion_audit.jsonl .pytest_cache
