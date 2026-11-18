"""Generate notebooks/results.ipynb (execute it afterwards with nbconvert)."""

from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = ROOT / "notebooks" / "results.ipynb"

md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell

nb = nbf.v4.new_notebook()
nb.cells = [
    md("""# Lakehouse Erasure Lab — Results

Analysis of `results/scale_benchmark.csv`, produced by `python scripts/benchmark.py`
against the **integrated DuckDB + Apache Iceberg pipeline**.

Questions answered here:

1. Does the pipeline verifiably erase the target user at every size? (correctness)
2. How long does current-state deletion take, and how much of that is Iceberg? (latency)
3. After current-state deletion, how many physical copies of the user still exist, and when do they disappear? (exposure window)
4. How much Parquet data does cleanup reclaim? (storage)

**Scope:** single machine, synthetic data, 5 repeats per size. Latencies are not production figures.
Cost columns use illustrative rates, not real billing data."""),
    code("""import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path.cwd()
if not (ROOT / "results" / "scale_benchmark.csv").exists():
    ROOT = Path.cwd().parent

df = pd.read_csv(ROOT / "results" / "scale_benchmark.csv")
env = json.loads((ROOT / "results" / "benchmark_environment.json").read_text())
print("Environment:", {k: env[k] for k in ("python", "duckdb", "pyiceberg", "pyarrow", "cpu_count")})
df"""),
    md("## 1. Correctness: did every run erase and verify?"),
    code("""total = int(df["repeats"].sum())
print(f"Deletion verified (current state): {int(df['verified_runs'].sum())}/{total}")
print(f"Cleanup verified (no physical copy left): {int(df['cleanup_verified_runs'].sum())}/{total}")

assert df["all_runs_verified"].all()
assert df["all_cleanups_verified"].all()
assert (df["residual_copies_after_cleanup"] == 0).all()"""),
    md("""## 2. Deletion latency and where the time goes

`delete_user` = DuckDB transaction over 4 tables + Iceberg delete. The split shows which part dominates."""),
    code("""fig, ax = plt.subplots(figsize=(8, 4.5))
x = range(len(df))
ax.bar(x, df["mean_duckdb_latency_ms"], label="DuckDB (4 tables, 1 transaction)")
ax.bar(x, df["mean_iceberg_latency_ms"], bottom=df["mean_duckdb_latency_ms"], label="Iceberg delete + verify")
other = df["mean_latency_ms"] - df["mean_duckdb_latency_ms"] - df["mean_iceberg_latency_ms"]
ax.bar(x, other, bottom=df["mean_duckdb_latency_ms"] + df["mean_iceberg_latency_ms"], label="Other (DuckDB connect, bookkeeping)", color="0.75")
ax.set_xticks(list(x))
ax.set_xticklabels([f"{u:,}" for u in df["users"]])
ax.set_xlabel("Synthetic users")
ax.set_ylabel("Mean latency (ms)")
ax.set_title("Current-state deletion latency, by component")
ax.legend()
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(ROOT / "results" / "deletion_latency.png", dpi=150)
plt.show()

df["iceberg_share_pct"] = (100 * df["mean_iceberg_latency_ms"] / df["mean_latency_ms"]).round(1)
df[["users", "mean_latency_ms", "median_latency_ms", "min_latency_ms", "max_latency_ms", "iceberg_share_pct"]]"""),
    md("""## 3. Exposure window: deleting rows is not erasing data

Right after current-state deletion, copies of the user remain on disk: the Parquet history copy
and the old Iceberg data file (Iceberg rewrites data files on delete and keeps the old one until
snapshot expiry). Cleanup removes them."""),
    code("""exposure = df[["users", "residual_copies_after_deletion", "residual_copies_after_cleanup", "mean_cleanup_latency_ms"]]
exposure"""),
    code("""fig, ax = plt.subplots(figsize=(8, 4))
ax.plot(df["users"], df["mean_latency_ms"], marker="o", label="Current-state deletion")
ax.plot(df["users"], df["mean_cleanup_latency_ms"], marker="s", label="Historical cleanup")
ax.fill_between(df["users"], df["min_latency_ms"], df["max_latency_ms"], alpha=0.15, label="Deletion min-max")
ax.set_xscale("log")
ax.set_xlabel("Synthetic users (log scale)")
ax.set_ylabel("Latency (ms)")
ax.set_title("Deletion vs cleanup latency")
ax.legend()
ax.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(ROOT / "results" / "latency_vs_cleanup.png", dpi=150)
plt.show()"""),
    md("## 4. Storage reclaimed and illustrative cost"),
    code("""cost = df[["users", "mean_storage_reclaimed_bytes", "estimated_total_compute_cost_usd", "estimated_monthly_storage_savings_usd"]].copy()
cost["mean_storage_reclaimed_kib"] = (cost["mean_storage_reclaimed_bytes"] / 1024).round(1)
cost"""),
    md("""**Reading the cost columns:** at these sizes both numbers are on the order of micro-dollars and use
assumed rates ($0.05/compute-hour, $0.023/GB-month). They show the method, not a real bill."""),
    code("""summary = df[["users", "repeats", "mean_latency_ms", "median_latency_ms", "verified_runs", "all_runs_verified"]]
summary.to_csv(ROOT / "results" / "benchmark_summary.csv", index=False)
summary"""),
]

nb.metadata = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python"},
}

NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
nbf.write(nb, NOTEBOOK_PATH)
print(f"Notebook created: {NOTEBOOK_PATH} ({len(nb.cells)} cells)")
