
from pathlib import Path
import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = ROOT / "notebooks" / "results.ipynb"

nb = nbf.v4.new_notebook()

nb.cells = [
    nbf.v4.new_markdown_cell(
        """# Lakehouse Erasure Lab — Results

This notebook analyzes reproducible deletion benchmarks from the local
DuckDB/Parquet implementation.

The experiment measures current-state deletion latency across four
synthetic dataset sizes. Each size is tested three times.

Historical snapshot expiry is a separate operation and is not included
in deletion latency measurements.

These results do not represent Apache Iceberg or production vector
database performance."""
    ),
    nbf.v4.new_code_cell(
        """from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path.cwd()
if not (ROOT / "results" / "scale_benchmark.csv").exists():
    ROOT = Path.cwd().parent

csv_path = ROOT / "results" / "scale_benchmark.csv"
df = pd.read_csv(csv_path)
display(df)"""
    ),
    nbf.v4.new_code_cell(
        """print("Dataset sizes:", len(df))
print("Total runs:", int(df["repeats"].sum()))
print("Verified runs:", int(df["verified_runs"].sum()))
print("All runs verified:", bool(df["all_runs_verified"].all()))

assert df["all_runs_verified"].all()
assert (df["mean_latency_ms"] > 0).all()"""
    ),
    nbf.v4.new_code_cell(
        """plt.figure(figsize=(8, 5))
plt.plot(df["users"], df["mean_latency_ms"], marker="o")
plt.xlabel("Number of synthetic users")
plt.ylabel("Mean deletion latency (ms)")
plt.title("Deletion Latency vs Dataset Size")
plt.grid(True, alpha=0.3)
plt.tight_layout()

chart_path = ROOT / "results" / "deletion_latency.png"
plt.savefig(chart_path, dpi=150, bbox_inches="tight")
plt.show()

print(f"Chart saved to {chart_path}")"""
    ),
    nbf.v4.new_code_cell(
        """summary = df[
    [
        "users",
        "repeats",
        "mean_latency_ms",
        "median_latency_ms",
        "verified_runs",
        "all_runs_verified",
    ]
]

summary.to_csv(ROOT / "results" / "benchmark_summary.csv", index=False)
display(summary)"""
    ),
]

nb.metadata = {
    "kernelspec": {
        "display_name": "Python 3 (.venv)",
        "language": "python",
        "name": "python3",
    },
    "language_info": {"name": "python", "version": "3.11"},
}

NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
nbf.write(nb, NOTEBOOK_PATH)

print(f"Notebook created: {NOTEBOOK_PATH}")
print(f"Cells created: {len(nb.cells)}")
