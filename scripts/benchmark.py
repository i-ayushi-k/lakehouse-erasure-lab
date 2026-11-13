"""Scale benchmark for the integrated DuckDB + Iceberg erasure pipeline.

Run from the repository root with either:
    python scripts/benchmark.py
    python -m scripts.benchmark

For every dataset size the benchmark repeats the full cycle
(generate -> delete one user -> expire history) and records:

* current-state deletion latency, split into DuckDB and Iceberg parts;
* historical cleanup latency;
* how many physical copies of the erased user still exist right after the
  current-state deletion (the "exposure window") and after cleanup;
* bytes of Parquet data reclaimed by cleanup.

Everything runs in a scratch directory, so the demo database and Iceberg
warehouse in ``data/`` are never touched.
"""

import csv
import json
import os
import platform
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

# Make ``src`` importable when executed as a plain script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.erasure import iceberg_store, pipeline  # noqa: E402

SIZES = [100, 1_000, 5_000, 10_000]
REPEATS = 5
SCRATCH_ROOT = None

# Illustrative assumptions, NOT actual cloud billing rates.
COMPUTE_RATE_USD_PER_HOUR = 0.05
STORAGE_RATE_USD_PER_GB_MONTH = 0.023
BYTES_PER_GB = 1024 ** 3


def use_scratch_dir(base: Path) -> None:
    """Give DuckDB and Iceberg separate storage directories."""

    results = base / "results"

    pipeline.DATA_DIR = base / "data"
    pipeline.RESULTS_DIR = results
    pipeline.DB_PATH = pipeline.DATA_DIR / "lakehouse.duckdb"
    pipeline.SNAPSHOT_DIR = pipeline.DATA_DIR / "snapshots"
    pipeline.AUDIT_PATH = results / "deletion_audit.jsonl"

    # Keep Iceberg completely separate from DuckDB data.
    iceberg_store.ICEBERG_DIR = base / "iceberg"


def reset_scratch() -> None:
    """Remove all state between repeats so runs are independent."""
    shutil.rmtree(pipeline.DATA_DIR, ignore_errors=True)


def data_file_bytes() -> int:
    """Bytes of Parquet data files that can hold personal data.

    Iceberg metadata (manifests, metadata.json) is deliberately excluded:
    it grows with every commit and would mask the data-file savings.
    """
    total = 0
    for directory in (pipeline.SNAPSHOT_DIR, iceberg_store.pipeline_data_dir()):
        if directory.exists():
            total += sum(p.stat().st_size for p in directory.glob("*.parquet"))
    return total


def residual_copies(user_id: str) -> int:
    """Physical copies of the user outside the active DuckDB/Iceberg state."""
    check = pipeline.verify_user_absent(user_id)
    copies = len(check["remaining_snapshot_files"])
    if check["iceberg"] is not None:
        copies += len(check["iceberg"]["physical_files_containing_user"])
    return copies


def compute_cost(execution_ms: float) -> float:
    return execution_ms / 3_600_000 * COMPUTE_RATE_USD_PER_HOUR


def one_cycle(users: int, seed: int) -> dict:
    global SCRATCH_ROOT

    # Every repetition gets a fresh, independent directory.
    cycle_dir = Path(
        tempfile.mkdtemp(
            prefix="cycle_",
            dir=SCRATCH_ROOT
        )
    )

    use_scratch_dir(cycle_dir)

    pipeline.create_demo_data(users=users, seed=seed)
    target = f"U{users // 2:06d}"

    deletion = pipeline.delete_user(target)
    residual_before = residual_copies(target)
    bytes_before = data_file_bytes()

    started = time.perf_counter()
    cleanup = pipeline.expire_snapshots()
    cleanup_ms = (time.perf_counter() - started) * 1000

    bytes_after = data_file_bytes()
    residual_after = residual_copies(target)

    return {
        "deletion_ms": deletion["latency_ms"],
        "duckdb_ms": deletion["duckdb_latency_ms"],
        "iceberg_ms": deletion["iceberg_latency_ms"],
        "cleanup_ms": cleanup_ms,
        "deletion_ok": bool(deletion["verified_current_state"]),
        # Strict: no operation errors AND no physical copy of the user remains.
        "cleanup_ok": bool(cleanup["verified"]) and residual_after == 0,
        "residual_before": residual_before,
        "residual_after": residual_after,
        "reclaimed_bytes": bytes_before - bytes_after,
    }


def benchmark_one_size(users: int) -> dict:
    runs = [one_cycle(users, seed=42 + i) for i in range(REPEATS)]

    def col(key):
        return [r[key] for r in runs]

    mean_del = statistics.mean(col("deletion_ms"))
    mean_clean = statistics.mean(col("cleanup_ms"))
    reclaimed = statistics.mean(col("reclaimed_bytes"))
    reclaimed_gb = reclaimed / BYTES_PER_GB
    compute_total = compute_cost(mean_del) + compute_cost(mean_clean)
    monthly_savings = reclaimed_gb * STORAGE_RATE_USD_PER_GB_MONTH

    return {
        "users": users,
        "repeats": REPEATS,
        "mean_latency_ms": round(mean_del, 3),
        "median_latency_ms": round(statistics.median(col("deletion_ms")), 3),
        "min_latency_ms": round(min(col("deletion_ms")), 3),
        "max_latency_ms": round(max(col("deletion_ms")), 3),
        "mean_duckdb_latency_ms": round(statistics.mean(col("duckdb_ms")), 3),
        "mean_iceberg_latency_ms": round(statistics.mean(col("iceberg_ms")), 3),
        "mean_cleanup_latency_ms": round(mean_clean, 3),
        "verified_runs": sum(col("deletion_ok")),
        "all_runs_verified": all(col("deletion_ok")),
        "cleanup_verified_runs": sum(col("cleanup_ok")),
        "all_cleanups_verified": all(col("cleanup_ok")),
        "residual_copies_after_deletion": statistics.mean(col("residual_before")),
        "residual_copies_after_cleanup": statistics.mean(col("residual_after")),
        "mean_storage_reclaimed_bytes": round(reclaimed, 2),
        "compute_rate_usd_per_hour": COMPUTE_RATE_USD_PER_HOUR,
        "storage_rate_usd_per_gb_month": STORAGE_RATE_USD_PER_GB_MONTH,
        "estimated_total_compute_cost_usd": round(compute_total, 10),
        "estimated_monthly_storage_savings_usd": round(monthly_savings, 10),
    }


def environment() -> dict:
    import duckdb
    import pandas
    import pyarrow
    import pyiceberg

    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "duckdb": duckdb.__version__,
        "pyiceberg": pyiceberg.__version__,
        "pyarrow": pyarrow.__version__,
        "pandas": pandas.__version__,
        "sizes": SIZES,
        "repeats": REPEATS,
    }


def main() -> None:
    results_dir = pipeline.ROOT / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    scratch = Path(tempfile.mkdtemp(prefix="erasure_bench_"))
    SCRATCH_ROOT = scratch

    print("LAKEHOUSE ERASURE BENCHMARK")
    print("===========================")
    print(f"Dataset sizes: {SIZES}   repeats per size: {REPEATS}")
    print(f"Scratch directory: {scratch}")

    try:
        # Warm-up (not recorded): imports, catalog creation, filesystem cache.
        one_cycle(100, seed=0)

        rows = []
        for size in SIZES:
            print(f"Benchmarking {size:,} users...", flush=True)
            row = benchmark_one_size(size)
            rows.append(row)
            print(
                f"  deletion {row['mean_latency_ms']} ms "
                f"(duckdb {row['mean_duckdb_latency_ms']} / "
                f"iceberg {row['mean_iceberg_latency_ms']}), "
                f"cleanup {row['mean_cleanup_latency_ms']} ms, "
                f"copies {row['residual_copies_after_deletion']} -> "
                f"{row['residual_copies_after_cleanup']}, "
                f"verified {row['verified_runs']}/{REPEATS}"
            )
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    output = results_dir / "scale_benchmark.csv"
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    (results_dir / "benchmark_environment.json").write_text(
        json.dumps(environment(), indent=2) + "\n", encoding="utf-8"
    )

    print(f"\nSaved: {output}")


if __name__ == "__main__":
    main()
