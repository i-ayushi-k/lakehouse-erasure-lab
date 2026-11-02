
import csv
import statistics
import time
from pathlib import Path

from src.erasure import pipeline


SIZES = [100, 1_000, 5_000, 10_000]
REPEATS = 3

# Illustrative assumptions, NOT actual cloud billing rates.
COMPUTE_RATE_USD_PER_HOUR = 0.05
STORAGE_RATE_USD_PER_GB_MONTH = 0.023

BYTES_PER_GB = 1024 ** 3


def get_storage_bytes() -> int:
    """Measure files in the demo's historical storage locations."""

    directories = [
        pipeline.SNAPSHOT_DIR,
        (
            pipeline.ROOT
            / "data"
            / "iceberg_warehouse"
            / "erasure_lab"
            / "pipeline_users"
        ),
    ]

    total = 0

    for directory in directories:
        if directory.exists():
            for path in directory.rglob("*"):
                if path.is_file():
                    total += path.stat().st_size

    return total


def compute_cost(execution_ms: float) -> float:
    """Estimate compute cost using the declared hourly rate."""

    execution_hours = execution_ms / 3_600_000

    return execution_hours * COMPUTE_RATE_USD_PER_HOUR


def benchmark_one_size(users: int) -> dict:
    deletion_latencies = []
    cleanup_latencies = []

    deletion_costs = []
    cleanup_costs = []

    reclaimed_bytes = []
    verified_runs = 0
    cleanup_verified_runs = 0

    for repeat in range(REPEATS):
        # Create a fresh synthetic dataset for each repetition.
        pipeline.create_demo_data(
            users=users,
            seed=42 + repeat,
        )

        target = f"U{users // 2:06d}"

        # Measure storage immediately before deletion and cleanup.
        storage_before = get_storage_bytes()

        # Measure current-state deletion.
        deletion_result = pipeline.delete_user(target)

        deletion_ms = deletion_result["latency_ms"]
        deletion_latencies.append(deletion_ms)
        deletion_costs.append(compute_cost(deletion_ms))

        if deletion_result["verified_current_state"]:
            verified_runs += 1

        # Measure historical snapshot and orphan-file cleanup separately.
        cleanup_started = time.perf_counter()

        cleanup_result = pipeline.expire_snapshots()

        cleanup_ms = (
            time.perf_counter() - cleanup_started
        ) * 1000

        cleanup_latencies.append(cleanup_ms)
        cleanup_costs.append(compute_cost(cleanup_ms))

        if cleanup_result["verified"]:
            cleanup_verified_runs += 1

        # Measure physical storage after cleanup.
        storage_after = get_storage_bytes()

        reclaimed = max(0, storage_before - storage_after)
        reclaimed_bytes.append(reclaimed)

    mean_deletion_cost = statistics.mean(deletion_costs)
    mean_cleanup_cost = statistics.mean(cleanup_costs)

    mean_reclaimed_bytes = statistics.mean(reclaimed_bytes)
    mean_reclaimed_gb = mean_reclaimed_bytes / BYTES_PER_GB

    monthly_storage_savings = (
        mean_reclaimed_gb * STORAGE_RATE_USD_PER_GB_MONTH
    )

    total_compute_cost = (
        mean_deletion_cost + mean_cleanup_cost
    )

    net_first_month_cost = (
        total_compute_cost - monthly_storage_savings
    )

    return {
        "users": users,
        "repeats": REPEATS,

        "mean_latency_ms": round(
            statistics.mean(deletion_latencies), 3
        ),
        "median_latency_ms": round(
            statistics.median(deletion_latencies), 3
        ),
        "min_latency_ms": round(min(deletion_latencies), 3),
        "max_latency_ms": round(max(deletion_latencies), 3),

        "mean_cleanup_latency_ms": round(
            statistics.mean(cleanup_latencies), 3
        ),

        "verified_runs": verified_runs,
        "all_runs_verified": verified_runs == REPEATS,

        "cleanup_verified_runs": cleanup_verified_runs,
        "all_cleanups_verified": (
            cleanup_verified_runs == REPEATS
        ),

        "compute_rate_usd_per_hour": COMPUTE_RATE_USD_PER_HOUR,
        "storage_rate_usd_per_gb_month": (
            STORAGE_RATE_USD_PER_GB_MONTH
        ),

        "estimated_deletion_compute_cost_usd": round(
            mean_deletion_cost, 10
        ),
        "estimated_cleanup_compute_cost_usd": round(
            mean_cleanup_cost, 10
        ),
        "estimated_total_compute_cost_usd": round(
            total_compute_cost, 10
        ),

        "mean_storage_reclaimed_bytes": round(
            mean_reclaimed_bytes, 2
        ),
        "mean_storage_reclaimed_gb": round(
            mean_reclaimed_gb, 10
        ),

        "estimated_monthly_storage_savings_usd": round(
            monthly_storage_savings, 10
        ),

        "estimated_net_first_month_cost_usd": round(
            net_first_month_cost, 10
        ),
    }


def main():
    output = (
        pipeline.ROOT
        / "results"
        / "scale_benchmark.csv"
    )

    output.parent.mkdir(parents=True, exist_ok=True)

    rows = []

    print("LAKEHOUSE ERASURE BENCHMARK")
    print("===========================")
    print(f"Dataset sizes: {SIZES}")
    print(f"Repetitions per size: {REPEATS}")
    print(
        "Illustrative compute rate: "
        f"${COMPUTE_RATE_USD_PER_HOUR}/hour"
    )
    print(
        "Illustrative storage rate: "
        f"${STORAGE_RATE_USD_PER_GB_MONTH}/GB-month"
    )

    for size in SIZES:
        print(f"\nBenchmarking {size:,} users...")

        row = benchmark_one_size(size)
        rows.append(row)

        for key, value in row.items():
            print(f"{key}: {value}")

    with output.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=rows[0].keys(),
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nSaved benchmark results to: {output}")


if __name__ == "__main__":
    main()

