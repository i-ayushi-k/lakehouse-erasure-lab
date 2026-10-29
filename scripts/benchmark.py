
import csv
import statistics
import time

from src.erasure import pipeline


SIZES = [100, 1_000, 5_000, 10_000]
REPEATS = 3


def benchmark_one_size(users: int) -> dict:
    latencies = []
    verified_runs = 0

    for repeat in range(REPEATS):
        # Recreate the dataset for every repetition.
        pipeline.create_demo_data(users=users, seed=42 + repeat)

        # Select a known user that exists in this dataset.
        target = f"U{users // 2:06d}"

        result = pipeline.delete_user(target)
        latencies.append(result["latency_ms"])

        if result["verified_current_state"]:
            verified_runs += 1

        # Expire historical snapshots before the next repetition.
        pipeline.expire_snapshots()

    return {
        "users": users,
        "repeats": REPEATS,
        "mean_latency_ms": round(statistics.mean(latencies), 3),
        "median_latency_ms": round(statistics.median(latencies), 3),
        "min_latency_ms": round(min(latencies), 3),
        "max_latency_ms": round(max(latencies), 3),
        "verified_runs": verified_runs,
        "all_runs_verified": verified_runs == REPEATS,
    }


def main():
    output = pipeline.ROOT / "results" / "scale_benchmark.csv"
    output.parent.mkdir(parents=True, exist_ok=True)

    rows = []

    for size in SIZES:
        print(f"Benchmarking {size:,} users...")
        row = benchmark_one_size(size)
        rows.append(row)
        print(row)

    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nSaved benchmark results to: {output}")


if __name__ == "__main__":
    main()
