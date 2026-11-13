
from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from src.erasure.iceberg_store import (
    replace_pipeline_users,
    delete_pipeline_user,
    verify_pipeline_storage,
    expire_pipeline_history,
)


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "processed"
RESULTS_DIR = ROOT / "results"
DB_PATH = DATA_DIR / "lakehouse.duckdb"
SNAPSHOT_DIR = DATA_DIR / "snapshots"
AUDIT_PATH = RESULTS_DIR / "deletion_audit.jsonl"


def ensure_dirs() -> None:
    for path in (DATA_DIR, RESULTS_DIR, SNAPSHOT_DIR):
        path.mkdir(parents=True, exist_ok=True)


def connect() -> duckdb.DuckDBPyConnection:
    ensure_dirs()
    return duckdb.connect(str(DB_PATH))


def create_demo_data(users: int = 1000, seed: int = 42) -> dict:
    """Create synthetic data in DuckDB and synchronize it to Iceberg."""

    if users < 10:
        raise ValueError("Use at least 10 users for the demo.")

    ensure_dirs()

    rng = np.random.default_rng(seed)
    user_ids = [f"U{i:06d}" for i in range(users)]
    now = datetime.now(timezone.utc).isoformat()

    source = pd.DataFrame({
        "user_id": user_ids,
        "age": rng.integers(18, 70, size=users),
        "rating": rng.integers(1, 6, size=users),
        "created_at": [now] * users,
    })

    derived = source[["user_id", "rating"]].copy()
    derived["rating_bucket"] = pd.cut(
        derived["rating"],
        bins=[0, 2, 4, 5],
        labels=["low", "medium", "high"],
    ).astype(str)

    features = pd.DataFrame({
        "user_id": user_ids,
        "rating_mean": source["rating"].astype(float),
        "activity_count": rng.integers(1, 100, size=users),
    })

    vectors = pd.DataFrame({
        "vector_id": [f"V{i:06d}" for i in range(users)],
        "user_id": user_ids,
        "embedding": [
            json.dumps(row.tolist())
            for row in rng.normal(size=(users, 8))
        ],
    })

    con = connect()

    try:
        for name, frame in (
            ("source_records", source),
            ("derived_records", derived),
            ("feature_store", features),
            ("vector_index", vectors),
        ):
            con.register("incoming_frame", frame)
            con.execute(
                f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM incoming_frame"
            )
            con.unregister("incoming_frame")

        snapshot_id = uuid.uuid4().hex[:12]
        snapshot_file = SNAPSHOT_DIR / f"snapshot_{snapshot_id}.parquet"

        snapshot_path_sql = (
            str(snapshot_file).replace("\\", "/").replace("'", "''")
        )

        con.execute(
            f"COPY source_records TO '{snapshot_path_sql}' (FORMAT PARQUET)"
        )

        con.execute("""
            CREATE TABLE IF NOT EXISTS snapshot_catalog (
                snapshot_id VARCHAR,
                file_path VARCHAR,
                created_at TIMESTAMP,
                expired BOOLEAN DEFAULT FALSE
            )
        """)

        con.execute(
            "INSERT INTO snapshot_catalog VALUES (?, ?, current_timestamp, FALSE)",
            [snapshot_id, str(snapshot_file)],
        )

    finally:
        con.close()

    iceberg_sync = replace_pipeline_users(user_ids)

    return {
        "users": users,
        "snapshot_id": snapshot_id,
        "snapshot_path": str(snapshot_file),
        "iceberg_sync": iceberg_sync,
    }


def count_user_rows(con, user_id: str) -> dict[str, int]:
    """Count a user's records in the four DuckDB stores."""

    tables = (
        "source_records",
        "derived_records",
        "feature_store",
        "vector_index",
    )

    counts = {}

    for table in tables:
        counts[table] = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE user_id = ?",
            [user_id],
        ).fetchone()[0]

    return counts


def delete_user(user_id: str) -> dict:
    """
    Delete a user from DuckDB and Iceberg.

    DuckDB and Iceberg do not share a distributed transaction.
    Partial failures are reported explicitly.
    """

    if not user_id or not user_id.strip():
        raise ValueError("user_id cannot be empty")

    started = time.perf_counter()
    request_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    con = connect()
    iceberg_result = None
    iceberg_error = None
    duckdb_started = time.perf_counter()

    try:
        before = count_user_rows(con, user_id)

        con.execute("BEGIN TRANSACTION")

        for table in (
            "source_records",
            "derived_records",
            "feature_store",
            "vector_index",
        ):
            con.execute(
                f"DELETE FROM {table} WHERE user_id = ?",
                [user_id],
            )

        con.execute("COMMIT")

        after = count_user_rows(con, user_id)

    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise

    finally:
        con.close()

    duckdb_ms = (time.perf_counter() - duckdb_started) * 1000
    iceberg_started = time.perf_counter()

    try:
        iceberg_result = delete_pipeline_user(user_id)
    except Exception as exc:
        iceberg_error = str(exc)

    iceberg_ms = (time.perf_counter() - iceberg_started) * 1000
    elapsed_ms = (time.perf_counter() - started) * 1000

    duckdb_verified = all(value == 0 for value in after.values())

    iceberg_verified = (
        iceberg_result is not None
        and iceberg_result["verified"]
    )

    result = {
        "request_id": request_id,
        "user_id": user_id,
        "timestamp": timestamp,
        "before": before,
        "after": after,
        "iceberg": iceberg_result,
        "iceberg_error": iceberg_error,
        "verified_current_state": (
            duckdb_verified and iceberg_verified
        ),
        "latency_ms": round(elapsed_ms, 3),
        "duckdb_latency_ms": round(duckdb_ms, 3),
        "iceberg_latency_ms": round(iceberg_ms, 3),
        "historical_snapshots_expired": False,
        "note": (
            "Current-state deletion is separate from historical "
            "snapshot and physical-file cleanup."
        ),
    }

    ensure_dirs()

    with AUDIT_PATH.open("a", encoding="utf-8") as file:
        file.write(json.dumps(result) + "\n")

    return result


def _display_path(path: Path) -> str:
    """Project-relative path when possible, absolute otherwise."""
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def expire_snapshots() -> dict:
    """Expire DuckDB snapshots and Iceberg historical snapshots."""

    con = connect()
    removed = []
    failures = []
    deleted_files = []

    try:
        snapshots = con.execute(
            """
            SELECT snapshot_id, file_path
            FROM snapshot_catalog
            WHERE expired = FALSE
            """
        ).fetchall()

        for snapshot_id, raw_path in snapshots:
            path = Path(raw_path)

            try:
                existed = path.exists()

                if existed:
                    path.unlink()

                if path.exists():
                    failures.append(snapshot_id)
                else:
                    if existed:
                        deleted_files.append(
                            {
                                "store": "parquet_history_copy",
                                "snapshot_id": snapshot_id,
                                "path": _display_path(path),
                            }
                        )
                    con.execute(
                        """
                        UPDATE snapshot_catalog
                        SET expired = TRUE
                        WHERE snapshot_id = ?
                        """,
                        [snapshot_id],
                    )
                    removed.append(snapshot_id)

            except OSError:
                failures.append(snapshot_id)

    finally:
        con.close()

    iceberg_cleanup = None
    iceberg_error = None

    try:
        iceberg_cleanup = expire_pipeline_history()
    except Exception as exc:
        iceberg_error = str(exc)

    for orphan in (iceberg_cleanup or {}).get("orphan_file_paths_removed", []):
        deleted_files.append(
            {
                "store": "iceberg_data_file",
                "path": _display_path(Path(orphan)),
            }
        )

    # Keep original result keys for backward compatibility with tests.
    return {
        "deleted_files": deleted_files,
        "snapshots_removed": len(removed),
        "snapshot_ids_removed": removed,
        "failures": failures,

        "duckdb_snapshots_removed": len(removed),
        "duckdb_snapshot_ids_removed": removed,
        "duckdb_failures": failures,

        "iceberg_cleanup": iceberg_cleanup,
        "iceberg_error": iceberg_error,

        "verified": (
            not failures
            and iceberg_error is None
        ),
    }


def verify_user_absent(user_id: str) -> dict:
    """Verify absence from DuckDB, historical files and Iceberg."""

    if not user_id or not user_id.strip():
        raise ValueError("user_id cannot be empty")

    con = connect()

    try:
        current = count_user_rows(con, user_id)

        snapshot_rows = con.execute(
            """
            SELECT snapshot_id, file_path, expired
            FROM snapshot_catalog
            """
        ).fetchall()

    finally:
        con.close()

    remaining_snapshot_files = []

    for snapshot_id, raw_path, expired in snapshot_rows:
        path = Path(raw_path)

        if path.exists():
            try:
                frame = pd.read_parquet(path)

                if (
                    "user_id" in frame
                    and (frame["user_id"] == user_id).any()
                ):
                    remaining_snapshot_files.append(snapshot_id)

            except Exception as exc:
                remaining_snapshot_files.append({
                    "snapshot_id": snapshot_id,
                    "error": str(exc),
                })

    try:
        iceberg = verify_pipeline_storage(user_id)
        iceberg_error = None

    except Exception as exc:
        iceberg = None
        iceberg_error = str(exc)

    duckdb_verified = (
        all(value == 0 for value in current.values())
        and not remaining_snapshot_files
    )

    iceberg_verified = (
        iceberg is not None
        and iceberg["verified"]
    )

    return {
        "user_id": user_id,
        "current_store_rows": current,
        "remaining_snapshot_files": remaining_snapshot_files,
        "iceberg": iceberg,
        "iceberg_error": iceberg_error,
        "verified": duckdb_verified and iceberg_verified,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the end-to-end erasure demonstration."
    )
    parser.add_argument("--users", type=int, default=1000,
                        help="number of synthetic users (default: 1000)")
    parser.add_argument("--user-id", default="U000007",
                        help="user to erase (default: U000007)")
    args = parser.parse_args(argv)

    created = create_demo_data(users=args.users)

    print("\nDATA CREATED:")
    print(json.dumps(created, indent=2))

    print("\nDELETION:")
    print(json.dumps(delete_user(args.user_id), indent=2))

    cleanup = expire_snapshots()

    print("\nSNAPSHOT EXPIRY:")
    print(json.dumps(cleanup, indent=2))

    print("\nFILES DELETED BY CLEANUP:")
    if cleanup["deleted_files"]:
        for item in cleanup["deleted_files"]:
            print(f"  [{item['store']}] {item['path']}")
    else:
        print("  (none)")

    print("\nFINAL VERIFICATION:")
    final = verify_user_absent(args.user_id)
    print(json.dumps(final, indent=2))

    # Non-zero exit code if the checked locations still contain the user.
    return 0 if final["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
