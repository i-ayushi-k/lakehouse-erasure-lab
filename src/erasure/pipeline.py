
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


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
    """Create reproducible source, derived, feature, and vector records."""
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
        derived["rating"], bins=[0, 2, 4, 5],
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
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM incoming_frame")
            con.unregister("incoming_frame")

        # Export a Parquet snapshot before deletion to model retained history.
        snapshot_id = uuid.uuid4().hex[:12]
        snapshot_file = SNAPSHOT_DIR / f"snapshot_{snapshot_id}.parquet"
        snapshot_path_sql = str(snapshot_file).replace("\\", "/").replace("'", "''")
        con.execute(
            f"COPY source_records TO '{snapshot_path_sql}' (FORMAT PARQUET)"
        )

        # Snapshot catalog supports explicit retention and expiry checks.
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

    return {
        "users": users,
        "snapshot_id": snapshot_id,
        "snapshot_path": str(snapshot_file),
    }


def count_user_rows(con, user_id: str) -> dict[str, int]:
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
    """Delete a user from all current-state stores and verify the result.

    Historical snapshot files are handled by the separate retention workflow.
    """
    if not user_id or not user_id.strip():
        raise ValueError("user_id cannot be empty")

    started = time.perf_counter()
    request_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    con = connect()

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

    elapsed_ms = (time.perf_counter() - started) * 1000
    verified = all(count == 0 for count in after.values())

    result = {
        "request_id": request_id,
        "user_id": user_id,
        "timestamp": timestamp,
        "before": before,
        "after": after,
        "verified_current_state": verified,
        "latency_ms": round(elapsed_ms, 3),
        "historical_snapshots_expired": False,
        "note": "Current-state deletion does not erase retained snapshot files.",
    }

    ensure_dirs()
    with AUDIT_PATH.open("a", encoding="utf-8") as file:
        file.write(json.dumps(result) + "\n")

    return result


def expire_snapshots() -> dict:
    """Expire registered snapshot files and verify their removal."""
    con = connect()
    removed = []
    failures = []

    try:
        snapshots = con.execute(
            "SELECT snapshot_id, file_path FROM snapshot_catalog WHERE expired = FALSE"
        ).fetchall()

        for snapshot_id, raw_path in snapshots:
            path = Path(raw_path)
            try:
                if path.exists():
                    path.unlink()
                if path.exists():
                    failures.append(snapshot_id)
                else:
                    con.execute(
                        "UPDATE snapshot_catalog SET expired = TRUE WHERE snapshot_id = ?",
                        [snapshot_id],
                    )
                    removed.append(snapshot_id)
            except OSError:
                failures.append(snapshot_id)
    finally:
        con.close()

    return {
        "snapshots_removed": len(removed),
        "snapshot_ids_removed": removed,
        "failures": failures,
        "verified": not failures,
    }


def verify_user_absent(user_id: str) -> dict:
    con = connect()
    try:
        current = count_user_rows(con, user_id)
        snapshot_rows = con.execute(
            "SELECT snapshot_id, file_path, expired FROM snapshot_catalog"
        ).fetchall()
    finally:
        con.close()

    remaining_snapshot_files = []
    for snapshot_id, raw_path, expired in snapshot_rows:
        path = Path(raw_path)
        if path.exists():
            try:
                frame = pd.read_parquet(path)
                if "user_id" in frame and (frame["user_id"] == user_id).any():
                    remaining_snapshot_files.append(snapshot_id)
            except Exception as exc:
                remaining_snapshot_files.append(
                    {"snapshot_id": snapshot_id, "error": str(exc)}
                )

    return {
        "user_id": user_id,
        "current_store_rows": current,
        "remaining_snapshot_files": remaining_snapshot_files,
        "verified": (
            all(v == 0 for v in current.values())
            and not remaining_snapshot_files
        ),
    }


if __name__ == "__main__":
    created = create_demo_data()
    target = "U000007"
    print("DATA CREATED:", json.dumps(created, indent=2))
    print("DELETION:", json.dumps(delete_user(target), indent=2))
    print("SNAPSHOT EXPIRY:", json.dumps(expire_snapshots(), indent=2))
    print("FINAL VERIFICATION:", json.dumps(verify_user_absent(target), indent=2))
