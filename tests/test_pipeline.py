
import os
import subprocess
import sys

import pytest

from src.erasure import iceberg_store, pipeline

TARGET = "U000007"


@pytest.fixture
def isolated_lab(tmp_path, monkeypatch):
    """Give each test its own DuckDB, snapshots, Iceberg catalog and audit log."""

    data_dir = tmp_path / "data"
    results_dir = tmp_path / "results"
    iceberg_dir = tmp_path / "iceberg"

    monkeypatch.setattr(pipeline, "DATA_DIR", data_dir)
    monkeypatch.setattr(pipeline, "RESULTS_DIR", results_dir)
    monkeypatch.setattr(pipeline, "DB_PATH", data_dir / "lakehouse.duckdb")
    monkeypatch.setattr(pipeline, "SNAPSHOT_DIR", data_dir / "snapshots")
    monkeypatch.setattr(pipeline, "AUDIT_PATH", results_dir / "deletion_audit.jsonl")

    # Keep Iceberg storage separate from DuckDB and Parquet storage.
    monkeypatch.setattr(iceberg_store, "ICEBERG_DIR", iceberg_dir)

    pipeline.create_demo_data(users=20, seed=42)

    yield


def test_delete_user_from_all_stores(isolated_lab):
    result = pipeline.delete_user(TARGET)

    assert result["verified_current_state"] is True
    assert all(count == 0 for count in result["after"].values())
    assert result["iceberg"]["before"] == 1
    assert result["iceberg"]["after"] == 0

    verification = pipeline.verify_user_absent(TARGET)

    assert all(
        count == 0
        for count in verification["current_store_rows"].values()
    )


def test_deletion_leaves_historical_copies_until_cleanup(isolated_lab):
    """Current-state deletion alone must NOT be enough: copies persist until cleanup."""

    pipeline.delete_user(TARGET)

    before = pipeline.verify_user_absent(TARGET)

    assert before["verified"] is False
    assert before["remaining_snapshot_files"]
    assert before["iceberg"]["current_table_rows"] == 0
    assert before["iceberg"]["physical_files_containing_user"]

    cleanup = pipeline.expire_snapshots()

    assert cleanup["verified"] is True
    assert cleanup["snapshots_removed"] == 1
    assert cleanup["iceberg_cleanup"]["orphan_files_removed"]

    after = pipeline.verify_user_absent(TARGET)

    assert after["verified"] is True
    assert after["remaining_snapshot_files"] == []
    assert after["iceberg"]["physical_files_containing_user"] == []


def test_other_users_survive_erasure(isolated_lab):
    pipeline.delete_user(TARGET)
    pipeline.expire_snapshots()

    con = pipeline.connect()

    try:
        remaining = con.execute(
            "SELECT COUNT(*) FROM source_records"
        ).fetchone()[0]
    finally:
        con.close()

    assert remaining == 19

    # Iceberg's active data file must still hold the other 19 users.
    table = iceberg_store.create_pipeline_table()

    assert table.scan().to_arrow().num_rows == 19


def test_empty_user_id_is_rejected(isolated_lab):
    with pytest.raises(ValueError):
        pipeline.delete_user("")

    with pytest.raises(ValueError):
        pipeline.delete_user("   ")


def test_repeated_deletion_is_idempotent(isolated_lab):
    first = pipeline.delete_user(TARGET)
    second = pipeline.delete_user(TARGET)

    assert first["verified_current_state"] is True
    assert second["verified_current_state"] is True
    assert second["before"]["source_records"] == 0

    assert all(
        count == 0
        for count in second["after"].values()
    )


def test_iceberg_failure_is_reported_not_hidden(isolated_lab, monkeypatch):
    """No cross-system transaction: DuckDB deletes, Iceberg fails, result says so."""

    def boom(_user_id):
        raise RuntimeError("simulated iceberg outage")

    monkeypatch.setattr(pipeline, "delete_pipeline_user", boom)

    result = pipeline.delete_user(TARGET)

    assert all(
        count == 0
        for count in result["after"].values()
    )

    assert result["iceberg"] is None
    assert "simulated iceberg outage" in result["iceberg_error"]
    assert result["verified_current_state"] is False


def test_audit_log_records_request(isolated_lab):
    result = pipeline.delete_user(TARGET)

    lines = pipeline.AUDIT_PATH.read_text(
        encoding="utf-8"
    ).strip().splitlines()

    assert len(lines) == 1
    assert result["request_id"] in lines[0]


def test_pipeline_cli_is_independent_of_working_directory(tmp_path):
    """Regression: relative Iceberg paths used to depend on the cwd."""

    env = dict(os.environ)
    root = pipeline.ROOT
    env["PYTHONPATH"] = str(root)

    driver = (
        "from src.erasure import pipeline, iceberg_store as ib;"
        f"from pathlib import Path; d=Path(r'{tmp_path}')/'data';"
        "pipeline.DATA_DIR=d; pipeline.RESULTS_DIR=Path(r'%s')/'r';" % tmp_path
        + "pipeline.DB_PATH=d/'l.duckdb'; pipeline.SNAPSHOT_DIR=d/'s';"
        "pipeline.AUDIT_PATH=pipeline.RESULTS_DIR/'a.jsonl';"
        "ib.ICEBERG_DIR=d/'iceberg';"
        "pipeline.create_demo_data(users=20);"
        "pipeline.delete_user('U000003');"
        "pipeline.expire_snapshots();"
        "import sys; sys.exit(0 if "
        "pipeline.verify_user_absent('U000003')['verified'] else 1)"
    )

    proc = subprocess.run(
        [sys.executable, "-c", driver],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, proc.stderr


def test_cleanup_can_run_repeatedly_in_one_process(isolated_lab):
    """Regression: cleanup can run repeatedly without stale Iceberg metadata."""

    import shutil

    for seed in (1, 2, 3):

        # Remove only DuckDB and Parquet snapshot storage.
        # Iceberg catalog and warehouse remain intact.
        shutil.rmtree(pipeline.DATA_DIR, ignore_errors=True)

        pipeline.ensure_dirs()

        # Recreate data and synchronize with the existing Iceberg table.
        pipeline.create_demo_data(users=20, seed=seed)

        pipeline.delete_user(TARGET)

        result = pipeline.expire_snapshots()

        assert result["iceberg_error"] is None, result["iceberg_error"]

        verification = pipeline.verify_user_absent(TARGET)

        assert verification["verified"] is True


def test_cleanup_lists_every_deleted_file(isolated_lab):
    """The cleanup result names each removed file, and each is really gone."""

    from pathlib import Path

    pipeline.delete_user(TARGET)

    result = pipeline.expire_snapshots()

    stores = sorted(
        item["store"]
        for item in result["deleted_files"]
    )

    assert stores == [
        "iceberg_data_file",
        "parquet_history_copy",
    ]

    for item in result["deleted_files"]:

        path = Path(item["path"])

        if not path.is_absolute():
            path = pipeline.ROOT / path

        assert path.suffix == ".parquet"
        assert not path.exists()
