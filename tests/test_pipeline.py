
import shutil

import pytest

from src.erasure import pipeline


@pytest.fixture
def isolated_lab(tmp_path, monkeypatch):
    """Give each test its own database, snapshots, and audit log."""
    data_dir = tmp_path / "data"
    results_dir = tmp_path / "results"

    monkeypatch.setattr(pipeline, "DATA_DIR", data_dir)
    monkeypatch.setattr(pipeline, "RESULTS_DIR", results_dir)
    monkeypatch.setattr(pipeline, "DB_PATH", data_dir / "lakehouse.duckdb")
    monkeypatch.setattr(pipeline, "SNAPSHOT_DIR", data_dir / "snapshots")
    monkeypatch.setattr(pipeline, "AUDIT_PATH", results_dir / "deletion_audit.jsonl")

    pipeline.create_demo_data(users=20, seed=42)
    yield
    shutil.rmtree(tmp_path, ignore_errors=True)


def test_delete_user_from_all_stores(isolated_lab):
    result = pipeline.delete_user("U000007")

    assert result["verified_current_state"] is True
    assert all(count == 0 for count in result["after"].values())

    verification = pipeline.verify_user_absent("U000007")
    assert all(count == 0 for count in verification["current_store_rows"].values())



def test_snapshot_expiry_removes_historical_copy(isolated_lab):
    # First remove the user from all current-state stores.
    deletion = pipeline.delete_user("U000007")
    assert deletion["verified_current_state"] is True

    # The historical snapshot should still contain the user's record.
    before = pipeline.verify_user_absent("U000007")
    assert before["remaining_snapshot_files"]

    # Expire the historical snapshot.
    result = pipeline.expire_snapshots()

    assert result["verified"] is True
    assert result["snapshots_removed"] == 1

    # Verify that neither current stores nor retained snapshots contain the user.
    after = pipeline.verify_user_absent("U000007")
    assert after["verified"] is True
    assert after["remaining_snapshot_files"] == []



def test_empty_user_id_is_rejected(isolated_lab):
    with pytest.raises(ValueError):
        pipeline.delete_user("")


def test_repeated_deletion_is_idempotent(isolated_lab):
    first = pipeline.delete_user("U000007")
    second = pipeline.delete_user("U000007")

    assert first["verified_current_state"] is True
    assert second["verified_current_state"] is True
    assert all(count == 0 for count in second["after"].values())
