
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType
from pyiceberg.partitioning import PartitionSpec


# ==================================================
# CONFIGURATION
# ==================================================

ROOT = Path(__file__).resolve().parents[2]

WAREHOUSE = "./data/iceberg_warehouse"

CATALOG = load_catalog(
    "lakehouse",
    **{
        "type": "sql",
        "uri": "sqlite:///data/iceberg_catalog.db",
        "warehouse": WAREHOUSE,
    },
)

TABLE_NAME = "erasure_lab.users"
PIPELINE_TABLE_NAME = "erasure_lab.pipeline_users"


# ==================================================
# ICEBERG SCHEMA
# ==================================================

SCHEMA = Schema(
    NestedField(1, "user_id", StringType(), required=True),
    NestedField(2, "event_id", LongType(), required=True),
    NestedField(3, "event_type", StringType(), required=True),
    NestedField(4, "payload", StringType(), required=False),
)


def arrow_schema():
    """Return an Arrow schema compatible with Iceberg."""

    return pa.schema([
        pa.field("user_id", pa.string(), nullable=False),
        pa.field("event_id", pa.int64(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("payload", pa.string(), nullable=True),
    ])


# ==================================================
# GENERIC TABLE CREATION
# ==================================================

def get_or_create_table(table_name):
    """Load an existing Iceberg table or create it."""

    if CATALOG.table_exists(table_name):
        return CATALOG.load_table(table_name)

    namespace = table_name.split(".")[0]

    if not CATALOG.namespace_exists(namespace):
        CATALOG.create_namespace(namespace)

    return CATALOG.create_table(
        identifier=table_name,
        schema=SCHEMA,
        partition_spec=PartitionSpec(),
    )


# ==================================================
# STANDALONE ICEBERG DEMONSTRATION
# ==================================================

def create_table():
    return get_or_create_table(TABLE_NAME)


def insert_demo_data():
    """Insert standalone demonstration records."""

    table = create_table()

    existing = table.scan().to_arrow()

    if existing.num_rows == 0:

        data = pa.table(
            {
                "user_id": [
                    "user_001",
                    "user_002",
                    "user_001",
                    "user_003",
                ],
                "event_id": [1, 2, 3, 4],
                "event_type": [
                    "purchase",
                    "login",
                    "click",
                    "login",
                ],
                "payload": [
                    "order-A",
                    "session-B",
                    "page-C",
                    "session-D",
                ],
            },
            schema=arrow_schema(),
        )

        table.append(data)

    return table


def read_users():
    table = create_table()
    return table.scan().to_arrow()


def delete_user(user_id: str):
    """Delete a user from the standalone Iceberg table."""

    if not user_id or not user_id.strip():
        raise ValueError("user_id cannot be empty")

    safe_id = user_id.replace("'", "''")

    table = create_table()
    table.delete(f"user_id == '{safe_id}'")

    return table


def verify_user_absent(user_id: str):
    """Verify absence in the standalone Iceberg table."""

    if not user_id or not user_id.strip():
        raise ValueError("user_id cannot be empty")

    safe_id = user_id.replace("'", "''")

    table = create_table()

    result = table.scan(
        row_filter=f"user_id == '{safe_id}'"
    ).to_arrow()

    return result.num_rows == 0


# ==================================================
# MAIN PIPELINE ICEBERG INTEGRATION
# ==================================================

def create_pipeline_table():
    """Create or load the Iceberg table for the main pipeline."""

    return get_or_create_table(PIPELINE_TABLE_NAME)


def replace_pipeline_users(user_ids):
    """
    Synchronize synthetic pipeline users into Iceberg.

    Called after the DuckDB demo data is generated.
    """

    user_ids = list(user_ids)

    table = create_pipeline_table()

    data = pa.table(
        {
            "user_id": user_ids,
            "event_id": list(range(len(user_ids))),
            "event_type": ["profile"] * len(user_ids),
            "payload": ["synthetic-demo"] * len(user_ids),
        },
        schema=arrow_schema(),
    )

    table.overwrite(data)

    return {
        "table": PIPELINE_TABLE_NAME,
        "users_loaded": len(user_ids),
    }


def delete_pipeline_user(user_id: str):
    """Delete a user from the main pipeline Iceberg table."""

    if not user_id or not user_id.strip():
        raise ValueError("user_id cannot be empty")

    safe_id = user_id.replace("'", "''")

    table = create_pipeline_table()

    before = table.scan(
        row_filter=f"user_id == '{safe_id}'"
    ).to_arrow().num_rows

    if before > 0:
        table.delete(f"user_id == '{safe_id}'")

    after = table.scan(
        row_filter=f"user_id == '{safe_id}'"
    ).to_arrow().num_rows

    return {
        "before": before,
        "after": after,
        "verified": after == 0,
    }


def verify_pipeline_user_absent(user_id: str):
    """Verify that a user is absent from the pipeline Iceberg table."""

    if not user_id or not user_id.strip():
        raise ValueError("user_id cannot be empty")

    safe_id = user_id.replace("'", "''")

    table = create_pipeline_table()

    result = table.scan(
        row_filter=f"user_id == '{safe_id}'"
    ).to_arrow()

    return result.num_rows == 0


# ==================================================
# PIPELINE SNAPSHOT EXPIRATION AND FILE CLEANUP
# ==================================================

def expire_pipeline_history():
    """
    Expire older snapshots while preserving the latest snapshot.

    Then remove obsolete Parquet data files from this dedicated
    local demonstration table's data directory.
    """

    table = create_pipeline_table()

    snapshots = table.inspect.snapshots().to_pandas()

    expired_ids = []

    if not snapshots.empty:

        snapshots = snapshots.sort_values("committed_at")

        latest_id = int(
            snapshots.iloc[-1]["snapshot_id"]
        )

        old_ids = [
            int(snapshot_id)
            for snapshot_id in snapshots["snapshot_id"].tolist()
            if int(snapshot_id) != latest_id
        ]

        for snapshot_id in old_ids:

            table.maintenance.expire_snapshots().by_id(
                snapshot_id
            ).commit()

            expired_ids.append(snapshot_id)

            table = CATALOG.load_table(
                PIPELINE_TABLE_NAME
            )

    # Determine files referenced by the current Iceberg table.
    active_files = set()

    file_records = table.inspect.files().to_pandas()

    for raw_path in file_records["file_path"].tolist():

        path = Path(str(raw_path))

        if not path.is_absolute():
            path = ROOT / path

        active_files.add(path.resolve())

    # Only inspect this dedicated pipeline table directory.
    data_dir = (
        ROOT
        / "data"
        / "iceberg_warehouse"
        / "erasure_lab"
        / "pipeline_users"
        / "data"
    )

    removed_files = []

    if data_dir.exists():

        for path in data_dir.glob("*.parquet"):

            if path.resolve() not in active_files:

                path.unlink()
                removed_files.append(path.name)

    return {
        "expired_snapshot_ids": expired_ids,
        "orphan_files_removed": removed_files,
        "active_data_files": len(active_files),
    }


# ==================================================
# MAIN PIPELINE VERIFICATION
# ==================================================

def verify_pipeline_storage(user_id: str):
    """Check current Iceberg state and remaining physical data files."""

    if not user_id or not user_id.strip():
        raise ValueError("user_id cannot be empty")

    table = create_pipeline_table()

    safe_id = user_id.replace("'", "''")

    result = table.scan(
        row_filter=f"user_id == '{safe_id}'"
    ).to_arrow()

    remaining_files = []

    data_dir = (
        ROOT
        / "data"
        / "iceberg_warehouse"
        / "erasure_lab"
        / "pipeline_users"
        / "data"
    )

    if data_dir.exists():

        for path in data_dir.glob("*.parquet"):

            try:
                parquet = pq.read_table(path)

                if user_id in parquet.column(
                    "user_id"
                ).to_pylist():

                    remaining_files.append(path.name)

            except Exception as exc:

                remaining_files.append({
                    "file": path.name,
                    "error": str(exc),
                })

    return {
        "user_id": user_id,
        "current_table_rows": result.num_rows,
        "physical_files_containing_user": remaining_files,
        "verified": (
            result.num_rows == 0
            and not remaining_files
        ),
    }


# ==================================================
# STANDALONE EXECUTION
# ==================================================

if __name__ == "__main__":

    table = insert_demo_data()

    print("\nAPACHE ICEBERG DEMONSTRATION")
    print("----------------------------")

    print("Table:", table.name())

    before = read_users()

    print("Rows before deletion:", before.num_rows)

    delete_user("user_001")

    after = read_users()

    print("Rows after deletion:", after.num_rows)

    print(
        "User user_001 absent:",
        verify_user_absent("user_001"),
    )

    print("\nIceberg integration completed.")
