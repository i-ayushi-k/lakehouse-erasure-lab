# Architecture and Design

## 1. Purpose

Lakehouse Erasure Lab is a local prototype for exploring user-level deletion across several categories of analytical data.

It models a common challenge in data platforms: deleting a record from the original dataset does not automatically remove copies, historical snapshots, and derived records created by downstream processing.

The prototype combines DuckDB, Apache Iceberg, and Parquet to demonstrate current-state deletion, historical snapshot cleanup, and verification.

## 2. Components

1. **Synthetic source records:** Generated user-level records used as test data.
2. **DuckDB source table:** The primary local analytical representation.
3. **Derived records:** A table representing downstream transformations.
4. **Feature-like table:** A simulation of data that might otherwise be held in a feature store.
5. **Vector-like table:** A simulation of embedding records. Embeddings are represented as JSON strings rather than managed by an external vector database.
6. **Parquet historical copy:** An exported file used to test whether a registered historical copy can be removed.
7. **Apache Iceberg table:** A local table-format integration using a SQLite catalog and Parquet data files.
8. **Deletion and verification logic:** The pipeline removes a target user from current-state tables and checks for remaining records in the tested locations.
9. **Audit log:** A local JSON Lines log used to record deletion activity. Runtime audit output is excluded from version control.

## 3. Erasure workflow

The workflow is divided into current-state deletion and historical cleanup.

1. Receive a user identifier.
2. Validate that the identifier is not empty.
3. Count the target user's existing records.
4. Delete matching records from the four DuckDB current-state tables in a DuckDB transaction.
5. Invoke the Iceberg deletion operation on the dedicated pipeline table.
6. Record the operation and its results in the local audit log.
7. Check whether the target user remains in the current-state tables.
8. Expire registered historical Parquet copies.
9. Expire eligible historical Iceberg snapshots and clean up unreferenced local data files in the dedicated demo table.
10. Verify the checked current-state and historical storage locations.

The final verification reports success only when the implemented DuckDB and Iceberg checks succeed.

**Cross-system transaction limitation:** DuckDB and Iceberg do not share a distributed transaction. The DuckDB deletion can succeed even if the Iceberg operation fails. The implementation reports Iceberg errors separately rather than claiming atomic deletion across both systems.

The exact guarantees are limited to the code paths and storage locations exercised by the implementation and tests.

## 4. Storage and historical data

### 4.1 DuckDB

The project uses four DuckDB tables:

| Table | Purpose |
|---|---|
| `source_records` | Synthetic source user records |
| `derived_records` | Derived user attributes |
| `feature_store` | Simulated feature records |
| `vector_index` | Simulated vector-related records |

The feature and vector tables are local simulations, not external services.

### 4.2 Historical Parquet copies

The pipeline exports source records to Parquet files under:

```text
data/processed/snapshots/
```

The `snapshot_catalog` table tracks the snapshot ID, file path, and expiration status.

The cleanup operation attempts to remove registered historical copies and marks successfully removed files as expired.

This is a simplified file-based historical-copy mechanism, not an Iceberg-managed snapshot.

### 4.3 Apache Iceberg

The project integrates a local Apache Iceberg table through:

- A SQLite catalog.
- A local warehouse directory.
- Iceberg snapshot metadata.
- Parquet data files.

The implementation is located in:

```text
src/erasure/iceberg_store.py
```

The dedicated pipeline table is:

```text
erasure_lab.pipeline_users
```

A separate standalone demonstration table is:

```text
erasure_lab.users
```

The pipeline synchronizes generated user records into the dedicated table and removes the requested user's records from its current state.

In Apache Iceberg, snapshot expiration is a metadata-aware table maintenance operation. It expires eligible snapshots and makes files no longer required by retained snapshots eligible for removal.

The prototype performs this operation on its dedicated local demo table and then identifies and removes unreferenced local Parquet data files.

This local cleanup approach must not be treated as a general-purpose production garbage collector. Production systems need safeguards for concurrent readers and writers, retention requirements, and file references.

Reference: [Apache Iceberg maintenance documentation](https://iceberg.apache.org/docs/latest/maintenance/)

## 5. Verification strategy

The verification logic checks:

- The target user's row counts in all four DuckDB tables.
- Registered historical Parquet files for matching user IDs.
- The current Iceberg table state.
- Remaining physical Iceberg Parquet data files for matching user IDs.

The verification result is limited to these implemented checks. It does not establish that every possible copy of a user's data has been erased.

## 6. Audit logging

Deletion activity is appended to:

```text
results/deletion_audit.jsonl
```

The audit record includes:

- Request ID.
- Target user ID.
- Timestamp.
- Row counts before and after deletion.
- Iceberg deletion result or error.
- Current-state verification result.
- Measured deletion latency.

Production audit systems should also consider access controls, retention policies, and minimizing personal information in logs.

## 7. Failure modes and production requirements

A production-grade system would need to address at least the following:

- Partial failure between stores and safe retries.
- Stable request IDs and idempotent processing.
- Authorization and identity verification before deletion.
- Lineage tracking for derived records and model features.
- Historical snapshots, replicas, backups, caches, and downstream exports.
- Retention requirements that may constrain immediate physical deletion.
- Independent verification and durable, privacy-conscious audit records.
- Concurrency with active writers and readers.
- Storage lifecycle and orphan-file cleanup.
- Monitoring, alerting, recovery, and operational ownership.
- Coordination across storage systems that do not share a transaction.

The current prototype is intended to make these concerns concrete, not to claim that all of them have been solved.

## 8. Running the implementation

From the repository root:

Run the pipeline:

```bash
python -m src.erasure.pipeline
```

Run the automated tests:

```bash
python -m pytest -v
```

The demonstration generates synthetic records, deletes a selected user, performs historical cleanup, and reports the final verification result.

## 9. Scope and limitations

This project is an educational local prototype.

It does not implement external feature-store or vector-database deletion, cloud-storage version deletion, backup and replica cleanup, downstream deletion propagation, production authorization, or regulatory compliance certification.

Successful verification demonstrates only that the target user's records were not found in the storage locations checked by this implementation.