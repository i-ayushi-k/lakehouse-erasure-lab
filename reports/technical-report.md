# Technical Report: User Erasure in a Local Lakehouse Prototype

## Abstract

This project investigates a practical data-engineering problem: a user-level deletion request may need to propagate beyond a primary dataset into derived records, historical copies, and table-format storage.

The implementation is a local prototype using DuckDB, Apache Iceberg, and Parquet. It includes a deletion pipeline, current-state verification, historical snapshot cleanup, automated tests, and a synthetic-data benchmark.

The original recorded DuckDB benchmark shows mean deletion latency between approximately 46.8 ms and 59.5 ms for dataset sizes from 100 to 10,000 records, with all 12 recorded runs passing the implemented verification check. These are baseline measurements from the original local prototype, not performance measurements of the integrated Iceberg implementation.

The project demonstrates selected erasure operations under local test conditions. It does not establish production performance, complete physical erasure across distributed systems, or regulatory compliance.

## 1. Problem Statement

Analytical platforms commonly create multiple representations of the same underlying information. A source table may feed derived tables, features, embeddings, exports, and historical snapshots. Removing a user from only the source does not guarantee that their data has disappeared from those other locations.

The goal of this lab is to implement and test a repeatable workflow that deletes a selected synthetic user from several local storage representations, checks the current state, and cleans up historical copies.

The project also explores Apache Iceberg snapshot management and measures local deletion latency at several dataset sizes.

## 2. System Design and Implementation

The project uses Python, DuckDB, Apache Iceberg, and Parquet.

### 2.1 DuckDB storage

The pipeline creates synthetic user records and represents four logical storage locations with DuckDB tables:

| Table | Purpose |
|---|---|
| `source_records` | Source user records |
| `derived_records` | Downstream derived records |
| `feature_store` | Simulated feature records |
| `vector_index` | Simulated vector-related records |

Vector-like values are represented as JSON strings. The feature and vector components are local simulations, not integrations with external production services.

### 2.2 Historical Parquet copies

The project exports a historical Parquet copy and maintains a registry of these copies in `snapshot_catalog`.

The cleanup function attempts to remove registered files and marks successfully removed copies as expired.

This is a simplified historical-copy mechanism. It is separate from Iceberg-managed snapshot metadata.

### 2.3 Apache Iceberg integration

The project includes a local Apache Iceberg implementation in:

`src/erasure/iceberg_store.py`

It uses a local SQLite catalog, a local warehouse, Iceberg table metadata, and Parquet data files.

The dedicated pipeline table is:

`erasure_lab.pipeline_users`

A separate table, `erasure_lab.users`, supports the standalone Iceberg demonstration.

The pipeline synchronizes generated user IDs into the dedicated Iceberg table. During deletion, it removes the target user's records from the current Iceberg table state and verifies the result.

The historical cleanup operation:

1. Expires eligible older Iceberg snapshots while retaining the latest snapshot.
2. Identifies data files referenced by the active table state.
3. Identifies unreferenced Parquet data files.
4. Removes eligible orphan files from the dedicated local demonstration table directory.

This demonstrates the distinction between current-state deletion and historical snapshot maintenance.

**Cross-system limitation:** DuckDB and Iceberg do not share a distributed transaction. The DuckDB transaction can succeed even if the Iceberg operation fails. The implementation records Iceberg errors separately and does not claim atomic deletion across both systems.

The local orphan-file cleanup is designed for the dedicated demonstration table. It is not a general-purpose production garbage collector.

### 2.4 Deletion and verification

The deletion pipeline accepts a user identifier and rejects an empty identifier.

It removes matching records from the four DuckDB current-state tables within a DuckDB transaction, then invokes the Iceberg deletion operation.

Verification checks:

- The target user's row counts in the four DuckDB tables.
- Remaining registered historical Parquet files.
- The current Iceberg table state.
- Remaining physical Iceberg Parquet files containing the target user.

The verification result is limited to the implemented storage locations.

A local JSON Lines audit log records deletion activity, including the request ID, user identifier, timestamp, before-and-after counts, Iceberg result, and verification status.

### 2.5 Test coverage

The test suite includes four cases:

- Deletion from all simulated current-state stores.
- Removal of the historical Parquet copy during snapshot cleanup.
- Rejection of an empty user identifier.
- Idempotent behavior for repeated deletion.

Run the tests using:

```bash
python -m pytest -v
```

On the current Windows development environment, all four tests passed after the Iceberg integration and backward-compatible cleanup result fields were added.

These tests validate the behaviors covered by the existing test suite; they do not prove complete erasure from every storage location.

## 3. Experimental Method

The original benchmark driver runs at four synthetic dataset sizes: 100, 1,000, 5,000, and 10,000 records. Each size is repeated three times.

The recorded output includes mean, median, minimum, and maximum latency, along with the number of runs whose verification check succeeded.

The original measurements were collected from the local DuckDB prototype. They should not be interpreted as benchmark results for the integrated Iceberg implementation.

The benchmark is a local experiment rather than a controlled distributed performance study. Timing may vary with the host machine, software environment, background processes, caching, and other factors.

## 4. Results

### 4.1 Original DuckDB baseline

| Records | Repeats | Mean latency (ms) | Median latency (ms) | Min (ms) | Max (ms) | Verified |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 3 | 46.793 | 46.398 | 45.988 | 47.994 | 3/3 |
| 1,000 | 3 | 48.284 | 47.376 | 46.767 | 50.708 | 3/3 |
| 5,000 | 3 | 58.759 | 53.760 | 49.375 | 73.141 | 3/3 |
| 10,000 | 3 | 59.477 | 60.276 | 57.831 | 60.323 | 3/3 |

All 12 recorded baseline runs passed the implemented verification check.

Mean latency rose from 46.793 ms at 100 records to 59.477 ms at 10,000 records. Within this limited experiment, latency increased modestly as the synthetic dataset grew.

The measurements are not sufficient to infer asymptotic behavior or predict performance at larger scales. Three repetitions per size are also too few for strong statistical conclusions.

### 4.2 Integrated implementation demonstration

The integrated pipeline was also executed locally with 1,000 synthetic users.

The demonstration successfully:

- Created the DuckDB tables and historical Parquet copy.
- Synchronized 1,000 user records into the Iceberg pipeline table.
- Deleted target user `U000007` from the current DuckDB and Iceberg states.
- Expired the registered DuckDB historical copy.
- Expired an older Iceberg snapshot.
- Removed an unreferenced Iceberg Parquet data file.
- Verified that the target user was absent from the checked current and historical storage locations.

The pipeline reported successful final verification.

This is a functional demonstration, not a repeated performance benchmark of the integrated implementation.

### 4.3 Result artifacts

The repository includes:

- `results/scale_benchmark.csv`
- `results/benchmark_summary.csv`
- `results/deletion_latency.png`
- `notebooks/results.ipynb`
- `notebooks/results_executed.ipynb`

The original benchmark artifacts should remain labelled as DuckDB baseline results unless they are regenerated using a documented integrated benchmark methodology.

## 5. Discussion: Prototype Versus Production Lakehouse

The main result is that the project can exercise a multi-store deletion workflow and verify the tested local state.

The experiment makes an important distinction visible: deleting current rows and removing historical representations are different operations.

The DuckDB implementation demonstrates deletion from source, derived, feature-like, and vector-like tables. The Iceberg integration adds table-format snapshot metadata and a separate historical cleanup process.

However, several production challenges remain.

**Partial failure:** DuckDB and Iceberg do not share a distributed transaction. A successful operation in one system does not guarantee success in the other. Production systems would need durable request tracking, retries, and recovery mechanisms.

**External systems:** The feature and vector components are simulated tables. The project does not delete records from real external feature stores or vector databases.

**Historical retention:** The local cleanup demonstration does not cover backups, replicas, cloud object versions, or independently retained downstream exports.

**Concurrency:** The prototype does not implement production-grade coordination with active writers and readers during physical-file cleanup.

**Audit and privacy:** Audit records must be designed carefully so they do not unnecessarily preserve personal information that the erasure request seeks to remove.

A production implementation would require identity verification, authorization, durable request tracking, failure recovery, lineage-based propagation, retention policies, monitoring, and independently verifiable deletion evidence.

## 6. Conclusion and Future Work

This lab delivers a runnable local prototype integrating DuckDB and Apache Iceberg, four passing automated tests, a synthetic-data benchmark baseline, and an executable results notebook.

The integrated demonstration successfully deleted a selected user from the current DuckDB and Iceberg states, expired historical snapshots, cleaned up an eligible local orphan data file, and verified the checked storage locations.

The original benchmark measurements show mean latency below 60 ms for the tested DuckDB baseline. These measurements should not be presented as integrated Iceberg performance results.

Future work should include:

- Failure-injection tests for partial deletion.
- Durable request tracking and retry handling.
- Explicit data-lineage tracking.
- External feature-store and vector-database integrations.
- Backup and replica coverage.
- Concurrency-safe historical file cleanup.
- Repeated benchmarks of the integrated implementation.
- Independent verification across all relevant storage systems.

The project should be presented as an educational proof of concept for erasure propagation, not as a production deletion service or evidence of legal compliance.

## References

1. Apache Iceberg, "Maintenance — Expire Snapshots." https://iceberg.apache.org/docs/latest/maintenance/
2. Project source code: `src/erasure/pipeline.py`.
3. Iceberg integration: `src/erasure/iceberg_store.py`.
4. Benchmark driver: `scripts/benchmark.py`.
5. Test suite: `tests/test_pipeline.py`.
6. Recorded baseline measurements: `results/scale_benchmark.csv`.