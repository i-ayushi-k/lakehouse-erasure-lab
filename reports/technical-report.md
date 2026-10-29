# Technical Report: User Erasure in a Local Lakehouse Prototype

## Abstract

This project investigates a practical data-engineering problem: a user-level deletion request may need to propagate beyond a primary dataset into derived records and historical copies. The implementation is a local prototype using DuckDB tables and Parquet exports. It includes a deletion pipeline, verification checks, automated tests, and a synthetic-data benchmark. The recorded benchmark shows mean deletion latency between approximately 46.8 ms and 59.5 ms for dataset sizes from 100 to 10,000 records, with all 12 recorded runs passing the implemented verification check. These results demonstrate the behavior of this prototype under its local test conditions; they do not establish production performance or complete regulatory compliance.

## 1. Problem statement

Analytical platforms commonly create multiple representations of the same underlying information. A source table may feed derived tables, features, embeddings, exports, and historical snapshots. Removing a user from only the source does not guarantee that their data has disappeared from those other locations.

The goal of this lab is to implement and test a small, repeatable workflow that deletes a selected synthetic user from several simulated data stores, checks the current state, and removes a registered historical Parquet copy. The work also measures latency at several dataset sizes.

## 2. System design and implementation

The project uses Python, DuckDB, and Parquet. The pipeline creates synthetic user records and represents four logical storage locations with DuckDB tables: source records, derived records, a feature-like store, and a vector-like store. Vector-like values are represented as JSON strings. This arrangement keeps the experiment easy to run locally without external services.

The deletion pipeline accepts a user identifier, rejects an empty identifier, removes matching records from the simulated current-state tables, and provides verification logic to check whether records for the selected user remain. A local JSON Lines audit log records operation activity. The project also exports a Parquet historical copy and maintains a registry of those copies for cleanup testing.

The historical-copy test verifies that a registered file is removed during cleanup. This is deliberately a simplified model: it does not manage a real table-format metadata graph.

### 2.1 Test coverage

The test suite includes four cases:

- Deletion from all simulated current-state stores.
- Removal of the historical Parquet copy during snapshot-copy cleanup.
- Rejection of an empty user identifier.
- Idempotent behavior for repeated deletion.

The test command is `python -m pytest -v`. On the current Windows environment, all four tests passed.

## 3. Experimental method

The benchmark driver runs at four synthetic dataset sizes: 100, 1,000, 5,000, and 10,000 records. Each size is repeated three times. The recorded output includes mean, median, minimum, and maximum latency, along with the number of runs whose verification check succeeded.

The benchmark is a local experiment rather than a controlled distributed performance study. Timing may vary with the host machine, software environment, background processes, caching, and other factors. Only the recorded measurements are reported here; no unmeasured production behavior is inferred.

## 4. Results

| Records | Repeats | Mean latency (ms) | Median latency (ms) | Min (ms) | Max (ms) | Verified |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 3 | 46.793 | 46.398 | 45.988 | 47.994 | 3/3 |
| 1,000 | 3 | 48.284 | 47.376 | 46.767 | 50.708 | 3/3 |
| 5,000 | 3 | 58.759 | 53.760 | 49.375 | 73.141 | 3/3 |
| 10,000 | 3 | 59.477 | 60.276 | 57.831 | 60.323 | 3/3 |

All 12 recorded runs passed the implemented verification check. Mean latency rose from 46.793 ms at 100 records to 59.477 ms at 10,000 records. Within this limited experiment, latency increased modestly as the synthetic dataset grew. The measurements are not sufficient to infer asymptotic behavior or predict performance at larger scales. Three repetitions per size are also too few for strong statistical conclusions.

The repository includes `results/scale_benchmark.csv`, `results/benchmark_summary.csv`, and `results/deletion_latency.png`. The results notebook is available as `notebooks/results.ipynb`, with a saved execution in `notebooks/results_executed.ipynb`.

## 5. Discussion: prototype versus production lakehouse

The main result is that the project can exercise a multi-table deletion workflow and verify the tested local state. The experiment also makes the historical-data problem visible: deleting current rows and removing an exported file are different operations.

The implementation does **not** currently use Apache Iceberg. In a real Iceberg table, each write creates a snapshot, and snapshot expiration updates table metadata while deleting files that are no longer required by retained snapshots. This is not equivalent to deleting a Parquet file directly. A native Iceberg extension would need to create and mutate Iceberg tables, apply a deliberate retention policy, execute the table's snapshot-expiration procedure, and verify results using Iceberg metadata and storage state.

Likewise, the feature and vector components are simulated tables, not integrations with production feature-store or vector-database products. The benchmark does not include distributed storage, concurrent requests, backups, replicas, or downstream consumer systems.

A production implementation would require identity verification, authorization, durable request tracking, retries, failure recovery, lineage-based propagation, retention and backup policies, monitoring, and independently verifiable deletion evidence. Audit records must be designed carefully so they do not unnecessarily preserve the personal data the request seeks to remove.

## 6. Conclusion and future work

This lab delivers a runnable local prototype, four passing tests, benchmark measurements across four dataset sizes, and an executable results notebook. The measured runs all passed the prototype's verification checks, and mean latency remained below 60 ms in the recorded experiment.

The next technical milestone is a native Apache Iceberg implementation with metadata-aware snapshot expiration. Further work should add failure-injection tests, persistent request IDs, retries, explicit data lineage, backup and replica coverage, and broader verification. Larger benchmarks and repeated controlled trials would be needed before making meaningful scalability claims.

The project should be presented as an educational proof of concept for erasure propagation, not as a production deletion service or evidence of legal compliance.

## References

1. Apache Iceberg, “Maintenance — Expire Snapshots.” https://iceberg.apache.org/docs/latest/maintenance/
2. Project source code: `src/erasure/pipeline.py`.
3. Benchmark driver: `scripts/benchmark.py`.
4. Test suite: `tests/test_pipeline.py`.
5. Recorded measurements: `results/scale_benchmark.csv`.
