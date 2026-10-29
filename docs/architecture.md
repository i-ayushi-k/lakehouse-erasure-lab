# Architecture and Design

## 1. Purpose

Lakehouse Erasure Lab is a local prototype for exploring user-level deletion across several categories of analytical data. It models a common difficulty in data platforms: deleting a record from the original dataset does not automatically remove copies and derived records created by downstream processing.

## 2. Components

1. **Synthetic source records:** generated user-level records used as test data.
2. **DuckDB source table:** the primary local analytical representation.
3. **Derived records:** a table representing downstream transformations.
4. **Feature-like table:** a simulation of data that might otherwise be held in a feature store.
5. **Vector-like table:** a simulation of embedding records. Embeddings are represented as JSON strings rather than managed by an external vector database.
6. **Parquet historical copy:** an exported file used to test whether a registered historical copy can be removed.
7. **Deletion and verification logic:** the pipeline removes a target user from current-state tables and checks for remaining records in the tested locations.
8. **Audit log:** a local JSON Lines log used to record deletion activity. Runtime audit output is excluded from version control.

## 3. Erasure workflow

The conceptual workflow is:

1. Receive a user identifier.
2. Validate that the identifier is not empty.
3. Delete matching records from the simulated current-state tables.
4. Record the operation in the local audit log.
5. Check whether the target user remains in the current-state tables.
6. Expire the registered historical Parquet copy and verify its removal.
7. Report whether the checks succeeded.

The exact guarantees are limited to the code paths and storage locations exercised by the implementation and tests.

## 4. Storage and historical data

The project uses DuckDB for local tables and Parquet for a historical export. The cleanup function removes the registered file; it does not implement a transactional lakehouse table format.

In Apache Iceberg, snapshot expiration is a metadata-aware table maintenance operation. It expires eligible snapshots and removes data files that are no longer referenced by retained snapshots. Simply deleting a Parquet file is not equivalent to this operation.

Reference: https://iceberg.apache.org/docs/latest/maintenance/

## 5. Failure modes and production requirements

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

The current prototype is intended to make these concerns concrete, not to claim that all of them have been solved.
