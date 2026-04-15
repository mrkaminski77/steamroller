# Ingestion Framework

## Overview

The ingestion framework moves data from a landing zone through a medallion pipeline (Landing → Bronze → Silver). The orchestrator drives each stage by passing a batch of file paths and a `batch_id` that flows through every layer, making it straightforward to trace, reprocess, or audit any set of records end-to-end.

---

## Batch Model

A **batch** is a set of files deposited in the landing zone that belong to the same logical extract. The orchestrator assigns a `batch_id` — typically a pipeline run ID or a deterministic key derived from the source and time window — before calling any ingestion function.

The same `batch_id` is:

- Written to every Bronze row as `_batch_id`
- Written to the audit table as `batch_id`
- Passed downstream to Silver so it can identify exactly which Bronze rows to process

This means reprocessing a batch is always possible: select `WHERE _batch_id = '<id>'` at any layer.

---

## Stages

### Landing → Bronze: `ingest_landing_to_bronze`

The orchestrator calls `ingest_landing_to_bronze` with:

| Argument | Description |
|---|---|
| `blob_addresses` | List of `abfss://` paths for this batch |
| `source_metadata` | Source system, entity, query window, and extraction parameters |
| `dq_audit_path` | Delta path for the audit table |
| `bronze_path` | Delta path for the Bronze table |
| `batch_id` | Identifier assigned by the orchestrator |
| `schema` | Optional `StructType` or `abfss://` path to a schema JSON blob |

The function:

1. Reads all files in `blob_addresses` into a single DataFrame, capturing malformed rows via Spark's `PERMISSIVE` mode rather than failing the job.
2. Optionally enforces a schema and detects drift (missing or unexpected columns vs. the declared schema).
3. Appends `_ingested_at`, `_source_system`, `_source_entity`, and `_batch_id` metadata columns.
4. Writes the data to the Bronze Delta table with `mode=append`.
5. Always writes one audit record to the audit Delta table — even if the ingest fails — recording counts, schema state, duration, and status.

> **Performance note:** Because each `ingest_landing_to_bronze` call appends a discrete set of files containing only that batch's rows, Delta Lake's per-file min/max statistics are sufficient for data skipping — a `WHERE _batch_id = '<id>'` filter will skip every unrelated file without a full table scan.

### Bronze → Silver: `ingest_bronze_to_silver` _(planned)_

The Silver function receives the same `batch_id` from the orchestrator. It uses it to scope the Bronze read:

```python
df = spark.read.format("delta").load(bronze_path) \
         .filter(F.col("_batch_id") == batch_id)
```

This guarantees Silver processes exactly the records that were landed in this batch, without re-scanning the full Bronze table or relying on timestamps.

A `mode` argument controls how records are written to Silver:

| Mode | Description |
|---|---|
| `merge` | Upsert using business keys — insert new records, update changed records. The standard choice when stable keys are available. |
| `append` | Insert all rows from the batch without checking for duplicates. Suitable for immutable event streams. |
| `full_overwrite` | Replace the entire Silver table with the current batch. Suitable for small reference datasets extracted in full each time. |
| `slice_overwrite` | Replace only the slice of Silver that falls within the batch's `query_window`. Suitable for time-partitioned data where a full overwrite is too broad. |
| `scd_type2` | Slowly Changing Dimension Type 2 — expire changed rows by closing their effective date and insert new versions. Preserves full history. |

Silver responsibilities (planned):

- Apply type casting and business-level transformations
- Write to Silver Delta table using the specified `mode`
- Append a corresponding audit record

---

## Audit Table

Every invocation of an ingestion function appends one row to the shared audit Delta table regardless of success or failure. This provides a complete history of every batch at every layer.

Key audit fields:

| Field | Description |
|---|---|
| `batch_id` | Orchestrator-assigned identifier |
| `source_system` / `source_entity` | Origin of the data |
| `query_start_time` / `query_end_time` | Time window of the extract |
| `record_count` | Clean records written |
| `bad_record_count` | Malformed rows captured but not written |
| `schema_enforced` | Whether a schema was provided |
| `schema_drift` | Missing or unexpected columns relative to the declared schema |
| `status` | `success` or `failed` |
| `error_message` | Exception message if status is `failed` |
| `duration_seconds` | Wall-clock time for the operation |

---

## Orchestrator Contract

The orchestrator is responsible for:

1. Enumerating the files to ingest for a given extract.
2. Generating or receiving the `batch_id` (e.g., from the pipeline run ID).
3. Populating `source_metadata`, including `query_window` (start/end timestamps) and any `query_params` used during extraction. When stable business keys are not available, `query_window` provides the basis for watermark-based deduplication and incremental processing in downstream layers.
4. Calling `ingest_landing_to_bronze` with the file list and `batch_id`.
5. Passing the same `batch_id` to subsequent Silver processing.

The ingestion functions themselves are stateless with respect to batch tracking — all state flows through `batch_id`.
