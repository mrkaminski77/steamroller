# steamroller

A PySpark utility library for flattening and processing nested JSON data.

## Functions

### `drill_and_flatten(df, json_path, delimiter="__", literal_dot_replacement="_")`

Drills into a nested struct/array column by dot-notation path and flattens it in place. Arrays are exploded with `explode_outer`, and structs are expanded into prefixed columns. Child structs under the target path are recursively flattened as well.

**Parameters:**
- `df` — PySpark DataFrame
- `json_path` — Dot-notation path to the target column (e.g. `"customer.address"`)
- `delimiter` — Separator used between levels when naming flattened columns (default: `"__"`)
- `literal_dot_replacement` — Character used to replace literal dots in field names (default: `"_"`)

---

### `prune_to_relationship(df, json_path, delimiter="__", literal_dot_replacement="_")`

Reduces a DataFrame to only the columns relevant to a specific nested relationship: all columns under the target path (excluding nested arrays) plus any top-level primitive columns. Sibling branches are dropped.

**Parameters:**
- `df` — PySpark DataFrame
- `json_path` — Dot-notation path identifying the relationship to keep (e.g. `"order.items"`)
- `delimiter` — Column name delimiter matching what was used in `drill_and_flatten` (default: `"__"`)
- `literal_dot_replacement` — Dot replacement matching what was used in `drill_and_flatten` (default: `"_"`)

---

### `add_metadata(df, json_path, run_id)`

Appends standard pipeline metadata columns to a DataFrame.

| Column | Value |
|---|---|
| `_source_file` | Input file path (`input_file_name()`) |
| `_load_timestamp` | Current timestamp |
| `_pipeline_run_id` | Provided `run_id` literal |
| `_target_grain` | Provided `json_path` literal |
| `_record_hash` | `xxhash64` over all columns |

**Parameters:**
- `df` — PySpark DataFrame
- `json_path` — Path label to store as the target grain identifier
- `run_id` — Pipeline run identifier to tag records with

---

### `add_surrogate_key(df, key_parts, key_name="row_wid", delimiter="__")`

Generates a deterministic surrogate key column using `xxhash64` over a set of specified fields. Nulls are coalesced to `"NA"` to ensure hash stability.

**Parameters:**
- `df` — PySpark DataFrame
- `key_parts` — List of dot-notation paths whose flattened columns form the key (e.g. `["customer.id", "order_id"]`)
- `key_name` — Name of the output key column (default: `"row_wid"`)
- `delimiter` — Column name delimiter matching what was used in `drill_and_flatten` (default: `"__"`)

---

### `ingest_landing_to_bronze(blob_addresses, source_metadata, dq_audit_path, bronze_path, schema=None)`

Reads raw JSON files from blob storage into a Bronze Delta table and writes a structured audit record. Malformed JSON rows are counted but not silently dropped. The audit record is always written — even on failure — via a `finally` block.

> **Note:** Requires an Azure Synapse runtime environment (`mssparkutils` and an active `SparkSession`).

**Parameters:**
- `blob_addresses` — List of blob storage paths to ingest (e.g. `["abfss://container@account.dfs.core.windows.net/raw/file.json"]`)
- `source_metadata` — Dict describing the data being ingested (see below)
- `dq_audit_path` — Delta table path to append the audit record to
- `bronze_path` — Delta table path to append the ingested data to
- `schema` — Optional `StructType`, or an `abfss://` path to a JSON blob containing a Spark schema (as produced by `StructType.json()`). If provided, Spark skips inference (faster reads, stable types) and drift detection is enabled. If omitted, schema is inferred from the data.

**`source_metadata` fields:**

| Field | Required | Description |
|---|---|---|
| `source_system` | Yes | Name of the source system (e.g. `"Salesforce"`, `"AzureSQL"`) |
| `source_entity` | Yes | Entity or table being ingested (e.g. `"Accounts"`, `"Transactions"`) |
| `query_window` | No | Dict with `"start"` and `"end"` keys (Python `datetime` or `None`) representing the time window of the extract |
| `query_params` | No | Dict of any extraction parameters used (e.g. filters, batch IDs) — stored as strings in the audit table |

**Audit table columns written:**

| Column | Description |
|---|---|
| `source_system` / `source_entity` | From `source_metadata` |
| `pipeline_run_id` | Synapse pipeline run ID from job context |
| `query_start_time` / `query_end_time` | From `query_window` if provided |
| `query_params` | From `source_metadata` |
| `spark_ui_url` | Link to the Spark UI for this job |
| `record_count` | Clean records successfully read |
| `file_count` | Number of files submitted |
| `bad_record_count` | Malformed JSON rows detected |
| `column_count` | Number of columns in the raw schema |
| `schema_snapshot` | JSON array of column names actually present in the data |
| `schema_enforced` | `"true"` if a schema was provided, `"false"` if inferred |
| `schema_drift` | Describes missing/unexpected columns vs. the provided schema; `null` if no schema was given or no drift detected |
| `duration_seconds` | Wall-clock time for the full ingestion |
| `status` | `"success"` or `"failed"` |
| `error_message` | Exception message if the pipeline failed |
| `processed_at` | Timestamp when the audit record was written |

**Example:**

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import steamroller

accounts_schema = StructType([
    StructField("Id", StringType()),
    StructField("Name", StringType()),
    StructField("AnnualRevenue", LongType()),
    StructField("CreatedDate", TimestampType()),
])

steamroller.ingest_landing_to_bronze(
    blob_addresses=[
        "abfss://raw@mystorageaccount.dfs.core.windows.net/salesforce/accounts/2026/04/14/batch_001.json"
    ],
    source_metadata={
        "source_system": "Salesforce",
        "source_entity": "Accounts",
        "query_window": {
            "start": datetime(2026, 4, 13),
            "end": datetime(2026, 4, 14),
        },
        "query_params": {"batch_id": "batch_001", "filter": "IsDeleted=false"},
    },
    dq_audit_path="abfss://bronze@mystorageaccount.dfs.core.windows.net/_audit/ingestion",
    bronze_path="abfss://bronze@mystorageaccount.dfs.core.windows.net/salesforce/accounts",
    schema=accounts_schema,  # optional — omit to fall back to inference
)
