# steamroller

A PySpark utility library for building medallion data pipelines on Azure Synapse. It handles the mechanical work of ingesting raw JSON from blob storage into Delta Lake and processing nested JSON structures, so pipelines stay focused on business logic.

## Features

- **Bronze ingestion** — Reads raw JSON files from `abfss://` blob paths into a Bronze Delta table with schema enforcement, drift detection, and malformed-row counting.
- **Audit trail** — Every ingestion call writes a structured audit record to a Delta table regardless of success or failure, capturing record counts, schema state, duration, and error messages.
- **Batch model** — A `batch_id` flows through every layer, making it straightforward to trace, reprocess, or audit any set of records end-to-end.
- **JSON flattening** — Utilities for drilling into nested struct/array columns, pruning to a specific relationship, adding pipeline metadata, and generating deterministic surrogate keys.

## Installation

```bash
pip install steamroller
```

> Requires Python 3.8+ and an Azure Synapse runtime (`mssparkutils` and an active `SparkSession`).

## Modules

### `ingestion` — Landing → Bronze

```python
import steamroller

steamroller.ingest_landing_to_bronze(
    blob_addresses=["abfss://raw@account.dfs.core.windows.net/salesforce/accounts/batch_001.json"],
    source_metadata={
        "source_system": "Salesforce",
        "source_entity": "Accounts",
        "query_window": {"start": datetime(2026, 4, 13), "end": datetime(2026, 4, 14)},
        "query_params": {"batch_id": "batch_001"},
    },
    dq_audit_path="abfss://bronze@account.dfs.core.windows.net/_audit/ingestion",
    bronze_path="abfss://bronze@account.dfs.core.windows.net/salesforce/accounts",
    pipeline_run_id="<pipeline-run-id>",
    schema=accounts_schema,  # optional StructType or abfss:// path to schema JSON
)
```

See [docs/ingestion.md](docs/ingestion.md) for full parameter reference and audit table schema.

### `json_processor` — Nested JSON utilities

```python
from steamroller.json_processor import drill_and_flatten, prune_to_relationship, add_metadata, add_surrogate_key
```

| Function | Description |
|---|---|
| `drill_and_flatten(df, json_path)` | Drills into a nested struct/array column by dot-notation path and flattens it in place |
| `prune_to_relationship(df, json_path)` | Reduces a DataFrame to only the columns relevant to a specific nested relationship |
| `add_metadata(df, json_path, run_id)` | Appends standard pipeline metadata columns (`_source_file`, `_load_timestamp`, `_pipeline_run_id`, `_target_grain`, `_record_hash`) |
| `add_surrogate_key(df, key_parts)` | Generates a deterministic `xxhash64` surrogate key column |

See [docs/json-processor.md](docs/json-processor.md) for full parameter reference.

## Medallion Pipeline

```
Landing  →  Bronze  →  Silver (planned)
```

The ingestion framework is designed around a batch model where the orchestrator assigns a `batch_id` that flows through every layer. See [docs/ingestion-framework.md](docs/ingestion-framework.md) for the full architecture.

## Project Structure

```
src/steamroller/
    ingestion.py        # ingest_landing_to_bronze
    json_processor.py   # drill_and_flatten, prune_to_relationship, add_metadata, add_surrogate_key
docs/
    ingestion.md              # ingestion function reference
    ingestion-framework.md    # medallion pipeline architecture
    json-processor.md         # JSON processor function reference
test/
    ingest_landing_to_bronze.ps1
    make_test_data.ps1
```
