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

