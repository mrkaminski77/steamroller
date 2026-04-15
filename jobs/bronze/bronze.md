# bronze

Ingests raw JSON blobs from a landing zone into a Bronze Delta table.

## Arguments

| Argument | Required | Description |
|---|---|---|
| `--blob_addresses` | Yes | Base64-encoded JSON array of blob addresses to ingest |
| `--source_metadata` | Yes | Base64-encoded JSON object with source metadata |
| `--dq_audit_path` | Yes | ABFSS path to write data quality audit logs |
| `--bronze_path` | Yes | ABFSS path to write the Bronze Delta table |
| `--batch_id` | Yes | Unique identifier for this ingestion batch |
| `--schema` | No | Base64-encoded JSON schema string, or a path to a JSON schema file |

Both `--blob_addresses` and `--source_metadata` must be base64-encoded JSON. Encode them before passing:

```python
import json, base64
base64.b64encode(json.dumps(value).encode()).decode()
```

---

## Using in a Synapse Pipeline (Spark Job Definition Activity)

1. In Synapse Studio, add a **Spark Job Definition** activity to your pipeline.
2. Set the **Spark job definition** to `bronze` (under the `Ingestion` folder).
3. Under **Settings > Arguments**, provide the arguments as a space-separated string:

```
--blob_addresses <base64> --source_metadata <base64> --dq_audit_path abfss://<container>@<account>.dfs.core.windows.net/dq/bronze --bronze_path abfss://<container>@<account>.dfs.core.windows.net/bronze/<table> --batch_id <batch_id>
```

Use pipeline expressions to generate base64-encoded values dynamically, e.g. with `@base64(string(variables('blobAddresses')))`.

---

## Calling Directly via the Synapse REST API

Submit a Spark batch job using the Synapse Spark API.

**Endpoint:**
```
POST https://{workspace}.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/{spark_pool}/batches
```

**Request body:**
```json
{
  "name": "bronze-run",
  "file": "abfss://{container}@{account}.dfs.core.windows.net/scripts/bronze.py",
  "args": [
    "--blob_addresses", "<base64>",
    "--source_metadata", "<base64>",
    "--dq_audit_path", "abfss://{container}@{account}.dfs.core.windows.net/dq/bronze",
    "--bronze_path", "abfss://{container}@{account}.dfs.core.windows.net/bronze/{table}",
    "--batch_id", "{batch_id}"
  ],
  "numExecutors": 2,
  "executorCores": 4,
  "executorMemory": "28g",
  "driverCores": 4,
  "driverMemory": "28g",
  "conf": {
    "spark.sql.sources.partitionOverwriteMode": "dynamic"
  }
}
```

**Authentication:** Use a Bearer token obtained via `az account get-access-token --resource https://dev.azuresynapse.net`.

**Via Azure CLI:**
```powershell
$token = (az account get-access-token --resource https://dev.azuresynapse.net | ConvertFrom-Json).accessToken

Invoke-RestMethod `
  -Method Post `
  -Uri "https://{workspace}.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/{spark_pool}/batches" `
  -Headers @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" } `
  -Body ($body | ConvertTo-Json -Depth 10)
```

Poll the returned `id` at `.../batches/{id}` until `state` is `success` or `dead`.
