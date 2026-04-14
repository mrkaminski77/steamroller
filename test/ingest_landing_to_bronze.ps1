$source_container = "abfss://temp@sra1dstasynapsews.dfs.core.windows.net"

$source_data = @'
{
    "testdata": [
        {"id": 1, "name": "Alice",   "age": 30, "city": "New York"},
        {"id": 2, "name": "Bob",     "age": 25, "city": "Los Angeles"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"}
    ]
}
'@

$source_schema = @'
{
    "type": "struct",
    "fields": [
        {
            "name": "testdata",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {"name": "id",   "type": "integer", "nullable": true, "metadata": {}},
                        {"name": "name", "type": "string",  "nullable": true, "metadata": {}},
                        {"name": "age",  "type": "integer", "nullable": true, "metadata": {}},
                        {"name": "city", "type": "string",  "nullable": true, "metadata": {}}
                    ]
                },
                "containsNull": true
            },
            "nullable": true,
            "metadata": {}
        }
    ]
}
'@

# Write the test data to a temp file and upload to blob storage
$tempFile = [System.IO.Path]::GetTempFileName() + ".json"
$source_data | Set-Content -Path $tempFile -Encoding UTF8
az storage blob upload `
    --account-name sra1dstasynapsews `
    --container-name temp `
    --name testdata.json `
    --file $tempFile `
    --overwrite `
    --auth-mode login
Remove-Item $tempFile

$github = "https://github.com/mrkaminski77/steamroller.git"

$sparkjob = @"
import urllib.request
import zipfile
import json
import sys
import os

def _install_from_zip(repo_url):
    # Convert: https://github.com/mrkaminski77/steamroller.git
    # To:      https://github.com/mrkaminski77/steamroller/archive/refs/heads/main.zip
    zip_url = repo_url.replace(".git", "/archive/refs/heads/main.zip")
    extract_path = "/tmp/custom_modules"
    zip_path = "/tmp/repo.zip"

    urllib.request.urlretrieve(zip_url, zip_path)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    module_path = os.path.join(extract_path, "steamroller-main")
    if os.path.exists(os.path.join(module_path, "src")):
        module_path = os.path.join(module_path, "src")

    if module_path not in sys.path:
        sys.path.insert(0, module_path)

_install_from_zip("$github")

from pyspark.sql.types import StructType
from steamroller import ingest_landing_to_bronze

schema = StructType.fromJson(json.loads('''$source_schema'''))

result = ingest_landing_to_bronze(
    blob_addresses=["$source_container/testdata.json"],
    source_metadata={
        "source_system": "AzureBlobStorage",
        "source_entity": "testdata",
        "query_window": {"start": "2024-01-01T00:00:00Z", "end": "2024-12-31T23:59:59Z"},
        "query_params": {"param1": "value1", "param2": "value2"},
    },
    dq_audit_path="$source_container/dq_audit/testdata_dq_audit",
    bronze_path="$source_container/bronze/testdata_bronze",
    schema=schema,
)

print(result)
"@

# --- Synapse / Livy config ---
$workspace_name  = "sra1dsynapsews"
$spark_pool_name = "adhoc"   # replace with your pool name
$livy_base       = "https://$workspace_name.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/$spark_pool_name"

# --- Get a bearer token scoped to the Synapse dev endpoint ---
$token = (az account get-access-token --resource https://dev.azuresynapse.net | ConvertFrom-Json).accessToken

$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type"  = "application/json"
}

# --- 1. Create an interactive session ---
$session_body = @{ kind = "pyspark" } | ConvertTo-Json
$session = Invoke-RestMethod -Method Post -Uri "$livy_base/sessions" -Headers $headers -Body $session_body
$session_id = $session.id
Write-Host "Created session ID: $session_id"

# --- 2. Wait for the session to become idle ---
do {
    Start-Sleep -Seconds 10
    $session_state = (Invoke-RestMethod -Method Get -Uri "$livy_base/sessions/$session_id" -Headers $headers).state
    Write-Host "Session state: $session_state"
} while ($session_state -in @("starting", "not_started"))

if ($session_state -ne "idle") {
    Write-Error "Session failed to start. Final state: $session_state"
    exit 1
}

# --- 3. Submit the PySpark code as a statement ---
$statement_body = @{ code = $sparkjob } | ConvertTo-Json -Depth 10
$statement = Invoke-RestMethod -Method Post -Uri "$livy_base/sessions/$session_id/statements" -Headers $headers -Body $statement_body
$statement_id = $statement.id
Write-Host "Submitted statement ID: $statement_id"

# --- 4. Poll until the statement completes ---
do {
    Start-Sleep -Seconds 5
    $result = Invoke-RestMethod -Method Get -Uri "$livy_base/sessions/$session_id/statements/$statement_id" -Headers $headers
    Write-Host "Statement state: $($result.state)"
} while ($result.state -in @("waiting", "running"))

# --- 5. Print output ---
$output = $result.output
if ($output.status -eq "error") {
    Write-Error "PySpark error: $($output.evalue)`n$($output.traceback -join "`n")"
} else {
    Write-Host "Output: $($output.data.'text/plain')"
}

# --- 6. Clean up session ---
Invoke-RestMethod -Method Delete -Uri "$livy_base/sessions/$session_id" -Headers $headers | Out-Null
Write-Host "Session $session_id deleted."
