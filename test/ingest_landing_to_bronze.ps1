$source_container = "abfss://temp@sra1dstasynapsews.dfs.core.windows.net"
$schema_container = "abfss://config@sra1dstasynapsews.dfs.core.windows.net"
$source_file = "output-agentEvents-639116714846793699.json"
$schema_file = "transperth/agentEvents.json"
$subscription_id = "e6dbcc53-5170-441b-8c16-e6d1c5a3c092"
$tenant_id = "f93616dd-45a6-40c8-9e29-adab2fb5f25c"





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

import sys
import types
import unittest.mock as mock

# mssparkutils is only injected in pipeline/notebook contexts, not Livy interactive sessions.
# Stub it out so the function can be exercised end-to-end from a test script.
fake_context = json.dumps({
    "pipelineRunId": "test-run-001",
    "workspaceName": "sra1dsynapsews",
    "sparkPoolName": "adhoc",
    "jobId":         "test-job-001",
})

mssparkutils_mock = types.ModuleType("mssparkutils")
mssparkutils_mock.env = mock.MagicMock()
mssparkutils_mock.env.getJobContext = mock.MagicMock(return_value=fake_context)
mssparkutils_mock.env.getTenantId = mock.MagicMock(return_value="$tenant_id")
sys.modules["mssparkutils"] = mssparkutils_mock

from pyspark.sql.types import StructType
from steamroller import ingest_landing_to_bronze

schema = StructType.fromJson(json.loads('''$source_schema'''))

result = ingest_landing_to_bronze(
    blob_addresses=["$source_container/$source_file"],
    source_metadata={
        "source_system": "AzureBlobStorage",
        "source_entity": "agentEvents",
        "query_window": {"start": "2024-01-01T00:00:00Z", "end": "2024-12-31T23:59:59Z"},
        "query_params": {"param1": "value1", "param2": "value2"},
    },
    dq_audit_path="$source_container/dq_audit/agentEvents_dq_audit",
    bronze_path="$source_container/bronze/agentEvents_bronze",
    schema="$schema_container/$schema_file",
)

print(result)
"@

. "$PSScriptRoot\SynapseLiby.ps1"

# --- 1. Create an interactive session ---
$session = New-SynapseSparkSession `
    -WorkspaceName "sra1dsynapsews" `
    -PoolName      "adhoc" `
    -SessionName   "steamroller_ingest_test"

# --- 2. Submit the ingestion job ---
$output = Invoke-SparkStatement -Session $session -Code $sparkjob
if ($output.status -eq "error") {
    Write-Error "PySpark error: $($output.evalue)`n$($output.traceback -join "`n")"
} else {
    Write-Host "Output: $($output.data.'text/plain')"
}

# --- 6. Read back the bronze table ---
Write-Host "`n--- Bronze table ---"
$bronze_output = Invoke-SparkStatement -Session $session -Code @"
df = spark.read.format('delta').load('$source_container/bronze/agentEvents_bronze')
df.show(truncate=False)
print(f'{df.count()} rows, {len(df.columns)} columns')
"@
if ($bronze_output.status -eq "error") {
    Write-Error "Bronze read error: $($bronze_output.evalue)"
} else {
    Write-Host $bronze_output.data.'text/plain'
}

# --- 7. Read back the audit table ---
Write-Host "`n--- Audit table ---"
$audit_output = Invoke-SparkStatement -Session $session -Code @"
df = spark.read.format('delta').load('$source_container/dq_audit/agentEvents_dq_audit')
df.show(truncate=False)
"@
if ($audit_output.status -eq "error") {
    Write-Error "Audit read error: $($audit_output.evalue)"
} else {
    Write-Host $audit_output.data.'text/plain'
}

# --- 8. Clean up session ---
Remove-SynapseSparkSession -Session $session
