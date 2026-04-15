$workspace        = "sra1dsynapsews"
$spark_pool       = "adhoc"
$storage_account  = "sra1dstasynapsews"
$code_container   = "code"
$temp_container   = "temp"

$bronze_script        = "abfss://$code_container@$storage_account.dfs.core.windows.net/scripts/bronze.py"
$steamroller_package  = "abfss://$code_container@$storage_account.dfs.core.windows.net/packages/steamroller.zip"
$bronze_path      = "abfss://$temp_container@$storage_account.dfs.core.windows.net/test/bronze/agentEvents"
$dq_audit_path    = "abfss://$temp_container@$storage_account.dfs.core.windows.net/test/dq_audit/agentEvents"
$batch_id         = "test-bronze-$(Get-Date -Format 'yyyyMMddHHmmss')"

$blob_addresses   = @("abfss://$temp_container@$storage_account.dfs.core.windows.net/output-agentEvents-639116714846793699.json")
$source_metadata  = @{
    source_system  = "AzureBlobStorage"
    source_entity  = "agentEvents"
    query_window   = @{ start = "2024-01-01T00:00:00Z"; end = "2024-12-31T23:59:59Z" }
    query_params   = @{ param1 = "value1" }
}

$blob_addresses_b64  = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes(($blob_addresses  | ConvertTo-Json -Compress)))
$source_metadata_b64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes(($source_metadata | ConvertTo-Json -Compress)))

# --- Submit Livy batch job ---
$livy_base = "https://$workspace.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/$spark_pool"
$token     = (az account get-access-token --resource https://dev.azuresynapse.net | ConvertFrom-Json).accessToken
$headers   = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

$body = @{
    name           = "test-bronze-job"
    file           = $bronze_script
    pyFiles        = @($steamroller_package)
    args           = @(
        "--blob_addresses",  $blob_addresses_b64,
        "--source_metadata", $source_metadata_b64,
        "--dq_audit_path",   $dq_audit_path,
        "--bronze_path",     $bronze_path,
        "--batch_id",        $batch_id
    )
    numExecutors   = 2
    executorCores  = 4
    executorMemory = "28g"
    driverCores    = 4
    driverMemory   = "28g"
    conf           = @{ "spark.sql.sources.partitionOverwriteMode" = "dynamic" }
} | ConvertTo-Json -Depth 10

$batch = Invoke-RestMethod -Method Post -Uri "$livy_base/batches" -Headers $headers -Body $body
Write-Host "Submitted batch ID: $($batch.id)"

# --- Poll until complete ---
do {
    Start-Sleep -Seconds 15
    $batch = Invoke-RestMethod -Method Get -Uri "$livy_base/batches/$($batch.id)" -Headers $headers
    Write-Host "Batch state: $($batch.state)"
} while ($batch.state -in @("starting", "not_started", "running"))

if ($batch.state -ne "success") {
    Write-Error "Batch job failed with state: $($batch.state)"
    Write-Host ($batch.log -join "`n")
    exit 1
}

Write-Host "Batch job completed successfully."

# --- Read back output tables to verify ---
. "$PSScriptRoot\SynapseLiby.ps1"

$session = New-SynapseSparkSession `
    -WorkspaceName $workspace `
    -PoolName      $spark_pool `
    -SessionName   "test_bronze_job_verify"

Write-Host "`n--- Bronze table ---"
$bronze_out = Invoke-SparkStatement -Session $session -Code @"
df = spark.read.format('delta').load('$bronze_path')
df.show(truncate=False)
print(f'{df.count()} rows, {len(df.columns)} columns')
"@
if ($bronze_out.status -eq "error") {
    Write-Error "Bronze read error: $($bronze_out.evalue)`n$($bronze_out.traceback -join "`n")"
} else {
    Write-Host $bronze_out.data.'text/plain'
}

Write-Host "`n--- Audit table ---"
$audit_out = Invoke-SparkStatement -Session $session -Code @"
df = spark.read.format('delta').load('$dq_audit_path')
df.show(truncate=False)
print(f'{df.count()} rows')
"@
if ($audit_out.status -eq "error") {
    Write-Error "Audit read error: $($audit_out.evalue)`n$($audit_out.traceback -join "`n")"
} else {
    Write-Host $audit_out.data.'text/plain'
}

Remove-SynapseSparkSession -Session $session
