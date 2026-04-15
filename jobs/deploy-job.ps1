param (
    [string]$synapse_workspace,
    [string]$storage_account,
    [string]$storage_container,
    [string]$job_name,
    [string]$spark_pool
)
if ((Split-Path -Leaf $pwd) -ne "jobs") {
    Write-Host "Please set-location to the 'jobs' directory before running this script."
    exit
}
\# This script assumes you have the Azure CLI installed and are logged in with access to the target Synapse workspace.
$jobTemplateFile = "job.json"
$jobOutputDir = "$job_name"
$jobOutputFile = "$jobOutputDir/$job_name.json"

if (-not (Test-Path $jobTemplateFile)) {
    Write-Host "Template file not found: $jobTemplateFile"
    exit
}

New-Item -ItemType Directory -Force -Path $jobOutputDir | Out-Null

$jobContent = Get-Content $jobTemplateFile | ConvertFrom-Json
$jobContent.targetBigDataPool.referenceName = $spark_pool
$jobContent.jobProperties.file = "abfss://$storage_container@$storage_account.dfs.core.windows.net/scripts/$job_name.py"

$jobContent | ConvertTo-Json -Depth 10 | Set-Content $jobOutputFile

az storage blob upload `
    --account-name $storage_account `
    --container-name $storage_container `
    --name "scripts/$job_name.py" `
    --file "$job_name/$job_name.py" `
    --overwrite `
    --auth-mode login

az synapse spark-job-definition create `
    --workspace-name $synapse_workspace `
    --name $job_name `
    --file "@$jobOutputFile" `
    --folder-path "Ingestion"


