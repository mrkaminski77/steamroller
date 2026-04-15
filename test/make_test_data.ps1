$source_container = "abfss://temp@sra1dstasynapsews.dfs.core.windows.net"
$subscription_id = "e6dbcc53-5170-441b-8c16-e6d1c5a3c092"
$tenant_id = "f93616dd-45a6-40c8-9e29-adab2fb5f25c"

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

$tempFile = [System.IO.Path]::GetTempFileName() + ".json"
$source_schema | Set-Content -Path $tempFile -Encoding UTF8
az storage blob upload `
    --account-name sra1dstasynapsews `
    --container-name temp `
    --name testschema.json `
    --file $tempFile `
    --overwrite `
    --auth-mode login
Remove-Item $tempFile
