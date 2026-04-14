function New-SynapseSparkSession {
    param(
        [Parameter(Mandatory)][string] $WorkspaceName,
        [Parameter(Mandatory)][string] $PoolName,
        [Parameter(Mandatory)][string] $SessionName,
        [int]    $DriverCores    = 4,
        [string] $DriverMemory   = "28g",
        [int]    $ExecutorCores  = 4,
        [string] $ExecutorMemory = "28g",
        [int]    $NumExecutors   = 2
    )

    $livy_base = "https://$WorkspaceName.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/$PoolName"
    $token     = (az account get-access-token --resource https://dev.azuresynapse.net | ConvertFrom-Json).accessToken

    $headers = @{
        "Authorization" = "Bearer $token"
        "Content-Type"  = "application/json"
    }

    $body = @{
        kind           = "pyspark"
        name           = $SessionName
        driverCores    = $DriverCores
        driverMemory   = $DriverMemory
        executorCores  = $ExecutorCores
        executorMemory = $ExecutorMemory
        numExecutors   = $NumExecutors
    } | ConvertTo-Json

    $session = Invoke-RestMethod -Method Post -Uri "$livy_base/sessions" -Headers $headers -Body $body
    Write-Host "Created session ID: $($session.id)"

    # Wait until idle
    do {
        Start-Sleep -Seconds 10
        $state = (Invoke-RestMethod -Method Get -Uri "$livy_base/sessions/$($session.id)" -Headers $headers).state
        Write-Host "Session state: $state"
    } while ($state -in @("starting", "not_started"))

    if ($state -ne "idle") {
        throw "Session failed to start. Final state: $state"
    }

    # Return a session object with everything callers need
    return [PSCustomObject]@{
        Id        = $session.id
        LivyBase  = $livy_base
        Headers   = $headers
    }
}

function Invoke-SparkStatement {
    param(
        [Parameter(Mandatory)][PSCustomObject] $Session,
        [Parameter(Mandatory)][string]         $Code
    )

    $body = @{ kind = "pyspark"; code = $Code } | ConvertTo-Json -Depth 10
    $stmt = Invoke-RestMethod -Method Post -Uri "$($Session.LivyBase)/sessions/$($Session.Id)/statements" -Headers $Session.Headers -Body $body
    Write-Host "Submitted statement ID: $($stmt.id)"

    do {
        Start-Sleep -Seconds 5
        $stmt = Invoke-RestMethod -Method Get -Uri "$($Session.LivyBase)/sessions/$($Session.Id)/statements/$($stmt.id)" -Headers $Session.Headers
        Write-Host "Statement state: $($stmt.state)"
    } while ($stmt.state -in @("waiting", "running"))

    return $stmt.output
}

function Remove-SynapseSparkSession {
    param(
        [Parameter(Mandatory)][PSCustomObject] $Session
    )

    Invoke-RestMethod -Method Delete -Uri "$($Session.LivyBase)/sessions/$($Session.Id)" -Headers $Session.Headers | Out-Null
    Write-Host "Session $($Session.Id) deleted."
}
