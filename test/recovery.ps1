$ErrorActionPreference = 'Stop'
$settings = @{}
Get-Content (Join-Path $PSScriptRoot '../.env') | ForEach-Object {
    if ($_ -match '^([A-Z_]+)=(.*)$') { $settings[$matches[1]] = $matches[2] }
}
$base = 'http://localhost:8080'
$body = @{username=$settings.AUTH_ADMIN_USERNAME; password=$settings.AUTH_ADMIN_PASSWORD} | ConvertTo-Json
$login = Invoke-RestMethod -Method Post "$base/api/auth/login" -ContentType application/json -Body $body
$headers = @{Authorization="Bearer $($login.token)"}
function TotalClicks {
    $response = Invoke-RestMethod "$base/api/query?limit=1000" -Headers $headers
    return [double]($response.data | Measure-Object -Property click_count -Sum).Sum
}
$before = TotalClicks
try {
    docker compose stop clickhouse-server
    if ($LASTEXITCODE) { throw 'Could not stop test dependency' }
    try {
        Invoke-RestMethod "$base/health/ready" | Out-Null
        throw 'Readiness should fail while ClickHouse is stopped'
    } catch {
        if (-not $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 503) { throw }
    }
    Invoke-RestMethod -Method Post "$base/api/sources" -Headers $headers -ContentType application/json -Body '{"topic":"events","count":3,"interval":0}' | Out-Null
    # Restart the consumer as well, proving that uncommitted events replay.
    docker compose stop go-api
    if ($LASTEXITCODE) { throw 'Could not stop consumer' }
} finally {
    docker compose up -d --wait --wait-timeout 120 clickhouse-server go-api
    if ($LASTEXITCODE) { throw 'Could not restore test services' }
}
$deadline = (Get-Date).AddSeconds(30)
do {
    if ((TotalClicks) -ge $before + 3) {
        Write-Output 'PASS: readiness fails on database outage; Kafka events survive API restart and reach ClickHouse'
        exit 0
    }
    Start-Sleep -Milliseconds 500
} while ((Get-Date) -lt $deadline)
throw 'Events were lost during recovery'
