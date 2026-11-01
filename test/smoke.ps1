param([string]$BaseUrl = 'http://localhost:8080')
$ErrorActionPreference = 'Stop'

# Load demo credentials without changing the caller's environment.
$settings = @{}
Get-Content (Join-Path $PSScriptRoot '../.env') | ForEach-Object {
    if ($_ -match '^([A-Z_]+)=(.*)$') { $settings[$matches[1]] = $matches[2] }
}
function Login($user, $password) {
    $body = @{username=$user; password=$password} | ConvertTo-Json
    $response = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/auth/login" -ContentType application/json -Body $body
    return @{Authorization="Bearer $($response.token)"}
}
function ExpectStatus($status, [scriptblock]$action) {
    try { & $action | Out-Null } catch {
        if ([int]$_.Exception.Response.StatusCode -eq $status) { return }
        throw
    }
    throw "Expected HTTP $status"
}

Invoke-RestMethod "$BaseUrl/health/ready" | Out-Null
$admin = Login $settings.AUTH_ADMIN_USERNAME $settings.AUTH_ADMIN_PASSWORD
$viewer = Login $settings.AUTH_VIEWER_USERNAME $settings.AUTH_VIEWER_PASSWORD
$query = "$BaseUrl/api/query?limit=1000"
ExpectStatus 401 { Invoke-RestMethod $query }
ExpectStatus 400 { Invoke-RestMethod "$BaseUrl/api/query?sql=SELECT%201%20LIMIT%201" -Headers $admin }
ExpectStatus 403 {
    Invoke-RestMethod -Method Post "$BaseUrl/api/sources" -Headers $viewer -ContentType application/json -Body '{"topic":"events","count":1,"interval":0}'
}
$before = Invoke-RestMethod $query -Headers $viewer
$baseline = ($before.data | Measure-Object -Property click_count -Sum).Sum
Invoke-RestMethod -Method Post "$BaseUrl/api/sources" -Headers $admin -ContentType application/json -Body '{"topic":"events","count":10,"interval":0}' | Out-Null
$deadline = (Get-Date).AddSeconds(30)
do {
    $after = Invoke-RestMethod $query -Headers $viewer
    $total = ($after.data | Measure-Object -Property click_count -Sum).Sum
    if ($total -ge $baseline + 10) { break }
    Start-Sleep -Milliseconds 500
} while ((Get-Date) -lt $deadline)
if ($total -lt $baseline + 10) { throw 'Published events did not appear in ClickHouse' }
Invoke-WebRequest "$BaseUrl/metrics" -UseBasicParsing | Out-Null
Write-Output 'PASS: login, RBAC, query validation, metrics, API -> Kafka -> ClickHouse (10 events)'
