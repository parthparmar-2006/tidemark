param(
    [string]$Label,
    [string]$DatabaseUrl = "postgres://tidemark:tidemark@localhost:5433/tidemark?sslmode=disable",
    [string]$Duration = "15s",
    [int]$Workers = 16
)

$env:DATABASE_URL = $DatabaseUrl
$service = Start-Process bin\service.exe -PassThru -NoNewWindow -RedirectStandardError "results\raw\service-$Label.log"
Start-Sleep -Milliseconds 500

$env:DURATION = $Duration
$env:WORKERS = "$Workers"
Write-Output "=== $Label (workers=$Workers)"
bin\loadgen.exe | Select-String "committed|rejected|failed|latency p50|latency p99"

Stop-Process -Id $service.Id
