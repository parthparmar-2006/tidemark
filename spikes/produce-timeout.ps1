param(
    [int]$PauseAt = 10,
    [int]$PauseFor = 30,
    [int]$Seconds = 60
)

$base = "postgres://tidemark:tidemark@localhost:5433/tidemark?sslmode=disable"
$raw = "results\raw\produce-timeout"
New-Item -ItemType Directory -Force $raw | Out-Null
Remove-Item "$raw\*" -ErrorAction SilentlyContinue

$env:DATABASE_URL = $base
$service = Start-Process bin\service.exe -PassThru -NoNewWindow -RedirectStandardError "$raw\service.log"
$env:DATABASE_URL = "$base&application_name=relay"
$relay = Start-Process bin\relay-poll.exe -PassThru -NoNewWindow -RedirectStandardError "$raw\relay.log"
Start-Sleep -Seconds 1
if ($service.HasExited -or $relay.HasExited) {
    Write-Output "service or relay failed to start"
    Get-Content "$raw\service.log", "$raw\relay.log"
    exit 2
}

$env:DURATION = "$($Seconds)s"
$env:WORKERS = "4"
$loadgen = Start-Process bin\loadgen.exe -PassThru -NoNewWindow -RedirectStandardOutput "$raw\loadgen.txt"

$started = Get-Date
$paused = $false
$sql = "SELECT (SELECT count(*) FROM outbox), COALESCE((SELECT round(EXTRACT(EPOCH FROM max(now() - xact_start))::numeric, 1) FROM pg_stat_activity WHERE application_name = 'relay' AND xact_start IS NOT NULL), 0)"
Write-Output "second,backlog,relay_tx_age_s,note"
while (-not $loadgen.HasExited) {
    $elapsed = [int]((Get-Date) - $started).TotalSeconds
    $note = ""
    if (-not $paused -and $elapsed -ge $PauseAt -and $elapsed -lt $PauseAt + $PauseFor) {
        docker compose pause kafka 2>&1 | Out-Null
        $paused = $true
        $note = "pause"
    }
    elseif ($paused -and $elapsed -ge $PauseAt + $PauseFor) {
        docker compose unpause kafka 2>&1 | Out-Null
        $paused = $false
        $PauseAt = 100000
        $note = "unpause"
    }
    $sample = docker compose exec -T postgres psql -U tidemark -tA -F "," -c $sql 2>$null
    Write-Output "$elapsed,$sample,$note"
    Start-Sleep -Seconds 2
}

Start-Sleep -Seconds 5
Stop-Process -Id $relay.Id, $service.Id -Force
Write-Output "--- relay log"
Get-Content "$raw\relay.log"
