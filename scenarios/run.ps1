param(
    [ValidateSet("baseline", "relay-kill", "kafka-kill", "kafka-pause", "postgres-kill")]
    [string]$Scenario = "baseline",
    [int]$Seconds = 60,
    [int]$FaultAt = 15,
    [int]$FaultFor = 15,
    [double]$MinFreeGB = 0.5
)

$ErrorActionPreference = "Continue"
$baseUrl = "postgres://tidemark:tidemark@localhost:5433/tidemark?sslmode=disable"
$raw = "results\raw\$Scenario"
New-Item -ItemType Directory -Force $raw | Out-Null

function Reset-Stack {
    docker compose down -v 2>&1 | Out-Null
    docker compose up -d --wait postgres kafka 2>&1 | Out-Null
    docker compose run --rm kafka-init 2>&1 | Out-Null
}

function Start-Relay {
    $env:DATABASE_URL = "$baseUrl&application_name=relay"
    $logName = "$raw\relay-$(Get-Date -Format HHmmssfff).log"
    return Start-Process bin\relay-poll.exe -PassThru -NoNewWindow -RedirectStandardError $logName
}

function Get-Sample {
    $sql = "SELECT (SELECT count(*) FROM outbox), COALESCE((SELECT round(EXTRACT(EPOCH FROM max(now() - xact_start))::numeric, 1) FROM pg_stat_activity WHERE application_name = 'relay' AND xact_start IS NOT NULL), 0), (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'accounts'), (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'outbox')"
    $output = docker compose exec -T postgres psql -U tidemark -tA -F "," -c $sql 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $output) {
        return "db-down,db-down,db-down,db-down"
    }
    return $output.Trim()
}

function Get-Backlog {
    $output = docker compose exec -T postgres psql -U tidemark -tA -c "SELECT count(*) FROM outbox" 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $output) {
        return -1
    }
    return [int]$output.Trim()
}

function Test-Preflight {
    $freeGB = [math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory / 1MB, 2)
    $others = docker ps --format "{{.Names}}" | Where-Object { $_ -notlike "tidemark-*" }
    Write-Host "preflight: host free memory $freeGB GB, other containers: $(@($others).Count)"
    if ($freeGB -lt $MinFreeGB) {
        Write-Host "preflight FAILED: less than $MinFreeGB GB free, results would not be trustworthy"
        return $false
    }
    if (@($others).Count -gt 0) {
        Write-Host "preflight FAILED: other containers running: $($others -join ', ')"
        return $false
    }
    return $true
}

Write-Output "=== scenario: $Scenario ($Seconds s, fault at $FaultAt s for $FaultFor s)"
Reset-Stack
if (-not (Test-Preflight)) {
    exit 2
}

$env:DATABASE_URL = $baseUrl
$service = Start-Process bin\service.exe -PassThru -NoNewWindow -RedirectStandardError "$raw\service.log"
$relay = Start-Relay
Start-Sleep -Seconds 1
if ($service.HasExited -or $relay.HasExited) {
    Write-Output "service or relay failed to start; see $raw"
    exit 2
}

$env:DURATION = "$($Seconds)s"
$env:WORKERS = "16"
$loadgen = Start-Process bin\loadgen.exe -PassThru -NoNewWindow -RedirectStandardOutput "$raw\loadgen.txt" -RedirectStandardError "$raw\loadgen.log"
$env:WORKERS = "4"
$rogue = Start-Process bin\rogue-writer.exe -PassThru -NoNewWindow -RedirectStandardOutput "$raw\rogue.txt" -RedirectStandardError "$raw\rogue.log"

$samples = New-Object System.Collections.Generic.List[string]
$samples.Add("second,backlog,relay_tx_age_s,accounts_dead,outbox_dead,note")
$started = Get-Date
$nextKill = 3
$relayKills = 0
$faultActive = $false
$vacuumChecked = $false

while (-not $loadgen.HasExited) {
    $elapsed = [int]((Get-Date) - $started).TotalSeconds
    $note = ""

    if ($Scenario -eq "relay-kill" -and $elapsed -ge $nextKill) {
        Stop-Process -Id $relay.Id -Force
        $relayKills++
        Start-Sleep -Milliseconds 300
        $relay = Start-Relay
        $nextKill = $elapsed + (Get-Random -Minimum 2 -Maximum 6)
        $note = "relay killed"
    }

    if ($Scenario -in @("kafka-kill", "kafka-pause", "postgres-kill")) {
        if (-not $faultActive -and $elapsed -ge $FaultAt -and $elapsed -lt $FaultAt + $FaultFor) {
            if ($Scenario -eq "kafka-kill") { docker compose kill kafka 2>&1 | Out-Null }
            if ($Scenario -eq "kafka-pause") { docker compose pause kafka 2>&1 | Out-Null }
            if ($Scenario -eq "postgres-kill") { docker compose kill postgres 2>&1 | Out-Null }
            $faultActive = $true
            $note = "fault start"
        }
        elseif ($faultActive -and $Scenario -ne "postgres-kill" -and $elapsed -ge $FaultAt + $FaultFor - 3 -and -not $vacuumChecked) {
            $vacuum = docker compose exec -T postgres psql -U tidemark -c "VACUUM (VERBOSE) accounts" 2>&1 | Select-String "dead but not yet removable"
            $vacuumChecked = $true
            $note = "vacuum: $($vacuum.Line.Trim() -replace ',', ';')"
        }
        elseif ($faultActive -and $elapsed -ge $FaultAt + $FaultFor) {
            if ($Scenario -eq "kafka-kill") { docker compose start kafka 2>&1 | Out-Null }
            if ($Scenario -eq "kafka-pause") { docker compose unpause kafka 2>&1 | Out-Null }
            if ($Scenario -eq "postgres-kill") { docker compose start postgres 2>&1 | Out-Null }
            $faultActive = $false
            $FaultAt = 100000
            $note = "fault end"
        }
    }

    $samples.Add("$elapsed,$(Get-Sample),$note")
    Start-Sleep -Seconds 1
}

$drainStarted = Get-Date
$rogue.WaitForExit()
$backlog = Get-Backlog
while ($backlog -ne 0 -and ((Get-Date) - $drainStarted).TotalSeconds -lt 180) {
    $elapsed = [int]((Get-Date) - $started).TotalSeconds
    $samples.Add("$elapsed,$(Get-Sample),draining")
    Start-Sleep -Seconds 1
    $backlog = Get-Backlog
}
$drainSeconds = [math]::Round(((Get-Date) - $drainStarted).TotalSeconds, 1)

Stop-Process -Id $relay.Id -Force
Stop-Process -Id $service.Id -Force
$samples | Out-File -Encoding utf8 "$raw\samples.csv"

Write-Output "--- load"
Get-Content "$raw\loadgen.txt"
Write-Output "--- rogue writer"
Get-Content "$raw\rogue.txt"
Write-Output "--- relay"
Write-Output "relay kills      : $relayKills"
Write-Output "drain after load : $drainSeconds s (backlog at end: $backlog)"
Write-Output "--- verify"
$env:DATABASE_URL = $baseUrl
bin\verify.exe
Write-Output "verify exit code : $LASTEXITCODE"
