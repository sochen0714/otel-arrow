[CmdletBinding()]
param(
    [switch] $SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$exampleDirectory = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $exampleDirectory 'compose.yaml'
$overlayFile = Join-Path $exampleDirectory 'compose.dataflow.yaml'
$composeArgs = @('-f', $composeFile, '-f', $overlayFile)
$topic = 'otlp-logs'
$group = 'terminal-failure-drop-count'

function Invoke-Compose {
    param([Parameter(Mandatory)][string[]] $Arguments)

    $output = & docker compose @composeArgs @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        $output | Write-Host
        throw "docker compose $($Arguments -join ' ') failed."
    }
    $output
}

function Get-TopicMessageCount {
    $output = & docker compose @composeArgs exec -T kafka `
        kafka-get-offsets --bootstrap-server kafka:9092 --topic $topic 2>$null
    if ($LASTEXITCODE -ne 0) {
        return 0
    }

    $total = 0
    foreach ($line in $output) {
        if ($line -match ':(\d+)$') {
            $total += [int] $Matches[1]
        }
    }
    $total
}

function Get-GroupState {
    $output = & docker compose @composeArgs exec -T kafka `
        kafka-consumer-groups --bootstrap-server kafka:9092 `
        --describe --group $group 2>$null
    if ($LASTEXITCODE -ne 0) {
        return $null
    }

    foreach ($line in $output) {
        $columns = @($line.Trim() -split '\s+')
        if ($columns.Count -ge 6 -and
            $columns[0] -eq $group -and
            $columns[1] -eq $topic -and
            $columns[2] -eq '0') {
            return [pscustomobject]@{
                CurrentOffset = [int] $columns[3]
                LogEndOffset = [int] $columns[4]
                Lag = [int] $columns[5]
            }
        }
    }
    $null
}

function Get-Metrics {
    $metrics = & curl.exe -s --max-time 2 `
        http://localhost:8080/api/v1/telemetry/metrics 2>$null
    if ($LASTEXITCODE -ne 0) {
        return @()
    }
    @($metrics)
}

function Get-ReceivedCount {
    $total = 0
    foreach ($line in (Get-Metrics)) {
        if ($line -match '^records_received_total\{.*\}\s+(\d+)(?:\s|$)' -and
            $line -like '*otel_scope_name="receiver.kafka.consumer"*') {
            $total += [int] $Matches[1]
        }
    }
    $total
}

function Get-TerminalNackCount {
    $total = 0
    foreach ($line in (Get-Metrics)) {
        if ($line -match '^responses_total\{.*\}\s+(\d+)(?:\s|$)' -and
            $line -like '*otel_scope_name="receiver.kafka.acknowledgements"*' -and
            $line -like '*outcome="refused"*' -and
            $line -like '*signal="logs"*') {
            $total += [int] $Matches[1]
        }
    }
    $total
}

function Get-InFlightCount {
    $total = 0
    foreach ($line in (Get-Metrics)) {
        if ($line -match '^records_inflight\{.*\}\s+(-?\d+)(?:\s|$)' -and
            $line -like '*otel_scope_name="receiver.kafka.consumer"*') {
            $total += [int] $Matches[1]
        }
    }
    $total
}

function Wait-Until {
    param(
        [Parameter(Mandatory)][scriptblock] $Condition,
        [Parameter(Mandatory)][int] $TimeoutSeconds,
        [Parameter(Mandatory)][string] $Description
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        if (& $Condition) {
            return
        }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)

    throw "Timed out waiting for $Description."
}

Write-Host 'Starting a fresh terminal-failure validation stack...'
$null = Invoke-Compose -Arguments @('down', '--remove-orphans')
$upArgs = @('up', '-d')
if (-not $SkipBuild) {
    $upArgs += '--build'
}
$upArgs += @('kafka', 'kafka-init', 'console', 'producer')
$null = Invoke-Compose -Arguments $upArgs

Wait-Until -TimeoutSeconds 60 -Description 'three Kafka messages' -Condition {
    (Get-TopicMessageCount) -eq 3
}
Write-Host 'PASS: producer created offsets 0, 1, and 2 in one partition.'

$null = Invoke-Compose -Arguments @(
    'up', '-d', '--no-deps', '--force-recreate', 'consumer'
)

Wait-Until -TimeoutSeconds 30 -Description 'all records to reach the failing sink' -Condition {
    (Get-ReceivedCount) -eq 3 -and (Get-TerminalNackCount) -eq 3
}
Write-Host 'PASS: all 3 failed records were counted as terminal refused responses.'

Wait-Until -TimeoutSeconds 30 -Description 'the consumer group to drain' -Condition {
    $state = Get-GroupState
    $null -ne $state -and
        $state.CurrentOffset -eq 3 -and
        $state.LogEndOffset -eq 3 -and
        $state.Lag -eq 0 -and
        (Get-InFlightCount) -eq 0
}
Write-Host 'PASS: terminal Nacks advanced the committed offset to 3 and drained lag.'

Start-Sleep -Seconds 3
if ((Get-ReceivedCount) -ne 3 -or (Get-TerminalNackCount) -ne 3) {
    throw 'A terminally failed record was retried or redelivered.'
}
Write-Host 'PASS: the pipeline stayed live without retrying the failed records.'

Write-Host ''
Write-Host 'All terminal failure drop-and-count validations passed.'
Write-Host 'Redpanda Console remains available at http://localhost:8082'
Write-Host 'Receiver metrics remain available at http://localhost:8080/api/v1/telemetry/metrics'
