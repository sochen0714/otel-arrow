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
    param([Parameter(Mandatory)][string] $Group)

    $output = & docker compose @composeArgs exec -T kafka `
        kafka-consumer-groups --bootstrap-server kafka:9092 `
        --describe --group $Group 2>$null
    if ($LASTEXITCODE -ne 0) {
        return $null
    }

    foreach ($line in $output) {
        $columns = @($line.Trim() -split '\s+')
        if ($columns.Count -ge 6 -and
            $columns[0] -eq $Group -and
            $columns[1] -eq $topic -and
            $columns[2] -eq '0') {
            $current = if ($columns[3] -eq '-') { $null } else { [int] $columns[3] }
            $lag = if ($columns[5] -eq '-') { $null } else { [int] $columns[5] }
            return [pscustomobject]@{
                CurrentOffset = $current
                LogEndOffset = [int] $columns[4]
                Lag = $lag
            }
        }
    }
    $null
}

function Get-ReceivedCount {
    $metrics = & curl.exe -s --max-time 2 `
        http://localhost:8080/api/v1/telemetry/metrics 2>$null
    if ($LASTEXITCODE -ne 0) {
        return 0
    }

    $total = 0
    foreach ($line in $metrics) {
        if ($line -match '^records_received_total\{.*\}\s+(\d+)(?:\s|$)') {
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

Write-Host 'Starting a fresh single-partition Kafka stack...'
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

Write-Host ''
Write-Host 'Case 1: in-order acknowledgements advance the committed offset.'
$env:KAFKA_GROUP_ID = 'offset-in-order'
$env:ACK_DELAY = '3s'
$null = Invoke-Compose -Arguments @(
    'up', '-d', '--no-deps', '--force-recreate', 'consumer'
)

$sawIntermediateOffset = $false
$completed = $false
$lastOffset = -1
$deadline = (Get-Date).AddSeconds(30)
do {
    $state = Get-GroupState -Group $env:KAFKA_GROUP_ID
    if ($null -ne $state -and $null -ne $state.CurrentOffset) {
        if ($state.CurrentOffset -ne $lastOffset) {
            Write-Host "  committed=$($state.CurrentOffset), lag=$($state.Lag)"
            $lastOffset = $state.CurrentOffset
        }
        if ($state.CurrentOffset -in @(1, 2)) {
            $sawIntermediateOffset = $true
        }
        if ($state.CurrentOffset -eq 3 -and $state.Lag -eq 0) {
            $completed = $true
            break
        }
    }
    Start-Sleep -Milliseconds 500
} while ((Get-Date) -lt $deadline)

if (-not $completed) {
    throw 'The in-order group did not commit offset 3 and drain lag to zero.'
}
if (-not $sawIntermediateOffset) {
    throw 'No intermediate committed offset was observed before offset 3.'
}
Write-Host 'PASS: committed offset advanced only after downstream Ack.'

Write-Host ''
Write-Host 'Case 2: a crash before Ack causes replay after restart.'
$null = Invoke-Compose -Arguments @('rm', '-sf', 'consumer')
$env:KAFKA_GROUP_ID = 'offset-crash-replay'
$env:ACK_DELAY = '15s'
$null = Invoke-Compose -Arguments @(
    'up', '-d', '--no-deps', '--force-recreate', 'consumer'
)

Wait-Until -TimeoutSeconds 20 -Description 'the first delivery of all messages' -Condition {
    (Get-ReceivedCount) -eq 3
}
$beforeCrash = Get-GroupState -Group $env:KAFKA_GROUP_ID
if ($null -ne $beforeCrash -and
    $null -ne $beforeCrash.CurrentOffset -and
    $beforeCrash.CurrentOffset -gt 0) {
    throw "Offset advanced to $($beforeCrash.CurrentOffset) before the forced crash."
}
Write-Host '  first process received all 3 messages; committed offset has not advanced.'

$null = Invoke-Compose -Arguments @('kill', 'consumer')
$null = Invoke-Compose -Arguments @('rm', '-f', 'consumer')
$null = Invoke-Compose -Arguments @('up', '-d', '--no-deps', 'consumer')

Wait-Until -TimeoutSeconds 30 -Description 'all messages to be replayed after restart' -Condition {
    (Get-ReceivedCount) -eq 3
}
Write-Host '  restarted process received the same 3 messages again.'

Wait-Until -TimeoutSeconds 75 -Description 'the replay group to drain' -Condition {
    $state = Get-GroupState -Group $env:KAFKA_GROUP_ID
    $null -ne $state -and $state.CurrentOffset -eq 3 -and $state.Lag -eq 0
}
Write-Host 'PASS: restart replayed uncommitted messages, then committed offset 3.'

Write-Host ''
Write-Host 'All at-least-once offset validations passed.'
Write-Host 'Redpanda Console remains available at http://localhost:8082'
