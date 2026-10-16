#requires -Version 7.0
[CmdletBinding()]
param(
    [switch] $SkipBuild,
    [string] $Image,
    [ValidateRange(15, 180)][int] $CaseTimeoutSeconds = 45
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$example = Split-Path -Parent $PSScriptRoot
$repo = (Resolve-Path (Join-Path $example '..\..\..\..\..')).Path
$source = (& git -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Cannot identify source revision.' }
$runId = "ff-sasl-5750616-$([guid]::NewGuid().ToString('N').Substring(0, 10))"
$run = Join-Path $example ".runs\$runId"
$credentials = Join-Path $run 'credentials'
$null = New-Item -ItemType Directory -Path $credentials -Force
$utf8 = [System.Text.UTF8Encoding]::new($false)
$commands = [System.Collections.Generic.List[object]]::new()
$results = [System.Collections.Generic.List[object]]::new()
# These are public, synthetic local-test credentials from the reused broker.
$password = 'plain-secret'
$wrongPassword = 'ff-wrong-password-5750616'
$wrongUsername = 'ff-wrong-username-5750616'
$malformedMarker = 'ff-private-file-content-5750616'
$secretValues = @($password, $wrongPassword, $wrongUsername, $malformedMarker)
$environmentNames = @(
    'FLAT_FILE_IMAGE', 'FLAT_FILE_SOURCE_SHA', 'FLAT_FILE_RUN_DIR',
    'FLAT_FILE_GROUP', 'FLAT_FILE_CONFIG', 'FLAT_FILE_USERNAME_FILE',
    'FLAT_FILE_PASSWORD_FILE'
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name)
}
if (-not $Image) { $Image = "otap-flat-file-sasl:$($source.Substring(0, 12))" }
$env:FLAT_FILE_IMAGE = $Image
$env:FLAT_FILE_SOURCE_SHA = $source
$env:FLAT_FILE_RUN_DIR = $run
$env:FLAT_FILE_GROUP = $runId
$env:FLAT_FILE_CONFIG = 'receiver-inline.yaml'
$env:FLAT_FILE_USERNAME_FILE = '/credentials/username'
$env:FLAT_FILE_PASSWORD_FILE = '/credentials/password'
$compose = @(
    'compose', '--project-name', $runId,
    '-f', (Join-Path $example 'compose.yaml'),
    '-f', (Join-Path $example 'compose.dataflow.yaml')
)

function Protect-Text {
    param([AllowEmptyString()][string] $Text)
    foreach ($value in $secretValues) { $Text = $Text.Replace($value, '[REDACTED]') }
    $Text
}

function Write-Artifact {
    param([string] $Name, [AllowEmptyString()][string] $Text)
    [IO.File]::WriteAllText((Join-Path $run $Name), (Protect-Text $Text), $utf8)
}

function Invoke-Docker {
    param(
        [Parameter(Mandatory)][string[]] $Arguments,
        [int] $TimeoutSeconds = 60,
        [AllowNull()][string] $InputText,
        [switch] $AllowFailure
    )
    $start = [Diagnostics.ProcessStartInfo]::new('docker')
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.RedirectStandardInput = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    $null = $process.Start()
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    if ($null -ne $InputText) { $process.StandardInput.Write($InputText) }
    $process.StandardInput.Close()
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        $process.Kill($true)
        $process.WaitForExit()
        throw "Docker command exceeded ${TimeoutSeconds}s: $(Protect-Text ($Arguments -join ' '))"
    }
    $result = [pscustomobject]@{
        ExitCode = $process.ExitCode
        Stdout = $stdout.GetAwaiter().GetResult()
        Stderr = $stderr.GetAwaiter().GetResult()
    }
    $commands.Add([pscustomobject]@{
        Command = Protect-Text ("docker " + ($Arguments -join ' '))
        ExitCode = $result.ExitCode
    })
    $process.Dispose()
    if ($result.ExitCode -ne 0 -and -not $AllowFailure) {
        throw "Docker command failed ($($result.ExitCode)): $(Protect-Text ($result.Stdout + $result.Stderr))"
    }
    $result
}

function Invoke-Compose {
    param(
        [string[]] $Arguments,
        [int] $TimeoutSeconds = 60,
        [AllowNull()][string] $InputText
    )
    Invoke-Docker -Arguments ($compose + $Arguments) -TimeoutSeconds $TimeoutSeconds -InputText $InputText
}

function Assert-NoSecret {
    param([string] $Text, [string] $Context)
    foreach ($secret in $secretValues) {
        if ($Text.Contains($secret)) {
            throw "Credential content leaked in $Context; evidence is redacted."
        }
    }
}

function Get-ReceiverLogs {
    $result = Invoke-Compose @('logs', '--no-color', '--no-log-prefix', 'receiver')
    $raw = $result.Stdout + $result.Stderr
    Write-Artifact "$caseName.log" $raw
    Assert-NoSecret $raw $caseName
    $raw
}

function Get-OutputBodies {
    param([string] $Logs)
    foreach ($line in ($Logs -split '\r?\n')) {
        if ($line.StartsWith('{')) {
            $record = ConvertFrom-Json -InputObject $line -AsHashtable
            if ($record.ContainsKey('body')) { [string] $record.body }
        }
    }
}

# Scenario: Both extension username sources authenticate and decode known Kafka log records.
# Guarantees: Exact expected bodies, not startup messages or offsets alone, reach the console exporter.
function Test-Delivery {
    param([string] $Name, [string] $Config)
    $script:caseName = $Name
    $env:FLAT_FILE_CONFIG = $Config
    $env:FLAT_FILE_GROUP = "$runId-$Name"
    $env:FLAT_FILE_USERNAME_FILE = '/credentials/username'
    $env:FLAT_FILE_PASSWORD_FILE = '/credentials/password'
    $null = Invoke-Compose @('up', '-d', '--no-deps', '--force-recreate', 'receiver')
    $deadline = [DateTime]::UtcNow.AddSeconds($CaseTimeoutSeconds)
    do {
        $logs = Get-ReceiverLogs
        $bodies = @(Get-OutputBodies $logs)
        if ($bodies.Count -eq $expectedBodies.Count) {
            if (@(Compare-Object $expectedBodies $bodies).Count -ne 0) {
                throw "$Name emitted incorrect telemetry bodies."
            }
            $null = Invoke-Compose @('stop', '-t', '5', 'receiver')
            $bodies = @(Get-OutputBodies (Get-ReceiverLogs))
            if ($bodies.Count -ne $expectedBodies.Count -or
                @(Compare-Object $expectedBodies $bodies).Count -ne 0) {
                throw "$Name final output differs from the exact expected telemetry."
            }
            $results.Add([pscustomobject]@{
                Case = $Name; Outcome = 'PASS'; Bodies = $bodies; Log = "$Name.log"
            })
            Write-Host "PASS: $Name - $($bodies.Count) exact decoded log bodies."
            return
        }
        $state = Invoke-Compose @('ps', '-a', '--format', 'json', 'receiver')
        if ($state.Stdout -match '"State"\s*:\s*"exited"') {
            throw "$Name receiver exited before delivering telemetry; inspect $run."
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "$Name did not deliver all expected log bodies within ${CaseTimeoutSeconds}s."
}

# Scenario: File acquisition fails, or selected file credentials are rejected by Kafka.
# Guarantees: No fallback delivery, no credential disclosure, and a bounded observable failure.
function Test-Rejection {
    param(
        [string] $Name, [string] $Config,
        [string] $UsernameFile = '/credentials/username',
        [string] $PasswordFile = '/credentials/password',
        [string] $ErrorPattern,
        [switch] $StartupFailure
    )
    $script:caseName = $Name
    $env:FLAT_FILE_CONFIG = $Config
    $env:FLAT_FILE_GROUP = "$runId-$Name"
    $env:FLAT_FILE_USERNAME_FILE = $UsernameFile
    $env:FLAT_FILE_PASSWORD_FILE = $PasswordFile
    $null = Invoke-Compose @('up', '-d', '--no-deps', '--force-recreate', 'receiver')
    $id = (Invoke-Compose @('ps', '-aq', 'receiver')).Stdout.Trim()
    if (-not $id) { throw "$Name has no receiver container." }
    $deadline = [DateTime]::UtcNow.AddSeconds($CaseTimeoutSeconds)
    $matched = $false
    $exitCode = $null
    do {
        $logs = Get-ReceiverLogs
        if (@(Get-OutputBodies $logs).Count -ne 0) {
            throw "$Name unexpectedly consumed telemetry (possible inline fallback)."
        }
        if ($logs -match $ErrorPattern) { $matched = $true }
        $state = (Invoke-Docker @('inspect', '--format', '{{json .State}}', $id)).Stdout | ConvertFrom-Json
        if (-not $state.Running) {
            $exitCode = $state.ExitCode
            if ($exitCode -eq 0) { throw "$Name exited successfully instead of rejecting credentials." }
            break
        }
        # Broker authentication is asynchronous; observe failure plus a no-delivery window.
        if (-not $StartupFailure -and $matched) {
            Start-Sleep -Seconds 3
            $logs = Get-ReceiverLogs
            if (@(Get-OutputBodies $logs).Count -ne 0) {
                throw "$Name consumed telemetry after an authentication failure."
            }
            break
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not $matched) { throw "$Name did not emit its expected safe error: $ErrorPattern" }
    if ($StartupFailure -and $null -eq $exitCode) {
        throw "$Name did not fail startup within ${CaseTimeoutSeconds}s."
    }
    $null = Invoke-Compose @('stop', '-t', '5', 'receiver')
    if (@(Get-OutputBodies (Get-ReceiverLogs)).Count -ne 0) {
        throw "$Name emitted telemetry before final shutdown (possible inline fallback)."
    }
    $results.Add([pscustomobject]@{
        Case = $Name; Outcome = 'PASS'; ExitCode = $exitCode
        ErrorPattern = $ErrorPattern; Bodies = @(); Log = "$Name.log"
    })
    Write-Host "PASS: $Name - safe rejection, no decoded records."
}

$cleanupSucceeded = $false
$allPassed = $false
$script:caseName = 'setup'
try {
    $null = Invoke-Docker @('version', '--format', '{{.Server.Version}}')
    if (-not $SkipBuild) {
        $build = Invoke-Compose -Arguments @('build', 'receiver') -TimeoutSeconds 2400
        Write-Artifact 'build.log' ($build.Stdout + $build.Stderr)
    }
    $imageInfo = (Invoke-Docker @('image', 'inspect', $Image)).Stdout | ConvertFrom-Json
    $labels = $imageInfo[0].Config.Labels
    $revisionLabel = if ($null -ne $labels) {
        $labels.PSObject.Properties['org.opencontainers.image.revision']
    } else { $null }
    if ($null -eq $revisionLabel -or $revisionLabel.Value -ne $source) {
        throw 'Image source revision does not match HEAD; rebuild instead of testing a stale binary.'
    }
    # Hash the actual executable from this image, without assuming a shell in distroless.
    $binaryContainer = (Invoke-Docker @('create', $Image, '--help')).Stdout.Trim()
    try {
        $null = Invoke-Docker @('cp', "${binaryContainer}:/home/nonroot/df_engine", (Join-Path $run 'df_engine'))
        $binaryHash = (Get-FileHash (Join-Path $run 'df_engine') -Algorithm SHA256).Hash.ToLowerInvariant()
    } finally {
        $null = Invoke-Docker @('rm', $binaryContainer)
    }
    Remove-Item -LiteralPath (Join-Path $run 'df_engine')
    Write-Artifact 'provenance.json' (@{
        Source = $source; Image = $Image; ImageId = $imageInfo[0].Id
        BinarySha256 = $binaryHash; Mechanism = 'PLAIN'; Transport = 'SASL_SSL'
        Encoding = 'plaintext'; Project = $runId; StartedUtc = [DateTime]::UtcNow.ToString('o')
        Features = 'kafka,flat-file-sasl-auth'; Rustflags = '-C target-cpu=x86-64'
    } | ConvertTo-Json)
    [IO.File]::WriteAllText((Join-Path $credentials 'username'), "plain`r`n", $utf8)
    [IO.File]::WriteAllText((Join-Path $credentials 'password'), "$password`r`n", $utf8)
    [IO.File]::WriteAllText((Join-Path $credentials 'wrong-password'), $wrongPassword, $utf8)
    [IO.File]::WriteAllText((Join-Path $credentials 'wrong-username'), $wrongUsername, $utf8)
    [IO.File]::WriteAllText((Join-Path $credentials 'empty'), "`r`n", $utf8)
    [IO.File]::WriteAllBytes((Join-Path $credentials 'malformed'),
        ($utf8.GetBytes($malformedMarker) + [byte[]]@(0xff, 0xfe)))
    $null = Invoke-Compose -Arguments @('up', '-d', '--wait', '--wait-timeout', '180', 'kafka') -TimeoutSeconds 240
    $brokerInfo = (Invoke-Docker @('image', 'inspect', 'confluentinc/cp-kafka:8.3.1')).Stdout | ConvertFrom-Json
    Write-Artifact 'broker-image.json' (@{
        Image = 'confluentinc/cp-kafka:8.3.1'; Id = $brokerInfo[0].Id; RepoDigests = $brokerInfo[0].RepoDigests
    } | ConvertTo-Json)
    $properties = @"
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="plain" password="$password";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
ssl.truststore.password=changeit
request.timeout.ms=10000
default.api.timeout.ms=20000
"@
    $null = Invoke-Compose -Arguments @('exec', '-T', 'kafka', 'sh', '-c', 'umask 077; cat > /tmp/flat-file-client.properties') -InputText $properties
    $null = Invoke-Compose @('exec', '-T', 'kafka', 'kafka-topics',
        '--bootstrap-server', 'kafka:9093', '--command-config', '/tmp/flat-file-client.properties',
        '--create', '--topic', 'flat-file-sasl-logs', '--partitions', '1', '--replication-factor', '1')
    $expectedBodies = @(1..3 | ForEach-Object { "task-5750616-$runId-record-$_" })
    Write-Artifact 'expected-bodies.json' (ConvertTo-Json -InputObject $expectedBodies)
    $null = Invoke-Compose -Arguments @('exec', '-T', 'kafka', 'kafka-console-producer',
        '--bootstrap-server', 'kafka:9093', '--producer.config', '/tmp/flat-file-client.properties',
        '--topic', 'flat-file-sasl-logs') -InputText (($expectedBodies -join "`n") + "`n")
    # Independent broker-side count is supplementary; decoded exporter bodies are the oracle.
    $offsets = Invoke-Compose @('exec', '-T', 'kafka', 'kafka-get-offsets',
        '--bootstrap-server', 'kafka:9093', '--command-config', '/tmp/flat-file-client.properties',
        '--topic', 'flat-file-sasl-logs')
    Write-Artifact 'topic-offsets.log' $offsets.Stdout
    if ($offsets.Stdout.Trim() -ne 'flat-file-sasl-logs:0:3') { throw 'Producer did not create exactly three Kafka records.' }
    Test-Delivery 'inline-username' 'receiver-inline.yaml'
    Test-Delivery 'file-username' 'receiver-files.yaml'
    foreach ($field in @('password', 'username')) {
        foreach ($kind in @('missing', 'malformed', 'empty')) {
            $fieldName = if ($field -eq 'password') { 'password_secret_file' } else { 'username_file' }
            $detail = switch ($kind) {
                'missing' { "failed to read .$fieldName." }
                'malformed' { "invalid credential from .$fieldName..*valid UTF-8" }
                'empty' { "invalid credential from .$fieldName..*cannot be empty" }
            }
            $parameters = @{
                Name = "$kind-$field"; Config = 'receiver-files.yaml'; StartupFailure = $true
                ErrorPattern = $detail
            }
            $parameters["$($field)File"] = "/credentials/$kind"
            Test-Rejection @parameters
        }
    }
    Test-Rejection -Name 'wrong-password' -Config 'receiver-inline.yaml' `
        -PasswordFile '/credentials/wrong-password' -ErrorPattern '(?i)SASL authentication (error|failed)|Authentication failure|SaslAuthenticationFailed'
    Test-Rejection -Name 'wrong-username-no-fallback' -Config 'receiver-files.yaml' `
        -UsernameFile '/credentials/wrong-username' -ErrorPattern '(?i)SASL authentication (error|failed)|Authentication failure|SaslAuthenticationFailed'
    if ($results.Count -ne 10) { throw 'The required ten-case matrix did not finish.' }
    $allPassed = $true
    Write-Host "All $($results.Count) runtime cases passed."
} finally {
    try {
        try {
            $brokerLogs = Invoke-Compose @('logs', '--no-color', '--no-log-prefix', 'kafka', 'certgen')
            Write-Artifact 'broker.log' ($brokerLogs.Stdout + $brokerLogs.Stderr)
        } finally {
            $null = Invoke-Compose -Arguments @('down', '--volumes', '--remove-orphans', '--timeout', '10') -TimeoutSeconds 90
            $cleanupSucceeded = $true
        }
    } finally {
        Write-Artifact 'results.json' (ConvertTo-Json -InputObject @($results.ToArray()) -Depth 8)
        Write-Artifact 'commands.json' (ConvertTo-Json -InputObject @($commands.ToArray()) -Depth 5)
        Write-Artifact 'cleanup.json' (@{ Succeeded = $cleanupSucceeded; Project = $runId } | ConvertTo-Json)
        Write-Artifact 'summary.json' (@{
            TestsSucceeded = $allPassed; CleanupSucceeded = $cleanupSucceeded
            ExpectedCases = 10; PassedCases = $results.Count; LastCase = $script:caseName
        } | ConvertTo-Json)
        foreach ($file in @('username', 'password', 'wrong-password', 'wrong-username', 'empty', 'malformed')) {
            $path = Join-Path $credentials $file
            if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path }
        }
        foreach ($name in $environmentNames) {
            [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name])
        }
        Write-Host "Sanitized evidence: $run"
    }
}
