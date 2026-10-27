#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{40}$')][string] $SourceCommit,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{40}$')][string] $ProductionCommit,
    [Parameter(Mandatory)][string] $RuntimeInput,
    [switch] $ExecutionApproved,
    [ValidateSet('managed_identity', 'development')][string] $IdentityMode = 'managed_identity',
    [switch] $ManagedIdentityVmApproved,
    [switch] $SkipBuild,
    [string] $Image,
    [ValidatePattern('^akv-sasl-[0-9a-f]{12}$')][string] $ProjectName,
    [ValidatePattern('^otap-akv-e2e-login(-[a-z0-9-]+)?$')]
    [string] $LoginVolume = 'otap-akv-e2e-login',
    [ValidateRange(30, 180)][int] $CaseTimeoutSeconds = 90
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $ExecutionApproved) {
    throw 'Execution requires explicit approval for the selected runtime and identity.'
}
if ($IdentityMode -eq 'managed_identity' -and (-not $ManagedIdentityVmApproved -or -not $IsLinux)) {
    throw 'Managed identity may run only on the explicitly approved Linux Azure VM.'
}
if ($IdentityMode -eq 'development' -and $ManagedIdentityVmApproved) {
    throw 'The approved-VM switch applies only to managed identity mode.'
}
$example = Split-Path -Parent $PSScriptRoot
$repo = (Resolve-Path (Join-Path $example '..' '..' '..' '..' '..')).Path
$source = (& git -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $source -ne $SourceCommit) {
    throw 'SourceCommit must be the exact integrated validation checkout HEAD.'
}
& git -C $repo merge-base --is-ancestor $ProductionCommit HEAD
if ($LASTEXITCODE -ne 0) {
    & git -C $repo cat-file -e "$ProductionCommit^{commit}"
    if ($LASTEXITCODE -ne 0) { throw 'The handed-off final production commit is not available locally.' }
    $integratedRevisions = & git -C $repo log '--format=%(trailers:key=Production-Source-Commit,valueonly)' HEAD
    if ($LASTEXITCODE -ne 0 -or $ProductionCommit -notin @($integratedRevisions | ForEach-Object { $_.Trim() })) {
        throw 'Production source is neither an ancestor nor recorded by an explicit net-integration commit.'
    }
}
$dirty = & git -C $repo status --porcelain --untracked-files=normal
if ($LASTEXITCODE -ne 0 -or $dirty) { throw 'Commit the validation checkout before building or executing.' }
function Get-PublishedSdk {
    param([string] $Lock)
    $expected = [ordered]@{
        azure_core = '1.2.0-beta.1'
        azure_identity = '1.1.0-beta.1'
        azure_security_keyvault_secrets = '1.1.0-beta.1'
    }
    $records = [regex]::Split($Lock, '(?m)^\[\[package\]\]\r?$')
    foreach ($name in $expected.Keys) {
        $version = [regex]::Escape($expected[$name])
        $matching = @($records | Where-Object {
            $_ -match "(?m)^name = `"$name`"`r?$" -and $_ -match "(?m)^version = `"$version`"`r?$"
        })
        if ($matching.Count -ne 1 -or
            $matching[0] -notmatch '(?m)^source = "registry\+https://github\.com/rust-lang/crates\.io-index"\r?$') {
            throw 'Published Azure SDK provenance mismatch.'
        }
    }
    return $expected
}
$lock = Get-Content -Raw (Join-Path $repo 'rust' 'otap-dataflow' 'Cargo.lock')
$sdk = Get-PublishedSdk -Lock $lock
$manifest = Get-Content -Raw (Join-Path $repo 'rust' 'otap-dataflow' 'Cargo.toml')
if (-not $manifest.Contains('azure-key-vault-sasl-auth')) { throw 'The AKV feature is not integrated.' }
$inputPath = (Resolve-Path -LiteralPath $RuntimeInput).Path
$localRoot = Join-Path $example '.local'
$pathComparison = if ($IsWindows) { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
if (-not $inputPath.StartsWith("$localRoot$([IO.Path]::DirectorySeparatorChar)", $pathComparison)) {
    throw 'Runtime input must be under this example''s ignored .local directory.'
}
& git -C $repo check-ignore --quiet -- $inputPath
if ($LASTEXITCODE -ne 0) { throw 'Runtime input is not ignored by Git.' }

$runId = if ($ProjectName) { $ProjectName } else { "akv-sasl-$([guid]::NewGuid().ToString('N').Substring(0, 12))" }
$run = Join-Path $example '.runs' $runId
$runtime = Join-Path $run 'runtime'
$null = New-Item -ItemType Directory -Path $runtime -Force
$utf8 = [Text.UTF8Encoding]::new($false)
$commands = [Collections.Generic.List[object]]::new()
$results = [Collections.Generic.List[object]]::new()
$expectedBodies = @(1..3 | ForEach-Object { "$runId-record-$_" })
$engineImage = "otap-akv-e2e-engine:$($source.Substring(0, 12))"
if (-not $Image) { $Image = "otap-akv-e2e-development:$($source.Substring(0, 12))" }
$environmentNames = @(
    'AKV_IMAGE', 'AKV_INPUT_FILE', 'AKV_RUN_DIR', 'AKV_GROUP',
    'AKV_LOGIN_VOLUME', 'AKV_IDENTITY_MODE', 'AKV_MANAGED_IDENTITY_APPROVED'
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name)
}
$env:AKV_IMAGE = $Image
$env:AKV_INPUT_FILE = $inputPath
$env:AKV_RUN_DIR = $run
$env:AKV_GROUP = $runId
$env:AKV_IDENTITY_MODE = $IdentityMode
$env:AKV_MANAGED_IDENTITY_APPROVED = if ($ManagedIdentityVmApproved) { 'true' } else { 'false' }
$env:AKV_LOGIN_VOLUME = if ($IdentityMode -eq 'development') { $LoginVolume } else { $null }
$compose = @(
    'compose', '--project-name', $runId,
    '-f', (Join-Path $example 'compose.yaml'),
    '-f', (Join-Path $example 'compose.dataflow.yaml')
)
if ($IdentityMode -eq 'development') {
    $compose += @('-f', (Join-Path $example 'compose.development.yaml'))
}
$script:lastSnapshot = $null
$script:lastContainer = $null
$script:caseName = 'setup'
$cleanupSucceeded = $false
$allPassed = $false
$matrixPassed = $false
$expectedCases = 5
$binaryContainer = $null
$brokerProvisioned = $false

function Write-Artifact {
    param([string] $Name, [object] $Value)
    [IO.File]::WriteAllText((Join-Path $run $Name), (ConvertTo-Json -InputObject $Value -Depth 12), $utf8)
}

function Invoke-Docker {
    param(
        [Parameter(Mandatory)][string[]] $Arguments,
        [int] $TimeoutSeconds = 60,
        [AllowNull()][string] $InputText,
        [string] $Action = 'docker'
    )
    $start = [Diagnostics.ProcessStartInfo]::new('docker')
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.RedirectStandardInput = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    $clock = [Diagnostics.Stopwatch]::StartNew()
    $started = $false
    try {
        $started = $process.Start()
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        if ($null -ne $InputText) {
            $write = $process.StandardInput.WriteAsync($InputText)
            if (-not $write.Wait([TimeSpan]::FromSeconds($TimeoutSeconds))) {
                throw "Timed out sending private stdin for $Action."
            }
        }
        $process.StandardInput.Close()
        $remaining = [Math]::Max(1, ($TimeoutSeconds * 1000) - [int]$clock.ElapsedMilliseconds)
        if (-not $process.WaitForExit($remaining)) { throw "Docker operation timed out: $Action." }
        $commands.Add(@{ Action = $Action; ExitCode = $process.ExitCode })
        if ($process.ExitCode -ne 0) {
            # Never echo stdout/stderr: even unexpected Docker/helper errors may contain private data.
            throw "Docker operation failed ($($process.ExitCode)): $Action. Raw details suppressed."
        }
        [pscustomobject]@{
            Stdout = $stdout.GetAwaiter().GetResult()
            Stderr = $stderr.GetAwaiter().GetResult()
        }
    } finally {
        if ($started -and -not $process.HasExited) {
            $process.Kill($true)
            $process.WaitForExit()
        }
        $process.Dispose()
    }
}

function Invoke-Compose {
    param(
        [string[]] $Arguments,
        [int] $TimeoutSeconds = 60,
        [AllowNull()][string] $InputText,
        [string] $Action = 'compose'
    )
    Invoke-Docker -Arguments ($compose + $Arguments) -TimeoutSeconds $TimeoutSeconds `
        -InputText $InputText -Action $Action
}

function Invoke-Helper {
    param(
        [string[]] $Arguments,
        [AllowNull()][string] $InputText,
        [int] $TimeoutSeconds = 60
    )
    Invoke-Compose -Arguments (@('run', '--rm', '--no-deps', '-T', 'bootstrap') + $Arguments) `
        -InputText $InputText -TimeoutSeconds $TimeoutSeconds -Action "private-helper-$($Arguments[0])"
}

function Get-Snapshot {
    param([string] $Container)
    for ($attempt = 0; $attempt -lt 2; $attempt++) {
        $raw = Invoke-Docker -Arguments @('logs', $Container) -Action 'read-private-receiver-output' -TimeoutSeconds 15
        $request = ConvertTo-Json -InputObject @{
            logs = $raw.Stdout + $raw.Stderr; expected = $expectedBodies
        } -Compress
        $scan = Invoke-Helper -Arguments @('scan') -InputText $request -TimeoutSeconds 15
        $summary = ConvertFrom-Json -InputObject $scan.Stdout
        $stateResult = Invoke-Docker -Arguments @('inspect', '--format', '{{json .State}}', $Container) `
            -Action 'receiver-process-state' -TimeoutSeconds 10
        $state = ConvertFrom-Json -InputObject $stateResult.Stdout
        $script:lastSnapshot = @{
            Output = $summary; Running = $state.Running
            ExitCode = $state.ExitCode; OomKilled = $state.OOMKilled
        }
        Write-Artifact "$($script:caseName).json" $script:lastSnapshot
        if ($summary.LeakDetected) { throw 'Credential disclosure detected; raw output was not saved.' }
        if ($state.OOMKilled) { throw 'Receiver ran out of memory; this is not a credential failure.' }
        if ($attempt -eq 1 -and $state.Running) { throw 'Receiver restarted after a terminal observation.' }
        if ($state.Running -or $attempt -eq 1) { return [pscustomobject]$script:lastSnapshot }
        # The receiver may exit while the helper scans earlier logs. Recapture
        # once after observing its exit; never pair those earlier logs with it.
    }
}

function Start-Case {
    param([string] $Name)
    $script:caseName = $Name
    $script:lastSnapshot = $null
    $env:AKV_GROUP = "$runId-$Name"
    $null = Invoke-Helper -Arguments @('render', $Name, $runId)
    $null = Invoke-Compose -Arguments @('up', '-d', '--pull', 'never', '--no-deps', '--force-recreate', 'receiver') `
        -Action "start-$Name" -TimeoutSeconds 30
    $id = (Invoke-Compose -Arguments @('ps', '-aq', 'receiver') -Action 'receiver-id').Stdout.Trim()
    if ($id -notmatch '^[0-9a-f]{64}$' -or $id -eq $script:lastContainer) {
        throw 'Each case requires a newly created receiver container.'
    }
    $script:lastContainer = $id
    $id
}

function Stop-Receiver {
    $null = Invoke-Compose -Arguments @('stop', '-t', '5', 'receiver') -Action 'stop-receiver' -TimeoutSeconds 20
}

# Scenario: The real extension acquires both configured secrets before Kafka authentication.
# Guarantees: Exactly three known plaintext bodies reach the real record_json console exporter.
function Test-Delivery {
    $id = Start-Case 'configured'
    $deadline = [DateTime]::UtcNow.AddSeconds($CaseTimeoutSeconds)
    do {
        $snapshot = Get-Snapshot $id
        if ($snapshot.Output.OutputCount -gt 3) { throw 'Unexpected duplicate or extra telemetry.' }
        if ($snapshot.Output.ExactBodies) {
            if (-not $snapshot.Running) { throw 'Receiver exited unexpectedly after delivery.' }
            Stop-Receiver
            $final = Get-Snapshot $id
            if ($final.Running) { throw 'Receiver did not stop after delivery.' }
            if (-not $final.Output.ExactBodies) { throw 'Final output is not the exact three expected bodies.' }
            $results.Add(@{ Case = 'configured'; Outcome = 'PASS'; Output = $final.Output })
            Write-Host 'PASS: configured - three exact decoded log bodies.'
            return
        }
        if (-not $snapshot.Running) { throw 'Receiver exited before exact delivery.' }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'Configured AKV credentials did not deliver the exact expected telemetry before the deadline.'
}

# Scenario: A negative acquisition case reports its expected source failure and exact readiness gate.
# Guarantees: Polling and final capture apply the same strict classifier, including only the scoped 1 ms race.
function Test-StartupFailure {
    param([object] $Snapshot, [string] $Name, [string] $Stage, [string] $Category)
    $classified = @($Snapshot.Output.StartupFailures | Where-Object {
        $_.Category -eq $Category -and (-not $Stage -or $_.Stage -eq $Stage)
    }).Count -gt 0
    $expectedTimeout = if ($Name -eq 'timeout') { '1ms' } else { '15s' }
    $readinessFailed = @($Snapshot.Output.ReadinessFailures | Where-Object {
        $_.Extension -eq 'credentials' -and $_.Variant -eq 'shared' -and
        $_.Timeout -eq $expectedTimeout
    }).Count -gt 0
    # At 1 ms the engine's readiness timer can win the acquisition timer.
    $readinessFailed -and (
        $classified -or ($Name -eq 'timeout' -and $Category -eq 'timeout' -and -not $Stage)
    )
}

# Scenario: Acquisition fails before a complete pair, or the local broker rejects that pair.
# Guarantees: Scoped readiness failure, zero records, and a real nonzero acquisition exit.
function Test-Rejection {
    param([string] $Name, [string] $Stage, [string] $Category, [switch] $Broker)
    $id = Start-Case $Name
    $deadline = [DateTime]::UtcNow.AddSeconds($CaseTimeoutSeconds)
    $matched = $false
    do {
        $snapshot = Get-Snapshot $id
        if ($snapshot.Output.OutputCount -ne 0) { throw 'A negative case unexpectedly delivered telemetry.' }
        if ($Broker) {
            $matched = $snapshot.Output.BrokerAuthentication
            if ($matched) {
                Start-Sleep -Seconds 3
                break
            }
            if (-not $snapshot.Running) { throw 'Receiver exited without an explicit SASL rejection.' }
        } else {
            $matched = Test-StartupFailure -Snapshot $snapshot -Name $Name -Stage $Stage -Category $Category
            if (-not $snapshot.Running) {
                if ($snapshot.ExitCode -eq 0 -or -not $matched) {
                    throw 'Startup failure was not a nonzero exit with the expected safe category/stage.'
                }
                break
            }
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not $matched -or (-not $Broker -and $snapshot.Running)) {
        throw 'Expected bounded credential failure was not observed.'
    }
    Stop-Receiver
    $final = Get-Snapshot $id
    if ($final.Output.OutputCount -ne 0) { throw 'Telemetry appeared during negative-case shutdown.' }
    $finalMatched = if ($Broker) { $final.Output.BrokerAuthentication } else {
        $final.ExitCode -ne 0 -and (Test-StartupFailure -Snapshot $final -Name $Name -Stage $Stage -Category $Category)
    }
    if ($final.Running -or -not $finalMatched) { throw 'Final negative-case state or diagnostic did not preserve the accepted failure.' }
    $results.Add(@{ Case = $Name; Outcome = 'PASS'; Output = $final.Output; ExitCode = $final.ExitCode })
    Write-Host "PASS: $Name - classified failure with no decoded records."
}

try {
    $server = Invoke-Docker -Arguments @('info', '--format', '{{.OSType}}') -Action 'linux-engine'
    if ($server.Stdout.Trim() -ne 'linux') { throw 'Docker Desktop must be running Linux containers.' }
    if ($IdentityMode -eq 'development') {
        $null = Invoke-Docker -Arguments @('volume', 'inspect', $LoginVolume) -Action 'dedicated-login-volume'
    }
    # Resolve without saving the result, and reject a host-injected inherited JAAS override.
    $resolved = (Invoke-Compose -Arguments @('config', '--format', 'json') -Action 'compose-structure').Stdout |
        ConvertFrom-Json -AsHashtable
    $jaas = $resolved.services.kafka.environment[
        'KAFKA_LISTENER_NAME_SASL__SSL_PLAIN_SASL_JAAS_CONFIG'
    ]
    if ($null -ne $jaas) { throw 'Unexpected inline broker JAAS override.' }
    if (-not $SkipBuild) {
        $null = Invoke-Docker -Arguments @(
            'buildx', 'build', '--load', '--platform', 'linux/amd64',
            '--build-context', "otel-arrow=$repo",
            '--build-arg', 'TARGETPLATFORM=linux/amd64', '--build-arg', 'FEATURES=kafka,azure-key-vault-sasl-auth',
            '--build-arg', 'RUSTFLAGS=-C target-cpu=x86-64', '--build-arg', 'CARGO_BUILD_JOBS=2',
            '--label', "org.opencontainers.image.revision=$source",
            '-f', (Join-Path $example '..' 'Dockerfile.dataflow'),
            '-t', $engineImage, (Join-Path $repo 'rust' 'otap-dataflow')
        ) -TimeoutSeconds 3600 -Action 'build-actual-engine'
        $null = Invoke-Docker -Arguments @(
            'buildx', 'build', '--load', '--platform', 'linux/amd64', '--build-arg', "ENGINE_IMAGE=$engineImage",
            '--label', "org.opencontainers.image.revision=$source",
            '--label', "org.otel.akv.production-revision=$ProductionCommit",
            '--label', 'org.otel.akv.features=kafka,azure-key-vault-sasl-auth',
            '-f', (Join-Path $example 'Dockerfile.development'), '-t', $Image, $example
        ) -TimeoutSeconds 1200 -Action 'build-development-cli-image'
    }
    $imageInfo = ConvertFrom-Json -InputObject (
        Invoke-Docker -Arguments @('image', 'inspect', $Image) -Action 'engine-image-provenance'
    ).Stdout
    $labels = $imageInfo[0].Config.Labels
    if ($labels.'org.opencontainers.image.revision' -ne $source -or
        $labels.'org.otel.akv.production-revision' -ne $ProductionCommit -or
        $labels.'org.otel.akv.features' -ne 'kafka,azure-key-vault-sasl-auth') {
        throw 'Image provenance does not match the exact source, production handoff, and required features.'
    }
    $binaryContainer = (Invoke-Docker -Arguments @(
        'create', '--label', "com.docker.compose.project=$runId", $Image, '--help'
    ) -Action 'extract-binary-container').Stdout.Trim()
    $binaryPath = Join-Path $run 'df_engine'
    $null = Invoke-Docker -Arguments @('cp', "${binaryContainer}:/home/nonroot/df_engine", $binaryPath) -Action 'extract-binary'
    $binaryHash = (Get-FileHash -LiteralPath $binaryPath -Algorithm SHA256).Hash.ToLowerInvariant()
    Remove-Item -LiteralPath $binaryPath
    $null = Invoke-Docker -Arguments @('rm', $binaryContainer) -Action 'remove-extraction-container'
    $binaryContainer = $null
    $cli = Invoke-Docker -Arguments @('run', '--rm', '--network', 'none', '--entrypoint', 'az', $Image, 'version', '-o', 'json') `
        -Action 'cli-version'
    $cliVersion = (ConvertFrom-Json -InputObject $cli.Stdout).'azure-cli'
    Write-Artifact 'provenance.json' @{
        SourceCommit = $source; ProductionCommit = $ProductionCommit
        SdkSource = 'registry+https://github.com/rust-lang/crates.io-index'
        SdkPackages = $sdk
        HarnessBaseline = '92c43e96cba049d018d55f5446d677f1cc66d00e'
        ImageId = $imageInfo[0].Id; Image = $Image; ExecutableSha256 = $binaryHash
        Features = 'kafka,azure-key-vault-sasl-auth'; AzureCliVersion = $cliVersion
        Identity = $IdentityMode; Mechanism = 'PLAIN'; Transport = 'SASL_SSL'
        Encoding = 'plaintext'; Project = $runId; StartedUtc = [DateTime]::UtcNow.ToString('o')
    }
    Write-Artifact 'expected-bodies.json' $expectedBodies
    # First Azure access occurs here, after all source/image/execution gates.
    $null = Invoke-Compose -Arguments @('up', '-d', '--wait', '--wait-timeout', '240', 'kafka') `
        -Action 'provision-private-broker' -TimeoutSeconds 300
    $brokerProvisioned = $true
    $broker = ConvertFrom-Json -InputObject (
        Invoke-Docker -Arguments @('image', 'inspect', 'confluentinc/cp-kafka:8.3.1') -Action 'broker-image'
    ).Stdout
    Write-Artifact 'broker-image.json' @{ Id = $broker[0].Id; RepoDigests = $broker[0].RepoDigests }
    # Public synthetic admin only; the real AKV pair stays in private Docker volumes.
    $properties = @'
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="plain" password="plain-secret";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks
ssl.truststore.password=changeit
ssl.endpoint.identification.algorithm=https
request.timeout.ms=10000
default.api.timeout.ms=20000
'@
    $null = Invoke-Compose -Arguments @('exec', '-T', 'kafka', 'sh', '-c', 'umask 077; cat > /tmp/akv-client.properties') `
        -InputText $properties -Action 'synthetic-producer-properties'
    $null = Invoke-Compose -Arguments @('exec', '-T', 'kafka', 'kafka-topics', '--bootstrap-server', 'kafka:9093',
        '--command-config', '/tmp/akv-client.properties', '--create', '--topic', 'akv-sasl-logs',
        '--partitions', '1', '--replication-factor', '1') -Action 'create-topic'
    $null = Invoke-Compose -Arguments @('exec', '-T', 'kafka', 'kafka-console-producer', '--bootstrap-server', 'kafka:9093',
        '--producer.config', '/tmp/akv-client.properties', '--topic', 'akv-sasl-logs') `
        -InputText (($expectedBodies -join "`n") + "`n") -Action 'produce-three-records'
    $offsets = Invoke-Compose -Arguments @('exec', '-T', 'kafka', 'kafka-get-offsets', '--bootstrap-server', 'kafka:9093',
        '--command-config', '/tmp/akv-client.properties', '--topic', 'akv-sasl-logs') -Action 'broker-record-count'
    if ($offsets.Stdout.Trim() -ne 'akv-sasl-logs:0:3') { throw 'Broker does not contain exactly three fixture records.' }
    Test-Delivery
    Test-Rejection -Name 'missing-username' -Stage 'username' -Category 'not_found'
    Test-Rejection -Name 'missing-password' -Stage 'password' -Category 'not_found'
    Test-Rejection -Name 'timeout' -Category 'timeout'
    if ((Get-Content -Raw (Join-Path $runtime 'authorization-enabled')).Trim() -eq 'true') {
        $expectedCases++
        Test-Rejection -Name 'authorization' -Category 'authorization'
    } else {
        $results.Add(@{
            Case = 'authorization'; Outcome = 'NOT_RUN'
            Reason = 'No explicitly authorized known-denied vault supplied; no Azure changes or probing.'
        })
    }
    # Alter only the disposable broker's password; never write or replace an Azure secret.
    $null = Invoke-Compose -Arguments @('stop', '-t', '10', 'kafka') -Action 'stop-local-broker'
    $null = Invoke-Helper -Arguments @('reject-broker')
    $null = Invoke-Compose -Arguments @('up', '-d', '--no-deps', '--wait', '--wait-timeout', '180', 'kafka') `
        -Action 'restart-rejecting-broker' -TimeoutSeconds 200
    Test-Rejection -Name 'broker-rejection' -Broker
    if (@($results | Where-Object Outcome -eq 'PASS').Count -ne $expectedCases) {
        throw 'The required case matrix did not finish.'
    }
    $matrixPassed = $true
} finally {
    try {
        if ($brokerProvisioned) {
            $raw = Invoke-Compose -Arguments @('logs', '--no-color', '--no-log-prefix', 'kafka') `
                -Action 'read-private-broker-output' -TimeoutSeconds 20
            $request = ConvertTo-Json -InputObject @{
                logs = $raw.Stdout + $raw.Stderr; expected = $expectedBodies
            } -Compress
            $scan = Invoke-Helper -Arguments @('scan') -InputText $request -TimeoutSeconds 20
            $brokerOutput = ConvertFrom-Json -InputObject $scan.Stdout
            Write-Artifact 'broker-output.json' $brokerOutput
            if ($brokerOutput.LeakDetected) {
                throw 'Credential disclosure detected in broker output; raw output was not saved.'
            }
            $allPassed = $matrixPassed
        }
        if ($binaryContainer) {
            $null = Invoke-Docker -Arguments @('rm', '-f', $binaryContainer) -Action 'cleanup-extraction-container'
        }
    } finally {
        try {
            $null = Invoke-Compose -Arguments @('down', '--volumes', '--remove-orphans', '--timeout', '10') `
                -Action 'scoped-cleanup' -TimeoutSeconds 90
            foreach ($kind in @('container', 'network', 'volume')) {
                $arguments = if ($kind -eq 'container') { @('ps', '-aq') } else { @($kind, 'ls', '-q') }
                $remaining = Invoke-Docker -Arguments (
                    $arguments + @('--filter', "label=com.docker.compose.project=$runId")
                ) -Action "verify-$kind-cleanup" -TimeoutSeconds 15
                if ($remaining.Stdout.Trim()) { throw "Run-scoped $kind resources remain; cleanup is incomplete." }
            }
            if ($IdentityMode -eq 'development') {
                $null = Invoke-Docker -Arguments @('volume', 'inspect', $LoginVolume) `
                    -Action 'verify-login-preserved' -TimeoutSeconds 15
            }
            $cleanupSucceeded = $true
        } finally {
            # External login volume is intentionally preserved; never save raw broker/receiver logs.
            Write-Artifact 'results.json' @($results.ToArray())
            Write-Artifact 'commands.json' @($commands.ToArray())
            Write-Artifact 'summary.json' @{
                TestsSucceeded = $allPassed; CleanupSucceeded = $cleanupSucceeded
                ExpectedCases = $expectedCases; PassedCases = @($results | Where-Object Outcome -eq 'PASS').Count
                LastCase = $script:caseName; LastObservation = $script:lastSnapshot
                AuthorizationCovered = (@($results | Where-Object {
                    $_.Case -eq 'authorization' -and $_.Outcome -eq 'PASS'
                }).Count -eq 1)
            }
            foreach ($file in @('receiver.yaml', 'authorization-enabled')) {
                $path = Join-Path $runtime $file
                if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path }
            }
            $path = Join-Path $run 'df_engine'
            if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path }
            foreach ($name in $environmentNames) {
                [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name])
            }
            Write-Host "Sanitized observations only: $run"
        }
    }
}
