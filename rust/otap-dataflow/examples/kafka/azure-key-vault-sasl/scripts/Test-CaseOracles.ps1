#requires -Version 7.0
# Offline fixtures only; no Docker, Azure, runtime inputs, or real E2E evidence.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$tokens = $null
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $PSScriptRoot 'Test-AzureKeyVaultSasl.ps1'), [ref]$tokens, [ref]$errors
)
if ($errors.Count) { throw 'Driver syntax is invalid.' }
foreach ($name in @('Test-Rejection', 'Start-Case', 'Get-Snapshot', 'Test-Delivery', 'Stop-Receiver', 'Test-StartupFailure')) {
    $definition = $ast.Find({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
    }, $true)
    if (-not $definition) { throw 'Required driver function is missing.' }
    Set-Variable -Name ($name.Replace('-', '') + 'Definition') -Value $definition.Extent.Text
}
. ([scriptblock]::Create($TestRejectionDefinition))
. ([scriptblock]::Create($TestStartupFailureDefinition))
function Start-Case { param($Name) 'fixture-container' }
function Stop-Receiver {}
function Start-Sleep { param($Milliseconds, $Seconds) }
function Write-Host { param($Object) }
function Get-Snapshot { param($Container) $script:snapshot }

# Scenario: Acquisition results vary in classification, readiness, liveness, and exit status.
# Guarantees: Only the expected stage/category and gate plus an actual nonzero terminal exit pass.
function Test-AcquisitionOracle {
    param(
        [string] $Name, [string] $ObservedStage, [string] $ObservedCategory,
        [string] $Readiness, [bool] $Running, [int] $ExitCode, [bool] $Expected
    )
    $script:snapshot = [pscustomobject]@{
        Running = $Running; ExitCode = $ExitCode
        Output = [pscustomobject]@{
            OutputCount = 0
            StartupFailures = @(
                if ($ObservedCategory) { @{ Stage = $ObservedStage; Category = $ObservedCategory } }
            )
            ReadinessFailures = @(
                if ($Readiness) { @{ Extension = 'credentials'; Variant = 'shared'; Timeout = $Readiness } }
            )
        }
    }
    $results = [Collections.Generic.List[object]]::new()
    $CaseTimeoutSeconds = 0
    $stage = if ($Name -eq 'missing-username') { 'username' } else { $null }
    $category = if ($Name -eq 'timeout') { 'timeout' } else { 'not_found' }
    $accepted = $true
    try {
        Test-Rejection -Name $Name -Stage $stage -Category $category
    } catch {
        if ($_.Exception.Message -notmatch '^(Startup failure was not|Expected bounded credential failure)') {
            throw
        }
        $accepted = $false
    }
    if ($accepted -ne $Expected -or $results.Count -ne [int]$Expected) {
        throw 'Acquisition oracle accepted or rejected the wrong fixture.'
    }
}

Test-AcquisitionOracle missing-username username not_found 15s $false 1 $true
Test-AcquisitionOracle missing-username username not_found 15s $true 0 $false
Test-AcquisitionOracle missing-username password not_found 15s $false 1 $false
Test-AcquisitionOracle missing-username username authentication 15s $false 1 $false
Test-AcquisitionOracle missing-username '' '' 15s $false 1 $false
Test-AcquisitionOracle missing-username username not_found '' $false 1 $false
Test-AcquisitionOracle missing-username username not_found 1ms $false 1 $false
Test-AcquisitionOracle timeout '' '' 1ms $false 1 $true
Test-AcquisitionOracle timeout acquisition timeout '' $false 1 $false
Test-AcquisitionOracle timeout '' '' 15s $false 1 $false
Test-AcquisitionOracle timeout '' '' 1ms $false 0 $false

# Scenario: Compose returns the previous case's ID despite a requested force-recreate.
# Guarantees: The driver refuses a reused container rather than accepting stale credentials or output.
function Test-FreshContainer {
    . ([scriptblock]::Create($StartCaseDefinition))
    function Invoke-Helper { param($Arguments) }
    function Invoke-Compose { param($Arguments, $Action, $TimeoutSeconds) @{ Stdout = 'a' * 64 } }
    $runId = 'akv-sasl-0123456789ab'
    $script:lastContainer = $null
    $null = Start-Case configured
    $rejected = $false
    try { $null = Start-Case missing-username } catch {
        if ($_.Exception.Message -ne 'Each case requires a newly created receiver container.') { throw }
        $rejected = $true
    }
    if (-not $rejected) { throw 'A reused container was accepted.' }
}
Test-FreshContainer

# Scenario: The receiver exits during scanning, or changes its final output/state during stop.
# Guarantees: Actual driver functions accept coherent terminal evidence only and retain every strict oracle.
function Test-SnapshotLifecycle {
    param([string] $Mode, [bool] $Expected)
    foreach ($definition in @(
        $GetSnapshotDefinition, $TestDeliveryDefinition, $TestRejectionDefinition, $StopReceiverDefinition,
        $TestStartupFailureDefinition
    )) {
        . ([scriptblock]::Create($definition))
    }
    $script:fixtureMode = $Mode
    $script:fixturePositive = $Mode.StartsWith('positive')
    $script:fixtureBroker = $Mode.StartsWith('broker')
    $script:fixtureRunning = $script:fixturePositive -or $script:fixtureBroker -or
        $Mode -in @('negative-race', 'negative-running', 'negative-initial-leak')
    $script:fixtureExit = if ($script:fixtureRunning -or $Mode -eq 'negative-zero-exit') { 0 } else { 1 }
    $script:fixtureExitDuringScan = $Mode -in @('negative-race', 'positive-race', 'negative-initial-leak')
    $script:fixtureStopped = $false
    $script:fixtureReads = 0
    $script:fixtureReadsBeforeStop = 0
    $script:caseName = if ($script:fixturePositive) { 'configured' } else { 'missing-password' }
    $script:lastSnapshot = $null
    $CaseTimeoutSeconds = 0
    $expectedBodies = @('fixture-record-1', 'fixture-record-2', 'fixture-record-3')
    $results = [Collections.Generic.List[object]]::new()

    function Start-Case { param($Name) $script:caseName = $Name; 'fixture-container' }
    function Write-Artifact { param($Name, $Value) }
    function Invoke-Docker {
        param($Arguments, $Action, $TimeoutSeconds)
        if ($Action -eq 'read-private-receiver-output') {
            $script:fixtureReads++
            if ($script:fixtureMode -eq 'negative-capture-error' -and $script:fixtureReads -eq 2) {
                throw 'Fixture terminal capture failed.'
            }
            return @{ Stdout = $(if ($script:fixtureRunning) { 'running' } else { 'terminal' }); Stderr = '' }
        }
        if ($Action -ne 'receiver-process-state') { throw 'Unexpected fixture Docker action.' }
        @{ Stdout = (ConvertTo-Json -Compress @{
            Running = $script:fixtureRunning; ExitCode = $script:fixtureExit
            OOMKilled = ($script:fixtureMode -eq 'negative-oom')
        }) }
    }
    function Invoke-Helper {
        param($Arguments, $InputText, $TimeoutSeconds)
        $request = ConvertFrom-Json $InputText
        $terminal = $request.logs -eq 'terminal'
        if ($script:fixtureExitDuringScan) {
            $script:fixtureRunning = $false
            $script:fixtureExit = 1
            $script:fixtureExitDuringScan = $false
        }
        if ($script:fixtureMode -eq 'negative-restart-during-capture' -and $script:fixtureReads -eq 2) {
            $script:fixtureRunning = $true
            $script:fixtureExit = 0
        }
        $mode = $script:fixtureMode
        $final = $script:fixtureStopped
        $extra = $mode -eq 'negative-output' -or
            ($final -and $mode -in @('positive-extra', 'negative-final-output', 'broker-final-output'))
        $count = if ($script:fixturePositive) { 3 + [int]$extra } else { [int]$extra }
        $stage = if ($mode -eq 'negative-wrong-stage') { 'username' } else { 'password' }
        $category = if ($mode -eq 'negative-wrong-category' -or
            ($final -and $mode -eq 'negative-final-category')) { 'authentication' } else { 'not_found' }
        $readiness = $terminal -and $mode -ne 'negative-missing-readiness' -and
            -not ($final -and $mode -eq 'negative-final-gate')
        @{ Stdout = (ConvertTo-Json -Depth 6 -Compress @{
            LeakDetected = ($mode -eq 'negative-leak' -or
                ($mode -eq 'negative-initial-leak' -and -not $terminal) -or
                ($final -and $mode -in @('positive-leak', 'negative-final-leak')))
            OutputCount = $count
            ExactBodies = ($script:fixturePositive -and $count -eq 3)
            StartupFailures = @(
                if (-not $script:fixturePositive -and -not $script:fixtureBroker) {
                    @{ Stage = $stage; Category = $category }
                    @{ Stage = $stage; Category = $category }
                }
            )
            ReadinessFailures = @(
                if ($readiness) { @{ Extension = 'credentials'; Variant = 'shared'; Timeout = '15s' } }
            )
            BrokerAuthentication = ($script:fixtureBroker -and
                -not ($final -and $mode -eq 'broker-final-classification'))
        }) }
    }
    function Invoke-Compose {
        param($Arguments, $Action, $TimeoutSeconds)
        if ($Action -ne 'stop-receiver') { throw 'Unexpected fixture Compose action.' }
        $script:fixtureReadsBeforeStop = $script:fixtureReads
        $script:fixtureStopped = $true
        $script:fixtureRunning = $script:fixtureMode -in @(
            'positive-running', 'negative-final-running', 'broker-final-running'
        )
        if ($script:fixturePositive -or $script:fixtureBroker -or $script:fixtureMode -eq 'negative-final-exit') {
            $script:fixtureExit = 0
        }
    }

    $accepted = $true
    try {
        if ($script:fixturePositive) {
            Test-Delivery
        } else {
            Test-Rejection -Name 'missing-password' -Stage 'password' -Category 'not_found' -Broker:$script:fixtureBroker
        }
    } catch {
        if ($_.Exception.Message -notmatch '^(Startup failure was not|Expected bounded credential failure|A negative case unexpectedly|Receiver exited unexpectedly|Receiver restarted|Final output|Receiver did not stop|Telemetry appeared|Credential disclosure detected|Receiver ran out of memory|Final negative|Fixture terminal capture failed)') {
            throw
        }
        $accepted = $false
    }
    if ($accepted -ne $Expected -or $results.Count -ne [int]$Expected) {
        throw "Snapshot lifecycle fixture failed: $Mode (accepted=$accepted)."
    }
    if ($Mode -eq 'negative-race' -and $script:fixtureReadsBeforeStop -lt 2) {
        throw 'Terminal failure was accepted without a post-exit log capture.'
    }
}

Test-SnapshotLifecycle negative-race $true
Test-SnapshotLifecycle negative-running $false
Test-SnapshotLifecycle negative-missing-readiness $false
Test-SnapshotLifecycle negative-wrong-stage $false
Test-SnapshotLifecycle negative-wrong-category $false
Test-SnapshotLifecycle negative-zero-exit $false
Test-SnapshotLifecycle negative-leak $false
Test-SnapshotLifecycle negative-initial-leak $false
Test-SnapshotLifecycle negative-final-leak $false
Test-SnapshotLifecycle negative-oom $false
Test-SnapshotLifecycle negative-output $false
Test-SnapshotLifecycle negative-final-gate $false
Test-SnapshotLifecycle negative-final-category $false
Test-SnapshotLifecycle negative-final-running $false
Test-SnapshotLifecycle negative-final-exit $false
Test-SnapshotLifecycle negative-final-output $false
Test-SnapshotLifecycle negative-capture-error $false
Test-SnapshotLifecycle negative-restart-during-capture $false
Test-SnapshotLifecycle positive $true
Test-SnapshotLifecycle positive-race $false
Test-SnapshotLifecycle positive-extra $false
Test-SnapshotLifecycle positive-leak $false
Test-SnapshotLifecycle positive-running $false
Test-SnapshotLifecycle broker $true
Test-SnapshotLifecycle broker-final-output $false
Test-SnapshotLifecycle broker-final-classification $false
Test-SnapshotLifecycle broker-final-running $false
[Console]::WriteLine('POWERSHELL_CASE_ORACLES_OK: 39 offline scenarios; not live acceptance.')
