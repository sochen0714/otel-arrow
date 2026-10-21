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
foreach ($name in @('Test-Rejection', 'Start-Case')) {
    $definition = $ast.Find({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
    }, $true)
    if (-not $definition) { throw 'Required driver function is missing.' }
    Set-Variable -Name ($name.Replace('-', '') + 'Definition') -Value $definition.Extent.Text
}
. ([scriptblock]::Create($TestRejectionDefinition))
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
[Console]::WriteLine('POWERSHELL_CASE_ORACLES_OK: 12 offline scenarios; not live acceptance.')
