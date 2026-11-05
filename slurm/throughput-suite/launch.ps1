[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_.@:-]*$')]
    [string]$Server,
    [string]$RemoteBase = 'ConsensusArena',
    [ValidatePattern('^[A-Za-z0-9_-]+$')]
    [string]$SlurmAccount = 'dcl',
    [ValidateRange(1, 6)]
    [int]$MaxParallel = 6,
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_.-]*\.toml$')]
    [string]$Spec = 'experiment.toml',
    [string]$FrozenBinary,
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedBinarySHA256,
    [switch]$PrepareOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$suitePath = $PSScriptRoot
$slurmPath = Split-Path -Parent $suitePath
$repoPath = Split-Path -Parent $slurmPath

function Invoke-Checked {
    param([string]$Program, [string[]]$Arguments)
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Program failed with exit code $LASTEXITCODE. Stop here; inspect any job IDs already printed before retrying."
    }
}

function ConvertTo-ShellLiteral {
    param([string]$Value)
    return "'" + $Value.Replace("'", "'\''") + "'"
}

Push-Location $repoPath
try {
    Write-Host "Validating ring-7 workload-B throughput specification: $Spec ..."
    $planText = Invoke-Checked -Program python -Arguments @('-B', (Join-Path $suitePath 'runner.py'), 'plan', '--spec', (Join-Path $suitePath $Spec))
    $plan = ($planText -join "`n") | ConvertFrom-Json
    $protocolCount = @($plan.spec.protocols).Count
    if ($FrozenBinary) {
        if (-not $ExpectedBinarySHA256) { throw 'Supply ExpectedBinarySHA256 when reusing a frozen executable.' }
        $binaryPath = (Resolve-Path -LiteralPath $FrozenBinary).Path
        $binaryHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $binaryPath).Hash.ToLowerInvariant()
        if ($binaryHash -ne $ExpectedBinarySHA256) { throw 'Frozen executable SHA256 mismatch.' }
        $artifact = [PSCustomObject]@{ FrozenPath = $binaryPath; SHA256 = $binaryHash }
        Write-Host 'Reusing the verified frozen executable from the earlier experiment.'
    }
    else {
        if ($ExpectedBinarySHA256) { throw 'ExpectedBinarySHA256 requires FrozenBinary.' }
        Write-Host 'Building the current branch as a frozen Linux executable...'
        $artifactText = Invoke-Checked -Program powershell -Arguments @(
            '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $slurmPath 'build-linux.ps1'))
        $artifact = ($artifactText -join "`n") | ConvertFrom-Json
    }
    $proxy = (& (Join-Path $slurmPath 'fetch-toxiproxy.ps1')) | ConvertFrom-Json
    Write-Host "Binary SHA256: $($artifact.SHA256)"
    Write-Host "Proxy SHA256:  $($proxy.SHA256)"
    if ($PrepareOnly) {
        Write-Host "Prepared $protocolCount protocol workers locally. No SSH, upload or submission was performed."
        return
    }

    # Each invocation gets an immutable remote workspace: later uploads cannot
    # alter scripts or inputs used by jobs from an earlier launch.
    $launchId = 'throughput-ring7-B-' + (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
    $remoteRelative = $RemoteBase.TrimEnd('/') + '/launches/' + $launchId
    $quotedRelative = ConvertTo-ShellLiteral $remoteRelative
    $setup = "set -eu; command -v sbatch >/dev/null; command -v python3 >/dev/null; command -v curl >/dev/null; command -v ping >/dev/null; mkdir -p $quotedRelative; cd $quotedRelative; pwd -P"
    $remoteOutput = Invoke-Checked -Program ssh -Arguments @('-o', 'ConnectTimeout=30', $Server, $setup)
    $remoteRoot = [string](@($remoteOutput)[-1])
    $remoteRoot = $remoteRoot.Trim()
    if ($remoteRoot -notmatch '^/[A-Za-z0-9_./-]+$') {
        throw "Unexpected remote workspace path: $remoteRoot"
    }
    $remoteBinary = "$remoteRoot/consensusarena-linux-amd64"
    $remoteProxy = "$remoteRoot/toxiproxy-server-linux-amd64"
    Write-Host "Uploading executable, proxy and all Slurm scripts to $remoteRoot ..."
    Invoke-Checked -Program scp -Arguments @('-o', 'ConnectTimeout=30', [string]$artifact.FrozenPath, "${Server}:$remoteBinary") | Out-Host
    Invoke-Checked -Program scp -Arguments @('-o', 'ConnectTimeout=30', [string]$proxy.Path, "${Server}:$remoteProxy") | Out-Host
    Invoke-Checked -Program scp -Arguments @('-o', 'ConnectTimeout=30', '-r', $slurmPath, "${Server}:$remoteRoot/") | Out-Host

    $quotedAccount = ConvertTo-ShellLiteral $SlurmAccount
    $submit = @'
set -euo pipefail
cd -- "$(dirname -- "$0")"
root=$(pwd -P)
binary="$root/consensusarena-linux-amd64"
proxy="$root/toxiproxy-server-linux-amd64"
chmod +x "$binary" "$proxy"
file "$binary"
printf '%s  %s\n' '__BINARY_HASH__' "$binary" '__PROXY_HASH__' "$proxy" | sha256sum -c -
export CONSENSUSARENA_BINARY="$binary" CONSENSUSARENA_BINARY_SHA256=__BINARY_HASH__
export CONSENSUSARENA_TOXIPROXY_SERVER="$proxy" CONSENSUSARENA_TOXIPROXY_SHA256=__PROXY_HASH__
unset CONSENSUSARENA_RUN_DIR
unset SBATCH_DEPENDENCY SBATCH_ARRAY_INX
python3 slurm/throughput-suite/runner.py submit --spec slurm/throughput-suite/__SPEC__ --account __ACCOUNT__ --max-parallel __PARALLEL__ | tee submit-throughput.log
printf '\nAll submissions accepted. Slurm runs the adaptive searches and collector independently of SSH.\n'
pwd -P
'@
    $submit = $submit.Replace('__BINARY_HASH__', [string]$artifact.SHA256).Replace('__PROXY_HASH__', [string]$proxy.SHA256)
    $submit = $submit.Replace('__ACCOUNT__', $quotedAccount)
    $submit = $submit.Replace('__PARALLEL__', [string]$MaxParallel)
    $submit = $submit.Replace('__SPEC__', $Spec)
    $submit = $submit.Replace("`r`n", "`n")
    # Upload UTF-8/LF script bytes rather than passing nested Bash expressions
    # through Windows' native command-line quoting. No SSH identity is stored.
    $stagingDirectory = Join-Path $repoPath 'results/launch-scripts'
    New-Item -ItemType Directory -Force -Path $stagingDirectory | Out-Null
    $stagingFile = Join-Path $stagingDirectory ($launchId + '.sh')
    [System.IO.File]::WriteAllText($stagingFile, $submit + "`n", (New-Object System.Text.UTF8Encoding($false)))
    Invoke-Checked -Program scp -Arguments @('-o', 'ConnectTimeout=30', $stagingFile, "${Server}:$remoteRoot/submit-all.sh") | Out-Host
    Write-Host "Submitting $protocolCount protocol workers; at most $MaxParallel algorithms run concurrently."
    Invoke-Checked -Program ssh -Arguments @('-o', 'ConnectTimeout=30', $Server, ('bash ' + (ConvertTo-ShellLiteral "$remoteRoot/submit-all.sh"))) | Out-Host
    Write-Host "Submission logs are retained in $remoteRoot/submit-throughput.log. You can now close PowerShell."
}
finally {
    Pop-Location
}
