[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_.@:-]*$')]
    [string]$Server,
    [string]$RemoteBase = 'ConsensusArena',
    [ValidatePattern('^[A-Za-z0-9_-]+$')]
    [string]$SlurmAccount = 'dcl',
    [ValidateSet('swiftpaxos', 'bodega', 'paxos', 'kcensus', 'agreement', 'pql', 'pqlfrontier')]
    [string[]]$Protocols = @('swiftpaxos', 'bodega', 'paxos', 'kcensus', 'agreement', 'pql'),
    [ValidateRange(1, 6)]
    [int]$MaxParallel = 1,
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

if ($Protocols.Count -eq 0 -or @($Protocols | Select-Object -Unique).Count -ne $Protocols.Count) {
    throw 'Choose a non-empty list of distinct protocols.'
}

Push-Location $repoPath
try {
    Write-Host 'Validating the four 7-replica / A:B:C / 20,000 requests/s inputs...'
    Invoke-Checked -Program python -Arguments @('-B', (Join-Path $suitePath 'validate.py')) | Out-Host
    Write-Host 'Building the current branch as a frozen Linux executable...'
    $artifactText = Invoke-Checked -Program powershell -Arguments @(
        '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $slurmPath 'build-linux.ps1'))
    $artifact = ($artifactText -join "`n") | ConvertFrom-Json
    $proxy = (& (Join-Path $slurmPath 'fetch-toxiproxy.ps1')) | ConvertFrom-Json
    Write-Host "Binary SHA256: $($artifact.SHA256)"
    Write-Host "Proxy SHA256:  $($proxy.SHA256)"
    if ($PrepareOnly) {
        Write-Host "Prepared $($Protocols.Count * 12) scenes locally. No SSH, upload or submission was performed."
        return
    }

    # Each invocation gets an immutable remote workspace: later uploads cannot
    # alter scripts or inputs used by jobs from an earlier launch.
    $launchId = 'kcensus-n7-20krps-' + (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
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

    $protocolWords = ($Protocols | ForEach-Object { ConvertTo-ShellLiteral $_ }) -join ' '
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
common_exports="ALL,CONSENSUSARENA_BINARY=$binary,CONSENSUSARENA_BINARY_SHA256=__BINARY_HASH__,CONSENSUSARENA_TOXIPROXY_SERVER=$proxy,CONSENSUSARENA_TOXIPROXY_SHA256=__PROXY_HASH__"
unset CONSENSUSARENA_RUN_DIR
unset SBATCH_DEPENDENCY SBATCH_ARRAY_INX
for protocol in __PROTOCOLS__; do
    printf '\nSubmitting %s: four deployments x A/B/C (12 scenes)\n' "$protocol"
    bash slurm/kcensus-suite/submit.sh "$protocol" --account __ACCOUNT__ --max-parallel __PARALLEL__ \
        --export "$common_exports,CONSENSUSARENA_PROTOCOL=$protocol" | tee "submit-$protocol.log"
done
printf '\nAll submissions accepted. Slurm runs the jobs and collectors independently of SSH.\n'
pwd -P
'@
    $submit = $submit.Replace('__BINARY_HASH__', [string]$artifact.SHA256).Replace('__PROXY_HASH__', [string]$proxy.SHA256)
    $submit = $submit.Replace('__PROTOCOLS__', $protocolWords).Replace('__ACCOUNT__', $quotedAccount)
    $submit = $submit.Replace('__PARALLEL__', [string]$MaxParallel)
    $submit = $submit.Replace("`r`n", "`n")
    # Upload UTF-8/LF script bytes rather than passing nested Bash expressions
    # through Windows' native command-line quoting. No SSH identity is stored.
    $stagingDirectory = Join-Path $repoPath 'results/launch-scripts'
    New-Item -ItemType Directory -Force -Path $stagingDirectory | Out-Null
    $stagingFile = Join-Path $stagingDirectory ($launchId + '.sh')
    [System.IO.File]::WriteAllText($stagingFile, $submit + "`n", (New-Object System.Text.UTF8Encoding($false)))
    Invoke-Checked -Program scp -Arguments @('-o', 'ConnectTimeout=30', $stagingFile, "${Server}:$remoteRoot/submit-all.sh") | Out-Host
    Write-Host "Submitting $($Protocols.Count * 12) scenes; at most $MaxParallel active scene(s) per protocol."
    Invoke-Checked -Program ssh -Arguments @('-o', 'ConnectTimeout=30', $Server, ('bash ' + (ConvertTo-ShellLiteral "$remoteRoot/submit-all.sh"))) | Out-Host
    Write-Host "Submission logs are retained in $remoteRoot/submit-*.log. You can now close PowerShell."
}
finally {
    Pop-Location
}
