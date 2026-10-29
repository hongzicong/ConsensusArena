[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_.@:-]*$')]
    [string]$Server,
    [string]$RemoteBase = 'ConsensusArena',
    [ValidatePattern('^[A-Za-z0-9_-]+$')]
    [string]$SlurmAccount = 'dcl',
    [ValidateSet('full', 'no-cold', 'no-hot', 'neither')]
    [string[]]$Arms = @('full', 'no-cold', 'no-hot', 'neither'),
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

if ($Arms.Count -eq 0 -or @($Arms | Select-Object -Unique).Count -ne $Arms.Count) {
    throw 'Choose a non-empty list of distinct arms.'
}
$sceneCount = 0
foreach ($arm in $Arms) {
    if ($arm -eq 'no-cold') { $sceneCount += 2 } else { $sceneCount += 8 }
}

Push-Location $repoPath
try {
    Write-Host 'Validating shared KCensus settings before the Agreement ablation...'
    Invoke-Checked -Program python -Arguments @('-B', (Join-Path $slurmPath 'kcensus-suite/validate.py')) | Out-Host
    Write-Host 'Building the current branch as a frozen Linux executable...'
    $artifactText = Invoke-Checked -Program powershell -Arguments @(
        '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $slurmPath 'build-linux.ps1'))
    $artifact = ($artifactText -join "`n") | ConvertFrom-Json
    $proxy = (& (Join-Path $slurmPath 'fetch-toxiproxy.ps1')) | ConvertFrom-Json
    Write-Host "Binary SHA256: $($artifact.SHA256)"
    Write-Host "Proxy SHA256:  $($proxy.SHA256)"
    if ($PrepareOnly) {
        Write-Host "Prepared $sceneCount scenes locally. No SSH, upload or submission was performed."
        return
    }

    # Each invocation gets an immutable remote workspace: later uploads cannot
    # alter scripts or inputs used by jobs from an earlier launch.
    $launchId = 'agreement-ablation-n7-20krps-' + (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
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

    $protocolWords = ($Arms | ForEach-Object { ConvertTo-ShellLiteral $_ }) -join ' '
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
unset CONSENSUSARENA_RUN_DIR CONSENSUSARENA_CASE CONSENSUSARENA_CASE_SHA256 CONSENSUSARENA_CASES
unset CONSENSUSARENA_ARRIVAL_RATE CONSENSUSARENA_PLAN_SHA256 CONSENSUSARENA_GENERATED_ROOT
unset SBATCH_DEPENDENCY SBATCH_ARRAY_INX
for arm in __ARMS__; do
    profiles=A:B
    selection=
    count=8
    if [ "$arm" = no-cold ]; then
        profiles=A
        selection=,CONSENSUSARENA_CASES=7-1:7-4
        count=2
    fi
    printf '\nSubmitting Agreement %s: %s scenes\n' "$arm" "$count"
    python3 slurm/benchmark-suite.py submit latency --suite generated --generated-root "$root/slurm/agreement-ablation/$arm" \
        --account __ACCOUNT__ --max-parallel __PARALLEL__ --time 00:20:00 \
        --export "$common_exports,CONSENSUSARENA_PROTOCOL=agreement,CONSENSUSARENA_REPLICAS=7,CONSENSUSARENA_YCSB_PROFILES=$profiles,CONSENSUSARENA_WORKLOAD_SEED=1,CONSENSUSARENA_LATENCY_JITTER_MS=0,CONSENSUSARENA_BANDWIDTH_KBPS=125000$selection" | tee "submit-$arm.log"
done
printf '\nAll submissions accepted. Slurm runs the jobs and collectors independently of SSH.\n'
pwd -P
'@
    $submit = $submit.Replace('__BINARY_HASH__', [string]$artifact.SHA256).Replace('__PROXY_HASH__', [string]$proxy.SHA256)
    $submit = $submit.Replace('__ARMS__', $protocolWords).Replace('__ACCOUNT__', $quotedAccount)
    $submit = $submit.Replace('__PARALLEL__', [string]$MaxParallel)
    $submit = $submit.Replace("`r`n", "`n")
    # Upload UTF-8/LF script bytes rather than passing nested Bash expressions
    # through Windows' native command-line quoting. No SSH identity is stored.
    $stagingDirectory = Join-Path $repoPath 'results/launch-scripts'
    New-Item -ItemType Directory -Force -Path $stagingDirectory | Out-Null
    $stagingFile = Join-Path $stagingDirectory ($launchId + '.sh')
    [System.IO.File]::WriteAllText($stagingFile, $submit + "`n", (New-Object System.Text.UTF8Encoding($false)))
    Invoke-Checked -Program scp -Arguments @('-o', 'ConnectTimeout=30', $stagingFile, "${Server}:$remoteRoot/submit-all.sh") | Out-Host
    Write-Host "Submitting $sceneCount scenes; at most $MaxParallel active scene(s) per arm."
    Invoke-Checked -Program ssh -Arguments @('-o', 'ConnectTimeout=30', $Server, ('bash ' + (ConvertTo-ShellLiteral "$remoteRoot/submit-all.sh"))) | Out-Host
    Write-Host "Submission logs are retained in $remoteRoot/submit-*.log. You can now close PowerShell."
}
finally {
    Pop-Location
}
