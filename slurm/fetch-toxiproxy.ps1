[CmdletBinding()]
param(
    [string]$Output
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$version = "2.12.0"
$expectedSha256 = "556d891134a3c582dc1e1a3f7335fd55142e5965769855a00b944e13e48302fc"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if (-not $Output) {
    $Output = Join-Path $repoRoot "results\tools\toxiproxy\v$version\toxiproxy-server-linux-amd64"
}
$outputPath = [System.IO.Path]::GetFullPath($Output)
$outputDirectory = Split-Path -Parent $outputPath
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null

if (-not (Test-Path -LiteralPath $outputPath)) {
    $uri = "https://github.com/Shopify/toxiproxy/releases/download/v$version/toxiproxy-server-linux-amd64"
    Invoke-WebRequest -UseBasicParsing -Uri $uri -OutFile $outputPath
}

$actualSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $outputPath).Hash.ToLowerInvariant()
if ($actualSha256 -ne $expectedSha256) {
    throw "Toxiproxy SHA-256 mismatch: got $actualSha256, expected $expectedSha256"
}

[PSCustomObject]@{
    Version = $version
    Path = $outputPath
    SHA256 = $actualSha256
} | ConvertTo-Json -Compress
