#Requires -Version 7.0
<#
.SYNOPSIS
    Run Project Sentinel locally, end to end, in one command. Windows equivalent of run.sh.

.DESCRIPTION
    There are two halves, and only one of them is a server.

      Backend   the Spark pipeline: generate -> landing -> bronze -> silver -> gold,
                writing Delta tables under .\data. It runs, finishes and exits. There
                is deliberately no API service — the dashboard is fed by a static
                export, so nothing has to stay up for the page to work.

      Frontend  the Vite dashboard, reading JSON that `sentinel-web-export` wrote out
                of the Gold layer.

    The first run installs the Python environment, a project-local JDK 17 and the
    dashboard's dependencies, and takes a few minutes. Later runs skip whatever is
    already in place.

    This is not a translation of run.sh — it does the install work directly rather
    than shelling out to make, which Windows does not ship. Everything else behaves
    the same, including the flags.

.PARAMETER Scale
    Generator scale; 1.0 is ~200k transactions.

.PARAMETER Fresh
    Delete generated data and every layer, then rebuild from scratch.

.PARAMETER ServeOnly
    Skip the pipeline; re-export the existing Gold layer and serve.

.PARAMETER Build
    Production build served on 4173, instead of the dev server on 5173.

.PARAMETER Port
    Override the port.

.PARAMETER NoServe
    Run the pipeline and export, but do not start a server.

.EXAMPLE
    .\run.ps1
.EXAMPLE
    .\run.ps1 -Fresh -Scale 0.1
.EXAMPLE
    .\run.ps1 -ServeOnly
#>

[CmdletBinding()]
param(
    [double] $Scale = 1.0,
    [switch] $Fresh,
    [switch] $ServeOnly,
    [switch] $Build,
    [int]    $Port = 0,
    [switch] $NoServe
)

# Stop on the first error from a cmdlet. Native executables do not participate in
# this, so every external call below is followed by an explicit exit-code check —
# PowerShell will happily continue past a failed uv or npm otherwise.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

Set-Location -LiteralPath $PSScriptRoot

# PowerShell runs on Linux and macOS too, and this script must not. It installs a
# *Windows* JDK into JAVA_HOME, and on a machine that already has a working JDK there
# — every Linux user of run.sh — that replaces it with binaries the platform cannot
# execute. Found by doing exactly that during development.
if (-not $IsWindows) {
    Write-Host '[error] run.ps1 is the Windows script. On Linux and macOS use ./run.sh' -ForegroundColor Red
    exit 1
}

# ---------------------------------------------------------------- output

function Write-Step { param([string] $Text) Write-Host "`n> $Text" -ForegroundColor White }
function Write-Ok   { param([string] $Text) Write-Host "       [ok] $Text" -ForegroundColor Green }
function Write-Skip { param([string] $Text) Write-Host "       [--] $Text" -ForegroundColor DarkGray }
function Write-Warn { param([string] $Text) Write-Host "[warn] $Text" -ForegroundColor Yellow }
function Stop-WithError {
    param([string] $Text)
    Write-Host "[error] $Text" -ForegroundColor Red
    exit 1
}

function Test-Tool { param([string] $Name) [bool] (Get-Command $Name -ErrorAction SilentlyContinue) }

# Native commands do not trip $ErrorActionPreference, so their exit codes are checked
# by hand. Without this the script sails past a failed pipeline and serves stale data.
function Invoke-Checked {
    param([string] $What, [scriptblock] $Command)
    & $Command
    if ($LASTEXITCODE -ne 0) { Stop-WithError $What }
}

if ($Port -eq 0) { $Port = if ($Build) { 4173 } else { 5173 } }

# Spark 3.5 supports JDK 8/11/17 only. A project-local JDK keeps whatever Java is
# already on this machine untouched.
$JdkHome = if ($env:JAVA_HOME) { $env:JAVA_HOME } else { Join-Path $HOME '.local/jdks/jdk-17' }
$env:JAVA_HOME = $JdkHome
$env:SENTINEL_ENV = 'local'

# ---------------------------------------------------------------- 1. prerequisites
#
# Everything is checked before anything is installed, so a machine without node is
# told so immediately rather than after several minutes of building a Python
# environment and a JDK it will then be asked to wait for again.

Write-Step 'Prerequisites'

$needVenv = -not (Test-Path '.venv')
$needJdk  = -not (Test-Path "$JdkHome/bin/java.exe")
$needWeb  = -not $NoServe
$needNpm  = $needWeb -and -not (Test-Path 'dashboards/web/node_modules')

$missing = [System.Collections.Generic.List[string]]::new()
if (-not (Test-Tool 'uv')) {
    $missing.Add('uv   - the Python package manager.  winget install astral-sh.uv')
}
if ($needWeb -and -not (Test-Tool 'npm')) {
    $missing.Add('npm  - for the dashboard.  winget install OpenJS.NodeJS  (or use -NoServe)')
}

if ($missing.Count -gt 0) {
    Write-Warn 'Missing tools this run needs:'
    $missing | ForEach-Object { Write-Host "         $_" }
    Stop-WithError 'Install the above, then re-run.'
}

# No make, curl or tar in the list, unlike run.sh: this script does the install work
# itself and uses .NET for the download and the unzip, so Windows needs nothing beyond
# uv and npm.
Write-Ok ('uv' + $(if ($needWeb) { ', npm' } else { '' }))

if ($needVenv -or $needJdk -or $needNpm) {
    $parts = @()
    if ($needVenv) { $parts += 'the Python environment' }
    if ($needJdk)  { $parts += 'a project-local JDK 17' }
    if ($needNpm)  { $parts += 'dashboard dependencies' }
    Write-Host "       first run: installing $($parts -join ', ')"
    Write-Host '       this takes a few minutes'
}

# ---------------------------------------------------------------- 2. toolchain

Write-Step 'Toolchain'

if ($needVenv) {
    Invoke-Checked 'uv venv failed' { uv venv --python 3.11 }
    Invoke-Checked 'dependency install failed' { uv pip install -e '.[local,generate,dev]' --quiet }
    Write-Ok 'python environment created'
} else {
    Write-Skip 'python environment present'
}

if ($needJdk) {
    # The Windows build is a .zip, not the .tar.gz run.sh fetches, and the API path
    # says windows/x64 rather than linux/x64. Downloading the Linux archive here is
    # the one mistake that *looks* like it worked: it unpacks, and then Spark cannot
    # start because there is no java.exe in it.
    $url = 'https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse?project=jdk'
    $zip = Join-Path ([System.IO.Path]::GetTempPath()) 'temurin17.zip'
    $staging = Join-Path ([System.IO.Path]::GetTempPath()) 'temurin17-extract'

    Write-Host '       downloading Temurin JDK 17...'
    try {
        # Invoke-WebRequest's progress bar makes this several times slower in some
        # hosts; the download is ~180 MB, so it is worth turning off.
        $previous = $ProgressPreference
        $ProgressPreference = 'SilentlyContinue'
        Invoke-WebRequest -Uri $url -OutFile $zip -MaximumRedirection 5
        $ProgressPreference = $previous

        if (Test-Path $staging) { Remove-Item $staging -Recurse -Force }
        Expand-Archive -LiteralPath $zip -DestinationPath $staging -Force

        # The archive holds a single jdk-17.x.y+z directory; the JDK is one level in.
        $inner = Get-ChildItem -LiteralPath $staging -Directory | Select-Object -First 1
        if (-not $inner) { Stop-WithError 'The JDK archive did not contain what was expected.' }

        New-Item -ItemType Directory -Force -Path (Split-Path $JdkHome -Parent) | Out-Null

        # Refuse to delete a directory somebody else owns. JAVA_HOME may point at a
        # system JDK, and "install a JDK" must never mean "replace whatever is there".
        # Only a directory this script created — or an empty one — is fair game.
        if (Test-Path $JdkHome) {
            $existing = @(Get-ChildItem -LiteralPath $JdkHome -Force -ErrorAction SilentlyContinue)
            if ($existing.Count -gt 0) {
                Stop-WithError "$JdkHome already exists but has no java.exe. Delete it yourself and re-run, or point JAVA_HOME at a working JDK 17."
            }
            Remove-Item $JdkHome -Recurse -Force
        }
        Move-Item -LiteralPath $inner.FullName -Destination $JdkHome
    } catch {
        Stop-WithError "Could not install a JDK 17: $($_.Exception.Message)"
    } finally {
        Remove-Item $zip, $staging -Recurse -Force -ErrorAction SilentlyContinue
    }
    Write-Ok 'JDK 17 installed'
}

if (-not (Test-Path "$JdkHome/bin/java.exe")) {
    Stop-WithError "No java.exe under $JdkHome. Delete that directory and re-run to reinstall."
}
Write-Ok "JDK 17 at $JdkHome"

if ($needNpm) {
    Push-Location 'dashboards/web'
    try {
        Invoke-Checked 'npm install failed' { npm install --no-audit --no-fund --silent }
    } finally { Pop-Location }
    Write-Ok 'dashboard dependencies installed'
} elseif ($needWeb) {
    Write-Skip 'dashboard dependencies present'
}

# One Spark start-up costs ~10s, so this is not free — but a JDK/Delta mismatch
# otherwise surfaces as an opaque JVM crash inside a streaming query, minutes later,
# with the real cause nowhere in the traceback.
if (-not $ServeOnly) {
    uv run python -m sentinel.doctor 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Stop-WithError 'Spark cannot start. Run `uv run python -m sentinel.doctor` to see the failure.'
    }
    Write-Ok 'Spark + Delta verified'
}

# ---------------------------------------------------------------- 3. backend

$gold = 'data/gold/fact_txn_scored'

if ($Fresh) {
    Write-Step 'Clean'
    # Mirrors the Makefile's clean-all target. Spelled out because there is no make.
    foreach ($path in @('data', 'build', 'dist', '.pytest_cache', '.ruff_cache', '.mypy_cache')) {
        if (Test-Path $path) { Remove-Item $path -Recurse -Force }
    }
    Write-Ok 'generated data and every layer removed'
}

if ($ServeOnly) {
    Write-Step 'Pipeline'
    if (-not (Test-Path $gold)) {
        Stop-WithError '-ServeOnly, but there is no Gold layer yet. Run without it first.'
    }
    Write-Skip 'skipped - serving the existing Gold layer'
} elseif ((Test-Path $gold) -and -not $Fresh) {
    Write-Step 'Pipeline'
    Write-Skip 'Gold layer already present - use -Fresh to rebuild it'
} else {
    Write-Step 'Generate'
    # Not silenced: the injected-defect counts it prints are the point of the generator.
    Invoke-Checked 'generation failed' { uv run sentinel-gen --scale $Scale }

    Write-Step 'Pipeline'
    Invoke-Checked 'the pipeline failed' { uv run sentinel-run all }
}

# ---------------------------------------------------------------- 4. export

Write-Step 'Export'
uv run sentinel-web-export 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { Stop-WithError 'export failed - is there a Gold layer?' }
Write-Ok 'Gold layer exported to dashboards/web/public/data'

if ($NoServe) {
    Write-Step 'Done'
    Write-Host '       Pipeline and export complete. Start the dashboard with:  .\run.ps1 -ServeOnly'
    exit 0
}

# ---------------------------------------------------------------- 5. frontend

Write-Step 'Dashboard'

# A stale server on the port would leave the browser showing the previous run's data
# while this one reports success. Get-NetTCPConnection is the Windows equivalent of
# the `ss` check in run.sh, and is absent on PowerShell for Linux/macOS — hence the
# guard, which degrades to skipping the check rather than failing.
if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
    $inUse = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    if ($inUse) { Stop-WithError "Port $Port is already in use. Stop that server, or pass -Port." }
}

if ($Build) {
    Push-Location 'dashboards/web'
    try {
        Invoke-Checked 'the production build failed' { npm run build --silent }
    } finally { Pop-Location }
    Write-Ok 'production build'
    Write-Host "`n       http://localhost:$Port/   (Ctrl-C to stop)`n" -ForegroundColor White
    npm --prefix dashboards/web run preview -- --port $Port
} else {
    Write-Host "`n       http://localhost:$Port/   (hot reload; Ctrl-C to stop)`n" -ForegroundColor White
    npm --prefix dashboards/web run dev -- --port $Port
}
