#Requires -Version 7.0
<#
.SYNOPSIS
    Deploy Project Sentinel to a Databricks workspace. Windows equivalent of deploy.sh.

.DESCRIPTION
    What it does, in order:

      1. preflight   - CLI present, authenticated, uv available
      2. compute     - work out which cloud this workspace is on, and pick a node type
      3. wheel       - build it, and verify conf/ is inside before uploading anything
      4. catalog     - create the catalog, schemas and volumes if they are missing
      5. publish     - upload the wheel and the notebooks
      6. job         - create it, or update it in place if it already exists
      7. run         - optional

    Every step is idempotent: running this twice does not produce two jobs, two
    volumes, or two copies of anything.

    NOT YET RUN AGAINST A LIVE WORKSPACE. Every CLI invocation was checked against
    `databricks --help` for v1.11.0 and -DryRun exercises the whole script except the
    remote calls, but no cluster has ever executed this. Start with -DryRun.

.PARAMETER Profile
    Databricks CLI profile. Defaults to $env:DATABRICKS_PROFILE, else the CLI default.

.PARAMETER Catalog
    Unity Catalog to deploy into.

.PARAMETER Serverless
    Omit job_clusters entirely and let the workspace supply serverless compute.
    Required on Free Edition, which has no all-purpose clusters.

.PARAMETER DryRun
    Render and validate everything without calling the workspace.

.EXAMPLE
    .\databricks\deploy.ps1 -DryRun
.EXAMPLE
    .\databricks\deploy.ps1 -Serverless -Run
#>

[CmdletBinding()]
param(
    [string] $Profile      = $env:DATABRICKS_PROFILE,
    [string] $Catalog      = 'sentinel',
    [string] $JobName      = 'sentinel_pipeline',
    [string] $NotebookDir  = '/Shared/sentinel/notebooks',
    [string] $NodeType     = '',
    [int]    $NumWorkers   = 2,
    [string] $SparkVersion = '15.4.x-scala2.12',
    [string] $Scale        = '1.0',
    [string] $Out          = '',
    [switch] $Serverless,
    [switch] $Run,
    [switch] $DryRun
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# The repo root is one level above this script.
Set-Location -LiteralPath (Split-Path $PSScriptRoot -Parent)

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

# Schemas the pipeline writes to, and the volumes it needs. These must match
# conf/databricks.yaml; tests/unit/test_deploy.py asserts that they do.
$Schemas = @('raw', 'landing', 'bronze', 'silver', 'gold', 'quarantine')
$Volumes = @('upi_drop', 'checkpoints', 'truth', 'autoloader_schema', 'libs')

# Every workspace call goes through here, so -Profile is honoured in one place and
# -DryRun can intercept everything without each call site remembering to check.
function Invoke-Databricks {
    param([Parameter(ValueFromRemainingArguments = $true)] [string[]] $Arguments)

    if ($DryRun) {
        Write-Host "       would run: databricks $($Arguments -join ' ')" -ForegroundColor DarkGray
        $script:LastDatabricksExit = 0
        return ''
    }

    $full = if ($Profile) { @('--profile', $Profile) + $Arguments } else { $Arguments }
    $output = & databricks @full 2>&1
    $script:LastDatabricksExit = $LASTEXITCODE
    return ($output | Out-String)
}

# ---------------------------------------------------------------- 1. preflight

Write-Step 'Preflight'

foreach ($tool in @('databricks', 'uv', 'python3')) {
    # python is called `python` on Windows more often than `python3`; accept either.
    $names = if ($tool -eq 'python3') { @('python3', 'python') } else { @($tool) }
    $found = $names | Where-Object { Get-Command $_ -ErrorAction SilentlyContinue } | Select-Object -First 1
    if (-not $found) {
        Stop-WithError "$tool not found. databricks: winget install Databricks.DatabricksCLI  |  uv: winget install astral-sh.uv"
    }
    if ($tool -eq 'python3') { $script:Python = $found }
}
Write-Ok "databricks, uv, $script:Python"

if (-not $DryRun) {
    # A clear failure here beats an opaque 401 six steps later, after the wheel has
    # already been built.
    $me = Invoke-Databricks 'current-user' 'me' '-o' 'json'
    if ($script:LastDatabricksExit -ne 0) {
        Stop-WithError 'Not authenticated. Run:  databricks auth login --host https://<workspace-host>'
    }
    $user = try { ($me | ConvertFrom-Json).userName } catch { '?' }
    Write-Ok "authenticated as $user"
} else {
    Write-Skip 'dry run - skipping the authentication check'
}

# ---------------------------------------------------------------- 2. compute

Write-Step 'Compute'

# The workspace hostname says which cloud it is on, and node type ids are entirely
# cloud-specific — an Azure id on AWS fails at cluster start, several minutes into the
# first run, with an error that does not mention the cloud.
$hostUrl = $env:DATABRICKS_HOST
if (-not $hostUrl -and -not $DryRun) {
    $envJson = Invoke-Databricks 'auth' 'env'
    if ($script:LastDatabricksExit -eq 0) {
        $hostUrl = try { ($envJson | ConvertFrom-Json).env.DATABRICKS_HOST } catch { '' }
    }
}

$cloud, $defaultNode = switch -Wildcard ($hostUrl) {
    '*azuredatabricks.net*' { 'azure', 'Standard_DS3_v2'; break }
    '*gcp.databricks.com*'  { 'gcp',   'n2-highmem-4';    break }
    '*databricks.com*'      { 'aws',   'm5d.large';       break }
    default                 { 'unknown', 'Standard_DS3_v2' }
}

if ($Serverless) {
    # Never read in this mode — the job_clusters block is removed entirely — but the
    # renderer substitutes unconditionally, so it must not be empty.
    $NodeType = 'n/a'
    Write-Ok 'serverless - no job clusters will be declared'
} else {
    if (-not $NodeType) { $NodeType = $defaultNode }
    if ($cloud -eq 'unknown') {
        Write-Warn "Could not determine the workspace's cloud from the host."
        Write-Warn "Defaulting node_type_id to $NodeType, which is an Azure id."
        Write-Warn 'On AWS or GCP pass -NodeType, or use -Serverless.'
    } else {
        Write-Ok "$cloud workspace -> node_type_id $NodeType"
    }
    Write-Ok "runtime $SparkVersion, $NumWorkers workers"
}

# ---------------------------------------------------------------- 3. wheel

Write-Step 'Wheel'

# Config lives inside the wheel (see the force-include in pyproject.toml), so there is
# no second deployment step for it and no way for the two to drift apart. A wheel
# missing conf/ installs cleanly and then fails on the first config read — minutes
# into a cluster run — so it is checked here instead.
if (Test-Path 'dist') { Remove-Item 'dist' -Recurse -Force }
uv build --wheel *> $null
if ($LASTEXITCODE -ne 0) { Stop-WithError 'uv build failed' }

$wheel = Get-ChildItem 'dist/sentinel-*.whl' | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $wheel) { Stop-WithError 'No wheel produced in dist/' }

Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [System.IO.Compression.ZipFile]::OpenRead($wheel.FullName)
try {
    $names = $archive.Entries.FullName
    foreach ($required in @('sentinel/conf/base.yaml', 'sentinel/conf/databricks.yaml')) {
        if ($names -notcontains $required) {
            Stop-WithError "Wheel is missing $required - check the force-include in pyproject.toml"
        }
    }
} finally { $archive.Dispose() }
Write-Ok "$($wheel.Name) (conf/ included)"

$volumeRoot = "/Volumes/$Catalog/raw"
$wheelPath  = "$volumeRoot/libs/$($wheel.Name)"

# conf/databricks.yaml is what the notebooks read at runtime. If it points somewhere
# other than the catalog being deployed to, the job runs and writes to the wrong
# place — so the mismatch is caught here rather than discovered in the data.
$configured = (Select-String -Path 'conf/databricks.yaml' -Pattern '^catalog:\s*"?([A-Za-z0-9_]+)"?' `
    | Select-Object -First 1).Matches[0].Groups[1].Value
if ($configured -ne $Catalog) {
    Stop-WithError "conf/databricks.yaml has catalog: '$configured', deploying to '$Catalog'. Pass -Catalog $configured or edit the config."
}
Write-Ok "conf/databricks.yaml targets $Catalog"

# ---------------------------------------------------------------- 4. catalog

Write-Step 'Unity Catalog'

# `create` on something that exists is an error, not a no-op, so each object is probed
# first. Failures here are warnings rather than fatal: on a workspace where the catalog
# is pre-provisioned by an admin, the deployer legitimately lacks CREATE CATALOG and
# the objects are already there.
if ($DryRun) {
    Write-Skip "dry run - would create catalog $Catalog, schemas ($($Schemas -join ' ')), volumes ($($Volumes -join ' '))"
} else {
    Invoke-Databricks 'catalogs' 'get' $Catalog | Out-Null
    if ($script:LastDatabricksExit -eq 0) {
        Write-Skip "catalog $Catalog exists"
    } else {
        Invoke-Databricks 'catalogs' 'create' $Catalog | Out-Null
        if ($script:LastDatabricksExit -eq 0) { Write-Ok "created catalog $Catalog" }
        else { Write-Warn "could not create catalog $Catalog - assuming it exists and continuing" }
    }

    foreach ($schema in $Schemas) {
        Invoke-Databricks 'schemas' 'get' "$Catalog.$schema" | Out-Null
        if ($script:LastDatabricksExit -eq 0) { Write-Skip "schema $schema"; continue }
        Invoke-Databricks 'schemas' 'create' $schema $Catalog | Out-Null
        if ($script:LastDatabricksExit -eq 0) { Write-Ok "created schema $schema" }
        else { Write-Warn "could not create schema $Catalog.$schema" }
    }

    foreach ($volume in $Volumes) {
        Invoke-Databricks 'volumes' 'read' "$Catalog.raw.$volume" | Out-Null
        if ($script:LastDatabricksExit -eq 0) { Write-Skip "volume raw.$volume"; continue }
        Invoke-Databricks 'volumes' 'create' $Catalog 'raw' $volume 'MANAGED' | Out-Null
        if ($script:LastDatabricksExit -eq 0) { Write-Ok "created volume raw.$volume" }
        else { Write-Warn "could not create volume $Catalog.raw.$volume" }
    }
}

# ---------------------------------------------------------------- 5. publish

Write-Step 'Publish'

# A Unity Catalog volume rather than DBFS root: DBFS root is deprecated, and on a
# UC-enabled workspace installing libraries from it is restricted by cluster access
# mode — which surfaces as a library-install failure at cluster start, not here.
Invoke-Databricks 'fs' 'cp' '--overwrite' $wheel.FullName "dbfs:$wheelPath" | Out-Null
if ($script:LastDatabricksExit -eq 0) { Write-Ok "wheel -> $wheelPath" }
elseif (-not $DryRun) { Stop-WithError "Failed to upload the wheel to $wheelPath" }

Invoke-Databricks 'workspace' 'mkdirs' $NotebookDir | Out-Null
foreach ($notebook in Get-ChildItem 'notebooks/*.py') {
    $name = $notebook.BaseName
    Invoke-Databricks 'workspace' 'import' "$NotebookDir/$name" `
        '--file' $notebook.FullName '--language' 'PYTHON' '--format' 'SOURCE' '--overwrite' | Out-Null
    if ($script:LastDatabricksExit -eq 0) { Write-Ok "notebook $name" }
    elseif (-not $DryRun) { Write-Warn "failed to import $name" }
}

# ---------------------------------------------------------------- 6. job

Write-Step 'Job'

$rendered = Join-Path ([System.IO.Path]::GetTempPath()) "sentinel-job-$([guid]::NewGuid().ToString('N')).json"

# The template's placeholders are substituted here, and the serverless case removes
# structure — the job_clusters block and every job_cluster_key — which is a JSON edit
# rather than a text substitution.
$raw = Get-Content 'databricks/job_sentinel_pipeline.json' -Raw
$substitutions = @{
    'JOB_NAME'      = $JobName
    'SPARK_VERSION' = $SparkVersion
    'NODE_TYPE'     = $NodeType
    'NOTEBOOK_DIR'  = $NotebookDir
    'WHEEL_PATH'    = $wheelPath
    'SCALE'         = $Scale
}
foreach ($key in $substitutions.Keys) {
    # ConvertTo-Json on the bare value, minus its quotes: escapes anything that would
    # otherwise break out of the JSON string it is being substituted into.
    $escaped = ($substitutions[$key] | ConvertTo-Json).Trim('"')
    $raw = $raw.Replace('${' + $key + '}', $escaped)
}
$raw = $raw.Replace('${NUM_WORKERS}', [string] $NumWorkers)

$job = $raw | ConvertFrom-Json
$job.PSObject.Properties.Remove('_comment')

if ($Serverless) {
    $job.PSObject.Properties.Remove('job_clusters')
    foreach ($task in $job.tasks) { $task.PSObject.Properties.Remove('job_cluster_key') }
}

$json = $job | ConvertTo-Json -Depth 20
if ($json -match '\$\{[A-Z_]+\}') {
    Stop-WithError "Unsubstituted placeholder left in the job definition: $($Matches[0])"
}
Set-Content -LiteralPath $rendered -Value $json -Encoding utf8
Write-Ok "job definition rendered ($($job.tasks.Count) tasks)"

if ($Out) {
    Copy-Item $rendered $Out -Force
    Write-Ok "written to $Out"
}

try {
    if ($DryRun) {
        Write-Skip 'dry run - the rendered definition follows'
        $json -split "`n" | ForEach-Object { Write-Host "       $_" }
    } else {
        # Idempotency: look the job up by name. Without this, every deploy creates
        # another job with the same name and the workspace slowly fills with
        # duplicates that all run on the same schedule.
        $listed = Invoke-Databricks 'jobs' 'list' '--name' $JobName '-o' 'json'
        $existing = $null
        if ($script:LastDatabricksExit -eq 0 -and $listed.Trim()) {
            $existing = try { (($listed | ConvertFrom-Json) | Select-Object -First 1).job_id } catch { $null }
        }

        if ($existing) {
            $resetPath = "$rendered.reset"
            @{ job_id = [int] $existing; new_settings = $job } | ConvertTo-Json -Depth 20 `
                | Set-Content -LiteralPath $resetPath -Encoding utf8
            Invoke-Databricks 'jobs' 'reset' '--json' "@$resetPath" | Out-Null
            if ($script:LastDatabricksExit -ne 0) { Stop-WithError "Failed to update job $existing" }
            Remove-Item $resetPath -Force -ErrorAction SilentlyContinue
            $jobId = $existing
            Write-Ok "updated job $jobId in place"
        } else {
            $created = Invoke-Databricks 'jobs' 'create' '--json' "@$rendered" '-o' 'json'
            if ($script:LastDatabricksExit -ne 0) { Stop-WithError 'Failed to create the job' }
            $jobId = ($created | ConvertFrom-Json).job_id
            Write-Ok "created job $jobId"
        }

        if ($hostUrl) { Write-Host "       $($hostUrl.TrimEnd('/'))/jobs/$jobId" }
    }
} finally {
    Remove-Item $rendered -Force -ErrorAction SilentlyContinue
}

# ---------------------------------------------------------------- 7. run

if ($Run) {
    Write-Step 'Run'
    if ($DryRun) {
        Write-Skip "dry run - would trigger job $JobName"
    } else {
        Write-Host "       Triggering job $jobId - this waits for completion (Ctrl-C is safe, the run continues)"
        Invoke-Databricks 'jobs' 'run-now' $jobId '--timeout' '60m' | Out-Null
        if ($script:LastDatabricksExit -ne 0) { Stop-WithError 'The run failed. Check the job page above.' }
        Write-Ok 'run finished'
    }
}

Write-Step 'Done'
if ($DryRun) {
    Write-Host '       Dry run only - nothing was uploaded, created or changed.'
    Write-Host '       Re-run without -DryRun to deploy.'
} else {
    Write-Host "       Deployed to $Catalog. Trigger it with:  .\databricks\deploy.ps1 -Run"
}
