<#
.SYNOPSIS
Sets up a Python venv, installs dependencies, verifies Elasticsearch connectivity,
and (optionally) starts the ELSER v2 model deployment.

.PARAMETER ProjectDir
Folder containing run_bert_elser_test.py and bert_elser_pipeline.py

.PARAMETER EsUrl
Elasticsearch URL (default http://localhost:9200)

.PARAMETER EsUser
Elasticsearch username (default elastic)

.PARAMETER EsPass
Elasticsearch password (default changeme)

.PARAMETER StartElser
Switch: if provided, attempts to start the ELSER model deployment
#>

param(
  [string]$ProjectDir = ".",
  [string]$EsUrl = "http://localhost:9200",
  [string]$EsUser = "elastic",
  [string]$EsPass = "changeme",
  [switch]$StartElser
)

$ErrorActionPreference = "Stop"

Write-Host "== Step 1: Enter project directory ==" -ForegroundColor Cyan
Set-Location $ProjectDir

Write-Host "== Step 2: Create and activate Python venv ==" -ForegroundColor Cyan
python -m venv .venv
if (!(Test-Path ".\.venv\Scripts\Activate.ps1")) {
  throw "Virtual environment not created (.venv missing)"
}
. .\.venv\Scripts\Activate.ps1

Write-Host "== Step 3: Upgrade pip & install dependencies ==" -ForegroundColor Cyan
python -m pip install --upgrade pip
# Core deps used by your scripts
pip install "elasticsearch>=8.12.0" pandas openpyxl python-dateutil

Write-Host "== Step 4: Verify Elasticsearch connectivity ==" -ForegroundColor Cyan
try {
  $resp = Invoke-RestMethod -Method GET -Uri "$EsUrl" -Authentication Basic -Credential (New-Object System.Management.Automation.PSCredential($EsUser,(ConvertTo-SecureString $EsPass -AsPlainText -Force)))
  Write-Host ("Elasticsearch name: " + $resp.name)
  Write-Host ("Elasticsearch version: " + $resp.version.number)
} catch {
  Write-Warning "Could not reach Elasticsearch at $EsUrl. You can still run BM25-only mode with --bm25-only."
}

if ($StartElser) {
  Write-Host "== Step 5 (optional): Start ELSER v2 model deployment ==" -ForegroundColor Cyan
  # Model id used by your script defaults: .elser_model_2_linux-x86_64
  $ModelId = ".elser_model_2_linux-x86_64"

  try {
    # Start deployment (idempotent if already started)
    $startUri = "$EsUrl/_ml/trained_models/$ModelId/deployment/_start"
    $startBody = @{ timeout = "2m" } | ConvertTo-Json
    $cred = New-Object System.Management.Automation.PSCredential($EsUser,(ConvertTo-SecureString $EsPass -AsPlainText -Force))
    Invoke-RestMethod -Method POST -Uri $startUri -Authentication Basic -Credential $cred -ContentType "application/json" -Body $startBody | Out-Null
    Write-Host "ELSER model start request sent."

    Start-Sleep -Seconds 3

    # Verify it shows up
    $getUri = "$EsUrl/_ml/trained_models/$ModelId/_stats"
    $stats = Invoke-RestMethod -Method GET -Uri $getUri -Authentication Basic -Credential $cred
    Write-Host "ELSER stats retrieved. (If not deployed yet, ES will continue spinning it up.)"
  } catch {
    Write-Warning "Starting ELSER failed. You can still run the script in --bm25-only mode."
  }
}

Write-Host "== Done. Virtual env ready. ==" -ForegroundColor Green
Write-Host "To activate later:  . .\.venv\Scripts\Activate.ps1"
