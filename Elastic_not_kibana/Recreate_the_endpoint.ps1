# === reset_elser_endpoint.ps1 ===
param(
  [string]$EsUrl        = "http://localhost:9200",
  [string]$User         = "elastic",
  [string]$Pass         = "changeme",
  [string]$EndpointName = "elser-local"
)

$ErrorActionPreference = "Stop"
$auth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("$User`:$Pass"))
$hdr  = @{ "Authorization"="Basic $auth"; "Content-Type"="application/json" }

function Try-DELETE {
  try {
    Invoke-RestMethod -Method DELETE -Uri "$EsUrl/_inference/$EndpointName" -Headers @{ "Authorization"="Basic $auth" } | Out-Null
  } catch { <# ignore 404 #> }
}

# 0) quick sanity
Invoke-RestMethod -Method GET -Uri "$EsUrl/_cluster/health" -Headers @{ "Authorization"="Basic $auth" } | Out-Null

# 1) ensure model artifacts are in the container path (names must exist already)
#    elser_model_2.metadata.json / .vocab.json / .pt under /usr/share/elasticsearch/config/models
#    (you already copied them to C:\ml-models -> bind mount)

# 2) delete any old/incorrect endpoint
Try-DELETE

# 3) create the proper ELSER endpoint (9.x)
$createBody = @{
  service = "elser"
  service_settings = @{
    num_allocations = 1
    num_threads     = 1
  }
} | ConvertTo-Json -Depth 6

Invoke-RestMethod -Method PUT `
  -Uri "$EsUrl/_inference/sparse_embedding/$EndpointName" `
  -Headers $hdr -Body $createBody | Out-Null

# 4) tiny infer (correct 9.x request shape)
$inferBody = @{ input = "hello world from addis ababa" } | ConvertTo-Json
$resp = Invoke-RestMethod -Method POST `
  -Uri "$EsUrl/_inference/sparse_embedding/$EndpointName" `
  -Headers $hdr -Body $inferBody

$resp | ConvertTo-Json -Depth 12
Write-Host "`n✅ ELSER endpoint ready & responding." -ForegroundColor Green
