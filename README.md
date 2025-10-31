# ELSER Hybrid Search (Python + Elasticsearch)

This project provides a simple CLI to index a spreadsheet (CSV/XLSX) into Elasticsearch and run **hybrid semantic search** (ELSER + BM25) or **BM25-only**.

---

## 📁 Files Included

- `run_bert_elser_test.py` — main CLI script  
- `bert_elser_pipeline.py` — reusable ELSER + BM25 pipeline  
- *(optional)* `setup_elser_env.ps1` — PowerShell script for setup and dependency installation

---

1. Prerequisites

- **Windows 10/11** with **PowerShell**
- **Python 3.9+** installed and available in PATH  
  Check:
  ```powershell
  python --version
Quick Elasticsearch via Docker
  ```powershell
  docker run --name es8 -p 9200:9200 `
  -e "discovery.type=single-node" `
  -e "xpack.security.enabled=true" `
  -e "ELASTIC_PASSWORD=changeme" `
  docker.elastic.co/elasticsearch/elasticsearch:8.14.0
```
Check if it’s reachable:
  ```powershell
Invoke-RestMethod -Method GET -Uri "http://localhost:9200" `
  -Authentication Basic `
  -Credential (New-Object System.Management.Automation.PSCredential("elastic",(ConvertTo-SecureString "changeme" -AsPlainText -Force)))
```
Run the setup script (this creates venv, installs deps, and optionally starts ELSER):
  ```powershell
# Basic install (no ELSER start)
.\setup_elser_env.ps1 -ProjectDir "." -EsUrl "http://localhost:9200" -EsUser "elastic" -EsPass "changeme"

# Or, try to start ELSER too:
.\setup_elser_env.ps1 -ProjectDir "." -EsUrl "http://localhost:9200" -EsUser "elastic" -EsPass "changeme" -StartElser
```
(Later, when opening a new shell) Activate the venv:
  ```powershell
. .\.venv\Scripts\Activate.ps1
```
Run the script, Interactive mode (type multiple queries)
```
python run_bert_elser_test.py `
  --file "C:\path\to\your\sheet.xlsx" `
  --col "Description" `
  --reindex
# at the prompt: type queries; use :quit to exit

```
One-shot query with reindex
```
python run_bert_elser_test.py `
  --file "C:\path\to\your\sheet.xlsx" `
  --col "Description" `
  --query "BlueSky Airlines safety compliance" `
  --reindex
```
