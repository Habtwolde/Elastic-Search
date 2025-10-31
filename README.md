# ELSER Hybrid Search (Python + Elasticsearch)

This project provides a simple CLI to index a spreadsheet (CSV/XLSX) into Elasticsearch and run **hybrid semantic search** (ELSER + BM25) or **BM25-only**.

---

## 📁 Files Included

- `run_bert_elser_test.py` — main CLI script  
- `bert_elser_pipeline.py` — reusable ELSER + BM25 pipeline  
- *(optional)* `setup_elser_env.ps1` — PowerShell script for setup and dependency installation

---

## ⚙️ 1. Prerequisites

- **Windows 10/11** with **PowerShell**
- **Python 3.9+** installed and available in PATH  
  Check:
  ```powershell
  python --version
Quick Elasticsearch via Docker
