
Docker Compose Setup (Elasticsearch + Kibana)

Replace the docker-compose.yml with this:

 ```   
  services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.14.3
    container_name: es01
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=true
      - xpack.license.self_generated.type=trial
      - xpack.ml.enabled=true
      - ES_JAVA_OPTS=-Xms2g -Xmx2g
      - ELASTIC_PASSWORD=changeme
      - xpack.ml.model_repository=file:///usr/share/elasticsearch/config/models
    ulimits:
      memlock:
        soft: -1
        hard: -1
    ports:
      - "9200:9200"
      - "9300:9300"
    volumes:
      # Map the *subfolder* so it becomes the repo root in the container
      - type: bind
        source: "C:\\ml-models\\.elser_model_2_linux-x86_64"
        target: /usr/share/elasticsearch/config/models
        read_only: true

  kibana:
    image: docker.elastic.co/kibana/kibana:8.14.3
    container_name: kb01
    depends_on:
      - elasticsearch
    environment:
      - ELASTICSEARCH_HOSTS=["http://elasticsearch:9200"]
      - ELASTICSEARCH_USERNAME=kibana_system
      - ELASTICSEARCH_PASSWORD=kibana_password123
      - SERVER_PUBLICBASEURL=http://localhost:5601
      - SERVER_HOST=0.0.0.0
      - XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    ports:
      - "5601:5601"
  logstash:
    build:
      context: "C:\\Users\\dell\\oracle-to-es"
    container_name: ls01
    depends_on:
      - elasticsearch
    environment:
      LS_JAVA_OPTS: "-Xms1g -Xmx1g"
 ```

Registering the Model in Kibana → Dev Tools

open Kibana Dev Tools and run:

 ```
PUT _ml/trained_models/.elser_model_2_linux-x86_64
{
  "model_type": "pytorch",
  "model_package": {
    "packaged_model_id": "elser_model_2_linux-x86_64"
  },
  "input": { "field_names": ["text_field"] },
  "inference_config": { "text_expansion": {} }
}

```
Then deploy it:

```
POST _ml/trained_models/.elser_model_2_linux-x86_64/deployment/_start
{
  "number_of_allocations": 1,
  "threads_per_allocation": 1,
  "queue_capacity": 512
}
```
verify it was running:

```
GET _ml/trained_models/.elser_model_2_linux-x86_64/_stats
```

Creating the Ingest Pipeline (Semantic Embedding)

```
PUT _ingest/pipeline/elser_v2_pipeline
{
  "processors": [
    {
      "inference": {
        "model_id": ".elser_model_2_linux-x86_64",
        "input_output": [
          { "input_field": "content", "output_field": "ml.tokens" }
        ],
        "inference_config": { "text_expansion": {} }
      }
    }
  ]
}
```
This will add ELSER embeddings to text automatically.

Installing Python Dependencies

set up a virtual environment and installed everything required for Excel ingestion:

```
python -m venv .venv
.venv\Scripts\activate
```
```
pip install elasticsearch pandas openpyxl python-dateutil pymupdf
```

Searching from Kibana

```
POST excel_elser_index/_search
{
  "size": 5,
  "query": {
    "text_expansion": {
      "ml.tokens": {
        "model_id": ".elser_model_2_linux-x86_64",
        "model_text": "who set records in long distance running?"
      }
    }
  },
  "_source": ["Name","Event","Country","Date","Position"]
}
```

Searching from python 

complete, final working version of 'ingest_to_es_elser.py'

```
import argparse, json, os, sys, time, uuid
from pathlib import Path
from datetime import datetime
import pandas as pd
from dateutil import parser as dtparser
import fitz  # PyMuPDF
from elasticsearch import Elasticsearch, helpers

# ---- Elasticsearch connection ----
ES_URL  = os.environ.get("ES_URL", "http://localhost:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASS = os.environ.get("ES_PASS", "changeme")

MODEL_ID     = ".elser_model_2_linux-x86_64"
PIPELINE_ID  = "elser_v2_pipeline"
TOKENS_FIELD = "ml.tokens"

ES = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS), request_timeout=120)


# ---------- Helpers ----------
def wait_es(timeout_s=60):
    """Wait until Elasticsearch responds."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            ES.info()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Elasticsearch not responding")


def ensure_model_started():
    """Verify ELSER model is deployed."""
    try:
        stats = ES.ml.get_trained_models_stats(model_id=MODEL_ID)
        tms = stats.get("trained_model_stats", [])
        if tms:
            dstats = tms[0].get("deployment_stats") or {}
            if dstats.get("state") == "started":
                print("✅ Model is running")
                return
    except Exception:
        pass

    print("🚀 Starting ELSER model deployment...")
    try:
        ES.ml.start_trained_model_deployment(
            model_id=MODEL_ID,
            number_of_allocations=1,
            threads_per_allocation=1,
            queue_capacity=512,
        )
    except Exception as e:
        print(f"(warn) Could not start model: {e}")


def ensure_pipeline():
    """Create/update the ELSER pipeline."""
    pipeline = {
        "processors": [
            {
                "inference": {
                    "model_id": MODEL_ID,
                    "input_output": [
                        {"input_field": "content", "output_field": TOKENS_FIELD}
                    ],
                    "inference_config": {"text_expansion": {}}
                }
            }
        ]
    }
    ES.ingest.put_pipeline(id=PIPELINE_ID, processors=pipeline["processors"])
    print(f"✅ Pipeline '{PIPELINE_ID}' ensured")


def ensure_index(index: str, with_extra_fields=None):
    """Create index with proper mapping."""
    if ES.indices.exists(index=index):
        return
    props = {
        "content": {"type": "text"},
        "ml": {"properties": {"tokens": {"type": "rank_features"}}}
    }
    if with_extra_fields:
        props.update(with_extra_fields)
    ES.indices.create(index=index, body={"mappings": {"properties": props}})
    print(f"✅ Index '{index}' created")


# ---------- Excel / CSV ingestion ----------
def to_dt(v):
    if pd.isna(v):
        return None
    if isinstance(v, datetime):
        return v
    try:
        return dtparser.parse(str(v))
    except Exception:
        return None


def ingest_table(index: str, file_path: Path, sheet=None,
                 id_col="id", title_col="title", body_col="body",
                 updated_col="updated_at", batch=1000):
    """Read Excel/CSV and bulk index into Elasticsearch."""
    if file_path.suffix.lower() == ".xlsx":
        sheet_name = None
        if sheet is not None:
            try:
                sheet_name = int(sheet)
            except ValueError:
                sheet_name = sheet
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
    elif file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path)
    else:
        raise SystemExit("Unsupported file format. Use .xlsx or .csv")

    # column mapping
    cols = {c.lower().strip(): c for c in df.columns}
    def col(name): return cols.get(name.lower(), name)

    id_col, title_col, body_col, updated_col = (
        col(id_col), col(title_col), col(body_col), col(updated_col)
    )

    missing = [c for c in [id_col, title_col] if c not in df.columns]
    if missing:
        print(f"ERROR: Missing required columns: {missing}", file=sys.stderr)
        sys.exit(2)

    actions = []
    for _, row in df.iterrows():
        rid = row.get(id_col)
        title = row.get(title_col)
        body = row.get(body_col) if body_col in df.columns else None
        updated = to_dt(row.get(updated_col)) if updated_col in df.columns else None

        content = f"{title or ''}\n{body or ''}".strip()
        doc = {"id": rid, "title": title, "body": body, "content": content}
        if updated is not None:
            doc["updated_at"] = updated.isoformat()

        actions.append({
            "_op_type": "index",
            "_index": index,
            "_id": str(rid) if rid is not None else None,
            "pipeline": PIPELINE_ID,
            "_source": doc
        })

    print(f"📊 Indexing {len(actions)} rows from '{file_path.name}' → '{index}'...")
    success, fail = helpers.bulk(ES, actions, stats_only=True, chunk_size=batch)
    ES.indices.refresh(index=index)
    print(f"✅ Done. success={success}, failed={fail}")


# ---------- PDF ingestion (optional) ----------
def chunk_text(text: str, chunk_size: int = 1200, overlap: int = 200):
    text = (text or "").strip()
    if not text:
        return []
    chunks, n, start = [], len(text), 0
    while start < n:
        end = min(n, start + chunk_size)
        slice_ = text[start:end]
        if end < n:
            last_dot = slice_.rfind(".")
            if last_dot > int(chunk_size * 0.6):
                end = start + last_dot + 1
                slice_ = text[start:end]
        chunk = slice_.strip()
        if chunk:
            chunks.append(chunk)
        start = end if end >= n else end - overlap
    return chunks


def extract_pdf(path: Path, max_pages: int | None = None):
    with fitz.open(path) as doc:
        total = doc.page_count
        limit = total if max_pages is None else min(max_pages, total)
        for i in range(limit):
            page = doc.load_page(i)
            txt = page.get_text("text") or ""
            if txt.strip():
                yield (i + 1, txt)


def ingest_pdf(index: str, input_path: Path, chunk_size=1200, overlap=200, max_pages=None, batch=500):
    """Index PDF text chunks."""
    pdfs = [input_path] if input_path.is_file() else [p for p in input_path.rglob("*.pdf")]
    if not pdfs:
        print("No PDFs found.")
        return
    print(f"📚 Indexing {len(pdfs)} PDF(s) → '{index}'...")
    buf, sent = [], 0
    for pdf in pdfs:
        try:
            for page_num, text in extract_pdf(pdf, max_pages=max_pages):
                for j, chunk in enumerate(chunk_text(text, chunk_size, overlap), start=1):
                    meta = {"index": {"_index": index, "_id": str(uuid.uuid4())}}
                    src = {"path": str(pdf), "page": page_num, "chunk": j, "content": chunk}
                    buf.append(json.dumps(meta) + "\n" + json.dumps(src, ensure_ascii=False) + "\n")
                    if len(buf) >= batch * 2:
                        ES.bulk(body="".join(buf), pipeline=PIPELINE_ID)
                        sent += len(buf) // 2
                        buf = []
        except Exception as e:
            print(f"Error processing {pdf}: {e}")
    if buf:
        ES.bulk(body="".join(buf), pipeline=PIPELINE_ID)
        sent += len(buf) // 2
    ES.indices.refresh(index=index)
    print(f"✅ Done. Indexed ~{int(sent)} chunks.")


# ---------- Query ----------
def semantic_search(index: str, query: str, size: int = 5):
    try:
        cnt = ES.count(index=index).get("count", 0)
        if cnt == 0:
            print(f"(Index '{index}' has 0 docs — nothing to search yet.)")
    except Exception:
        pass

    body = {
        "size": size,
        "query": {
            "text_expansion": {
                TOKENS_FIELD: {"model_id": MODEL_ID, "model_text": query}
            }
        },
        "_source": ["id", "title", "content", "path", "page", "updated_at"]
    }
    res = ES.search(index=index, body=body)
    print(f"\n🔍 Query: {query}")
    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        print("- no hits -")
        return res
    for h in hits:
        src = h["_source"]
        title = src.get("title") or Path(src.get("path", "")).name
        preview = (src.get("content") or "")[:150].replace("\n", " ")
        print(f"- score={h['_score']:.3f}  title={title}  preview={preview}")
    return res


# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(
        description="Ingest Excel/CSV/PDF into Elasticsearch and search via ELSER"
    )
    ap.add_argument("--index", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--id-col", default="id")
    ap.add_argument("--title-col", default="title")
    ap.add_argument("--body-col", default="body")
    ap.add_argument("--updated-col", default="updated_at")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--query", default=None)
    ap.add_argument("--topk", type=int, default=5)
    args = ap.parse_args()

    wait_es()
    ensure_model_started()
    ensure_pipeline()

    p = Path(args.file)
    if p.suffix.lower() in [".xlsx", ".csv"]:
        ensure_index(args.index, with_extra_fields={
            "id": {"type": "keyword"},
            "title": {"type": "text"},
            "body": {"type": "text"},
            "updated_at": {"type": "date"}
        })
        ingest_table(args.index, p, args.sheet, args.id_col, args.title_col, args.body_col, args.updated_col, args.batch)
    elif p.suffix.lower() == ".pdf" or p.is_dir():
        ensure_index(args.index, with_extra_fields={
            "path": {"type": "keyword"}, "page": {"type": "integer"}, "chunk": {"type": "integer"}
        })
        ingest_pdf(args.index, p)
    else:
        print("Unsupported file type.")

    if args.query:
        semantic_search(args.index, args.query, args.topk)


if __name__ == "__main__":
    main()
```

Optional 'ingest_to_es_elser.py'

```
import argparse, json, math, os, sys, time, uuid
from pathlib import Path
from datetime import datetime

import pandas as pd
from dateutil import parser as dtparser
import fitz  # PyMuPDF
from elasticsearch import Elasticsearch, helpers

# ---- Common ES/ELSER config ----
ES_URL   = os.environ.get("ES_URL", "http://localhost:9200")
ES_USER  = os.environ.get("ES_USER", "elastic")
ES_PASS  = os.environ.get("ES_PASS", "changeme")

MODEL_ID     = ".elser_model_2_linux-x86_64"
PIPELINE_ID  = "elser_v2_pipeline"
TOKENS_FIELD = "ml.tokens"

# ---- Connect ----
ES = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS), request_timeout=120)

def wait_es(timeout_s=60):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            ES.info()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Elasticsearch not responding")

def ensure_model_started():
    """Verify the ELSER model is deployed; start if not."""
    try:
        stats = ES.ml.get_trained_models_stats(model_id=MODEL_ID)
        tms = stats.get("trained_model_stats", [])
        if tms:
            dstats = tms[0].get("deployment_stats") or {}
            if dstats.get("state") == "started":
                return
    except Exception:
        pass
    try:
        ES.ml.start_trained_model_deployment(
            model_id=MODEL_ID,
            number_of_allocations=1,
            threads_per_allocation=1,
            queue_capacity=1024,
        )
    except Exception:
        # no-op if already started / transient
        pass

def ensure_pipeline():
    """Create/update the ELSER pipeline (idempotent)."""
    pipeline = {
        "processors": [
            {
                "inference": {
                    "model_id": MODEL_ID,
                    "input_output": [
                        {"input_field": "content", "output_field": TOKENS_FIELD}
                    ],
                    "inference_config": {"text_expansion": {}}
                }
            }
        ]
    }
    ES.ingest.put_pipeline(id=PIPELINE_ID, processors=pipeline["processors"])

def ensure_index(index: str, with_extra_fields=None):
    """Create index with ELSER mapping if missing."""
    if ES.indices.exists(index=index):
        return
    props = {
        "content": {"type": "text"},
        "ml": {"properties": {"tokens": {"type": "rank_features"}}}
    }
    if with_extra_fields:
        props.update(with_extra_fields)
    ES.indices.create(index=index, body={"mappings": {"properties": props}})

# ---------- helpers ----------
def to_dt(v):
    if pd.isna(v):
        return None
    if isinstance(v, datetime):
        return v
    try:
        return dtparser.parse(str(v))
    except Exception:
        return None

# ---------- Excel / CSV ingestion ----------
def ingest_table(
    index: str,
    file_path: Path,
    sheet=None,
    *,
    id_col="id",
    title_col="title",
    body_col="body",
    updated_col="updated_at",
    country_col="Country",
    date_col="Date",
    time_col="Time (HH:MM:SS)",
    position_col="Position",
    batch=1000,
):
    # read df
    if file_path.suffix.lower() == ".xlsx":
        sheet_name = None
        if sheet is not None:
            try:
                sheet_name = int(sheet)
            except ValueError:
                sheet_name = sheet
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
    elif file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path)
    else:
        raise SystemExit("Unsupported tabular format. Use .xlsx or .csv")

    # case-insensitive column mapping
    cols = {c.lower().strip(): c for c in df.columns}
    def col(name): return cols.get(name.lower(), name)

    id_col       = col(id_col)
    title_col    = col(title_col)   # Name
    body_col     = col(body_col)    # Event (by your CLI)
    updated_col  = col(updated_col)
    country_col  = col(country_col)
    date_col     = col(date_col)
    time_col     = col(time_col)
    position_col = col(position_col)

    missing = [c for c in [id_col, title_col] if c not in df.columns]
    if missing:
        print(f"ERROR: Missing required columns: {missing}", file=sys.stderr)
        sys.exit(2)

    actions = []
    for _, row in df.iterrows():
        rid      = row.get(id_col)
        title    = row.get(title_col)        # Name
        event    = row.get(body_col) if body_col in df.columns else None
        country  = row.get(country_col) if country_col in df.columns else None
        date_val = row.get(date_col) if date_col in df.columns else None
        time_val = row.get(time_col) if time_col in df.columns else None
        pos_val  = row.get(position_col) if position_col in df.columns else None
        updated  = to_dt(row.get(updated_col)) if updated_col in df.columns else None

        # Normalize date to ISO string if present
        date_iso = None
        if date_val is not None:
            try:
                date_iso = to_dt(date_val).date().isoformat()
            except Exception:
                pass

        # Rich content text for ELSER
        parts = []
        if title:    parts.append(str(title))
        if event:    parts.append(f"Event: {event}")
        if country:  parts.append(f"Country: {country}")
        if date_iso: parts.append(f"Date: {date_iso}")
        if time_val is not None and str(time_val).strip() != "":
            parts.append(f"Time: {time_val}")
        if pos_val is not None and str(pos_val).strip() != "":
            parts.append(f"Position: {pos_val}")

        content = ". ".join(parts) if parts else (str(title) or "")

        # Document to index (kept structured fields too)
        doc = {
            "id":       rid,
            "title":    title,          # Name
            "body":     event,          # Event (compat)
            "event":    event,
            "country":  country,
            "date":     date_iso,
            "time_raw": str(time_val) if time_val is not None else None,
            "position": int(pos_val) if str(pos_val).isdigit() else None,
            "content":  content,
        }
        if updated is not None:
            doc["updated_at"] = updated.isoformat()

        actions.append({
            "_op_type": "index",
            "_index": index,
            "_id": str(rid) if rid is not None else None,
            "pipeline": PIPELINE_ID,
            "_source": doc
        })

    if not actions:
        print("No rows to index.")
        return

    print(f"Indexing {len(actions)} rows from '{file_path.name}' → '{index}' via '{PIPELINE_ID}'...")
    success, fail = helpers.bulk(ES, actions, stats_only=True, chunk_size=batch, request_timeout=120)
    ES.indices.refresh(index=index)
    print(f"Done. success={success}, failed={fail}")

# ---------- PDF ingestion ----------
def chunk_text(text: str, chunk_size: int = 1200, overlap: int = 200):
    text = (text or "").strip()
    if not text:
        return []
    chunks = []
    n = len(text)
    start = 0
    while start < n:
        end = min(n, start + chunk_size)
        slice_ = text[start:end]
        if end < n:
            last_dot = slice_.rfind(".")
            if last_dot > int(chunk_size * 0.6):
                end = start + last_dot + 1
                slice_ = text[start:end]
        chunk = slice_.strip()
        if chunk:
            chunks.append(chunk)
        start = end if end >= n else end - overlap
    return chunks

def extract_pdf(path: Path, max_pages: int | None = None):
    with fitz.open(path) as doc:
        total = doc.page_count
        limit = total if max_pages is None else min(max_pages, total)
        for i in range(limit):
            page = doc.load_page(i)
            txt = page.get_text("text") or ""
            if txt.strip():
                yield (i + 1, txt)

def bulk_line(index: str, pdf_path: Path, page: int, chunk_id: int, content: str):
    meta = {"index": {"_index": index, "_id": str(uuid.uuid4())}}
    src = {"path": str(pdf_path), "page": page, "chunk": chunk_id, "content": content}
    return json.dumps(meta) + "\n" + json.dumps(src, ensure_ascii=False) + "\n"

def ingest_pdf(index: str, input_path: Path, chunk_size=1200, overlap=200, max_pages=None, batch=500):
    pdfs = [input_path] if input_path.is_file() else [p for p in input_path.rglob("*.pdf")]
    if not pdfs:
        print("No PDFs found.")
        return
    print(f"Indexing {len(pdfs)} PDF(s) → '{index}' via '{PIPELINE_ID}'...")
    buf, sent = [], 0
    for pdf in pdfs:
        try:
            for page_num, text in extract_pdf(pdf, max_pages=max_pages):
                for j, chunk in enumerate(chunk_text(text, chunk_size, overlap), start=1):
                    buf.append(bulk_line(index, pdf, page_num, j, chunk))
                    if len(buf) >= batch * 2:  # 2 lines per action
                        resp = ES.bulk(body="".join(buf), pipeline=PIPELINE_ID)
                        if resp.get("errors"):
                            first = next((it for it in resp["items"] if "error" in list(it.values())[0]), None)
                            print("Bulk errors; first:", first)
                        sent += len(buf) // 2
                        buf = []
        except Exception as e:
            print(f"Error processing {pdf}: {e}")
    if buf:
        resp = ES.bulk(body="".join(buf), pipeline=PIPELINE_ID)
        if resp.get("errors"):
            first = next((it for it in resp["items"] if "error" in list(it.values())[0]), None)
            print("Bulk errors; first:", first)
        sent += len(buf) // 2
    ES.indices.refresh(index=index)
    print(f"Done. Indexed ~{int(sent)} chunks.")

# ---------- Query ----------
def semantic_search(index: str, query: str, size: int = 5):
    try:
        cnt = ES.count(index=index).get("count", 0)
        if cnt == 0:
            print(f"(Index '{index}' has 0 docs — nothing to search yet.)")
    except Exception:
        pass

    body = {
        "size": size,
        "query": {
            "text_expansion": {
                TOKENS_FIELD: {
                    "model_id": MODEL_ID,
                    "model_text": query
                }
            }
        },
        "_source": ["id","title","event","country","content","path","page","updated_at"]
    }
    res = ES.search(index=index, body=body)
    print(f"\nQuery: {query}")
    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        print("- no hits -")
        return res
    for h in hits:
        src = h["_source"]
        title = src.get("title") or Path(src.get("path","")).name or ""
        preview = (src.get("content") or "")[:160].replace("\n"," ")
        print(f"- score={h['_score']:.3f}  title={title}  page={src.get('page')}  preview={preview}")
    return res

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(
        description="Ingest Excel/CSV/PDF into Elasticsearch with ELSER pipeline (ml.tokens) and optionally query."
    )
    ap.add_argument("--index", required=True, help="Target index (e.g., oracle_elser_index or pdf_elser_index)")
    ap.add_argument("--file", required=True, help="Path to .xlsx / .csv / .pdf or a folder of PDFs")
    ap.add_argument("--sheet", default=None, help="Excel sheet name or index (for .xlsx)")
    ap.add_argument("--id-col", default="id")
    ap.add_argument("--title-col", default="title")
    ap.add_argument("--body-col", default="body")
    ap.add_argument("--updated-col", default="updated_at")
    ap.add_argument("--country-col",  default="Country")
    ap.add_argument("--date-col",     default="Date")
    ap.add_argument("--time-col",     default="Time (HH:MM:SS)")
    ap.add_argument("--position-col", default="Position")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--chunk-size", type=int, default=1200, help="PDF chars per chunk")
    ap.add_argument("--overlap", type=int, default=200, help="PDF chunk overlap")
    ap.add_argument("--max-pages", type=int, default=None, help="PDF page limit per doc")
    ap.add_argument("--query", default=None, help="Optional: run a semantic question after ingest")
    ap.add_argument("--topk", type=int, default=5, help="Hits to return for --query")

    args = ap.parse_args()

    wait_es()
    ensure_model_started()
    ensure_pipeline()

    p = Path(args.file)

    if p.suffix.lower() in [".xlsx", ".csv"]:
        # table-like index (id/title/body/content/updated_at + extras)
        ensure_index(args.index, with_extra_fields={
            "id":        {"type": "keyword"},
            "title":     {"type": "text"},
            "body":      {"type": "text"},
            "country":   {"type": "keyword"},
            "event":     {"type": "keyword"},
            "date":      {"type": "date"},
            "time_raw":  {"type": "keyword"},
            "position":  {"type": "integer"},
            "updated_at":{"type": "date"}
        })
        ingest_table(
            index=args.index,
            file_path=p,
            sheet=args.sheet,
            id_col=args.id_col,
            title_col=args.title_col,
            body_col=args.body_col,
            updated_col=args.updated_col,
            country_col=args.country_col,
            date_col=args.date_col,
            time_col=args.time_col,
            position_col=args.position_col,
            batch=args.batch
        )
    else:
        # PDF mode: support single file or folder
        ensure_index(args.index, with_extra_fields={
            "path": {"type": "keyword"},
            "page": {"type": "integer"},
            "chunk": {"type": "integer"},
        })
        ingest_pdf(
            index=args.index,
            input_path=p,
            chunk_size=args.chunk_size,
            overlap=args.overlap,
            max_pages=args.max_pages,
            batch=args.batch
        )

    if args.query:
        semantic_search(args.index, args.query, size=args.topk)

if __name__ == "__main__":
    main()
```

Run (Excel)

```
python ingest_to_es_elser.py ^
  --index excel_elser_index ^
  --file "C:\Users\dell\elser-python\long_distance_runners_record.xlsx" ^
  --sheet Sheet1 ^
  --id-col "Runner ID" ^
  --title-col "Name" ^
  --body-col "Event" ^
  --query "who set records in long distance running?"
```
