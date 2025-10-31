# run_bert_elser_test.py
# One-shot or interactive semantic (ELSER+BM25) or BM25-only search.
# Uses bert_elser_pipeline.BertDescriptionElser

import sys
from pathlib import Path
import argparse
import pandas as pd

# Ensure we can import the class module sitting next to this file
HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from bert_elser_pipeline import BertDescriptionElser  # noqa: E402


def ensure_indexed(pipe: BertDescriptionElser, file_path: str, reindex: bool) -> None:
    """Create mapping/pipeline and index the provided file if requested or if index is empty."""
    count = 0
    if pipe.es.indices.exists(index=pipe.index_name):
        try:
            count = pipe.es.count(index=pipe.index_name)["count"]
        except Exception:
            count = 0

    if reindex or count == 0:
        if pipe.es.indices.exists(index=pipe.index_name):
            pipe.es.indices.delete(index=pipe.index_name, ignore_unavailable=True)
        pipe.ensure_index()
        pipe.ensure_pipeline()  # no-op if ML unavailable
        pipe.bulk_index_file(file_path, id_field=None)
        count = pipe.es.count(index=pipe.index_name)["count"]
        print(f"[INFO] Indexed docs: {count}")
    else:
        print(f"[INFO] Using existing index '{pipe.index_name}' with {count} docs.")


def main():
    ap = argparse.ArgumentParser(description="ELSER or BM25 search without hard-coded queries.")
    ap.add_argument("--file", "-f", required=True, help="Path to .xlsx/.xls/.csv to index and search.")
    ap.add_argument("--col", "-c", default="Description", help="Text column to index and search. Default: Description")
    ap.add_argument("--query", "-q", default=None, help="One-shot query text. If omitted, enters interactive mode.")
    ap.add_argument("--reindex", action="store_true", help="Recreate index and re-ingest the file.")
    ap.add_argument("--index-name", default="chat_elser_description_only", help="Elasticsearch index name.")
    ap.add_argument("--pipeline-id", default="elser_v2_description_only", help="Elasticsearch ingest pipeline id.")
    ap.add_argument("--es-url", default="http://localhost:9200", help="Elasticsearch URL.")
    ap.add_argument("--es-user", default="elastic", help="Elasticsearch username.")
    ap.add_argument("--es-pass", default="changeme", help="Elasticsearch password.")
    ap.add_argument("--model-id", default=".elser_model_2_linux-x86_64", help="ELSER model id.")
    ap.add_argument("--size", type=int, default=10, help="Number of hits to return. Default: 10")
    ap.add_argument("--bm25-only", action="store_true", help="Force BM25-only (ignore ELSER/text_expansion).")
    args = ap.parse_args()

    # Preview columns to help catch typos early
    fp = args.file
    if not Path(fp).exists():
        raise SystemExit(f"Input file not found: {fp}")

    if fp.lower().endswith((".xlsx", ".xls")):
        df_preview = pd.read_excel(fp)
    elif fp.lower().endswith(".csv"):
        df_preview = pd.read_csv(fp)
    else:
        raise SystemExit("Only .xlsx, .xls, or .csv are supported.")

    if args.col not in df_preview.columns:
        raise SystemExit(f"Column '{args.col}' not found. Available: {list(df_preview.columns)}")

    print("\n=== DATA PREVIEW (first 3 rows) ===")
    print(df_preview.head(3))
    print("\n=== COLUMNS ===")
    print(list(df_preview.columns))

    pipe = BertDescriptionElser(
        es_url=args.es_url,
        es_user=args.es_user,
        es_pass=args.es_pass,
        index_name=args.index_name,
        pipeline_id=args.pipeline_id,
        model_id=args.model_id,
        description_col=args.col,
        use_ml=(not args.bm25_only),  # allow forcing BM25-only
    )

    # Back-compat shim: safe no-op that ensures pipeline if ML requested
    pipe.ensure_ready()
    ensure_indexed(pipe, fp, reindex=args.reindex)

    def do_query(q: str):
        hits = pipe.semantic_search(
            question=q,
            size=args.size,
            hybrid=(not args.bm25_only),  # BM25 always; add ELSER if allowed and available
        )
        if hits.empty:
            print("(no matches)")
        else:
            # Show a compact view: score and the description column (if present) plus any timestamp
            cols = ["_score"]
            if args.col in hits.columns:
                cols.append(args.col)
            if "timestamp" in hits.columns:
                cols.append("timestamp")
            print(hits[cols] if set(cols).issubset(hits.columns) else hits)

    if args.query:
        print(f"\n=== SEARCH RESULTS for: {args.query!r} ===")
        do_query(args.query)
    else:
        # Interactive loop
        print("\nInteractive mode. Type your query and press Enter.")
        print("Commands: :quit to exit, :help for help.\n")
        while True:
            try:
                q = input("query> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting.")
                break
            if not q:
                continue
            if q in {":quit", ":exit"}:
                print("Exiting.")
                break
            if q in {":help", "help", "?"}:
                print("Enter any text to search. Use :quit to exit.")
                continue
            print(f"\n=== SEARCH RESULTS for: {q!r} ===")
            do_query(q)
            print("")


if __name__ == "__main__":
    main()
