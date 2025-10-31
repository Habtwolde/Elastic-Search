"""
bert_elser_pipeline.py
Robust ELSER v2 + BM25 search with automatic query-time fallback.

Design:
- No license or deployment probing up front.
- Indexing: if ELSER is available, you may attach an ingest pipeline to produce ml.description_tokens.
  If not, indexing still works; you just won’t have semantic tokens.
- Search: first attempt ELSER text_expansion + BM25. On any 4xx/5xx error, transparently retry BM25-only.

This avoids false negatives from license/deployment checks and works across cluster configs.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterable, Optional, List, Sequence, Union

import pandas as pd
from dateutil import parser as dtparser
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from elastic_transport import ApiError


def _coerce_str(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def to_iso(v) -> Optional[str]:
    if pd.isna(v):
        return None
    try:
        if isinstance(v, datetime):
            return v.isoformat()
        return dtparser.parse(str(v)).isoformat()
    except Exception:
        return None


class BertDescriptionElser:
    def __init__(
        self,
        es_url: str = "http://localhost:9200",
        es_user: str = "elastic",
        es_pass: str = "changeme",
        index_name: str = "chat_elser_description_only",
        pipeline_id: str = "elser_v2_description_only",
        model_id: str = ".elser_model_2_linux-x86_64",
        description_col: str = "Description",
        request_timeout: int = 120,
        use_ml: bool = True,
    ) -> None:
        self.es = Elasticsearch(
            es_url,
            basic_auth=(es_user, es_pass),
            request_timeout=request_timeout,
            verify_certs=False,
        )
        self.index_name = index_name
        self.pipeline_id = pipeline_id
        self.model_id = model_id
        self.description_col = description_col
        self.use_ml_requested = use_ml  # user preference to try ELSER

    # --------------------------
    # Mapping and pipeline
    # --------------------------
    def ensure_index(self) -> None:
        """Create or update a minimal mapping. Add rank_features if we expect ML tokens."""
        props: Dict[str, Any] = {
            self.description_col: {"type": "text"},
            "timestamp": {"type": "date", "ignore_malformed": True},
        }
        # It is safe to declare the token field even if it won’t be used.
        props.setdefault("ml", {"properties": {}})
        props["ml"]["properties"]["description_tokens"] = {"type": "rank_features"}

        body = {"mappings": {"properties": props}}
        if self.es.indices.exists(index=self.index_name):
            self.es.indices.put_mapping(index=self.index_name, properties=props)
        else:
            self.es.indices.create(index=self.index_name, **body)

    def ensure_pipeline(self) -> None:
        """
        Create or update ingest pipeline that writes to ml.description_tokens.
        If the model is unavailable, putting this pipeline still succeeds; any error would occur at ingest-time.
        """
        if not self.use_ml_requested:
            return
        processors = [
            {
                "inference": {
                    "model_id": self.model_id,
                    "inference_config": {
                        "text_expansion": {"results_field": "ml.description_tokens"}
                    },
                    "field_map": {self.description_col: "text_field"},
                }
            }
        ]
        self.es.ingest.put_pipeline(id=self.pipeline_id, processors=processors)

    def ensure_ready(self) -> None:
        """
        Backward-compat shim for older scripts that call `ensure_ready()`.
        Keep it side-effect-free for indexing; just make sure the ingest
        pipeline exists if ML was requested. Index creation happens in
        the caller (e.g., ensure_indexed()).
        """
        try:
            if self.use_ml_requested:
                self.ensure_pipeline()
        except Exception:
            # Don't fail here; actual search/indexing will gracefully fall back.
            pass

    # --------------------------
    # Ingestion
    # --------------------------
    def _sanitize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.description_col not in df.columns:
            raise ValueError(
                f"Required column '{self.description_col}' not found. "
                f"Available columns: {list(df.columns)}"
            )
        s = df[self.description_col].astype(str).map(lambda x: x.strip())
        df = df.copy()
        df[self.description_col] = s
        df = df[df[self.description_col].astype(bool)]
        if df.empty:
            raise ValueError(
                f"All rows are empty in '{self.description_col}'. Provide non-empty text."
            )
        return df

    def _iter_actions(self, df: pd.DataFrame, id_field: Optional[str]) -> Iterable[Dict[str, Any]]:
        for _, row in df.iterrows():
            doc: Dict[str, Any] = {}
            for c in df.columns:
                val = row[c]
                if pd.isna(val):
                    continue
                doc[c] = val

            # Optional timestamp detection
            for cand in ("created_dttm", "created_at", "timestamp", "time", "date"):
                if cand in df.columns and not pd.isna(row.get(cand)):
                    iso = to_iso(row[cand])
                    if iso:
                        doc["timestamp"] = iso
                        break

            action = {
                "_op_type": "index",
                "_index": self.index_name,
                "_source": doc,
            }
            if self.use_ml_requested:
                action["pipeline"] = self.pipeline_id  # safe; errors surface at bulk time
            if id_field and id_field in row and pd.notna(row[id_field]):
                action["_id"] = str(row[id_field])
            yield action

    def bulk_index_dataframe(self, df: pd.DataFrame, id_field: Optional[str] = None, chunk_size: int = 500) -> None:
        df = self._sanitize_dataframe(df)
        try:
            helpers.bulk(
                self.es,
                self._iter_actions(df, id_field),
                chunk_size=chunk_size,
                refresh="wait_for",
            )
        except BulkIndexError as bie:
            errors = getattr(bie, "errors", [])
            preview = errors[:3]
            raise RuntimeError(
                f"Bulk indexing failed for {len(errors)} documents. First errors: {preview}"
            ) from bie

    def bulk_index_file(self, csv_or_xlsx: Union[str, Path], id_field: Optional[str] = None) -> None:
        p = Path(csv_or_xlsx)
        if not p.exists():
            raise FileNotFoundError(p)
        if p.suffix.lower() == ".csv":
            df = pd.read_csv(p)
        elif p.suffix.lower() in (".xlsx", ".xls"):
            # openpyxl engine required for some environments
            df = pd.read_excel(p, engine="openpyxl")
        else:
            raise ValueError("Only .csv, .xlsx, or .xls are supported")
        self.bulk_index_dataframe(df, id_field=id_field)

    # --------------------------
    # Search
    # --------------------------
    def _build_body(self, question: str, size: int, include_elser: bool, fields_to_return: Optional[Sequence[str]]) -> Dict[str, Any]:
        should: List[Dict[str, Any]] = []
        # BM25 always present
        should.append({"match": {self.description_col: {"query": question, "boost": 0.6}}})
        # ELSER if requested
        if include_elser and self.use_ml_requested:
            should.append({
                "text_expansion": {
                    "ml.description_tokens": {
                        "model_id": self.model_id,
                        "model_text": question,
                    }
                }
            })

        body: Dict[str, Any] = {
            "size": size,
            "query": {"bool": {"should": should, "minimum_should_match": 1}},
        }
        if fields_to_return:
            body["_source"] = list(fields_to_return)
        return body

    def semantic_search(
        self,
        question: str,
        size: int = 10,
        hybrid: bool = True,
        fields_to_return: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        if not _coerce_str(question):
            raise ValueError("Provide a non-empty search question.")

        # Try ELSER + BM25 first; on API error, retry BM25-only
        try:
            body = self._build_body(question, size, include_elser=hybrid, fields_to_return=fields_to_return)
            res = self.es.search(index=self.index_name, body=body)
        except ApiError:
            body = self._build_body(question, size, include_elser=False, fields_to_return=fields_to_return)
            res = self.es.search(index=self.index_name, body=body)

        rows: List[Dict[str, Any]] = []
        for h in res.get("hits", {}).get("hits", []):
            src = h.get("_source", {})
            rows.append({"_score": h.get("_score", 0.0), **src})
        return pd.DataFrame(rows)
