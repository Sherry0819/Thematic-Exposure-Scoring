#!/usr/bin/env python3
"""
02b_load_to_postgres.py

Load:
- documents metadata from data/raw/sp500_filings_table.csv
- cleaned sentences from data/processed/sentences_clean.csv

Into Postgres tables:
- public.documents (doc_id, company_id, ticker, cik, date, file_path, source_type)
- public.sentences (doc_id, sentence_id, text)

This keeps the pipeline coherent with 03_theme_score_SP500.py, which expects these tables.

Notes:
- This script uses UPSERTs so it is safe to rerun.
- Keep large raw filings out of Git; store under data/raw and ignore via .gitignore.
"""

from __future__ import annotations

import argparse
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5432")),
    dbname=os.getenv("PGDATABASE", "theme_project"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", ""),
)

DDL_CORE = """
CREATE TABLE IF NOT EXISTS public.documents (
  doc_id      text PRIMARY KEY,
  company_id  text,
  ticker      text,
  cik         text,
  source_type text,
  date        date,
  file_path   text
);

CREATE TABLE IF NOT EXISTS public.sentences (
  doc_id       text NOT NULL,
  sentence_id  int  NOT NULL,
  text         text NOT NULL,
  PRIMARY KEY (doc_id, sentence_id),
  FOREIGN KEY (doc_id) REFERENCES public.documents(doc_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.themes (
  theme_id  text PRIMARY KEY,
  theme     text NOT NULL,
  keywords  text
);
"""

def parse_args():
    p = argparse.ArgumentParser(description="Load documents + cleaned sentences CSVs into Postgres.")
    p.add_argument("--documents-csv", default="data/raw/sp500_filings_table.csv")
    p.add_argument("--sentences-csv", default="data/processed/sentences_clean.csv")
    p.add_argument("--schema", default=os.getenv("PGSCHEMA", "public"))
    p.add_argument("--create-core-tables", action="store_true",
                   help="Create core tables (documents, sentences, themes) if missing.")
    return p.parse_args()

def main():
    args = parse_args()
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        if args.create_core_tables:
            with conn.cursor() as cur:
                cur.execute(DDL_CORE.replace("public.", f"{args.schema}."))
            conn.commit()

        docs = pd.read_csv(args.documents_csv, dtype=str)
        # minimal column normalization
        required = {"doc_id","company_id","ticker","cik","source_type","date","file_path"}
        missing = required - set(docs.columns)
        if missing:
            raise ValueError(f"documents CSV missing columns: {sorted(missing)}")

        docs = docs[list(required)].dropna(subset=["doc_id"]).copy()
        docs["doc_id"] = docs["doc_id"].astype(str)

        sents = pd.read_csv(args.sentences_csv, dtype={"doc_id": str, "sentence_id": int, "text": str})
        required_s = {"doc_id","sentence_id","text"}
        missing_s = required_s - set(sents.columns)
        if missing_s:
            raise ValueError(f"sentences CSV missing columns: {sorted(missing_s)}")
        sents = sents.dropna(subset=["doc_id","text"]).copy()
        sents["doc_id"] = sents["doc_id"].astype(str)

        with conn.cursor() as cur:
            sql_docs = f"""
            INSERT INTO {args.schema}.documents (doc_id, company_id, ticker, cik, source_type, date, file_path)
            VALUES %s
            ON CONFLICT (doc_id) DO UPDATE SET
              company_id  = EXCLUDED.company_id,
              ticker      = EXCLUDED.ticker,
              cik         = EXCLUDED.cik,
              source_type = EXCLUDED.source_type,
              date        = EXCLUDED.date,
              file_path   = EXCLUDED.file_path;
            """
            execute_values(cur, sql_docs, docs.values.tolist(), page_size=2000)

            sql_sents = f"""
            INSERT INTO {args.schema}.sentences (doc_id, sentence_id, text)
            VALUES %s
            ON CONFLICT (doc_id, sentence_id) DO UPDATE SET
              text = EXCLUDED.text;
            """
            execute_values(cur, sql_sents, sents[["doc_id","sentence_id","text"]].values.tolist(), page_size=5000)

        conn.commit()
        print(f"✅ Loaded {len(docs)} documents and {len(sents)} sentences into {args.schema}.documents / {args.schema}.sentences")
        print("ℹ️ Next: run scripts/03_theme_score_SP500.py to compute scores.")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
