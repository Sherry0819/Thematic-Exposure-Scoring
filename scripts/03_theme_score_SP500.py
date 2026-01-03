#!/usr/bin/env python3
"""
Compute ThemeScore for each (sentence, theme) with two-day recency weighting.

ThemeScore(s,t) = (α * SemanticSim + β * PhoneticSim) * Polarity
Final100        = clip(100 * ThemeScore * TimeWeight, -100, +100)

TimeWeight = W_RECENT for newest doc_date, W_OLD for older docs
CompanyThemeScore = mean of Final100 across that company's sentences for that theme.
"""

import os
import math
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer, util
from transformers import pipeline
import phonetics
from rapidfuzz import fuzz
from datetime import date

import argparse

def parse_args():
    p = argparse.ArgumentParser(description="Compute theme scores and write results to Postgres.")
    p.add_argument("--dry-run", action="store_true", help="Compute scores but do not write to DB.")
    return p.parse_args()


# ---------------- ENV & CONFIG ----------------
DB = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", "5432")),
    dbname=os.getenv("PGDATABASE", "theme_project"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", ""),
)

EMBED_MODEL     = os.getenv("EMBED_MODEL", "all-MiniLM-L6-v2")
SENTIMENT_MODEL = os.getenv("SENT_MODEL", "distilbert-base-uncased-finetuned-sst-2-english")

ALPHA   = float(os.getenv("ALPHA", 0.8))
BETA    = float(os.getenv("BETA", 0.2))
W_RECENT = float(os.getenv("W_RECENT", 1.0))  # newest doc date
W_OLD    = float(os.getenv("W_OLD", 0.6))    # older doc date

SENT_FETCH_BATCH = int(os.getenv("SENT_FETCH_BATCH", 3000))
SENT_PIPE_BATCH  = int(os.getenv("SENT_PIPE_BATCH", 128))

# ---------------- HELPER FUNCTIONS ----------------
def metaphone_text(s: str) -> str:
    toks = [t for t in s.split() if t]
    enc = [phonetics.metaphone(t) or "" for t in toks]
    return " ".join(enc).strip()

def phonetic_sim(sentence: str, kw_text: str) -> float:
    if not kw_text:
        return 0.0
    ms = metaphone_text(sentence.lower())
    mt = metaphone_text(kw_text.lower())
    if not ms or not mt:
        return 0.0
    return fuzz.ratio(ms, mt) / 100.0  # [0,1]

def polarity_and_confidence(sentences, sent_pipe):
    """
    Map model labels to polarity in {-1, 0, +1}, scaled by confidence.

    - POSITIVE → +conf
    - NEGATIVE → -conf
    - NEUTRAL / OTHER → 0.0
    """
    out_pol, out_conf = [], []
    for i in range(0, len(sentences), SENT_PIPE_BATCH):
        chunk = sentences[i:i + SENT_PIPE_BATCH]
        res = sent_pipe(chunk, truncation=True)
        for r in res:
            label = str(r["label"]).upper()
            conf = float(r["score"])

            if "POS" in label:         # POSITIVE
                pol = +conf            # e.g. +0.95
            elif "NEG" in label:       # NEGATIVE
                pol = -conf            # e.g. -0.88
            else:                      # NEUTRAL or anything weird
                pol = 0.0

            out_pol.append(pol)
            out_conf.append(conf)
    return out_pol, out_conf



# ---------------- DATABASE HELPERS ----------------
DDL_SENTENCE = """
CREATE TABLE IF NOT EXISTS public.sentence_theme_scores_v2 (
  doc_id         text    NOT NULL,
  sentence_id    int     NOT NULL,
  company_id     text    NOT NULL,
  theme_id       text    NOT NULL,
  semantic_sim   double precision NOT NULL,
  phonetic_sim   double precision NOT NULL,
  polarity       double precision NOT NULL,
  confidence     double precision NOT NULL,
  alpha          double precision NOT NULL DEFAULT 0.8,
  beta           double precision NOT NULL DEFAULT 0.2,
  raw_base       double precision,
  time_weight    double precision,
  final_score_100 double precision,
  theme_score    double precision,
  scored_at      timestamp DEFAULT now(),
  PRIMARY KEY (doc_id, sentence_id, theme_id)
);
"""

DDL_COMPANY = """
CREATE TABLE IF NOT EXISTS public.company_theme_scores_v2 (
  company_id     text    NOT NULL,
  theme_id       text    NOT NULL,
  avg_score      double precision NOT NULL,
  n_sentences    int NOT NULL,
  avg_final_100  double precision,
  updated_at     timestamp DEFAULT now(),
  PRIMARY KEY (company_id, theme_id)
);
"""

def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_SENTENCE)
        cur.execute(DDL_COMPANY)
    conn.commit()

def newest_doc_date(conn):
    q = "SELECT MAX(date) AS newest FROM public.documents"
    newest = pd.read_sql(q, conn)["newest"][0]
    return pd.to_datetime(newest).date() if pd.notna(newest) else None

def sentence_batches(conn):
    total = pd.read_sql("SELECT COUNT(*) n FROM public.sentences", conn)["n"][0]
    off = 0
    print(f"Total sentences: {total}")
    while off < total:
        q = f"""
        SELECT s.doc_id, s.sentence_id, s.text, d.company_id, d.date AS doc_date
        FROM public.sentences s
        JOIN public.documents d USING(doc_id)
        ORDER BY s.doc_id, s.sentence_id
        OFFSET {off} LIMIT {SENT_FETCH_BATCH};
        """
        batch = pd.read_sql(q, conn)
        if batch.empty:
            break
        yield batch
        off += len(batch)

def upsert_sentence_scores(conn, rows):
    if not rows:
        return
    sql = """
    INSERT INTO public.sentence_theme_scores_v2
      (doc_id, sentence_id, company_id, theme_id,
       semantic_sim, phonetic_sim, polarity, confidence,
       alpha, beta, raw_base, time_weight, final_score_100, theme_score)
    VALUES %s
    ON CONFLICT (doc_id, sentence_id, theme_id)
    DO UPDATE SET
       semantic_sim     = EXCLUDED.semantic_sim,
       phonetic_sim     = EXCLUDED.phonetic_sim,
       polarity         = EXCLUDED.polarity,
       confidence       = EXCLUDED.confidence,
       alpha            = EXCLUDED.alpha,
       beta             = EXCLUDED.beta,
       raw_base         = EXCLUDED.raw_base,
       time_weight      = EXCLUDED.time_weight,
       final_score_100  = EXCLUDED.final_score_100,
       theme_score      = EXCLUDED.theme_score,
       scored_at        = now();
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=5000)

def refresh_company_theme_agg(conn):
    sql = """
    INSERT INTO public.company_theme_scores_v2 AS cts
      (company_id, theme_id, avg_score, n_sentences, avg_final_100, updated_at)
    SELECT company_id, theme_id,
           AVG(theme_score)      AS avg_score,
           COUNT(*)              AS n_sentences,
           AVG(final_score_100)  AS avg_final_100,
           now()
    FROM public.sentence_theme_scores_v2
    GROUP BY company_id, theme_id
    ON CONFLICT (company_id, theme_id)
    DO UPDATE SET
       avg_score     = EXCLUDED.avg_score,
       n_sentences   = EXCLUDED.n_sentences,
       avg_final_100 = EXCLUDED.avg_final_100,
       updated_at    = now();
    """
    with conn.cursor() as cur:
        cur.execute(sql)

# ---------------- MAIN PIPELINE ----------------
def main():
    args = parse_args()
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    try:
        ensure_tables(conn)
        themes = load_themes(conn)
        print(f"✅ Loaded {len(themes)} themes from database.")

        embedder = SentenceTransformer(EMBED_MODEL)
        sent_pipe = pipeline("sentiment-analysis", model=SENTIMENT_MODEL)
        theme_embs = embedder.encode(themes["theme_text"].tolist(), normalize_embeddings=True)
        theme_info = list(zip(themes["theme_id"], themes["theme_text"], themes["kw_text"], theme_embs))
        newest = newest_doc_date(conn)
        print(f"🕒 Most recent document date: {newest}")

        for batch_idx, batch in enumerate(sentence_batches(conn), 1):
            sents = batch["text"].tolist()
            print(f"\n📦 Batch {batch_idx}: {len(sents)} sentences fetched.")
            sent_embs = embedder.encode(sents, normalize_embeddings=True)
            polys, confs = polarity_and_confidence(sents, sent_pipe)

            # Assign time weights
            doc_dates = pd.to_datetime(batch["doc_date"]).dt.date
            time_weights = [
                W_RECENT if (newest and d == newest) else W_OLD for d in doc_dates
            ]

            for theme_id, theme_text, kw_text, tvec in theme_info:
                print(f" → Scoring theme {theme_id} ...")
                semantic = [max(0.0, float(x)) for x in util.cos_sim(sent_embs, tvec).cpu().numpy().ravel()]
                phon_list = [phonetic_sim(s, kw_text) if kw_text else 0.0 for s in sents]

                rows = []
                for (doc_id, sid, company_id), sem, pho, pol, conf, tw in zip(
                    batch[["doc_id", "sentence_id", "company_id"]].itertuples(index=False, name=None),
                    semantic, phon_list, polys, confs, time_weights
                ):
                    raw_base = (ALPHA * sem + BETA * pho) * (1 if pol >= 0 else -1)
                    final100 = max(-100.0, min(100.0, 100.0 * raw_base * tw))
                    rows.append((
                        doc_id, sid, company_id, theme_id,
                        float(sem), float(pho), int(pol), float(conf),
                        ALPHA, BETA, float(raw_base), float(tw), float(final100), float(raw_base)
                    ))

                if not args.dry_run:

                    upsert_sentence_scores(conn, rows)
            if not args.dry_run:
                conn.commit()
            print(f"✅ Committed batch {batch_idx} ({len(batch)} sentences).")

        if not args.dry_run:

            refresh_company_theme_agg(conn)
        if not args.dry_run:
            conn.commit()
        print("\n🎯 Done! Theme scoring completed" + (" (dry-run; no DB writes)." if args.dry_run else " and DB tables updated."))
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()

