# SEC Filings → Sentences Dataset (S&P 500)

This repo provides a small, reproducible pipeline to:
1) Build a table of the latest S&P 500 filings (10-K/10-Q) and optionally download the primary filing HTML files.
2) Convert filings into a sentence-level CSV.
3) Clean/filter/deduplicate sentences into a modeling-ready dataset.

## Quickstart

### 0) Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 1) Build filings table (and optionally download filings)
```bash
python scripts/00_sp500_filings_table.py \
  --out data/raw/sp500_filings_table.csv \
  --base-dir data/raw/SEC_Filings \
  --download-files \
  --user-agent "Your Name your@email.com"
```

### 2) Build sentence CSV
```bash
python scripts/01_build_sentences_csv.py \
  --in-table data/raw/sp500_filings_table.csv \
  --out data/interim/sentences.csv \
  --user-agent "Your Name your@email.com"
```

### 3) Clean + filter + reindex
```bash
python scripts/02_clean_sentences_csv.py \
  --in data/interim/sentences.csv \
  --out data/processed/sentences_clean.csv
```

## Outputs

- `data/raw/sp500_filings_table.csv`: one row per filing with `doc_id`, `ticker`, `cik`, `date`, `file_path`, ...
- `data/interim/sentences.csv`: raw-ish sentences (`doc_id`, `sentence_id`, `text`)
- `data/processed/sentences_clean.csv`: cleaned & reindexed sentences

## Notes
- SEC requests must include a descriptive `User-Agent` with contact info.
- Do **not** commit downloaded filings or large datasets to GitHub. See `.gitignore`.


## Step 3 — Compute Theme Scores (Postgres)

This step assumes you **store your cleaned sentences in Postgres** (recommended for scaling & querying).

1) Load CSVs into Postgres (creates core tables if missing):

```bash
python scripts/02b_load_to_postgres.py --create-core-tables
```

2) Add themes (one-time):
- Insert rows into `public.themes (theme_id, theme, keywords)`
- Keep the **table structure** in GitHub (`sql/schema.sql`), but do **not** commit sensitive data.

3) Run scoring:

```bash
python scripts/03_theme_score_SP500.py
```

Outputs:
- `public.sentence_theme_scores_v2` (per sentence × theme, with interpretability)
- `public.company_theme_scores_v2` (company × theme aggregate)

## How to Showcase SQL Data on GitHub (without leaking data)

Recommended “portfolio-friendly” approach:

- ✅ Commit **schema only**: `sql/schema.sql`
- ✅ Commit **example queries**: `sql/sample_queries.sql`
- ✅ Add a small **public sample dataset** (e.g., 2–3 docs, 100 sentences) under `data/sample/`
- ✅ In README, include:
  - an ERD screenshot (export from pgAdmin / dbdiagram)
  - 2–3 query outputs (small, anonymized)
  - a “How to reproduce locally” section (`docker-compose.yml` + `.env.example`)

Tip: if your real data is large/private, provide a **`make demo`** style path:
- start Postgres via `docker compose up`
- load `data/sample/*`
- run `scripts/03_theme_score_SP500.py --dry-run` or against the sample DB
