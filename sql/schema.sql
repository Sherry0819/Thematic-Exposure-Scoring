-- Schema (DDL) for sharing in GitHub (structure only; no private data)

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

-- Outputs
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

CREATE TABLE IF NOT EXISTS public.company_theme_scores_v2 (
  company_id     text    NOT NULL,
  theme_id       text    NOT NULL,
  avg_score      double precision NOT NULL,
  n_sentences    int NOT NULL,
  avg_final_100  double precision,
  updated_at     timestamp DEFAULT now(),
  PRIMARY KEY (company_id, theme_id)
);
