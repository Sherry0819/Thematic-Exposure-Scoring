-- Example queries you can show in README / interviews (safe, no raw data needed)

-- 1) How many documents & sentences?
SELECT COUNT(*) AS n_docs FROM public.documents;
SELECT COUNT(*) AS n_sents FROM public.sentences;

-- 2) Top themes for a company (by avg_final_100)
SELECT c.theme_id, t.theme, c.avg_final_100, c.n_sentences
FROM public.company_theme_scores_v2 c
JOIN public.themes t USING(theme_id)
WHERE c.company_id = 'AAPL'
ORDER BY c.avg_final_100 DESC
LIMIT 10;

-- 3) Evidence sentences for one theme (showcase interpretability)
SELECT s.doc_id, s.sentence_id, s.text, st.final_score_100
FROM public.sentence_theme_scores_v2 st
JOIN public.sentences s USING(doc_id, sentence_id)
WHERE st.company_id = 'AAPL' AND st.theme_id = 'AI'
ORDER BY st.final_score_100 DESC
LIMIT 20;
