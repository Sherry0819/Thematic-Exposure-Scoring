#!/usr/bin/env python3
"""
01_build_sentences_csv.py

Read sp500_filings_table.csv, read each filing (local if present; otherwise fetch from SEC),
convert HTML -> text, split into sentences, do *light* cleaning, then write:

  doc_id, sentence_id, text

This step is intentionally conservative; stricter filtering & dedup should happen in
02_clean_sentences_csv.py.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

SEC_ARCHIVES_BASE = "https://www.sec.gov"


def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
    return s


# Keep only alnum, whitespace, comma, period; collapse spaces
CLEAN_RE = re.compile(r"[^A-Za-z0-9\s,\.]")
SPACE_RE = re.compile(r"\s+")
# Sentence split: ., !, ?, ; or line breaks
SPLIT_RE = re.compile(r"[\.!?;]+|\n+")


def accession_from_path(p: Path) -> str:
    """
    Extract accession with NO dashes from the expected path:
      <base>/<TICKER>/<FORM>/<ACCESSION_NODASH>/<primary_doc>
    """
    try:
        return p.parent.name
    except Exception:
        return ""


def build_sec_url(cik: str, acc_nodash: str, filename: str) -> str:
    return f"{SEC_ARCHIVES_BASE}/Archives/edgar/data/{int(cik)}/{acc_nodash}/{filename}"


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ")
    return SPACE_RE.sub(" ", text).strip()


def read_local_or_download(session: requests.Session, file_path: str, cik: str) -> str:
    """
    If the local file exists, read it. Otherwise reconstruct the SEC URL from the path and download.
    """
    p = Path(file_path).expanduser()
    if p.exists():
        try:
            return p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return p.read_text(encoding="latin-1", errors="ignore")

    filename = p.name
    acc_nodash = accession_from_path(p)
    if not filename or not acc_nodash:
        raise FileNotFoundError(f"Cannot reconstruct SEC URL from file_path: {file_path}")

    url = build_sec_url(cik, acc_nodash, filename)
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text


def split_sentences(text: str) -> Iterable[str]:
    for s in SPLIT_RE.split(text):
        s = SPACE_RE.sub(" ", s).strip()
        if s:
            yield s


def clean_sentence(s: str) -> str:
    s = CLEAN_RE.sub(" ", s)
    return SPACE_RE.sub(" ", s).strip()


def process_document(session: requests.Session, doc_id: str, cik: str, file_path: str, min_len: int) -> List[Tuple[str, int, str]]:
    try:
        raw = read_local_or_download(session, file_path, cik)
    except Exception as e:
        sys.stderr.write(f"[WARN] {doc_id}: cannot read/fetch {file_path}: {e}\n")
        return []

    text = html_to_text(raw)
    out_rows: List[Tuple[str, int, str]] = []
    sid = 0
    for s in split_sentences(text):
        s = clean_sentence(s)
        if len(s) < min_len:
            continue
        out_rows.append((doc_id, sid, s))
        sid += 1
    return out_rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-table", default="data/raw/sp500_filings_table.csv", help="Input filings table CSV")
    ap.add_argument("--out", default="data/interim/sentences.csv", help="Output sentences CSV")
    ap.add_argument("--user-agent", required=True, help='SEC-compliant UA, e.g. "Name email@domain.com"')
    ap.add_argument("--limit", type=int, default=0, help="Process only first N documents (0 = no limit)")
    ap.add_argument("--min-len", type=int, default=20, help="Drop sentences shorter than this (after light cleaning)")
    args = ap.parse_args()

    in_table = Path(args.in_table)
    if not in_table.exists():
        raise FileNotFoundError(f"Input table not found: {in_table.resolve()} (run scripts/00_sp500_filings_table.py first)")

    df = pd.read_csv(in_table, dtype=str)
    needed = {"doc_id", "cik", "file_path"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {in_table}: {sorted(missing)}")

    if args.limit and args.limit > 0:
        df = df.head(args.limit)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    session = make_session(args.user_agent)

    all_rows: List[Tuple[str, int, str]] = []
    for _, r in tqdm(df.iterrows(), total=len(df), desc="Processing documents"):
        all_rows.extend(process_document(session, r["doc_id"], r["cik"], r["file_path"], int(args.min_len)))

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        w.writerow(["doc_id", "sentence_id", "text"])
        w.writerows(all_rows)

    print(f"Done. Wrote: {out_path.resolve()}  (rows={len(all_rows)})")


if __name__ == "__main__":
    main()
