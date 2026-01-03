#!/usr/bin/env python3
"""
02_clean_sentences_csv.py

Strict cleaning on sentence-level CSV:
- Normalize whitespace & allowed characters
- Filter by symbol ratio and digit ratio (computed on non-space characters)
- Trim extreme lengths (both tails)
- (Optional) drop duplicate text within each doc_id
- Reindex sentence_id within each doc_id from 0..n-1
"""
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

import pandas as pd

# keep letters/digits/space/comma/period
CLEAN_RE = re.compile(r"[^A-Za-z0-9\s,\.]")
SPACE_RE = re.compile(r"\s+")
# symbols = NOT alnum, NOT whitespace, NOT comma, NOT period
SYMBOL_RE = re.compile(r"[^A-Za-z0-9\s,\.]")


def clean_sentence(s: str) -> str:
    if not isinstance(s, str):
        s = "" if pd.isna(s) else str(s)
    s = CLEAN_RE.sub(" ", s)
    return SPACE_RE.sub(" ", s).strip()


def ratio_over(text: str, regex: re.Pattern) -> float:
    # ratio over non-space characters
    t = SPACE_RE.sub("", text)
    if not t:
        return 1.0
    return len(regex.findall(t)) / len(t)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default="data/interim/sentences.csv",
                    help="Input CSV with columns: doc_id,sentence_id,text")
    ap.add_argument("--out", dest="out_path", default="data/processed/sentences_clean.csv",
                    help="Output cleaned CSV")
    ap.add_argument("--symbol-thresh", type=float, default=0.70, help="Drop if symbol ratio > this")
    ap.add_argument("--digit-thresh", type=float, default=0.70, help="Drop if digit ratio > this")
    ap.add_argument("--trim-pct", type=float, default=0.01, help="Trim shortest/longest pct by length (each side)")
    ap.add_argument("--keep-dupe-text", action="store_true",
                    help="If set, do NOT drop duplicate text within each doc_id before reindexing.")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path.resolve()}")

    df = pd.read_csv(in_path, dtype={"doc_id": str, "sentence_id": "Int64", "text": str})

    before = len(df)

    # Basic hygiene
    df = df.dropna(subset=["doc_id", "text"]).copy()
    df["doc_id"] = df["doc_id"].astype(str)
    df["text"] = df["text"].astype(str)
    df = df[df["doc_id"].str.lower() != "doc_id"].copy()  # accidental header rows

    # Drop exact row duplicates early
    df = df.drop_duplicates().copy()

    # Clean text
    df["text"] = df["text"].map(clean_sentence)
    df = df[df["text"].str.len() > 0].copy()

    # Symbol ratio filter
    df["sym_ratio"] = df["text"].map(lambda s: ratio_over(s, SYMBOL_RE))
    df = df[df["sym_ratio"] <= float(args.symbol_thresh)].copy()

    # Digit ratio filter
    digit_re = re.compile(r"\d")
    df["dig_ratio"] = df["text"].map(lambda s: ratio_over(s, digit_re))
    df = df[df["dig_ratio"] <= float(args.digit_thresh)].copy()

    # Length trimming
    df["len"] = df["text"].str.len()
    if len(df) > 0 and float(args.trim_pct) > 0:
        p = float(args.trim_pct)
        p_low = df["len"].quantile(p)
        p_high = df["len"].quantile(1.0 - p)
        df = df[(df["len"] >= p_low) & (df["len"] <= p_high)].copy()

    # Optional: dedupe text within doc_id
    if not bool(args.keep_dupe_text):
        df = df.sort_values(["doc_id", "sentence_id"]).copy()
        df = df.drop_duplicates(subset=["doc_id", "text"]).copy()

    # Reindex sentence_id within each doc_id
    df = df.sort_values(["doc_id", "sentence_id"]).copy()
    df["sentence_id"] = df.groupby("doc_id").cumcount()

    # Write
    out_path = Path(args.out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cols = ["doc_id", "sentence_id", "text"]
    df[cols].to_csv(out_path, index=False, encoding="utf-8",
                    lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

    after = len(df)
    print(f"Wrote: {out_path.resolve()}  (rows {before} -> {after})")


if __name__ == "__main__":
    main()
