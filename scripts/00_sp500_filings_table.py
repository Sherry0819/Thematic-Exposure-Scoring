#!/usr/bin/env python3
"""
00_sp500_filings_table.py

Build a table of the *most recent* filings (default: 10-K/10-Q) for S&P 500 constituents.
Optionally download the primary filing HTML files to a local folder.

Output CSV columns:
  doc_id, company_id, ticker, cik, source_type, date, file_path
"""
from __future__ import annotations

import argparse
import csv
import math
import time
from pathlib import Path
from typing import Optional, List, Sequence

import pandas as pd
import requests

SEC_DATA_BASE = "https://data.sec.gov"
SEC_ARCHIVES_BASE = "https://www.sec.gov"
DEFAULT_SLEEP_SECS = 0.2


def cik10(n: int) -> str:
    return f"{int(n):010d}"


def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
    return s


def fetch_json(session: requests.Session, url: str, timeout: int = 30) -> Optional[dict]:
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def sp500_list() -> pd.DataFrame:
    """
    Fetch current S&P 500 list from Wikipedia.

    Notes:
      - Wikipedia sometimes 403s; we use a browser-like UA.
      - Tickers like BRK.B appear as BRK.B; SEC uses BRK-B, so we normalize '.' -> '-'.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code == 403:
        r = requests.get("https://en.m.wikipedia.org/wiki/List_of_S%26P_500_companies", headers=headers, timeout=30)
    r.raise_for_status()

    tables = pd.read_html(r.text)
    df = tables[0].rename(columns={"Symbol": "ticker", "Security": "name"})
    df["ticker"] = df["ticker"].astype(str).str.replace(".", "-", regex=False)
    return df[["ticker", "name"]]


def sec_ticker_map(session: requests.Session) -> pd.DataFrame:
    """
    SEC provides a mapping file of company tickers to CIK.
    """
    data = fetch_json(session, f"{SEC_ARCHIVES_BASE}/files/company_tickers.json") or {}
    rows = [{"ticker": v["ticker"], "cik_num": int(v["cik_str"])} for v in data.values()]
    return pd.DataFrame(rows)


def most_recent(subs: dict, form: str) -> Optional[dict]:
    if not subs or "filings" not in subs or "recent" not in subs["filings"]:
        return None
    df = pd.DataFrame(subs["filings"]["recent"])
    if df.empty:
        return None
    df = df[df["form"] == form]
    if df.empty:
        return None
    return df.sort_values("filingDate", ascending=False).head(1).iloc[0].to_dict()


def primary_url(cik: int, accession: str, doc: str) -> str:
    acc = accession.replace("-", "")
    return f"{SEC_ARCHIVES_BASE}/Archives/edgar/data/{int(cik)}/{acc}/{doc}"


def local_path(base: Path, ticker: str, form: str, accession: str, doc: str) -> Path:
    # Store as: <base>/<TICKER>/<FORM>/<ACCESSION_NODASH>/<primary_doc>
    return base / ticker / form / accession.replace("-", "") / doc


def build_table(
    session: requests.Session,
    *,
    download_files: bool,
    base_dir: Path,
    forms: Sequence[str],
    sleep_secs: float,
) -> pd.DataFrame:
    base_dir = base_dir.expanduser()
    sp = sp500_list().merge(sec_ticker_map(session), on="ticker", how="left")

    rows: List[dict] = []
    k = 1
    for _, r in sp.iterrows():
        cik = r.get("cik_num")
        ticker = r.get("ticker")
        if not isinstance(ticker, str) or not ticker:
            continue
        if not (isinstance(cik, (int, float)) and not math.isnan(cik)):
            continue
        cik = int(cik)

        subs = fetch_json(session, f"{SEC_DATA_BASE}/submissions/CIK{cik10(cik)}.json")
        time.sleep(sleep_secs)

        for form in forms:
            f = most_recent(subs, form)
            if not f:
                continue

            acc, dt, doc = f["accessionNumber"], f["filingDate"], f["primaryDocument"]
            path = local_path(base_dir, ticker, form, acc, doc)

            if download_files:
                try:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    resp = session.get(primary_url(cik, acc, doc), timeout=60)
                    resp.raise_for_status()
                    path.write_bytes(resp.content)
                    time.sleep(sleep_secs)
                except Exception:
                    # Keep the row even if download fails; downstream can re-fetch.
                    pass

            rows.append(
                {
                    "doc_id": f"D{k:03d}",
                    "company_id": str(cik),
                    "ticker": ticker,
                    "cik": str(cik),
                    "source_type": form,
                    "date": dt,
                    "file_path": str(path),
                }
            )
            k += 1

    return pd.DataFrame(rows, columns=["doc_id", "company_id", "ticker", "cik", "source_type", "date", "file_path"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/sp500_filings_table.csv", help="Output CSV path")
    ap.add_argument("--base-dir", default="data/raw/SEC_Filings", help="Folder to store downloaded filings")
    ap.add_argument("--download-files", action="store_true", help="Download primary filing HTML files")
    ap.add_argument("--forms", default="10-K,10-Q", help="Comma-separated forms (default: 10-K,10-Q)")
    ap.add_argument("--sleep-secs", type=float, default=DEFAULT_SLEEP_SECS, help="Polite delay between SEC requests")
    ap.add_argument("--user-agent", required=True, help='SEC-compliant UA, e.g. "Name email@domain.com"')
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    session = make_session(args.user_agent)
    forms = [f.strip() for f in str(args.forms).split(",") if f.strip()]
    df = build_table(
        session,
        download_files=bool(args.download_files),
        base_dir=Path(args.base_dir),
        forms=forms,
        sleep_secs=float(args.sleep_secs),
    )
    df.to_csv(out, index=False, encoding="utf-8", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote: {out.resolve()} (rows={len(df)})")


if __name__ == "__main__":
    main()
