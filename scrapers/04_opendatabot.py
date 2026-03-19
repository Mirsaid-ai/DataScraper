"""
04_opendatabot.py — Download business registration counts from OpenDataBot.

Strategy:
  1. Try the OpenDataBot public API endpoint for analytics data.
  2. Fall back to HTML scraping of their analytics/statistics page.

Target fields: new_companies (ТОВ/ПрАТ etc.) and new_fops (ФОП) per region/month.

Usage:
    python scrapers/04_opendatabot.py
    python scrapers/04_opendatabot.py --test      # Kharkiv, 2022-01 only

Outputs:
    data/raw/opendatabot/YYYYMMDD_<type>.json  or  .html
    data/clean/opendatabot/opendatabot_region_month.csv
        schema: source | month | region_raw | new_companies | new_fops
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    CLEAN_OPENDATABOT, DATE_END, DATE_START, DELAY_SECONDS,
    RAW_OPENDATABOT, REGIONS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
}

# ── OpenDataBot endpoints (best-effort; verify current URLs before running) ──
# These endpoints were active as of early 2024; may require updating.
API_STATS_URL  = "https://opendatabot.ua/api/v2/stats"
HTML_STATS_URL = "https://opendatabot.ua/analytics"

# Oblast name variants used by OpenDataBot
OBLAST_MAP = {
    "Сумська":      ["Сумська", "Суми", "Sumy"],
    "Чернігівська": ["Чернігівська", "Чернігів", "Chernihiv"],
    "Харківська":   ["Харківська", "Харків", "Kharkiv"],
}


def month_range(start: str, end: str):
    s = pd.Period(start, "M")
    e = pd.Period(end, "M")
    p = s
    while p <= e:
        yield p.year, p.month
        p += 1


# ── Strategy 1: API ───────────────────────────────────────────────────────────

def try_api(session: requests.Session, region: str,
            year: int, month: int) -> dict | None:
    """
    Attempt to fetch monthly registration stats from OpenDataBot API.
    Returns dict with 'new_companies' and 'new_fops' or None on failure.
    """
    params = {
        "region": region,
        "year":   year,
        "month":  month,
    }
    try:
        resp = session.get(API_STATS_URL, params=params, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            # Response structure: {"companies": N, "fop": N, ...}  (inferred)
            companies = data.get("companies") or data.get("new_companies")
            fops      = data.get("fop") or data.get("new_fops")
            if companies is not None or fops is not None:
                return {
                    "new_companies": int(companies or 0),
                    "new_fops":      int(fops or 0),
                }
        log.debug("API returned %s for %s %d-%02d", resp.status_code, region, year, month)
    except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
        log.debug("API exception for %s %d-%02d: %s", region, year, month, exc)
    return None


# ── Strategy 2: HTML scraping ─────────────────────────────────────────────────

def try_html(session: requests.Session, region: str,
             year: int, month: int) -> tuple[dict | None, str]:
    """
    Scrape the OpenDataBot analytics HTML page.
    Returns (result_dict_or_None, raw_html).
    """
    params = {
        "region": region,
        "period": f"{year}-{month:02d}",
    }
    try:
        resp = session.get(HTML_STATS_URL, params=params, headers=HEADERS, timeout=20)
        html = resp.text

        if resp.status_code != 200:
            log.warning("HTML %s for %s %d-%02d", resp.status_code, region, year, month)
            return None, html

        soup = BeautifulSoup(html, "lxml")
        result = _parse_html_stats(soup)
        return result, html

    except requests.RequestException as exc:
        log.error("HTML fetch failed %s %d-%02d: %s", region, year, month, exc)
        return None, ""


def _parse_html_stats(soup: BeautifulSoup) -> dict | None:
    """
    Parse company/FOP counts from OpenDataBot analytics HTML.
    Tries several common patterns.
    """
    # Pattern 1: look for labelled stat blocks
    companies = _find_stat(soup, ["ТОВ", "компаній", "companies", "юридич"])
    fops      = _find_stat(soup, ["ФОП", "підприємц", "fop", "фізична особа"])

    if companies is not None or fops is not None:
        return {
            "new_companies": int(companies or 0),
            "new_fops":      int(fops or 0),
        }

    # Pattern 2: embedded JSON data
    for script in soup.find_all("script"):
        text = script.string or ""
        m_comp = re.search(r'"companies"\s*:\s*(\d+)', text)
        m_fop  = re.search(r'"fop"\s*:\s*(\d+)', text)
        if m_comp or m_fop:
            return {
                "new_companies": int(m_comp.group(1)) if m_comp else 0,
                "new_fops":      int(m_fop.group(1))  if m_fop  else 0,
            }

    return None


def _find_stat(soup: BeautifulSoup, keywords: list[str]) -> int | None:
    """Find a numeric value near any element containing a keyword."""
    for kw in keywords:
        for el in soup.find_all(string=re.compile(kw, re.IGNORECASE)):
            # Look in parent and sibling elements for a number
            parent = el.parent if el.parent else el
            for candidate in [parent] + list(parent.find_next_siblings())[:3]:
                text = candidate.get_text(" ", strip=True) if hasattr(candidate, "get_text") else str(candidate)
                nums = re.findall(r"\b(\d{1,7})\b", text.replace("\u00a0", ""))
                if nums:
                    try:
                        val = int(nums[0])
                        if 0 < val < 500_000:
                            return val
                    except ValueError:
                        pass
    return None


# ── Save helpers ──────────────────────────────────────────────────────────────

def save_raw_html(html: str, region: str, year: int, month: int):
    RAW_OPENDATABOT.mkdir(parents=True, exist_ok=True)
    stamp = datetime.today().strftime("%Y%m%d")
    safe  = region.replace(" ", "_")
    out   = RAW_OPENDATABOT / f"{stamp}_{safe}_{year}_{month:02d}.html"
    # Never overwrite
    if not out.exists():
        out.write_text(html, encoding="utf-8")


def save_raw_json(data: dict, region: str, year: int, month: int):
    RAW_OPENDATABOT.mkdir(parents=True, exist_ok=True)
    stamp = datetime.today().strftime("%Y%m%d")
    safe  = region.replace(" ", "_")
    out   = RAW_OPENDATABOT / f"{stamp}_{safe}_{year}_{month:02d}.json"
    if not out.exists():
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(regions: list[str], months: list[tuple[int, int]]) -> pd.DataFrame:
    session = requests.Session()
    records: list[dict] = []
    failed:  list[str]  = []

    combos = [(r, y, mo) for r in regions for y, mo in months]

    for region, year, month in tqdm(combos, desc="OpenDataBot"):
        result = try_api(session, region, year, month)
        source = "opendatabot_api"

        if result is None:
            log.debug("API miss for %s %d-%02d; trying HTML", region, year, month)
            result, html = try_html(session, region, year, month)
            source = "opendatabot_html"
            if html:
                save_raw_html(html, region, year, month)
        else:
            save_raw_json(result, region, year, month)

        if result is None:
            log.warning("No data: %s %d-%02d", region, year, month)
            failed.append(f"{region} {year}-{month:02d}")
        else:
            records.append({
                "source":        source,
                "month":         f"{year:04d}-{month:02d}",
                "region_raw":    region,
                "new_companies": result.get("new_companies", 0),
                "new_fops":      result.get("new_fops", 0),
            })

        time.sleep(DELAY_SECONDS)

    if failed:
        log.warning("No data for %d region-months: %s", len(failed), failed[:10])

    return pd.DataFrame(records)


def save_clean(df: pd.DataFrame) -> Path:
    CLEAN_OPENDATABOT.mkdir(parents=True, exist_ok=True)
    out = CLEAN_OPENDATABOT / "opendatabot_region_month.csv"
    df.to_csv(out, index=False)
    log.info("Clean data → %s  (%d rows)", out, len(df))
    return out


def main():
    parser = argparse.ArgumentParser(description="OpenDataBot business registration scraper")
    parser.add_argument("--test", action="store_true",
                        help="Test: Харківська, 2022-01 only")
    args = parser.parse_args()

    if args.test:
        regions = ["Харківська"]
        months  = [(2022, 1)]
        log.info("TEST MODE: Харківська, 2022-01")
    else:
        regions = REGIONS
        months  = list(month_range(DATE_START, DATE_END))

    df = run(regions, months)

    if df.empty:
        log.warning("No data collected — endpoints may have changed.")
        log.warning("Manual check: visit %s and %s", API_STATS_URL, HTML_STATS_URL)
        sys.exit(1)

    save_clean(df)

    print("\n── OpenDataBot summary ────────────────────────────────")
    print(f"  Rows collected  : {len(df):,}")
    print(f"  Regions         : {df['region_raw'].unique().tolist()}")
    print(f"  Month range     : {df['month'].min()} – {df['month'].max()}")
    print(f"  Total companies : {df['new_companies'].sum():,}")
    print(f"  Total FOPs      : {df['new_fops'].sum():,}")
    print("  Sample rows:")
    print(df.head(5).to_string(index=False))
    print("───────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
