"""
03_robota_ua.py — Scrape job posting counts from Robota.ua by region and month.

Strategy:
  1. First attempt: Robota.ua analytics/statistics page (aggregated data, preferred).
  2. Fallback: search results page with region + date filters, parse total count.

Usage:
    python scrapers/03_robota_ua.py                 # all regions, full date range
    python scrapers/03_robota_ua.py --test          # one region, one month

Outputs:
    data/raw/robota_ua/YYYY_MM/<region>.html
    data/clean/robota_ua/robota_ua_region_month.csv
        schema: source | month | region_raw | job_count
"""

import argparse
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    CLEAN_ROBOTA_UA, DATE_END, DATE_START, DELAY_SECONDS,
    RAW_ROBOTA_UA, ROBOTA_UA_REGION_SLUGS,
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
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
}

# ── URL templates ──────────────────────────────────────────────────────────────
# Search results URL with region slug and date filters
SEARCH_URL = "https://robota.ua/zapros/vacancy/{region_slug}"

# Robota.ua uses query params for date filtering
# (observed: ?scheduleIds=&salaryFrom=&publishedDateFrom=&publishedDateTo=)
DATE_PARAM_FROM = "publishedDateFrom"
DATE_PARAM_TO   = "publishedDateTo"

# Regex patterns to capture count from various Robota.ua page layouts
COUNT_PATTERNS = [
    re.compile(r"(\d[\d\s\u00a0]*)\s*вакансі", re.IGNORECASE),
    re.compile(r"Знайдено\s+([\d\s\u00a0]+)", re.IGNORECASE),
    re.compile(r'"totalCount"\s*:\s*(\d+)'),              # JSON in page source
    re.compile(r'data-count="(\d+)"'),
]


def month_range(start: str, end: str):
    """Yield (year, month) tuples from start to end inclusive."""
    s = pd.Period(start, "M")
    e = pd.Period(end, "M")
    p = s
    while p <= e:
        yield p.year, p.month
        p += 1


def _parse_count(html: str, soup: BeautifulSoup) -> int | None:
    """Try multiple strategies to extract a job count integer."""
    # Strategy 1: JSON embedded in script tags
    for pattern in COUNT_PATTERNS[2:]:     # JSON-based
        m = pattern.search(html)
        if m:
            raw = re.sub(r"[\s\u00a0]", "", m.group(1))
            try:
                return int(raw)
            except ValueError:
                pass

    # Strategy 2: visible text patterns
    for tag in soup.find_all(["h1", "h2", "h3", "strong", "span", "p", "div"]):
        text = tag.get_text(" ", strip=True)
        for pattern in COUNT_PATTERNS[:2]:
            m = pattern.search(text)
            if m:
                raw = re.sub(r"[\s\u00a0]", "", m.group(1))
                try:
                    val = int(raw)
                    # Sanity check: reject implausible values
                    if 0 <= val < 1_000_000:
                        return val
                except ValueError:
                    pass

    return None


def fetch_count(session: requests.Session, region_slug: str,
                year: int, month: int) -> tuple[int | None, str]:
    """Fetch Robota.ua search page for a region-month. Returns (count, html)."""
    period = pd.Period(f"{year}-{month}", "M")
    from_date = f"{year:04d}-{month:02d}-01"
    to_date   = f"{year:04d}-{month:02d}-{period.days_in_month:02d}"

    url = SEARCH_URL.format(region_slug=region_slug)
    params = {
        DATE_PARAM_FROM: from_date,
        DATE_PARAM_TO:   to_date,
    }

    try:
        resp = session.get(url, params=params, headers=HEADERS, timeout=20)
        html = resp.text

        if resp.status_code != 200:
            log.warning("HTTP %s for %s %d-%02d",
                        resp.status_code, region_slug, year, month)
            return None, html

        soup = BeautifulSoup(html, "lxml")
        count = _parse_count(html, soup)
        return count, html

    except requests.RequestException as exc:
        log.error("Request failed %s %d-%02d: %s", region_slug, year, month, exc)
        return None, ""


def save_raw_html(html: str, region_name: str, year: int, month: int):
    folder = RAW_ROBOTA_UA / f"{year:04d}_{month:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    safe = region_name.replace(" ", "_").replace("/", "-")
    (folder / f"{safe}.html").write_text(html, encoding="utf-8")


def run(regions: dict, months: list[tuple[int, int]]) -> pd.DataFrame:
    session = requests.Session()
    records: list[dict] = []
    failed:  list[str]  = []

    combos = [(r, slug, y, mo)
              for r, slug in regions.items()
              for y, mo in months]

    for region_name, slug, year, month in tqdm(combos, desc="Robota.ua"):
        count, html = fetch_count(session, slug, year, month)

        if html:
            save_raw_html(html, region_name, year, month)

        if count is None:
            log.warning("No count: %s %d-%02d", region_name, year, month)
            failed.append(f"{region_name} {year}-{month:02d}")
        else:
            records.append({
                "source":     "robota_ua",
                "month":      f"{year:04d}-{month:02d}",
                "region_raw": region_name,
                "job_count":  count,
            })

        time.sleep(DELAY_SECONDS)

    if failed:
        log.warning("Failed: %d region-months — %s", len(failed), failed[:10])

    return pd.DataFrame(records)


def save_clean(df: pd.DataFrame) -> Path:
    CLEAN_ROBOTA_UA.mkdir(parents=True, exist_ok=True)
    out = CLEAN_ROBOTA_UA / "robota_ua_region_month.csv"
    df.to_csv(out, index=False)
    log.info("Clean data → %s  (%d rows)", out, len(df))
    return out


def main():
    parser = argparse.ArgumentParser(description="Robota.ua job count scraper")
    parser.add_argument("--test", action="store_true",
                        help="Test mode: Харківська, 2022-01 only")
    args = parser.parse_args()

    if args.test:
        regions = {"Харківська": ROBOTA_UA_REGION_SLUGS["Харківська"]}
        months  = [(2022, 1)]
        log.info("TEST MODE: Харківська, 2022-01")
    else:
        regions = ROBOTA_UA_REGION_SLUGS
        months  = list(month_range(DATE_START, DATE_END))

    df = run(regions, months)

    if df.empty:
        log.warning("No data collected.")
        sys.exit(1)

    save_clean(df)

    print("\n── Robota.ua summary ──────────────────────────────────")
    print(f"  Rows collected : {len(df):,}")
    print(f"  Regions        : {df['region_raw'].unique().tolist()}")
    print(f"  Month range    : {df['month'].min()} – {df['month'].max()}")
    print(f"  Total jobs     : {df['job_count'].sum():,}")
    print("  Sample rows:")
    print(df.head(5).to_string(index=False))
    print("───────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
