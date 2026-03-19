"""
02_work_ua.py — Scrape job posting counts from Work.ua by region and month.

Strategy: Hit the Work.ua search page with oblast + period filters and parse
the total-count string ("Знайдено N вакансій") shown at the top of results.
We do NOT scrape individual job ads — only the aggregate count per page.

Usage:
    python scrapers/02_work_ua.py                  # all regions, full date range
    python scrapers/02_work_ua.py --test           # one region, one month only

Outputs:
    data/raw/work_ua/YYYY_MM/<region>.html   — raw HTML per region-month
    data/clean/work_ua/work_ua_region_month.csv
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
    CLEAN_WORK_UA, DELAY_SECONDS, DATE_END, DATE_START,
    RAW_WORK_UA, WORK_UA_REGION_IDS,
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

# Work.ua search URL:
# https://www.work.ua/jobs-<city_id>/?period=<days>
# Unfortunately Work.ua doesn't expose a clean month-range filter in its
# public search URL, so we use their "period" param (30 = last 30 days)
# and anchor to the 1st of each month by querying on that date.
# A more reliable workaround: use the "from_date" + "to_date" hidden params
# observed in the XHR requests.
BASE_URL = "https://www.work.ua/jobs-{city_id}/"

# Regex to capture the total count from strings like:
#   "Знайдено 1\u00a0234 вакансій"   (non-breaking space as thousands sep)
#   "Знайдено 567 вакансій"
COUNT_RE = re.compile(r"Знайдено\s+([\d\s\u00a0]+)\s*вакансій", re.IGNORECASE)


def month_range(start: str, end: str):
    """Yield (year, month) tuples from start to end inclusive."""
    s = pd.Period(start, "M")
    e = pd.Period(end, "M")
    p = s
    while p <= e:
        yield p.year, p.month
        p += 1


def fetch_count(session: requests.Session, city_id: int,
                year: int, month: int) -> tuple[int | None, str]:
    """
    Fetch Work.ua search results page for city_id filtered to a specific month.
    Returns (count_or_None, raw_html).
    """
    from_date = f"{year:04d}-{month:02d}-01"
    last_day  = pd.Period(f"{year}-{month}", "M").days_in_month
    to_date   = f"{year:04d}-{month:02d}-{last_day:02d}"

    url = BASE_URL.format(city_id=city_id)
    params = {
        "from_date": from_date,
        "to_date":   to_date,
    }

    try:
        resp = session.get(url, params=params, headers=HEADERS, timeout=20)
        html = resp.text

        if resp.status_code != 200:
            log.warning("HTTP %s for city_id=%s %d-%02d",
                        resp.status_code, city_id, year, month)
            return None, html

        soup = BeautifulSoup(html, "lxml")

        # Primary: look for the count in the H1 / strong tag
        count = _parse_count_from_soup(soup)
        return count, html

    except requests.RequestException as exc:
        log.error("Request failed city_id=%s %d-%02d: %s", city_id, year, month, exc)
        return None, ""


def _parse_count_from_soup(soup: BeautifulSoup) -> int | None:
    """Try multiple selectors to extract total job count from Work.ua page."""
    # Selector 1: the "Знайдено N вакансій" heading
    for tag in soup.find_all(["h1", "h2", "strong", "span", "p"]):
        text = tag.get_text(" ", strip=True)
        m = COUNT_RE.search(text)
        if m:
            raw = re.sub(r"[\s\u00a0]", "", m.group(1))
            return int(raw)

    # Selector 2: data-count attribute (observed in some Work.ua markup)
    el = soup.find(attrs={"data-count": True})
    if el:
        try:
            return int(el["data-count"])
        except (ValueError, TypeError):
            pass

    return None


def save_raw_html(html: str, region_name: str, year: int, month: int):
    """Save raw HTML to data/raw/work_ua/YYYY_MM/<region>.html"""
    folder = RAW_WORK_UA / f"{year:04d}_{month:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    safe_name = region_name.replace(" ", "_").replace("/", "-")
    out = folder / f"{safe_name}.html"
    out.write_text(html, encoding="utf-8")


def run(regions: dict, months: list[tuple[int, int]]) -> pd.DataFrame:
    """Main scrape loop. Returns tidy DataFrame."""
    session = requests.Session()
    records: list[dict] = []
    failed: list[str] = []

    combos = [(r, cfg, y, mo)
              for r, cfg in regions.items()
              for y, mo in months]

    for region_name, cfg, year, month in tqdm(combos, desc="Work.ua"):
        count, html = fetch_count(session, cfg["city_id"], year, month)

        if html:
            save_raw_html(html, region_name, year, month)

        if count is None:
            log.warning("No count parsed: %s %d-%02d", region_name, year, month)
            failed.append(f"{region_name} {year}-{month:02d}")
        else:
            records.append({
                "source":     "work_ua",
                "month":      f"{year:04d}-{month:02d}",
                "region_raw": region_name,
                "job_count":  count,
            })

        time.sleep(DELAY_SECONDS)

    if failed:
        log.warning("Failed to parse count for %d region-months: %s",
                    len(failed), failed[:10])

    return pd.DataFrame(records)


def save_clean(df: pd.DataFrame) -> Path:
    CLEAN_WORK_UA.mkdir(parents=True, exist_ok=True)
    out = CLEAN_WORK_UA / "work_ua_region_month.csv"
    df.to_csv(out, index=False)
    log.info("Clean data saved → %s  (%d rows)", out, len(df))
    return out


def main():
    parser = argparse.ArgumentParser(description="Work.ua job count scraper")
    parser.add_argument("--test", action="store_true",
                        help="Test mode: one region (Харківська), one month (2022-01)")
    args = parser.parse_args()

    if args.test:
        regions = {"Харківська": WORK_UA_REGION_IDS["Харківська"]}
        months  = [(2022, 1)]
        log.info("TEST MODE: Харківська, 2022-01 only")
    else:
        regions = WORK_UA_REGION_IDS
        months  = list(month_range(DATE_START, DATE_END))

    df = run(regions, months)

    if df.empty:
        log.warning("No data collected — check selectors or network access.")
        sys.exit(1)

    save_clean(df)

    print("\n── Work.ua summary ────────────────────────────────────")
    print(f"  Rows collected   : {len(df):,}")
    print(f"  Regions          : {df['region_raw'].unique().tolist()}")
    print(f"  Month range      : {df['month'].min()} – {df['month'].max()}")
    print(f"  Total job-months : {df['job_count'].sum():,}")
    print("  Sample rows:")
    print(df.head(5).to_string(index=False))
    print("───────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
