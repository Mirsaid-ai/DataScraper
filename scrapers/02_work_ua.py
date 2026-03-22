"""
02_work_ua.py — Snapshot current job posting counts from Work.ua by region.

NOTE ON SCOPE: Work.ua only exposes currently active listings — no historical
archive is available. This scraper is designed for ONGOING monitoring (run it
periodically to build a forward-looking time series). For the 2021-2024
historical panel, use official Ukrstat/DSZ data (see 06_ukrstat.py).

Strategy: Hit the Work.ua city search page and parse the total-count string
("Зараз у нас N актуальних вакансій") shown at the top of results.

Usage:
    python scrapers/02_work_ua.py          # snapshot all 3 regions right now
    python scrapers/02_work_ua.py --test   # one region only

Outputs:
    data/raw/work_ua/YYYY_MM/<region>.html      — raw HTML snapshot
    data/clean/work_ua/work_ua_region_month.csv — appends each run
        schema: source | snapshot_date | region_raw | job_count
"""

import argparse
import logging
import re
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    CLEAN_WORK_UA, DELAY_SECONDS,
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

# URL: /jobs-<slug>/ works reliably; city_id numeric URLs redirect to slug
BASE_URL = "https://www.work.ua/jobs-{slug}/"

# Matches: "Зараз у нас 3 140 актуальних вакансій"
#          "3 140 вакансій у Харкові"
COUNT_RE = re.compile(r"([\d][\d\s\u00a0]{0,9})\s*(?:актуальних\s+)?вакансій", re.IGNORECASE)


def fetch_count(session: requests.Session, slug: str,
                region_name: str) -> tuple[int | None, str]:
    """Fetch Work.ua city page and extract current job count."""
    url = BASE_URL.format(slug=slug)
    try:
        resp = session.get(url, headers=HEADERS, timeout=20)
        html = resp.text

        if resp.status_code != 200:
            log.warning("HTTP %s for %s", resp.status_code, region_name)
            return None, html

        soup = BeautifulSoup(html, "lxml")
        count = _parse_count(soup)
        return count, html

    except requests.RequestException as exc:
        log.error("Request failed %s: %s", region_name, exc)
        return None, ""


def _parse_count(soup: BeautifulSoup) -> int | None:
    """Extract job count from Work.ua page."""
    for tag in soup.find_all(["h1", "h2", "h3", "p", "span", "div"]):
        text = tag.get_text(" ", strip=True)
        m = COUNT_RE.search(text)
        if m:
            raw = re.sub(r"[\s\u00a0]", "", m.group(1))
            try:
                val = int(raw)
                if 0 < val < 500_000:
                    return val
            except ValueError:
                pass
    return None


def save_raw_html(html: str, region_name: str, snapshot_date: date):
    folder = RAW_WORK_UA / f"{snapshot_date.year:04d}_{snapshot_date.month:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    safe = region_name.replace(" ", "_").replace("/", "-")
    (folder / f"{safe}.html").write_text(html, encoding="utf-8")


def run(regions: dict) -> pd.DataFrame:
    session = requests.Session()
    records: list[dict] = []
    today = date.today()

    for region_name, cfg in tqdm(regions.items(), desc="Work.ua"):
        count, html = fetch_count(session, cfg["slug"], region_name)

        if html:
            save_raw_html(html, region_name, today)

        if count is None:
            log.warning("No count parsed: %s", region_name)
        else:
            records.append({
                "source":        "work_ua",
                "snapshot_date": today.strftime("%Y-%m-%d"),
                "region_raw":    region_name,
                "job_count":     count,
            })

        time.sleep(DELAY_SECONDS)

    return pd.DataFrame(records)


def save_clean(df: pd.DataFrame) -> Path:
    CLEAN_WORK_UA.mkdir(parents=True, exist_ok=True)
    out = CLEAN_WORK_UA / "work_ua_region_month.csv"
    # Append to existing file if present
    if out.exists():
        existing = pd.read_csv(out)
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates(
            subset=["snapshot_date", "region_raw"]
        )
    df.to_csv(out, index=False)
    log.info("Clean data → %s  (%d rows total)", out, len(df))
    return out


def main():
    parser = argparse.ArgumentParser(description="Work.ua current job count snapshot")
    parser.add_argument("--test", action="store_true",
                        help="Test: Харківська only")
    args = parser.parse_args()

    regions = ({"Харківська": WORK_UA_REGION_IDS["Харківська"]}
               if args.test else WORK_UA_REGION_IDS)

    df = run(regions)

    if df.empty:
        log.warning("No data collected.")
        sys.exit(1)

    save_clean(df)

    print("\n── Work.ua snapshot summary ───────────────────────────")
    print(f"  Date           : {date.today()}")
    print(f"  Regions        : {df['region_raw'].tolist()}")
    print(f"  Counts         :")
    for _, row in df.iterrows():
        print(f"    {row['region_raw']:20s} {row['job_count']:,} active vacancies")
    print("───────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
