"""
03_robota_ua.py — Snapshot current job posting counts from Robota.ua by oblast.

NOTE ON SCOPE: Like Work.ua, Robota.ua only exposes currently active listings —
no historical archive. This scraper is designed for ONGOING monitoring (run it
periodically to build a forward-looking time series). For 2021-2024 historical
data, use official Ukrstat/DSZ labour statistics.

Strategy:
  GET https://api.robota.ua/dictionary/city  → list of all cities, each with
  vacancyCount and centerId (= region/oblast ID).
  Sum vacancyCount by centerId for the 3 target oblasts.

  Region IDs: Харківська=21, Сумська=19, Чернігівська=25

Usage:
    python scrapers/03_robota_ua.py          # snapshot all 3 oblasts now
    python scrapers/03_robota_ua.py --test   # dry-run, print without saving

Outputs:
    data/clean/robota_ua/robota_ua_region_month.csv — appends each run
        schema: source | snapshot_date | region_raw | job_count
"""

import argparse
import logging
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import CLEAN_ROBOTA_UA, DELAY_SECONDS

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
    "Accept": "application/json",
}

CITY_DICT_URL = "https://api.robota.ua/dictionary/city"

# centerId → canonical oblast name
TARGET_OBLASTS = {
    19: "Сумська",
    21: "Харківська",
    25: "Чернігівська",
}


def fetch_oblast_counts() -> dict[str, int]:
    """
    Fetch city dictionary and sum vacancyCount by centerId for target oblasts.
    Returns {oblast_name: total_vacancies}.
    """
    resp = requests.get(CITY_DICT_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    cities = resp.json()

    sums: dict[int, int] = defaultdict(int)
    for city in cities:
        cid = city.get("centerId")
        if cid in TARGET_OBLASTS:
            sums[cid] += city.get("vacancyCount", 0)

    return {TARGET_OBLASTS[cid]: count for cid, count in sums.items()}


def save_clean(df: pd.DataFrame) -> Path:
    CLEAN_ROBOTA_UA.mkdir(parents=True, exist_ok=True)
    out = CLEAN_ROBOTA_UA / "robota_ua_region_month.csv"
    if out.exists():
        existing = pd.read_csv(out)
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates(
            subset=["snapshot_date", "region_raw"]
        )
    df.to_csv(out, index=False)
    log.info("Clean data → %s  (%d rows total)", out, len(df))
    return out


def main():
    parser = argparse.ArgumentParser(description="Robota.ua current job count snapshot")
    parser.add_argument("--test", action="store_true",
                        help="Dry-run: print counts without saving")
    args = parser.parse_args()

    today = date.today().strftime("%Y-%m-%d")

    log.info("Fetching city dictionary from Robota.ua API …")
    counts = fetch_oblast_counts()

    if not counts:
        log.error("No counts returned.")
        sys.exit(1)

    records = [
        {"source": "robota_ua", "snapshot_date": today,
         "region_raw": region, "job_count": count}
        for region, count in counts.items()
    ]
    df = pd.DataFrame(records)

    print("\n── Robota.ua snapshot summary ──────────────────────────")
    print(f"  Date    : {today}")
    for _, row in df.iterrows():
        print(f"  {row['region_raw']:20s} {row['job_count']:,} active vacancies")
    print("────────────────────────────────────────────────────────\n")

    if args.test:
        log.info("--test flag: not saving.")
        return

    save_clean(df)


if __name__ == "__main__":
    main()
