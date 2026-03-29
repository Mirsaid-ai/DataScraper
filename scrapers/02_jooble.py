"""
02_jooble.py — Job posting counts from ua.jooble.org by oblast.

HISTORICAL DATA STATUS
──────────────────────
Jooble (like work.ua and robota.ua) provides no historical archive.  The
official REST API returns only *currently active* listings; there is no
date-range filter.  The Wayback Machine contains 9 snapshots of the
all-Ukraine SearchResult page (2021-2024) but zero oblast-specific snapshots,
so oblast-level historical counts cannot be recovered.

MODES
──────
  api      Use the Jooble REST API (requires free key from jooble.org/api/about).
           Returns `totalCount` of current active vacancies per oblast.
           Designed as an ongoing snapshot monitor — run monthly to build a
           forward-looking panel column.

  wayback  Query the Wayback Machine CDX API for archived Jooble pages.
           For each snapshot found, fetches the HTML and extracts the Ukraine-
           wide vacancy count.  Yields up to ~9 national data points from
           2021-2024 — useful as a coarse national-trend reference.

Usage:
    python scrapers/02_jooble.py --mode api --key YOUR_JOOBLE_KEY
    python scrapers/02_jooble.py --mode api --key YOUR_JOOBLE_KEY --test
    python scrapers/02_jooble.py --mode wayback
    python scrapers/02_jooble.py --mode wayback --test

Outputs (appended; deduplicated on snapshot_date + region_raw):
    data/clean/jooble/jooble_oblast_snapshot.csv
        schema: source | mode | snapshot_date | region_raw | job_count
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    CLEAN_JOOBLE,
    DELAY_SECONDS,
    JOOBLE_API_BASE,
    JOOBLE_OBLAST_LOCATIONS,
    JOOBLE_SEARCH_URL,
    REGIONS_EN,
    WAYBACK_CDX_URL,
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
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
}

# Patterns tried in order to extract the total vacancy count from Jooble HTML.
# Jooble changed its page structure over 2021-2024:
#   - 2021-2022: visible text "98 056 вакансій" / "45 121 вакансія"
#   - 2023+:     embedded JSON field "activeJobsCount":42900 in script block
_COUNT_PATTERNS: list[re.Pattern] = [
    # Visible text — match any grammatical form: вакансій/вакансія/вакансії
    re.compile(r"(\d[\d\s\u00a0\xa0]*)\s*вакансі[яій]", re.IGNORECASE),
    # JSON fields embedded in <script> blocks (2023+ structure)
    re.compile(r'"activeJobsCount"\s*:\s*(\d+)', re.IGNORECASE),
    re.compile(r'"jobsCount"\s*:\s*(\d+)', re.IGNORECASE),
    re.compile(r'"totalCount"\s*:\s*(\d+)', re.IGNORECASE),
    re.compile(r'"vacanciesCount"\s*:\s*(\d+)', re.IGNORECASE),
]


# ── Jooble REST API mode ──────────────────────────────────────────────────────

def _api_get_count(location: str, api_key: str, session: requests.Session) -> int | None:
    """
    POST to the Jooble API for a given location and return totalCount.
    Returns None on failure.
    """
    url = f"{JOOBLE_API_BASE}/{api_key}"
    payload = {"keywords": "", "location": location, "page": "1"}
    try:
        resp = session.post(url, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return int(data.get("totalCount", 0))
    except requests.exceptions.HTTPError as exc:
        log.error("HTTP error for location '%s': %s", location, exc)
    except Exception as exc:
        log.error("Unexpected error for location '%s': %s", location, exc)
    return None


def run_api_mode(api_key: str) -> pd.DataFrame:
    """
    Query the Jooble REST API for each target oblast.
    Returns a DataFrame with one row per oblast.
    """
    today = date.today().strftime("%Y-%m-%d")
    records: list[dict] = []
    session = requests.Session()

    for oblast_ua, location_str in JOOBLE_OBLAST_LOCATIONS.items():
        log.info("Querying Jooble API for: %s (%s)", location_str, oblast_ua)
        count = _api_get_count(location_str, api_key, session)
        if count is None:
            log.warning("  → failed, skipping %s", oblast_ua)
            continue
        log.info("  → %s: %s vacancies", oblast_ua, f"{count:,}")
        records.append({
            "source": "jooble",
            "mode": "api",
            "snapshot_date": today,
            "region_raw": oblast_ua,
            "job_count": count,
        })
        time.sleep(DELAY_SECONDS)

    return pd.DataFrame(records)


# ── Wayback Machine mode ──────────────────────────────────────────────────────

def _wayback_get_snapshots(
    url: str, from_year: int = 2021, to_year: int = 2024, retries: int = 4
) -> list[tuple[str, str]]:
    """
    Query the Wayback CDX API and return a list of (timestamp, original_url)
    tuples — one per calendar month at most (collapsed).
    Retries up to `retries` times with exponential backoff for timeouts.
    """
    params = {
        "url": url,
        "output": "json",
        "limit": "200",
        "from": f"{from_year}0101",
        "to": f"{to_year}1231",
        "filter": "statuscode:200",
        "fl": "timestamp,original",
        "collapse": "timestamp:6",   # one per month
    }
    for attempt in range(retries):
        try:
            resp = requests.get(WAYBACK_CDX_URL, params=params, timeout=60)
            resp.raise_for_status()
            rows = resp.json()
            return [(r[0], r[1]) for r in rows[1:]]
        except requests.exceptions.Timeout:
            wait = 5 * (2 ** attempt)
            log.warning("CDX query timed out (attempt %d/%d); retrying in %ds …",
                        attempt + 1, retries, wait)
            time.sleep(wait)
        except Exception as exc:
            log.error("CDX query failed: %s", exc)
            return []
    log.error("CDX query failed after %d retries", retries)
    return []


def _wayback_fetch_and_parse(timestamp: str, original_url: str) -> int | None:
    """
    Fetch an archived Jooble page from the Wayback Machine and extract
    the Ukraine-wide vacancy count from the rendered HTML.

    Uses urllib.request (not requests) because the Wayback Machine serves
    different content depending on the Accept-Encoding and other headers
    that requests sets by default.  urllib.request produces a plainer request
    that reliably returns the rendered HTML snapshot.
    """
    wayback_url = f"https://web.archive.org/web/{timestamp}/{original_url}"
    try:
        req = urllib.request.Request(
            wayback_url,
            headers={"User-Agent": HEADERS["User-Agent"]},
        )
        with urllib.request.urlopen(req, timeout=45) as response:
            html = response.read().decode("utf-8", errors="replace")
    except Exception as exc:
        log.warning("Could not fetch snapshot %s: %s", timestamp, exc)
        return None

    for pattern in _COUNT_PATTERNS:
        match = pattern.search(html)
        if match:
            raw_num = re.sub(r"[\s\u00a0\xa0,]", "", match.group(1))
            try:
                value = int(raw_num)
                if value > 100:   # sanity-check: ignore spurious tiny values
                    return value
            except ValueError:
                continue

    log.warning("No vacancy count found in snapshot %s", timestamp)
    return None


def run_wayback_mode() -> pd.DataFrame:
    """
    Harvest all available Wayback Machine snapshots of the Jooble all-Ukraine
    SearchResult page and extract vacancy counts.

    NOTE: This gives NATIONAL totals only (no oblast breakdown) and sparse
    monthly coverage.  Results are tagged region_raw='Ukraine (national)'.
    """
    log.info("Querying Wayback CDX for archived Jooble SearchResult snapshots …")
    snapshots = _wayback_get_snapshots(JOOBLE_SEARCH_URL)

    if not snapshots:
        log.warning("No snapshots found in Wayback Machine for %s", JOOBLE_SEARCH_URL)
        return pd.DataFrame()

    log.info("Found %d monthly snapshots (2021-2024)", len(snapshots))

    records: list[dict] = []
    for ts, orig_url in snapshots:
        # Parse YYYYMMDD → YYYY-MM-DD
        snap_date = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"
        log.info("Fetching snapshot %s (%s) …", ts, snap_date)
        count = _wayback_fetch_and_parse(ts, orig_url)
        if count is not None:
            log.info("  → Ukraine national: %s vacancies", f"{count:,}")
            records.append({
                "source": "jooble_wayback",
                "mode": "wayback",
                "snapshot_date": snap_date,
                "region_raw": "Ukraine (national)",
                "job_count": count,
            })
        time.sleep(DELAY_SECONDS)

    return pd.DataFrame(records)


# ── Oblast-specific Wayback scan ──────────────────────────────────────────────

def run_wayback_oblast_scan() -> pd.DataFrame:
    """
    Attempts to find any Wayback Machine snapshots of oblast-specific Jooble
    search pages.  Based on CDX investigation, zero such snapshots exist
    (2021-2024), but this check is included for completeness and future re-runs.
    """
    oblast_url_patterns = [
        # SearchResult with location query param
        f"ua.jooble.org/SearchResult*{loc}"
        for loc in JOOBLE_OBLAST_LOCATIONS.values()
    ] + [
        # City-specific pages using Ukrainian city names
        "ua.jooble.org/*%D0%A5%D0%B0%D1%80%D0%BA%D1%96%D0%B2*",  # Харків
        "ua.jooble.org/*%D0%A1%D1%83%D0%BC%D0%B8*",               # Суми
        "ua.jooble.org/*%D0%A7%D0%B5%D1%80%D0%BD%D1%96%D0%B3%D1%96%D0%B2*",  # Чернігів
    ]

    all_records: list[dict] = []
    for pattern in oblast_url_patterns:
        params = {
            "url": pattern,
            "output": "json",
            "limit": "50",
            "from": "20210101",
            "to": "20241231",
            "filter": "statuscode:200",
            "fl": "timestamp,original",
            "collapse": "timestamp:6",
        }
        try:
            resp = requests.get(WAYBACK_CDX_URL, params=params, timeout=20)
            resp.raise_for_status()
            rows = resp.json()
            hits = rows[1:]  # skip header
            if hits:
                log.info("Pattern '%s': %d snapshot(s) found", pattern[:60], len(hits))
                for ts, orig_url in hits:
                    snap_date = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"
                    count = _wayback_fetch_and_parse(ts, orig_url)
                    if count is not None:
                        all_records.append({
                            "source": "jooble_wayback_oblast",
                            "mode": "wayback_oblast",
                            "snapshot_date": snap_date,
                            "region_raw": orig_url,
                            "job_count": count,
                        })
                    time.sleep(DELAY_SECONDS)
            else:
                log.info("Pattern '%s': no snapshots found", pattern[:60])
        except Exception as exc:
            log.warning("CDX query failed for pattern '%s': %s", pattern[:40], exc)
        time.sleep(1)

    if not all_records:
        log.info("Oblast-specific Wayback scan: no archived Jooble region pages found.")
    return pd.DataFrame(all_records)


# ── Output helpers ────────────────────────────────────────────────────────────

def save_clean(df: pd.DataFrame) -> Path:
    CLEAN_JOOBLE.mkdir(parents=True, exist_ok=True)
    out = CLEAN_JOOBLE / "jooble_oblast_snapshot.csv"
    if out.exists():
        existing = pd.read_csv(out)
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates(
            subset=["snapshot_date", "region_raw", "mode"]
        )
    df = df.sort_values(["snapshot_date", "region_raw"]).reset_index(drop=True)
    df.to_csv(out, index=False)
    log.info("Clean data → %s  (%d rows total)", out, len(df))
    return out


def print_summary(df: pd.DataFrame) -> None:
    if df.empty:
        print("\n── No data collected ─────────────────────────────────────")
        return
    print("\n── Jooble snapshot summary ───────────────────────────────────")
    for _, row in df.iterrows():
        print(
            f"  [{row['snapshot_date']}]  {row['region_raw']:<30s}"
            f"  {int(row['job_count']):>8,}  ({row['mode']})"
        )
    print("──────────────────────────────────────────────────────────────\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Jooble job posting scraper (snapshot + Wayback)"
    )
    parser.add_argument(
        "--mode",
        choices=["api", "wayback", "both"],
        default="wayback",
        help="api = live REST API snapshot; wayback = Wayback Machine harvest; "
             "both = run wayback then api",
    )
    parser.add_argument(
        "--key",
        default="",
        help="Jooble API key (required for --mode api or both). "
             "Register free at https://jooble.org/api/about",
    )
    parser.add_argument("--test", action="store_true",
                        help="Dry-run: print results without saving")
    args = parser.parse_args()

    frames: list[pd.DataFrame] = []

    if args.mode in ("wayback", "both"):
        log.info("=== Wayback Machine mode: national all-Ukraine snapshots ===")
        df_wb = run_wayback_mode()
        if not df_wb.empty:
            frames.append(df_wb)

        log.info("=== Wayback Machine mode: oblast-specific scan ===")
        df_oblast = run_wayback_oblast_scan()
        if not df_oblast.empty:
            frames.append(df_oblast)

    if args.mode in ("api", "both"):
        if not args.key:
            log.error(
                "--key is required for API mode.  "
                "Register at https://jooble.org/api/about to get a free key."
            )
            sys.exit(1)
        log.info("=== Jooble REST API mode: current active vacancies per oblast ===")
        df_api = run_api_mode(args.key)
        if not df_api.empty:
            frames.append(df_api)

    if not frames:
        log.warning("No data collected from any source.")
        sys.exit(0)

    df_all = pd.concat(frames, ignore_index=True)
    print_summary(df_all)

    if args.test:
        log.info("--test flag: not saving.")
        return

    save_clean(df_all)


if __name__ == "__main__":
    main()
