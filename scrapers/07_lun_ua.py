"""
LUN.ua — Ukrainian Real Estate Statistics Scraper
================================================
Fetches housing price data for Kharkiv, Sumy, and Chernihiv oblasts.

Data sources (in order of priority):
  1. Direct LUN stat API  — primary market prices back to Jan 2021
     Base URL: https://lun.ua/stat/api/data/
  2. Wayback Machine      — archived LUN stat pages (supplementary)
  3. Firecrawl API        — crawl/scrape the stat pages (optional, needs key)

City IDs (from network inspection):
  Kharkiv  = 120   (stat slug: kharkiv)
  Sumy     = 118   (stat slug: sumy)
  Chernihiv = 125  (stat slug: chernihiv)

Outputs (written to data/clean/lun_ua/):
  lun_primary_class_month.csv   — primary-market price by housing class, Jan 2021–Dec 2024
  lun_price_history.csv         — flat-price history by room count (primary + rental), May 2023–Dec 2024
  lun_raw_<timestamp>.json      — raw API responses (immutable archive)

Usage:
  python scrapers/07_lun_ua.py [--test] [--firecrawl-key FC_KEY]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ── Project root and config ─────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR / "scrapers"))
from config import (
    DATE_END,
    DATE_START,
    DELAY_SECONDS,
    REGIONS_EN,
    WAYBACK_CDX_URL,
)

# ── LUN.ua constants ─────────────────────────────────────────────────────────
LUN_STAT_API = "https://lun.ua/stat/api/data"
LUN_STAT_BASE = "https://lun.ua/stat"

LUN_CITIES = {
    "Харківська": {"city_id": 120, "slug": "kharkiv", "en": "Kharkiv"},
    "Сумська":    {"city_id": 118, "slug": "sumy",    "en": "Sumy"},
    "Чернігівська": {"city_id": 125, "slug": "chernihiv", "en": "Chernihiv"},
}

# contractTypeId mapping (discovered via network inspection):
#   1 = primary market (new construction) sale prices
#   2 = rental prices (per month)
CONTRACT_TYPES = {
    1: "primary_sale",
    2: "rental",
}

PANEL_START = DATE_START[:7]   # "2021-01"
PANEL_END   = DATE_END[:7]     # "2024-12"

CLEAN_LUN = ROOT_DIR / "data" / "clean" / "lun_ua"
RAW_LUN   = ROOT_DIR / "data" / "raw" / "lun_ua"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "uk,en;q=0.9",
    "Referer": "https://lun.ua/stat/primary/kharkiv",
}


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_json(url: str, timeout: int = 20) -> dict:
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        print(f"  HTTP {exc.code}: {url}", file=sys.stderr)
        raise
    except urllib.error.URLError as exc:
        print(f"  URLError: {exc.reason} — {url}", file=sys.stderr)
        raise


def _safe_get_json(url: str, timeout: int = 20) -> dict | None:
    try:
        return _get_json(url, timeout)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Panel date filter
# ─────────────────────────────────────────────────────────────────────────────

def _in_panel(month_str: str) -> bool:
    """Return True if YYYY-MM is within the panel range."""
    ym = month_str[:7]
    return PANEL_START <= ym <= PANEL_END


def _to_month(date_str: str) -> str:
    """Convert 'YYYY-MM-DD' or 'YYYY-MM-DDThh:mm:ss.sssZ' → 'YYYY-MM'."""
    return str(date_str)[:7]


# ─────────────────────────────────────────────────────────────────────────────
# Method 1: Direct LUN stat API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_price_by_class(city_id: int, city_en: str) -> list[dict]:
    """
    Fetch primary-market prices by housing class for a city.
    Endpoint: GET /price?cityId={id}
    Returns cityClasses and cityAvg arrays (monthly, from Jan 2021).
    """
    url = f"{LUN_STAT_API}/price?cityId={city_id}"
    print(f"  [API] price?cityId={city_id} ({city_en})")
    data = _get_json(url)

    rows: list[dict] = []
    payload = data.get("data", {})

    # cityClasses: economy / comfort / business / premium by month
    for item in payload.get("cityClasses", []):
        month = _to_month(item["monthDate"])
        if not _in_panel(month):
            continue
        rows.append({
            "oblast":       city_en,
            "month":        month,
            "market_type":  "primary_sale",
            "segment":      "class",
            "class_label":  item.get("label", ""),
            "room_count":   None,
            "avg_uah_m2":   item.get("averageUah"),
            "avg_usd_m2":   item.get("averageUsd"),
            "listing_count": item.get("count"),
            "source":       "lun_ua_api_price",
        })

    # cityAvg: overall average (all classes) by month
    for item in payload.get("cityAvg", []):
        month = _to_month(item["monthDate"])
        if not _in_panel(month):
            continue
        rows.append({
            "oblast":       city_en,
            "month":        month,
            "market_type":  "primary_sale",
            "segment":      "all_classes",
            "class_label":  "all",
            "room_count":   None,
            "avg_uah_m2":   item.get("averageUah"),
            "avg_usd_m2":   item.get("averageUsd"),
            "listing_count": item.get("count"),
            "source":       "lun_ua_api_price",
        })

    return rows


def fetch_flat_price_history(city_id: int, city_en: str, contract_type: int) -> list[dict]:
    """
    Fetch flat-price history by room count.
    contractTypeId=1 → primary_sale (from 2023-05)
    contractTypeId=2 → rental      (from 2023-05)
    """
    market = CONTRACT_TYPES.get(contract_type, f"type_{contract_type}")
    url = f"{LUN_STAT_API}/flat-price-history?cityId={city_id}&contractTypeId={contract_type}"
    print(f"  [API] flat-price-history?cityId={city_id}&contractTypeId={contract_type} ({city_en}/{market})")
    data = _safe_get_json(url)
    if not data:
        return []

    rows: list[dict] = []
    for item in data.get("data", []):
        month = _to_month(item["date"])
        if not _in_panel(month):
            continue
        rows.append({
            "oblast":        city_en,
            "month":         month,
            "market_type":   market,
            "segment":       "room_count",
            "class_label":   None,
            "room_count":    item.get("roomCount"),
            "median_uah":    item.get("medianUah"),       # total flat price UAH
            "median_m2_uah": item.get("medianM2Uah"),     # price per m² UAH
            "median_usd":    item.get("medianUsd"),       # total flat price USD
            "median_m2_usd": item.get("medianM2Usd"),     # price per m² USD
            "listing_count": item.get("count"),
            "source":        f"lun_ua_api_flat_history_ctype{contract_type}",
        })

    return rows


def fetch_rent_history(city_id: int, city_en: str) -> list[dict]:
    """
    sale-in-rent-history: ratio of apartment price to annual rent.
    (Number of years of rental needed to buy the apartment.)
    Stored as a supplementary affordability indicator.
    """
    url = f"{LUN_STAT_API}/sale-in-rent-history?cityId={city_id}"
    print(f"  [API] sale-in-rent-history?cityId={city_id} ({city_en})")
    data = _safe_get_json(url)
    if not data:
        return []

    rows: list[dict] = []
    for item in data.get("data", []):
        month = _to_month(item["date"])
        if not _in_panel(month):
            continue
        rows.append({
            "oblast":             city_en,
            "month":              month,
            "room_count":         item.get("roomCount"),
            "years_rent_to_buy":  item.get("value"),
            "source":             "lun_ua_api_sale_in_rent",
        })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Method 2: Wayback Machine
# ─────────────────────────────────────────────────────────────────────────────

def _wayback_cdx(url: str, from_ts: str = "20210101", to_ts: str = "20241231") -> list[list[str]]:
    """Query Wayback CDX API for archived snapshots of a URL."""
    api = (
        f"{WAYBACK_CDX_URL}?url={url}&output=json"
        f"&limit=100&from={from_ts}&to={to_ts}"
        f"&fl=timestamp,original,statuscode&collapse=timestamp:6"
    )
    req = urllib.request.Request(api, headers={"User-Agent": HEADERS["User-Agent"]})
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            results = json.loads(resp.read())
            return results[1:] if results else []   # skip header row
    except Exception as exc:
        print(f"  [Wayback] CDX error for {url}: {exc}", file=sys.stderr)
        return []


def _fetch_wayback_snapshot(timestamp: str, url: str) -> str | None:
    """Fetch the HTML of a Wayback Machine snapshot."""
    wb_url = f"https://web.archive.org/web/{timestamp}/{url}"
    req = urllib.request.Request(wb_url, headers={"User-Agent": HEADERS["User-Agent"]})
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        print(f"  [Wayback] fetch error: {exc}", file=sys.stderr)
        return None


def scrape_wayback(city_slug: str, city_en: str) -> list[dict]:
    """
    Try to find archived LUN stat pages on the Wayback Machine.
    The lun.ua/stat/ module launched in May 2023, so pre-May-2023
    snapshots are unlikely to exist. This function is included for
    completeness and future proofing.
    """
    rows: list[dict] = []
    urls_to_try = [
        f"lun.ua/stat/primary/{city_slug}",
        f"lun.ua/stat/sale/{city_slug}",
        f"lun.ua/stat/rent/{city_slug}",
    ]

    for target in urls_to_try:
        snapshots = _wayback_cdx(target)
        print(f"  [Wayback] {target}: {len(snapshots)} snapshots")
        for ts, orig_url, status in snapshots[:5]:
            if status != "200":
                continue
            html = _fetch_wayback_snapshot(ts, f"https://{target}")
            if not html:
                continue
            # Archived stat pages are JavaScript-rendered (Next.js/Remix);
            # the chart data is not present in the HTML. Skip.
            print(f"    Snapshot {ts[:8]}: fetched {len(html)} chars (JS-rendered, no inline data)")
            time.sleep(DELAY_SECONDS)

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Method 3: Firecrawl API (optional)
# ─────────────────────────────────────────────────────────────────────────────

def _firecrawl_scrape(url: str, api_key: str) -> dict | None:
    """
    Use Firecrawl scrape endpoint to render and extract content from a page.
    Docs: https://docs.firecrawl.dev/features/crawl
    """
    import json as _json
    fc_url = "https://api.firecrawl.dev/v1/scrape"
    payload = _json.dumps({
        "url": url,
        "formats": ["markdown"],
        "onlyMainContent": True,
    }).encode()
    req = urllib.request.Request(
        fc_url,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return _json.loads(resp.read())
    except Exception as exc:
        print(f"  [Firecrawl] scrape error for {url}: {exc}", file=sys.stderr)
        return None


def scrape_firecrawl(city_slug: str, city_en: str, api_key: str) -> list[dict]:
    """
    Use Firecrawl to scrape the LUN stat pages and extract embedded
    statistics from the rendered markdown content.
    Returns any additional price data rows found.
    """
    import re
    rows: list[dict] = []

    pages = {
        "primary_sale": f"{LUN_STAT_BASE}/primary/{city_slug}",
        "secondary_sale": f"{LUN_STAT_BASE}/sale/{city_slug}",
        "rental": f"{LUN_STAT_BASE}/rent/{city_slug}",
    }

    for market_type, page_url in pages.items():
        print(f"  [Firecrawl] scraping {page_url}")
        result = _firecrawl_scrape(page_url, api_key)
        time.sleep(DELAY_SECONDS)
        if not result:
            continue

        markdown = result.get("data", {}).get("markdown", "") or ""

        # Extract price figures from the markdown.
        # LUN stat pages display tables like: "29 000 грн/м²" or "$650/м²"
        uah_m2_matches = re.findall(r"(\d[\d\s]*)\s*грн/м²", markdown)
        usd_m2_matches = re.findall(r"\$\s*(\d[\d,\.]*)\s*/м²", markdown)

        if uah_m2_matches or usd_m2_matches:
            snapshot_month = datetime.now(tz=timezone.utc).strftime("%Y-%m")
            for val in uah_m2_matches[:3]:
                clean = int(val.replace(" ", "").replace("\u00a0", ""))
                rows.append({
                    "oblast":        city_en,
                    "month":         snapshot_month,
                    "market_type":   market_type,
                    "segment":       "firecrawl_snapshot",
                    "avg_uah_m2":    clean,
                    "source":        "lun_ua_firecrawl",
                })

        print(f"    Found {len(uah_m2_matches)} UAH/m² and {len(usd_m2_matches)} USD/m² values")

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# CSV writer
# ─────────────────────────────────────────────────────────────────────────────

def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        print(f"  No data to write for {path.name}")
        return
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows):,} rows → {path.relative_to(ROOT_DIR)}")


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestration
# ─────────────────────────────────────────────────────────────────────────────

def run(test: bool = False, firecrawl_key: str | None = None, wayback: bool = False) -> None:
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    CLEAN_LUN.mkdir(parents=True, exist_ok=True)
    RAW_LUN.mkdir(parents=True, exist_ok=True)

    all_class_rows: list[dict] = []
    all_history_rows: list[dict] = []
    all_affordability_rows: list[dict] = []
    all_firecrawl_rows: list[dict] = []
    raw_dump: dict = {}

    for region_uk, meta in LUN_CITIES.items():
        city_id = meta["city_id"]
        city_en = meta["en"]
        slug    = meta["slug"]

        print(f"\n{'='*60}")
        print(f"City: {city_en}  (cityId={city_id}, slug={slug})")
        print(f"{'='*60}")

        # ── Method 1a: Primary-market price by class (full panel 2021–2024) ──
        class_rows = fetch_price_by_class(city_id, city_en)
        all_class_rows.extend(class_rows)
        raw_dump[f"price_{slug}"] = class_rows
        time.sleep(DELAY_SECONDS)

        # ── Method 1b: Flat-price history by room count ──────────────────────
        for ctype in (1, 2):
            hist_rows = fetch_flat_price_history(city_id, city_en, ctype)
            all_history_rows.extend(hist_rows)
            raw_dump[f"flat_hist_{slug}_ct{ctype}"] = hist_rows
            time.sleep(DELAY_SECONDS)

        # ── Method 1c: Affordability ratio (years of rent to buy) ────────────
        afford_rows = fetch_rent_history(city_id, city_en)
        all_affordability_rows.extend(afford_rows)
        raw_dump[f"rent_history_{slug}"] = afford_rows
        time.sleep(DELAY_SECONDS)

        # ── Method 2: Wayback Machine ─────────────────────────────────────────
        if wayback:
            print(f"\n  Checking Wayback Machine for {slug}...")
            scrape_wayback(slug, city_en)
            time.sleep(DELAY_SECONDS)

        # ── Method 3: Firecrawl ───────────────────────────────────────────────
        if firecrawl_key:
            print(f"\n  Scraping via Firecrawl for {slug}...")
            fc_rows = scrape_firecrawl(slug, city_en, firecrawl_key)
            all_firecrawl_rows.extend(fc_rows)

        if test:
            print(f"\n  [TEST] Breaking after first city (--test mode)")
            break

    # ── Save raw dump ─────────────────────────────────────────────────────────
    raw_path = RAW_LUN / f"lun_raw_{ts}.json"
    with open(raw_path, "w", encoding="utf-8") as fh:
        json.dump(raw_dump, fh, ensure_ascii=False, indent=2)
    print(f"\nRaw dump: {raw_path.relative_to(ROOT_DIR)}")

    # ── Save cleaned CSVs ─────────────────────────────────────────────────────
    print("\n── Writing clean CSVs ──")

    _write_csv(
        CLEAN_LUN / "lun_primary_class_month.csv",
        all_class_rows,
    )
    _write_csv(
        CLEAN_LUN / "lun_flat_price_history.csv",
        all_history_rows,
    )
    _write_csv(
        CLEAN_LUN / "lun_affordability_month.csv",
        all_affordability_rows,
    )
    if all_firecrawl_rows:
        _write_csv(
            CLEAN_LUN / "lun_firecrawl_snapshot.csv",
            all_firecrawl_rows,
        )

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n── Summary ──")

    # Primary class data coverage
    if all_class_rows:
        from collections import defaultdict
        coverage: dict[str, set] = defaultdict(set)
        for r in all_class_rows:
            if r.get("avg_uah_m2") and r["avg_uah_m2"] > 0:
                coverage[r["oblast"]].add(r["month"])
        for oblast, months in sorted(coverage.items()):
            months_sorted = sorted(months)
            print(
                f"  {oblast} primary (by class): "
                f"{months_sorted[0]} → {months_sorted[-1]}, "
                f"{len(months_sorted)} months with data"
            )

    # Flat-price history coverage
    if all_history_rows:
        from collections import defaultdict
        h_coverage: dict[str, set] = defaultdict(set)
        for r in all_history_rows:
            key = f"{r['oblast']}/{r['market_type']}"
            h_coverage[key].add(r["month"])
        for key, months in sorted(h_coverage.items()):
            months_sorted = sorted(months)
            print(
                f"  {key}: "
                f"{months_sorted[0]} → {months_sorted[-1]}, "
                f"{len(months_sorted)} months"
            )


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape LUN.ua real estate statistics for Kharkiv, Sumy, Chernihiv"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode: only fetch first city, skip Wayback Machine",
    )
    parser.add_argument(
        "--firecrawl-key",
        metavar="FC_KEY",
        default=None,
        help="Firecrawl API key (optional). If provided, also scrapes stat pages via Firecrawl.",
    )
    parser.add_argument(
        "--wayback",
        action="store_true",
        help=(
            "Also query the Wayback Machine for archived LUN stat pages. "
            "Disabled by default because the lun.ua/stat/ module launched in May 2023 "
            "and has no pre-launch Wayback archives."
        ),
    )
    args = parser.parse_args()

    run(test=args.test, firecrawl_key=args.firecrawl_key, wayback=args.wayback)
