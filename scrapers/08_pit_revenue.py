"""
scrapers/08_pit_revenue.py
─────────────────────────────────────────────────────────────────────────────
Download hromada-level Personal Income Tax (PIT / ПДФО) revenue from the
Ukrainian State Treasury Open Budget portal (openbudget.gov.ua).

API: https://api.openbudget.gov.ua
Endpoint: GET /api/public/localBudgetData
  ?budgetCode=<10-or-11-digit code>
  &budgetItem=INCOMES
  &period=MONTH
  &year=<YYYY>

Output
------
  data/raw/pit_revenue/<budgetCode>_<year>.csv     — raw per-hromada-year CSV
  data/clean/pit_revenue/pit_hromada_month.csv     — clean panel, hromada × month

Schema of clean output
----------------------
  hromada_code    — KATOTTG code (UA63...) — stable hromada identifier
  hromada_name    — hromada name (Ukrainian)
  oblast          — Kharkiv | Sumy | Chernihiv
  budget_code     — openbudget budget code used for this year's data
  month           — YYYY-MM
  pit_total_uah   — sum of all PIT sub-codes (1101xxxx), general fund, UAH

PIT income codes included (all sub-codes of 11010000):
  11010100 — PIT withheld by tax agents from salary income
  11010200 — PIT from military service pay
  11010400 — PIT from other income (non-salary)
  11010500 — PIT from investment income
  110106xx — self-employed / FOP PIT
  (any other code starting with 11010)
  Parent code 11010000 is also included if present.

Usage
-----
  python scrapers/08_pit_revenue.py [--peek] [--years 2021 2022 2023 2024]
  --peek          dry-run: print hromada list and exit
  --years         space-separated years to download (default: 2021 2022 2023 2024)
  --rebuild-budg  force re-download of BUDG dictionary (default: use cache)
"""

import argparse
import csv
import io
import json
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
API_BASE = "https://api.openbudget.gov.ua"
DELAY    = 1.5          # seconds between API calls (polite)
YEARS    = [2021, 2022, 2023, 2024]

# signBudg values that identify hromada (territorial community) budgets.
# These include: sil'ska (ss), selyscha (ss), misto (m), selyshche-misto (smt),
# misto z raionnym podilom (mrz), misto oblasnoho znachennia (moz) — all with (g) suffix.
# The '(g)' suffix signals the budget has a direct relationship with the State Budget.
# We exclude 'r' (raion), 'gs' (raion consolidated), and bare 's'/'m' (old-format councils).
HROMADA_SIGN_CONTAINS = "(g)"   # all community budgets end with (g) in their sign

# KATOTTG prefixes that identify our three target oblasts
OBLAST_PREFIXES = {
    "Sumy":      "UA59",
    "Kharkiv":   "UA63",
    "Chernihiv": "UA74",
}

# PIT income codes: we sum all codes whose first 6 digits are 110100
PIT_CODE_PREFIX = "11010"

ROOT_DIR  = Path(__file__).resolve().parent.parent
RAW_DIR   = ROOT_DIR / "data" / "raw"   / "pit_revenue"
CLEAN_DIR = ROOT_DIR / "data" / "clean" / "pit_revenue"
CACHE_DIR = ROOT_DIR / "data" / "raw"   / "_cache"

BUDG_CACHE_FILE = CACHE_DIR / "openbudget_budg_dict.json"

RAW_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://api.openbudget.gov.ua/swagger-ui.html",
}


# ── Helpers ─────────────────────────────────────────────────────────────────

def get_json(url: str, retries: int = 3) -> object:
    """Fetch JSON from URL with retries."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=120) as r:
                raw = r.read()
                return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"  [retry {attempt+1}/{retries}] {e} — waiting {wait}s")
                time.sleep(wait)
            else:
                raise


def get_csv(url: str, retries: int = 3) -> list[dict]:
    """Fetch CSV from openbudget API; returns list of row dicts."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=120) as r:
                raw = r.read()
            # The Content-Type claims windows-1251 but it's actually UTF-8
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("windows-1251", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            return list(reader)
        except Exception as e:
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"  [retry {attempt+1}/{retries}] {e} — waiting {wait}s")
                time.sleep(wait)
            else:
                raise


# ── Step 1: BUDG Dictionary ──────────────────────────────────────────────────

def load_budg_dict(rebuild: bool = False) -> list[dict]:
    """
    Download (and cache) the full BUDG dictionary from openbudget.
    Returns the raw list of ~217k budget entries.
    """
    if BUDG_CACHE_FILE.exists() and not rebuild:
        print(f"Using cached BUDG dictionary: {BUDG_CACHE_FILE}")
        with open(BUDG_CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)

    print("Downloading BUDG dictionary from openbudget (~180 MB)…")
    url = f"{API_BASE}/items/BUDG"
    data = get_json(url)
    with open(BUDG_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"  Saved {len(data):,} entries to {BUDG_CACHE_FILE}")
    return data


# ── Step 2: Filter to target hromadas ───────────────────────────────────────

def extract_hromada_codes(
    budg_dict: list[dict],
    years: list[int],
) -> dict[str, dict]:
    """
    Build a mapping keyed by (codebudg, year_key) — one entry per unique
    hromada budget per year — to avoid double-counting.

    The BUDG dictionary has multiple entries with the same codebudg because
    each settlement (village/town) within a hromada has its own KATOTTG code.
    Since PIT revenue is reported at the hromada budget level, we must
    deduplicate by codebudg before making API calls.

    Returns dict keyed by codebudg:
      codebudg → {
        "name":       hromada budget name (Ukrainian),
        "oblast":     Kharkiv | Sumy | Chernihiv,
        "katottg":    representative KATOTTG code (shortest = highest level),
        "years":      list of years this code covers,
      }
    """
    prefix_to_oblast = {v: k for k, v in OBLAST_PREFIXES.items()}

    # codebudg → best entry info
    hromadas: dict[str, dict] = {}

    skipped_no_katottg      = 0
    skipped_wrong_oblast    = 0
    skipped_not_community   = 0
    total_matched           = 0

    for entry in budg_dict:
        katottg = entry.get("katottg", "")
        if not katottg or not katottg.startswith("UA"):
            skipped_no_katottg += 1
            continue

        # Match oblast prefix (UA59=Sumy, UA63=Kharkiv, UA74=Chernihiv)
        oblast = None
        for prefix, ob_name in prefix_to_oblast.items():
            if katottg.startswith(prefix):
                oblast = ob_name
                break
        if oblast is None:
            skipped_wrong_oblast += 1
            continue

        # Only hromada (community-level) budgets: signBudg must contain "(g)"
        # This covers ss(g), s(g), m(g), smt(g), mrz(g), moz(g) etc.
        # Excludes raion budgets (r, gs, gss) and state/oblast consolidations.
        sign = entry.get("signBudg") or ""
        if HROMADA_SIGN_CONTAINS not in sign:
            skipped_not_community += 1
            continue

        total_matched += 1

        codebudg  = (entry.get("codebudg") or "").strip()
        namebudg  = (entry.get("namebudg") or "").strip()
        begin_str = entry.get("beginDate") or "2000-01-01"
        end_str   = entry.get("endDate")   or "2099-12-31"

        if not codebudg:
            continue

        try:
            begin_year = int(begin_str[:4])
            end_year   = int(end_str[:4])
        except (ValueError, TypeError):
            begin_year, end_year = 2000, 2099

        valid_years = [yr for yr in years if begin_year <= yr <= end_year]

        # The BUDG dictionary only starts from 2022. However, the API returns
        # historical data (incl. 2021) when queried with a 2022 budget code.
        # If this is the earliest code (begins 2022) and 2021 is requested,
        # we extend coverage to 2021 so the caller can query year=2021 with
        # the 2022 budget code.
        if begin_year == 2022 and 2021 in years and 2021 not in valid_years:
            valid_years = [2021] + valid_years

        if not valid_years:
            continue

        if codebudg not in hromadas:
            hromadas[codebudg] = {
                "name":    namebudg,
                "oblast":  oblast,
                "katottg": katottg,   # will prefer shortest (hromada-level)
                "years":   valid_years,
            }
        else:
            # Merge valid years
            existing_years = set(hromadas[codebudg]["years"])
            hromadas[codebudg]["years"] = sorted(existing_years | set(valid_years))
            # Prefer the longest (most descriptive) budget name
            if len(namebudg) > len(hromadas[codebudg]["name"]):
                hromadas[codebudg]["name"] = namebudg
            # Prefer the shorter KATOTTG — shorter = higher admin level
            if len(katottg) < len(hromadas[codebudg]["katottg"]):
                hromadas[codebudg]["katottg"] = katottg

    print(
        f"\nFiltering stats:"
        f"\n  Total BUDG entries:                  {len(budg_dict):,}"
        f"\n  Skipped (no katottg):                {skipped_no_katottg:,}"
        f"\n  Skipped (wrong oblast):              {skipped_wrong_oblast:,}"
        f"\n  Skipped (not community budget):      {skipped_not_community:,}"
        f"\n  Matched entries (3 target oblasts):  {total_matched:,}"
        f"\n  Unique hromada budget codes:         {len(hromadas):,}"
    )

    by_oblast: dict[str, int] = defaultdict(int)
    for h in hromadas.values():
        by_oblast[h["oblast"]] += 1
    for ob, cnt in sorted(by_oblast.items()):
        print(f"    {ob}: {cnt} unique budget codes")

    return hromadas


# ── Step 3: Download income data ─────────────────────────────────────────────

def download_incomes(budget_code: str, year: int) -> list[dict]:
    """
    Fetch monthly income data for a single hromada-year from openbudget API.
    Returns list of CSV row dicts.
    """
    url = (
        f"{API_BASE}/api/public/localBudgetData"
        f"?budgetCode={budget_code}"
        f"&budgetItem=INCOMES"
        f"&period=MONTH"
        f"&year={year}"
    )
    return get_csv(url)


def parse_pit_monthly(rows: list[dict], budget_code: str, year: int) -> dict[str, float]:
    """
    Filter rows to PIT income codes, general fund only (FUND_TYP == 'C'),
    and sum FAKT_AMT by month.
    Returns {month: total_pit_uah} e.g. {'2022-01': 123456.78}
    """
    monthly: dict[str, float] = {}

    for row in rows:
        cod_inco = (row.get("COD_INCO") or "").strip()
        fund_typ = (row.get("FUND_TYP") or "").strip()
        rep_period = (row.get("REP_PERIOD") or "").strip()  # e.g. "01.2022"

        # Only PIT codes and general fund
        if not cod_inco.startswith(PIT_CODE_PREFIX):
            continue
        if fund_typ != "C":
            continue

        # Parse period "MM.YYYY" → "YYYY-MM"
        if "." not in rep_period:
            continue
        mm, yyyy = rep_period.split(".", 1)
        month_key = f"{yyyy}-{mm.zfill(2)}"

        # Check year matches requested year
        try:
            if int(yyyy) != year:
                continue
        except ValueError:
            continue

        # Parse actual amount
        fakt_str = (row.get("FAKT_AMT") or "").strip()
        if not fakt_str:
            continue
        try:
            fakt = float(fakt_str.replace(",", "."))
        except ValueError:
            continue

        monthly[month_key] = monthly.get(month_key, 0.0) + fakt

    return monthly


# ── Step 4: Orchestrate and save ─────────────────────────────────────────────

def run(years: list[int], peek: bool = False, rebuild_budg: bool = False) -> None:
    # Load BUDG dictionary
    budg_dict = load_budg_dict(rebuild=rebuild_budg)

    # Extract target hromadas
    hromadas = extract_hromada_codes(budg_dict, years)

    if peek:
        print(f"\n[PEEK] First 20 unique hromada budget codes:")
        for i, (codebudg, info) in enumerate(list(hromadas.items())[:20]):
            years_str = ", ".join(str(y) for y in info["years"])
            print(
                f"  {codebudg} | {info['oblast']:10} | "
                f"years=[{years_str}] | {info['name'][:50]}"
            )
        print(f"\n  (showing 20 of {len(hromadas)} unique budget codes)")
        return

    # Collect all rows for the clean output
    all_rows: list[dict] = []
    n_hromadas  = len(hromadas)
    n_requests  = 0
    n_errors    = 0

    print(f"\nDownloading PIT data for {n_hromadas} unique hromada budget codes…")

    for h_idx, (codebudg, info) in enumerate(hromadas.items()):
        oblast    = info["oblast"]
        hrom_name = info["name"]
        katottg   = info["katottg"]

        for year in years:
            if year not in info["years"]:
                continue

            # Check raw cache — keyed by (budget_code, year) to avoid re-downloading
            raw_file = RAW_DIR / f"{codebudg}_{year}.csv"
            if raw_file.exists():
                with open(raw_file, encoding="utf-8") as f:
                    rows = list(csv.DictReader(f, delimiter=";"))
            else:
                try:
                    rows = download_incomes(codebudg, year)
                    if rows:
                        with open(raw_file, "w", encoding="utf-8", newline="") as f:
                            writer = csv.DictWriter(
                                f, fieldnames=rows[0].keys(), delimiter=";"
                            )
                            writer.writeheader()
                            writer.writerows(rows)
                    n_requests += 1
                    time.sleep(DELAY)
                except Exception as e:
                    print(f"  ERROR {codebudg} {year}: {e}")
                    n_errors += 1
                    continue

            # Parse PIT monthly totals
            monthly = parse_pit_monthly(rows, codebudg, year)

            # Build full 12-month panel (0 for months with no PIT data)
            for m in range(1, 13):
                month_key = f"{year}-{m:02d}"
                all_rows.append({
                    "hromada_code":  katottg,
                    "hromada_name":  hrom_name,
                    "oblast":        oblast,
                    "budget_code":   codebudg,
                    "month":         month_key,
                    "pit_total_uah": monthly.get(month_key, 0.0),
                })

        # Progress log every 50 hromadas
        if (h_idx + 1) % 50 == 0 or (h_idx + 1) == n_hromadas:
            print(
                f"  [{h_idx+1}/{n_hromadas}] "
                f"requests={n_requests} errors={n_errors}"
            )

    # Write clean output
    out_file = CLEAN_DIR / "pit_hromada_month.csv"
    if all_rows:
        fieldnames = ["hromada_code", "hromada_name", "oblast", "budget_code",
                      "month", "pit_total_uah"]
        with open(out_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n✓ Wrote {len(all_rows):,} rows to {out_file}")
    else:
        print("\nNo rows collected — check errors above.")

    print(f"\nSummary: {n_requests} API calls | {n_errors} errors | {n_skipped} missing codes")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download hromada-level PIT revenue from openbudget.gov.ua"
    )
    parser.add_argument(
        "--peek",
        action="store_true",
        help="Dry-run: print hromada list and exit without downloading",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=YEARS,
        metavar="YEAR",
        help="Years to download (default: 2021 2022 2023 2024)",
    )
    parser.add_argument(
        "--rebuild-budg",
        action="store_true",
        dest="rebuild_budg",
        help="Force re-download of BUDG dictionary even if cache exists",
    )
    args = parser.parse_args()

    run(years=args.years, peek=args.peek, rebuild_budg=args.rebuild_budg)
