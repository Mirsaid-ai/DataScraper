"""
01_acled.py — Download ACLED conflict events for Sumy, Chernihiv, Kharkiv oblasts.

Auth: OAuth Bearer token (auto-fetched from ACLED_EMAIL + ACLED_PASSWORD in .env).
      Falls back to ACLED_REFRESH_TOKEN if present and password not set.

Usage:
    python scrapers/01_acled.py

Outputs:
    data/raw/acled/acled_ukraine_YYYYMMDD.csv   — full raw download
    data/clean/acled/acled_raion_month.csv      — raion-month aggregates
"""

import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# ── Path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    ACLED_API_URL, ACLED_AUTH_URL, ACLED_FIELDS, ACLED_OBLAST_NAMES,
    ACLED_PAGE_SIZE, DATE_START, DATE_END,
    RAW_ACLED, CLEAN_ACLED, DELAY_SECONDS,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SHELLING_SUBTYPES = {
    "Shelling/artillery/missile attack",
    "Air/drone strike",
    "Remote explosive/landmine/IED",
}


def get_bearer_token() -> str:
    """Obtain a Bearer token via OAuth password grant or refresh token."""
    load_dotenv()
    email    = os.getenv("ACLED_EMAIL", "").strip()
    password = os.getenv("ACLED_PASSWORD", "").strip()
    refresh  = os.getenv("ACLED_REFRESH_TOKEN", "").strip()

    if email and password:
        log.info("Requesting access token for %s …", email)
        resp = requests.post(
            ACLED_AUTH_URL,
            data={
                "username":   email,
                "password":   password,
                "grant_type": "password",
                "client_id":  "acled",
            },
            timeout=20,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        log.info("Access token obtained (valid 24 h).")
        return token

    if refresh:
        log.info("Refreshing access token …")
        resp = requests.post(
            ACLED_AUTH_URL,
            data={
                "refresh_token": refresh,
                "grant_type":    "refresh_token",
                "client_id":     "acled",
            },
            timeout=20,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        log.info("Access token refreshed.")
        return token

    log.error("No credentials found. Set ACLED_EMAIL + ACLED_PASSWORD in .env")
    sys.exit(1)


def fetch_page(session: requests.Session, page: int) -> list[dict]:
    """Fetch one page of ACLED results. Returns list of event dicts."""
    params = {
        "country":          "Ukraine",
        "admin1":           "|".join(ACLED_OBLAST_NAMES),
        "event_date":       f"{DATE_START}|{DATE_END}",
        "event_date_where": "BETWEEN",
        "fields":           "|".join(ACLED_FIELDS),
        "limit":            ACLED_PAGE_SIZE,
        "page":             page,
    }

    resp = session.get(ACLED_API_URL, params=params, timeout=30)

    if resp.status_code == 401:
        log.error("401 Unauthorized — token may have expired. Re-run the script.")
        raise RuntimeError("401 Unauthorized")
    if resp.status_code != 200:
        log.error("HTTP %s on page %d: %s", resp.status_code, page, resp.text[:300])
        raise RuntimeError(f"HTTP {resp.status_code}")

    payload = resp.json()

    # New API wraps data in {"data": [...]} or returns list directly
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        # Check for error message
        if payload.get("status") == 400 or payload.get("error"):
            log.error("API error on page %d: %s", page, payload)
            raise RuntimeError(str(payload))
        return payload.get("data", [])

    return []


def download_all(token: str) -> pd.DataFrame:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "User-Agent":    "econ590-research/1.0",
    })

    all_rows: list[dict] = []
    page = 1

    log.info("Downloading ACLED events (oblasts: %s, %s–%s)",
             ", ".join(ACLED_OBLAST_NAMES), DATE_START, DATE_END)

    with tqdm(desc="ACLED pages", unit="page") as pbar:
        while True:
            rows = fetch_page(session, page)
            if not rows:
                break
            all_rows.extend(rows)
            pbar.update(1)
            pbar.set_postfix(total_rows=len(all_rows))

            if len(rows) < ACLED_PAGE_SIZE:
                break  # last page

            page += 1
            time.sleep(DELAY_SECONDS)

    log.info("Downloaded %d events across %d pages", len(all_rows), page)
    return pd.DataFrame(all_rows)


def save_raw(df: pd.DataFrame) -> Path:
    RAW_ACLED.mkdir(parents=True, exist_ok=True)
    stamp = datetime.today().strftime("%Y%m%d")
    out = RAW_ACLED / f"acled_ukraine_{stamp}.csv"
    counter = 1
    while out.exists():
        out = RAW_ACLED / f"acled_ukraine_{stamp}_{counter}.csv"
        counter += 1
    df.to_csv(out, index=False)
    log.info("Raw data saved → %s  (%d rows)", out, len(df))
    return out


def build_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df = df.dropna(subset=["event_date"])
    df["month"] = df["event_date"].dt.to_period("M").astype(str)
    df["fatalities"]  = pd.to_numeric(df.get("fatalities", 0), errors="coerce").fillna(0)
    df["is_shelling"] = df["sub_event_type"].isin(SHELLING_SUBTYPES).astype(int)

    agg = (
        df.groupby(["admin2", "month"])
        .agg(
            conflict_events=("event_date", "count"),
            fatalities=("fatalities", "sum"),
            shelling_events=("is_shelling", "sum"),
        )
        .reset_index()
        .rename(columns={"admin2": "unit_id"})
    )
    agg["fatalities"]      = agg["fatalities"].astype(int)
    agg["shelling_events"] = agg["shelling_events"].astype(int)
    return agg.sort_values(["unit_id", "month"]).reset_index(drop=True)


def save_clean(agg: pd.DataFrame) -> Path:
    CLEAN_ACLED.mkdir(parents=True, exist_ok=True)
    out = CLEAN_ACLED / "acled_raion_month.csv"
    agg.to_csv(out, index=False)
    log.info("Clean data saved → %s  (%d rows)", out, len(agg))
    return out


def main():
    token  = get_bearer_token()
    df_raw = download_all(token)

    if df_raw.empty:
        log.warning("No data returned — check region names or API response.")
        sys.exit(0)

    save_raw(df_raw)
    agg = build_clean(df_raw)
    save_clean(agg)

    print("\n── ACLED summary ──────────────────────────────────────")
    print(f"  Raw events downloaded : {len(df_raw):,}")
    print(f"  Raion-month rows      : {len(agg):,}")
    print(f"  Oblasts found         : {sorted(df_raw['admin1'].unique().tolist())}")
    print(f"  Date range            : {df_raw['event_date'].min()} – {df_raw['event_date'].max()}")
    print(f"  Total fatalities      : {int(agg['fatalities'].sum()):,}")
    print("  Sample rows:")
    print(agg.head(5).to_string(index=False))
    print("───────────────────────────────────────────────────────\n")

    # Spot-check: Kharkiv Feb–May 2022 spike
    kharkiv = agg[
        (agg["unit_id"].str.contains("Kharkiv|Харків", case=False, na=False)) &
        (agg["month"].between("2022-02", "2022-05"))
    ]
    if not kharkiv.empty:
        print("Kharkiv conflict events Feb–May 2022 (spot-check):")
        print(kharkiv.to_string(index=False))
    else:
        log.warning("Spot-check: no Kharkiv raion rows for Feb–May 2022 "
                    "(check admin2 name format in raw CSV).")


if __name__ == "__main__":
    main()
