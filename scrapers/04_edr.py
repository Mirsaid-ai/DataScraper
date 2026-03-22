"""
04_edr.py — Build NATIONAL business registration panel from Ukraine's official EDR.

Source: Єдиний державний реєстр юридичних осіб, ФОП та громадських формувань
        https://data.gov.ua/dataset/a1799820-195b-4982-8141-6e84f58103e7

LIMITATION — NO OBLAST FILTER:
  The bulk XML export from data.gov.ua does NOT include an ADDRESS field
  (confirmed by the official UO_schema.xsd). Oblast-level filtering is not
  possible from this dataset alone. As a workaround, this script produces
  NATIONAL monthly totals. To obtain oblast-level counts you would need a
  cross-reference file mapping EDRPOU codes to KOATUU (oblast) codes; such a
  file is not freely available in bulk form as of March 2026.

Strategy:
  Download UO.zip (companies, ~385 MB) and FOP.zip (sole proprietors, ~534 MB)
  once. Stream-parse the XML inside each ZIP with iterparse (memory-efficient).
  Aggregate ALL new registrations by month (national scope).

Usage:
    python scrapers/04_edr.py            # full run (downloads if needed ~900 MB)
    python scrapers/04_edr.py --peek     # inspect first 5 XML records and exit
    python scrapers/04_edr.py --no-download  # skip download, use existing ZIPs

Outputs:
    data/raw/opendatabot/uo.zip
    data/raw/opendatabot/fop.zip
    data/clean/opendatabot/edr_national_month.csv
        schema: source | month | new_companies | new_fops
"""

import argparse
import logging
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    CLEAN_OPENDATABOT, DATE_END, DATE_START,
    RAW_OPENDATABOT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── EDR download URLs ─────────────────────────────────────────────────────────
EDR_URLS = {
    "uo":  "https://data.gov.ua/dataset/03cc1239-3988-4451-aa0d-aadb77448714/resource/d40cc921-39bb-44fd-be06-dc02589f45c6/download/uo.zip",
    "fop": "https://data.gov.ua/dataset/03cc1239-3988-4451-aa0d-aadb77448714/resource/c262938f-cce7-4489-a805-2fd7c5a44e0b/download/fop.zip",
}

# ── Date parsing ──────────────────────────────────────────────────────────────
# REGISTRATION field: "29.12.1995; 27.10.2006; 14801200000030147"
# The first date is initial registration date (used for new-business counting).
DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

DATE_START_PD = pd.Period(DATE_START, "M")
DATE_END_PD   = pd.Period(DATE_END,   "M")


def parse_reg_date(reg_str: str) -> pd.Period | None:
    """Parse 'DD.MM.YYYY ...' → pd.Period month, or None."""
    if not reg_str:
        return None
    m = DATE_RE.search(reg_str)
    if not m:
        return None
    try:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        p = pd.Period(f"{year}-{month:02d}", "M")
        if DATE_START_PD <= p <= DATE_END_PD:
            return p
    except (ValueError, Exception):
        pass
    return None


# ── Download ──────────────────────────────────────────────────────────────────

def download_zip(key: str, out_path: Path, force: bool = False, max_retries: int = 10):
    """Download UO or FOP zip to out_path with resume support. Skip if complete."""
    url = EDR_URLS[key]
    RAW_OPENDATABOT.mkdir(parents=True, exist_ok=True)

    headers = {"User-Agent": "econ590-research/1.0"}

    # Get expected file size
    head = requests.head(url, headers=headers, timeout=30, allow_redirects=True)
    total = int(head.headers.get("Content-Length", 0))

    if out_path.exists() and not force:
        existing = out_path.stat().st_size
        if total and existing >= total:
            log.info("%s already complete (%s MB), skipping.",
                     out_path.name, f"{existing / 1e6:.0f}")
            return
        if existing > 0:
            log.info("%s partial (%s / %s MB), resuming …",
                     out_path.name, f"{existing / 1e6:.0f}", f"{total / 1e6:.0f}")

    log.info("Downloading %s → %s", url.split("/")[-1], out_path)

    for attempt in range(1, max_retries + 1):
        offset = out_path.stat().st_size if out_path.exists() else 0
        req_headers = {**headers}
        if offset:
            req_headers["Range"] = f"bytes={offset}-"

        try:
            resp = requests.get(url, headers=req_headers, stream=True, timeout=120)
            if resp.status_code == 416:  # Range Not Satisfiable → already complete
                log.info("%s already complete (server says 416).", out_path.name)
                return
            resp.raise_for_status()

            mode = "ab" if offset else "wb"
            with open(out_path, mode) as f, tqdm(
                total=total, initial=offset,
                unit="B", unit_scale=True, desc=out_path.name,
            ) as pbar:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
                    pbar.update(len(chunk))

            log.info("Saved %s (%.0f MB)", out_path.name, out_path.stat().st_size / 1e6)
            return

        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError) as exc:
            log.warning("Attempt %d/%d failed: %s — retrying in 5 s …",
                        attempt, max_retries, exc)
            time.sleep(5)

    raise RuntimeError(f"Failed to download {url} after {max_retries} attempts")


# ── XML stream parser ─────────────────────────────────────────────────────────

def iter_subjects(zip_path: Path, peek: int = 0):
    """
    Yield dicts with keys: REGISTRATION, NAME, STAN, RECORD.
    Note: ADDRESS is not present in the bulk export (confirmed by schema).
    Uses iterparse for memory efficiency on 1-2 GB XML files.
    If peek > 0, stop after that many subjects.
    """
    with zipfile.ZipFile(zip_path) as zf:
        xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not xml_names:
            log.error("No XML file found in %s", zip_path)
            return
        xml_name = xml_names[0]
        log.info("Parsing %s / %s", zip_path.name, xml_name)

        with zf.open(xml_name) as fh:
            subject: dict = {}
            count = 0
            for event, elem in ET.iterparse(fh, events=("start", "end")):
                if event == "start" and elem.tag == "SUBJECT":
                    subject = {}
                elif event == "end":
                    tag = elem.tag
                    text = (elem.text or "").strip()
                    if tag in ("REGISTRATION", "RECORD", "NAME", "STAN"):
                        subject[tag] = text
                    elif tag == "SUBJECT":
                        yield subject
                        count += 1
                        elem.clear()
                        if peek and count >= peek:
                            return
                        if count % 200_000 == 0:
                            log.info("  … %d subjects processed", count)


# ── Aggregation (national) ────────────────────────────────────────────────────

def aggregate(zip_path: Path, kind: str) -> dict[str, int]:
    """
    Parse ZIP and return {'YYYY-MM': count} of new registrations nationally.
    kind: 'uo' (companies) or 'fop' (sole proprietors).
    """
    counts: dict[str, int] = defaultdict(int)
    skipped_out_of_range = 0

    for subj in iter_subjects(zip_path):
        reg_str = subj.get("REGISTRATION", "")
        period = parse_reg_date(reg_str)
        if period is None:
            skipped_out_of_range += 1
            continue
        counts[str(period)] += 1

    log.info(
        "%s: %d in-range | %d out_of_range",
        kind.upper(), sum(counts.values()), skipped_out_of_range,
    )
    return dict(counts)


# ── Build clean DataFrame ─────────────────────────────────────────────────────

def build_clean(uo_counts: dict, fop_counts: dict) -> pd.DataFrame:
    """Merge UO and FOP national counts into a tidy month DataFrame."""
    all_months = sorted(set(uo_counts) | set(fop_counts))
    rows = []
    for month in all_months:
        rows.append({
            "source":        "edr_data.gov.ua",
            "month":         month,
            "new_companies": uo_counts.get(month, 0),
            "new_fops":      fop_counts.get(month, 0),
        })
    df = pd.DataFrame(rows)
    return df.sort_values("month").reset_index(drop=True)


def save_clean(df: pd.DataFrame) -> Path:
    CLEAN_OPENDATABOT.mkdir(parents=True, exist_ok=True)
    out = CLEAN_OPENDATABOT / "edr_national_month.csv"
    df.to_csv(out, index=False)
    log.info("Clean data → %s  (%d rows)", out, len(df))
    return out


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="EDR business registration scraper")
    parser.add_argument("--peek", action="store_true",
                        help="Print first 20 XML records and exit (no aggregation)")
    parser.add_argument("--no-download", action="store_true",
                        help="Skip download, use existing ZIPs in data/raw/opendatabot/")
    args = parser.parse_args()

    uo_path  = RAW_OPENDATABOT / "uo.zip"
    fop_path = RAW_OPENDATABOT / "fop.zip"

    # ── Download ──────────────────────────────────────────────────────────────
    if not args.no_download:
        download_zip("uo",  uo_path)
        download_zip("fop", fop_path)

    for p in (uo_path, fop_path):
        if not p.exists():
            log.error("Missing: %s — run without --no-download", p)
            sys.exit(1)

    # ── Peek mode ─────────────────────────────────────────────────────────────
    if args.peek:
        log.info("=== UO peek ===")
        for i, subj in enumerate(iter_subjects(uo_path, peek=5)):
            print(f"UO {i+1}: {subj}")
        log.info("=== FOP peek ===")
        for i, subj in enumerate(iter_subjects(fop_path, peek=5)):
            print(f"FOP {i+1}: {subj}")
        return

    # ── Aggregate ─────────────────────────────────────────────────────────────
    log.info("Aggregating companies (UO) — national totals …")
    uo_counts = aggregate(uo_path, "uo")

    log.info("Aggregating sole proprietors (FOP) — national totals …")
    fop_counts = aggregate(fop_path, "fop")

    if not uo_counts and not fop_counts:
        log.warning("No in-range records found. Run with --peek to inspect XML structure.")
        sys.exit(1)

    df = build_clean(uo_counts, fop_counts)
    save_clean(df)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n── EDR national summary ────────────────────────────────────────")
    print(f"  NOTE: oblast filter not available (no ADDRESS in bulk export)")
    print(f"  Month range     : {df['month'].min()} – {df['month'].max()}")
    print(f"  Rows            : {len(df)}")
    print(f"  Total companies : {df['new_companies'].sum():,}")
    print(f"  Total FOPs      : {df['new_fops'].sum():,}")
    print("\n  Sample (first 8 months):")
    print(df.head(8).to_string(index=False))
    print("────────────────────────────────────────────────────────────────\n")

    # Spot-check: expect national drop in Feb-May 2022
    feb22 = df[df["month"] == "2022-02"]
    jan22 = df[df["month"] == "2022-01"]
    if not feb22.empty and not jan22.empty:
        print("Spot-check — national new companies Jan vs Feb 2022:")
        print(f"  Jan 2022: {int(jan22['new_companies'].values[0]):,}")
        print(f"  Feb 2022: {int(feb22['new_companies'].values[0]):,} ← expect sharp drop")


if __name__ == "__main__":
    main()
