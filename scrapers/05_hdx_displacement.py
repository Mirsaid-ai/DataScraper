"""
05_hdx_displacement.py — Download IOM DTM / UNHCR displacement data from HDX.

Uses the CKAN API at data.humdata.org to search for Ukraine displacement
datasets, then downloads available CSVs programmatically.

Usage:
    python scrapers/05_hdx_displacement.py

Outputs:
    data/raw/hdx_displacement/<dataset_name>/<resource_name>.csv
    data/clean/hdx_displacement/  (future: raion-month IDP counts)

Note: HDX data coverage and format varies by dataset. This script downloads
all matching resources; manual review is required to select the right files
and build the raion-month aggregate.
"""

import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    DELAY_SECONDS, HDX_API_BASE, HDX_SEARCH_TERMS,
    RAW_HDX,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "econ590-research/1.0"}

# Only download these file formats
ALLOWED_FORMATS = {"CSV", "XLSX", "XLS"}

# Cap on total resources to download per search term (safety limit)
MAX_RESOURCES_PER_TERM = 50


def search_datasets(session: requests.Session, query: str,
                    rows: int = 20) -> list[dict]:
    """Search HDX CKAN for datasets matching query. Returns list of dataset dicts."""
    url = f"{HDX_API_BASE}/action/package_search"
    params = {
        "q":              query,
        "rows":           rows,
        "fq":             'organization:"iom" OR organization:"unhcr"',
        "sort":           "metadata_modified desc",
    }
    try:
        resp = session.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        datasets = result.get("result", {}).get("results", [])
        log.info("Query '%s' → %d datasets", query, len(datasets))
        return datasets
    except (requests.RequestException, KeyError, ValueError) as exc:
        log.error("Dataset search failed for '%s': %s", query, exc)
        return []


def list_resources(dataset: dict) -> list[dict]:
    """Extract downloadable CSV/XLSX resources from a dataset dict."""
    resources = []
    for r in dataset.get("resources", []):
        fmt = (r.get("format") or "").upper()
        if fmt in ALLOWED_FORMATS and r.get("url"):
            resources.append({
                "dataset_name": dataset.get("name", "unknown"),
                "dataset_title": dataset.get("title", ""),
                "resource_id":   r.get("id", ""),
                "resource_name": r.get("name", ""),
                "format":        fmt,
                "url":           r["url"],
                "last_modified": r.get("last_modified", ""),
            })
    return resources


def safe_filename(name: str) -> str:
    """Convert a string to a safe filename component."""
    return re.sub(r"[^\w\-.]", "_", name)[:80]


def download_resource(session: requests.Session, resource: dict) -> Path | None:
    """Download one resource file. Returns saved path or None on failure."""
    dataset_dir = RAW_HDX / safe_filename(resource["dataset_name"])
    dataset_dir.mkdir(parents=True, exist_ok=True)

    ext  = resource["format"].lower()
    name = safe_filename(resource["resource_name"] or resource["resource_id"])
    out  = dataset_dir / f"{name}.{ext}"

    # Never overwrite raw data
    if out.exists():
        log.debug("Already exists, skipping: %s", out)
        return out

    try:
        resp = session.get(resource["url"], headers=HEADERS, timeout=60, stream=True)
        if resp.status_code != 200:
            log.warning("HTTP %s for %s", resp.status_code, resource["url"])
            return None

        with open(out, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        log.info("Downloaded → %s", out)
        return out

    except requests.RequestException as exc:
        log.error("Download failed %s: %s", resource["url"], exc)
        return None


def save_manifest(resources: list[dict], downloaded: list[Path | None]):
    """Save a manifest CSV listing all found resources and download status."""
    RAW_HDX.mkdir(parents=True, exist_ok=True)
    stamp = datetime.today().strftime("%Y%m%d")
    out   = RAW_HDX / f"manifest_{stamp}.csv"

    rows = []
    for r, path in zip(resources, downloaded):
        rows.append({
            **r,
            "local_path": str(path) if path else "",
            "downloaded": path is not None,
        })

    df = pd.DataFrame(rows)
    df.to_csv(out, index=False)
    log.info("Manifest saved → %s  (%d resources)", out, len(df))
    return out


def build_clean_stub(downloaded_paths: list[Path]):
    """
    Attempt basic cleaning on downloaded CSVs to identify IDP count columns.
    Saves a stub merged file for manual review; full raion-month aggregation
    requires dataset-specific column mapping.
    """
    clean_dir = RAW_HDX.parent.parent / "clean" / "hdx_displacement"
    clean_dir.mkdir(parents=True, exist_ok=True)

    summaries = []
    for path in downloaded_paths:
        if path is None or not path.exists():
            continue
        try:
            if path.suffix.lower() == ".csv":
                df = pd.read_csv(path, nrows=5, encoding="utf-8", errors="replace")
            else:
                df = pd.read_excel(path, nrows=5)

            summaries.append({
                "file":    str(path.relative_to(RAW_HDX)),
                "rows":    "?",
                "columns": ", ".join(df.columns.tolist()[:10]),
            })
        except Exception as exc:
            log.debug("Could not preview %s: %s", path, exc)

    if summaries:
        summary_df = pd.DataFrame(summaries)
        out = clean_dir / "dataset_column_preview.csv"
        summary_df.to_csv(out, index=False)
        log.info("Column preview → %s", out)


def main():
    session = requests.Session()
    all_resources: list[dict] = []

    # Step 1: Search for relevant datasets
    for term in HDX_SEARCH_TERMS:
        datasets = search_datasets(session, term)
        for ds in datasets:
            resources = list_resources(ds)
            all_resources.extend(resources)
            if len(all_resources) >= MAX_RESOURCES_PER_TERM * len(HDX_SEARCH_TERMS):
                break
        time.sleep(DELAY_SECONDS)

    # De-duplicate by resource_id
    seen: set[str] = set()
    unique_resources: list[dict] = []
    for r in all_resources:
        key = r["resource_id"] or r["url"]
        if key not in seen:
            seen.add(key)
            unique_resources.append(r)

    log.info("Total unique resources to download: %d", len(unique_resources))

    if not unique_resources:
        log.warning("No resources found. Check HDX search terms and API availability.")
        sys.exit(0)

    # Step 2: Download
    downloaded: list[Path | None] = []
    for resource in tqdm(unique_resources, desc="HDX downloads"):
        path = download_resource(session, resource)
        downloaded.append(path)
        time.sleep(DELAY_SECONDS)

    # Step 3: Save manifest
    save_manifest(unique_resources, downloaded)

    # Step 4: Quick column preview for manual mapping
    build_clean_stub(downloaded)

    # ── Summary ───────────────────────────────────────────────────────────────
    n_ok = sum(1 for p in downloaded if p is not None)
    print("\n── HDX Displacement summary ───────────────────────────")
    print(f"  Resources found    : {len(unique_resources)}")
    print(f"  Successfully saved : {n_ok}")
    print(f"  Failed             : {len(unique_resources) - n_ok}")
    print(f"  Raw directory      : {RAW_HDX}")
    print()
    print("  Next step: review data/raw/hdx_displacement/manifest_*.csv")
    print("  and data/clean/hdx_displacement/dataset_column_preview.csv")
    print("  to identify the correct files and column mappings for")
    print("  raion-month IDP counts.")
    print("───────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
