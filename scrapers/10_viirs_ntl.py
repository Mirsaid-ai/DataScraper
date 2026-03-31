"""
scrapers/10_viirs_ntl.py
─────────────────────────────────────────────────────────────────────────────
Download and extract VIIRS Nighttime Lights (NTL) per hromada.

Data: NASA/NOAA VIIRS DNB Monthly Composites (VNL v2, cloud-free average radiance)
Source: Earth Observation Group (EOG), Colorado School of Mines
URL: https://eogdata.mines.edu/products/vnl/

AUTHENTICATION: EOG monthly data requires a free account (register at
  https://eogdata.mines.edu/realms/eog/login-actions/registration).
  After registering, set:
    export EOG_USER=your@email.com
    export EOG_PASS=yourpassword

ALTERNATIVE (no auth): NASA AppEEARS API (https://appeears.earthdatacloud.nasa.gov)
  Uses NASA Earthdata login (free registration at https://urs.earthdata.nasa.gov).
  Set: export EARTHDATA_USER=xxx  export EARTHDATA_PASS=xxx

OUTPUT
------
  data/raw/viirs/              — raw monthly GeoTIFFs (Ukraine subset)
  data/clean/viirs/ntl_hromada_month.csv — extracted per-hromada radiance:
    adm3_pcode | month | mean_rad | median_rad | sum_rad | n_pixels

USAGE
-----
  python scrapers/10_viirs_ntl.py --method eog --year 2021 2022 2023 2024
  python scrapers/10_viirs_ntl.py --method appeears
  python scrapers/10_viirs_ntl.py --peek    # check what's available
"""

import argparse
import json
import os
import time
import urllib.request
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

# Optional imports (required for raster processing)
try:
    import rasterio
    from rasterio.mask import mask as rasterio_mask
    HAS_RASTERIO = True
except ImportError:
    HAS_RASTERIO = False

ROOT         = Path(__file__).resolve().parent.parent
RAW_VIIRS    = ROOT / "data/raw/viirs"
CLEAN_VIIRS  = ROOT / "data/clean/viirs"
HROMADA_GEOJSON = ROOT / "data/raw/boundaries/geojson/ukr_admin3.geojson"

RAW_VIIRS.mkdir(parents=True, exist_ok=True)
CLEAN_VIIRS.mkdir(parents=True, exist_ok=True)

YEARS = [2021, 2022, 2023, 2024]

# Ukraine bounding box for tile subsetting (WGS84)
UKR_BBOX = (22.0, 44.3, 40.3, 52.4)  # (west, south, east, north)

# EOG monthly file URL pattern (v10, vcmcfg product)
# Files require EOG login
EOG_BASE = "https://eogdata.mines.edu/nighttime_light/monthly/v10"

# Target oblast prefixes
OBLAST_PREFIXES = ("UA59", "UA63", "UA74")


# ─────────────────────────────────────────────────────────────────────────────
# EOG authentication
# ─────────────────────────────────────────────────────────────────────────────

def get_eog_token() -> str | None:
    """Obtain EOG bearer token via Keycloak password grant."""
    user = os.getenv("EOG_USER")
    pwd  = os.getenv("EOG_PASS")
    if not user or not pwd:
        print("  EOG credentials not set. Set EOG_USER and EOG_PASS env vars.")
        print("  Register free at https://eogdata.mines.edu/realms/eog/login-actions/registration")
        return None

    token_url = "https://eogdata.mines.edu/realms/eog/protocol/openid-connect/token"
    data = f"client_id=eogdata-new-apache&grant_type=password&username={user}&password={pwd}"
    req  = urllib.request.Request(
        token_url,
        data=data.encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.load(r)
            token = resp.get("access_token")
            print(f"  EOG auth OK — token valid {resp.get('expires_in',0)//60} min")
            return token
    except Exception as e:
        print(f"  EOG auth failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# EOG: list and download monthly composite tiles for Ukraine
# ─────────────────────────────────────────────────────────────────────────────

def list_eog_files(year: int, month: int, token: str) -> list[str]:
    """List available VNL files for a given year-month from EOG."""
    ym = f"{year}{month:02d}"
    url = f"{EOG_BASE}/{year}/{ym}/"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
        # Extract .tif file links containing avg_rade9h (average radiance)
        import re
        files = re.findall(r'href="([^"]+avg_rade9h\.tif\.gz)"', html)
        return [url + f if not f.startswith("http") else f for f in files]
    except Exception as e:
        print(f"  EOG listing failed for {ym}: {e}")
        return []


def download_eog_tile(file_url: str, out_path: Path, token: str) -> bool:
    """Download a single EOG .tif.gz file."""
    if out_path.exists() and out_path.stat().st_size > 1_000_000:
        return True  # cached
    req = urllib.request.Request(file_url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            data = r.read()
        with open(out_path, "wb") as f:
            f.write(data)
        print(f"    Downloaded {out_path.name} ({len(data)//1024:,} KB)")
        return True
    except Exception as e:
        print(f"    Download failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# NASA AppEEARS API (alternative, no EOG account needed — uses NASA Earthdata)
# ─────────────────────────────────────────────────────────────────────────────

def submit_appeears_task(hromada_gdf: gpd.GeoDataFrame) -> str | None:
    """
    Submit an AppEEARS area extraction task for VNP46A3 (monthly Black Marble)
    over the 3 target oblasts. Returns task_id.

    Requires NASA Earthdata account — register at https://urs.earthdata.nasa.gov
    Set env vars: EARTHDATA_USER, EARTHDATA_PASS
    """
    user = os.getenv("EARTHDATA_USER")
    pwd  = os.getenv("EARTHDATA_PASS")
    if not user or not pwd:
        print("  AppEEARS: set EARTHDATA_USER and EARTHDATA_PASS")
        print("  Register free at https://urs.earthdata.nasa.gov/users/new")
        return None

    api = "https://appeears.earthdatacloud.nasa.gov/api"

    # 1. Login
    import base64
    creds = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    req = urllib.request.Request(
        f"{api}/login",
        method="POST",
        headers={"Authorization": f"Basic {creds}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            token = json.load(r)["token"]
    except Exception as e:
        print(f"  AppEEARS login failed: {e}")
        return None

    # 2. Prepare GeoJSON of target hromada polygons
    target_gdf = hromada_gdf[
        hromada_gdf["adm3_pcode"].str.startswith(OBLAST_PREFIXES)
    ].copy()
    geojson_str = target_gdf.geometry.to_json()

    # 3. Submit task
    task = {
        "task_type": "area",
        "task_name": "ukraine_ntl_hromadas",
        "params": {
            "dates": [
                {"startDate": "01-01-2021", "endDate": "12-31-2024"}
            ],
            "layers": [
                {
                    "product": "VNP46A3.001",   # Black Marble monthly
                    "layer": "AllAngle_Composite_Snow_Free",
                }
            ],
            "output": {"format": {"type": "geotiff"}, "projection": "geographic"},
            "geo": json.loads(geojson_str),
        },
    }

    req = urllib.request.Request(
        f"{api}/task",
        data=json.dumps(task).encode(),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.load(r)
            task_id = resp.get("task_id")
            print(f"  AppEEARS task submitted: {task_id}")
            print(f"  Monitor at: https://appeears.earthdatacloud.nasa.gov/task/{task_id}")
            return task_id
    except Exception as e:
        print(f"  AppEEARS task submission failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Zonal statistics: extract per-hromada radiance from GeoTIFF
# ─────────────────────────────────────────────────────────────────────────────

def extract_zonal_stats(
    tif_path: Path,
    hromada_gdf: gpd.GeoDataFrame,
    month: str,
) -> pd.DataFrame:
    """
    Extract mean/median/sum radiance for each hromada polygon from a GeoTIFF.
    Returns DataFrame with: adm3_pcode | month | mean_rad | median_rad | sum_rad | n_pixels
    """
    if not HAS_RASTERIO:
        raise ImportError("rasterio is required: pip install rasterio")

    rows = []
    with rasterio.open(tif_path) as src:
        for _, hrom in hromada_gdf.iterrows():
            pcode = hrom["adm3_pcode"]
            geom  = [hrom.geometry.__geo_interface__]
            try:
                out_image, _ = rasterio_mask(src, geom, crop=True, nodata=src.nodata)
                data = out_image.flatten()
                # Remove no-data and negative values (cloud mask = -1.5e9)
                nodata = src.nodata if src.nodata else -1.5e9
                data = data[(data != nodata) & (data >= 0)]
                if len(data) == 0:
                    rows.append({"adm3_pcode": pcode, "month": month,
                                  "mean_rad": 0.0, "median_rad": 0.0,
                                  "sum_rad": 0.0, "n_pixels": 0})
                else:
                    rows.append({
                        "adm3_pcode":  pcode,
                        "month":       month,
                        "mean_rad":    float(np.mean(data)),
                        "median_rad":  float(np.median(data)),
                        "sum_rad":     float(np.sum(data)),
                        "n_pixels":    len(data),
                    })
            except Exception:
                rows.append({"adm3_pcode": pcode, "month": month,
                              "mean_rad": np.nan, "median_rad": np.nan,
                              "sum_rad": np.nan, "n_pixels": 0})
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run_eog(years: list[int]) -> None:
    """Download monthly EOG VIIRS tiles and extract per-hromada radiance."""
    print("\n=== EOG VIIRS Download ===")

    token = get_eog_token()
    if not token:
        return

    # Load hromada polygons
    gdf = gpd.read_file(HROMADA_GEOJSON)
    gdf = gdf[gdf["adm3_pcode"].str.startswith(OBLAST_PREFIXES)].copy()
    gdf = gdf.to_crs("EPSG:4326")
    print(f"Target hromadas: {len(gdf)}")

    all_rows = []

    for year in years:
        for month in range(1, 13):
            month_str = f"{year}-{month:02d}"
            print(f"\n  {month_str}:")

            # List and download tile
            files = list_eog_files(year, month, token)
            if not files:
                print(f"    No files found")
                continue

            # Download the first available file (usually global composite)
            f_url  = files[0]
            f_name = f_url.split("/")[-1]
            out_gz = RAW_VIIRS / f_name
            out_tif = RAW_VIIRS / f_name.replace(".gz", "")

            if not out_tif.exists():
                if download_eog_tile(f_url, out_gz, token):
                    # Decompress
                    import gzip, shutil
                    with gzip.open(out_gz, "rb") as gz_in:
                        with open(out_tif, "wb") as tif_out:
                            shutil.copyfileobj(gz_in, tif_out)
                    out_gz.unlink()  # remove .gz
                else:
                    continue

            # Extract zonal stats
            if HAS_RASTERIO and out_tif.exists():
                rows = extract_zonal_stats(out_tif, gdf, month_str)
                all_rows.append(rows)
                print(f"    Extracted radiance for {len(rows)} hromadas")

            time.sleep(0.5)  # polite

    if all_rows:
        df = pd.concat(all_rows, ignore_index=True)
        out = CLEAN_VIIRS / "ntl_hromada_month.csv"
        df.to_csv(out, index=False)
        print(f"\n✓ VIIRS NTL saved: {out} ({len(df):,} rows)")
    else:
        print("\nNo data extracted.")


def run_appeears() -> None:
    """Submit AppEEARS task for Black Marble monthly extraction."""
    print("\n=== NASA AppEEARS Submission ===")
    gdf = gpd.read_file(HROMADA_GEOJSON)
    task_id = submit_appeears_task(gdf)
    if task_id:
        # Save task ID for later retrieval
        with open(CLEAN_VIIRS / "appeears_task_id.txt", "w") as f:
            f.write(task_id)


def run_peek() -> None:
    """Check what VIIRS data is already available locally."""
    print("\n=== VIIRS Data Status ===")
    tifs = sorted(RAW_VIIRS.glob("*.tif"))
    print(f"Raw TIF files: {len(tifs)}")
    for f in tifs[:10]:
        print(f"  {f.name} ({f.stat().st_size//1024:,} KB)")

    out = CLEAN_VIIRS / "ntl_hromada_month.csv"
    if out.exists():
        df = pd.read_csv(out)
        print(f"\nExtracted NTL rows: {len(df):,}")
        print(f"Hromadas: {df['adm3_pcode'].nunique()}")
        print(f"Months:   {df['month'].nunique()} ({df['month'].min()} → {df['month'].max()})")
        print(f"Mean radiance range: {df['mean_rad'].min():.4f} – {df['mean_rad'].max():.4f}")
    else:
        print("\nNo extracted NTL data yet.")
        print("\nTo get VIIRS data, choose one of:")
        print("  Option A — EOG (free account required):")
        print("    1. Register at https://eogdata.mines.edu/realms/eog/login-actions/registration")
        print("    2. export EOG_USER=your@email.com EOG_PASS=yourpassword")
        print("    3. python scrapers/10_viirs_ntl.py --method eog")
        print()
        print("  Option B — NASA AppEEARS (free NASA Earthdata account):")
        print("    1. Register at https://urs.earthdata.nasa.gov/users/new")
        print("    2. export EARTHDATA_USER=xxx EARTHDATA_PASS=xxx")
        print("    3. python scrapers/10_viirs_ntl.py --method appeears")
        print("    4. Data delivered to your AppEEARS account in ~30 min")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download VIIRS NTL per hromada")
    parser.add_argument("--method", choices=["eog", "appeears", "peek"], default="peek")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    args = parser.parse_args()

    if args.method == "eog":
        run_eog(args.years)
    elif args.method == "appeears":
        run_appeears()
    else:
        run_peek()
