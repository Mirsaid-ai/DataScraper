"""
SpatialRDD/distance.py
─────────────────────────────────────────────────────────────────────────────
Step 1+2+3 of the Spatial RDD pipeline:

  1. Load hromada polygons (HDX OCHA Admin-3) for Chernihiv, Sumy, Kharkiv.
  2. Load ACLED events from the bulk xlsx (Feb 24 – Apr 5, 2022) to construct
     the March 2022 Russian occupation boundary.
  3. Spatially join events to hromadas to classify each hromada as
     "occupied" (had Russian military events inside) or "control".
  4. Build the occupation boundary as the border between occupied and
     non-occupied hromada polygons.
  5. Compute the signed distance (km) from each hromada's centroid to the
     boundary:  negative = occupied side, positive = control side.

Outputs
-------
  data/clean/spatial/hromada_rdd_base.csv  — hromada-level RDD base table
  data/raw/boundaries/occupation_boundary.geojson  — occupation boundary line

Schema of hromada_rdd_base.csv
-------------------------------
  adm3_pcode    — KATOTTG hromada code (e.g. UA6302001)
  adm3_name     — hromada name (English)
  adm1_name     — oblast name (English)
  center_lon    — computed centroid longitude (WGS84)
  center_lat    — computed centroid latitude (WGS84)
  occupied_mar2022   — 1 if hromada had Russian military events in March 2022, 0 otherwise
  dist_km       — signed distance to occupation boundary (negative = occupied side)
  n_events_feb_apr_2022 — count of ACLED events inside this hromada, Feb 24 – Apr 5

Usage
-----
  python SpatialRDD/distance.py [--peek] [--acled-window DAYS]

  --peek             print classification summary without saving outputs
  --acled-window N   use N days from Feb 24 as the occupation window
                     (default: 40 days = Feb 24 → Apr 4)
"""

import argparse
import json
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import unary_union

warnings.filterwarnings("ignore", ".*initial implementation.*")

# ── Paths ─────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
HROMADA_GEOJSON = ROOT / "data/raw/boundaries/geojson/ukr_admin3.geojson"
ACLED_XLSX      = ROOT / "ukraine_full_data_up_to-2026-02-27.xlsx"
OUT_DIR         = ROOT / "data/clean/spatial"
BOUNDARY_OUT    = ROOT / "data/raw/boundaries/occupation_boundary.geojson"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# Target oblasts: ADM1 pcode prefixes
OBLAST_PREFIXES = {
    "UA59": "Sumy",
    "UA63": "Kharkiv",
    "UA74": "Chernihiv",
}

# ACLED oblast names → pcode prefixes
ACLED_OBLAST_MAP = {
    "Sumy":     "UA59",
    "Kharkiv":  "UA63",
    "Chernihiv":"UA74",
}

# Occupation window: Feb 24 – Apr 5, 2022 (40 days)
WINDOW_START = pd.Timestamp("2022-02-24")
DEFAULT_WINDOW_DAYS = 40


# ── Step 1: Load hromada polygons ─────────────────────────────────────────

def load_hromadas() -> gpd.GeoDataFrame:
    """Load and filter hromada polygons for the 3 target oblasts."""
    print("Loading hromada polygons (HDX OCHA Admin-3)…")
    gdf = gpd.read_file(HROMADA_GEOJSON)
    gdf = gdf.to_crs("EPSG:4326")

    # Filter to 3 target oblasts
    mask = gdf["adm3_pcode"].str.startswith(tuple(OBLAST_PREFIXES.keys()))
    gdf = gdf[mask].copy().reset_index(drop=True)

    # Compute centroids (projected to UTM zone 36N for accurate km distances)
    gdf_proj = gdf.to_crs("EPSG:32636")   # UTM zone 36N — covers eastern Ukraine
    gdf["center_lon"] = gdf_proj.geometry.centroid.to_crs("EPSG:4326").x
    gdf["center_lat"] = gdf_proj.geometry.centroid.to_crs("EPSG:4326").y

    # Add oblast English name
    for prefix, name in OBLAST_PREFIXES.items():
        gdf.loc[gdf["adm3_pcode"].str.startswith(prefix), "oblast_en"] = name

    print(f"  Loaded {len(gdf)} hromadas:")
    for name in sorted(gdf["oblast_en"].unique()):
        print(f"    {name}: {(gdf['oblast_en'] == name).sum()} hromadas")

    return gdf


# ── Step 2: Load ACLED events ─────────────────────────────────────────────

def load_acled_events(window_days: int) -> pd.DataFrame:
    """
    Load ACLED events for Feb 24 – (Feb 24 + window_days) 2022,
    in the 3 target oblasts.
    Returns a DataFrame with latitude, longitude, event_type, sub_event_type.
    """
    window_end = WINDOW_START + pd.Timedelta(days=window_days)
    print(f"\nLoading ACLED events: {WINDOW_START.date()} → {window_end.date()}…")

    df = pd.read_excel(
        ACLED_XLSX,
        usecols=["EVENT_DATE", "EVENT_TYPE", "SUB_EVENT_TYPE", "ACTOR1",
                 "ADMIN1", "LATITUDE", "LONGITUDE", "FATALITIES"],
        dtype={"LATITUDE": float, "LONGITUDE": float},
    )
    df["EVENT_DATE"] = pd.to_datetime(df["EVENT_DATE"], dayfirst=True, errors="coerce")

    # Filter: date window + 3 target oblasts
    date_mask  = (df["EVENT_DATE"] >= WINDOW_START) & (df["EVENT_DATE"] <= window_end)
    oblast_mask = df["ADMIN1"].isin(ACLED_OBLAST_MAP.keys())
    df = df[date_mask & oblast_mask].copy()

    # Focus on events that indicate Russian military presence:
    # Battles, shelling, air/drone strikes — NOT civilian targeting
    relevant_types = [
        "Battles", "Explosions/Remote violence", "Strategic developments"
    ]
    df = df[df["EVENT_TYPE"].isin(relevant_types)]

    print(f"  {len(df)} relevant events in window ({', '.join(relevant_types)})")
    for ob, cnt in df["ADMIN1"].value_counts().items():
        print(f"    {ob}: {cnt}")

    return df


# ── Step 3: Spatial join events → hromadas ───────────────────────────────

def classify_hromadas(
    hromadas: gpd.GeoDataFrame,
    events: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """
    For each event point, find which hromada polygon it falls in.
    Mark that hromada as 'occupied' (Russian military activity inside it).
    """
    print("\nSpatially joining events to hromada polygons…")

    # Create event GeoDataFrame
    events_gdf = gpd.GeoDataFrame(
        events,
        geometry=gpd.points_from_xy(events["LONGITUDE"], events["LATITUDE"]),
        crs="EPSG:4326",
    )

    # Spatial join: events → hromada polygons
    joined = gpd.sjoin(
        events_gdf[["geometry", "EVENT_TYPE"]],
        hromadas[["adm3_pcode", "geometry"]],
        how="left",
        predicate="within",
    )

    # Count events per hromada
    event_counts = joined.groupby("adm3_pcode").size().rename("n_events_feb_apr_2022")

    # Merge back
    hromadas = hromadas.merge(event_counts, on="adm3_pcode", how="left")
    hromadas["n_events_feb_apr_2022"] = hromadas["n_events_feb_apr_2022"].fillna(0).astype(int)

    # Classify: occupied = at least 1 relevant military event inside the hromada
    hromadas["occupied_mar2022"] = (hromadas["n_events_feb_apr_2022"] > 0).astype(int)

    print("\nClassification summary:")
    for ob in sorted(hromadas["oblast_en"].unique()):
        sub = hromadas[hromadas["oblast_en"] == ob]
        occ = sub["occupied_mar2022"].sum()
        print(f"  {ob}: {occ}/{len(sub)} hromadas classified as occupied")

    occupied_names = hromadas[hromadas["occupied_mar2022"] == 1][["adm3_name", "oblast_en", "n_events_feb_apr_2022"]]
    print(f"\nOccupied hromadas ({len(occupied_names)} total):")
    for _, row in occupied_names.sort_values(["oblast_en", "adm3_name"]).iterrows():
        print(f"  {row['oblast_en']:10} | {row['adm3_name']} ({row['n_events_feb_apr_2022']} events)")

    return hromadas


# ── Step 4: Build occupation boundary ────────────────────────────────────

def build_boundary(hromadas: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Construct the occupation boundary as the shared border between
    occupied and non-occupied hromada polygons.

    The boundary is a LineString / MultiLineString representing the
    March 2022 maximum advance line within our 3 target oblasts.
    """
    print("\nBuilding occupation boundary line…")

    occupied     = hromadas[hromadas["occupied_mar2022"] == 1]["geometry"]
    not_occupied = hromadas[hromadas["occupied_mar2022"] == 0]["geometry"]

    if len(occupied) == 0:
        raise ValueError("No occupied hromadas found — check ACLED data")

    occ_union     = unary_union(occupied)
    not_occ_union = unary_union(not_occupied)

    # The boundary is the intersection of the two merged polygons' borders
    boundary = occ_union.boundary.intersection(not_occ_union.boundary)

    # Also include the external border of occupied hromadas (where Russia
    # bordered other oblasts or the Russian border itself)
    outer_boundary = occ_union.boundary

    boundary_gdf = gpd.GeoDataFrame(
        {"name": ["occupation_boundary"], "type": ["shared_border"]},
        geometry=[boundary],
        crs="EPSG:4326",
    )

    print(f"  Boundary geometry type: {boundary.geom_type}")
    print(f"  Boundary saved to: {BOUNDARY_OUT}")
    return boundary_gdf, boundary


# ── Step 5: Compute signed distances ──────────────────────────────────────

def compute_signed_distances(
    hromadas: gpd.GeoDataFrame,
    boundary,
) -> gpd.GeoDataFrame:
    """
    Compute signed distance (km) from each hromada centroid to the
    occupation boundary.
    Negative = occupied side, positive = control (non-occupied) side.
    """
    print("\nComputing signed distances to occupation boundary…")

    # Project to UTM 36N for accurate km distances
    hromadas_proj = hromadas.to_crs("EPSG:32636")
    boundary_proj  = (
        gpd.GeoDataFrame(geometry=[boundary], crs="EPSG:4326")
        .to_crs("EPSG:32636")
        .geometry[0]
    )

    distances = []
    for _, row in hromadas_proj.iterrows():
        centroid = Point(row["center_lon"], row["center_lat"])
        centroid_proj = (
            gpd.GeoDataFrame(geometry=[centroid], crs="EPSG:4326")
            .to_crs("EPSG:32636")
            .geometry[0]
        )
        dist_m = centroid_proj.distance(boundary_proj)
        dist_km = dist_m / 1000.0
        # Sign: negative for occupied hromadas
        sign = -1 if row["occupied_mar2022"] == 1 else 1
        distances.append(sign * dist_km)

    hromadas["dist_km"] = distances

    print(f"  Distance range: {min(distances):.1f} km to {max(distances):.1f} km")
    print(f"  Occupied side: {len([d for d in distances if d < 0])} hromadas (dist < 0)")
    print(f"  Control side:  {len([d for d in distances if d > 0])} hromadas (dist > 0)")

    return hromadas


# ── Main ──────────────────────────────────────────────────────────────────

def run(peek: bool = False, window_days: int = DEFAULT_WINDOW_DAYS) -> None:
    # Step 1: Hromada polygons
    hromadas = load_hromadas()

    # Step 2: ACLED events
    events = load_acled_events(window_days)

    # Step 3: Classify occupied vs. control
    hromadas = classify_hromadas(hromadas, events)

    if peek:
        print("\n[PEEK MODE] — not saving outputs.")
        return

    # Step 4: Build boundary
    boundary_gdf, boundary_geom = build_boundary(hromadas)
    boundary_gdf.to_file(BOUNDARY_OUT, driver="GeoJSON")

    # Step 5: Signed distances
    hromadas = compute_signed_distances(hromadas, boundary_geom)

    # Save RDD base table
    out_cols = [
        "adm3_pcode", "adm3_name", "adm1_name", "oblast_en",
        "center_lon", "center_lat",
        "occupied_mar2022", "dist_km", "n_events_feb_apr_2022",
    ]
    # adm1_name is from the GeoJSON; map it in English
    if "adm1_name" not in hromadas.columns:
        hromadas["adm1_name"] = hromadas["oblast_en"]

    out = hromadas[[c for c in out_cols if c in hromadas.columns]].copy()
    out_path = OUT_DIR / "hromada_rdd_base.csv"
    out.to_csv(out_path, index=False)
    print(f"\n✓ Saved RDD base table: {out_path}  ({len(out)} rows)")

    # Quick bandwidth check
    for bw in [10, 20, 30, 50]:
        in_bw = ((out["dist_km"].abs() <= bw)).sum()
        occ_bw = ((out["dist_km"] >= -bw) & (out["dist_km"] < 0)).sum()
        ctrl_bw = ((out["dist_km"] > 0) & (out["dist_km"] <= bw)).sum()
        print(f"  Bandwidth ±{bw:2d} km: {in_bw} hromadas total "
              f"({occ_bw} occupied / {ctrl_bw} control)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build RDD running variable: signed distance to March 2022 occupation boundary"
    )
    parser.add_argument("--peek", action="store_true",
                        help="Print summary without saving outputs")
    parser.add_argument("--acled-window", type=int, default=DEFAULT_WINDOW_DAYS,
                        dest="window_days",
                        help=f"Days from Feb 24 to use as occupation window (default: {DEFAULT_WINDOW_DAYS})")
    args = parser.parse_args()
    run(peek=args.peek, window_days=args.window_days)
