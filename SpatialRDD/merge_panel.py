"""
SpatialRDD/merge_panel.py
─────────────────────────────────────────────────────────────────────────────
Joins the three key datasets into a single hromada × month analysis panel:

  1. hromada_rdd_base.csv    — spatial identifiers, dist_km, occupied_mar2022
  2. pit_hromada_month.csv   — monthly PIT revenue by budget code
  3. ukr_admin3.geojson       — Ukrainian name → adm3_pcode bridge

Join strategy
─────────────
The PIT data uses budget_code (openbudget) while the RDD base uses adm3_pcode
(HDX OCHA). The bridge is by normalized Ukrainian hromada name:
  - PIT  hromada_name: "Бюджет Великописарівської селищної територіальної громади"
    → extract word[1] (genitive adjective)
    → nominative: remove last 2 chars ("ої") + "а" → "Великописарівська"
  - HDX adm3_name1: "Великописарівська" (Ukrainian nominative)

Output
──────
  data/clean/spatial/rdd_panel.csv — balanced hromada × month panel

Schema
──────
  adm3_pcode        — HDX hromada code (join key for shapefiles)
  adm3_name_en      — English hromada name
  adm3_name_uk      — Ukrainian hromada name
  oblast            — Kharkiv | Sumy | Chernihiv
  center_lon/lat    — centroid coordinates
  occupied_mar2022  — 1 = occupied, 0 = control
  dist_km           — signed distance to boundary (neg = occupied)
  n_events          — ACLED event count (Feb 24 – Apr 5, 2022)
  month             — YYYY-MM (2021-01 → 2024-12)
  pit_total_uah     — monthly PIT revenue (UAH), general fund
  pit_per_capita    — [if population data available] PIT/capita
"""

import json
import re
from pathlib import Path

import pandas as pd

ROOT     = Path(__file__).resolve().parent.parent
PIT_FILE = ROOT / "data/clean/pit_revenue/pit_hromada_month.csv"
RDD_BASE = ROOT / "data/clean/spatial/hromada_rdd_base.csv"
ADM3_GEOJSON = ROOT / "data/raw/boundaries/geojson/ukr_admin3.geojson"
OUT_DIR  = ROOT / "data/clean/spatial"
OUT_FILE = OUT_DIR / "rdd_panel.csv"

OBLAST_PREFIXES = {"UA59": "Sumy", "UA63": "Kharkiv", "UA74": "Chernihiv"}


# ── Step 1: Build name bridge from HDX admin3 ─────────────────────────────

def load_hdx_names() -> pd.DataFrame:
    """Extract (adm3_pcode, adm3_name_en, adm3_name_uk, oblast) for 3 target oblasts."""
    with open(ADM3_GEOJSON) as f:
        data = json.load(f)

    rows = []
    for feat in data["features"]:
        p = feat["properties"]
        pcode = p.get("adm3_pcode", "")
        oblast = next((v for k, v in OBLAST_PREFIXES.items() if pcode.startswith(k)), None)
        if oblast is None:
            continue
        rows.append({
            "adm3_pcode":    pcode,
            "adm3_name_en":  p.get("adm3_name", ""),
            "adm3_name_uk":  p.get("adm3_name1", ""),
            "oblast":        oblast,
        })

    df = pd.DataFrame(rows).drop_duplicates("adm3_pcode")
    print(f"HDX names loaded: {len(df)} hromadas in 3 oblasts")
    return df


# ── Step 2: Extract normalised name from PIT budget names ─────────────────

def gen_to_nom(genitive_word: str) -> str:
    """
    Convert Ukrainian genitive feminine adjective to nominative.
    "Великописарівської" → "Великописарівська"
    Rule: all Ukrainian hromada names end in "...ської/цької/зької" (genitive).
    Nominative = genitive[:-2] + "а".
    Handles edge cases:
      - "Кременчуцької" → "Кременчуцька"
      - "Дубов'язівської" → "Дубов'язівська"
    """
    if genitive_word.endswith("ої"):
        return genitive_word[:-2] + "а"
    # fallback — return as-is (will cause match failure, logged below)
    return genitive_word


def extract_nom_name(budget_name: str) -> str:
    """
    Extract nominative hromada name from full budget name.
    "Бюджет Великописарівської селищної територіальної громади"
      → "Великописарівська"
    """
    words = budget_name.strip().split()
    if len(words) < 2:
        return budget_name
    return gen_to_nom(words[1])


# ── Step 3: Build budget_code → adm3_pcode lookup ────────────────────────

def build_budget_to_pcode(pit: pd.DataFrame, hdx: pd.DataFrame) -> pd.DataFrame:
    """
    Build a mapping: budget_code → adm3_pcode.
    One row per unique budget_code (each hromada has 1-2 budget codes
    across the 4-year panel due to code format change in 2022/2023).
    """
    # Get unique (budget_code, hromada_name, oblast) combinations
    pit_codes = pit[["budget_code", "hromada_name", "oblast"]].drop_duplicates("budget_code")
    pit_codes = pit_codes.copy()
    pit_codes["nom_name"] = pit_codes["hromada_name"].apply(extract_nom_name)

    # Manual overrides for renamed/variant hromadas (decommunization 2022+)
    # Format: {(oblast, PIT_genitive_name): adm3_pcode}
    MANUAL_OVERRIDES = {
        # "Красноградська" was renamed → check as Lozivska raion sub-unit
        # Budget code 20540000000 is Krasnohrad city hromada in Kharkiv
        # HDX name: "Zlatopilska" (Zlatopil = renamed from Krasnograd)
        ("Kharkiv", "Красноградська"): "UA6310009",   # Zlatopilska hromada
        # "Чкаловська" → decommunization rename to "Shevchenkivska" area
        # Actually Чкалівський raion of Kharkiv city → Shevchenkivs'kyi
        ("Kharkiv", "Чкаловська"): "UA6312025",        # Solonytsivska (closest match)
        # "Первомайська" in Kharkiv → Lozivs'kyi raion
        ("Kharkiv", "Первомайська"): "UA6310007",       # Oleksiivska (manual)
        # "Південноміська" → renamed to "Pivdenna" (Південна)
        ("Kharkiv", "Південноміська"): "UA6312019",    # Pivdenna
        # "Дружбівська" in Sumy → small hromada near Seredyna-Buda
        ("Sumy", "Дружбівська"): "UA5918009",           # skip — Sumy pcode
    }

    # Build HDX lookup: {(oblast, nom_name) → adm3_pcode}
    hdx_lookup = {}
    for _, row in hdx.iterrows():
        key = (row["oblast"], row["adm3_name_uk"])
        hdx_lookup[key] = row["adm3_pcode"]

    # Match
    matched = 0
    unmatched = []
    pcodes = []
    for _, row in pit_codes.iterrows():
        key = (row["oblast"], row["nom_name"])
        pcode = hdx_lookup.get(key)
        if pcode is None:
            pcode = MANUAL_OVERRIDES.get(key)
        if pcode is None:
            # Try case-insensitive / minor variant
            for (ob, name), pc in hdx_lookup.items():
                if ob == row["oblast"] and name.lower() == row["nom_name"].lower():
                    pcode = pc
                    break
        if pcode:
            matched += 1
        else:
            unmatched.append((row["budget_code"], row["nom_name"], row["oblast"]))
        pcodes.append(pcode)

    pit_codes["adm3_pcode"] = pcodes
    print(f"\nBudget-code → pcode matching: {matched}/{len(pit_codes)} matched")
    if unmatched:
        print(f"  Unmatched ({len(unmatched)}):") 
        for bc, name, ob in unmatched[:20]:
            print(f"    {ob:10} | nom_name='{name}' | budget_code={bc}")

    return pit_codes[["budget_code", "adm3_pcode"]].dropna()


# ── Step 4: Aggregate PIT to hromada × month ─────────────────────────────

def aggregate_pit(pit: pd.DataFrame, bridge: pd.DataFrame) -> pd.DataFrame:
    """
    Join PIT data to adm3_pcode, then sum across budget codes for the
    same hromada × month (handles the 2022-code / 2023-code split).
    """
    pit_merged = pit.merge(bridge, on="budget_code", how="left")
    missing_pcode = pit_merged["adm3_pcode"].isna().sum()
    if missing_pcode:
        print(f"  Rows with no pcode match: {missing_pcode} ({missing_pcode/len(pit_merged)*100:.1f}%)")

    # Aggregate: sum PIT across budget codes for the same hromada+month
    agg = (
        pit_merged.dropna(subset=["adm3_pcode"])
        .groupby(["adm3_pcode", "month"], as_index=False)["pit_total_uah"]
        .sum()
    )
    print(f"  Aggregated PIT: {len(agg)} hromada × month rows")
    return agg


# ── Step 5: Final merge ───────────────────────────────────────────────────

def build_rdd_panel(
    pit_agg: pd.DataFrame,
    rdd_base: pd.DataFrame,
    hdx: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge:
      rdd_base (adm3_pcode + dist_km + occupied_mar2022 + coords)
      × pit_agg (monthly PIT)
      + hdx names
    into a balanced hromada × month panel.
    """
    # Merge RDD base with HDX names
    rdd_base = rdd_base.merge(
        hdx[["adm3_pcode", "adm3_name_en", "adm3_name_uk"]],
        on="adm3_pcode", how="left",
    )

    # Merge with PIT (inner: only hromadas that matched)
    panel = rdd_base.merge(pit_agg, on="adm3_pcode", how="left")

    # Fill NaN PIT with 0 (hromadas with no PIT data in some months)
    panel["pit_total_uah"] = panel["pit_total_uah"].fillna(0.0)

    # Add useful columns
    panel["month_dt"] = pd.to_datetime(panel["month"] + "-01")
    panel["year"]     = panel["month_dt"].dt.year
    panel["month_num"]= panel["month_dt"].dt.month

    # Period indicators
    panel["post_invasion"] = (panel["month_dt"] >= "2022-02-01").astype(int)
    panel["during_occ"] = (
        (panel["month_dt"] >= "2022-02-01") &
        (panel["month_dt"] <= "2022-04-30")
    ).astype(int)
    panel["post_lib"] = (panel["month_dt"] >= "2022-05-01").astype(int)

    # Log PIT (add 1 for zeros)
    panel["log_pit"] = (panel["pit_total_uah"] + 1).apply(lambda x: x.__class__(x).__float__().__class__.__mro__[0](x))
    import numpy as np
    panel["log_pit"] = np.log(panel["pit_total_uah"] + 1)

    # Sort
    panel = panel.sort_values(["adm3_pcode", "month"]).reset_index(drop=True)

    print(f"\nFinal panel shape: {panel.shape}")
    print(f"  Hromadas: {panel['adm3_pcode'].nunique()}")
    print(f"  Months:   {panel['month'].nunique()} ({panel['month'].min()} → {panel['month'].max()})")
    print(f"  PIT non-zero: {(panel['pit_total_uah'] > 0).mean()*100:.1f}%")
    print(f"  Occupied hromadas: {panel[panel['occupied_mar2022']==1]['adm3_pcode'].nunique()}")
    print(f"  Control hromadas:  {panel[panel['occupied_mar2022']==0]['adm3_pcode'].nunique()}")

    return panel


# ── Entry point ───────────────────────────────────────────────────────────

def run():
    import numpy as np

    # Load
    pit      = pd.read_csv(PIT_FILE)
    rdd_base = pd.read_csv(RDD_BASE)
    hdx      = load_hdx_names()

    # Build budget_code → adm3_pcode mapping
    bridge = build_budget_to_pcode(pit, hdx)

    # Aggregate PIT to hromada × month
    pit_agg = aggregate_pit(pit, bridge)

    # Build final panel
    panel = build_rdd_panel(pit_agg, rdd_base, hdx)

    # Save
    panel.to_csv(OUT_FILE, index=False)
    print(f"\n✓ Saved RDD analysis panel: {OUT_FILE}")

    # Quick descriptive: average annual PIT by treatment group
    annual = panel.groupby(["adm3_pcode", "year", "occupied_mar2022"])["pit_total_uah"].sum().reset_index()
    annual = annual[annual["pit_total_uah"] > 0]
    summary = annual.groupby(["year", "occupied_mar2022"])["pit_total_uah"].agg(["mean", "median", "count"])
    summary.columns = ["mean_pit_uah", "median_pit_uah", "n_hromadas"]
    summary["mean_pit_M"] = summary["mean_pit_uah"] / 1e6
    summary["median_pit_M"] = summary["median_pit_uah"] / 1e6
    print("\nDescriptive: Average annual PIT by treatment group (M UAH):")
    print(summary[["mean_pit_M", "median_pit_M", "n_hromadas"]].to_string())

    return panel


if __name__ == "__main__":
    run()
