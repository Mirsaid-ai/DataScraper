"""
scrapers/09_enrich_panel.py
─────────────────────────────────────────────────────────────────────────────
Enriches the RDD analysis panel with three additions:

  1. MILITARY vs CIVILIAN PIT SPLIT
     Re-aggregates the raw pit_revenue CSVs separating:
       - 11010200  = PIT from military service pay (кошти грошового забезпечення)
       - All other 1101xxxx codes = civilian PIT (salary + FOPs + investment)
     Adds pit_military_uah and pit_civilian_uah to the panel.

  2. POPULATION NORMALIZATION
     Downloads KSE hromada population 2022 (ua-pop-2022.csv) and joins to panel.
     Adds pop_2022 and per-capita PIT variables:
       pit_total_pc  = pit_total_uah   / pop_2022
       pit_civilian_pc = pit_civilian_uah / pop_2022
       pit_military_pc = pit_military_uah / pop_2022

  3. OCCUPATION STATUS CROSS-CHECK
     Adds war_zone_27_04_2022 from KSE full_dataset.csv as an alternative
     binary treatment indicator to cross-check our ACLED-derived classification.

Output
------
  data/clean/spatial/rdd_panel_enriched.csv — enriched panel (all columns)
"""

import csv
import io
from pathlib import Path

import pandas as pd
import numpy as np

ROOT    = Path(__file__).resolve().parent.parent
RAW_PIT = ROOT / "data/raw/pit_revenue"
PANEL   = ROOT / "data/clean/spatial/rdd_panel.csv"
KSE_POP = ROOT / "data/raw/kse/ua-pop-2022.csv"
KSE_FULL= ROOT / "data/raw/kse/full_dataset_sample.csv"
OUT     = ROOT / "data/clean/spatial/rdd_panel_enriched.csv"

# PIT income code classification
MILITARY_CODES = {"11010200"}  # PIT withheld from military service pay
PIT_PREFIX     = "11010"       # all PIT sub-codes (matches original scraper exactly)
# Note: Ukraine exempted combat soldiers from PIT starting late 2022,
# so 11010200 drops to near-zero in 2023-2024 — this is real policy, not missing data.


# ─────────────────────────────────────────────────────────────────────────────
# 1. Military vs civilian PIT split
# ─────────────────────────────────────────────────────────────────────────────

def rebuild_pit_split(budget_codes: list[str]) -> pd.DataFrame:
    """
    Re-aggregate raw PIT CSVs splitting military (11010200) from civilian.
    Returns DataFrame: budget_code × month → pit_military_uah, pit_civilian_uah
    """
    print("\n[1] Splitting PIT into military vs civilian from raw CSVs…")
    rows = []
    n_files = 0
    for bc in budget_codes:
        for f in RAW_PIT.glob(f"{bc}_*.csv"):
            year = int(f.stem.split("_")[-1])
            with open(f, encoding="utf-8") as fh:
                for row in csv.DictReader(fh, delimiter=";"):
                    cod  = (row.get("COD_INCO") or "").strip()
                    fund = (row.get("FUND_TYP") or "").strip()
                    period = (row.get("REP_PERIOD") or "").strip()
                    fakt = (row.get("FAKT_AMT") or "").strip()

                    if not cod.startswith(PIT_PREFIX) or fund != "C" or not fakt or "." not in period:
                        continue
                    try:
                        amt = float(fakt.replace(",", "."))
                        mm, yyyy = period.split(".", 1)
                        if int(yyyy) != year:
                            continue
                        month = f"{yyyy}-{mm.zfill(2)}"
                    except (ValueError, TypeError):
                        continue

                    is_military = cod in MILITARY_CODES
                    rows.append({
                        "budget_code": bc,
                        "month": month,
                        "amt": amt,
                        "military": is_military,
                    })
            n_files += 1

    print(f"  Processed {n_files} raw CSV files, {len(rows):,} PIT rows")
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    mil = df[df["military"]].groupby(["budget_code", "month"])["amt"].sum().rename("pit_military_uah")
    civ = df[~df["military"]].groupby(["budget_code", "month"])["amt"].sum().rename("pit_civilian_uah")
    result = pd.concat([mil, civ], axis=1).fillna(0.0).reset_index()
    print(f"  Split complete: {result.shape[0]:,} budget_code × month rows")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 2. Population normalization
# ─────────────────────────────────────────────────────────────────────────────

def _norm_name(name: str) -> str:
    """Normalise Ukrainian name for matching: lowercase, unify apostrophes."""
    return (name.lower()
            .replace("\u2019", "'")   # right single quotation → ascii
            .replace("\u02bc", "'")   # modifier letter apostrophe → ascii
            .replace("ʼ", "'")
            .strip())


def load_population() -> pd.DataFrame:
    """
    Load KSE 2022 population data keyed by normalised hromada name.
    Returns DataFrame: adm3_name_uk_norm → pop_2022
    """
    print("\n[2] Loading KSE 2022 population data…")
    pop = {}
    with open(KSE_POP, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("hromada_name", "").strip()
            p = row.get("total_population_2022", "0") or "0"
            try:
                pop[_norm_name(name)] = int(p)
            except ValueError:
                pass
    print(f"  Loaded {len(pop):,} hromada population entries")
    return pop


def load_kse_war_status() -> dict:
    """
    Load KSE full_dataset war zone status (27 April 2022 indicator).
    Returns dict: norm_name → war_zone_27_04_2022 (0/1)
    """
    print("\n[3] Loading KSE war zone status (27-Apr-2022)…")
    status = {}
    with open(KSE_FULL, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("hromada_name", "").strip()
            val  = row.get("war_zone_27_04_2022", "0") or "0"
            try:
                status[_norm_name(name)] = int(float(val))
            except ValueError:
                pass
    occ = sum(1 for v in status.values() if v == 1)
    print(f"  Loaded {len(status):,} entries | war-zone=1: {occ}")
    return status


# ─────────────────────────────────────────────────────────────────────────────
# Main enrichment pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    # Load base panel
    panel = pd.read_csv(PANEL)
    panel["month_dt"] = pd.to_datetime(panel["month"] + "-01")
    panel["year"] = panel["month_dt"].dt.year
    print(f"Base panel: {panel.shape[0]:,} rows, {panel['adm3_pcode'].nunique()} hromadas")

    # ── 1. Military / civilian split ──────────────────────────────────────
    # Get all raw PIT CSV budget codes from the raw directory
    unique_codes = sorted(set(
        f.stem.rsplit("_", 1)[0] for f in RAW_PIT.glob("*.csv")
    ))
    split = rebuild_pit_split(unique_codes)

    if not split.empty:
        # Also need to map budget_code → adm3_pcode using the original PIT file
        pit_file = ROOT / "data/clean/pit_revenue/pit_hromada_month.csv"
        if pit_file.exists():
            pit_raw = pd.read_csv(pit_file)[["budget_code", "hromada_name", "month"]].drop_duplicates(
                ["budget_code", "month"]
            )
            # Import the bridge from merge_panel to get adm3_pcode
            import sys
            sys.path.insert(0, str(ROOT / "SpatialRDD"))
            from merge_panel import load_hdx_names, build_budget_to_pcode
            hdx = load_hdx_names()
            pit_all = pd.read_csv(pit_file)
            bridge = build_budget_to_pcode(pit_all, hdx)
            # Merge split with bridge (normalise budget_code dtype)
            split["budget_code"] = split["budget_code"].astype(str)
            bridge["budget_code"] = bridge["budget_code"].astype(str)
            split = split.merge(bridge, on="budget_code", how="left")
            # Aggregate to adm3_pcode × month (sum across budget codes)
            split_pcode = (
                split.dropna(subset=["adm3_pcode"])
                .groupby(["adm3_pcode", "month"], as_index=False)
                [["pit_military_uah", "pit_civilian_uah"]].sum()
            )
        else:
            split_pcode = pd.DataFrame(columns=["adm3_pcode", "month", "pit_military_uah", "pit_civilian_uah"])

        panel = panel.merge(split_pcode, on=["adm3_pcode", "month"], how="left")

    if not split.empty:
        panel["pit_military_uah"]  = panel["pit_military_uah"].fillna(0.0)
        panel["pit_civilian_uah"]  = panel["pit_civilian_uah"].fillna(0.0)

        # Sanity: civilian + military should ≈ total
        diff = (panel["pit_civilian_uah"] + panel["pit_military_uah"] - panel["pit_total_uah"]).abs()
        max_diff = diff.max()
        print(f"  Max absolute discrepancy (civ+mil vs total): {max_diff:.2f} UAH")

        # Shares (avoid division by zero)
        total = panel["pit_total_uah"].replace(0, np.nan)
        panel["share_military"] = panel["pit_military_uah"] / total
        panel["share_civilian"] = panel["pit_civilian_uah"] / total
    else:
        panel["pit_military_uah"] = np.nan
        panel["pit_civilian_uah"] = np.nan
        panel["share_military"] = np.nan
        panel["share_civilian"] = np.nan

    # ── 2. Population normalization ───────────────────────────────────────
    pop_lookup  = load_population()
    war_lookup  = load_kse_war_status()

    # Join by normalised Ukrainian hromada name
    if "adm3_name_uk" in panel.columns:
        panel["_name_norm"] = panel["adm3_name_uk"].fillna("").apply(_norm_name)
    else:
        # Fallback: extract from hromada_name (genitive → nominative)
        def gen_to_nom(s):
            words = s.split()
            return words[1][:-2] + "а" if len(words) > 1 and words[1].endswith("ої") else s
        panel["_name_norm"] = panel["hromada_name"].fillna("").apply(
            lambda x: _norm_name(gen_to_nom(x))
        )

    panel["pop_2022"]            = panel["_name_norm"].map(pop_lookup)
    panel["kse_war_zone_apr2022"] = panel["_name_norm"].map(war_lookup)

    matched_pop = panel["pop_2022"].notna().sum() // panel["month"].nunique()
    matched_war = panel["kse_war_zone_apr2022"].notna().sum() // panel["month"].nunique()
    print(f"\n  Population matched: {matched_pop} hromadas")
    print(f"  War status matched: {matched_war} hromadas")

    # Per-capita PIT (UAH per person)
    pop = panel["pop_2022"].replace(0, np.nan)
    panel["pit_total_pc"]    = panel["pit_total_uah"]    / pop
    panel["pit_civilian_pc"] = panel["pit_civilian_uah"] / pop
    panel["pit_military_pc"] = panel["pit_military_uah"] / pop
    panel["log_pit_pc"]      = np.log(panel["pit_total_pc"] + 1)
    panel["log_pit_civ_pc"]  = np.log(panel["pit_civilian_pc"] + 1)

    # ── Descriptive summary ───────────────────────────────────────────────
    annual = panel[panel["pit_total_uah"] > 0].groupby(
        ["adm3_pcode", "year", "occupied_mar2022", "kse_war_zone_apr2022"]
    ).agg(
        pit_total=("pit_total_uah", "sum"),
        pit_civ=("pit_civilian_uah", "sum"),
        pit_mil=("pit_military_uah", "sum"),
        pop=("pop_2022", "first"),
    ).reset_index()
    annual["share_mil"] = annual["pit_mil"] / annual["pit_total"].replace(0, np.nan)

    print("\n─── Military share of PIT by treatment group and year ───")
    summary = annual.groupby(["year", "occupied_mar2022"])["share_mil"].agg(
        ["mean", "median", "count"]
    )
    summary.columns = ["mean_mil_share", "median_mil_share", "n"]
    print(summary.to_string())

    print("\n─── KSE war-zone vs ACLED classification agreement ───")
    if "kse_war_zone_apr2022" in panel.columns:
        comp = panel[["adm3_pcode", "occupied_mar2022", "kse_war_zone_apr2022"]].drop_duplicates("adm3_pcode")
        comp = comp.dropna(subset=["kse_war_zone_apr2022"])
        agree = (comp["occupied_mar2022"] == comp["kse_war_zone_apr2022"]).sum()
        total = len(comp)
        print(f"  Agreement: {agree}/{total} ({agree/total*100:.1f}%)")
        print("  Confusion matrix:")
        print(pd.crosstab(comp["occupied_mar2022"], comp["kse_war_zone_apr2022"],
                          rownames=["ACLED (ours)"], colnames=["KSE war-zone"]))

    print("\n─── Per-capita PIT (median, 2021, UAH/person) ───")
    pre = annual[annual["year"] == 2021]
    for grp in [0, 1]:
        sub = pre[pre["occupied_mar2022"] == grp]
        sub = sub[sub["pop"].notna() & (sub["pop"] > 0)]
        sub["pit_pc"] = sub["pit_total"] / sub["pop"]
        label = "Control" if grp == 0 else "Occupied"
        print(f"  {label}: median PIT/capita = {sub['pit_pc'].median():,.0f} UAH")

    # ── Clean up and save ─────────────────────────────────────────────────
    panel = panel.drop(columns=["_name_norm"], errors="ignore")
    panel.to_csv(OUT, index=False)
    print(f"\n✓ Enriched panel saved: {OUT}  ({len(panel):,} rows)")
    print(f"  New columns: pit_military_uah, pit_civilian_uah, share_military,")
    print(f"               pop_2022, pit_total_pc, pit_civilian_pc, pit_military_pc,")
    print(f"               log_pit_pc, log_pit_civ_pc, kse_war_zone_apr2022")


if __name__ == "__main__":
    run()
