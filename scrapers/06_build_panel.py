"""
06_build_panel.py — Assemble the final oblast-month research panel.

Unit: oblast × month  |  3 oblasts × 48 months (Jan 2021–Dec 2024) = 144 rows

Sources merged:
  ┌──────────────────────────────┬──────────────┬──────────────────────────────────┐
  │ Source                       │ Coverage     │ Column(s)                        │
  ├──────────────────────────────┼──────────────┼──────────────────────────────────┤
  │ HDX/IOM DTM displacement     │ Feb22–Jan26  │ idp_present                      │
  │ EDR (national)               │ Jan21–Dec24  │ edr_new_companies, edr_new_fops  │
  │ ACLED conflict               │ PENDING      │ conflict_events, fatalities,     │
  │                              │              │ shelling_events  (NaN for now)   │
  └──────────────────────────────┴──────────────┴──────────────────────────────────┘

Notes:
  - IDP data covers only Feb 2022 onward (pre-invasion months → NaN, treated as 0 below).
  - EDR is NATIONAL only (no oblast filter); the same monthly count is broadcast
    to all 3 rows so it can be used as a national control / time FE proxy.
  - Work.ua / Robota.ua are single current snapshots, not a time series; they are
    NOT merged into the panel (see data/clean/{work_ua,robota_ua}/).
  - ACLED columns are stubbed as NaN; run 01_acled.py once the API key is active.

Usage:
    python scrapers/06_build_panel.py

Output:
    data/final/panel/panel_oblast_month.csv
    data/final/panel/panel_metadata.txt
"""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import CLEAN_DIR, DATE_END, DATE_START, FINAL_DIR, REGIONS, REGIONS_EN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Name-mapping helpers ──────────────────────────────────────────────────────

# HDX IDP file uses e.g. "Kharkivska"; REGIONS_EN uses "Kharkiv"
HDX_TO_EN = {
    "Kharkivska":    "Kharkiv",
    "Sumska":        "Sumy",
    "Chernihivska":  "Chernihiv",
}

# Ukrainian → English short name (from config REGIONS_EN)
UA_TO_EN = REGIONS_EN   # {"Сумська": "Sumy", "Чернігівська": "Chernihiv", ...}
EN_OBLASTS = [REGIONS_EN[r] for r in REGIONS]  # ["Sumy", "Chernihiv", "Kharkiv"]


# ── 1. Build spine ────────────────────────────────────────────────────────────

def build_spine() -> pd.DataFrame:
    """144-row balanced panel: 3 oblasts × 48 months."""
    months = pd.period_range(start=DATE_START, end=DATE_END, freq="M")
    rows = [
        {"oblast": ob, "month": str(p)}
        for ob in sorted(EN_OBLASTS)
        for p in months
    ]
    df = pd.DataFrame(rows)

    # Convenience columns for regressions
    df["year"]       = df["month"].str[:4].astype(int)
    df["month_num"]  = df["month"].str[5:7].astype(int)

    # Treatment dummies
    df["post_invasion"] = (df["month"] >= "2022-02").astype(int)

    # Oblast-specific occupation phase (rough dates based on public record)
    #   Sumy / Chernihiv: occupied Feb 24 – ~Apr 2, 2022  → months 2022-02, 2022-03, 2022-04
    #   Kharkiv:          partial occupation / battle phase Feb 24 – Sep 2022
    occ_sumy_chern = {"2022-02", "2022-03", "2022-04"}
    occ_kharkiv    = {f"2022-{m:02d}" for m in range(2, 10)}  # 2022-02 … 2022-09

    df["occupation_phase"] = 0
    df.loc[
        df["oblast"].isin(["Sumy", "Chernihiv"]) & df["month"].isin(occ_sumy_chern),
        "occupation_phase"
    ] = 1
    df.loc[
        (df["oblast"] == "Kharkiv") & df["month"].isin(occ_kharkiv),
        "occupation_phase"
    ] = 1

    log.info("Spine: %d rows, %d oblasts, %d months",
             len(df), df["oblast"].nunique(), df["month"].nunique())
    return df


# ── 2. IDP displacement ───────────────────────────────────────────────────────

def load_idp() -> pd.DataFrame:
    path = CLEAN_DIR / "hdx_displacement" / "idp_oblast_month.csv"
    df = pd.read_csv(path)
    df["oblast"] = df["oblast"].map(HDX_TO_EN)
    df = df.dropna(subset=["oblast"])
    df = df.rename(columns={"idp_present": "idp_present"})
    df = df[["oblast", "month", "idp_present"]]
    log.info("IDP: %d rows, months %s – %s",
             len(df), df["month"].min(), df["month"].max())
    return df


# ── 3. EDR national ───────────────────────────────────────────────────────────

def load_edr() -> pd.DataFrame:
    """National counts — same value broadcast to all 3 oblasts."""
    path = CLEAN_DIR / "opendatabot" / "edr_national_month.csv"
    df = pd.read_csv(path)[["month", "new_companies", "new_fops"]]
    df = df.rename(columns={
        "new_companies": "edr_new_companies_nat",
        "new_fops":      "edr_new_fops_nat",
    })
    log.info("EDR national: %d months", len(df))
    return df


# ── 4. ACLED stubs ────────────────────────────────────────────────────────────

def acled_stub(spine: pd.DataFrame) -> pd.DataFrame:
    """Return spine with NaN ACLED columns (populated once 01_acled.py runs)."""
    for col in ("conflict_events", "fatalities", "shelling_events"):
        spine[col] = float("nan")
    return spine


# ── 5. Merge & save ───────────────────────────────────────────────────────────

def merge_all(spine, idp, edr) -> pd.DataFrame:
    df = spine.merge(idp,  on=["oblast", "month"], how="left")
    df = df.merge(edr, on="month", how="left")

    # IDP: pre-invasion months have no entry → fill with 0
    df["idp_present"] = df["idp_present"].fillna(0).astype(int)

    # ACLED stubs
    for col in ("conflict_events", "fatalities", "shelling_events"):
        df[col] = float("nan")

    # Canonical column order
    cols = [
        "oblast", "month", "year", "month_num",
        "post_invasion", "occupation_phase",
        # Outcomes / covariates
        "idp_present",
        "edr_new_companies_nat", "edr_new_fops_nat",
        # ACLED (pending)
        "conflict_events", "fatalities", "shelling_events",
    ]
    return df[cols].sort_values(["oblast", "month"]).reset_index(drop=True)


def save(df: pd.DataFrame) -> Path:
    FINAL_DIR.mkdir(parents=True, exist_ok=True)
    out = FINAL_DIR / "panel_oblast_month.csv"
    df.to_csv(out, index=False)
    log.info("Panel saved → %s  (%d rows × %d cols)", out, len(df), len(df.columns))
    return out


def write_metadata(df: pd.DataFrame):
    meta_path = FINAL_DIR / "panel_metadata.txt"
    lines = [
        "panel_oblast_month.csv — metadata",
        "=" * 50,
        f"Rows:    {len(df)}",
        f"Oblasts: {sorted(df['oblast'].unique().tolist())}",
        f"Months:  {df['month'].min()} – {df['month'].max()}",
        "",
        "Columns:",
    ]
    for col in df.columns:
        n_nan = df[col].isna().sum()
        lines.append(f"  {col:35s}  NaN={n_nan:4d}  "
                     f"dtype={df[col].dtype}")
    lines += [
        "",
        "Data sources:",
        "  idp_present            : HDX/IOM DTM (Feb 2022–Jan 2026; pre-invasion → 0)",
        "  edr_new_companies_nat  : EDR data.gov.ua NATIONAL (no oblast filter available)",
        "  edr_new_fops_nat       : EDR data.gov.ua NATIONAL",
        "  conflict_events et al. : ACLED — PENDING (run 01_acled.py once API activated)",
        "",
        "NOT in panel (snapshot only):",
        "  Work.ua job counts     : data/clean/work_ua/work_ua_region_month.csv",
        "  Robota.ua job counts   : data/clean/robota_ua/robota_ua_region_month.csv",
    ]
    meta_path.write_text("\n".join(lines))
    log.info("Metadata → %s", meta_path)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    spine  = build_spine()
    idp    = load_idp()
    edr    = load_edr()
    panel  = merge_all(spine, idp, edr)
    out    = save(panel)
    write_metadata(panel)

    print("\n── Panel summary ──────────────────────────────────────")
    print(panel.groupby("oblast")[["idp_present","edr_new_companies_nat"]].describe(
        percentiles=[]).loc[:, (slice(None), ["count","mean","min","max"])].to_string())
    print()
    print("  IDP non-zero months per oblast:")
    nonzero = panel[panel["idp_present"] > 0].groupby("oblast")["month"].agg(["min","max","count"])
    print(nonzero.to_string())
    print()
    print("  EDR spot-check (national Mar 2022 — expect sharp drop):")
    war_rows = panel[(panel["month"].isin(["2022-01","2022-02","2022-03","2022-04"]))
                     & (panel["oblast"] == "Kharkiv")]
    print(war_rows[["month","edr_new_companies_nat","post_invasion","occupation_phase"]]
          .to_string(index=False))
    print(f"\n  Output: {out}")
    print("───────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
