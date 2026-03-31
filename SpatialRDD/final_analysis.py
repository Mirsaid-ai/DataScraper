"""
SpatialRDD/final_analysis.py
─────────────────────────────────────────────────────────────────────────────
DEFINITIVE ANALYSIS — all improvements integrated:

  Treatment:  KSE government-certified war zones (kse_war_zone_apr2022)
              → 45 treated vs 112 control (vs. ACLED's noisy 90 vs 74)

  Outcome:    log(civilian PIT per capita)  — removes both confounds:
              (1) military-pay bias  (2) population-size bias

  Controls:   IDP outflows at oblast × month level (separates "fewer workers"
              from "lower wages / fewer firms" channels)

  Phases:     4 separate post-war periods instead of one pooled "post" dummy:
              Phase 1 (Feb–Apr 2022): occupation period
              Phase 2 (May–Dec 2022): post-liberation adjustment
              Phase 3 (2023):          recovery / scarring
              Phase 4 (2024):          long-run

  Subsamples: Full sample + Sumy-only (cleanest — KSE agrees, most disrupted)

  Also runs:  Cross-sectional DiD-D (Design A) with KSE treatment for
              comparison against the panel FE approach.

Outputs
-------
  data/clean/spatial/final_results.csv    — all coefficient estimates
  SpatialRDD/findings.md (updated)
"""

import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats

warnings.filterwarnings("ignore")

ROOT        = Path(__file__).resolve().parent.parent
PANEL_FILE  = ROOT / "data/clean/spatial/rdd_panel_enriched.csv"
IDP_FILE    = ROOT / "data/clean/hdx_displacement/idp_oblast_month.csv"
OUT_DIR     = ROOT / "data/clean/spatial"
FINDINGS_MD = ROOT / "SpatialRDD/findings.md"


# ─────────────────────────────────────────────────────────────────────────────
# Data loading and preparation
# ─────────────────────────────────────────────────────────────────────────────

def load_panel() -> pd.DataFrame:
    panel = pd.read_csv(PANEL_FILE)
    panel["month_dt"] = pd.to_datetime(panel["month"] + "-01")
    panel["year"]     = panel["month_dt"].dt.year

    # ── Phase dummies ─────────────────────────────────────────────────────
    panel["ph1_occ"] = ((panel["month_dt"] >= "2022-02-01") &
                         (panel["month_dt"] <= "2022-04-30")).astype(int)
    panel["ph2_lib"] = ((panel["month_dt"] >= "2022-05-01") &
                         (panel["month_dt"] <= "2022-12-31")).astype(int)
    panel["ph3_rec"] = ((panel["month_dt"] >= "2023-01-01") &
                         (panel["month_dt"] <= "2023-12-31")).astype(int)
    panel["ph4_lr"]  = ((panel["month_dt"] >= "2024-01-01") &
                         (panel["month_dt"] <= "2024-12-31")).astype(int)
    panel["post"]    = (panel["month_dt"] >= "2022-02-01").astype(int)

    # ── Clean outcome ─────────────────────────────────────────────────────
    panel = panel[(panel["pit_civilian_uah"] > 0) & (panel["pop_2022"] > 0)].copy()
    panel["log_civ_pc"] = np.log(panel["pit_civilian_pc"] + 1)

    # ── KSE treatment ─────────────────────────────────────────────────────
    panel["T_kse"]  = panel["kse_war_zone_apr2022"].fillna(0).astype(int)
    panel["T_acled"] = panel["occupied_mar2022"].astype(int)

    print(f"Panel loaded: {panel['adm3_pcode'].nunique()} hromadas × "
          f"{panel['month'].nunique()} months = {len(panel):,} obs")
    kse_h = panel.drop_duplicates("adm3_pcode")
    print(f"KSE treatment:   {kse_h['T_kse'].sum():.0f} treated / "
          f"{(1-kse_h['T_kse']).sum():.0f} control")
    print(f"ACLED treatment: {kse_h['T_acled'].sum():.0f} treated / "
          f"{(1-kse_h['T_acled']).sum():.0f} control")
    return panel


def add_idp_controls(panel: pd.DataFrame) -> pd.DataFrame:
    """
    Merge oblast-level IDP stock to panel.
    Normalises IDP count to thousands and fills pre-war months with 0.
    """
    idp = pd.read_csv(IDP_FILE)
    # Standardise oblast names to match panel
    name_map = {
        "Chernihivska": "Chernihiv",
        "Kharkivska":   "Kharkiv",
        "Sumska":       "Sumy",
    }
    idp["oblast_en"] = idp["oblast"].map(name_map)
    idp = idp.dropna(subset=["oblast_en"])
    idp["idp_k"] = idp["idp_present"] / 1000   # in thousands

    panel = panel.merge(
        idp[["oblast_en", "month", "idp_k"]],
        on=["oblast_en", "month"],
        how="left",
    )
    panel["idp_k"] = panel["idp_k"].fillna(0.0)
    print(f"IDP control added: non-zero in {(panel['idp_k']>0).sum():,} rows")
    return panel


# ─────────────────────────────────────────────────────────────────────────────
# Core estimator (reused from did_d.py)
# ─────────────────────────────────────────────────────────────────────────────

def panel_did_d(
    df: pd.DataFrame,
    treatment_col: str,
    phase_vars: list[str],
    idp_control: bool = True,
    label: str = "",
) -> list[dict]:
    """
    Panel DiD-D with hromada + month FEs, optional IDP control.
    Returns list of result dicts — one per phase.
    """
    df = df.copy()
    df["T"] = df[treatment_col]

    idp_term = "+ idp_k" if idp_control and "idp_k" in df.columns else ""

    results = []
    for pv in phase_vars:
        formula = (
            f"log_civ_pc ~ C(adm3_pcode) + C(month) {idp_term} "
            f"+ T:{pv} + dist_km:{pv} + dist_km:T:{pv} - 1"
        )
        try:
            mod = smf.ols(formula, data=df).fit(
                cov_type="cluster",
                cov_kwds={"groups": df["adm3_pcode"]},
            )
        except Exception as e:
            print(f"  ERROR [{pv}]: {e}")
            continue

        cname = next((c for c in mod.params.index if f"T:{pv}" in c), None)
        if cname is None:
            continue

        d, se, p = mod.params[cname], mod.bse[cname], mod.pvalues[cname]
        pct = (np.exp(d) - 1) * 100
        results.append({
            "label":    label,
            "treatment": treatment_col,
            "phase":    pv,
            "delta":    d,
            "se":       se,
            "p":        p,
            "ci_lo":    d - 1.96 * se,
            "ci_hi":    d + 1.96 * se,
            "pct":      pct,
            "n_obs":    int(mod.nobs),
            "n_hrom":   df["adm3_pcode"].nunique(),
            "n_treated":int(df.drop_duplicates("adm3_pcode")["T"].sum()),
            "idp_ctrl": idp_control,
            "r2":       mod.rsquared,
            "sig":      "***" if p<0.01 else "**" if p<0.05 else "*" if p<0.1 else "",
        })
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Cross-sectional DiD-D with KSE treatment (Design A)
# ─────────────────────────────────────────────────────────────────────────────

def triangle_kernel(dist, bw):
    return np.maximum(0.0, 1.0 - np.abs(dist) / bw)


def local_linear_rdd(y, x, bw, cutoff=0.0):
    xc = x - cutoff
    t  = (xc < 0).astype(float)
    m  = np.abs(xc) <= bw
    ys, xs, ts = y[m], xc[m], t[m]
    if len(ys) < 8:
        return dict(tau=np.nan, se=np.nan, p=np.nan, n=len(ys))
    w   = triangle_kernel(xs, bw)
    Xd  = np.column_stack([np.ones(len(ys)), ts, xs, ts*xs])
    W   = np.diag(w)
    try:
        XtW  = Xd.T @ W
        beta = np.linalg.solve(XtW @ Xd, XtW @ ys)
    except Exception:
        return dict(tau=np.nan, se=np.nan, p=np.nan, n=len(ys))
    tau   = beta[1]
    n, k  = len(ys), 4
    res   = ys - Xd @ beta
    meat  = (Xd*(w*res)[:,None]).T @ (Xd*(w*res)[:,None])
    V     = np.linalg.inv(XtW@Xd) @ meat @ np.linalg.inv(XtW@Xd) * (n/(n-k))
    se    = np.sqrt(np.diag(V)[1])
    p     = 2*stats.t.sf(abs(tau/se), df=n-k)
    return dict(tau=tau, se=se, p=p, n=n, ci_lo=tau-1.96*se, ci_hi=tau+1.96*se)


def cross_sectional_did_d(panel: pd.DataFrame, treatment_col: str, bw: float = 20.0) -> list[dict]:
    """
    Design A: cross-sectional DiD-D with KSE treatment as binary.
    Uses log(civilian PIT per capita) as outcome.
    Compares jump at boundary across years, normalised to pre-war jump.
    """
    annual = panel.groupby(
        ["adm3_pcode", "year", treatment_col, "dist_km"]
    )["pit_civilian_uah"].sum().reset_index()
    pop_map = panel.drop_duplicates("adm3_pcode").set_index("adm3_pcode")["pop_2022"]
    annual["pop"] = annual["adm3_pcode"].map(pop_map)
    annual = annual[annual["pop"] > 0].copy()
    annual["pit_pc"] = annual["pit_civilian_uah"] / annual["pop"]
    annual["log_pit_pc"] = np.log(annual["pit_pc"] + 1)

    results = []
    pre = annual[annual["year"] == 2021]
    res_pre = local_linear_rdd(pre["log_pit_pc"].values, pre["dist_km"].values, bw)

    for year in [2022, 2023, 2024]:
        post = annual[annual["year"] == year]
        res_post = local_linear_rdd(post["log_pit_pc"].values, post["dist_km"].values, bw)
        tau_dd = res_post["tau"] - res_pre["tau"]
        se_dd  = np.sqrt(res_post["se"]**2 + res_pre["se"]**2)
        p_dd   = 2 * stats.t.sf(abs(tau_dd/se_dd), df=max(res_post["n"]-4,1)) if se_dd>0 else np.nan
        pct    = (np.exp(tau_dd)-1)*100
        results.append({
            "label": f"Cross-sect DiD-D ({treatment_col})",
            "phase": str(year),
            "delta": tau_dd, "se": se_dd, "p": p_dd,
            "ci_lo": tau_dd-1.96*se_dd, "ci_hi": tau_dd+1.96*se_dd,
            "pct": pct, "n_obs": res_post["n"],
            "sig": "***" if p_dd<0.01 else "**" if p_dd<0.05 else "*" if p_dd<0.1 else "",
        })
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Print results
# ─────────────────────────────────────────────────────────────────────────────

PHASE_LABELS = {
    "post":    "Pooled post-war",
    "ph1_occ": "Phase 1: Occupation  (Feb–Apr 2022)",
    "ph2_lib": "Phase 2: Liberation  (May–Dec 2022)",
    "ph3_rec": "Phase 3: Recovery    (2023)",
    "ph4_lr":  "Phase 4: Long-run    (2024)",
}


def print_block(title: str, results: list[dict]) -> None:
    print(f"\n{'═'*78}")
    print(f"  {title}")
    print(f"{'─'*78}")
    print(f"  {'Phase':35} {'δ':>9} {'SE':>7} {'p':>8} {'%gap':>8}  {'N':>5}  sig")
    for r in results:
        phase_label = PHASE_LABELS.get(r["phase"], r["phase"])
        idp_tag = " +IDP" if r.get("idp_ctrl") else ""
        print(f"  {phase_label:35} {r['delta']:>9.4f} {r['se']:>7.4f} "
              f"{r['p']:>8.4f} {r['pct']:>7.1f}%  {r['n_obs']:>5}  {r['sig']}{idp_tag}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Loading and preparing panel…")
    panel = load_panel()
    panel = add_idp_controls(panel)

    phases_all   = ["post", "ph1_occ", "ph2_lib", "ph3_rec", "ph4_lr"]
    phases_nopool = ["ph1_occ", "ph2_lib", "ph3_rec", "ph4_lr"]
    all_results  = []

    # ── TABLE 1: KSE treatment, full sample, with IDP control ─────────────
    df_kse = panel.dropna(subset=["kse_war_zone_apr2022"]).copy()
    r = panel_did_d(df_kse, "T_kse", phases_all, idp_control=True, label="KSE/full/IDP")
    print_block(
        "TABLE 1 — KSE treatment | Full sample | Civilian PIT per capita | +IDP control",
        r
    )
    all_results.extend(r)

    # ── TABLE 2: KSE treatment, without IDP (cleaner interpretation) ──────
    r2 = panel_did_d(df_kse, "T_kse", phases_all, idp_control=False, label="KSE/full/noIDP")
    print_block(
        "TABLE 2 — KSE treatment | Full sample | No IDP control (baseline)",
        r2
    )
    all_results.extend(r2)

    # ── TABLE 3: Sumy only, KSE treatment — no IDP (absorbed by month FEs) ──
    # For single-oblast regressions, the IDP variable is identical for ALL
    # observations in the same month (oblast-level), so it is perfectly
    # collinear with the month fixed effects and must be excluded.
    df_sumy = df_kse[df_kse["oblast_en"] == "Sumy"].copy()
    r3 = panel_did_d(df_sumy, "T_kse", phases_all, idp_control=False, label="KSE/Sumy")
    print_block(
        "TABLE 3 — KSE treatment | SUMY ONLY | (IDP omitted — collinear with month FE)",
        r3
    )
    all_results.extend(r3)

    # ── TABLE 4: ACLED treatment + IDP for comparison ─────────────────────
    r4 = panel_did_d(panel, "T_acled", phases_all, idp_control=True, label="ACLED/full/IDP")
    print_block(
        "TABLE 4 — ACLED treatment | Full sample | +IDP control  (for comparison)",
        r4
    )
    all_results.extend(r4)

    # ── TABLE 5a: Cross-sectional DiD-D, ACLED, BW=20km (boundary-based) ──
    # NOTE: The cross-sectional DiD-D is designed for ACLED treatment because
    # it measures a jump AT the occupation boundary LINE. KSE treatment is a
    # government classification that doesn't map 1:1 to one side of the line,
    # so the boundary jump is attenuated. We show both for transparency.
    r5a = cross_sectional_did_d(panel, "T_acled", bw=20.0)
    print_block(
        "TABLE 5a — Cross-sectional DiD-D | ACLED boundary | BW ±20km",
        r5a
    )
    all_results.extend(r5a)

    r5b = cross_sectional_did_d(df_kse, "T_kse", bw=20.0)
    print_block(
        "TABLE 5b — Cross-sectional DiD-D | KSE treatment | BW ±20km (attenuated — see note)",
        r5b
    )
    all_results.extend(r5b)

    # ── Summary comparison: KSE vs ACLED Phase 3 (cleanest contrast) ──────
    print(f"\n{'═'*78}")
    print("  SUMMARY — Phase 3 (2023 Scarring) and Phase 4 (2024 Long-run)")
    print(f"{'─'*78}")
    print(f"  {'Specification':40} {'Phase':8} {'δ':>9} {'%':>8}  sig")
    for r in all_results:
        if r.get("phase") in ("ph3_rec", "ph4_lr"):
            ph = "2023" if r["phase"] == "ph3_rec" else "2024"
            print(f"  {r['label']:40} {ph:8} {r['delta']:>9.4f} {r['pct']:>7.1f}%  {r['sig']}")

    # ── IDP contribution: compare with vs without ──────────────────────────
    print(f"\n{'═'*78}")
    print("  IDP CONTROL IMPACT — does adding IDP change estimates? (KSE, Phase 3)")
    print(f"{'─'*78}")
    no_idp = next((r for r in all_results if r["label"] == "KSE/full/noIDP" and r["phase"]=="ph3_rec"), None)
    w_idp  = next((r for r in all_results if r["label"] == "KSE/full/IDP"   and r["phase"]=="ph3_rec"), None)
    if no_idp and w_idp:
        print(f"  Without IDP control:  δ = {no_idp['delta']:+.4f}  ({no_idp['pct']:+.1f}%)  {no_idp['sig']}")
        print(f"  With    IDP control:  δ = {w_idp['delta']:+.4f}  ({w_idp['pct']:+.1f}%)  {w_idp['sig']}")
        change = w_idp["delta"] - no_idp["delta"]
        print(f"  IDP attenuation:      Δδ = {change:+.4f}  "
              f"({'more negative' if change < 0 else 'less negative'} — "
              f"{'IDP explains some of the gap' if change > 0 else 'IDP amplifies the gap'})")

    # ── Save results ───────────────────────────────────────────────────────
    df_out = pd.DataFrame(all_results)
    out_path = OUT_DIR / "final_results.csv"
    df_out.to_csv(out_path, index=False)
    print(f"\n✓ Results saved: {out_path}")

    return df_out


if __name__ == "__main__":
    main()
