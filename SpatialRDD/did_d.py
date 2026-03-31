"""
SpatialRDD/did_d.py
─────────────────────────────────────────────────────────────────────────────
Full Difference-in-Discontinuities (DiD-D) estimation — primary specification.

Three complementary estimators:

  (A) Event Study — monthly boundary jumps τ_t
      For each of the 48 months, estimate the local linear RDD and recover the
      boundary discontinuity τ_t. Pre-war months should show τ_t ≈ 0 after
      normalising by the pre-war average (parallel trends test). The evolution
      of τ_t post-Feb 2022 traces the dynamic treatment effect.

  (B) Panel DiD-D Regression (primary)
      Stack all hromada × month observations. Absorb hromada and time fixed
      effects; identify the treatment effect from within-hromada variation:

        log(PIT_it) = α_i + λ_t + δ*(T_i × Post_t)
                    + β₁*(dist_km_i × Post_t) + β₂*(dist_km_i × T_i × Post_t)
                    + ε_it

      where T_i = occupied_mar2022, Post_t = 1 if month ≥ 2022-02.
      α_i = hromada FE absorbs pre-war size (removing the pre-existing jump).
      λ_t = month FE absorbs Ukraine-wide fiscal trends.
      δ = DiD-D estimate = causal effect of occupation on log PIT.

      Variants:
        • Full post-war period (Feb 2022 – Dec 2024)
        • Year-by-year interactions (2022, 2023, 2024)
        • Within ±20 km bandwidth only

  (C) Sumy-Only Panel DiD-D
      The heterogeneity analysis shows Sumy drives all results. Verify that the
      Sumy-only estimate is consistent with the pooled estimate.

Outputs
-------
  data/clean/spatial/event_study.csv   — monthly τ_t estimates (for plotting)
  data/clean/spatial/panel_did_d.txt   — regression table
"""

import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats

warnings.filterwarnings("ignore")

ROOT       = Path(__file__).resolve().parent.parent
PANEL_FILE = ROOT / "data/clean/spatial/rdd_panel.csv"
OUT_DIR    = ROOT / "data/clean/spatial"


# ── Reuse core RDD estimator from rdd_analysis ───────────────────────────

def triangle_kernel(dist: np.ndarray, bw: float) -> np.ndarray:
    return np.maximum(0.0, 1.0 - np.abs(dist) / bw)


def local_linear_rdd(y, x, bw, cutoff=0.0, donut=0.0):
    x_c    = x - cutoff
    treated = (x_c < 0).astype(float)
    in_bw  = np.abs(x_c) <= bw
    if donut > 0:
        in_bw &= np.abs(x_c) >= donut
    y_s, x_s, t_s = y[in_bw], x_c[in_bw], treated[in_bw]
    if len(y_s) < 8:
        return {"tau": np.nan, "se": np.nan, "p_value": np.nan, "n": len(y_s),
                "ci_lo": np.nan, "ci_hi": np.nan}
    w   = triangle_kernel(x_s, bw)
    X_d = np.column_stack([np.ones(len(y_s)), t_s, x_s, t_s * x_s])
    W   = np.diag(w)
    try:
        XtW  = X_d.T @ W
        beta = np.linalg.solve(XtW @ X_d, XtW @ y_s)
    except np.linalg.LinAlgError:
        return {"tau": np.nan, "se": np.nan, "p_value": np.nan, "n": len(y_s),
                "ci_lo": np.nan, "ci_hi": np.nan}
    tau = beta[1]
    n, k = len(y_s), 4
    resid = y_s - X_d @ beta
    meat  = (X_d * (w * resid)[:, None]).T @ (X_d * (w * resid)[:, None])
    V     = np.linalg.inv(XtW @ X_d) @ meat @ np.linalg.inv(XtW @ X_d) * (n / (n - k))
    se    = np.sqrt(np.diag(V)[1])
    p     = 2 * stats.t.sf(abs(tau / se), df=n - k)
    return {"tau": tau, "se": se, "p_value": p, "n": n,
            "ci_lo": tau - 1.96 * se, "ci_hi": tau + 1.96 * se}


# ═══════════════════════════════════════════════════════════════════════════
# (A)  EVENT STUDY — monthly τ_t
# ═══════════════════════════════════════════════════════════════════════════

def run_event_study(panel: pd.DataFrame, bw: float = 20.0) -> pd.DataFrame:
    """
    For each month, estimate local linear RDD on log monthly PIT.
    Returns a DataFrame with columns:
      month, month_dt, tau, se, ci_lo, ci_hi, p_value, n,
      tau_norm  (normalised by pre-war mean),
      relative_month  (months since Feb 2022)
    """
    print(f"\n{'═'*70}")
    print(f"(A) EVENT STUDY — Monthly boundary discontinuity, BW=±{bw:.0f}km")
    print(f"{'─'*70}")

    months = sorted(panel["month"].dropna().unique())
    rows   = []

    for month in months:
        sub = panel[panel["month"] == month].dropna(subset=["dist_km", "pit_total_uah"])
        sub = sub[sub["pit_total_uah"] > 0].copy()
        sub["log_pit"] = np.log(sub["pit_total_uah"])
        res = local_linear_rdd(sub["log_pit"].values, sub["dist_km"].values, bw=bw)
        res["month"] = month
        rows.append(res)

    df = pd.DataFrame(rows)
    df["month_dt"] = pd.to_datetime(df["month"] + "-01")

    # Normalise: subtract pre-war mean (Jan 2021 – Jan 2022)
    pre = df[df["month_dt"] < "2022-02-01"]["tau"].dropna()
    pre_mean = pre.mean()
    pre_se   = pre.std() / np.sqrt(len(pre))
    df["tau_norm"] = df["tau"] - pre_mean

    # Relative month index (0 = Feb 2022)
    feb2022 = pd.Timestamp("2022-02-01")
    df["rel_month"] = ((df["month_dt"] - feb2022) / pd.Timedelta("31D")).round().astype(int)

    # Print table
    print(f"\n  Pre-war mean τ̄: {pre_mean:.4f}  (SE={pre_se:.4f})")
    print(f"  {'Month':>8} {'τ_t':>8} {'τ_norm':>8} {'SE':>7} {'p':>7}  {'N':>4}")
    for _, r in df.iterrows():
        sig = "***" if r["p_value"]<0.01 else "**" if r["p_value"]<0.05 else "*" if r["p_value"]<0.1 else ""
        mark = " ◀" if pd.Timestamp("2022-02-01") <= r["month_dt"] <= pd.Timestamp("2022-04-01") else ""
        print(f"  {r['month']:>8} {r['tau']:>8.3f} {r['tau_norm']:>8.3f} "
              f"{r['se']:>7.3f} {r['p_value']:>7.4f}  {int(r['n']):>4}  {sig}{mark}")

    return df


# ═══════════════════════════════════════════════════════════════════════════
# (B)  PANEL DiD-D REGRESSION
# ═══════════════════════════════════════════════════════════════════════════

def run_panel_did_d(panel: pd.DataFrame, bw: float = None) -> None:
    """
    Panel regression with hromada and month fixed effects.

    Model:
      log_pit_it = α_i + λ_t + δ*(occupied_i × post_t)
                 + β*(dist_km_i × post_t)
                 + γ*(dist_km_i × occupied_i × post_t) + ε_it

    where:
      α_i  = hromada FE (absorbs permanent size differences, removing pre-war jump)
      λ_t  = month FE (absorbs Ukraine-wide fiscal shocks)
      δ    = DiD-D treatment effect (the key parameter)

    Cluster SEs at the hromada level (within-hromada autocorrelation).
    """
    print(f"\n{'═'*70}")
    print("(B) PANEL DiD-D REGRESSION")
    print("  Model: log(PIT) ~ hromada_FE + month_FE + occupied×post + dist×post + dist×occ×post")
    print(f"{'─'*70}")

    df = panel.copy()
    df = df[df["pit_total_uah"] > 0].copy()
    df["log_pit"] = np.log(df["pit_total_uah"])
    df["post"]    = (df["month_dt"] >= "2022-02-01").astype(int)
    df["T"]       = df["occupied_mar2022"].astype(int)
    df["dist"]    = df["dist_km"]

    # Apply bandwidth filter if specified
    if bw is not None:
        df = df[df["dist"].abs() <= bw].copy()
        bw_label = f"±{bw:.0f}km"
    else:
        bw_label = "full sample"

    print(f"\n  Sample: {df['adm3_pcode'].nunique()} hromadas × "
          f"{df['month'].nunique()} months = {len(df)} obs  [{bw_label}]")
    print(f"  Treatment: {df[df['T']==1]['adm3_pcode'].nunique()} occupied, "
          f"{df[df['T']==0]['adm3_pcode'].nunique()} control\n")

    specs = {
        "Pooled post (2022–2024)": "post",
        "2022 only":               "yr2022",
        "2023 only":               "yr2023",
        "2024 only":               "yr2024",
    }

    df["yr2022"] = ((df["month_dt"] >= "2022-01-01") & (df["month_dt"] < "2023-01-01")).astype(int)
    df["yr2023"] = ((df["month_dt"] >= "2023-01-01") & (df["month_dt"] < "2024-01-01")).astype(int)
    df["yr2024"] = ((df["month_dt"] >= "2024-01-01") & (df["month_dt"] < "2025-01-01")).astype(int)

    # Convert FEs to strings for C() notation
    df["hrom_fe"] = df["adm3_pcode"].astype("category")
    df["time_fe"] = df["month"].astype("category")

    results_summary = []

    for label, post_var in specs.items():
        formula = (
            f"log_pit ~ C(hrom_fe) + C(time_fe) "
            f"+ T:{post_var} + dist:{post_var} + dist:T:{post_var} - 1"
        )
        try:
            mod  = smf.ols(formula, data=df).fit(
                cov_type="cluster",
                cov_kwds={"groups": df["hrom_fe"]},
            )
            # Extract the T:post term (DiD-D coefficient)
            coef_name = f"T:{post_var}"
            if coef_name not in mod.params.index:
                # Try alternate naming
                coef_name = next((c for c in mod.params.index if f"T:{post_var}" in c or f"{post_var}:T" in c), None)
            if coef_name is None:
                print(f"  [{label}] Could not find coefficient — available: {[c for c in mod.params.index if 'T' in c or post_var in c][:8]}")
                continue

            delta  = mod.params[coef_name]
            se     = mod.bse[coef_name]
            pval   = mod.pvalues[coef_name]
            ci_lo  = delta - 1.96 * se
            ci_hi  = delta + 1.96 * se
            r2     = mod.rsquared
            n      = int(mod.nobs)
            pct    = (np.exp(delta) - 1) * 100

            sig = "***" if pval<0.01 else "**" if pval<0.05 else "*" if pval<0.1 else ""
            print(f"  [{label}]  BW={bw_label}")
            print(f"    δ (DiD-D) = {delta:+.4f}  SE={se:.4f}  p={pval:.4f}  {sig}")
            print(f"    95% CI: [{ci_lo:.4f}, {ci_hi:.4f}]")
            print(f"    Interpretation: occupied hromadas show {pct:+.1f}% PIT growth gap vs control")
            print(f"    N={n}  R²={r2:.3f}\n")

            results_summary.append({
                "spec": label, "bw": bw_label,
                "delta": delta, "se": se, "p": pval,
                "ci_lo": ci_lo, "ci_hi": ci_hi, "pct": pct,
                "n": n, "r2": r2, "sig": sig,
            })

        except Exception as e:
            print(f"  [{label}] ERROR: {e}\n")

    return results_summary


def run_event_study_panel(panel: pd.DataFrame, bw: float = None) -> pd.DataFrame:
    """
    Panel event study: interact T_i with each month dummy to get month-by-month δ_t.

    log_pit_it = α_i + λ_t + Σ_t δ_t*(T_i × 1[month=t]) + ε_it

    δ_t for pre-war months = 0 (parallel trends test)
    δ_t for post-war months = dynamic treatment effect

    Reference month: January 2022 (one month before invasion).
    """
    print(f"\n{'═'*70}")
    bw_label = f"±{bw:.0f}km" if bw is not None else "full"
    print(f"(B2) PANEL EVENT STUDY — month-by-month δ_t, BW={bw_label}")
    print(f"{'─'*70}")

    df = panel.copy()
    df = df[df["pit_total_uah"] > 0].copy()
    df["log_pit"] = np.log(df["pit_total_uah"])
    df["T"]       = df["occupied_mar2022"].astype(int)

    if bw is not None:
        df = df[df["dist_km"].abs() <= bw].copy()

    # Create month interaction dummies (all except reference month Jan 2022)
    months = sorted(df["month"].unique())
    ref    = "2022-01"
    non_ref = [m for m in months if m != ref]

    # Build interaction columns T * 1[month=m]
    for m in non_ref:
        # Replace hyphens so statsmodels formula parser accepts the column name
        col = f"Tx{m.replace('-', '_')}"
        df[col] = (df["T"] * (df["month"] == m)).astype(float)

    non_ref_cols   = [f"Tx{m.replace('-','_')}" for m in non_ref]
    interact_terms = " + ".join(non_ref_cols)
    formula = f"log_pit ~ C(adm3_pcode) + C(month) + {interact_terms} - 1"

    print(f"  Fitting panel event study ({len(df)} obs, ref month = {ref})…")
    try:
        mod = smf.ols(formula, data=df).fit(
            cov_type="cluster",
            cov_kwds={"groups": df["adm3_pcode"]},
        )
    except Exception as e:
        print(f"  ERROR: {e}")
        return pd.DataFrame()

    # Extract δ_t coefficients
    rows = []
    for m in months:
        if m == ref:
            rows.append({"month": m, "delta": 0.0, "se": 0.0, "p": 1.0,
                          "ci_lo": 0.0, "ci_hi": 0.0})
            continue
        cname = f"Tx{m.replace('-','_')}"
        if cname not in mod.params.index:
            rows.append({"month": m, "delta": np.nan, "se": np.nan, "p": np.nan,
                          "ci_lo": np.nan, "ci_hi": np.nan})
            continue
        d  = mod.params[cname]
        se = mod.bse[cname]
        p  = mod.pvalues[cname]
        rows.append({"month": m, "delta": d, "se": se, "p": p,
                      "ci_lo": d - 1.96*se, "ci_hi": d + 1.96*se})

    es = pd.DataFrame(rows)
    es["month_dt"]  = pd.to_datetime(es["month"] + "-01")
    es["rel_month"] = ((es["month_dt"] - pd.Timestamp("2022-02-01")) / pd.Timedelta("31D")).round().astype(int)

    # Pre-trend test: joint F-test for pre-war δ_t = 0
    pre_months = [m for m in non_ref if m < "2022-02"]
    pre_params = [f"Tx{m}" for m in pre_months if f"Tx{m}" in mod.params.index]
    if pre_params:
        ftest = mod.f_test([f"{p} = 0" for p in pre_params])
        print(f"\n  Pre-trend F-test ({len(pre_params)} pre-war months):")
        print(f"    F({len(pre_params)}, dof) = {ftest.fvalue:.3f},  p = {ftest.pvalue:.4f}")
        if ftest.pvalue > 0.1:
            print("    ✓ Pre-trends parallel (p > 0.1) — DiD-D design is valid")
        else:
            print("    ✗ WARNING: Pre-trends may not be parallel (p < 0.1)")

    # Print event study table
    print(f"\n  {'Month':>8} {'rel_mo':>7} {'δ_t':>9} {'SE':>7} {'p':>7}  {'[95% CI]':>22}")
    for _, r in es.iterrows():
        if pd.isna(r["delta"]):
            continue
        sig  = "***" if r["p"]<0.01 else "**" if r["p"]<0.05 else "*" if r["p"]<0.1 else ""
        mark = "" if r["month"] < "2022-02" else (" ◀" if r["month"] <= "2022-04" else "")
        ref_mark = " [REF]" if r["month"] == ref else ""
        ci = f"[{r['ci_lo']:.3f}, {r['ci_hi']:.3f}]"
        print(f"  {r['month']:>8} {int(r['rel_month']):>7} {r['delta']:>9.4f} "
              f"{r['se']:>7.4f} {r['p']:>7.4f}  {ci:>22}  {sig}{mark}{ref_mark}")

    return es


# ═══════════════════════════════════════════════════════════════════════════
# (C)  SUMY-ONLY DiD-D
# ═══════════════════════════════════════════════════════════════════════════

def run_sumy_did_d(panel: pd.DataFrame, bw: float = 20.0) -> None:
    """
    Run the full panel DiD-D on Sumy Oblast only.
    """
    print(f"\n{'═'*70}")
    print("(C) SUMY-ONLY PANEL DiD-D (robustness — heterogeneity driver)")
    print(f"{'─'*70}")
    sumy = panel[panel["oblast_en"] == "Sumy"].copy() if "oblast_en" in panel.columns else panel.copy()
    if len(sumy) == 0:
        print("  (no Sumy data found)")
        return
    run_panel_did_d(sumy, bw=bw)


# ═══════════════════════════════════════════════════════════════════════════
# Summary table
# ═══════════════════════════════════════════════════════════════════════════

def print_summary_table(all_results: list) -> None:
    print(f"\n{'═'*80}")
    print("SUMMARY — DiD-D Panel Regression Results (Primary Specification)")
    print(f"{'─'*80}")
    print(f"  {'Specification':30} {'δ (DiD-D)':>10} {'SE':>8} {'p':>8} {'%gap':>8}  {'N':>6}  sig")
    for r in all_results:
        print(f"  {r['spec']:30} {r['delta']:>10.4f} {r['se']:>8.4f} "
              f"{r['p']:>8.4f} {r['pct']:>7.1f}%  {r['n']:>6}  {r['sig']}")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("Loading RDD panel…")
    panel = pd.read_csv(PANEL_FILE)
    panel["month_dt"] = pd.to_datetime(panel["month"] + "-01")
    panel["year"]     = panel["month_dt"].dt.year
    print(f"  {panel['adm3_pcode'].nunique()} hromadas × "
          f"{panel['month'].nunique()} months = {len(panel)} obs")

    # ─── (A) Event study: monthly cross-sectional τ_t ────────────────────
    es_cs = run_event_study(panel, bw=20)
    es_cs.to_csv(OUT_DIR / "event_study_crosssection.csv", index=False)

    # ─── (B) Panel regression DiD-D ──────────────────────────────────────
    print(f"\n{'─'*70}")
    print("Full sample (all 164 hromadas):")
    res_full  = run_panel_did_d(panel, bw=None)

    print(f"\n{'─'*70}")
    print("Bandwidth restricted to ±20 km (148 hromadas):")
    res_20    = run_panel_did_d(panel, bw=20)

    # ─── (B2) Panel event study: month-by-month δ_t ──────────────────────
    es_panel = run_event_study_panel(panel, bw=20)
    if len(es_panel):
        es_panel.to_csv(OUT_DIR / "event_study_panel.csv", index=False)
        print(f"\n  ✓ Panel event study saved: {OUT_DIR}/event_study_panel.csv")

    # ─── (C) Sumy only ───────────────────────────────────────────────────
    run_sumy_did_d(panel, bw=20)

    # ─── Summary ─────────────────────────────────────────────────────────
    all_res = (res_full or []) + (res_20 or [])
    if all_res:
        print_summary_table(all_res)

    print(f"\n✓ Event study data saved to {OUT_DIR}/event_study_*.csv")


if __name__ == "__main__":
    main()
