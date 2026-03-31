"""
SpatialRDD/rdd_analysis.py
─────────────────────────────────────────────────────────────────────────────
Spatial Regression Discontinuity Design — core estimation.

Research question:
  "What is the local causal effect of temporary Russian occupation on
   fiscal resilience (PIT revenue) in hromadas near the March 2022
   occupation boundary?"

Design:
  - Running variable: signed distance to occupation boundary (dist_km)
    negative = occupied side, positive = control side
  - Cutoff: 0 (the boundary)
  - Outcome: log annual PIT revenue (normalized to pre-war baseline)
  - Estimator: local linear regression with triangle kernel
  - Bandwidth: data-driven (Imbens-Kalyanaraman 2012 simplified) + fixed BW table

Specifications:
  1. Pre-war balance check (2021): should show NO jump → validates design
  2. Occupation period (2022): main treatment effect
  3. Recovery (2023, 2024): persistence / recovery estimates
  4. Difference-in-Discontinuities (DiD-D): Δtau = tau_post - tau_pre
  5. Donut-hole robustness: exclude ±5km around boundary
"""

import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore")

ROOT      = Path(__file__).resolve().parent.parent
PANEL_FILE = ROOT / "data/clean/spatial/rdd_panel.csv"
OUT_DIR   = ROOT / "data/clean/spatial"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Core RDD estimation functions
# ─────────────────────────────────────────────────────────────────────────────

def triangle_kernel(dist: np.ndarray, bw: float) -> np.ndarray:
    """Triangle kernel weights: K(u) = 1 - |u/h|, zero outside bandwidth."""
    u = np.abs(dist) / bw
    return np.maximum(0.0, 1.0 - u)


def local_linear_rdd(
    y: np.ndarray,
    x: np.ndarray,
    bw: float,
    cutoff: float = 0.0,
    donut: float = 0.0,
) -> dict:
    """
    Local linear RDD estimate with triangle kernel.

    Fits: y_i = α + τ*T_i + β₁*x_i + β₂*T_i*x_i + ε_i
    where T_i = 1 if x_i < cutoff (occupied side = negative dist → treated).

    Parameters
    ----------
    y       : outcome variable (aligned with x)
    x       : running variable (dist_km; negative = treated)
    bw      : bandwidth (km)
    cutoff  : threshold (default 0)
    donut   : inner exclusion radius (km); exclude |x| < donut

    Returns
    -------
    dict with tau (treatment effect), se, t_stat, p_value, n, n_treated, n_control
    """
    x_centered = x - cutoff
    treated    = (x_centered < 0).astype(float)

    # Bandwidth filter
    in_bw = np.abs(x_centered) <= bw
    # Donut filter (exclude the inner band)
    if donut > 0:
        in_bw &= np.abs(x_centered) >= donut

    y_s = y[in_bw]
    x_s = x_centered[in_bw]
    t_s = treated[in_bw]

    if len(y_s) < 10:
        return {"tau": np.nan, "se": np.nan, "t_stat": np.nan,
                "p_value": np.nan, "n": len(y_s), "n_treated": 0, "n_control": 0,
                "ci_lo": np.nan, "ci_hi": np.nan, "bw": bw}

    # Triangle kernel weights
    w = triangle_kernel(x_s, bw)

    # Design matrix: [1, T, x, T*x]
    X_des = np.column_stack([np.ones(len(y_s)), t_s, x_s, t_s * x_s])

    # Weighted OLS: β = (X'WX)^{-1} X'Wy
    W = np.diag(w)
    try:
        XtW  = X_des.T @ W
        XtWX = XtW @ X_des
        XtWy = XtW @ y_s
        beta = np.linalg.solve(XtWX, XtWy)
    except np.linalg.LinAlgError:
        return {"tau": np.nan, "se": np.nan, "t_stat": np.nan,
                "p_value": np.nan, "n": len(y_s), "n_treated": int(t_s.sum()),
                "n_control": int((1 - t_s).sum()), "ci_lo": np.nan, "ci_hi": np.nan, "bw": bw}

    tau = beta[1]   # Treatment effect (jump at cutoff)

    # Robust (HC1) standard errors
    y_hat    = X_des @ beta
    residuals = y_s - y_hat
    n = len(y_s)
    k = X_des.shape[1]
    # HC1 sandwich: multiply by n/(n-k)
    meat = (X_des * (w * residuals)[:, None]).T @ (X_des * (w * residuals)[:, None])
    bread = np.linalg.inv(XtWX)
    V = bread @ meat @ bread * (n / (n - k))
    se = np.sqrt(np.diag(V)[1])   # SE for tau

    t_stat  = tau / se
    p_value = 2 * stats.t.sf(np.abs(t_stat), df=n - k)
    ci_lo   = tau - 1.96 * se
    ci_hi   = tau + 1.96 * se

    return {
        "tau":       tau,
        "se":        se,
        "t_stat":    t_stat,
        "p_value":   p_value,
        "ci_lo":     ci_lo,
        "ci_hi":     ci_hi,
        "n":         n,
        "n_treated": int(t_s.sum()),
        "n_control": int((1 - t_s).sum()),
        "bw":        bw,
    }


def ik_bandwidth(y: np.ndarray, x: np.ndarray, cutoff: float = 0.0) -> float:
    """
    Simplified Imbens-Kalyanaraman (2012) bandwidth selector.
    Uses a pilot regression to estimate the curvature on each side,
    then selects h to minimize the asymptotic MSE.

    This is a simplified (non-iterative) implementation.
    """
    x_c = x - cutoff
    treated = x_c < 0

    results = {}
    for side_name, mask in [("ctrl", ~treated), ("trt", treated)]:
        x_side = x_c[mask]
        y_side = y[mask]
        if len(y_side) < 5:
            results[side_name] = {"m2": 1.0, "sigma2": 1.0, "n": len(y_side), "f": 1.0}
            continue
        # Pilot: quadratic fit to estimate second derivative (curvature)
        X_pilot = np.column_stack([np.ones(len(x_side)), x_side, x_side**2])
        try:
            beta_pilot = np.linalg.lstsq(X_pilot, y_side, rcond=None)[0]
            m2 = 2 * beta_pilot[2]   # second derivative at cutoff
        except Exception:
            m2 = 1.0
        resid = y_side - X_pilot @ beta_pilot
        sigma2 = np.var(resid)
        # Density estimate at cutoff (kernel density using Silverman rule)
        bw_pilot = 1.06 * np.std(x_side) * len(x_side) ** (-0.2)
        f = np.mean(np.abs(x_side) < bw_pilot) / (2 * bw_pilot) if bw_pilot > 0 else 0.01
        results[side_name] = {"m2": m2, "sigma2": sigma2, "n": len(y_side), "f": max(f, 0.001)}

    n = len(y)
    f0 = (results["ctrl"]["f"] + results["trt"]["f"]) / 2
    sigma2 = (results["ctrl"]["sigma2"] + results["trt"]["sigma2"]) / 2
    # Bias-variance terms (MSE-optimal BW formula)
    m2_sq = (results["ctrl"]["m2"] ** 2 + results["trt"]["m2"] ** 2) / 2
    if m2_sq < 1e-10:
        m2_sq = 0.01  # fallback
    # h* = (sigma2 / (n * f0 * m2_sq))^(1/5)
    h_opt = (sigma2 / (n * f0 * m2_sq + 1e-10)) ** 0.2
    # Clamp to reasonable range [5, 50] km
    h_opt = float(np.clip(h_opt, 5.0, 50.0))
    return h_opt


# ─────────────────────────────────────────────────────────────────────────────
# Build annual cross-sections for RDD
# ─────────────────────────────────────────────────────────────────────────────

def build_annual_outcome(panel: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Aggregate monthly PIT to annual totals for a given year.
    Returns one row per hromada with: dist_km, occupied_mar2022, pit_uah, log_pit
    """
    yr_data = panel[panel["year"] == year].copy()
    grp_cols = [c for c in ["adm3_pcode", "adm3_name_en", "oblast_en", "occupied_mar2022", "dist_km"] if c in yr_data.columns]
    agg = yr_data.groupby(grp_cols)["pit_total_uah"].sum().reset_index()
    agg = agg[agg["pit_total_uah"] > 0].copy()
    agg["log_pit"] = np.log(agg["pit_total_uah"])
    return agg


def build_log_ratio_outcome(panel: pd.DataFrame, year_post: int, year_pre: int = 2021) -> pd.DataFrame:
    """
    Build log ratio: log(pit_year_post / pit_year_pre) for each hromada.
    Controls for pre-war size differences — the preferred RDD outcome variable.
    """
    pre  = build_annual_outcome(panel, year_pre).rename(columns={"pit_total_uah": "pit_pre", "log_pit": "lpit_pre"})
    post = build_annual_outcome(panel, year_post).rename(columns={"pit_total_uah": "pit_post", "log_pit": "lpit_post"})
    merged = pre.merge(
        post[["adm3_pcode", "pit_post", "lpit_post"]],
        on="adm3_pcode", how="inner",
    )
    merged = merged[(merged["pit_pre"] > 0) & (merged["pit_post"] > 0)]
    # Log ratio: growth in PIT relative to baseline
    merged["log_ratio"] = merged["lpit_post"] - merged["lpit_pre"]
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Main RDD routine
# ─────────────────────────────────────────────────────────────────────────────

def run_rdd_table(
    panel: pd.DataFrame,
    bandwidths: list[float] = [10, 20, 30, 50],
    donut: float = 0.0,
    outcome_label: str = "log_pit",
) -> pd.DataFrame:
    """
    Run RDD for multiple years × bandwidths.
    Returns a results table.
    """
    results = []

    for year in [2021, 2022, 2023, 2024]:
        if year == 2021:
            # Balance check: log level of 2021 PIT
            df = build_annual_outcome(panel, 2021)
            y  = df["log_pit"].values
            x  = df["dist_km"].values
            outcome = "log_pit_level (balance)"
        else:
            # Main outcome: log ratio vs. 2021 baseline
            df = build_log_ratio_outcome(panel, year)
            y  = df["log_ratio"].values
            x  = df["dist_km"].values
            outcome = f"log(pit_{year}/pit_2021)"

        # Data-driven bandwidth
        h_ik = ik_bandwidth(y, x)

        for bw in sorted(set(bandwidths + [round(h_ik, 1)])):
            res = local_linear_rdd(y, x, bw=bw, donut=donut)
            res["year"]    = year
            res["outcome"] = outcome
            results.append(res)

    df_res = pd.DataFrame(results)
    return df_res


def print_rdd_table(df: pd.DataFrame) -> None:
    """Pretty-print the RDD results table."""
    print("\n" + "═" * 90)
    print("SPATIAL RDD RESULTS — Effect of March 2022 Occupation on PIT Revenue")
    print("Estimator: Local Linear Regression (Triangle Kernel, HC1 robust SE)")
    print("Running variable: Signed distance to occupation boundary (km)")
    print("Treatment: hromada was on occupied side (dist < 0)")
    print("═" * 90)

    for year in [2021, 2022, 2023, 2024]:
        year_res = df[df["year"] == year].sort_values("bw")
        print(f"\n{'─'*90}")
        label = year_res["outcome"].iloc[0]
        print(f"Year {year}  |  Outcome: {label}")
        print(f"  {'BW (km)':>8} {'tau':>10} {'SE':>8} {'t-stat':>8} {'p-value':>10} "
              f"{'95% CI':>20} {'N':>5} {'N_trt':>6} {'N_ctrl':>7}")
        for _, row in year_res.iterrows():
            sig = "***" if row["p_value"] < 0.01 else "**" if row["p_value"] < 0.05 else "*" if row["p_value"] < 0.1 else "   "
            ci  = f"[{row['ci_lo']:.3f}, {row['ci_hi']:.3f}]"
            bw_str = f"±{row['bw']:.1f}" if not pd.isna(row['bw']) else "N/A"
            print(f"  {bw_str:>8} {row['tau']:>10.4f} {row['se']:>8.4f} "
                  f"{row['t_stat']:>8.2f} {row['p_value']:>10.4f} {ci:>20}  "
                  f"{sig} {int(row['n']):>5} {int(row['n_treated']):>6} {int(row['n_control']):>7}")

    print(f"\n{'─'*90}")
    print("Significance: * p<0.1, ** p<0.05, *** p<0.01")
    print("Note: tau < 0 means occupied hromadas had lower PIT growth than control hromadas.")
    print("      2021 row = balance test (should show tau ≈ 0 if RDD is valid).")


def run_did_d(panel: pd.DataFrame, bw: float) -> None:
    """
    Difference-in-Discontinuities:
      Δτ = τ_post - τ_pre
    Compares the boundary jump post-war to the pre-war boundary jump.
    This eliminates time-invariant spatial sorting near the boundary.
    """
    print(f"\n{'═'*70}")
    print("DIFFERENCE-IN-DISCONTINUITIES (DiD-D) — Bandwidth ±{:.0f} km".format(bw))
    print(f"{'─'*70}")

    # Pre-war: 2021 log PIT level
    pre  = build_annual_outcome(panel, 2021)
    y_pre, x_pre = pre["log_pit"].values, pre["dist_km"].values
    res_pre = local_linear_rdd(y_pre, x_pre, bw=bw)

    print(f"  Pre-war jump (2021 log PIT level):  τ = {res_pre['tau']:+.4f}  "
          f"SE={res_pre['se']:.4f}  p={res_pre['p_value']:.4f}")

    for year in [2022, 2023, 2024]:
        post = build_log_ratio_outcome(panel, year)
        y_post, x_post = post["log_ratio"].values, post["dist_km"].values
        res_post = local_linear_rdd(y_post, x_post, bw=bw)

        # DiD-D estimate: tau_post - tau_pre
        tau_dd = res_post["tau"] - res_pre["tau"]
        # Conservative SE: sqrt(var_post + var_pre) assuming independence
        se_dd  = np.sqrt(res_post["se"]**2 + res_pre["se"]**2)
        t_dd   = tau_dd / se_dd if se_dd > 0 else np.nan
        p_dd   = 2 * stats.t.sf(np.abs(t_dd), df=res_post["n"] - 4)
        sig = "***" if p_dd < 0.01 else "**" if p_dd < 0.05 else "*" if p_dd < 0.1 else ""

        # Convert log point to approximate % change
        pct = (np.exp(tau_dd) - 1) * 100

        print(f"\n  {year} log(PIT_{year}/PIT_2021) jump:  τ = {res_post['tau']:+.4f}  "
              f"SE={res_post['se']:.4f}  p={res_post['p_value']:.4f}  "
              f"N={res_post['n']} ({res_post['n_treated']} occ / {res_post['n_control']} ctrl)")
        print(f"  DiD-D estimate (Δτ = τ_{year} − τ_pre): "
              f"Δτ = {tau_dd:+.4f}  SE={se_dd:.4f}  t={t_dd:+.2f}  "
              f"p={p_dd:.4f} {sig}")
        print(f"  Interpretation: occupied hromadas had PIT growth {pct:+.1f}% different")
        print(f"  from control hromadas, relative to pre-war spatial pattern {sig}")


def run_donut_robustness(panel: pd.DataFrame, bw: float, donuts: list[float]) -> None:
    """Donut-hole robustness: exclude hromadas within d km of boundary."""
    print(f"\n{'═'*70}")
    print(f"DONUT-HOLE ROBUSTNESS — Outcome: log(PIT_2022/PIT_2021), BW=±{bw:.0f}km")
    print(f"{'─'*70}")
    print(f"  {'Donut (km)':>12} {'tau':>10} {'SE':>8} {'p-value':>10} {'N':>5}")

    post = build_log_ratio_outcome(panel, 2022)
    y, x = post["log_ratio"].values, post["dist_km"].values

    for d in [0.0] + donuts:
        res = local_linear_rdd(y, x, bw=bw, donut=d)
        sig = "***" if res["p_value"] < 0.01 else "**" if res["p_value"] < 0.05 else "*" if res["p_value"] < 0.1 else ""
        label = "none" if d == 0.0 else f"±{d:.0f} km"
        print(f"  {label:>12} {res['tau']:>10.4f} {res['se']:>8.4f} "
              f"{res['p_value']:>10.4f} {int(res['n']):>5}  {sig}")


# ─────────────────────────────────────────────────────────────────────────────
# Descriptive statistics
# ─────────────────────────────────────────────────────────────────────────────

def print_descriptives(panel: pd.DataFrame) -> None:
    """Print key descriptive statistics."""
    print("\n" + "═" * 70)
    print("DESCRIPTIVE STATISTICS")
    print("─" * 70)

    annual = panel.groupby(["adm3_pcode", "year", "occupied_mar2022", "dist_km"])["pit_total_uah"].sum().reset_index()
    annual = annual[annual["pit_total_uah"] > 0]

    print("\nPanel coverage:")
    print(f"  Total hromada-year obs: {len(annual)}")
    print(f"  Occupied: {annual[annual['occupied_mar2022']==1]['adm3_pcode'].nunique()} hromadas")
    print(f"  Control:  {annual[annual['occupied_mar2022']==0]['adm3_pcode'].nunique()} hromadas")
    print(f"  Distance range: [{annual['dist_km'].min():.1f}, {annual['dist_km'].max():.1f}] km")

    print("\nAnnual PIT by group (Median, M UAH):")
    print(f"  {'Year':>6} {'Ctrl':>12} {'Occupied':>12}  {'Ratio':>8}")
    for yr in [2021, 2022, 2023, 2024]:
        ctrl = annual[(annual["year"]==yr) & (annual["occupied_mar2022"]==0)]["pit_total_uah"].median() / 1e6
        occ  = annual[(annual["year"]==yr) & (annual["occupied_mar2022"]==1)]["pit_total_uah"].median() / 1e6
        ratio = occ / ctrl if ctrl > 0 else np.nan
        print(f"  {yr:>6} {ctrl:>12.1f} {occ:>12.1f}  {ratio:>8.2f}x")

    print("\nMedian log PIT growth vs. 2021 by group:")
    pre = annual[annual["year"] == 2021].set_index("adm3_pcode")["pit_total_uah"].rename("pit_2021")
    for yr in [2022, 2023, 2024]:
        post = annual[annual["year"] == yr].set_index("adm3_pcode")["pit_total_uah"].rename("pit_post")
        merged = pd.concat([pre, post], axis=1).dropna()
        merged["log_ratio"] = np.log(merged["pit_post"] / merged["pit_2021"])
        merged = merged.merge(
            annual[annual["year"]==yr][["adm3_pcode","occupied_mar2022"]].set_index("adm3_pcode"),
            left_index=True, right_index=True, how="left"
        )
        g = merged.groupby("occupied_mar2022")["log_ratio"].median()
        ctrl_gr = g.get(0, np.nan)
        occ_gr  = g.get(1, np.nan)
        diff    = occ_gr - ctrl_gr
        print(f"  {yr}: ctrl={ctrl_gr:+.3f}  occ={occ_gr:+.3f}  diff={diff:+.3f} "
              f"({(np.exp(diff)-1)*100:+.1f}% occ vs ctrl)")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_donut_all_years(panel: pd.DataFrame, bw: float, donuts: list[float]) -> None:
    """Donut-hole robustness for all post-war years."""
    print(f"\n{'═'*80}")
    print(f"DONUT-HOLE ROBUSTNESS — All Years, BW=±{bw:.0f}km (DiD-D estimates)")
    print(f"{'─'*80}")
    print(f"  {'Donut':>10} {'Year':>6} {'Δτ (DiD-D)':>12} {'SE':>8} {'p-value':>10} {'N':>5}")

    # Pre-war baseline (same across all donut specs)
    pre = build_annual_outcome(panel, 2021)

    for d in [0.0] + donuts:
        label = "none" if d == 0.0 else f"±{d:.0f}km"
        res_pre = local_linear_rdd(pre["log_pit"].values, pre["dist_km"].values, bw=bw, donut=d)
        for year in [2022, 2023, 2024]:
            post = build_log_ratio_outcome(panel, year)
            res_post = local_linear_rdd(post["log_ratio"].values, post["dist_km"].values, bw=bw, donut=d)
            tau_dd = res_post["tau"] - res_pre["tau"]
            se_dd  = np.sqrt(res_post["se"]**2 + res_pre["se"]**2)
            p_dd   = 2 * stats.t.sf(np.abs(tau_dd / se_dd), df=max(res_post["n"]-4, 1))
            sig = "***" if p_dd<0.01 else "**" if p_dd<0.05 else "*" if p_dd<0.1 else ""
            print(f"  {label:>10} {year:>6} {tau_dd:>12.4f} {se_dd:>8.4f} {p_dd:>10.4f} {res_post['n']:>5}  {sig}")
        print()


def run_placebo_cutoffs(panel: pd.DataFrame, bw: float) -> None:
    """
    Placebo test: shift the cutoff to fake distances (±10, ±20 km) and
    check that the DiD-D effect disappears. If the effect is real (at 0),
    fake cutoffs should show no significant jump.
    """
    print(f"\n{'═'*80}")
    print(f"PLACEBO CUTOFF TEST — Fake boundary shifts, BW=±{bw:.0f}km, Year=2022")
    print("(Real effect should only appear near cutoff = 0; fake cuts should be null)")
    print(f"{'─'*80}")
    print(f"  {'Cutoff':>12} {'τ_post':>10} {'SE':>8} {'p-value':>10} {'N':>5}")

    post = build_log_ratio_outcome(panel, 2022)
    y, x = post["log_ratio"].values, post["dist_km"].values

    for cutoff in [-25, -15, 0, 15, 25]:
        # Only use observations far from the real cutoff for fake tests
        if cutoff != 0:
            mask = (x < cutoff - bw/2) | (x > cutoff + bw/2)  # not straddling real cutoff
        else:
            mask = np.ones(len(x), dtype=bool)
        y_p, x_p = y[mask], x[mask]
        res = local_linear_rdd(y_p, x_p, bw=bw, cutoff=cutoff)
        sig = "***" if res["p_value"]<0.01 else "**" if res["p_value"]<0.05 else "*" if res["p_value"]<0.1 else "   "
        tag = " ← REAL" if cutoff == 0 else ""
        print(f"  {cutoff:>+12} km {res['tau']:>10.4f} {res['se']:>8.4f} "
              f"{res['p_value']:>10.4f} {res['n']:>5}  {sig}{tag}")


def run_by_oblast(panel: pd.DataFrame, bw: float) -> None:
    """
    Run DiD-D separately for each of the 3 oblasts as a heterogeneity check.
    """
    print(f"\n{'═'*80}")
    print(f"HETEROGENEITY BY OBLAST — DiD-D, BW=±{bw:.0f}km")
    print(f"{'─'*80}")

    oblasts = panel["oblast_en"].dropna().unique() if "oblast_en" in panel.columns else []
    if len(oblasts) == 0:
        print("  (oblast_en column not available — skipping)")
        return

    pre_all = build_annual_outcome(panel, 2021)

    for ob in sorted(oblasts):
        ob_panel = panel[panel["oblast_en"] == ob]
        pre_ob   = pre_all[pre_all["adm3_pcode"].isin(ob_panel["adm3_pcode"].unique())]
        res_pre  = local_linear_rdd(pre_ob["log_pit"].values, pre_ob["dist_km"].values, bw=bw)

        print(f"\n  {ob} (n={ob_panel['adm3_pcode'].nunique()} hromadas):")
        print(f"    {'Year':>6} {'τ_post':>10} {'Δτ DiD-D':>10} {'SE':>8} {'p-value':>10}")

        for year in [2022, 2023, 2024]:
            post_ob = build_log_ratio_outcome(ob_panel, year)
            res_post = local_linear_rdd(post_ob["log_ratio"].values, post_ob["dist_km"].values, bw=bw)
            tau_dd = res_post["tau"] - res_pre["tau"]
            se_dd  = np.sqrt(res_post["se"]**2 + res_pre["se"]**2)
            p_dd   = 2 * stats.t.sf(np.abs(tau_dd/se_dd) if se_dd>0 else 0, df=max(res_post["n"]-4,1))
            sig = "***" if p_dd<0.01 else "**" if p_dd<0.05 else "*" if p_dd<0.1 else ""
            print(f"    {year:>6} {res_post['tau']:>10.4f} {tau_dd:>10.4f} {se_dd:>8.4f} "
                  f"{p_dd:>10.4f}  {sig}")


def run_bandwidth_sensitivity(panel: pd.DataFrame) -> None:
    """
    Coefficient stability plot data: run DiD-D at every 2km bandwidth from 5 to 50.
    Shows whether the effect is stable or driven by a particular BW choice.
    """
    print(f"\n{'═'*80}")
    print("BANDWIDTH SENSITIVITY — DiD-D τ at every 2km step (Year=2024, most significant)")
    print(f"{'─'*80}")
    print(f"  {'BW':>6} {'Δτ':>10} {'SE':>8} {'p-value':>8}  {'[95% CI]':>22}  N")

    pre = build_annual_outcome(panel, 2021)
    post = build_log_ratio_outcome(panel, 2024)

    for bw in range(5, 52, 3):
        res_pre  = local_linear_rdd(pre["log_pit"].values, pre["dist_km"].values, bw=bw)
        res_post = local_linear_rdd(post["log_ratio"].values, post["dist_km"].values, bw=bw)
        tau_dd = res_post["tau"] - res_pre["tau"]
        se_dd  = np.sqrt(res_post["se"]**2 + res_pre["se"]**2)
        p_dd   = 2 * stats.t.sf(np.abs(tau_dd/se_dd) if se_dd>0 else 0, df=max(res_post["n"]-4,1))
        sig = "***" if p_dd<0.01 else "**" if p_dd<0.05 else "*" if p_dd<0.1 else ""
        ci = f"[{tau_dd - 1.96*se_dd:.3f}, {tau_dd + 1.96*se_dd:.3f}]"
        print(f"  {bw:>6} {tau_dd:>10.4f} {se_dd:>8.4f} {p_dd:>8.4f}  {ci:>22}  {res_post['n']}  {sig}")


def main():
    print("Loading RDD panel…")
    panel = pd.read_csv(PANEL_FILE)
    panel["month_dt"] = pd.to_datetime(panel["month"] + "-01")
    panel["year"]     = panel["month_dt"].dt.year

    print(f"  {panel['adm3_pcode'].nunique()} hromadas × "
          f"{panel['month'].nunique()} months = {len(panel)} rows")

    # 1. Descriptives
    print_descriptives(panel)

    # 2. Main RDD table (multiple bandwidths, all years)
    bandwidths = [10, 20, 30, 50]
    df_res = run_rdd_table(panel, bandwidths=bandwidths)
    print_rdd_table(df_res)

    # 3. Difference-in-Discontinuities (main bandwidth ≈ 20 km)
    run_did_d(panel, bw=20)

    # 4. Donut-hole robustness — all years
    run_donut_all_years(panel, bw=20, donuts=[3, 5, 10])

    # 5. Placebo cutoff test
    run_placebo_cutoffs(panel, bw=20)

    # 6. By-oblast heterogeneity
    run_by_oblast(panel, bw=20)

    # 7. Bandwidth sensitivity
    run_bandwidth_sensitivity(panel)

    # 8. Save results table
    out_path = OUT_DIR / "rdd_results.csv"
    df_res.to_csv(out_path, index=False)
    print(f"\n✓ Results saved to {out_path}")


if __name__ == "__main__":
    main()
