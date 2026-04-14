"""
StreetBets - Dashboard API
FastAPI serving dashboard data from SQLite.
Runs alongside the scheduler process on Railway.
"""

import os
from datetime import date, timedelta
from typing import Optional
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import db

app = FastAPI(title="StreetBets Dashboard API", docs_url="/api/docs")


def _trading_dates(n: int = 5) -> list[str]:
    """Return last n trading dates (Mon-Fri) up to and including today."""
    dates = []
    d = date.today()
    while len(dates) < n:
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d -= timedelta(days=1)
    return dates


@app.get("/api/experiment-info")
def experiment_info():
    conn = db.get_conn()
    config = {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM experiment_config").fetchall()
    }
    stats = conn.execute("""
        SELECT
            COUNT(DISTINCT date)            as trading_days,
            COUNT(DISTINCT snapshot_id)     as total_snapshots,
            MIN(date)                       as first_date,
            MAX(date)                       as last_date
        FROM snapshots
    """).fetchone()
    cycles = conn.execute("""
        SELECT COUNT(DISTINCT expiry_date) as expiry_cycles FROM snapshots
    """).fetchone()
    conn.close()
    return {
        "config": config,
        "stats": dict(stats),
        "expiry_cycles": cycles["expiry_cycles"],
    }


@app.get("/api/reconciliation-dates")
def reconciliation_dates():
    """Return dates that have reconciled data, last 10."""
    conn = db.get_conn()
    rows = conn.execute("""
        SELECT DISTINCT date FROM reconciled_predictions
        ORDER BY date DESC LIMIT 10
    """).fetchall()
    conn.close()
    return [r["date"] for r in rows]


@app.get("/api/summary")
def summary(
    underlying: str = "NIFTY",
    date: str = None,
    snapshot_label: str = None,
    option_type: str = None,
    accuracy_threshold_pct: float = 0.10,
):
    """Summary strip metrics for the reconciliation page."""
    conn = db.get_conn()
    filters, params = _build_filters(underlying, date, snapshot_label, option_type, params=[])

    # MAE long
    row = conn.execute(f"""
        SELECT
            AVG(ABS(error_long))  as mae_long,
            AVG(ABS(error_short)) as mae_short,
            COUNT(*)              as total,
            SUM(CASE WHEN ABS(pct_error_long)  <= ? THEN 1 ELSE 0 END) as within_long,
            SUM(CASE WHEN ABS(pct_error_short) <= ? THEN 1 ELSE 0 END) as within_short
        FROM reconciled_predictions
        WHERE is_nearest_match=1 {filters}
    """, [accuracy_threshold_pct, accuracy_threshold_pct] + params).fetchone()

    # Best snapshot by lowest MAE
    best = conn.execute(f"""
        SELECT snapshot_label, AVG(ABS(error_long)) as mae
        FROM reconciled_predictions
        WHERE is_nearest_match=1 {filters}
        GROUP BY snapshot_label
        ORDER BY mae ASC LIMIT 1
    """, params).fetchone()

    # Worst strike range (ATM vs OTM)
    worst = conn.execute(f"""
        SELECT
            CASE
                WHEN ABS(strike - (SELECT spot_price FROM snapshots s2
                    WHERE s2.snapshot_id=rp.snapshot_id)) /
                    (SELECT spot_price FROM snapshots s2 WHERE s2.snapshot_id=rp.snapshot_id)
                    <= 0.01 THEN 'ATM (±1%)'
                WHEN ABS(strike - (SELECT spot_price FROM snapshots s2
                    WHERE s2.snapshot_id=rp.snapshot_id)) /
                    (SELECT spot_price FROM snapshots s2 WHERE s2.snapshot_id=rp.snapshot_id)
                    <= 0.03 THEN 'Near OTM (1-3%)'
                ELSE 'Far OTM (>3%)'
            END as range_label,
            AVG(ABS(error_long)) as mae
        FROM reconciled_predictions rp
        WHERE is_nearest_match=1 {filters}
        GROUP BY range_label
        ORDER BY mae DESC LIMIT 1
    """, params).fetchone()

    conn.close()
    total = row["total"] or 1
    return {
        "mae_long":         round(row["mae_long"] or 0, 2),
        "mae_short":        round(row["mae_short"] or 0, 2),
        "total_predictions": total,
        "within_threshold_long_pct":  round((row["within_long"] or 0) / total * 100, 1),
        "within_threshold_short_pct": round((row["within_short"] or 0) / total * 100, 1),
        "best_snapshot":    best["snapshot_label"] if best else None,
        "worst_range":      worst["range_label"]   if worst else None,
    }


@app.get("/api/scatter")
def scatter(
    underlying: str = "NIFTY",
    date: str = None,
    snapshot_label: str = None,
    option_type: str = None,
    direction: str = "long",
):
    """Projected vs Actual scatter data."""
    conn = db.get_conn()
    filters, params = _build_filters(underlying, date, snapshot_label, option_type)

    pnl_col    = "projected_pnl_long"  if direction == "long"  else "projected_pnl_short"
    actual_col = "actual_pnl_long"     if direction == "long"  else "actual_pnl_short"
    error_col  = "error_long"          if direction == "long"  else "error_short"
    within_col = "within_threshold_long" if direction == "long" else "within_threshold_short"

    rows = conn.execute(f"""
        SELECT
            strike, option_type, snapshot_label,
            {pnl_col}    as projected_pnl,
            {actual_col} as actual_pnl,
            {error_col}  as error,
            {within_col} as within_threshold,
            entry_ltp, actual_close_ltp, invested_amount, pricing_method
        FROM reconciled_predictions
        WHERE is_nearest_match=1 {filters}
        ORDER BY ABS({error_col}) DESC
    """, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/heatmap")
def heatmap(
    underlying: str = "NIFTY",
    snapshot_label: str = "9:17",
    date: str = None,
    security_id: int = None,
    direction: str = "long",
):
    """PnL heatmap data for a specific contract."""
    conn = db.get_conn()

    # Get snapshot_id for this date + label + underlying
    snap = conn.execute("""
        SELECT snapshot_id, spot_price, expiry_date
        FROM snapshots
        WHERE underlying=? AND snapshot_label=? AND date=?
    """, (underlying, snapshot_label, date)).fetchone()

    if not snap:
        conn.close()
        return {"error": "Snapshot not found", "matrix": []}

    snapshot_id = snap["snapshot_id"]
    pnl_col = "projected_pnl_long" if direction == "long" else "projected_pnl_short"

    # If no security_id, pick ATM call
    if security_id is None:
        spot = snap["spot_price"]
        atm = conn.execute("""
            SELECT security_id FROM contract_states
            WHERE snapshot_id=? AND option_type='CE'
            ORDER BY ABS(strike - ?) ASC LIMIT 1
        """, (snapshot_id, spot)).fetchone()
        if atm:
            security_id = atm["security_id"]

    rows = conn.execute(f"""
        SELECT predicted_spot_pct, predicted_day, {pnl_col} as pnl, pricing_method
        FROM predictions
        WHERE snapshot_id=? AND security_id=?
        ORDER BY predicted_day, predicted_spot_pct
    """, (snapshot_id, security_id)).fetchall()

    # Get actual EOD dot if reconciled
    actual = conn.execute("""
        SELECT rp.predicted_spot_pct, rp.date,
               ea.actual_spot_pct,
               rp.actual_pnl_long, rp.actual_pnl_short
        FROM reconciled_predictions rp
        JOIN eod_actuals ea ON ea.date=rp.date AND ea.security_id=rp.security_id
        WHERE rp.snapshot_id=? AND rp.security_id=? AND rp.is_nearest_match=1
    """, (snapshot_id, security_id)).fetchall()

    # Get contract info
    contract = conn.execute("""
        SELECT strike, option_type, ltp, lot_size, invested_amount
        FROM contract_states WHERE snapshot_id=? AND security_id=?
    """, (snapshot_id, security_id)).fetchone()

    conn.close()
    return {
        "snapshot_id":  snapshot_id,
        "security_id":  security_id,
        "spot":         snap["spot_price"],
        "contract":     dict(contract) if contract else None,
        "matrix":       [dict(r) for r in rows],
        "actual_dots":  [dict(r) for r in actual],
    }


@app.get("/api/contracts")
def contracts(underlying: str = "NIFTY", snapshot_label: str = "9:17", date: str = None):
    """List contracts available for a snapshot."""
    conn = db.get_conn()
    snap = conn.execute("""
        SELECT snapshot_id FROM snapshots
        WHERE underlying=? AND snapshot_label=? AND date=?
    """, (underlying, snapshot_label, date)).fetchone()

    if not snap:
        conn.close()
        return []

    rows = conn.execute("""
        SELECT security_id, strike, option_type, ltp, iv, oi, invested_amount
        FROM contract_states WHERE snapshot_id=?
        ORDER BY strike, option_type
    """, (snap["snapshot_id"],)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/snapshot-accuracy")
def snapshot_accuracy(
    underlying: str = "NIFTY",
    option_type: str = None,
):
    """MAE per snapshot label — accuracy decay curve data."""
    conn = db.get_conn()
    filters, params = _build_filters(underlying, None, None, option_type)

    rows = conn.execute(f"""
        SELECT
            snapshot_label,
            date,
            AVG(ABS(error_long))  as mae_long,
            AVG(ABS(error_short)) as mae_short,
            COUNT(*)              as count
        FROM reconciled_predictions
        WHERE is_nearest_match=1 {filters}
        GROUP BY snapshot_label, date
        ORDER BY date, snapshot_label
    """, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/iv-shift")
def iv_shift(underlying: str = "NIFTY", date: str = None):
    """IV smile at each snapshot vs EOD (from nearest snapshot to close)."""
    conn = db.get_conn()

    snaps = conn.execute("""
        SELECT snapshot_id, snapshot_label
        FROM snapshots WHERE underlying=? AND date=?
        ORDER BY snapshot_id
    """, (underlying, date)).fetchall()

    result = {}
    for snap in snaps:
        rows = conn.execute("""
            SELECT strike, option_type, iv
            FROM contract_states WHERE snapshot_id=?
            ORDER BY strike
        """, (snap["snapshot_id"],)).fetchall()
        result[snap["snapshot_label"]] = [dict(r) for r in rows]

    conn.close()
    return result


@app.get("/api/insights")
def insights(underlying: str = "NIFTY", accuracy_threshold_pct: float = 0.10):
    """Auto-generated insight cards from reconciled data."""
    conn = db.get_conn()
    cards = []

    # Insight 1: Overall accuracy rate
    overall = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(within_threshold_long) as within,
            AVG(ABS(error_long)) as mae
        FROM reconciled_predictions
        WHERE underlying=? AND is_nearest_match=1
    """, (underlying,)).fetchone()

    if overall["total"] > 0:
        pct = round(overall["within"] / overall["total"] * 100, 1)
        cards.append({
            "type": "accuracy",
            "positive": pct >= 70,
            "title": f"{pct}% within threshold",
            "body": f"Across all snapshots and contracts, {pct}% of projected PnLs are within "
                    f"{accuracy_threshold_pct*100:.0f}% of invested amount. "
                    f"Mean absolute error: ₹{overall['mae']:.0f}.",
            "metric": pct,
        })

    # Insight 2: Best vs worst snapshot
    snap_perf = conn.execute("""
        SELECT snapshot_label,
               AVG(ABS(error_long)) as mae,
               COUNT(*) as cnt
        FROM reconciled_predictions
        WHERE underlying=? AND is_nearest_match=1
        GROUP BY snapshot_label ORDER BY mae ASC
    """, (underlying,)).fetchall()

    if len(snap_perf) >= 2:
        best  = snap_perf[0]
        worst = snap_perf[-1]
        ratio = round(worst["mae"] / best["mae"], 1) if best["mae"] else 0
        cards.append({
            "type": "snapshot_comparison",
            "positive": True,
            "title": f"{best['snapshot_label']} is {ratio}x more accurate than {worst['snapshot_label']}",
            "body": f"The {best['snapshot_label']} snapshot (MAE ₹{best['mae']:.0f}) "
                    f"outperforms {worst['snapshot_label']} (MAE ₹{worst['mae']:.0f}). "
                    f"Prediction accuracy improves significantly closer to close.",
            "metric": ratio,
        })

    # Insight 3: ATM vs OTM accuracy
    atm_otm = conn.execute("""
        SELECT
            CASE
                WHEN ABS(rp.strike - s.spot_price) / s.spot_price <= 0.01 THEN 'ATM'
                WHEN ABS(rp.strike - s.spot_price) / s.spot_price <= 0.03 THEN 'Near OTM'
                ELSE 'Far OTM'
            END as zone,
            AVG(ABS(error_long)) as mae,
            SUM(within_threshold_long) * 100.0 / COUNT(*) as accuracy_pct
        FROM reconciled_predictions rp
        JOIN snapshots s ON rp.snapshot_id = s.snapshot_id
        WHERE rp.underlying=? AND rp.is_nearest_match=1
        GROUP BY zone
    """, (underlying,)).fetchall()

    for row in atm_otm:
        if row["zone"] == "ATM" and row["accuracy_pct"]:
            cards.append({
                "type": "atm_accuracy",
                "positive": row["accuracy_pct"] >= 70,
                "title": f"ATM contracts: {row['accuracy_pct']:.0f}% accurate",
                "body": f"ATM options (within 1% of spot) meet the accuracy threshold "
                        f"{row['accuracy_pct']:.0f}% of the time with MAE ₹{row['mae']:.0f}. "
                        f"These are the safest contracts to surface to retail users first.",
                "metric": round(row["accuracy_pct"], 1),
            })

    # Insight 4: Expiry day BSM breakdown
    expiry_data = conn.execute("""
        SELECT
            pricing_method,
            AVG(ABS(error_long)) as mae,
            COUNT(*) as cnt
        FROM reconciled_predictions
        WHERE underlying=? AND is_nearest_match=1
        GROUP BY pricing_method
    """, (underlying,)).fetchall()

    methods = {r["pricing_method"]: r for r in expiry_data}
    if "BSM" in methods and "INTRINSIC" in methods:
        bsm_mae = methods["BSM"]["mae"]
        int_mae = methods["INTRINSIC"]["mae"]
        cards.append({
            "type": "pricing_method",
            "positive": int_mae <= bsm_mae,
            "title": f"Intrinsic pricing {'more' if int_mae <= bsm_mae else 'less'} accurate on expiry day",
            "body": f"BSM MAE: ₹{bsm_mae:.0f} | Intrinsic MAE: ₹{int_mae:.0f}. "
                    f"{'Intrinsic value correctly prices expiry-day contracts.' if int_mae <= bsm_mae else 'Expiry day still has pricing variance — early snapshots use BSM while time value remains.'}",
            "metric": round(int_mae, 2),
        })

    conn.close()
    return cards


def _build_filters(
    underlying: str = None,
    date: str = None,
    snapshot_label: str = None,
    option_type: str = None,
    params: list = None,
) -> tuple[str, list]:
    if params is None:
        params = []
    clauses = []
    if underlying:
        clauses.append("AND underlying=?")
        params.append(underlying)
    if date:
        clauses.append("AND date=?")
        params.append(date)
    if snapshot_label:
        clauses.append("AND snapshot_label=?")
        params.append(snapshot_label)
    if option_type and option_type != "BOTH":
        clauses.append("AND option_type=?")
        params.append(option_type)
    return " ".join(clauses), params


# Serve dashboard static files
# Use absolute path relative to this file — works regardless of working directory
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

@app.on_event("startup")
async def startup():
    if os.path.exists(STATIC_DIR):
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
        print(f"[Web] Static files served from {STATIC_DIR}")
    else:
        print(f"[Web] WARNING: static dir not found at {STATIC_DIR}")

@app.get("/")
def root():
    index = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(index):
        return {"status": "StreetBets running", "error": f"Dashboard not found at {index}", "static_dir": STATIC_DIR, "files": os.listdir(os.path.dirname(STATIC_DIR)) if os.path.exists(os.path.dirname(STATIC_DIR)) else []}
    return FileResponse(index)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
