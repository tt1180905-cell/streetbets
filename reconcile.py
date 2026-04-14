"""
StreetBets - EOD Reconciliation Job
Runs at 15:35 IST daily.
For each underlying: fetches closing prices, finds nearest predictions,
computes actual vs projected PnL, writes reconciled_predictions.
"""

import logging
from datetime import datetime, date
from zoneinfo import ZoneInfo

import db
import dhan

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def reconcile(underlying: str, reconcile_date: date = None) -> int:
    """
    Reconcile all snapshots for `underlying` on `reconcile_date`.
    Returns count of reconciled prediction rows.
    """
    if reconcile_date is None:
        reconcile_date = date.today()
    date_str = reconcile_date.isoformat()

    logger.info(f"[Reconcile] Starting {underlying} for {date_str}")

    conn = db.get_conn()
    try:
        c = conn.cursor()

        # ── 1. Get all snapshots for this date and underlying ─────────────────
        snapshots = c.execute("""
            SELECT snapshot_id, snapshot_label, spot_price, expiry_date
            FROM snapshots
            WHERE date=? AND underlying=?
            ORDER BY snapshot_id
        """, (date_str, underlying)).fetchall()

        if not snapshots:
            logger.warning(f"[Reconcile] No snapshots found for {underlying} {date_str}")
            return 0

        expiry_str = snapshots[0]["expiry_date"]

        # ── 2. Get all security_ids traded today ──────────────────────────────
        security_ids = [
            row["security_id"] for row in c.execute("""
                SELECT DISTINCT cs.security_id
                FROM contract_states cs
                JOIN snapshots s ON cs.snapshot_id = s.snapshot_id
                WHERE s.date=? AND s.underlying=?
            """, (date_str, underlying)).fetchall()
        ]

        if not security_ids:
            logger.warning(f"[Reconcile] No contracts found for {underlying} {date_str}")
            return 0

        # ── 3. Fetch actual EOD data from Dhan ────────────────────────────────
        eod_data = dhan.get_eod_ltp(underlying, expiry_str, security_ids)
        actual_spot = eod_data.get("spot")
        actual_ltps = eod_data.get("ltps", {})

        if actual_spot is None:
            logger.error(f"[Reconcile] Failed to get EOD data for {underlying}")
            return 0

        logger.info(f"[Reconcile] {underlying} EOD spot: {actual_spot} | {len(actual_ltps)} contract LTPs")

        # ── 4. Store eod_actuals ──────────────────────────────────────────────
        for snapshot in snapshots:
            snapshot_spot = snapshot["spot_price"]
            actual_spot_pct = round((actual_spot - snapshot_spot) / snapshot_spot, 6)

            # Get unique contracts for this snapshot
            contracts_for_snap = c.execute("""
                SELECT security_id, strike, option_type, lot_size
                FROM contract_states
                WHERE snapshot_id=?
            """, (snapshot["snapshot_id"],)).fetchall()

            for con in contracts_for_snap:
                sid = con["security_id"]
                actual_ltp = actual_ltps.get(sid)
                if actual_ltp is None:
                    continue

                c.execute("""
                    INSERT OR REPLACE INTO eod_actuals
                    (date, underlying, security_id, strike, option_type,
                     actual_close_ltp, actual_spot_close, actual_spot_pct)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (
                    date_str, underlying, sid, con["strike"], con["option_type"],
                    actual_ltp, actual_spot, actual_spot_pct,
                ))

        conn.commit()

        # ── 5. Reconcile predictions ──────────────────────────────────────────
        total_reconciled = 0
        threshold_pct = float(db.get_config("accuracy_threshold_pct") or 0.10)

        for snapshot in snapshots:
            snapshot_id    = snapshot["snapshot_id"]
            snapshot_label = snapshot["snapshot_label"]
            snapshot_spot  = snapshot["spot_price"]
            actual_spot_pct = round((actual_spot - snapshot_spot) / snapshot_spot, 6)

            # Find nearest predicted_spot_pct to actual move
            pcts = c.execute("""
                SELECT DISTINCT predicted_spot_pct FROM predictions
                WHERE snapshot_id=? AND predicted_day=?
                ORDER BY ABS(predicted_spot_pct - ?) ASC
                LIMIT 1
            """, (snapshot_id, date_str, actual_spot_pct)).fetchone()

            if not pcts:
                continue
            nearest_pct = pcts["predicted_spot_pct"]

            # Get all predictions for this snapshot × today × nearest pct
            preds = c.execute("""
                SELECT p.*, cs.ltp as entry_ltp, cs.lot_size, cs.invested_amount,
                       cs.strike, cs.option_type
                FROM predictions p
                JOIN contract_states cs ON p.state_id = cs.state_id
                WHERE p.snapshot_id=? AND p.predicted_day=?
            """, (snapshot_id, date_str)).fetchall()

            for pred in preds:
                sid          = pred["security_id"]
                actual_ltp   = actual_ltps.get(sid)
                if actual_ltp is None:
                    continue

                entry_ltp       = pred["entry_ltp"]
                lot_size        = pred["lot_size"]
                invested_amount = pred["invested_amount"]
                is_nearest      = int(abs(pred["predicted_spot_pct"] - nearest_pct) < 1e-6)

                actual_pnl_long  = round((actual_ltp - entry_ltp) * lot_size, 2)
                actual_pnl_short = round((entry_ltp - actual_ltp) * lot_size, 2)

                error_long  = round(pred["projected_pnl_long"]  - actual_pnl_long,  2)
                error_short = round(pred["projected_pnl_short"] - actual_pnl_short, 2)

                pct_error_long  = round(abs(error_long)  / invested_amount, 6) if invested_amount else 0
                pct_error_short = round(abs(error_short) / invested_amount, 6) if invested_amount else 0

                within_long  = int(pct_error_long  <= threshold_pct)
                within_short = int(pct_error_short <= threshold_pct)

                c.execute("""
                    INSERT OR REPLACE INTO reconciled_predictions
                    (date, snapshot_id, snapshot_label, underlying, security_id,
                     strike, option_type, lot_size, entry_ltp, invested_amount,
                     predicted_spot_pct, projected_ltp, projected_pnl_long, projected_pnl_short,
                     actual_close_ltp, actual_pnl_long, actual_pnl_short,
                     error_long, error_short, pct_error_long, pct_error_short,
                     accuracy_threshold_pct, within_threshold_long, within_threshold_short,
                     pricing_method, is_nearest_match)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    date_str, snapshot_id, snapshot_label, underlying, sid,
                    pred["strike"], pred["option_type"], lot_size, entry_ltp, invested_amount,
                    pred["predicted_spot_pct"], pred["projected_ltp"],
                    pred["projected_pnl_long"], pred["projected_pnl_short"],
                    actual_ltp, actual_pnl_long, actual_pnl_short,
                    error_long, error_short, pct_error_long, pct_error_short,
                    threshold_pct, within_long, within_short,
                    pred["pricing_method"], is_nearest,
                ))
                total_reconciled += 1

            conn.commit()
            logger.info(
                f"[Reconcile] {underlying} {snapshot_label}: "
                f"{total_reconciled} rows | nearest_pct={nearest_pct:.4f} | "
                f"actual_move={actual_spot_pct:.4f}"
            )

        logger.info(f"[Reconcile] {underlying} {date_str} complete: {total_reconciled} total rows")
        return total_reconciled

    except Exception as e:
        conn.rollback()
        logger.error(f"[Reconcile] Error for {underlying} {date_str}: {e}", exc_info=True)
        return 0
    finally:
        conn.close()


def reconcile_all(reconcile_date: date = None) -> dict:
    """Run reconciliation for all underlyings."""
    if reconcile_date is None:
        reconcile_date = date.today()
    results = {}
    for underlying in ["NIFTY", "SENSEX"]:
        results[underlying] = reconcile(underlying, reconcile_date)
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    db.init_db()
    results = reconcile_all()
    print(f"Reconciliation results: {results}")
