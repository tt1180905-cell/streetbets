"""
StreetBets - Snapshot Job
Runs at 9:17, 10:30, 12:15, 13:30, 15:00 IST.
For each underlying: fetches chain, stores contracts, generates full prediction matrix.
"""

import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import db
import dhan
import pricing

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

SNAPSHOT_LABELS = {
    (9, 17): "9:17",
    (10, 30): "10:30",
    (12, 15): "12:15",
    (13, 30): "1:30",
    (15, 0): "3:00",
}

SPOT_RANGE = 0.05  # ±5% contract filter


def _prediction_pcts() -> list[float]:
    """
    Variable-density prediction grid:
      ±0–1%   : 0.05% steps  (41 points)
      ±1–3%   : 0.10% steps  (40 points)
      ±3–5%   : 0.25% steps  (16 points)
    Total: 97 unique points
    """
    pcts = set()

    def add_range(start, stop, step):
        v = start
        while v <= stop + 1e-9:
            pcts.add(round(v, 4))
            v = round(v + step, 4)

    # Core: -1% to +1% at 0.05%
    add_range(-0.01, 0.01, 0.0005)

    # Mid: -3% to -1% and +1% to +3% at 0.1%
    add_range(-0.03, -0.01, 0.001)
    add_range( 0.01,  0.03, 0.001)

    # Outer: -5% to -3% and +3% to +5% at 0.25%
    add_range(-0.05, -0.03, 0.0025)
    add_range( 0.03,  0.05, 0.0025)

    return sorted(pcts)


def run_snapshot(underlying: str, now: datetime = None) -> int:
    """
    Execute a full snapshot for one underlying.
    Returns snapshot_id on success, -1 on failure.
    """
    if now is None:
        now = datetime.now(IST)

    label = SNAPSHOT_LABELS.get((now.hour, now.minute))
    if label is None:
        # Find closest label
        label = f"{now.hour}:{now.minute:02d}"

    today       = now.date()
    today_str   = today.isoformat()
    ts_str      = now.isoformat()

    logger.info(f"[Snapshot] {underlying} | {label} | {today_str}")

    # ── 1. Fetch nearest expiry ───────────────────────────────────────────────
    try:
        expiry_str = dhan.get_nearest_expiry(underlying)
        if not expiry_str:
            logger.error(f"[Snapshot] No active expiry found for {underlying}")
            return -1
        expiry_date = date.fromisoformat(expiry_str)
    except Exception as e:
        logger.error(f"[Snapshot] Expiry fetch failed for {underlying}: {e}")
        return -1

    is_expiry_day   = (today == expiry_date)
    trading_days    = dhan.get_trading_days_between(today, expiry_date)
    dte_calendar    = (expiry_date - today).days
    dte_trading     = len(trading_days)

    # ── 2. Fetch option chain ─────────────────────────────────────────────────
    import time
    try:
        raw   = dhan.get_option_chain(underlying, expiry_str)
        time.sleep(3.1)  # respect rate limit
        chain = dhan.parse_chain(underlying, expiry_str, raw, SPOT_RANGE)
    except Exception as e:
        logger.error(f"[Snapshot] Chain fetch failed for {underlying}: {e}")
        return -1

    spot      = chain["spot"]
    contracts = chain["contracts"]
    if not contracts:
        logger.warning(f"[Snapshot] No contracts returned for {underlying}")
        return -1

    conn = db.get_conn()
    try:
        c = conn.cursor()

        # ── 3. Insert snapshot row ────────────────────────────────────────────
        c.execute("""
            INSERT OR REPLACE INTO snapshots
            (ts, date, snapshot_label, underlying, spot_price,
             expiry_date, dte, trading_days_to_expiry, is_expiry_day)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (ts_str, today_str, label, underlying, spot,
              expiry_str, dte_calendar, dte_trading, int(is_expiry_day)))
        snapshot_id = c.lastrowid

        # ── 4. Insert contract states ─────────────────────────────────────────
        state_map = {}  # security_id -> state_id
        for con in contracts:
            c.execute("""
                INSERT OR REPLACE INTO contract_states
                (snapshot_id, underlying, security_id, strike, option_type,
                 ltp, iv, bid, ask, oi, volume, delta, theta, gamma, vega,
                 lot_size, invested_amount)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                snapshot_id, underlying, con["security_id"], con["strike"], con["option_type"],
                con["ltp"], con["iv"], con["bid"], con["ask"], con["oi"], con["volume"],
                con["delta"], con["theta"], con["gamma"], con["vega"],
                con["lot_size"], con["invested_amount"],
            ))
            state_map[con["security_id"]] = c.lastrowid

        conn.commit()

        # ── 5. Generate prediction matrix ─────────────────────────────────────
        prediction_pcts = _prediction_pcts()
        rows_inserted   = 0

        for pred_day in trading_days:
            pred_day_str   = pred_day.isoformat()
            days_offset    = trading_days.index(pred_day)
            is_expiry_pred = (pred_day == expiry_date)

            # T is fixed per prediction day — compute once outside contract loop
            # Uses calendar convention matching Dhan's IV calibration
            t_years = pricing.t_years_for_prediction(pred_day, expiry_date)

            for pct in prediction_pcts:
                predicted_spot = round(spot * (1 + pct), 2)

                # IV for this predicted spot — also fixed per pct, compute once
                iv_ce = pricing.select_iv_for_predicted_spot(predicted_spot, "CE", contracts)
                iv_pe = pricing.select_iv_for_predicted_spot(predicted_spot, "PE", contracts)

                for con in contracts:
                    iv_to_use = iv_ce if con["option_type"] == "CE" else iv_pe

                    # Compute projected LTP
                    if is_expiry_pred:
                        result = pricing.intrinsic_price(
                            con["option_type"], predicted_spot, con["strike"]
                        )
                    else:
                        result = pricing.bsm_price(
                            con["option_type"],
                            predicted_spot,
                            con["strike"],
                            iv_to_use or con["iv"] or 0.15,
                            t_years,
                        )

                    pnl_long, pnl_short = pricing.compute_pnl(
                        result.projected_ltp, con["ltp"], con["lot_size"]
                    )
                    threshold = con["invested_amount"] * float(
                        db.get_config("accuracy_threshold_pct") or 0.10
                    )

                    c.execute("""
                        INSERT OR REPLACE INTO predictions
                        (snapshot_id, state_id, security_id, underlying, option_type,
                         strike, predicted_spot_pct, predicted_spot, predicted_day,
                         days_offset, pricing_method, projected_ltp,
                         projected_pnl_long, projected_pnl_short, accuracy_threshold)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        snapshot_id,
                        state_map.get(con["security_id"]),
                        con["security_id"],
                        underlying,
                        con["option_type"],
                        con["strike"],
                        pct,
                        predicted_spot,
                        pred_day_str,
                        days_offset,
                        result.method,
                        result.projected_ltp,
                        pnl_long,
                        pnl_short,
                        threshold,
                    ))
                    rows_inserted += 1

                    # Batch commit every 5000 rows
                    if rows_inserted % 5000 == 0:
                        conn.commit()
                        logger.debug(f"[Snapshot] {rows_inserted} prediction rows committed")

        conn.commit()
        logger.info(
            f"[Snapshot] {underlying} {label} complete | "
            f"snapshot_id={snapshot_id} | "
            f"{len(contracts)} contracts | "
            f"{rows_inserted} predictions"
        )
        return snapshot_id

    except Exception as e:
        conn.rollback()
        logger.error(f"[Snapshot] DB error for {underlying}: {e}", exc_info=True)
        return -1
    finally:
        conn.close()


def run_all_snapshots(now: datetime = None) -> dict:
    """Run snapshot for all underlyings. Returns results dict."""
    if now is None:
        now = datetime.now(IST)
    results = {}
    for underlying in ["NIFTY", "SENSEX"]:
        import time
        results[underlying] = run_snapshot(underlying, now)
        time.sleep(3.5)  # rate limit buffer between underlyings
    return results


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    db.init_db()
    results = run_all_snapshots()
    print(f"Snapshot results: {results}")
