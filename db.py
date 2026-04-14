"""
StreetBets - Database Setup
SQLite schema for snapshots, predictions, and EOD reconciliation.
"""

import sqlite3
import os
from pathlib import Path

DB_PATH = os.environ.get("DB_PATH", "/data/streetbets.db")


def get_conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    # ── snapshots ──────────────────────────────────────────────────────────────
    # One row per (time, underlying). Captures market state at snapshot moment.
    c.executescript("""
    CREATE TABLE IF NOT EXISTS snapshots (
        snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        ts              TEXT    NOT NULL,           -- ISO8601 IST e.g. 2025-04-17T09:17:00+05:30
        date            TEXT    NOT NULL,           -- YYYY-MM-DD trading date
        snapshot_label  TEXT    NOT NULL,           -- '9:17' | '10:30' | '12:15' | '1:30' | '3:00'
        underlying      TEXT    NOT NULL,           -- 'NIFTY' | 'SENSEX'
        spot_price      REAL    NOT NULL,
        expiry_date     TEXT    NOT NULL,           -- nearest expiry YYYY-MM-DD
        dte             INTEGER NOT NULL,           -- calendar days to expiry
        trading_days_to_expiry INTEGER NOT NULL,   -- trading days remaining incl today
        is_expiry_day   INTEGER NOT NULL DEFAULT 0,
        UNIQUE(date, snapshot_label, underlying)
    );

    -- ── contract_states ────────────────────────────────────────────────────────
    -- One row per contract per snapshot. Raw market data from Dhan Option Chain.
    CREATE TABLE IF NOT EXISTS contract_states (
        state_id        INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id     INTEGER NOT NULL REFERENCES snapshots(snapshot_id),
        underlying      TEXT    NOT NULL,
        security_id     INTEGER NOT NULL,
        strike          REAL    NOT NULL,
        option_type     TEXT    NOT NULL,           -- 'CE' | 'PE'
        ltp             REAL    NOT NULL,           -- entry price for PnL calc
        iv              REAL,                       -- implied volatility (decimal, e.g. 0.12)
        bid             REAL,
        ask             REAL,
        oi              INTEGER,
        volume          INTEGER,
        delta           REAL,
        theta           REAL,
        gamma           REAL,
        vega            REAL,
        lot_size        INTEGER NOT NULL,
        invested_amount REAL    NOT NULL,           -- ltp * lot_size
        UNIQUE(snapshot_id, security_id)
    );

    -- ── predictions ───────────────────────────────────────────────────────────
    -- One row per (snapshot × contract × predicted_spot_pct × predicted_day).
    -- This is the bulk table. All PnL projections live here.
    CREATE TABLE IF NOT EXISTS predictions (
        prediction_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id         INTEGER NOT NULL REFERENCES snapshots(snapshot_id),
        state_id            INTEGER NOT NULL REFERENCES contract_states(state_id),
        security_id         INTEGER NOT NULL,
        underlying          TEXT    NOT NULL,
        option_type         TEXT    NOT NULL,
        strike              REAL    NOT NULL,
        predicted_spot_pct  REAL    NOT NULL,       -- e.g. -0.04 = -4% from spot
        predicted_spot      REAL    NOT NULL,       -- absolute price
        predicted_day       TEXT    NOT NULL,       -- YYYY-MM-DD (trading day)
        days_offset         INTEGER NOT NULL,       -- 0=today, 1=tomorrow, etc.
        pricing_method      TEXT    NOT NULL,       -- 'BSM' | 'INTRINSIC'
        projected_ltp       REAL    NOT NULL,       -- theoretical price at prediction
        projected_pnl_long  REAL    NOT NULL,       -- (projected_ltp - entry_ltp) * lot
        projected_pnl_short REAL    NOT NULL,       -- (entry_ltp - projected_ltp) * lot
        accuracy_threshold  REAL    NOT NULL,       -- 10% of invested_amount (default)
        UNIQUE(snapshot_id, security_id, predicted_spot_pct, predicted_day)
    );

    -- ── eod_actuals ───────────────────────────────────────────────────────────
    -- One row per (trading_date × underlying × contract).
    -- Filled by EOD reconciliation job at 3:35 IST.
    CREATE TABLE IF NOT EXISTS eod_actuals (
        actual_id               INTEGER PRIMARY KEY AUTOINCREMENT,
        date                    TEXT    NOT NULL,   -- YYYY-MM-DD the day being reconciled
        underlying              TEXT    NOT NULL,
        security_id             INTEGER NOT NULL,
        strike                  REAL    NOT NULL,
        option_type             TEXT    NOT NULL,
        actual_close_ltp        REAL    NOT NULL,   -- closing LTP from Dhan
        actual_spot_close       REAL    NOT NULL,   -- underlying close price
        actual_spot_pct         REAL    NOT NULL,   -- % move from each snapshot's spot
        UNIQUE(date, security_id)
    );

    -- ── reconciled_predictions ────────────────────────────────────────────────
    -- Joins predictions to actuals. One row per prediction that has been reconciled.
    -- 'nearest' prediction to actual spot is flagged with is_nearest_match=1.
    CREATE TABLE IF NOT EXISTS reconciled_predictions (
        reconcile_id            INTEGER PRIMARY KEY AUTOINCREMENT,
        date                    TEXT    NOT NULL,
        snapshot_id             INTEGER NOT NULL REFERENCES snapshots(snapshot_id),
        snapshot_label          TEXT    NOT NULL,
        underlying              TEXT    NOT NULL,
        security_id             INTEGER NOT NULL,
        strike                  REAL    NOT NULL,
        option_type             TEXT    NOT NULL,
        lot_size                INTEGER NOT NULL,
        entry_ltp               REAL    NOT NULL,   -- from contract_states
        invested_amount         REAL    NOT NULL,
        predicted_spot_pct      REAL    NOT NULL,
        projected_ltp           REAL    NOT NULL,
        projected_pnl_long      REAL    NOT NULL,
        projected_pnl_short     REAL    NOT NULL,
        actual_close_ltp        REAL    NOT NULL,
        actual_pnl_long         REAL    NOT NULL,   -- (actual_close - entry) * lot
        actual_pnl_short        REAL    NOT NULL,   -- (entry - actual_close) * lot
        error_long              REAL    NOT NULL,   -- projected_long - actual_long
        error_short             REAL    NOT NULL,
        pct_error_long          REAL    NOT NULL,   -- abs(error) / invested_amount
        pct_error_short         REAL    NOT NULL,
        accuracy_threshold_pct  REAL    NOT NULL DEFAULT 0.10,
        within_threshold_long   INTEGER NOT NULL,   -- 1 if pct_error <= threshold
        within_threshold_short  INTEGER NOT NULL,
        pricing_method          TEXT    NOT NULL,
        is_nearest_match        INTEGER NOT NULL DEFAULT 0,
        UNIQUE(date, snapshot_id, security_id, predicted_spot_pct)
    );

    -- ── experiment_config ─────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS experiment_config (
        key     TEXT PRIMARY KEY,
        value   TEXT NOT NULL,
        updated TEXT NOT NULL
    );

    -- ── indexes ───────────────────────────────────────────────────────────────
    CREATE INDEX IF NOT EXISTS idx_snapshots_date       ON snapshots(date, underlying);
    CREATE INDEX IF NOT EXISTS idx_states_snapshot      ON contract_states(snapshot_id);
    CREATE INDEX IF NOT EXISTS idx_states_security      ON contract_states(security_id);
    CREATE INDEX IF NOT EXISTS idx_predictions_snapshot ON predictions(snapshot_id);
    CREATE INDEX IF NOT EXISTS idx_predictions_security ON predictions(security_id, predicted_day);
    CREATE INDEX IF NOT EXISTS idx_reconciled_date      ON reconciled_predictions(date, underlying);
    CREATE INDEX IF NOT EXISTS idx_reconciled_snapshot  ON reconciled_predictions(snapshot_id);
    CREATE INDEX IF NOT EXISTS idx_eod_date             ON eod_actuals(date, underlying);
    """)

    # Seed default config
    defaults = {
        "accuracy_threshold_pct": "0.10",
        "nifty_lot_size": "65",
        "sensex_lot_size": "20",
        "spot_range_pct": "0.05",
        "prediction_interval_pct": "0.002",
        "snapshot_times_ist": "09:17,10:30,12:15,13:30,15:00",
        "eod_reconcile_time_ist": "15:35",
        "experiment_start": "",
    }
    for k, v in defaults.items():
        c.execute("""
            INSERT INTO experiment_config(key, value, updated)
            VALUES(?, ?, datetime('now'))
            ON CONFLICT(key) DO NOTHING
        """, (k, v))

    conn.commit()
    conn.close()
    print(f"[DB] Initialized at {DB_PATH}")


def get_config(key: str) -> str:
    conn = get_conn()
    row = conn.execute(
        "SELECT value FROM experiment_config WHERE key=?", (key,)
    ).fetchone()
    conn.close()
    return row["value"] if row else None


def set_config(key: str, value: str):
    conn = get_conn()
    conn.execute("""
        INSERT INTO experiment_config(key, value, updated)
        VALUES(?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated=excluded.updated
    """, (key, value))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
