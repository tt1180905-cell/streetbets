"""
StreetBets - Combined Entry Point
Runs APScheduler (background thread) + FastAPI (web server) in one process.
Single Railway service, single volume at /data.

Scheduler: 9:17, 10:30, 12:15, 13:30, 15:00 IST snapshots + 15:35 EOD reconcile
Web:       FastAPI dashboard on $PORT
"""

import logging
import os
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import uvicorn

import db
import dhan
import snapshot
import reconcile
from api import app

# ── Logging ───────────────────────────────────────────────────────────────────
log_path = os.environ.get("LOG_PATH", "/data/streetbets.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_path),
    ]
)
logger = logging.getLogger("main")
IST = ZoneInfo("Asia/Kolkata")


# ── Scheduler jobs ────────────────────────────────────────────────────────────
def is_trading_day_now() -> bool:
    return dhan.is_trading_day(datetime.now(IST).date())


def snapshot_job():
    if not is_trading_day_now():
        logger.info("[Scheduler] Skipping snapshot — not a trading day")
        return
    now = datetime.now(IST)
    logger.info(f"[Scheduler] Snapshot triggered at {now.strftime('%H:%M IST')}")
    results = snapshot.run_all_snapshots(now)
    logger.info(f"[Scheduler] Snapshot complete: {results}")


def reconcile_job():
    if not is_trading_day_now():
        logger.info("[Scheduler] Skipping reconcile — not a trading day")
        return
    logger.info("[Scheduler] EOD reconciliation triggered")
    results = reconcile.reconcile_all()
    logger.info(f"[Scheduler] Reconcile complete: {results}")


def start_scheduler():
    """Start APScheduler in background thread."""
    scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

    snapshot_times = [(9, 17), (10, 30), (12, 15), (13, 30), (15, 0)]
    for hour, minute in snapshot_times:
        scheduler.add_job(
            snapshot_job,
            CronTrigger(hour=hour, minute=minute, timezone="Asia/Kolkata"),
            id=f"snapshot_{hour:02d}{minute:02d}",
            name=f"Snapshot {hour}:{minute:02d} IST",
            misfire_grace_time=120,
        )
        logger.info(f"[Scheduler] Registered snapshot at {hour}:{minute:02d} IST")

    scheduler.add_job(
        reconcile_job,
        CronTrigger(hour=15, minute=35, timezone="Asia/Kolkata"),
        id="eod_reconcile",
        name="EOD Reconcile 15:35 IST",
        misfire_grace_time=300,
    )
    logger.info("[Scheduler] Registered EOD reconcile at 15:35 IST")

    scheduler.start()
    logger.info("[Scheduler] Running in background — waiting for market hours")
    return scheduler


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("  StreetBets starting up")
    logger.info("=" * 60)

    # Init DB
    db.init_db()

    # Mark experiment start
    if not db.get_config("experiment_start"):
        db.set_config("experiment_start", datetime.now(IST).date().isoformat())
        logger.info(f"[Main] Experiment started: {db.get_config('experiment_start')}")

    # Start scheduler in background
    start_scheduler()

    # Start FastAPI in main thread (blocks here)
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"[Web] Starting dashboard on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")


if __name__ == "__main__":
    main()
