"""
StreetBets - Scheduler Entry Point
APScheduler with IST timezone.
Snapshot jobs: 9:17, 10:30, 12:15, 13:30, 15:00
Reconcile job: 15:35
Only runs on trading days (Mon–Fri).
"""

import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

import db
import dhan
import snapshot
import reconcile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.environ.get("LOG_PATH", "/data/streetbets.log")),
    ]
)
logger = logging.getLogger("main")
IST = ZoneInfo("Asia/Kolkata")


def is_trading_day_now() -> bool:
    return dhan.is_trading_day(datetime.now(IST).date())


def snapshot_job():
    if not is_trading_day_now():
        logger.info("[Scheduler] Skipping snapshot — not a trading day")
        return
    now = datetime.now(IST)
    logger.info(f"[Scheduler] Snapshot job triggered at {now.strftime('%H:%M IST')}")
    results = snapshot.run_all_snapshots(now)
    logger.info(f"[Scheduler] Snapshot complete: {results}")


def reconcile_job():
    if not is_trading_day_now():
        logger.info("[Scheduler] Skipping reconcile — not a trading day")
        return
    logger.info("[Scheduler] EOD reconciliation triggered")
    results = reconcile.reconcile_all()
    logger.info(f"[Scheduler] Reconcile complete: {results}")


def main():
    logger.info("=" * 60)
    logger.info("StreetBets starting up")
    logger.info("=" * 60)

    # Init DB on startup
    db.init_db()

    # Mark experiment start if first run
    start = db.get_config("experiment_start")
    if not start:
        db.set_config("experiment_start", datetime.now(IST).date().isoformat())
        logger.info(f"[Main] Experiment started: {db.get_config('experiment_start')}")

    scheduler = BlockingScheduler(timezone="Asia/Kolkata")

    # Snapshot jobs — IST cron times
    snapshot_times = [
        (9, 17),
        (10, 30),
        (12, 15),
        (13, 30),
        (15, 0),
    ]
    for hour, minute in snapshot_times:
        scheduler.add_job(
            snapshot_job,
            CronTrigger(hour=hour, minute=minute, timezone="Asia/Kolkata"),
            id=f"snapshot_{hour:02d}{minute:02d}",
            name=f"Snapshot {hour}:{minute:02d} IST",
            misfire_grace_time=120,  # 2 min tolerance
        )
        logger.info(f"[Scheduler] Registered snapshot job at {hour}:{minute:02d} IST")

    # EOD reconciliation job
    scheduler.add_job(
        reconcile_job,
        CronTrigger(hour=15, minute=35, timezone="Asia/Kolkata"),
        id="eod_reconcile",
        name="EOD Reconciliation 15:35 IST",
        misfire_grace_time=300,
    )
    logger.info("[Scheduler] Registered EOD reconciliation at 15:35 IST")

    logger.info("[Scheduler] Starting — waiting for market hours...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[Scheduler] Shutting down gracefully")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
