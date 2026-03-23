from apscheduler.schedulers.background import BackgroundScheduler

from logger import log, log_step

from config import SCHEDULE_TYPE, SCHEDULE_CRON, SCHEDULE_INTERVAL_HOURS


def schedule_bulk(bulk_callable):
    """Schedule bulk ingestion.

    The callable is invoked with no args, matching previous lambda usage.
    """

    with log_step("Scheduling bulk ingestion"):
        scheduler = BackgroundScheduler()
        if SCHEDULE_TYPE == "cron":
            from apscheduler.triggers.cron import CronTrigger

            scheduler.add_job(
                bulk_callable, CronTrigger.from_crontab(SCHEDULE_CRON)
            )
            log.info(f"Scheduled bulk ingestion with CRON: {SCHEDULE_CRON}")
        else:
            scheduler.add_job(
                bulk_callable, "interval", hours=SCHEDULE_INTERVAL_HOURS
            )
            log.info(f"Scheduled bulk ingestion every {SCHEDULE_INTERVAL_HOURS} hours")
        scheduler.start()
