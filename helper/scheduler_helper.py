from apscheduler.schedulers.background import BackgroundScheduler
from config import SCHEDULE_CRON, SCHEDULE_INTERVAL_HOURS, SCHEDULE_TYPE
from logger import log, log_step

def schedule_bulk(bulk_callable):
    with log_step('Scheduling bulk ingestion'):
        scheduler = BackgroundScheduler()
        if SCHEDULE_TYPE == 'cron':
            from apscheduler.triggers.cron import CronTrigger
            scheduler.add_job(bulk_callable, CronTrigger.from_crontab(SCHEDULE_CRON))
            log.info(f'Scheduled bulk ingestion with CRON: {SCHEDULE_CRON}')
        else:
            scheduler.add_job(bulk_callable, 'interval', hours=SCHEDULE_INTERVAL_HOURS)
            log.info(f'Scheduled bulk ingestion every {SCHEDULE_INTERVAL_HOURS} hours')
        scheduler.start()
