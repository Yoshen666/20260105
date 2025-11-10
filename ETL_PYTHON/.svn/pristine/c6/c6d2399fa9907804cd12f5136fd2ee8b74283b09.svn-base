import logging
from datetime import datetime, timedelta

from apscheduler import events
from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler

from xinxiang import config
from xinxiang.jobs_etl import etl_cron
from xinxiang.util import my_file, my_date, my_cron


def handle_job_exception(event):
    logging.info("Job处理错误 - {job}：{traceback_str}".format(job=event.job_id, traceback_str=event.exception))


def main():
    '''
    这里编写所有JOB注册的代码
    :return:
    '''
    my_file.init_folder()

    logging.info("ELT JOB Runing...")

    schedule = BlockingScheduler({
        'apscheduler.executors.default': {
            'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
            'max_workers': '200'
        },
        'apscheduler.job_defaults.coalesce': True,
    })

    # Etl
    etl_cron.set_etl_cron(schedule)

    schedule.add_listener(handle_job_exception, events.EVENT_JOB_ERROR)

    schedule.start()