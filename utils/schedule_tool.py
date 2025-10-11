# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : schedule_tool
@Description :
@Time        : 2023/11/27 23:41
"""
from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger
from apscheduler.executors.pool import ThreadPoolExecutor
import pytz
from apscheduler.events import EVENT_JOB_ERROR
import logging


def get_scheduler():
    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.ERROR)
    executors = {
        "thread_pool": ThreadPoolExecutor(100),
    }
    scheduler = BlockingScheduler(executors=executors,
                                  timezone=pytz.timezone("Asia/Shanghai"))

    def listener(event):
        err = event.exception
        if err:
            logger.exception(err)

    scheduler.add_listener(listener, EVENT_JOB_ERROR)
    return scheduler
