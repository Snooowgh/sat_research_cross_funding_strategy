import signal
import sys
from loguru import logger
from arbitrage_info_show import notify_arbitrage_info
from multi_exchange_info_show import notify_multi_exchange_arbitrage_info
from utils.decorators import async_me
from utils.notify_tools import init_logger, send_slack_message
from utils.schedule_tool import get_scheduler
from utils.time_utils import get_datetime_now_str


@async_me
def async_task_runner():
    logger.info("启动所有异步任务..")


def on_exit(signum, frame):
    logger.info(f"🚨 程序被终止")

    sys.exit(0)


def schedule_pending():
    # 套利分钟级更新
    scheduler = get_scheduler()
    # scheduler.add_job(notify_arbitrage_info,
    #                   trigger="cron",
    #                   minute="*/15",
    #                   second=0,
    #                   name=f"Lighter Arbitrage")

    scheduler.add_job(notify_multi_exchange_arbitrage_info,
                      trigger="cron",
                      minute="*/5",
                      second=0,
                      name=f"Multi Arbitrage")

    # notify_arbitrage_info()  # 启动时立即运行一次
    notify_multi_exchange_arbitrage_info()
    logger.info("👍 darwin_light定时任务部署完毕...")
    scheduler.start()


if __name__ == '__main__':
    project_name = "darwin_light"
    signal.signal(signal.SIGTERM, on_exit)
    init_logger("./logs")
    logger.info(f"{project_name} 启动...")
    send_slack_message(f"✅ {project_name}部署成功...\n" + get_datetime_now_str())
    async_task_runner()
    try:
        schedule_pending()
    except KeyboardInterrupt:
        logger.info(f"{project_name}程序结束...")
