# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : time_utils
@Description :
@Time        : 2024/9/4 08:54
"""
import datetime

import pytz


def format_datetime_delta(delta):
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days > 0:
        formatted_string = f"{days} days, {hours:02}:{minutes:02}:{seconds:02}"
    elif hours > 0:
        formatted_string = f"{hours:02}:{minutes:02}:{seconds:02}"
    else:
        formatted_string = f"{minutes:02}:{seconds:02}"
    return formatted_string


def get_datetime_now_str(datetime_obj=None):
    utc8 = pytz.timezone("Asia/Shanghai")
    if datetime_obj is None:
        now = datetime.datetime.now().astimezone(utc8)
    else:
        if isinstance(datetime_obj, int) or isinstance(datetime_obj, float):
            now = datetime.datetime.fromtimestamp(datetime_obj).astimezone(utc8)
        else:
            now = datetime_obj.astimezone(utc8)
    return now.strftime('%Y-%m-%d %H:%M:%S')


def get_utc8_now():
    # 转换为 UTC+8 时区时间
    utc8 = pytz.timezone("Asia/Shanghai")
    now = datetime.datetime.now().astimezone(utc8)
    return now


def is_within_time_window(target_hours=None, time_window=1, before=True):
    """
    判断当前时间是否在目标时间之前的指定时间窗口内。

    参数：
    - target_hours: list, 目标时间点（以小时为单位）。
    - time_window: int 或 float，时间窗口（小时），表示目标时间之前的范围。

    返回：
    - dict，键为目标时间点（小时），值为布尔值，表示是否在目标时间之前的时间范围内。
    """
    # 获取当前时间
    if target_hours is None:
        target_hours = [0, 8, 16]
    now = get_utc8_now()
    results = {}

    for hour in target_hours:
        # 构造当天的目标时间点
        target_time = now.replace(hour=hour, minute=0, second=0, microsecond=0)

        # 如果当前时间已过目标时间，跳过当天，检查次日目标时间
        if now > target_time:
            target_time += datetime.timedelta(days=1)

        # 判断当前时间是否在目标时间之前的 time_window 小时范围内
        if before:
            within_range = 0 <= (target_time - now).total_seconds() <= time_window * 3600
        else:
            within_range = 0 <= (now - target_time).total_seconds() <= time_window * 3600
        results[hour] = within_range

    return results


def is_cex_funding_time_in_hour(hour=1):
    """
        CEX 还有x小时收取费用
    :param hour:
    :return:
    """
    r = is_within_time_window([0, 8, 16], hour, before=True)
    return any(r.values())


def is_cex_funding_time_after_hour(hour=1):
    """
        CEX 距离收取费用后x小时内
    """
    r = is_within_time_window([0, 8, 16], hour, before=False)
    return all(r.values())


# 示例调用
if __name__ == "__main__":
    print(get_datetime_now_str())
    print(get_utc8_now())
    result = is_within_time_window_before()
    print(is_cex_funding_time_in_hour(1))
    print("是否在目标时间范围内:", result)
