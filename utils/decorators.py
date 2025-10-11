# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : decorators
@Description :
@Time        : 2023/11/27 19:53
"""
import functools
import multiprocessing
import os
import pickle
import time
import traceback
from loguru import logger

import threading


def time_logger(timeout=None):
    """
    装饰器，用于记录函数的执行时间，如果执行时间超过指定的timeout，则发出告警。

    :param timeout: 超时阈值，单位为秒
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)  # 执行目标函数
            end_time = time.time()
            exec_time = end_time - start_time

            # 记录函数执行时间
            msg = f"Function '{func.__name__}' cost {exec_time:.4f} seconds"
            logger.info(msg)

            # 如果执行时间超过设定的超时阈值，发出告警
            if timeout is not None and exec_time > timeout:
                logger.error("⚠️ " + msg)

            return result

        return wrapper

    return decorator


def async_timed_cache(timeout=None):
    """支持异步函数的缓存装饰器，可选TTL过期时间"""
    cache = {}

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # 根据参数生成缓存键，注意要处理关键字参数
            key = str(args) + str(sorted(kwargs.items()))

            if key in cache:
                cached_value, timestamp = cache[key]
                # 检查TTL是否过期
                if timeout is None or (time.time() - timestamp < timeout):
                    return cached_value
                else:
                    # 如果过期，删除缓存项
                    del cache[key]

            # 缓存中没有或已过期，则调用原异步函数
            result = await func(*args, **kwargs)
            cache[key] = (result, time.time())
            return result

        return async_wrapper

    return decorator


def timed_cache(timeout):
    """缓存装饰器，带有超时功能。

    Args:
        timeout (int): 缓存的超时时间，以秒为单位。

    Returns:
        function: 带有缓存功能的装饰器。
    """

    def decorator(func):
        cache = {}

        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            key = (args, frozenset(kwargs.items()))
            current_time = time.time()

            # 检查缓存是否存在并且是否已过期
            if key in cache:
                result, timestamp = cache[key]
                if current_time - timestamp < timeout:
                    return result

            # 如果缓存不存在或已过期，重新计算并缓存结果
            result = func(*args, **kwargs)
            cache[key] = (result, current_time)
            return result

        return wrapped

    return decorator


class LocalFileManager:
    dir_name = "./trading_data"

    @staticmethod
    def get_file_path(file_name):
        return "/".join([LocalFileManager.dir_name, file_name])

    @staticmethod
    def is_file_exists(file_name):
        return os.path.exists(LocalFileManager.get_file_path(file_name))

    @staticmethod
    def make_1dir():
        # if not os.path.exists(LocalFileManager.dir_name):
        #     os.mkdir(LocalFileManager.dir_name)
        os.makedirs(LocalFileManager.dir_name, exist_ok=True)

    @staticmethod
    def save_to_file(py_obj, file_name):
        LocalFileManager.make_1dir()
        file_path = LocalFileManager.get_file_path(file_name)
        with open(file_path, "wb") as f:
            pickle.dump(py_obj, f)

    @staticmethod
    def load_from_file(file_name):
        LocalFileManager.make_1dir()
        file_path = LocalFileManager.get_file_path(file_name)
        if not LocalFileManager.is_file_exists(file_name):
            raise Exception(f"{file_name} not exist")
        with open(file_path, "rb") as f:
            py_obj = pickle.load(f)
        return py_obj


def cache_with_timeout(timeout=60, file_name=None):
    """
        保存函数返回结果到本地文件
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if file_name is None:
                cache_file_name = f"{func.__name__}.pkl"
            else:
                cache_file_name = f"{file_name}.pkl"
            if LocalFileManager.is_file_exists(cache_file_name):
                # Load cached result if available
                cached_result = LocalFileManager.load_from_file(cache_file_name)
                timestamp = cached_result.get('timestamp', 0)
                result = cached_result.get('result')
                if time.time() - timestamp <= timeout:
                    return result

            # Generate new result and save to cache
            result = func(*args, **kwargs)
            timestamped_result = {'timestamp': time.time(), 'result': result}
            LocalFileManager.save_to_file(timestamped_result, cache_file_name)
            return result

        return wrapper

    return decorator


def exception_notify_wrapper():
    def _exception_notify_wrapper(method):
        @functools.wraps(method)
        def _decorator(*args, **kwargs):
            start_time = time.time()
            try:
                result = method(*args, **kwargs)
            except Exception as e:
                trace_info = traceback.format_exc()
                result = None
                title = "❌ {}-{}:{}".format(method.__name__, method.__doc__ or "", str(e))
                content = trace_info
                logger.error("\n".join([title, content, trace_info]))
            cost = time.time() - start_time
            if cost > 10:
                logger.info("{}执行时间:{:.2f}s".format(method.__name__, cost))
            return result

        return _decorator

    return _exception_notify_wrapper


def async_me(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        def a(*args, **kwargs):
            t = threading.Thread(target=f, args=args, kwargs=kwargs, daemon=True)
            t.start()

        return a

    return wrapper()


def async_process_me(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        def a(*args, **kwargs):
            t = multiprocessing.Process(target=f, args=args, kwargs=kwargs, daemon=True)
            t.daemon = True
            t.start()

        return a

    return wrapper()


def protect_task(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        f_name = "./task_running.lock"
        file = open(f_name, "w")
        file.close()
        result = f(*args, **kwargs)
        if os.path.exists(f_name):
            os.remove(f_name)
        return result

    return wrapper()


def prevent_duplicate_run(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not hasattr(func, 'is_running') or not func.is_running:
            func.is_running = True
            try:
                return func(*args, **kwargs)
            finally:
                func.is_running = False
        else:
            logger.info(f"{func.__name__} is already running. Skipping...")
            return None

    return wrapper


from collections import deque


def call_rate_limit(max_calls, period=300, raise_error=True):
    """
    一个装饰器，使得函数在指定时间段内最多调用指定次数。
    :param raise_error:    超出限制时是否抛出异常
    :param max_calls: 最大允许调用次数
    :param period: 时间段（秒）
    """

    def decorator(func):
        calls = deque()  # 使用 deque 记录调用时间戳

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 获取当前时间
            now = time.time()

            # 清理掉已经超出时间窗口的调用
            while calls and now - calls[0] > period:
                calls.popleft()

            if len(calls) < max_calls:
                # 如果未达到调用限制，调用函数并记录时间戳
                calls.append(now)
                return func(*args, **kwargs)
            else:
                # 超出限制时抛出异常或返回自定义提示
                if raise_error:
                    raise Exception("调用频率超出限制，稍后再试")
                else:
                    logger.error(f"{func.__name__} {args} {kwargs} 调用频率超出限制，稍后再试")

        return wrapper

    return decorator
