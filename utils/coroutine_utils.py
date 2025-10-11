# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : coroutine_utils
@Description : 协程工具函数库，提供统一的协程处理接口
@Time        : 2025/10/6 09:20
"""
import asyncio
import concurrent.futures
import threading
from functools import wraps
from typing import Any, Callable, Optional, Union
from loguru import logger



def run_coroutine_sync(coro: Any, timeout: Optional[float] = None) -> Any:
    """
    在同步环境中安全运行协程

    Args:
        coro: 协程对象或协程函数
        timeout: 超时时间（秒）

    Returns:
        协程的执行结果

    Raises:
        RuntimeError: 如果在已有事件循环中运行且无法处理
        TimeoutError: 如果执行超时
        Exception: 协程执行中的异常
    """
    # 如果是协程函数，先调用它
    if asyncio.iscoroutinefunction(coro):
        coro = coro()

    # 如果不是协程对象，直接返回
    if not asyncio.iscoroutine(coro):
        return coro

    try:
        # 尝试获取当前事件循环
        try:
            loop = asyncio.get_running_loop()
            # 如果当前有运行的事件循环，使用线程池执行
            logger.debug("在现有事件循环中运行协程，使用线程池")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result(timeout=timeout)
        except RuntimeError:
            # 没有运行的事件循环，直接运行
            logger.debug("直接运行协程")
            return asyncio.run(coro)

    except RuntimeError as e:
        # 作为最后的备选方案，在新线程中运行
        if "cannot be called from a running event loop" in str(e) or "There is no current event loop" in str(e):
            logger.warning("事件循环冲突，使用新线程运行协程")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result(timeout=timeout)
        else:
            raise
    except Exception as e:
        logger.error(f"运行协程时发生错误: {e}")
        raise


async def run_coroutine_async(coro: Any) -> Any:
    """
    在异步环境中运行协程（处理同步函数和协程函数的统一接口）

    Args:
        coro: 协程对象、协程函数或普通函数

    Returns:
        函数执行结果
    """
    # 如果是协程函数，先调用它
    if asyncio.iscoroutinefunction(coro):
        return await coro()

    # 如果是协程对象，直接等待
    if asyncio.iscoroutine(coro):
        return await coro

    # 如果是普通函数，直接调用
    return coro()


async def safe_execute_async(func: Callable, *args, **kwargs) -> Any:
    """
    安全地异步执行函数，自动处理同步和异步函数

    Args:
        func: 要执行的函数
        *args: 位置参数
        **kwargs: 关键字参数

    Returns:
        函数执行结果
    """
    if asyncio.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        return func(*args, **kwargs)


def safe_execute_sync(func: Callable, *args, timeout: Optional[float] = None, **kwargs) -> Any:
    """
    安全地同步执行函数，自动处理同步和异步函数

    Args:
        func: 要执行的函数
        *args: 位置参数
        timeout: 异步函数的超时时间
        **kwargs: 关键字参数

    Returns:
        函数执行结果
    """
    if asyncio.iscoroutinefunction(func):
        return run_coroutine_sync(func(*args, **kwargs), timeout=timeout)
    else:
        return func(*args, **kwargs)


def async_to_sync(timeout: Optional[float] = None):
    """
    装饰器：将异步函数转换为同步函数

    Args:
        timeout: 超时时间（秒）

    Returns:
        装饰器函数
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return run_coroutine_sync(func(*args, **kwargs), timeout=timeout)
        return wrapper
    return decorator


class CoroutineRunner:
    """协程运行器，提供更高级的协程管理功能"""

    def __init__(self, default_timeout: Optional[float] = 10):
        """
        初始化协程运行器

        Args:
            default_timeout: 默认超时时间（秒）
        """
        self.default_timeout = default_timeout
        self._loop = None
        self._executor = None

    async def run_async(self, func: Callable, *args, **kwargs) -> Any:
        """在异步环境中运行函数"""
        return await safe_execute_async(func, *args, **kwargs)

    def run_sync(self, func: Callable, *args, timeout: Optional[float] = None, **kwargs) -> Any:
        """在同步环境中运行函数"""
        if timeout is None:
            timeout = self.default_timeout
        return safe_execute_sync(func, *args, timeout=timeout, **kwargs)

    def run_multiple_sync(self, tasks: list, timeout: Optional[float] = None) -> list:
        """
        在同步环境中并行运行多个任务

        Args:
            tasks: 任务列表，每个元素为 (func, args, kwargs) 元组
            timeout: 每个任务的超时时间

        Returns:
            结果列表
        """
        if timeout is None:
            timeout = self.default_timeout

        results = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []

            for task in tasks:
                if len(task) == 1:
                    func, args, kwargs = task[0], (), {}
                elif len(task) == 2:
                    func, args, kwargs = task[0], task[1], {}
                else:
                    func, args, kwargs = task

                # 使用 safe_execute_sync 来统一处理同步和异步函数
                future = executor.submit(safe_execute_sync, func, *args, timeout=timeout, **kwargs)
                futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=timeout)
                    results.append(result)
                except Exception as e:
                    logger.error(f"并行任务执行失败: {e}")
                    results.append(None)

        return results


# 全局默认协程运行器实例
default_runner = CoroutineRunner()

# 便捷函数
run_async = default_runner.run_async
run_sync = default_runner.run_sync
run_multiple_sync = default_runner.run_multiple_sync


# 使用示例和测试代码
if __name__ == "__main__":
    import time

    # 测试同步函数
    def sync_func(x, y):
        time.sleep(0.1)
        return x + y

    # 测试异步函数
    async def async_func(x, y):
        await asyncio.sleep(0.1)
        return x * y

    # 测试协程运行器
    print("🧪 测试协程工具函数")

    # 测试同步执行
    result1 = run_sync(sync_func, 2, 3)
    print(f"同步函数结果: {result1}")

    result2 = run_sync(async_func, 2, 3)
    print(f"异步函数结果: {result2}")

    # 测试并行执行（仅使用同步函数以避免事件循环问题）
    tasks = [
        (sync_func, (1, 2), {}),
        (sync_func, (3, 4), {}),
        (sync_func, (5, 6), {})
    ]

    results = run_multiple_sync(tasks)
    print(f"并行执行结果: {results}")

    # 测试装饰器
    print("\n🧪 测试装饰器")

    @async_to_sync()
    async def decorated_async_func(x, y):
        await asyncio.sleep(0.05)
        return x + y

    result3 = decorated_async_func(10, 20)
    print(f"装饰器结果: {result3}")

    print("✅ 协程工具函数测试完成")
