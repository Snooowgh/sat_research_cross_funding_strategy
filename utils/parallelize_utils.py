# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : parallelize_utils
@Description : 并行化工具
@Time        : 2024/10/23 20:18
"""
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Callable, List, Any, Tuple
import asyncio


# 通用并行化函数
def parallelize_tasks(task_funcs: List[Tuple[Callable, Tuple, dict]],
                      use_thread: bool = True, max_workers: int = 50) -> List[Any]:
    """
    通用的任务并行化函数，可以通过线程池或进程池来并行执行任务。

    参数:
        task_funcs: 每个任务的 (函数, args, kwargs) 组成的列表。
        use_thread: 执行模式, 可选 线程 或 进程。
        max_workers: 最大的并行 worker 数。

    返回:
        每个任务执行后的结果列表。
    """
    # 根据模式选择线程池或进程池
    if len(task_funcs) == 0:
        return []
    Executor = ThreadPoolExecutor if use_thread else ProcessPoolExecutor

    max_workers = min(max_workers, len(task_funcs) + 1)

    # 存储结果的占位列表
    results = [None] * len(task_funcs)
    # 提交任务并记录每个任务的索引
    with Executor(max_workers=max_workers) as executor:
        futures = [
            (executor.submit(func, *args, **kwargs), index)
            for index, (func, args, kwargs) in enumerate(task_funcs)
        ]
        # 按任务提交顺序收集结果
        for future, index in futures:
            results[index] = future.result()  # 按任务的索引收集结果
    return results


# 异步并行化函数
async def parallelize_tasks_async(
        task_funcs: List[Tuple[Callable, Tuple, dict]]
) -> List[Any]:
    """
    支持不同任务函数的异步并行化函数。

    参数:
        task_funcs: 每个任务的 (函数, args, kwargs) 组成的列表。
    返回:
        每个任务执行后的结果列表。
    """
    if len(task_funcs) == 0:
        return []
    # 使用 asyncio.gather 并发执行任务
    tasks = [
        func(*args, **kwargs)  # 动态传递参数
        for func, args, kwargs in task_funcs
    ]
    results = await asyncio.gather(*tasks)
    return results


async def batch_process_with_concurrency_limit(
    items: List[Any],
    process_func: Callable,
    max_concurrency: int = 5,
    progress_callback: Callable = None,
) -> List[Any]:
    """
    带并发限制的批量处理工具函数

    参数:
        items: 要处理的参数列表
        process_func: 异步处理函数，签名为 async def func(item, *args, **kwargs)
        max_concurrency: 最大并发数
        progress_callback: 进度回调函数，签名为 callback(completed, total, result)
        *func_args: 传递给处理函数的位置参数
        **func_kwargs: 传递给处理函数的关键字参数

    返回:
        处理结果列表（包含None值，表示处理失败的项目）
    """
    if not items:
        return []

    semaphore = asyncio.Semaphore(max_concurrency)

    async def process_with_semaphore(item):
        async with semaphore:
            return await process_func(*item)

    tasks = [process_with_semaphore(item) for item in items]
    results = []
    completed_count = 0

    for future in asyncio.as_completed(tasks):
        result = await future
        results.append(result)
        completed_count += 1

        if progress_callback:
            progress_callback(completed_count, len(items), result)

    return results
