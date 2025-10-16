# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : hedge_spread_analyzer
@Description : 对冲交易价差分析器，用于计算两个交易所历史价差的统计信息
@Time        : 2025/10/6
"""
import asyncio
from typing import List, Dict, Tuple, Optional, Union
from dataclasses import dataclass
from loguru import logger
import numpy as np
from cex_tools.async_exchange_adapter import AsyncExchangeAdapter


@dataclass
class SpreadStatistics:
    """价差统计信息"""
    mean_spread: float          # 价差均值
    std_spread: float           # 价差标准差
    min_spread: float           # 最小价差
    max_spread: float           # 最大价差
    median_spread: float        # 价差中位数
    percentile_25: float        # 25分位数
    percentile_75: float        # 75分位数
    sample_count: int           # 样本数量
    confidence_interval_95: Tuple[float, float]  # 95%置信区间

    def __str__(self):
        return f"""
价差统计信息:
样本数量: {self.sample_count}
均值: {self.mean_spread:.4%}
标准差: {self.std_spread:.4%}
最小值: {self.min_spread:.4%}
最大值: {self.max_spread:.4%}
中位数: {self.median_spread:.4%}
25分位数: {self.percentile_25:.4%}
75分位数: {self.percentile_75:.4%}
95%置信区间: [{self.confidence_interval_95[0]:.4%}, {self.confidence_interval_95[1]:.4%}]
        """


@dataclass
class KlineData:
    """统一的K线数据结构"""
    timestamp: int     # 时间戳
    close: float       # 收盘价


class HedgeSpreadAnalyzer:
    """
    对冲交易价差分析器

    用于计算两个交易所同一币种历史K线收盘价的价差统计信息，
    为对冲交易开平仓提供最小价差收益率参考。
    """

    def __init__(self, exchange1: AsyncExchangeAdapter, exchange2: AsyncExchangeAdapter):
        """
        初始化价差分析器

        Args:
            exchange1: 异步交易所1对象
            exchange2: 异步交易所2对象
        """
        self.exchange1 = exchange1
        self.exchange2 = exchange2
        self.exchange_pair = f"{exchange1.exchange_code}-{exchange2.exchange_code}"

    async def get_aligned_klines(self, symbol: str, interval: str = "1m", limit: int = 1000) -> Tuple[
        List[KlineData], List[KlineData]]:
        """
        获取对齐的K线数据

        Args:
            symbol: 交易对符号
            interval: K线间隔
            limit: K线数量限制

        Returns:
            两个交易所的对齐K线数据列表
        """
        try:
            # 并行获取两个交易所的K线数据
            klines1, klines2 = await asyncio.gather(
                self.exchange1.get_klines(symbol, interval, limit),
                self.exchange2.get_klines(symbol, interval, limit)
            )
            # 转换为统一格式
            klines_data1 = [KlineData(timestamp=kline.open_time, close=kline.close) for kline in klines1]
            klines_data2 = [KlineData(timestamp=kline.open_time, close=kline.close) for kline in klines2]

            # 按时间戳排序
            klines_data1.sort(key=lambda x: x.timestamp)
            klines_data2.sort(key=lambda x: x.timestamp)

            # 对齐时间戳（取交集）
            timestamps1 = {k.timestamp for k in klines_data1}
            timestamps2 = {k.timestamp for k in klines_data2}
            common_timestamps = timestamps1.intersection(timestamps2)

            if len(common_timestamps) < 100:
                logger.warning(f"对齐后的K线数据较少: {len(common_timestamps)} 条记录")

            # 过滤出相同时间戳的K线数据
            aligned_klines1 = [k for k in klines_data1 if k.timestamp in common_timestamps]
            aligned_klines2 = [k for k in klines_data2 if k.timestamp in common_timestamps]

            # 再次按时间戳排序确保完全对齐
            aligned_klines1.sort(key=lambda x: x.timestamp)
            aligned_klines2.sort(key=lambda x: x.timestamp)
            if len(klines_data1) != len(aligned_klines2):
                logger.warning(f"{symbol} {self.exchange_pair} {interval} {limit} 对齐后K线数据: {len(aligned_klines1)} 条记录")

            return aligned_klines1, aligned_klines2

        except Exception as e:
            logger.error(f"{symbol} {self.exchange_pair} 获取对齐K线数据失败: {e}")
            raise

    def calculate_price_spreads(self, klines1: List[KlineData], klines2: List[KlineData]) -> List[float]:
        """
        计算价差百分比

        Args:
            klines1: 交易所1的K线数据
            klines2: 交易所2的K线数据

        Returns:
            价差百分比列表 (exchange1 - exchange2) / exchange2
        """
        if len(klines1) != len(klines2):
            raise ValueError(f"K线数据长度不匹配: {len(klines1)} vs {len(klines2)}")

        spreads = []
        for i, (k1, k2) in enumerate(zip(klines1, klines2)):
            if k2.close == 0:
                logger.warning(f"交易所2价格为0，跳过第{i}条记录")
                continue

            spread = (k1.close - k2.close) / k2.close
            spreads.append(spread)

        return spreads

    def calculate_spread_statistics(self, spreads: List[float]) -> SpreadStatistics:
        """
        计算价差统计信息

        Args:
            spreads: 价差列表

        Returns:
            价差统计信息
        """
        if not spreads:
            raise ValueError("价差列表为空")

        spreads_array = np.array(spreads)

        # 基础统计
        mean_spread = np.mean(spreads_array)
        std_spread = np.std(spreads_array, ddof=1)  # 使用样本标准差
        min_spread = np.min(spreads_array)
        max_spread = np.max(spreads_array)
        median_spread = np.median(spreads_array)
        percentile_25 = np.percentile(spreads_array, 25)
        percentile_75 = np.percentile(spreads_array, 75)

        # 95%置信区间
        margin_error = 1.96 * (std_spread / np.sqrt(len(spreads_array)))
        confidence_interval_95 = (mean_spread - margin_error, mean_spread + margin_error)

        return SpreadStatistics(
            mean_spread=mean_spread,
            std_spread=std_spread,
            min_spread=min_spread,
            max_spread=max_spread,
            median_spread=median_spread,
            percentile_25=percentile_25,
            percentile_75=percentile_75,
            sample_count=len(spreads),
            confidence_interval_95=confidence_interval_95
        )

    async def analyze_spread(self, symbol: str, interval: str = "1m", limit: int = 1000) -> SpreadStatistics:
        """
        分析指定交易对的价差统计信息

        Args:
            symbol: 交易对符号
            interval: K线间隔
            limit: K线数量限制

        Returns:
            价差统计信息
        """

        # 获取对齐的K线数据
        klines1, klines2 = await self.get_aligned_klines(symbol, interval, limit)

        if len(klines1) < 50:
            raise ValueError(f"对齐后的K线数据太少: {len(klines1)} 条，至少需要50条")

        # 计算价差
        spreads = self.calculate_price_spreads(klines1, klines2)

        if len(spreads) < 50:
            raise ValueError(f"有效价差数据太少: {len(spreads)} 条，至少需要50条")

        # 计算统计信息
        stats = self.calculate_spread_statistics(spreads)

        return stats

    async def get_minimum_profit_threshold(self, stats, confidence_level: float = 0.95) -> float:
        """
        获取最小盈利阈值

        基于历史价差统计，计算在指定置信水平下的最小盈利阈值

        Args:
            symbol: 交易对符号
            confidence_level: 置信水平 (0.5, 0.8, 0.95等)
            interval: K线间隔
            limit: K线数量限制

        Returns:
            最小盈利阈值（百分比）
        """

        if confidence_level <= 0.5:
            return stats.median_spread
        elif confidence_level <= 0.8:
            return stats.percentile_25 if stats.mean_spread > 0 else stats.percentile_75
        elif confidence_level <= 0.95:
            return stats.confidence_interval_95[0] if stats.mean_spread > 0 else stats.confidence_interval_95[1]
        else:
            return stats.min_spread if stats.mean_spread > 0 else stats.max_spread


# 使用示例
if __name__ == "__main__":
    # 这里是使用示例，实际使用时需要传入真实的交易所对象
    pass