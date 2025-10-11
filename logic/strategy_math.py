# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : strategy_math
@Description :
@Time        : 2025/10/10 18:52
"""
from cex_tools.cex_enum import TradeSide


def calculate_zscore(current_spread,
                     spread_stats,
                     funding_rate1, funding_rate2, side1=None, fee_rate=None):
    ma_spread = spread_stats.mean_spread if spread_stats else 0.0
    ma_spread_std = spread_stats.std_spread if spread_stats else 0.0
    # 计算当前价差（包含资金费率）
    current_spread += funding_rate1 / 365 / 3 - funding_rate2 / 365 / 3  # 加上资金费率差异(8小时)
    if side1:
        sign = 1 if side1==TradeSide.BUY else -1
        z_score = (current_spread - ma_spread + sign * fee_rate) / ma_spread_std if ma_spread_std > 0 else 0
    else:
        # 计算Z-Score
        z_score = (current_spread - ma_spread) / ma_spread_std if ma_spread_std > 0 else 0
    return z_score


def infer_optimal_spread_by_zscore(z_score_threshold, spread_stats,
                                   funding_rate1, funding_rate2):
    """
    根据Z-Score反推最优价差
    """
    ma_spread = spread_stats.mean_spread if spread_stats else 0.0
    ma_spread_std = spread_stats.std_spread if spread_stats else 0.0
    optimal_spread = ma_spread + z_score_threshold * ma_spread_std - (funding_rate1 / 365 / 3 - funding_rate2 / 365 / 3)
    return optimal_spread
