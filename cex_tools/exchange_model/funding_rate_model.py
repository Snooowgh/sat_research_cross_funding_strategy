# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : funding_rate_model
@Description : 资金费率历史数据模型
@Time        : 2025/10/12
"""

from typing import List, Optional, Union
from datetime import datetime
from .base_model import BaseModel


class FundingRateHistory(BaseModel):
    """
    历史资金费率数据模型

    表示某个交易对在特定时间的资金费率信息
    """

    def __init__(self, symbol: str = None, funding_rate: float = None,
                 funding_time: int = None, annualized_rate: float = None):
        """
        初始化历史资金费率数据

        Args:
            symbol: 交易对符号 (如 "BTCUSDT")
            funding_rate: 资金费率 (单次，小数形式，如 0.0001)
            funding_time: 资金费率时间戳 (毫秒)
            annualized_rate: 年化资金费率 (小数形式，如 0.0365)
        """
        self.symbol = symbol
        self.funding_rate = funding_rate
        self.funding_time = funding_time
        self.annualized_rate = annualized_rate

    @property
    def funding_time_str(self) -> str:
        """格式化的资金费率时间字符串"""
        if self.funding_time:
            return datetime.fromtimestamp(self.funding_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
        return ""

    @property
    def funding_rate_percentage(self) -> str:
        """资金费率百分比表示"""
        if self.funding_rate is not None:
            return f"{self.funding_rate * 100:.4f}%"
        return ""

    @property
    def annualized_rate_percentage(self) -> str:
        """年化资金费率百分比表示"""
        if self.annualized_rate is not None:
            return f"{self.annualized_rate * 100:.2f}%"
        return ""


class FundingHistory(BaseModel):
    """
    用户资金费收取历史数据模型

    表示用户仓位实际支付或收到的资金费记录
    """

    def __init__(self, symbol: str = None, funding_rate: float = None,
                 funding_amount: float = None, position_side: str = None,
                 funding_time: int = None, transaction_id: str = None,
                 income_type: str = None):
        """
        初始化用户资金费历史数据

        Args:
            symbol: 交易对符号 (如 "BTCUSDT")
            funding_rate: 资金费率 (小数形式)
            funding_amount: 资金费金额 (正数表示收到，负数表示支付)
            position_side: 持仓方向 ("LONG"/"SHORT"/"BOTH")
            funding_time: 资金费时间戳 (毫秒)
            transaction_id: 交易ID
            income_type: 收入类型 (如 "FUNDING_FEE")
        """
        self.symbol = symbol
        self.funding_rate = funding_rate
        self.funding_amount = funding_amount
        self.position_side = position_side
        self.funding_time = funding_time
        self.transaction_id = transaction_id
        self.income_type = income_type

    @property
    def funding_time_str(self) -> str:
        """格式化的资金费时间字符串"""
        if self.funding_time:
            return datetime.fromtimestamp(self.funding_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
        return ""

    @property
    def funding_amount_str(self) -> str:
        """格式化的资金费金额字符串"""
        if self.funding_amount is not None:
            return f"{self.funding_amount:.4f} USDT"
        return ""

    @property
    def is_funding_received(self) -> bool:
        """是否收到了资金费（正数表示收到）"""
        return self.funding_amount is not None and self.funding_amount > 0

    @property
    def is_funding_paid(self) -> bool:
        """是否支付了资金费（负数表示支付）"""
        return self.funding_amount is not None and self.funding_amount < 0


class FundingRateHistoryResponse(BaseModel):
    """
    历史资金费率查询响应模型
    """

    def __init__(self, symbol: str = None, data: List[FundingRateHistory] = None,
                 limit: int = None, total: int = None, start_time: int = None,
                 end_time: int = None):
        """
        初始化历史资金费率响应

        Args:
            symbol: 交易对符号
            data: 历史资金费率数据列表
            limit: 查询限制数量
            total: 总数量
            start_time: 开始时间戳 (毫秒)
            end_time: 结束时间戳 (毫秒)
        """
        self.symbol = symbol
        self.data = data or []
        self.limit = limit
        self.total = total
        self.start_time = start_time
        self.end_time = end_time

    def add_rate(self, rate_data: FundingRateHistory):
        """添加资金费率数据"""
        self.data.append(rate_data)

    def sort_by_time(self, reverse: bool = False):
        """按时间排序"""
        self.data.sort(key=lambda x: x.funding_time or 0, reverse=reverse)

    def get_latest_rate(self) -> Optional[FundingRateHistory]:
        """获取最新的资金费率"""
        if not self.data:
            return None
        return max(self.data, key=lambda x: x.funding_time or 0)

    def get_average_rate(self, annualized: bool = False) -> float:
        """获取平均资金费率"""
        if not self.data:
            return 0.0

        if annualized:
            rates = [x.annualized_rate for x in self.data if x.annualized_rate is not None]
        else:
            rates = [x.funding_rate for x in self.data if x.funding_rate is not None]

        return sum(rates) / len(rates) if rates else 0.0


class FundingHistoryResponse(BaseModel):
    """
    用户资金费历史查询响应模型
    """

    def __init__(self, symbol: str = None, data: List[FundingHistory] = None,
                 limit: int = None, total: int = None, start_time: int = None,
                 end_time: int = None):
        """
        初始化用户资金费历史响应

        Args:
            symbol: 交易对符号
            data: 用户资金费历史数据列表
            limit: 查询限制数量
            total: 总数量
            start_time: 开始时间戳 (毫秒)
            end_time: 结束时间戳 (毫秒)
        """
        self.symbol = symbol
        self.data = data or []
        self.limit = limit
        self.total = total
        self.start_time = start_time
        self.end_time = end_time

    def add_funding_record(self, funding_data: FundingHistory):
        """添加资金费记录"""
        self.data.append(funding_data)

    def sort_by_time(self, reverse: bool = False):
        """按时间排序"""
        self.data.sort(key=lambda x: x.funding_time or 0, reverse=reverse)

    def get_total_funding_amount(self) -> float:
        """获取总资金费金额"""
        return sum(x.funding_amount for x in self.data if x.funding_amount is not None)

    def get_total_received(self) -> float:
        """获取总收到的资金费"""
        return sum(x.funding_amount for x in self.data if x.funding_amount and x.funding_amount > 0)

    def get_total_paid(self) -> float:
        """获取总支付的资金费"""
        return sum(x.funding_amount for x in self.data if x.funding_amount and x.funding_amount < 0)

    def filter_by_symbol(self, symbol: str) -> 'FundingHistoryResponse':
        """按交易对过滤"""
        filtered_data = [x for x in self.data if x.symbol == symbol]
        return FundingHistoryResponse(
            symbol=symbol,
            data=filtered_data,
            limit=self.limit,
            total=len(filtered_data),
            start_time=self.start_time,
            end_time=self.end_time
        )