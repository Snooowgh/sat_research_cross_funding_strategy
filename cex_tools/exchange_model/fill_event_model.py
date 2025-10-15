# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : fill_event_model
@Description : 成交事件数据模型
@Time        : 2025/10/15
"""
from dataclasses import dataclass, asdict
from typing import Optional
from time import time


@dataclass
class FillEvent:
    """成交事件数据模型"""
    exchange_code: str  # 交易所代码
    symbol: str         # 交易对符号 (如 "BTCUSDT")
    order_id: str       # 订单ID
    side: str           # 交易方向 "BUY"/"SELL"
    filled_quantity: float  # 成交数量
    filled_price: float     # 成交价格
    trade_id: str       # 成交ID
    timestamp: float    # 成交时间戳
    commission: float = 0.0    # 手续费
    commission_asset: str = ""  # 手续费币种

    @property
    def filled_value_usd(self) -> float:
        """计算成交金额（美元）"""
        return self.filled_quantity * self.filled_price

    @property
    def is_buy(self) -> bool:
        """判断是否为买单"""
        return self.side == 'BUY'

    @property
    def is_sell(self) -> bool:
        """判断是否为卖单"""
        return self.side == 'SELL'

    @property
    def position_change(self) -> float:
        """计算仓位变化量（正数表示多头增加，负数表示空头增加）"""
        return self.filled_quantity if self.is_buy else -self.filled_quantity

    def to_dict(self) -> dict:
        """转换为字典格式"""
        return asdict(self)

    def __str__(self) -> str:
        """字符串表示"""
        return (f"FillEvent({self.exchange_code} {self.symbol} {self.side} "
                f"{self.filled_quantity}@{self.filled_price} "
                f"value=${self.filled_value_usd:.2f} "
                f"time={self.timestamp})")

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()


@dataclass
class OrderStatus:
    """订单状态数据模型"""
    exchange_code: str
    symbol: str
    order_id: str
    side: str
    order_type: str  # "LIMIT"/"MARKET"
    original_quantity: float
    filled_quantity: float
    average_price: float
    status: str  # "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "FAILED"
    create_time: float
    update_time: float
    commission: float = 0.0
    commission_asset: str = ""

    @property
    def remaining_quantity(self) -> float:
        """剩余数量"""
        return self.original_quantity - self.filled_quantity

    @property
    def fill_percentage(self) -> float:
        """成交百分比"""
        if self.original_quantity == 0:
            return 0.0
        return (self.filled_quantity / self.original_quantity) * 100

    @property
    def is_filled(self) -> bool:
        """是否完全成交"""
        return self.status == 'FILLED'

    @property
    def is_partially_filled(self) -> bool:
        """是否部分成交"""
        return self.status == 'PARTIALLY_FILLED'

    @property
    def is_active(self) -> bool:
        """是否为活跃订单"""
        return self.status in ['NEW', 'PARTIALLY_FILLED']

    @property
    def filled_value_usd(self) -> float:
        """已成交金额"""
        return self.filled_quantity * self.average_price

    def add_fill(self, fill_quantity: float, fill_price: float) -> None:
        """添加成交，更新平均价格"""
        if self.filled_quantity == 0:
            self.average_price = fill_price
        else:
            # 计算新的平均价格
            total_value = self.filled_quantity * self.average_price + fill_quantity * fill_price
            self.filled_quantity += fill_quantity
            self.average_price = total_value / self.filled_quantity

        # 更新状态
        if self.remaining_quantity <= 0:
            self.status = 'FILLED'
            self.filled_quantity = self.original_quantity
        else:
            self.status = 'PARTIALLY_FILLED'

        self.update_time = time()

    def to_dict(self) -> dict:
        """转换为字典格式"""
        return asdict(self)

    def __str__(self) -> str:
        """字符串表示"""
        return (f"OrderStatus({self.exchange_code} {self.symbol} {self.side} "
                f"{self.order_type} {self.filled_quantity}/{self.original_quantity} "
                f"@{self.average_price} [{self.status}] "
                f"value=${self.filled_value_usd:.2f})")

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()


@dataclass
class HedgeEvent:
    """对冲事件数据模型"""
    original_fill: FillEvent
    hedge_symbol: str
    hedge_exchange: str
    hedge_side: str
    hedge_quantity: float
    hedge_order_id: str
    hedge_price: float
    hedge_timestamp: float
    latency_ms: float = 0.0  # 从原成交到对冲执行的延迟

    def __post_init__(self):
        """后处理"""
        # 标准化交易所代码
        self.hedge_exchange = self.hedge_exchange.lower()

        # 标准化交易方向
        if self.hedge_side.upper() in ['BUY', 'SELL']:
            self.hedge_side = self.hedge_side.upper()

        # 确保时间戳为浮点数
        if isinstance(self.hedge_timestamp, (int, str)):
            self.hedge_timestamp = float(self.hedge_timestamp)

        # 计算延迟
        if self.latency_ms == 0.0:
            self.latency_ms = (self.hedge_timestamp - self.original_fill.timestamp) * 1000

    @property
    def hedge_value_usd(self) -> float:
        """对冲金额"""
        return self.hedge_quantity * self.hedge_price

    @property
    def is_effective_hedge(self) -> bool:
        """判断是否为有效对冲（方向相反）"""
        return self.original_fill.side != self.hedge_side

    def to_dict(self) -> dict:
        """转换为字典格式"""
        return asdict(self)

    def __str__(self) -> str:
        """字符串表示"""
        return (f"HedgeEvent({self.hedge_exchange} {self.hedge_symbol} {self.hedge_side} "
                f"{self.hedge_quantity}@{self.hedge_price} "
                f"latency={self.latency_ms:.2f}ms "
                f"original={self.original_fill.exchange_code} {self.original_fill.side})")

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()