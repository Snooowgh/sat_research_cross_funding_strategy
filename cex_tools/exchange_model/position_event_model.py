# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : position_event_model
@Description : 仓位事件数据模型
@Time        : 2025/10/15
"""
import time
from enum import Enum
from typing import Optional, Dict, Any
from cex_tools.exchange_model.position_model import BinancePositionDetail, OkxPositionDetail, \
    BitgetPositionDetail, HyperliquidPositionDetail, LighterPositionDetail, BybitPositionDetail


class PositionEventType(Enum):
    """仓位事件类型"""
    UPDATE = "update"          # 仓位更新
    OPEN = "open"             # 开仓
    CLOSE = "close"           # 平仓
    INCREASE = "increase"     # 加仓
    DECREASE = "decrease"     # 减仓
    LIQUIDATION = "liquidation"  # 强平
    FUNDING = "funding"       # 资金费用
    ADL = "adl"              # 自动减仓


class PositionEvent:
    """仓位事件数据模型"""

    def __init__(self,
                 exchange_code: str,
                 symbol: str,
                 event_type: PositionEventType,
                 position_detail: Any = None,
                 previous_position: Any = None,
                 timestamp: float = None):
        """
        初始化仓位事件

        Args:
            exchange_code: 交易所代码
            symbol: 交易对符号
            event_type: 事件类型
            position_detail: 当前仓位详情
            previous_position: 前一次仓位详情（用于计算变化）
            timestamp: 事件时间戳
        """
        self.exchange_code = exchange_code
        self.symbol = symbol
        self.event_type = event_type
        self.position_detail = position_detail
        self.previous_position = previous_position
        self.timestamp = timestamp or time.time()

        # 计算仓位变化
        self._calculate_changes()

    def _calculate_changes(self):
        """计算仓位变化"""
        if self.previous_position and self.position_detail:
            prev_amt = getattr(self.previous_position, 'positionAmt', 0)
            curr_amt = getattr(self.position_detail, 'positionAmt', 0)
            self.size_change = curr_amt - prev_amt

            prev_pnl = getattr(self.previous_position, 'unRealizedProfit', 0)
            curr_pnl = getattr(self.position_detail, 'unRealizedProfit', 0)
            self.pnl_change = curr_pnl - prev_pnl
        else:
            self.size_change = 0
            self.pnl_change = 0

    @property
    def position_size(self) -> float:
        """当前仓位大小"""
        if self.position_detail:
            return getattr(self.position_detail, 'positionAmt', 0)
        return 0

    @property
    def unrealized_pnl(self) -> float:
        """当前未实现盈亏"""
        if self.position_detail:
            return getattr(self.position_detail, 'unRealizedProfit', 0)
        return 0

    @property
    def position_side(self) -> Optional[str]:
        """仓位方向"""
        if self.position_detail:
            side = getattr(self.position_detail, 'position_side', None)
            if side:
                return side.value
        return None

    @property
    def entry_price(self) -> Optional[float]:
        """入场价格"""
        if self.position_detail:
            return getattr(self.position_detail, 'entryPrice', None)
        return None

    @property
    def mark_price(self) -> Optional[float]:
        """标记价格"""
        if self.position_detail:
            return getattr(self.position_detail, 'markPrice', None)
        return None

    @property
    def notional_value(self) -> float:
        """名义价值"""
        if self.position_detail:
            return getattr(self.position_detail, 'notional', 0)
        return 0

    @property
    def leverage(self) -> Optional[float]:
        """杠杆倍数"""
        if self.position_detail:
            return getattr(self.position_detail, 'leverage', None)
        return None

    def is_open_position(self) -> bool:
        """是否为开仓状态"""
        return self.position_size != 0

    def is_long_position(self) -> bool:
        """是否为多头仓位"""
        return self.position_size > 0

    def is_short_position(self) -> bool:
        """是否为空头仓位"""
        return self.position_size < 0

    def get_position_summary(self) -> str:
        """获取仓位摘要信息"""
        if not self.is_open_position():
            return f"{self.symbol}: 无仓位"

        side = "多头" if self.is_long_position() else "空头"
        return (f"{self.symbol} {side} {abs(self.position_size):.4f} "
                f"入场价: {self.entry_price} "
                f"盈亏: {self.unrealized_pnl:.4f}")

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'exchange_code': self.exchange_code,
            'symbol': self.symbol,
            'event_type': self.event_type.value,
            'timestamp': self.timestamp,
            'position_size': self.position_size,
            'position_side': self.position_side,
            'entry_price': self.entry_price,
            'mark_price': self.mark_price,
            'unrealized_pnl': self.unrealized_pnl,
            'notional_value': self.notional_value,
            'leverage': self.leverage,
            'size_change': self.size_change,
            'pnl_change': self.pnl_change
        }

    def __str__(self):
        return (f"PositionEvent[{self.exchange_code}] {self.symbol} "
                f"{self.event_type.value} {self.get_position_summary()} "
                f"change: {self.size_change}")

    @classmethod
    def create_from_position_detail(cls,
                                   exchange_code: str,
                                   position_detail: Any,
                                   previous_position: Any = None,
                                   event_type: PositionEventType = PositionEventType.UPDATE) -> 'PositionEvent':
        """
        从仓位详情创建事件

        Args:
            exchange_code: 交易所代码
            position_detail: 仓位详情
            previous_position: 前一次仓位详情
            event_type: 事件类型

        Returns:
            PositionEvent: 仓位事件实例
        """
        symbol = getattr(position_detail, 'symbol', '')
        return cls(
            exchange_code=exchange_code,
            symbol=symbol,
            event_type=event_type,
            position_detail=position_detail,
            previous_position=previous_position
        )

    @classmethod
    def detect_event_type(cls, previous_size: float, current_size: float) -> PositionEventType:
        """
        根据仓位大小变化检测事件类型

        Args:
            previous_size: 前一次仓位大小
            current_size: 当前仓位大小

        Returns:
            PositionEventType: 检测到的事件类型
        """
        if previous_size == 0 and current_size != 0:
            return PositionEventType.OPEN
        elif previous_size != 0 and current_size == 0:
            return PositionEventType.CLOSE
        elif previous_size * current_size > 0:  # 同向
            if abs(current_size) > abs(previous_size):
                return PositionEventType.INCREASE
            else:
                return PositionEventType.DECREASE
        elif previous_size * current_size < 0:  # 反向（调仓）
            return PositionEventType.CLOSE if abs(current_size) < abs(previous_size) else PositionEventType.OPEN
        else:
            return PositionEventType.UPDATE