# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : position_stream
@Description : 仓位WebSocket流监听基类
@Time        : 2025/10/15
"""
import time
from abc import ABC, abstractmethod
from typing import Optional, Callable, Dict, List
from loguru import logger
from cex_tools.exchange_model.position_event_model import PositionEvent, PositionEventType


class PositionWebSocketStream(ABC):
    """仓位WebSocket流监听器抽象基类"""

    def __init__(self, exchange_name: str, on_position_callback: Callable[[PositionEvent], None]):
        """
        初始化仓位WebSocket流

        Args:
            exchange_name: 交易所名称
            on_position_callback: 仓位事件回调函数
        """
        self.exchange_name = exchange_name
        self.on_position_callback = on_position_callback
        self.latest_positions: Dict[str, any] = {}  # symbol -> position_detail
        self._running = False
        self._last_update_time = 0

    @abstractmethod
    async def start(self):
        """启动 WebSocket 连接"""
        pass

    @abstractmethod
    async def stop(self):
        """停止 WebSocket 连接"""
        pass

    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        return self._running

    def get_latest_position(self, symbol: str) -> Optional[any]:
        """获取指定交易对的最新仓位信息"""
        return self.latest_positions.get(symbol)

    def get_all_positions(self) -> Dict[str, any]:
        """获取所有最新仓位信息"""
        return self.latest_positions.copy()

    def get_open_positions(self) -> Dict[str, any]:
        """获取所有开仓信息"""
        return {
            symbol: pos for symbol, pos in self.latest_positions.items()
            if hasattr(pos, 'positionAmt') and pos.positionAmt != 0
        }

    def _on_position_update(self, position_detail: any, event_type: PositionEventType = None):
        """
        内部回调：当仓位更新时调用

        Args:
            position_detail: 仓位详情
            event_type: 事件类型（可选，自动检测）
        """
        try:
            symbol = getattr(position_detail, 'symbol', '')
            if not symbol:
                logger.warning(f"[{self.exchange_name}] 仓位更新缺少symbol信息")
                return

            # 获取前一次仓位用于比较
            previous_position = self.latest_positions.get(symbol)

            # 自动检测事件类型
            if event_type is None and previous_position:
                prev_size = getattr(previous_position, 'positionAmt', 0)
                curr_size = getattr(position_detail, 'positionAmt', 0)
                event_type = PositionEvent.detect_event_type(prev_size, curr_size)
            elif event_type is None:
                event_type = PositionEventType.UPDATE

            # 更新缓存
            self.latest_positions[symbol] = position_detail
            self._last_update_time = time.time()

            # 创建仓位事件
            position_event = PositionEvent.create_from_position_detail(
                exchange_code=self.exchange_name,
                position_detail=position_detail,
                previous_position=previous_position,
                event_type=event_type
            )

            # 触发用户回调
            try:
                self.on_position_callback(position_event)
            except Exception as e:
                logger.error(f"[{self.exchange_name}] 仓位回调异常: {e}")

        except Exception as e:
            logger.error(f"[{self.exchange_name}] 处理仓位更新异常: {e}")

    def _on_positions_update(self, positions: List[any]):
        """
        批量处理多个仓位更新

        Args:
            positions: 仓位列表
        """
        try:
            # 创建临时副本用于检测已关闭的仓位
            previous_positions = self.latest_positions.copy()
            current_symbols = set()

            # 处理当前所有仓位
            for position_detail in positions:
                symbol = getattr(position_detail, 'symbol', '')
                if symbol:
                    current_symbols.add(symbol)
                    self._on_position_update(position_detail)

            # 检测已关闭的仓位
            for symbol, prev_position in previous_positions.items():
                if symbol not in current_symbols and hasattr(prev_position, 'positionAmt') and prev_position.positionAmt != 0:
                    # 创建空仓位表示关闭
                    empty_position = type(prev_position)({})
                    if hasattr(empty_position, '__init__'):
                        empty_position.__init__({})
                    # 设置必要属性
                    for attr in ['symbol', 'pair', 'exchange_code']:
                        if hasattr(prev_position, attr):
                            setattr(empty_position, attr, getattr(prev_position, attr))
                    if hasattr(empty_position, 'positionAmt'):
                        empty_position.positionAmt = 0

                    self._on_position_update(empty_position, PositionEventType.CLOSE)

        except Exception as e:
            logger.error(f"[{self.exchange_name}] 批量处理仓位更新异常: {e}")

    def get_status_report(self) -> str:
        """获取状态报告"""
        age = time.time() - self._last_update_time if self._last_update_time > 0 else float('inf')
        open_positions = len(self.get_open_positions())
        total_positions = len(self.latest_positions)

        report = f"📊 {self.exchange_name} 仓位WebSocket流状态:\n"
        report += f"  • 连接状态: {'🟢 运行中' if self._running else '🔴 已停止'}\n"
        report += f"  • 仓位总数: {total_positions}\n"
        report += f"  • 开仓数量: {open_positions}\n"
        report += f"  • 最后更新: {age:.1f}秒前"

        if open_positions > 0:
            report += f"\n  • 开仓列表:"
            for symbol, position in self.get_open_positions().items():
                size = getattr(position, 'positionAmt', 0)
                pnl = getattr(position, 'unRealizedProfit', 0)
                side = "多" if size > 0 else "空"
                report += f"\n    - {symbol}: {side}{abs(size):.4f} 盈亏:{pnl:.4f}"

        return report

    def is_position_stale(self, max_age_sec: float = 30.0) -> bool:
        """判断仓位数据是否过期"""
        return (time.time() - self._last_update_time) > max_age_sec

    def get_position_summary(self) -> str:
        """获取仓位摘要"""
        open_positions = self.get_open_positions()
        if not open_positions:
            return f"{self.exchange_name}: 无开仓"

        total_pnl = sum(getattr(pos, 'unRealizedProfit', 0) for pos in open_positions.values())
        long_count = sum(1 for pos in open_positions.values() if getattr(pos, 'positionAmt', 0) > 0)
        short_count = sum(1 for pos in open_positions.values() if getattr(pos, 'positionAmt', 0) < 0)

        return (f"{self.exchange_name}: {len(open_positions)}个开仓 "
                f"(多:{long_count} 空:{short_count}) 总盈亏:{total_pnl:.4f}")

    async def health_check(self) -> bool:
        """健康检查"""
        if not self._running:
            return False
        return not self.is_position_stale()