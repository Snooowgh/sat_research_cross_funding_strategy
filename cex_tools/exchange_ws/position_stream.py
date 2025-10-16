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

    def __init__(self, exchange_code: str, **kwargs):
        """
        初始化仓位WebSocket流

        Args:
            exchange_code: 交易所名称
            on_position_callback: 仓位事件回调函数
        """
        self.exchange_code = exchange_code
        async def default_callback(e):
            pass
        self.on_account_callback = kwargs.get("on_account_callback", default_callback)
        self.on_position_callback = kwargs.get("on_position_callback", default_callback)
        self.on_order_update_callback = kwargs.get("on_order_update_callback", default_callback)
        self.latest_positions: Dict[str, any] = {}  # symbol -> position_detail
        self._running = False
        self._last_update_time = 0

    def set_order_update_callback(self, call_back):
        self.on_order_update_callback = call_back

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
