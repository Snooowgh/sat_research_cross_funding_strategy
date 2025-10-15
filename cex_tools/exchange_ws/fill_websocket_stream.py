# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : fill_websocket_stream
@Description : 成交WebSocket流抽象基类
@Time        : 2025/10/15
"""
import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Callable, Optional, Any
from loguru import logger

from cex_tools.exchange_model.fill_event_model import FillEvent


class FillWebSocketStream(ABC):
    """成交WebSocket流抽象基类"""

    def __init__(self, exchange_code: str, on_fill_callback: Callable[[FillEvent], None]):
        """
        初始化成交WebSocket流

        Args:
            exchange_code: 交易所代码
            on_fill_callback: 成交事件回调函数
        """
        self.exchange_code = exchange_code.lower()
        self.on_fill_callback = on_fill_callback

        # 连接状态
        self._running = False
        self._ws = None
        self._reconnect_count = 0
        self._max_reconnect = 10
        self._reconnect_delay = 3  # 初始重连延迟（秒）

        # 统计信息
        self._stats = {
            'connected_time': 0.0,
            'last_connect_time': 0.0,
            'total_reconnects': 0,
            'total_fills': 0,
            'last_fill_time': 0.0,
            'connection_errors': 0
        }

    @abstractmethod
    async def connect(self):
        """连接WebSocket"""
        pass

    @abstractmethod
    async def subscribe_user_events(self):
        """订阅用户事件"""
        pass

    @abstractmethod
    def _parse_message(self, message: dict) -> Optional[FillEvent]:
        """解析消息为成交事件"""
        pass

    async def start(self):
        """启动WebSocket监控"""
        if self._running:
            logger.warning(f"⚠️ {self.exchange_code} 成交WebSocket已在运行")
            return

        logger.info(f"🚀 启动 {self.exchange_code} 成交WebSocket监控")
        self._running = True
        self._reconnect_count = 0

        while self._running and self._reconnect_count < self._max_reconnect:
            try:
                await self._connection_loop()

            except Exception as e:
                self._stats['connection_errors'] += 1
                logger.error(f"❌ {self.exchange_code} WebSocket连接异常: {e}")

                self._reconnect_count += 1
                if self._running:
                    await self._handle_reconnect()

        logger.error(f"🛑 {self.exchange_code} WebSocket停止，重连次数: {self._reconnect_count}")

    async def stop(self):
        """停止WebSocket监控"""
        logger.info(f"⏹️ 停止 {self.exchange_code} 成交WebSocket")
        self._running = False

        if self._ws:
            try:
                await self._ws.close()
            except Exception as e:
                logger.warning(f"⚠️ 关闭 {self.exchange_code} WebSocket时异常: {e}")

    async def _connection_loop(self):
        """连接循环"""
        try:
            # 1. 建立连接
            await self.connect()
            self._stats['last_connect_time'] = time.time()
            self._stats['total_reconnects'] = self._reconnect_count

            logger.info(f"✅ {self.exchange_code} WebSocket连接成功")

            # 2. 订阅事件
            await self.subscribe_user_events()

            # 3. 开始监听消息
            await self._listen_messages()

        except Exception as e:
            if self._running:
                logger.error(f"❌ {self.exchange_code} WebSocket连接循环异常: {e}")
            raise

    async def _listen_messages(self):
        """监听WebSocket消息"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            while self._running:
                try:
                    # 使用较短的超时来检查运行状态
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)

                    # 解析消息
                    if isinstance(message, str):
                        data = json.loads(message)
                    else:
                        data = message

                    # 处理消息
                    await self._handle_message(data)

                except asyncio.TimeoutError:
                    # 超时是正常的，用于检查运行状态
                    continue

                except Exception as e:
                    logger.error(f"❌ {self.exchange_code} 处理消息异常: {e}")
                    break

        except Exception as e:
            if self._running:
                logger.error(f"❌ {self.exchange_code} 消息监听循环异常: {e}")
            raise

    async def _handle_message(self, data: dict):
        """处理收到的消息"""
        try:
            # 解析成交事件
            fill_event = self._parse_message(data)

            if fill_event:
                self._stats['total_fills'] += 1
                self._stats['last_fill_time'] = fill_event.timestamp

                # 调用回调函数
                try:
                    self.on_fill_callback(fill_event)
                except Exception as e:
                    logger.error(f"❌ {self.exchange_code} 处理成交事件回调异常: {e}")

        except Exception as e:
            logger.error(f"❌ {self.exchange_code} 处理消息异常: {e}")

    async def _handle_reconnect(self):
        """处理重连逻辑"""
        if not self._running:
            return

        # 计算重连延迟（指数退避）
        delay = min(self._reconnect_delay * (2 ** self._reconnect_count), 30)

        logger.info(f"🔄 {self.exchange_code} 将在 {delay:.1f}秒后重连 (第{self._reconnect_count}次)")

        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            logger.info(f"👋 {self.exchange_code} 重连被取消")
            raise

    def get_stats(self) -> dict:
        """获取连接统计信息"""
        stats = self._stats.copy()

        if stats['last_connect_time'] > 0:
            stats['connected_duration'] = time.time() - stats['last_connect_time']

        if stats['total_fills'] > 0:
            stats['fills_per_hour'] = stats['total_fills'] / max(stats['connected_duration'] / 3600, 0.01)
        else:
            stats['fills_per_hour'] = 0.0

        return stats

    def get_status_report(self) -> str:
        """获取状态报告"""
        stats = self.get_stats()

        report = f"📊 {self.exchange_code.upper()} 成交WebSocket状态\n"
        report += f"  • 运行状态: {'🟢 运行中' if self._running else '🔴 已停止'}\n"
        report += f"  • 重连次数: {stats['total_reconnects']}\n"
        report += f"  • 连接错误: {stats['connection_errors']}\n"

        if stats['last_connect_time'] > 0:
            duration = stats.get('connected_duration', 0)
            report += f"  • 连接时长: {duration/60:.1f}分钟\n"

        report += f"  • 总成交数: {stats['total_fills']}\n"
        report += f"  • 成交频率: {stats['fills_per_hour']:.1f}/小时\n"

        if stats['last_fill_time'] > 0:
            last_fill_age = time.time() - stats['last_fill_time']
            report += f"  • 最后成交: {last_fill_age/60:.1f}分钟前\n"

        return report

    def __str__(self) -> str:
        """字符串表示"""
        return f"FillWebSocketStream({self.exchange_code}, running={self._running})"

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()


class FillWebSocketError(Exception):
    """成交WebSocket异常"""

    def __init__(self, exchange_code: str, message: str, original_error: Exception = None):
        self.exchange_code = exchange_code
        self.message = message
        self.original_error = original_error
        super().__init__(f"[{exchange_code}] {message}")


class FillWebSocketTimeoutError(FillWebSocketError):
    """成交WebSocket超时异常"""
    pass


class FillWebSocketConnectionError(FillWebSocketError):
    """成交WebSocket连接异常"""
    pass