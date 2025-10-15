# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : hyperliquid_fill_websocket
@Description : HyperLiquid成交WebSocket实现
@Time        : 2025/10/15
"""
import asyncio
import json
import time
import websockets
from typing import Optional, Callable
from loguru import logger

from cex_tools.exchange_ws.fill_websocket_stream import FillWebSocketStream, FillWebSocketError, FillWebSocketConnectionError
from cex_tools.exchange_model.fill_event_model import FillEvent


class HyperliquidFillWebSocket(FillWebSocketStream):
    """HyperLiquid成交WebSocket实现"""

    def __init__(self, address: str, on_fill_callback: Callable[[FillEvent], None]):
        """
        初始化HyperLiquid成交WebSocket

        Args:
            address: 用户钱包地址
            on_fill_callback: 成交事件回调函数
        """
        super().__init__("hyperliquid", on_fill_callback)
        self.address = address.lower()

        # HyperLiquid WebSocket端点
        self.ws_url = "wss://api.hyperliquid.xyz/ws"

        # 订阅管理
        self._subscribed = False

    async def connect(self):
        """连接WebSocket"""
        try:
            logger.debug(f"🔌 连接HyperLiquid WebSocket: {self.ws_url}")

            self._ws = await websockets.connect(
                self.ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )

            # 重置重连计数
            if self._reconnect_count > 0:
                logger.info(f"✅ HyperLiquid WebSocket重连成功")

        except Exception as e:
            raise FillWebSocketConnectionError("hyperliquid", f"连接失败: {str(e)}", e)

    async def subscribe_user_events(self):
        """订阅用户事件"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            # 订阅用户事件
            subscribe_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "userEvents"
                }
            }

            await self._ws.send(json.dumps(subscribe_msg))
            logger.debug("✅ HyperLiquid用户事件订阅请求已发送")

            # 等待订阅确认
            await asyncio.wait_for(self._wait_for_subscription_confirmation(), timeout=10.0)

        except asyncio.TimeoutError:
            raise FillWebSocketError("hyperliquid", "订阅用户事件超时")
        except Exception as e:
            raise FillWebSocketError("hyperliquid", f"订阅用户事件失败: {str(e)}", e)

    async def _wait_for_subscription_confirmation(self):
        """等待订阅确认"""
        try:
            while not self._subscribed and self._running:
                try:
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)
                    data = json.loads(message)

                    if data.get('channel') == 'subscribed':
                        self._subscribed = True
                        logger.debug("✅ HyperLiquid用户事件订阅确认")
                        break

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            logger.error(f"❌ 等待HyperLiquid订阅确认异常: {e}")
            raise

    def _parse_message(self, message: dict) -> Optional[FillEvent]:
        """解析HyperLiquid WebSocket消息为成交事件"""
        try:
            channel = message.get('channel', '')

            if channel == 'userEvents':
                return self._parse_user_events(message)
            elif channel == 'error':
                error_msg = message.get('message', '未知错误')
                logger.error(f"❌ HyperLiquid WebSocket错误: {error_msg}")
                return None
            else:
                # 其他频道，如订单簿等
                logger.debug(f"📝 HyperLiquid收到其他频道: {channel}")
                return None

        except Exception as e:
            logger.error(f"❌ 解析HyperLiquid消息失败: {e}")
            logger.debug(f"原始消息: {message}")
            return None

    def _parse_user_events(self, message: dict) -> Optional[FillEvent]:
        """解析用户事件消息"""
        try:
            data = message.get('data', {})

            # 检查是否有成交事件
            if 'fills' in data:
                fills = data['fills']
                if not isinstance(fills, list):
                    fills = [fills]

                # 处理每个成交事件
                for fill in fills:
                    fill_event = self._parse_single_fill(fill)
                    if fill_event:
                        return fill_event

            # 检查是否有订单更新事件
            if 'orderEvents' in data:
                order_events = data['orderEvents']
                if not isinstance(order_events, list):
                    order_events = [order_events]

                for order_event in order_events:
                    if order_event.get('status') == 'filled':
                        fill_event = self._parse_order_event(order_event)
                        if fill_event:
                            return fill_event

            return None

        except Exception as e:
            logger.error(f"❌ 解析HyperLiquid用户事件失败: {e}")
            return None

    def _parse_single_fill(self, fill: dict) -> Optional[FillEvent]:
        """解析单个成交事件"""
        try:
            # 提取成交信息
            coin = fill.get('coin', '')
            side = fill.get('side', '')
            oid = str(fill.get('oid', ''))
            px = float(fill.get('px', 0))
            sz = float(fill.get('sz', 0))
            hash_val = fill.get('hash', '')
            time_val = fill.get('time', 0)

            # 验证必要字段
            if not all([coin, side, oid, px > 0, sz > 0, time_val > 0]):
                logger.warning(f"⚠️ HyperLiquid成交事件缺少必要字段: {fill}")
                return None

            # 转换时间戳（毫秒转秒）
            timestamp = time_val / 1000

            # 构造symbol（添加USDT后缀）
            symbol = coin + 'USDT'

            fill_event = FillEvent(
                exchange_code='hyperliquid',
                symbol=symbol,
                order_id=oid,
                side=side.upper(),
                filled_quantity=sz,
                filled_price=px,
                trade_id=hash_val,
                timestamp=timestamp,
                commission=0.0,  # HyperLiquid成交事件中可能不包含手续费信息
                commission_asset=''
            )

            logger.debug(f"📈 HyperLiquid成交事件解析成功: {fill_event}")
            return fill_event

        except Exception as e:
            logger.error(f"❌ 解析HyperLiquid单个成交失败: {e}")
            return None

    def _parse_order_event(self, order_event: dict) -> Optional[FillEvent]:
        """解析订单事件（作为成交事件的补充）"""
        try:
            # 订单事件中的成交信息通常不如fills事件详细
            # 这里只作为备用解析方式
            coin = order_event.get('coin', '')
            side = order_event.get('side', '')
            oid = str(order_event.get('oid', ''))
            px = float(order_event.get('px', 0))
            sz = float(order_event.get('sz', 0))
            status = order_event.get('status', '')
            time_val = order_event.get('time', 0)

            if status != 'filled':
                return None

            # 验证必要字段
            if not all([coin, side, oid, px > 0, sz > 0, time_val > 0]):
                return None

            timestamp = time_val / 1000
            symbol = coin + 'USDT'

            fill_event = FillEvent(
                exchange_code='hyperliquid',
                symbol=symbol,
                order_id=oid,
                side=side.upper(),
                filled_quantity=sz,
                filled_price=px,
                trade_id=f"order_{oid}",
                timestamp=timestamp
            )

            logger.debug(f"📈 HyperLiquid订单事件解析成功: {fill_event}")
            return fill_event

        except Exception as e:
            logger.error(f"❌ 解析HyperLiquid订单事件失败: {e}")
            return None

    async def _listen_messages(self):
        """重写消息监听，添加订阅状态检查"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            while self._running:
                try:
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
                    logger.error(f"❌ HyperLiquid处理消息异常: {e}")
                    break

        except Exception as e:
            if self._running:
                logger.error(f"❌ HyperLiquid消息监听循环异常: {e}")
            raise

    async def _handle_message(self, data: dict):
        """重写消息处理，添加订阅状态检查"""
        channel = data.get('channel', '')

        # 检查订阅确认
        if channel == 'subscribed':
            subscription = data.get('subscription', {})
            if subscription.get('type') == 'userEvents':
                self._subscribed = True
                logger.debug("✅ HyperLiquid用户事件订阅确认")

        # 检查错误消息
        elif channel == 'error':
            error_msg = data.get('message', '未知错误')
            logger.error(f"❌ HyperLiquid WebSocket错误: {error_msg}")

        # 处理其他消息
        else:
            await super()._handle_message(data)

    async def stop(self):
        """停止WebSocket连接"""
        await super().stop()

        # 重置订阅状态
        self._subscribed = False

    def get_stats(self) -> dict:
        """获取连接统计信息"""
        stats = super().get_stats()
        stats['subscribed'] = self._subscribed
        return stats

    def get_status_report(self) -> str:
        """获取状态报告"""
        stats = self.get_stats()

        report = f"📊 HYPERLIQUID 成交WebSocket状态\n"
        report += f"  • 运行状态: {'🟢 运行中' if self._running else '🔴 已停止'}\n"
        report += f"  • 订阅状态: {'🟢 已订阅' if stats['subscribed'] else '🔴 未订阅'}\n"
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