# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : hyperliquid_position_stream
@Description : Hyperliquid仓位WebSocket流实现
@Time        : 2025/10/15
"""
import asyncio
import json
import time
import websockets
from loguru import logger
from typing import Optional

from cex_tools.exchange_ws.position_stream import PositionWebSocketStream
from cex_tools.exchange_model.position_model import HyperliquidPositionDetail
from cex_tools.exchange_model.position_event_model import PositionEventType


class HyperliquidPositionWebSocket(PositionWebSocketStream):
    """Hyperliquid仓位WebSocket流实现"""

    def __init__(self, api_key: str = None, secret: str = None, testnet: bool = False, **kwargs):
        """
        初始化Hyperliquid仓位WebSocket流

        Args:
            api_key: API密钥
            secret: API密钥
            testnet: 是否使用测试网
            **kwargs: 其他配置参数
        """
        super().__init__("Hyperliquid", kwargs.get('on_position_callback'))
        self.api_key = api_key
        self.secret = secret
        self.testnet = testnet

        # Hyperliquid WebSocket地址
        self.ws_url = (
            "wss://api.hyperliquid-testnet.xyz/ws" if testnet
            else "wss://api.hyperliquid.xyz/ws"
        )

        self._ws_connection = None
        self._listen_task: Optional[asyncio.Task] = None
        self._subscription_sent = False

    async def _send_subscription_message(self):
        """
        发送用户数据订阅消息
        """
        try:
            # Hyperliquid用户数据订阅
            subscribe_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "user"
                }
            }

            await self._ws_connection.send(json.dumps(subscribe_msg))
            logger.debug(f"[{self.exchange_code}] 发送用户数据订阅消息")
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 发送订阅消息失败: {e}")

    async def _handle_message(self, message: dict):
        """
        处理WebSocket消息

        Args:
            message: WebSocket消息数据
        """
        try:
            channel = message.get("channel")
            data = message.get("data")

            if channel == "subscriptionResponse":
                # 订阅响应
                method = data.get("method")
                subscription = data.get("subscription", {})
                logger.debug(f"[{self.exchange_code}] 订阅响应: {method} - {subscription}")

                if subscription.get("type") == "user":
                    logger.info(f"[{self.exchange_code}] 用户数据订阅成功")
                    self._subscription_sent = True

            elif channel == "user":
                # 用户数据更新（包含仓位信息）
                await self._handle_user_update(data)

            else:
                logger.debug(f"[{self.exchange_code}] 未知频道: {channel}")

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理消息异常: {e}")

    async def _handle_user_update(self, data: dict):
        """
        处理用户数据更新

        Args:
            data: 用户数据

        Hyperliquid用户数据格式:
        {
            "positions": [
                {
                    "coin": "BTC",
                    "szi": "0.1",
                    "entryPx": "50000",
                    "positionValue": "5000",
                    "unrealizedPnl": "100",
                    "leverage": {"type": "cross", "value": 10},
                    "liquidationPx": "45000",
                    "cumFunding": {"allTime": "50", "sinceOpen": "10"}
                }
            ],
            "marginSummary": {...},
            "time": 1234567890000
        }
        """
        try:
            positions = data.get("positions", [])
            if positions:
                logger.debug(f"[{self.exchange_code}] 收到 {len(positions)} 个仓位更新")
                self._on_positions_update(positions)

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理用户更新异常: {e}")

    def _convert_hyperliquid_position(self, position_data: dict) -> HyperliquidPositionDetail:
        """
        转换Hyperliquid仓位数据格式

        Args:
            position_data: Hyperliquid原始仓位数据

        Returns:
            HyperliquidPositionDetail: 标准化的仓位详情
        """
        try:
            # 确保exchange_code字段
            position_data['exchange_code'] = self.exchange_code
            return HyperliquidPositionDetail(position_data, exchange_code=self.exchange_code)
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 转换仓位数据异常: {e}")
            # 返回一个空的仓位对象
            empty_position = HyperliquidPositionDetail({})
            empty_position.exchange_code = self.exchange_code
            return empty_position

    async def _listen_websocket(self):
        """持续监听 WebSocket 消息（支持自动重连）"""
        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）

        while self._running:
            try:
                if retry_count == 0:
                    logger.debug(f"[{self.exchange_code}] 连接到用户数据流: {self.ws_url}")
                else:
                    logger.debug(f"[{self.exchange_code}] 重连用户数据流 (第{retry_count}次)")

                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=5
                ) as websocket:
                    self._ws_connection = websocket
                    logger.debug(f"[{self.exchange_code}] WebSocket 连接成功")

                    # 发送订阅消息
                    await self._send_subscription_message()

                    # 重连成功，重置计数器
                    if retry_count > 0:
                        logger.debug(f"[{self.exchange_code}] 重连成功！")
                    retry_count = 0
                    self._subscription_sent = False

                    # 持续接收消息
                    while self._running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=10)
                            data = json.loads(message)
                            await self._handle_message(data)

                        except asyncio.TimeoutError:
                            logger.debug(f"[{self.exchange_code}] WebSocket 超时，发送 ping...")
                            await websocket.ping()
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"[{self.exchange_code}] WebSocket 连接关闭，准备重连...")
                            break
                        except Exception as e:
                            logger.error(f"[{self.exchange_code}] 处理消息异常: {e}")
                            break

            except Exception as e:
                if not self._running:
                    logger.debug(f"[{self.exchange_code}] WebSocket已主动停止")
                    return

                # 连接失败，重置订阅状态
                self._subscription_sent = False

                if "no pong" in str(e) or "ConnectionClosed" in str(type(e).__name__):
                    logger.warning(f"[{self.exchange_code}] WebSocket连接断开: {e}")
                else:
                    logger.error(f"[{self.exchange_code}] WebSocket 连接异常: {e}")

                retry_count += 1

                # 等待后重连
                if self._running:
                    logger.debug(f"[{self.exchange_code}] {retry_delay}秒后重连...")
                    await asyncio.sleep(retry_delay)

            finally:
                self._ws_connection = None
                self._subscription_sent = False

        logger.debug(f"[{self.exchange_code}] WebSocket监听线程退出")

    async def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            logger.warning(f"[{self.exchange_code}] 仓位WebSocket 已在运行")
            return

        self._running = True

        # 启动监听任务
        self._listen_task = asyncio.create_task(self._listen_websocket())

        logger.debug(f"[{self.exchange_code}] 仓位WebSocket 已启动")

    async def stop(self):
        """停止 WebSocket 连接"""
        self._running = False

        # 关闭 WebSocket 连接
        if self._ws_connection:
            await self._ws_connection.close()

        # 取消监听任务
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        logger.debug(f"[{self.exchange_code}] 仓位WebSocket 已停止")