# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : bybit_position_stream
@Description : Bybit仓位WebSocket流实现
@Time        : 2025/10/15
"""
import asyncio
import json
import time
import websockets
from loguru import logger
from typing import Optional
import hashlib
import hmac
import base64

from cex_tools.exchange_ws.position_stream import PositionWebSocketStream
from cex_tools.exchange_model.position_model import BybitPositionDetail
from cex_tools.exchange_model.position_event_model import PositionEventType


class BybitPositionWebSocket(PositionWebSocketStream):
    """Bybit仓位WebSocket流实现"""

    def __init__(self, api_key: str = None, secret: str = None, testnet: bool = False, **kwargs):
        """
        初始化Bybit仓位WebSocket流

        Args:
            api_key: API密钥
            secret: API密钥
            testnet: 是否使用测试网
            **kwargs: 其他配置参数
        """
        super().__init__("Bybit", kwargs.get('on_position_callback'))
        self.api_key = api_key
        self.secret = secret
        self.testnet = testnet

        # Bybit WebSocket地址
        if testnet:
            self.ws_url = "wss://stream-testnet.bybit.com/v5/private"
        else:
            self.ws_url = "wss://stream.bybit.com/v5/private"

        self._ws_connection = None
        self._listen_task: Optional[asyncio.Task] = None
        self._auth_sent = False

    def _generate_signature(self, expires: int, api_key: str, secret_key: str) -> str:
        """
        生成API签名

        Args:
            expires: 过期时间戳
            api_key: API密钥
            secret_key: 密钥

        Returns:
            str: 十六进制签名字符串
        """
        try:
            message = f"GET/realtime{expires}"
            signature = hmac.new(
                secret_key.encode('utf-8'),
                message.encode('utf-8'),
                digestmod=hashlib.sha256
            ).hexdigest()
            return signature
        except Exception as e:
            logger.error(f"[{self.exchange_name}] 生成签名异常: {e}")
            return ""

    async def _send_auth_message(self):
        """
        发送认证消息
        """
        try:
            expires = int(time.time() * 1000) + 5000  # 5秒后过期
            signature = self._generate_signature(expires, self.api_key, self.secret)

            auth_msg = {
                "op": "auth",
                "args": [self.api_key, expires, signature]
            }

            await self._ws_connection.send(json.dumps(auth_msg))
            logger.debug(f"[{self.exchange_name}] 发送认证消息")
        except Exception as e:
            logger.error(f"[{self.exchange_name}] 发送认证消息失败: {e}")

    async def _send_subscription_message(self):
        """
        发送仓位订阅消息
        """
        try:
            subscribe_msg = {
                "op": "subscribe",
                "args": [
                    "position.linear"  # 线性合约仓位
                ]
            }

            await self._ws_connection.send(json.dumps(subscribe_msg))
            logger.debug(f"[{self.exchange_name}] 发送仓位订阅消息")
        except Exception as e:
            logger.error(f"[{self.exchange_name}] 发送订阅消息失败: {e}")

    async def _handle_message(self, message: dict):
        """
        处理WebSocket消息

        Args:
            message: WebSocket消息数据
        """
        try:
            topic = message.get("topic")

            if topic is None and message.get("op") == "auth":
                # 认证响应
                if message.get("success") is True:
                    logger.info(f"[{self.exchange_name}] 认证成功")
                    self._auth_sent = True
                    # 认证成功后发送订阅消息
                    await self._send_subscription_message()
                else:
                    logger.error(f"[{self.exchange_name}] 认证失败: {message.get('ret_msg')}")

            elif topic == "position.linear":
                # 仓位数据更新
                data = message.get("data", [])
                if data:
                    logger.debug(f"[{self.exchange_name}] 收到 {len(data)} 个仓位更新")
                    await self._handle_positions_update(data)

            elif topic is None and message.get("op") == "subscribe":
                # 订阅响应
                if message.get("success") is True:
                    logger.info(f"[{self.exchange_name}] 仓位订阅成功")
                else:
                    logger.error(f"[{self.exchange_name}] 仓位订阅失败: {message.get('ret_msg')}")

            elif topic is None and message.get("op") == "ping":
                # ping消息，回复pong
                pong_msg = {"op": "pong"}
                await self._ws_connection.send(json.dumps(pong_msg))
                logger.debug(f"[{self.exchange_name}] 回复pong")

            else:
                logger.debug(f"[{self.exchange_name}] 未知消息: {topic}")

        except Exception as e:
            logger.error(f"[{self.exchange_name}] 处理消息异常: {e}")

    async def _handle_positions_update(self, positions_data: list):
        """
        处理仓位更新

        Args:
            positions_data: 仓位数据列表

        Bybit仓位数据格式:
        [
            {
                "topic": "position.linear",
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "positionIdx": 0,
                        "riskId": 1,
                        "riskLimitValue": "20000",
                        "leverage": "10",
                        "positionBalance": "0.001",
                        "liqPrice": "45000",
                        "bustPrice": "44999",
                        "takeProfit": "0",
                        "stopLoss": "0",
                        "trailingStop": "0",
                        "unrealisedPnl": "10",
                        "createdTime": "1234567890000",
                        "updatedTime": "1234567890000",
                        "tpslMode": "Full",
                        "adlRankIndicator": 1,
                        "isReduceOnly": false,
                        "positionIM": "500",
                        "positionMM": "50",
                        "currency": "USDT",
                        "leverageSYS": "10",
                        "positionIdxSYS": 0
                    }
                ]
            }
        ]
        """
        try:
            for position_data in positions_data:
                # 过滤掉空仓位（需要根据实际情况判断）
                # Bybit可能通过positionBalance或unrealisedPnl判断是否有仓位
                position_balance = float(position_data.get("positionBalance", 0))
                if position_balance == 0:
                    # 这可能是一个平仓消息，需要创建空仓位对象
                    position_detail = self._convert_bybit_position(position_data)
                    self._on_position_update(position_detail, PositionEventType.CLOSE)
                else:
                    # 有仓位的更新
                    position_detail = self._convert_bybit_position(position_data)
                    self._on_position_update(position_detail)

        except Exception as e:
            logger.error(f"[{self.exchange_name}] 处理仓位更新异常: {e}")

    def _convert_bybit_position(self, position_data: dict) -> BybitPositionDetail:
        """
        转换Bybit仓位数据格式

        Args:
            position_data: Bybit原始仓位数据

        Returns:
            BybitPositionDetail: 标准化的仓位详情
        """
        try:
            # 确保exchange_code字段
            position_data['exchange_code'] = self.exchange_name
            return BybitPositionDetail(position_data, exchange_code=self.exchange_name)
        except Exception as e:
            logger.error(f"[{self.exchange_name}] 转换仓位数据异常: {e}")
            # 返回一个空的仓位对象
            empty_position = BybitPositionDetail({})
            empty_position.exchange_code = self.exchange_name
            return empty_position

    async def _listen_websocket(self):
        """持续监听 WebSocket 消息（支持自动重连）"""
        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）

        while self._running:
            try:
                if retry_count == 0:
                    logger.debug(f"[{self.exchange_name}] 连接到私有数据流: {self.ws_url}")
                else:
                    logger.debug(f"[{self.exchange_name}] 重连私有数据流 (第{retry_count}次)")

                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=5
                ) as websocket:
                    self._ws_connection = websocket
                    logger.debug(f"[{self.exchange_name}] WebSocket 连接成功")

                    # 发送认证消息
                    await self._send_auth_message()

                    # 重连成功，重置计数器
                    if retry_count > 0:
                        logger.debug(f"[{self.exchange_name}] 重连成功！")
                    retry_count = 0
                    self._auth_sent = False

                    # 持续接收消息
                    while self._running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=10)
                            data = json.loads(message)
                            await self._handle_message(data)

                        except asyncio.TimeoutError:
                            logger.debug(f"[{self.exchange_name}] WebSocket 超时")
                            # Bybit会自动处理ping/pong，不需要手动发送
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"[{self.exchange_name}] WebSocket 连接关闭，准备重连...")
                            break
                        except Exception as e:
                            logger.error(f"[{self.exchange_name}] 处理消息异常: {e}")
                            break

            except Exception as e:
                if not self._running:
                    logger.debug(f"[{self.exchange_name}] WebSocket已主动停止")
                    return

                # 连接失败，重置认证状态
                self._auth_sent = False

                if "no pong" in str(e) or "ConnectionClosed" in str(type(e).__name__):
                    logger.warning(f"[{self.exchange_name}] WebSocket连接断开: {e}")
                else:
                    logger.error(f"[{self.exchange_name}] WebSocket 连接异常: {e}")

                retry_count += 1

                # 等待后重连
                if self._running:
                    logger.debug(f"[{self.exchange_name}] {retry_delay}秒后重连...")
                    await asyncio.sleep(retry_delay)

            finally:
                self._ws_connection = None
                self._auth_sent = False

        logger.debug(f"[{self.exchange_name}] WebSocket监听线程退出")

    async def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            logger.warning(f"[{self.exchange_name}] 仓位WebSocket 已在运行")
            return

        if not all([self.api_key, self.secret]):
            logger.error(f"[{self.exchange_name}] 缺少必要的API凭据，无法启动私有数据流")
            return

        self._running = True

        # 启动监听任务
        self._listen_task = asyncio.create_task(self._listen_websocket())

        logger.debug(f"[{self.exchange_name}] 仓位WebSocket 已启动")

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

        logger.debug(f"[{self.exchange_name}] 仓位WebSocket 已停止")