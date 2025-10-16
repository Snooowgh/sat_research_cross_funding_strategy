# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : aster_position_stream
@Description : Aster仓位WebSocket流实现
@Time        : 2025/10/15
"""
import asyncio
import json
import time
import websockets
from loguru import logger
from typing import Optional

from cex_tools.exchange_ws.position_stream import PositionWebSocketStream
from cex_tools.exchange_model.position_model import BinancePositionDetail  # 使用Binance模型作为基础
from cex_tools.exchange_model.position_event_model import PositionEventType


class AsterPositionWebSocket(PositionWebSocketStream):
    """Aster仓位WebSocket流实现"""

    def __init__(self, api_key: str = None, secret: str = None, **kwargs):
        """
        初始化Aster仓位WebSocket流

        Args:
            api_key: API密钥
            secret: API密钥
            **kwargs: 其他配置参数
        """
        super().__init__("Aster", kwargs.get('on_position_callback'))
        self.api_key = api_key
        self.secret = secret

        # Aster WebSocket地址（需要根据实际API文档确认）
        self.ws_url = "wss://api.aster.exchange/ws"  # 示例URL，需要替换为实际地址

        self._ws_connection = None
        self._listen_task: Optional[asyncio.Task] = None
        self._auth_sent = False

    async def _send_auth_message(self):
        """
        发送认证消息
        """
        try:
            # Aster认证消息格式（需要根据实际API文档调整）
            auth_msg = {
                "method": "auth",
                "params": {
                    "api_key": self.api_key,
                    "timestamp": int(time.time()),
                    "signature": self._generate_signature()  # 需要实现签名算法
                }
            }

            await self._ws_connection.send(json.dumps(auth_msg))
            logger.debug(f"[{self.exchange_code}] 发送认证消息")
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 发送认证消息失败: {e}")

    def _generate_signature(self) -> str:
        """
        生成API签名
        需要根据Aster的具体签名算法实现
        """
        # TODO: 实现Aster的签名算法
        # 这里只是一个占位符
        return "signature_placeholder"

    async def _send_subscription_message(self):
        """
        发送仓位订阅消息
        """
        try:
            # Aster订阅消息格式（需要根据实际API文档调整）
            subscribe_msg = {
                "method": "subscribe",
                "params": {
                    "channel": "positions"
                }
            }

            await self._ws_connection.send(json.dumps(subscribe_msg))
            logger.debug(f"[{self.exchange_code}] 发送仓位订阅消息")
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 发送订阅消息失败: {e}")

    async def _handle_message(self, message: dict):
        """
        处理WebSocket消息

        Args:
            message: WebSocket消息数据
        """
        try:
            method = message.get("method")

            if method == "auth":
                # 认证响应
                if message.get("result") is True:
                    logger.info(f"[{self.exchange_code}] 认证成功")
                    self._auth_sent = True
                    # 认证成功后发送订阅消息
                    await self._send_subscription_message()
                else:
                    logger.error(f"[{self.exchange_code}] 认证失败: {message.get('error')}")

            elif method == "subscribe":
                # 订阅响应
                if message.get("result") is True:
                    logger.info(f"[{self.exchange_code}] 仓位订阅成功")
                else:
                    logger.error(f"[{self.exchange_code}] 仓位订阅失败: {message.get('error')}")

            elif message.get("channel") == "positions":
                # 仓位数据更新
                data = message.get("data", [])
                if data:
                    logger.debug(f"[{self.exchange_code}] 收到 {len(data)} 个仓位更新")
                    await self._handle_positions_update(data)

            elif method == "ping":
                # ping消息，回复pong
                pong_msg = {"method": "pong"}
                await self._ws_connection.send(json.dumps(pong_msg))
                logger.debug(f"[{self.exchange_code}] 回复pong")

            else:
                logger.debug(f"[{self.exchange_code}] 未知消息类型: {method}")

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理消息异常: {e}")

    async def _handle_positions_update(self, positions_data: list):
        """
        处理仓位更新

        Args:
            positions_data: 仓位数据列表

        Aster仓位数据格式需要根据实际API确认
        这里假设有一个标准格式
        """
        try:
            for position_data in positions_data:
                # 过滤掉空仓位
                if float(position_data.get("size", 0)) == 0:
                    continue

                # 转换为标准格式
                position_detail = self._convert_aster_position(position_data)
                self._on_position_update(position_detail)

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理仓位更新异常: {e}")

    def _convert_aster_position(self, position_data: dict) -> BinancePositionDetail:
        """
        转换Aster仓位数据格式

        Args:
            position_data: Aster原始仓位数据

        Returns:
            BinancePositionDetail: 标准化的仓位详情
        """
        try:
            # 将Aster仓位数据转换为BinancePositionDetail格式
            # 这里需要根据Aster的实际数据格式进行转换
            converted_data = {
                "symbol": position_data.get("symbol", ""),
                "positionAmt": position_data.get("size", 0),
                "entryPrice": position_data.get("entry_price", 0),
                "markPrice": position_data.get("mark_price", 0),
                "unRealizedProfit": position_data.get("unrealized_pnl", 0),
                "notional": position_data.get("notional", 0),
                "leverage": position_data.get("leverage", 1),
                "liquidationPrice": position_data.get("liquidation_price", 0),
                "updateTime": position_data.get("timestamp", int(time.time() * 1000))
            }

            return BinancePositionDetail(converted_data, exchange_code=self.exchange_code)

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 转换仓位数据异常: {e}")
            # 返回一个空的仓位对象
            empty_position = BinancePositionDetail({})
            empty_position.exchange_code = self.exchange_code
            return empty_position

    async def _listen_websocket(self):
        """持续监听 WebSocket 消息（支持自动重连）"""
        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）

        while self._running:
            try:
                if retry_count == 0:
                    logger.debug(f"[{self.exchange_code}] 连接到私有数据流: {self.ws_url}")
                else:
                    logger.debug(f"[{self.exchange_code}] 重连私有数据流 (第{retry_count}次)")

                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=5
                ) as websocket:
                    self._ws_connection = websocket
                    logger.debug(f"[{self.exchange_code}] WebSocket 连接成功")

                    # 发送认证消息
                    await self._send_auth_message()

                    # 重连成功，重置计数器
                    if retry_count > 0:
                        logger.debug(f"[{self.exchange_code}] 重连成功！")
                    retry_count = 0
                    self._auth_sent = False

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

                # 连接失败，重置认证状态
                self._auth_sent = False

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
                self._auth_sent = False

        logger.debug(f"[{self.exchange_code}] WebSocket监听线程退出")

    async def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            logger.warning(f"[{self.exchange_code}] 仓位WebSocket 已在运行")
            return

        if not all([self.api_key, self.secret]):
            logger.error(f"[{self.exchange_code}] 缺少必要的API凭据，无法启动私有数据流")
            return

        logger.warning(f"[{self.exchange_code}] Aster仓位WebSocket流是基础实现，可能需要根据实际API调整")

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