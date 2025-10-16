# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : okx_position_stream
@Description : OKX仓位WebSocket流实现
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
from cex_tools.exchange_model.position_model import OkxPositionDetail
from cex_tools.exchange_model.position_event_model import PositionEventType


class OkxPositionWebSocket(PositionWebSocketStream):
    """OKX仓位WebSocket流实现"""

    def __init__(self, api_key: str = None, secret: str = None, passphrase: str = None,
                 sandbox: bool = False, **kwargs):
        """
        初始化OKX仓位WebSocket流

        Args:
            api_key: API密钥
            secret: API密钥
            passphrase: API passphrase
            sandbox: 是否使用沙盒环境
            **kwargs: 其他配置参数
        """
        super().__init__("OKX", kwargs.get('on_position_callback'))
        self.api_key = api_key
        self.secret = secret
        self.passphrase = passphrase
        self.sandbox = sandbox

        # OKX WebSocket地址
        self.ws_url = (
            "wss://wspap.okx.com:8443/ws/v5/public" if sandbox
            else "wss://ws.okx.com:8443/ws/v5/public"
        )

        # 私有数据需要使用不同的端点
        self.private_ws_url = (
            "wss://wspap.okx.com:8443/ws/v5/private" if sandbox
            else "wss://ws.okx.com:8443/ws/v5/private"
        )

        self._ws_connection = None
        self._listen_task: Optional[asyncio.Task] = None
        self._login_sent = False

    def _generate_signature(self, timestamp: str, method: str = "GET",
                           request_path: str = "/users/self/verify") -> str:
        """
        生成API签名

        Args:
            timestamp: 时间戳
            method: 请求方法
            request_path: 请求路径

        Returns:
            str: base64编码的签名
        """
        try:
            if not self.secret:
                return ""

            message = timestamp + method + request_path
            signature = hmac.new(
                self.secret.encode('utf-8'),
                message.encode('utf-8'),
                digestmod=hashlib.sha256
            ).digest()

            return base64.b64encode(signature).decode('utf-8')
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 生成签名异常: {e}")
            return ""

    async def _send_login_message(self):
        """
        发送登录消息
        """
        try:
            timestamp = str(time.time())
            signature = self._generate_signature(timestamp)

            login_msg = {
                "op": "login",
                "args": [
                    {
                        "apiKey": self.api_key,
                        "passphrase": self.passphrase,
                        "timestamp": timestamp,
                        "sign": signature
                    }
                ]
            }

            await self._ws_connection.send(json.dumps(login_msg))
            logger.debug(f"[{self.exchange_code}] 发送登录消息")
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 发送登录消息失败: {e}")

    async def _send_subscription_message(self):
        """
        发送仓位订阅消息
        """
        try:
            subscribe_msg = {
                "op": "subscribe",
                "args": [{
                        "channel": "account"
                    },
                    {
                        "channel": "positions",
                        "instType": "SWAP"  # 合约类型
                    }, {
                        "channel": "orders",
                        "instType": "SWAP"  # 合约类型
                    }
                ]
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
            event = message.get("event")

            if event == "login":
                # 登录响应
                code = message.get("code")
                if code == "0":
                    logger.info(f"[{self.exchange_code}] 登录成功")
                    self._login_sent = True
                    # 登录成功后发送订阅消息
                    await self._send_subscription_message()
                else:
                    logger.error(f"[{self.exchange_code}] 登录失败: {message.get('msg')}")

            elif event == "subscribe":
                # 订阅响应
                channel = message.get("arg", {}).get("channel")
                if channel == "positions":
                    logger.info(f"[{self.exchange_code}] {channel} 订阅成功")
                else:
                    logger.error(f"[{self.exchange_code}] {channel} 订阅失败: {message.get('msg')}")

            elif message.get("arg", {}).get("channel") == "positions":
                # 仓位数据更新
                data = message.get("data", [])
                if data:
                    logger.debug(f"[{self.exchange_code}] 收到 {len(data)} 个仓位更新")
                    await self._handle_positions_update(data)

            elif message.get("op") == "error":
                # 错误消息
                logger.error(f"[{self.exchange_code}] 收到错误消息: {message}")

            else:
                logger.debug(f"[{self.exchange_code}] 未知消息类型: {message}")

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理消息异常: {e}")

    async def _handle_positions_update(self, positions_data: list):
        """
        处理仓位更新

        Args:
            positions_data: 仓位数据列表

        OKX仓位数据格式:
        [
            {
                "instId": "BTC-USDT-SWAP",
                "instType": "SWAP",
                "mgnMode": "cross",
                "pos": "0.1",
                "baseBal": "0",
                "quoteBal": "0",
                "posCcy": "BTC",
                "availPos": "0.1",
                "avgPx": "50000",
                "upl": "100",
                "uplRatio": "0.002",
                "instType": "SWAP",
                "lever": "10",
                "liqPx": "45000",
                "markPx": "50100",
                "imr": "500",
                "margin": "500",
                "mgnMode": "cross",
                "ccy": "",
                "notionalUsd": "5000",
                "adl": "1",
                "cTime": "1234567890000",
                "uTime": "1234567890000"
            }
        ]
        """
        try:
            for position_data in positions_data:
                # 过滤掉空仓位
                if float(position_data.get("pos", 0)) == 0:
                    continue

                # 转换为标准格式
                position_detail = self._convert_okx_position(position_data)
                self._on_position_update(position_detail)

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理仓位更新异常: {e}")

    def _convert_okx_position(self, position_data: dict) -> OkxPositionDetail:
        """
        转换OKX仓位数据格式

        Args:
            position_data: OKX原始仓位数据

        Returns:
            OkxPositionDetail: 标准化的仓位详情
        """
        try:
            # 确保exchange_code字段
            position_data['exchange_code'] = self.exchange_code
            return OkxPositionDetail(position_data, exchange_code=self.exchange_code)
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 转换仓位数据异常: {e}")
            # 返回一个空的仓位对象
            empty_position = OkxPositionDetail({})
            empty_position.exchange_code = self.exchange_code
            return empty_position

    async def _listen_websocket(self):
        """持续监听 WebSocket 消息（支持自动重连）"""
        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）

        while self._running:
            try:
                if retry_count == 0:
                    logger.debug(f"[{self.exchange_code}] 连接到私有数据流: {self.private_ws_url}")
                else:
                    logger.debug(f"[{self.exchange_code}] 重连私有数据流 (第{retry_count}次)")

                async with websockets.connect(
                    self.private_ws_url,
                    ping_interval=20,
                    ping_timeout=5
                ) as websocket:
                    self._ws_connection = websocket
                    logger.debug(f"[{self.exchange_code}] WebSocket 连接成功")

                    # 发送登录消息
                    await self._send_login_message()

                    # 重连成功，重置计数器
                    if retry_count > 0:
                        logger.debug(f"[{self.exchange_code}] 重连成功！")
                    retry_count = 0
                    self._login_sent = False

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

                # 连接失败，重置登录状态
                self._login_sent = False

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
                self._login_sent = False

        logger.debug(f"[{self.exchange_code}] WebSocket监听线程退出")

    async def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            logger.warning(f"[{self.exchange_code}] 仓位WebSocket 已在运行")
            return

        if not all([self.api_key, self.secret, self.passphrase]):
            logger.error(f"[{self.exchange_code}] 缺少必要的API凭据，无法启动私有数据流")
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