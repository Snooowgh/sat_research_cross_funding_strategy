# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : binance_position_stream
@Description : Binance仓位WebSocket流实现（使用官方binance-futures-connector库）
@Time        : 2025/10/15
"""
import asyncio
import json
import time
import websockets
from loguru import logger
from typing import Optional

from cex_tools.exchange_ws.position_stream import PositionWebSocketStream
from cex_tools.exchange_model.position_model import BinancePositionDetail
from cex_tools.exchange_model.position_event_model import PositionEventType

from binance.um_futures import UMFutures


class BinancePositionWebSocket(PositionWebSocketStream):
    """Binance仓位WebSocket流实现（使用官方binance-futures-connector库）"""

    def __init__(self, api_key: str = None, secret: str = None, testnet: bool = False, **kwargs):
        """
        初始化Binance仓位WebSocket流

        Args:
            api_key: API密钥
            secret: API密钥
            testnet: 是否使用测试网
            **kwargs: 其他配置参数
        """
        super().__init__("Binance", kwargs.get('on_position_callback'))
        self.api_key = api_key
        self.secret = secret
        self.testnet = testnet

        # 创建UMFutures客户端
        if testnet:
            self.client = UMFutures(
                key=api_key,
                secret=secret,
                base_url="https://testnet.binancefuture.com"
            )
        else:
            self.client = UMFutures(key=api_key, secret=secret)

        self._ws_connection = None
        self._listen_task: Optional[asyncio.Task] = None
        self._listen_key = None
        self._base_url = "wss://stream.binancefuture.com/ws" if testnet else "wss://fstream.binance.com/ws"

    async def _get_listen_key(self) -> Optional[str]:
        """
        获取用户数据流listen key

        Returns:
            str: listen key，失败返回None
        """
        try:
            response = self.client.new_listen_key()
            if response and 'listenKey' in response:
                listen_key = response['listenKey']
                logger.debug(f"[{self.exchange_name}] 获取listen key成功: {listen_key[:10]}...")
                return listen_key
            else:
                logger.error(f"[{self.exchange_name}] 获取listen key失败: {response}")
                return None
        except Exception as e:
            logger.error(f"[{self.exchange_name}] 获取listen key异常: {e}")
            return None

    async def _keep_alive_listen_key(self):
        """
        定期保持listen key活跃（每30分钟）
        """
        try:
            response = self.client.renew_listen_key(self._listen_key)
            if response and response.get('code') == 200:
                logger.debug(f"[{self.exchange_name}] listen key保活成功")
            else:
                logger.warning(f"[{self.exchange_name}] listen key保活失败: {response}")
        except Exception as e:
            logger.error(f"[{self.exchange_name}] listen key保活异常: {e}")

    async def _listen_websocket(self):
        """持续监听 WebSocket 消息（支持自动重连）"""
        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）
        keep_alive_interval = 25 * 60  # 25分钟（比30分钟稍微提前）

        while self._running:
            try:
                # 获取listen key
                if not self._listen_key:
                    self._listen_key = await self._get_listen_key()
                    if not self._listen_key:
                        logger.error(f"[{self.exchange_name}] 无法获取listen key，{retry_delay}秒后重试...")
                        await asyncio.sleep(retry_delay)
                        continue

                # 构建WebSocket URL
                ws_url = f"{self._base_url}/{self._listen_key}"

                if retry_count == 0:
                    logger.debug(f"[{self.exchange_name}] 连接到用户数据流: {ws_url}")
                else:
                    logger.debug(f"[{self.exchange_name}] 重连用户数据流 (第{retry_count}次)")

                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=5) as websocket:
                    self._ws_connection = websocket
                    logger.debug(f"[{self.exchange_name}] WebSocket 连接成功")

                    # 重连成功，重置计数器
                    if retry_count > 0:
                        logger.debug(f"[{self.exchange_name}] 重连成功！")
                    retry_count = 0

                    # 启动listen key保活任务
                    keep_alive_task = asyncio.create_task(self._keep_alive_periodic(keep_alive_interval))

                    try:
                        while self._running:
                            try:
                                message = await asyncio.wait_for(websocket.recv(), timeout=10)
                                data = json.loads(message)

                                # 处理不同类型的消息
                                await self._handle_message(data)

                            except asyncio.TimeoutError:
                                logger.debug(f"[{self.exchange_name}] WebSocket 超时，发送 ping...")
                                await websocket.ping()
                            except websockets.exceptions.ConnectionClosed:
                                logger.warning(f"[{self.exchange_name}] WebSocket 连接关闭，准备重连...")
                                break
                            except Exception as e:
                                logger.error(f"[{self.exchange_name}] 处理消息异常: {e}")
                                break
                    finally:
                        keep_alive_task.cancel()
                        try:
                            await keep_alive_task
                        except asyncio.CancelledError:
                            pass

            except Exception as e:
                if not self._running:
                    logger.debug(f"[{self.exchange_name}] WebSocket已主动停止")
                    return

                # 连接失败，重置listen key
                self._listen_key = None

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

        logger.debug(f"[{self.exchange_name}] WebSocket监听线程退出")

    async def _keep_alive_periodic(self, interval: int):
        """定期保活listen key"""
        while self._running:
            try:
                await asyncio.sleep(interval)
                if self._running:
                    await self._keep_alive_listen_key()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.exchange_name}] 定期保活异常: {e}")

    async def _handle_message(self, data: dict):
        """
        处理WebSocket消息

        Args:
            data: WebSocket消息数据
        """
        try:
            msg_type = data.get("e")

            if msg_type == "ACCOUNT_UPDATE":
                # 账户更新消息，包含仓位信息
                await self._handle_account_update(data)
            elif msg_type == "ORDER_TRADE_UPDATE":
                # 订单更新消息，可能间接影响仓位
                logger.debug(f"[{self.exchange_name}] 收到订单更新: {data.get('o', {}).get('i', '')}")
            else:
                logger.debug(f"[{self.exchange_name}] 收到未知消息类型: {msg_type}")

        except Exception as e:
            logger.error(f"[{self.exchange_name}] 处理消息异常: {e}")

    async def _handle_account_update(self, data: dict):
        """
        处理账户更新消息

        Args:
            data: 账户更新数据
        """
        try:
            # 处理仓位更新
            positions = data.get("a", {}).get("P", [])
            if positions:
                logger.debug(f"[{self.exchange_name}] 收到 {len(positions)} 个仓位更新")
                self._on_positions_update(positions)

        except Exception as e:
            logger.error(f"[{self.exchange_name}] 处理账户更新异常: {e}")

    def _convert_binance_position(self, position_data: dict) -> BinancePositionDetail:
        """
        转换Binance仓位数据格式

        Args:
            position_data: Binance原始仓位数据

        Returns:
            BinancePositionDetail: 标准化的仓位详情
        """
        try:
            return BinancePositionDetail(position_data, exchange_code=self.exchange_name)
        except Exception as e:
            logger.error(f"[{self.exchange_name}] 转换仓位数据异常: {e}")
            # 返回一个空的仓位对象
            empty_position = BinancePositionDetail({})
            return empty_position

    async def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            logger.warning(f"[{self.exchange_name}] 仓位WebSocket 已在运行")
            return

        if not all([self.api_key, self.secret]):
            logger.error(f"[{self.exchange_name}] 缺少API密钥，无法启动用户数据流")
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

        # 清理listen key
        if self._listen_key:
            try:
                await self._close_listen_key()
            except Exception as e:
                logger.error(f"[{self.exchange_name}] 关闭listen key异常: {e}")

        logger.debug(f"[{self.exchange_name}] 仓位WebSocket 已停止")

    async def _close_listen_key(self):
        """关闭listen key"""
        try:
            response = self.client.close_listen_key(self._listen_key)
            if response and response.get('code') == 200:
                logger.debug(f"[{self.exchange_name}] listen key关闭成功")
            else:
                logger.warning(f"[{self.exchange_name}] listen key关闭失败: {response}")
        except Exception as e:
            logger.error(f"[{self.exchange_name}] 关闭listen key异常: {e}")

    def get_status_report(self) -> str:
        """获取状态报告"""
        base_report = super().get_status_report()

        # 添加Binance特有的状态信息
        ws_status = ""
        if self._ws_connection:
            ws_status = f"\n  • WebSocket状态: 🟢 已连接"
        elif self._listen_key:
            ws_status = f"\n  • WebSocket状态: 🟡 已断开"
        else:
            ws_status = f"\n  • WebSocket状态: 🔴 未初始化"

        testnet_status = f"\n  • 网络: {'测试网' if self.testnet else '主网'}"

        return base_report + ws_status + testnet_status