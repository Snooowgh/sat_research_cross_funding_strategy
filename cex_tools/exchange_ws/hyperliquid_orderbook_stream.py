# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : hyperliquid_orderbook_stream
@Description : Hyperliquid交易所WebSocket订单簿流监听器
@Time        : 2025/10/3 00:00
"""
import asyncio
import json
import time
import websockets
from loguru import logger
from typing import Optional

from cex_tools.exchange_ws.orderbook_stream import OrderBookStream, OrderBookData


class HyperliquidOrderBookStream(OrderBookStream):
    """Hyperliquid交易所订单簿流监听器"""

    def __init__(self, testnet: bool = False):
        super().__init__("Hyperliquid")
        self.testnet = testnet
        self.ws_url = (
            "wss://api.hyperliquid-testnet.xyz/ws" if testnet
            else "wss://api.hyperliquid.xyz/ws"
        )
        self._ws_connection = None
        self._listen_task: Optional[asyncio.Task] = None

    def subscribe(self, pair: str, callback=None):
        """
        订阅交易对
        :param pair: 交易对，例如 "BTC", "ETH" (Hyperliquid使用币种名称，不是交易对格式)
        """
        # Hyperliquid使用币种名称，如 "BTC", "ETH", "SOL"
        # 如果用户传入 "BTCUSDT"，自动转换为 "BTC"
        if pair.endswith("USDT") or pair.endswith("USD"):
            pair = pair.replace("USDT", "").replace("USD", "")

        super().subscribe(pair, callback)

    def _format_pair_for_display(self, coin: str) -> str:
        """
        格式化币种名称以便显示（统一格式为 XXXUSDT）
        :param coin: 币种名称，如 "BTC"
        :return: 统一格式，如 "BTCUSDT"
        """
        return f"{coin}USDT"

    async def _send_subscribe_message(self, coin: str):
        """
        发送订阅消息
        :param coin: 币种名称，如 "BTC"
        """
        subscribe_msg = {
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": coin
            }
        }

        try:
            await self._ws_connection.send(json.dumps(subscribe_msg))
            logger.debug(f"[{self.exchange_name}] 发送订阅消息: {coin}")
        except Exception as e:
            logger.error(f"[{self.exchange_name}] 发送订阅消息失败 {coin}: {e}")

    async def _handle_message(self, message: dict):
        """
        处理 WebSocket 消息
        :param message: 消息数据
        """
        try:
            channel = message.get("channel")
            data = message.get("data")

            if channel == "subscriptionResponse":
                # 订阅响应
                method = data.get("method")
                subscription = data.get("subscription", {})
                logger.debug(f"[{self.exchange_name}] 订阅响应: {method} - {subscription}")

            elif channel == "l2Book":
                # 订单簿更新
                await self._handle_l2book_update(data)

            else:
                logger.debug(f"[{self.exchange_name}] 未知频道: {channel}")

        except Exception as e:
            logger.error(f"[{self.exchange_name}] 处理消息异常: {e}")
            logger.exception(e)

    async def _handle_l2book_update(self, data: dict):
        """
        处理订单簿更新
        :param data: 订单簿数据

        数据格式:
        {
            "coin": "BTC",
            "levels": [
                [ [{"px": "50000", "sz": "1.5", "n": 3}, ...], // bids
                  [{"px": "50100", "sz": "2.0", "n": 5}, ...] ], // asks
            ],
            "time": 1234567890000
        }
        """
        try:
            coin = data.get("coin")
            levels = data.get("levels", [[],[]])
            timestamp_ms = data.get("time", 0)

            # 只处理订阅的币种
            if coin not in self.orderbook_callbacks:
                return

            # 解析订单簿数据
            # levels[0] = bids (买单), levels[1] = asks (卖单)
            bids_raw = levels[0] if len(levels) > 0 else []
            asks_raw = levels[1] if len(levels) > 1 else []

            # 转换为标准格式 [[price, quantity], ...]
            bids = [[float(level["px"]), float(level["sz"])] for level in bids_raw]
            asks = [[float(level["px"]), float(level["sz"])] for level in asks_raw]

            # 排序（bids从高到低，asks从低到高）
            bids = sorted(bids, key=lambda x: x[0], reverse=True)
            asks = sorted(asks, key=lambda x: x[0])

            # 创建 OrderBookData 对象（使用统一格式的交易对名称）
            display_pair = self._format_pair_for_display(coin)
            orderbook = OrderBookData(
                pair=display_pair,
                bids=bids,
                asks=asks,
                timestamp=timestamp_ms / 1000.0  # 转换为秒
            )

            # 触发回调（使用原始coin作为key）
            # 需要在回调查找时也使用coin
            self._on_orderbook_update_with_coin(coin, orderbook)

        except Exception as e:
            logger.error(f"[{self.exchange_name}] 处理订单簿更新异常: {e}")
            logger.exception(e)

    def _on_orderbook_update_with_coin(self, coin: str, orderbook: OrderBookData):
        """
        触发订单簿更新回调（使用coin作为key）
        :param coin: 币种名称
        :param orderbook: 订单簿数据
        """
        # 更新缓存（使用display pair）
        self.latest_orderbooks[orderbook.pair] = orderbook

        # 触发用户回调（使用coin查找）
        callbacks = self.orderbook_callbacks.get(coin, [])
        for callback in callbacks:
            try:
                callback(orderbook)
            except Exception as e:
                logger.error(f"[{self.exchange_name}] 订单簿回调异常: {e}")

    async def _listen_websocket(self):
        """持续监听 WebSocket 消息（支持自动重连）"""
        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）

        while self._running:
            try:
                if retry_count == 0:
                    logger.debug(f"[{self.exchange_name}] 连接到 WebSocket: {self.ws_url}")
                else:
                    logger.debug(f"[{self.exchange_name}] 重连WebSocket (第{retry_count}次)")

                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=5
                ) as websocket:
                    self._ws_connection = websocket
                    logger.debug(f"[{self.exchange_name}] WebSocket 连接成功")

                    # 发送订阅消息
                    for coin in self.orderbook_callbacks.keys():
                        await self._send_subscribe_message(coin)

                    # 重连成功，重置计数器
                    if retry_count > 0:
                        logger.debug(f"[{self.exchange_name}] 重连成功！")
                    retry_count = 0

                    # 持续接收消息
                    while self._running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=5)
                            data = json.loads(message)
                            await self._handle_message(data)

                        except asyncio.TimeoutError:
                            logger.warning(f"[{self.exchange_name}] WebSocket 超时，发送 ping...")
                            await websocket.ping()
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"[{self.exchange_name}] WebSocket 连接关闭，准备重连...")
                            break
                        except Exception as e:
                            logger.error(f"[{self.exchange_name}] 处理消息异常: {e}")
                            logger.exception(e)

            except Exception as e:
                if not self._running:
                    # 如果是主动停止，不记录错误
                    logger.debug(f"[{self.exchange_name}] WebSocket已主动停止")
                    return

                # 所有异常都尝试重连
                if "no pong" in str(e) or "ConnectionClosed" in str(type(e).__name__):
                    logger.warning(f"[{self.exchange_name}] WebSocket连接断开: {e}")
                else:
                    logger.error(f"[{self.exchange_name}] WebSocket 连接异常: {e}")
                    logger.exception(e)

                retry_count += 1

                # 等待后重连
                if self._running:
                    logger.debug(f"[{self.exchange_name}] {retry_delay}秒后重连...")
                    await asyncio.sleep(retry_delay)

            finally:
                self._ws_connection = None

        logger.debug(f"[{self.exchange_name}] WebSocket监听线程退出")

    async def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            logger.warning(f"[{self.exchange_name}] WebSocket 已在运行")
            return

        if not self.orderbook_callbacks:
            logger.error(f"[{self.exchange_name}] 没有订阅任何交易对")
            return

        self._running = True

        # 启动监听任务
        self._listen_task = asyncio.create_task(self._listen_websocket())

        logger.debug(f"[{self.exchange_name}] WebSocket 已启动，订阅 {len(self.orderbook_callbacks)} 个交易对")

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

        logger.debug(f"[{self.exchange_name}] WebSocket 已停止")