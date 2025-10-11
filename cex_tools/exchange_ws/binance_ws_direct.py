# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : binance_ws_direct
@Description : 直接使用 websockets 库实现Binance WebSocket（绕过CCXT Pro依赖问题）
@Time        : 2025/10/2 20:50
"""
import asyncio
import json
import time
import aiohttp
import websockets
from loguru import logger
from typing import Optional, Dict

from cex_tools.exchange_ws.orderbook_stream import OrderBookStream, OrderBookData


class BinanceOrderBookStreamDirect(OrderBookStream):
    """直接使用 websockets 实现的 Binance 订单簿流，维护本地订单簿"""

    def __init__(self, api_key: str = None, secret: str = None):
        super().__init__("Binance")
        self.api_key = api_key
        self.secret = secret
        self._ws_connection = None
        self._listen_task: Optional[asyncio.Task] = None

        # 本地订单簿维护
        self._local_orderbooks: Dict[str, Dict] = {}  # pair -> {bids: {price: qty}, asks: {price: qty}}
        self._initialized: Dict[str, bool] = {}  # pair -> initialized flag
        self.last_update_id = None  # 记录最后处理的消息ID

    def subscribe(self, pair: str, callback=None):
        """订阅交易对"""
        if not pair.endswith("USDT") and not pair.endswith("USDC"):
            pair += "USDT"
        super().subscribe(pair, callback)

    def _get_stream_name(self, pair: str) -> str:
        """
        获取 Binance WebSocket 流名称
        BTCUSDT -> btcusdt@depth@100ms
        """
        return f"{pair.lower()}@depth@100ms"

    async def _fetch_orderbook_snapshot(self, pair: str) -> Optional[dict]:
        """
        从 REST API 获取订单簿快照
        :param pair: 交易对
        :return: {"lastUpdateId": int, "bids": [...], "asks": [...]}
        """
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={pair.lower()}&limit=1000"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.debug(f"[{self.exchange_name}] {pair} 获取快照成功，lastUpdateId={data.get('lastUpdateId')}")
                        return data
                    else:
                        logger.error(f"[{self.exchange_name}] {pair} 获取快照失败: HTTP {response.status}")
                        return None
        except Exception as e:
            logger.error(f"[{self.exchange_name}] {pair} 获取快照异常: {e}")
            return None

    async def _initialize_orderbook(self, pair: str):
        """
        初始化本地订单簿：获取快照
        :param pair: 交易对
        """
        try:
            # 获取快照
            snapshot = await self._fetch_orderbook_snapshot(pair)
            if not snapshot:
                logger.error(f"[{self.exchange_name}] {pair} 初始化失败：无法获取快照")
                return

            # 初始化本地订单簿
            self._local_orderbooks[pair] = {
                "bids": {},
                "asks": {}
            }

            # 加载快照数据
            for price, qty in snapshot.get("bids", []):
                self._local_orderbooks[pair]["bids"][float(price)] = float(qty)

            for price, qty in snapshot.get("asks", []):
                self._local_orderbooks[pair]["asks"][float(price)] = float(qty)

            logger.debug(f"[{self.exchange_name}] {pair} 本地订单簿初始化完成，"
                       f"bids={len(self._local_orderbooks[pair]['bids'])}, "
                       f"asks={len(self._local_orderbooks[pair]['asks'])}")

            # 标记为已初始化
            self._initialized[pair] = True

            # 立即发布一次订单簿
            self._publish_orderbook(pair)

        except Exception as e:
            logger.error(f"[{self.exchange_name}] {pair} 初始化订单簿异常: {e}")
            logger.exception(e)

    def _apply_depth_update(self, pair: str, event: dict):
        """
        应用增量更新到本地订单簿
        :param pair: 交易对
        :param event: WebSocket 事件数据
        """
        try:
            # 更新 bids
            for price, qty in event.get("b", []):
                price = float(price)
                qty = float(qty)
                if qty == 0:
                    # 删除该价格层级
                    self._local_orderbooks[pair]["bids"].pop(price, None)
                else:
                    # 更新/添加该价格层级
                    self._local_orderbooks[pair]["bids"][price] = qty

            # 更新 asks
            for price, qty in event.get("a", []):
                price = float(price)
                qty = float(qty)
                if qty == 0:
                    self._local_orderbooks[pair]["asks"].pop(price, None)
                else:
                    self._local_orderbooks[pair]["asks"][price] = qty
            self.last_update_id = event['u']  # 更新最后处理的消息ID
            # 生成并推送 OrderBookData
            self._publish_orderbook(pair)

        except Exception as e:
            logger.error(f"[{self.exchange_name}] {pair} 应用增量更新异常: {e}")
            logger.exception(e)

    def _publish_orderbook(self, pair: str):
        """
        发布本地订单簿数据
        :param pair: 交易对
        """
        try:
            orderbook_dict = self._local_orderbooks.get(pair)
            if not orderbook_dict:
                return

            # 排序并转换为列表格式
            bids = sorted(orderbook_dict["bids"].items(), key=lambda x: x[0], reverse=True)
            asks = sorted(orderbook_dict["asks"].items(), key=lambda x: x[0])

            # 转换为 [[price, qty], ...] 格式
            bids_list = [[price, qty] for price, qty in bids]
            asks_list = [[price, qty] for price, qty in asks]

            # 创建 OrderBookData
            orderbook = OrderBookData(
                pair=pair,
                bids=bids_list,
                asks=asks_list,
                timestamp=time.time()
            )

            # 触发回调
            self._on_orderbook_update(orderbook)

        except Exception as e:
            logger.error(f"[{self.exchange_name}] {pair} 发布订单簿异常: {e}")
            logger.exception(e)

    async def _listen_websocket(self):
        """持续监听 WebSocket 消息（支持自动重连）"""
        # 构建订阅流
        streams = [self._get_stream_name(pair) for pair in self.orderbook_callbacks.keys()]
        stream_names = "/".join(streams)

        # Binance USD-M Futures WebSocket地址
        ws_url = f"wss://fstream.binance.com/stream?streams={stream_names}"

        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）

        while self._running:
            try:
                if retry_count == 0:
                    logger.debug(f"[{self.exchange_name}] 连接到 WebSocket: {ws_url}")
                else:
                    logger.debug(f"[{self.exchange_name}] 重连WebSocket (第{retry_count}次)")

                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=5) as websocket:
                    self._ws_connection = websocket
                    logger.debug(f"[{self.exchange_name}] WebSocket 连接成功")

                    # 重连成功，重置计数器
                    if retry_count > 0:
                        logger.debug(f"[{self.exchange_name}] 重连成功！")
                    retry_count = 0

                    while self._running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=5)
                            data = json.loads(message)

                            # Binance 返回格式: {"stream": "btcusdt@depth@100ms", "data": {...}}
                            if "data" in data and "stream" in data:
                                await self._handle_depth_update(data["stream"], data["data"])

                        except asyncio.TimeoutError:
                            logger.warning(f"[{self.exchange_name}] WebSocket 超时，发送 ping...")
                            await websocket.ping()
                            self._initialized.clear()
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"[{self.exchange_name}] WebSocket 连接关闭，准备重连...")
                            self._initialized.clear()
                            break
                        except Exception as e:
                            logger.error(f"[{self.exchange_name}] 处理消息异常: {e}")
                            logger.exception(e)
                            self._initialized.clear()

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

    async def _handle_depth_update(self, stream: str, data: dict):
        """
        处理订单簿深度更新（增量更新）
        :param stream: 流名称，如 "btcusdt@depth@100ms"
        :param data: 订单簿增量数据
        """
        try:
            # 从流名称提取交易对
            pair = stream.split("@")[0].upper()

            # 只处理订阅的交易对
            if pair not in self.orderbook_callbacks:
                return

            # 如果还未初始化，先初始化
            if not self._initialized.get(pair, False):
                await self._initialize_orderbook(pair)
                return

            # 直接应用增量更新
            self._apply_depth_update(pair, data)

        except Exception as e:
            logger.error(f"[{self.exchange_name}] 处理订单簿更新异常: {e}")
            logger.exception(e)

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