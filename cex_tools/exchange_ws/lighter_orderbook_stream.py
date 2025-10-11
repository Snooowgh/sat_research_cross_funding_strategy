# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : lighter_orderbook_stream
@Description : Lighter交易所WebSocket订单簿流监听器
@Time        : 2025/10/2 14:05
"""
import asyncio
import json
import threading
import lighter
from loguru import logger
from typing import Optional, Dict

from cex_tools.exchange_ws.orderbook_stream import OrderBookStream, OrderBookData


class LighterOrderBookStreamAsync(OrderBookStream):
    """Lighter交易所订单簿流监听器（线程版本，使用同步 run()）"""

    def __init__(self):
        super().__init__("Lighter")
        self.ws_client: Optional[lighter.WsClient] = None
        self.market_id_to_pair: Dict[int, str] = {}
        self.pair_to_market_id: Dict[str, int] = {}
        self._ws_thread: Optional[threading.Thread] = None

    async def init_market_mapping(self):
        """初始化市场ID与交易对的映射关系"""
        try:
            config = lighter.Configuration(host="https://mainnet.zklighter.elliot.ai")
            api_client = lighter.ApiClient(configuration=config)
            order_api = lighter.OrderApi(api_client=api_client)

            order_book_details_ret = await order_api.order_book_details()
            order_book_details = order_book_details_ret.order_book_details

            for detail in order_book_details:
                if detail.status == "active":
                    pair = detail.symbol + "USDT"
                    market_id = detail.market_id
                    self.market_id_to_pair[market_id] = pair
                    self.pair_to_market_id[pair] = market_id

            await api_client.close()
            logger.debug(f"[Lighter] 市场映射初始化完成，共 {len(self.market_id_to_pair)} 个市场")

        except Exception as e:
            logger.error(f"[Lighter] 初始化市场映射失败: {e}")
            raise

    def subscribe(self, pair: str, callback=None):
        """订阅交易对"""
        if not pair.endswith("USDT"):
            pair += "USDT"
        super().subscribe(pair, callback)

    def _on_order_book_update_ws(self, market_id: int, order_book_data: dict):
        """WebSocket订单簿更新回调"""
        try:
            pair = self.market_id_to_pair[int(market_id)]
            if not pair or pair not in self.orderbook_callbacks:
                return

            bids = [[float(item["price"]), float(item["size"])] for item in order_book_data.get("bids", [])]
            bids = list(sorted(bids, key=lambda x: x[0], reverse=True))  # 价格从高到低
            asks = [[float(item["price"]), float(item["size"])] for item in order_book_data.get("asks", [])]
            asks = list(sorted(asks, key=lambda x: x[0]))  # 价格从低到高
            orderbook = OrderBookData(
                pair=pair,
                bids=bids,
                asks=asks,
                timestamp=order_book_data.get("timestamp", None)
            )
            self._on_orderbook_update(orderbook)

        except Exception as e:
            logger.error(f"[Lighter] 处理订单簿更新异常: {e}")
            logger.exception(e)

    def _run_ws_blocking(self):
        """在独立线程中运行WebSocket（阻塞调用，支持自动重连）"""
        import json
        import time
        from websockets.sync.client import connect

        max_retries = 999  # 几乎无限重连
        retry_count = 0
        retry_delay = 1  # 重连延迟（秒）

        while self._running:
            try:
                # 只订阅用户指定的交易对对应的 market_id
                order_book_ids = [
                    self.pair_to_market_id[pair]
                    for pair in self.orderbook_callbacks.keys()
                    if pair in self.pair_to_market_id
                ]

                if not order_book_ids:
                    logger.error("[Lighter] 没有有效的订阅市场")
                    return

                if retry_count == 0:
                    logger.debug(
                        f"[Lighter] 启动WebSocket，订阅市场 ID: {order_book_ids} (交易对: {list(self.orderbook_callbacks.keys())})")
                else:
                    logger.debug(f"[Lighter] 重连WebSocket (第{retry_count}次)")

                # 创建 WebSocket 客户端（但我们不使用它的 run 方法）
                self.ws_client = lighter.WsClient(
                    order_book_ids=order_book_ids,
                    account_ids=[],
                    on_order_book_update=self._on_order_book_update_ws,
                    on_account_update=None,
                )

                # 创建 WebSocket 连接
                # 注意: websockets.sync.client.connect 不支持 ping_interval 等参数
                # 心跳是自动处理的，默认每20秒ping，20秒超时
                ws = connect(
                    self.ws_client.base_url,
                    close_timeout=10,  # 关闭连接的超时时间
                    open_timeout=15  # 打开连接的超时时间
                )
                self.ws_client.ws = ws

                def handle_unhandled_message(msg):
                    try:
                        if msg.get("type") == "ping":
                            pong_msg = json.dumps({"type": "pong"})
                            self.ws_client.ws.send(pong_msg)
                    except Exception as e:
                        # 其他潜在异常
                        raise Exception(f"Unhandled message: {message} {e}")
                self.ws_client.handle_unhandled_message = handle_unhandled_message
                logger.debug("[Lighter] WebSocket连接已建立，开始接收消息...")

                # 重连成功，重置计数器
                if retry_count > 0:
                    logger.debug(f"[Lighter] 重连成功！")
                retry_count = 0

                # 手动处理消息循环
                for message in ws:
                    if not self._running:
                        logger.debug("[Lighter] 收到停止信号，退出消息循环")
                        return
                    self.ws_client.on_message(ws, message)

            except Exception as e:
                if not self._running:
                    # 如果是主动停止，不记录错误
                    logger.debug("[Lighter] WebSocket已主动停止")
                    return

                # 所有异常都尝试重连
                if "no pong" in str(e) or "ConnectionClosed" in str(type(e).__name__):
                    logger.warning(f"[Lighter] WebSocket连接断开: {e}")
                else:
                    logger.error(f"[Lighter] WebSocket运行异常: {e}")

                retry_count += 1

                # 关闭当前连接
                if hasattr(self, 'ws_client') and self.ws_client and self.ws_client.ws:
                    try:
                        self.ws_client.ws.close()
                    except:
                        pass

                # 等待后重连
                if self._running:
                    logger.debug(f"[Lighter] {retry_delay}秒后重连...")
                    time.sleep(retry_delay)

        self._running = False
        logger.debug("[Lighter] WebSocket线程退出")

    async def start(self):
        """启动WebSocket连接"""
        if self._running:
            logger.warning("[Lighter] WebSocket已在运行")
            return

        # 检查是否有订阅
        if not self.orderbook_callbacks:
            logger.error("[Lighter] 没有订阅任何交易对，请先调用 subscribe()")
            return

        # 初始化市场映射
        if not self.market_id_to_pair:
            await self.init_market_mapping()

        # 检查订阅的交易对是否有效
        missing_pairs = [
            pair for pair in self.orderbook_callbacks.keys()
            if pair not in self.pair_to_market_id
        ]
        if missing_pairs:
            logger.error(f"[Lighter] 以下交易对没有找到对应的 market_id: {missing_pairs}")
            logger.debug(f"[Lighter] 可用的交易对: {list(self.pair_to_market_id.keys())}")
            return

        self._running = True

        # 在独立线程中运行 WebSocket
        self._ws_thread = threading.Thread(
            target=self._run_ws_blocking,
            daemon=True,
            name="LighterWebSocketThread"
        )
        self._ws_thread.start()

        # 等待连接建立
        await asyncio.sleep(2)
        logger.debug("[Lighter] WebSocket已启动")

    async def stop(self):
        """停止WebSocket连接"""
        logger.debug("[Lighter] 正在停止WebSocket连接...")
        self._running = False

        # 尝试关闭 WebSocket 客户端
        if self.ws_client and self.ws_client.ws:
            try:
                # 关闭底层的 websocket 连接
                if hasattr(self.ws_client.ws, 'close'):
                    # 同步版本的 websocket
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self.ws_client.ws.close)
                    logger.debug("[Lighter] WebSocket连接已关闭")
            except Exception as e:
                logger.warning(f"[Lighter] 关闭WebSocket时出现警告: {e}")

        # 等待线程结束（最多等待3秒）
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3.0)
            if self._ws_thread.is_alive():
                logger.warning("[Lighter] WebSocket线程未能及时结束")
            else:
                logger.debug("[Lighter] WebSocket线程已结束")

        logger.debug("[Lighter] WebSocket已停止")
