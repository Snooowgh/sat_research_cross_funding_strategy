# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : bybit_orderbook_stream
@Description : Bybit交易所WebSocket订单簿流监听器
@Time        : 2025/10/4
"""
import asyncio
import json
import threading
import time
from typing import Optional
from loguru import logger
import websockets

from cex_tools.exchange_ws.orderbook_stream import OrderBookStream, OrderBookData


class BybitOrderBookStreamAsync(OrderBookStream):
    """Bybit交易所订单簿流监听器（V5 API）"""

    def __init__(self, testnet=False):
        super().__init__("Bybit")
        # Bybit V5 WebSocket URL
        if testnet:
            self.ws_url = "wss://stream-testnet.bybit.com/v5/public/linear"
        else:
            self.ws_url = "wss://stream.bybit.com/v5/public/linear"

        self.ws_connection = None
        self._ws_thread: Optional[threading.Thread] = None
        self._local_orderbooks = {}  # 本地维护的完整订单簿

    def subscribe(self, pair: str, callback=None):
        """订阅交易对"""
        if not pair.endswith("USDT"):
            pair += "USDT"
        super().subscribe(pair, callback)

    def _on_orderbook_message(self, message: dict):
        """
        处理订单簿消息

        Bybit订单簿消息格式:
        {
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot" | "delta",
            "ts": 1672304486868,
            "data": {
                "s": "BTCUSDT",
                "b": [["40000.00", "0.5"], ...],  // bids
                "a": [["40001.00", "0.3"], ...],  // asks
                "u": 1234567,                      // update ID
                "seq": 9876543210                  // sequence
            }
        }
        """
        try:
            topic = message.get("topic", "")
            msg_type = message.get("type", "")
            data = message.get("data", {})

            if not topic.startswith("orderbook."):
                return

            symbol = data.get("s", "")
            if not symbol or symbol not in self.orderbook_callbacks:
                return

            bids = [[float(item[0]), float(item[1])] for item in data.get("b", [])]
            asks = [[float(item[0]), float(item[1])] for item in data.get("a", [])]

            if msg_type == "snapshot":
                # 快照：直接替换本地订单簿
                self._local_orderbooks[symbol] = {
                    "bids": bids,
                    "asks": asks,
                    "u": data.get("u", 0)
                }
            elif msg_type == "delta":
                # 增量更新：合并到本地订单簿
                if symbol not in self._local_orderbooks:
                    # 如果还没有快照，等待快照
                    logger.warning(f"[Bybit] {symbol} 收到delta但没有snapshot，等待快照")
                    return

                local_book = self._local_orderbooks[symbol]
                local_book["bids"] = self._merge_orderbook_levels(local_book["bids"], bids)
                local_book["asks"] = self._merge_orderbook_levels(local_book["asks"], asks)
                local_book["u"] = data.get("u", local_book["u"])

            # 获取最终订单簿
            if symbol in self._local_orderbooks:
                local_book = self._local_orderbooks[symbol]

                # 排序并截取前50档
                sorted_bids = sorted(local_book["bids"], key=lambda x: x[0], reverse=True)[:50]
                sorted_asks = sorted(local_book["asks"], key=lambda x: x[0])[:50]

                # 创建订单簿对象
                orderbook = OrderBookData(
                    pair=symbol,
                    bids=sorted_bids,
                    asks=sorted_asks,
                    timestamp=int(message.get("ts", time.time() * 1000)) / 1000
                )

                # 触发回调
                self._on_orderbook_update(orderbook)

        except Exception as e:
            logger.error(f"[Bybit] 处理订单簿消息异常: {e}")
            logger.exception(e)

    def _merge_orderbook_levels(self, existing_levels: list, new_levels: list) -> list:
        """
        合并订单簿层级
        :param existing_levels: 现有层级 [[price, qty], ...]
        :param new_levels: 新层级 [[price, qty], ...]
        :return: 合并后的层级
        """
        # 将现有层级转换为字典
        levels_dict = {float(level[0]): float(level[1]) for level in existing_levels}

        # 更新/删除层级
        for price, qty in new_levels:
            price = float(price)
            qty = float(qty)
            if qty == 0:
                # 数量为0，删除该价格层级
                levels_dict.pop(price, None)
            else:
                # 更新价格层级
                levels_dict[price] = qty

        # 转换回列表格式
        return [[price, qty] for price, qty in levels_dict.items()]

    def _run_ws_blocking(self):
        """在独立线程中运行WebSocket（阻塞调用，支持自动重连）"""
        max_retries = 999
        retry_count = 0
        retry_delay = 1

        while self._running:
            try:
                # 构建订阅参数
                subscribe_args = [
                    f"orderbook.50.{pair}"  # 使用50档深度
                    for pair in self.orderbook_callbacks.keys()
                ]

                if not subscribe_args:
                    logger.error("[Bybit] 没有有效的订阅参数")
                    return

                if retry_count == 0:
                    logger.debug(f"[Bybit] 启动WebSocket，订阅: {subscribe_args}")
                else:
                    logger.debug(f"[Bybit] 重连WebSocket (第{retry_count}次)")

                # 创建同步 WebSocket 连接
                import websockets.sync.client
                ws = websockets.sync.client.connect(
                    self.ws_url,
                    close_timeout=10,
                    open_timeout=15
                )
                self.ws_connection = ws

                logger.debug("[Bybit] WebSocket连接已建立")

                # 发送订阅消息
                subscribe_message = {
                    "op": "subscribe",
                    "args": subscribe_args
                }
                ws.send(json.dumps(subscribe_message))
                logger.debug(f"[Bybit] 已发送订阅消息")

                # 重连成功，重置计数器
                if retry_count > 0:
                    logger.debug(f"[Bybit] 重连成功！")
                retry_count = 0

                # 心跳定时器
                last_ping_time = time.time()
                ping_interval = 20  # 每20秒发送ping

                # 消息循环
                for raw_message in ws:
                    if not self._running:
                        logger.debug("[Bybit] 收到停止信号，退出消息循环")
                        return

                    # 发送心跳
                    if time.time() - last_ping_time > ping_interval:
                        try:
                            ws.send(json.dumps({"op": "ping"}))
                            last_ping_time = time.time()
                        except Exception as e:
                            logger.warning(f"[Bybit] 发送ping失败: {e}")

                    try:
                        message = json.loads(raw_message)

                        # 处理不同类型的消息
                        if "op" in message:
                            # 操作响应消息
                            op = message["op"]
                            if op == "subscribe":
                                if message.get("success"):
                                    logger.debug(f"[Bybit] 订阅成功")
                                else:
                                    logger.error(f"[Bybit] 订阅失败: {message}")
                            elif op == "pong":
                                # pong响应，忽略
                                pass
                        elif "topic" in message:
                            # 数据消息
                            self._on_orderbook_message(message)

                    except json.JSONDecodeError as e:
                        logger.warning(f"[Bybit] JSON解析失败: {e}")
                    except Exception as e:
                        logger.error(f"[Bybit] 处理消息异常: {e}")
                        logger.exception(e)

            except Exception as e:
                if not self._running:
                    logger.debug("[Bybit] WebSocket已主动停止")
                    return

                # 判断异常类型
                error_msg = str(e)
                if "ConnectionClosed" in str(type(e).__name__):
                    logger.warning(f"[Bybit] WebSocket连接断开: {e}")
                elif "timeout" in error_msg.lower():
                    logger.warning(f"[Bybit] WebSocket超时: {e}")
                else:
                    logger.error(f"[Bybit] WebSocket运行异常: {e}")
                    logger.exception(e)

                retry_count += 1

                # 关闭当前连接
                if self.ws_connection:
                    try:
                        self.ws_connection.close()
                    except:
                        pass
                    self.ws_connection = None

                # 清空本地订单簿
                self._local_orderbooks.clear()

                # 等待后重连
                if self._running:
                    # 指数退避
                    actual_delay = min(retry_delay * (1.5 ** min(retry_count - 1, 5)), 30)
                    logger.debug(f"[Bybit] {actual_delay:.1f}秒后重连...")
                    time.sleep(actual_delay)

        self._running = False
        logger.debug("[Bybit] WebSocket线程退出")

    async def start(self):
        """启动WebSocket连接"""
        if self._running:
            logger.warning("[Bybit] WebSocket已在运行")
            return

        # 检查是否有订阅
        if not self.orderbook_callbacks:
            logger.error("[Bybit] 没有订阅任何交易对，请先调用 subscribe()")
            return

        self._running = True

        # 在独立线程中运行 WebSocket
        self._ws_thread = threading.Thread(
            target=self._run_ws_blocking,
            daemon=True,
            name="BybitWebSocketThread"
        )
        self._ws_thread.start()

        # 等待连接建立
        await asyncio.sleep(2)
        logger.debug("[Bybit] WebSocket已启动")

    async def stop(self):
        """停止WebSocket连接"""
        logger.debug("[Bybit] 正在停止WebSocket连接...")
        self._running = False

        # 关闭 WebSocket 连接
        if self.ws_connection:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.ws_connection.close)
                logger.debug("[Bybit] WebSocket连接已关闭")
            except Exception as e:
                logger.warning(f"[Bybit] 关闭WebSocket时出现警告: {e}")

        # 等待线程结束
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3.0)
            if self._ws_thread.is_alive():
                logger.warning("[Bybit] WebSocket线程未能及时结束")
            else:
                logger.debug("[Bybit] WebSocket线程已结束")

        # 清空本地订单簿
        self._local_orderbooks.clear()

        logger.debug("[Bybit] WebSocket已停止")


if __name__ == '__main__':
    """测试代码"""
    async def test_bybit_orderbook_stream():
        # 创建流
        stream = BybitOrderBookStreamAsync(testnet=False)

        # 订阅回调
        def on_orderbook_update(orderbook: OrderBookData):
            logger.debug(f"收到订单簿更新: {orderbook.pair}")
            logger.debug(f"  最优买价: {orderbook.best_bid:.2f}")
            logger.debug(f"  最优卖价: {orderbook.best_ask:.2f}")
            logger.debug(f"  中间价: {orderbook.mid_price:.2f}")
            logger.debug(f"  价差: {orderbook.spread_pct:.4%}")
            logger.debug(f"  买盘前3档: {orderbook.bids[:3]}")
            logger.debug(f"  卖盘前3档: {orderbook.asks[:3]}")
            logger.debug("-" * 80)

        # 订阅交易对
        stream.subscribe("BTCUSDT", on_orderbook_update)

        # 启动
        await stream.start()

        # 运行30秒
        await asyncio.sleep(30)

        # 停止
        await stream.stop()

    # 运行测试
    asyncio.run(test_bybit_orderbook_stream())
