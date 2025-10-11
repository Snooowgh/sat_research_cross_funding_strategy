# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : okx_orderbook_stream
@Description : OKX交易所WebSocket订单簿流监听器
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


class OkxOrderBookStreamAsync(OrderBookStream):
    """OKX交易所订单簿流监听器"""

    def __init__(self):
        super().__init__("OKX")
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.ws_connection = None
        self._ws_thread: Optional[threading.Thread] = None
        self._ping_task: Optional[asyncio.Task] = None

    def subscribe(self, pair: str, callback=None):
        """订阅交易对"""
        if not pair.endswith("USDT"):
            pair += "USDT"
        super().subscribe(pair, callback)

    def _convert_pair_to_inst_id(self, pair: str) -> str:
        """
        转换交易对格式为OKX格式
        :param pair: 交易对 (如 "BTCUSDT")
        :return: OKX格式 (如 "BTC-USDT-SWAP")
        """
        if pair.endswith("USDT"):
            base = pair.replace("USDT", "")
            return f"{base}-USDT-SWAP"
        return pair

    def _convert_inst_id_to_pair(self, inst_id: str) -> str:
        """
        转换OKX格式为标准交易对
        :param inst_id: OKX格式 (如 "BTC-USDT-SWAP")
        :return: 标准格式 (如 "BTCUSDT")
        """
        # BTC-USDT-SWAP -> BTCUSDT
        parts = inst_id.split("-")
        if len(parts) >= 2:
            return parts[0] + parts[1]
        return inst_id

    def _on_orderbook_update_ws(self, message: dict):
        """
        处理订单簿更新消息

        OKX订单簿消息格式:
        {
            "arg": {
                "channel": "books",
                "instId": "BTC-USDT-SWAP"
            },
            "action": "snapshot" | "update",
            "data": [{
                "asks": [
                    ["41006.8", "0.60038921", "0", "1"],  // [价格, 数量, 已弃用, 订单数量]
                    ...
                ],
                "bids": [
                    ["41006.3", "0.30178218", "0", "2"],
                    ...
                ],
                "ts": "1597026383085",
                "checksum": -855196043,
                "prevSeqId": -1,
                "seqId": 123456
            }]
        }
        """
        try:
            if "arg" not in message or "data" not in message:
                return

            inst_id = message["arg"].get("instId", "")
            pair = self._convert_inst_id_to_pair(inst_id)

            if not pair or pair not in self.orderbook_callbacks:
                return

            data = message["data"]
            if not data or len(data) == 0:
                return

            orderbook_data = data[0]

            # 提取买卖盘数据 (只取价格和数量，忽略其他字段)
            bids = [[float(item[0]), float(item[1])] for item in orderbook_data.get("bids", [])]
            asks = [[float(item[0]), float(item[1])] for item in orderbook_data.get("asks", [])]

            # action字段: snapshot(快照) 或 update(增量更新)
            action = message.get("action", "snapshot")

            if action == "snapshot":
                # 快照：直接使用
                updated_bids = bids
                updated_asks = asks
            else:
                # 增量更新：合并到现有订单簿
                existing_orderbook = self.latest_orderbooks.get(pair)
                if existing_orderbook:
                    updated_bids = self._merge_orderbook_levels(existing_orderbook.bids, bids)
                    updated_asks = self._merge_orderbook_levels(existing_orderbook.asks, asks)
                else:
                    # 如果没有现有订单簿，则直接使用
                    updated_bids = bids
                    updated_asks = asks

            # 排序并截取前50档
            updated_bids = sorted(updated_bids, key=lambda x: x[0], reverse=True)[:50]
            updated_asks = sorted(updated_asks, key=lambda x: x[0])[:50]

            # 创建订单簿对象
            timestamp = int(orderbook_data.get("ts", time.time() * 1000)) / 1000
            orderbook = OrderBookData(
                pair=pair,
                bids=updated_bids,
                asks=updated_asks,
                timestamp=timestamp
            )

            # 触发回调
            self._on_orderbook_update(orderbook)

        except Exception as e:
            logger.error(f"[OKX] 处理订单簿更新异常: {e}")
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
                    {
                        "channel": "books",  # 使用完整深度 (books) 而非前5档 (books5)
                        "instId": self._convert_pair_to_inst_id(pair)
                    }
                    for pair in self.orderbook_callbacks.keys()
                ]

                if not subscribe_args:
                    logger.error("[OKX] 没有有效的订阅参数")
                    return

                if retry_count == 0:
                    logger.debug(f"[OKX] 启动WebSocket，订阅: {subscribe_args}")
                else:
                    logger.debug(f"[OKX] 重连WebSocket (第{retry_count}次)")

                # 创建同步 WebSocket 连接
                import websockets.sync.client
                ws = websockets.sync.client.connect(
                    self.ws_url,
                    close_timeout=10,
                    open_timeout=15
                )
                self.ws_connection = ws

                logger.debug("[OKX] WebSocket连接已建立")

                # 发送订阅消息
                subscribe_message = {
                    "op": "subscribe",
                    "args": subscribe_args
                }
                ws.send(json.dumps(subscribe_message))
                logger.debug(f"[OKX] 已发送订阅消息: {subscribe_message}")

                # 重连成功，重置计数器
                if retry_count > 0:
                    logger.debug(f"[OKX] 重连成功！")
                retry_count = 0

                # 消息循环
                for raw_message in ws:
                    if not self._running:
                        logger.debug("[OKX] 收到停止信号，退出消息循环")
                        return

                    try:
                        message = json.loads(raw_message)

                        # 处理不同类型的消息
                        if "event" in message:
                            # 事件消息 (如订阅确认、错误等)
                            event = message["event"]
                            if event == "subscribe":
                                logger.debug(f"[OKX] 订阅成功: {message.get('arg', {})}")
                            elif event == "error":
                                logger.error(f"[OKX] 订阅错误: {message}")
                            else:
                                logger.debug(f"[OKX] 事件消息: {message}")
                        elif "arg" in message and "data" in message:
                            # 数据消息 (订单簿更新)
                            self._on_orderbook_update_ws(message)
                        else:
                            logger.debug(f"[OKX] 未知消息类型: {message}")

                    except json.JSONDecodeError as e:
                        logger.warning(f"[OKX] JSON解析失败: {e}")
                    except Exception as e:
                        logger.error(f"[OKX] 处理消息异常: {e}")
                        logger.exception(e)

            except Exception as e:
                if not self._running:
                    logger.debug("[OKX] WebSocket已主动停止")
                    return

                # 判断异常类型
                error_msg = str(e)
                if "ConnectionClosed" in str(type(e).__name__):
                    logger.warning(f"[OKX] WebSocket连接断开: {e}")
                elif "timeout" in error_msg.lower():
                    logger.warning(f"[OKX] WebSocket超时: {e}")
                else:
                    logger.error(f"[OKX] WebSocket运行异常: {e}")
                    logger.exception(e)

                retry_count += 1

                # 关闭当前连接
                if self.ws_connection:
                    try:
                        self.ws_connection.close()
                    except:
                        pass
                    self.ws_connection = None

                # 等待后重连
                if self._running:
                    # 指数退避
                    actual_delay = min(retry_delay * (1.5 ** min(retry_count - 1, 5)), 30)
                    logger.debug(f"[OKX] {actual_delay:.1f}秒后重连...")
                    time.sleep(actual_delay)

        self._running = False
        logger.debug("[OKX] WebSocket线程退出")

    async def start(self):
        """启动WebSocket连接"""
        if self._running:
            logger.warning("[OKX] WebSocket已在运行")
            return

        # 检查是否有订阅
        if not self.orderbook_callbacks:
            logger.error("[OKX] 没有订阅任何交易对，请先调用 subscribe()")
            return

        self._running = True

        # 在独立线程中运行 WebSocket
        self._ws_thread = threading.Thread(
            target=self._run_ws_blocking,
            daemon=True,
            name="OKXWebSocketThread"
        )
        self._ws_thread.start()

        # 等待连接建立
        await asyncio.sleep(2)
        logger.debug("[OKX] WebSocket已启动")

    async def stop(self):
        """停止WebSocket连接"""
        logger.debug("[OKX] 正在停止WebSocket连接...")
        self._running = False

        # 关闭 WebSocket 连接
        if self.ws_connection:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.ws_connection.close)
                logger.debug("[OKX] WebSocket连接已关闭")
            except Exception as e:
                logger.warning(f"[OKX] 关闭WebSocket时出现警告: {e}")

        # 等待线程结束
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3.0)
            if self._ws_thread.is_alive():
                logger.warning("[OKX] WebSocket线程未能及时结束")
            else:
                logger.debug("[OKX] WebSocket线程已结束")

        logger.debug("[OKX] WebSocket已停止")


if __name__ == '__main__':
    """测试代码"""
    async def test_okx_orderbook_stream():
        # 创建流
        stream = OkxOrderBookStreamAsync()

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
    asyncio.run(test_okx_orderbook_stream())