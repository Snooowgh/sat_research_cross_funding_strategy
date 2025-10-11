# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : orderbook_stream
@Description : WebSocket订单簿流监听基类
@Time        : 2025/10/2 14:00
"""
import time
from abc import ABC, abstractmethod
from typing import Optional, Callable, Dict
from loguru import logger


class OrderBookData:
    """订单簿数据结构"""

    def __init__(self, pair: str, bids: list, asks: list, timestamp: float = None):
        self.pair = pair
        self.bids = bids  # [[price, quantity], ...]
        self.asks = asks  # [[price, quantity], ...]
        self.timestamp = timestamp or time.time()

    @property
    def best_bid(self) -> Optional[float]:
        """最优买价"""
        return float(self.bids[0][0]) if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        """最优卖价"""
        return float(self.asks[0][0]) if self.asks else None

    @property
    def mid_price(self) -> Optional[float]:
        """中间价"""
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return None

    @property
    def spread(self) -> Optional[float]:
        """价差"""
        if self.best_bid and self.best_ask:
            return self.best_ask - self.best_bid
        return None

    @property
    def spread_pct(self) -> Optional[float]:
        """价差百分比"""
        if self.best_bid and self.best_ask and self.mid_price:
            return (self.best_ask - self.best_bid) / self.mid_price
        return None

    def get_depth(self, side: str, limit: int = 10) -> float:
        """
        计算指定方向的深度（数量）
        :param side: "bid" 或 "ask"
        :param limit: 深度层级数
        :return: 总数量
        """
        levels = self.bids if side == "bid" else self.asks
        total_qty = sum(float(level[1]) for level in levels[:limit])
        return total_qty

    def get_liquidity_usd(self, side: str, limit: int = 10) -> float:
        """
        计算指定方向的流动性（美元价值）
        :param side: "bid" 或 "ask"
        :param limit: 深度层级数
        :return: 总美元价值
        """
        levels = self.bids if side == "bid" else self.asks
        total_value = sum(float(level[0]) * float(level[1]) for level in levels[:limit])
        return total_value

    def is_stale(self, max_age_sec: float = 2.0) -> bool:
        """判断数据是否过期"""
        return (time.time() - self.timestamp) > max_age_sec

    def __str__(self):
        age = time.time() - self.timestamp
        return (f"OrderBook({self.pair} mid={self.mid_price}"
                f"spread={self.spread_pct:.4%} age={age * 1000:.2f}ms)\n {self.bids[:3]} \n {self.asks[:3]} ")


class OrderBookStream(ABC):
    """订单簿流监听器抽象基类"""

    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.orderbook_callbacks: Dict[str, list] = {}  # pair -> [callbacks]
        self.latest_orderbooks: Dict[str, OrderBookData] = {}  # pair -> OrderBookData
        self._running = False

    def subscribe(self, pair: str, callback: Optional[Callable] = None):
        """
        订阅交易对的订单簿更新
        :param pair: 交易对（统一格式，如 "BTCUSDT"）
        :param callback: 回调函数 callback(orderbook_data: OrderBookData)
        """
        if pair not in self.orderbook_callbacks:
            self.orderbook_callbacks[pair] = []
        if callback:
            self.orderbook_callbacks[pair].append(callback)
        logger.info(f"[{self.exchange_name}] 订阅 {pair} 订单簿")

    def get_latest_orderbook(self, pair: str) -> Optional[OrderBookData]:
        """获取最新的订单簿数据"""
        return self.latest_orderbooks.get(pair)

    def _on_orderbook_update(self, orderbook: OrderBookData):
        """
        内部回调：当订单簿更新时调用
        :param orderbook: 订单簿数据
        """
        # 更新缓存
        self.latest_orderbooks[orderbook.pair] = orderbook

        # 触发用户回调
        callbacks = self.orderbook_callbacks.get(orderbook.pair, [])
        for callback in callbacks:
            try:
                callback(orderbook)
            except Exception as e:
                logger.error(f"[{self.exchange_name}] 订单簿回调异常: {e}")

    @abstractmethod
    async def start(self):
        """启动 WebSocket 连接"""
        pass

    @abstractmethod
    async def stop(self):
        """停止 WebSocket 连接"""
        pass

    @property
    def is_running(self) -> bool:
        """是否正在运行"""
        return self._running
