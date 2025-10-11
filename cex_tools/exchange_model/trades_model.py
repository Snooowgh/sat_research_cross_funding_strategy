# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : trade_model
@Description :
@Time        : 2024/9/27 08:56
"""
import time

from cex_tools.exchange_model.base_model import OkxBaseModel, TradeDirection, BaseModel


class OkxTradeModel(OkxBaseModel):
    def __init__(self, trade):
        self._pair = trade["instId"]
        super().__init__(self._pair)
        self.tradeId = trade["tradeId"]
        self.price = float(trade["px"])
        self.size = float(trade["sz"])
        self.volume = self.price * self.size
        self.side = TradeDirection.long if trade["side"] == "buy" else TradeDirection.short
        self.ts = int(trade["ts"])
        self.count = int(trade["count"])
        self.time = self.ts


class HyperLiquidTradeModel(BaseModel):
    """
        聚合Trade数据(一段列表)
    """

    def __init__(self, trades):
        self.pair = trades[0]["coin"] + "USDT"
        self.tradeId = trades[0]["tid"]
        self.size = 0
        self.volume = 0
        self.side = 0
        self.total_volume = 0
        for trade in trades:
            size = float(trade["sz"])
            price = float(trade["px"])
            self.size += size
            self.volume += price * size
            side_sign = 1 if trade["side"] == "B" else -1
            self.total_volume += side_sign * self.volume
        self.side = TradeDirection.long if self.total_volume > 0 else TradeDirection.short
        self.price = self.volume / self.size
        self.ts = trades[0]["time"]
        self.count = len(trades)
        self.time = self.ts

    def __str__(self):
        return f"{self.pair} {self.side} {self.price} {self.size} {time.time() - self.ts / 1000:.3f}s"
