# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : kline_bar_model
@Description :
@Time        : 2024/9/26 12:44
"""
import time

from cex_tools.exchange_model.base_model import BaseModel, OkxBaseModel


class KlineBaseModel(BaseModel):

    @property
    def change_rate(self):
        return (self.close - self.open) / self.open

    @property
    def max_change_rate(self):
        return (self.high - self.low) / self.low


class CcxtKlineBar(BaseModel):

    def __init__(self, kline_bar):
        # TOHLCV
        self.open_time = kline_bar[0]
        self.open = float(kline_bar[1])
        self.high = float(kline_bar[2])
        self.low = float(kline_bar[3])
        self.close = float(kline_bar[4])
        self.volume = float(kline_bar[5])

    def is_up(self):
        return self.close > self.open

    def is_down(self):
        return self.close < self.open

    def is_hammer(self):
        """
        判断是否是锤子线（见底信号）
        """
        body_length = abs(self.close - self.open)
        range_hl = self.high - self.low
        if range_hl == 0:
            return False  # 避免除零错误

        # 实体不超过区间范围的30%
        if body_length > 0.3 * range_hl:
            return False

        lower_shadow = min(self.open, self.close) - self.low
        # 下影线至少是实体长度的两倍
        if lower_shadow < 2 * body_length:
            return False

        upper_shadow = self.high - max(self.open, self.close)
        # 上影线不超过实体长度的10%
        if upper_shadow > 0.1 * body_length:
            return False

        # 实体顶部位于K线上半部分
        max_oc = max(self.open, self.close)
        if max_oc < (self.high + self.low) / 2:
            return False

        return True

    def is_hanging_man(self):
        """
        判断是否是吊颈线（见顶信号）
        """
        body_length = abs(self.close - self.open)
        range_hl = self.high - self.low
        if range_hl == 0:
            return False  # 避免除零错误

        # 实体不超过区间范围的30%
        if body_length > 0.3 * range_hl:
            return False

        lower_shadow = min(self.open, self.close) - self.low
        # 下影线至少是实体长度的两倍
        if lower_shadow < 2 * body_length:
            return False

        upper_shadow = self.high - max(self.open, self.close)
        # 上影线不超过实体长度的10%
        if upper_shadow > 0.1 * body_length:
            return False

        # 实体顶部位于K线下半部分
        max_oc = max(self.open, self.close)
        if max_oc >= (self.high + self.low) / 2:
            return False

        return True

    def is_big_body(self, threshold=0.6):
        """判断是否是大实体K线（实体占比超过阈值）"""
        body = abs(self.close - self.open)
        total_range = self.high - self.low
        return body > total_range * threshold if total_range != 0 else False

    def __str__(self):
        if self.is_hammer():
            return "🔨 见底锤子线"
        elif self.is_hanging_man():
            return "📉 见顶吊颈线"
        else:
            return ""


class BinanceKlineBar(BaseModel):

    def __init__(self, kline_bar):
        # TOHLCV
        self.open_time = kline_bar[0]
        self.open = float(kline_bar[1])
        self.high = float(kline_bar[2])
        self.low = float(kline_bar[3])
        self.close = float(kline_bar[4])
        self.volume = float(kline_bar[5])
        self.close_time = kline_bar[6]
        self.quote_asset_volume = float(kline_bar[7])
        self.number_of_trades = kline_bar[8]
        self.taker_buy_base_asset_volume = float(kline_bar[9])
        self.taker_buy_quote_asset_volume = float(kline_bar[10])
        self.ignore = float(kline_bar[11])
        self.time = self.open_time


class OkxKlineBar(OkxBaseModel):

    def __init__(self, kline_bar, _pair=None):
        # TOHLCV
        super().__init__(_pair)
        self._pair = _pair
        self.open_time = kline_bar[0]
        self.open = float(kline_bar[1])
        self.high = float(kline_bar[2])
        self.low = float(kline_bar[3])
        self.close = float(kline_bar[4])

        # self.volume = float(kline_bar[5])
        # self.close_time = kline_bar[6]
        # self.quote_asset_volume = float(kline_bar[7])
        # self.number_of_trades = kline_bar[8]
        # self.taker_buy_base_asset_volume = float(kline_bar[9])
        # self.taker_buy_quote_asset_volume = float(kline_bar[10])
        # self.ignore = float(kline_bar[11])
        self.confirmed = float(kline_bar[-1]) == 1
        self.time = self.open_time


class BitgetKlineBar(BaseModel):

    def __init__(self, kline_bar):
        # TOHLCV
        self.open_time = kline_bar[0]
        self.open = float(kline_bar[1])
        self.high = float(kline_bar[2])
        self.low = float(kline_bar[3])
        self.close = float(kline_bar[4])

        self.volume = float(kline_bar[5])
        # self.close_time = kline_bar[6]
        self.quote_asset_volume = float(kline_bar[6])
        # self.number_of_trades = kline_bar[8]
        # self.taker_buy_base_asset_volume = float(kline_bar[9])
        # self.taker_buy_quote_asset_volume = float(kline_bar[10])
        # self.ignore = float(kline_bar[11])
        # self.confirmed = float(kline_bar[-1]) == 1
        self.time = self.open_time


class BybitKlineBar(BaseModel):

    def __init__(self, kline_bar):
        # Bybit K线格式: [start_time, open, high, low, close, volume, turnover]
        self.open_time = int(kline_bar[0])  # 开始时间
        self.open = float(kline_bar[1])  # 开盘价
        self.high = float(kline_bar[2])  # 最高价
        self.low = float(kline_bar[3])  # 最低价
        self.close = float(kline_bar[4])  # 收盘价
        self.volume = float(kline_bar[5])  # 成交量
        self.turnover = float(kline_bar[6]) if len(kline_bar) > 6 else 0  # 成交额
        self.time = self.open_time

    @property
    def change_rate(self):
        """计算涨跌幅"""
        if self.open == 0:
            return 0
        return (self.close - self.open) / self.open

    def is_up(self):
        """判断是否上涨"""
        return self.close > self.open

    def is_down(self):
        """判断是否下跌"""
        return self.close < self.open


class HyperLiquidKlineBar(KlineBaseModel):

    def __init__(self, kline_bar):
        self.pair = kline_bar["s"] + "USDT"
        self.open_time = kline_bar["t"]
        self.open = float(kline_bar["o"])
        self.high = float(kline_bar["h"])
        self.low = float(kline_bar["l"])
        self.close = float(kline_bar["c"])
        self.volume = float(kline_bar["v"])
        self.interval = kline_bar["i"]
        self.time = self.open_time
        # self.volume = float(kline_bar[5])
        # self.close_time = kline_bar[6]
        # self.quote_asset_volume = float(kline_bar[7])
        # self.number_of_trades = kline_bar[8]
        # self.taker_buy_base_asset_volume = float(kline_bar[9])
        # self.taker_buy_quote_asset_volume = float(kline_bar[10])
        # self.ignore = float(kline_bar[11])



class LighterKlineBar(BaseModel):

    def __init__(self, candlestick):
        # Bybit K线格式: [start_time, open, high, low, close, volume, turnover]
        self.open_time = candlestick.timestamp  # 开始时间
        self.open = candlestick.open
        self.high = candlestick.high
        self.low = candlestick.low
        self.close = candlestick.close
        self.volume = candlestick.volume0  # 成交量
        self.turnover = candlestick.volume1  # 成交量  # 成交额
        self.time = self.open_time

    @property
    def change_rate(self):
        """计算涨跌幅"""
        if self.open == 0:
            return 0
        return (self.close - self.open) / self.open

    def is_up(self):
        """判断是否上涨"""
        return self.close > self.open

    def is_down(self):
        """判断是否下跌"""
        return self.close < self.open
