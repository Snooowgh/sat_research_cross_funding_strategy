# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : orderbook_model
@Description :
@Time        : 2024/9/26 12:46
"""
import time


class BinanceOrderBook:
    class OrderBookItems:
        def __init__(self, order_book_item):
            self.price = float(order_book_item[0])
            self.quantity = float(order_book_item[1])

        @property
        def usd_value(self):
            return self.price * self.quantity

        def __str__(self):
            return f"{self.price}({self.quantity})"

        def __repr__(self):
            return self.__str__()

    def __init__(self, order_book_res, _pair=None):
        self._pair = _pair
        self.asks = [self.OrderBookItems(d) for d in order_book_res["asks"]]
        self.bids = [self.OrderBookItems(d) for d in order_book_res["bids"]]
        self.mid_price = (self.asks[0].price + self.bids[0].price) / 2
        self.time = None

    @property
    def pair(self):
        return self._pair

    def get_mid_price(self):
        return self.mid_price

    def get_sell_price_by_level(self, level=0):
        return self.asks[level].price

    def get_buy_price_by_level(self, level=0):
        return self.bids[level].price

    def get_sell_price_vwap(self):
        """
        获取合理的挂单卖价
        - 大单前方
        - 价格离一档有距离
        :return:
        """
        ask_volume = sum([x.quantity * x.price for x in self.asks[1:]])
        ask_quantity = sum([x.quantity for x in self.asks[1:]])
        # ask_count = sum([x.order_count for x in self.asks])
        ask_vwap = ask_volume / ask_quantity if ask_quantity != 0 else 0
        return ask_vwap

    def get_buy_price_vwap(self):
        """
        获取合理的挂单买价
        - 大单前方
        - 价格离一档有距离
        :return:
        """
        bid_volume = sum([x.quantity * x.price for x in self.bids[1:]])
        bid_quantity = sum([x.quantity for x in self.bids[1:]])
        # bid_count = sum([x.order_count for x in self.bids])
        bid_vwap = bid_volume / bid_quantity if bid_quantity != 0 else 0
        return bid_vwap

    def __str__(self):
        text = f"{self.mid_price} {self.bids[:3]}/{self.asks[:3]}"
        if self.time:
            text += f" {(time.time() - self.time / 1000):.3f}s"
        return text


class OkxOrderBook(BinanceOrderBook):
    class OrderBookItems(BinanceOrderBook.OrderBookItems):
        def __init__(self, order_book_item):
            super().__init__(order_book_item)
            self.order_count = float(order_book_item[3])

    def __init__(self, order_book_res, _pair=None):
        BinanceOrderBook.__init__(self, order_book_res)
        self.time = int(order_book_res["ts"])
        self._pair = _pair

    @property
    def pair(self):
        if self._pair is None:
            raise ValueError("Pair is not set")
        return self._pair.replace("-SWAP", "").replace("-", "")


class HyperLiquidOrderBook(BinanceOrderBook):
    class OrderBookItems(BinanceOrderBook.OrderBookItems):
        def __init__(self, item_data):
            self.price = float(item_data["px"])
            self.quantity = float(item_data["sz"])
            self.order_count = int(item_data["n"])

    def __init__(self, data):
        self._pair = data["coin"] + "USDT"
        levels = data["levels"]
        self.asks = [self.OrderBookItems(d) for d in levels[1]]
        self.bids = [self.OrderBookItems(d) for d in levels[0]]
        self.mid_price = (self.asks[0].price + self.bids[0].price) / 2
        self.time = data["time"]


class BybitOrderBook(BinanceOrderBook):
    class OrderBookItems(BinanceOrderBook.OrderBookItems):
        def __init__(self, order_book_item):
            self.price = float(order_book_item[0])
            self.quantity = float(order_book_item[1])

    def __init__(self, order_book_res, _pair=None):
        self._pair = _pair
        self.bids = [self.OrderBookItems(d) for d in order_book_res.get("b", [])]
        self.asks = [self.OrderBookItems(d) for d in order_book_res.get("a", [])]
        if self.bids and self.asks:
            self.mid_price = (self.asks[0].price + self.bids[0].price) / 2
        else:
            self.mid_price = 0
        self.time = int(order_book_res.get("ts", 0))

    @property
    def pair(self):
        return self._pair
