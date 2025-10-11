# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : order_model
@Description :
@Time        : 2024/9/26 12:45
"""
import datetime
import time

from cex_tools.exchange_model.base_model import BaseModel
from cex_tools.cex_enum import TradeSide


class BinanceOrderStatus:
    FILLED = "FILLED"
    NEW = "NEW"
    CANCELED = "CANCELED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    EXPIRED = "EXPIRED"
    # hl
    REJECTED = "REJECTED"


class BinanceOrderType:
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    STOP = "STOP"
    TAKE_PROFIT = "TAKE_PROFIT"


class BaseOrderModel(BaseModel):

    def __str__(self):
        try:
            return f"{self.orderId} {self.status} {self.pair} {self.type} {self.side} {self.avgPrice} {self.executedQty} / {self.origQty} {self.get_order_create_datetime_str()} {time.time() - self.time / 1000:.3f}s"
        except:
            super().__str__()

    def get_order_create_timestamp_sec(self):
        return int(self.time / 1000)

    def get_order_create_datetime_str(self):
        return datetime.datetime.fromtimestamp(self.get_order_create_timestamp_sec()).strftime('%Y-%m-%d %H:%M:%S')


class OkxOrder(BaseOrderModel):

    def __init__(self, order_info):
        self.orderId = order_info['ordId']  # 30125120004,
        self.symbol = order_info['instId'].replace("-SWAP", "").replace("-", "")  # 'LINKUSDT',
        self.pair = self.symbol
        if order_info['state'] == "canceled":
            self.status = BinanceOrderStatus.CANCELED
        elif order_info['state'] == "live":
            self.status = BinanceOrderStatus.NEW
        elif order_info['state'] == "partially_filled":
            self.status = BinanceOrderStatus.PARTIALLY_FILLED
        elif order_info['state'] == "filled":
            self.status = BinanceOrderStatus.FILLED
        elif order_info['state'] == "mmp_canceled":
            self.status = BinanceOrderStatus.CANCELED  # 做市商保护的撤单
        self.clientOrderId = order_info['clOrdId']  # 'p27cmg6ima2fOmUzb1wVGb',
        self.price = float(order_info['px']) if order_info['px'] else 0  # '15.275',
        self.avgPrice = float(order_info['avgPx']) if order_info['avgPx'] else 0  # '15.27500',
        # ⚠️ 合约张数 数值需要转换
        self.origQty = float(order_info['sz']) if order_info['sz'] else 0  # '654.88',
        self.executedQty = float(order_info['accFillSz']) if order_info['accFillSz'] else 0  # '654.88', accFillSz?
        self.cumQuote = self.avgPrice * self.executedQty  # '10003.29200',
        self.timeInForce = ""  # 'GTC',
        self.type = order_info['ordType'].upper()  # 'LIMIT',
        self.reduceOnly = order_info['reduceOnly'] == 'true'  # False,
        self.closePosition = ""  # False,
        self.side = order_info['side'].upper()  # 'BUY',
        self.positionSide = order_info['posSide']  # 'net',
        self.stopPrice = float(order_info['slOrdPx']) if order_info['slOrdPx'] else 0  # '0',
        self.workingType = ""  # 'CONTRACT_PRICE',
        self.priceMatch = ""  # 'NONE',
        self.selfTradePreventionMode = ""  # 'NONE',
        self.goodTillDate = ""  # 0,
        self.priceProtect = ""  # False,
        self.origType = order_info['ordType'].upper()  # 'LIMIT',
        self.time = int(order_info['cTime'] if order_info['cTime'] else 0)  # 1705372648922,
        self.fillTime = int(order_info['fillTime'] if order_info['fillTime'] else 0)  # 1705372648922,
        self.updateTime = int(order_info['uTime'] if order_info['uTime'] else 0)  # 1705372661393


class BinanceOrder(BaseOrderModel):

    def __init__(self, order_info):
        self.orderId = order_info['orderId']  # 30125120004,
        self.symbol = order_info['symbol']  # 'LINKUSDT',
        self.pair = self.symbol
        self.status = order_info['status']  # 'FILLED',
        self.clientOrderId = order_info['clientOrderId']  # 'p27cmg6ima2fOmUzb1wVGb',
        self.price = float(order_info['price']) if order_info['price'] else 0  # '15.275',
        self.avgPrice = float(order_info['avgPrice']) if order_info['avgPrice'] else 0  # '15.27500',
        self.origQty = float(order_info['origQty']) if order_info['origQty'] else 0  # '654.88',
        self.executedQty = float(order_info['executedQty']) if order_info['executedQty'] else 0  # '654.88',
        self.cumQuote = float(order_info['cumQuote']) if order_info['cumQuote'] else 0  # '10003.29200',
        self.timeInForce = order_info['timeInForce']  # 'GTC',
        self.type = order_info['type']  # 'LIMIT',
        self.reduceOnly = order_info['reduceOnly']  # False,
        self.closePosition = order_info['closePosition']  # False,
        self.side = order_info['side']  # 'BUY',
        self.positionSide = order_info['positionSide']  # 'BOTH',
        self.stopPrice = float(order_info['stopPrice']) if order_info['stopPrice'] else 0  # '0',
        self.workingType = order_info['workingType']  # 'CONTRACT_PRICE',
        self.priceMatch = order_info['priceMatch']  # 'NONE',
        self.selfTradePreventionMode = order_info['selfTradePreventionMode']  # 'NONE',
        self.goodTillDate = order_info['goodTillDate']  # 0,
        self.priceProtect = order_info['priceProtect']  # False,
        self.origType = order_info['origType']  # 'LIMIT',
        self.time = int(order_info['time'])  # 1705372648922,
        self.updateTime = int(order_info['updateTime'])  # 1705372661393


class HyperLiquidOrder(BaseOrderModel):

    def __init__(self, order_info):
        self.orderId = order_info['oid']  # 30125120004,
        self.symbol = order_info['coin'] + "USDT"  # 'LINKUSDT',
        self.pair = self.symbol
        self.status = None  # 'FILLED',
        self.clientOrderId = None  # 'p27cmg6ima2fOmUzb1wVGb',
        self.price = float(order_info['limitPx']) if order_info.get('limitPx') else 0  # '15.275',
        self.avgPrice = float(order_info['limitPx']) if order_info.get('limitPx') else 0
        self.origQty = float(order_info['origSz']) if order_info.get('origSz') else 0  # '654.88',
        self.executedQty = self.origQty - float(order_info['sz']) if order_info.get('sz') else 0  # '654.88',
        self.cumQuote = None
        self.timeInForce = None
        self.type = order_info.get("orderType", "").upper()
        self.reduceOnly = order_info.get("reduceOnly", False)
        self.closePosition = None
        if order_info['side'] == "B":
            self.side = TradeSide.BUY  # 'BUY',
        elif order_info['side'] == "A":
            self.side = TradeSide.SELL
        self.positionSide = None
        self.stopPrice = None
        self.workingType = None
        self.priceMatch = None
        self.selfTradePreventionMode = None
        self.goodTillDate = None
        self.priceProtect = None
        self.origType = None
        self.time = int(order_info['timestamp'])  # 1705372648922,
        self.updateTime = None

    def set_status(self, status):
        if status == "open":
            self.status = BinanceOrderStatus.NEW
        elif status == "canceled":
            self.status = BinanceOrderStatus.CANCELED
        elif status == "filled":
            self.status = BinanceOrderStatus.FILLED
        elif status == 'rejected':
            self.status = BinanceOrderStatus.REJECTED


class LighterOrder(BaseOrderModel):

    def __init__(self, order_info):
        order_info = order_info.to_dict()
        self.orderId = order_info['order_id']  # 30125120004,
        self.symbol = order_info['market_index']  # 'LINKUSDT',
        self.pair = self.symbol
        self.status = order_info['status'].upper()  # 'FILLED',
        self.clientOrderId = order_info['client_order_id']  # 'p27cmg6ima2fOmUzb1wVGb',
        self.price = float(order_info['price']) if order_info['price'] else 0  # '15.275',
        if float(order_info['filled_base_amount']) == 0:
            self.avgPrice = 0
        else:
            self.avgPrice = float(order_info["filled_quote_amount"]) / float(order_info['filled_base_amount'])
        self.origQty = float(order_info['initial_base_amount']) if order_info['initial_base_amount'] else 0  # '654.88',
        self.executedQty = float(order_info['filled_base_amount']) if order_info['filled_base_amount'] else 0  # '654.88',
        self.cumQuote = float(order_info['filled_quote_amount']) if order_info['filled_quote_amount'] else 0  # '10003.29200',
        self.timeInForce = order_info['time_in_force']  # "immediate-or-cancel",
        self.type = order_info['type'].upper()  # 'LIMIT',
        self.reduceOnly = order_info['reduce_only']  # False,
        # self.closePosition = order_info['closePosition']  # False,
        self.side = "SELL" if order_info["is_ask"] else "BUY"  # 'BUY',
        # self.positionSide = order_info['positionSide']  # 'BOTH',
        self.stopPrice = float(order_info['trigger_price']) if order_info['trigger_price'] else 0  # '0',
        # self.workingType = order_info['workingType']  # 'CONTRACT_PRICE',
        # self.priceMatch = order_info['priceMatch']  # 'NONE',
        # self.selfTradePreventionMode = order_info['selfTradePreventionMode']  # 'NONE',
        # self.goodTillDate = order_info['goodTillDate']  # 0,
        # self.priceProtect = order_info['priceProtect']  # False,
        self.origType = self.type  # 'LIMIT',
        self.time = int(order_info['created_at'] * 1000)  # 1705372648922, # 1758433998
        self.updateTime = int(order_info['updated_at'] * 1000)  # 1705372661393


class BybitOrder(BaseOrderModel):

    def __init__(self, order_info):
        self.orderId = order_info.get('orderId')  # 订单ID
        self.symbol = order_info.get('symbol')  # 交易对符号
        self.pair = self.symbol
        self.status = order_info.get('orderStatus')  # 订单状态
        self.clientOrderId = order_info.get('orderLinkId')  # 客户端订单ID
        self.price = float(order_info.get('price', 0)) if order_info.get('price') else 0  # 价格
        self.avgPrice = float(order_info.get('avgPrice', 0)) if order_info.get('avgPrice') else 0  # 平均价格
        self.origQty = float(order_info.get('qty', 0)) if order_info.get('qty') else 0  # 原始数量
        self.executedQty = float(order_info.get('cumExecQty', 0)) if order_info.get('cumExecQty') else 0  # 已执行数量
        self.cumQuote = float(order_info.get('cumExecValue', 0)) if order_info.get('cumExecValue') else 0  # 累计成交金额
        self.timeInForce = order_info.get('timeInForce')  # 时效性
        self.type = order_info.get('orderType')  # 订单类型
        self.reduceOnly = order_info.get('reduceOnly', False)  # 只减仓
        self.closePosition = order_info.get('closeOnTrigger', False)  # 触发平仓
        self.side = order_info.get('side')  # 买卖方向
        self.positionSide = order_info.get('positionSide')  # 持仓方向
        self.stopPrice = float(order_info.get('triggerPrice', 0)) if order_info.get('triggerPrice') else 0  # 触发价格
        self.workingType = order_info.get('triggerBy')  # 触发类型
        self.priceMatch = order_info.get('priceMatch')  # 价格匹配
        self.selfTradePreventionMode = order_info.get('stpMode')  # 自成交预防模式
        self.goodTillDate = order_info.get('goodTillDate')  # 有效期
        self.priceProtect = order_info.get('tpslMode')  # 止盈止损模式
        self.origType = order_info.get('orderType')  # 原始订单类型
        self.time = int(order_info.get('createdTime', 0))  # 创建时间
        self.updateTime = int(order_info.get('updatedTime', 0))  # 更新时间
