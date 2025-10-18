# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : order_event_model
@Description : 订单事件数据模型
@Time        : 2025/10/15
"""

class OrderType:
    """订单类型"""
    LIMIT = "LIMIT"  # 限价单
    MARKET = "MARKET"  # 市价单
    LIQUIDATION = "LIQUIDATION"  # 爆仓


class ExecutionType:
    """执行类型"""
    NEW = "NEW"
    CANCELED = "CANCELED"
    CALCULATED = "CALCULATED"
    EXPIRED = "EXPIRED"
    TRADE = "TRADE"


class OrderStatusType:
    """状态"""
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    LIVE = "LIVE"
    EXPIRED = "EXPIRED"
    EXPIRED_IN_MATCH = "EXPIRED_IN_MATCH"


class OrderUpdateEvent:
    """订单更新时间模型"""

    def __init__(self,
                 exchange_code: str,
                 symbol: str,
                 client_order_id: str,
                 order_id,
                 trade_id,
                 side,
                 order_type: OrderType,
                 original_quantity,
                 price,
                 avg_price,
                 order_status: OrderStatusType,
                 order_last_filled_quantity,
                 order_filled_accumulated_quantity,
                 last_filled_price,
                 reduce_only,
                 position_side_mode,
                 timestamp):
        """
            Binance:
                https://developers.binance.com/docs/derivatives/portfolio-margin/user-data-streams/Event-Futures-Order-update

        """
        # 交易所代码
        self.exchange_code = exchange_code
        # 交易对符号
        self.symbol = symbol
        # 客户端订单ID
        self.client_order_id = client_order_id
        # 订单方向（买入/卖出）
        self.side = side
        # 订单类型（限价/市价）
        self.order_type = order_type
        # 订单数量
        self.original_quantity = original_quantity
        # 订单价格
        self.price = price
        # 平均成交价格
        self.avg_price = avg_price
        # 订单状态
        self.order_status = order_status
        # 交易所订单ID
        self.order_id = order_id
        # 交易ID
        self.trade_id = trade_id
        # 最近一次成交数量
        self.order_last_filled_quantity = order_last_filled_quantity
        # 累计成交数量
        self.order_filled_accumulated_quantity = order_filled_accumulated_quantity
        # 最近一次成交价格
        self.last_filled_price = last_filled_price
        # 是否只减仓
        self.reduce_only = reduce_only
        # 仓位方向（多仓/空仓）
        self.position_side_mode = position_side_mode
        # 事件时间戳
        self.timestamp = timestamp

    def __str__(self):
        return (f"OrderEvent(exchange_code={self.exchange_code}, symbol={self.symbol}, order_id={self.order_id}, "
                f"client_order_id={self.client_order_id}, trade_id={self.trade_id}, side={self.side}, "
                f"order_type={self.order_type}, quantity={self.original_quantity}, price={self.price}, "
                f"avg_price={self.avg_price}, "
                f"order_status={self.order_status}, filled_quantity={self.order_filled_accumulated_quantity}, "
                f"order_last_filled_quantity={self.order_last_filled_quantity}, "
                f"order_filled_accumulated_quantity={self.order_filled_accumulated_quantity}, "
                f"last_filled_price={self.last_filled_price}, "
                f"reduce_only={self.reduce_only}, position_side={self.position_side_mode}, timestamp={self.timestamp})")
