# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : cex_enum
@Description :
@Time        : 2024/7/5 17:19
"""
from enum import Enum


class TradeSide:
    BUY = "BUY"
    SELL = "SELL"


class TradingMode(Enum):
    """交易模式枚举"""
    TAKER_TAKER = "taker_taker"  # 市价单模式（taker+taker）
    LIMIT_TAKER = "limit_taker"  # 挂单对冲模式（maker + taker）


class ExchangeEnum:
    BINANCE = "binance"
    OKX = "okx"
    OKX_SPOT = "okx_spot"
    BITGET = "bitget"
    HUOBI = "huobi"
    BITMEX = "bitmex"
    DERIBIT = "deribit"
    BYBIT = "bybit"
    COINBASE = "coinbase"
    HYPERLIQUID = "hyperliquid"

    LIGHTER = "lighter"
    EDGEX = "edgex"
    ASTER = "aster"
