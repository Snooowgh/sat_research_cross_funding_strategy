# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : position_model
@Description :
@Time        : 2024/9/26 12:45
"""
from cex_tools.exchange_model.base_model import BaseModel, TradeDirection


class BinancePositionDetail(BaseModel):

    def __init__(self, binance_position, exchange_code=None):
        self.exchange_code = exchange_code
        self.adl = int(binance_position.get("adl") or 0)  # 1~5
        self.entryPrice = float(binance_position.get("entryPrice"))  # "0.00000",
        self.breakEvenPrice = float(binance_position.get("breakEvenPrice", 0))  # "0.0",
        self.marginType = binance_position.get("marginType")  # "isolated",
        self.isAutoAddMargin = binance_position.get("isAutoAddMargin")  # "false",
        self.isolatedMargin = float(binance_position.get("isolatedMargin", 0))  # "0.00000000",
        # self.leverage = float(binance_position.get("leverage"))  # "10",
        self.liquidationPrice = float(binance_position.get("liquidationPrice"))  # "0",
        self.fundingFee = 0
        self.markPrice = float(binance_position.get("markPrice"))  # "6679.50671178",
        # self.maxNotionalValue = float(binance_position.get("maxNotionalValue"))  # "20000000",
        self.positionAmt = float(binance_position.get("positionAmt"))  # "0.000",
        self.notional = float(binance_position.get("notional"))  # "0", ,
        self.isolatedWallet = float(binance_position.get("isolatedWallet", 0))  # "0",
        self.pair = binance_position.get("symbol")  # "BTCUSDT",
        self.symbol = self.pair.replace("USDT", "")
        self.unRealizedProfit = float(binance_position.get("unRealizedProfit"))  # "0.00000000",
        self.positionSide = binance_position.get("positionSide")  # "BOTH",
        self.updateTime = float(binance_position.get("updateTime"))  # 0
        self.position_side = TradeDirection.long if self.positionAmt > 0 else TradeDirection.short
        self.funding_rate = None

    @property
    def profit_rate(self):
        if self.notional != 0:
            return self.unRealizedProfit / abs(self.notional)
        return 0

    def set_funding_rate(self, funding_rate):
        self.funding_rate = funding_rate

    def get_funding_rate(self):
        if self.funding_rate is None:
            return 0
        return self.funding_rate

    def __str__(self):
        return (f"Position(symbol={self.symbol}, side={self.position_side}, "
                f"size={self.positionAmt}, entry_price={self.entryPrice}, "
                f"pnl={self.unRealizedProfit:.2f}, "
                f"pnl_rate={self.profit_rate:.4%})")



class OkxPositionDetail(BinancePositionDetail):

    def __init__(self, binance_position, exchange_code=None):
        self.exchange_code = exchange_code
        self.adl = int(binance_position.get("adl") or 1)  # 1~5
        self.entryPrice = float(binance_position.get("avgPx")) if binance_position.get("avgPx") else 0  # "0.00000",
        # self.breakEvenPrice = float(binance_position.get("breakEvenPrice"))  # "0.0",
        # self.marginType = binance_position.get("marginType")  # "isolated",
        # self.isAutoAddMargin = binance_position.get("isAutoAddMargin")  # "false",
        # self.isolatedMargin = float(binance_position.get("isolatedMargin"))  # "0.00000000",
        # self.leverage = float(binance_position.get("leverage"))  # "10",
        self.fundingFee = float(binance_position.get("fundingFee")) if binance_position.get("fundingFee") else 0
        self.liquidationPrice = float(binance_position.get("liqPx")) if binance_position.get("liqPx") else 0  # "0",
        self.markPrice = float(binance_position.get("markPx")) if binance_position.get(
            "markPx") else 0  # "6679.50671178",
        # self.maxNotionalValue = float(binance_position.get("maxNotionalValue"))  # "20000000",
        self.positionSheetAmt = float(binance_position.get("pos")) if binance_position.get("pos") else 0
        self.positionAmt = float(binance_position.get("amt")) if binance_position.get("amt") else 0
        self.notional = float(binance_position.get("notionalUsd")) if binance_position.get(
            "notionalUsd") else 0  # "0", ,
        self.notional *= -1 if self.positionAmt < 0 else 1
        # self.isolatedWallet = float(binance_position.get("isolatedWallet"))  # "0",
        self.pair = binance_position.get("instId").replace("SWAP", "").replace("-", "")
        self.symbol = self.pair.replace("USDT", "")  # BTC-USDT-SWAP "BTCUSDT",
        self.unRealizedProfit = float(binance_position.get("upl")) if binance_position.get("upl") else 0
        # self.positionSide = binance_position.get("positionSide")  # "BOTH",
        self.updateTime = float(binance_position.get("uTime")) if binance_position.get("uTime") else 0  # 0
        if self.positionAmt > 0:
            self.position_side = TradeDirection.long
        elif self.positionAmt < 0:
            self.position_side = TradeDirection.short
        else:
            self.position_side = None
        self.funding_rate = None



class BitgetPositionDetail(BinancePositionDetail):

    def __init__(self, binance_position, exchange_code=None):
        self.exchange_code = exchange_code
        self.adl = int(binance_position.get("adl") or 1)  # 1~5
        self.entryPrice = None  # "0.00000",
        self.breakEvenPrice = float(binance_position.get("breakEvenPrice"))  # "0.0",
        self.marginType = binance_position.get("marginMode")  # "isolated",
        # self.isAutoAddMargin = binance_position.get("isAutoAddMargin")  # "false",
        # self.isolatedMargin = float(binance_position.get("isolatedMargin"))  # "0.00000000",
        self.leverage = float(binance_position.get("leverage"))  # "10",
        self.liquidationPrice = float(binance_position.get("liquidationPrice"))  # "0",
        self.fundingFee = 0
        self.markPrice = float(binance_position.get("markPrice"))  # "6679.50671178",
        # self.maxNotionalValue = float(binance_position.get("maxNotionalValue"))  # "20000000",
        self.positionAmt = float(binance_position.get("positionAmt"))  # "0.000",
        self.notional = float(binance_position.get("notional"))  # "0", ,
        self.isolatedWallet = float(binance_position.get("isolatedWallet"))  # "0",
        self.pair = binance_position.get("symbol")
        self.symbol = self.pair.replace("USDT", "")  # "BTCUSDT",
        self.unRealizedProfit = float(binance_position.get("unRealizedProfit"))  # "0.00000000",
        self.positionSide = binance_position.get("positionSide")  # "BOTH",
        self.updateTime = float(binance_position.get("updateTime"))  # 0
        self.position_side = TradeDirection.long if self.positionAmt > 0 else TradeDirection.short
        self.funding_rate = None



class HyperliquidPositionDetail(BinancePositionDetail):

    def __init__(self, binance_position, exchange_code=None):
        self.exchange_code = exchange_code
        self.adl = int(binance_position.get("adl") or 1)  # 1~5
        self.entryPrice = float(binance_position.get("entryPx"))  # "0.00000",
        self.breakEvenPrice = 0  # "0.0",
        self.marginType = binance_position.get("leverage")["type"]  # "isolated",
        self.isAutoAddMargin = None  # "false",
        self.isolatedMargin = None  # "0.00000000",
        self.leverage = float(binance_position.get("leverage")["value"])  # "10",
        self.liquidationPrice = float(
            binance_position.get("liquidationPx") if binance_position.get("liquidationPx") else 0)  # "0",
        # allTime, sinceOpen, sinceChange
        self.fundingFee = -float(binance_position.get("cumFunding")["sinceOpen"])
        self.markPrice = None  # "6679.50671178",
        self.maxNotionalValue = None  # "20000000",
        self.positionAmt = float(binance_position.get("szi"))  # "0.000",
        self.notional = float(binance_position.get("positionValue")) * (
                self.positionAmt / abs(self.positionAmt))  # "0", ,
        self.isolatedWallet = None  # "0",
        self.symbol = binance_position.get("coin")
        self.pair = self.symbol + "USDT"
        self.unRealizedProfit = float(binance_position.get("unrealizedPnl"))  # "0.00000000",
        self.updateTime = None  # 0
        self.position_side = TradeDirection.long if self.positionAmt > 0 else TradeDirection.short
        self.funding_rate = None



class LighterPositionDetail(BinancePositionDetail):

    def __init__(self, binance_position, exchange_code=None):
        self.exchange_code = exchange_code
        binance_position = binance_position.to_dict()
        self.adl = int(binance_position.get("adl") or 0)  # 1~5
        self.entryPrice = float(binance_position.get("avg_entry_price"))  # "0.00000",
        # self.breakEvenPrice = float(binance_position.get("breakEvenPrice"))  # "0.0",
        # self.marginType = binance_position.get("marginType")  # "isolated",
        # self.isAutoAddMargin = binance_position.get("isAutoAddMargin")  # "false",
        # self.isolatedMargin = float(binance_position.get("isolatedMargin"))  # "0.00000000",
        # self.leverage = float(binance_position.get("leverage"))  # "10",
        self.liquidationPrice = float(binance_position.get("liquidation_price"))  # "0",
        self.fundingFee = 0
        # self.markPrice = float(binance_position.get("markPrice"))  # "6679.50671178",
        # self.maxNotionalValue = float(binance_position.get("maxNotionalValue"))  # "20000000",
        self.positionAmt = float(binance_position.get("position")) * binance_position.get("sign")  # "0.000",
        self.notional = float(binance_position.get("position_value")) * binance_position.get("sign")  # "0", ,
        # self.isolatedWallet = float(binance_position.get("isolatedWallet"))  # "0",
        self.symbol = binance_position.get("symbol")
        self.pair = self.symbol + "USDT"  # "BTCUSDT"
        self.unRealizedProfit = float(binance_position.get("unrealized_pnl"))  # "0.00000000",
        self.position_side = TradeDirection.long if self.positionAmt > 0 else TradeDirection.short
        self.funding_rate = None



class BinanceUnifiedPositionDetail(BinancePositionDetail):
    """
    基于Binance Portfolio Margin SDK数据的仓位对象
    具有与BinancePositionDetail完全相同的属性，但直接从Portfolio Margin SDK数据对象中获取值
    """

    def __init__(self, portfolio_margin_position_data, exchange_code=None):
        """
        从Portfolio Margin SDK的仓位数据对象初始化

        Args:
            portfolio_margin_position_data: Portfolio Margin SDK返回的仓位数据对象
            exchange_code: 交易所代码
        """
        self.exchange_code = exchange_code

        # Portfolio Margin SDK字段映射到BinancePositionDetail标准字段
        # 根据注释中的数据格式: QueryUmPositionInformationResponse(entry_price='0.30923', leverage='5', mark_price='0.3092429', max_notional_value='6000000.0', position_amt='19.0', notional='5.8756151', symbol='TRXUSDT', un_realized_profit='0.0002451', liquidation_price='0', position_side='BOTH', update_time=1760718563795, additional_properties={})

        self.adl = 0  # Portfolio Margin SDK可能没有这个字段，设为默认值
        self.entryPrice = float(portfolio_margin_position_data.entry_price) if portfolio_margin_position_data.entry_price else 0
        self.breakEvenPrice = 0  # 默认值
        self.marginType = "cross"  # Portfolio Margin默认使用全仓
        self.isAutoAddMargin = "false"  # 默认值
        self.isolatedMargin = 0  # 默认值
        self.leverage = float(portfolio_margin_position_data.leverage) if portfolio_margin_position_data.leverage else 1
        self.liquidationPrice = float(portfolio_margin_position_data.liquidation_price) if portfolio_margin_position_data.liquidation_price else 0
        self.fundingFee = 0  # 默认值
        self.markPrice = float(portfolio_margin_position_data.mark_price) if portfolio_margin_position_data.mark_price else 0
        self.maxNotionalValue = float(portfolio_margin_position_data.max_notional_value) if portfolio_margin_position_data.max_notional_value else 0
        self.positionAmt = float(portfolio_margin_position_data.position_amt) if portfolio_margin_position_data.position_amt else 0
        self.notional = float(portfolio_margin_position_data.notional) if portfolio_margin_position_data.notional else 0
        self.isolatedWallet = 0  # 默认值
        self.pair = portfolio_margin_position_data.symbol
        self.symbol = self.pair.replace("USDT", "")
        self.unRealizedProfit = float(portfolio_margin_position_data.un_realized_profit) if portfolio_margin_position_data.un_realized_profit else 0
        self.positionSide = portfolio_margin_position_data.position_side
        self.updateTime = float(portfolio_margin_position_data.update_time) if portfolio_margin_position_data.update_time else 0

        # 确定仓位方向
        if self.positionAmt > 0:
            self.position_side = TradeDirection.long
        elif self.positionAmt < 0:
            self.position_side = TradeDirection.short
        else:
            self.position_side = None

        self.funding_rate = None


class BybitPositionDetail(BinancePositionDetail):
    """Bybit持仓详情"""

    def __init__(self, binance_position, exchange_code=None):
        self.exchange_code = exchange_code
        self.adl = int(binance_position.get("adlRankIndicator") or 0)  # 0~5
        self.entryPrice = float(binance_position.get("avgPrice") or 0)  # 平均入场价
        self.liquidationPrice = float(binance_position.get("liqPrice") or 0)  # 强平价格
        self.fundingFee = 0  # Bybit在仓位信息中不直接返回funding fee
        self.markPrice = float(binance_position.get("markPrice") or 0)  # 标记价格
        self.leverage = float(binance_position.get("leverage") or 1)  # 杠杆倍数

        # 仓位数量（Bybit使用size字段）
        size = float(binance_position.get("size") or 0)
        side = binance_position.get("side")  # "Buy" or "Sell"

        # 根据方向确定仓位正负
        self.positionAmt = size if side == "Buy" else -size
        self.notional = float(binance_position.get("positionValue") or 0)
        self.notional = self.notional if side == "Buy" else -self.notional
        self.pair = binance_position.get("symbol")
        self.symbol = self.pair.replace("USDT", "")  # "BTCUSDT"
        self.unRealizedProfit = float(binance_position.get("unrealisedPnl") or 0)  # 未实现盈亏
        self.updateTime = int(binance_position.get("updatedTime") or 0)  # 更新时间（毫秒）

        # 确定仓位方向
        if self.positionAmt > 0:
            self.position_side = TradeDirection.long
        elif self.positionAmt < 0:
            self.position_side = TradeDirection.short
        else:
            self.position_side = None

        self.funding_rate = None

