# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : spot_position_model
@Description :
@Time        : 2024/11/26 12:43
"""
from cex_tools.exchange_model.base_model import BaseModel, TradeDirection


class OkxSpotDetail(BaseModel):

    def __init__(self, spot_info):
        self.ccy = spot_info["ccy"]  # 'OL'
        if self.ccy == "USDT":
            self.pair = self.ccy
        else:
            self.pair = self.ccy + "USDT"  # 'OLUSDT'
        self.symbol = self.ccy  # 'OL'
        self.position_side = TradeDirection.long
        self.accAvgPx = float(spot_info.get("accAvgPx") or 0)  # '0.1318860746331176'
        self.availBal = float(spot_info.get("availBal") or 0)  # '757.159125'
        self.availEq = float(spot_info.get("availEq") or 0)  # '757.159125'
        self.borrowFroz = float(spot_info.get("borrowFroz") or 0)  # '0'
        self.cashBal = float(spot_info.get("cashBal") or 0)  # '757.159125'
        self.clSpotInUseAmt = spot_info["clSpotInUseAmt"]  # ''
        self.crossLiab = float(spot_info.get("crossLiab") or 0)  # '0'
        self.disEq = float(spot_info.get("disEq") or 0)  # '83.3941117548'
        self.eq = float(spot_info.get("eq") or 0)  # '757.159125'
        self.eqUsd = float(spot_info.get("eqUsd") or 0)  # '104.2426396935'
        self.fixedBal = float(spot_info.get("fixedBal") or 0)  # '0'
        self.frozenBal = float(spot_info.get("frozenBal") or 0)  # '0'
        self.imr = spot_info["imr"]  # ''
        self.interest = float(spot_info.get("interest") or 0)  # '0'
        self.isoEq = float(spot_info.get("isoEq") or 0)  # '0'
        self.isoLiab = float(spot_info.get("isoLiab") or 0)  # '0'
        self.isoUpl = float(spot_info.get("isoUpl") or 0)  # '0'
        self.liab = float(spot_info.get("liab") or 0)  # '0'
        self.maxLoan = float(spot_info.get("maxLoan") or 0)  # '2671.6171377454893'
        self.maxSpotInUse = spot_info["maxSpotInUse"]  # ''
        self.mgnRatio = spot_info["mgnRatio"]  # ''
        self.mmr = spot_info["mmr"]  # ''
        self.notionalLever = spot_info["notionalLever"]  # ''
        self.openAvgPx = float(spot_info.get("openAvgPx") or 0)  # '0.1318267258995327'
        self.ordFrozen = float(spot_info.get("ordFrozen") or 0)  # '0'
        self.rewardBal = spot_info["rewardBal"]  # ''
        self.smtSyncEq = float(spot_info.get("smtSyncEq") or 0)  # '0'
        self.spotBal = float(spot_info.get("spotBal") or 0)  # '757.159125'
        self.spotCopyTradingEq = float(spot_info.get("spotCopyTradingEq") or 0)  # '0'
        self.spotInUseAmt = spot_info["spotInUseAmt"]  # ''
        self.spotIsoBal = float(spot_info.get("spotIsoBal") or 0)  # '0'
        self.spotUpl = float(spot_info.get("spotUpl") or 0)  # '4.428831259794996'
        self.spotUplRatio = float(spot_info.get("spotUplRatio") or 0)  # '0.0443709275228844'
        self.stgyEq = float(spot_info.get("stgyEq") or 0)  # '0'
        self.totalPnl = float(spot_info.get("totalPnl") or 0)  # '4.383894824603985'
        self.totalPnlRatio = float(spot_info.get("totalPnlRatio") or 0)  # '0.0439009606054991'
        self.twap = float(spot_info.get("twap") or 0)  # '0'
        self.uTime = int(spot_info["uTime"])  # '1732594096563'
        self.upl = float(spot_info.get("upl") or 0)  # '0'
        self.uplLiab = float(spot_info.get("uplLiab") or 0)  # '0'

        self.adl = 0
        self.entryPrice = self.openAvgPx
        self.fundingFee = 0
        self.funding_rate = 0
        self.positionAmt = self.spotBal
        self.notional = self.eqUsd
        self.unRealizedProfit = self.spotUpl
        self.updateTime = self.uTime
