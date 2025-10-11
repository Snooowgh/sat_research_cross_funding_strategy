# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : account_summary_model
@Description :
@Time        : 2024/9/27 13:08
"""
import time

from cex_tools.exchange_model.base_model import BaseModel
from cex_tools.exchange_model.position_model import OkxPositionDetail, HyperliquidPositionDetail


class BaseAccountSummaryModel(BaseModel):

    def __init__(self):
        self.exchange_code = ""
        self.total_balance = 0
        self.available_balance = 0
        self.notionalUsd = 0
        self.cur_position_list = []
        self.ts = 0
        self.time = 0

    def __str__(self):
        if self.cur_position_list is None:
            self.cur_position_list = []
        if self.total_balance is None:
            self.total_balance = 0
        pair_list = [x.symbol for x in self.cur_position_list]
        pair_list.sort()
        total_pos_value = sum([abs(x.notional) for x in self.cur_position_list])
        return f"📌 {self.exchange_code.upper()} ${self.total_balance:.2f}(杠杆率: {self.leverage:.2f}, 持仓比例: {self.position_ratio:.2%}) 持仓总金额: ${total_pos_value:.2f} {time.time() - self.time / 1000:.3f}s"

    @property
    def leverage(self):
        try:
            total_pos_value = sum([abs(x.notional) for x in self.cur_position_list])
            return total_pos_value / self.total_balance
        except:
            return 0

    @property
    def position_ratio(self):
        return 1 - self.available_balance / self.total_balance


class OkxAccountSummaryModel(BaseAccountSummaryModel):

    def __init__(self, account_summary_data, exchange_code=""):
        super().__init__()
        # 同时处理账户信息, 仓位信息
        self.exchange_code = exchange_code
        self.total_balance = None
        self.cur_position_list = None
        self.available_balance = None
        self.notionalUsd = None
        if not isinstance(account_summary_data, list):
            # 账户信息
            # https://www.okx.com/docs-v5/zh/#trading-account-websocket-account-channel
            self.total_balance = float(account_summary_data["totalEq"])
            self.available_balance = sum([float(x["availBal"]) for x in account_summary_data["details"]])
            self.notionalUsd = float(account_summary_data["notionalUsd"])
            # self.imr = float(account_summary_data["imr"])
            # self.isoEq = float(account_summary_data["isoEq"])
            # self.mgnRatio = float(account_summary_data["mgnRatio"])
            # self.mmr = float(account_summary_data["mmr"])
            # self.notionalUsd = float(account_summary_data["notionalUsd"])
            # self.ordFroz = float(account_summary_data["ordFroz"])
            # self.totalEq = float(account_summary_data["totalEq"])
            # self.uTime = float(account_summary_data["uTime"])
            # self.upl = float(account_summary_data["upl"])
        else:
            # 仓位信息
            self.cur_position_list = list(map(lambda x: OkxPositionDetail(x), account_summary_data))


class HyperLiquidAccountSummaryModel(BaseAccountSummaryModel):

    def __init__(self, account_summary_data, exchange_code=""):
        super().__init__()
        self.exchange_code = exchange_code
        account_summary_data = account_summary_data["clearinghouseState"]
        self.total_balance = float(account_summary_data['marginSummary']['accountValue'])
        self.available_balance = float(account_summary_data['withdrawable'])
        self.notionalUsd = float(account_summary_data['marginSummary']['totalNtlPos'])
        self.cur_position_list = list(map(lambda x: HyperliquidPositionDetail(x["position"]),
                                          account_summary_data['assetPositions']))
        self.time = account_summary_data['time']
        self.ts = self.time