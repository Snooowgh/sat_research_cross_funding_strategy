# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : chance_searcher
@Description :
@Time        : 2024/9/23 20:27
"""
import time
from typing import List, Dict
from tenacity import retry, wait_exponential, stop_after_attempt
from tabulate import tabulate
from loguru import logger

from cex_tools.exchange_model.base_model import BaseModel, TradeDirection
from cex_tools.binance_future import BinanceFuture
from cex_tools.cex_enum import ExchangeEnum
from utils.parallelize_utils import parallelize_tasks

"""
    寻找交易机会
    - 有足够价差
    - 合适的费率/OI/成交额/订单簿深度
"""


class ChanceInfo(BaseModel):

    def __init__(self, chance_dict):
        info1 = chance_dict["info1"]
        self.diff = chance_dict["diff"]
        self.midPx = float(chance_dict["midPx"])
        self.funding1 = float(chance_dict.get("funding1", 0))
        if chance_dict.get("funding2") is None:
            # 处理HyperLiquid
            self.funding2 = float(chance_dict["funding"]) * 24 * 365
        else:
            # 处理CEX内
            self.funding2 = float(chance_dict["funding2"])
        self.funding_diff = self.funding1 - self.funding2
        self.funding_diff_abs = abs(self.funding_diff)
        self.funding_spot_profit_rate = self.funding_diff_abs
        self.funding_profit_rate = self.funding_diff_abs / 2
        if self.funding_diff > 0:
            self.position_side1 = TradeDirection.short
            self.position_side2 = TradeDirection.long
            self.price_diff_profit_rate = self.diff
        else:
            self.position_side1 = TradeDirection.long
            self.position_side2 = TradeDirection.short
            self.price_diff_profit_rate = -self.diff
        self.openInterestUSD = float(chance_dict.get("openInterest", 0)) * self.midPx
        self.prevDayPx = float(chance_dict.get("prevDayPx", 0))
        self.dayNtlVlm = float(chance_dict.get("dayNtlVlm", 0))
        self.premium = float(chance_dict.get("premium", 0))
        self.oraclePx = float(chance_dict.get("oraclePx", 0))
        self.markPx = float(chance_dict.get("markPx", 0))
        self.impactPxs = [float(chance_dict.get("impactPxs", [0])[0]), float(chance_dict.get("impactPxs", [0, 0])[1])]
        self.szDecimals = float(chance_dict.get("szDecimals", 0))
        self.name = chance_dict["name"]
        self.pair = self.name
        self.maxLeverage = float(chance_dict.get("maxLeverage", 1))
        self.vol1 = float(info1.get("volCcy24h", 0)) * float(info1.get("midPx", 0))
        # self.onlyIsolated = chance_dict["onlyIsolated"]


class ChanceSearcher:
    def __init__(self, exchange1, exchange2):
        self.exchange1 = exchange1
        self.exchange2 = exchange2

    def _needs_usdt_suffix_for_funding(self, exchange) -> bool:
        """
        判断交易所的get_funding_rate()是否需要USDT后缀

        :param exchange: 交易所对象
        :return: True表示需要加USDT后缀
        """
        from cex_tools.cex_enum import ExchangeEnum

        # Binance和Aster需要完整符号（如CRVUSDT）
        if hasattr(exchange, 'exchange_code'):
            if exchange.exchange_code in [ExchangeEnum.BINANCE, ExchangeEnum.ASTER]:
                return True
            # HyperLiquid和Lighter会自动转换，不需要后缀
            elif exchange.exchange_code in [ExchangeEnum.HYPERLIQUID, ExchangeEnum.LIGHTER]:
                return False

        # 默认返回True（保守策略）
        return True

    async def get_common_pairs(self) -> set:
        """
        获取两个交易所都支持的交易对集合

        :return: 交易对名称集合（不带USDT）
        """
        # 获取两个交易所的所有交易对
        from utils.coroutine_utils import safe_execute_async

        all_tick_info1 = await safe_execute_async(self.exchange1.get_all_tick_price)
        all_tick_info2 = await safe_execute_async(self.exchange2.get_all_tick_price)

        # 提取交易对名称
        pairs1 = {info["name"] for info in all_tick_info1}
        pairs2 = {info["name"] for info in all_tick_info2}

        # 返回交集
        common = pairs1 & pairs2
        return common

    @retry(wait=wait_exponential(multiplier=1, max=3), stop=stop_after_attempt(3))
    async def get_all_market_price_diff_map(self):
        start = time.time()
        # 使用统一的异步调用
        from utils.coroutine_utils import safe_execute_async

        all_tick_info1 = await safe_execute_async(self.exchange1.get_all_tick_price)
        all_tick_info2 = await safe_execute_async(self.exchange2.get_all_tick_price)

        if time.time() - start > 0.5:
            print(
                f"⚠️ {self.exchange1.exchange_code}-{self.exchange2.exchange_code} 获取价格信息耗时: {time.time() - start:.2f}s")
        price_diff_map = {}
        for info1 in all_tick_info1:
            for info2 in all_tick_info2:
                if info1["name"] == info2["name"]:
                    mid_price_diff = (info1["midPx"] - info2["midPx"]) / info2["midPx"]
                    price_diff_map[info1["name"]] = mid_price_diff
                    break
        # price_diff_list = list(filter(lambda x: abs(x["diff"]) > 0.001, price_diff_list))
        # price_diff_list.sort(key=lambda x: abs(x["diff"]), reverse=True)
        return price_diff_map, all_tick_info1, all_tick_info2

    async def search_all_chances(self, limit=None, include_hedge_chance=False, use_common_pairs_filter=True) -> (Dict[str, float], List[ChanceInfo]):
        """
            ‼️ 交易机会搜索

            include_hedge_chance: 包含对冲机会(低费率差 高价差)
            use_common_pairs_filter: 是否使用交易对预过滤（提升性能）
        :return:
        """
        price_diff_map, all_tick_info1, all_tick_info2 = await self.get_all_market_price_diff_map()

        # 可选：获取两个交易所的交易对交集，减少无效API调用
        common_pairs = None
        if use_common_pairs_filter:
            common_pairs = await self.get_common_pairs()

        chance_list = []

        MIN_PRICE_DIFF_PROFIT_RATE = 0.001
        MAX_REASONABLE_PRICE_DIFF = 0.10  # 最大合理价差10%，超过说明可能是不同资产

        price_diff_sorted_list = sorted(price_diff_map.items(), key=lambda x: abs(x[1]), reverse=True)
        if limit is not None:
            price_diff_sorted_list = price_diff_sorted_list[:limit]

        for symbol, diff in price_diff_sorted_list:
            # 预过滤：跳过交易所不都支持的交易对
            if use_common_pairs_filter and common_pairs and symbol not in common_pairs:
                continue

            # 过滤异常价差（可能是不同资产或数据错误）
            if abs(diff) > MAX_REASONABLE_PRICE_DIFF:
                logger.debug(
                    f"⚠️ {symbol} 价差异常 ({diff:.2%})，可能是不同资产或数据错误，已跳过"
                )
                continue
            info2 = list(filter(lambda x: x["name"] == symbol, all_tick_info2))[0]
            pair_name = info2["name"]
            diff = price_diff_map.get(pair_name)
            if not diff:
                continue
            try:
                if abs(diff) > MIN_PRICE_DIFF_PROFIT_RATE:
                    tmp = list(filter(lambda x: x["name"] == pair_name, all_tick_info1))
                    info2["info1"] = tmp[0]
                    info2["diff"] = diff

                    # 根据交易所类型动态添加USDT后缀
                    from utils.coroutine_utils import safe_execute_async
                    symbol1 = info2["name"] + "USDT" if self._needs_usdt_suffix_for_funding(self.exchange1) else info2["name"]
                    symbol2 = info2["name"] + "USDT" if self._needs_usdt_suffix_for_funding(self.exchange2) else info2["name"]

                    # 使用统一的异步调用获取费率
                    info2["funding1"] = await safe_execute_async(self.exchange1.get_funding_rate, symbol1)
                    info2["funding2"] = await safe_execute_async(self.exchange2.get_funding_rate, symbol2)
                else:
                    continue
                chance = ChanceInfo(info2)
                # if chance.funding2 < 0 and chance.funding1 < 0:
                #     # 费率不能同时为负
                #     continue
                if abs(chance.funding2) > 1 and abs(chance.funding1) > 1:
                    # 费率不能同时>100%
                    continue
                if not include_hedge_chance:
                    if chance.funding_profit_rate < 0.08 or chance.price_diff_profit_rate < MIN_PRICE_DIFF_PROFIT_RATE:
                        # 费率收益率和价差收益率不能太低
                        continue
                chance_list.append(chance)

            except Exception as e:
                print(f"{pair_name} {info2} 信息错误 error: {e}")
                continue
        chance_list.sort(key=lambda x: (x.price_diff_profit_rate, x.funding_profit_rate), reverse=True)
        if limit is not None:
            chance_list = chance_list[:limit]
        return price_diff_map, chance_list

    async def search_all_chances_cross_repeat(self, cnt=3, sleep=1, limit=None) -> (Dict[str, float], List[ChanceInfo]):
        # 循环多次 对比结果
        price_diff_map, chance_list = await self.search_all_chances(limit=limit)
        time.sleep(sleep)
        for _ in range(cnt - 1):
            new_price_diff_map, new_chance_list = await self.search_all_chances(limit=limit)
            new_chance_list = list(filter(lambda x: x.pair in [a.pair for a in chance_list], new_chance_list))
            for pair, diff in new_price_diff_map.items():
                if pair in price_diff_map:
                    price_diff_map[pair] = (price_diff_map[pair] + diff) / 2
            if len(new_chance_list) == 0:
                return price_diff_map, []
            else:
                chance_list = new_chance_list
            time.sleep(sleep)
        chance_list.sort(key=lambda x: (x.price_diff_profit_rate, x.funding_profit_rate), reverse=True)
        return price_diff_map, chance_list

    async def search_all_chances_spot_future(self, limit=None) -> (Dict[str, float], List[ChanceInfo]):
        """
            ‼️ 交易机会搜索
        :return:
        """
        price_diff_map, all_tick_info1, all_tick_info2 = self.get_all_market_price_diff_map()

        chance_list = []

        MAX_SPREAD = 0.0008
        MIN_PRICE_DIFF_PROFIT_RATE = 0.0008
        MIN_FUTURE_VOL_USD = 500_0000

        for future_info in all_tick_info2:
            pair_name = future_info["name"]
            diff = price_diff_map.get(pair_name)
            if not diff:
                continue
            try:
                if self.exchange2.exchange_code == ExchangeEnum.HYPERLIQUID:
                    future_vol_usd = float(future_info["dayNtlVlm"])
                    abs_spread = abs(
                        (float(future_info["impactPxs"][0]) - float(future_info["impactPxs"][1])) / float(
                            future_info["impactPxs"][1]))
                elif self.exchange2.exchange_code == ExchangeEnum.OKX:
                    future_vol_usd = float(future_info["volCcy24h"]) * float(future_info["midPx"])
                    abs_spread = abs(
                        (float(future_info["askPx"]) - float(future_info["bidPx"])) / float(future_info["bidPx"]))
                else:
                    raise Exception("交易2未支持")
                if abs_spread <= MAX_SPREAD \
                        and future_vol_usd >= MIN_FUTURE_VOL_USD \
                        and abs(diff) > 0.001:
                    future_info["info1"] = list(filter(lambda x: x["name"] == pair_name, all_tick_info1))[0]
                    future_info["diff"] = diff
                    future_info["funding2"] = self.exchange2.get_funding_rate(future_info["name"])
                    chance = ChanceInfo(future_info)
                    if chance.funding2 < 0.1:
                        # 费率不能为负
                        continue
                    if chance.funding_spot_profit_rate < 0.2 \
                            or chance.price_diff_profit_rate < MIN_PRICE_DIFF_PROFIT_RATE:
                        # 费率收益率和价差收益率不能太低
                        continue
                    chance_list.append(chance)
            except Exception as e:
                print(f"{pair_name} {future_info} 信息错误 error: {e}")
                continue
        chance_list.sort(key=lambda x: (x.price_diff_profit_rate, x.funding_profit_rate), reverse=True)
        if limit is not None:
            chance_list = chance_list[:limit]
        return price_diff_map, chance_list

    def search_abnormal_pair(self, limit=None) -> (Dict[str, float], List[ChanceInfo]):
        """
            ‼️ 异常交易对搜索
        :return:
        """
        price_diff_map, all_tick_info1, all_tick_info2 = self.get_all_market_price_diff_map()

        chance_list = []

        MIN_OI = 500_0000
        MIN_DAY_VOLUME = 500_0000
        MIN_FUNDING = 0.0057  # APY > 50%

        for info2 in all_tick_info2:
            pair_name = info2["name"]
            diff = price_diff_map.get(pair_name)
            if not diff:
                continue
            try:
                oi_usd = float(info2["openInterest"]) * float(info2["midPx"])
                if oi_usd >= MIN_OI \
                        and float(info2["dayNtlVlm"]) >= MIN_DAY_VOLUME \
                        and abs(float(info2["funding"])) * 100 >= MIN_FUNDING:
                    info2["info1"] = list(filter(lambda x: x["name"] == pair_name, all_tick_info1))[0]
                    info2["diff"] = diff
                    info2["funding1"] = self.exchange1.get_funding_rate(info2["name"])
                    chance = ChanceInfo(info2)
                    if chance.funding2 < 0 and chance.funding1 < 0:
                        # 费率不能同时为负
                        continue
                    if chance.funding2 > 1 and chance.funding1 > 1:
                        # 费率不能同时>100%
                        continue
                    if chance.funding_profit_rate < 0.1 or chance.price_diff_profit_rate < 0.0015:
                        # 费率收益率和价差收益率不能太低
                        continue
                    chance_list.append(chance)
            except Exception as e:
                print(f"{pair_name} {info2} 信息错误 error: {e}")
                continue
        chance_list.sort(key=lambda x: (x.price_diff_profit_rate, x.funding_profit_rate), reverse=True)
        if limit is not None:
            chance_list = chance_list[:limit]
        return price_diff_map, chance_list

    async def print_arbitrage_opportunities(self, chance_show_limit, use_repeat=False):
        """
        打印套利机会表格
        输入: 套利机会字典列表，包含以下字段:
            - pair: 交易对 (str)
            - diff: 价差 (float)
            - funding_diff_abs: 费率差绝对值 (float)
            - openInterestUSD: 市场持仓金额 (float)
            - maxLeverage: 最大杠杆率 (float)
        """
        if use_repeat:
            print("🔍 交易机会深度搜索中...")
            _, opportunities = await self.search_all_chances_cross_repeat(limit=chance_show_limit)
        else:
            _, opportunities = await self.search_all_chances(limit=chance_show_limit)
        # 准备表格数据
        table_data = []
        for opp in opportunities:
            # 格式化金额为百万/十亿单位 (保留2位小数)
            oi_usd = opp.openInterestUSD
            if oi_usd >= 1e9:
                oi_str = f"{oi_usd / 1e9:.2f}B"
            elif oi_usd >= 1e6:
                oi_str = f"{oi_usd / 1e6:.2f}M"
            elif oi_usd == 0:
                oi_str = "--"
            else:
                oi_str = f"{oi_usd:,.2f}"

            table_data.append([
                opp.pair,
                f"{opp.diff:.3%}",  # 价差保留6位小数
                f"{opp.funding_diff_abs:.2%}",  # 费率差保留6位小数
                f"{opp.funding1:.2%} {opp.funding2:.2%}",
                oi_str,  # 格式化持仓金额
                f"{opp.maxLeverage:.0f}x"  # 杠杆率添加"x"单位
            ])

        # 表格头定义
        headers = ["Pair", "Price Diff", "Funding Diff", "Funding1-2", "OI", "Leverage"]

        # 使用grid格式打印表格（支持中文对齐）
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        return opportunities

if __name__ == '__main__':
    # a = ChanceSearcher(OkxFuture(), HyperLiquidFuture())
    a = ChanceSearcher(BinanceFuture(), HyperLiquidFuture())
    # a = ChanceSearcher(OkxFuture(), BinanceFuture())
    # a = ChanceSearcher(BinanceFuture(), HyperLiquidFuture()).search_all_chances()
    a.print_arbitrage_opportunities()
    exit()
    price_diff_map, chance_list = a.search_all_chances()
    print(price_diff_map)
    print(len(chance_list))
    print(chance_list)
