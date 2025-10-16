# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : async_funding_spread_searcher
@Description : 基于异步交易所和资金费率缓存的套利机会搜索器
@Time        : 2025/10/7
"""
import asyncio
from typing import List, Dict, Tuple, Optional, Set
from dataclasses import dataclass, field
from loguru import logger
from tabulate import tabulate

from arbitrage_param import ArbitrageWhiteListParam
from cex_tools.async_exchange_adapter import AsyncExchangeAdapter
from cex_tools.hedge_spread_analyzer import HedgeSpreadAnalyzer, SpreadStatistics
from cex_tools.funding_rate_cache import FundingRateCache
from cex_tools.exchange_model.base_model import BaseModel, TradeDirection
from utils.parallelize_utils import batch_process_with_concurrency_limit


@dataclass
class FundingOpportunity:
    """费率套利机会"""
    pair: str                          # 交易对
    exchange1: str                     # 交易所1代码
    exchange2: str                     # 交易所2代码
    funding_rate1: float              # 交易所1费率（年化）
    funding_rate2: float              # 交易所2费率（年化）
    funding_diff_abs: float           # 费率差绝对值（年化）
    funding_profit_rate: float        # 费率收益率（年化/2）
    position_side1: TradeDirection    # 交易所1仓位方向
    position_side2: TradeDirection    # 交易所2仓位方向

    # 价格统计信息
    spread_stats: Optional[SpreadStatistics] = None  # 价差统计
    mean_spread_profit_rate: float = 0  # 价差均值收益率

    # 市场信息
    price1: float = 0                   # 交易所1价格
    price2: float = 0                   # 交易所2价格

    @property
    def is_funding_opportunity(self) -> bool:
        """是否为有效的费率机会"""
        return self.funding_profit_rate >= 0.08  # 年化8%以上

    @property
    def cur_price_diff_pct(self):
        """当前价格差百分比"""
        if self.price1 > 0 and self.price2 > 0:
            return (self.price1 - self.price2) / ((self.price1 + self.price2) / 2)
        return 0

    @property
    def combined_profit_potential(self) -> float:
        """综合盈利潜力 = 费率收益 + 价格回归潜力"""
        return self.funding_profit_rate + abs(self.spread_stats.mean_spread) if self.spread_stats else self.funding_profit_rate


@dataclass
class SearchConfig:
    """搜索配置"""
    min_funding_diff: float = 0.08      # 最小费率差（年化）
    max_opportunities: int = 50         # 最大返回机会数
    include_spread_analysis: bool = True  # 是否包含价差分析
    spread_analysis_interval: str = "1m"   # K线间隔
    spread_analysis_limit: int = 1000     # K线数量
    use_white_list: bool = False          # 是否使用白名单
    min_mean_spread_profit_rate: float = 0.0005  # 最小均值价差收益率


class AsyncFundingSpreadSearcher:
    """
    基于异步交易所和资金费率缓存的套利机会搜索器

    功能：
    1. 使用缓存的费率数据找到费率差值最大的交易对
    2. 使用HedgeSpreadAnalyzer计算价差均值作为参考
    3. 支持多个交易所组合的异步搜索
    4. 提供详细的统计分析和置信水平建议
    """

    def __init__(self,
                 exchange1: AsyncExchangeAdapter,
                 exchange2: AsyncExchangeAdapter,
                 search_config: Optional[SearchConfig] = None):
        """
        初始化搜索器

        Args:
            exchange1: 异步交易所1
            exchange2: 异步交易所2
            search_config: 搜索配置
        """
        self.exchange1 = exchange1
        self.exchange2 = exchange2
        self.config = search_config or SearchConfig()

        # 初始化组件
        self.funding_cache = FundingRateCache()
        self.spread_analyzer = HedgeSpreadAnalyzer(exchange1, exchange2)

        logger.info(f"费率价差搜索: {exchange1.exchange_code} vs {exchange2.exchange_code}")

    async def get_common_pairs(self) -> Set[str]:
        """
        获取两个交易所都支持的交易对集合

        Returns:
            交易对名称集合（不带USDT后缀）
        """
        try:
            # 并行获取两个交易所的所有交易对
            all_pairs1, all_pairs2 = await asyncio.gather(
                self.exchange1.get_all_tick_price(),
                self.exchange2.get_all_tick_price()
            )

            # 提取交易对名称（移除USDT后缀）
            pairs1 = {self._normalize_pair_name(info["name"]) for info in all_pairs1}
            pairs2 = {self._normalize_pair_name(info["name"]) for info in all_pairs2}

            # 返回交集
            common_pairs = pairs1 & pairs2
            logger.info(f"共同交易对: {len(common_pairs)} 个 "
                        f"({self.exchange1.exchange_code}: {len(pairs1)}, "
                        f"{self.exchange2.exchange_code}: {len(pairs2)})")

            return common_pairs

        except Exception as e:
            logger.error(f"获取共同交易对失败: {e}")
            return set()

    def _normalize_pair_name(self, pair_name: str) -> str:
        """标准化交易对名称（移除USDT/USDC后缀）"""
        return pair_name.upper().replace("USDT", "").replace("USDC", "")

    async def get_funding_rate_diffs(self, common_pairs: Set[str]) -> Dict[str, float]:
        """
        获取所有共同交易对的费率差值

        Args:
            common_pairs: 共同交易对集合

        Returns:
            交易对费率差值字典 {pair: abs(funding1 - funding2)}
        """
        funding_diffs = {}
        exchange1_code = self.exchange1.exchange_code.lower()
        exchange2_code = self.exchange2.exchange_code.lower()

        for pair in common_pairs:
            try:
                # 从缓存获取费率数据（年化）
                rate1 = self.funding_cache.get_funding_rate(exchange1_code, pair)
                rate2 = self.funding_cache.get_funding_rate(exchange2_code, pair)

                if rate1 is not None and rate2 is not None:
                    # 转换为年化（缓存中是单次费率，8小时周期）
                    annual_rate1 = rate1 * 3 * 365  # 8小时 * 3 * 365天
                    annual_rate2 = rate2 * 3 * 365

                    diff_abs = abs(annual_rate1 - annual_rate2)
                    funding_diffs[pair] = diff_abs

            except Exception as e:
                logger.warning(f"计算 {pair} 费率差失败: {e}")
                continue

        return funding_diffs

    async def create_opportunity(self, pair: str, funding_diff: float) -> Optional[FundingOpportunity]:
        """
        创建单个套利机会对象

        Args:
            pair: 交易对名称
            funding_diff: 费率差值

        Returns:
            套利机会对象，如果创建失败返回None
        """
        try:
            exchange1_code = self.exchange1.exchange_code.lower()
            exchange2_code = self.exchange2.exchange_code.lower()

            # 获取费率数据
            rate1 = self.funding_cache.get_funding_rate(exchange1_code, pair)
            rate2 = self.funding_cache.get_funding_rate(exchange2_code, pair)

            if rate1 is None or rate2 is None:
                return None

            # 转换为年化
            annual_rate1 = rate1 * 3 * 365
            annual_rate2 = rate2 * 3 * 365

            # 确定仓位方向
            if annual_rate1 > annual_rate2:
                # 交易所1费率高，做空；交易所2费率低，做多
                position_side1 = TradeDirection.short
                position_side2 = TradeDirection.long
            else:
                # 交易所2费率高，做空；交易所1费率低，做多
                position_side1 = TradeDirection.long
                position_side2 = TradeDirection.short

            # 获取实时价格信息
            try:
                symbol1 = self._get_symbol_for_exchange(pair, self.exchange1.exchange_code)
                symbol2 = self._get_symbol_for_exchange(pair, self.exchange2.exchange_code)

                price1, price2 = await asyncio.gather(
                    self.exchange1.get_tick_price(symbol1),
                    self.exchange2.get_tick_price(symbol2)
                )

            except Exception as e:
                logger.warning(f"获取 {pair} 价格信息失败: {e}")
                price1 = price2 = 0

            # 创建机会对象
            opportunity = FundingOpportunity(
                pair=pair,
                exchange1=self.exchange1.exchange_code,
                exchange2=self.exchange2.exchange_code,
                funding_rate1=annual_rate1,
                funding_rate2=annual_rate2,
                funding_diff_abs=funding_diff,
                funding_profit_rate=funding_diff / 2,
                position_side1=position_side1,
                position_side2=position_side2,
                price1=price1,
                price2=price2
            )

            # 进行价差分析（如果启用）
            if self.config.include_spread_analysis and price1 > 0 and price2 > 0:
                try:
                    symbol = self._get_symbol_for_exchange(pair, self.exchange1.exchange_code)
                    spread_stats = await self.spread_analyzer.analyze_spread(
                        symbol,
                        self.config.spread_analysis_interval,
                        self.config.spread_analysis_limit
                    )

                    opportunity.spread_stats = spread_stats
                    if opportunity.funding_rate1 > opportunity.funding_rate2:
                        opportunity.mean_spread_profit_rate = spread_stats.mean_spread
                    else:
                        opportunity.mean_spread_profit_rate = -spread_stats.mean_spread
                    logger.debug(f"{pair} 费率收益率={abs(opportunity.funding_rate1 - opportunity.funding_rate2) / 2:.2%} 均值={spread_stats.mean_spread:.4%}({opportunity.mean_spread_profit_rate:.4%})")

                except Exception as e:
                    logger.warning(f"{pair} 价差分析失败: {e}")

            return opportunity

        except Exception as e:
            logger.error(f"创建 {pair} 机会对象失败: {e}")
            return None

    def _get_symbol_for_exchange(self, pair: str, exchange_code: str) -> str:
        """
        根据交易所代码获取正确的交易对符号

        Args:
            pair: 标准化交易对名称（无后缀）
            exchange_code: 交易所代码

        Returns:
            交易所特定的交易对符号
        """
        # Binance需要USDT后缀
        if exchange_code.lower() in ['binance', 'aster', 'bybit', 'okx']:
            return pair + "USDT"
        # HyperLiquid和Lighter不需要后缀
        else:
            return pair

    async def search_opportunities(self) -> List[FundingOpportunity]:
        """
        搜索费率套利机会

        Returns:
            排序后的套利机会列表
        """
        # 1. 获取共同交易对
        common_pairs = await self.get_common_pairs()

        if self.config.use_white_list:
            common_pairs = {pair for pair in common_pairs if pair in ArbitrageWhiteListParam.SYMBOL_LIST}

        if not common_pairs:
            logger.warning("未找到共同交易对")
            return []

        # 2. 计算费率差值
        funding_diffs = await self.get_funding_rate_diffs(common_pairs)
        if not funding_diffs:
            logger.warning("未找到有效的费率数据")
            return []

        # 3. 按费率差排序，筛选符合条件的机会
        sorted_pairs = sorted(
            funding_diffs.items(),
            key=lambda x: x[1],
            reverse=True
        )

        # 筛选满足最小费率差要求的交易对
        if self.config.use_white_list:
            qualified_pairs = sorted_pairs
        else:
            qualified_pairs = [
                (pair, diff) for pair, diff in sorted_pairs
                if diff >= self.config.min_funding_diff
            ]

            if not qualified_pairs:
                logger.warning(f"没有交易对满足最小费率差要求 {self.config.min_funding_diff:.2%}")
                return []

            if not qualified_pairs:
                logger.warning(f"没有交易对满足最小均值价差收益率要求 {self.config.min_mean_spread_profit_rate:.2%}")
                return []


        # 4. 创建机会对象（限制数量）
        if not self.config.use_white_list:
            max_opportunities = min(len(qualified_pairs), self.config.max_opportunities)
            qualified_pairs = qualified_pairs[:max_opportunities]

        # 定义进度回调函数
        def progress_callback(completed, total, _):
            if completed % 10 == 0 or completed == total:
                logger.info(f"已完成 {completed}/{total} 个交易对数据处理")

        # 使用批量处理工具函数，限制并发数避免API限制
        all_opportunities = await batch_process_with_concurrency_limit(
            items=qualified_pairs,
            process_func=self.create_opportunity,
            max_concurrency=3,  # 限制并发数，避免超过交易所API限制
            progress_callback=progress_callback,
            delay=0.3
        )

        # 过滤掉None值（处理失败的项目）
        opportunities = [opp for opp in all_opportunities if opp is not None]

        # 5. 按综合潜力排序
        opportunities.sort(
            key=lambda x: (x.funding_profit_rate, x.combined_profit_potential),
            reverse=True
        )
        # 筛选满足最小均值价差收益率要求的交易对
        if not self.config.use_white_list:
            opportunities = [
                opp for opp in opportunities
                if opp.mean_spread_profit_rate >= self.config.min_mean_spread_profit_rate
            ]
        logger.info(f"{self.exchange1.exchange_code}-{self.exchange2.exchange_code}找到 {len(opportunities)} 个有效机会")
        return opportunities

    async def print_opportunities_table(self, opportunities: List[FundingOpportunity]) -> None:
        """
        打印机会表格

        Args:
            opportunities: 机会列表
        """
        if not opportunities:
            print("未找到有效的套利机会")
            return

        taker_fee = (self.exchange1.taker_fee_rate + self.exchange2.taker_fee_rate) * 2
        # 准备表格数据
        table_data = []
        for opp in opportunities:

            # 价差统计信息
            if opp.spread_stats:
                spread_info = f"{opp.spread_stats.mean_spread:.3%}±{opp.spread_stats.std_spread:.3%}"
            else:
                spread_info = "未分析"
            if opp.funding_profit_rate > 0:
                chance_profit = taker_fee / (opp.funding_profit_rate / 365 / 24)
            else:
                chance_profit = 0
            table_data.append([
                opp.pair,
                f"{opp.funding_profit_rate:.2%}",  # 费率收益率
                f"{opp.funding_rate1:.2%}/{opp.funding_rate2:.2%}",  # 各交易所费率
                f"{opp.position_side1}/{opp.position_side2}",  # 仓位方向
                f"{opp.price1:.2f}/{opp.price2:.2f}",  # 价格
                spread_info,  # 价差统计
                f"{opp.cur_price_diff_pct:.3%}",
                f"{chance_profit:.1f}h" if chance_profit > 0 else "N/A"  # 成本覆盖时间
            ])

        # 表格头
        headers = [
            "Pair",
            "Funding Profit",
            "Rates",
            "Direction",
            "Prices",
            "Spread (Mean±Std)",
            "Cur Spread",
            f"Cost Cover Hours({taker_fee:.3%})"
        ]
        white_list_note = " (白名单)" if self.config.use_white_list else ""
        print(f"\n{self.exchange1.exchange_code} vs {self.exchange2.exchange_code} 费率套利机会{white_list_note}")
        print("="*120)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
