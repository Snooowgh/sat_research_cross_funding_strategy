# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : realtime_hedge_engine
@Description : 基于WebSocket实时订单簿的对冲交易引擎
@Time        : 2025/10/2 14:15
"""
import traceback

from cex_tools.exchange_model.multi_exchange_info_model import MultiExchangeCombinedInfoModel
from cex_tools.exchange_model.position_model import BinancePositionDetail
from cex_tools.hedge_spread_analyzer import HedgeSpreadAnalyzer
import asyncio
import random
import time
from dataclasses import dataclass
from typing import Optional
from loguru import logger
from rich.prompt import FloatPrompt, Confirm

from cex_tools.exchange_ws.orderbook_stream import OrderBookStream, OrderBookData
from cex_tools.cex_enum import TradeSide
from config.env_config import env_config
from logic.strategy_math import calculate_zscore, infer_optimal_spread_by_zscore
from utils.decorators import async_timed_cache
from utils.math_utils import align_with_decimal
from utils.notify_tools import async_notify_telegram, CHANNEL_TYPE


@dataclass
class RiskConfig:
    """风控配置"""
    max_orderbook_age_sec: float = 1.0  # 订单簿最大过期时间（秒）
    max_spread_pct: float = 0.0015  # 最大买卖价差（0.15%）
    min_liquidity_usd: float = 1000  # 最小流动性（美元）
    min_profit_rate: float = 0.0005  # 开仓时最小价差收益率（0.05%）
    reduce_pos_min_profit_rate: float = -0.001  # 平仓时最小价差收益率（0.1%）
    liquidity_depth_levels: int = 10  # 流动性检查深度层级
    user_min_profit_rate: float = 0.001  # 用户设置的最小收益率底线（不可低于此值）
    enable_dynamic_profit_rate: bool = True  # 是否启用动态调整收益率
    profit_rate_adjust_step: float = 0.00005  # 收益率调整步长（0.005%）
    profit_rate_adjust_threshold: int = 3  # 连续多少笔交易触发调整
    no_trade_reduce_timeout_sec: float = 0  # 无成交多久后降低收益率（秒，0表示禁用）
    no_trade_reduce_step_multiplier: float = 1.5  # 无成交降低收益率时的步长倍数（相对于正常步长）
    auto_pos_balance_usd_value_limit: float = 1000.0 # 自动平衡仓位金额的最大USD值


@dataclass
class TradeConfig:
    """交易配置"""
    pair1: str  # 交易对1（如 "BTCUSDT"）
    pair2: str  # 交易对2（如 "BTCUSDT"）
    side1: str  # 交易方向1（"BUY" 或 "SELL"）
    side2: str  # 交易方向2（"BUY" 或 "SELL"）
    amount_min: float = 0  # 单笔交易最小数量
    amount_max: float = 0  # 单笔交易最大数量
    amount_step: float = 1.0  # 数量步长
    total_amount: float = 0.0  # 总交易数量
    trade_interval_sec: float = 0.1  # 交易间隔时间（秒）
    use_dynamic_amount: bool = True  # 是否根据订单簿动态调整下单数量
    max_first_level_ratio: float = 1  # 最大吃掉第一档流动性的比例（0.5 = 50%）
    no_trade_timeout_sec: float = 0  # 无交易自动关闭超时时间（秒，0表示禁用）
    min_order_value_usd: float = 20.0  # 单笔最小订单金额（美元）
    max_order_value_usd: float = 500.0  # 单笔最大订单金额（美元）
    daemon_mode: bool = False  # 是否持续运行 (no_trade_timeout_sec>0 如果有交易 则不再超时)
    zscore_threshold: float = env_config.get_float("RH_DEFAULT_ZSCORE_THRESHOLD", 2.0)


@dataclass
class TradeSignal:
    """交易信号"""
    pair1: str
    pair2: str
    price1: float
    price2: float
    side1: float
    side2: float
    spread: float  # 价差
    optimal_spread: float  # 价差
    spread_rate: float  # 价差收益率
    ma_spread: float
    std_spread: float
    timestamp: float
    funding_rate_diff_apy: float
    z_score: float = None
    z_score_after_fee: float = None
    zscore_threshold: float = 2
    pass_risk_check: bool = False
    risk_message: str = ""
    _is_add_position: bool = None
    signal_generate_start_time: float = None

    def is_zscore_triggered(self):
        optimal_side1 = None
        if self.z_score_after_fee <= -self.zscore_threshold:
            # Z-Score触发，进行统计套利
            # 价差被低估，做多价差：主交易所maker做多，从交易所taker做空
            optimal_side1 = TradeSide.BUY  # 主交易所做多
        elif self.z_score_after_fee >= self.zscore_threshold:
            # 价差被高估，做空价差：主交易所maker做空，从交易所taker做多
            optimal_side1 = TradeSide.SELL  # 主交易所做空
        return self.side1 == optimal_side1

    def is_add_position(self):
        return self._is_add_position

    def delay_ms(self):
        return (time.time()-self.signal_generate_start_time) * 1000

    def __str__(self):
        return (f"TradeSignal(pair1={self.pair1}, side1={self.side1}, price1={self.price1}, price2={self.price2}, "
                f"spread_rate={self.spread_rate:.4%}, ma_spread={self.ma_spread:.4%}, std_spread={self.std_spread:.4%}, "
                f"optimal_spread={self.optimal_spread:.4%}, z_score={self.z_score:.2f}, "
                f"z_score_after_fee={self.z_score_after_fee:.2f},funding_rate_diff_apy={self.funding_rate_diff_apy:.2%}, "
                f"risk_check_passed={self.pass_risk_check}, risk_message='{self.risk_message}', "
                f"_is_add_position={self._is_add_position}, "
                f"delay={self.delay_ms():.2f}ms)")


class RealtimeHedgeEngine:
    """实时对冲交易引擎"""

    def __init__(
            self,
            stream1: OrderBookStream,
            stream2: OrderBookStream,
            exchange1,  # 交易所1对象（用于下单）
            exchange2,  # 交易所2对象（用于下单）
            trade_config: TradeConfig,
            exchange_combined_info_cache=None,
            risk_config: RiskConfig = None
    ):
        self.stream1 = stream1
        self.stream2 = stream2
        self.exchange1 = exchange1
        self.exchange2 = exchange2
        self.trade_config = trade_config
        self.symbol = trade_config.pair1.replace("USDT", "")
        self.exchange_code_list = [exchange1.exchange_code, exchange2.exchange_code]
        self.exchange_pair = f"{exchange1.exchange_code}-{exchange2.exchange_code}"
        self.taker_fee_rate = exchange1.taker_fee_rate + exchange2.taker_fee_rate
        self.risk_config = risk_config or RiskConfig()
        if self.trade_config.daemon_mode is True and exchange_combined_info_cache is None:
            raise Exception("持续运行模式必须提供 exchange_combined_info_cache 用于风控检查")
        self.exchange_combined_info_cache = exchange_combined_info_cache

        self.spread_analyzer = HedgeSpreadAnalyzer(exchange1, exchange2)
        self._running = False
        self._trade_count = 0
        self._cum_volume = 0.0
        self._cum_profit = 0.0
        self._remaining_amount = trade_config.total_amount

        # 最新订单簿缓存
        self._latest_orderbook1: Optional[OrderBookData] = None
        self._latest_orderbook2: Optional[OrderBookData] = None

        # 最新仓位缓存
        self._position1: Optional[BinancePositionDetail] = None
        self._position2: Optional[BinancePositionDetail] = None

        # 订单簿更新锁
        self._lock = asyncio.Lock()

        # 无交易超时跟踪
        self._last_trade_time = time.time()  # 上次交易时间
        self._timeout_enabled = trade_config.no_trade_timeout_sec > 0

        # 动态收益率调整机制
        self._recent_profit_rates = []  # 最近N笔交易的收益率
        self._last_adjustment_trade_count = 0  # 上次调整时的交易笔数
        # 记录用户期望的安全初始值（无成交降低时可以降到此值，而不是破坏用户设置）
        self._initial_min_profit_rate = risk_config.user_min_profit_rate
        # 无交易最多下调多少次最小收益率
        self._reduce_min_profit_rate_cnt = 0

    async def update_exchange_info_helper(self):
        raise Exception("未传参！!")

    @async_timed_cache(timeout=3600)
    async def _get_pair_market_info(self):
        """获取交易对的市场信息, 1h更新一次"""
        # 使用异步方法获取市场信息
        try:
            spread_stats = await self.spread_analyzer.analyze_spread(symbol=self.symbol,
                                                                     interval="1m", limit=1000)
        except Exception as e:
            logger.warning(f"获取价差统计失败: {e}")
            spread_stats = None

        # 使用完整的交易对符号，而不是简化的symbol
        funding_fetch_start = time.time()
        funding_rate1 = await self.exchange1.get_funding_rate(self.trade_config.pair1)
        funding_rate2 = await self.exchange2.get_funding_rate(self.trade_config.pair2)
        if funding_fetch_start - time.time() > 1:
            logger.warning(f"❌ 缓存失效: 获取资金费率耗时过长: {funding_fetch_start - time.time()}s")
        return spread_stats, funding_rate1, funding_rate2

    def _get_risk_data(self) -> MultiExchangeCombinedInfoModel:
        if time.time() - self.exchange_combined_info_cache.get("update_time") > 31 * 60:
            logger.warning("风控缓存数据未更新，可能存在风险")
        return self.exchange_combined_info_cache.get("risk_data")

    def _get_max_open_notional_value(self):
        return self._get_risk_data().max_open_notional_value(self.exchange_code_list)

    def _can_add_position(self):
        """是否可以加仓"""
        combined_info: MultiExchangeCombinedInfoModel = self._get_risk_data()
        for info in combined_info.exchange_infos:
            if info.exchange_code in [self.exchange1.exchange_code, self.exchange2.exchange_code]:
                if not info.can_add_position:
                    return False
        return True

    def _on_orderbook1_update(self, orderbook: OrderBookData):
        """交易所1订单簿更新回调"""
        self._latest_orderbook1 = orderbook

    def _on_orderbook2_update(self, orderbook: OrderBookData):
        """交易所2订单簿更新回调"""
        self._latest_orderbook2 = orderbook

    def _check_orderbook_freshness(self) -> tuple[bool, str]:
        """
        检查订单簿数据新鲜度
        :return: (通过, 消息)
        """
        if not self._latest_orderbook1 or not self._latest_orderbook2:
            return False, "订单簿数据不完整"

        # 检查过期时间
        if self._latest_orderbook1.is_stale(self.risk_config.max_orderbook_age_sec):
            age = time.time() - self._latest_orderbook1.timestamp
            return False, f"{self.exchange1.exchange_code} {self._latest_orderbook1.pair}订单簿过期 ({age:.2f}s)"

        if self._latest_orderbook2.is_stale(self.risk_config.max_orderbook_age_sec):
            age = time.time() - self._latest_orderbook2.timestamp
            return False, f"{self.exchange2.exchange_code} {self._latest_orderbook1.pair}订单簿过期 ({age:.2f}s)"

        return True, "订单簿数据新鲜"

    def _check_spread(self) -> tuple[bool, str]:
        """
        检查买卖价差
        :return: (通过, 消息)
        """
        spread1 = self._latest_orderbook1.spread_pct
        spread2 = self._latest_orderbook2.spread_pct

        if spread1 and spread1 > self.risk_config.max_spread_pct:
            return False, f"{self.exchange1.exchange_code} {self._latest_orderbook1.pair}盘口价差过大 ({spread1:.4%} > {self.risk_config.max_spread_pct:.4%})"

        if spread2 and spread2 > self.risk_config.max_spread_pct:
            return False, f"{self.exchange2.exchange_code} {self._latest_orderbook1.pair}盘口价差过大 ({spread2:.4%} > {self.risk_config.max_spread_pct:.4%})"

        return True, "价差正常"

    def _check_liquidity(self, signal: TradeSignal) -> tuple[bool, str]:
        """
        检查流动性
        :return: (通过, 消息)
        """
        # 根据交易方向检查对应方向的深度
        side1_check = "ask" if signal.side1 == TradeSide.BUY else "bid"
        side2_check = "ask" if signal.side2 == TradeSide.BUY else "bid"

        liquidity1 = self._latest_orderbook1.get_liquidity_usd(
            side1_check,
            self.risk_config.liquidity_depth_levels
        )
        liquidity2 = self._latest_orderbook2.get_liquidity_usd(
            side2_check,
            self.risk_config.liquidity_depth_levels
        )

        if liquidity1 < self.risk_config.min_liquidity_usd:
            return False, f"{self.exchange1.exchange_code} {self._latest_orderbook1.pair}流动性不足 (${liquidity1:.2f})"

        if liquidity2 < self.risk_config.min_liquidity_usd:
            return False, f"{self.exchange2.exchange_code} {self._latest_orderbook1.pair}流动性不足 (${liquidity2:.2f})"

        return True, f"流动性充足 (${liquidity1:.0f}/${liquidity2:.0f})"

    def get_current_spread(self):
        if not self._latest_orderbook1 or not self._latest_orderbook2:
            return None
        current_spread = ((self._latest_orderbook1.mid_price - self._latest_orderbook2.mid_price) /
                          self._latest_orderbook2.mid_price)
        return current_spread

    async def _calculate_spread_by_daemon(self) -> Optional[TradeSignal]:
        """
        Daemon模式下的Z-Score价差策略

        核心逻辑：
           Z-Score统计套利：当价差偏离均值过大时进行反向操作

        :return: 交易信号
        """
        current_spread = self.get_current_spread()
        if current_spread is None:
            return None
        # 仓位数据
        pos1, pos2 = self._position1, self._position2
        spread_stats, funding_rate1, funding_rate2 = await self._get_pair_market_info()
        # 计算Z-Score
        z_score = calculate_zscore(current_spread, spread_stats, funding_rate1, funding_rate2, side1=None,
                                   fee_rate=None)
        optimal_spread = infer_optimal_spread_by_zscore(self.trade_config.zscore_threshold, spread_stats,
                                                        funding_rate1, funding_rate2)
        # Z-Score分析
        zscore_threshold = self.trade_config.zscore_threshold
        if self._can_add_position():
            if z_score <= 0:
                # Z-Score触发，进行统计套利
                # 价差被低估，做多价差：主交易所maker做多，从交易所taker做空
                optimal_side1 = TradeSide.BUY  # 主交易所做多
                optimal_side2 = TradeSide.SELL  # 从交易所做空
            else:
                # 价差被高估，做空价差：主交易所maker做空，从交易所taker做多
                optimal_side1 = TradeSide.SELL  # 主交易所做空
                optimal_side2 = TradeSide.BUY  # 从交易所做多
        else:
            # 因为风控无法加仓时，只能减仓
            if pos1 is None or pos2 is None:
                return None  # 无仓位且无法加仓，跳过
            optimal_side1 = TradeSide.BUY if pos1.position_side == TradeSide.SELL else TradeSide.SELL
            optimal_side2 = TradeSide.BUY if pos2.position_side == TradeSide.SELL else TradeSide.SELL

        # 计算Z-Score
        z_score_after_fee = calculate_zscore(current_spread, spread_stats, funding_rate1, funding_rate2,
                                             side1=optimal_side1, fee_rate=self.taker_fee_rate)
        funding_rate_diff_apy = funding_rate1 - funding_rate2
        # 根据交易方向获取成交价格
        if optimal_side1 == TradeSide.BUY:
            price1 = self._latest_orderbook1.best_ask  # 买入取卖一价
        else:
            price1 = self._latest_orderbook1.best_bid  # 卖出取买一价

        if optimal_side2 == TradeSide.BUY:
            price2 = self._latest_orderbook2.best_ask
        else:
            price2 = self._latest_orderbook2.best_bid

        if not price1 or not price2:
            return None

        # 计算价差
        spread = price1 - price2
        # 根据交易方向调整价差符号
        if optimal_side1 == TradeSide.BUY:
            # 做多exchange1，做空exchange2，需要price1 < price2才有收益
            spread_rate = -spread / price1
        else:
            # 做空exchange1，做多exchange2，需要price1 > price2才有收益
            spread_rate = spread / price1

        signal = TradeSignal(
            pair1=self.trade_config.pair1,
            pair2=self.trade_config.pair2,
            price1=price1,
            price2=price2,
            side1=optimal_side1,
            side2=optimal_side2,
            spread=spread,
            ma_spread=spread_stats.mean_spread,
            std_spread=spread_stats.std_spread,
            spread_rate=spread_rate,
            timestamp=time.time(),
            z_score=z_score,
            z_score_after_fee=z_score_after_fee,
            optimal_spread=optimal_spread,
            funding_rate_diff_apy=funding_rate_diff_apy,
            zscore_threshold=zscore_threshold
        )
        # 根据仓位情况判断是什么操作
        if pos1 is None or pos2 is None:
            signal._is_add_position = True
        elif optimal_side1 == pos1.position_side and optimal_side2 == pos2.position_side:
            signal._is_add_position = True
        else:
            signal._is_add_position = False

        return signal

    async def _calculate_spread(self) -> Optional[TradeSignal]:
        """
        计算价差和交易信号(命令行模式下 只有一个交易方向)
        :return: 交易信号
        """
        current_spread = self.get_current_spread()
        if current_spread is None:
            return None

        # 根据交易方向获取价格
        if self.trade_config.side1 == TradeSide.BUY:
            price1 = self._latest_orderbook1.best_ask  # 买入取卖一价
        else:
            price1 = self._latest_orderbook1.best_bid  # 卖出取买一价

        if self.trade_config.side2 == TradeSide.BUY:
            price2 = self._latest_orderbook2.best_ask
        else:
            price2 = self._latest_orderbook2.best_bid

        if not price1 or not price2:
            return None

        # 计算价差
        spread = price1 - price2
        # 根据交易方向调整价差符号
        if self.trade_config.side1 == TradeSide.BUY:
            # 做多exchange1，做空exchange2，需要price1 < price2才有收益
            spread_rate = -spread / price1
        else:
            # 做空exchange1，做多exchange2，需要price1 > price2才有收益
            spread_rate = spread / price1
        spread_stats, funding_rate1, funding_rate2 = await self._get_pair_market_info()
        # 计算Z-Score
        z_score = calculate_zscore(current_spread, spread_stats, funding_rate1, funding_rate2, side1=None,
                                   fee_rate=None)
        optimal_spread = infer_optimal_spread_by_zscore(self.trade_config.zscore_threshold, spread_stats,
                                                        funding_rate1, funding_rate2)
        funding_rate_diff_apy = funding_rate1 - funding_rate2
        signal = TradeSignal(
            pair1=self.trade_config.pair1,
            pair2=self.trade_config.pair2,
            price1=price1,
            price2=price2,
            side1=self.trade_config.side1,
            side2=self.trade_config.side2,
            spread=spread,
            spread_rate=spread_rate,
            ma_spread=spread_stats.mean_spread,
            std_spread=spread_stats.std_spread,
            timestamp=time.time(),
            z_score=z_score,
            optimal_spread=optimal_spread,
            funding_rate_diff_apy=funding_rate_diff_apy
        )

        return signal

    def _generate_random_amount(self, min_amount: float, max_amount: float, step: float) -> float:
        """
        根据最小数量、最大数量和数量单位变化，随机生成一个数量。

        :param min_amount: 最小数量（包含）
        :param max_amount: 最大数量（包含）
        :param step: 数量单位变化（步长）
        :return: 一个在 [min_amount, max_amount] 范围内且为 step 整数倍的随机数
        """
        # 计算可能取值的数量
        number_of_possible_values = int((max_amount - min_amount) / step) + 1
        # 生成一个随机索引，表示第几个倍数
        random_index = random.randint(0, number_of_possible_values - 1)
        # 计算并返回最终的随机数量
        random_amount = min_amount + random_index * step
        return random_amount

    async def _calculate_trade_amount(self, signal: TradeSignal) -> float:
        """
        计算本次交易数量，综合考虑：
        1. 随机区间 [amount_min, amount_max]
        2. 订单簿第一档流动性限制
        3. 剩余待执行数量
        4. 最小订单金额限制（除非剩余仓位不足50美金）
        5. 自动仓位管理（daemon模式）

        :param signal: 交易信号（用于获取价格）
        :return: 本次交易数量
        """
        # 基础随机数量或仓位管理数量
        if self.trade_config.daemon_mode:
            # 在daemon模式下使用自动仓位管理
            base_amount = None
        else:
            base_amount = self._generate_random_amount(
                self.trade_config.amount_min,
                self.trade_config.amount_max,
                self.trade_config.amount_step
            )

        # 如果启用动态调整，根据订单簿第一档流动性限制
        if self.trade_config.use_dynamic_amount and self._latest_orderbook1 and self._latest_orderbook2:
            # 根据交易方向获取第一档流动性
            if signal.side1 == TradeSide.BUY:
                # 买入时，查看卖一档的数量
                first_level_qty1 = float(self._latest_orderbook1.asks[0][1]) if self._latest_orderbook1.asks else 0
            else:
                # 卖出时，查看买一档的数量
                first_level_qty1 = float(self._latest_orderbook1.bids[0][1]) if self._latest_orderbook1.bids else 0

            if self.trade_config.side2 == TradeSide.BUY:
                first_level_qty2 = float(self._latest_orderbook2.asks[0][1]) if self._latest_orderbook2.asks else 0
            else:
                first_level_qty2 = float(self._latest_orderbook2.bids[0][1]) if self._latest_orderbook2.bids else 0

            # 取两个交易所第一档流动性的最小值
            min_first_level_qty = min(first_level_qty1, first_level_qty2)

            # 最大允许吃掉第一档流动性的比例
            max_allowed_qty = min_first_level_qty * self.trade_config.max_first_level_ratio

            # 限制下单数量不超过第一档流动性限制
            if base_amount is None or base_amount > max_allowed_qty:
                base_amount = max_allowed_qty
                logger.debug(f"订单簿流动性限制: {base_amount:.4f} -> {max_allowed_qty:.4f} "
                             f"(第一档: {first_level_qty1:.4f}/{first_level_qty2:.4f})")

        # 检查最小订单金额限制（20美金）
        # 使用两个交易所价格的平均值来计算，确保两边的订单价值都接近限制
        avg_price = (signal.price1 + signal.price2) / 2

        # 如果计算出的订单金额小于20美金，但剩余仓位大于20美金，则调整到最小金额
        while base_amount * avg_price < self.trade_config.min_order_value_usd:
            base_amount *= 2
            logger.debug(
                f"订单金额低, 翻倍处理: {base_amount:.4f} (${base_amount * avg_price:.2f})"
            )
        # 如果翻倍后超过最大订单金额，则限制在最大订单金额
        while base_amount * avg_price > self.trade_config.max_order_value_usd\
                or base_amount * avg_price > self._get_max_open_notional_value():
            base_amount = align_with_decimal(base_amount / 2, base_amount)
            logger.debug(
                f"订单金额高, 减半处理: {base_amount:.4f} (${base_amount * avg_price:.2f})"
            )

        if self.trade_config.daemon_mode:
            final_amount = base_amount
        else:
            # 确保不超过剩余数量
            final_amount = min(base_amount, self._remaining_amount)
        final_amount = min(await self.exchange1.convert_size(self.trade_config.pair1, final_amount),
                           await self.exchange2.convert_size(self.trade_config.pair2, final_amount))
        return final_amount

    async def _risk_check(self, signal: TradeSignal) -> tuple[bool, str]:
        """
        综合风控检查 - 增强版开仓条件判断

        检查项：
        1. 订单簿数据新鲜度
        2. 盘口价差合理性
        3. 流动性充足性
        4. 价差收益率达标
        5. 资金费率套利机会验证（仅daemon模式）
        6. 市场异常状态检测

        :param signal: 交易信号
        :return: (通过, 消息)
        """
        if self.trade_config.daemon_mode and not self._can_add_position() and signal.is_add_position():
            return False, f"当前无法加仓，风控限制({signal.spread_rate:.3%}|{signal.z_score:.2f})"

        # 1. 检查订单簿新鲜度
        passed, msg = self._check_orderbook_freshness()
        if not passed:
            return False, msg

        # 2. 检查盘口价差
        passed, msg = self._check_spread()
        if not passed:
            return False, msg

        # 3. 检查流动性
        passed, msg = self._check_liquidity(signal)
        if not passed:
            return False, msg

        # 4. 检查价差收益率
        if signal.is_add_position():
            if signal.spread_rate < self.risk_config.min_profit_rate:
                return False, f"保底价差收益率不足 ({signal.spread_rate:.4%} < {self.risk_config.min_profit_rate:.4%})"
        else:
            if signal.spread_rate < self.risk_config.reduce_pos_min_profit_rate:
                return False, f"平仓保底价差收益率不足 ({signal.spread_rate:.4%} < {self.risk_config.min_profit_rate:.4%})"

        # 5. 增强检查：市场异常状态检测
        if self.trade_config.daemon_mode:
            # 在持续模式下，额外检查资金费率套利是否仍然有效
            try:
                spread_stats, funding_rate1, funding_rate2 = await self._get_pair_market_info()
                ma_spread = spread_stats.mean_spread if spread_stats else 0.0
                std_spread = spread_stats.std_spread if spread_stats else 0.0

                # 检查当前价差是否偏离历史均值过大（可能市场异常）
                if ma_spread != 0:
                    current_mid_diff = (self._latest_orderbook1.mid_price - self._latest_orderbook2.mid_price) / self._latest_orderbook2.mid_price
                    deviation_ratio = abs(current_mid_diff - ma_spread) / abs(ma_spread) if ma_spread != 0 else 0
                    if deviation_ratio > 3.0:  # 价差偏离历史均值超过3倍
                        return False, f"价差异常:{current_mid_diff:.4%} (偏离历史均值{deviation_ratio:.1f}倍)，市场可能不稳定"

            except Exception as e:
                logger.warning(f"市场状态检查异常: {e}")

        # 6. 动态收益率检查：在daemon模式或Z-Score策略下，优先使用计算的最优收益率
        if self.trade_config.daemon_mode and hasattr(signal, 'optimal_min_spread_profit_rate'):
            if signal.optimal_min_spread_profit_rate > 0:
                # 使用计算出的最优收益率作为基准
                if signal.spread_rate < signal.optimal_min_spread_profit_rate:
                    return False, f"动态收益率不足 ({signal.spread_rate:.4%} < {signal.optimal_min_spread_profit_rate:.4%})"

        if self.trade_config.daemon_mode and not signal.is_zscore_triggered():
            return False, f"zscore收益率不足, Z-Score:{signal.z_score_after_fee:.2f}"

        return True, "风控检查通过"

    async def _execute_trade(self, signal: TradeSignal, amount: float):
        """
        执行对冲交易
        :param signal: 交易信号
        :param amount: 交易数量
        """
        try:
            logger.info(f"🔨 {self.symbol} {self.exchange_pair} 执行对冲交易: {amount:.4f}(${amount*signal.price1:.2f}) @ {signal.price1}/{signal.price2} "
                        f"价差收益率={signal.spread_rate:.4%} {signal.z_score:.2f}({signal.zscore_threshold:.2f})({signal.delay_ms():.2f}ms)")
            logger.debug(signal)
            if time.time() - signal.signal_generate_start_time > 0.050:
                logger.error(f"❌❌ {self.symbol} {self.exchange_pair} 交易前总耗时: {signal.delay_ms():.2f}ms 过大, 拒绝交易")
                return
            elif time.time() - signal.signal_generate_start_time > 0.010:
                # > 10ms 记录警告日志
                logger.warning(f"⚠️ {self.symbol} {self.exchange_pair} 交易前总耗时: {signal.delay_ms():.2f}ms")
            # 并发下单（传入参考价格）
            order1_task = asyncio.create_task(
                self._place_order_exchange1(self.trade_config.pair1, signal.side1, amount, signal.price1, reduceOnly=(not signal.is_add_position()))
            )
            order2_task = asyncio.create_task(
                self._place_order_exchange2(self.trade_config.pair2, signal.side2, amount, signal.price2, reduceOnly=(not signal.is_add_position()))
            )

            # 等待两个订单都完成
            order1, order2 = await asyncio.gather(order1_task, order2_task)

            # 等待订单成交
            await asyncio.sleep(0.1)

            # 获取成交均价
            order1_avg_price = await self._get_order_avg_price(self.exchange1, order1, self.trade_config.pair1)
            order2_avg_price = await self._get_order_avg_price(self.exchange2, order2, self.trade_config.pair2)

            # 计算实际价差收益
            actual_spread = order1_avg_price - order2_avg_price
            if signal.side1 == TradeSide.BUY:
                spread_profit = -actual_spread * amount
            else:
                spread_profit = actual_spread * amount

            # 更新统计
            self._trade_count += 1
            self._cum_volume += amount * order1_avg_price
            self._cum_profit += spread_profit
            self._remaining_amount -= amount
            if self._remaining_amount > 0:
                remaining_info = f"剩余 {self._remaining_amount:.4f}"
            else:
                remaining_info = ""

            # 更新最后交易时间
            self._last_trade_time = time.time()
            executed_spread_profit_rate = spread_profit / (amount * order1_avg_price)
            trade_msg = (f"✅ {self.symbol} {self.exchange_pair} 交易完成 #{self._trade_count}: "
                        f"成交价 {order1_avg_price:.2f}/{order2_avg_price:.2f} "
                        f"收益 ${spread_profit:.2f} ({executed_spread_profit_rate:.4%}) "
                        f"累计 ${self._cum_volume:.2f} (${self._cum_profit:.2f}){remaining_info}")
            logger.info(trade_msg)
            await async_notify_telegram(f"信号触发:{str(signal)}", channel_type=CHANNEL_TYPE.QUIET)
            await async_notify_telegram(trade_msg, channel_type=CHANNEL_TYPE.QUIET)
            # 机会交易有持仓，转为持续交易
            if self._trade_count == 1 and self.trade_config.daemon_mode and self.trade_config.no_trade_timeout_sec > 0:
                self.trade_config.no_trade_timeout_sec = 0
                self.trade_config._timeout_enabled = False
                logger.info(f"🔄 {self.trade_config.pair1} 建立了仓位，转为持续交易模式")

            # 动态调整最小收益率
            await self._adjust_min_profit_rate(executed_spread_profit_rate)

            # 判断是否需要暂停交易
            is_add_position = signal.is_add_position()
            use_min_profit_rate = self.risk_config.min_profit_rate if is_add_position else self.risk_config.reduce_pos_min_profit_rate
            delay_time = (use_min_profit_rate - executed_spread_profit_rate) / abs(
                use_min_profit_rate)
            delay_time = min(delay_time, 3)  # 最多暂停3min
            if delay_time > 0:
                logger.info(
                    f"⚠️ 价差收益率 {executed_spread_profit_rate:.2%} < {use_min_profit_rate:.2%}"
                    f"暂停交易{int(delay_time * 60)}s")
                try:
                    await asyncio.sleep(int(60 * delay_time))
                except KeyboardInterrupt:
                    logger.info("🚧 人工终止暂停")
                    new_min_profit_rate = FloatPrompt.ask("修改最小价差收益率?",
                                                          default=self.risk_config.min_profit_rate)
                    if new_min_profit_rate != self.risk_config.min_profit_rate:
                        min_profit_rate = new_min_profit_rate
                        print(f"🚀 修改最小价差收益率: {min_profit_rate:.2%}")
                    if not Confirm.ask("是否继续执行交易?", default=True):
                        raise Exception("用户终止交易")

        except Exception as e:
            if "用户终止交易" not in str(e):
                logger.error(f"❌ 交易执行异常: {e}")
                raise e

    async def _place_order_exchange1(self, pair: str, side: str, amount: float, price: float, reduceOnly):
        """在交易所1下单（异步接口）"""
        return await self.exchange1.make_new_order(pair, side, "MARKET", amount, price=price, reduceOnly=reduceOnly)

    async def _place_order_exchange2(self, pair: str, side: str, amount: float, price: float, reduceOnly):
        """在交易所2下单（异步接口）"""
        return await self.exchange2.make_new_order(pair, side, "MARKET", amount, price=price, reduceOnly=reduceOnly)

    async def _adjust_min_profit_rate(self, executed_profit_rate: float):
        """
        根据实际执行收益率动态调整最小收益率要求

        调整逻辑：
        1. 记录最近N笔交易的实际收益率
        2. 如果连续N笔交易收益率都明显高于当前最小要求，适当提高要求
        3. 如果连续N笔交易收益率都接近最小要求，适当降低要求（但不低于用户设置底线）
        4. 调整幅度为 profit_rate_adjust_step

        :param executed_profit_rate: 本次交易的实际执行收益率
        """
        if not self.risk_config.enable_dynamic_profit_rate:
            return

        # 记录最近的收益率
        self._recent_profit_rates.append(executed_profit_rate)

        # 只保留最近N笔
        max_records = self.risk_config.profit_rate_adjust_threshold
        if len(self._recent_profit_rates) > max_records:
            self._recent_profit_rates = self._recent_profit_rates[-max_records:]

        # 至少需要N笔交易数据才能调整
        if len(self._recent_profit_rates) < self.risk_config.profit_rate_adjust_threshold:
            return

        # 检查是否需要调整（距离上次调整至少N笔交易）
        trades_since_last_adjustment = self._trade_count - self._last_adjustment_trade_count
        if trades_since_last_adjustment < self.risk_config.profit_rate_adjust_threshold:
            return

        # 计算平均收益率
        avg_profit_rate = sum(self._recent_profit_rates) / len(self._recent_profit_rates)
        current_min_rate = self.risk_config.min_profit_rate

        # 判断是否需要调整
        # 情况1: 实际收益率持续超出当前要求较多，提高要求以获得更好的entry
        if avg_profit_rate > current_min_rate * 1.5:
            new_min_rate = current_min_rate + self.risk_config.profit_rate_adjust_step
            self.risk_config.min_profit_rate = new_min_rate
            self._last_adjustment_trade_count = self._trade_count
            logger.info(
                f"📈 动态调整: 实际收益率 {avg_profit_rate:.4%} 持续超出要求，"
                f"提高最小收益率 {current_min_rate:.4%} -> {new_min_rate:.4%}"
            )
            # 清空记录，重新统计
            self._recent_profit_rates.clear()

        # 情况2: 实际收益率接近当前要求，适当降低要求（但不低于用户底线）
        elif (current_min_rate * 1.05 < avg_profit_rate < current_min_rate * 1.1
              and current_min_rate > self.risk_config.user_min_profit_rate
              and current_min_rate > self._initial_min_profit_rate):
            new_min_rate = max(
                current_min_rate - self.risk_config.profit_rate_adjust_step,
                self.risk_config.user_min_profit_rate
            )
            if new_min_rate < current_min_rate:
                self.risk_config.min_profit_rate = new_min_rate
                self._last_adjustment_trade_count = self._trade_count
                logger.info(
                    f"📉 动态调整: 实际收益率 {avg_profit_rate:.4%} 接近最小要求，"
                    f"降低最小收益率 {current_min_rate:.4%} -> {new_min_rate:.4%} "
                    f"(底线: {self.risk_config.user_min_profit_rate:.4%})"
                )
                # 清空记录，重新统计
                self._recent_profit_rates.clear()

    async def _get_order_avg_price(self, exchange, order: dict, pair: str) -> float:
        """获取订单成交均价"""
        from cex_tools.cex_enum import ExchangeEnum

        order_id = order.get("orderId")

        # HyperLiquid直接返回成交均价
        if hasattr(exchange, 'exchange_code') and exchange.exchange_code == ExchangeEnum.HYPERLIQUID:
            return float(order.get("avgPx", 0))

        # 其他交易所需要查询订单详情
        max_retries = 30
        for _ in range(max_retries):
            try:
                order_info = await exchange.get_recent_order(pair, orderId=order_id)

                if order_info:
                    return order_info.avgPrice

                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(f"{exchange.exchange_code} 获取订单均价失败: {e}")
                await asyncio.sleep(0.3)

        raise Exception(f"{exchange.exchange_code} 获取订单 {order_id} 成交均价失败")

    async def start(self):
        """启动实时对冲引擎"""
        logger.info(f"🚀 启动实时对冲引擎")
        logger.info(f"   交易对: {self.trade_config.pair1} / {self.trade_config.pair2}")
        logger.info(f"   默认方向: {self.trade_config.side1} / {self.trade_config.side2}")
        logger.info(f"   总数量: {self.trade_config.total_amount:.4f}")
        logger.info(f"   单笔数量区间: {self.trade_config.amount_min:.4f} ~ {self.trade_config.amount_max:.4f} "
                    f"(步长 {self.trade_config.amount_step:.4f})")
        logger.info(f"   动态调整: {'启用' if self.trade_config.use_dynamic_amount else '禁用'} "
                    f"(最大吃掉第一档 {self.trade_config.max_first_level_ratio:.1%})")
        logger.info(f"   风控配置: 订单簿过期={self.risk_config.max_orderbook_age_sec}s "
                    f"价差<{self.risk_config.max_spread_pct:.2%} "
                    f"最小收益率>{self.risk_config.min_profit_rate:.4%}")

        # 如果启用了无成交降低收益率机制
        if (self.risk_config.enable_dynamic_profit_rate and
                self.risk_config.no_trade_reduce_timeout_sec > 0):
            logger.info(f"   无成交降低机制: {self.risk_config.no_trade_reduce_timeout_sec}秒无成交后降低收益率 "
                        f"(底线: {self.risk_config.user_min_profit_rate:.4%})")

        # 订阅订单簿
        self.stream1.subscribe(self.trade_config.pair1, self._on_orderbook1_update)
        self.stream2.subscribe(self.trade_config.pair2, self._on_orderbook2_update)

        # 启动WebSocket流
        await self.stream1.start()
        await self.stream2.start()

        for _ in range(50):  # 最多等待5秒
            if self._latest_orderbook1 and self._latest_orderbook2:
                break
            await asyncio.sleep(0.1)

        while not self._latest_orderbook1 or not self._latest_orderbook2:
            logger.error(f"❌ {self.symbol} {self.exchange_pair} 订单簿数据未就绪")
            await asyncio.sleep(1)
            continue
        await asyncio.sleep(0.1)  # 确保数据稳定
        logger.info(f"✅ 订单簿数据就绪: {self._latest_orderbook1} / {self._latest_orderbook2}")

        # 开始交易循环
        self._running = True
        await self._trading_loop()

    def _should_log_waiting(self) -> bool:
        """
        判断是否应该记录等待日志（避免刷屏）
        每10秒记录一次
        """
        current_time = time.time()
        if not hasattr(self, '_last_wait_log_time'):
            self._last_wait_log_time = 0

        if current_time - self._last_wait_log_time > 10:
            self._last_wait_log_time = current_time
            return True
        return False

    async def auto_force_reduce_position_to_safe(self):
        force_reduce_value = 0
        total_spread_profit = 0
        while True:
            risk_data = self._get_risk_data()
            if not risk_data.should_force_reduce():
                break
            reduce_side1 = self._position2.position_side
            reduce_side2 = self._position1.position_side
            amount = self._position1.positionAmt
            mid_price = await self.exchange1.get_tick_price(self.symbol)
            while amount * mid_price > self.trade_config.max_order_value_usd:
                amount = amount / 2
            amount = await self.exchange1.convert_size(self.trade_config.pair1, amount)
            order1_task = asyncio.create_task(
                self._place_order_exchange1(self.trade_config.pair1, reduce_side1, amount, mid_price,
                                            reduceOnly=True)
            )
            order2_task = asyncio.create_task(
                self._place_order_exchange2(self.trade_config.pair2, reduce_side2, amount, mid_price,
                                            reduceOnly=True)
            )

            # 等待两个订单都完成
            order1, order2 = await asyncio.gather(order1_task, order2_task)

            # 等待订单成交
            await asyncio.sleep(0.1)

            # 获取成交均价
            order1_avg_price = await self._get_order_avg_price(self.exchange1, order1, self.trade_config.pair1)
            order2_avg_price = await self._get_order_avg_price(self.exchange2, order2, self.trade_config.pair2)

            # 计算实际价差收益
            actual_spread = order1_avg_price - order2_avg_price
            if reduce_side1 == TradeSide.BUY:
                spread_profit = -actual_spread * amount
            else:
                spread_profit = actual_spread * amount
            logger.warning(f"⚠️ ⚠️ {self.symbol} {self.exchange_pair} 触发强制减仓: ${amount * mid_price:.2f}, "
                           f"价差收益:${spread_profit:.2f}")
            force_reduce_value += amount * mid_price
            total_spread_profit += spread_profit
            await self._update_exchange_info()
        if force_reduce_value > 0:
            await async_notify_telegram(f"⚠️ ⚠️ {self.symbol} {self.exchange_pair} "
                                        f"触发强制减仓: ${force_reduce_value:.2f} "
                                        f"价差收益:${total_spread_profit:.2f} ({total_spread_profit/force_reduce_value:.3%})")

    async def _trading_loop(self):
        """
        交易主循环
        持续监控订单簿，一旦发现满足条件的信号立即执行交易
        """
        logger.info(f"{self.symbol} {self.exchange_pair} 启动交易...")
        await self._update_exchange_info()
        logger.info(f"当前持仓: {self._position1} / {self._position2}")
        # 如果启用了超时，记录超时配置
        if self._timeout_enabled:
            logger.info(f"⏱️ {self.symbol} {self.exchange_pair}超时: {self.trade_config.no_trade_timeout_sec}秒")

        while self._running and (self._remaining_amount > 0 or self.trade_config.daemon_mode):
            try:
                if self.trade_config.daemon_mode and not self._get_risk_data():
                    logger.warning(f"⚠️ {self.symbol} {self.exchange_pair} 获取风控数据缓存失败... 等待")
                    await asyncio.sleep(1)  # 订单簿数据or 缓存未就绪，短暂等待
                    continue
                # 检查无交易超时
                if self._timeout_enabled:
                    elapsed_since_last_trade = time.time() - self._last_trade_time
                    if elapsed_since_last_trade > self.trade_config.no_trade_timeout_sec:
                        logger.warning(
                            f"⏱️  超过 {self.trade_config.no_trade_timeout_sec}秒 无交易，自动停止引擎"
                        )
                        logger.info(
                            f"   上次交易: {elapsed_since_last_trade:.1f}秒前, "
                            f"已完成交易: {self._trade_count}笔"
                        )
                        self._running = False
                        break

                # 检查无成交降低收益率机制（在超时前主动降低收益率以提高成交概率）
                # 关键逻辑：只有当前收益率被上调过（高于初始值）时，才允许降低
                # 这样可以避免破坏用户精心设置的初始收益率
                if (self.risk_config.enable_dynamic_profit_rate and
                        self.risk_config.no_trade_reduce_timeout_sec > 0):
                    elapsed_since_last_trade = time.time() - self._last_trade_time

                    # 核心条件：
                    # 1. 超过设定时间无成交
                    # 2. 当前收益率高于初始值（说明被上调过）
                    # 3. 当前收益率高于底线（防止降到底线以下）
                    if (elapsed_since_last_trade > self.risk_config.no_trade_reduce_timeout_sec and
                            self.risk_config.min_profit_rate > self._initial_min_profit_rate and
                            self.risk_config.min_profit_rate > self.risk_config.user_min_profit_rate and
                            self._reduce_min_profit_rate_cnt < 5):

                        # 降低收益率（使用更大的步长以加快调整）
                        reduce_step = (self.risk_config.profit_rate_adjust_step *
                                       self.risk_config.no_trade_reduce_step_multiplier)

                        # 降低目标：不低于初始值，也不低于底线
                        new_min_rate = max(
                            self.risk_config.min_profit_rate - reduce_step,
                            self._initial_min_profit_rate,
                            self.risk_config.user_min_profit_rate
                        )

                        if new_min_rate < self.risk_config.min_profit_rate:
                            old_rate = self.risk_config.min_profit_rate
                            self.risk_config.min_profit_rate = new_min_rate
                            logger.warning(
                                f"⚠️  无成交超时调整: {elapsed_since_last_trade:.0f}秒无成交，"
                                f"降低最小收益率 {old_rate:.4%} -> {new_min_rate:.4%} "
                                f"(初始值: {self._initial_min_profit_rate:.4%}, "
                                f"底线: {self.risk_config.user_min_profit_rate:.4%})"
                            )
                            # 重置上次交易时间，避免连续降低
                            self._last_trade_time = time.time()
                            self._reduce_min_profit_rate_cnt += 1
                # 计算交易信号
                signal_generate_start_time = time.time()
                if self.trade_config.daemon_mode:
                    signal = await self._calculate_spread_by_daemon()
                else:
                    signal = await self._calculate_spread()

                if not signal:
                    await asyncio.sleep(0.05)  # 订单簿数据or 缓存未就绪，短暂等待
                    continue
                else:
                    signal.signal_generate_start_time = signal_generate_start_time

                if time.time() - signal.signal_generate_start_time > 0.010:
                    # > 10ms 记录警告日志
                    logger.warning(
                        f"⚠️ {self.symbol} {self.exchange_pair} 信号生成: {signal.delay_ms():.2f}ms")

                # 风控检查
                passed, msg = await self._risk_check(signal)
                signal.pass_risk_check = passed
                signal.risk_message = msg

                if time.time() - signal.signal_generate_start_time > 0.010:
                    # > 10ms 记录警告日志
                    logger.warning(
                        f"⚠️ {self.symbol} {self.exchange_pair} 风控检查: {signal.delay_ms():.2f}ms")

                if not passed:
                    # 不满足条件，记录日志但继续寻找机会（避免刷屏）
                    if "收益率不足" in msg:
                        if self._should_log_waiting():
                            # 计算剩余超时时间
                            timeout_info = ""
                            if self._timeout_enabled:
                                elapsed = time.time() - self._last_trade_time
                                remaining_timeout = self.trade_config.no_trade_timeout_sec - elapsed
                                timeout_info = f", 剩余: {remaining_timeout:.0f}秒"
                            if self._remaining_amount > 0:
                                remaining_info = f"剩余 {self._remaining_amount:.4f}"
                            else:
                                remaining_info = ""
                            logger.debug(signal)
                            logger.info(
                                f"⏳ {self.symbol} {self.exchange_pair} Spread Profit: {signal.spread_rate:.4%} "
                                f"z_score:{signal.z_score:.2f}/{signal.z_score_after_fee:.2f}({self.trade_config.zscore_threshold:.2f}) {signal.delay_ms():.2f}ms"
                                f"{remaining_info}{timeout_info}"
                            )
                        # 价差不足时等待较长时间再检查
                        await asyncio.sleep(0.1)
                    else:
                        if self._should_log_waiting():
                            # 其他风控问题（订单簿过期、价差过大等），仅debug级别，短暂等待
                            logger.warning(f"⚠️ {self.symbol} {self.exchange_pair}{msg}")
                        await asyncio.sleep(0.3)

                    continue

                # 找到满足条件的机会！动态计算交易数量
                trade_amount = await self._calculate_trade_amount(signal)

                if time.time() - signal.signal_generate_start_time > 0.010:
                    # > 10ms 记录警告日志
                    logger.warning(
                        f"⚠️ {self.symbol} {self.exchange_pair} 计算成交额: {signal.delay_ms():.2f}ms")

                if trade_amount <= 0:
                    logger.warning("计算的交易数量为0，跳过本次交易")
                    await asyncio.sleep(0.05)
                    continue

                # 执行交易
                await self._execute_trade(signal, trade_amount)

                await self._update_exchange_info()

                await self.auto_force_reduce_position_to_safe()

                # 交易间隔（给市场一点时间恢复）
                await asyncio.sleep(self.trade_config.trade_interval_sec)

            except KeyboardInterrupt:
                logger.info("🚧 用户中断交易")
                # 询问是否调整参数
                from rich.prompt import Confirm, FloatPrompt

                new_min_profit_rate = FloatPrompt.ask(
                    "输入新的最小价差收益率",
                    default=self.risk_config.min_profit_rate
                )
                self.risk_config.min_profit_rate = new_min_profit_rate
                logger.info(f"✅ 已更新最小价差收益率: {self.risk_config.min_profit_rate:.4%}")

                if not Confirm.ask("是否继续执行交易?", default=True):
                    self._running = False
                    logger.info("🛑 用户选择停止交易")
                    break
                else:
                    # 继续交易，重置超时计时器
                    self._last_trade_time = time.time()

            except Exception as e:
                error_msg = (f"❌❌❌ {self.symbol} {self.exchange_pair} 交易进程结束, 错误内容: {e}\n"
                             f"错误详情:\n{traceback.format_exc()}")
                await async_notify_telegram(error_msg)
                break

        logger.info(
            f"🏁 交易进程结束: 执行 {self._trade_count} 笔，累计 ${self._cum_volume:.2f}，收益 ${self._cum_profit:.2f}")
        await self._auto_balance_position()

    async def _auto_balance_position(self):
        """
            自动平衡仓位
            - 优先减仓
        """
        risk_data = self.exchange_combined_info_cache['risk_data']
        imbalance_value = risk_data.get_pos_imbalanced_value(self.symbol, self.exchange_code_list)
        if abs(imbalance_value) < 50:
            return
        imbalance_amt = risk_data.get_pos_imbalanced_value(self.symbol, self.exchange_code_list)
        if imbalance_amt > 0:
            # 做空
            side = TradeSide.SELL
        else:
            # 做多
            side = TradeSide.BUY
        use_exchange, other_exchange = (self.exchange1, self.exchange2) if self._position1.position_side != side else (self.exchange2, self.exchange1)
        mid_price = await use_exchange.get_tick_price(self.symbol)
        trade_amt = abs(imbalance_amt)
        if abs(imbalance_value) < self.risk_config.auto_pos_balance_usd_value_limit:
            try:
                await use_exchange.make_new_order(self.trade_config.pair1,
                                                side,
                                                order_type="MARKET",
                                                quantity=trade_amt,
                                                  price=mid_price, reduceOnly=True)
                text = (f"⚠️ {self._position1.pair}({use_exchange.exchange_code}) {side} "
                        f"自动平衡仓位, 减仓:  {imbalance_amt} ${imbalance_value:.4f}")
            except Exception as e:
                await other_exchange.make_new_order(self.trade_config.pair2,
                                              side,
                                              order_type="MARKET",
                                              quantity=trade_amt, price=mid_price)
                text = (f"⚠️⚠️ {self.trade_config.pair2}({other_exchange.exchange_code}), "
                        f"自动执行加仓: {imbalance_amt} ${imbalance_value:.4f} "
                        f"| ❌{use_exchange.exchange_code}减仓交易异常:{e}")
                self._running = False
        else:
            text = (f"❌ {self._position1.pair}({use_exchange.exchange_code}), "
                    f"金额超限, 需要手动执行减仓: {trade_amt} ${imbalance_value:.2f}")
        logger.warning(text)
        await async_notify_telegram(text)


    async def _update_exchange_info(self):
        risk_data, update_time = await self.update_exchange_info_helper()
        # 分发给所有引擎进程
        self.exchange_combined_info_cache['risk_data'] = risk_data
        self.exchange_combined_info_cache['update_time'] = update_time
        position_list = risk_data.get_symbol_exchange_positions(self.symbol,
                                                                self.exchange_code_list)
        if len(position_list) >= 2:
            self._position1 = position_list[0]
            self._position2 = position_list[1]
            await self._auto_balance_position()
        else:
            self._position1 = None
            self._position2 = None

    async def stop(self):
        """停止引擎"""
        self._running = False

        # 并行停止两个WebSocket流以加快关闭速度，添加超时控制
        stop_tasks = []
        if self.stream1:
            stop_tasks.append(self._safe_stop_stream(self.stream1, "stream1"))
        if self.stream2:
            stop_tasks.append(self._safe_stop_stream(self.stream2, "stream2"))

        if stop_tasks:
            try:
                # 给WebSocket流停止最多3秒时间
                await asyncio.wait_for(
                    asyncio.gather(*stop_tasks, return_exceptions=True),
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                logger.warning("⚠️ WebSocket流停止超时，继续强制关闭引擎")
            except Exception as e:
                logger.warning(f"停止WebSocket流时出现警告: {e}")


    async def _safe_stop_stream(self, stream, name):
        """安全停止WebSocket流，包含超时控制"""
        try:
            await asyncio.wait_for(stream.stop(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ {name} 停止超时")
        except Exception as e:
            logger.warning(f"⚠️ {name} 停止时出现异常: {e}")

    @property
    def is_running(self) -> bool:
        """检查引擎是否正在运行"""
        return self._running

    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            "trade_count": self._trade_count,
            "cum_volume": self._cum_volume,
            "cum_profit": self._cum_profit,
            "remaining_amount": self._remaining_amount,
            "progress": (self.trade_config.total_amount - self._remaining_amount) / self.trade_config.total_amount
        }
