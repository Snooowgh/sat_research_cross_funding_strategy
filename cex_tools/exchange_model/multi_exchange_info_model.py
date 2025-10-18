# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : multi_exchange_info_model
@Description : 单个交易所和多交易所综合信息模型
@Time        : 2024/10/24
"""
from typing import List

from utils.time_utils import get_datetime_now_str
from utils.notify_img_generator import NotifyImgGenerator


class SingleExchangeInfoModel:
    """单个交易所详细信息模型"""

    def __init__(self):
        # 基础信息
        self.exchange_code = ""
        self.taker_fee_rate = 0
        self.maker_fee_rate = 0

        self.default_safe_leverage = 0.5
        # self.default_safe_leverage = 6
        self.default_safe_maintenance_margin_ratio = 0.7
        self.default_safe_cross_margin_usage = 0.7

        self.target_leverage = 3
        # self.target_leverage = 9
        self.target_maintenance_margin_ratio = 0.8
        self.target_margin_usage_ratio = 0.8

        self.danger_leverage = 5
        # self.danger_leverage = 12
        self.danger_maintenance_margin_ratio = 0.9
        self.danger_margin_usage_ratio = 0.9

        self.force_reduce_leverage = 10
        self.force_reduce_maintenance_margin_ratio = 0.9

        # 账户资金信息
        self.total_margin = 0
        self.available_margin = 0
        self.pending_deposit_usd = 0
        self.maintenance_margin_ratio = 0

        # 仓位信息
        self.positions = []

    @property
    def can_add_position(self):
        """是否可以开新仓"""
        return self.leverage < self.default_safe_leverage and \
            self.maintenance_margin_ratio < self.default_safe_maintenance_margin_ratio and \
            self.cross_margin_usage < self.default_safe_cross_margin_usage and \
            self.total_margin > 100 and self.available_margin > 200 and self.max_open_notional_value() > 200

    def get_pair_pos_by_symbol(self, symbol):
        for pos1 in self.positions:
            if pos1.symbol == symbol:
                return pos1
        return None

    def max_open_notional_value(self):
        return self.available_margin * self.default_safe_leverage

    def get_pos_cnt_filter_by_notional(self, min_notional=10):
        return sum([1 for pos in self.positions if abs(pos.notional) > min_notional])

    @property
    def hold_pos_cnt(self):
        return len(self.positions)

    @property
    def total_notional(self):
        """总名义价值"""
        return sum([abs(pos.notional) for pos in self.positions])

    @property
    def leverage(self):
        """杠杆率（包含待入金）"""
        return self.total_notional / self.total_margin if self.total_margin != 0 else 0

    @property
    def balance_leverage(self):
        """余额杠杆率（包含待入金）"""
        return self.total_notional / (self.total_margin + self.pending_deposit_usd) \
            if (self.total_margin + self.pending_deposit_usd) != 0 else 0

    @property
    def total_funding_fee(self):
        return sum([pos.fundingFee for pos in self.positions])

    @property
    def cross_margin_usage(self):
        """全仓模式下可用资金比例"""
        return 1 - self.available_margin / self.total_margin if self.total_margin != 0 else 0

    @property
    def total_unrealized_pnl(self):
        """总未实现盈亏"""
        return sum([pos.unRealizedProfit for pos in self.positions])

    def should_notify_risk(self):
        """是否需要风险预警"""
        if self.leverage >= self.danger_leverage:
            return True, f"{self.exchange_code}杠杆率过高: {self.leverage:.2f}"
        if self.maintenance_margin_ratio >= self.danger_maintenance_margin_ratio:
            return True, f"{self.exchange_code}维持保证金比例过高: {self.maintenance_margin_ratio:.2%}"
        if self.cross_margin_usage >= self.danger_margin_usage_ratio:
            return True, f"{self.exchange_code}保证金使用比例过高: {self.cross_margin_usage:.2%}"
        return False, ""

    def should_force_reduce(self):
        return (self.leverage >= self.force_reduce_leverage or
                self.maintenance_margin_ratio >= self.force_reduce_maintenance_margin_ratio)

    def __str__(self):
        """格式化输出交易所信息"""
        result = []

        # 头部信息
        result.append(f"🏦 {self.exchange_code} | 总资金: ${self.total_margin:.2f}")

        # 杠杆信息
        balance_leverage_desc = f"({self.balance_leverage:.2f})" if self.pending_deposit_usd > 1 else ""
        prefix_img = "✅" if self.leverage < self.target_leverage else "⚠️"
        result.append(f"{prefix_img} 杠杆率: {self.leverage:.2f}{balance_leverage_desc}")

        # 保证金使用情况
        prefix_img = "✅" if self.cross_margin_usage < self.target_margin_usage_ratio else "⚠️"
        result.append(f"{prefix_img} 保证金使用比例: {self.cross_margin_usage:.2%}")

        # 维持保证金比例
        prefix_img = "✅" if self.maintenance_margin_ratio < self.target_maintenance_margin_ratio else "⚠️"
        result.append(f"{prefix_img} 维持保证金比例: {self.maintenance_margin_ratio:.2%}")

        # 仓位信息
        if self.positions:
            active_positions = self.get_pos_cnt_filter_by_notional(10000)
            result.append(f"🐳 仓位({active_positions}|{self.hold_pos_cnt}): ${self.total_notional:,.2f}")

            # 显示各个仓位
            for pos in self.positions:
                side_emoji = "🟢" if pos.position_side == "BUY" else "🔴"
                pnl_emoji = "🟢" if pos.unRealizedProfit > 0 else "🔴"
                funding_emoji = "📈" if pos.fundingFee > 0 else "📉"
                pos_funding_rate = pos.funding_rate if hasattr(pos, 'funding_rate') and pos.funding_rate else 0
                result.append(
                    f"  {side_emoji} {pos.symbol.replace('USDT', '')} ({pos_funding_rate:.2%}) ${abs(pos.notional):,.0f} | {pnl_emoji} ${pos.unRealizedProfit:+,.2f} | {funding_emoji} {pos.fundingFee:+.2f}")

        # 资金费用
        if self.total_funding_fee != 0:
            funding_emoji = "📈" if self.total_funding_fee > 0 else "📉"
            result.append(f"{funding_emoji} 总资金费用: {self.total_funding_fee:+,.2f}")

        # 未实现盈亏
        if self.total_unrealized_pnl != 0:
            pnl_emoji = "🟢" if self.total_unrealized_pnl > 0 else "🔴"
            result.append(f"{pnl_emoji} 未实现盈亏: {self.total_unrealized_pnl:+,.2f}")

        # 时间戳
        result.append("🏷️" * 3 + get_datetime_now_str() + "🏷️" * 3)

        return "\n".join(result)


class MultiExchangeCombinedInfoModel:
    """多交易所综合信息模型"""

    def __init__(self):
        # 基础配置
        self.default_safe_leverage = 4.5
        self.target_leverage = 8
        self.target_maintenance_margin_ratio = 0.8
        self.target_margin_usage_ratio = 0.95

        # 交易所信息列表
        self.exchange_infos: List[SingleExchangeInfoModel] = []

        # 汇总信息
        self.total_margin = 0
        self.total_available_margin = 0
        self.total_unrealized_pnl = 0
        self.total_funding_fee = 0
        self.total_notional = 0

        # 合并后的仓位信息
        self.merged_positions = []

        # 费率套利机会
        self.funding_opportunities = []

        # 性能指标
        self.time_cost = 0

    @property
    def holding_symbol_list(self):
        ret = [pos["symbol"] for pos in self.merged_positions]
        return ret

    @property
    def total_leverage(self):
        """综合杠杆率"""
        return self.total_notional / self.total_margin if self.total_margin != 0 else 0

    @property
    def cross_margin_usage(self):
        """综合保证金使用率"""
        return 1 - self.total_available_margin / self.total_margin if self.total_margin != 0 else 0

    @property
    def exchange_count(self):
        """交易所数量"""
        return len(self.exchange_infos)

    @property
    def exchange_codes(self):
        """交易所代码"""
        return "-".join([e.exchange_code for e in self.exchange_infos])

    @property
    def active_position_count(self):
        """活跃仓位数量（合并后）"""
        return len([pos for pos in self.merged_positions if abs(pos['notional']) > 10000])

    def get_exchange_info_by_code(self, exchange_code: str):
        """根据交易所代码获取交易所信息"""
        for exchange_info in self.exchange_infos:
            if exchange_info.exchange_code == exchange_code:
                return exchange_info
        return None

    def get_symbol_exchange_positions(self, symbol: str, exchange_codes: List[str] = None) -> list:
        """
        获取指定交易对和交易所的所有仓位
        Args:
            symbol: 交易对符号（如 "BTC" 或 "BTCUSDT"）
            exchange_codes:
        Returns:
            list: 仓位对象列表
        """
        positions = []
        if exchange_codes is None:
            for exchange_info in self.exchange_infos:
                for pos in exchange_info.positions:
                    if pos.symbol == symbol:
                        positions.append(pos)
        else:
            # and (exchange_codes is None or pos.exchange_code in exchange_codes)
            for exchange_code in exchange_codes:
                for exchange_info in self.exchange_infos:
                    if exchange_info.exchange_code == exchange_code:
                        has_pos = False
                        for pos in exchange_info.positions:
                            if pos.symbol == symbol:
                                positions.append(pos)
                                has_pos = True
                        if not has_pos:
                            positions.append(None)
        return positions

    def get_pos_imbalanced_value(self, symbol: str, exchange_codes: List[str] = None):
        positions_list = self.get_symbol_exchange_positions(symbol, exchange_codes)
        value = 0
        for pos in positions_list:
            if pos is None:
                amt = 0
            else:
                amt = pos.positionAmt
                value += amt * pos.entryPrice
        return value

    def get_pos_imbalanced_amt(self, symbol: str, exchange_codes: List[str] = None):
        positions_list = self.get_symbol_exchange_positions(symbol, exchange_codes)
        value = 0
        for pos in positions_list:
            if pos is None:
                amt = 0
            else:
                amt = pos.positionAmt
            value += amt
        return value

    def merge_positions(self):
        """合并相同交易对的仓位"""
        position_map = {}

        for exchange_info in self.exchange_infos:
            for pos in exchange_info.positions:
                symbol = pos.symbol.replace('USDT', '')
                if symbol not in position_map:
                    position_map[symbol] = {
                        'symbol': symbol,
                        'total_notional': 0,
                        'notional': 0,
                        'total_unrealized_pnl': 0,
                        'total_funding_fee': 0,
                        'exchanges': [],
                        'avg_entry_price': 0,
                        'position_side': [],
                        'total_amount': 0,
                        "spread_profit": 0,
                        "funding_rate": [],
                        'hold_amount_list': []
                    }

                pos_info = position_map[symbol]
                pos_info['total_notional'] += pos.notional
                pos_info['refer_price'] = pos.entryPrice
                pos_info['notional'] += abs(pos.notional)
                pos_info['total_unrealized_pnl'] += pos.unRealizedProfit
                pos_info['total_funding_fee'] += getattr(pos, 'fundingFee', 0)
                pos_info['exchanges'].append(exchange_info.exchange_code)
                pos_info['position_side'].append(pos.position_side)
                pos_info['funding_rate'].append(getattr(pos, 'funding_rate', 0))
                pos_info['total_amount'] += pos.positionAmt
                pos_info['hold_amount_list'].append(pos.positionAmt)

                # 计算加权平均入场价格
                if hasattr(pos, 'entryPrice') and pos.entryPrice and pos.positionAmt != 0:
                    pos_info['spread_profit'] += -pos.entryPrice * pos.positionAmt

        # 计算最终的加权平均价格和其他属性
        for symbol, pos_info in position_map.items():
            # 计算盈亏率
            pos_info['spread_profit_rate'] = pos_info['spread_profit'] / abs(pos_info['notional']) if pos_info['notional'] != 0 else 0
            pos_info['notional'] /= 2
            pos_info["funding_profit_rate_apy"] = 0
            for funding_rate, side in zip(pos_info['funding_rate'], pos_info['position_side']):
                if side == "BUY":
                    pos_info["funding_profit_rate_apy"] += -funding_rate
                else:
                    pos_info["funding_profit_rate_apy"] += funding_rate
            # 添加到合并列表
            self.merged_positions.append(pos_info)
        self.merged_positions = sorted(self.merged_positions, key=lambda x: abs(x['notional']), reverse=True)

    def calculate_summary(self):
        """计算汇总信息"""
        self.total_margin = sum(ex_info.total_margin for ex_info in self.exchange_infos)
        self.total_available_margin = sum(ex_info.available_margin for ex_info in self.exchange_infos)
        self.total_unrealized_pnl = sum(ex_info.total_unrealized_pnl for ex_info in self.exchange_infos)
        self.total_funding_fee = sum(ex_info.total_funding_fee for ex_info in self.exchange_infos)
        self.total_notional = sum(ex_info.total_notional for ex_info in self.exchange_infos)

    def should_notify_risk(self):
        """是否需要风险预警"""
        total_should, total_msg = False, ""
        for pos_info in self.merged_positions:
            unbalanced_value = pos_info["total_amount"] * pos_info["refer_price"]
            if abs(unbalanced_value) > 200 and len(pos_info["exchanges"]) > 1:
                total_should = True
                total_msg += f"{pos_info['symbol']}持仓不平衡金额: ${unbalanced_value:.2f}\n"
        for ex in self.exchange_infos:
            should, msg = ex.should_notify_risk()
            if should:
                total_should = True
                total_msg += msg + "\n"
        return total_should, total_msg

    def should_force_reduce(self):
        return any(ex.should_force_reduce() for ex in self.exchange_infos)

    def max_open_notional_value(self, exchange_codes: List[str] = None):
        max_open_notional_value = None
        for ex in self.exchange_infos:
            if exchange_codes is None or ex.exchange_code in exchange_codes:
                if max_open_notional_value is None:
                    max_open_notional_value = ex.max_open_notional_value()
                else:
                    max_open_notional_value = min(max_open_notional_value, ex.max_open_notional_value())
        return max_open_notional_value

    def __str__(self):
        """格式化输出多交易所综合信息"""
        result = []

        # 头部信息
        result.append(f"🏦 {self.exchange_codes.upper()}")
        result.append(f"💰 总资金: ${self.total_margin:,.2f} | 可用: ${self.total_available_margin:,.2f}")

        # 杠杆信息
        leverage_emoji = "✅" if self.total_leverage < self.target_leverage else "⚠️"
        result.append(f"{leverage_emoji} 综合杠杆率: {self.total_leverage:.2f}")

        # 保证金使用情况
        margin_emoji = "✅" if self.cross_margin_usage < self.target_margin_usage_ratio else "⚠️"
        result.append(f"{margin_emoji} 保证金使用比例: {self.cross_margin_usage:.2%}")
        # 各交易所信息概览
        result.append("📊 各交易所概览:")
        for ex_info in self.exchange_infos:
            # 杠杆率状态
            leverage_emoji = "✅" if ex_info.leverage < self.target_leverage else "⚠️"

            # 保证金使用比例状态
            margin_usage_emoji = "✅" if ex_info.cross_margin_usage < self.target_margin_usage_ratio else "⚠️"

            # 维持保证金比例状态
            maintenance_emoji = "✅" if ex_info.maintenance_margin_ratio < self.target_maintenance_margin_ratio else "⚠️"

            result.append(
                f"  • {ex_info.exchange_code.upper()}: ${ex_info.total_margin:,.2f}({ex_info.total_margin / self.total_margin if self.total_margin > 0 else 0:.1%})|${ex_info.total_notional:,.2f}|({ex_info.get_pos_cnt_filter_by_notional(10000)}/{len(ex_info.positions)})")
            result.append(
                f"      杠杆 {leverage_emoji}{ex_info.leverage:.2f} | 保证金使用 {margin_usage_emoji}{ex_info.cross_margin_usage:.2%} | 维持保证金 {maintenance_emoji}{ex_info.maintenance_margin_ratio:.2%}")

        # 合并仓位信息
        if self.merged_positions:
            result.append(
                f"🐳 合并仓位({self.active_position_count}|{len(self.merged_positions)}): ${self.total_notional:,.2f}")
            # 显示各个合并后的仓位
            for pos in self.merged_positions:
                pnl_emoji = "🟢" if pos['total_unrealized_pnl'] > 0 else "🔴"
                funding_fee_emoji = "📈" if pos['total_funding_fee'] > 0 else "📉"
                spread_profit_rate = pos['spread_profit_rate']
                funding_profit_rate_apy = pos['funding_profit_rate_apy']
                funding_emoji = NotifyImgGenerator.get_expected_month_profit_rate_img(
                    funding_profit_rate_apy / 12 * 5) if funding_profit_rate_apy > 0 else "🔴"
                spread_emoji = "🟢" if spread_profit_rate > 0 else "🔴"
                exchanges_str = ", ".join(pos['exchanges']).upper()
                solo_pos_emoji = "(⚠️ SOLO)" if len(pos["exchanges"]) == 1 else ""
                result.append(
                    f"  {funding_emoji}{spread_emoji} {pos['symbol']} {funding_profit_rate_apy:.2%}({spread_profit_rate:.2%}) ${pos['notional']:,.0f} | {pnl_emoji} ${pos['total_unrealized_pnl']:+,.2f} | {funding_fee_emoji} {pos['total_funding_fee']:+.2f}")
                hold_amt_info = f" 持仓数量: {','.join([f'{amt}' for amt in pos['hold_amount_list']])}" if len(
                    pos['exchanges']) > 2 else ""
                result.append(f"      持有交易所: {exchanges_str}({', '.join(pos['position_side'])}){hold_amt_info}{solo_pos_emoji}")

        # 费率套利机会
        if self.funding_opportunities:
            result.append(f"🎯 费率套利机会(TOP{len(self.funding_opportunities)}):")
            for i, opp in enumerate(self.funding_opportunities, 1):
                # 计算成本覆盖时间
                taker_fee = sum([a.taker_fee_rate for a in self.exchange_infos])  # 假设平均taker费率
                if opp.funding_profit_rate > 0:
                    cost_cover_hours = taker_fee / (opp.funding_profit_rate / 365 / 24)
                else:
                    cost_cover_hours = 0

                # 价差信息
                if opp.spread_stats:
                    spread_info = f"{opp.spread_stats.mean_spread:.2%}±{opp.spread_stats.std_spread:.2%}"
                else:
                    spread_info = "未分析"
                img = NotifyImgGenerator.get_expected_month_profit_rate_img(opp.funding_profit_rate / 12 * 5)
                img2 = NotifyImgGenerator.get_spread_profit_rate_img(opp.mean_spread_profit_rate)
                result.append(
                    f"  {img} {opp.pair}: {opp.funding_profit_rate:.2%} | {opp.exchange1}/{opp.exchange2} | {opp.position_side1}/{opp.position_side2}")
                result.append(
                    f"      {img2} 价差: {opp.cur_price_diff_pct:.3%}({spread_info}) | 回本: {cost_cover_hours:.1f}h")

        # 资金费用
        if self.total_funding_fee != 0:
            funding_emoji = "📈" if self.total_funding_fee > 0 else "📉"
            result.append(f"{funding_emoji} 总资金费用: {self.total_funding_fee:+,.2f}")

        # 未实现盈亏
        if self.total_unrealized_pnl != 0:
            pnl_emoji = "🟢" if self.total_unrealized_pnl > 0 else "🔴"
            result.append(f"{pnl_emoji} 总未实现盈亏: {self.total_unrealized_pnl:+,.2f}")
        profit_year = sum([pos["notional"] * pos["funding_profit_rate_apy"] for pos in self.merged_positions])
        profit_year_rate = profit_year / self.total_margin if self.total_margin != 0 else 0
        result.append(f"🧮 预估年化: {profit_year_rate:.2%} | ${profit_year:,.2f}")
        # 时间戳
        result.append("🏷️" * 3 + get_datetime_now_str() + f" | 耗时: {self.time_cost:.2f}s" + "🏷️" * 3)

        return "\n".join(result)
