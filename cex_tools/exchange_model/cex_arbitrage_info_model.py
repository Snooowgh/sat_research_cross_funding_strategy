# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : arbitrage_info_model
@Description :
@Time        : 2024/10/24 19:57
"""
from cex_tools.exchange_model.base_model import TradeDirection
from utils.notify_img_generator import NotifyImgGenerator
from utils.time_utils import get_datetime_now_str


class CexArbitrageInfoModel:
    # 两个交易所所有的套利信息综合
    class PairPositionDetail:

        def __init__(self):
            self.pair = ""
            self.pos1_trading_direction = None
            self.entry_diff = 0
            self.price_diff = 0
            self.funding_diff = 0
            self.pos_notional_abs = 0
            self.adl = 0

            self.total_funding_fee = 0
            self.total_profit_apy = 0
            self.profit_year = 0
            self.position_detail_desc = ""

        @property
        def entry_spread_profit_rate(self):
            return self.entry_diff if self.pos1_trading_direction == TradeDirection.short else -self.entry_diff

        @property
        def close_spread_profit_rate(self):
            return self.price_diff if self.pos1_trading_direction == TradeDirection.long else -self.price_diff

        @property
        def funding_profit_rate(self):
            return self.funding_diff / 2 if self.pos1_trading_direction == TradeDirection.short else -self.funding_diff / 2

        @property
        def funding_spot_profit_rate(self):
            return self.funding_diff if self.pos1_trading_direction == TradeDirection.short else -self.funding_diff

        def __str__(self):
            return self.position_detail_desc

    def __init__(self):
        self.default_safe_leverage = 4.5
        self.usdt_borrow_interest_rate = 0
        self.margin1 = 0
        self.margin2 = 0
        self.aave_balance = 0
        self.morpho_oracle_str = ""
        self.margin_ratio1 = 0
        self.margin_ratio2 = 0
        self.target_leverage = 10
        self.pending_deposit_usd1 = 0
        self.pending_deposit_usd2 = 0
        self.target_margin_ratio = 0.6
        self.target_position_ratio = 0.95
        self.exchange1_available_balance = 0
        self.exchange1_spot_withdraw_balance = 0
        self.exchange2_available_balance = 0
        self.pair_positions = []
        self.exchange1_positions = []
        self.exchange2_positions = []
        self.pair_position_details = []
        self.chance_descs = []
        self.trading_pairs = []
        self.time_cost = 0

    @property
    def actual_leverage1(self):
        total_notional = sum([abs(pos1.notional) for pos1 in self.exchange1_positions])
        return total_notional / self.margin1 if self.margin1 != 0 else 0

    @property
    def actual_leverage2(self):
        total_notional = sum([abs(pos.notional) for pos in self.exchange2_positions])
        return total_notional / self.margin2 if self.margin2 != 0 else 0

    def get_pair_pos_by_symbol(self, symbol):
        for pos1, pos2 in self.pair_positions:
            if pos1.symbol == symbol:
                return pos1, pos2
        return None, None

    def get_pos_cnt(self, min_notional=0):
        return sum([1 for pos, _ in self.pair_positions if abs(pos.notional) > min_notional])

    @property
    def hold_pos_cnt(self):
        return len(self.pair_positions)

    @property
    def interest_cost_year(self):
        if self.usdt_borrow_interest_rate > 0 > self.exchange1_available_balance:
            interest_cost_year = self.exchange1_available_balance * self.usdt_borrow_interest_rate
        else:
            interest_cost_year = 0
        return interest_cost_year

    @property
    def total_profit_year(self):
        return sum([pos.profit_year for pos in self.pair_position_details]) + self.interest_cost_year

    @property
    def total_funding_fee(self):
        return sum([pos.total_funding_fee for pos in self.pair_position_details])

    @property
    def total_notional(self):
        return self.total_notional1

    @property
    def total_notional1(self):
        total_notional = 0
        for pos1, _ in self.pair_positions:
            total_notional += abs(pos1.notional)
        return total_notional

    @property
    def total_notional2(self):
        total_notional = 0
        for _, pos2 in self.pair_positions:
            total_notional += abs(pos2.notional)
        return total_notional

    @property
    def total_leverage(self):
        return (abs(self.total_notional1) + abs(self.total_notional2)) / self.total_margin \
            if self.total_margin != 0 else 0

    @property
    def leverage1(self):
        return self.total_notional1 / self.margin1 if self.margin1 != 0 else 0

    @property
    def balance_leverage1(self):
        return self.total_notional1 / (self.margin1 + self.pending_deposit_usd1) if self.margin1 != 0 else 0

    @property
    def leverage2(self):
        return self.total_notional2 / self.margin2 if self.margin2 != 0 else 0

    @property
    def balance_leverage2(self):
        return self.total_notional2 / (self.margin2 + self.pending_deposit_usd2) if self.margin2 != 0 else 0

    @property
    def total_margin(self):
        return self.margin1 + self.margin2

    @property
    def arbitrage_total_fund(self):
        return self.total_margin + self.aave_balance + self.pending_deposit_usd1 + self.pending_deposit_usd2

    @property
    def margin_balance_diff(self):
        return (self.margin1 - self.margin2) / 2

    @property
    def profit_rate_month(self):
        return self.total_profit_year / self.total_margin / 12 if self.total_margin != 0 else 0

    @property
    def position_ratio1(self):
        return 1 - self.exchange1_available_balance / self.margin1 if self.margin1 != 0 else 0

    @property
    def position_ratio2(self):
        return 1 - self.exchange2_available_balance / self.margin2 if self.margin2 != 0 else 0

    #
    # @property
    # def quiet_client_info(self):
    #     quiet_text = f"📌 策略A\n"
    #     prefix_img = "✅" if self.total_leverage < self.target_leverage else "⚠️"
    #     quiet_text += f"{prefix_img} 杠杆率: *{self.total_leverage:.2f}* \n"
    #     prefix_img = NotifyImgGenerator.get_expected_month_profit_rate_img(self.profit_rate_month)
    #     quiet_text += f"{prefix_img} 预期年化: *{self.profit_rate_month * 12:.2%}* \n"
    #     quiet_text += f"🔨 持有仓位: {', '.join([p[0].symbol.replace('USDT', '') for p in self.pair_positions])}\n"
    #     quiet_text += "🏷️" * 3 + get_datetime_now_str() + "🏷️" * 3 + "\n"
    #     return quiet_text

    @property
    def require_margin_release(self):
        if (self.margin_balance_diff > 0 and self.exchange1_available_balance < self.margin_balance_diff * 0.8) \
                or (self.margin_balance_diff < 0 and self.exchange2_available_balance < abs(
            self.margin_balance_diff) * 0.8):
            return True
        return False

    @property
    def is_account_very_safe(self):
        return self.leverage1 < self.default_safe_leverage \
            and self.leverage2 < self.default_safe_leverage \
            and self.total_leverage < self.default_safe_leverage

    def __str__(self):
        quiet_text = ""
        aave_text = f"(AAVE:${self.aave_balance:.2f})" if self.aave_balance > 0 else ""
        quiet_text += f"📌 ${self.arbitrage_total_fund:.2f}{aave_text}|${self.total_funding_fee:.2f}\n"
        if self.actual_leverage1 != self.leverage1 or self.actual_leverage2 != self.leverage2:
            quiet_text += f"⚠️ Actual: *{self.actual_leverage1:.2f} / {self.actual_leverage2:.2f}*\n"
        prefix_img = "✅" if self.total_leverage < self.target_leverage else "⚠️"
        leverage1_desc = ""
        if self.pending_deposit_usd1 > 1:
            leverage1_desc = f"({self.balance_leverage1:.2f})"
        leverage2_desc = ""
        if self.pending_deposit_usd2 > 1:
            leverage2_desc = f"({self.balance_leverage2:.2f})"
        quiet_text += f"{prefix_img} Leverage: *{self.leverage1:.2f}{leverage1_desc} / {self.leverage2:.2f}{leverage2_desc} / {self.total_leverage:.2f}* " \
                      f"(${self.margin_balance_diff:.2f})\n"
        prefix_img = "✅" if self.margin_ratio1 < self.target_margin_ratio and self.margin_ratio2 < self.target_margin_ratio else "⚠️"
        quiet_text += f"{prefix_img} Margin Ratio: *{self.margin_ratio1:.2%} / {self.margin_ratio2:.2%}*\n"

        prefix_img = "✅" if self.position_ratio1 < self.target_position_ratio and self.position_ratio2 < self.target_position_ratio else "⚠️"
        quiet_text += f"{prefix_img} Pos Ratio: *{self.position_ratio1:.2%} / {self.position_ratio2:.2%}*\n"

        prefix_img = NotifyImgGenerator.get_expected_month_profit_rate_img(self.profit_rate_month)
        quiet_text += f"{prefix_img} ROI: *{self.profit_rate_month:.2%}* (${(self.total_profit_year / 12):.2f})\n"
        quiet_text += f"🐳 POS({self.get_pos_cnt(min_notional=10000)}|{len(self.pair_positions)}): ${self.total_notional1:,.2f} / ${self.total_notional2:,.2f}\n"
        if self.chance_descs:
            quiet_text += "\n".join(self.chance_descs) + "\n"
        quiet_text += "\n".join([str(p) for p in self.pair_position_details]) + "\n"

        if (
                not self.is_account_very_safe) and self.margin_balance_diff > 0 and self.exchange1_available_balance < self.margin_balance_diff \
                and self.total_leverage > 1:
            quiet_text += f"❌ Exchange1 可用保证金: ${self.exchange1_available_balance:.2f} < {self.margin_balance_diff:.2f}, 需要释放\n"
        elif (
                not self.is_account_very_safe) and self.margin_balance_diff < 0 and self.exchange2_available_balance < abs(
            self.margin_balance_diff) \
                and self.total_leverage > 1:
            quiet_text += f"❌ Exchange2 可用保证金: ${self.exchange2_available_balance:.2f} < {-self.margin_balance_diff:.2f}, 需要释放\n"
        if self.trading_pairs:
            quiet_text += f"🔨 Trading: {','.join(self.trading_pairs)}\n"
        if self.morpho_oracle_str:
            quiet_text += f"{self.morpho_oracle_str}\n"
        quiet_text += "🏷️" * 3 + get_datetime_now_str() + f" {self.time_cost:.1f}s " + "🏷️" * 3 + "\n"
        return quiet_text

    def get_dex_spot_arb_str(self):
        quiet_text = ""
        aave_text = f"(AAVE:${self.aave_balance:.2f})" if self.aave_balance > 0 else ""
        quiet_text += f"📌 ${self.arbitrage_total_fund:.2f}{aave_text}|${self.total_funding_fee:.2f} \n"
        prefix_img = "✅" if self.total_leverage < self.target_leverage else "⚠️"
        leverage1_desc = ""
        if self.pending_deposit_usd1 > 1:
            leverage1_desc = f"({self.balance_leverage1:.2f})"
        leverage2_desc = ""
        if self.pending_deposit_usd2 > 1:
            leverage2_desc = f"({self.balance_leverage2:.2f})"
        quiet_text += f"{prefix_img} Leverage: *{self.leverage1:.2f}{leverage1_desc} / {self.leverage2:.2f}{leverage2_desc} / {self.total_leverage:.2f}* " \
                      f"(${self.margin_balance_diff:.2f})\n"

        prefix_img = "✅" if self.margin_ratio2 < self.target_margin_ratio else "⚠️"
        quiet_text += f"{prefix_img} Margin Ratio: *{self.margin_ratio2:.2%}*\n"

        prefix_img = "✅" if self.position_ratio2 < self.target_position_ratio else "⚠️"
        quiet_text += f"{prefix_img} Pos Ratio: *{self.position_ratio2:.2%}*\n"

        prefix_img = NotifyImgGenerator.get_expected_month_profit_rate_img(self.profit_rate_month)
        quiet_text += f"{prefix_img} ROI: *{self.profit_rate_month:.2%}* (${(self.total_profit_year / 12):.2f})\n"
        quiet_text += f"🐳 POS({len(self.pair_positions)}): ${self.total_notional1:,.2f} / ${self.total_notional2:,.2f}\n"
        if self.chance_descs:
            quiet_text += "\n".join(self.chance_descs) + "\n"
        quiet_text += "\n".join([str(p) for p in self.pair_position_details]) + "\n"
        if self.margin_balance_diff > 0 and self.exchange1_spot_withdraw_balance < self.margin_balance_diff:
            quiet_text += f"❌ SPOT可用保证金: ${self.exchange1_spot_withdraw_balance:.2f} < {self.margin_balance_diff:.2f}, 需要释放\n"
        elif self.margin_balance_diff < 0 and self.exchange2_available_balance < abs(self.margin_balance_diff):
            quiet_text += f"❌ FUTURE可用保证金: ${self.exchange2_available_balance:.2f} < {-self.margin_balance_diff:.2f}, 需要释放\n"
        if self.trading_pairs:
            quiet_text += f"🔨 Trading: {','.join(self.trading_pairs)}\n"
        if self.usdt_borrow_interest_rate > 0:
            quiet_text += f"🏦 USDT IR: {self.usdt_borrow_interest_rate:.2%}(${self.interest_cost_year / 12:.2f})\n"
        quiet_text += "🏷️" * 3 + get_datetime_now_str() + f" {self.time_cost:.1f}s " + "🏷️" * 3 + "\n"
        return quiet_text
