# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : notify_img_generator
@Description :
@Time        : 2024/9/9 15:51
"""
from cex_tools.exchange_model.base_model import TradeDirection


class NotifyImgGenerator:

    @staticmethod
    def get_pre_img_by_sign(num):
        if num > 0:
            return "🟢"
        else:
            return "🔴"

    @staticmethod
    def get_pre_img_by_side_and_price_diff(side1, price_diff):
        if side1 == TradeDirection.short:
            if price_diff > 0:
                return "🟢"
            else:
                return "🔴"
        else:
            if price_diff > 0:
                return "🔴"
            else:
                return "🟢"

    @staticmethod
    def get_pre_img_by_side_and_price_diff_level(side1, price_diff):
        if side1 == TradeDirection.short:
            if price_diff > 0.0015:
                return "🚀"
            elif price_diff > 0:
                return "🟢"
            else:
                return "🔴"
        else:
            if price_diff > 0:
                return "🔴"
            elif price_diff < -0.0015:
                return "🚀"
            else:
                return "🟢"

    @staticmethod
    def get_pre_img_by_skew(skew):
        compare_skew = abs(skew)
        if compare_skew > 100_0000:
            return "🚨 "
        elif compare_skew > 50_0000:
            return "🚀 "
        elif compare_skew > 10_0000:
            return "🚧 "
        else:
            return ""

    @staticmethod
    def get_pos_status_img(cex_position_side, funding_rate_diff, currentFundingVelocity):
        prefix_img = ""
        if cex_position_side == TradeDirection.long:
            profit_rate = funding_rate_diff
            profit_rate_velocity = currentFundingVelocity
        else:
            profit_rate = -funding_rate_diff
            profit_rate_velocity = -currentFundingVelocity
        if profit_rate > 0:
            prefix_img += "🟢"
        else:
            prefix_img += "🔴"
        if profit_rate_velocity > 0:
            prefix_img += " 🟢"
        else:
            prefix_img += " 🔴"
        return prefix_img

    @staticmethod
    def get_expected_month_profit_rate_img(funding_rate):
        if funding_rate > 0.3:
            prefix_img = "🏆"
        elif funding_rate > 0.2:
            prefix_img = "🔥"
        elif funding_rate > 0.1:
            prefix_img = "🚀"
        elif funding_rate > 0.05:
            prefix_img = "✈️"
        elif funding_rate > 0:
            prefix_img = "😭"
        else:
            prefix_img = "❌"
        return prefix_img

    @staticmethod
    def get_spot_profit_rate_img(funding_rate):
        if funding_rate > 0.3:
            prefix_img = "🏆"
        elif funding_rate > 0.2:
            prefix_img = "🔥"
        elif funding_rate > 0.1:
            prefix_img = "🚀"
        elif funding_rate > 0.05:
            prefix_img = "✈️"
        elif funding_rate > 0:
            prefix_img = "👌"
        elif funding_rate > -0.05:
            prefix_img = "😭"
        elif funding_rate > -0.1:
            prefix_img = "😱"
        else:
            prefix_img = "❌"
        return prefix_img

    @staticmethod
    def get_spread_profit_rate_img(spread_profit_rate):
        if spread_profit_rate > 0.003:
            prefix_img = "🏆"
        elif spread_profit_rate > 0.002:
            prefix_img = "🔥"
        elif spread_profit_rate > 0.0015:
            prefix_img = "🚀"
        elif spread_profit_rate > 0.001:
            prefix_img = "✈️"
        elif spread_profit_rate > 0.0005:
            prefix_img = "😭"
        else:
            prefix_img = "❌"
        return prefix_img

    @staticmethod
    def get_susd_price_img(susd_price):
        prefix_img = "🎵 🎵"
        if susd_price < 0.95:
            prefix_img = "☠️ ☠️"
        elif susd_price < 0.99:
            prefix_img = "❌ ❌"
        elif susd_price < 0.995:
            prefix_img = "⚠️ ⚠️"
        elif susd_price < 1.005:
            prefix_img = "🎵 🎵"
        elif susd_price < 1.05:
            prefix_img = "🎉 🎉"
        return prefix_img

    @staticmethod
    def get_gas_price_img(cur_gas_price):
        gas_price_img = ""
        if cur_gas_price > 1:
            gas_price_img = "☠️ "
        elif cur_gas_price > 0.1:
            gas_price_img = "❌ "
        elif cur_gas_price > 0.01:
            gas_price_img = "⚠️ "
        return gas_price_img
