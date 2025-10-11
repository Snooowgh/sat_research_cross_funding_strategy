# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : arbitrage_info_show
@Description :
@Time        : 2025/9/22 08:18
"""
import time
from loguru import logger
import asyncio

from arbitrage_param import MultiExchangeArbitrageParam, HyperliquidLighterArbitrageParam, BinanceLighterArbitrageParam, \
    BinanceHyperliquidArbitrageParam
from cex_tools.cex_enum import TradeSide
from cex_tools.chance_searcher import ChanceSearcher
from cex_tools.exchange_model.base_model import TradeDirection
from cex_tools.exchange_model.cex_arbitrage_info_model import CexArbitrageInfoModel
from utils.coroutine_utils import safe_execute_async
from utils.notify_img_generator import NotifyImgGenerator
from utils.notify_tools import send_slack_message, CHANNEL_TYPE, notify_telegram, async_notify_telegram


async def close_order_and_balance_pos(exchange1, exchange2, enable_trade=True, limit_pair=None,
                                      reduce_only=True,
                                      min_diff_usd_value=30):
    """
    :param min_diff_usd_value:
    :param reduce_only:
    :param limit_pair:  限制操作的交易对
    :param exchange1:
    :param exchange2:
    :param enable_trade:
    :return:
    """
    # if exchange1.exchange_code == ExchangeEnum.BINANCE:
    #     logger.info("⚠️ Binance 跳过仓位检查...")
    #     return 0

    if limit_pair is None:
        logger.info("‼️ 关闭所有订单, 执行仓位检查...")
    else:
        logger.info(f"‼️ {limit_pair} 关闭订单, 执行仓位检查...")
    exchange1.cancel_all_orders(limit_pair)
    await exchange2.cancel_all_orders(limit_pair)
    time.sleep(0.1)
    pair_positions, _, _ = query_pair_positions(exchange1, exchange2)
    exec_desc = "减" if reduce_only else "加"
    trade_volume = 0
    for pos1, pos2 in pair_positions:
        if limit_pair is not None and pos1.pair != limit_pair:
            continue
        amt_diff = pos1.positionAmt + pos2.positionAmt
        diff_usd_value = abs(amt_diff) * pos1.entryPrice
        if diff_usd_value == 0:
            pass
        elif diff_usd_value < min_diff_usd_value:
            logger.info(
                f"✅ 仓位OK: {pos1.pair} ${diff_usd_value:.4f}")
            continue
        else:
            trade_amt = abs(amt_diff)
            if amt_diff > 0:
                # 做空
                side = TradeDirection.short
                other_side = TradeDirection.long
            else:
                # 做多
                side = TradeDirection.long
                other_side = TradeDirection.short
            use_exchange, other_exchange = (exchange1, exchange2) if (reduce_only and pos1.position_side != side) or (
                    not reduce_only and pos1.position_side == side) else (exchange2, exchange1)
            trade_amt = use_exchange.convert_size(pos2.pair,
                                                  size=trade_amt)
            if float(trade_amt) == 0:
                continue

            mid_price = exchange1.get_tick_price(pos1.pair)
            if pos1.adl == 5 or pos2.adl == 5:
                diff_usd_value_limit = 25000
            else:
                diff_usd_value_limit = 5000
            if enable_trade and diff_usd_value < diff_usd_value_limit:
                try:
                    use_exchange.make_new_order(pos2.pair,
                                                side,
                                                order_type="MARKET",
                                                quantity=trade_amt, price=mid_price)
                    text = f"⚠️ {pos1.pair}({use_exchange.exchange_code}), 自动执行{exec_desc}仓:  {amt_diff} ${diff_usd_value:.4f}"
                except Exception as e:
                    other_exchange.make_new_order(pos2.pair,
                                                  side,
                                                  order_type="MARKET",
                                                  quantity=trade_amt, price=mid_price)
                    other_exec_desc = "加" if reduce_only else "减"
                    text = f"⚠️⚠️ {pos1.pair}({other_exchange.exchange_code}), 自动执行{other_exec_desc}仓:  {amt_diff} ${diff_usd_value:.4f}"
                logger.warning(text)
                send_slack_message(text, channel_type=CHANNEL_TYPE.CEX_QUIET)
                trade_volume += abs(diff_usd_value)
            else:
                text = f"❌ {pos1.pair}({use_exchange.exchange_code}), 需要手动执行{exec_desc}仓: {amt_diff} ${diff_usd_value:.4f}"
                logger.warning(text)
                send_slack_message(text)
    return trade_volume


async def query_pair_positions(exchange1, exchange2, sort_by_notional=True):
    exchange1_positions = await safe_execute_async(exchange1.get_all_cur_positions)
    exchange2_positions = await safe_execute_async(exchange2.get_all_cur_positions)
    pair_positions = []
    for pos1 in exchange1_positions:
        for pos2 in exchange2_positions:
            if pos1.pair == pos2.pair:
                pair_positions.append((pos1, pos2))
                break
    if sort_by_notional:
        pair_positions.sort(key=lambda x: abs(x[0].notional), reverse=True)
    return pair_positions, exchange1_positions, exchange2_positions


async def get_arbitrage_info_model(arbitrage_param) -> CexArbitrageInfoModel:
    """
    获取双交易所套利信息（兼容旧版本）

    :param arbitrage_param: 包含 exchange1 和 exchange2 的套利参数
    :return: CexArbitrageInfoModel
    """
    arbitrage_info = CexArbitrageInfoModel()
    chance_searcher = ChanceSearcher(arbitrage_param.exchange1, arbitrage_param.exchange2)
    start = time.time()

    # 确保异步适配器已初始化
    if not hasattr(arbitrage_param, 'async_exchange1') or not arbitrage_param.async_exchange1:
        await arbitrage_param.init_async_exchanges()

    # 使用异步适配器获取账户信息
    arbitrage_info.exchange1_available_balance = await arbitrage_param.async_exchange1.get_available_margin()
    arbitrage_info.margin1 = await arbitrage_param.async_exchange1.get_total_margin()
    arbitrage_info.exchange2_available_balance = await arbitrage_param.async_exchange2.get_available_margin()
    arbitrage_info.margin2 = await arbitrage_param.async_exchange2.get_total_margin()

    arbitrage_info.pair_positions, \
        arbitrage_info.exchange1_positions, \
        arbitrage_info.exchange2_positions = await query_pair_positions(arbitrage_param.exchange1,
                                                                        arbitrage_param.exchange2)
    price_diff_map, chance_list = await chance_searcher.search_all_chances(3)

    # 检查仓位不匹配
    for pos1, pos2 in arbitrage_info.pair_positions:
        pos1.funding_rate = await arbitrage_param.async_exchange1.get_funding_rate(pos1.symbol)
        pos2.funding_rate = await arbitrage_param.async_exchange2.get_funding_rate(pos2.symbol)
        amt_diff = pos1.positionAmt + pos2.positionAmt
        diff_usd_value = abs(amt_diff) * pos1.entryPrice
        # if diff_usd_value > 30:
        #     await async_notify_telegram(
        #         f"❌❌❌ {arbitrage_param.exchange1.exchange_code}-{arbitrage_param.exchange2.exchange_code} "
        #         f"{pos1.pair} 仓位金额不匹配: {amt_diff}(${diff_usd_value:.4f})",
        #         channel_type=CHANNEL_TYPE.TRADE)
        if diff_usd_value > 30:
            logger.warning(f"⚠️ {arbitrage_param.exchange1.exchange_code}-{arbitrage_param.exchange2.exchange_code} "
                           f"{pos1.pair} 仓位金额不匹配: {amt_diff}(${diff_usd_value:.4f})")

    # 检查高额费率预警
    for pos1, pos2 in arbitrage_info.pair_positions:
        if (pos2.funding_rate > 1 and pos2.position_side == TradeSide.BUY) or \
                (pos2.funding_rate < -1 and pos2.position_side == TradeSide.SELL):
            await async_notify_telegram(
                f"⚠️ {arbitrage_param.exchange2.exchange_code} {pos2.symbol} {pos2.position_side} 高额费率预警: "
                f"{pos2.funding_rate:.2%} 预计亏损 ${pos2.funding_rate * abs(pos2.notional) / 365 / 24}/h")

        # 构建仓位详情
        pair_pos_detail = CexArbitrageInfoModel.PairPositionDetail()
        pair_pos_detail.pair = pos1.symbol
        pair_pos_detail.pos1_trading_direction = pos1.position_side
        pair_pos_detail.pos_notional_abs = abs(pos1.notional)
        price_diff = price_diff_map.get(pos1.symbol.replace("USDT", ""))
        pair_pos_detail.price_diff = price_diff
        pair_pos_detail.funding_diff = pos1.funding_rate - pos2.funding_rate
        pair_pos_detail.adl = max(pos1.adl, pos2.adl)
        close_prefix_img = NotifyImgGenerator.get_pre_img_by_side_and_price_diff_level(pos1.position_side,
                                                                                       price_diff)
        pair_pos_detail.total_funding_fee = pos1.fundingFee + pos2.fundingFee
        pair_pos_detail.profit_year = -(pos1.notional * pos1.funding_rate + pos2.notional * pos2.funding_rate)
        pair_pos_detail.total_profit_apy = -((pos1.notional * pos1.funding_rate / abs(pos1.notional) +
                                              pos2.notional * pos2.funding_rate / abs(pos2.notional)) / 2)
        prefix_img = "🟢" if pair_pos_detail.total_profit_apy > 0 else "🔴"
        pos1_img = "" if pos1.notional * pos1.funding_rate < 0 else "^"
        pos2_img = "" if pos2.notional * pos2.funding_rate < 0 else "^"

        entry_diff = (pos1.entryPrice - pos2.entryPrice) / pos2.entryPrice
        pair_pos_detail.entry_diff = entry_diff
        pos_adl_notify = "" if pair_pos_detail.adl <= 1 else f"({pair_pos_detail.adl})"
        pair_pos_detail.position_detail_desc += f"{prefix_img}{close_prefix_img} {pos1.symbol.replace('USDT', '')}{pos_adl_notify} " \
                                                f"*{pair_pos_detail.total_profit_apy:.2%}* ({pos1_img}{pos1.funding_rate:.2%}/{pos2_img}{pos2.funding_rate:.2%}) / *{price_diff:.2%}* ({entry_diff:.2%}|{pair_pos_detail.total_funding_fee / arbitrage_info.arbitrage_total_fund:.2%})"
        arbitrage_info.pair_position_details.append(pair_pos_detail)

    # 添加机会列表
    holding_pairs = [pos1.symbol for pos1, _ in arbitrage_info.pair_positions]
    for chance in chance_list:
        if chance.name in holding_pairs:
            continue
        max_profit_rate = abs(chance.funding1 - chance.funding2)
        prefix_img = NotifyImgGenerator.get_expected_month_profit_rate_img(max_profit_rate / 2 / 12 * 8)
        chance_desc = f"➡️{prefix_img} {chance.name.replace('USDT', '')} {max_profit_rate / 2:.2%}({chance.funding1:.2%}/{chance.funding2:.2%}) / {chance.diff:.2%}"
        arbitrage_info.chance_descs.append(chance_desc)

    arbitrage_info.time_cost = time.time() - start
    return arbitrage_info


async def arbitrage_info_show(arbitrage_param=None):
    if arbitrage_param is None:
        # arbitrage_param = HyperliquidLighterArbitrageParam()
        # arbitrage_param = BinanceLighterArbitrageParam()
        # await arbitrage_param.exchange2.init()
        arbitrage_param = MultiExchangeArbitrageParam()
    if hasattr(arbitrage_param.exchange2, 'init'):
        await arbitrage_param.exchange2.init()
    arbitrage_info = None
    try:
        arbitrage_info = await get_arbitrage_info_model(arbitrage_param)
        if arbitrage_info.actual_leverage1 > arbitrage_param.danger_leverage:
            notify_telegram(
                f"❌❌❌ {arbitrage_param.exchange1.exchange_code} {arbitrage_info.actual_leverage1:.2f}倍杠 🚨🚨🚨",
                channel_type=CHANNEL_TYPE.TRADE)
        if arbitrage_info.actual_leverage2 > arbitrage_param.danger_leverage:
            notify_telegram(
                f"❌❌❌ {arbitrage_param.exchange2.exchange_code} {arbitrage_info.actual_leverage2:.2f}倍杠 🚨🚨🚨",
                channel_type=CHANNEL_TYPE.TRADE)
        quiet_text = str(arbitrage_info)
    except Exception as e:
        logger.exception(e)
        quiet_text = f"⚠️ ⚠️ darwin_light数据获取出错: {str(e)}"
    extra_info = ""
    quiet_text = f"1️⃣ {arbitrage_param.exchange1.exchange_code}-{arbitrage_param.exchange2.exchange_code}\n" + extra_info + quiet_text
    print(quiet_text)
    # await arbitrage_param.exchange2.close()
    return quiet_text


def notify_arbitrage_info():
    info1 = asyncio.run(arbitrage_info_show(BinanceHyperliquidArbitrageParam()))
    notify_telegram(info1, channel_type=CHANNEL_TYPE.CEX_QUIET)
    info2 = asyncio.run(arbitrage_info_show(BinanceLighterArbitrageParam()))
    notify_telegram(info2, channel_type=CHANNEL_TYPE.CEX_QUIET)


if __name__ == '__main__':
    # print(get_notify_extra_info())
    notify_arbitrage_info()
