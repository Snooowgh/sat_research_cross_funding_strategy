# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : simple_pair_position_builder
@Description :
@Time        : 2025/9/22 08:17
"""
import random
import time
import asyncio
from dataclasses import dataclass
from loguru import logger

from arbitrage_info_show import query_pair_positions, arbitrage_info_show
from arbitrage_param import MultiExchangeArbitrageParam
from rich.prompt import Prompt, Confirm, FloatPrompt
from cex_tools.cex_enum import TradeSide, ExchangeEnum
from cex_tools.chance_searcher import ChanceSearcher
from cex_tools.async_funding_spread_searcher import AsyncFundingSpreadSearcher, SearchConfig
from cex_tools.async_exchange_adapter import AsyncExchangeFactory
from cex_tools.hedge_spread_analyzer import HedgeSpreadAnalyzer
from utils.coroutine_utils import safe_execute_async
from utils.notify_tools import async_notify_telegram, CHANNEL_TYPE
from cex_tools.exchange_ws.binance_ws_direct import BinanceOrderBookStreamDirect
from cex_tools.exchange_ws.lighter_orderbook_stream import LighterOrderBookStreamAsync
from cex_tools.exchange_ws.aster_orderbook_stream import AsterOrderBookStreamAsync
from cex_tools.exchange_ws.hyperliquid_orderbook_stream import HyperliquidOrderBookStream
from cex_tools.exchange_ws.okx_orderbook_stream import OkxOrderBookStreamAsync
from cex_tools.exchange_ws.bybit_orderbook_stream import BybitOrderBookStreamAsync
from logic.realtime_hedge_engine import RealtimeHedgeEngine, TradeConfig, RiskConfig, TradeMode
from utils.notify_img_generator import NotifyImgGenerator


def create_orderbook_stream(exchange):
    """
    工厂方法：根据交易所类型创建对应的订单簿流

    :param exchange: 交易所对象
    :return: OrderBookStream实例，如果不支持则返回None
    """
    exchange_code = exchange.exchange_code

    if exchange_code == ExchangeEnum.BINANCE:
        return BinanceOrderBookStreamDirect()
    elif exchange_code == ExchangeEnum.ASTER:
        return AsterOrderBookStreamAsync()
    elif exchange_code == ExchangeEnum.HYPERLIQUID:
        return HyperliquidOrderBookStream(testnet=False)
    elif exchange_code == ExchangeEnum.LIGHTER:
        return LighterOrderBookStreamAsync()
    elif exchange_code == ExchangeEnum.OKX:
        return OkxOrderBookStreamAsync()
    elif exchange_code == ExchangeEnum.BYBIT:
        return BybitOrderBookStreamAsync(testnet=False)
    else:
        return None


@dataclass
class TradeParams:
    """交易参数封装"""
    arbitrage_param: MultiExchangeArbitrageParam
    exchange1_pair: str
    exchange2_pair: str
    exchange1_side: str
    exchange2_side: str
    single_amount_low: float
    single_amount_high: float
    single_amount_change: float
    total_amount: float
    min_profit_rate: float
    pair_price: float
    is_close_position: bool = False
    use_dynamic_amount: bool = True
    max_first_level_ratio: float = 0.5


def generate_random_amount(single_amount_low, single_amount_high, single_amount_change):
    """
    根据最小数量、最大数量和数量单位变化，随机生成一个数量。

    参数:
    single_amount_low: 最小数量（包含）
    single_amount_high: 最大数量（包含）
    single_amount_change: 数量单位变化（步长）

    返回:
    一个在 [single_amount_low, single_amount_high] 范围内且为 single_amount_change 整数倍的随机数。
    """
    # 计算可能取值的数量
    number_of_possible_values = int((single_amount_high - single_amount_low) / single_amount_change) + 1
    # 生成一个随机索引，表示第几个倍数
    random_index = random.randint(0, number_of_possible_values - 1)
    # 计算并返回最终的随机数量
    random_amount = single_amount_low + random_index * single_amount_change
    return random_amount


async def simple_pair_position_builder_cli(arbitrage_param=None):
    """💎 简单构建配对仓位"""
    # ========== 第一步：初始化套利参数和展示信息 ==========
    await arbitrage_info_show(arbitrage_param)

    # 如果未提供套利参数，则使用默认的 Binance-Lighter 套利参数
    if arbitrage_param is None:
        # arbitrage_param = HyperliquidLighterArbitrageParam()
        # arbitrage_param = BinanceLighterArbitrageParam()
        # await arbitrage_param.exchange2.init()
        arbitrage_param = MultiExchangeArbitrageParam()
        await arbitrage_param.lighter_exchange.init()
    # 创建异步交易所适配器
    async_exchange1 = AsyncExchangeFactory.create_async_exchange(arbitrage_param.exchange1, arbitrage_param.exchange1.exchange_code)
    async_exchange2 = AsyncExchangeFactory.create_async_exchange(arbitrage_param.exchange2, arbitrage_param.exchange2.exchange_code)

    # 创建新的异步费率价差搜索器
    search_config = SearchConfig(
        min_funding_diff=0.08,  # 8%年化最小费率差
        max_opportunities=20,
        include_spread_analysis=True,
        spread_analysis_interval="1m",  # K线间隔
        spread_analysis_limit=1000,  # K线数量
        use_white_list=True
    )
    funding_spread_searcher = AsyncFundingSpreadSearcher(async_exchange1, async_exchange2, search_config)

    # 创建传统机会搜索器作为备用
    chance_searcher = ChanceSearcher(arbitrage_param.exchange1, arbitrage_param.exchange2)
    price_diff_map, _, _ = await chance_searcher.get_all_market_price_diff_map()

    # 显示警告和使用提示
    print("⚠️⚠️⚠️ 存在手动交易时，必须重启程序使用")
    print("🔥 找机会参考 https://app.lighter.xyz/quant")
    use_exchange_pair = f"{arbitrage_param.exchange1.exchange_code}-{arbitrage_param.exchange2.exchange_code}"
    print(f"使用交易所: {use_exchange_pair}")

    # ========== 第二步：选择交易对 ==========
    while True:
        default_pair = ""
        show_chance = Confirm.ask(f"展示交易机会?", default=True)
        chance_show_limit = 20
        if show_chance:
            # 询问使用传统搜索器还是新的费率价差搜索器
            use_new_searcher = Confirm.ask("使用新的费率价差搜索器? (提供费率差和价差统计)", default=True)
            use_white_list = Confirm.ask("使用白名单?", default=False)
            funding_spread_searcher.config.use_white_list = use_white_list
            if use_new_searcher:
                print(f"\n🔍 使用新的异步费率价差搜索器搜索机会...")
                try:
                    # 使用新的搜索器
                    funding_spread_searcher.config.max_opportunities = chance_show_limit
                    funding_opportunities = await funding_spread_searcher.search_opportunities()

                    # 限制展示数量
                    funding_opportunities = funding_opportunities[:chance_show_limit]

                    # 打印机会表格
                    await funding_spread_searcher.print_opportunities_table(funding_opportunities)

                    # 获取第一个推荐的交易对
                    default_pair = funding_opportunities[0].pair if funding_opportunities else ""

                except Exception as e:
                    logger.exception(f"新搜索器执行失败: {e}")
                    print(f"⚠️ 新搜索器失败，回退到传统搜索器")
                    use_new_searcher = False

            if not use_new_searcher:
                # 使用传统搜索器
                use_repeat = False  # 是否深度搜索
                opportunities = await chance_searcher.print_arbitrage_opportunities(chance_show_limit,
                                                                                    use_repeat=use_repeat)
                default_pair = opportunities[0].pair if opportunities else ""

        # 输入要操作的交易对
        exchange1_pair = Prompt.ask("输入操作交易对", default=default_pair).upper()
        if not exchange1_pair:
            continue

        # 验证交易对是否存在于价格差异映射中
        if exchange1_pair.replace("USDT", "") not in price_diff_map:
            print(f"{exchange1_pair} 不存在")
            continue

        # 自动添加 USDT 后缀（如果不是 USDT 或 USDC 结尾）
        if not exchange1_pair.endswith("USDT") and not exchange1_pair.endswith("USDC"):
            exchange1_pair += "USDT"
        break

    # ========== 第三步：获取交易对相关信息 ==========
    # 将交易所2的交易对统一为 USDT（Lighter 只支持 USDT）
    exchange2_pair = exchange1_pair.replace("USDC", "USDT")

    # 查询当前配对仓位
    pair_positions, _, _ = await query_pair_positions(arbitrage_param.exchange1, arbitrage_param.exchange2)
    pair_pos = list(filter(lambda x: x[0].pair == exchange1_pair, pair_positions))

    # 获取当前价格和资金费率
    pair_price = arbitrage_param.exchange1.get_tick_price(exchange1_pair)
    funding_rate1 = arbitrage_param.exchange1.get_funding_rate(exchange1_pair)
    funding_rate2 = await safe_execute_async(arbitrage_param.exchange2.get_funding_rate, exchange2_pair)
    funding_diff = funding_rate1 - funding_rate2
    print(f"💰 费率数据: {funding_rate1:.2%} {funding_rate2:.2%}")
    print(f"🏦 {exchange1_pair} 价格: {pair_price:.2f} 价差: {price_diff_map.get(exchange1_pair.replace('USDT', '')):.2%}")

    # 采样3次价格差异，计算平均价差
    # avg_diff = 0
    # for _ in range(4):
    #     price1 = arbitrage_param.exchange1.get_tick_price(exchange1_pair)
    #     price2 = await safe_execute_async(arbitrage_param.exchange2.get_tick_price, exchange2_pair)
    #     diff = (price1 - price2) / price2
    #     avg_diff += diff
    #     print(f"价格差: {price1} / {price2} / {diff:.4%}")
    #     time.sleep(0.5)

    analyser = HedgeSpreadAnalyzer(arbitrage_param.async_exchange1, arbitrage_param.async_exchange2)
    stats = await analyser.analyze_spread(exchange1_pair.replace("USDT", ""),
                                            interval="1m", limit=1000)
    print(stats)
    # 判断仓位情况，提前询问用户操作类型
    pos1 = None
    pos2 = None
    is_close_position = False  # 是否为减仓操作
    exchange1_side = None  # 默认交易方向

    if pair_pos:
        pos1, pos2 = pair_pos[0]
        print(f"\n{'='*60}")
        print(f"📊 检测到现有仓位：")
        print(f"   {arbitrage_param.exchange1.exchange_code}: {pos1.positionAmt:.4f} (${pos1.notional:.2f}) {pos1.position_side}")
        print(f"   {arbitrage_param.exchange2.exchange_code}: {pos2.positionAmt:.4f} (${pos2.notional:.2f}) {pos2.position_side}")
        print(f"{'='*60}\n")

        # 提前询问用户操作意图
        operation_type = Confirm.ask(
            "是否执行加仓操作",
            default=(abs(funding_rate1-funding_rate2) / 2 > 0.1)
        )

        if not operation_type:
            is_close_position = True
            print(f"🔻 减仓模式：将平掉当前所有仓位")
            # 减仓时，交易方向与当前仓位相反
            exchange1_side = TradeSide.SELL if pos1.position_side == TradeSide.BUY else TradeSide.BUY
            print(f"   减仓方向: {arbitrage_param.exchange1.exchange_code} {exchange1_side}")
        else:
            print(f"🔺 加仓模式：继续增加当前仓位")
            # 加仓时，默认使用原仓位方向
            exchange1_side = pos1.position_side
            print(f"   加仓方向: {arbitrage_param.exchange1.exchange_code} {exchange1_side}")

    # ========== 第四步：配置交易参数 ==========
    if is_close_position:
        # 减仓模式：使用现有仓位数量作为总数量
        total_amount = FloatPrompt.ask("减仓总数量", default=float(abs(pos1.positionAmt)))
        print(f"🔻 减仓总数量: {total_amount:.4f} (${total_amount * pair_price:.2f})")

        # 配置单笔减仓数量范围
        print(f"📚 500u数量为: {500 / pair_price:.4f}")
        single_amount_low = FloatPrompt.ask("单笔减仓数量最小", default=float(int(500 / pair_price)))
        single_amount_high = FloatPrompt.ask("单笔减仓数量最多", default=single_amount_low * 2)
        single_amount_change = FloatPrompt.ask("单笔减仓数量变化量", default=1.0)
        print(f"🚀 单笔减仓金额: ${single_amount_low * pair_price:.2f} ~ ${single_amount_high * pair_price:.2f}")
    else:
        # 开仓/加仓模式：用户输入开仓总数量
        print(f"5wu数量为: {50000 / pair_price:.4f} (2~5wu才开始有分)")
        total_amount = FloatPrompt.ask("开仓总数量", default=float(int(50000 / pair_price)))
        print(f"🚀 开仓金额: ${total_amount * pair_price:.2f}")

        # 配置单笔下单数量范围
        print(f"📚 500u数量为: {500 / pair_price:.4f}")
        single_amount_low = FloatPrompt.ask("单笔下单数量最小", default=float(int(500 / pair_price)))
        single_amount_high = FloatPrompt.ask("单笔下单数量最多", default=single_amount_low * 2)
        single_amount_change = FloatPrompt.ask("单笔下单数量变化量", default=1.0)
        print(f"🚀 单笔开仓金额: ${single_amount_low * pair_price:.2f} ~ ${single_amount_high * pair_price:.2f}")

    # 配置最小价差收益率
    if funding_diff < 0:
        spread_profit_rate = -stats.mean_spread
    else:
        spread_profit_rate = stats.mean_spread
    print(f"参考价差收益率: {spread_profit_rate:.2%}")
    min_profit_rate = FloatPrompt.ask("最小价差收益率", default=spread_profit_rate)

    # 如果是开仓操作，则需要询问交易方向
    if exchange1_side is None:
        exchange1_side = Prompt.ask(f"{arbitrage_param.exchange1.exchange_code} 执行做多/做空",
                                    choices=[TradeSide.BUY, TradeSide.SELL], default=TradeSide.BUY)

        # 判断是加仓还是开仓
        if pos1:
            if pos1.position_side == exchange1_side:
                print(f"⚠️⚠️⚠️ {arbitrage_param.exchange1.exchange_code} {exchange1_pair} 加仓")
            else:
                print(f"⚠️⚠️⚠️ {arbitrage_param.exchange1.exchange_code} {exchange1_pair} 减仓")
        else:
            print(f"🚀 开仓 {arbitrage_param.exchange1.exchange_code} {exchange1_pair} {exchange1_side} "
                  f"{total_amount}(${total_amount * pair_price:.2f})")

    # 交易所2的交易方向与交易所1相反（对冲）
    exchange2_side = TradeSide.SELL if exchange1_side == TradeSide.BUY else TradeSide.BUY

    # ========== 第五步：选择执行方式 ==========
    print(f"\n{'='*60}")
    print("📋 选择交易执行方式:")
    print("  1. WebSocket实时对冲引擎 (推荐，延迟更低)")
    print("  2. 传统轮询模式 (原有方式)")
    print(f"{'='*60}\n")

    use_realtime_engine = Confirm.ask("使用WebSocket实时对冲引擎?", default=True)

    # 如果使用实时引擎，询问是否配置超时时间
    no_trade_timeout_sec = 0
    use_dynamic_amount = False
    max_first_level_ratio = 1
    if use_realtime_engine:
        # 询问是否启用动态数量调整
        use_dynamic_amount = Confirm.ask("是否根据订单簿动态调整下单数量?", default=True)
        max_first_level_ratio = 0.5
        if use_dynamic_amount:
            max_first_level_ratio = FloatPrompt.ask("最大吃掉第一档流动性比例 (0.0-1.0)", default=0.8)
            print(f"✅ 启用动态调整，最多吃掉第一档 {max_first_level_ratio:.1%}")
        else:
            print(f"⚠️ 禁用动态调整，将使用纯随机数量")

        enable_timeout = Confirm.ask("是否启用无交易自动关闭? (节省资源)", default=True)
        if enable_timeout:
            no_trade_timeout_sec = FloatPrompt.ask("无交易自动关闭超时时间(秒)", default=100.0)
            print(f"⏱️  启用超时: {no_trade_timeout_sec}秒无交易将自动停止引擎")
        else:
            print(f"♾️  超时已禁用，引擎将持续运行直到完成所有交易")

    # ========== 第六步：确认执行 ==========
    operation_desc = "减仓" if is_close_position else "开仓"
    confirm_exec = Confirm.ask(f"执行 {exchange1_pair} {operation_desc} {exchange1_side} 数量 {total_amount:.4f}", default=True)
    if not confirm_exec:
        return

    # 封装交易参数
    trade_params = TradeParams(
        arbitrage_param=arbitrage_param,
        exchange1_pair=exchange1_pair,
        exchange2_pair=exchange2_pair,
        exchange1_side=exchange1_side,
        exchange2_side=exchange2_side,
        single_amount_low=single_amount_low,
        single_amount_high=single_amount_high,
        single_amount_change=single_amount_change,
        total_amount=total_amount,
        min_profit_rate=min_profit_rate,
        pair_price=pair_price,
        is_close_position=is_close_position,
        use_dynamic_amount=use_dynamic_amount,
        max_first_level_ratio=max_first_level_ratio
    )

    if use_realtime_engine:
        # ========== 第七步A：使用实时对冲引擎执行交易 ==========
        await execute_with_realtime_engine(trade_params, no_trade_timeout_sec)
    else:
        # ========== 第七步B：使用传统轮询模式执行交易 ==========
        await execute_with_traditional_mode(trade_params)


async def verify_and_notify_positions(trade_params: TradeParams, stats: dict):
    """验证仓位一致性并发送通知"""
    if stats['cum_volume'] <= 0:
        print("❌ 未执行任何交易，跳过仓位检查")
        return
    # 重新查询配对仓位
    pair_positions, _, _ = await query_pair_positions(
        trade_params.arbitrage_param.exchange1,
        trade_params.arbitrage_param.exchange2
    )
    pair_pos = list(filter(lambda x: x[0].pair == trade_params.exchange1_pair, pair_positions))

    if not pair_pos:
        pos_inf = "👌 仓位已平仓"
    else:
        pos1, pos2 = pair_pos[0]
        # 计算两个交易所的入场价格差异
        entry_diff = (pos1.entryPrice - pos2.entryPrice) / pos2.entryPrice
        pos_inf = f"🐮 仓位价值: ${pos1.notional:.2f} 数量:{pos1.positionAmt} 价差: {entry_diff:.2%}"
        # 检查仓位数量是否一致（对冲仓位应该完全抵消）
        pos_amt_diff = pos1.positionAmt + pos2.positionAmt
        if abs(pos_amt_diff) > 0:
            warn_msg = (f"❌ ❌ ❌ {pos1.pair} 仓位数量不一致: {pos_amt_diff}"
                       f"(${pos_amt_diff * trade_params.pair_price:.2f}) "
                       f"({pos1.positionAmt}, {pos2.positionAmt})")
            print(warn_msg)
            await async_notify_telegram(warn_msg, channel_type=CHANNEL_TYPE.TRADE)
    print(pos_inf)
    # 发送交易完成通知
    use_exchange_pair = (f"{trade_params.arbitrage_param.exchange1.exchange_code}-"
                         f"{trade_params.arbitrage_param.exchange2.exchange_code}")
    await async_notify_telegram(
        f"🔖 {use_exchange_pair.upper()}\n🔥 {trade_params.exchange1_side} {trade_params.exchange1_pair} 交易结束, "
        f"累计金额:${stats['cum_volume']:.2f}(${stats['cum_profit']:.2f}, "
        f"{stats['cum_profit']/stats['cum_volume'] if stats['cum_volume'] > 0 else 0:.2%})\n{pos_inf}",
        channel_type=CHANNEL_TYPE.TRADE
    )


async def execute_with_traditional_mode(trade_params: TradeParams):
    """使用传统轮询模式执行交易"""
    print(f"\n{'='*60}")
    print(f"🔄 启动传统轮询模式...")
    print(f"{'='*60}\n")

    # 计算交易参数
    single_trade_volume_usd = trade_params.single_amount_high * trade_params.pair_price
    trade_interval_time_sec = 0.1

    # 初始化累计统计变量
    cum_volume = 0
    cum_profit = 0
    remaining_amount = trade_params.total_amount
    trade_count = 0
    min_profit_rate = trade_params.min_profit_rate

    # 执行交易循环
    while remaining_amount > 0:
        start = time.time()
        try:
            # 生成随机下单数量
            if trade_params.is_close_position:
                max_single = min(trade_params.single_amount_high, remaining_amount)
                min_single = min(trade_params.single_amount_low, remaining_amount)
                if remaining_amount < trade_params.single_amount_low:
                    single_amount = remaining_amount
                else:
                    single_amount = generate_random_amount(min_single, max_single, trade_params.single_amount_change)
                    single_amount = min(single_amount, remaining_amount)
            else:
                if remaining_amount < trade_params.single_amount_low:
                    single_amount = remaining_amount
                else:
                    max_single = min(trade_params.single_amount_high, remaining_amount)
                    single_amount = generate_random_amount(trade_params.single_amount_low, max_single, trade_params.single_amount_change)
                    single_amount = min(single_amount, remaining_amount)

            # 先在交易所2下单（对冲方向）
            order2 = await safe_execute_async(trade_params.arbitrage_param.exchange2.make_new_order,
                trade_params.exchange2_pair, trade_params.exchange2_side, "MARKET",
                single_amount, price=trade_params.pair_price)
            # 再在交易所1下单（主方向）
            order1 = await safe_execute_async(trade_params.arbitrage_param.exchange1.make_new_order,
                trade_params.exchange1_pair, trade_params.exchange1_side, "MARKET",
                single_amount, price=None)

            time.sleep(0.1)

            # 获取订单成交均价
            if trade_params.arbitrage_param.exchange1.exchange_code == ExchangeEnum.HYPERLIQUID:
                order1_avg_price = float(order1["avgPx"])
            else:
                order1_info = None
                while order1_info is None:
                    try:
                        order1_info = await safe_execute_async(trade_params.arbitrage_param.exchange1.get_recent_order,
                            trade_params.exchange1_pair, orderId=order1["orderId"])
                        if order1_info is None:
                            time.sleep(0.1)
                    except KeyboardInterrupt:
                        print("获取订单失败, 跳过检查.....")
                        break
                if order1_info is None:
                    time.sleep(3)
                    continue
                order1_avg_price = order1_info.avgPrice

            if trade_params.arbitrage_param.exchange2.exchange_code == ExchangeEnum.HYPERLIQUID:
                order2_avg_price = float(order2["avgPx"])
            else:
                order2_info = None
                while order2_info is None:
                    try:
                        order2_info = await safe_execute_async(trade_params.arbitrage_param.exchange2.get_recent_order,
                            trade_params.exchange2_pair, orderId=order2["orderId"])
                        if order2_info is None:
                            time.sleep(0.1)
                    except KeyboardInterrupt:
                        print("获取订单失败, 跳过检查.....")
                        break
                if order2_info is None:
                    time.sleep(3)
                    continue
                order2_avg_price = order2_info.avgPrice

            # 计算价差收益
            spread_profit = (order1_avg_price - order2_avg_price) * single_amount
            spread_profit = -spread_profit if trade_params.exchange1_side == TradeSide.BUY else spread_profit
            spread_profit_rate = spread_profit / single_trade_volume_usd
            img = NotifyImgGenerator.get_spread_profit_rate_img(spread_profit_rate)

            print(f"🔨 执行价格: {order1_avg_price} {order2_avg_price} 💰 金额: ${single_amount * order1_avg_price:.2f}(${spread_profit:.2f})")

            cum_volume += single_amount * order1_avg_price
            cum_profit += spread_profit
            remaining_amount -= single_amount
            trade_count += 1

            executed_amount = trade_params.total_amount - remaining_amount
            progress_pct = (executed_amount / trade_params.total_amount)
            print(f"已完成:{trade_count} {(time.time() - start) * 1000:.1f}ms "
                  f"进度:{executed_amount:.4f}/{trade_params.total_amount:.4f} ({progress_pct:.1%}) "
                  f"累计金额:${cum_volume:.2f}(${cum_profit:.2f})")

            if remaining_amount <= 0:
                print(f"👌 任务结束...")
                break

            # 判断是否需要暂停交易
            if min_profit_rate == 0 or spread_profit / single_trade_volume_usd >= min_profit_rate:
                logger.info(f"{img} 价差收益率: {spread_profit_rate:.2%}, 继续...")
                time.sleep(trade_interval_time_sec)
                continue
            else:
                delay_time = (min_profit_rate - (spread_profit / single_trade_volume_usd)) / abs(min_profit_rate)
                logger.info(f"⚠️ 订单价差收益率 {spread_profit / single_trade_volume_usd:.2%} < {min_profit_rate:.2%}, "
                            f"暂停交易{int(delay_time * 60)}s")
                try:
                    time.sleep(int(60 * delay_time))
                except KeyboardInterrupt:
                    logger.info("🚧 人工终止暂停")
                    new_min_profit_rate = FloatPrompt.ask("修改最小价差收益率?", default=min_profit_rate)
                    if new_min_profit_rate != min_profit_rate:
                        min_profit_rate = new_min_profit_rate
                        print(f"🚀 修改最小价差收益率: {min_profit_rate:.2%}")
                    if Confirm.ask("是否继续执行交易?", default=True):
                        continue
                    else:
                        break
        except KeyboardInterrupt:
            print("用户中断交易...")
            break
        except Exception as e:
            logger.error(f"❌ 订单执行异常: {e}")
            logger.exception(e)
            break

    # 验证仓位并发送通知
    stats = {'cum_volume': cum_volume, 'cum_profit': cum_profit, 'trade_count': trade_count}
    await verify_and_notify_positions(trade_params, stats)


async def execute_with_realtime_engine(trade_params: TradeParams, no_trade_timeout_sec: float = 0):
    """使用实时对冲引擎执行交易"""
    print(f"\n{'='*60}")
    print(f"🚀 启动实时对冲引擎...")
    print(f"{'='*60}\n")

    # 确保异步适配器已初始化
    await trade_params.arbitrage_param.init_async_exchanges()

    # 创建交易配置
    trade_config = TradeConfig(
        pair1=trade_params.exchange1_pair,
        pair2=trade_params.exchange2_pair,
        side1=trade_params.exchange1_side,
        side2=trade_params.exchange2_side,
        amount_min=trade_params.single_amount_low,
        amount_max=trade_params.single_amount_high,
        amount_step=trade_params.single_amount_change,
        total_amount=trade_params.total_amount,
        trade_interval_sec=0.1,
        use_dynamic_amount=trade_params.use_dynamic_amount,
        max_first_level_ratio=trade_params.max_first_level_ratio,
        no_trade_timeout_sec=no_trade_timeout_sec,
        trade_mode=TradeMode.TAKER_TAKER
    )

    # 创建风控配置
    risk_config = RiskConfig(
        max_orderbook_age_sec=3.0,
        max_spread_pct=0.001,
        min_liquidity_usd=1000,
        min_profit_rate=trade_params.min_profit_rate,
        liquidity_depth_levels=5
    )

    # 创建订单簿流 - 使用工厂方法
    stream1 = create_orderbook_stream(trade_params.arbitrage_param.exchange1)
    stream2 = create_orderbook_stream(trade_params.arbitrage_param.exchange2)

    # 检查是否支持
    if stream1 is None:
        print(f"❌ 交易所1 {trade_params.arbitrage_param.exchange1.exchange_code} 暂不支持WebSocket实时对冲")
        print(f"   支持的交易所: Binance, Aster, HyperLiquid, Lighter")
        return

    if stream2 is None:
        print(f"❌ 交易所2 {trade_params.arbitrage_param.exchange2.exchange_code} 暂不支持WebSocket实时对冲")
        print(f"   支持的交易所: Binance, Aster, HyperLiquid, Lighter")
        return

    # 创建实时对冲引擎（使用异步适配器）
    engine = RealtimeHedgeEngine(
        stream1=stream1,
        stream2=stream2,
        exchange1=trade_params.arbitrage_param.async_exchange1,
        exchange2=trade_params.arbitrage_param.async_exchange2,
        trade_config=trade_config,
        risk_config=risk_config
    )

    # 启动引擎
    try:
        await engine.start()

        # 等待交易完成
        while engine._running and engine._remaining_amount > 0:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("用户中断交易")
        engine._running = False
    except Exception as e:
        logger.error(f"交易引擎异常: {e}")
        logger.exception(e)
    finally:
        await engine.stop()

    # 获取交易统计并验证仓位
    stats = engine.get_stats()
    print(f"\n{'='*60}")
    print(f"📊 交易统计")
    print(f"{'='*60}")
    print(f"  总笔数: {stats['trade_count']}")
    print(f"  累计交易额: ${stats['cum_volume']:.2f}")
    print(f"  累计收益: ${stats['cum_profit']:.2f}")
    if stats['cum_volume'] > 0:
        print(f"  平均收益率: {stats['cum_profit'] / stats['cum_volume']:.4%}")
    print(f"{'='*60}\n")

    await verify_and_notify_positions(trade_params, stats)

