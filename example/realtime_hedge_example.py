# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : realtime_hedge_example
@Description : 实时对冲交易引擎使用示例
@Time        : 2025/10/2 14:20
"""
import asyncio
from loguru import logger

from arbitrage_param import BinanceLighterArbitrageParam
from cex_tools.cex_enum import TradeSide
from cex_tools.exchange_ws.binance_ws_direct import BinanceOrderBookStreamDirect
from cex_tools.exchange_ws.lighter_orderbook_stream import LighterOrderBookStreamAsync
from logic.realtime_hedge_engine import (
    RealtimeHedgeEngine,
    TradeConfig,
    RiskConfig
)


async def realtime_hedge_example():
    """实时对冲交易示例"""

    # ========== 第一步：初始化交易所 ==========
    logger.info("初始化交易所...")
    arbitrage_param = BinanceLighterArbitrageParam()
    await arbitrage_param.init_async_exchanges()

    binance = arbitrage_param.async_exchange1  # Binance交易所（异步适配器）
    lighter = arbitrage_param.async_exchange2  # Lighter交易所（异步适配器）

    # ========== 第二步：配置交易参数 ==========
    pair = "ETHUSDT"  # 交易对

    trade_config = TradeConfig(
        pair1=pair,
        pair2=pair,
        side1=TradeSide.BUY,  # Binance做多
        side2=TradeSide.SELL,  # Lighter做空（对冲）
        amount_min=0.01,  # 单笔交易最小数量 (ETH)
        amount_max=0.02,  # 单笔交易最大数量 (ETH)
        amount_step=0.001,  # 数量步长
        total_amount=0.1,  # 总交易数量 (ETH)
        trade_interval_sec=0.5,  # 交易间隔0.5秒
        use_dynamic_amount=True,  # 启用动态数量调整
        max_first_level_ratio=0.5  # 最大吃掉第一档流动性的50%
    )

    # ========== 第三步：配置风控参数 ==========
    # 可选：自动计算合理的最小价差收益率
    # funding_rate1 = binance.get_funding_rate(pair)
    # funding_rate2 = await lighter.get_funding_rate(pair)
    # price1 = binance.get_tick_price(pair)
    # price2 = await lighter.get_tick_price(pair)
    # current_spread = (price1 - price2) / price2
    # auto_min_profit_rate = calculate_optimal_min_profit_rate(
    #     current_spread_rate=current_spread,
    #     funding_rate1=funding_rate1,
    #     funding_rate2=funding_rate2
    # )
    # logger.info(f"自动计算的最小价差收益率: {auto_min_profit_rate:.4%}")

    risk_config = RiskConfig(
        max_orderbook_age_sec=1.0,  # 订单簿数据最大过期时间1秒
        max_spread_pct=0.002,  # 最大买卖价差0.2%
        min_liquidity_usd=5000,  # 最小流动性5000美元
        min_profit_rate=0.0015,  # 最小价差收益率0.15% (或使用上面auto_min_profit_rate)
        liquidity_depth_levels=10  # 检查前10档深度
    )

    # ========== 第四步：创建WebSocket订单簿流 ==========
    logger.info("创建订单簿流...")

    # Binance订单簿流
    binance_stream = BinanceOrderBookStreamDirect(
        api_key=binance.key,
        secret=binance.secret
    )

    # Lighter订单簿流
    lighter_stream = LighterOrderBookStreamAsync()

    # ========== 第五步：创建实时对冲引擎 ==========
    engine = RealtimeHedgeEngine(
        stream1=binance_stream,
        stream2=lighter_stream,
        exchange1=binance,
        exchange2=lighter,
        trade_config=trade_config,
        risk_config=risk_config
    )

    # ========== 第六步：启动引擎 ==========
    try:
        await engine.start()

        # 等待交易完成或用户中断
        while engine._running:
            await asyncio.sleep(1)

            # 显示进度
            stats = engine.get_stats()
            logger.info(f"进度: {stats['progress']:.1%} "
                        f"({stats['trade_count']} 笔，"
                        f"${stats['cum_volume']:.2f}，"
                        f"收益 ${stats['cum_profit']:.2f})")

    except KeyboardInterrupt:
        logger.info("用户中断")
    finally:
        # 停止引擎
        await engine.stop()
        await lighter.close()

    # ========== 第七步：显示最终统计 ==========
    stats = engine.get_stats()
    logger.info(f"交易完成！")
    logger.info(f"  总笔数: {stats['trade_count']}")
    logger.info(f"  累计交易额: ${stats['cum_volume']:.2f}")
    logger.info(f"  累计收益: ${stats['cum_profit']:.2f}")
    logger.info(f"  平均收益率: {stats['cum_profit'] / stats['cum_volume']:.4%}")


async def simple_test():
    """简单测试：只监听订单簿，不执行交易"""

    logger.info("=== 订单簿流测试 ===")

    # 创建Binance订单簿流
    binance_stream = BinanceOrderBookStreamDirect()

    def on_orderbook_update(orderbook):
        logger.info(f"{orderbook}")

    # 订阅交易对
    binance_stream.subscribe("BTCUSDT", on_orderbook_update)
    binance_stream.subscribe("ETHUSDT", on_orderbook_update)

    # 启动
    await binance_stream.start()

    # 监听30秒
    await asyncio.sleep(30)

    # 停止
    await binance_stream.stop()


async def lighter_test():
    """Lighter订单簿流测试"""

    logger.info("=== Lighter订单簿流测试 ===")

    lighter_stream = LighterOrderBookStreamAsync()

    def on_orderbook_update(orderbook):
        logger.info(f"{orderbook}")

    # 订阅交易对
    lighter_stream.subscribe("ETHUSDT", on_orderbook_update)
    lighter_stream.subscribe("BTCUSDT", on_orderbook_update)

    # 启动
    await lighter_stream.start()

    # 监听30秒
    await asyncio.sleep(30)

    # 停止
    await lighter_stream.stop()


if __name__ == "__main__":
    # 运行实时对冲示例
    asyncio.run(realtime_hedge_example())

    # 或者运行简单测试
    # asyncio.run(simple_test())
    # asyncio.run(lighter_test())