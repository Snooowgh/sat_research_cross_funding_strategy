# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : aster_realtime_hedge_example
@Description : Aster交易所实时对冲示例
@Time        : 2025/10/4
"""
import asyncio
from loguru import logger

from cex_tools.aster_future import AsterFuture
from cex_tools.lighter_future import LighterFuture
from cex_tools.exchange_ws.aster_orderbook_stream import AsterOrderBookStreamAsync
from cex_tools.exchange_ws.lighter_orderbook_stream import LighterOrderBookStreamAsync
from logic.realtime_hedge_engine import RealtimeHedgeEngine, TradeConfig, RiskConfig
from cex_tools.cex_enum import TradeSide
from cex_tools.async_exchange_adapter import AsyncExchangeFactory


async def main():
    """Aster-Lighter 实时对冲示例"""
    logger.info("=" * 80)
    logger.info("Aster-Lighter 实时对冲引擎示例")
    logger.info("=" * 80)

    # ========== 第一步：初始化交易所 ==========
    logger.info("初始化交易所...")

    # Aster交易所
    aster = AsterFuture(
        key="your_aster_api_key",
        secret="your_aster_api_secret",
        erc20_deposit_addr="your_deposit_address"
    )

    # Lighter交易所
    lighter = LighterFuture(
        public_key="your_public_key",
        private_key="your_private_key",
        chain_id=105021,
        subnet_id=3
    )
    await lighter.init()

    # 创建异步适配器
    aster_async = AsyncExchangeFactory.create_async_exchange(aster, "aster")
    lighter_async = AsyncExchangeFactory.create_async_exchange(lighter, "lighter")

    # 初始化异步适配器
    await aster_async.init()
    await lighter_async.init()

    logger.info(f"✅ 交易所初始化完成")
    logger.info(f"   Aster 可用余额: ${await aster_async.get_available_balance('USDT'):.2f}")
    logger.info(f"   Lighter 可用余额: ${await lighter_async.get_available_balance():.2f}")

    # ========== 第二步：创建WebSocket订单簿流 ==========
    logger.info("\n创建WebSocket订单簿流...")

    aster_stream = AsterOrderBookStreamAsync()
    lighter_stream = LighterOrderBookStreamAsync()

    # ========== 第三步：配置交易参数 ==========
    pair1 = "BTCUSDT"  # Aster交易对
    pair2 = "BTCUSDT"  # Lighter交易对

    trade_config = TradeConfig(
        pair1=pair1,
        pair2=pair2,
        side1=TradeSide.BUY,   # Aster做多
        side2=TradeSide.SELL,  # Lighter做空（对冲）
        amount_min=0.001,      # 最小下单数量
        amount_max=0.002,      # 最大下单数量
        amount_step=0.0001,    # 数量步长
        total_amount=0.01,     # 总交易数量
        trade_interval_sec=0.5,  # 交易间隔
        use_dynamic_amount=True,  # 启用动态数量调整
        max_first_level_ratio=0.5,  # 最多吃掉第一档50%
        no_trade_timeout_sec=60,  # 60秒无交易则自动停止
        min_order_value_usd=50.0  # 最小订单金额50美金
    )

    # 风控配置
    risk_config = RiskConfig(
        max_orderbook_age_sec=2.0,  # 订单簿最大过期时间2秒
        max_spread_pct=0.001,  # 最大盘口价差0.1%
        min_liquidity_usd=5000,  # 最小流动性5000美金
        min_profit_rate=0.0015,  # 最小价差收益率0.15%
        liquidity_depth_levels=10,  # 检查前10档流动性
        user_min_profit_rate=0.001,  # 用户设置的底线0.1%
        enable_dynamic_profit_rate=True,  # 启用动态收益率调整
        profit_rate_adjust_step=0.0001,  # 调整步长0.01%
        profit_rate_adjust_threshold=3,  # 连续3笔触发调整
        no_trade_reduce_timeout_sec=15.0,  # 15秒无成交降低收益率
        no_trade_reduce_step_multiplier=1.5  # 降低时步长倍数
    )

    logger.info(f"\n交易配置:")
    logger.info(f"  交易对: {pair1} / {pair2}")
    logger.info(f"  方向: {trade_config.side1} / {trade_config.side2}")
    logger.info(f"  总数量: {trade_config.total_amount}")
    logger.info(f"  单笔数量: {trade_config.amount_min} ~ {trade_config.amount_max}")
    logger.info(f"  最小收益率: {risk_config.min_profit_rate:.4%}")
    logger.info(f"  动态数量调整: {'启用' if trade_config.use_dynamic_amount else '禁用'}")

    # ========== 第四步：创建实时对冲引擎 ==========
    logger.info("\n创建实时对冲引擎...")

    engine = RealtimeHedgeEngine(
        stream1=aster_stream,
        stream2=lighter_stream,
        exchange1=aster_async,
        exchange2=lighter_async,
        trade_config=trade_config,
        risk_config=risk_config
    )

    # ========== 第五步：启动引擎 ==========
    logger.info("\n🚀 启动实时对冲引擎...")
    logger.info("=" * 80)

    try:
        await engine.start()

        # 等待交易完成
        while engine._running and engine._remaining_amount > 0:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("⚠️  用户中断交易")
        engine._running = False
    except Exception as e:
        logger.error(f"❌ 引擎异常: {e}")
        logger.exception(e)
    finally:
        await engine.stop()

    # ========== 第六步：输出交易统计 ==========
    stats = engine.get_stats()
    logger.info("\n" + "=" * 80)
    logger.info("📊 交易统计")
    logger.info("=" * 80)
    logger.info(f"  总笔数: {stats['trade_count']}")
    logger.info(f"  累计交易额: ${stats['cum_volume']:.2f}")
    logger.info(f"  累计收益: ${stats['cum_profit']:.2f}")
    if stats['cum_volume'] > 0:
        logger.info(f"  平均收益率: {stats['cum_profit'] / stats['cum_volume']:.4%}")
    logger.info(f"  完成进度: {stats['progress']:.1%}")
    logger.info("=" * 80)

    # 清理
    await lighter.close()


if __name__ == '__main__':
    # 运行示例
    asyncio.run(main())