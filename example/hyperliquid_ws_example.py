# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : hyperliquid_ws_example
@Description : Hyperliquid交易所WebSocket订单簿示例
@Time        : 2025/10/3 00:05
"""
import asyncio
from loguru import logger
from cex_tools.exchange_ws.hyperliquid_orderbook_stream import HyperliquidOrderBookStream
from cex_tools.exchange_ws.orderbook_stream import OrderBookData


def on_btc_orderbook_update(orderbook: OrderBookData):
    """BTC订单簿更新回调"""
    logger.info(f"[BTC] {orderbook}")
    logger.info(f"  最优买价: {orderbook.best_bid:.2f}")
    logger.info(f"  最优卖价: {orderbook.best_ask:.2f}")
    logger.info(f"  中间价: {orderbook.mid_price:.2f}")
    logger.info(f"  价差: {orderbook.spread:.2f} ({orderbook.spread_pct:.4%})")
    logger.info(f"  买单深度(前10档): {orderbook.get_depth('bid', 10):.4f} BTC")
    logger.info(f"  卖单深度(前10档): {orderbook.get_depth('ask', 10):.4f} BTC")
    logger.info(f"  买单流动性(前10档): ${orderbook.get_liquidity_usd('bid', 10):,.2f}")
    logger.info(f"  卖单流动性(前10档): ${orderbook.get_liquidity_usd('ask', 10):,.2f}")


def on_eth_orderbook_update(orderbook: OrderBookData):
    """ETH订单簿更新回调"""
    logger.info(f"[ETH] {orderbook}")


async def main():
    # 创建 Hyperliquid 订单簿流监听器
    stream = HyperliquidOrderBookStream(testnet=False)

    # 订阅交易对（可以传入 "BTC" 或 "BTCUSDT"，会自动转换）
    stream.subscribe("BTC", callback=on_btc_orderbook_update)
    stream.subscribe("ETH", callback=on_eth_orderbook_update)
    stream.subscribe("SOL")  # 不指定回调，只存储到缓存

    # 启动 WebSocket
    await stream.start()

    # 运行一段时间
    try:
        await asyncio.sleep(60)  # 运行60秒

        # 获取最新的订单簿数据（从缓存）
        btc_orderbook = stream.get_latest_orderbook("BTCUSDT")
        if btc_orderbook:
            logger.info(f"从缓存获取 BTC 订单簿: {btc_orderbook}")

        sol_orderbook = stream.get_latest_orderbook("SOLUSDT")
        if sol_orderbook:
            logger.info(f"从缓存获取 SOL 订单簿: {sol_orderbook}")

    except KeyboardInterrupt:
        logger.info("收到中断信号，停止...")

    finally:
        # 停止 WebSocket
        await stream.stop()


if __name__ == "__main__":
    asyncio.run(main())