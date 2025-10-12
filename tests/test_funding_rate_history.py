#!/usr/bin/env python3
# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : test_funding_rate_history
@Description : 测试所有交易所的历史资金费率功能
@Time        : 2025/10/12
"""

import asyncio
import os
import time
from datetime import datetime, timedelta
from loguru import logger

from cex_tools.binance_future import BinanceFuture
from cex_tools.lighter_future import LighterFuture
from cex_tools.hyperliquid_future import HyperLiquidFuture
from cex_tools.aster_future import AsterFuture
from cex_tools.okx_future import OkxFuture
from cex_tools.bybit_future import BybitFuture


def format_time(timestamp_ms):
    """格式化时间戳"""
    return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')


def test_binance_funding_history():
    """测试Binance历史资金费率功能"""
    logger.info("=== 测试 Binance 历史资金费率功能 ===")

    try:
        # 从环境变量获取API密钥
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_SECRET_KEY")

        if not api_key or not api_secret:
            logger.warning("Binance API密钥未配置，跳过测试")
            return

        client = BinanceFuture(key=api_key, secret=api_secret)

        # 测试历史资金费率
        symbol = "BTCUSDT"
        end_time = int(time.time() * 1000)
        start_time = end_time - (7 * 24 * 60 * 60 * 1000)  # 7天前

        logger.info(f"获取 {symbol} 历史资金费率...")
        rate_history = client.get_funding_rate_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time,
            apy=True
        )

        logger.info(f"获取到 {len(rate_history.data)} 条历史资金费率数据")
        if rate_history.data:
            latest = rate_history.get_latest_rate()
            logger.info(f"最新资金费率: {latest.funding_rate_percentage} (年化: {latest.annualized_rate_percentage})")
            logger.info(f"平均年化费率: {rate_history.get_average_rate(annualized=True):.6f}")

        # 测试用户资金费历史
        logger.info(f"获取用户资金费历史...")
        funding_history = client.get_funding_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time
        )

        logger.info(f"获取到 {len(funding_history.data)} 条用户资金费记录")
        if funding_history.data:
            total_received = funding_history.get_total_received()
            total_paid = funding_history.get_total_paid()
            logger.info(f"总收到资金费: {total_received:.4f} USDT")
            logger.info(f"总支付资金费: {abs(total_paid):.4f} USDT")

        logger.success("Binance 测试完成 ✓")

    except Exception as e:
        logger.error(f"Binance 测试失败: {e}")


async def test_lighter_funding_history():
    """测试Lighter历史资金费率功能"""
    logger.info("=== 测试 Lighter 历史资金费率功能 ===")

    try:
        # 从环境变量获取配置
        l1_addr = os.getenv("LIGHTER_L1_ADDR")
        api_private_key = os.getenv("LIGHTER_API_PRIVATE_KEY")
        account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))

        if not all([l1_addr, api_private_key]):
            logger.warning("Lighter 配置未完整，跳过测试")
            return

        client = LighterFuture(
            l1_addr=l1_addr,
            api_private_key=api_private_key,
            account_index=account_index,
            api_key_index=api_key_index
        )

        await client.init()

        # 测试历史资金费率
        symbol = "BTC"
        end_time = int(time.time() * 1000)
        start_time = end_time - (7 * 24 * 60 * 60 * 1000)  # 7天前

        logger.info(f"获取 {symbol} 历史资金费率...")
        rate_history = await client.get_funding_rate_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time,
            apy=True
        )

        logger.info(f"获取到 {len(rate_history.data)} 条历史资金费率数据")
        if rate_history.data:
            latest = rate_history.get_latest_rate()
            logger.info(f"最新资金费率: {latest.funding_rate_percentage} (年化: {latest.annualized_rate_percentage})")
            logger.info(f"平均年化费率: {rate_history.get_average_rate(annualized=True):.6f}")

        # 测试用户资金费历史
        logger.info(f"获取用户资金费历史...")
        funding_history = await client.get_funding_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time
        )

        logger.info(f"获取到 {len(funding_history.data)} 条用户资金费记录")
        if funding_history.data:
            total_received = funding_history.get_total_received()
            total_paid = funding_history.get_total_paid()
            logger.info(f"总收到资金费: {total_received:.4f} USDT")
            logger.info(f"总支付资金费: {abs(total_paid):.4f} USDT")

        await client.close()
        logger.success("Lighter 测试完成 ✓")

    except Exception as e:
        logger.error(f"Lighter 测试失败: {e}")


def test_hyperliquid_funding_history():
    """测试HyperLiquid历史资金费率功能"""
    logger.info("=== 测试 HyperLiquid 历史资金费率功能 ===")

    try:
        # 从环境变量获取配置
        private_key = os.getenv("HYPERLIQUID_PRIVATE_KEY")
        public_key = os.getenv("HYPERLIQUID_PUBLIC_KEY")

        if not private_key:
            logger.warning("HyperLiquid 配置未配置，跳过测试")
            return

        client = HyperLiquidFuture(
            private_key=private_key,
            public_key=public_key
        )

        # 测试历史资金费率
        symbol = "BTC"
        end_time = int(time.time() * 1000)
        start_time = end_time - (7 * 24 * 60 * 60 * 1000)  # 7天前

        logger.info(f"获取 {symbol} 历史资金费率...")
        rate_history = client.get_funding_rate_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time,
            apy=True
        )

        logger.info(f"获取到 {len(rate_history.data)} 条历史资金费率数据")
        if rate_history.data:
            latest = rate_history.get_latest_rate()
            logger.info(f"最新资金费率: {latest.funding_rate_percentage} (年化: {latest.annualized_rate_percentage})")
            logger.info(f"平均年化费率: {rate_history.get_average_rate(annualized=True):.6f}")

        # 测试用户资金费历史
        logger.info(f"获取用户资金费历史...")
        funding_history = client.get_funding_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time
        )

        logger.info(f"获取到 {len(funding_history.data)} 条用户资金费记录")
        if funding_history.data:
            total_received = funding_history.get_total_received()
            total_paid = funding_history.get_total_paid()
            logger.info(f"总收到资金费: {total_received:.4f} USDT")
            logger.info(f"总支付资金费: {abs(total_paid):.4f} USDT")

        logger.success("HyperLiquid 测试完成 ✓")

    except Exception as e:
        logger.error(f"HyperLiquid 测试失败: {e}")


def test_aster_funding_history():
    """测试Aster历史资金费率功能"""
    logger.info("=== 测试 Aster 历史资金费率功能 ===")

    try:
        # 从环境变量获取配置
        api_key = os.getenv("ASTER_API_KEY")
        api_secret = os.getenv("ASTER_SECRET_KEY")

        if not api_key or not api_secret:
            logger.warning("Aster API密钥未配置，跳过测试")
            return

        client = AsterFuture(key=api_key, secret=api_secret)

        # 测试历史资金费率
        symbol = "BTCUSDT"
        end_time = int(time.time() * 1000)
        start_time = end_time - (7 * 24 * 60 * 60 * 1000)  # 7天前

        logger.info(f"获取 {symbol} 历史资金费率...")
        rate_history = client.get_funding_rate_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time,
            apy=True
        )

        logger.info(f"获取到 {len(rate_history.data)} 条历史资金费率数据")
        if rate_history.data:
            latest = rate_history.get_latest_rate()
            logger.info(f"最新资金费率: {latest.funding_rate_percentage} (年化: {latest.annualized_rate_percentage})")
            logger.info(f"平均年化费率: {rate_history.get_average_rate(annualized=True):.6f}")

        # 测试用户资金费历史
        logger.info(f"获取用户资金费历史...")
        funding_history = client.get_funding_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time
        )

        logger.info(f"获取到 {len(funding_history.data)} 条用户资金费记录")
        if funding_history.data:
            total_received = funding_history.get_total_received()
            total_paid = funding_history.get_total_paid()
            logger.info(f"总收到资金费: {total_received:.4f} USDT")
            logger.info(f"总支付资金费: {abs(total_paid):.4f} USDT")

        logger.success("Aster 测试完成 ✓")

    except Exception as e:
        logger.error(f"Aster 测试失败: {e}")


def test_okx_funding_history():
    """测试OKX历史资金费率功能"""
    logger.info("=== 测试 OKX 历史资金费率功能 ===")

    try:
        # 从环境变量获取配置
        api_key = os.getenv("OKX_API_KEY")
        secret_key = os.getenv("OKX_SECRET_KEY")
        passphrase = os.getenv("OKX_PASSPHRASE")

        if not all([api_key, secret_key, passphrase]):
            logger.warning("OKX API密钥未配置，跳过测试")
            return

        client = OkxFuture(
            api_key=api_key,
            secret_key=secret_key,
            passphrase=passphrase
        )

        # 测试历史资金费率
        symbol = "BTCUSDT"
        end_time = int(time.time() * 1000)
        start_time = end_time - (7 * 24 * 60 * 60 * 1000)  # 7天前

        logger.info(f"获取 {symbol} 历史资金费率...")
        rate_history = client.get_funding_rate_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time,
            apy=True
        )

        logger.info(f"获取到 {len(rate_history.data)} 条历史资金费率数据")
        if rate_history.data:
            latest = rate_history.get_latest_rate()
            logger.info(f"最新资金费率: {latest.funding_rate_percentage} (年化: {latest.annualized_rate_percentage})")
            logger.info(f"平均年化费率: {rate_history.get_average_rate(annualized=True):.6f}")

        # 测试用户资金费历史
        logger.info(f"获取用户资金费历史...")
        funding_history = client.get_funding_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time
        )

        logger.info(f"获取到 {len(funding_history.data)} 条用户资金费记录")
        if funding_history.data:
            total_received = funding_history.get_total_received()
            total_paid = funding_history.get_total_paid()
            logger.info(f"总收到资金费: {total_received:.4f} USDT")
            logger.info(f"总支付资金费: {abs(total_paid):.4f} USDT")

        logger.success("OKX 测试完成 ✓")

    except Exception as e:
        logger.error(f"OKX 测试失败: {e}")


def test_bybit_funding_history():
    """测试Bybit历史资金费率功能"""
    logger.info("=== 测试 Bybit 历史资金费率功能 ===")

    try:
        # 从环境变量获取配置
        api_key = os.getenv("BYBIT_API_KEY")
        secret_key = os.getenv("BYBIT_SECRET_KEY")

        if not api_key or not secret_key:
            logger.warning("Bybit API密钥未配置，跳过测试")
            return

        client = BybitFuture(api_key=api_key, secret_key=secret_key)

        # 测试历史资金费率
        symbol = "BTCUSDT"
        end_time = int(time.time() * 1000)
        start_time = end_time - (7 * 24 * 60 * 60 * 1000)  # 7天前

        logger.info(f"获取 {symbol} 历史资金费率...")
        rate_history = client.get_funding_rate_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time,
            apy=True
        )

        logger.info(f"获取到 {len(rate_history.data)} 条历史资金费率数据")
        if rate_history.data:
            latest = rate_history.get_latest_rate()
            logger.info(f"最新资金费率: {latest.funding_rate_percentage} (年化: {latest.annualized_rate_percentage})")
            logger.info(f"平均年化费率: {rate_history.get_average_rate(annualized=True):.6f}")

        # 测试用户资金费历史
        logger.info(f"获取用户资金费历史...")
        funding_history = client.get_funding_history(
            symbol=symbol,
            limit=10,
            start_time=start_time,
            end_time=end_time
        )

        logger.info(f"获取到 {len(funding_history.data)} 条用户资金费记录")
        if funding_history.data:
            total_received = funding_history.get_total_received()
            total_paid = funding_history.get_total_paid()
            logger.info(f"总收到资金费: {total_received:.4f} USDT")
            logger.info(f"总支付资金费: {abs(total_paid):.4f} USDT")

        logger.success("Bybit 测试完成 ✓")

    except Exception as e:
        logger.error(f"Bybit 测试失败: {e}")


async def main():
    """主函数"""
    logger.info("开始测试所有交易所的历史资金费率功能")
    logger.info("请确保已正确配置各交易所的API密钥环境变量")

    # 测试同步交易所
    test_binance_funding_history()
    test_hyperliquid_funding_history()
    test_aster_funding_history()
    test_okx_funding_history()
    test_bybit_funding_history()

    # 测试异步交易所
    await test_lighter_funding_history()

    logger.success("所有测试完成！")


if __name__ == "__main__":
    # 设置日志
    logger.add(
        "../logs/test_funding_rate_history.log",
        rotation="10 MB",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )

    # 运行测试
    asyncio.run(main())