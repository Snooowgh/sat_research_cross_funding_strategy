#!/usr/bin/env python3
# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : test_individual_streams
@Description : 单个交易所WebSocket流独立测试
@Time        : 2025/10/8
"""
import asyncio
import time
import sys
from typing import Dict, Any
from loguru import logger

# 添加项目路径
sys.path.append('..')

from cex_tools.exchange_ws.binance_ws_direct import BinanceOrderBookStreamDirect
from cex_tools.exchange_ws.lighter_orderbook_stream import LighterOrderBookStreamAsync
from cex_tools.exchange_ws.hyperliquid_orderbook_stream import HyperliquidOrderBookStream
from cex_tools.exchange_ws.bybit_orderbook_stream import BybitOrderBookStreamAsync
from cex_tools.exchange_ws.okx_orderbook_stream import OkxOrderBookStreamAsync
from cex_tools.exchange_ws.aster_orderbook_stream import AsterOrderBookStreamAsync
from cex_tools.exchange_ws.orderbook_stream import OrderBookData


class IndividualStreamTester:
    """单个流测试器"""

    def __init__(self):
        self.test_results: Dict[str, Dict[str, Any]] = {}
        self.received_data: Dict[str, int] = {}

    def create_callback(self, exchange_name: str):
        """创建测试回调函数"""
        def callback(orderbook: OrderBookData):
            # 统计接收到的数据
            if exchange_name not in self.received_data:
                self.received_data[exchange_name] = 0
            self.received_data[exchange_name] += 1

            # 每10条数据打印一次
            if self.received_data[exchange_name] % 10 == 0:
                logger.info(f"📊 [{exchange_name}] 收到第 {self.received_data[exchange_name]} 条数据")
                logger.info(f"  交易对: {orderbook.pair}")
                logger.info(f"  最优买价: {orderbook.best_bid}")
                logger.info(f"  最优卖价: {orderbook.best_ask}")
                logger.info(f"  价差: {orderbook.spread_pct:.4%}")
                logger.info(f"  时间戳: {orderbook.timestamp}")

        return callback

    async def test_binance_stream(self, symbol: str = "BTCUSDT", duration: int = 30):
        """测试Binance流"""
        logger.info(f"🧪 测试 Binance {symbol} 流...")

        try:
            stream = BinanceOrderBookStreamDirect()

            # 订阅
            stream.subscribe(symbol, self.create_callback("Binance"))

            # 启动
            await stream.start()
            logger.success("✅ Binance 流启动成功")

            # 等待数据
            await asyncio.sleep(duration)

            # 停止
            await stream.stop()

            # 检查结果
            data_count = self.received_data.get("Binance", 0)
            success = data_count > 0

            self.test_results["binance"] = {
                "success": success,
                "data_count": data_count,
                "error": None
            }

            if success:
                logger.success(f"✅ Binance 测试成功，收到 {data_count} 条数据")
            else:
                logger.error(f"❌ Binance 测试失败，未收到数据")

        except Exception as e:
            logger.error(f"❌ Binance 测试异常: {e}")
            self.test_results["binance"] = {
                "success": False,
                "data_count": 0,
                "error": str(e)
            }

    async def test_lighter_stream(self, symbol: str = "BTCUSDT", duration: int = 30):
        """测试Lighter流"""
        logger.info(f"🧪 测试 Lighter {symbol} 流...")

        try:
            stream = LighterOrderBookStreamAsync()

            # 订阅
            stream.subscribe(symbol, self.create_callback("Lighter"))

            # 启动
            await stream.start()
            logger.success("✅ Lighter 流启动成功")

            # 等待数据
            await asyncio.sleep(duration)

            # 停止
            await stream.stop()

            # 检查结果
            data_count = self.received_data.get("Lighter", 0)
            success = data_count > 0

            self.test_results["lighter"] = {
                "success": success,
                "data_count": data_count,
                "error": None
            }

            if success:
                logger.success(f"✅ Lighter 测试成功，收到 {data_count} 条数据")
            else:
                logger.error(f"❌ Lighter 测试失败，未收到数据")

        except Exception as e:
            logger.error(f"❌ Lighter 测试异常: {e}")
            self.test_results["lighter"] = {
                "success": False,
                "data_count": 0,
                "error": str(e)
            }

    async def test_hyperliquid_stream(self, symbol: str = "BTC", duration: int = 30):
        """测试HyperLiquid流"""
        logger.info(f"🧪 测试 HyperLiquid {symbol} 流...")

        try:
            stream = HyperliquidOrderBookStream()

            # 订阅
            stream.subscribe(symbol, self.create_callback("HyperLiquid"))

            # 启动
            await stream.start()
            logger.success("✅ HyperLiquid 流启动成功")

            # 等待数据
            await asyncio.sleep(duration)

            # 停止
            await stream.stop()

            # 检查结果
            data_count = self.received_data.get("HyperLiquid", 0)
            success = data_count > 0

            self.test_results["hyperliquid"] = {
                "success": success,
                "data_count": data_count,
                "error": None
            }

            if success:
                logger.success(f"✅ HyperLiquid 测试成功，收到 {data_count} 条数据")
            else:
                logger.error(f"❌ HyperLiquid 测试失败，未收到数据")

        except Exception as e:
            logger.error(f"❌ HyperLiquid 测试异常: {e}")
            self.test_results["hyperliquid"] = {
                "success": False,
                "data_count": 0,
                "error": str(e)
            }

    async def test_bybit_stream(self, symbol: str = "BTCUSDT", duration: int = 30):
        """测试Bybit流"""
        logger.info(f"🧪 测试 Bybit {symbol} 流...")

        try:
            stream = BybitOrderBookStreamAsync()

            # 订阅
            stream.subscribe(symbol, self.create_callback("Bybit"))

            # 启动
            await stream.start()
            logger.success("✅ Bybit 流启动成功")

            # 等待数据
            await asyncio.sleep(duration)

            # 停止
            await stream.stop()

            # 检查结果
            data_count = self.received_data.get("Bybit", 0)
            success = data_count > 0

            self.test_results["bybit"] = {
                "success": success,
                "data_count": data_count,
                "error": None
            }

            if success:
                logger.success(f"✅ Bybit 测试成功，收到 {data_count} 条数据")
            else:
                logger.error(f"❌ Bybit 测试失败，未收到数据")

        except Exception as e:
            logger.error(f"❌ Bybit 测试异常: {e}")
            self.test_results["bybit"] = {
                "success": False,
                "data_count": 0,
                "error": str(e)
            }

    async def test_okx_stream(self, symbol: str = "BTCUSDT", duration: int = 30):
        """测试OKX流"""
        logger.info(f"🧪 测试 OKX {symbol} 流...")

        try:
            stream = OkxOrderBookStreamAsync()

            # 订阅
            stream.subscribe(symbol, self.create_callback("OKX"))

            # 启动
            await stream.start()
            logger.success("✅ OKX 流启动成功")

            # 等待数据
            await asyncio.sleep(duration)

            # 停止
            await stream.stop()

            # 检查结果
            data_count = self.received_data.get("OKX", 0)
            success = data_count > 0

            self.test_results["okx"] = {
                "success": success,
                "data_count": data_count,
                "error": None
            }

            if success:
                logger.success(f"✅ OKX 测试成功，收到 {data_count} 条数据")
            else:
                logger.error(f"❌ OKX 测试失败，未收到数据")

        except Exception as e:
            logger.error(f"❌ OKX 测试异常: {e}")
            self.test_results["okx"] = {
                "success": False,
                "data_count": 0,
                "error": str(e)
            }

    async def test_aster_stream(self, symbol: str = "BTCUSDT", duration: int = 30):
        """测试Aster流"""
        logger.info(f"🧪 测试 Aster {symbol} 流...")

        try:
            stream = AsterOrderBookStreamAsync()

            # 订阅
            stream.subscribe(symbol, self.create_callback("Aster"))

            # 启动
            await stream.start()
            logger.success("✅ Aster 流启动成功")

            # 等待数据
            await asyncio.sleep(duration)

            # 停止
            await stream.stop()

            # 检查结果
            data_count = self.received_data.get("Aster", 0)
            success = data_count > 0

            self.test_results["aster"] = {
                "success": success,
                "data_count": data_count,
                "error": None
            }

            if success:
                logger.success(f"✅ Aster 测试成功，收到 {data_count} 条数据")
            else:
                logger.error(f"❌ Aster 测试失败，未收到数据")

        except Exception as e:
            logger.error(f"❌ Aster 测试异常: {e}")
            self.test_results["aster"] = {
                "success": False,
                "data_count": 0,
                "error": str(e)
            }

    async def test_specific_exchange(self, exchange: str, symbol: str = None, duration: int = 30):
        """测试指定交易所"""
        if symbol is None:
            symbol = "BTCUSDT" if exchange != "hyperliquid" else "BTC"

        exchange = exchange.lower()

        if exchange == "binance":
            await self.test_binance_stream(symbol, duration)
        elif exchange == "lighter":
            await self.test_lighter_stream(symbol, duration)
        elif exchange == "hyperliquid":
            await self.test_hyperliquid_stream(symbol, duration)
        elif exchange == "bybit":
            await self.test_bybit_stream(symbol, duration)
        elif exchange == "okx":
            await self.test_okx_stream(symbol, duration)
        elif exchange == "aster":
            await self.test_aster_stream(symbol, duration)
        else:
            logger.error(f"❌ 不支持的交易所: {exchange}")

    async def test_all_exchanges(self, symbol: str = None, duration: int = 30):
        """测试所有交易所"""
        logger.info("🚀 开始测试所有交易所WebSocket流...")

        exchanges = ["binance", "lighter", "hyperliquid", "bybit", "okx", "aster"]

        for exchange in exchanges:
            logger.info(f"\n{'='*60}")
            await self.test_specific_exchange(exchange, symbol, duration)
            # 短暂休息避免连接过快
            await asyncio.sleep(2)

    def generate_test_report(self):
        """生成测试报告"""
        logger.info("\n" + "="*60)
        logger.info("📊 单个交易所流测试报告")
        logger.info("="*60)

        total_exchanges = len(self.test_results)
        successful_exchanges = sum(1 for result in self.test_results.values() if result["success"])

        logger.info(f"总交易所数: {total_exchanges}")
        logger.info(f"成功: {successful_exchanges} ✅")
        logger.info(f"失败: {total_exchanges - successful_exchanges} ❌")

        logger.info("\n详细结果:")
        for exchange, result in self.test_results.items():
            if result["success"]:
                logger.info(f"  ✅ {exchange.upper()}: 收到 {result['data_count']} 条数据")
            else:
                logger.error(f"  ❌ {exchange.upper()}: {result.get('error', '未知错误')}")

        logger.info("="*60)

    async def run_performance_test(self, exchange: str, symbol: str = None, duration: int = 60):
        """运行性能测试"""
        logger.info(f"🚀 运行 {exchange.upper()} 性能测试...")

        if symbol is None:
            symbol = "BTCUSDT" if exchange != "hyperliquid" else "BTC"

        start_time = time.time()
        self.received_data.clear()

        await self.test_specific_exchange(exchange, symbol, duration)

        if exchange in self.test_results and self.test_results[exchange]["success"]:
            data_count = self.test_results[exchange]["data_count"]
            elapsed_time = time.time() - start_time

            # 计算性能指标
            data_rate = data_count / elapsed_time if elapsed_time > 0 else 0

            logger.info(f"📊 {exchange.upper()} 性能指标:")
            logger.info(f"  测试时长: {elapsed_time:.2f} 秒")
            logger.info(f"  接收数据: {data_count} 条")
            logger.info(f"  数据速率: {data_rate:.2f} 条/秒")
            logger.info(f"  平均间隔: {1000/data_rate:.2f} 毫秒" if data_rate > 0 else "  平均间隔: N/A")


async def main():
    """主测试函数"""
    import argparse

    parser = argparse.ArgumentParser(description="单个交易所WebSocket流测试")
    parser.add_argument("--exchange", type=str, help="测试指定交易所 (binance|lighter|hyperliquid|bybit|okx|aster)")
    parser.add_argument("--symbol", type=str, help="交易对符号 (默认: BTCUSDT, HyperLiquid为BTC)")
    parser.add_argument("--duration", type=int, default=30, help="测试持续时间（秒）")
    parser.add_argument("--all", action="store_true", help="测试所有交易所")
    parser.add_argument("--performance", action="store_true", help="运行性能测试")
    parser.add_argument("--verbose", action="store_true", help="详细输出")

    args = parser.parse_args()

    # 设置日志级别
    if args.verbose:
        logger.remove()
        logger.add(sys.stdout, level="DEBUG")
    else:
        logger.remove()
        logger.add(sys.stdout, level="INFO")

    # 运行测试
    tester = IndividualStreamTester()

    if args.all:
        await tester.test_all_exchanges(args.symbol, args.duration)
    elif args.exchange:
        if args.performance:
            await tester.run_performance_test(args.exchange, args.symbol, args.duration)
        else:
            await tester.test_specific_exchange(args.exchange, args.symbol, args.duration)
    else:
        logger.error("请指定 --exchange 或 --all 参数")
        sys.exit(1)

    # 生成报告
    tester.generate_test_report()


if __name__ == "__main__":
    asyncio.run(main())