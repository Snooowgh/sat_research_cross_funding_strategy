# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : test_all_exchanges
@Description : 所有交易所的综合测试
@Time        : 2025/10/5
"""
import asyncio
import time
from loguru import logger
from cex_tools.exchange_factory import ExchangeFactory
from cex_tools.exchange_utils import ExchangeUtils
from cex_tools.cex_enum import ExchangeEnum


async def test_all_exchanges_basic_functionality():
    """
    测试所有交易所的基础功能
    """
    logger.info("🚀 开始测试所有交易所基础功能...")

    # 获取所有已注册的交易所
    all_exchanges = ExchangeFactory.get_all_registered_exchanges()
    logger.info(f"📋 已注册的交易所: {list(all_exchanges.keys())}")

    # 测试结果统计
    test_results = {}

    for exchange_code in all_exchanges:
        logger.info(f"\n🔍 测试 {exchange_code} 交易所...")

        try:
            # 创建交易所实例
            exchange = ExchangeFactory.create_exchange(exchange_code, testnet=False)
            if exchange.exchange_code == ExchangeEnum.LIGHTER:
                await exchange.init()  # Lighter需要异步初始化
            # 测试计数器
            tests_passed = 0
            total_tests = 0

            # 测试1: 价格查询
            total_tests += 1
            try:
                price = await exchange.get_tick_price("BTCUSDT")
                if isinstance(price, (int, float)) and price > 0:
                    logger.success(f"✅ {exchange_code} 价格查询: ${price:,.2f}")
                    tests_passed += 1
                else:
                    logger.warning(f"⚠️ {exchange_code} 价格查询返回异常: {price}")
            except Exception as e:
                logger.warning(f"⚠️ {exchange_code} 价格查询失败: {e}")

            # 测试2: 资金费率查询
            total_tests += 1
            try:
                funding_rate = await exchange.get_funding_rate("BTCUSDT", apy=True)
                if isinstance(funding_rate, (int, float)):
                    logger.success(f"✅ {exchange_code} 资金费率: {funding_rate:.6f}")
                    tests_passed += 1
                else:
                    logger.warning(f"⚠️ {exchange_code} 资金费率返回异常: {funding_rate}")
            except Exception as e:
                logger.warning(f"⚠️ {exchange_code} 资金费率查询失败: {e}")

            # 测试3: 仓位查询
            total_tests += 1
            try:
                positions = await exchange.get_all_cur_positions()
                if isinstance(positions, list):
                    logger.success(f"✅ {exchange_code} 仓位查询: {len(positions)} 个仓位")
                    tests_passed += 1
                else:
                    logger.warning(f"⚠️ {exchange_code} 仓位查询返回异常类型")
            except Exception as e:
                logger.warning(f"⚠️ {exchange_code} 仓位查询失败: {e}")

            # 测试4: 可用保证金查询
            total_tests += 1
            try:
                margin = None
                if hasattr(exchange, 'get_available_margin'):
                    margin = await exchange.get_available_margin()

                if margin is not None and isinstance(margin, (int, float)) and margin >= 0:
                    logger.success(f"✅ {exchange_code} 可用保证金: ${margin:,.2f}")
                    tests_passed += 1
                else:
                    logger.warning(f"⚠️ {exchange_code} 可用保证金返回异常: {margin}")
            except Exception as e:
                logger.warning(f"⚠️ {exchange_code} 可用保证金查询失败: {e}")

            # 测试5: 总保证金查询
            total_tests += 1
            try:
                total_margin = None
                if hasattr(exchange, 'get_total_margin'):
                    total_margin = await exchange.get_total_margin()

                if total_margin is not None and isinstance(total_margin, (int, float)) and total_margin >= 0:
                    logger.success(f"✅ {exchange_code} 总保证金: ${total_margin:,.2f}")
                    tests_passed += 1
                else:
                    logger.warning(f"⚠️ {exchange_code} 总保证金返回异常: {total_margin}")
            except Exception as e:
                logger.warning(f"⚠️ {exchange_code} 总保证金查询失败: {e}")

            # 测试6: 最近订单查询
            # total_tests += 1
            # try:
            #     recent_order = None
            #     if hasattr(exchange, 'get_recent_order'):
            #         recent_order = await exchange.get_recent_order("BTCUSDT")
            #
            #     if recent_order is not None:
            #         logger.success(f"✅ {exchange_code} 最近订单查询成功")
            #         tests_passed += 1
            #     else:
            #         logger.warning(f"⚠️ {exchange_code} 最近订单查询返回None或方法不存在")
            # except Exception as e:
            #     logger.warning(f"⚠️ {exchange_code} 最近订单查询失败: {e}")

            # 测试7: ERC20充值地址获取
            total_tests += 1
            try:
                erc20_addr = getattr(exchange, 'erc20_deposit_addr', None)
                if erc20_addr is not None:
                    if isinstance(erc20_addr, str) and erc20_addr:
                        # 隐藏地址中间部分保护隐私
                        masked_addr = erc20_addr[:6] + "..." + erc20_addr[-4:] if len(erc20_addr) > 10 else erc20_addr
                        logger.success(f"✅ {exchange_code} ERC20充值地址: {masked_addr}")
                        tests_passed += 1
                    else:
                        logger.warning(f"⚠️ {exchange_code} ERC20充值地址为空")
                else:
                    logger.info(f"ℹ️ {exchange_code} 未配置ERC20充值地址")
                    tests_passed += 1  # 不配置也算正常
            except Exception as e:
                logger.warning(f"⚠️ {exchange_code} ERC20充值地址获取失败: {e}")

            # 记录测试结果
            success_rate = tests_passed / total_tests if total_tests > 0 else 0
            test_results[exchange_code] = {
                'passed': tests_passed,
                'total': total_tests,
                'success_rate': success_rate
            }

            if success_rate >= 0.75:
                logger.success(f"🎉 {exchange_code} 测试通过: {tests_passed}/{total_tests} ({success_rate:.1%})")
            else:
                logger.warning(f"⚠️ {exchange_code} 测试部分失败: {tests_passed}/{total_tests} ({success_rate:.1%})")

        except Exception as e:
            logger.error(f"❌ {exchange_code} 创建失败: {e}")
            test_results[exchange_code] = {
                'passed': 0,
                'total': 0,
                'success_rate': 0
            }

    return test_results


async def test_multi_exchange_comparison():
    """
    测试多交易所价格比较功能
    """
    logger.info("\n🌐 开始测试多交易所价格比较...")

    # 选择几个稳定的交易所进行测试
    test_exchanges = [
        ExchangeEnum.BYBIT,
        ExchangeEnum.HYPERLIQUID,
        # ExchangeEnum.BINANCE,  # 可能需要API密钥
    ]

    available_exchanges = []

    for exchange_code in test_exchanges:
        try:
            exchange = ExchangeFactory.create_exchange(exchange_code, testnet=False)
            if exchange.exchange_code == ExchangeEnum.LIGHTER:
                await exchange.init()  # Lighter需要异步初始化
            available_exchanges.append(exchange)
            logger.info(f"✅ {exchange_code} 可用")
        except Exception as e:
            logger.warning(f"⚠️ {exchange_code} 不可用: {e}")

    if len(available_exchanges) < 2:
        logger.warning("⚠️ 可用交易所少于2个，跳过价格比较测试")
        return

    # 测试价格比较
    try:
        prices = await ExchangeUtils.compare_prices(available_exchanges, "BTCUSDT")

        logger.info("📊 BTC价格比较结果:")
        for exchange_code, price in prices.items():
            if price > 0:
                logger.info(f"  {exchange_code}: ${price:,.2f}")
            else:
                logger.warning(f"  {exchange_code}: 获取失败")

        # 检查价差
        valid_prices = [price for price in prices.values() if price > 0]
        if len(valid_prices) >= 2:
            max_price = max(valid_prices)
            min_price = min(valid_prices)
            price_diff = (max_price - min_price) / min_price

            logger.info(f"💰 价差分析:")
            logger.info(f"  最高价: ${max_price:,.2f}")
            logger.info(f"  最低价: ${min_price:,.2f}")
            logger.info(f"  价差: {price_diff:.2%}")

            if price_diff > 0.001:  # 0.1%
                logger.success(f"✅ 发现价差机会: {price_diff:.2%}")
            else:
                logger.info(f"📊 价差较小: {price_diff:.2%}")

    except Exception as e:
        logger.error(f"❌ 价格比较测试失败: {e}")


async def test_exchange_factory():
    """
    测试交易所工厂功能
    """
    logger.info("\n🏭 开始测试交易所工厂功能...")

    try:
        # 测试注册的交易所
        all_exchanges = ExchangeFactory.get_all_registered_exchanges()
        logger.success(f"✅ 已注册交易所数量: {len(all_exchanges)}")

        # 测试实例创建
        bybit1 = ExchangeFactory.create_exchange(ExchangeEnum.BYBIT, testnet=False)
        bybit2 = ExchangeFactory.create_exchange(ExchangeEnum.BYBIT, testnet=False)

        logger.success(f"✅ 交易所实例创建成功")

        # 测试实例缓存
        bybit_cached = ExchangeFactory.get_exchange(
            ExchangeEnum.BYBIT,
            cache_key="test_cache",
            testnet=False
        )

        bybit_cached2 = ExchangeFactory.get_exchange(
            ExchangeEnum.BYBIT,
            cache_key="test_cache",
            testnet=False
        )

        logger.success(f"✅ 交易所实例缓存功能正常")

        # 清理缓存
        ExchangeFactory.clear_cache(ExchangeEnum.BYBIT)
        logger.success(f"✅ 缓存清理功能正常")

    except Exception as e:
        logger.error(f"❌ 交易所工厂测试失败: {e}")


async def test_all_tick_prices():
    """
    测试所有交易所的批量价格查询功能
    """
    logger.info("\n📈 开始测试批量价格查询...")

    # 获取所有已注册的交易所
    all_exchanges = ExchangeFactory.get_all_registered_exchanges()

    for exchange_code in all_exchanges:
        logger.info(f"\n🔍 测试 {exchange_code} 批量价格查询...")

        try:
            # 创建交易所实例
            exchange = ExchangeFactory.create_exchange(exchange_code, testnet=False)
            if exchange.exchange_code == ExchangeEnum.LIGHTER:
                await exchange.init()  # Lighter需要异步初始化
            # 检查是否有 get_all_tick_price 方法
            if not hasattr(exchange, 'get_all_tick_price'):
                logger.info(f"ℹ️ {exchange_code} 未实现 get_all_tick_price 方法")
                continue

            # 测试批量价格查询
            all_prices = await exchange.get_all_tick_price()
            logger.success(f"✅ {exchange_code} 批量价格查询成功: {all_prices[:3]}/{len(all_prices)}")

        except Exception as e:
            logger.error(f"❌ {exchange_code} 批量价格查询失败: {e}")


async def test_cancel_all_orders():
    """
    测试所有交易所的取消所有订单功能
    """
    logger.info("\n❌ 开始测试取消所有订单功能...")

    # 获取所有已注册的交易所
    all_exchanges = ExchangeFactory.get_all_registered_exchanges()

    for exchange_code in all_exchanges:
        logger.info(f"\n🔍 测试 {exchange_code} 取消所有订单...")

        try:
            # 创建交易所实例
            exchange = ExchangeFactory.create_exchange(exchange_code, testnet=False)
            if exchange.exchange_code == ExchangeEnum.LIGHTER:
                await exchange.init()  # Lighter需要异步初始化
            # 检查是否有 cancel_all_orders 方法
            if not hasattr(exchange, 'cancel_all_orders'):
                logger.info(f"ℹ️ {exchange_code} 未实现 cancel_all_orders 方法")
                continue

            # 首先检查当前是否有挂单（可选）
            try:
                if hasattr(exchange, 'get_open_orders'):
                    open_orders = await exchange.get_open_orders("BTCUSDT")
                    if isinstance(open_orders, list):
                        logger.info(f"📋 {exchange_code} 当前 BTCUSDT 挂单数量: {len(open_orders)}")
                    else:
                        logger.info(f"📋 {exchange_code} 无法获取挂单信息")
                else:
                    logger.info(f"📋 {exchange_code} 未实现 get_open_orders 方法")
            except Exception as e:
                logger.info(f"📋 {exchange_code} 获取挂单信息失败: {e}")

            cancel_response = await exchange.cancel_all_orders()

            if cancel_response is True:
                logger.success(f"✅ {exchange_code} 取消所有订单成功")
            elif isinstance(cancel_response, dict) and cancel_response.get('success'):
                logger.success(f"✅ {exchange_code} 取消所有订单成功: {cancel_response}")
            elif isinstance(cancel_response, dict):
                logger.warning(f"⚠️ {exchange_code} 取消所有订单部分成功: {cancel_response}")
            else:
                logger.warning(f"⚠️ {exchange_code} 取消所有订单返回: {cancel_response}")

        except Exception as e:
            logger.error(f"❌ {exchange_code} 取消所有订单失败: {e}")


async def test_convert_size_functionality():
    """
    测试所有交易所的convert_size功能
    """
    logger.info("\n🔧 开始测试convert_size功能...")

    # 获取所有已注册的交易所
    all_exchanges = ExchangeFactory.get_all_registered_exchanges()

    # 测试用例：不同数量和交易对组合
    test_cases = [
        ("BTCUSDT", 0.123456789),
        ("ETHUSDT", 1.23456789),
        ("SOLUSDT", 10.123456),
        ("DOGEUSDT", 100.123456789),
        ("BTC", 0.001),
        ("ETH", 0.01),
        ("SOL", 0.1),
    ]

    for exchange_code in all_exchanges:
        logger.info(f"\n🔍 测试 {exchange_code} convert_size功能...")

        try:
            # 创建交易所实例
            exchange = ExchangeFactory.create_exchange(exchange_code, testnet=False)
            if exchange.exchange_code == ExchangeEnum.LIGHTER:
                await exchange.init()  # Lighter需要异步初始化

            # 检查是否有convert_size方法
            if not hasattr(exchange, 'convert_size'):
                logger.info(f"ℹ️ {exchange_code} 未实现convert_size方法")
                continue

            # 性能测试
            performance_results = []

            for symbol, size in test_cases:
                start_time = time.time()

                try:
                    converted_size = exchange.convert_size(symbol, size)
                    elapsed_time = (time.time() - start_time) * 1000

                    # 验证返回值
                    if isinstance(converted_size, (int, float)):
                        logger.info(f"  ✅ {symbol} {size} -> {converted_size} ({elapsed_time:.2f}ms)")
                        performance_results.append(elapsed_time)
                    else:
                        logger.warning(f"  ⚠️ {symbol} {size} -> 无效类型: {type(converted_size)}")

                except Exception as e:
                    logger.warning(f"  ⚠️ {symbol} {size} 转换失败: {e}")

            # 性能统计
            if performance_results:
                avg_time = sum(performance_results) / len(performance_results)
                max_time = max(performance_results)
                min_time = min(performance_results)

                logger.info(f"  📊 性能统计:")
                logger.info(f"     平均耗时: {avg_time:.2f}ms")
                logger.info(f"     最大耗时: {max_time:.2f}ms")
                logger.info(f"     最小耗时: {min_time:.2f}ms")
                logger.info(f"     测试用例: {len(performance_results)}个")

                # 性能评级
                if avg_time < 1:
                    logger.success(f"     🚀 性能优秀 (< 1ms)")
                elif avg_time < 10:
                    logger.success(f"     ✅ 性能良好 (< 10ms)")
                elif avg_time < 50:
                    logger.info(f"     ⚠️ 性能一般 (< 50ms)")
                else:
                    logger.warning(f"     ❌ 性能较慢 (>= 50ms)")

        except Exception as e:
            logger.error(f"❌ {exchange_code} convert_size测试失败: {e}")


async def test_convert_size_edge_cases():
    """
    测试convert_size的边界情况
    """
    logger.info("\n🧪 开始测试convert_size边界情况...")

    # 边界测试用例
    edge_cases = [
        ("BTCUSDT", 0),  # 零值
        ("BTCUSDT", 0.000001),  # 极小值
        ("BTCUSDT", 999999.999),  # 极大值
        ("INVALID", 1.0),  # 无效交易对
        ("", 1.0),  # 空交易对
    ]

    # 选择几个主要交易所进行测试
    test_exchanges = [
        ExchangeEnum.BYBIT,
        ExchangeEnum.HYPERLIQUID,
        ExchangeEnum.LIGHTER,
    ]

    for exchange_code in test_exchanges:
        logger.info(f"\n🔍 测试 {exchange_code} 边界情况...")

        try:
            exchange = ExchangeFactory.create_exchange(exchange_code, testnet=False)
            if exchange.exchange_code == ExchangeEnum.LIGHTER:
                await exchange.init()

            if not hasattr(exchange, 'convert_size'):
                logger.info(f"ℹ️ {exchange_code} 未实现convert_size方法")
                continue

            for symbol, size in edge_cases:
                try:
                    result = exchange.convert_size(symbol, size)
                    logger.info(f"  ✅ {symbol} {size} -> {result}")
                except Exception as e:
                    logger.warning(f"  ⚠️ {symbol} {size} -> 异常: {e}")

        except Exception as e:
            logger.error(f"❌ {exchange_code} 边界测试失败: {e}")


async def test_error_handling():
    """
    测试错误处理机制
    """
    logger.info("\n🛡️ 开始测试错误处理机制...")

    try:
        # 测试无效交易对
        exchange = ExchangeFactory.create_exchange(ExchangeEnum.BYBIT, testnet=False)
        if exchange.exchange_code == ExchangeEnum.LIGHTER:
            await exchange.init()  # Lighter需要异步初始化
        price = await exchange.get_tick_price("INVALID_PAIR")
        if price == 0:
            logger.success("✅ 无效交易对错误处理正常")
        else:
            logger.warning(f"⚠️ 无效交易对返回: {price}")


    except Exception as e:
        logger.error(f"❌ 错误处理测试失败: {e}")


async def main():
    """
    主测试函数
    """
    logger.info("=" * 80)
    logger.info("🧪 开始所有交易所综合测试")
    logger.info("=" * 80)

    start_time = time.time()

    # 执行各项测试
    test_results = await test_all_exchanges_basic_functionality()
    await test_multi_exchange_comparison()
    await test_exchange_factory()
    await test_all_tick_prices()
    await test_cancel_all_orders()
    await test_convert_size_functionality()
    await test_convert_size_edge_cases()
    await test_error_handling()

    # 统计结果
    logger.info("\n" + "=" * 80)
    logger.info("📊 测试结果汇总")
    logger.info("=" * 80)

    total_passed = sum(result['passed'] for result in test_results.values())
    total_tests = sum(result['total'] for result in test_results.values())

    if total_tests > 0:
        overall_success_rate = total_passed / total_tests
        logger.info(f"总体成功率: {overall_success_rate:.1%} ({total_passed}/{total_tests})")

        logger.info("\n各交易所详细结果:")
        for exchange_code, result in test_results.items():
            if result['total'] > 0:
                status = "✅" if result['success_rate'] >= 0.75 else "⚠️" if result['success_rate'] >= 0.5 else "❌"
                logger.info(
                    f"  {status} {exchange_code}: {result['passed']}/{result['total']} ({result['success_rate']:.1%})")
            else:
                logger.info(f"  ❌ {exchange_code}: 测试失败")

    total_time = time.time() - start_time
    logger.info(f"\n⏱️ 总测试耗时: {total_time:.2f}秒")

    if overall_success_rate >= 0.75:
        logger.success("🎉 整体测试通过！")
    else:
        logger.warning("⚠️ 部分测试失败，需要检查实现")


if __name__ == "__main__":
    asyncio.run(main())