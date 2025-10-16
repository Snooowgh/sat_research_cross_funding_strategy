# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : arbitrage_param
@Description : 套利参数配置 - 使用环境变量配置，统一异步接口
@Time        : 2025/9/22 08:19
"""
import asyncio
from cex_tools.aster_future import AsterFuture
from cex_tools.binance_future import BinanceFuture
from cex_tools.hyperliquid_future import HyperLiquidFuture
from cex_tools.lighter_future import LighterFuture
from cex_tools.bybit_future import BybitFuture
from cex_tools.okx_future import OkxFuture
from cex_tools.async_exchange_adapter import AsyncExchangeFactory
from config.exchange_config import ExchangeConfig
from loguru import logger


class ArbitrageWhiteListParam:
    SYMBOL_LIST = [
        # "BTC",
        # "ETH",
        # "SOL",
        "HYPE",
        "ARB",
        # "PUMP",
        # "DOGE",
        # "AAVE",
        # "UNI",
        # "PENDLE",
        # "ASTER",
        # "XRP",
        # "BNB",
        # "ENA",
        # "SUI",
        # "AVAX",
        # "LINK",
        # "PENGU",
        # "WIF"
    ]


class MultiExchangeArbitrageParam:

    def __init__(self, exchange_codes=None, auto_init=True):
        """
        多交易所套利参数类

        Args:
            exchange_codes: 要初始化的交易所代码列表，如 ['binance', 'bybit']，None表示初始化所有
            auto_init: 是否自动初始化交易所
        """
        # 获取通用配置
        general_config = ExchangeConfig.get_general_config()
        self.default_leverage = general_config["default_leverage"]
        self.danger_leverage = general_config['danger_leverage']

        # 交易所映射配置
        self.exchange_configs = {
            'binance': ('Binance', BinanceFuture, ExchangeConfig.get_binance_config),
            # 'lighter': ('Lighter', LighterFuture, ExchangeConfig.get_lighter_config),
            # 'hyperliquid': ('HyperLiquid', HyperLiquidFuture, ExchangeConfig.get_hyperliquid_config),
            # 'bybit': ('Bybit', BybitFuture, ExchangeConfig.get_bybit_config),
            # 'aster': ('Aster', AsterFuture, ExchangeConfig.get_aster_config),
            'okx': ('OKX', OkxFuture, ExchangeConfig.get_okx_config),
        }

        # 初始化交易所列表和实例
        self.exchange_list = []
        self.exchanges = {}  # 代码到实例的映射
        self.exchange1 = None  # 主要交易所1
        self.exchange2 = None  # 主要交易所2

        # 兼容性属性
        self.hyperliquid_exchange = None
        self.binance_exchange = None
        self.lighter_exchange = None
        self.aster_exchange = None
        self.bybit_exchange = None
        self.okx_exchange = None

        # 异步适配器
        self.async_exchanges = {}  # 异步适配器字典
        self.async_exchange_list = []  # 异步适配器字典
        self.async_exchange1 = None  # 异步适配器1
        self.async_exchange2 = None  # 异步适配器2

        if auto_init:
            self.initialize_exchanges(exchange_codes)

    def initialize_exchanges(self, exchange_codes=None):
        """
        初始化交易所

        Args:
            exchange_codes: 要初始化的交易所代码列表，None表示初始化所有可用交易所
        """
        if exchange_codes is None:
            exchange_codes = list(self.exchange_configs.keys())

        success_count = 0
        for code in exchange_codes:
            if self.initialize_exchange(code):
                success_count += 1

        # 如果有至少2个交易所，设置默认的配对
        if len(self.exchange_list) >= 2:
            self.exchange1 = self.exchange_list[0]
            self.exchange2 = self.exchange_list[1]

    def initialize_exchange(self, exchange_code):
        """
        初始化单个交易所

        Args:
            exchange_code: 交易所代码

        Returns:
            bool: 初始化是否成功
        """
        if exchange_code not in self.exchange_configs:
            logger.warning(f"⚠️ 不支持的交易所: {exchange_code}")
            return False

        name, exchange_class, config_func = self.exchange_configs[exchange_code]

        try:
            config = config_func()
            if not config:
                logger.warning(f"⚠️ {name} 交易所配置为空，跳过初始化")
                return False

            exchange_instance = exchange_class(**config)
            self.exchanges[exchange_code] = exchange_instance
            self.exchange_list.append(exchange_instance)

            # 兼容性属性
            setattr(self, f"{exchange_code}_exchange", exchange_instance)

            return True

        except Exception as e:
            logger.error(f"❌ {name} 交易所初始化失败: {e}")
            return False

    def get_available_exchange_codes(self):
        """获取可用的交易所代码列表"""
        return list(self.exchange_configs.keys())

    def get_initialized_exchange_codes(self):
        """获取已初始化的交易所代码列表"""
        return list(self.exchanges.keys())

    def set_exchange_pair(self, exchange1_code, exchange2_code):
        """
        设置主要交易所配对

        Args:
            exchange1_code: 交易所1代码
            exchange2_code: 交易所2代码
        """
        if exchange1_code not in self.exchanges:
            raise ValueError(f"交易所1 {exchange1_code} 未初始化")
        if exchange2_code not in self.exchanges:
            raise ValueError(f"交易所2 {exchange2_code} 未初始化")

        self.exchange1 = self.exchanges[exchange1_code]
        self.exchange2 = self.exchanges[exchange2_code]
        logger.info(f"📊 设置交易所配对: {exchange1_code} <-> {exchange2_code}")

    # ========== 异步方法 ==========

    async def init_async_exchanges(self):
        """初始化异步适配器"""
        self.async_exchanges = await AsyncExchangeFactory.create_multiple_async_exchanges(
            self.exchanges
        )

        # 设置默认异步配对
        if len(self.async_exchanges) >= 2:
            codes = list(self.async_exchanges.keys())
            self.async_exchange1 = self.async_exchanges[codes[0]]
            self.async_exchange2 = self.async_exchanges[codes[1]]
        self.async_exchange_list = list(self.async_exchanges.values())
        # 兼容性属性
        for exchange_code, exchange_instance in self.async_exchanges.items():
            setattr(self, f"{exchange_code}_exchange", exchange_instance)
        return self.async_exchanges

    async def set_async_exchange_pair(self, exchange1_code, exchange2_code):
        """设置异步交易所配对"""
        if exchange1_code not in self.async_exchanges:
            raise ValueError(f"异步交易所1 {exchange1_code} 未初始化")
        if exchange2_code not in self.async_exchanges:
            raise ValueError(f"异步交易所2 {exchange2_code} 未初始化")

        self.async_exchange1 = self.async_exchanges[exchange1_code]
        self.async_exchange2 = self.async_exchanges[exchange2_code]
        logger.info(f"📊 设置异步交易所配对: {exchange1_code} <-> {exchange2_code}")

    async def get_async_exchange_by_code(self, exchange_code: str):
        """根据交易所代码获取异步交易所对象"""
        if not self.async_exchanges:
            await self.init_async_exchanges()
        return self.async_exchanges.get(exchange_code.lower())

    async def get_async_exchange_pair(self, exchange1_code: str, exchange2_code: str):
        """获取指定的异步交易所对"""
        if not self.async_exchanges:
            await self.init_async_exchanges()

        ex1 = self.async_exchanges.get(exchange1_code)
        ex2 = self.async_exchanges.get(exchange2_code)
        if ex1 and ex2:
            return ex1, ex2
        return None, None

    async def close_async_exchanges(self):
        """关闭所有异步交易所连接"""
        for code, async_exchange in self.async_exchanges.items():
            try:
                if hasattr(async_exchange, 'close') and asyncio.iscoroutinefunction(async_exchange.close):
                    await async_exchange.close()
                    logger.info(f"✅ 异步交易所 {code} 连接已关闭")
            except Exception as e:
                logger.error(f"❌ 关闭异步交易所 {code} 连接失败: {e}")

        self.async_exchanges.clear()
        self.async_exchange1 = None
        self.async_exchange2 = None


class HyperliquidLighterArbitrageParam:

    def __init__(self):
        # 获取通用配置
        general_config = ExchangeConfig.get_general_config()
        self.danger_leverage = general_config['danger_leverage']

        # 初始化交易所
        self.exchange1 = None
        self.exchange2 = None

        # 初始化HyperLiquid交易所
        try:
            hl_config = ExchangeConfig.get_hyperliquid_config()
            if hl_config:
                self.exchange1 = HyperLiquidFuture(**hl_config)
                logger.info("✅ HyperLiquid 交易所初始化成功")
        except Exception as e:
            logger.error(f"❌ HyperLiquid 交易所初始化失败: {e}")

        # 初始化Lighter交易所
        try:
            lighter_config = ExchangeConfig.get_lighter_config()
            if lighter_config:
                self.exchange2 = LighterFuture(**lighter_config)
                logger.info("✅ Lighter 交易所初始化成功")
        except Exception as e:
            logger.error(f"❌ Lighter 交易所初始化失败: {e}")

        logger.info(f"🚀 HyperLiquid-Lighter 套利参数初始化完成")

    def get_exchange_pair_status(self):
        """获取当前交易所配对状态"""
        status = {
            'total_exchanges': 0,
            'initialized_codes': [],
            'current_pair': None
        }

        # 统计已初始化的交易所
        if self.exchange1:
            status['total_exchanges'] += 1
            status['initialized_codes'].append(self.exchange1.exchange_code)
        if self.exchange2:
            status['total_exchanges'] += 1
            status['initialized_codes'].append(self.exchange2.exchange_code)

        # 设置当前配对
        if self.exchange1 and self.exchange2:
            status['current_pair'] = {
                'exchange1': self.exchange1.exchange_code,
                'exchange2': self.exchange2.exchange_code
            }

        return status

    async def init_async_exchanges(self):
        """初始化异步适配器"""
        from cex_tools.async_exchange_adapter import AsyncExchangeFactory

        exchanges = {}
        if self.exchange1:
            exchanges[self.exchange1.exchange_code] = self.exchange1
        if self.exchange2:
            exchanges[self.exchange2.exchange_code] = self.exchange2

        self.async_exchanges = await AsyncExchangeFactory.create_multiple_async_exchanges(exchanges)

        # 设置默认异步配对
        if len(self.async_exchanges) >= 2:
            codes = list(self.async_exchanges.keys())
            self.async_exchange1 = self.async_exchanges[codes[0]]
            self.async_exchange2 = self.async_exchanges[codes[1]]

        return self.async_exchanges

    async def close_async_exchanges(self):
        """关闭所有异步交易所连接"""
        if hasattr(self, 'async_exchanges'):
            for code, async_exchange in self.async_exchanges.items():
                try:
                    await async_exchange.close()
                    logger.info(f"✅ 异步交易所 {code} 连接已关闭")
                except Exception as e:
                    logger.error(f"❌ 关闭异步交易所 {code} 连接失败: {e}")

            self.async_exchanges.clear()
            self.async_exchange1 = None
            self.async_exchange2 = None


class BinanceLighterArbitrageParam:

    def __init__(self):
        # 获取通用配置
        general_config = ExchangeConfig.get_general_config()
        self.danger_leverage = general_config['danger_leverage']

        # 初始化交易所
        self.exchange1 = None
        self.exchange2 = None

        # 初始化Binance交易所
        try:
            binance_config = ExchangeConfig.get_binance_config()
            if binance_config:
                self.exchange1 = BinanceFuture(**binance_config)
                logger.info("✅ Binance 交易所初始化成功")
        except Exception as e:
            logger.error(f"❌ Binance 交易所初始化失败: {e}")

        # 初始化Lighter交易所
        try:
            lighter_config = ExchangeConfig.get_lighter_config()
            if lighter_config:
                self.exchange2 = LighterFuture(**lighter_config)
                logger.info("✅ Lighter 交易所初始化成功")
        except Exception as e:
            logger.error(f"❌ Lighter 交易所初始化失败: {e}")

        logger.info(f"🚀 Binance-Lighter 套利参数初始化完成")

    def get_exchange_pair_status(self):
        """获取当前交易所配对状态"""
        status = {
            'total_exchanges': 0,
            'initialized_codes': [],
            'current_pair': None
        }

        # 统计已初始化的交易所
        if self.exchange1:
            status['total_exchanges'] += 1
            status['initialized_codes'].append(self.exchange1.exchange_code)
        if self.exchange2:
            status['total_exchanges'] += 1
            status['initialized_codes'].append(self.exchange2.exchange_code)

        # 设置当前配对
        if self.exchange1 and self.exchange2:
            status['current_pair'] = {
                'exchange1': self.exchange1.exchange_code,
                'exchange2': self.exchange2.exchange_code
            }

        return status

    async def init_async_exchanges(self):
        """初始化异步适配器"""
        from cex_tools.async_exchange_adapter import AsyncExchangeFactory

        exchanges = {}
        if self.exchange1:
            exchanges[self.exchange1.exchange_code] = self.exchange1
        if self.exchange2:
            exchanges[self.exchange2.exchange_code] = self.exchange2

        self.async_exchanges = await AsyncExchangeFactory.create_multiple_async_exchanges(exchanges)

        # 设置默认异步配对
        if len(self.async_exchanges) >= 2:
            codes = list(self.async_exchanges.keys())
            self.async_exchange1 = self.async_exchanges[codes[0]]
            self.async_exchange2 = self.async_exchanges[codes[1]]

        return self.async_exchanges

    async def close_async_exchanges(self):
        """关闭所有异步交易所连接"""
        if hasattr(self, 'async_exchanges'):
            for code, async_exchange in self.async_exchanges.items():
                try:
                    await async_exchange.close()
                    logger.info(f"✅ 异步交易所 {code} 连接已关闭")
                except Exception as e:
                    logger.error(f"❌ 关闭异步交易所 {code} 连接失败: {e}")

            self.async_exchanges.clear()
            self.async_exchange1 = None
            self.async_exchange2 = None


class BinanceHyperliquidArbitrageParam:

    def __init__(self):
        # 获取通用配置
        general_config = ExchangeConfig.get_general_config()
        self.danger_leverage = general_config['danger_leverage']

        # 初始化交易所
        self.exchange1 = None
        self.exchange2 = None

        # 初始化Binance交易所
        try:
            binance_config = ExchangeConfig.get_binance_config()
            if binance_config:
                self.exchange1 = BinanceFuture(**binance_config)
                logger.info("✅ Binance 交易所初始化成功")
        except Exception as e:
            logger.error(f"❌ Binance 交易所初始化失败: {e}")

        # 初始化HyperLiquid交易所
        try:
            hl_config = ExchangeConfig.get_hyperliquid_config()
            if hl_config:
                self.exchange2 = HyperLiquidFuture(**hl_config)
                logger.info("✅ HyperLiquid 交易所初始化成功")
        except Exception as e:
            logger.error(f"❌ HyperLiquid 交易所初始化失败: {e}")

        logger.info(f"🚀 Binance-HyperLiquid 套利参数初始化完成")

    def get_exchange_pair_status(self):
        """获取当前交易所配对状态"""
        status = {
            'total_exchanges': 0,
            'initialized_codes': [],
            'current_pair': None
        }

        # 统计已初始化的交易所
        if self.exchange1:
            status['total_exchanges'] += 1
            status['initialized_codes'].append(self.exchange1.exchange_code)
        if self.exchange2:
            status['total_exchanges'] += 1
            status['initialized_codes'].append(self.exchange2.exchange_code)

        # 设置当前配对
        if self.exchange1 and self.exchange2:
            status['current_pair'] = {
                'exchange1': self.exchange1.exchange_code,
                'exchange2': self.exchange2.exchange_code
            }

        return status

    async def init_async_exchanges(self):
        """初始化异步适配器"""
        from cex_tools.async_exchange_adapter import AsyncExchangeFactory

        exchanges = {}
        if self.exchange1:
            exchanges[self.exchange1.exchange_code] = self.exchange1
        if self.exchange2:
            exchanges[self.exchange2.exchange_code] = self.exchange2

        self.async_exchanges = await AsyncExchangeFactory.create_multiple_async_exchanges(exchanges)

        # 设置默认异步配对
        if len(self.async_exchanges) >= 2:
            codes = list(self.async_exchanges.keys())
            self.async_exchange1 = self.async_exchanges[codes[0]]
            self.async_exchange2 = self.async_exchanges[codes[1]]

        return self.async_exchanges

    async def close_async_exchanges(self):
        """关闭所有异步交易所连接"""
        if hasattr(self, 'async_exchanges'):
            for code, async_exchange in self.async_exchanges.items():
                try:
                    await async_exchange.close()
                    logger.info(f"✅ 异步交易所 {code} 连接已关闭")
                except Exception as e:
                    logger.error(f"❌ 关闭异步交易所 {code} 连接失败: {e}")

            self.async_exchanges.clear()
            self.async_exchange1 = None
            self.async_exchange2 = None
