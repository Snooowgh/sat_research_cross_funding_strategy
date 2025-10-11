# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : exchange_factory
@Description : 交易所工厂类，统一管理交易所实例
@Time        : 2025/10/5
"""
import os
import asyncio
from typing import Dict, Type, Optional, Any
from loguru import logger
from cex_tools.cex_enum import ExchangeEnum
from cex_tools.base_exchange import BaseExchange, FutureExchange, SpotExchange


class ExchangeFactory:
    """
    交易所工厂类

    提供统一的交易所实例创建和管理接口，支持：
    - 按需创建交易所实例
    - 配置管理
    - 实例缓存
    - 错误处理
    """

    _exchange_registry: Dict[ExchangeEnum, Type[BaseExchange]] = {}
    _instances: Dict[str, BaseExchange] = {}
    _configs: Dict[ExchangeEnum, Dict] = {}

    @classmethod
    def register_exchange(cls, exchange_code: ExchangeEnum, exchange_class: Type[BaseExchange]):
        """
        注册交易所类

        Args:
            exchange_code: 交易所代码
            exchange_class: 交易所类
        """
        cls._exchange_registry[exchange_code] = exchange_class
        logger.info(f"注册交易所: {exchange_code} -> {exchange_class.__name__}")

    @classmethod
    def set_config(cls, exchange_code: ExchangeEnum, config: Dict):
        """
        设置交易所配置

        Args:
            exchange_code: 交易所代码
            config: 配置字典
        """
        cls._configs[exchange_code] = config
        logger.info(f"设置 {exchange_code} 配置: {list(config.keys())}")

    @classmethod
    def create_exchange(cls, exchange_code: ExchangeEnum, **kwargs) -> BaseExchange:
        """
        创建交易所实例

        Args:
            exchange_code: 交易所代码
            **kwargs: 初始化参数

        Returns:
            异步交易所实例
        """
        if exchange_code not in cls._exchange_registry:
            raise ValueError(f"未注册的交易所: {exchange_code}")

        # 合并配置
        config = cls._configs.get(exchange_code, {}).copy()
        config.update(kwargs)

        # 设置默认testnet值
        if 'testnet' not in config:
            config['testnet'] = cls._get_default_testnet()

        exchange_class = cls._exchange_registry[exchange_code]
        sync_instance = exchange_class(**config)

        # 检查是否需要异步包装
        if cls._is_async_exchange(sync_instance):
            # 已经是异步的交易所（如LighterFuture），直接返回
            logger.info(f"创建 {exchange_code} 异步交易所实例: {sync_instance}")
            ret = sync_instance
        else:
            # 同步交易所，包装为异步
            if isinstance(sync_instance, FutureExchange):
                async_instance = AsyncFutureExchangeWrapper(sync_instance)
            else:
                async_instance = AsyncExchangeWrapper(sync_instance)

            logger.info(f"创建 {exchange_code} 异步交易所实例: {async_instance}")
            ret = async_instance
        return ret

    @classmethod
    def get_exchange(cls, exchange_code: ExchangeEnum, cache_key: Optional[str] = None, **kwargs) -> BaseExchange:
        """
        获取交易所实例（支持缓存）

        Args:
            exchange_code: 交易所代码
            cache_key: 缓存键，None表示不缓存
            **kwargs: 初始化参数

        Returns:
            异步交易所实例
        """
        if cache_key and cache_key in cls._instances:
            instance = cls._instances[cache_key]
            logger.debug(f"从缓存获取 {exchange_code} 异步实例: {cache_key}")
            return instance

        instance = cls.create_exchange(exchange_code, **kwargs)

        if cache_key:
            cls._instances[cache_key] = instance
            logger.debug(f"缓存 {exchange_code} 异步实例: {cache_key}")

        return instance

    @classmethod
    def _is_async_exchange(cls, exchange_instance) -> bool:
        """
        检查交易所实例是否为异步实现

        Args:
            exchange_instance: 交易所实例

        Returns:
            是否为异步实现
        """
        # 检查关键方法是否为异步方法
        key_methods = ['make_new_order', 'get_orderbook', 'get_tick_price']

        for method_name in key_methods:
            if hasattr(exchange_instance, method_name):
                method = getattr(exchange_instance, method_name)
                if asyncio.iscoroutinefunction(method):
                    return True

        return False

    @classmethod
    def get_all_registered_exchanges(cls) -> Dict[ExchangeEnum, Type[BaseExchange]]:
        """获取所有已注册的交易所"""
        return cls._exchange_registry.copy()

    @classmethod
    def clear_cache(cls, exchange_code: Optional[ExchangeEnum] = None):
        """
        清除缓存

        Args:
            exchange_code: 指定交易所，None表示清除所有
        """
        if exchange_code:
            keys_to_remove = [k for k in cls._instances.keys()
                              if str(exchange_code) in k]
            for key in keys_to_remove:
                del cls._instances[key]
            logger.info(f"清除 {exchange_code} 缓存")
        else:
            cls._instances.clear()
            logger.info("清除所有交易所缓存")

    @classmethod
    def _get_default_testnet(cls) -> bool:
        """获取默认testnet设置"""
        return os.getenv('EXCHANGE_TESTNET', 'false').lower() == 'true'


class AsyncExchangeWrapper:
    """
    异步交易所包装器基类
    为同步交易所方法提供异步接口
    """

    def __init__(self, sync_exchange):
        self._sync_exchange = sync_exchange
        self.exchange_code = sync_exchange.exchange_code
        # 处理testnet属性
        if hasattr(sync_exchange, 'testnet'):
            self.testnet = sync_exchange.testnet
        else:
            self.testnet = False  # 默认值

    async def convert_symbol(self, symbol: str) -> str:
        """异步符号转换"""
        return self._sync_exchange.convert_symbol(symbol)

    async def get_tick_price(self, symbol: str) -> float:
        """异步获取价格"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.get_tick_price, symbol)

    async def get_klines(self, symbol: str, interval: str, limit: int = 200):
        """异步获取K线数据"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.get_klines, symbol, interval, limit)

    async def get_all_cur_positions(self):
        """异步获取所有持仓"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.get_all_cur_positions)

    async def get_position(self, symbol: str):
        """异步获取指定交易对持仓"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.get_position, symbol)

    async def get_available_balance(self, asset: str = "USDT") -> float:
        """异步获取可用余额"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.get_available_balance, asset)

    async def make_new_order(self, symbol: str, side: str, order_type: str,
                             quantity: float, price: Optional[float] = None, **kwargs):
        """异步下单"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.make_new_order,
                                          symbol, side, order_type, quantity, price, **kwargs)

    async def get_funding_rate(self, symbol: str, apy: bool = True) -> float:
        """异步获取资金费率"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.get_funding_rate, symbol, apy)

    async def get_all_tick_price(self):
        """异步获取所有交易对价格"""
        if hasattr(self._sync_exchange, 'get_all_tick_price'):
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._sync_exchange.get_all_tick_price)
        return []

    async def get_available_margin(self) -> float:
        """异步获取可用保证金"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.get_available_margin)

    async def get_total_margin(self) -> float:
        """异步获取总保证金"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.get_total_margin)

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> bool:
        """异步取消所有订单"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.cancel_all_orders, symbol)

    def __getattr__(self, name: str):
        """代理其他属性到原始交易所实例"""
        return getattr(self._sync_exchange, name)

    def __str__(self):
        return f"Async{self._sync_exchange}"

    def __repr__(self):
        return self.__str__()


class AsyncFutureExchangeWrapper(AsyncExchangeWrapper):
    """
    异步永续合约交易所包装器
    """

    def __init__(self, sync_future_exchange: FutureExchange):
        super().__init__(sync_future_exchange)

    async def calculate_funding_apy(self, rate: float) -> float:
        """异步计算资金费率年化收益率"""
        return self._sync_exchange.calculate_funding_apy(rate)

    async def get_funding_frequency(self) -> int:
        """异步获取资金费率收取频率"""
        return self._sync_exchange.get_funding_frequency()

    async def set_leverage_with_validation(self, symbol: str, leverage: int) -> bool:
        """异步设置杠杆（带验证）"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_exchange.set_leverage_with_validation, symbol, leverage)


def auto_register_exchanges():
    """
    自动注册所有可用的交易所
    此函数应在导入时调用，自动发现并注册交易所类
    """
    try:
        # 注册各个交易所
        from cex_tools.binance_future import BinanceFuture
        ExchangeFactory.register_exchange(ExchangeEnum.BINANCE, BinanceFuture)
    except ImportError:
        logger.warning("BinanceFuture 不可用")

    try:
        from cex_tools.bybit_future import BybitFuture
        ExchangeFactory.register_exchange(ExchangeEnum.BYBIT, BybitFuture)
    except ImportError:
        logger.warning("BybitFuture 不可用")

    try:
        from cex_tools.hyperliquid_future import HyperLiquidFuture
        ExchangeFactory.register_exchange(ExchangeEnum.HYPERLIQUID, HyperLiquidFuture)
    except ImportError:
        logger.warning("HyperLiquidFuture 不可用")

    try:
        from cex_tools.lighter_future import LighterFuture
        ExchangeFactory.register_exchange(ExchangeEnum.LIGHTER, LighterFuture)
    except ImportError:
        logger.warning("LighterFuture 不可用")

    try:
        from cex_tools.aster_future import AsterFuture
        ExchangeFactory.register_exchange(ExchangeEnum.ASTER, AsterFuture)
    except ImportError:
        logger.warning("AsterFuture 不可用")

    try:
        from cex_tools.okx_future import OkxFuture
        ExchangeFactory.register_exchange(ExchangeEnum.OKX, OkxFuture)
    except ImportError:
        logger.warning("OKXFuture 不可用")

    logger.info(f"自动注册完成，共注册 {len(ExchangeFactory._exchange_registry)} 个交易所")


def load_exchange_configs_from_env():
    """从环境变量加载交易所配置"""
    from config.exchange_config import ExchangeConfig

    # 定义配置方法映射 - 使用与枚举一致的键名
    config_methods = {
        'BINANCE': ExchangeConfig.get_binance_config,
        'BYBIT': ExchangeConfig.get_bybit_config,
        'HYPERLIQUID': ExchangeConfig.get_hyperliquid_config,
        'LIGHTER': ExchangeConfig.get_lighter_config,
        'ASTER': ExchangeConfig.get_aster_config,
        'OKX': ExchangeConfig.get_okx_config,
    }

    for exchange_name, config_method in config_methods.items():
        try:
            config = config_method()
            exchange_enum = getattr(ExchangeEnum, exchange_name)
            # 过滤掉None值和空字符串
            valid_config = {k: v for k, v in config.items() if v is not None and v != ''}
            if valid_config:
                ExchangeFactory.set_config(exchange_enum, valid_config)
                logger.info(f"加载 {exchange_name} 配置: {len(valid_config)} 个参数")
        except AttributeError:
            logger.debug(f"跳过未知交易所: {exchange_name}")
        except Exception as e:
            logger.warning(f"加载 {exchange_name} 配置失败: {e}")


# 在模块加载时自动注册交易所
auto_register_exchanges()
load_exchange_configs_from_env()
