# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : base_exchange
@Description : 交易所抽象基类，定义通用接口和功能
@Time        : 2025/10/5
"""
import time
import asyncio
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Union
from loguru import logger
from utils.decorators import timed_cache
from cex_tools.cex_enum import ExchangeEnum, TradeSide
from cex_tools.exchange_model.base_model import BaseModel


class BaseExchange(ABC):
    """
    交易所抽象基类

    所有交易所实现都应该继承此类，实现标准化的接口：
    - 价格查询
    - 订单簿获取
    - 仓位管理
    - 交易执行
    - 资金查询
    """

    def __init__(self, exchange_code: ExchangeEnum, testnet: bool = False):
        """
        初始化交易所客户端

        Args:
            exchange_code: 交易所代码枚举
            testnet: 是否使用测试网络
        """
        self.exchange_code = exchange_code
        self.testnet = testnet
        self.maker_fee_rate = 0.0001  # 默认maker费率
        self.taker_fee_rate = 0.0004  # 默认taker费率

        # 缓存相关
        self._price_cache = {}
        self._orderbook_cache = {}
        self._last_cache_update = {}
        self._cache_ttl = {
            'price': 1,      # 价格缓存1秒
            'orderbook': 0.5, # 订单簿缓存0.5秒
            'balance': 3      # 余额缓存3秒
        }

        logger.info(f"初始化 {exchange_code} 交易所客户端 (testnet={testnet})")

    @abstractmethod
    def convert_symbol(self, symbol: str) -> str:
        """
        转换交易对格式为该交易所的标准格式

        Args:
            symbol: 交易对符号 (如 "BTC", "BTCUSDT")

        Returns:
            转换后的交易对符号
        """
        pass

    @abstractmethod
    def get_tick_price(self, symbol: str) -> float:
        """
        获取指定交易对的价格

        Args:
            symbol: 交易对符号

        Returns:
            当前价格，失败返回0
        """
        pass

    @abstractmethod
    def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[BaseModel]:
        """
        获取K线数据

        Args:
            symbol: 交易对符号
            interval: 时间间隔
            limit: 数量限制

        Returns:
            K线数据列表
        """
        pass

    @abstractmethod
    def get_all_cur_positions(self) -> List[BaseModel]:
        """
        获取所有当前持仓

        Returns:
            仓位列表
        """
        pass

    @abstractmethod
    def get_position(self, symbol: str) -> Optional[BaseModel]:
        """
        获取指定交易对的仓位

        Args:
            symbol: 交易对符号

        Returns:
            仓位对象，无仓位返回None
        """
        pass

    @abstractmethod
    def get_available_balance(self, asset: str = "USDT") -> float:
        """
        获取可用余额

        Args:
            asset: 资产类型

        Returns:
            可用余额
        """
        pass

    @abstractmethod
    def make_new_order(self, symbol: str, side: str, order_type: str,
                      quantity: float, price: Optional[float] = None, **kwargs) -> Optional[Dict]:
        """
        下单

        Args:
            symbol: 交易对符号
            side: 买卖方向 (BUY/SELL)
            order_type: 订单类型 (MARKET/LIMIT)
            quantity: 数量
            price: 价格（限价单必需）
            **kwargs: 其他参数

        Returns:
            订单结果，失败返回None
        """
        pass

    @abstractmethod
    def get_funding_rate(self, symbol: str, apy: bool = True) -> float:
        """
        获取资金费率

        Args:
            symbol: 交易对符号
            apy: 是否转换为年化收益率

        Returns:
            资金费率
        """
        pass

    # ========== 通用辅助方法 ==========

    def get_all_tick_price(self) -> List[Dict]:
        """
        获取所有交易对价格
        默认实现，子类可重写优化

        Returns:
            价格列表 [{"name": "BTC", "midPx": 50000}, ...]
        """
        logger.warning(f"{self.exchange_code} 未实现 get_all_tick_price 优化，使用默认方法")
        return []

    def get_pair_max_leverage(self, pair: str) -> int:
        """
        获取交易对最大杠杆
        默认返回保守值，子类应重写

        Args:
            pair: 交易对

        Returns:
            最大杠杆倍数
        """
        return 20  # 默认保守值

    def get_available_margin(self) -> float:
        """
        获取可用保证金
        默认实现，获取USDT余额

        Returns:
            可用保证金
        """
        return self.get_available_balance("USDT")

    def get_total_margin(self) -> float:
        """
        获取总保证金
        默认实现，返回可用余额
        子类应重写实现真实的总保证金计算

        Returns:
            总保证金
        """
        return self.get_available_margin()

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """
        设置杠杆
        默认实现，子类应重写

        Args:
            symbol: 交易对
            leverage: 杠杆倍数

        Returns:
            设置是否成功
        """
        logger.warning(f"{self.exchange_code} 未实现杠杆设置功能")
        return False

    def cancel_all_orders(self, symbol: Optional[str] = None) -> bool:
        """
        取消所有订单
        默认实现，子类应重写

        Args:
            symbol: 交易对，None表示取消所有

        Returns:
            取消是否成功
        """
        logger.warning(f"{self.exchange_code} 未实现取消订单功能")
        return False

    # ========== 缓存管理 ==========

    def _is_cache_valid(self, cache_type: str, key: str) -> bool:
        """检查缓存是否有效"""
        if key not in self._last_cache_update:
            return False

        age = time.time() - self._last_cache_update[key]
        return age < self._cache_ttl.get(cache_type, 1)

    def _update_cache(self, cache_type: str, key: str, value):
        """更新缓存"""
        if cache_type == 'price':
            self._price_cache[key] = value
        elif cache_type == 'orderbook':
            self._orderbook_cache[key] = value

        self._last_cache_update[key] = time.time()

    def _get_cached_price(self, symbol: str) -> Optional[float]:
        """获取缓存的价格"""
        if self._is_cache_valid('price', symbol):
            return self._price_cache.get(symbol)
        return None

    def _get_cached_orderbook(self, symbol: str) -> Optional[BaseModel]:
        """获取缓存的订单簿"""
        if self._is_cache_valid('orderbook', symbol):
            return self._orderbook_cache.get(symbol)
        return None

    # ========== 通用工具方法 ==========

    def _validate_symbol(self, symbol: str) -> str:
        """验证并转换交易对格式"""
        if not symbol:
            raise ValueError("交易对符号不能为空")

        return self.convert_symbol(symbol)

    def _validate_order_params(self, symbol: str, side: str, order_type: str,
                              quantity: float, price: Optional[float] = None):
        """验证下单参数"""
        symbol = self._validate_symbol(symbol)

        if side not in [TradeSide.BUY, TradeSide.SELL]:
            raise ValueError(f"无效的买卖方向: {side}")

        if order_type not in ["MARKET", "LIMIT"]:
            raise ValueError(f"无效的订单类型: {order_type}")

        if quantity <= 0:
            raise ValueError(f"订单数量必须大于0: {quantity}")

        if order_type == "LIMIT" and (price is None or price <= 0):
            raise ValueError(f"限价单必须指定有效价格: {price}")

        return symbol

    def _handle_api_error(self, error: Exception, operation: str, symbol: str = ""):
        """统一处理API错误"""
        error_msg = str(error)

        if "timeout" in error_msg.lower():
            logger.warning(f"{self.exchange_code} {operation} 超时: {symbol}")
        elif "rate limit" in error_msg.lower():
            logger.warning(f"{self.exchange_code} {operation} 触发速率限制: {symbol}")
        elif "invalid" in error_msg.lower() or "symbol" in error_msg.lower():
            logger.error(f"{self.exchange_code} {operation} 无效交易对: {symbol}")
        else:
            logger.error(f"{self.exchange_code} {operation} 失败: {symbol} - {error}")

    def __str__(self):
        return f"{self.exchange_code} (testnet={self.testnet})"

    def __repr__(self):
        return self.__str__()


class FutureExchange(BaseExchange):
    """
    永续合约交易所基类
    继承自BaseExchange，添加永续合约特有的功能
    """

    def __init__(self, exchange_code: ExchangeEnum, testnet: bool = False):
        super().__init__(exchange_code, testnet)

        # 永续合约特有属性
        self.funding_interval = 8 * 3600  # 默认8小时资金费率
        self.default_leverage = 10
        self.max_leverage = 100

        logger.info(f"初始化 {exchange_code} 永续合约交易所")

    @abstractmethod
    def get_funding_rate(self, symbol: str, apy: bool = True) -> float:
        """
        获取永续合约资金费率

        Args:
            symbol: 交易对符号
            apy: 是否转换为年化收益率

        Returns:
            资金费率
        """
        pass

    def calculate_funding_apy(self, rate: float) -> float:
        """
        计算资金费率年化收益率

        Args:
            rate: 单次资金费率

        Returns:
            年化收益率
        """
        # 每年结算次数 = 24小时 / 资金费率间隔
        settlements_per_year = 24 * 3600 / self.funding_interval
        return rate * settlements_per_year * 365

    def get_funding_frequency(self) -> int:
        """获取资金费率收取频率（小时）"""
        return self.funding_interval // 3600

    def set_leverage_with_validation(self, symbol: str, leverage: int) -> bool:
        """
        设置杠杆（带验证）

        Args:
            symbol: 交易对
            leverage: 杠杆倍数

        Returns:
            设置是否成功
        """
        if leverage <= 0 or leverage > self.max_leverage:
            logger.error(f"无效的杠杆倍数: {leverage} (最大: {self.max_leverage})")
            return False

        return self.set_leverage(symbol, leverage)


class SpotExchange(BaseExchange):
    """
    现货交易所基类
    继承自BaseExchange，添加现货特有的功能
    """

    def __init__(self, exchange_code: ExchangeEnum, testnet: bool = False):
        super().__init__(exchange_code, testnet)
        logger.info(f"初始化 {exchange_code} 现货交易所")

    def get_funding_rate(self, symbol: str, apy: bool = True) -> float:
        """
        现货交易没有资金费率，返回0

        Returns:
            0
        """
        return 0.0