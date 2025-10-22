# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : async_exchange_adapter
@Description : 异步交易所适配器，将同步交易所包装为异步接口
@Time        : 2025/10/6 09:45
"""
import asyncio
from typing import Dict, List, Optional, Any, Union
from loguru import logger
from cex_tools.exchange_model.order_model import BaseOrderModel
from utils.coroutine_utils import safe_execute_async


class AsyncExchangeAdapter:
    """
    异步交易所适配器
    统一将同步和异步交易所包装为异步接口
    """

    def __init__(self, exchange, exchange_code: str):
        """
        初始化适配器

        Args:
            exchange: 交易所实例（同步或异步）
            exchange_code: 交易所代码
        """
        self.exchange = exchange
        self.exchange_code = exchange_code

    async def _call_method(self, method_name: str, *args, **kwargs):
        """
        统一调用方法，自动处理同步/异步
        同步方法使用 asyncio.to_thread 在线程池中执行，避免阻塞事件循环

        Args:
            method_name: 方法名
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            方法执行结果
        """
        if not hasattr(self.exchange, method_name):
            raise AttributeError(f"{self.exchange_code} 没有方法: {method_name}")

        method = getattr(self.exchange, method_name)
        # return await safe_execute_async(method, *args, **kwargs)
        # 检查方法是否为异步
        if asyncio.iscoroutinefunction(method):
            # 异步方法直接调用
            return await method(*args, **kwargs)
        else:
            # 同步方法使用 asyncio.to_thread 在线程池中执行，避免阻塞事件循环
            return await asyncio.to_thread(method, *args, **kwargs)

    # ========== 异步接口方法 ==========

    async def init(self):
        """初始化交易所"""
        if hasattr(self.exchange, 'init'):
            return await self._call_method('init')
        else:
            # 如果交易所没有init方法，直接返回
            return None

    async def close(self):
        """关闭连接"""
        if hasattr(self.exchange, 'close'):
            return await self._call_method('close')
        else:
            return None

    async def convert_size(self, symbol: str, size: float) -> float:
        """获取最新价格"""
        return await self._call_method('convert_size', symbol, size)

    async def get_tick_price(self, symbol: str) -> float:
        """获取最新价格"""
        return await self._call_method('get_tick_price', symbol)

    async def get_all_tick_price(self, symbol: str = None) -> List[Dict]:
        """获取所有价格"""
        return await self._call_method('get_all_tick_price', symbol)

    async def get_all_cur_positions(self) -> List[Dict]:
        """获取所有当前仓位"""
        return await self._call_method('get_all_cur_positions')

    async def get_position(self, symbol: str) -> Optional[Dict]:
        """获取指定交易对仓位"""
        return await self._call_method('get_position', symbol)

    async def get_available_margin(self) -> float:
        """获取可用保证金"""
        return await self._call_method('get_available_margin')

    async def get_total_margin(self) -> float:
        """获取总保证金"""
        return await self._call_method('get_total_margin')

    async def get_available_balance(self, asset: str = "USDT") -> float:
        """获取可用余额"""
        return await self._call_method('get_available_balance', asset)

    async def make_new_order(self, symbol: str, side: str, order_type: str,
                           amount: float, price: float, **kwargs) -> Dict:
        """创建新订单"""
        return await self._call_method('make_new_order', symbol, side,
                                     order_type, amount, price, **kwargs)

    async def cancel_order(self, symbol: str, order_id: str) -> Dict:
        """取消订单"""
        return await self._call_method('cancel_order', symbol, order_id)

    async def cancel_all_orders(self, symbol: str, **kwargs) -> Dict:
        """取消订单"""
        return await self._call_method('cancel_all_orders', symbol, **kwargs)

    async def get_open_orders(self, symbol: str, **kwargs) -> List[Dict]:
        """获取当前交易对活跃订单列表"""
        return await self._call_method('get_open_orders', symbol, **kwargs)

    async def get_orders(self, symbol: str, **kwargs) -> List[Dict]:
        """获取订单列表"""
        return await self._call_method('get_orders', symbol, **kwargs)

    async def get_recent_order(self, symbol: str, orderId=None, **kwargs) -> BaseOrderModel:
        """获取最近订单"""
        return await self._call_method('get_recent_order', symbol, orderId=orderId, **kwargs)

    async def get_history_order(self, symbol: str, **kwargs) -> List[Dict]:
        """获取历史订单"""
        return await self._call_method('get_history_order', symbol, **kwargs)

    async def get_account_info(self) -> Dict:
        """获取账户信息"""
        return await self._call_method('get_account_info')

    async def get_pair_info(self, pair: str) -> Dict:
        """获取交易对信息"""
        return await self._call_method('get_pair_info', pair)

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List:
        """获取K线数据"""
        return await self._call_method('get_klines', symbol, interval, limit)

    async def get_funding_rate(self, symbol: str, apy: bool = True) -> float:
        """获取资金费率"""
        return await self._call_method('get_funding_rate', symbol, apy)

    async def get_funding_rate_history(self, symbol: str, limit: int = 100,
                                      start_time: int = None, end_time: int = None,
                                      apy: bool = True):
        """获取交易品种的历史资金费率"""
        return await self._call_method('get_funding_rate_history',
                                     symbol, limit, start_time, end_time, apy)

    async def get_funding_history(self, symbol: str = None, limit: int = 100,
                                 start_time: int = None, end_time: int = None):
        """获取用户仓位收取的资金费历史记录"""
        return await self._call_method('get_funding_history',
                                     symbol, limit, start_time, end_time)

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """设置杠杆"""
        return await self._call_method('set_leverage', symbol, leverage)

  
    async def get_cross_margin_ratio(self) -> float:
        """获取维持保证金比例"""
        return await self._call_method('get_cross_margin_ratio')

    async def erc20_deposit_addr(self) -> str:
        """获取ERC20充值地址"""
        if hasattr(self.exchange, 'erc20_deposit_addr'):
            return await self._call_method('erc20_deposit_addr')
        return ""

    # ========== 属性访问代理 ==========

    def __getattr__(self, name: str):
        """代理访问原始交易所的属性"""
        if hasattr(self.exchange, name):
            return getattr(self.exchange, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __str__(self):
        return f"AsyncExchangeAdapter({self.exchange_code})"

    def __repr__(self):
        return self.__str__()


class AsyncExchangeFactory:
    """
    异步交易所工厂
    创建统一的异步交易所接口
    """

    @staticmethod
    def create_async_exchange(exchange, exchange_code: str) -> AsyncExchangeAdapter:
        """
        创建异步交易所适配器

        Args:
            exchange: 交易所实例
            exchange_code: 交易所代码

        Returns:
            异步交易所适配器
        """
        return AsyncExchangeAdapter(exchange, exchange_code)

    @staticmethod
    async def create_multiple_async_exchanges(exchanges: Dict[str, Any]) -> Dict[str, AsyncExchangeAdapter]:
        """
        批量创建异步交易所适配器

        Args:
            exchanges: 交易所字典 {exchange_code: exchange_instance}

        Returns:
            异步交易所适配器字典
        """
        async_exchanges = {}

        for code, exchange in exchanges.items():
            async_exchange = AsyncExchangeAdapter(exchange, code)
            async_exchanges[code] = async_exchange

            # 初始化交易所（如果需要）
            try:
                await async_exchange.init()
            except AttributeError as e:
                if "没有方法: init" not in str(e):
                    logger.error(f"❌ 异步适配器初始化失败: {code} - {e}")
            except Exception as e:
                logger.error(f"❌ 异步适配器初始化失败: {code} - {e}")

        return async_exchanges

