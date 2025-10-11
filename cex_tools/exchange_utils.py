# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : exchange_utils
@Description : 交易所通用工具模块
@Time        : 2025/10/5
"""
import time
import asyncio
from typing import List, Dict, Tuple, Optional, Union
from loguru import logger
from cex_tools.base_exchange import BaseExchange
from cex_tools.cex_enum import ExchangeEnum


class ExchangeUtils:
    """
    交易所通用工具类

    提供跨交易所的通用功能：
    - 价格比较
    - 套利机会分析
    - 资金费率比较
    - 交易所状态监控
    """

    @staticmethod
    async def compare_prices(exchanges: List[BaseExchange], symbol: str) -> Dict[ExchangeEnum, float]:
        """
        比较多个交易所的价格

        Args:
            exchanges: 交易所列表
            symbol: 交易对符号

        Returns:
            价格字典 {交易所: 价格}
        """
        prices = {}

        # 并发获取价格
        async def get_all_prices():
            tasks = []
            for exchange in exchanges:
                tasks.append(exchange.get_tick_price(symbol))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                exchange_code = exchanges[i].exchange_code
                if isinstance(result, Exception):
                    logger.error(f"{exchange_code} 价格查询异常: {result}")
                    prices[exchange_code] = 0
                else:
                    prices[exchange_code] = result

        await get_all_prices()

        return prices

    @staticmethod
    async def find_arbitrage_opportunity(exchanges: List[BaseExchange], symbol: str,
                                 min_profit_rate: float = 0.001) -> Optional[Dict]:
        """
        寻找套利机会

        Args:
            exchanges: 交易所列表
            symbol: 交易对符号
            min_profit_rate: 最小利润率

        Returns:
            套利机会字典，无机会返回None
        """
        prices = await ExchangeUtils.compare_prices(exchanges, symbol)

        # 过滤有效价格
        valid_prices = {ex: price for ex, price in prices.items() if price > 0}
        if len(valid_prices) < 2:
            return None

        # 找出最高价和最低价
        max_exchange = max(valid_prices, key=valid_prices.get)
        min_exchange = min(valid_prices, key=valid_prices.get)
        max_price = valid_prices[max_exchange]
        min_price = valid_prices[min_exchange]

        # 计算利润率
        profit_rate = (max_price - min_price) / min_price

        if profit_rate >= min_profit_rate:
            return {
                'symbol': symbol,
                'buy_exchange': min_exchange,
                'sell_exchange': max_exchange,
                'buy_price': min_price,
                'sell_price': max_price,
                'profit_rate': profit_rate,
                'profit_amount': max_price - min_price,
                'all_prices': valid_prices
            }

        return None

    @staticmethod
    async def compare_funding_rates(exchanges: List[BaseExchange], symbols: List[str],
                            apy: bool = True) -> Dict[str, Dict[ExchangeEnum, float]]:
        """
        比较资金费率

        Args:
            exchanges: 交易所列表
            symbols: 交易对列表
            apy: 是否转换为年化

        Returns:
            资金费率字典 {交易对: {交易所: 费率}}
        """
        funding_rates = {}

        for symbol in symbols:
            rates = {}
            # 并发获取所有交易所的费率
            tasks = []
            for exchange in exchanges:
                tasks.append(exchange.get_funding_rate(symbol, apy=apy))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                exchange_code = exchanges[i].exchange_code
                if isinstance(result, Exception):
                    logger.error(f"获取 {exchange_code} {symbol} 资金费率失败: {result}")
                    rates[exchange_code] = 0
                else:
                    rates[exchange_code] = result

            funding_rates[symbol] = rates

        return funding_rates

    @staticmethod
    async def find_funding_arbitrage(exchanges: List[BaseExchange], symbol: str,
                             min_rate_diff: float = 0.01) -> Optional[Dict]:
        """
        寻找资金费率套利机会

        Args:
            exchanges: 交易所列表
            symbol: 交易对符号
            min_rate_diff: 最小费率差

        Returns:
            套利机会字典，无机会返回None
        """
        rates = await ExchangeUtils.compare_funding_rates(exchanges, [symbol], apy=True)
        symbol_rates = rates.get(symbol, {})

        # 过滤有效费率
        valid_rates = {ex: rate for ex, rate in symbol_rates.items() if rate != 0}
        if len(valid_rates) < 2:
            return None

        # 找出最高费率和最低费率
        max_exchange = max(valid_rates, key=valid_rates.get)
        min_exchange = min(valid_rates, key=valid_rates.get)
        max_rate = valid_rates[max_exchange]
        min_rate = valid_rates[min_exchange]

        # 计算费率差
        rate_diff = abs(max_rate - min_rate)

        if rate_diff >= min_rate_diff:
            # 负费率表示做空付费，正费率表示做多付费
            return {
                'symbol': symbol,
                'positive_funding': max_exchange if max_rate > 0 else min_exchange,
                'negative_funding': min_exchange if min_rate < 0 else max_exchange,
                'positive_rate': max(max_rate, min_rate),
                'negative_rate': min(max_rate, min_rate),
                'rate_diff': rate_diff,
                'strategy': 'long_negative_short_positive',
                'all_rates': valid_rates
            }

        return None

    @staticmethod
    def calculate_order_book_depth(orderbook, depth_levels: int = 5) -> Dict[str, float]:
        """
        计算订单簿深度

        Args:
            orderbook: 订单簿对象
            depth_levels: 深度档位

        Returns:
            深度信息
        """
        if not orderbook or not hasattr(orderbook, 'bids') or not hasattr(orderbook, 'asks'):
            return {}

        bids_depth = sum(bid.quantity * bid.price for bid in orderbook.bids[:depth_levels])
        asks_depth = sum(ask.quantity * ask.price for ask in orderbook.asks[:depth_levels])
        total_depth = bids_depth + asks_depth

        spread = orderbook.asks[0].price - orderbook.bids[0].price if orderbook.bids and orderbook.asks else 0
        spread_pct = spread / orderbook.mid_price if orderbook.mid_price > 0 else 0

        return {
            'bid_depth': bids_depth,
            'ask_depth': asks_depth,
            'total_depth': total_depth,
            'spread': spread,
            'spread_pct': spread_pct,
            'levels_analyzed': min(depth_levels, len(orderbook.bids), len(orderbook.asks))
        }

    @staticmethod
    async def monitor_exchange_health(exchanges: List[BaseExchange], timeout: float = 5.0) -> Dict[ExchangeEnum, Dict]:
        """
        监控交易所健康状态

        Args:
            exchanges: 交易所列表
            timeout: 超时时间（秒）

        Returns:
            健康状态字典
        """
        health_status = {}

        async def check_exchange_health(exchange):
            start_time = time.time()
            try:
                # 测试价格查询（异步调用）
                price = await exchange.get_tick_price("BTCUSDT")
                response_time = time.time() - start_time

                status = {
                    'healthy': price > 0,
                    'response_time': response_time,
                    'last_check': time.time(),
                    'error': None,
                    'price': price
                }
            except Exception as e:
                status = {
                    'healthy': False,
                    'response_time': timeout,
                    'last_check': time.time(),
                    'error': str(e),
                    'price': 0
                }

            return exchange.exchange_code, status

        async def check_all_exchanges():
            tasks = []
            for exchange in exchanges:
                tasks.append(check_exchange_health(exchange))

            results = await asyncio.gather(*tasks)

            for exchange_code, status in results:
                health_status[exchange_code] = status

        await check_all_exchanges()

        return health_status

    @staticmethod
    def get_liquidation_risk(position, current_price: float, liquidation_price: float) -> Dict[str, float]:
        """
        计算清算风险

        Args:
            position: 仓位对象
            current_price: 当前价格
            liquidation_price: 清算价格

        Returns:
            风险指标
        """
        if not position or current_price <= 0 or liquidation_price <= 0:
            return {}

        # 计算价格距离
        if hasattr(position, 'position_side'):
            is_long = position.position_side.value if hasattr(position.position_side, 'value') else position.position_side == 'long'
        else:
            # 根据仓位数量判断方向
            is_long = position.positionAmt > 0 if hasattr(position, 'positionAmt') else True

        if is_long:
            # 多头：清算价格 < 当前价格
            price_distance = (current_price - liquidation_price) / current_price
        else:
            # 空头：清算价格 > 当前价格
            price_distance = (liquidation_price - current_price) / current_price

        # 风险等级
        if price_distance > 0.2:
            risk_level = 'low'
        elif price_distance > 0.1:
            risk_level = 'medium'
        elif price_distance > 0.05:
            risk_level = 'high'
        else:
            risk_level = 'critical'

        return {
            'price_distance_pct': price_distance,
            'risk_level': risk_level,
            'is_liquidating_soon': price_distance < 0.02,
            'current_price': current_price,
            'liquidation_price': liquidation_price
        }

    @staticmethod
    def format_currency(amount: float, currency: str = "USDT", decimal_places: int = 2) -> str:
        """
        格式化货币显示

        Args:
            amount: 金额
            currency: 货币单位
            decimal_places: 小数位数

        Returns:
            格式化字符串
        """
        if amount >= 1000000:
            return f"${amount/1000000:.{decimal_places}f}M {currency}"
        elif amount >= 1000:
            return f"${amount/1000:.{decimal_places}f}K {currency}"
        else:
            return f"${amount:.{decimal_places}f} {currency}"

    @staticmethod
    def calculate_position_size(base_amount: float, leverage: int, risk_ratio: float = 0.02) -> float:
        """
        计算仓位大小

        Args:
            base_amount: 基础资金
            leverage: 杠杆倍数
            risk_ratio: 风险比例

        Returns:
            建议仓位大小
        """
        return base_amount * leverage * risk_ratio

    @staticmethod
    def get_trading_signal(prices: Dict[ExchangeEnum, float], orderbooks: Dict[ExchangeEnum, object],
                         min_spread: float = 0.002) -> Optional[Dict]:
        """
        生成交易信号

        Args:
            prices: 价格字典
            orderbooks: 订单簿字典
            min_spread: 最小价差

        Returns:
            交易信号
        """
        if len(prices) < 2 or len(orderbooks) < 2:
            return None

        # 找出最佳买入和卖出价格
        exchanges = list(prices.keys())
        best_buy_exchange = min(exchanges, key=lambda x: prices[x])
        best_sell_exchange = max(exchanges, key=lambda x: prices[x])

        buy_price = prices[best_buy_exchange]
        sell_price = prices[best_sell_exchange]
        spread = (sell_price - buy_price) / buy_price

        if spread >= min_spread:
            # 检查订单簿深度
            buy_depth = ExchangeUtils.calculate_order_book_depth(orderbooks[best_buy_exchange])
            sell_depth = ExchangeUtils.calculate_order_book_depth(orderbooks[best_sell_exchange])

            return {
                'signal': 'arbitrage',
                'buy_exchange': best_buy_exchange,
                'sell_exchange': best_sell_exchange,
                'buy_price': buy_price,
                'sell_price': sell_price,
                'spread_pct': spread,
                'buy_depth': buy_depth.get('total_depth', 0),
                'sell_depth': sell_depth.get('total_depth', 0),
                'confidence': min(spread / min_spread, 1.0)
            }

        return None