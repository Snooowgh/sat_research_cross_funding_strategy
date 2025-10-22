# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : binance_unified_future
@Description : 基于Binance Portfolio Margin SDK的统一账户适配器
@Time        : 2025/10/17
"""
import time
import ccxt
import requests
from loguru import logger
from typing import Dict, List, Optional, Any, Union

from cex_tools.binance_future import BinanceFuture
from cex_tools.exchange_model.position_model import BinancePositionDetail, BinanceUnifiedPositionDetail
from cex_tools.exchange_model.order_model import BinanceOrder, BinanceOrderStatus, BinanceUnifiedOrder
from cex_tools.exchange_model.kline_bar_model import BinanceKlineBar
from cex_tools.exchange_model.orderbook_model import BinanceOrderBook
from cex_tools.exchange_model.funding_rate_model import FundingRateHistory, FundingHistory, FundingRateHistoryResponse, \
    FundingHistoryResponse
from cex_tools.cex_enum import ExchangeEnum, TradeSide
from cex_tools.funding_rate_cache import FundingRateCache
from utils.decorators import timed_cache
from utils.notify_tools import send_slack_message
from binance_common.configuration import ConfigurationRestAPI
from binance_common.constants import DERIVATIVES_TRADING_PORTFOLIO_MARGIN_REST_API_PROD_URL
from binance_sdk_derivatives_trading_portfolio_margin.derivatives_trading_portfolio_margin import DerivativesTradingPortfolioMargin
from binance_sdk_derivatives_trading_portfolio_margin.rest_api.models import AccountInformationResponse


class BinanceUnifiedFuture:
    """
    基于Binance Portfolio Margin SDK的统一账户适配器

    主要特性：
    - 使用官方Binance Portfolio Margin SDK
    - 支持统一保证金账户（Portfolio Margin）
    - 跨交易所保证金共享
    - 更高效的资金利用
    - 支持现货、合约、期权等多产品交易
    - 自动重连和错误处理
    - 统一的API接口
    """

    def __init__(self, key=None, secret=None, erc20_deposit_addr="", maker_fee_rate=0.00018,
                 taker_fee_rate=0.00045, recvWindow=5000, timeout=10000, testnet=False, **kwargs):
        """
        初始化币安统一账户客户端

        Args:
            key: API密钥
            secret: API密钥密码
            erc20_deposit_addr: ERC20充值地址
            maker_fee_rate: Maker手续费率
            taker_fee_rate: Taker手续费率
            recvWindow: 接收窗口时间
            timeout: 请求超时时间
            testnet: 是否使用测试网
        """
        self.exchange_code = ExchangeEnum.BINANCE_UNIFIED
        self.future_base_url = "https://fapi.binance.com"
        self.api_key = key
        self.api_secret = secret
        configuration = ConfigurationRestAPI(api_key=self.api_key, api_secret=self.api_secret,
                                             base_path=DERIVATIVES_TRADING_PORTFOLIO_MARGIN_REST_API_PROD_URL,
                                             timeout=5)
        self.client = DerivativesTradingPortfolioMargin(config_rest_api=configuration)
        self.rest_client = self.client.rest_api
        if self.rest_client.get_um_current_position_mode().data().dual_side_position is True:
            logger.info(f"{self.exchange_code} 当前账户双向持仓模式，已切换为单向持仓模式")
            self.rest_client.change_um_position_mode("false")
        self.testnet = testnet

        # 配置参数
        self.erc20_deposit_addr = erc20_deposit_addr
        self.maker_fee_rate = maker_fee_rate
        self.taker_fee_rate = taker_fee_rate
        self.recvWindow = recvWindow
        self.timeout = timeout

        # 所有公共信息可以直接用原有接口
        self.base_binance_future = BinanceFuture()

    def __convert_to_ccxt_symbol(self, symbol: str) -> str:
        """转换为CCXT格式的交易对符号"""
        if symbol.endswith("USDT"):
            return symbol[0:-4] + "/USDT:USDT"
        elif symbol.endswith("USDC"):
            return symbol[0:-4] + "/USDC:USDC"
        return symbol

    def convert_symbol(self, symbol: str) -> str:
        """
        标准化交易对格式
        :param symbol: BTCUSDT
        :return: BTCUSDT
        """
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        return symbol

    def get_pair_info(self, pair):
        """获取交易对信息 - 使用基础Binance接口"""
        return self.base_binance_future.get_pair_info(pair)

    def convert_size(self, symbol: str, size: float) -> float:
        """转换数量精度 - 使用基础Binance接口"""
        return self.base_binance_future.convert_size(symbol, size)

    def convert_price(self, symbol: str, price: float) -> str:
        """转换价格精度 - 使用基础Binance接口"""
        return self.base_binance_future.convert_price(symbol, price)

    def get_all_cur_positions(self):
        """获取所有当前仓位 - 使用Portfolio Margin SDK"""
        try:
            # 根据账户类型选择合适的端点
            positions = self.rest_client.query_um_position_information().data()
            # Portfolio Margin SDK返回的数据格式示例:
            # [QueryUmPositionInformationResponse(entry_price='0.30923', leverage='5', mark_price='0.3092429',
            #  max_notional_value='6000000.0', position_amt='19.0', notional='5.8756151',
            #  symbol='TRXUSDT', un_realized_profit='0.0002451', liquidation_price='0',
            #  position_side='BOTH', update_time=1760718563795, additional_properties={})]

            if positions:
                # 过滤有仓位的数据
                active_positions = list(filter(lambda pos: float(pos.position_amt or 0) != 0, positions))
                return [BinanceUnifiedPositionDetail(pos, self.exchange_code) for pos in active_positions]
            return []
        except Exception as e:
            logger.error(f"{self.exchange_code} 获取仓位失败: {e}")
            return []

    def get_position(self, symbol: str) -> Optional[BinanceUnifiedPositionDetail]:
        """获取指定交易对仓位 - 使用Portfolio Margin SDK"""
        symbol = self.convert_symbol(symbol)
        try:
            positions = self.rest_client.query_um_position_information(symbol).data()
            if positions:
                # 过滤有仓位的数据
                active_positions = list(filter(lambda pos: float(pos.position_amt or 0) != 0, positions))
                if active_positions:
                    return BinanceUnifiedPositionDetail(active_positions[0], self.exchange_code)
            return None
        except Exception as e:
            logger.error(f"{self.exchange_code} 获取{symbol}仓位失败: {e}")
            return None

    @timed_cache(timeout=3)
    def get_balance_list_cache(self):
        """获取账户余额缓存"""
        try:
            # 根据账户类型选择合适的端点
            balance = self.rest_client.account_balance().data().actual_instance
            return balance
        except Exception as e:
            logger.error(f"{self.exchange_code} 获取余额失败: {e}")
            return []

    @timed_cache(timeout=3)
    def _get_account_info(self):
        return self.rest_client.account_information().data()

    def get_available_balance(self, asset: str = "USDT") -> float:
        """获取可用余额"""
        try:
            balance_list = self.get_balance_list_cache()
            for info in balance_list:
                if info.asset == asset:
                    # 统一账户使用availableBalance
                    return float(info.cross_margin_free)
            return 0
        except Exception as e:
            logger.error(f"{self.exchange_code} 获取{asset}可用余额失败: {e}")
            return 0

    def get_available_margin(self) -> float:
        """获取可用保证金"""
        return float(self._get_account_info().total_available_balance)

    def get_total_margin(self) -> float:
        """获取总保证金"""
        try:
            # 根据账户类型选择合适的端点
            return float(self._get_account_info().account_equity)
        except Exception as e:
            logger.error(f"{self.exchange_code} 获取总保证金失败: {e}")
            return 0

    def get_all_tick_price(self, symbol: str = None) -> Union[float, List[Dict]]:
        """获取价格信息 - 使用基础Binance接口"""
        return self.base_binance_future.get_all_tick_price(symbol)

    def get_tick_price(self, symbol: str) -> float:
        """获取最新价格"""
        return self.get_all_tick_price(symbol)

    def get_klines(self, symbol: str, interval: str, limit: int = 500) -> List[BinanceKlineBar]:
        """获取K线数据 - 使用基础Binance接口"""
        return self.base_binance_future.get_klines(symbol, interval, limit)

    def make_new_order(self, symbol: str, side: str, order_type: str, quantity: float,
                      price: float = None, msg: str = "", **kwargs) -> Dict:
        """创建新订单 - 使用Portfolio Margin SDK"""
        try:
            # 确保symbol格式正确
            symbol = self.convert_symbol(symbol)
            # 处理数量精度
            quantity = self.convert_size(symbol, quantity)
            order_type = order_type.upper()
            side = side.upper()
            # 限价单需要价格
            if order_type.upper() == "LIMIT" and price is not None:
                price = float(self.convert_price(symbol, price))
                time_in_force="GTC"
            else:
                time_in_force = None
                price = None

            # 止损单需要stopPrice
            if "stopPrice" in kwargs and kwargs["stopPrice"] is not None:
                stopPrice = float(self.convert_price(symbol, kwargs["stopPrice"]))

            start_time = time.time()
            # 调用Portfolio Margin SDK下单
            response = self.rest_client.new_um_order(symbol=symbol,
                                                     side=side,
                                                     type=order_type,
                                                     time_in_force=time_in_force,
                                                     quantity=quantity,
                                                     reduce_only=kwargs.get("reduce_only", False),
                                                     price=price)

            content = f"⚠️ {msg} {self.exchange_code} 下单: {symbol} {side} {order_type} {price} {quantity} " \
                      f"{(time.time() - start_time) * 1000:.2f}ms"
            logger.info(content)
            if msg:
                send_slack_message(content)
            return {"orderId": response.data().order_id}

        except Exception as e:
            logger.error(f"Binance Unified下单失败: {e}")
            raise

    def cancel_order(self, symbol: str, orderId: Union[str, int]) -> Dict:
        """取消订单 - 使用Portfolio Margin SDK"""
        try:
            symbol = self.convert_symbol(symbol)
            response = self.rest_client.cancel_um_order(symbol=symbol, order_id=orderId)
            return response.data() if hasattr(response, 'data') else response
        except Exception as e:
            logger.error(f"binance unified取消订单失败: {e}")
            raise

    def get_open_orders(self, symbol: str, limit: int = 10) -> List[BinanceOrder]:
        """获取当前活跃订单 - 使用Portfolio Margin SDK"""
        try:
            symbol = self.convert_symbol(symbol)
            # 使用Portfolio Margin SDK获取活跃订单
            response = self.rest_client.query_all_um_orders(symbol)
            orders = response.data() if hasattr(response, 'data') else response

            return [BinanceUnifiedOrder(o) for o in orders]

        except Exception as e:
            raise e

    def cancel_all_orders(self, symbol: str) -> None:
        """取消所有订单"""
        try:
            symbol = self.convert_symbol(symbol)
            return self.rest_client.cancel_all_um_open_orders(symbol).data()
        except Exception as e:
            logger.error(f"{self.exchange_code} 取消所有订单失败: {symbol} - {e}")

    def get_recent_order(self, symbol: str, orderId: Union[str, int] = None) -> Optional[BinanceUnifiedOrder]:
        """获取最近订单 - 使用Portfolio Margin SDK"""
        symbol = self.convert_symbol(symbol)
        try:
            data = self.rest_client.query_um_order(symbol, order_id=orderId).data()
            # Portfolio Margin SDK返回的数据格式示例:
            # avg_price='0.00000' client_order_id='6QhLyprDuvfMnARmWjM36' cum_quote='0.00000' executed_qty='0.000'
            # order_id=794470752033 orig_qty='0.001' orig_type='LIMIT' price='105000.00' reduce_only=False
            # side='BUY' position_side='BOTH' status='NEW' symbol='BTCUSDT' time=1760716785029
            # time_in_force='GTC' type='LIMIT' update_time=1760716785029 self_trade_prevention_mode='EXPIRE_MAKER'
            # good_till_date=0 price_match='NONE' additional_properties={}

            if data:
                return BinanceUnifiedOrder(data)
            return None

        except Exception as e:
            logger.error(f"{self.exchange_code} 获取最近订单失败: {symbol} {orderId} - {e}")
            return None


    @timed_cache(timeout=60)
    def get_funding_rate(self, symbol: str, apy: bool = True) -> float:
        """获取资金费率 - 使用基础Binance接口"""
        return self.base_binance_future.get_funding_rate(symbol, apy)

    def get_order_books(self, symbol: str, limit: int = 20) -> BinanceOrderBook:
        """获取订单簿 - 使用基础Binance接口"""
        return self.base_binance_future.get_order_books(symbol, limit)

    def set_leverage(self, symbol: str, leverage: int) -> Dict:
        """设置杠杆倍数 - 使用基础Binance接口"""
        return self.base_binance_future.set_leverage(symbol, leverage)

    def get_cross_margin_ratio(self) -> float:
        """获取维持保证金比例 - 使用Portfolio Margin"""
        # 使用Portfolio Margin SDK获取账户信息
        account_info = self._get_account_info()
        total_maint_margin = float(account_info.account_maint_margin)
        total_wallet_balance = float(account_info.account_equity)

        if total_wallet_balance == 0:
            return 0
        return total_maint_margin / total_wallet_balance

    def get_funding_rate_history(self, symbol: str, limit: int = 100,
                                 start_time: int = None, end_time: int = None,
                                 apy: bool = True) -> FundingRateHistoryResponse:
        """获取历史资金费率 - 使用基础Binance接口"""
        return self.base_binance_future.get_funding_rate_history(symbol, limit, start_time, end_time, apy)

    def get_funding_history(self, symbol: str = None, limit: int = 100,
                            start_time: int = None, end_time: int = None) -> FundingHistoryResponse:
        """获取用户资金费历史 - 使用基础Binance接口"""
        return self.base_binance_future.get_funding_history(symbol, limit, start_time, end_time)

    def close(self):
        """关闭连接"""
        if hasattr(self, 'session'):
            self.session.close()
        # 关闭基础Binance客户端
        if hasattr(self.base_binance_future, 'close'):
            self.base_binance_future.close()

    def __str__(self):
        return f"BinanceUnifiedFuture({self.exchange_code})"

    def __repr__(self):
        return self.__str__()

    # 补充其他常用方法，使用基础Binance接口
    def get_open_interest(self, symbol: str, usd: bool = True) -> float:
        """获取持仓量 - 使用基础Binance接口"""
        return self.base_binance_future.get_open_interest(symbol, usd)

    def convert_order_qty_to_size(self, symbol: str, order_qty: float) -> float:
        """转换订单数量 - 使用基础Binance接口"""
        return self.base_binance_future.convert_order_qty_to_size(symbol, order_qty)

    def get_orders_by_type(self, symbol: str, order_type: str = None, status: str = None) -> List[BinanceOrder]:
        """根据类型获取订单 - 使用基础Binance接口"""
        return self.base_binance_future.get_orders_by_type(symbol, order_type, status)

    def get_history_order(self, pair: str):
        """获取历史订单 - 使用基础Binance接口"""
        return self.rest_client.query_all_um_orders(pair)

    def get_position_max_open_usd_amount(self, pair: str = None) -> float:
        """获取最大开仓金额 - 使用基础Binance接口"""
        pass


if __name__ == '__main__':
    a = BinanceUnifiedFuture(key="JysfgWvPEc04s7SwdNmLscEOAMecCTPNMjr6NCBN3kQPz9lvpRNP44BoG4Z5aOrB",
                            secret="ZGFMevlQlx66pZ6OY4lUmip2G5Lz4zHztSGrhTsq4gOvQIb0UTnhMAn8JrRcIl6H")
    # print(a.make_new_order("TRX", "SELL", "MARKET", 19))
    # print(a.rest_client.account_information().data())
    # print(a.get_all_cur_positions())
    # 交易相关测试
    # symbol = "BTC"
    # o = a.make_new_order(symbol, "BUY", "LIMIT", 0.001, 105000)
    # print(o)
    # print(a.get_open_orders(symbol))
    # print(a.get_recent_order(symbol, o["orderId"]))
    # print(a.cancel_all_orders(symbol))

    # print(a.rest_client.account_balance().data())
    # print(a.get_available_margin())
    # print(a.get_total_margin())
