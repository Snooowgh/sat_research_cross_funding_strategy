# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : bybit_future
@Description : Bybit永续合约交易所封装
@Time        : 2025/10/4
"""
import time
import ccxt
from loguru import logger
from pybit.unified_trading import HTTP

from cex_tools.cex_enum import ExchangeEnum, TradeSide
from cex_tools.base_exchange import FutureExchange
from cex_tools.exchange_model.position_model import BybitPositionDetail
from cex_tools.exchange_model.order_model import BybitOrder
from cex_tools.exchange_model.kline_bar_model import BybitKlineBar
from cex_tools.exchange_model.orderbook_model import BybitOrderBook
from cex_tools.funding_rate_cache import FundingRateCache
from utils.decorators import timed_cache
from utils.notify_tools import send_slack_message

"""
    Bybit API V5 Documentation
    https://bybit-exchange.github.io/docs/v5/intro

    Features:
    - 支持 USDT 永续合约
    - 资金费率每8小时收取
    - 最大杠杆 100x
    - 支持多种订单类型
    - 完善的 WebSocket 支持
"""


class BybitFuture(FutureExchange):

    def __init__(self, api_key="", secret_key="", erc20_deposit_addr="", testnet=False, **kwargs):
        """
        初始化 Bybit 交易所客户端

        Args:
            api_key: API密钥
            secret_key: 密钥
            erc20_deposit_addr: ERC20充值地址（暂未使用）
            testnet: 是否使用测试网络
            **kwargs: 其他参数
        """
        # 初始化基类
        super().__init__(ExchangeEnum.BYBIT, testnet)

        # 存储认证信息
        self.api_key = api_key
        self.secret_key = secret_key
        self.erc20_deposit_addr = erc20_deposit_addr
        self.exchange_code = ExchangeEnum.BYBIT
        # 设置 Bybit 特有参数
        self.funding_interval = 8 * 3600  # 8小时资金费率
        self.maker_fee_rate = 0.0001  # Bybit maker fee
        self.taker_fee_rate = 0.0006  # Bybit taker fee
        self.max_leverage = 100  # 最大杠杆100x

        # 初始化 Bybit 客户端
        self._init_clients(api_key, secret_key, testnet)
        self.pair_info = {}
        self.load_pair_info()
        logger.info(f"Bybit 交易所初始化完成 (testnet={testnet})")

    def _init_clients(self, api_key: str, secret_key: str, testnet: bool):
        """
        初始化 API 客户端

        Args:
            api_key: API密钥
            secret_key: 密钥
            testnet: 是否使用测试网络
        """
        try:
            # 主要客户端：使用 pybit 官方SDK
            self.client = HTTP(
                testnet=testnet,
                api_key=api_key,
                api_secret=secret_key,
            )

            # 备用客户端：使用 ccxt
            self.ccxt_exchange = ccxt.bybit({
                "apiKey": api_key,
                "secret": secret_key,
                "options": {
                    "defaultType": "linear",  # linear for USDT perpetual
                }
            })
            if testnet:
                self.ccxt_exchange.set_sandbox_mode(True)

            logger.debug("Bybit API 客户端初始化成功")

        except Exception as e:
            logger.error(f"Bybit 客户端初始化失败: {e}")
            raise

    def load_pair_info(self):
        """
        获取交易对信息
        :param symbol: 交易对
        :return: 交易对信息字典
        """
        result = self.client.get_instruments_info(
            category="linear"
        )
        instruments = result.get("result", {}).get("list", [])
        for inst in instruments:
            self.pair_info[inst["symbol"]] = inst

    def convert_size(self, symbol, size):
        """
        转换下单数量精度 - 高性能版本，使用缓存
        根据交易对的qtyStep字段格式化数量精度
        :param symbol: 交易对
        :param size: 数量
        :return: 格式化后的数量
        """
        # 使用缓存的交易对信息，避免实时API调用
        pair_info = self.pair_info.get(symbol)
        if not pair_info:
            return size

        # Bybit使用lotSizeFilter中的qtyStep字段
        lot_size_filter = pair_info.get("lotSizeFilter", {})
        qty_step = float(lot_size_filter.get("qtyStep", "0.001"))

        # 使用qtyStep对数量进行取整
        if qty_step > 0:
            rounded_size = round(size / qty_step) * qty_step
            # 避免浮点数精度问题，保留合理的小数位数
            return float(f"{rounded_size:.8g}")
        else:
            return size

    def convert_symbol(self, symbol):
        """
        标准化交易对格式
        :param symbol: BTCUSDT
        :return: BTCUSDT (Bybit使用相同格式)
        """
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        return symbol

    def set_leverage(self, symbol, leverage=10):
        """
        设置杠杆
        :param symbol: 交易对
        :param leverage: 杠杆倍数
        :return: 设置结果
        """
        try:
            symbol = self.convert_symbol(symbol)
            result = self.client.set_leverage(
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage)
            )
            logger.info(f"设置 {symbol} 杠杆为 {leverage}x: {result}")
            return result
        except Exception as e:
            logger.warning(f"设置{symbol} {leverage}x杠杆出错: {e}")
            return None

    def get_all_cur_positions(self):
        """获取所有当前持仓"""
        try:
            result = self.client.get_positions(
                category="linear",
                settleCoin="USDT"
            )
            positions = result.get("result", {}).get("list", [])
            # 过滤掉零仓位
            positions = [BybitPositionDetail(p) for p in positions if float(p.get("size", 0)) != 0]
            return positions
        except Exception as e:
            logger.error(f"获取Bybit仓位失败: {e}")
            return []

    def get_position(self, symbol):
        """
        获取指定交易对的仓位
        :param symbol: 交易对
        :return: BybitPositionDetail 或 None
        """
        try:
            symbol = self.convert_symbol(symbol)
            result = self.client.get_positions(
                category="linear",
                symbol=symbol
            )
            positions = result.get("result", {}).get("list", [])
            if positions and float(positions[0].get("size", 0)) != 0:
                return BybitPositionDetail(positions[0])
            return None
        except Exception as e:
            logger.error(f"获取 {symbol} 仓位失败: {e}")
            return None

    @timed_cache(timeout=3)
    def get_wallet_balance_cache(self):
        """缓存钱包余额查询"""
        try:
            result = self.client.get_wallet_balance(
                accountType="UNIFIED",  # 统一账户
                coin="USDT"
            )
            return result
        except Exception as e:
            logger.error(f"获取Bybit余额失败: {e}")
            return {}

    def get_available_balance(self, asset=None):
        """
        获取可用余额
        :param asset: 资产类型（默认USDT）
        :return: 可用余额
        """
        if asset is None:
            asset = ["USDT", "USDC"]
        ret = 0
        try:
            wallet_data = self.get_wallet_balance_cache()
            coins = wallet_data.get("result", {}).get("list", [{}])[0].get("coin", [])
            for coin in coins:
                if coin.get("coin") in asset:
                    r = coin.get("availableToWithdraw")
                    if r:
                        ret += float(r)
            return ret
        except Exception as e:
            logger.error(f"获取{asset}可用余额失败: {e}")
            return ret

    def get_available_margin(self):
        """获取可用保证金"""
        return self.get_available_balance(["USDT", "USDC"])

    def get_tick_price(self, symbol):
        """
        获取最新价格
        :param symbol: 交易对
        :return: 最新价格
        """
        try:
            symbol = self.convert_symbol(symbol)
            result = self.client.get_tickers(
                category="linear",
                symbol=symbol
            )
            tickers = result.get("result", {}).get("list", [])
            if tickers:
                return float(tickers[0].get("lastPrice", 0))
            return 0
        except Exception as e:
            logger.error(f"获取 {symbol} 价格失败: {e}")
            return 0

    def get_all_tick_price(self):
        """获取所有USDT合约的价格"""
        try:
            result = self.client.get_tickers(category="linear")
            tickers = result.get("result", {}).get("list", [])
            ret = []
            for ticker in tickers:
                symbol = ticker.get("symbol", "")
                if symbol.endswith("USDT"):
                    ret.append({
                        "name": symbol.replace("USDT", ""),
                        "midPx": float(ticker.get("lastPrice", 0))
                    })
            return ret
        except Exception as e:
            logger.error(f"获取所有价格失败: {e}")
            return []

    def get_orderbooks(self, symbol, limit=50):
        """
        获取订单簿
        :param symbol: 交易对
        :param limit: 深度（1, 50, 200, 500）
        :return: BybitOrderBook
        """
        try:
            symbol = self.convert_symbol(symbol)
            result = self.client.get_orderbook(
                category="linear",
                symbol=symbol,
                limit=limit
            )
            orderbook_data = result.get("result", {})
            return BybitOrderBook(orderbook_data)
        except Exception as e:
            logger.error(f"获取 {symbol} 订单簿失败: {e}")
            return None

    def get_klines(self, symbol, interval, limit=200):
        """
        获取K线数据
        :param symbol: 交易对
        :param interval: 时间间隔（1, 3, 5, 15, 30, 60, 120, 240, 360, 720, D, W, M）
        :param limit: 数量限制（最多1000）
        :return: K线列表
        """
        try:
            symbol = self.convert_symbol(symbol)
            result = self.client.get_kline(
                category="linear",
                symbol=symbol,
                interval=interval,
                limit=limit
            )
            klines = result.get("result", {}).get("list", [])
            return [BybitKlineBar(k) for k in klines]
        except Exception as e:
            logger.error(f"获取 {symbol} K线失败: {e}")
            return []

    def get_total_margin(self):
        """
        获取总保证金
        :return: 总保证金（USDT）
        """
        try:
            wallet_data = self.get_wallet_balance_cache()
            accounts = wallet_data.get("result", {}).get("list", [])
            if accounts:
                return float(accounts[0].get("totalEquity", 0))
            return 0
        except Exception as e:
            logger.error(f"获取总保证金失败: {e}")
            return 0

    def make_new_order(self, symbol, side, order_type, quantity, price=None, **kwargs):
        """
        下单
        :param symbol: 交易对
        :param side: 方向（BUY/SELL）
        :param order_type: 订单类型（MARKET/LIMIT）
        :param quantity: 数量
        :param price: 价格（限价单必填）
        :return: 订单信息
        """
        try:
            symbol = self.convert_symbol(symbol)

            # 转换方向
            bybit_side = "Buy" if side == TradeSide.BUY else "Sell"

            # 转换订单类型
            bybit_order_type = "Market" if order_type == "MARKET" else "Limit"

            # 下单参数
            order_params = {
                "category": "linear",
                "symbol": symbol,
                "side": bybit_side,
                "orderType": bybit_order_type,
                "qty": str(quantity),
            }

            # 限价单需要价格
            if bybit_order_type == "Limit" and price:
                order_params["price"] = str(price)

            # 添加额外参数
            if "timeInForce" in kwargs:
                order_params["timeInForce"] = kwargs["timeInForce"]

            start_time = time.time()
            result = self.client.place_order(**order_params)

            elapsed = (time.time() - start_time) * 1000
            logger.info(
                f"⚠️ {self.exchange_code} 下单: {symbol} {side} {order_type} {price} {quantity} {elapsed:.2f}ms")

            if result.get("retCode") == 0:
                order_data = result.get("result", {})
                return {"orderId": order_data.get("orderId")}
            else:
                logger.error(f"Bybit下单失败: {result}")
                return None

        except Exception as e:
            logger.error(f"Bybit下单异常: {e}")
            logger.exception(e)
            return None

    def get_recent_order(self, symbol, orderId):
        """
        获取订单详情
        :param symbol: 交易对
        :param orderId: 订单ID
        :return: BybitOrder 或 None
        """
        try:
            symbol = self.convert_symbol(symbol)
            result = self.client.get_order_history(
                category="linear",
                symbol=symbol,
                orderId=orderId
            )
            orders = result.get("result", {}).get("list", [])
            if orders:
                return BybitOrder(orders[0])
            return None
        except Exception as e:
            logger.error(f"获取订单 {orderId} 失败: {e}")
            return None

    def cancel_all_orders(self, symbol=None):
        """
        取消所有订单
        :param symbol: 交易对（None则取消所有）
        """
        try:
            params = {"category": "linear"}
            if symbol:
                params["symbol"] = self.convert_symbol(symbol)

            result = self.client.cancel_all_orders(**params)
            logger.info(f"取消订单: {result}")
            return result
        except Exception as e:
            logger.error(f"取消订单失败: {e}")
            return None

    @timed_cache(timeout=60)
    def get_funding_rate(self, symbol, apy=True):
        """
        获取资金费率
        :param symbol: 交易对
        :param apy: 是否转换为年化
        :return: 资金费率
        """
        # 优先从缓存获取
        cache = FundingRateCache()
        cached_rate = cache.get_funding_rate("bybit", symbol)

        if cached_rate is not None:
            # 缓存中的是单次费率，Bybit每8小时一次，一天3次
            if apy:
                return cached_rate * 3 * 365
            else:
                return cached_rate

        # 缓存未命中，使用原接口
        try:
            symbol = self.convert_symbol(symbol)
            result = self.client.get_tickers(
                category="linear",
                symbol=symbol
            )
            tickers = result.get("result", {}).get("list", [])
            if tickers:
                funding_rate = float(tickers[0].get("fundingRate", 0))
                if apy:
                    # Bybit 每8小时收取一次，一天3次
                    funding_rate *= 3 * 365
                return funding_rate
            return 0
        except Exception as e:
            logger.error(f"获取 {symbol} 资金费率失败: {e}")
            return 0

    def get_pair_max_leverage(self, pair):
        """
        获取交易对最大杠杆
        :param pair: 交易对
        :return: 最大杠杆倍数
        """
        # Bybit USDT永续最大杠杆一般为100x，但需要根据风险等级调整
        # 这里返回保守值
        return 50

    def get_cross_margin_ratio(self):
        """
            维持仓位的保证金比例(达到100%会被清算)
        """
        try:
            wallet_data = self.get_wallet_balance_cache()
            accounts = wallet_data.get("result", {}).get("list", [])

            if not accounts:
                return 0

            account_info = accounts[0]
            total_margin_balance = float(account_info.get("totalMarginBalance", 0))
            total_equity = float(account_info.get("totalEquity", 0))

            if total_equity == 0:
                return 0

            return total_margin_balance / total_equity
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 获取全仓保证金比例失败: {e}")
            return 0


if __name__ == '__main__':
    pass
