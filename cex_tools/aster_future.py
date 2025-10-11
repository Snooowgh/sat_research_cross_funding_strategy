# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : aster_future
@Description : Aster Finance 永续合约交易所实现
@Time        : 2025/10/3
"""
import time
import math
import json
import hashlib
import hmac
from typing import Optional, List
import requests
from loguru import logger
from web3 import Web3
from eth_abi import encode

from cex_tools.exchange_model.position_model import BinancePositionDetail
from cex_tools.exchange_model.order_model import BinanceOrder, BinanceOrderStatus
from cex_tools.exchange_model.orderbook_model import BinanceOrderBook
from cex_tools.exchange_model.kline_bar_model import BinanceKlineBar
from cex_tools.cex_enum import ExchangeEnum, TradeSide
from utils.decorators import timed_cache


class AsterFuture:
    """
    Aster Finance 永续合约交易所

    API文档: https://github.com/asterdex/api-docs
    Base URL: https://fapi.asterdex.com
    """

    def __init__(self, key=None, secret=None, erc20_deposit_addr="", **kwargs):
        self.api_key = key
        self.api_secret = secret
        self.base_url = "https://fapi.asterdex.com"
        self.erc20_deposit_addr = erc20_deposit_addr
        self.exchange_code = ExchangeEnum.ASTER

        # 费率设置
        self.maker_fee_rate = 0.00018
        self.taker_fee_rate = 0.00045

        # 缓存
        self.exchange_info_cache = None
        self.pair_info_map = {}

        # 请求配置
        self.recvWindow = 5000
        self.timeout = 10
        self.load_pair_info_map()
        logger.info(f"[{self.exchange_code}] 初始化完成")

    def _generate_signature_hmac(self, query_string: str) -> str:
        """
        生成HMAC SHA256签名 (标准Binance兼容方式)

        :param query_string: 查询字符串
        :return: 签名hex字符串
        """
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def _generate_nonce(self) -> int:
        """生成nonce (microsecond timestamp)"""
        return math.trunc(time.time() * 1000000)

    def _request(self, method: str, endpoint: str, params: dict = None, signed: bool = False) -> dict:
        """
        发送HTTP请求

        :param method: HTTP方法 (GET, POST, DELETE等)
        :param endpoint: API端点
        :param params: 请求参数
        :param signed: 是否需要签名
        :return: 响应数据
        """
        url = f"{self.base_url}{endpoint}"
        headers = {
            "X-MBX-APIKEY": self.api_key
        }

        if params is None:
            params = {}

        if signed:
            # 添加timestamp和签名
            params['timestamp'] = int(time.time() * 1000)
            params['recvWindow'] = self.recvWindow

            # 生成查询字符串 (不包括signature, 参数按字母顺序)
            query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])

            # 生成签名
            signature = self._generate_signature_hmac(query_string)

            # 将签名添加到URL，而不是params
            url = f"{url}?{query_string}&signature={signature}"
            params = {}  # 清空params,因为已经在URL中

        try:
            if method == "GET":
                response = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            elif method == "POST":
                response = requests.post(url, params=params, headers=headers, timeout=self.timeout)
            elif method == "DELETE":
                response = requests.delete(url, params=params, headers=headers, timeout=self.timeout)
            elif method == "PUT":
                response = requests.put(url, params=params, headers=headers, timeout=self.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"[{self.exchange_code}] API请求失败: {method} {endpoint} - {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise

    def load_pair_info_map(self):
        """加载交易对信息"""
        if self.exchange_info_cache is None:
            self.exchange_info_cache = self._request("GET", "/fapi/v1/exchangeInfo")

        self.pair_info_map = {}
        for info in self.exchange_info_cache.get("symbols", []):
            symbol = info["symbol"]
            self.pair_info_map[symbol] = info

            # 提取tick size
            for filter_item in info.get("filters", []):
                if filter_item.get("filterType") == "PRICE_FILTER":
                    self.pair_info_map[symbol]["tick_size"] = filter_item.get("tickSize")
                    break

    def get_pair_info(self, pair: str):
        """获取交易对信息"""
        if pair not in self.pair_info_map:
            self.load_pair_info_map()
        return self.pair_info_map.get(pair)

    def get_all_cur_positions(self) -> List[BinancePositionDetail]:
        """获取所有当前持仓"""
        try:
            positions = self._request("GET", "/fapi/v2/positionRisk", signed=True)
            positions = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
            return [BinancePositionDetail(p) for p in positions]
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 获取持仓失败: {e}")
            time.sleep(1)
            positions = self._request("GET", "/fapi/v2/positionRisk", signed=True)
            positions = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
            return [BinancePositionDetail(p) for p in positions]

    def get_position(self, symbol: str) -> Optional[BinancePositionDetail]:
        """获取指定交易对的持仓"""
        positions = self._request("GET", "/fapi/v2/positionRisk", params={"symbol": symbol}, signed=True)
        if positions:
            return BinancePositionDetail(positions[0])
        return None

    @timed_cache(timeout=3)
    def get_balance_list_cache(self):
        """获取账户余额列表 (带缓存)"""
        return self._request("GET", "/fapi/v2/balance", signed=True)

    def get_available_balance(self, asset="USDT") -> float:
        """获取可用余额"""
        balance_list = self.get_balance_list_cache()
        for info in balance_list:
            if info["asset"] == asset:
                return float(info.get("availableBalance", 0))
        return 0.0

    def get_available_margin(self) -> float:
        """获取可用保证金"""
        return self.get_available_balance("USDT") + self.get_available_balance("USDC")

    def get_tick_price(self, symbol: str) -> float:
        """获取最新价格"""
        result = self._request("GET", "/fapi/v1/ticker/price", params={"symbol": symbol})
        return float(result["price"])

    def get_all_tick_price(self, symbol: str = None):
        """获取所有交易对价格"""
        if symbol:
            return self.get_tick_price(symbol)
        else:
            datas = self._request("GET", "/fapi/v1/ticker/price")
            ret = []
            for d in datas:
                if not d["symbol"].endswith("USDT"):
                    continue
                ret.append({
                    "name": d["symbol"].replace("USDT", ""),
                    "midPx": float(d["price"])
                })
            return ret

    def get_total_margin(self, total_eq=False) -> float:
        """获取总保证金"""
        balance_list = self._request("GET", "/fapi/v2/balance", signed=True)
        total = 0.0
        for info in balance_list:
            balance = float(info.get("crossWalletBalance", 0)) + float(info.get("crossUnPnl", 0))
            if balance != 0 and "USD" in info["asset"]:
                total += balance
            elif balance != 0:
                try:
                    price = self.get_tick_price(info["asset"] + "USDT")
                    total += price * balance
                except:
                    pass
        return total

    @timed_cache(timeout=60)
    def get_funding_rate(self, symbol: str, apy=True) -> float:
        """
        获取资金费率

        :param symbol: 交易对
        :param apy: 是否返回年化费率
        :return: 资金费率
        """
        if symbol.endswith("USDT") is False and symbol.endswith("USDC") is False:
            symbol = f"{symbol}USDT"
        info = self._request("GET", "/fapi/v1/premiumIndex", params={"symbol": symbol})
        rate = float(info["lastFundingRate"])

        # Aster默认8小时费率
        if apy:
            return rate * 3 * 365
        else:
            return rate

    def get_order_books(self, symbol: str, limit: int = 20):
        """获取订单簿"""
        data = self._request("GET", "/fapi/v1/depth", params={"symbol": symbol, "limit": limit})
        return BinanceOrderBook(data)

    def get_open_interest(self, symbol: str, usd=True) -> float:
        """获取持仓量"""
        info = self._request("GET", "/fapi/v1/openInterest", params={"symbol": symbol})
        oi = float(info["openInterest"])
        if usd:
            oi *= self.get_tick_price(symbol)
        return oi

    def convert_symbol(self, symbol):
        """
        标准化交易对格式
        :param symbol: BTCUSDT
        :return: BTCUSDT (Bybit使用相同格式)
        """
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        return symbol

    def convert_size(self, symbol: str, size: float) -> str:
        """转换数量精度"""
        symbol = self.convert_symbol(symbol)
        pair_info = self.get_pair_info(symbol)
        if not pair_info:
            return size
        quantity_precision = pair_info.get("quantityPrecision", 3)
        return float(f"{size:.{quantity_precision}f}")

    def convert_price(self, symbol: str, price: float) -> str:
        """转换价格精度"""
        pair_info = self.get_pair_info(symbol)
        if not pair_info:
            return str(price)

        tick_size = float(pair_info.get("tick_size", "0.01"))
        rounded_number = round(price / tick_size)
        converted_number = rounded_number * tick_size

        price_precision = pair_info.get("pricePrecision", 2)
        return f"{converted_number:.{price_precision}f}"

    def make_new_order(self, symbol: str, side: str, order_type: str, quantity: float,
                       price: float = None, msg: str = "", **kwargs):
        """
        下单

        :param symbol: 交易对
        :param side: 方向 (BUY/SELL)
        :param order_type: 订单类型 (MARKET/LIMIT等)
        :param quantity: 数量
        :param price: 价格
        :param msg: 日志消息
        :return: 订单响应
        """
        pair_info = self.get_pair_info(symbol)
        if not pair_info:
            raise ValueError(f"Unknown symbol: {symbol}")

        order_type = order_type.upper()
        side = side.upper()

        # 转换数量精度
        quantity = float(quantity)
        if quantity == 0:
            logger.warning(f"[{self.exchange_code}] 下单数量为0: {symbol} {side} {order_type}")
            return None

        quantity_str = self.convert_size(symbol, quantity)

        # 构建订单参数
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity_str
        }

        if order_type == "LIMIT":
            if price is None:
                raise ValueError("LIMIT order requires price")
            params["price"] = self.convert_price(symbol, price)
            params["timeInForce"] = "GTC"

        # 添加额外参数
        params.update(kwargs)

        start_time = time.time()
        response = self._request("POST", "/fapi/v1/order", params=params, signed=True)

        elapsed_ms = (time.time() - start_time) * 1000
        content = f"[{self.exchange_code}] {msg} 下单: {symbol} {side} {order_type} {price} {quantity_str} {elapsed_ms:.2f}ms"
        logger.info(content)

        return response

    def get_recent_order(self, symbol: str, orderId: int = None):
        """获取最近订单"""
        params = {"symbol": symbol}
        if orderId:
            params["orderId"] = int(orderId)

        for _ in range(5):
            try:
                all_orders = self._request("GET", "/fapi/v1/allOrders", params=params, signed=True)
                if orderId is None:
                    return BinanceOrder(all_orders[-1]) if all_orders else None
                else:
                    orderId = int(orderId)
                    matching = [o for o in all_orders if o["orderId"] == orderId]
                    return BinanceOrder(matching[-1]) if matching else None
            except Exception as e:
                logger.warning(f"[{self.exchange_code}] 获取订单失败: {symbol} {orderId} - {e}")
                time.sleep(0.5)
        return None

    def cancel_order(self, symbol: str, orderId: int):
        """取消订单"""
        return self._request("DELETE", "/fapi/v1/order",
                             params={"symbol": symbol, "orderId": orderId}, signed=True)

    def cancel_all_orders(self, symbol: str = None, limit: int = 3):
        """取消所有订单"""
        params = {}
        if symbol:
            params["symbol"] = symbol

        all_orders = self._request("GET", "/fapi/v1/openOrders", params=params, signed=True)

        for order in all_orders[:limit]:
            if order["status"] not in [BinanceOrderStatus.FILLED,
                                       BinanceOrderStatus.CANCELED,
                                       BinanceOrderStatus.EXPIRED]:
                try:
                    self.cancel_order(order["symbol"], order["orderId"])
                except Exception as e:
                    logger.error(f"[{self.exchange_code}] 取消订单失败: {e}")

    def set_leverage(self, symbol: str, default_leverage: int = 10):
        """设置杠杆"""
        try:
            return self._request("POST", "/fapi/v1/leverage",
                                 params={"symbol": symbol, "leverage": default_leverage},
                                 signed=True)
        except Exception as e:
            logger.warning(f"[{self.exchange_code}] 设置{symbol} {default_leverage}x杠杆失败: {e}")
            return None

    def get_klines(self, symbol: str, interval: str, limit: int = 500):
        """
        获取K线数据

        :param symbol: 交易对符号 (如 "BTC", 会自动添加 "USDT" 后缀)
        :param interval: 时间间隔 (如 "1m", "5m", "1h", "4h", "1d")
        :param limit: 返回数据条数，最大500
        :return: K线数据列表 (BinanceKlineBar对象列表)
        """
        # 添加USDT后缀
        if not symbol.endswith("USDT"):
            symbol = f"{symbol}USDT"

        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": min(limit, 500)  # 确保不超过最大限制
        }

        try:
            result = self._request("GET", "/fapi/v1/klines", params=params)

            # 转换为BinanceKlineBar对象列表
            klines = [BinanceKlineBar(k) for k in result]

            logger.debug(f"[{self.exchange_code}] 获取K线数据成功: {symbol} {interval} {len(klines)}条")
            return klines

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 获取K线数据失败: {symbol} {interval} - {e}")
            raise

    def get_position_max_open_usd_amount(self, pair: str = None) -> float:
        """获取最大可开仓金额"""
        avail = self.get_available_margin()
        max_leverage = 10
        return avail * max_leverage

    def get_cross_margin_ratio(self):
        """
            维持仓位的保证金比例(达到100%会被清算)
            计算维持保证金使用率：维持保证金 / 账户总价值
        """
        try:
            # 尝试使用账户信息接口获取总维持保证金和总钱包余额
            account_info = self._request("GET", "/fapi/v2/account", signed=True)
            total_maint_margin = float(account_info["totalMaintMargin"])  # 维持保证金
            total_wallet_balance = float(account_info["totalWalletBalance"])  # 账户总余额

            if total_wallet_balance == 0:
                return 0

            return total_maint_margin / total_wallet_balance
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 获取全仓保证金比例失败: {e}")
            return 0


if __name__ == '__main__':
    pass
