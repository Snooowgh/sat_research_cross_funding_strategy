# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : binance_future
@Description :
@Time        : 2023/11/27 22:58
"""
import time

import ccxt
from binance.um_futures import UMFutures
from loguru import logger

from cex_tools.exchange_model.position_model import BinancePositionDetail
from cex_tools.exchange_model.order_model import BinanceOrder, BinanceOrderStatus
from cex_tools.exchange_model.kline_bar_model import BinanceKlineBar
from cex_tools.exchange_model.orderbook_model import BinanceOrderBook
from cex_tools.cex_enum import ExchangeEnum
from cex_tools.funding_rate_cache import FundingRateCache
from utils.decorators import timed_cache
from utils.notify_tools import send_slack_message

"""
    https://developers.binance.com/docs/derivatives/Introduction
"""


class BinanceFuture(UMFutures):

    def __init__(self, key=None, secret=None, erc20_deposit_addr="", maker_fee_rate=0.00018,
                 taker_fee_rate=0.00045, recvWindow=5000, timeout=10000, testnet=False, **kwargs):
        # 根据testnet参数选择不同的基类和URL
        if testnet:
            from binance.um_futures import UMFutures
            super().__init__(key, secret, base_url="https://testnet.binancefuture.com", **kwargs)
            self.ccxt_exchange = ccxt.binanceusdm({
                "apiKey": key,
                "secret": secret,
                "sandbox": True
            })
            self.ccxt_spot_exchange = ccxt.binance({
                "apiKey": key,
                "secret": secret,
                "sandbox": True
            })
        else:
            from binance.um_futures import UMFutures
            super().__init__(key, secret, **kwargs)
            self.ccxt_exchange = ccxt.binanceusdm({
                "apiKey": key,
                "secret": secret
            })
            self.ccxt_spot_exchange = ccxt.binance({
                "apiKey": key,
                "secret": secret
            })
        self.exchange_code = ExchangeEnum.BINANCE
        time_diff = int(time.time() * 1000) - self.time()["serverTime"]
        if abs(time_diff) >= 1000:
            logger.error(f"{self.exchange_code} 本地与服务器时间差: {time_diff}ms")
        else:
            logger.info(f"{self.exchange_code} 本地与服务器时间差: {time_diff}ms")

        self.exchange_info_cache = None
        self.pair_info_map = {}
        self.erc20_deposit_addr = erc20_deposit_addr
        self.maker_fee_rate = maker_fee_rate
        self.taker_fee_rate = taker_fee_rate
        self.recvWindow = recvWindow
        self.timeout = timeout
        self.load_pair_info_map()

    # def convert_symbol(self, symbol):
    #     if symbol.endswith("USDT"):
    #         return symbol.replace("USDT", "") + '-USDT-SWAP'
    #     else:
    #         return symbol

    def load_pair_info_map(self):
        if self.exchange_info_cache is None:
            self.exchange_info_cache = self.exchange_info()
        self.pair_info_map = dict((info["symbol"], info) for info in self.exchange_info_cache["symbols"])
        for symbol, info in self.pair_info_map.items():
            tick_size = list(filter(lambda x: x["filterType"] == "PRICE_FILTER", info["filters"]))[0]["tickSize"]
            self.pair_info_map[symbol]["tick_size"] = tick_size

    def __convert_to_ccxt_symbol(self, symbol):
        if symbol.endswith("USDT"):
            return symbol[0:-4] + "/USDT:USDT"
        elif symbol.endswith("USDC"):
            return symbol[0:-4] + "/USDC:USDC"

    def set_leverage(self, symbol, default_leverage=10):
        """
            :param symbol:
            :param default_leverage:
            :return: {'symbol': 'PENGUUSDT', 'leverage': 20, 'maxNotionalValue': '200000'}
        """
        try:
            return self.ccxt_exchange.set_leverage(default_leverage, self.__convert_to_ccxt_symbol(symbol))
        except Exception as e:
            logger.warning(f"设置{symbol} {default_leverage}x杠杆出错: {e}")

    def get_pair_info(self, pair):
        try:
            return self.pair_info_map.get(pair)
        except:
            self.load_pair_info_map()
            return self.pair_info_map.get(pair)

    def get_all_cur_positions(self):
        try:
            binance_positions = self.get_position_risk(recvWindow=self.recvWindow)
        except:
            time.sleep(1)
            binance_positions = self.get_position_risk(recvWindow=self.recvWindow)
        binance_positions = list(filter(lambda a: float(a.get("positionAmt")) != 0, binance_positions))
        binance_positions = list(map(lambda a: BinancePositionDetail(a), binance_positions))
        return binance_positions

    def get_position(self, symbol):
        binance_positions = self.get_position_risk(symbol=symbol, recvWindow=self.recvWindow)
        if binance_positions:
            return BinancePositionDetail(binance_positions[0])
        else:
            return None

    @timed_cache(timeout=3)
    def get_balance_list_cache(self):
        balance_list = self.balance(recvWindow=self.recvWindow)
        return balance_list

    def get_available_balance(self, asset="USDT"):
        balance_list = self.get_balance_list_cache()
        for info in balance_list:
            if info["asset"] == asset:
                # usdt_balance = float(info.get("availableBalance"))
                usdt_balance = float(info.get('maxWithdrawAmount'))
                return usdt_balance
        return 0

    def get_available_margin(self):
        return self.get_available_balance("USDT") + self.get_available_balance("USDC")

    def get_all_tick_price(self, symbol=None):
        if symbol:
            return float(self.ticker_price(symbol)["price"])
        else:
            datas = self.ticker_price(symbol)
            ret = []
            for d in datas:
                if not d["symbol"].endswith("USDT"):
                    continue
                t = {}
                try:
                    t["name"] = d["symbol"].replace("USDT", "")
                    t["midPx"] = float(d["price"])
                    ret.append(t)
                except:
                    pass
            return ret

    def get_tick_price(self, symbol):
        return float(self.get_all_tick_price(symbol))

    def get_klines(self, symbol, interval, limit=500):
        # 添加USDT后缀
        if not symbol.endswith("USDT"):
            symbol = f"{symbol}USDT"

        return [BinanceKlineBar(k) for k in self.klines(symbol=symbol, interval=interval, limit=limit)]

    def get_total_margin(self, total_eq=False):
        balance_list = self.balance(recvWindow=self.recvWindow)
        total = 0
        for info in balance_list:
            balance = float(info.get("crossWalletBalance")) + float(info.get("crossUnPnl"))
            if balance != 0 and "USD" in info["asset"]:
                total += balance
            elif balance != 0:
                price = self.get_all_tick_price(info["asset"] + "USDT")
                total += price * balance
        return total

    def convert_order_qty_to_size(self, symbol, order_qty):
        return order_qty

    def convert_symbol(self, symbol):
        """
        标准化交易对格式
        :param symbol: BTCUSDT
        :return: BTCUSDT (Bybit使用相同格式)
        """
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        return symbol

    def convert_size(self, symbol, size):
        symbol = self.convert_symbol(symbol)
        pair_info = self.get_pair_info(symbol)
        if not pair_info:
            return size
        quantity_precision = pair_info["quantityPrecision"]
        return float(("{:." + str(quantity_precision) + "f}").format(size))

    def convert_price(self, symbol, price):
        pair_info = self.get_pair_info(symbol)
        tick_size = pair_info["tick_size"]

        # 将字符串类型的tickSize转换为浮点数类型
        tickSize_float = float(tick_size)
        # 将数字除以tickSize_float，得到对应精确度的整数值
        rounded_number = round(price / tickSize_float)
        # 将整数值乘以tickSize_float，得到转换后的浮点数值
        converted_number = rounded_number * tickSize_float

        return ("{:." + str(pair_info["pricePrecision"]) + "f}").format(converted_number)

    def get_orders_by_type(self, symbol, order_type=None, status=None):
        ret = []
        all_orders = self.get_orders(symbol=symbol, recvWindow=self.recvWindow)
        for order in all_orders:
            if order_type is None or order["type"] == order_type:
                if status is None or order["status"] == status:
                    ret.append(BinanceOrder(order))
        return ret

    def get_history_order(self, pair):
        pass

    def cancel_all_orders(self, symbol, limit=3):
        all_orders = self.get_all_orders(symbol=symbol, limit=limit, recvWindow=self.recvWindow)
        for order in all_orders:
            if order["status"] not in [BinanceOrderStatus.FILLED, BinanceOrderStatus.CANCELED,
                                       BinanceOrderStatus.EXPIRED]:
                self.cancel_order(symbol=symbol, orderId=order["orderId"])

    def make_new_order(self, symbol, side, order_type, quantity, price, msg="", **kwargs):
        pair_info = self.get_pair_info(symbol)
        order_type = order_type.upper()
        side = side.upper()
        tick_size = pair_info["tick_size"]
        stopPrice = None
        # 将字符串类型的tickSize转换为浮点数类型
        tickSize_float = float(tick_size)

        if price is not None:
            # 将数字除以tickSize_float，得到对应精确度的整数值
            price = float(price)
            rounded_number = round(price / tickSize_float)
            # 将整数值乘以tickSize_float，得到转换后的浮点数值
            converted_number = rounded_number * tickSize_float
            price = ("{:." + str(pair_info["pricePrecision"]) + "f}").format(converted_number)
        if quantity is not None:
            quantity = float(quantity)
            if quantity == 0:
                logger.warning(f"⚠️ {self.exchange_code}下单数量为0: {symbol} {side} {order_type} {price} {quantity}")
                return
            quantity = ("{:." + str(pair_info["quantityPrecision"]) + "f}").format(quantity)

        if kwargs.get("stopPrice"):
            kwargs["stopPrice"] = ("{:." + str(pair_info["pricePrecision"]) + "f}").format(kwargs["stopPrice"])
        timeInForce = ""
        if order_type == "LIMIT":
            timeInForce = "GTC"
        if order_type == "MARKET":
            price = ''
        if order_type in ["STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT"]:
            stopPrice = price
            price = ''
        start_time = time.time()

        response = self.new_order(symbol=symbol,
                                  side=side,
                                  type=order_type,
                                  quantity=quantity,
                                  timeInForce=timeInForce,
                                  price=price,
                                  stopPrice=stopPrice,
                                  **kwargs)
        content = f"⚠️ {msg} {self.exchange_code} 下单: {symbol} {side} {order_type} {price} {quantity} " \
                  f"{(time.time() - start_time) * 1000:.2f}ms"
        logger.info(content)
        if msg:
            send_slack_message(content)
        logger.debug(response)
        return response

    def get_recent_order(self, binance_symbol, orderId=None):
        cnt = 5
        for _ in range(cnt):
            try:
                all_orders = self.get_all_orders(binance_symbol, orderId=orderId, recvWindow=self.recvWindow)
                if orderId is None:
                    return BinanceOrder(all_orders[-1])
                else:
                    orderId = int(orderId)
                    return BinanceOrder(list(filter(lambda a: a["orderId"] == orderId, all_orders))[-1])
            except:
                logger.warning(f"⚠️ {self.exchange_code}获取订单失败: {binance_symbol} {orderId} 重试...")
                time.sleep(0.5)
                continue

    def cancel_order(self, symbol, orderId):
        return super().cancel_order(symbol, orderId)
        # return UMFutures.cancel_order(self, symbol, orderId)

    def __convert_network(self, deposit_network):
        if "Arbitrum" in deposit_network:
            deposit_network = "ARBITRUM"
        if "Avalanche" in deposit_network:
            deposit_network = "AVAXC"
        return deposit_network

    def get_dep_wd_status(self, deposit_margin_coin, deposit_network):
        deposit_network = self.__convert_network(deposit_network)
        canDep = False
        canWd = False
        result = self.ccxt_exchange.fetch_currencies()
        networks = result.get(deposit_margin_coin, {}).get("networks", {})
        r = networks.get(deposit_network, {})
        if r:
            canWd = r["withdraw"]
            canDep = r["deposit"]
        return canDep, canWd

    @timed_cache(timeout=60)
    def get_funding_rate(self, symbol, apy=True):
        # 优先从缓存获取
        cache = FundingRateCache()
        cached_rate = cache.get_funding_rate("binance", symbol)

        if cached_rate is not None:
            # 缓存中的是单次费率，需要根据间隔转换
            # Binance通常是8小时一次，一天3次
            if apy:
                return cached_rate * 3 * 365
            else:
                return cached_rate

        # 缓存未命中，使用原接口
        if symbol.endswith("USDT") is False and symbol.endswith("USDC") is False:
            symbol = f"{symbol}USDT"
        info = self.mark_price(symbol)
        info = float(info["lastFundingRate"])
        interval = self.ccxt_exchange.fetch_funding_interval(self.__convert_to_ccxt_symbol(symbol))["interval"]
        interval = int(interval.rstrip("h"))
        if apy:
            return info * 24 / interval * 365
        else:
            return info

    def get_order_books(self, symbol, limit=20):
        return BinanceOrderBook(self.depth(symbol, limit=limit))

    def get_open_interest(self, symbol, usd=True):
        info = self.open_interest(symbol)
        r = float(info["openInterest"])
        if usd:
            r *= self.get_tick_price(symbol)
        return r

    def transfer_all_fund_to_future(self):
        data = self.ccxt_spot_exchange.fetch_balance({"type": "spot"})
        margin_usd_list = ["USDT", "USDC"]
        total_transfer = 0
        for coin in margin_usd_list:
            free_usd = data[coin]["free"]
            if data.get(coin) and free_usd > 0:
                try:
                    self.ccxt_exchange.transfer(coin, free_usd, "spot", "future")
                except Exception as e:
                    logger.warning(f"⚠️ {self.exchange_code} 划转失败: {free_usd} {coin} {e}")
                    continue
                else:
                    total_transfer += free_usd
        return total_transfer

    def withdraw_coin(self, withdraw_margin_coin, amount, withdraw_network, to_addr):
        withdraw_network = self.__convert_network(withdraw_network)
        self.ccxt_exchange.transfer(withdraw_margin_coin, amount, "future", "spot")
        time.sleep(1)
        withdraw_result = self.ccxt_exchange.withdraw(code=withdraw_margin_coin, amount=amount,
                                                      address=to_addr, tag=None, params={"network": withdraw_network})
        logger.info(f"{self.exchange_code} 提币参数 {withdraw_margin_coin}, {amount}, {withdraw_network}, {to_addr}")
        logger.info(withdraw_result)
        return withdraw_result

    def create_stoploss_order(self, symbol, side, amount, stopPrice):
        ret = self.ccxt_exchange.create_stop_market_order(symbol, side, amount, stopPrice)
        logger.info(f"止损止盈单: {symbol} {side} {amount} {stopPrice}")
        logger.info(ret)
        return ret

    def get_cross_margin_ratio(self):
        """
            维持仓位的保证金比例(达到100%会被清算)
            计算维持保证金使用率：维持保证金 / 账户总价值
        """
        try:
            account_info = self.account(recvWindow=self.recvWindow)
            total_maint_margin = float(account_info["totalMaintMargin"])  # 维持保证金
            total_wallet_balance = float(account_info["totalMarginBalance"])  # 账户总余额

            if total_wallet_balance == 0:
                return 0

            return total_maint_margin / total_wallet_balance
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 获取全仓保证金比例失败: {e}")
            return 0

    def get_deposit_pending_token(self, token="USDC"):
        """获取正在充值的token数量"""
        # https://developers.binance.com/docs/wallet/capital/deposite-history
        # 0(0:pending, 6:credited but cannot withdraw, 7:Wrong Deposit, 8:Waiting User confirm, 1:success, 2:rejected)
        latest_deposit_info = self.ccxt_spot_exchange.fetch_deposits(code=token, limit=3)
        pending_token_amt = 0
        for info in latest_deposit_info:
            if info["info"]["status"] != 1:
                pending_token_amt += float(info["amount"])
        return pending_token_amt

    def wait_for_token(self, token, amount):
        # 等待到账
        max_waiting_second = 30 * 60
        start_time = time.time()
        pending_amt = 0
        while True:
            pending_amt = self.get_deposit_pending_token()
            if pending_amt > 1:
                break
            else:
                if start_time + max_waiting_second < time.time():
                    send_slack_message(f"❌ 存币{amount} {token}到{self.exchange_code}超时 ❌")
                    return
                time.sleep(1)
        send_slack_message("✅ 存币到{} {} {} 进行中, 用时: {:.2f}秒".format(self.exchange_code,
                                                                            pending_amt, token,
                                                                            time.time() - start_time))

    def deposit_all_token(self, token):
        pass

    def get_position_max_open_usd_amount(self, pair=None):
        availBuy = self.get_available_balance("USDT") + self.get_available_balance("USDC")
        maxLeverage = 10
        return float(availBuy) * maxLeverage


if __name__ == '__main__':
    pass
