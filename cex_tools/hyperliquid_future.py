# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : hyperliquid_future
@Description :
@Time        : 2024/9/17 19:36
"""
from cex_tools.funding_rate_cache import FundingRateCache
import time
import math
from typing import List
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log
import ccxt
import logging
from hyperliquid.utils.signing import OrderRequest
from loguru import logger
from cex_tools.cex_enum import ExchangeEnum, TradeSide
from cex_tools.exchange_model.position_model import HyperliquidPositionDetail
from cex_tools.exchange_model.order_model import HyperLiquidOrder
from cex_tools.exchange_model.kline_bar_model import HyperLiquidKlineBar
from hyperliquid.info import Info
from hyperliquid.utils import constants
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
import eth_account
from web3 import Web3
from utils.decorators import timed_cache
from utils.notify_tools import send_slack_message

"""
    doc: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
"""


class HyperLiquidFuture:
    def __init__(self, private_key=None, public_key=None, deposit_private_key=None,
                 maker_fee_rate=0.00013, taker_fee_rate=0.00039, max_leverage=10,
                 jump_fund_collect_borrow_amount=8000, **kwargs):
        if private_key is None:
            private_key = "0x24c03012010919bb81d4ae02a83053d4ccbc7723232c8fc79876e33717a6211b"  # 无用密钥
            deposit_private_key = private_key
        self.jump_fund_collect_borrow_amount = jump_fund_collect_borrow_amount
        self.deposit_private_key = deposit_private_key
        self.private_key = private_key
        self.exchange_code = ExchangeEnum.HYPERLIQUID
        self.base_url = constants.MAINNET_API_URL
        self.account: LocalAccount = eth_account.Account.from_key(private_key)
        # print("hl账户地址：", self.account.address)
        if public_key is None:
            self.address = self.account.address
        else:
            self.address = public_key
        self.exchange = Exchange(self.account, self.base_url, account_address=self.address)
        self.info = Info(self.base_url, skip_ws=True)
        self.meta_info_map = None
        self.size_decimals_map = {}

        self.maker_fee_rate = maker_fee_rate
        self.taker_fee_rate = taker_fee_rate
        self.max_leverage = max_leverage

        self.hyperliquid_deposit_bridge2_addr = "0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7"
        if self.deposit_private_key:
            self.deposit_account: LocalAccount = eth_account.Account.from_key(deposit_private_key)
            self.erc20_deposit_addr = self.deposit_account.address
            self.withdraw_account: LocalAccount = eth_account.Account.from_key(self.deposit_private_key)
            self.withdraw_exchange = Exchange(self.withdraw_account, self.base_url, account_address=self.address)
        else:
            self.deposit_account: LocalAccount = None
            self.erc20_deposit_addr = None
            self.withdraw_account = None
            self.withdraw_exchange = None
        # https://docs.ccxt.com/#/exchanges/hyperliquid?id=editorder
        self.ccxt_exchange = ccxt.hyperliquid({
            'walletAddress': public_key,
            'privateKey': private_key
        })
        self.get_meta_and_asset_ctxs()  # 预加载一次

    @timed_cache(timeout=60 * 60 * 24)  # 24小时缓存
    @retry(wait=wait_exponential(multiplier=1, max=3), stop=stop_after_attempt(3))
    def get_meta_and_asset_ctxs(self):
        ret = self.info.meta_and_asset_ctxs()
        info_map = {}
        for indx, symbol_info in enumerate(ret[0]["universe"]):
            ret[1][indx].update(symbol_info)
            info_map[symbol_info["name"]] = ret[1][indx]
        for k, v in info_map.items():
            self.size_decimals_map[k] = v["szDecimals"]
        return info_map

    @timed_cache(timeout=1)
    @retry(wait=wait_exponential(multiplier=1, max=3), stop=stop_after_attempt(3))
    def get_account_details(self):
        return self.info.user_state(self.address)

    def get_pair_max_leverage(self, pair):
        try:
            if self.meta_info_map is None:
                self.meta_info_map = self.get_meta_and_asset_ctxs()
            r = self.meta_info_map.get(self.__convert_pair(pair))
            return r.get("maxLeverage", 5)
        except Exception as e:
            logger.warning(f"{self.exchange_code}找不到 {pair} 杠杆率信息 {e}")
            return 5

    def set_leverage(self, symbol, default_leverage=None):
        return

    def get_position_max_open_usd_amount(self, pair):
        maxLeverage = self.get_pair_max_leverage(pair)
        availableBalance = self.get_available_balance()
        return availableBalance * maxLeverage

    def load_pair_info_map(self):
        pass

    def get_pair_info(self, pair):
        pass

    def get_all_cur_positions(self):
        all_pos = self.get_account_details()["assetPositions"]
        ret = [HyperliquidPositionDetail(pos["position"]) for pos in all_pos]
        return ret

    def get_position(self, pair):
        ret = None
        for i in self.get_all_cur_positions():
            if i.symbol == pair:
                ret = i
        return ret

    def get_available_balance(self, asset="USDT"):
        return float(self.get_account_details()["withdrawable"])

    def get_available_margin(self):
        return self.get_available_balance()

    def get_all_tick_price(self, pair=None):
        if pair:
            return self.get_tick_price(pair)
        ret = self.info.meta_and_asset_ctxs()
        ret_list = []
        for indx, symbol_info in enumerate(ret[0]["universe"]):
            ret[1][indx].update(symbol_info)
            if ret[1][indx]["midPx"] is None:
                continue
            ret[1][indx]["midPx"] = float(ret[1][indx]["midPx"])
            ret[1][indx]["name"] = ret[1][indx]["name"]
            ret_list.append(ret[1][indx])
        return ret_list

    def get_tick_price(self, pair):
        """WARNING: 该接口不能乱用"""
        ret = self.info.meta_and_asset_ctxs()
        for indx, symbol_info in enumerate(ret[0]["universe"]):
            if symbol_info["name"] == self.__convert_pair(pair):
                return float(ret[1][indx]["midPx"])
        return 0

    @timed_cache(timeout=60)  # 1分钟缓存
    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, RuntimeError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def get_klines(self, pair, interval, limit=500):
        """
        获取K线数据

        :param pair: 交易对 (如 "BTC", 会自动去除 "USDT" 后缀)
        :param interval: 时间间隔 (如 "1m", "5m", "15m", "1h", "4h", "1d")
        :param limit: 返回数据条数，最大1000
        :return: K线数据列表 (HyperLiquidKlineBar对象列表)
        """
        # 转换交易对格式 (去除USDT后缀)
        if pair.endswith("USDT"):
            coin = self.__convert_pair(pair)
        else:
            coin = pair

        # 转换时间间隔格式 (HyperLiquid使用分钟数)
        interval_mapping = {
            "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
            "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
            "1d": "1440", "3d": "4320", "1w": "10080"
        }

        if interval not in interval_mapping:
            raise ValueError(f"不支持的时间间隔: {interval}, 支持的间隔: {list(interval_mapping.keys())}")

        hl_interval = interval_mapping[interval]

        try:
            # 计算时间范围
            end_time = int(time.time() * 1000)  # 当前时间(毫秒)
            # 根据间隔计算开始时间，确保能获取到足够的数据
            interval_ms = int(hl_interval) * 60 * 1000
            start_time = end_time - (interval_ms * limit)

            # 调用HyperLiquid API
            result = self.info.candles_snapshot(
                name=coin,
                interval=interval,
                startTime=start_time,
                endTime=end_time
            )

            if not result:
                logger.warning(f"[{self.exchange_code}] 获取K线数据为空: {coin} {interval}")
                return []

            # 转换为HyperLiquidKlineBar对象列表
            klines = [HyperLiquidKlineBar(k) for k in result]

            # 按时间排序(从旧到新)并限制数量
            klines.sort(key=lambda x: x.open_time)
            klines = klines[-limit:]  # 取最新的limit条数据
            return klines

        except Exception as e:
            # 检查是否是429错误（Rate Limit）
            if hasattr(e, 'status') and e.status == 429:
                logger.warning(f"[{self.exchange_code}] API速率限制，等待重试: {coin} {interval} - {e}")
                raise  # 触发重试
            else:
                logger.error(f"[{self.exchange_code}] 获取K线数据失败: {coin} {interval} - {e}")
                raise

    def get_total_margin(self, total_eq=False):
        return float(self.get_account_details()["marginSummary"]["accountValue"])

    def __convert_pair(self, pair):
        return pair.replace("USDT", "")

    def convert_order_qty_to_size(self, pair, order_qty):
        return order_qty

    def convert_size(self, pair, size):
        """
        转换HyperLiquid下单数量精度
        根据交易对的szDecimals字段格式化数量精度
        """
        symbol = self.__convert_pair(pair)
        sz_decimals = self.size_decimals_map.get(symbol, None)

        if sz_decimals is not None:
            return float(("{:." + str(sz_decimals) + "f}").format(float(size)))
        else:
            # 如果找不到精度信息，使用默认6位小数
            return float("{:.6f}".format(float(size)))

    def convert_price(self, pair, price):

        return price

    def get_orders_by_type(self, pair, order_type=None, status=None):
        pass

    def get_history_order(self, pair):
        pass

    def cancel_all_orders(self, symbol=None, order_type=None):
        open_orders = self.info.open_orders(self.address)
        if not open_orders:
            return
        if symbol is None:
            return self.exchange.bulk_cancel(open_orders)
        else:
            r = []
            for open_order in open_orders:
                if open_order["coin"] == self.__convert_pair(symbol):
                    r.append(open_order)
            return self.exchange.bulk_cancel(r)

    def make_bulk_orders(self, order_requests: List[OrderRequest]):
        return self.exchange.bulk_orders(order_requests)

    def bulk_modify_orders_new(self, order_requests: List[OrderRequest]):
        r = self.exchange.bulk_modify_orders_new(order_requests)["response"]["data"]["statuses"]
        return [x["resting"]["oid"] for x in r]

    def make_new_order(self, pair, side, order_type, quantity, price=None, msg="", **kwargs):
        if side == TradeSide.BUY:
            is_buy = True
        elif side == TradeSide.SELL:
            is_buy = False
        else:
            raise Exception("side must be BUY or SELL")
        quantity = float(quantity)
        if quantity == 0:
            logger.warning(f"⚠️ {self.exchange_code}下单数量为0: {pair} {side} {order_type} {price} {quantity}")
            return
        # TODO: bug解决? 应该不需要的
        try:
            quantity = self.convert_size(pair, quantity)
        except Exception as e:
            logger.warning(
                f"⚠️ {self.exchange_code}下单数量转换失败: {pair} {side} {order_type} {price} {quantity} {e}")
            if quantity > 1:
                quantity = round(float(f"{quantity:.2g}"))
        symbol = self.__convert_pair(pair)
        reduce_only = kwargs.get("reduceOnly", False)
        slippage = kwargs.get("slippage", 0.095)
        start_time = time.time()
        if order_type == "LIMIT":
            gtc_limit = {"limit": {"tif": "Gtc"}}
            order_result = self.exchange.order(symbol,
                                               is_buy, quantity, round(float(f"{price:.5g}"), 6),
                                               gtc_limit,
                                               reduce_only=reduce_only)
        elif order_type == "post_only":
            alo_limit = {"limit": {"tif": "Alo"}}
            order_result = self.exchange.order(symbol,
                                               is_buy, quantity, round(float(f"{price:.5g}"), 6),
                                               alo_limit,
                                               reduce_only=reduce_only)
        elif order_type == "MARKET":
            # 使用限价单实现
            if price is None:
                price = self.get_tick_price(pair)
            if is_buy:
                price = price * (1 + slippage)
            else:
                price = price * (1 - slippage)
            price = round(float(f"{price:.5g}"), 6)
            order_result = self.exchange.order(symbol,
                                               is_buy, quantity, price,
                                               {"limit": {"tif": "Gtc"}},
                                               reduce_only=reduce_only)
            # if kwargs.get("reduceOnly", False) is True:
            #     order_result = self.exchange.market_close(symbol,
            #                                               sz=quantity, px=price, slippage=0.015)
            # else:
            #     order_result = self.exchange.market_open(symbol,
            #                                              is_buy, sz=quantity, px=price, slippage=0.015)
        else:
            raise Exception("order_type must be LIMIT or MARKET")
        if order_result["status"] != "ok":
            raise Exception(f"hyperliquid 下单失败: {order_result}")

        last_trade_info = order_result["response"]["data"]["statuses"][-1]
        if last_trade_info.get("error"):
            # Cannot place order 0.01 more aggressive than oracle when open interest is at cap. asset=149
            err = last_trade_info['error']
            info = f"❌ ❌ hyperliquid 下单失败, 参数: {pair} {side} {order_type} {quantity} {price} 错误信息: {err} {order_result}"
            raise Exception(info)
        elif last_trade_info.get("resting"):
            r = {"orderId": last_trade_info["resting"]["oid"]}
            last_trade_info_detail = last_trade_info["resting"]
        elif last_trade_info.get("filled"):
            r = {"orderId": last_trade_info["filled"]["oid"]}
            last_trade_info_detail = last_trade_info["filled"]
        else:
            raise Exception("order_type must be LIMIT or MARKET")
        r.update(last_trade_info_detail)
        content = f"⚠️ {msg} {self.exchange_code} 下单: {symbol} {side} {order_type} {price} {quantity} " \
                  f"{(time.time() - start_time) * 1000:.2f}ms"
        logger.info(content)
        if msg:
            send_slack_message(content + "\n" + msg)
        logger.debug(order_result)
        return r

    def edit_cur_order(self, symbol, ordId, new_size=None, new_price=None, order_info=None, **kwargs):
        if order_info is None:
            order_info = self.get_recent_order(symbol, ordId)
        is_buy = order_info.side == TradeSide.BUY
        if order_info.type == "LIMIT":
            order_type = {"limit": {"tif": "Alo"}}
        else:
            order_type = {"limit": {"tif": "Gtc"}}
        reduce_only = order_info.reduceOnly
        if new_size is None:
            new_size = order_info.origQty - order_info.executedQty
        if new_price is None:
            new_price = order_info.price
        start_time = time.time()
        order_result = self.exchange.modify_order(oid=ordId, name=self.__convert_pair(symbol),
                                                  is_buy=is_buy,
                                                  sz=new_size,
                                                  limit_px=new_price,
                                                  order_type=order_type,
                                                  reduce_only=reduce_only)
        last_trade_info = order_result["response"]["data"]["statuses"][-1]
        if last_trade_info.get("error"):
            info = f"❌ ❌ hyperliquid 下单失败, 参数: {symbol} {order_info.side} {order_type} {new_size} {new_price} 错误信息: {last_trade_info['error']} {order_result}"
            raise Exception(info)
        elif last_trade_info.get("resting"):
            r = {"orderId": last_trade_info["resting"]["oid"]}
            last_trade_info_detail = last_trade_info["resting"]
        elif last_trade_info.get("filled"):
            r = {"orderId": last_trade_info["filled"]["oid"]}
            last_trade_info_detail = last_trade_info["filled"]
        else:
            raise Exception("order_type must be LIMIT or MARKET")
        r.update(last_trade_info_detail)
        content = f"⚠️ {self.exchange_code} 修改订单: {symbol} {order_info.side} {order_type} {new_price} {new_size} " \
                  f"{(time.time() - start_time) * 1000:.2f}ms"
        logger.info(content)
        logger.debug(order_result)
        return r

    def get_recent_order(self, binance_symbol, orderId=None):
        order_info_ret = None
        retry_count = 0  # 初始化重试计数
        max_retries = 3
        retry_delay = 0.1
        while retry_count < max_retries:
            try:
                order_info_ret = self.info.query_order_by_oid(self.address, orderId)
                order_info = order_info_ret["order"]
                order = HyperLiquidOrder(order_info["order"])
                order.set_status(order_info["status"])
                return order
            except Exception as e:
                # 检查是否已达到最大重试次数
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"❌ ❌ hyperliquid 获取订单失败: {orderId} {order_info_ret} {e}")
                    raise e  # 重新抛出异常给调用者
                time.sleep(retry_delay)

    def cancel_order(self, pair, orderId):
        return self.exchange.cancel(self.__convert_pair(pair), orderId)

    def __convert_network(self, deposit_network):
        pass

    def get_dep_wd_status(self, deposit_margin_coin, deposit_network):
        if deposit_margin_coin == "USDC" and deposit_network == "Arbitrum One":
            # 默认都可以
            return True, True
        elif deposit_margin_coin == "USDT" and deposit_network == "Arbitrum One":
            # 默认都可以
            return True, True
        else:
            return False, False

    def get_funding_rate(self, pair, apy=True):
        # 优先从缓存获取
        cache = FundingRateCache()
        cached_rate = cache.get_funding_rate("hyperliquid", pair)

        if cached_rate is not None:
            # 缓存中的是8小时一次
            if apy:
                return cached_rate * 3 * 365
            else:
                return cached_rate

        # 缓存未命中，使用原接口
        funding = float(self.get_meta_and_asset_ctxs().get(self.__convert_pair(pair))["funding"])
        if apy:
            funding = funding * 365 * 24
        else:
            funding *= 8  # 对齐一般交易所
        return funding

    def get_order_books(self, pair, limit=20):
        raise NotImplementedError("HyperLiquid 不支持获取orderbook")

    def get_open_interest(self, pair, usd=True):
        infos = self.get_meta_and_asset_ctxs()
        symbol = self.__convert_pair(pair)
        ret = float(infos[symbol]["openInterest"])
        if usd:
            ret = ret * float(infos[symbol]["midPx"])
        return ret

    def transfer_all_fund_to_future(self):
        return 0

    def create_stoploss_order(self, pair, side, amount, stopPrice):
        pass

    def get_cross_margin_ratio(self):
        """
            维持仓位的保证金比例(达到100%会被清算)
        """
        account_info = self.get_account_details()
        crossMaintenanceMarginUsed = float(account_info["crossMaintenanceMarginUsed"])
        accountValue = float(account_info["marginSummary"]["accountValue"])
        if accountValue == 0:
            return 0
        return crossMaintenanceMarginUsed / accountValue


if __name__ == '__main__':
    a = HyperLiquidFuture()
    print(a.get_klines("BTC", "1m", limit=10))
    print(a.get_all_tick_price("BNBUSDT"))
