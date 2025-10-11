# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : okx_future
@Description :
@Time        : 2024/7/1 22:44
"""
import time

import ccxt
from loguru import logger
from okx.api import Public
from okx.app import OkxSWAP
from cex_tools.cex_enum import ExchangeEnum, TradeSide
from cex_tools.exchange_model.position_model import OkxPositionDetail
from cex_tools.exchange_model.order_model import OkxOrder
from cex_tools.exchange_model.kline_bar_model import OkxKlineBar
from cex_tools.exchange_model.orderbook_model import OkxOrderBook
from utils.decorators import timed_cache
from utils.notify_tools import send_slack_message
from utils.parallelize_utils import parallelize_tasks

"""
    https://www.okx.com/docs-v5/zh/
"""


class OkxFuture:

    def __init__(self, api_key="", secret_key="", passphrase="", erc20_deposit_addr="",
                 **kwargs):
        self.sub_account = kwargs.get("sub_account", "HyperLiquid")
        proxy_host = "https://www.okx.com/"
        self.api_key, self.secret_key, self.passphrase = api_key, secret_key, passphrase
        # try:
        self.okxSWAP = OkxSWAP(
            key=api_key, secret=secret_key, passphrase=passphrase, proxies={}, proxy_host=proxy_host,
        )
        self.account = self.okxSWAP.account
        self.market = self.okxSWAP.market
        self.trade = self.okxSWAP.trade
        # TODO: 可能导致新币数据不存在
        self.market.get_exchangeInfos(uly="", expire_seconds=24 * 3600)
        # except:
        #     self.okxSWAP = None
        #     self.account = None
        #     self.market = None
        #     self.trade = None
        #     self.market = None
        #     logger.warning("okx IP地址不可用, 禁止交易")

        self.public_api = Public(proxy_host=proxy_host)

        self.exchange_code = ExchangeEnum.OKX
        self.ccxt_exchange = ccxt.okx({
            "apiKey": api_key,
            "secret": secret_key,
            "password": passphrase,
        })
        self.erc20_deposit_addr = erc20_deposit_addr

        self.maker_fee_rate = kwargs.get("maker_fee_rate", 0.0002)
        self.taker_fee_rate = kwargs.get("taker_fee_rate", 0.0005)
        self.parent_account = kwargs.get("parent_account")
        self.get_symbol_sheet_to_amt("BTCUSDT")

    def set_leverage(self, symbol, default_leverage=None):
        if default_leverage is None:
            default_leverage = self.get_pair_max_leverage(symbol)
        self.account.set_leverage(lever=default_leverage,
                                  mgnMode="cross",
                                  instId=self.convert_symbol(symbol))

    def load_pair_info_map(self):
        pass

    def get_pair_info(self, pair):
        pass

    def get_all_cur_positions(self):
        okx_positions = self.account.get_positions()["data"]
        for k in okx_positions:
            k["amt"] = float(k["pos"]) * self.get_symbol_sheet_to_amt(k["instId"])
        okx_positions = list(map(lambda a: OkxPositionDetail(a), okx_positions))
        return okx_positions

    def convert_symbol(self, symbol):
        if symbol.endswith("USDT"):
            return symbol.replace("USDT", "") + '-USDT-SWAP'
        if symbol.endswith("-USDT-SWAP"):
            return symbol
        else:
            return symbol + '-USDT-SWAP'

    @timed_cache(timeout=24 * 60 * 60)
    def get_symbol_sheet_to_amt(self, symbol):
        return float(self.market.get_exchangeInfo(instId=self.convert_symbol(symbol))["data"]["ctVal"])

    def get_position(self, symbol):
        okx_positions = self.account.get_position(instId=self.convert_symbol(symbol))["data"]
        for k in okx_positions:
            k["amt"] = float(k["pos"]) * self.get_symbol_sheet_to_amt(symbol)
        okx_positions = list(map(lambda a: OkxPositionDetail(a), okx_positions))
        okx_positions = list(filter(lambda a: a.positionAmt != 0, okx_positions))
        if okx_positions:
            return okx_positions[0]
        else:
            return None

    def get_available_balance(self, asset="USDT"):
        get_balance = self.okxSWAP.account.get_balance(ccy=asset)
        detail = get_balance["data"]["details"]
        if detail.get("availBal"):
            return float(detail["availBal"])
        else:
            return 0

    def get_available_margin(self):
        return self.get_available_balance("USDT") + self.get_available_balance("USDC")

    def get_pair_max_leverage(self, pair=None):
        return 10

    def get_position_max_open_usd_amount(self, pair=None):
        r = self.account.api.get_max_avail_size(self.convert_symbol(pair),
                                                "cross")
        availBuy = r["data"][0]["availBuy"]
        maxLeverage = self.get_pair_max_leverage(pair)
        return float(availBuy) * maxLeverage

    def get_all_tick_price(self, symbol=None):
        if symbol:
            return float(self.market.get_ticker(instId=self.convert_symbol(symbol))["data"]["last"])
        else:
            datas = self.market.get_tickers()["data"]
            ret = []
            for d in datas:
                t = {}
                try:
                    t["name"] = d["instId"].replace("-SWAP", "").replace("-", "").replace("USDT", "")
                    t["midPx"] = (float(d["askPx"]) + float(d["bidPx"])) / 2
                    ret.append(t)
                except:
                    pass
            return ret

    def get_tick_price(self, symbol):
        return float(self.market.get_ticker(instId=self.convert_symbol(symbol))["data"]["last"])

    def get_order_books(self, symbol, limit=20):
        return OkxOrderBook(self.market.get_books(instId=self.convert_symbol(symbol), sz=limit)["data"])

    def get_klines(self, symbol, interval, limit=500):
        data = self.market.get_history_candle_latest(instId=self.convert_symbol(symbol), length=limit, bar=interval)[
            "data"]
        # 时间从前往后排
        return [OkxKlineBar(d) for d in data]

    def get_total_margin(self, total_eq=False):
        data = self.account.get_balances()["data"]
        if total_eq:
            return float(data["totalEq"])
        else:
            return float(data["adjEq"] or data["totalEq"])

    def convert_order_qty_to_size(self, symbol, order_qty):
        return order_qty * self.get_symbol_sheet_to_amt(symbol)

    def convert_size_to_contract(self, symbol, size):
        # round_quantity_result = self.convert_size(symbol, size)
        round_quantity_result = size
        contract_to_f_result = self.trade.quantity_to_f(
            quantity=round_quantity_result / self.get_symbol_sheet_to_amt(symbol),
            instId=self.convert_symbol(symbol),
        )["data"]
        return contract_to_f_result

    def convert_size(self, symbol, size):
        """
        代币size数量 合理格式化
        :param symbol:
        :param size:
        :return:
        """
        # 底层库TMD有问题
        sheet_to_amt = self.get_symbol_sheet_to_amt(symbol)
        round_quantity_result = self.trade.round_quantity(
            quantity=float(size) / sheet_to_amt,
            instId=self.convert_symbol(symbol),
            ordType='market',  # market | limit
        )['data'] * sheet_to_amt
        return round_quantity_result

    def convert_price(self, symbol, price):
        round_price_result = self.trade.round_price(
            price=price,
            instId=self.convert_symbol(symbol),
            type='FLOOR',
        )['data']
        price_to_f_result = self.trade.price_to_f(
            price=round_price_result,
            instId=self.convert_symbol(symbol),
        )["data"]
        return price_to_f_result

    def get_orders_by_type(self, symbol, order_type=None):
        pass

    def cancel_all_orders(self, symbol=None, order_type=None):
        get_orders_pending = self.trade.get_orders_pending()
        if symbol is not None:
            for order in get_orders_pending["data"]:
                if order["instId"] == self.convert_symbol(symbol):
                    self.trade.cancel_order(
                        instId=self.convert_symbol(symbol),
                        ordId=order["ordId"]
                    )
        else:
            func_list = [
                (self.trade.cancel_order, (), {"instId": order["instId"], "ordId": order["ordId"]})
                for order in get_orders_pending["data"]
            ]
            parallelize_tasks(func_list)

    def make_new_order(self, symbol, side, order_type, quantity, price=None, msg="", **kwargs):

        if kwargs.get("stopPrice"):
            kwargs["slTriggerPx"] = self.convert_price(self.convert_symbol(symbol), kwargs["stopPrice"])
            kwargs["slOrdPx"] = -1  # 市价止损
        quantity = float(quantity)
        if quantity == 0:
            logger.warning(f"⚠️ {self.exchange_code}下单数量为0: {symbol} {side} {order_type} {price} {quantity}")
            return
        contract_quantity = self.convert_size_to_contract(symbol, quantity)
        if price is not None:
            price = self.convert_price(symbol, price)
        start_time = time.time()
        set_order_result = self.trade.set_order(
            instId=self.convert_symbol(symbol),
            tdMode='cross',  # 持仓方式 isolated：逐仓 cross：全仓
            side="buy" if side == TradeSide.BUY else "sell",  # 持仓方向 long：多单 short：空单
            ordType=order_type.lower(),
            posSide='',
            px='' if order_type == "MARKET" else price,  # 价格
            sz=contract_quantity,  # 合约张数
            **kwargs
        )
        content = f"⚠️ {self.exchange_code} {msg} 下单: {symbol} {side} {order_type} {price} {quantity}({contract_quantity}) " \
                  f"{(time.time() - start_time) * 1000:.2f}ms"
        logger.info(content)
        if msg:
            send_slack_message(content + "\n" + msg)
        logger.debug(set_order_result)
        return {"orderId": set_order_result["data"]["ordId"]}
        #
        # if order_type == "LIMIT":
        #     price = self.convert_price(symbol, price)
        #     open_limit = self.trade.open_limit(
        #         instId=self.convert_symbol(symbol),
        #         tdMode='cross',  # 持仓方式 isolated：逐仓 cross：全仓
        #         posSide="long" if side == TradeSide.BUY else "short",  # 持仓方向 long：多单 short：空单
        #         lever=10,  # 杠杆倍数
        #         openPrice=price,  # 开仓价格
        #         # openMoney=2,  # 开仓金额 开仓金额openMoney和开仓数量quantityCT必须输入其中一个 优先级：quantityCT > openMoney
        #         quantityCT=self.convert_size(symbol, quantity),  # 平仓数量，注意：quantityCT是合约的张数，不是货币数量
        #         tag='',  # 订单标签
        #         clOrdId='',  # 客户自定义订单ID
        #     )
        #
        #     close_limit = self.trade.close_limit(
        #         instId=self.convert_symbol(symbol),  # 产品
        #         tdMode='cross',  # 持仓方式 isolated：逐仓 cross：全仓
        #         posSide="long" if side == TradeSide.BUY else "short",  # 持仓方向 long：多单 short：空单
        #         closePrice=price,  # 平仓价格 closePrice 和 tpRate必须填写其中一个
        #         # tpRate=0.1,  # 挂单止盈率
        #         quantityCT=self.convert_size(symbol, quantity),  # 平仓数量，注意：quantityCT是合约的张数，不是货币数量
        #         block=False,  # 是否堵塞
        #         tag='',  # 订单标签
        #         clOrdId='',  # 客户自定义订单ID
        #         meta={},  # 向回调函数中传递的参数字典
        #     )

    def edit_cur_order(self, symbol, ordId, new_size='', new_price='', **kwargs):
        """
            修改当前订单
        """
        set_order_result = self.trade.api.set_amend_order(instId=self.convert_symbol(symbol),
                                                          cxlOnFail=True,  # 当订单修改失败时，该订单是否需要自动撤销。默认为false
                                                          ordId=ordId,
                                                          newSz=new_size,
                                                          newPx=new_price)
        return {"orderId": set_order_result["data"][0]["ordId"]}

    def get_history_order(self, pair):
        ret = self.trade.api.get_orders_history(instType="SWAP",
                                                instId=self.convert_symbol(pair))["data"]
        return [OkxOrder(o) for o in ret]

    def get_recent_order(self, symbol, orderId):
        """
            获取单个订单详情
        :param symbol:
        :param orderId:
        :return:
        """
        get_order_result = self.trade.get_order(
            instId=self.convert_symbol(symbol),
            ordId=orderId
        )
        if isinstance(get_order_result, list) and len(get_order_result) == 0:
            raise Exception(f"订单不存在: {symbol} {orderId}")
        return OkxOrder(get_order_result["data"])

    def cancel_order(self, symbol, orderId):
        return self.trade.cancel_order(
            instId=self.convert_symbol(symbol),
            ordId=orderId
        )

    def get_dep_wd_status(self, deposit_margin_coin, deposit_network):
        canDep = False
        canWd = False
        result = self.ccxt_exchange.private_get_asset_currencies({"ccy": deposit_margin_coin})
        result = result["data"]
        network_name = "{}-{}".format(deposit_margin_coin, deposit_network)
        for r in result:
            if r["ccy"].lower() == deposit_margin_coin.lower() \
                    and r["chain"].lower() == network_name.lower():
                canDep = r["canDep"]
                canWd = r["canWd"]
        return canDep, canWd

    @timed_cache(timeout=60)
    def get_funding_rate(self, symbol, apy=True):
        # TODO: 数据准确性
        r = self.public_api.get_funding_rate(self.convert_symbol(symbol))["data"][-1]
        time_coef = 24 / ((float(r["nextFundingTime"]) - float(r["fundingTime"])) / 1000 / 60 / 60)
        funding_rate = float(r["fundingRate"])
        if apy:
            funding_rate *= 365 * time_coef
        return funding_rate

    def get_open_interest(self, symbol, usd=True):
        """返回USD OI"""
        r = float(self.public_api.get_open_interest("SWAP", instId=self.convert_symbol(symbol))["data"][-1]["oiCcy"])
        if usd:
            r *= self.get_tick_price(symbol)
        return r

    def transfer_all_fund_to_future(self):
        data = self.ccxt_exchange.fetch_balance({"type": "funding"})
        if not data.get("USDT"):
            return 0
        free_usdt = data["USDT"]["free"]
        if free_usdt > 0:
            self.ccxt_exchange.transfer("USDT", free_usdt, "funding", "trading")
        return free_usdt

    def withdraw_coin(self, withdraw_margin_coin, amount, withdraw_network, to_addr):
        if self.parent_account:
            okx = self.parent_account.ccxt_exchange
            self.transfer_usd_asset_to_sub_account(-amount, token_list=[withdraw_margin_coin])
            time.sleep(0.5)
        else:
            okx = self.ccxt_exchange
        params = {'tag': None, 'network': withdraw_network}
        withdraw_network = withdraw_margin_coin + "-" + withdraw_network
        for info in okx.private_get_asset_currencies({"ccy": withdraw_margin_coin})["data"]:
            if info["chain"].lower() == withdraw_network.lower():
                fee = info["minFee"]
                # 提币方式
                # 3：内部转账
                # 4：链上提币
                params.update({"fee": fee, "pwd": "",
                               "dest": 4, "chain": withdraw_network})
        # 划转资金
        okx.transfer(withdraw_margin_coin, amount, "trading", "funding")
        time.sleep(0.5)
        cex_transfer_margin = amount - float(params["fee"])
        logger.info(
            f"{self.exchange_code} 执行提币, 参数 {withdraw_margin_coin}, {amount}, {withdraw_network}, {to_addr}")
        withdraw_result = okx.withdraw(code=withdraw_margin_coin, amount=cex_transfer_margin,
                                       address=to_addr, tag=None, params=params)
        logger.info(withdraw_result)
        return withdraw_result

    def create_stoploss_order(self, symbol, side, amount, stopPrice):
        ret = self.ccxt_exchange.create_stop_market_order(symbol, side, amount, stopPrice)
        logger.info(f"止损止盈单: {symbol} {side} {amount} {stopPrice}")
        logger.info(ret)
        return ret

    def transfer_usd_asset_to_sub_account(self, amount,
                                          token_list=None):
        if token_list is None:
            token_list = ["USDT"]
        okx = self.parent_account.ccxt_exchange
        if amount > 0:
            transfer_type = "1"
            to_acc = "子账户"
            balance_info = self.parent_account.account.get_balances()
            balance_to_transfer = amount
            for ccy in token_list:
                for detail in balance_info["data"]["details"]:
                    if detail["ccy"] == ccy:
                        available_balance = float(detail["availBal"])
                        balance = int(min(balance_to_transfer, available_balance))
                        if balance < 1:
                            logger.info(f"⚠️ OKX主账户 {ccy}余额不足: {available_balance}")
                            continue
                        okx.private_post_asset_transfer({"ccy": ccy,
                                                         "amt": balance,
                                                         # 6：资金账户 18：交易账户
                                                         "from": "18",
                                                         "to": "18",
                                                         "subAcct": self.sub_account,
                                                         "type": transfer_type,
                                                         })
                        text = "⏰ OKX主账户划转{} {}到 {}".format(balance, ccy, to_acc)
                        logger.info(text)
                        send_slack_message(text)
                        time.sleep(1)
                        balance_to_transfer -= balance
            actual_transfer = abs(amount) - balance_to_transfer
            logger.info(f"OKX主账户共划转 {actual_transfer} USD 到{self.sub_account}")
            return actual_transfer
        else:
            transfer_type = "2"
            to_acc = "主账户"
        balance_to_transfer = abs(amount)
        balance_list = self.account.get_balances()["data"]["details"]
        for ccy in token_list:
            for info in balance_list:
                if info["ccy"] == ccy:
                    available_balance = float(info["availBal"])
                    balance = int(min(balance_to_transfer, available_balance))
                    if balance < 1:
                        logger.info(f"OKX子账户{self.sub_account} {ccy}余额不足")
                        continue
                    okx.private_post_asset_transfer({"ccy": ccy,
                                                     "amt": balance,
                                                     # 6：资金账户 18：交易账户
                                                     "from": "18",
                                                     "to": "18",
                                                     "subAcct": self.sub_account,
                                                     "type": transfer_type,
                                                     })
                    text = "⏰ OKX子账户{}划转{} {}到 {}".format(self.sub_account, balance, ccy, to_acc)
                    logger.info(text)
                    send_slack_message(text)
                    time.sleep(1)
                    balance_to_transfer -= balance
                    # 子账户间划转
                    # okx.private_post_asset_subaccount_transfer({"ccy": ccy,
                    #                                             "amt": balance,
                    #                                             "from": "6",
                    #                                             "to": "18",
                    #                                             "fromSubAccount": sub_account1,
                    #                                             "toSubAccount": sub_account2,
                    #                                             })

    def get_cross_margin_ratio(self):
        """
            维持仓位的保证金比例(达到100%会被清算)
        """
        r = self.account.get_balances()["data"]["mgnRatio"]
        if not r or r == "0":
            return 0
        return 1 / float(r)

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

    def get_deposit_pending_token(self, token="USDT"):
        """获取正在充值的USDT数量"""
        # https://www.okx.com/docs-v5/zh/#funding-account-rest-api-get-deposit-history
        params = {"type": "4", "state": "0", "ccy": token}
        deposit_history_list = self.ccxt_exchange.private_get_asset_deposit_history(params=params)["data"]
        amount = 0
        for info in deposit_history_list:
            amount += float(info["amt"])
        return amount


if __name__ == '__main__':
    pass
