# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : lighter_future
@Description :
@Time        : 2025/9/19 19:00
"""
import asyncio
import lighter
import requests

from cex_tools.cex_enum import ExchangeEnum
from cex_tools.exchange_model.kline_bar_model import LighterKlineBar
from cex_tools.exchange_model.order_model import LighterOrder
from cex_tools.exchange_model.position_model import LighterPositionDetail
from cex_tools.exchange_model.funding_rate_model import FundingRateHistory, FundingHistory, FundingRateHistoryResponse, FundingHistoryResponse
from cex_tools.funding_rate_cache import FundingRateCache
from utils.decorators import timed_cache, async_timed_cache
from utils.notify_tools import send_slack_message
from loguru import logger
import time


class LighterFuture:
    """https://github.com/elliottech/lighter-python"""

    def __init__(self, l1_addr, api_private_key, account_index, api_key_index,
                 base_url="https://mainnet.zklighter.elliot.ai", account_tier="premium", **kwargs):
        self.l1_addr = l1_addr
        self.api_key_index = api_key_index
        self.private_key = api_private_key
        self.base_url = base_url
        self.account_tier = account_tier
        self.account_index = account_index
        self.exchange_code = ExchangeEnum.LIGHTER
        self.order_book_detail_map = {}
        self.auth = None

        # 延迟初始化的组件
        self.api_client = None
        self.client = None
        self._initialized = False
        self.maker_fee_rate = kwargs.get("maker_fee_rate", 0)
        self.taker_fee_rate = kwargs.get("taker_fee_rate", 0)

    def changeAccountTier(self, new_tier="premium"):
        """
            https://apibetadocs.lighter.xyz/docs/account-types
        """
        # new_tier = "standard"
        if new_tier not in ["standard", "premium"]:
            raise ValueError(f"Invalid tier: only standard, premium")
        response = requests.post(
            f"{self.base_url}/api/v1/changeAccountTier",
            data={"account_index": self.account_index, "new_tier": new_tier},
            headers={"Authorization": self.auth},
        )
        if response.status_code != 200:
            print(f"Error: {response.text}")
            return response.json()
        return response.json()

    def __convert_symbol(self, symbol):
        if symbol is None:
            return symbol
        return symbol.replace("USDT", "")

    async def init(self):
        if not self._initialized:
            config = lighter.Configuration(host=self.base_url)
            self.api_client = lighter.ApiClient(configuration=config)
            self.client = lighter.SignerClient(
                url=self.base_url,
                private_key=self.private_key,
                account_index=self.account_index,
                api_key_index=self.api_key_index,
            )
            err = self.client.check_client()
            if err is not None:
                logger.error(f"Lighter CheckClient error: {err}")
            self._initialized = True

        self.order_book_detail_map = await self.get_pair_info_map()
        self.__update_auth()

    def __update_auth(self):
        # 120min
        deadline = int(time.time() + 120 * 60)
        self.auth, err = self.client.create_auth_token_with_expiry(deadline=deadline)

    @async_timed_cache(timeout=60 * 60)
    async def get_pair_info_map(self):
        order_book_details_ret = await self.client.order_api.order_book_details()
        order_book_details = order_book_details_ret.order_book_details
        order_book_detail_map = {}
        for order_book_detail in order_book_details:
            if order_book_detail.status == "active":
                order_book_detail_map[order_book_detail.symbol] = order_book_detail
        return order_book_detail_map

    async def get_all_tick_price(self, symbol=None):
        symbol = self.__convert_symbol(symbol)
        order_book_details_ret = await self.client.order_api.order_book_details()
        order_book_details = order_book_details_ret.order_book_details
        for order_book_detail in order_book_details:
            if symbol and order_book_detail.symbol == symbol:
                return float(order_book_detail.last_trade_price)
        ret = []
        for d in order_book_details:
            t = {}
            try:
                t["name"] = d.symbol
                t["midPx"] = float(d.last_trade_price)
                ret.append(t)
            except:
                pass
        return ret

    async def get_tick_price(self, symbol):
        symbol = self.__convert_symbol(symbol)
        return float(await self.get_all_tick_price(symbol))

    async def make_new_order(self, symbol, side, order_type, quantity, price, msg="", **kwargs):
        symbol = self.__convert_symbol(symbol)
        pair_info = self.order_book_detail_map[symbol]
        client_order_index = int(time.time() * 10000) - pair_info.market_id
        if quantity < float(pair_info.min_base_amount):
            print(f"{symbol=} size too small: {quantity} < {pair_info.min_base_amount} ")
            return
        order_type = order_type.upper()
        side = side.upper()
        if side not in ["BUY", "SELL"]:
            raise ValueError(f"Invalid side: {side}")
        if side == "BUY":
            is_ask = False
        else:
            is_ask = True
        if kwargs.get("reduceOnly") is True:
            reduce_only = 1
        else:
            reduce_only = 0
        base_amount = int(
            round(float(quantity), pair_info.supported_size_decimals) * 10 ** pair_info.size_decimals)
        start_time = time.time()
        if order_type == "LIMIT":
            order_type = lighter.SignerClient.ORDER_TYPE_LIMIT
            time_in_force = lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
            executed_price = int(
                round(float(price), pair_info.supported_price_decimals) * 10 ** pair_info.price_decimals)
            tx, tx_hash, err = await self.client.create_order(
                market_index=pair_info.market_id,
                client_order_index=client_order_index,
                base_amount=base_amount,
                price=executed_price,
                is_ask=is_ask,
                order_type=order_type,
                time_in_force=time_in_force,
                reduce_only=reduce_only
            )
        elif order_type == "MARKET":
            order_type = lighter.SignerClient.ORDER_TYPE_MARKET
            if is_ask:
                price = price * 0.97
            else:
                price = price * 1.03
            executed_price = int(
                round(float(price), pair_info.supported_price_decimals) * 10 ** pair_info.price_decimals)
            tx, tx_hash, err = await self.client.create_market_order(
                market_index=pair_info.market_id,
                client_order_index=client_order_index,
                base_amount=base_amount,
                avg_execution_price=executed_price,
                is_ask=is_ask,
                reduce_only=reduce_only
            )
        else:
            raise Exception("暂不支持该订单类型")
        content = f"⚠️ {msg} {self.exchange_code} 下单: {symbol} {side} {order_type} {price} {quantity}({client_order_index})" \
                  f"{(time.time() - start_time) * 1000:.2f}ms"
        logger.info(content)
        if err:
            raise Exception(f"❌ {lighter} 订单执行异常: {err}")
        if msg:
            send_slack_message(content)
        logger.debug(tx.to_json())
        return {"orderId": client_order_index}

    async def get_recent_order(self, symbol, orderId=None, limit=1):
        """
        Order(order_index=281475135771150, client_order_index=123, order_id='281475135771150',
        client_order_id='123', market_index=0, owner_account_index=105021,
        initial_base_amount='0.0100', price='4900.00', nonce=159060494,
        remaining_base_amount='0.0000', is_ask=True, base_size=0, base_price=490000,
        filled_base_amount='0.0000', filled_quote_amount='0.000000', side='', type='limit',
        time_in_force='good-till-time', reduce_only=False, trigger_price='0.00', order_expiry=1760697843825,
        status='canceled', trigger_status='na', trigger_time=0, parent_order_index=0, parent_order_id='0',
        to_trigger_order_id_0='0', to_trigger_order_id_1='0', to_cancel_order_id_0='0', block_height=50041233,
        timestamp=1758278645, additional_properties={'created_at': 1758278644, 'updated_at': 1758278645})
        :param symbol:
        :param orderId:
        :return:
        """
        symbol = self.__convert_symbol(symbol)
        try:
            # active_orders = await self.client.order_api.account_active_orders(self.account_index,
            #                                                   self.order_book_detail_map[symbol].market_id,
            #                                                   self.auth)
            time_now = int(time.time())
            between_timestamps = f"{time_now - 3600}-{time_now + 3600}"
            inactive_orders = await self.client.order_api.account_inactive_orders(self.account_index,
                                                                                  limit,
                                                                                  self.auth,
                                                                                  market_id=self.order_book_detail_map[
                                                                                      symbol].market_id,
                                                                                  ask_filter=-1,
                                                                                  between_timestamps=between_timestamps)
        except Exception as e:
            if "auth" in str(e):
                self.__update_auth()
                raise e
        if not orderId and len(inactive_orders.orders) > 0:
            return LighterOrder(inactive_orders.orders[-1])
        for order in inactive_orders.orders:
            # if order.order_index == orderId:
            if str(order.client_order_index) == str(orderId):
                return LighterOrder(order)
        return None

    async def cancel_order(self, symbol, orderId):
        symbol = self.__convert_symbol(symbol)
        return await self.client.cancel_order(
            market_index=self.order_book_detail_map[symbol].market_id,
            order_index=orderId,
        )

    @async_timed_cache(timeout=60)
    async def get_funding_rate(self, symbol, apy=True):
        # 优先从缓存获取
        cache = FundingRateCache()
        cached_rate = cache.get_funding_rate("lighter", symbol)

        if cached_rate is not None:
            # 缓存中的是单次费率，Lighter是每小时一次
            if apy:
                return cached_rate * 3 * 365
            else:
                return cached_rate

        # 缓存未命中，使用原接口
        symbol = self.__convert_symbol(symbol)
        market_id = self.order_book_detail_map[symbol].market_id
        resolution = "1h"
        start_timestamp = int(time.time() - 60 * 60)
        end_timestamp = int(time.time()) + 1
        count_back = 1
        funding_rates = await lighter.CandlestickApi(self.api_client).fundings(market_id, resolution,
                                                                               start_timestamp, end_timestamp,
                                                                               count_back)
        latest_funding = funding_rates.fundings[-1]
        funding1h = float(latest_funding.rate)
        if latest_funding.direction == "short":
            funding1h = -funding1h
        if apy:
            return funding1h * 24 * 365 / 100
        else:
            return funding1h / 100

    async def close(self):
        """关闭所有连接和清理资源"""
        try:
            if self.client:
                await self.client.close()
                logger.debug("[Lighter] SignerClient已关闭")
        except Exception as e:
            logger.warning(f"[Lighter] 关闭SignerClient时出现警告: {e}")

        try:
            if self.api_client:
                await self.api_client.close()
                logger.debug("[Lighter] ApiClient已关闭")
        except Exception as e:
            logger.warning(f"[Lighter] 关闭ApiClient时出现警告: {e}")

        self._initialized = False
        logger.info("[Lighter] 所有连接已关闭")

    @async_timed_cache(timeout=3)
    async def get_account_info(self):
        return (await lighter.AccountApi(self.api_client).account(by="index", value=str(self.account_index))).accounts[
            0]

    async def get_all_cur_positions(self):
        account_info = await self.get_account_info()
        positions = list(filter(lambda a: float(a.position) > 0, account_info.positions))
        return list(map(lambda a: LighterPositionDetail(a, self.exchange_code), positions))

    async def get_available_margin(self):
        return float((await self.get_account_info()).available_balance)

    async def get_total_margin(self):
        # total_asset_value
        return float((await self.get_account_info()).total_asset_value)
        # return float((await self.get_account_info()).collateral)

    async def cancel_all_orders(self, symbol=None, limit=3):
        market_id = self.order_book_detail_map[
            self.__convert_symbol(symbol)].market_id
        try:
            active_orders = await self.client.order_api.account_active_orders(self.account_index,
                                                                              market_id,
                                                                              self.auth)
        except Exception as e:
            if "auth" in str(e):
                self.__update_auth()
                raise e
        for order in active_orders.orders:
            await self.client.cancel_order(
                market_index=self.order_book_detail_map[symbol].market_id,
                order_index=order.order_index,
            )

    async def get_klines(self, symbol, interval, limit=500):
        symbol = self.__convert_symbol(symbol)
        market_id = self.order_book_detail_map[symbol].market_id
        resolution = interval
        if resolution.endswith("m"):
            minutes = int(resolution[:-1])
            start_timestamp = int(time.time() - minutes * 60 * limit)
        elif resolution.endswith("h"):
            hours = int(resolution[:-1])
            start_timestamp = int(time.time() - hours * 60 * 60 * limit)
        elif resolution.endswith("d"):
            days = int(resolution[:-1])
            start_timestamp = int(time.time() - days * 24 * 60 * 60 * limit)
        else:
            raise ValueError(f"Invalid interval: {interval}")
        # 结束时间多加1秒，防止取不到最新的K线
        end_timestamp = int(time.time()) + 1
        count_back = 2
        data = await lighter.CandlestickApi(self.api_client).candlesticks(market_id, resolution,
                                                                          start_timestamp, end_timestamp,
                                                                          count_back)

        return [LighterKlineBar(d) for d in data.candlesticks]

    async def get_cross_margin_ratio(self):
        """
            维持仓位的保证金比例(达到100%会被清算)
        """
        try:
            pair_info_map = await self.get_pair_info_map()
            account_info = await self.get_account_info()
            maintenance_margin = 0
            for pos in account_info.positions:
                maintenance_margin += float(pos.position_value) * float(pair_info_map[pos.symbol].maintenance_margin_fraction) / 10000
            total_asset_value = float(account_info.total_asset_value)

            if total_asset_value == 0:
                return 0

            return maintenance_margin / total_asset_value
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 获取全仓保证金比例失败: {e}")
            return 0

    def convert_size(self, symbol, size):
        """
        转换下单数量精度 - 高性能版本，使用预加载的缓存
        根据交易对的supported_size_decimals字段格式化数量精度
        :param symbol: 交易对
        :param size: 数量
        :return: 格式化后的数量
        """
        try:
            # 转换symbol格式 (Lighter使用不带USDT的格式)
            symbol = self.__convert_symbol(symbol)
            pair_info = self.order_book_detail_map.get(symbol)
            # 使用supported_size_decimals字段进行精度格式化
            size_decimals = getattr(pair_info, 'supported_size_decimals', 6)
            if size_decimals is not None and size_decimals >= 0:
                # 使用f-string格式化，性能更好
                return float(f"{size:.{size_decimals}f}")
            else:
                logger.warning(f"[Lighter] 交易对 {symbol} 的精度信息无效: {size_decimals}")
                return size

        except Exception as e:
            logger.error(f"[Lighter] 转换数量精度失败: {symbol} {size} - {e}")
            return size

    async def get_funding_rate_history(self, symbol: str, limit: int = 100,
                                     start_time: int = None, end_time: int = None,
                                     apy: bool = True) -> FundingRateHistoryResponse:
        """
        获取交易品种的历史资金费率

        Args:
            symbol: 交易对符号 (如 "BTC")
            limit: 返回数据条数，默认100
            start_time: 开始时间戳 (毫秒)，可选
            end_time: 结束时间戳 (毫秒)，可选
            apy: 是否返回年化费率，默认True

        Returns:
            FundingRateHistoryResponse: 历史资金费率响应对象
        """
        try:
            # 确保已初始化
            if not self._initialized:
                await self.init()

            # 转换symbol格式 (Lighter使用不带USDT的格式)
            symbol = self.__convert_symbol(symbol)
            if symbol not in self.order_book_detail_map:
                logger.error(f"[{self.exchange_code}] 交易对 {symbol} 不存在")
                return FundingRateHistoryResponse(symbol=symbol, limit=limit, total=0)

            market_id = self.order_book_detail_map[symbol].market_id

            # 设置时间范围
            if end_time is None:
                end_time = int(time.time() * 1000)
            if start_time is None:
                # 根据limit计算开始时间，假设每小时一个数据点
                start_time = end_time - (limit * 60 * 60 * 1000)

            # Lighter使用1h分辨率获取资金费率历史
            resolution = "1h"
            count_back = min(limit, 1000)  # 限制最大数量

            # 调用Lighter API获取历史资金费率
            funding_rates = await lighter.CandlestickApi(self.api_client).fundings(
                market_id, resolution, start_time, end_time, count_back
            )

            # 转换为统一的数据模型
            response = FundingRateHistoryResponse(
                symbol=symbol + "USDT",  # 转换回标准格式
                limit=limit,
                total=len(funding_rates.fundings),
                start_time=start_time,
                end_time=end_time
            )

            for funding in funding_rates.fundings:
                funding_rate = float(funding.rate) / 100  # Lighter返回的是百分比，需要转换为小数
                funding_time = int(funding.timestamp * 1000)  # 转换为毫秒

                # 根据direction调整费率正负
                if funding.direction == "short":
                    funding_rate = -funding_rate

                # 计算年化费率 (Lighter每小时一次，一天24次)
                annualized_rate = funding_rate * 24 * 365 if apy else funding_rate

                rate_data = FundingRateHistory(
                    symbol=symbol + "USDT",
                    funding_rate=funding_rate,
                    funding_time=funding_time,
                    annualized_rate=annualized_rate if apy else None
                )
                response.add_rate(rate_data)

            # 按时间排序（从旧到新）
            response.sort_by_time()

            logger.info(f"[{self.exchange_code}] 获取 {symbol} 历史资金费率成功: {len(response.data)} 条")
            return response

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 获取 {symbol} 历史资金费率失败: {e}")
            return FundingRateHistoryResponse(symbol=symbol, limit=limit, total=0)

    async def get_funding_history(self, symbol: str = None, limit: int = 100,
                                 start_time: int = None, end_time: int = None) -> FundingHistoryResponse:
        """
        获取用户仓位收取的资金费历史记录

        Args:
            symbol: 交易对符号 (如 "BTC")，可选，不指定则返回所有交易对
            limit: 返回数据条数，默认100
            start_time: 开始时间戳 (毫秒)，可选
            end_time: 结束时间戳 (毫秒)，可选

        Returns:
            FundingHistoryResponse: 用户资金费历史响应对象
        """
        try:
            # 确保已初始化
            if not self._initialized:
                await self.init()

            # 更新auth token
            self.__update_auth()

            # 设置时间范围
            if end_time is None:
                end_time = int(time.time())
            if start_time is None:
                start_time = end_time - (limit * 60 * 60)  # 假设每小时可能有一次资金费

            time_range = f"{start_time}-{end_time}"

            # 获取账户资金费历史
            # 注意：Lighter可能没有直接的资金费历史API，这里尝试不同的方法
            try:
                response = await lighter.AccountApi(self.api_client).position_funding(
                    account_index=self.account_index,
                    limit=limit,
                    auth=self.auth,
                    between_timestamps=time_range
                )
            except Exception as e:
                raise e

            logger.info(f"[{self.exchange_code}] 获取用户资金费历史成功: {len(response.data)} 条")
            return response

        except Exception as e:
            raise e


if __name__ == '__main__':
    pass
