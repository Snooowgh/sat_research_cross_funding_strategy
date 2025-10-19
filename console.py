# coding=utf-8
"""
@Project     : sat_research_cross_funding_strategy
@Author      : Arson
@File Name   : temp_script
@Description :
@Time        : 2025/10/18 10:48
"""
import asyncio
import time
from loguru import logger
from arbitrage_param import MultiExchangeArbitrageParam
from cex_tools.exchange_model.multi_exchange_info_model import SingleExchangeInfoModel, MultiExchangeCombinedInfoModel
from cex_tools.async_funding_spread_searcher import AsyncFundingSpreadSearcher, SearchConfig
from multi_exchange_info_show import get_multi_exchange_info_combined_model
from utils.notify_tools import notify_telegram, CHANNEL_TYPE


async def main():
    arbitrage_param = MultiExchangeArbitrageParam()
    await arbitrage_param.init_async_exchanges()

    get_combined_info = await get_multi_exchange_info_combined_model(arbitrage_param.async_exchange_list)
    print(get_combined_info)
    print(await arbitrage_param.binance_unified_exchange.get_all_cur_positions())
    print(await arbitrage_param.okx_exchange.get_all_cur_positions())

    # print(arbitrage_param.okx_exchange.okxSWAP.account.api.get_config())
    # await arbitrage_param.okx_exchange.make_new_order("BTC",
    #                                                 "BUY",
    #                                                 "LIMIT",
    #                                                 0.001,
    #                                                 105000,
    #                                               reduceOnly=True)
    # print(await arbitrage_param.binance_unified_exchange.make_new_order("BTC",
    #                                                               "BUY",
    #                                                               "LIMIT",
    #                                                               0.001,
    #                                                               105000))
    # await asyncio.sleep(3)
    # print(await arbitrage_param.binance_unified_exchange.get_open_orders(symbol="BTC"))
    # print(await arbitrage_param.binance_unified_exchange.cancel_all_orders(symbol="BTC"))
    # print(await arbitrage_param.binance_unified_exchange.get_all_cur_positions())


asyncio.run(main())
