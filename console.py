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
from utils.notify_tools import notify_telegram, CHANNEL_TYPE


async def main():
    arbitrage_param = MultiExchangeArbitrageParam()
    await arbitrage_param.init_async_exchanges()
    print(await arbitrage_param.binance_exchange.rest_client.rest_api.get_um_current_position_mode())
# asyncio.run(main())
