# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : single_exchange_info_show
@Description : 获取单个交易所详细信息
@Time        : 2024/10/24
"""
import asyncio
import time
from loguru import logger

from arbitrage_param import MultiExchangeArbitrageParam
from cex_tools.exchange_model.multi_exchange_info_model import SingleExchangeInfoModel, MultiExchangeCombinedInfoModel
from cex_tools.async_funding_spread_searcher import AsyncFundingSpreadSearcher, SearchConfig
from utils.notify_tools import notify_telegram, CHANNEL_TYPE


async def get_single_exchange_info_model(async_exchange) -> SingleExchangeInfoModel:
    """
    获取单个交易所的详细信息

    :param async_exchange: 交易所客户端实例
    :return: SingleExchangeInfoModel
    """
    exchange_info = SingleExchangeInfoModel()
    exchange_info.exchange_code = async_exchange.exchange_code
    exchange_info.taker_fee_rate = async_exchange.taker_fee_rate
    exchange_info.maker_fee_rate = async_exchange.maker_fee_rate
    start = time.time()

    try:
        # 获取账户资金信息
        exchange_info.total_margin = await async_exchange.get_total_margin()
        exchange_info.available_margin = await async_exchange.get_available_margin()
        exchange_info.maintenance_margin_ratio = await async_exchange.get_cross_margin_ratio()
        # 获取仓位信息
        exchange_info.positions = await async_exchange.get_all_cur_positions()
        # 获取仓位funding rate
        for pos in exchange_info.positions:
            pos.funding_rate = await async_exchange.get_funding_rate(pos.pair)
        exchange_info.time_cost = time.time() - start
        return exchange_info

    except Exception as e:
        logger.error(f"获取 {async_exchange.exchange_code} 交易所信息失败: {e}")
        exchange_info.time_cost = time.time() - start
        return exchange_info


async def get_multi_exchange_info_combined_model(async_exchange_list, find_opportunities=True, opportunity_limit=5) -> MultiExchangeCombinedInfoModel:
    """
    获取多个交易所的综合信息模型

    Args:
        async_exchange_list: 异步交易所列表

    Returns:
        MultiExchangeCombinedInfoModel: 多交易所综合信息模型
    """
    start_time = time.time()
    combined_info = MultiExchangeCombinedInfoModel()

    try:
        # 并行获取各个交易所的信息
        tasks = [get_single_exchange_info_model(exchange) for exchange in async_exchange_list]
        exchange_infos = await asyncio.gather(*tasks, return_exceptions=True)

        # 过滤出成功获取的信息
        successful_exchange_infos = []
        for i, result in enumerate(exchange_infos):
            if isinstance(result, Exception):
                logger.error(f"获取交易所 {async_exchange_list[i].exchange_code} 信息失败: {result}")
            else:
                successful_exchange_infos.append(result)
                logger.info(f"✅ 获取 {result.exchange_code} 交易所信息成功")

        # 设置交易所信息列表
        combined_info.exchange_infos = successful_exchange_infos

        # 计算汇总信息
        combined_info.calculate_summary()

        # 合并仓位信息
        combined_info.merge_positions()

        # 搜索费率套利机会（如果有至少2个交易所）
        if find_opportunities:
            if len(async_exchange_list) >= 2:
                logger.info("开始搜索费率套利机会...")
                await search_funding_opportunities(combined_info, async_exchange_list,
                                                   top_chance_limit=opportunity_limit)

        # 设置耗时
        combined_info.time_cost = time.time() - start_time

        logger.info(
            f"多交易所综合信息获取完成，共 {len(successful_exchange_infos)} 个交易所，耗时 {combined_info.time_cost:.2f}s")
        return combined_info

    except Exception as e:
        logger.error(f"获取多交易所综合信息失败: {e}")
        combined_info.time_cost = time.time() - start_time
        return combined_info


async def search_funding_opportunities(combined_info, async_exchange_list, top_chance_limit=5):
    """
    搜索费率套利机会并添加到综合信息模型中

    Args:
        combined_info: 多交易所综合信息模型
        async_exchange_list: 异步交易所列表
    """
    try:
        # 创建搜索配置
        search_config = SearchConfig(
            min_funding_diff=0.2,  # 最小费率差20%
            min_mean_spread_profit_rate=0.001,  # 最小均值价差收益率0.1%
            max_opportunities=10,  # 搜索最多10个机会
            include_spread_analysis=True,
            use_white_list=False,
        )

        all_opportunities = []

        # 遍历所有交易所组合
        for i in range(len(async_exchange_list)):
            for j in range(i + 1, len(async_exchange_list)):
                exchange1 = async_exchange_list[i]
                exchange2 = async_exchange_list[j]

                logger.info(f"搜索 {exchange1.exchange_code} vs {exchange2.exchange_code} 费率套利机会...")

                # 创建搜索器
                searcher = AsyncFundingSpreadSearcher(
                    exchange1=exchange1,
                    exchange2=exchange2,
                    search_config=search_config
                )

                # 搜索机会
                opportunities = await searcher.search_opportunities()
                all_opportunities.extend(opportunities)

                logger.info(
                    f"找到 {len(opportunities)} 个 {exchange1.exchange_code}-{exchange2.exchange_code} 套利机会")

        # 按收益率排序并取前x个
        all_opportunities.sort(key=lambda x: x.funding_profit_rate, reverse=True)
        combined_info.funding_opportunities = all_opportunities[:top_chance_limit]

        logger.info(f"总共找到 {len(all_opportunities)} 个费率套利机会，展示前{top_chance_limit}个最佳机会")

    except Exception as e:
        logger.error(f"搜索费率套利机会失败: {e}")
        combined_info.funding_opportunities = []


async def multi_exchange_arbitrage_info_show():
    """
    主函数示例，展示如何使用单个和多个交易所信息获取功能
    """
    arbitrage_param = MultiExchangeArbitrageParam()
    await arbitrage_param.init_async_exchanges()
    # 获取并显示多交易所综合信息
    combined_info = await get_multi_exchange_info_combined_model(arbitrage_param.async_exchange_list)
    await arbitrage_param.close_async_exchanges()
    return combined_info


def notify_multi_exchange_arbitrage_info():
    info = asyncio.run(multi_exchange_arbitrage_info_show())
    should, msg = info.should_notify_risk()
    if should:
        notify_telegram(f"❌❌ 风控提醒:\n{msg}")
    notify_telegram(str(info), channel_type=CHANNEL_TYPE.CEX_QUIET)


if __name__ == "__main__":
    notify_multi_exchange_arbitrage_info()
