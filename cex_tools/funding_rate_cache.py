# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : chance_hunter
@Description :
@Time        : 2025/9/25 22:21
"""
import requests
import time
import threading
from typing import Dict, Optional
from loguru import logger

"""
    根据funding数据 找出最优机会
    针对每个机会 获取1m K线数据判断价差 均值和标准差
    比较当前价差，判断能否开仓
    执行ws开仓 上限5wu
"""


class FundingRateCache:
    """资金费率缓存单例（支持Binance、Lighter、HyperLiquid、Bybit、Okx、Aster六所）"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._cache: Dict[str, Dict[str, float]] = {}  # {exchange: {symbol: rate}}
            self._last_update_time: float = 0
            self._update_interval: int = 1800  # 半小时更新一次
            self._updating: bool = False
            self._update_lock = threading.Lock()
            self._initialized = True

    def get_funding_rate(self, exchange: str, symbol: str) -> Optional[float]:
        """
        获取缓存的资金费率
        :param exchange: 交易所名称 (binance/lighter/hyperliquid/bybit)
        :param symbol: 交易对符号
        :return: 资金费率（非年化，单次费率），如果不存在返回None
        """
        # 如果缓存过期或为空，尝试更新
        if self._should_update():
            self._try_update()

        exchange_lower = exchange.lower()
        symbol_upper = symbol.upper().replace("USDT", "").replace("USDC", "")

        if exchange_lower in self._cache and symbol_upper in self._cache[exchange_lower]:
            return self._cache[exchange_lower][symbol_upper]
        return None

    def _should_update(self) -> bool:
        """判断是否需要更新缓存"""
        return (time.time() - self._last_update_time) >= self._update_interval or not self._cache

    def _try_update(self):
        """尝试更新缓存（非阻塞）"""
        if self._updating:
            return  # 已经在更新中，跳过

        # 使用后台线程更新，避免阻塞主线程
        threading.Thread(target=self._update_cache, daemon=True).start()

    def _update_cache(self):
        """更新缓存（从API获取数据）"""
        # 8h 制
        with self._update_lock:
            if self._updating:
                return
            self._updating = True

        try:
            # 重建缓存
            new_cache: Dict[str, Dict[str, float]] = {}

            # 1. 获取原有 API 数据
            try:
                url1 = "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"
                ret1 = requests.get(url=url1, timeout=10).json()
                funding_rates1 = ret1.get("funding_rates", [])

                for item in funding_rates1:
                    exchange = item.get("exchange", "").lower()
                    symbol = item.get("symbol", "").upper()
                    rate = float(item.get("rate", 0))

                    if exchange not in new_cache:
                        new_cache[exchange] = {}
                    new_cache[exchange][symbol] = rate
            except Exception as e:
                logger.warning(f"获取lighter funding API 数据失败: {e}")

            # 2. 获取新 Aster API 数据
            try:
                url2 = "https://www.asterdex.com/bapi/future/v1/public/future/aster/marketing/funding-rate-comparison"
                ret2 = requests.get(url=url2, timeout=10).json()
                ret2 = ret2.get("data", {})
                details = ret2.get("details", [])

                # 过滤 8h 周期数据并处理
                aster_count = 0
                for item in details:
                    period = item.get("period", "")
                    if period != "8h":
                        continue

                    pair = item.get("pair", "")  # 带有USDT后缀
                    symbol = pair.replace("USDT", "")  # 移除USDT后缀

                    # Aster 交易所
                    aster_rate = item.get("asterFundingRate")
                    if aster_rate is not None:
                        if "aster" not in new_cache:
                            new_cache["aster"] = {}
                        new_cache["aster"][symbol] = float(aster_rate)
                        aster_count += 1

                    # Binance 交易所 (可覆盖原API数据)
                    bn_rate = item.get("bnFundingRate")
                    if bn_rate is not None:
                        if "binance" not in new_cache:
                            new_cache["binance"] = {}
                        new_cache["binance"][symbol] = float(bn_rate)

                    # Bybit 交易所
                    bybit_rate = item.get("bybitFundingRate")
                    if bybit_rate is not None:
                        if "bybit" not in new_cache:
                            new_cache["bybit"] = {}
                        new_cache["bybit"][symbol] = float(bybit_rate)

                    # OKX 交易所
                    okx_rate = item.get("okxFundingRate")
                    if okx_rate is not None:
                        if "okx" not in new_cache:
                            new_cache["okx"] = {}
                        new_cache["okx"][symbol] = float(okx_rate)

            except Exception as e:
                logger.warning(f"获取 Aster API 数据失败: {e}")

            self._cache = new_cache
            self._last_update_time = time.time()

            logger.info(f"资金费率缓存更新成功，覆盖 {len(new_cache)} 个交易所，"
                       f"共 {sum(len(v) for v in new_cache.values())} 个交易对")
        except Exception as e:
            logger.error(f"更新资金费率缓存失败: {e}")
        finally:
            self._updating = False

    def force_update(self):
        """强制立即更新缓存（阻塞）"""
        self._update_cache()

    def get_cache_info(self) -> dict:
        """获取缓存信息"""
        return {
            "last_update": self._last_update_time,
            "age_seconds": time.time() - self._last_update_time,
            "exchanges": list(self._cache.keys()),
            "total_pairs": sum(len(v) for v in self._cache.values())
        }
