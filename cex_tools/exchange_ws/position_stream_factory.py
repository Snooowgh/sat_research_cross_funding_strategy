# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : position_stream_factory
@Description : 仓位WebSocket流工厂类
@Time        : 2025/10/15
"""
import asyncio
from typing import Dict, Optional, Callable, List
from loguru import logger

from cex_tools.exchange_ws.binance_unified_position_stream import BinanceUnifiedPositionWebSocket
from config import ExchangeConfig

# 导入仓位WebSocket流实现
from cex_tools.exchange_ws.position_stream import PositionWebSocketStream
from cex_tools.exchange_ws.binance_position_stream import BinancePositionWebSocket
from cex_tools.exchange_ws.hyperliquid_position_stream import HyperliquidPositionWebSocket
from cex_tools.exchange_ws.okx_position_stream import OkxPositionWebSocket
from cex_tools.exchange_ws.bybit_position_stream import BybitPositionWebSocket
from cex_tools.exchange_ws.lighter_position_stream import LighterPositionWebSocket
from cex_tools.exchange_ws.aster_position_stream import AsterPositionWebSocket

# 导入仓位事件模型
from cex_tools.exchange_model.position_event_model import PositionEvent


class PositionStreamFactory:
    """仓位WebSocket流工厂类"""

    # 支持的交易所流类映射
    STREAM_CLASSES = {
        'binance': BinancePositionWebSocket,
        'binance_unified': BinanceUnifiedPositionWebSocket,
        'hyperliquid': HyperliquidPositionWebSocket,
        'okx': OkxPositionWebSocket,
        'bybit': BybitPositionWebSocket,
        'lighter': LighterPositionWebSocket,
        'aster': AsterPositionWebSocket,
    }

    EXCHANGE_WS_CONFIGS = {
        'binance': ExchangeConfig.get_binance_ws_config,
        'binance_unified': ExchangeConfig.get_binance_unified_ws_config,
        'lighter': ExchangeConfig.get_lighter_ws_config,
        'hyperliquid': ExchangeConfig.get_hyperliquid_ws_config,
        'bybit': ExchangeConfig.get_bybit_ws_config,
        'aster': ExchangeConfig.get_aster_ws_config,
        'okx': ExchangeConfig.get_okx_ws_config,
    }

    @staticmethod
    def create_position_stream(exchange_code: str,
                           on_order_update_callback: Callable[[PositionEvent], None]) -> Optional[PositionWebSocketStream]:
        """
        创建指定交易所的仓位WebSocket流

        Args:
            exchange_code: 交易所代码 (如 "binance", "hyperliquid")
            on_order_update_callback: 仓位事件回调函数

        Returns:
            PositionWebSocketStream: 仓位WebSocket流实例，失败返回None
        """
        try:
            exchange_code = exchange_code.lower()

            if exchange_code not in PositionStreamFactory.STREAM_CLASSES:
                logger.error(f"❌ 不支持的交易所仓位流: {exchange_code}")
                return None

            stream_class = PositionStreamFactory.STREAM_CLASSES[exchange_code]

            stream = stream_class(**PositionStreamFactory.EXCHANGE_WS_CONFIGS[exchange_code](),
                                  on_order_update_callback=on_order_update_callback)

            return stream

        except Exception as e:
            logger.error(f"❌ 创建 {exchange_code} 仓位WebSocket流失败: {e}")
            logger.exception(e)
            return None

    @staticmethod
    def create_multiple_streams(exchange_codes,
                                on_order_update_callback: Callable[[PositionEvent], None]) -> Dict[str, PositionWebSocketStream]:
        """
        创建多个交易所的仓位WebSocket流

        Args:
            exchange_codes: 交易所
            on_order_update_callback: 仓位事件回调函数

        Returns:
            Dict[str, PositionWebSocketStream]: 仓位WebSocket流字典
        """
        streams = {}

        for exchange_code in exchange_codes:
            try:
                stream = PositionStreamFactory.create_position_stream(exchange_code, on_order_update_callback)
                if stream:
                    streams[exchange_code] = stream
                else:
                    logger.warning(f"⚠️ 跳过 {exchange_code} 仓位流创建")
            except Exception as e:
                logger.error(f"❌ 创建 {exchange_code} 仓位流异常: {e}")

        logger.info(f"✅ 成功创建 {len(streams)}/{len(exchange_codes)} 个仓位WebSocket流")
        return streams

    @staticmethod
    def get_supported_exchanges() -> List[str]:
        """
        获取支持的交易所列表

        Returns:
            List[str]: 支持的交易所代码列表
        """
        return list(PositionStreamFactory.STREAM_CLASSES.keys())

    @staticmethod
    def validate_exchange_support(exchange_code: str) -> bool:
        """
        验证是否支持指定交易所

        Args:
            exchange_code: 交易所代码

        Returns:
            bool: 是否支持
        """
        return exchange_code.lower() in PositionStreamFactory.STREAM_CLASSES


class PositionStreamManager:
    """仓位WebSocket流管理器"""

    def __init__(self):
        self.streams: Dict[str, PositionWebSocketStream] = {}
        self.is_running = False

    async def start_streams(self, exchange_codes: List[str],
                            on_order_update_callback: Callable[[PositionEvent], None] = None) -> bool:
        """
        启动多个仓位WebSocket流

        Args:
            exchange_codes: 交易所
            on_order_update_callback: 仓位事件回调函数

        Returns:
            bool: 是否全部启动成功
        """
        try:
            # 创建流
            self.streams = PositionStreamFactory.create_multiple_streams(
                exchange_codes, on_order_update_callback
            )

            if not self.streams:
                logger.error("❌ 没有可用的仓位WebSocket流")
                return False

            # 并发启动所有流
            start_tasks = []
            for exchange_code, stream in self.streams.items():
                task = asyncio.create_task(self._start_single_stream(exchange_code, stream))
                start_tasks.append(task)

            # 等待所有启动任务完成
            results = await asyncio.gather(*start_tasks, return_exceptions=True)

            # 检查启动结果
            success_count = 0
            for i, result in enumerate(results):
                exchange_code = list(self.streams.keys())[i]
                if isinstance(result, Exception):
                    logger.error(f"❌ {exchange_code} 流启动失败: {result}")
                    # 移除失败的流
                    self.streams.pop(exchange_code, None)
                else:
                    success_count += 1

            self.is_running = success_count > 0

            if success_count == len(self.streams):
                logger.success(f"✅ 所有 {success_count} 个仓位WebSocket流启动成功")
                return True
            else:
                logger.warning(f"⚠️ 部分仓位WebSocket流启动失败: {success_count}/{len(self.streams)}")
                return success_count > 0

        except Exception as e:
            logger.error(f"❌ 启动仓位WebSocket流失败: {e}")
            return False

    async def _start_single_stream(self, exchange_code: str, stream: PositionWebSocketStream):
        """启动单个WebSocket流"""
        try:
            await stream.start()
            logger.success(f"✅ {exchange_code} 仓位WebSocket流启动成功")
        except Exception as e:
            logger.error(f"❌ {exchange_code} 仓位WebSocket流启动失败: {e}")
            raise

    async def stop_all_streams(self):
        """停止所有WebSocket流"""
        if not self.streams:
            return

        logger.info("🛑 停止所有仓位WebSocket流")

        # 并发停止所有流
        stop_tasks = []
        for exchange_code, stream in self.streams.items():
            task = asyncio.create_task(self._stop_single_stream(exchange_code, stream))
            stop_tasks.append(task)

        # 等待所有停止任务完成
        await asyncio.gather(*stop_tasks, return_exceptions=True)

        self.streams.clear()
        self.is_running = False
        logger.info("✅ 所有仓位WebSocket流已停止")

    async def _stop_single_stream(self, exchange_code: str, stream: PositionWebSocketStream):
        """停止单个WebSocket流"""
        try:
            logger.info(f"⏹️ 停止 {exchange_code} 仓位WebSocket流")
            await stream.stop()
            logger.debug(f"✅ {exchange_code} 仓位WebSocket流已停止")
        except Exception as e:
            logger.warning(f"⚠️ 停止 {exchange_code} 仓位WebSocket流异常: {e}")

    def get_status_report(self) -> str:
        """获取所有流的状态报告"""
        if not self.streams:
            return "📊 仓位WebSocket管理器状态:\n  • 没有活跃的流"
        report = f"📊 仓位WebSocket管理器状态\n"
        report += f"  • 管理器状态: {'🟢 运行中' if self.is_running else '🔴 已停止'}\n"
        report += f"  • 活跃流数量: {len(self.streams)}\n\n"
        return report.strip()

    def get_running_exchanges(self) -> List[str]:
        """获取正在运行的交易所列表"""
        return list(self.streams.keys())

    def is_exchange_running(self, exchange_code: str) -> bool:
        """检查指定交易所是否正在运行"""
        return exchange_code.lower() in self.streams
