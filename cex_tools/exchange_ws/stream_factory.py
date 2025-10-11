# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : stream_factory
@Description : WebSocket订单簿流工厂类 - 支持所有交易所
@Time        : 2025/10/8
"""
import asyncio
from typing import Optional, Dict, Any
from loguru import logger

from cex_tools.exchange_ws.orderbook_stream import OrderBookStream, OrderBookData
from cex_tools.exchange_ws.binance_ws_direct import BinanceOrderBookStreamDirect
from cex_tools.exchange_ws.lighter_orderbook_stream import LighterOrderBookStreamAsync
from cex_tools.exchange_ws.hyperliquid_orderbook_stream import HyperliquidOrderBookStream
from cex_tools.exchange_ws.bybit_orderbook_stream import BybitOrderBookStreamAsync
from cex_tools.exchange_ws.okx_orderbook_stream import OkxOrderBookStreamAsync
from cex_tools.exchange_ws.aster_orderbook_stream import AsterOrderBookStreamAsync
from cex_tools.async_exchange_adapter import AsyncExchangeAdapter


class StreamFactory:
    """WebSocket订单簿流工厂类 - 支持所有交易所"""

    # 交易所流类映射
    STREAM_CLASSES = {
        'binance': BinanceOrderBookStreamDirect,
        'lighter': LighterOrderBookStreamAsync,
        'hyperliquid': HyperliquidOrderBookStream,
        'bybit': BybitOrderBookStreamAsync,
        'okx': OkxOrderBookStreamAsync,
        'aster': AsterOrderBookStreamAsync,
    }

    @staticmethod
    async def create_orderbook_stream(exchange_code, symbol: str) -> Optional[OrderBookStream]:
        """
        根据交易所类型创建对应的订单簿流

        Args:
            exchange_code: 异步交易所适配器
            symbol: 交易对符号（如 "BTCUSDT"）

        Returns:
            OrderBookStream: 订单簿流实例，失败返回None
        """
        if exchange_code not in StreamFactory.STREAM_CLASSES:
            raise Exception(f"❌ 不支持ws: {exchange_code}")
        stream_class = StreamFactory.STREAM_CLASSES[exchange_code]
        stream = stream_class()
        stream.subscribe(symbol)
        return stream

    @staticmethod
    async def create_symbol_streams(exchange_code1,
                                   exchange_code2,
                                   symbol: str,
                                   callback1=None,
                                   callback2=None) -> tuple[Optional[OrderBookStream], Optional[OrderBookStream]]:
        """
        为两个交易所创建同一交易对的订单簿流

        Args:
            exchange_code1: 交易所1
            exchange_code2: 交易所2
            symbol: 交易对符号
            callback1: 交易所1的回调函数
            callback2: 交易所2的回调函数

        Returns:
            tuple: (stream1, stream2)
        """
        # 并行创建两个流
        tasks = [
            StreamFactory.create_orderbook_stream(exchange_code1, symbol),
            StreamFactory.create_orderbook_stream(exchange_code2, symbol)
        ]

        streams = await asyncio.gather(*tasks, return_exceptions=True)

        stream1 = streams[0] if not isinstance(streams[0], Exception) else None
        stream2 = streams[1] if not isinstance(streams[1], Exception) else None

        # 设置回调函数
        if stream1 and callback1:
            stream1.orderbook_callbacks[symbol] = [callback1]
        if stream2 and callback2:
            stream2.orderbook_callbacks[symbol] = [callback2]

        return stream1, stream2

    @staticmethod
    async def start_streams(*streams: OrderBookStream):
        """启动多个订单簿流"""
        try:
            # 并行启动所有流
            start_tasks = []
            for stream in streams:
                if stream and not stream.is_running:
                    start_tasks.append(stream.start())
                else:
                    start_tasks.append(None)

            # 等待所有启动完成
            await asyncio.gather(*[task for task in start_tasks if task], return_exceptions=True)

            for i, (stream, result) in enumerate(zip(streams, [r for r in start_tasks if r])):
                if stream:
                    if isinstance(result, Exception):
                        logger.error(f"❌ 启动 {stream.__class__.__name__} 失败: {result}")
                    else:
                        logger.info(f"🔄 {stream.__class__.__name__} 已启动")

        except Exception as e:
            logger.error(f"❌ 启动订单簿流失败: {e}")
            raise

    @staticmethod
    async def stop_streams(*streams: OrderBookStream):
        """停止多个订单簿流"""
        try:
            # 并行停止所有流
            stop_tasks = []
            for stream in streams:
                if stream and stream.is_running:
                    stop_tasks.append(stream.stop())
                else:
                    stop_tasks.append(None)

            # 等待所有停止完成
            await asyncio.gather(*[task for task in stop_tasks if task], return_exceptions=True)

            for i, (stream, result) in enumerate(zip(streams, [r for r in stop_tasks if r])):
                if stream:
                    if isinstance(result, Exception):
                        logger.error(f"❌ 停止 {stream.__class__.__name__} 失败: {result}")
                    else:
                        logger.info(f"⏹️ {stream.__class__.__name__} 已停止")

        except Exception as e:
            logger.error(f"❌ 停止订单簿流失败: {e}")

    @staticmethod
    def get_supported_exchanges() -> list[str]:
        """获取支持的交易所列表"""
        return list(StreamFactory.STREAM_CLASSES.keys())

    @staticmethod
    def validate_exchange_support(exchange_code: str) -> bool:
        """验证是否支持指定交易所"""
        return exchange_code.lower() in StreamFactory.STREAM_CLASSES


class StreamManager:
    """订单簿流管理器 - 增强版"""

    def __init__(self):
        self.active_streams: Dict[str, list[OrderBookStream]] = {}
        self.stream_pairs: Dict[str, tuple[OrderBookStream, OrderBookStream]] = {}
        self.stream_info: Dict[str, Dict[str, Any]] = {}  # 额外的流信息

    async def create_and_start_streams(self,
                                     exchange1: AsyncExchangeAdapter,
                                     exchange2: AsyncExchangeAdapter,
                                     symbol: str,
                                     callback1=None,
                                     callback2=None) -> bool:
        """
        创建并启动交易对的订单簿流

        Args:
            exchange1: 交易所1
            exchange2: 交易所2
            symbol: 交易对符号
            callback1: 交易所1的回调函数
            callback2: 交易所2的回调函数

        Returns:
            bool: 是否成功创建并启动
        """
        try:
            logger.info(f"🔄 开始创建 {symbol} 订单簿流对: {exchange1.exchange_code} <-> {exchange2.exchange_code}")

            # 验证交易所支持
            if not StreamFactory.validate_exchange_support(exchange1.exchange_code):
                logger.error(f"❌ 不支持的交易所: {exchange1.exchange_code}")
                return False

            if not StreamFactory.validate_exchange_support(exchange2.exchange_code):
                logger.error(f"❌ 不支持的交易所: {exchange2.exchange_code}")
                return False

            # 创建流
            stream1, stream2 = await StreamFactory.create_symbol_streams(
                exchange1, exchange2, symbol, callback1, callback2
            )

            if not stream1 or not stream2:
                logger.error(f"❌ 创建 {symbol} 订单簿流失败")
                # 清理已创建的流
                if stream1:
                    await StreamFactory.stop_streams(stream1)
                if stream2:
                    await StreamFactory.stop_streams(stream2)
                return False

            # 启动流
            await StreamFactory.start_streams(stream1, stream2)

            # 记录活跃流
            key = f"{exchange1.exchange_code}-{exchange2.exchange_code}-{symbol}"
            self.stream_pairs[key] = (stream1, stream2)

            # 记录额外信息
            self.stream_info[key] = {
                'exchange1': exchange1.exchange_code,
                'exchange2': exchange2.exchange_code,
                'symbol': symbol,
                'created_time': asyncio.get_event_loop().time(),
                'callback1_set': callback1 is not None,
                'callback2_set': callback2 is not None,
            }

            # 更新活跃流统计
            for stream, exchange_code in [(stream1, exchange1.exchange_code), (stream2, exchange2.exchange_code)]:
                if exchange_code not in self.active_streams:
                    self.active_streams[exchange_code] = []
                self.active_streams[exchange_code].append(stream)

            logger.success(f"✅ {symbol} 订单簿流对创建并启动成功")
            return True

        except Exception as e:
            logger.error(f"❌ 创建并启动 {symbol} 订单簿流失败: {e}")
            logger.exception(e)
            return False

    def _get_exchange_code_from_stream(self, stream: OrderBookStream) -> str:
        """从流对象获取交易所代码"""
        stream_class_name = stream.__class__.__name__.lower()

        # 精确匹配
        for exchange_code, stream_class in StreamFactory.STREAM_CLASSES.items():
            if stream_class.__name__.lower() == stream_class_name:
                return exchange_code

        # 模糊匹配
        if 'binance' in stream_class_name:
            return 'binance'
        elif 'lighter' in stream_class_name:
            return 'lighter'
        elif 'hyperliquid' in stream_class_name:
            return 'hyperliquid'
        elif 'bybit' in stream_class_name:
            return 'bybit'
        elif 'okx' in stream_class_name:
            return 'okx'
        elif 'aster' in stream_class_name:
            return 'aster'
        else:
            return 'unknown'

    async def stop_and_cleanup_streams(self,
                                      exchange1: AsyncExchangeAdapter,
                                      exchange2: AsyncExchangeAdapter,
                                      symbol: str):
        """
        停止并清理交易对的订单簿流

        Args:
            exchange1: 交易所1
            exchange2: 交易所2
            symbol: 交易对符号
        """
        try:
            key = f"{exchange1.exchange_code}-{exchange2.exchange_code}-{symbol}"

            if key not in self.stream_pairs:
                logger.warning(f"⚠️ 未找到 {symbol} 的活跃订单簿流")
                return

            stream1, stream2 = self.stream_pairs[key]

            logger.info(f"🛑 停止 {symbol} 订单簿流对")

            # 停止流
            await StreamFactory.stop_streams(stream1, stream2)

            # 清理记录
            del self.stream_pairs[key]
            if key in self.stream_info:
                del self.stream_info[key]

            # 更新活跃流统计
            for stream in [stream1, stream2]:
                exchange_code = self._get_exchange_code_from_stream(stream)
                if exchange_code in self.active_streams:
                    if stream in self.active_streams[exchange_code]:
                        self.active_streams[exchange_code].remove(stream)

                    if not self.active_streams[exchange_code]:
                        del self.active_streams[exchange_code]

            logger.info(f"🧹 {symbol} 订单簿流对已清理")

        except Exception as e:
            logger.error(f"❌ 清理 {symbol} 订单簿流失败: {e}")
            logger.exception(e)

    async def stop_all_streams(self):
        """停止所有活跃的订单簿流"""
        logger.info("🛑 停止所有订单簿流...")

        stop_count = 0
        # 停止所有流对
        for key, (stream1, stream2) in list(self.stream_pairs.items()):
            try:
                await StreamFactory.stop_streams(stream1, stream2)
                stop_count += 1
            except Exception as e:
                logger.error(f"❌ 停止流对 {key} 失败: {e}")

        # 清理记录
        self.stream_pairs.clear()
        self.stream_info.clear()
        self.active_streams.clear()

        logger.success(f"✅ 所有订单簿流已停止，共停止 {stop_count} 个流对")

    def get_active_stream_count(self, exchange_code: str = None) -> int:
        """获取活跃流数量"""
        if exchange_code:
            return len(self.active_streams.get(exchange_code.lower(), []))
        else:
            return sum(len(streams) for streams in self.active_streams.values())

    def get_stream_pair_count(self) -> int:
        """获取活跃流对数量"""
        return len(self.stream_pairs)

    def get_status_report(self) -> str:
        """获取详细状态报告"""
        report = f"📊 订单簿流管理器状态报告\n\n"

        # 总体统计
        total_pairs = len(self.stream_pairs)
        total_streams = sum(len(streams) for streams in self.active_streams.values())

        report += f"🔢 总体统计:\n"
        report += f"  • 活跃流对数: {total_pairs}\n"
        report += f"  • 活跃流总数: {total_streams}\n"
        report += f"  • 支持的交易所: {', '.join(StreamFactory.get_supported_exchanges())}\n\n"

        # 按交易所统计
        if self.active_streams:
            report += f"🏢 各交易所流数量:\n"
            for exchange_code, streams in self.active_streams.items():
                report += f"  • {exchange_code.upper()}: {len(streams)}个\n"
            report += "\n"

        # 活跃流对详情
        if self.stream_pairs:
            report += f"🔗 活跃流对详情:\n"
            for key, info in self.stream_info.items():
                runtime = asyncio.get_event_loop().time() - info['created_time']
                runtime_str = f"{runtime // 60:.0f}m {runtime % 60:.0f}s"
                report += f"  • {key} (运行 {runtime_str})\n"
                report += f"    - 回调: 交易所1={'✅' if info['callback1_set'] else '❌'}, 交易所2={'✅' if info['callback2_set'] else '❌'}\n"
            report += "\n"

        # 系统状态
        current_time = asyncio.get_event_loop().time()
        report += f"⏰ 系统状态:\n"
        report += f"  • 当前时间: {current_time:.0f}\n"
        report += f"  • 流管理器状态: {'🟢 正常' if total_pairs > 0 else '🟡 空闲'}\n"

        return report

    def get_stream_info(self, exchange1_code: str, exchange2_code: str, symbol: str) -> Optional[Dict[str, Any]]:
        """获取指定流对的详细信息"""
        key = f"{exchange1_code}-{exchange2_code}-{symbol}"
        return self.stream_info.get(key)

    def is_stream_active(self, exchange1_code: str, exchange2_code: str, symbol: str) -> bool:
        """检查指定流对是否活跃"""
        key = f"{exchange1_code}-{exchange2_code}-{symbol}"
        return key in self.stream_pairs

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        health_status = {
            'overall_status': 'healthy',
            'total_stream_pairs': len(self.stream_pairs),
            'total_streams': sum(len(streams) for streams in self.active_streams.values()),
            'issues': [],
            'warnings': []
        }

        # 检查流是否正常运行
        unhealthy_count = 0
        for key, (stream1, stream2) in self.stream_pairs.items():
            try:
                # 检查流是否在运行
                if not stream1.is_running or not stream2.is_running:
                    health_status['issues'].append(f"流对 {key} 未正常运行")
                    unhealthy_count += 1

                # 检查最后更新时间
                if hasattr(stream1, 'last_update_time'):
                    time_since_update = asyncio.get_event_loop().time() - stream1.last_update_time
                    if time_since_update > 60:  # 超过1分钟没有更新
                        health_status['warnings'].append(f"流对 {key} 超过1分钟没有数据更新")

            except Exception as e:
                health_status['issues'].append(f"流对 {key} 健康检查异常: {e}")
                unhealthy_count += 1

        # 更新整体状态
        if unhealthy_count > 0:
            health_status['overall_status'] = 'degraded' if unhealthy_count < len(self.stream_pairs) else 'unhealthy'

        return health_status