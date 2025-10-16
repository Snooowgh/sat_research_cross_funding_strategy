# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : test_position_streams
@Description : 仓位WebSocket流测试启动文件
@Time        : 2025/10/15
"""
import asyncio
import os
import signal
import sys
from loguru import logger
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

from cex_tools.exchange_ws.position_stream_factory import PositionStreamFactory, PositionStreamManager
from cex_tools.exchange_model.position_event_model import PositionEvent, PositionEventType


class PositionStreamTester:
    """仓位流测试器"""

    def __init__(self):
        self.manager = PositionStreamManager()
        self.running = False

    def on_position_update(self, event: PositionEvent):
        """
        仓位事件回调函数

        Args:
            event: 仓位事件
        """
        # 基础事件信息
        logger.info(f"📊 仓位事件 [{event.exchange_code}] {event.symbol}")
        logger.info(f"   事件类型: {event.event_type.value}")
        logger.info(f"   当前仓位: {event.position_size}")
        logger.info(f"   仓位方向: {event.position_side}")
        logger.info(f"   入场价格: {event.entry_price}")
        logger.info(f"   标记价格: {event.mark_price}")
        logger.info(f"   未实现盈亏: {event.unrealized_pnl:.4f}")
        logger.info(f"   名义价值: {event.notional_value:.2f}")

        if event.size_change != 0:
            logger.info(f"   仓位变化: {event.size_change:+.4f}")
        if event.pnl_change != 0:
            logger.info(f"   盈亏变化: {event.pnl_change:+.4f}")

        logger.info("-" * 60)

        # 特殊事件处理
        if event.event_type == PositionEventType.OPEN:
            logger.success(f"🔓 开仓: {event.get_position_summary()}")
        elif event.event_type == PositionEventType.CLOSE:
            logger.warning(f"🔒 平仓: {event.get_position_summary()}")
        elif event.event_type == PositionEventType.INCREASE:
            logger.info(f"📈 加仓: {event.get_position_summary()}")
        elif event.event_type == PositionEventType.DECREASE:
            logger.info(f"📉 减仓: {event.get_position_summary()}")

    async def start_test(self, exchanges: list):
        """
        启动测试

        Args:
            exchanges: 要测试的交易所列表
        """
        try:
            logger.info("🚀 启动仓位WebSocket流测试...")
            logger.info(f"📋 测试交易所: {', '.join(exchanges)}")

            # 设置日志级别
            logger.remove()
            logger.add(
                sys.stdout,
                format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
                level="INFO"
            )

            # 启动流管理器
            self.running = True
            success = await self.manager.start_streams(exchanges, self.on_position_update)

            if not success:
                logger.error("❌ 无法启动任何仓位流，请检查API配置")
                return

            logger.success("✅ 仓位WebSocket流启动成功")

            # 启动状态监控任务
            # monitor_task = asyncio.create_task(self._monitor_status())

            # 等待中断信号
            await self._wait_for_shutdown()

            # 停止监控任务
            # monitor_task.cancel()
            # try:
            #     await monitor_task
            # except asyncio.CancelledError:
            #     pass

        except KeyboardInterrupt:
            logger.info("🛑 收到中断信号，正在停止...")
        except Exception as e:
            logger.error(f"❌ 测试异常: {e}")
            logger.exception(e)
        finally:
            await self.stop_test()

    async def _monitor_status(self):
        """状态监控任务"""
        while self.running:
            try:
                await asyncio.sleep(30)  # 每30秒报告一次状态

                if self.running:
                    report = self.manager.get_status_report()
                    logger.info("📈 状态报告:")
                    for line in report.split('\n'):
                        logger.info(f"   {line}")

                    # 统计总开仓数和总盈亏
                    open_positions = 0
                    total_pnl = 0.0

                    for stream in self.manager.streams.values():
                        positions = stream.get_open_positions()
                        open_positions += len(positions)
                        for pos in positions.values():
                            total_pnl += getattr(pos, 'unRealizedProfit', 0)

                    logger.info(f"📊 总开仓数: {open_positions}, 总盈亏: {total_pnl:.4f}")
                    logger.info("-" * 60)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ 状态监控异常: {e}")

    async def _wait_for_shutdown(self):
        """等待关闭信号"""
        # 创建一个永不完成的future，等待外部中断
        try:
            while self.running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    async def stop_test(self):
        """停止测试"""
        logger.info("🛑 正在停止仓位WebSocket流...")
        self.running = False

        try:
            await self.manager.stop_all_streams()
            logger.success("✅ 所有仓位流已停止")
        except Exception as e:
            logger.error(f"❌ 停止流时出现异常: {e}")


async def main():
    """主函数"""
    # 默认测试的交易所（根据你的环境变量配置调整）
    test_exchanges = [
        'binance',
        # 'hyperliquid',
        # 'okx',
        # 'bybit',
        # 'lighter'
    ]

    tester = PositionStreamTester()

    logger.info(f"🎯 开始测试交易所: {', '.join(test_exchanges)}")
    logger.info("💡 按 Ctrl+C 停止测试")
    logger.info("=" * 60)

    # 启动测试
    await tester.start_test(test_exchanges)


if __name__ == "__main__":
    # 设置事件循环策略（针对macOS）
    if sys.platform == 'darwin':
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

    # 运行主函数
    asyncio.run(main())