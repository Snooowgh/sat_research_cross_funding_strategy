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

from cex_tools.exchange_model.order_update_event_model import OrderUpdateEvent

# 加载环境变量
load_dotenv()

from cex_tools.exchange_ws.position_stream_factory import PositionStreamFactory, PositionStreamManager
from cex_tools.exchange_model.position_event_model import PositionEvent, PositionEventType


class PositionStreamTester:
    """仓位流测试器"""

    def __init__(self):
        self.manager = PositionStreamManager()
        self.running = False

    def on_order_update(self, event: OrderUpdateEvent):
        """
       订单更新事件回调函数

        Args:
            event:订单更新事件
        """
        # 基础事件信息
        logger.info(f"📊 订单更新事件 [{event.exchange_code}] {event.symbol}")
        logger.info(f"   订单ID: {event.order_id}")
        logger.info(f"   订单状态: {event.order_status}")
        logger.info(f"   订单类型: {event.order_type}")
        logger.info(f"   订单方向: {event.side}")
        logger.info(f"   订单数量: {event.original_quantity}")
        logger.info(f"   订单价格: {event.price}")
        logger.info(f"   {str(event)}")


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
            success = await self.manager.start_streams(exchanges, self.on_order_update)

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
        'okx',
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