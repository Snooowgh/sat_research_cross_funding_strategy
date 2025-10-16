# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : position_hedge_engine
@Description : 仓位对冲引擎，监听两个交易所的订单更新，自动执行对冲交易
@Time        : 2025/10/16
"""
from typing import Dict, Optional, Callable, Set
from loguru import logger
from dataclasses import dataclass

from cex_tools.exchange_model.order_update_event_model import OrderUpdateEvent, OrderStatusType, OrderType
from cex_tools.async_exchange_adapter import AsyncExchangeAdapter
from utils.notify_tools import async_notify_telegram


@dataclass
class HedgeConfig:
    """对冲配置"""
    # 最小对冲金额
    min_hedge_value_usd: float = 100


class PositionHedgeEngine:
    """
    仓位对冲引擎

    监听两个交易所的订单更新，当检测到limit order成交时，
    在另一个交易所下相反方向的市价对冲单，保持两边仓位平衡
    """

    def __init__(self,
                 exchange1: AsyncExchangeAdapter,
                 exchange2: AsyncExchangeAdapter,
                 stream1: any,  # position stream for exchange1
                 stream2: any,  # position stream for exchange2
                 config: HedgeConfig):
        """
        初始化对冲引擎

        Args:
            exchange1: 交易所1异步对象
            exchange2: 交易所2异步对象
            stream1: 交易所1的position stream
            stream2: 交易所2的position stream
            config: 对冲配置
        """
        self.exchange1 = exchange1
        self.exchange2 = exchange2
        self.exchange1_code = exchange1.exchange_code
        self.exchange2_code = exchange2.exchange_code
        self.stream1 = stream1
        self.stream2 = stream2
        self.config = config

        # 对冲状态跟踪
        self.is_running = False
        self.processing_orders: Set[str] = set()  # 正在处理的订单ID，防止重复处理

        # 统计信息
        self.stats = {
            'total_hedges': 0,
            'successful_hedges': 0,
            'failed_hedges': 0,
            'total_hedge_volume': 0.0
        }

        logger.info(f"🚀 初始化仓位对冲引擎: {self.exchange1_code} <-> {self.exchange2_code}")
        logger.info(f"📋 配置信息: {config}")

    def _get_order_key(self, event: OrderUpdateEvent) -> str:
        """生成订单唯一标识"""
        return f"{event.exchange_code}_{event.order_id}_{event.trade_id}"

    def _is_limit_order_filled(self, event: OrderUpdateEvent) -> bool:
        """
        检查是否为限价单成交

        Args:
            event: 订单更新事件

        Returns:
            bool: 是否为限价单成交
        """
        # 检查订单类型和状态
        if event.order_type != OrderType.LIMIT:
            return False

        # 检查是否有成交
        if event.order_status == OrderStatusType.FILLED or event.order_status == OrderStatusType.PARTIALLY_FILLED:
            # 检查最近一次成交数量大于0
            if event.order_last_filled_quantity > 0:
                return True
        return False

    def _get_hedge_side(self, original_side: str) -> str:
        """
        获取对冲方向

        Args:
            original_side: 原始订单方向

        Returns:
            str: 对冲方向
        """
        # 对冲方向与原始方向相反
        return "SELL" if original_side == "BUY" else "BUY"

    async def _execute_hedge_order(self,
                                   target_exchange: AsyncExchangeAdapter,
                                   symbol: str,
                                   side: str,
                                   amount: float,
                                   last_filled_price: float) -> Optional[Dict]:
        """
        执行对冲订单

        Args:
            target_exchange: 目标交易所
            symbol: 交易对
            side: 订单方向
            amount: 订单数量

        Returns:
            Dict: 订单结果，失败返回None
        """
        try:
            logger.info(f"🎯 执行对冲订单: {target_exchange.exchange_code} {symbol} {side} {amount}")

            # 下市价对冲单
            order_result = await target_exchange.make_new_order(
                symbol=symbol,
                side=side,
                order_type="MARKET",
                amount=amount,
                price=last_filled_price
            )

            if order_result:
                self.stats['successful_hedges'] += 1
                self.stats['total_hedge_volume'] += amount

                logger.success(f"✅ 对冲订单成功: {target_exchange.exchange_code} {symbol} {side} {amount}")
                logger.debug(f"📄 订单结果: {order_result}")

                return order_result
            else:
                self.stats['failed_hedges'] += 1
                logger.error(f"❌ 对冲订单失败: {target_exchange.exchange_code} {symbol} {side} {amount}")
                return None

        except Exception as e:
            self.stats['failed_hedges'] += 1
            logger.error(f"❌ 执行对冲订单异常: {target_exchange.exchange_code} {symbol} {side} {amount} - {e}")
            logger.exception(e)

            # 发送错误通知
            await async_notify_telegram(
                f"⚠️ 对冲订单执行失败\n"
                f"交易所: {target_exchange.exchange_code}\n"
                f"交易对: {symbol}\n"
                f"方向: {side}\n"
                f"数量: {amount}\n"
                f"错误: {str(e)}"
            )
            return None

    async def _handle_order_update(self, event: OrderUpdateEvent):
        """
        处理订单更新事件

        Args:
            event: 订单更新事件
        """
        try:
            # 检查是否为限价单成交
            if not self._is_limit_order_filled(event):
                return

            # 生成订单唯一标识
            order_key = self._get_order_key(event)

            # 防止重复处理
            if order_key in self.processing_orders:
                logger.debug(f"⏭️ 跳过重复处理的订单: {order_key}")
                return

            self.processing_orders.add(order_key)

            try:
                # 获取成交数量
                filled_quantity = float(event.order_last_filled_quantity)
                last_filled_price = float(event.last_filled_price)

                # 获取对冲交易对
                if event.exchange_code == self.exchange1_code:
                    target_exchange = self.exchange2
                else:
                    target_exchange = self.exchange1

                # 计算对冲数量
                hedge_amount = filled_quantity

                # 获取对冲方向
                hedge_side = self._get_hedge_side(event.side)

                logger.info(f"🔄 检测到对冲机会:")
                logger.info(
                    f"   源交易所: {event.exchange_code} {event.symbol} {event.side} {filled_quantity} {last_filled_price}")
                logger.info(f"   目标交易所: {target_exchange.exchange_code} {hedge_side} {hedge_amount}")

                # 执行对冲订单
                await self._execute_hedge_order(target_exchange, event.symbol, hedge_side, hedge_amount,
                                                last_filled_price)

                self.stats['total_hedges'] += 1

            finally:
                # 清理处理状态
                self.processing_orders.discard(order_key)

        except Exception as e:
            logger.error(f"❌ 处理订单更新异常: {e}")
            logger.exception(e)

    def _create_order_update_callback(self, exchange_code: str) -> Callable:
        """
        创建订单更新回调函数

        Args:
            exchange_code: 交易所代码

        Returns:
            Callable: 回调函数
        """

        async def on_order_update(event: OrderUpdateEvent):
            logger.info(f"📨 订单更新: {exchange_code} {event.symbol} {event.order_type} {event.order_status} "
                        f"fill size: {event.order_last_filled_quantity} fill price: {event.last_filled_price}")
            await self._handle_order_update(event)

        return on_order_update

    async def start(self):
        """启动对冲引擎"""
        try:
            if self.is_running:
                logger.warning("⚠️ 对冲引擎已经在运行")
                return

            logger.info("🚀 启动仓位对冲引擎...")

            # 创建订单更新回调函数
            callback1 = self._create_order_update_callback(self.exchange1_code)
            callback2 = self._create_order_update_callback(self.exchange2_code)

            self.stream1.set_order_update_callback(callback1)
            self.stream2.set_order_update_callback(callback2)

            # 启动streams（如果还没有启动）
            if not self.stream1.is_running:
                await self.stream1.start()

            if not self.stream2.is_running:
                await self.stream2.start()

            self.is_running = True
            logger.success(f"✅ 仓位对冲引擎启动成功: {self.exchange1_code} <-> {self.exchange2_code}")
        except Exception as e:
            logger.error(f"❌ 启动对冲引擎失败: {e}")
            logger.exception(e)
            raise

    async def stop(self):
        """停止对冲引擎"""
        try:
            if not self.is_running:
                logger.warning("⚠️ 对冲引擎已经停止")
                return

            logger.info("🛑 停止仓位对冲引擎...")

            self.is_running = False
            self.processing_orders.clear()

            logger.success("✅ 仓位对冲引擎已停止")

        except Exception as e:
            logger.error(f"❌ 停止对冲引擎失败: {e}")
            logger.exception(e)

    def get_stats(self) -> Dict:
        """获取对冲统计信息"""
        stats = self.stats.copy()
        stats.update({
            'success_rate': (stats['successful_hedges'] / max(1, stats['total_hedges'])) * 100,
            'is_running': self.is_running,
            'processing_orders_count': len(self.processing_orders),
            'exchange_pair': f"{self.exchange1_code} <-> {self.exchange2_code}"
        })
        return stats

    def update_config(self, **kwargs):
        """更新配置"""
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
                logger.info(f"📝 更新配置: {key} = {value}")
            else:
                logger.warning(f"⚠️ 无效的配置项: {key}")

    def __str__(self):
        return f"PositionHedgeEngine({self.exchange1_code} <-> {self.exchange2_code})"

    def __repr__(self):
        return self.__str__()


# ========== 工厂函数 ==========

def create_hedge_engine(exchange1: AsyncExchangeAdapter,
                        exchange2: AsyncExchangeAdapter,
                        stream1: any,
                        stream2: any) -> PositionHedgeEngine:
    """
    创建仓位对冲引擎的便捷函数

    Args:
        exchange1: 交易所1异步对象
        exchange2: 交易所2异步对象
        stream1: 交易所1的position stream
        stream2: 交易所2的position stream
        symbol_mapping: 交易对映射字典
        **config_kwargs: 其他配置参数

    Returns:
        PositionHedgeEngine: 对冲引擎实例
    """
    config = HedgeConfig()

    return PositionHedgeEngine(
        exchange1=exchange1,
        exchange2=exchange2,
        stream1=stream1,
        stream2=stream2,
        config=config
    )
