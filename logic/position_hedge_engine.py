# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : position_hedge_engine
@Description : 仓位对冲引擎，监听两个交易所的订单更新，自动执行对冲交易
@Time        : 2025/10/16
"""
import time
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
    min_hedge_value_usd: float = 50


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

        # 累计订单数据结构
        self.pending_hedges: Dict[str, Dict] = {}  # 待对冲的订单累计 {symbol_side: {amount, total_value, orders}}

        # 统计信息
        self.stats = {
            'total_hedges': 0,
            'successful_hedges': 0,
            'failed_hedges': 0,
            'total_hedge_volume': 0.0,
            'total_price_difference': 0.0,
            'total_slippage': 0.0,
            'total_delay_ms': 0.0,
            'avg_price_difference': 0.0,
            'avg_slippage': 0.0,
            'avg_delay_ms': 0.0,
            'profitable_hedges': 0,
            'loss_hedges': 0
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

    def _get_pending_hedge_key(self, target_exchange: str, symbol: str, hedge_side: str) -> str:
        """生成待对冲订单的唯一键"""
        return f"{target_exchange}_{symbol}_{hedge_side}"

    def _calculate_order_value_usd(self, quantity: float, price: float) -> float:
        """计算订单美元价值"""
        return quantity * price

    async def _add_to_pending_hedges(self, target_exchange: AsyncExchangeAdapter, symbol: str,
                                   hedge_side: str, quantity: float, price: float, event: OrderUpdateEvent):
        """
        添加订单到待对冲累计列表

        Args:
            target_exchange: 目标交易所
            symbol: 交易对
            hedge_side: 对冲方向
            quantity: 数量
            price: 价格
            event: 原始订单事件
        """
        pending_key = self._get_pending_hedge_key(target_exchange.exchange_code, symbol, hedge_side)
        order_value = self._calculate_order_value_usd(quantity, price)

        if pending_key not in self.pending_hedges:
            self.pending_hedges[pending_key] = {
                'target_exchange': target_exchange,
                'symbol': symbol,
                'hedge_side': hedge_side,
                'total_amount': 0.0,
                'total_value': 0.0,
                'avg_price': 0.0,
                'orders': []
            }

        # 添加到累计
        pending = self.pending_hedges[pending_key]
        pending['total_amount'] += quantity
        pending['total_value'] += order_value
        pending['avg_price'] = pending['total_value'] / pending['total_amount']
        pending['orders'].append({
            'order_key': self._get_order_key(event),
            'quantity': quantity,
            'price': price,
            'value': order_value,
            'timestamp': time.time()
        })

        logger.info(f"📦 累计对冲订单: {symbol} {hedge_side}")
        logger.info(f"   数量: +{quantity:.6f} (累计: {pending['total_amount']:.6f})")
        logger.info(f"   价值: +{order_value:.2f} USD (累计: {pending['total_value']:.2f} USD)")

        # 检查是否达到最小对冲金额
        if pending['total_value'] >= self.config.min_hedge_value_usd:
            logger.info(f"✅ 达到最小对冲金额，执行批量对冲")
            await self._execute_pending_hedge(pending_key)
        else:
            logger.info(f"⏳ 金额不足，继续累计 (还需: {self.config.min_hedge_value_usd - pending['total_value']:.2f} USD)")

    async def _execute_pending_hedge(self, pending_key: str):
        """
        执行累计的对冲订单

        Args:
            pending_key: 待对冲订单的键
        """
        if pending_key not in self.pending_hedges:
            logger.error(f"❌ 待对冲订单不存在: {pending_key}")
            return

        pending = self.pending_hedges[pending_key]

        try:
            # 构造一个复合的事件用于记录
            composite_event = type('CompositeEvent', (), {
                'exchange_code': pending['target_exchange'].exchange_code,
                'symbol': pending['symbol'],
                'side': pending['hedge_side'],
                'order_last_filled_quantity': pending['total_amount'],
                'last_filled_price': pending['avg_price']
            })()

            logger.info(f"🚀 执行批量对冲:")
            logger.info(f"   交易所: {pending['target_exchange'].exchange_code}")
            logger.info(f"   交易对: {pending['symbol']}")
            logger.info(f"   方向: {pending['hedge_side']}")
            logger.info(f"   总数量: {pending['total_amount']:.6f}")
            logger.info(f"   总价值: {pending['total_value']:.2f} USD")
            logger.info(f"   平均价格: {pending['avg_price']:.6f}")
            logger.info(f"   订单数: {len(pending['orders'])}")

            # 执行对冲订单
            await self._execute_hedge_order(
                pending['target_exchange'],
                pending['symbol'],
                pending['hedge_side'],
                pending['total_amount'],
                pending['avg_price'],
                composite_event
            )

            # 清理已执行的待对冲订单
            del self.pending_hedges[pending_key]

        except Exception as e:
            logger.error(f"❌ 批量对冲执行失败: {e}")
            logger.exception(e)

            # 发送错误通知
            await async_notify_telegram(
                f"⚠️ 批量对冲执行失败\n"
                f"交易所: {pending['target_exchange'].exchange_code}\n"
                f"交易对: {pending['symbol']}\n"
                f"方向: {pending['hedge_side']}\n"
                f"总数量: {pending['total_amount']:.6f}\n"
                f"总价值: {pending['total_value']:.2f} USD\n"
                f"错误: {str(e)}"
            )

    async def _execute_hedge_order(self,
                                   target_exchange: AsyncExchangeAdapter,
                                   symbol: str,
                                   side: str,
                                   amount: float,
                                   last_filled_price: float,
                                   event) -> Optional[Dict]:
        """
        执行对冲订单

        Args:
            target_exchange: 目标交易所
            symbol: 交易对
            side: 订单方向
            amount: 订单数量
            last_filled_price: 原始成交价格
            event: 原始订单事件

        Returns:
            Dict: 订单结果，失败返回None
        """
        try:
            # 记录开始时间
            hedge_start_time = time.time()

            logger.info(f"🎯 执行对冲订单: {target_exchange.exchange_code} {symbol} {side} {amount}")
            amount = min(await self.exchange1.convert_size(symbol, amount),
                         await self.exchange2.convert_size(symbol, amount))
            # 下市价对冲单
            order_result = await target_exchange.make_new_order(
                symbol=symbol,
                side=side,
                order_type="MARKET",
                amount=amount,
                price=last_filled_price
            )

            # 计算订单执行延迟
            hedge_end_time = time.time()
            delay_ms = (hedge_end_time - hedge_start_time) * 1000
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

        if order_result:
            orderId = order_result["orderId"]
            try:
                order_info = await target_exchange.get_recent_order(symbol, orderId)
                self.stats['successful_hedges'] += 1
                self.stats['total_hedge_volume'] += amount

                # 从订单结果中提取实际成交价格
                hedge_price = order_info.avgPrice

                # 计算价差和收益率
                price_difference = self._calculate_price_difference(
                    last_filled_price, hedge_price, event.side, side
                )

                # 计算滑点
                slippage = self._calculate_slippage(
                    last_filled_price, hedge_price, event.side, side
                )

                # 计算收益/亏损
                profit_usd = price_difference * amount
                is_profitable = profit_usd > 0

                # 更新统计数据
                self._update_hedge_stats(
                    price_difference, slippage, delay_ms, is_profitable
                )

                logger.success(f"✅ 对冲订单成功: {target_exchange.exchange_code} {symbol} {side} {amount}")
                logger.info(f"📊 对冲执行详情:")
                logger.info(f"   数量: {amount}")
                logger.info(f"   原始价格: {last_filled_price}")
                logger.info(f"   对冲价格: {hedge_price}")
                logger.info(f"   价差: {price_difference:.6f}")
                logger.info(f"   滑点: {slippage:.6f}")
                logger.info(f"   延迟: {delay_ms:.2f}ms")
                logger.info(f"   收益: {profit_usd:.6f} USD ({'盈利' if is_profitable else '亏损'})")
            except Exception as e:
                logger.warning(f"{target_exchange.exchange_code} 计算对冲单收益率失败: {e}")

            return order_result
        else:
            self.stats['failed_hedges'] += 1
            logger.error(f"❌ 对冲订单失败: {target_exchange.exchange_code} {symbol} {side} {amount}")
            return None


    def _calculate_price_difference(self, original_price: float, hedge_price: float,
                                    original_side: str, hedge_side: str) -> float:
        """
        计算价差（原始价格 - 对冲价格）

        Args:
            original_price: 原始成交价格
            hedge_price: 对冲价格
            original_side: 原始订单方向
            hedge_side: 对冲方向

        Returns:
            float: 价差
        """
        # 对冲方向与原始方向相反，所以价差计算方式为：
        # 如果原始是买入，对冲是卖出，价差 = 对冲价格 - 原始价格
        # 如果原始是卖出，对冲是买入，价差 = 原始价格 - 对冲价格
        if original_side == "BUY" and hedge_side == "SELL":
            return hedge_price - original_price
        elif original_side == "SELL" and hedge_side == "BUY":
            return original_price - hedge_price
        else:
            # 异常情况，简单返回差值
            return hedge_price - original_price

    def _calculate_slippage(self, original_price: float, hedge_price: float,
                            original_side: str, hedge_side: str) -> float:
        """
        计算滑点（相对于原始价格的百分比）

        Args:
            original_price: 原始成交价格
            hedge_price: 对冲价格
            original_side: 原始订单方向
            hedge_side: 对冲方向

        Returns:
            float: 滑点百分比（正数表示不利滑点）
        """
        if original_price == 0:
            return 0.0

        price_diff = hedge_price - original_price
        slippage_percent = (price_diff / original_price) * 100

        # 对于对冲交易，我们关注不利滑点
        if original_side == "BUY" and hedge_side == "SELL":
            # 买入后卖出，希望价格越高越好，负数是不利滑点
            return abs(min(0, slippage_percent))
        elif original_side == "SELL" and hedge_side == "BUY":
            # 卖出后买入，希望价格越低越好，正数是不利滑点
            return max(0, slippage_percent)
        else:
            return abs(slippage_percent)

    def _update_hedge_stats(self, price_difference: float, slippage: float,
                            delay_ms: float, is_profitable: bool):
        """
        更新对冲统计数据

        Args:
            price_difference: 价差
            slippage: 滑点
            delay_ms: 延迟（毫秒）
            is_profitable: 是否盈利
        """
        successful_hedges = self.stats['successful_hedges']

        # 更新总量统计
        self.stats['total_price_difference'] += price_difference
        self.stats['total_slippage'] += slippage
        self.stats['total_delay_ms'] += delay_ms

        # 更新平均统计
        self.stats['avg_price_difference'] = self.stats['total_price_difference'] / successful_hedges
        self.stats['avg_slippage'] = self.stats['total_slippage'] / successful_hedges
        self.stats['avg_delay_ms'] = self.stats['total_delay_ms'] / successful_hedges

        # 更新盈亏统计
        if is_profitable:
            self.stats['profitable_hedges'] += 1
        else:
            self.stats['loss_hedges'] += 1

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

                # 计算当前订单的美元价值
                current_order_value = self._calculate_order_value_usd(hedge_amount, last_filled_price)

                logger.info(f"🔄 检测到对冲机会:")
                logger.info(
                    f"   源交易所: {event.exchange_code} {event.symbol} {event.side} {filled_quantity} {last_filled_price}")
                logger.info(f"   目标交易所: {target_exchange.exchange_code} {hedge_side} {hedge_amount}")
                logger.info(f"   订单价值: {current_order_value:.2f} USD")

                # 检查订单价值
                if current_order_value >= self.config.min_hedge_value_usd:
                    # 订单价值足够，直接执行对冲
                    logger.info(f"💰 订单价值达到最小要求，直接执行对冲")
                    try:
                        await self._execute_hedge_order(target_exchange, event.symbol, hedge_side, hedge_amount,
                                                        last_filled_price, event)
                    except Exception as e:
                        logger.error(f"❌ 对冲订单执行异常: {e}")
                else:
                    # 订单价值不足，添加到累计列表
                    logger.info(f"💸 订单价值不足，添加到累计列表")
                    await self._add_to_pending_hedges(target_exchange, event.symbol, hedge_side,
                                                    hedge_amount, last_filled_price, event)

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
            if event.order_status in [OrderStatusType.FILLED,
                                      OrderStatusType.NEW,
                                      OrderStatusType.CANCELED]:
                logger.info(f"📨 订单更新: {exchange_code} {event.symbol} {event.side} {event.order_type} {event.order_status} "
                            f"size: {event.order_last_filled_quantity} price: {event.last_filled_price}")
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

            # 输出累计订单状态
            if self.pending_hedges:
                logger.warning(f"⚠️ 引擎停止时仍有 {len(self.pending_hedges)} 个待处理累计订单")
                for key, pending in self.pending_hedges.items():
                    logger.warning(f"   {key}: {pending['total_amount']:.6f} ({pending['total_value']:.2f} USD)")

                # 可选：在停止时强制执行所有累计订单
                # await self._force_execute_all_pending_hedges()

            logger.success("✅ 仓位对冲引擎已停止")

        except Exception as e:
            logger.error(f"❌ 停止对冲引擎失败: {e}")
            logger.exception(e)

    async def _force_execute_all_pending_hedges(self):
        """强制执行所有累计的对冲订单"""
        if not self.pending_hedges:
            return

        logger.info(f"🚀 强制执行 {len(self.pending_hedges)} 个累计订单")
        pending_keys = list(self.pending_hedges.keys())

        for pending_key in pending_keys:
            await self._execute_pending_hedge(pending_key)

    def get_pending_hedges_status(self) -> Dict:
        """获取累计订单状态"""
        status = {}
        for key, pending in self.pending_hedges.items():
            status[key] = {
                'symbol': pending['symbol'],
                'exchange': pending['target_exchange'].exchange_code,
                'side': pending['hedge_side'],
                'total_amount': pending['total_amount'],
                'total_value': pending['total_value'],
                'avg_price': pending['avg_price'],
                'order_count': len(pending['orders']),
                'min_value_needed': max(0, self.config.min_hedge_value_usd - pending['total_value']),
                'ready_to_execute': pending['total_value'] >= self.config.min_hedge_value_usd
            }
        return status

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
