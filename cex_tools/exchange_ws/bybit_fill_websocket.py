# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : bybit_fill_websocket
@Description : Bybit成交WebSocket实现
@Time        : 2025/10/15
"""
import asyncio
import json
import time
import hmac
import hashlib
import base64
import websockets
from typing import Optional, Callable
from loguru import logger

from cex_tools.exchange_ws.fill_websocket_stream import FillWebSocketStream, FillWebSocketError, FillWebSocketConnectionError
from cex_tools.exchange_model.fill_event_model import FillEvent


class BybitFillWebSocket(FillWebSocketStream):
    """Bybit成交WebSocket实现"""

    def __init__(self, api_key: str, secret: str,
                 on_fill_callback: Callable[[FillEvent], None],
                 is_testnet: bool = False):
        """
        初始化Bybit成交WebSocket

        Args:
            api_key: API密钥
            secret: API密钥
            on_fill_callback: 成交事件回调函数
            is_testnet: 是否使用测试网络
        """
        super().__init__("bybit", on_fill_callback)
        self.api_key = api_key
        self.secret = secret
        self.is_testnet = is_testnet

        # Bybit V5 WebSocket端点
        if is_testnet:
            self.ws_url = "wss://stream-testnet.bybit.com/v5/private"
        else:
            self.ws_url = "wss://stream.bybit.com/v5/private"

        # 订阅管理
        self._subscribed = False
        self._auth_response = ""

    def _generate_signature(self, expires: int) -> str:
        """生成签名"""
        message = f"GET/realtime{expires}"
        signature = hmac.new(
            bytes(self.secret, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod=hashlib.sha256
        ).hexdigest()
        return signature

    async def connect(self):
        """连接WebSocket"""
        try:
            logger.debug(f"🔌 连接Bybit WebSocket: {self.ws_url}")

            self._ws = await asyncio.wait_for(
                websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10
                ),
                timeout=10.0
            )

            # 重置重连计数
            if self._reconnect_count > 0:
                logger.info(f"✅ Bybit WebSocket重连成功")

        except Exception as e:
            raise FillWebSocketConnectionError("bybit", f"连接失败: {str(e)}", e)

    async def subscribe_user_events(self):
        """订阅用户事件"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            # 认证并订阅私有频道
            await self.authenticate_and_subscribe()

        except Exception as e:
            raise FillWebSocketError("bybit", f"订阅用户事件失败: {str(e)}", e)

    async def authenticate_and_subscribe(self):
        """认证并订阅私有频道"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            # 生成认证参数
            expires = int((time.time() + 1) * 1000)
            signature = self._generate_signature(expires)

            # 认证消息
            auth_msg = {
                "op": "auth",
                "req_id": f"auth_{int(time.time() * 1000)}",
                "args": [self.api_key, expires, signature]
            }

            await self._ws.send(json.dumps(auth_msg))
            logger.debug("✅ Bybit认证请求已发送")

            # 等待认证确认
            await asyncio.wait_for(self._wait_for_auth_confirmation(), timeout=10.0)

            # 认证成功后订阅频道
            await self._subscribe_private_channels()

        except asyncio.TimeoutError:
            raise FillWebSocketError("bybit", "认证或订阅超时")
        except Exception as e:
            raise FillWebSocketError("bybit", f"认证或订阅失败: {str(e)}", e)

    async def _wait_for_auth_confirmation(self):
        """等待认证确认"""
        try:
            while not self._subscribed and self._running:
                try:
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)
                    data = json.loads(message)

                    if data.get('op') == 'auth' and data.get('success') is True:
                        self._auth_response = data.get('ret_msg', '认证成功')
                        logger.debug(f"✅ Bybit认证成功: {self._auth_response}")
                        break
                    elif data.get('op') == 'auth' and data.get('success') is False:
                        error_msg = data.get('ret_msg', '未知认证错误')
                        raise FillWebSocketError("bybit", f"认证失败: {error_msg}")

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            logger.error(f"❌ 等待Bybit认证确认异常: {e}")
            raise

    async def _subscribe_private_channels(self):
        """订阅私有频道"""
        try:
            # 订阅订单频道
            orders_msg = {
                "op": "subscribe",
                "req_id": f"orders_{int(time.time() * 1000)}",
                "args": ["order"]
            }
            await self._ws.send(json.dumps(orders_msg))

            # 订阅成交频道
            execution_msg = {
                "op": "subscribe",
                "req_id": f"execution_{int(time.time() * 1000)}",
                "args": ["execution"]
            }

            # 订阅持仓频道
            position_msg = {
                "op": "subscribe",
                "req_id": f"position_{int(time.time() * 1000)}",
                "args": ["position"]
            }

            await self._ws.send(json.dumps(execution_msg))
            await self._ws.send(json.dumps(position_msg))

            logger.debug("✅ Bybit私有频道订阅请求已发送")

            # 等待订阅确认
            await asyncio.wait_for(self._wait_for_subscription_confirmation(), timeout=10.0)

        except Exception as e:
            logger.error(f"❌ Bybit订阅私有频道失败: {e}")
            raise

    async def _wait_for_subscription_confirmation(self):
        """等待订阅确认"""
        try:
            subscription_count = 0
            required_subscriptions = 3  # order, execution, position

            while subscription_count < required_subscriptions and self._running:
                try:
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)
                    data = json.loads(message)

                    if data.get('op') == 'subscribe' and data.get('success') is True:
                        subscription_count += 1
                        arg = data.get('req_id', '').split('_')[0]
                        logger.debug(f"✅ Bybit {arg} 频道订阅成功")

                    elif data.get('op') == 'subscribe' and data.get('success') is False:
                        error_msg = data.get('ret_msg', '未知订阅错误')
                        raise FillWebSocketError("bybit", f"订阅失败: {error_msg}")

                except asyncio.TimeoutError:
                    continue

            if subscription_count >= required_subscriptions:
                self._subscribed = True
                logger.debug("✅ Bybit所有私有频道订阅成功")

        except Exception as e:
            logger.error(f"❌ 等待Bybit订阅确认异常: {e}")
            raise

    def _parse_message(self, message: dict) -> Optional[FillEvent]:
        """解析Bybit WebSocket消息为成交事件"""
        try:
            data = message.get('data', {})
            topic = message.get('topic', '')

            if topic == 'execution':
                return self._parse_execution_data(data)
            elif topic == 'order':
                return self._parse_order_data(data)
            elif topic == 'position':
                # 持仓变化，不直接处理为成交事件
                logger.debug(f"📝 Bybit持仓更新: {topic}")
                return None
            elif message.get('op') == 'error':
                error_msg = message.get('ret_msg', '未知错误')
                logger.error(f"❌ Bybit WebSocket错误: {error_msg}")
                return None
            else:
                # 其他频道
                logger.debug(f"📝 Bybit收到其他频道: {topic}")
                return None

        except Exception as e:
            logger.error(f"❌ 解析Bybit消息失败: {e}")
            logger.debug(f"原始消息: {message}")
            return None

    def _parse_execution_data(self, execution_data: list) -> Optional[FillEvent]:
        """解析执行数据"""
        try:
            if not execution_data:
                return None

            # 处理每个执行记录
            for execution in execution_data if isinstance(execution_data, list) else [execution_data]:
                if execution.get('execType') == 'Trade':
                    return self._convert_execution_to_fill(execution)

            return None

        except Exception as e:
            logger.error(f"❌ 解析Bybit执行数据失败: {e}")
            return None

    def _convert_execution_to_fill(self, execution: dict) -> Optional[FillEvent]:
        """将执行记录转换为成交事件"""
        try:
            symbol = execution.get('symbol', '')
            order_id = execution.get('orderId', '')
            order_link_id = execution.get('orderLinkId', '')
            exec_price = float(execution.get('execPrice', 0))
            exec_qty = float(execution.get('execQty', 0))
            exec_side = execution.get('execSide', '')
            exec_value = float(execution.get('execValue', 0))
            exec_fee = float(execution.get('execFee', 0))
            fee_currency = execution.get('feeRate', '')
            exec_time = execution.get('execTime', '')

            # 验证必要字段
            if not all([symbol, order_id, exec_price > 0, exec_qty > 0, exec_time]):
                logger.warning(f"⚠️ Bybit执行记录缺少必要字段: {execution}")
                return None

            # 转换时间戳（微秒转秒）
            timestamp = int(exec_time) / 1000000

            # 计算仓位变化
            position_change = exec_qty if exec_side == 'Buy' else -exec_qty

            fill_event = FillEvent(
                exchange_code='bybit',
                symbol=symbol,
                order_id=order_link_id or order_id,
                side=exec_side.upper(),
                filled_quantity=exec_qty,
                filled_price=exec_price,
                trade_id=execution.get('execId', ''),
                timestamp=timestamp,
                commission=abs(exec_fee),  # Bybit费用可能是负数（返佣）
                commission_asset=fee_currency
            )

            logger.debug(f"📈 Bybit执行记录解析成功: {fill_event}")
            return fill_event

        except Exception as e:
            logger.error(f"❌ 转换Bybit执行记录为成交事件失败: {e}")
            return None

    def _parse_order_data(self, order_data: list) -> Optional[FillEvent]:
        """解析订单数据（作为成交事件的补充）"""
        try:
            if not order_data:
                return None

            # 处理每个订单
            for order in order_data if isinstance(order_data, list) else [order_data]:
                order_status = order.get('orderStatus', '')
                if order_status in ['PartiallyFilled', 'Filled']:
                    cum_exec_qty = float(order.get('cumExecQty', 0))
                    if cum_exec_qty > 0:
                        return self._convert_order_to_fill(order)

            return None

        except Exception as e:
            logger.error(f"❌ 解析Bybit订单数据失败: {e}")
            return None

    def _convert_order_to_fill(self, order: dict) -> Optional[FillEvent]:
        """将订单信息转换为成交事件"""
        try:
            symbol = order.get('symbol', '')
            order_id = order.get('orderId', '')
            order_link_id = order.get('orderLinkId', '')
            avg_price = float(order.get('avgPrice', 0))
            cum_exec_qty = float(order.get('cumExecQty', 0))
            side = order.get('side', '')
            cum_exec_value = float(order.get('cumExecValue', 0))
            cum_exec_fee = float(order.get('cumExecFee', 0))
            created_time = order.get('createdTime', '')

            if not all([symbol, order_id, avg_price > 0, cum_exec_qty > 0, created_time]):
                return None

            timestamp = int(created_time) / 1000000

            position_change = cum_exec_qty if side == 'Buy' else -cum_exec_qty

            fill_event = FillEvent(
                exchange_code='bybit',
                symbol=symbol,
                order_id=order_link_id or order_id,
                side=side.upper(),
                filled_quantity=cum_exec_qty,
                filled_price=avg_price,
                trade_id=f"order_{order_id}",
                timestamp=timestamp,
                commission=abs(cum_exec_fee),
                commission_asset='USDT'  # Bybit主要用USDT作为手续费
            )

            logger.debug(f"📈 Bybit订单事件解析成功: {fill_event}")
            return fill_event

        except Exception as e:
            logger.error(f"❌ 转换Bybit订单为成交事件失败: {e}")
            return None

    async def _listen_messages(self):
        """重写消息监听，添加认证流程"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            # 首先进行认证和订阅
            await self.authenticate_and_subscribe()

            # 然后开始监听消息
            while self._running:
                try:
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)

                    # 解析消息
                    if isinstance(message, str):
                        data = json.loads(message)
                    else:
                        data = message

                    # 跳过心跳消息
                    if data.get('op') == 'ping':
                        await self._handle_ping()
                        continue

                    # 处理消息
                    await self._handle_message(data)

                except asyncio.TimeoutError:
                    # 超时是正常的，用于检查运行状态
                    continue

                except Exception as e:
                    logger.error(f"❌ Bybit处理消息异常: {e}")
                    break

        except Exception as e:
            if self._running:
                logger.error(f"❌ Bybit消息监听循环异常: {e}")
            raise

    async def _handle_ping(self):
        """处理ping消息"""
        try:
            # 发送pong响应
            pong_msg = {"op": "pong"}
            await self._ws.send(json.dumps(pong_msg))
            logger.debug("🏓 Bybit发送pong响应")
        except Exception as e:
            logger.error(f"❌ Bybit发送pong响应失败: {e}")

    async def stop(self):
        """停止WebSocket连接"""
        await super().stop()

        # 重置订阅状态
        self._subscribed = False
        self._auth_response = ""

    def get_stats(self) -> dict:
        """获取连接统计信息"""
        stats = super().get_stats()
        stats['subscribed'] = self._subscribed
        stats['auth_response'] = self._auth_response
        return stats

    def get_status_report(self) -> str:
        """获取状态报告"""
        stats = self.get_stats()

        report = f"📊 BYBIT 成交WebSocket状态\n"
        report += f"  • 运行状态: {'🟢 运行中' if self._running else '🔴 已停止'}\n"
        report += f"  • 认证状态: {'🟢 已认证' if stats['subscribed'] else '🔴 未认证'}\n"
        report += f"  • 环境: {'测试网' if self.is_testnet else '主网'}\n"
        report += f"  • 重连次数: {stats['total_reconnects']}\n"
        report += f"  • 连接错误: {stats['connection_errors']}\n"

        if stats['last_connect_time'] > 0:
            duration = stats.get('connected_duration', 0)
            report += f"  • 连接时长: {duration/60:.1f}分钟\n"

        if self._auth_response:
            report += f"  • 认证响应: {self._auth_response}\n"

        report += f"  • 总成交数: {stats['total_fills']}\n"
        report += f"  • 成交频率: {stats['fills_per_hour']:.1f}/小时\n"

        if stats['last_fill_time'] > 0:
            last_fill_age = time.time() - stats['last_fill_time']
            report += f"  • 最后成交: {last_fill_age/60:.1f}分钟前\n"

        return report