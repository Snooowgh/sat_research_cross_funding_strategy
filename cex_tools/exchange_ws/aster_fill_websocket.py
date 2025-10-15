# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : aster_fill_websocket
@Description : Aster成交WebSocket实现
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


class AsterFillWebSocket(FillWebSocketStream):
    """Aster成交WebSocket实现"""

    def __init__(self, api_key: str, secret: str,
                 on_fill_callback: Callable[[FillEvent], None],
                 is_testnet: bool = False):
        """
        初始化Aster成交WebSocket

        Args:
            api_key: API密钥
            secret: API密钥
            on_fill_callback: 成交事件回调函数
            is_testnet: 是否使用测试网络
        """
        super().__init__("aster", on_fill_callback)
        self.api_key = api_key
        self.secret = secret
        self.is_testnet = is_testnet

        # Aster WebSocket端点
        if is_testnet:
            self.ws_url = "wss://testnet-api.aster.exchange/ws"
        else:
            self.ws_url = "wss://api.aster.exchange/ws"

        # 订阅管理
        self._subscribed = False
        self._auth_token = ""

    def _generate_signature(self, timestamp: str, method: str, path: str) -> str:
        """生成签名"""
        message = f"{timestamp}{method}{path}"
        signature = hmac.new(
            bytes(self.secret, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod=hashlib.sha256
        ).hexdigest()
        return base64.b64encode(signature.encode()).decode()

    async def connect(self):
        """连接WebSocket"""
        try:
            logger.debug(f"🔌 连接Aster WebSocket: {self.ws_url}")

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
                logger.info(f"✅ Aster WebSocket重连成功")

        except Exception as e:
            raise FillWebSocketConnectionError("aster", f"连接失败: {str(e)}", e)

    async def subscribe_user_events(self):
        """订阅用户事件"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            # 认证并订阅私有频道
            await self.authenticate_and_subscribe()

        except Exception as e:
            raise FillWebSocketError("aster", f"订阅用户事件失败: {str(e)}", e)

    async def authenticate_and_subscribe(self):
        """认证并订阅私有频道"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            # 生成认证参数
            timestamp = str(int(time.time() * 1000))
            signature = self._generate_signature(timestamp, "GET", "/ws/auth")

            # 认证消息
            auth_msg = {
                "id": int(time.time() * 1000),
                "method": "server.authenticate",
                "params": {
                    "api_key": self.api_key,
                    "timestamp": timestamp,
                    "signature": signature
                }
            }

            await self._ws.send(json.dumps(auth_msg))
            logger.debug("✅ Aster认证请求已发送")

            # 等待认证确认
            await asyncio.wait_for(self._wait_for_auth_confirmation(), timeout=10.0)

            # 认证成功后订阅频道
            await self._subscribe_private_channels()

        except asyncio.TimeoutError:
            raise FillWebSocketError("aster", "认证或订阅超时")
        except Exception as e:
            raise FillWebSocketError("aster", f"认证或订阅失败: {str(e)}", e)

    async def _wait_for_auth_confirmation(self):
        """等待认证确认"""
        try:
            while not self._subscribed and self._running:
                try:
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)
                    data = json.loads(message)

                    if data.get('id') and 'result' in data:
                        result = data.get('result', {})
                        if result.get('success'):
                            self._auth_token = result.get('token', '')
                            logger.debug(f"✅ Aster认证成功，Token: {self._auth_token[:12]}...")
                            break
                        else:
                            error_msg = result.get('error', '未知认证错误')
                            raise FillWebSocketError("aster", f"认证失败: {error_msg}")

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            logger.error(f"❌ 等待Aster认证确认异常: {e}")
            raise

    async def _subscribe_private_channels(self):
        """订阅私有频道"""
        try:
            # 订阅订单频道
            orders_msg = {
                "id": int(time.time() * 1000),
                "method": "server.subscribe",
                "params": {
                    "channel": "orders",
                    "token": self._auth_token
                }
            }
            await self._ws.send(json.dumps(orders_msg))

            # 订阅成交频道
            trades_msg = {
                "id": int(time.time() * 1000 + 1),
                "method": "server.subscribe",
                "params": {
                    "channel": "trades",
                    "token": self._auth_token
                }
            }
            await self._ws.send(json.dumps(trades_msg))

            # 订阅持仓频道
            positions_msg = {
                "id": int(time.time() * 1000 + 2),
                "method": "server.subscribe",
                "params": {
                    "channel": "positions",
                    "token": self._auth_token
                }
            }
            await self._ws.send(json.dumps(positions_msg))

            logger.debug("✅ Aster私有频道订阅请求已发送")

            # 等待订阅确认
            await asyncio.wait_for(self._wait_for_subscription_confirmation(), timeout=10.0)

        except Exception as e:
            logger.error(f"❌ Aster订阅私有频道失败: {e}")
            raise

    async def _wait_for_subscription_confirmation(self):
        """等待订阅确认"""
        try:
            subscription_count = 0
            required_subscriptions = 3  # orders, trades, positions

            while subscription_count < required_subscriptions and self._running:
                try:
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)
                    data = json.loads(message)

                    if data.get('method') == 'server.subscription':
                        channel = data.get('params', {}).get('channel')
                        if channel in ['orders', 'trades', 'positions']:
                            subscription_count += 1
                            logger.debug(f"✅ Aster {channel} 频道订阅成功")

                    elif data.get('error'):
                        error_msg = data.get('error', {}).get('message', '未知订阅错误')
                        raise FillWebSocketError("aster", f"订阅失败: {error_msg}")

                except asyncio.TimeoutError:
                    continue

            if subscription_count >= required_subscriptions:
                self._subscribed = True
                logger.debug("✅ Aster所有私有频道订阅成功")

        except Exception as e:
            logger.error(f"❌ 等待Aster订阅确认异常: {e}")
            raise

    def _parse_message(self, message: dict) -> Optional[FillEvent]:
        """解析Aster WebSocket消息为成交事件"""
        try:
            method = message.get('method', '')
            params = message.get('params', {})

            if method == 'server.trade':
                return self._parse_trade_data(params)
            elif method == 'server.order':
                return self._parse_order_data(params)
            elif method == 'server.position':
                # 持仓变化，不直接处理为成交事件
                logger.debug(f"📝 Aster持仓更新")
                return None
            elif message.get('error'):
                error_msg = message.get('error', {}).get('message', '未知错误')
                logger.error(f"❌ Aster WebSocket错误: {error_msg}")
                return None
            else:
                # 其他频道
                logger.debug(f"📝 Aster收到其他方法: {method}")
                return None

        except Exception as e:
            logger.error(f"❌ 解析Aster消息失败: {e}")
            logger.debug(f"原始消息: {message}")
            return None

    def _parse_trade_data(self, trade_data: dict) -> Optional[FillEvent]:
        """解析成交数据"""
        try:
            if not trade_data:
                return None

            # 提取成交信息
            symbol = trade_data.get('symbol', '')
            order_id = trade_data.get('order_id', '')
            trade_id = trade_data.get('trade_id', '')
            price = float(trade_data.get('price', 0))
            quantity = float(trade_data.get('quantity', 0))
            side = trade_data.get('side', '').upper()
            fee = float(trade_data.get('fee', 0))
            fee_currency = trade_data.get('fee_currency', '')
            timestamp = int(trade_data.get('timestamp', 0))

            # 验证必要字段
            if not all([symbol, order_id, trade_id, price > 0, quantity > 0, timestamp > 0]):
                logger.warning(f"⚠️ Aster成交事件缺少必要字段: {trade_data}")
                return None

            # 转换时间戳（毫秒转秒）
            timestamp_sec = timestamp / 1000

            # 计算仓位变化
            position_change = quantity if side == 'BUY' else -quantity

            fill_event = FillEvent(
                exchange_code='aster',
                symbol=symbol,
                order_id=order_id,
                side=side,
                filled_quantity=quantity,
                filled_price=price,
                trade_id=trade_id,
                timestamp=timestamp_sec,
                commission=abs(fee),
                commission_asset=fee_currency
            )

            logger.debug(f"📈 Aster成交事件解析成功: {fill_event}")
            return fill_event

        except Exception as e:
            logger.error(f"❌ 解析Aster成交数据失败: {e}")
            return None

    def _parse_order_data(self, order_data: dict) -> Optional[FillEvent]:
        """解析订单数据（作为成交事件的补充）"""
        try:
            if not order_data:
                return None

            order_status = order_data.get('status', '')
            if order_status in ['PARTIALLY_FILLED', 'FILLED']:
                filled_quantity = float(order_data.get('filled_quantity', 0))
                if filled_quantity > 0:
                    return self._convert_order_to_fill(order_data)

            return None

        except Exception as e:
            logger.error(f"❌ 解析Aster订单数据失败: {e}")
            return None

    def _convert_order_to_fill(self, order: dict) -> Optional[FillEvent]:
        """将订单信息转换为成交事件"""
        try:
            symbol = order.get('symbol', '')
            order_id = order.get('order_id', '')
            avg_price = float(order.get('average_price', 0))
            filled_quantity = float(order.get('filled_quantity', 0))
            side = order.get('side', '').upper()
            fee = float(order.get('fee', 0))
            fee_currency = order.get('fee_currency', '')
            created_time = int(order.get('created_at', 0))

            if not all([symbol, order_id, avg_price > 0, filled_quantity > 0, created_time > 0]):
                return None

            timestamp_sec = created_time / 1000

            position_change = filled_quantity if side == 'BUY' else -filled_quantity

            fill_event = FillEvent(
                exchange_code='aster',
                symbol=symbol,
                order_id=order_id,
                side=side,
                filled_quantity=filled_quantity,
                filled_price=avg_price,
                trade_id=f"order_{order_id}",
                timestamp=timestamp_sec,
                commission=abs(fee),
                commission_asset=fee_currency
            )

            logger.debug(f"📈 Aster订单事件解析成功: {fill_event}")
            return fill_event

        except Exception as e:
            logger.error(f"❌ 转换Aster订单为成交事件失败: {e}")
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

                    # 处理消息
                    await self._handle_message(data)

                except asyncio.TimeoutError:
                    # 超时是正常的，用于检查运行状态
                    continue

                except Exception as e:
                    logger.error(f"❌ Aster处理消息异常: {e}")
                    break

        except Exception as e:
            if self._running:
                logger.error(f"❌ Aster消息监听循环异常: {e}")
            raise

    async def stop(self):
        """停止WebSocket连接"""
        await super().stop()

        # 重置订阅状态
        self._subscribed = False
        self._auth_token = ""

    def get_stats(self) -> dict:
        """获取连接统计信息"""
        stats = super().get_stats()
        stats['subscribed'] = self._subscribed
        stats['auth_token'] = self._auth_token[:12] + "..." if self._auth_token else ""
        return stats

    def get_status_report(self) -> str:
        """获取状态报告"""
        stats = self.get_stats()

        report = f"📊 ASTER 成交WebSocket状态\n"
        report += f"  • 运行状态: {'🟢 运行中' if self._running else '🔴 已停止'}\n"
        report += f"  • 认证状态: {'🟢 已认证' if stats['subscribed'] else '🔴 未认证'}\n"
        report += f"  • 环境: {'测试网' if self.is_testnet else '主网'}\n"
        report += f"  • 重连次数: {stats['total_reconnects']}\n"
        report += f"  • 连接错误: {stats['connection_errors']}\n"

        if stats['last_connect_time'] > 0:
            duration = stats.get('connected_duration', 0)
            report += f"  • 连接时长: {duration/60:.1f}分钟\n"

        if self._auth_token:
            report += f"  • 认证令牌: {stats['auth_token']}\n"

        report += f"  • 总成交数: {stats['total_fills']}\n"
        report += f"  • 成交频率: {stats['fills_per_hour']:.1f}/小时\n"

        if stats['last_fill_time'] > 0:
            last_fill_age = time.time() - stats['last_fill_time']
            report += f"  • 最后成交: {last_fill_age/60:.1f}分钟前\n"

        return report