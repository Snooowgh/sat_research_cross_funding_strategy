# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : okx_fill_websocket
@Description : OKX成交WebSocket实现
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


class OkxFillWebSocket(FillWebSocketStream):
    """OKX成交WebSocket实现"""

    def __init__(self, api_key: str, secret: str, passphrase: str,
                 on_fill_callback: Callable[[FillEvent], None],
                 is_demo: bool = False):
        """
        初始化OKX成交WebSocket

        Args:
            api_key: API密钥
            secret: API密钥
            passphrase: API密码短语
            on_fill_callback: 成交事件回调函数
            is_demo: 是否使用模拟环境
        """
        super().__init__("okx", on_fill_callback)
        self.api_key = api_key
        self.secret = secret
        self.passphrase = passphrase
        self.is_demo = is_demo

        # OKX WebSocket端点
        if is_demo:
            self.ws_url = "wss://wspap.openapi.okx.com:8443/ws/v5/public"
            self.private_ws_url = "wss://wspapinyin.okx.com:8443/ws/v5/private"
        else:
            self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
            self.private_ws_url = "wss://ws.okx.com:8443/ws/v5/private"

        # 订阅管理
        self._subscribed = False
        self._login_timestamp = ""

    def _generate_signature(self, timestamp: str) -> str:
        """生成签名"""
        message = timestamp + "GET" + "/users/self/verify"
        mac = hmac.new(
            bytes(self.secret, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod=hashlib.sha256
        )
        return base64.b64encode(mac.digest()).decode()

    async def connect(self):
        """连接WebSocket"""
        try:
            logger.debug(f"🔌 连接OKX WebSocket: {self.private_ws_url}")

            self._ws = await asyncio.wait_for(
                websockets.connect(
                    self.private_ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10
                ),
                timeout=10.0
            )

            # 重置重连计数
            if self._reconnect_count > 0:
                logger.info(f"✅ OKX WebSocket重连成功")

        except Exception as e:
            raise FillWebSocketConnectionError("okx", f"连接失败: {str(e)}", e)

    async def subscribe_user_events(self):
        """订阅用户事件"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            # 登录并订阅私有频道
            await self.login_and_subscribe()

        except Exception as e:
            raise FillWebSocketError("okx", f"订阅用户事件失败: {str(e)}", e)

    async def login_and_subscribe(self):
        """登录并订阅私有频道"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            # 生成登录签名
            self._login_timestamp = str(int(time.time()))
            signature = self._generate_signature(self._login_timestamp)

            # 登录消息
            login_msg = {
                "op": "login",
                "args": [{
                    "apiKey": self.api_key,
                    "passphrase": self.passphrase,
                    "timestamp": self._login_timestamp,
                    "sign": signature
                }]
            }

            await self._ws.send(json.dumps(login_msg))
            logger.debug("✅ OKX登录请求已发送")

            # 等待登录确认
            await asyncio.wait_for(self._wait_for_login_confirmation(), timeout=10.0)

            # 登录成功后订阅订单和成交频道
            await self._subscribe_private_channels()

        except asyncio.TimeoutError:
            raise FillWebSocketError("okx", "登录或订阅超时")
        except Exception as e:
            raise FillWebSocketError("okx", f"登录或订阅失败: {str(e)}", e)

    async def _wait_for_login_confirmation(self):
        """等待登录确认"""
        try:
            while not self._subscribed and self._running:
                try:
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)
                    data = json.loads(message)

                    if data.get('event') == 'login' and data.get('code') == '0':
                        logger.debug("✅ OKX登录成功")
                        break
                    elif data.get('event') == 'error':
                        error_msg = data.get('msg', '未知登录错误')
                        raise FillWebSocketError("okx", f"登录失败: {error_msg}")

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            logger.error(f"❌ 等待OKX登录确认异常: {e}")
            raise

    async def _subscribe_private_channels(self):
        """订阅私有频道"""
        try:
            # 订阅订单频道
            orders_msg = {
                "op": "subscribe",
                "args": [{"channel": "orders"}]
            }
            await self._ws.send(json.dumps(orders_msg))

            # 订阅成交频道
            fills_msg = {
                "op": "subscribe",
                "args": [{"channel": "fills"}]
            }
            await self._ws.send(json.dumps(fills_msg))

            # 订阅账户频道
            account_msg = {
                "op": "subscribe",
                "args": [{"channel": "account"}]
            }
            await self._ws.send(json.dumps(account_msg))

            logger.debug("✅ OKX私有频道订阅请求已发送")

            # 等待订阅确认
            await asyncio.wait_for(self._wait_for_subscription_confirmation(), timeout=10.0)

        except Exception as e:
            logger.error(f"❌ OKX订阅私有频道失败: {e}")
            raise

    async def _wait_for_subscription_confirmation(self):
        """等待订阅确认"""
        try:
            subscription_count = 0
            required_subscriptions = 3  # orders, fills, account

            while subscription_count < required_subscriptions and self._running:
                try:
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)
                    data = json.loads(message)

                    if data.get('event') == 'subscribe':
                        channel = data.get('arg', {}).get('channel')
                        if channel in ['orders', 'fills', 'account']:
                            subscription_count += 1
                            logger.debug(f"✅ OKX {channel} 频道订阅成功")

                    elif data.get('event') == 'error':
                        error_msg = data.get('msg', '未知订阅错误')
                        raise FillWebSocketError("okx", f"订阅失败: {error_msg}")

                except asyncio.TimeoutError:
                    continue

            if subscription_count >= required_subscriptions:
                self._subscribed = True
                logger.debug("✅ OKX所有私有频道订阅成功")

        except Exception as e:
            logger.error(f"❌ 等待OKX订阅确认异常: {e}")
            raise

    def _parse_message(self, message: dict) -> Optional[FillEvent]:
        """解析OKX WebSocket消息为成交事件"""
        try:
            data = message.get('data', [])
            arg = message.get('arg', {})
            channel = arg.get('channel', '')

            if channel == 'fills':
                return self._parse_fills_data(data)
            elif channel == 'orders':
                return self._parse_orders_data(data)
            elif channel == 'error' or message.get('event') == 'error':
                error_msg = message.get('msg', '未知错误')
                logger.error(f"❌ OKX WebSocket错误: {error_msg}")
                return None
            else:
                # 其他频道，如账户信息等
                logger.debug(f"📝 OKX收到其他频道: {channel}")
                return None

        except Exception as e:
            logger.error(f"❌ 解析OKX消息失败: {e}")
            logger.debug(f"原始消息: {message}")
            return None

    def _parse_fills_data(self, fills_data: list) -> Optional[FillEvent]:
        """解析成交数据"""
        try:
            if not fills_data:
                return None

            # 取最新的成交
            fill = fills_data[0] if isinstance(fills_data, list) else fills_data

            # 提取成交信息
            inst_id = fill.get('instId', '')
            trade_id = fill.get('tradeId', '')
            fill_price = float(fill.get('fillPx', 0))
            fill_size = float(fill.get('fillSz', 0))
            side = fill.get('side', '').upper()
            pos_side = fill.get('posSide', '').upper()
            currency = fill.get('ccy', '')
            timestamp = int(fill.get('ts', 0))

            # 验证必要字段
            if not all([inst_id, trade_id, fill_price > 0, fill_size > 0, timestamp > 0]):
                logger.warning(f"⚠️ OKX成交事件缺少必要字段: {fill}")
                return None

            # 转换时间戳（毫秒转秒）
            timestamp_sec = timestamp / 1000

            # 构造symbol（OKX的instId通常是标准格式）
            symbol = inst_id

            # 计算仓位变化（考虑持仓方向）
            if pos_side == 'LONG':
                position_change = fill_size if side == 'BUY' else -fill_size
            elif pos_side == 'SHORT':
                position_change = -fill_size if side == 'BUY' else fill_size
            else:
                position_change = fill_size if side == 'BUY' else -fill_size

            fill_event = FillEvent(
                exchange_code='okx',
                symbol=symbol,
                order_id=trade_id,
                side=side,
                filled_quantity=fill_size,
                filled_price=fill_price,
                trade_id=trade_id,
                timestamp=timestamp_sec,
                commission=float(fill.get('fee', 0)) if fill.get('fee') else 0.0,
                commission_asset=currency if currency else 'USDT'
            )

            logger.debug(f"📈 OKX成交事件解析成功: {fill_event}")
            return fill_event

        except Exception as e:
            logger.error(f"❌ 解析OKX成交数据失败: {e}")
            return None

    def _parse_orders_data(self, orders_data: list) -> Optional[FillEvent]:
        """解析订单数据（作为成交事件的补充）"""
        try:
            if not orders_data:
                return None

            # 处理每个订单
            for order in orders_data if isinstance(orders_data, list) else [orders_data]:
                state = order.get('state', '')
                if state in ['partially_filled', 'fully_filled']:
                    return self._convert_order_to_fill(order)

            return None

        except Exception as e:
            logger.error(f"❌ 解析OKX订单数据失败: {e}")
            return None

    def _convert_order_to_fill(self, order: dict) -> Optional[FillEvent]:
        """将订单信息转换为成交事件"""
        try:
            inst_id = order.get('instId', '')
            order_id = order.get('ordId', '')
            avg_price = float(order.get('avgPx', 0))
            filled_size = float(order.get('fillSz', 0))
            side = order.get('side', '').upper()
            pos_side = order.get('posSide', '').upper()
            currency = order.get('ccy', '')
            timestamp = int(order.get('cTime', 0))

            if not all([inst_id, order_id, avg_price > 0, filled_size > 0, timestamp > 0]):
                return None

            timestamp_sec = timestamp / 1000
            symbol = inst_id

            # 计算仓位变化
            if pos_side == 'LONG':
                position_change = filled_size if side == 'BUY' else -filled_size
            elif pos_side == 'SHORT':
                position_change = -filled_size if side == 'BUY' else filled_size
            else:
                position_change = filled_size if side == 'BUY' else -filled_size

            fill_event = FillEvent(
                exchange_code='okx',
                symbol=symbol,
                order_id=order_id,
                side=side,
                filled_quantity=filled_size,
                filled_price=avg_price,
                trade_id=f"order_{order_id}",
                timestamp=timestamp_sec,
                commission=float(order.get('fee', 0)) if order.get('fee') else 0.0,
                commission_asset=currency if currency else 'USDT'
            )

            logger.debug(f"📈 OKX订单事件解析成功: {fill_event}")
            return fill_event

        except Exception as e:
            logger.error(f"❌ 转换OKX订单为成交事件失败: {e}")
            return None

    async def _listen_messages(self):
        """重写消息监听，添加登录流程"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        try:
            # 首先进行登录和订阅
            await self.login_and_subscribe()

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
                    if data.get('event') == 'ping':
                        await self._handle_ping()
                        continue

                    # 处理消息
                    await self._handle_message(data)

                except asyncio.TimeoutError:
                    # 超时是正常的，用于检查运行状态
                    continue

                except Exception as e:
                    logger.error(f"❌ OKX处理消息异常: {e}")
                    break

        except Exception as e:
            if self._running:
                logger.error(f"❌ OKX消息监听循环异常: {e}")
            raise

    async def _handle_ping(self):
        """处理ping消息"""
        try:
            # 发送pong响应
            pong_msg = {"op": "pong"}
            await self._ws.send(json.dumps(pong_msg))
            logger.debug("🏓 OKX发送pong响应")
        except Exception as e:
            logger.error(f"❌ OKX发送pong响应失败: {e}")

    async def stop(self):
        """停止WebSocket连接"""
        await super().stop()

        # 重置订阅状态
        self._subscribed = False
        self._login_timestamp = ""

    def get_stats(self) -> dict:
        """获取连接统计信息"""
        stats = super().get_stats()
        stats['subscribed'] = self._subscribed
        stats['login_timestamp'] = self._login_timestamp
        return stats

    def get_status_report(self) -> str:
        """获取状态报告"""
        stats = self.get_stats()

        report = f"📊 OKX 成交WebSocket状态\n"
        report += f"  • 运行状态: {'🟢 运行中' if self._running else '🔴 已停止'}\n"
        report += f"  • 登录状态: {'🟢 已登录' if stats['subscribed'] else '🔴 未登录'}\n"
        report += f"  • 环境: {'模拟' if self.is_demo else '实盘'}\n"
        report += f"  • 重连次数: {stats['total_reconnects']}\n"
        report += f"  • 连接错误: {stats['connection_errors']}\n"

        if stats['last_connect_time'] > 0:
            duration = stats.get('connected_duration', 0)
            report += f"  • 连接时长: {duration/60:.1f}分钟\n"

        if self._login_timestamp:
            login_time = int(self._login_timestamp)
            login_duration = (time.time() - login_time / 1000) / 60
            report += f"  • 登录时长: {login_duration:.1f}分钟\n"

        report += f"  • 总成交数: {stats['total_fills']}\n"
        report += f"  • 成交频率: {stats['fills_per_hour']:.1f}/小时\n"

        if stats['last_fill_time'] > 0:
            last_fill_age = time.time() - stats['last_fill_time']
            report += f"  • 最后成交: {last_fill_age/60:.1f}分钟前\n"

        return report