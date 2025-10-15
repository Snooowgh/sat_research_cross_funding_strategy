# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : binance_fill_websocket
@Description : Binance成交WebSocket实现
@Time        : 2025/10/15
"""
import asyncio
import aiohttp
import websockets
import json
import time
from typing import Optional, Callable
from loguru import logger

from cex_tools.exchange_ws.fill_websocket_stream import FillWebSocketStream, FillWebSocketError, FillWebSocketConnectionError
from cex_tools.exchange_model.fill_event_model import FillEvent


class BinanceFillWebSocket(FillWebSocketStream):
    """Binance成交WebSocket实现"""

    def __init__(self, api_key: str, secret: str, on_fill_callback: Callable[[FillEvent], None]):
        """
        初始化Binance成交WebSocket

        Args:
            api_key: Binance API密钥
            secret: Binance API密钥
            on_fill_callback: 成交事件回调函数
        """
        super().__init__("binance", on_fill_callback)
        self.api_key = api_key
        self.secret = secret
        self.listen_key = None

        # Binance API端点
        self.base_url = "https://fapi.binance.com"
        self.ws_base_url = "wss://fstream.binance.com/ws"

        # 会话管理
        self._session = None

    async def connect(self):
        """连接WebSocket并获取Listen Key"""
        try:
            # 1. 获取Listen Key
            await self._get_listen_key()

            # 2. 建立WebSocket连接
            ws_url = f"{self.ws_base_url}/{self.listen_key}"
            logger.debug(f"🔌 连接Binance WebSocket: {ws_url}")

            self._ws = await websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )

            # 重置重连计数
            if self._reconnect_count > 0:
                logger.info(f"✅ Binance WebSocket重连成功")

        except Exception as e:
            raise FillWebSocketConnectionError("binance", f"连接失败: {str(e)}", e)

    async def subscribe_user_events(self):
        """订阅用户事件"""
        # Binance的用户数据流在连接后自动推送所有事件，无需额外订阅
        logger.debug("✅ Binance用户数据流订阅完成")

    def _parse_message(self, message: dict) -> Optional[FillEvent]:
        """解析Binance WebSocket消息为成交事件"""
        try:
            # Binance用户数据流格式
            event_type = message.get('e')

            if event_type == 'ORDER_TRADE_UPDATE':
                return self._parse_order_trade_update(message)
            elif event_type == 'ACCOUNT_UPDATE':
                # 账户更新事件，可能包含余额变化
                return self._parse_account_update(message)
            else:
                # 其他事件类型，如 listenKey expired 等
                logger.debug(f"📝 Binance收到其他事件: {event_type}")
                return None

        except Exception as e:
            logger.error(f"❌ 解析Binance消息失败: {e}")
            logger.debug(f"原始消息: {message}")
            return None

    def _parse_order_trade_update(self, message: dict) -> Optional[FillEvent]:
        """解析订单交易更新事件"""
        try:
            order = message.get('o', {})
            if not order:
                return None

            order_status = order.get('X', '')
            if order_status not in ['FILLED', 'PARTIALLY_FILLED']:
                return None

            # 提取订单信息
            symbol = order.get('s', '')
            order_id = str(order.get('i', ''))
            side = order.get('S', '')
            order_type = order.get('o', '')

            # 成交信息
            filled_quantity = float(order.get('z', 0))
            executed_quantity = float(order.get('z', 0)) - float(order.get('apz', 0))  # 本次成交数量
            average_price = float(order.get('ap', 0))

            # 交易信息
            trade_id = str(order.get('t', ''))
            commission = float(order.get('n', 0))
            commission_asset = order.get('N', '')

            # 时间戳
            timestamp = message.get('T', 0) / 1000  # 转换为秒

            # 验证必要字段
            if not all([symbol, order_id, side, executed_quantity > 0, average_price > 0]):
                logger.warning(f"⚠️ Binance成交事件缺少必要字段: {order}")
                return None

            # 确保symbol格式统一（包含USDT后缀）
            if not symbol.endswith('USDT'):
                symbol = symbol + 'USDT'

            fill_event = FillEvent(
                exchange_code='binance',
                symbol=symbol,
                order_id=order_id,
                side=side,
                filled_quantity=executed_quantity,
                filled_price=average_price,
                trade_id=trade_id,
                timestamp=timestamp,
                commission=commission,
                commission_asset=commission_asset
            )

            logger.debug(f"📈 Binance成交事件解析成功: {fill_event}")
            return fill_event

        except Exception as e:
            logger.error(f"❌ 解析Binance订单交易更新失败: {e}")
            return None

    def _parse_account_update(self, message: dict) -> Optional[FillEvent]:
        """解析账户更新事件（通常不包含成交信息，但可以作为补充）"""
        # 账户更新主要包含余额变化，不是直接的成交事件
        # 这里暂时返回None，实际成交事件主要通过ORDER_TRADE_UPDATE获取
        return None

    async def _get_listen_key(self):
        """获取Binance用户数据流Listen Key"""
        url = f"{self.base_url}/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self.api_key}

        try:
            # 创建会话
            if not self._session:
                self._session = aiohttp.ClientSession()

            async with self._session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    self.listen_key = data.get("listenKey")

                    if not self.listen_key:
                        raise FillWebSocketError("binance", "获取Listen Key失败：响应中没有listenKey")

                    logger.debug(f"✅ 获取Binance Listen Key成功: {self.listen_key[:8]}...")
                    return self.listen_key
                else:
                    error_text = await response.text()
                    raise FillWebSocketError("binance", f"获取Listen Key失败: HTTP {response.status} - {error_text}")

        except asyncio.TimeoutError:
            raise FillWebSocketError("binance", "获取Listen Key超时")
        except Exception as e:
            raise FillWebSocketError("binance", f"获取Listen Key异常: {str(e)}", e)

    async def _keep_alive_listen_key(self):
        """定期刷新Listen Key保持活跃"""
        if not self.listen_key:
            return

        try:
            url = f"{self.base_url}/fapi/v1/listenKey"
            headers = {"X-MBX-APIKEY": self.api_key}
            params = {"listenKey": self.listen_key}

            if not self._session:
                self._session = aiohttp.ClientSession()

            async with self._session.put(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    logger.debug("✅ Binance Listen Key刷新成功")
                else:
                    logger.warning(f"⚠️ Binance Listen Key刷新失败: HTTP {response.status}")

        except Exception as e:
            logger.error(f"❌ Binance Listen Key刷新异常: {e}")

    async def _listen_messages(self):
        """重写消息监听，添加Listen Key保活逻辑"""
        if not self._ws:
            raise RuntimeError("WebSocket连接未建立")

        last_keepalive = time.time()
        keepalive_interval = 30 * 60  # 30分钟刷新一次

        try:
            while self._running:
                try:
                    # 使用较短的超时来检查运行状态和保活
                    message = await asyncio.wait_for(self._ws.recv(), timeout=5.0)

                    # 解析消息
                    if isinstance(message, str):
                        data = json.loads(message)
                    else:
                        data = message

                    # 检查是否为错误消息
                    if data.get('e') == 'error':
                        error_msg = data.get('m', '未知错误')
                        logger.error(f"❌ Binance WebSocket错误: {error_msg}")

                        # 如果是Listen Key过期，尝试重新获取
                        if 'listenKey expired' in error_msg.lower():
                            logger.warning("⚠️ Binance Listen Key已过期，尝试重新获取")
                            await self._get_listen_key()
                            continue

                    # 处理消息
                    await self._handle_message(data)

                    # 定期刷新Listen Key
                    current_time = time.time()
                    if current_time - last_keepalive > keepalive_interval:
                        await self._keep_alive_listen_key()
                        last_keepalive = current_time

                except asyncio.TimeoutError:
                    # 超时是正常的，用于检查运行状态
                    continue

                except Exception as e:
                    logger.error(f"❌ Binance处理消息异常: {e}")
                    break

        except Exception as e:
            if self._running:
                logger.error(f"❌ Binance消息监听循环异常: {e}")
            raise

    async def stop(self):
        """停止WebSocket连接"""
        await super().stop()

        # 关闭HTTP会话
        if self._session:
            try:
                await self._session.close()
            except Exception as e:
                logger.warning(f"⚠️ 关闭Binance HTTP会话异常: {e}")
            finally:
                self._session = None

        # 可选：删除Listen Key
        if self.listen_key:
            try:
                await self._delete_listen_key()
            except Exception as e:
                logger.warning(f"⚠️ 删除Binance Listen Key异常: {e}")

    async def _delete_listen_key(self):
        """删除Listen Key"""
        url = f"{self.base_url}/fapi/v1/listenKey"
        headers = {"X-MBX-APIKEY": self.api_key}
        params = {"listenKey": self.listen_key}

        try:
            if not self._session:
                self._session = aiohttp.ClientSession()

            async with self._session.delete(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
                if response.status == 200:
                    logger.debug("✅ Binance Listen Key删除成功")
                else:
                    logger.warning(f"⚠️ Binance Listen Key删除失败: HTTP {response.status}")

        except Exception as e:
            logger.error(f"❌ 删除Binance Listen Key异常: {e}")

    def __del__(self):
        """析构函数，确保资源清理"""
        if hasattr(self, '_session') and self._session:
            try:
                asyncio.create_task(self._session.close())
            except:
                pass