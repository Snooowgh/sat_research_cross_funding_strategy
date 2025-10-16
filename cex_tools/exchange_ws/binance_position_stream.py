# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : binance_position_stream
@Description : Binance仓位WebSocket流实现（使用binance_f WebSocket User Data Stream）
@Time        : 2025/10/15
"""
import asyncio
import json
import threading
from loguru import logger
from typing import Optional, Dict, Any
import websockets.client
from cex_tools.cex_enum import ExchangeEnum
from cex_tools.exchange_model.order_update_event_model import OrderUpdateEvent
from cex_tools.exchange_ws.position_stream import PositionWebSocketStream
from cex_tools.exchange_model.position_model import BinancePositionDetail
from binance.um_futures import UMFutures


class BinancePositionWebSocket(PositionWebSocketStream):
    """Binance仓位WebSocket流实现（使用binance_f WebSocket User Data Stream）"""

    def __init__(self, api_key: str = None, secret: str = None, sandbox: bool = False, **kwargs):
        """
        初始化Binance仓位WebSocket流

        Args:
            api_key: API密钥
            secret: API密钥
            sandbox: 是否使用沙盒环境
            **kwargs: 其他配置参数
        """
        super().__init__(ExchangeEnum.BINANCE, **kwargs)
        self.api_key = api_key
        self.secret = secret
        self.sandbox = sandbox

        # 创建binance_f客户端
        if sandbox:
            self.client = UMFutures(
                key=api_key,
                secret=secret,
                base_url="https://testnet.binancefuture.com"
            )
        else:
            self.client = UMFutures(
                key=api_key,
                secret=secret
            )

        # WebSocket配置
        self.base_ws_url = (
            "wss://stream.binancefuture.com/ws" if sandbox
            else "wss://fstream.binance.com/ws"
        )

        # 连接管理
        self.listen_key: Optional[str] = None
        self._ws_connection = None
        self._listen_task: Optional[asyncio.Task] = None
        self._renew_task: Optional[asyncio.Task] = None  # listenKey续期任务
        self._ws_thread: Optional[threading.Thread] = None

        # 状态管理
        self._running = False
        self._last_positions: Dict[str, BinancePositionDetail] = {}

    async def _get_listen_key(self) -> Optional[str]:
        """
        获取用户数据流listen key

        Returns:
            str: listen key，失败返回None
        """
        try:
            response = self.client.new_listen_key()
            if response and 'listenKey' in response:
                listen_key = response['listenKey']
                logger.debug(f"[{self.exchange_code}] 获取listen key成功: {listen_key[:10]}...")
                return listen_key
            else:
                logger.error(f"[{self.exchange_code}] 获取listen key失败: 响应格式错误 {response}")
                return None
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 获取listen key异常: {e}")
            return None

    async def _renew_listen_key(self):
        """
        定期续期listen key（每24小时）
        """
        while self._running and self.listen_key:
            try:
                # 每12小时续期一次（比24小时提前一些）
                await asyncio.sleep(12 * 3600)

                if self._running and self.listen_key:
                    response = self.client.renew_listen_key(self.listen_key)
                    if response and 'listenKey' in response:
                        logger.info(f"[{self.exchange_code}] listen key续期成功")
                    else:
                        logger.warning(f"[{self.exchange_code}] listen key续期失败，尝试重新获取")
                        await self._refresh_listen_key()
            except Exception as e:
                logger.error(f"[{self.exchange_code}] 续期listen key异常: {e}")
                await self._refresh_listen_key()

    async def _refresh_listen_key(self):
        """
        刷新listen key（重新获取）
        """
        try:
            # 关闭旧的listen key
            if self.listen_key:
                try:
                    self.client.close_listen_key(self.listen_key)
                except:
                    pass

            # 获取新的listen key
            new_listen_key = await self._get_listen_key()
            if new_listen_key:
                self.listen_key = new_listen_key
                logger.info(f"[{self.exchange_code}] listen key刷新成功")

                # 重启WebSocket连接
                if self._running:
                    await self._restart_websocket()
            else:
                logger.error(f"[{self.exchange_code}] listen key刷新失败")
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 刷新listen key异常: {e}")

    async def _restart_websocket(self):
        """
        重启WebSocket连接
        """
        logger.info(f"[{self.exchange_code}] 重启WebSocket连接")
        # 这里会触发重连机制
        if self._ws_connection:
            try:
                await self._ws_connection.close()
            except:
                pass
            self._ws_connection = None

    async def _handle_order_update(self, data):
        """
            {
                "s":"BTCUSDT",              // Symbol
                "c":"TEST",                 // Client Order Id
                  // special client order id:
                  // starts with "autoclose-": liquidation order
                  // "adl_autoclose": ADL auto close order
                  // "settlement_autoclose-": settlement order for delisting or delivery
                "S":"SELL",                 // Side
                "o":"MARKET", // Order Type
                "f":"GTC",                  // Time in Force
                "q":"0.001",                // Original Quantity
                "p":"0",                    // Original Price
                "ap":"0",                   // Average Price
                "sp":"7103.04",					    // Ignore
                "x":"NEW",                  // Execution Type
                "X":"NEW",                  // Order Status
                "i":8886774,                // Order Id
                "l":"0",                    // Order Last Filled Quantity
                "z":"0",                    // Order Filled Accumulated Quantity
                "L":"0",                    // Last Filled Price
                "N":"USDT",             // Commission Asset, will not push if no commission
                "n":"0",                // Commission, will not push if no commission
                "T":1568879465650,          // Order Trade Time
                "t":0,                      // Trade Id
                "b":"0",                    // Bids Notional
                "a":"9.91",                 // Ask Notional
                "m":false,                  // Is this trade the maker side?
                "R":false,                  // Is this reduce only
                "ps":"LONG",                // Position Side
                "rp":"0",                   // Realized Profit of the trade
                "st":"C_TAKE_PROFIT",       // Strategy type, only pushed with conditional order triggered
                "si":12893,                  // StrategyId,only pushed with conditional order triggered
                "V":"EXPIRE_TAKER",         // STP mode
                "gtd":0
              }
        """
        try:
            # 转换订单类型
            order_type_str = data.get('o', 'MARKET')
            # 转换订单状态
            status_str = data.get('X', 'NEW')
            # 创建订单事件
            event = OrderUpdateEvent(
                exchange_code=self.exchange_code,
                symbol=data.get('s', ''),
                client_order_id=data.get('c', ''),
                order_id=data.get('i', ''),
                trade_id=data.get('t', ''),
                side=data.get('S', ''),
                order_type=order_type_str,
                original_quantity=float(data.get('q', 0)),
                price=float(data.get('p', 0)),
                avg_price=float(data.get('ap', 0)),
                order_status=status_str,
                order_last_filled_quantity=float(data.get('l', 0)),
                order_filled_accumulated_quantity=float(data.get('z', 0)),
                last_filled_price=float(data.get('L', 0)),
                reduce_only=data.get('R', False),
                position_side_mode=data.get('ps', ''),
                timestamp=data.get('T', 0)
            )
            await self.on_order_update_callback(event)
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理订单更新数据异常: {e}, data: {data}")
            return None

    async def _handle_account_update(self, data: Dict[str, Any]):
        """
        处理账户更新消息

        Args:
            data: ACCOUNT_UPDATE消息数据
        """
        await self.on_account_callback(data)

    async def _handle_websocket_message(self, message: str):
        """
        处理WebSocket消息

        Args:
            message: WebSocket消息字符串
        """
        try:
            data = json.loads(message)
            event_type = data.get('e')

            if event_type == 'ACCOUNT_UPDATE':
                # 账户更新消息，包含仓位信息
                logger.debug(f"[{self.exchange_code}] 收到账户更新消息")
                await self._handle_account_update(data.get('a', {}))
            elif event_type == 'ORDER_TRADE_UPDATE':
                # 订单更新消息，暂时不处理
                logger.debug(f"[{self.exchange_code}] 收到Order更新消息")
                await self._handle_order_update(data.get('o', {}))
            elif event_type == 'MARGIN_CALL':
                # 保证金催缴消息
                logger.warning(f"[{self.exchange_code}] 收到保证金催缴: {data}")
            else:
                # 其他消息类型
                logger.debug(f"[{self.exchange_code}] 收到其他类型消息: {event_type}")

        except json.JSONDecodeError as e:
            logger.error(f"[{self.exchange_code}] 解析WebSocket消息失败: {e}")
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理WebSocket消息异常: {e}")

    async def _run_websocket_async(self):
        """
        异步运行WebSocket（支持自动重连）
        """
        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）

        while self._running:
            try:
                if retry_count == 0:
                    logger.debug(f"[{self.exchange_code}] 启动用户数据WebSocket")
                else:
                    logger.debug(f"[{self.exchange_code}] 重连用户数据WebSocket (第{retry_count}次)")

                # 确保有listen key
                if not self.listen_key:
                    new_listen_key = await self._get_listen_key()
                    if not new_listen_key:
                        logger.error(f"[{self.exchange_code}] 获取listen key失败")
                        raise Exception("获取listen key失败")
                    self.listen_key = new_listen_key

                # 创建WebSocket连接
                ws_url = f"{self.base_ws_url}/{self.listen_key}"
                async with websockets.client.connect(
                    ws_url,
                    close_timeout=10,
                    ping_interval=20,
                    ping_timeout=10
                ) as ws:
                    self._ws_connection = ws
                    logger.debug(f"[{self.exchange_code}] WebSocket连接已建立: {ws_url}")

                    # 重连成功，重置计数器
                    if retry_count > 0:
                        logger.debug(f"[{self.exchange_code}] 重连成功！")
                    retry_count = 0

                    # 持续接收消息
                    while self._running:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=30)
                            await self._handle_websocket_message(message)

                        except asyncio.TimeoutError:
                            # 超时，发送ping（websockets库会自动处理ping）
                            logger.debug(f"[{self.exchange_code}] WebSocket接收超时")
                            continue
                        except Exception as e:
                            if self._running:
                                logger.warning(f"[{self.exchange_code}] WebSocket接收消息异常: {e}")
                            break

            except Exception as e:
                if not self._running:
                    logger.debug(f"[{self.exchange_code}] WebSocket已主动停止")
                    return

                # 连接失败
                logger.warning(f"[{self.exchange_code}] WebSocket连接异常: {e}")
                retry_count += 1

                # 清理连接
                self._ws_connection = None

                # 等待后重连
                if self._running:
                    logger.debug(f"[{self.exchange_code}] {retry_delay}秒后重连...")
                    await asyncio.sleep(retry_delay)

        logger.debug(f"[{self.exchange_code}] WebSocket任务退出")

    def _run_websocket_blocking(self):
        """
        在独立线程中运行WebSocket（阻塞调用，支持自动重连）
        """
        try:
            # 获取或创建事件循环
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # 如果没有事件循环，创建一个新的
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # 在事件循环中运行异步WebSocket任务
            loop.run_until_complete(self._run_websocket_async())
        except Exception as e:
            logger.error(f"[{self.exchange_code}] WebSocket线程异常: {e}")
        finally:
            logger.debug(f"[{self.exchange_code}] WebSocket线程退出")

    async def start(self):
        """启动WebSocket连接"""
        if self._running:
            logger.warning(f"[{self.exchange_code}] 仓位WebSocket已在运行")
            return

        if not all([self.api_key, self.secret]):
            logger.error(f"[{self.exchange_code}] 缺少API密钥，无法启动仓位监听")
            return

        try:
            self._running = True

            # 获取listen key
            self.listen_key = await self._get_listen_key()
            if not self.listen_key:
                logger.error(f"[{self.exchange_code}] 获取listen key失败，无法启动WebSocket")
                self._running = False
                return

            # 获取初始仓位
            await self._load_initial_positions()

            # 在独立线程中运行WebSocket
            self._ws_thread = threading.Thread(
                target=self._run_websocket_blocking,
                daemon=True,
                name=f"{self.exchange_code}PositionWebSocketThread"
            )
            self._ws_thread.start()

            # 启动listen key续期任务
            self._renew_task = asyncio.create_task(self._renew_listen_key())

            # 等待连接建立
            await asyncio.sleep(2)

            logger.success(f"[{self.exchange_code}] 仓位WebSocket已启动")

        except Exception as e:
            self._running = False
            logger.error(f"[{self.exchange_code}] 启动失败: {e}")
            raise

    async def _load_initial_positions(self):
        """
        加载初始仓位数据
        """
        try:
            positions = self.client.get_position_risk()
            self._last_positions = positions
            logger.info(f"[{self.exchange_code}] 初始化 {len(self._last_positions)} 个仓位")

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 加载初始仓位失败: {e}")

    async def stop(self):
        """停止WebSocket连接"""
        logger.debug(f"[{self.exchange_code}] 正在停止仓位WebSocket连接...")
        self._running = False

        # 取消续期任务
        if self._renew_task:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass

        # 关闭WebSocket连接
        if self._ws_connection:
            try:
                await self._ws_connection.close()
            except:
                pass
            self._ws_connection = None

        # 等待线程结束
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3.0)
            if self._ws_thread.is_alive():
                logger.warning(f"[{self.exchange_code}] WebSocket线程未能及时结束")
            else:
                logger.debug(f"[{self.exchange_code}] WebSocket线程已结束")

        # 关闭listen key
        if self.listen_key:
            try:
                self.client.close_listen_key(self.listen_key)
                logger.debug(f"[{self.exchange_code}] listen key已关闭")
            except:
                pass
            self.listen_key = None

        logger.success(f"[{self.exchange_code}] 仓位WebSocket已停止")
