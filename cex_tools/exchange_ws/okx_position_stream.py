# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : okx_position_stream
@Description : OKX仓位WebSocket流实现
@Time        : 2025/10/15
"""
import asyncio
import json
import time
import websockets
from loguru import logger
from typing import Optional
import hashlib
import hmac
import base64

from cex_tools.cex_enum import ExchangeEnum
from cex_tools.exchange_model.order_update_event_model import OrderUpdateEvent
from cex_tools.exchange_ws.position_stream import PositionWebSocketStream
from cex_tools.exchange_model.position_model import OkxPositionDetail
from okx.app import OkxSWAP


class OkxPositionWebSocket(PositionWebSocketStream):
    """OKX仓位WebSocket流实现"""

    def __init__(self, api_key: str = None, secret: str = None, passphrase: str = None,
                 sandbox: bool = False, **kwargs):
        """
        初始化OKX仓位WebSocket流

        Args:
            api_key: API密钥
            secret: API密钥
            passphrase: API passphrase
            sandbox: 是否使用沙盒环境
            **kwargs: 其他配置参数
        """
        super().__init__(ExchangeEnum.OKX, **kwargs)
        self.api_key = api_key
        self.secret = secret
        self.passphrase = passphrase
        self.sandbox = sandbox

        # OKX WebSocket地址
        self.ws_url = (
            "wss://wspap.okx.com:8443/ws/v5/public" if sandbox
            else "wss://ws.okx.com:8443/ws/v5/public"
        )

        # 私有数据需要使用不同的端点
        self.private_ws_url = (
            "wss://wspap.okx.com:8443/ws/v5/private" if sandbox
            else "wss://ws.okx.com:8443/ws/v5/private"
        )

        self._ws_connection = None
        self._listen_task: Optional[asyncio.Task] = None
        self._login_sent = False

        # 合约信息缓存 (合约面值)
        self._contract_value_cache: dict = {}  # {symbol: ctVal}

        # 初始化OKX客户端用于获取合约信息
        try:
            proxy_host = "https://www.okx.com/"
            self.okx_client = OkxSWAP(
                key="",
                secret="",
                passphrase="",
                proxies={},
                proxy_host=proxy_host,
            )
            # 初始化交易所信息
            self.okx_client.market.get_exchangeInfos(uly="", expire_seconds=24 * 3600)
        except Exception as e:
            self.okx_client = None
            logger.error(f"[{self.exchange_code}] 初始化OKX客户端失败: {e}")

    def _generate_signature(self, timestamp: str, method: str = "GET",
                            request_path: str = "/users/self/verify") -> str:
        """
        生成API签名

        Args:
            timestamp: 时间戳
            method: 请求方法
            request_path: 请求路径

        Returns:
            str: base64编码的签名
        """
        try:
            if not self.secret:
                return ""

            message = timestamp + method + request_path
            signature = hmac.new(
                self.secret.encode('utf-8'),
                message.encode('utf-8'),
                digestmod=hashlib.sha256
            ).digest()

            return base64.b64encode(signature).decode('utf-8')
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 生成签名异常: {e}")
            return ""

    async def _send_login_message(self):
        """
        发送登录消息
        """
        try:
            timestamp = str(time.time())
            signature = self._generate_signature(timestamp)

            login_msg = {
                "op": "login",
                "args": [
                    {
                        "apiKey": self.api_key,
                        "passphrase": self.passphrase,
                        "timestamp": timestamp,
                        "sign": signature
                    }
                ]
            }

            await self._ws_connection.send(json.dumps(login_msg))
            logger.debug(f"[{self.exchange_code}] 发送登录消息")
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 发送登录消息失败: {e}")

    async def _send_subscription_message(self):
        """
        发送仓位订阅消息
        """
        try:
            subscribe_msg = {
                "op": "subscribe",
                "args": [{
                        "channel": "account"
                    },
                    {
                        "channel": "positions",
                        "instType": "SWAP"  # 合约类型
                    }, {
                        "channel": "orders",
                        "instType": "SWAP"  # 合约类型
                    }
                ]
            }

            await self._ws_connection.send(json.dumps(subscribe_msg))
            logger.debug(f"[{self.exchange_code}] 发送仓位订阅消息")
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 发送订阅消息失败: {e}")

    async def _handle_message(self, message: dict):
        """
        处理WebSocket消息

        Args:
            message: WebSocket消息数据
        """
        try:
            event = message.get("event")
            if event == "login":
                # 登录响应
                code = message.get("code")
                if code == "0":
                    logger.info(f"[{self.exchange_code}] 登录成功")
                    self._login_sent = True
                    # 登录成功后发送订阅消息
                    await self._send_subscription_message()
                else:
                    logger.error(f"[{self.exchange_code}] 登录失败: {message.get('msg')}")
            elif event == "subscribe":
                # 订阅响应
                channel = message.get("arg", {}).get("channel")
                logger.info(f"[{self.exchange_code}] {channel} 订阅成功")
            elif message.get("arg", {}).get("channel") == "positions":
                # 仓位数据更新
                data = message.get("data", [])
                if data:
                    logger.debug(f"[{self.exchange_code}] 收到 {len(data)} 个仓位更新")
                    await self._handle_positions_update(data)
            elif message.get("arg", {}).get("channel") == "orders":
                # 仓位数据更新
                data = message.get("data", [])
                logger.debug(f"[{self.exchange_code}] 收到{len(data)}个订单更新")
                for d in data:
                    await self._handle_orders_update(d)
            elif message.get("arg", {}).get("channel") == "account":
                # 仓位数据更新
                data = message.get("data", None)
                logger.debug(f"[{self.exchange_code}] 收到账户更新")
                await self._handle_account_update(data)
            elif message.get("op") == "error":
                # 错误消息
                logger.error(f"[{self.exchange_code}] 收到错误消息: {message}")

            else:
                logger.debug(f"[{self.exchange_code}] 未知消息类型: {message}")

        except Exception as e:
            logger.exception(f"[{self.exchange_code}] 处理消息异常: {e}")

    async def _handle_orders_update(self, order_data):
        """
            {'instType': 'SWAP', 'instId': 'ETH-USDT-SWAP', 'tgtCcy': '', 'ccy': 'USDT', 'tradeQuoteCcy': '', 'ordId': '2956222619053072384', 'clOrdId': '', 'algoClOrdId': '', 'algoId': '', 'tag': '', 'px': '3900', 'sz': '0.01', 'notionalUsd': '3.9009750000000007', 'ordType': 'limit', 'side': 'buy', 'posSide': 'net', 'tdMode': 'cross', 'accFillSz': '0', 'fillNotionalUsd': '', 'avgPx': '0', 'state': 'canceled', 'lever': '0', 'pnl': '0', 'feeCcy': 'USDT', 'fee': '0', 'rebateCcy': 'USDT', 'rebate': '0', 'category': 'normal', 'uTime': '1760604892870', 'cTime': '1760604699542', 'source': '', 'reduceOnly': 'false', 'cancelSource': '1', 'quickMgnType': '', 'stpId': '', 'stpMode': 'cancel_taker', 'attachAlgoClOrdId': '', 'lastPx': '3993.13', 'isTpLimit': 'false', 'slTriggerPx': '', 'slTriggerPxType': '', 'tpOrdPx': '', 'tpTriggerPx': '', 'tpTriggerPxType': '', 'slOrdPx': '', 'fillPx': '', 'tradeId': '', 'fillSz': '0', 'fillTime': '', 'fillPnl': '0', 'fillFee': '0', 'fillFeeCcy': '', 'execType': '', 'fillPxVol': '', 'fillPxUsd': '', 'fillMarkVol': '', 'fillFwdPx': '', 'fillMarkPx': '', 'fillIdxPx': '', 'amendSource': '', 'reqId': '', 'amendResult': '', 'code': '0', 'msg': '', 'pxType': '', 'pxUsd': '', 'pxVol': '', 'linkedAlgoOrd': {'algoId': ''}, 'attachAlgoOrds': []}
        """
        logger.info(f"订单更新:{order_data}")

        # 提取交易对符号
        instId = order_data.get('instId', '')
        symbol = instId.replace("-USDT-SWAP", "")

        # 获取合约张数
        original_quantity_contracts = float(order_data.get('sz', 0))
        order_last_filled_quantity_contracts = float(order_data.get('fillSz', 0))
        order_filled_accumulated_quantity_contracts = float(order_data.get('accFillSz', 0))

        # 转换为币种数量
        original_quantity = self._convert_contracts_to_currency_amount(symbol, original_quantity_contracts)
        order_last_filled_quantity = self._convert_contracts_to_currency_amount(symbol, order_last_filled_quantity_contracts)
        order_filled_accumulated_quantity = self._convert_contracts_to_currency_amount(symbol, order_filled_accumulated_quantity_contracts)

        event = OrderUpdateEvent(
            exchange_code=self.exchange_code,
            symbol=symbol,
            client_order_id=order_data.get('clOrdId', ''),
            order_id=order_data.get('ordId', ''),
            side=order_data.get('side', '').upper(),
            order_type=order_data.get('ordType', '').upper(),
            original_quantity=original_quantity,
            price=float(order_data.get('px', 0)),
            avg_price=float(order_data.get('avgPx', 0)),
            order_status=order_data.get('state', '').upper(),
            order_last_filled_quantity=order_last_filled_quantity,
            order_filled_accumulated_quantity=order_filled_accumulated_quantity,
            last_filled_price=float(order_data.get('fillPx') if order_data.get('fillPx') else 0),
            reduce_only=order_data.get('reduceOnly', 'false') == 'true',
            position_side_mode=order_data.get('posSide', ''),
            timestamp=int(order_data.get('uTime', 0))
        )
        self.on_order_update_callback(event)

    async def _handle_account_update(self, account_data):
        # logger.info(f"账户数据更新:{account_data}")
        self.on_account_callback(account_data)

    async def _handle_positions_update(self, positions_data: list):
        """
        处理仓位更新

        Args:
            positions_data: 仓位数据列表

        OKX仓位数据格式:
        [
            {
                "instId": "BTC-USDT-SWAP",
                "instType": "SWAP",
                "mgnMode": "cross",
                "pos": "0.1",
                "baseBal": "0",
                "quoteBal": "0",
                "posCcy": "BTC",
                "availPos": "0.1",
                "avgPx": "50000",
                "upl": "100",
                "uplRatio": "0.002",
                "instType": "SWAP",
                "lever": "10",
                "liqPx": "45000",
                "markPx": "50100",
                "imr": "500",
                "margin": "500",
                "mgnMode": "cross",
                "ccy": "",
                "notionalUsd": "5000",
                "adl": "1",
                "cTime": "1234567890000",
                "uTime": "1234567890000"
            }
        ]
        """
        try:
            for position_data in positions_data:
                # 过滤掉空仓位
                if float(position_data.get("pos", 0)) == 0:
                    continue

                # 转换为标准格式
                position_detail = self._convert_okx_position(position_data)
                self.on_position_callback(position_detail)

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理仓位更新异常: {e}")

    def _convert_okx_position(self, position_data: dict) -> OkxPositionDetail:
        """
        转换OKX仓位数据格式

        Args:
            position_data: OKX原始仓位数据

        Returns:
            OkxPositionDetail: 标准化的仓位详情
        """
        try:
            # 确保exchange_code字段
            position_data['exchange_code'] = self.exchange_code
            return OkxPositionDetail(position_data, exchange_code=self.exchange_code)
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 转换仓位数据异常: {e}")
            # 返回一个空的仓位对象
            empty_position = OkxPositionDetail({})
            empty_position.exchange_code = self.exchange_code
            return empty_position

    async def _listen_websocket(self):
        """持续监听 WebSocket 消息（支持自动重连）"""
        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）

        while self._running:
            try:
                if retry_count == 0:
                    logger.debug(f"[{self.exchange_code}] 连接到私有数据流: {self.private_ws_url}")
                else:
                    logger.debug(f"[{self.exchange_code}] 重连私有数据流 (第{retry_count}次)")

                async with websockets.connect(
                        self.private_ws_url,
                        ping_interval=20,
                        ping_timeout=5
                ) as websocket:
                    self._ws_connection = websocket
                    logger.debug(f"[{self.exchange_code}] WebSocket 连接成功")

                    # 发送登录消息
                    await self._send_login_message()

                    # 重连成功，重置计数器
                    if retry_count > 0:
                        logger.debug(f"[{self.exchange_code}] 重连成功！")
                    retry_count = 0
                    self._login_sent = False

                    # 持续接收消息
                    while self._running:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=10)
                            data = json.loads(message)
                            await self._handle_message(data)

                        except asyncio.TimeoutError:
                            logger.debug(f"[{self.exchange_code}] WebSocket 超时，发送 ping...")
                            await websocket.ping()
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"[{self.exchange_code}] WebSocket 连接关闭，准备重连...")
                            break
                        except Exception as e:
                            logger.error(f"[{self.exchange_code}] 处理消息异常: {e}")
                            break

            except Exception as e:
                if not self._running:
                    logger.debug(f"[{self.exchange_code}] WebSocket已主动停止")
                    return

                # 连接失败，重置登录状态
                self._login_sent = False

                if "no pong" in str(e) or "ConnectionClosed" in str(type(e).__name__):
                    logger.warning(f"[{self.exchange_code}] WebSocket连接断开: {e}")
                else:
                    logger.error(f"[{self.exchange_code}] WebSocket 连接异常: {e}")

                retry_count += 1

                # 等待后重连
                if self._running:
                    logger.debug(f"[{self.exchange_code}] {retry_delay}秒后重连...")
                    await asyncio.sleep(retry_delay)

            finally:
                self._ws_connection = None
                self._login_sent = False

        logger.debug(f"[{self.exchange_code}] WebSocket监听线程退出")

    async def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            logger.warning(f"[{self.exchange_code}] 仓位WebSocket 已在运行")
            return

        if not all([self.api_key, self.secret, self.passphrase]):
            logger.error(f"[{self.exchange_code}] 缺少必要的API凭据，无法启动私有数据流")
            return

        self._running = True

        # 启动监听任务
        self._listen_task = asyncio.create_task(self._listen_websocket())

        logger.debug(f"[{self.exchange_code}] 仓位WebSocket 已启动")

    async def stop(self):
        """停止 WebSocket 连接"""
        self._running = False

        # 关闭 WebSocket 连接
        if self._ws_connection:
            await self._ws_connection.close()

        # 取消监听任务
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        logger.debug(f"[{self.exchange_code}] WebSocket 已停止")

    def _convert_symbol_to_okx_format(self, symbol: str) -> str:
        """
        将标准交易对符号转换为OKX格式

        Args:
            symbol: 标准符号 (如 "BTCUSDT")

        Returns:
            str: OKX格式符号 (如 "BTC-USDT-SWAP")
        """
        if symbol.endswith("-USDT-SWAP"):
            return symbol
        elif symbol.endswith("USDT"):
            return symbol.replace("USDT", "-USDT-SWAP")
        else:
            return symbol + "-USDT-SWAP"

    def _get_contract_value(self, symbol: str) -> float:
        """
        获取合约面值 (ctVal)，用于将合约张数转换为币种数量

        Args:
            symbol: 交易对符号

        Returns:
            float: 合约面值
        """
        # 首先检查缓存
        if symbol in self._contract_value_cache:
            return self._contract_value_cache[symbol]

        # 如果缓存中没有，尝试从API获取
        try:
            if self.okx_client:
                okx_symbol = self._convert_symbol_to_okx_format(symbol)
                result = self.okx_client.market.get_exchangeInfo(instId=okx_symbol)
                if result and result.get("data"):
                    ct_val = float(result["data"]["ctVal"])
                    # 缓存结果
                    self._contract_value_cache[symbol] = ct_val
                    logger.debug(f"[{self.exchange_code}] 获取 {symbol} 合约面值: {ct_val}")
                    return ct_val
                else:
                    logger.warning(f"[{self.exchange_code}] 无法获取 {symbol} 合约信息: {result}")
            else:
                logger.warning(f"[{self.exchange_code}] OKX客户端未初始化，无法获取合约面值")
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 获取 {symbol} 合约面值失败: {e}")

        # 默认返回1，避免转换失败
        default_ct_val = 1.0
        self._contract_value_cache[symbol] = default_ct_val
        logger.warning(f"[{self.exchange_code}] 使用默认合约面值 {default_ct_val} for {symbol}")
        return default_ct_val

    def _convert_contracts_to_currency_amount(self, symbol: str, contract_amount: float) -> float:
        """
        将合约张数转换为币种数量

        Args:
            symbol: 交易对符号
            contract_amount: 合约张数

        Returns:
            float: 币种数量
        """
        if contract_amount == 0:
            return 0

        ct_val = self._get_contract_value(symbol)
        currency_amount = contract_amount * ct_val
        logger.debug(f"[{self.exchange_code}] 转换合约张数: {symbol} {contract_amount}张 -> {currency_amount}币 (ctVal={ct_val})")
        return currency_amount
