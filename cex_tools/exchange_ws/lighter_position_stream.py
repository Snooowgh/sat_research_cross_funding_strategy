# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : lighter_position_stream
@Description : Lighter仓位WebSocket流实现
@Time        : 2025/10/15
"""
import asyncio
import json
import threading
import lighter
import time
from loguru import logger
from typing import Optional, Dict, List

from cex_tools.exchange_ws.position_stream import PositionWebSocketStream
from cex_tools.exchange_model.position_model import LighterPositionDetail
from cex_tools.exchange_model.position_event_model import PositionEventType


class LighterPositionWebSocket(PositionWebSocketStream):
    """Lighter仓位WebSocket流实现"""

    def __init__(self, api_key: str = None, secret: str = None, account_id=None, **kwargs):
        """
        初始化Lighter仓位WebSocket流

        Args:
            api_key: API密钥
            secret: API密钥
            **kwargs: 其他配置参数
        """
        super().__init__("Lighter", kwargs.get('on_position_callback'))
        self.api_key = api_key
        self.secret = secret
        # 账户相关
        self.account_id: Optional[int] = account_id

        # Lighter WebSocket客户端
        self.ws_client: Optional[lighter.WsClient] = None
        self._ws_thread: Optional[threading.Thread] = None


    def _on_account_update_ws(self, account_id, account_data: dict):
        """
        WebSocket账户更新回调

        Args:
            account_id:
            account_data: 账户数据
        """
        try:
            # Lighter账户数据格式可能包含仓位信息
            positions = account_data.get("positions", [])
            if positions:
                logger.debug(f"[{self.exchange_code}] {account_id} 收到 {len(positions)} 个仓位更新")
                self._on_positions_update(positions)

        except Exception as e:
            logger.error(f"[{self.exchange_code}] 处理账户更新异常: {e}")

    def _convert_lighter_position(self, position_data: dict) -> LighterPositionDetail:
        """
        转换Lighter仓位数据格式

        Args:
            position_data: Lighter原始仓位数据

        Returns:
            LighterPositionDetail: 标准化的仓位详情
        """
        try:
            # 确保exchange_code字段
            if isinstance(position_data, dict):
                position_data['exchange_code'] = self.exchange_code
            return LighterPositionDetail(position_data, exchange_code=self.exchange_code)
        except Exception as e:
            logger.error(f"[{self.exchange_code}] 转换仓位数据异常: {e}")
            # 返回一个空的仓位对象
            empty_position = LighterPositionDetail({})
            empty_position.exchange_code = self.exchange_code
            return empty_position

    def _run_ws_blocking(self):
        """在独立线程中运行WebSocket（阻塞调用，支持自动重连）"""
        max_retries = 999  # 几乎无限重连
        retry_count = 0
        retry_delay = 3  # 重连延迟（秒）

        while self._running:
            try:
                if retry_count == 0:
                    logger.debug(f"[{self.exchange_code}] 启动用户数据WebSocket")
                else:
                    logger.debug(f"[{self.exchange_code}] 重连用户数据WebSocket (第{retry_count}次)")

                # 创建 WebSocket 客户端
                self.ws_client = lighter.WsClient(
                    order_book_ids=[],  # 不订阅订单簿
                    account_ids=[self.account_id] if self.account_id else [],  # 订阅指定账户
                    on_order_book_update=None,  # 不处理订单簿
                    on_account_update=self._on_account_update_ws,  # 处理账户更新
                )

                # 创建 WebSocket 连接
                from websockets.sync.client import connect
                ws = connect(
                    self.ws_client.base_url,
                    close_timeout=10,
                    open_timeout=15
                )
                self.ws_client.ws = ws

                def handle_unhandled_message(msg):
                    try:
                        if msg.get("type") == "ping":
                            pong_msg = json.dumps({"type": "pong"})
                            self.ws_client.ws.send(pong_msg)
                        elif msg.get("type") == "auth":
                            # 处理认证响应
                            if msg.get("success"):
                                logger.info(f"[{self.exchange_code}] 认证成功")
                            else:
                                logger.error(f"[{self.exchange_code}] 认证失败: {msg}")
                    except Exception as e:
                        logger.warning(f"[{self.exchange_code}] 处理未处理消息异常: {e}")

                self.ws_client.handle_unhandled_message = handle_unhandled_message
                logger.debug(f"[{self.exchange_code}] WebSocket连接已建立，开始接收消息...")

                # 重连成功，重置计数器
                if retry_count > 0:
                    logger.debug(f"[{self.exchange_code}] 重连成功！")
                retry_count = 0

                # 手动处理消息循环
                for message in ws:
                    if not self._running:
                        logger.debug(f"[{self.exchange_code}] 收到停止信号，退出消息循环")
                        return
                    self.ws_client.on_message(ws, message)

            except Exception as e:
                if not self._running:
                    logger.debug(f"[{self.exchange_code}] WebSocket已主动停止")
                    return

                # 所有异常都尝试重连
                if "no pong" in str(e) or "ConnectionClosed" in str(type(e).__name__):
                    logger.warning(f"[{self.exchange_code}] WebSocket连接断开: {e}")
                else:
                    logger.error(f"[{self.exchange_code}] WebSocket运行异常: {e}")

                retry_count += 1

                # 关闭当前连接
                if hasattr(self, 'ws_client') and self.ws_client and self.ws_client.ws:
                    try:
                        self.ws_client.ws.close()
                    except:
                        pass

                # 等待后重连
                if self._running:
                    logger.debug(f"[{self.exchange_code}] {retry_delay}秒后重连...")
                    time.sleep(retry_delay)

        self._running = False
        logger.debug(f"[{self.exchange_code}] WebSocket线程退出")

    async def start(self):
        """启动 WebSocket 连接"""
        if self._running:
            logger.warning(f"[{self.exchange_code}] 仓位WebSocket 已在运行")
            return

        if not all([self.api_key, self.secret]):
            logger.error(f"[{self.exchange_code}] 缺少必要的API凭据，无法启动用户数据流")
            return

        self._running = True

        # 在独立线程中运行 WebSocket
        self._ws_thread = threading.Thread(
            target=self._run_ws_blocking,
            daemon=True,
            name=f"{self.exchange_code}PositionWebSocketThread"
        )
        self._ws_thread.start()

        # 等待连接建立
        await asyncio.sleep(2)
        logger.debug(f"[{self.exchange_code}] 仓位WebSocket 已启动")

    async def stop(self):
        """停止 WebSocket 连接"""
        logger.debug(f"[{self.exchange_code}] 正在停止仓位WebSocket连接...")
        self._running = False

        # 尝试关闭 WebSocket 客户端
        if self.ws_client and self.ws_client.ws:
            try:
                if hasattr(self.ws_client.ws, 'close'):
                    # 同步版本的 websocket
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self.ws_client.ws.close)
                    logger.debug(f"[{self.exchange_code}] WebSocket连接已关闭")
            except Exception as e:
                logger.warning(f"[{self.exchange_code}] 关闭WebSocket时出现警告: {e}")

        # 等待线程结束（最多等待3秒）
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3.0)
            if self._ws_thread.is_alive():
                logger.warning(f"[{self.exchange_code}] WebSocket线程未能及时结束")
            else:
                logger.debug(f"[{self.exchange_code}] WebSocket线程已结束")

        logger.debug(f"[{self.exchange_code}] 仓位WebSocket 已停止")