# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : simple_arbitrage_manager_fixed
@Description : 多进程套利管理器 - 负责启动引擎和风控缓存分发（多进程版本）
@Time        : 2025/10/9
"""
import time
import multiprocessing as mp
from typing import Dict, List, Optional
from dataclasses import dataclass
import signal
from arbitrage_param import MultiExchangeArbitrageParam, ArbitrageWhiteListParam
from multi_exchange_info_show import get_multi_exchange_info_combined_model
from cex_tools.exchange_model.multi_exchange_info_model import MultiExchangeCombinedInfoModel
from utils.notify_tools import async_notify_telegram, CHANNEL_TYPE
from cex_tools.exchange_ws.stream_factory import StreamFactory
import asyncio
from loguru import logger
from logic.realtime_hedge_engine import RealtimeHedgeEngine, TradeConfig, RiskConfig


@dataclass
class EngineConfig:
    """引擎配置"""
    symbol: str
    exchange1_code: str
    exchange2_code: str
    daemon_mode: bool = True


@dataclass
class EngineHealthMetrics:
    """引擎健康指标"""
    process_id: int
    start_time: float
    restart_count: int = 0
    last_error: Optional[str] = None
    consecutive_failures: int = 0
    is_healthy: bool = True
    last_trade_time: Optional[float] = None
    memory_usage_mb: float = 0.0

@dataclass
class ManagerConfig:
    """管理器配置"""
    # 更新间隔
    risk_update_interval_min: int = 2  # 风控数据更新间隔(分钟)
    engine_check_interval_min: int = 15  # 引擎状态检查间隔(分钟)

    # 通知配置
    enable_notifications: bool = True
    notify_interval_min: int = 30  # 通知间隔(分钟)

    # 启动配置
    engine_startup_delay_sec: float = 5.0  # 引擎启动间隔(秒)，避免API请求过多

    # 健康管理配置
    max_restart_attempts: int = 3  # 最大重启尝试次数
    restart_backoff_factor: float = 2.0  # 重启退避因子
    memory_limit_mb: float = 1000.0  # 内存限制(MB)
    no_trade_timeout_min: int = 30  # 无交易超时时间(分钟)


async def create_stream_for_exchange(exchange_code: str, symbol: str):
    """为指定交易所创建WebSocket流 - 使用现有工厂类"""
    try:
        # 转换symbol格式：BTC -> BTCUSDT
        if not symbol.endswith('USDT'):
            full_symbol = f"{symbol}USDT"
        else:
            full_symbol = symbol

        stream = await StreamFactory.create_orderbook_stream(exchange_code, full_symbol)
        return stream

    except Exception as e:
        logger.error(f"❌ 创建 {exchange_code} {symbol} WebSocket流失败: {e}")
        return None

def run_real_engine_in_process(engine_config: EngineConfig,
                               risk_data_dict: Dict, stop_event):
    """
    在独立进程中运行真正的交易引擎

    Args:
        engine_config: 引擎配置
        risk_data_dict: 共享的风控数据字典
        stop_event: 停止事件
    """
    async def engine_main():
        """真正的交易引擎主程序"""
        engine = None
        try:
            # 设置进程日志
            logger.add(
                f"logs/engine_{engine_config.symbol}_{engine_config.exchange1_code}_{engine_config.exchange2_code}.log",
                rotation="1 day",
                retention="7 days",
                level="INFO",
                format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
            )

            logger.info(f"🚀 启动 {engine_config.symbol}({engine_config.exchange1_code}-{engine_config.exchange2_code}) 交易引擎 ")

            # 初始化交易所参数
            arbitrage_param = MultiExchangeArbitrageParam(auto_init=True)
            await arbitrage_param.init_async_exchanges()
            await asyncio.sleep(0.3)  # 确保交易所初始化完成
            # 获取交易所实例
            exchange1 = arbitrage_param.async_exchanges[engine_config.exchange1_code]
            exchange2 = arbitrage_param.async_exchanges[engine_config.exchange2_code]

            # 创建交易配置
            trade_config = TradeConfig(
                pair1=f"{engine_config.symbol}USDT",
                pair2=f"{engine_config.symbol}USDT",
                side1="BUY",  # daemon模式下由引擎自动决定
                side2="SELL",
                daemon_mode=True,
                no_trade_timeout_sec=0  # 持续运行
            )

            # 创建风控配置
            risk_config = RiskConfig()

            # 创建WebSocket流
            stream1 = await create_stream_for_exchange(engine_config.exchange1_code, engine_config.symbol)
            stream2 = await create_stream_for_exchange(engine_config.exchange2_code, engine_config.symbol)

            if stream1 is None or stream2 is None:
                logger.error(f"❌ {engine_config.symbol} WebSocket流创建失败，无法启动交易引擎")
                raise RuntimeError("WebSocket流创建失败，无法启动交易引擎")

            engine = RealtimeHedgeEngine(
                stream1=stream1,
                stream2=stream2,
                exchange1=exchange1,
                exchange2=exchange2,
                trade_config=trade_config,
                risk_config=risk_config,
                exchange_combined_info_cache=risk_data_dict
            )

            # 启动引擎
            await engine.start()

            # 运行直到收到停止信号
            while not stop_event.is_set():
                # 使用更短的睡眠以便更快响应停止信号
                try:
                    await asyncio.wait_for(asyncio.sleep(0.5), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                # 定期更新风控数据
                if 'risk_data' in risk_data_dict:
                    engine.exchange_combined_info_cache = risk_data_dict['risk_data']

            logger.info(f"🛑 {engine_config.symbol} 引擎收到停止信号，正在停止...")
            # 停止引擎（添加超时控制）
            try:
                await asyncio.wait_for(engine.stop(), timeout=3.0)
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ {engine_config.symbol} 引擎停止超时，强制继续")

            logger.info(f"🛑 {engine_config.symbol} 引擎收到停止信号，正在关闭...")

        except Exception as e:
            logger.error(f"❌ {engine_config.symbol} 引擎进程异常: {e}")
            import traceback
            traceback.print_exc()
        finally:
            try:
                if engine is not None:
                    await engine.stop()
                logger.info(f"✅ {engine_config.symbol} 引擎已关闭")
            except Exception as e:
                logger.error(f"❌ 关闭 {engine_config.symbol} 引擎失败: {e}")

    # 运行引擎主程序
    try:
        asyncio.run(engine_main())
    except KeyboardInterrupt:
        logger.info(f"👋 {engine_config.symbol} 引擎收到中断信号")
    except Exception as e:
        logger.error(f"❌ {engine_config.symbol} 引擎进程崩溃: {e}")
        import traceback
        traceback.print_exc()


class MultiProcessArbitrageManager:
    """多进程套利管理器 - 专注于引擎管理和风控数据分发"""

    def __init__(self, config: ManagerConfig = None):
        self.config = config or ManagerConfig()
        self.arbitrage_param: Optional[MultiExchangeArbitrageParam] = None
        self.is_running = False
        self.shutdown_event = asyncio.Event()

        # 多进程管理
        self.process_manager = mp.Manager()
        self.shared_risk_data = self.process_manager.dict()
        self.stop_events: Dict[str, mp.Event] = {}
        self.engine_processes: Dict[str, mp.Process] = {}

        # 引擎配置存储
        self.engine_configs: Dict[str, EngineConfig] = {}

        # 健康监控
        self.engine_health: Dict[str, EngineHealthMetrics] = {}

        # 风控数据缓存
        self.cached_risk_data: Optional[MultiExchangeCombinedInfoModel] = None
        self.last_risk_update_time = 0
        self.last_notify_time = 0

        # 统计信息
        self.stats = {
            'start_time': time.time(),
            'total_engines_started': 0,
            'total_engine_restarts': 0
        }

    async def initialize(self):
        """初始化管理器"""
        # 初始化交易所参数
        self.arbitrage_param = MultiExchangeArbitrageParam(auto_init=True)
        await self.arbitrage_param.init_async_exchanges()

        if len(self.arbitrage_param.async_exchanges) < 2:
            raise ValueError("需要至少2个可用交易所才能运行套利系统")
        # 初始化风控数据 - 必须在启动任何进程之前完成
        await self._initialize_risk_data()

    async def _initialize_risk_data(self):
        """初始化风控数据并分发到共享字典"""
        try:
            # 获取风控数据
            self.cached_risk_data = await get_multi_exchange_info_combined_model(
                async_exchange_list=self.arbitrage_param.async_exchange_list,
                find_opportunities=False,  # 管理器不需要寻找机会
                opportunity_limit=0
            )
            self.last_risk_update_time = time.time()

            # 立即分发给共享字典，确保进程启动时就能获取到
            self.shared_risk_data['risk_data'] = self.cached_risk_data
            self.shared_risk_data['update_time'] = time.time()
            logger.success(f"✅ 风控数据:\n {self.cached_risk_data}")

        except Exception as e:
            logger.error(f"❌ 获取交易所风控数据失败: {e}")
            exit()

    async def start_engines_for_whitelist_and_pos(self):
        """为白名单代币和持仓代币启动引擎"""
        if self.cached_risk_data is None:
            raise Exception("风控数据未初始化，无法启动引擎")
        start_engine_symbol_list = list(set(ArbitrageWhiteListParam.SYMBOL_LIST) | set(self.cached_risk_data.holding_symbol_list))
        # start_engine_symbol_list = ArbitrageWhiteListParam.SYMBOL_LIST
        # 顺序启动引擎，避免同时发起过多API请求
        for i, symbol in enumerate(start_engine_symbol_list):
            try:
                # 选择最佳交易所组合
                best_pair = await self._select_optimal_exchange_pair(symbol)
                if not best_pair:
                    logger.warning(f"❌ {symbol} 未找到可用交易所组合")
                    continue

                # 创建引擎配置
                engine_config = EngineConfig(
                    symbol=symbol,
                    exchange1_code=best_pair[0],
                    exchange2_code=best_pair[1],
                    daemon_mode=True
                )

                # 启动引擎进程
                await self._start_engine_process(engine_config)

                # 启动延迟，避免API请求过多（最后一个不需要延迟）
                if i < len(ArbitrageWhiteListParam.SYMBOL_LIST) - 1:
                    delay = self.config.engine_startup_delay_sec
                    logger.info(f"⏱️  等待 {delay}秒...")
                    await asyncio.sleep(delay)

            except Exception as e:
                logger.error(f"❌ 启动 {symbol} 引擎失败: {e}")
                # 即使失败也继续启动下一个引擎
                continue

    async def _select_optimal_exchange_pair(self, symbol: str) -> Optional[tuple]:
        """智能选择最优交易所组合"""
        available_exchanges = list(self.arbitrage_param.async_exchanges.keys())

        if len(available_exchanges) < 2:
            return None

        logger.info(f"🔍 为 {symbol} 分析最优交易所组合，候选交易所: {available_exchanges}")

        # 评分系统
        best_pair = None
        best_score = float('-inf')
        pair_scores = {}

        for i, exc1 in enumerate(available_exchanges):
            for j, exc2 in enumerate(available_exchanges):
                if i >= j:
                    continue  # 避免重复组合和自身组合

                # 计算综合评分
                score = await self._calculate_pair_score(exc1, exc2, symbol)
                pair_scores[f"{exc1}-{exc2}"] = score

                if score > best_score:
                    best_score = score
                    best_pair = (exc1, exc2)

        logger.info(f"📊 交易所组合评分: {pair_scores}")
        logger.success(f"✅ 选择最优组合: {best_pair}, 评分: {best_score:.4f}")

        return best_pair

    async def _calculate_pair_score(self, exc1: str, exc2: str, symbol: str) -> float:
        """计算交易所组合的综合评分"""
        try:
            # 1. 资金费率差异评分 (权重 40%)
            exchange1 = self.arbitrage_param.async_exchanges[exc1]
            exchange2 = self.arbitrage_param.async_exchanges[exc2]

            # 使用异步方法获取资金费率
            funding_rate1 = await exchange1.get_funding_rate(symbol)
            funding_rate2 = await exchange2.get_funding_rate(symbol)
            funding_rate_diff = abs(funding_rate1 - funding_rate2)
            funding_score = min(funding_rate_diff * 10000, 10.0) * 0.4  # 标准化到0-10分

            # 2. 手续费评分 (权重 20%)
            fee1 = getattr(self.arbitrage_param.async_exchanges[exc1], 'taker_fee_rate', 0.001)
            fee2 = getattr(self.arbitrage_param.async_exchanges[exc2], 'taker_fee_rate', 0.001)
            avg_fee = (fee1 + fee2) / 2
            fee_score = max(0, (0.002 - avg_fee) * 1000) * 0.2  # 费率越低分越高

            # 3. 可靠性评分 (权重 25%)
            reliability_scores = {
                'binance': 0.95,
                'hyperliquid': 0.90,
                'lighter': 0.85,
                'aster': 0.80,
                'okx': 0.90,
                'bybit': 0.85
            }
            rel1 = reliability_scores.get(exc1.lower(), 0.70)
            rel2 = reliability_scores.get(exc2.lower(), 0.70)
            reliability_score = (rel1 + rel2) / 2 * 10 * 0.25

            # 4. 流动性评分 (权重 15%)
            liquidity_score = 0.75  # 基础流动性评分

            total_score = funding_score + fee_score + reliability_score + liquidity_score

            logger.debug(f"📈 {exc1}-{exc2} 评分详情: 资金费率{funding_score:.2f} + 手续费{fee_score:.2f} + "
                        f"可靠性{reliability_score:.2f} + 流动性{liquidity_score:.2f} = {total_score:.2f}")

            return total_score

        except Exception as e:
            logger.error(f"计算交易所组合评分失败 {exc1}-{exc2}: {e}")
            return 0.0

    async def _start_engine_process(self, engine_config: EngineConfig):
        """启动单个引擎进程"""
        symbol = engine_config.symbol
        process_key = f"{symbol}_{engine_config.exchange1_code}_{engine_config.exchange2_code}"

        if process_key in self.engine_processes and self.engine_processes[process_key].is_alive():
            logger.info(f"⚠️  {symbol} 引擎进程已存在，跳过启动")
            return

        try:
            # 创建停止事件
            stop_event = self.process_manager.Event()
            self.stop_events[process_key] = stop_event

            # 创建并启动进程
            process = mp.Process(
                target=run_real_engine_in_process,
                args=(engine_config, self.shared_risk_data, stop_event),
                name=f"Engine_{process_key}"
            )

            process.start()
            self.engine_processes[process_key] = process
            self.engine_configs[process_key] = engine_config

            self.stats['total_engines_started'] += 1

            logger.success(f"✅ {symbol} 引擎进程启动成功 (PID: {process.pid}, {engine_config.exchange1_code}-{engine_config.exchange2_code})")

        except Exception as e:
            logger.error(f"❌ 启动 {symbol} 引擎进程失败: {e}")
            # 清理资源
            if process_key in self.stop_events:
                del self.stop_events[process_key]
            raise

    async def _update_risk_data(self):
        """更新风控数据缓存"""
        try:
            self.cached_risk_data = await get_multi_exchange_info_combined_model(
                async_exchange_list=self.arbitrage_param.async_exchange_list,
                find_opportunities=False,  # 管理器不需要寻找机会
                opportunity_limit=0
            )
            logger.debug(f"🔄 风控数据更新(间隔:{time.time()-self.last_risk_update_time:.0f}s):\n{self.cached_risk_data}")
            self.last_risk_update_time = time.time()

            should, msg = self.cached_risk_data.should_notify_risk()
            if should:
                await async_notify_telegram(f"❌❌ {self.arbitrage_param.async_exchanges.keys()}风控提醒:\n{msg}")
            # 分发给所有引擎进程
            self.shared_risk_data['risk_data'] = self.cached_risk_data
            self.shared_risk_data['update_time'] = time.time()
            logger.info(f"✅ 风控数据:\n{self.cached_risk_data}")

        except Exception as e:
            logger.error(f"❌ 更新风控数据失败: {e}")
            exit()


    async def _check_engine_health(self):
        """智能检查引擎进程健康状态"""
        failed_processes = []
        unhealthy_processes = []

        for process_key, process in list(self.engine_processes.items()):
            health_metrics = self.engine_health.get(process_key)

            # 初始化健康指标
            if health_metrics is None:
                self.engine_health[process_key] = EngineHealthMetrics(
                    process_id=process.pid,
                    start_time=time.time()
                )
                health_metrics = self.engine_health[process_key]

            # 1. 检查进程存活性
            if not process.is_alive():
                logger.warning(f"⚠️  {process_key} 引擎进程已停止运行 (退出码: {process.exitcode})")
                health_metrics.consecutive_failures += 1
                health_metrics.is_healthy = False
                failed_processes.append(process_key)
                continue

            # 2. 检查内存使用
            memory_usage = await self._get_process_memory_usage(process.pid)
            health_metrics.memory_usage_mb = memory_usage

            if memory_usage > self.config.memory_limit_mb:
                logger.warning(f"⚠️  {process_key} 内存使用过高: {memory_usage:.1f}MB > {self.config.memory_limit_mb}MB")
                unhealthy_processes.append(process_key)
                health_metrics.is_healthy = False

            # 3. 检查运行时长和重启次数
            if health_metrics.restart_count >= self.config.max_restart_attempts:
                logger.error(f"❌ {process_key} 重启次数已达上限 ({health_metrics.restart_count})")
                unhealthy_processes.append(process_key)
                continue

            # 4. 检查无交易超时
            if health_metrics.last_trade_time:
                time_since_trade = time.time() - health_metrics.last_trade_time
                if time_since_trade > self.config.no_trade_timeout_min * 60:
                    logger.warning(f"⚠️  {process_key} 长时间无交易活动: {time_since_trade/60:.1f}分钟")

            # 5. 更新健康状态
            if process.is_alive() and process_key not in unhealthy_processes:
                health_metrics.is_healthy = True

        # 处理失败进程
        await self._handle_failed_processes(failed_processes)

        # 处理不健康进程
        await self._handle_unhealthy_processes(unhealthy_processes)

    async def _get_process_memory_usage(self, pid: int) -> float:
        """获取进程内存使用量(MB)"""
        try:
            import psutil
            process = psutil.Process(pid)
            memory_bytes = process.memory_info().rss
            return memory_bytes / 1024 / 1024
        except ImportError:
            # psutil未安装，返回估算值
            return 100.0
        except Exception:
            return 0.0

    async def _handle_failed_processes(self, failed_processes: List[str]):
        """处理失败进程的智能重启"""
        for process_key in failed_processes:
            health_metrics = self.engine_health.get(process_key)
            if not health_metrics:
                continue

            # 检查是否应该重启
            if not self._should_restart_engine(health_metrics):
                logger.error(f"❌ {process_key} 不再重启，已达到最大尝试次数")
                continue

            try:
                logger.info(f"🔄 重启 {process_key} 引擎进程 (第{health_metrics.restart_count + 1}次)...")

                # 清理旧进程资源
                await self._cleanup_process(process_key)

                # 等待退避时间
                backoff_time = self.config.restart_backoff_factor ** health_metrics.restart_count
                if backoff_time > 1:
                    logger.info(f"⏱️  等待退避时间: {backoff_time:.1f}分钟")
                    await asyncio.sleep(backoff_time * 60)

                # 重新启动进程
                engine_config = self.engine_configs[process_key]
                await self._start_engine_process(engine_config)

                # 更新健康指标
                health_metrics.restart_count += 1
                health_metrics.consecutive_failures = 0
                health_metrics.start_time = time.time()

                self.stats['total_engine_restarts'] += 1
                logger.success(f"✅ {process_key} 引擎进程重启成功")

            except Exception as e:
                logger.error(f"❌ 重启 {process_key} 引擎进程失败: {e}")
                health_metrics.consecutive_failures += 1

    async def _handle_unhealthy_processes(self, unhealthy_processes: List[str]):
        """处理不健康的进程"""
        for process_key in unhealthy_processes:
            health_metrics = self.engine_health.get(process_key)
            if health_metrics:
                health_metrics.is_healthy = False

            # 可以选择重启不健康的进程
            logger.warning(f"⚠️  {process_key} 进程不健康，将监控是否需要重启")

    def _should_restart_engine(self, health_metrics: EngineHealthMetrics) -> bool:
        """判断是否应该重启引擎"""
        # 检查重启次数限制
        if health_metrics.restart_count >= self.config.max_restart_attempts:
            return False

        # 检查连续失败次数
        if health_metrics.consecutive_failures > 5:
            return False

        return True

    async def _cleanup_process(self, process_key: str):
        """清理进程资源"""
        try:
            # 发送停止信号
            if process_key in self.stop_events:
                self.stop_events[process_key].set()
                del self.stop_events[process_key]

            # 等待进程退出
            if process_key in self.engine_processes:
                process = self.engine_processes[process_key]
                process.join(timeout=10)

                if process.is_alive():
                    logger.warning(f"⚠️  {process_key} 进程未正常退出，强制终止")
                    process.terminate()
                    process.join(timeout=5)

                del self.engine_processes[process_key]

        except Exception as e:
            logger.error(f"❌ 清理 {process_key} 进程资源失败: {e}")

    async def _send_status_notification(self):
        """发送增强状态通知"""
        if not self.config.enable_notifications:
            return

        current_time = time.time()
        if current_time - self.last_notify_time < self.config.notify_interval_min * 60:
            return

        try:
            active_count = len([p for p in self.engine_processes.values() if p.is_alive()])
            total_started = self.stats['total_engines_started']
            total_restarts = self.stats['total_engine_restarts']

            # 计算健康统计
            healthy_count = len([h for h in self.engine_health.values() if h.is_healthy])
            avg_memory = sum(h.memory_usage_mb for h in self.engine_health.values()) / len(self.engine_health) if self.engine_health else 0
            max_restarts = max((h.restart_count for h in self.engine_health.values()), default=0)

            # 获取引擎详情
            engine_details = []
            for process_key, health in list(self.engine_health.items())[:5]:  # 只显示前5个
                status_emoji = "✅" if health.is_healthy else "❌"
                memory_str = f"{health.memory_usage_mb:.0f}MB" if health.memory_usage_mb > 0 else "N/A"
                restart_str = f"({health.restart_count}重启)" if health.restart_count > 0 else ""
                engine_details.append(f"{status_emoji} {process_key} {memory_str} {restart_str}")

            message = (
                f"📊 套利管理器智能状态报告\n"
                f"🤖 活跃引擎: {active_count} (健康: {healthy_count})\n"
                f"🚀 总启动/重启: {total_started}/{total_restarts}\n"
                f"💾 平均内存: {avg_memory:.0f}MB\n"
                f"🔄 最大重启次数: {max_restarts}\n"
                f"🕐 风控更新: {time.strftime('%H:%M:%S', time.localtime(self.last_risk_update_time))}\n"
                f"⏱️  运行时长: {int((time.time() - self.stats.get('start_time', time.time())) / 60)}分钟\n"
            )

            if engine_details:
                message += f"\n🔍 引擎详情:\n" + "\n".join(engine_details)

            await async_notify_telegram(message, channel_type=CHANNEL_TYPE.QUIET)
            await async_notify_telegram(str(self.cached_risk_data), channel_type=CHANNEL_TYPE.QUIET)
            self.last_notify_time = current_time

        except Exception as e:
            logger.error(f"❌ 发送通知失败: {e}")

    async def run(self):
        """运行管理器主循环"""
        await self.initialize()
        await self.start_engines_for_whitelist_and_pos()

        self.is_running = True
        self.stats['start_time'] = time.time()

        logger.success("🎯 多进程套利管理器启动完成")

        while self.is_running and not self.shutdown_event.is_set():
            try:
                # 更新风控数据
                await self._update_risk_data()

                # 检查引擎健康状态
                await self._check_engine_health()

                # 发送状态通知
                await self._send_status_notification()

                # 等待下一次循环，使用短间隔以便快速响应停止信号
                wait_interval = self.config.risk_update_interval_min * 60
                # 分解长等待为多个短等待，确保快速响应
                for _ in range(0, wait_interval, 5):  # 每5秒检查一次
                    if self.shutdown_event.is_set():
                        logger.info("🛑 管理器收到停止信号，退出主循环")
                        break
                    await asyncio.sleep(5)

                if self.shutdown_event.is_set():
                    break  # 退出主循环

            except Exception as e:
                logger.error(f"❌ 管理器运行异常: {e}")
                # 异常时能快速响应停止信号
                for _ in range(60):
                    if self.shutdown_event.is_set():
                        logger.info("🛑 管理器在异常处理中收到停止信号")
                        break
                    await asyncio.sleep(1)

    async def shutdown(self):
        """优雅关闭管理器"""
        # 关闭前再次检查仓位信息
        await self._update_risk_data()
        await async_notify_telegram(str(self.cached_risk_data), channel_type=CHANNEL_TYPE.TRADE)
        self.is_running = False
        self.shutdown_event.set()

        # 发送停止信号给所有引擎进程
        for process_key, stop_event in self.stop_events.items():
            try:
                logger.info(f"🛑 停止 {process_key} 引擎进程...")
                stop_event.set()
            except Exception as e:
                logger.error(f"❌ 发送停止信号给 {process_key} 失败: {e}")

        # 等待所有进程退出 - 使用更激进的超时策略
        for process_key, process in self.engine_processes.items():
            try:
                process.join(timeout=3)  # 减少等待时间到3秒
                if process.is_alive():
                    logger.warning(f"⚠️  {process_key} 进程未在3秒内退出，强制终止")
                    process.terminate()
                    process.join(timeout=2)  # 终止后等待2秒
                    if process.is_alive():
                        logger.error(f"🚨 {process_key} 进程无法终止，强制杀死")
                        process.kill()  # 使用kill强制杀死
                        process.join(timeout=1)
                logger.info(f"✅ {process_key} 引擎进程已关闭")
            except Exception as e:
                logger.error(f"❌ 关闭 {process_key} 引擎进程失败: {e}")

        # 发送最终统计报告
        await self._send_final_report()

        # 清理所有资源
        self.engine_processes.clear()
        self.engine_configs.clear()
        self.engine_health.clear()
        self.stop_events.clear()

        # 关闭多进程管理器
        try:
            self.process_manager.shutdown()
        except Exception as e:
            logger.error(f"❌ 关闭多进程管理器失败: {e}")

    async def _send_final_report(self):
        """发送最终运行报告"""
        if not self.config.enable_notifications:
            return

        try:
            runtime_minutes = int((time.time() - self.stats.get('start_time', time.time())) / 60)

            # 统计健康数据
            healthy_engines = len([h for h in self.engine_health.values() if h.is_healthy])
            total_restarts = sum(h.restart_count for h in self.engine_health.values())

            message = (
                f"🏁 套利管理器运行报告\n"
                f"⏱️  总运行时长: {runtime_minutes} 分钟\n"
                f"🚀 总启动次数: {self.stats['total_engines_started']}\n"
                f"🔄 总重启次数: {total_restarts}\n"
                f"💚 健康引擎: {healthy_engines}/{len(self.engine_health)}\n"
                f"👋 管理器已优雅关闭"
            )

            await async_notify_telegram(message)

        except Exception as e:
            logger.error(f"❌ 发送最终报告失败: {e}")


# 主程序入口
async def main():
    """主程序入口"""
    # 设置多进程启动方法
    mp.set_start_method('spawn', force=True)

    manager = MultiProcessArbitrageManager()

    # 设置信号处理
    shutdown_requested = asyncio.Event()
    signal_received = False

    def signal_handler(signum, _):
        nonlocal signal_received
        if signal_received:
            logger.info(f"🔄 信号 {signum} 已被处理，忽略重复信号")
            return

        signal_received = True
        logger.info(f"👋 收到停止信号 {signum}，立即开始关闭程序...")
        shutdown_requested.set()
        # 立即触发管理器关闭
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(manager.shutdown())
            else:
                # 如果事件循环还没运行，设置标志位让主循环检查
                logger.info("事件循环未运行，设置停止标志")
                manager.shutdown_event.set()
        except Exception as e:
            logger.warning(f"设置关闭任务时出现异常: {e}")
            # 至少设置标志位
            manager.shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # 创建任务来运行管理器
        manager_task = asyncio.create_task(manager.run())

        # 持续等待关闭信号
        await shutdown_requested.wait()

        logger.info("👋 收到关闭信号，正在停止管理器...")

        # 取消管理器任务
        manager_task.cancel()

        try:
            await manager_task
        except asyncio.CancelledError:
            pass

        # 尝试优雅关闭，但有超时限制
        try:
            await asyncio.wait_for(manager.shutdown(), timeout=10)
        except asyncio.TimeoutError:
            logger.error("❌ 管理器关闭超时，强制退出")
            # 强制关闭所有进程
            for process_key, process in manager.engine_processes.items():
                if process.is_alive():
                    logger.warning(f"🚨 强制杀死进程: {process_key}")
                    process.kill()
            return

    except KeyboardInterrupt:
        logger.info("👋 收到键盘中断信号，正在关闭...")
        try:
            await asyncio.wait_for(manager.shutdown(), timeout=10)
        except asyncio.TimeoutError:
            logger.error("❌ 键盘中断关闭超时，强制退出")
            return
    except Exception as e:
        logger.error(f"❌ 程序异常退出: {e}")
        await manager.shutdown()


if __name__ == "__main__":
    # 配置日志
    logger.remove()
    logger.add(
        "logs/simple_arbitrage_manager_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level="INFO"
    )

    # 运行主程序
    asyncio.run(main())