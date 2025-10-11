# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

**darwin_light** 是一个多交易所加密货币对冲套利交易系统，专注于跨交易所的费率套利和价差套利。

### 核心功能
- **跨交易所套利**: 支持 Binance、HyperLiquid、Lighter、EdgeX、Aster 等多个交易所
- **实时对冲交易**: 基于 WebSocket 订单簿的实时运算对冲引擎
- **费率套利**: 利用不同交易所间的资金费率差异进行套利
- **价差套利**: 捕捉交易所间的价格差异机会
- **自动化交易**: 支持定时任务、机会搜索、仓位管理

### 项目目标
- 构建稳定可靠的多交易所套利框架
- 实现高效的 WebSocket 实时交易引擎
- 优化交易执行策略（动态数量调整、动态收益率调整）
- 提供完善的风控机制（流动性检查、订单簿新鲜度检查等）

### 开发规范
- 使用异步编程（`asyncio`）提高并发性能，尤其是交易所相关函数
- 采用模块化设计，便于扩展和维护
- 使用 `loguru` 进行日志记录和错误追踪
- 程序鲁棒性强，支持自动重连和严谨的错误处理和通知
- 重要通知如资金变动，仓位变动，程序异常等要使用async_notify_telegram函数进行通知

## 核心架构

### 1. 交易所抽象层 (`cex_tools/`)

每个交易所实现独立的客户端类，继承或包装官方 SDK：
- `binance_future.py`: Binance 合约交易（基于 binance-futures-connector）
- `lighter_future.py`: Lighter 交易所（基于 lighter-sdk）
- `hyperliquid_future.py`: HyperLiquid（基于 hyperliquid-python-sdk）
- `aster_future.py`: Aster 交易所
- `okx_future.py`: Okx 交易所
- `bybit_future.py`: Bybit 交易所

**关键接口**:
- 下单: `make_new_order(pair, side, order_type, amount, price)`
- 获取当前仓位: `get_all_cur_positions()`
- 获取年化资金费率: `get_funding_rate()`
- 获取所有交易品种和价格: `get_all_tick_price()`
- 获取当前可用的保证金: `get_available_margin()`
- 获取账户总价值: `get_total_margin()`
- 获取最近的订单: `get_recent_order(symbol, order_id)`

### 2. WebSocket 订单簿流 (`cex_tools/exchange_ws/`)

实时订单簿数据流的统一抽象：
- `orderbook_stream.py`: 基础抽象类 `OrderBookStream` 和 `OrderBookData`
- `binance_ws_direct.py`: Binance WebSocket 实现
- `lighter_orderbook_stream.py`: Lighter WebSocket 实现（支持心跳）
- `hyperliquid_orderbook_stream.py`: HyperLiquid WebSocket 实现

**订阅模式**:
```python
stream = LighterOrderBookStreamAsync()
stream.subscribe("BTCUSDT", callback_function)
await stream.start()
```

### 3. 实时对冲引擎 (`cex_tools/exchange_ws/realtime_hedge_engine.py`)

核心交易引擎，实现基于 WebSocket 的实时对冲交易：

**关键组件**:
- `RealtimeHedgeEngine`: 主引擎类
- `TradeConfig`: 交易配置（数量范围、步长、超时等）
- `RiskConfig`: 风控配置（流动性、价差、收益率等）
- `TradeSignal`: 交易信号（价格、价差、收益率）

**核心特性**:
- 动态数量调整：根据订单簿第一档流动性动态调整下单数量
- 动态收益率调整：根据实际执行收益率自动调整最小收益率要求
- 最小订单金额限制：确保单笔订单至少 50 美金（可配置）
- 无交易超时：自动停止长时间无交易的引擎以节省资源
- 风控检查：订单簿新鲜度、价差、流动性、收益率多重检查

**使用示例**:
```python
engine = RealtimeHedgeEngine(
    stream1=binance_stream,
    stream2=lighter_stream,
    exchange1=binance_client,
    exchange2=lighter_client,
    trade_config=TradeConfig(...),
    risk_config=RiskConfig(...)
)
await engine.start()
```

### 4. 套利参数配置 (`arbitrage_param.py`)

定义不同交易所组合的套利参数：
- `BinanceLighterArbitrageParam`: Binance-Lighter 套利
- `HyperliquidLighterArbitrageParam`: HyperLiquid-Lighter 套利
- `MultiExchangeArbitrageParam`: 多交易所配置

### 5. 机会搜索 (`cex_tools/chance_searcher.py`)

搜索跨交易所的套利机会：
- 价差分析
- 资金费率对比
- 流动性评估
- 机会排序和展示

### 6. 交易执行入口 (`simple_pair_position_builder.py`)

交互式 CLI 工具，用于执行套利交易：
- 选择交易对
- 配置交易参数（数量、收益率等）
- 选择执行模式（实时引擎 vs 分批执行）
- 仓位管理（开仓/减仓）

## 常用命令

### 环境配置
```bash
# 安装依赖
pip install -r requirements.txt

# 注意：lighter-sdk 和 binance-futures-connector 使用自定义分支
```

### 运行程序
```bash
# 启动定时任务（套利机会监控）
python main.py

# 启动交互式交易 CLI
python simple_pair_position_builder.py

# 查看套利机会信息
python arbitrage_info_show.py
```

### 测试
```bash
# WebSocket 连接测试
python test_lighter_ws.py
python test_lighter_ws_reconnect.py

# 查看示例
python example/realtime_hedge_example.py
python example/lighter_ws_example.py
python example/hyperliquid_ws_example.py
```

## 数据模型 (`cex_tools/exchange_model/`)

统一的数据模型定义：
- `position_model.py`: 仓位模型
- `order_model.py`: 订单模型
- `orderbook_model.py`: 订单簿模型
- `kline_bar_model.py`: K线数据模型
- `account_summary_model.py`: 账户摘要模型

## 工具模块 (`utils/`)

- `notify_tools.py`: 通知工具（Telegram、Slack）
- `decorators.py`: 装饰器（缓存、重试等）
- `time_utils.py`: 时间工具
- `parallelize_utils.py`: 并行任务执行
- `schedule_tool.py`: 定时任务调度

## 关键设计决策

### WebSocket 重连机制
Lighter WebSocket 实现了自动重连，包括：
- 指数退避重连策略
- ping/pong 心跳保活
- 线程安全的订单簿更新

### 对冲交易数量一致性
实时对冲引擎确保两个交易所的下单数量完全一致：
- 使用两个交易所价格的平均值计算最小订单金额
- 返回统一的交易数量，避免两边数量不一致

### 动态参数调整
- **动态数量**: 根据订单簿第一档流动性自动调整，避免过度冲击市场
- **动态收益率**: 根据实际执行收益率调整最小要求，优化 entry 价格

### 风控分层
1. 订单簿数据新鲜度检查（默认 1 秒）
2. 盘口价差检查（防止滑点过大）
3. 流动性深度检查（确保足够深度）
4. 价差收益率检查（确保满足最小收益要求）

## 改进方向

### 短期优化
- [ ] 优化订单执行延迟（并发下单已实现）
- [ ] 添加更多交易所
- [ ] 自动寻找交易机会，自动开仓平仓

### 中期目标
- [ ] 实现多币种同时套利
- [ ] 优化资金管理和仓位分配
- [ ] 实现更智能的机会评分算法

### 长期规划
- [ ] 实现机器学习辅助的参数优化
- [ ] 支持更复杂的套利策略（三角套利、跨链套利等）

## 注意事项

### API 密钥管理
- 所有交易所 API 密钥和基本信息在 `.env` 中配置

### 日志配置
- 使用 loguru 记录日志，默认保存在 `./logs/` 目录
- 支持 Telegram 通知

### 异步编程
- 尽可能使用 asyncio 进行异步操作
- WebSocket 流运行在独立线程中
- 注意同步/异步代码的正确调用方式
- 注意使用信息缓存，提高性能

### 交易所特性
- 不同交易所的订单模型和响应格式差异较大，需要适配