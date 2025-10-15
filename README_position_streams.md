# 仓位WebSocket流测试指南

## 📋 概述

本测试脚本用于验证多交易所仓位WebSocket流的实时监听功能，支持 Binance、Hyperliquid、OKX、Bybit、Lighter 等交易所。

## 🔧 环境配置

### 1. 配置环境变量

复制 `.env.example` 文件为 `.env` 并填入你的API密钥：

```bash
cp .env.example .env
```

编辑 `.env` 文件，配置需要测试的交易所API密钥：

```bash
# Binance
BINANCE_API_KEY=your_binance_api_key
BINANCE_SECRET_KEY=your_binance_secret_key

# Hyperliquid
HYPERLIQUID_PRIVATE_KEY=your_hyperliquid_private_key

# OKX
OKX_API_KEY=your_okx_api_key
OKX_SECRET_KEY=your_okx_secret_key
OKX_PASSPHRASE=your_okx_passphrase

# Bybit
BYBIT_API_KEY=your_bybit_api_key
BYBIT_SECRET_KEY=your_bybit_secret_key

# Lighter
LIGHTER_API_PRIVATE_KEY=your_lighter_api_private_key
LIGHTER_API_KEY_INDEX=your_lighter_api_key_index

# Aster
ASTER_API_KEY=your_aster_api_key
ASTER_SECRET_KEY=your_aster_secret_key
```

### 2. 安装依赖

确保已安装所有必要的依赖包：

```bash
pip install -r requirements.txt
```

## 🚀 运行测试

### 1. 基本用法

测试所有支持的交易所：

```bash
python test_position_streams.py
```

### 2. 指定交易所

测试特定的交易所：

```bash
# 只测试Binance和Hyperliquid
python test_position_streams.py binance,hyperliquid

# 测试单个交易所
python test_position_streams.py binance
```

### 3. 支持的交易所

- `binance` - Binance合约
- `hyperliquid` - Hyperliquid
- `okx` - OKX合约
- `bybit` - Bybit合约
- `lighter` - Lighter
- `aster` - Aster Finance

## 📊 输出示例

### 启动信息
```
🔍 检查API配置...
✅ BINANCE API配置完整
✅ HYPERLIQUID API配置完整
🎯 开始测试交易所: binance,hyperliquid
💡 按 Ctrl+C 停止测试
============================================================
🚀 启动仓位WebSocket流测试...
📋 测试交易所: binance, hyperliquid
✅ 仓位WebSocket流启动成功
```

### 仓位事件
```
📊 仓位事件 [Binance] BTCUSDT
   事件类型: open
   当前仓位: 0.001
   仓位方向: long
   入场价格: 50000.0
   标记价格: 50100.0
   未实现盈亏: 0.1000
   名义价值: 50.00
   仓位变化: +0.0010
------------------------------------------------------------
🔓 开仓: BTCUSDT 多头0.0010 入场价: 50000 盈亏: 0.1000
```

### 状态报告
```
📈 状态报告:
   📊 Binance 仓位WebSocket流状态:
     • 连接状态: 🟢 运行中
     • 仓位总数: 2
     • 开仓数量: 1
     • 最后更新: 5.2秒前
     • 开仓列表:
       - BTCUSDT: 多0.0010 盈亏:0.1000

📊 总开仓数: 1, 总盈亏: 0.1000
```

## 🔍 监控功能

### 1. 实时事件
- **开仓事件**：新仓位建立
- **平仓事件**：仓位完全关闭
- **加仓事件**：同向仓位增加
- **减仓事件**：同向仓位减少
- **资金费用**：资金费率结算

### 2. 状态监控
- 每30秒自动输出状态报告
- 显示各交易所连接状态
- 统计总开仓数和总盈亏
- 监控数据新鲜度

### 3. 错误处理
- 自动重连机制
- 详细的错误日志
- 优雅的关闭处理

## 🛠️ 故障排除

### 1. API配置错误
```
❌ BINANCE 缺少环境变量: BINANCE_API_KEY, BINANCE_SECRET_KEY
```
**解决方案**：检查 `.env` 文件中的API密钥配置

### 2. 连接失败
```
❌ 无法启动任何仓位流，请检查API配置
```
**解决方案**：
- 检查网络连接
- 验证API密钥权限
- 确认交易所服务状态

### 3. 认证失败
```
❌ 认证失败: Invalid API key
```
**解决方案**：
- 检查API密钥是否正确
- 确认API密钥权限（需要交易权限）
- 检查IP白名单设置

### 4. 没有仓位数据
如果长时间没有收到仓位更新：
- 确保账户有活跃仓位
- 检查API密钥是否包含仓位查询权限
- 查看日志中的连接状态

## 📝 注意事项

1. **API权限**：确保API密钥具有查询仓位权限
2. **网络要求**：需要稳定的网络连接以维持WebSocket
3. **资源占用**：同时连接多个交易所会占用一定系统资源
4. **安全性**：妥善保管API密钥，不要提交到版本控制系统

## 🔄 停止测试

按 `Ctrl+C` 优雅停止所有WebSocket连接：

```
🛑 收到中断信号，正在停止...
🛑 正在停止仓位WebSocket流...
✅ 所有仓位流已停止
```

## 📚 更多信息

- 查看完整日志了解详细的连接和数据传输过程
- 可以修改 `test_position_streams.py` 中的回调函数来自定义处理逻辑
- 参考各交易所的官方API文档了解WebSocket数据格式