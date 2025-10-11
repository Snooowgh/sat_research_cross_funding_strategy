# 交易所开发指南

## 📖 概述

本文档描述了如何在新架构下开发和集成新的交易所。

## 🏗️ 架构概览

```
cex_tools/
├── base_exchange.py          # 抽象基类
├── exchange_factory.py       # 工厂类
├── exchange_utils.py         # 工具类
├── bybit_future.py          # Bybit 实现 (示例)
├── binance_future.py        # Binance 实现
└── ...                      # 其他交易所
```

## 🚀 快速开始

### 1. 创建新的交易所实现

```python
# cex_tools/new_exchange_future.py

from cex_tools.base_exchange import FutureExchange
from cex_tools.cex_enum import ExchangeEnum
from loguru import logger

class NewExchangeFuture(FutureExchange):
    """
    新交易所实现

    Features:
    - 支持的功能特点
    - 费率结构
    - API 限制等
    """

    def __init__(self, api_key="", secret_key="", testnet=False, **kwargs):
        """
        初始化交易所客户端

        Args:
            api_key: API密钥
            secret_key: 密钥
            testnet: 是否使用测试网络
        """
        # 调用基类初始化
        super().__init__(ExchangeEnum.NEW_EXCHANGE, testnet)

        # 设置交易所特有参数
        self.funding_interval = 8 * 3600  # 资金费率间隔
        self.maker_fee_rate = 0.0002      # Maker费率
        self.taker_fee_rate = 0.0004      # Taker费率
        self.max_leverage = 125           # 最大杠杆

        # 初始化客户端
        self._init_clients(api_key, secret_key, testnet)

        logger.info(f"NewExchange 初始化完成 (testnet={testnet})")

    def _init_clients(self, api_key: str, secret_key: str, testnet: bool):
        """初始化 API 客户端"""
        # 实现具体的客户端初始化
        self.client = YourSDK(api_key, secret_key, testnet=testnet)

    def convert_symbol(self, symbol: str) -> str:
        """
        转换交易对格式

        Args:
            symbol: 交易对符号 (如 "BTC", "BTCUSDT")

        Returns:
            该交易所的标准格式
        """
        if not symbol.endswith("USDT"):
            return symbol + "USDT"
        return symbol

    def get_tick_price(self, symbol: str) -> float:
        """
        获取最新价格

        Args:
            symbol: 交易对符号

        Returns:
            当前价格，失败返回0
        """
        try:
            symbol = self.convert_symbol(symbol)

            # 实现具体的价格获取逻辑
            result = self.client.get_ticker(symbol)
            price = float(result['price'])

            return price

        except Exception as e:
            self._handle_api_error(e, "获取价格", symbol)
            return 0

    def get_orderbook(self, symbol: str, limit: int = 50) -> OrderBookModel:
        """获取订单簿"""
        # 实现订单簿获取逻辑
        pass

    def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[KlineModel]:
        """获取K线数据"""
        # 实现 K线数据获取逻辑
        pass

    def get_all_cur_positions(self) -> List[PositionModel]:
        """获取所有持仓"""
        # 实现持仓获取逻辑
        pass

    def get_position(self, symbol: str) -> Optional[PositionModel]:
        """获取指定持仓"""
        # 实现单个持仓获取逻辑
        pass

    def get_available_balance(self, asset: str = "USDT") -> float:
        """获取可用余额"""
        # 实现余额获取逻辑
        pass

    def make_new_order(self, symbol: str, side: str, order_type: str,
                      quantity: float, price: Optional[float] = None, **kwargs) -> Optional[Dict]:
        """下单"""
        # 使用基类的参数验证
        symbol = self._validate_order_params(symbol, side, order_type, quantity, price)

        # 实现下单逻辑
        pass

    def get_funding_rate(self, symbol: str, apy: bool = True) -> float:
        """获取资金费率"""
        # 实现资金费率获取逻辑
        pass
```

### 2. 注册交易所

在 `exchange_factory.py` 中注册：

```python
# 在 auto_register_exchanges() 函数中添加
try:
    from cex_tools.new_exchange_future import NewExchangeFuture
    ExchangeFactory.register_exchange(ExchangeEnum.NEW_EXCHANGE, NewExchangeFuture)
except ImportError:
    logger.warning("NewExchangeFuture 不可用")
```

### 3. 在枚举中添加交易所代码

在 `cex_enum.py` 中添加：

```python
class ExchangeEnum(Enum):
    BINANCE = "binance"
    BYBIT = "bybit"
    NEW_EXCHANGE = "new_exchange"  # 新增
```

### 4. 配置环境变量

```bash
# .env 文件中添加
NEW_EXCHANGE_API_KEY="your_api_key"
NEW_EXCHANGE_SECRET_KEY="your_secret_key"
```

## 📋 必需实现的方法

### 抽象方法（必须实现）

```python
# 交易对转换
def convert_symbol(self, symbol: str) -> str

# 价格查询
def get_tick_price(self, symbol: str) -> float

# 订单簿获取
def get_orderbook(self, symbol: str, limit: int = 50) -> Optional[BaseModel]

# K线数据
def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[BaseModel]

# 仓位管理
def get_all_cur_positions(self) -> List[BaseModel]
def get_position(self, symbol: str) -> Optional[BaseModel]

# 资金查询
def get_available_balance(self, asset: str = "USDT") -> float

# 交易执行
def make_new_order(self, symbol: str, side: str, order_type: str,
                  quantity: float, price: Optional[float] = None, **kwargs) -> Optional[Dict]

# 永续合约特有
def get_funding_rate(self, symbol: str, apy: bool = True) -> float
```

### 可选重写的方法

```python
# 通用工具方法（有默认实现）
def get_all_tick_price(self) -> List[Dict]
def get_pair_max_leverage(self, pair: str) -> int
def get_available_margin(self) -> float
def get_total_margin(self) -> float
def set_leverage(self, symbol: str, leverage: int) -> bool
def cancel_all_orders(self, symbol: Optional[str] = None) -> bool
```

## 🔧 使用工厂模式

### 创建交易所实例

```python
from cex_tools.exchange_factory import ExchangeFactory
from cex_tools.cex_enum import ExchangeEnum

# 方式1：直接创建
bybit = ExchangeFactory.create_exchange(
    ExchangeEnum.BYBIT,
    api_key="your_key",
    secret_key="your_secret",
    testnet=True
)

# 方式2：使用缓存
bybit_main = ExchangeFactory.get_exchange(
    ExchangeEnum.BYBIT,
    cache_key="main_account",
    testnet=False
)

bybit_test = ExchangeFactory.get_exchange(
    ExchangeEnum.BYBIT,
    cache_key="test_account",
    testnet=True
)
```

### 批量创建多个交易所

```python
exchanges = []
for exchange_code in [ExchangeEnum.BYBIT, ExchangeEnum.BINANCE, ExchangeEnum.OKX]:
    try:
        exchange = ExchangeFactory.create_exchange(exchange_code, testnet=True)
        exchanges.append(exchange)
    except Exception as e:
        logger.error(f"创建 {exchange_code} 失败: {e}")
```

## 🛠️ 使用工具类

### 价格比较

```python
from cex_tools.exchange_utils import ExchangeUtils

# 比较多个交易所的价格
prices = ExchangeUtils.compare_prices(exchanges, "BTCUSDT")
for exchange, price in prices.items():
    print(f"{exchange}: ${price}")

# 寻找套利机会
opportunity = ExchangeUtils.find_arbitrage_opportunity(
    exchanges,
    "BTCUSDT",
    min_profit_rate=0.001
)

if opportunity:
    print(f"发现套利机会:")
    print(f"  买入: {opportunity['buy_exchange']} @ {opportunity['buy_price']}")
    print(f"  卖出: {opportunity['sell_exchange']} @ {opportunity['sell_price']}")
    print(f"  利润率: {opportunity['profit_rate']:.2%}")
```

### 资金费率套利

```python
# 比较资金费率
funding_rates = ExchangeUtils.compare_funding_rates(
    exchanges,
    ["BTCUSDT", "ETHUSDT"],
    apy=True
)

# 寻找资金费率套利机会
funding_opportunity = ExchangeUtils.find_funding_arbitrage(
    exchanges,
    "BTCUSDT",
    min_rate_diff=0.01
)

if funding_opportunity:
    print(f"资金费率套利机会:")
    print(f"  正费率: {funding_opportunity['positive_funding']} @ {funding_opportunity['positive_rate']:.4%}")
    print(f"  负费率: {funding_opportunity['negative_funding']} @ {funding_opportunity['negative_rate']:.4%}")
```

### 健康监控

```python
# 监控交易所健康状态
health_status = ExchangeUtils.monitor_exchange_health(exchanges)

for exchange, status in health_status.items():
    if status['healthy']:
        print(f"✅ {exchange}: 响应时间 {status['response_time']:.0f}ms")
    else:
        print(f"❌ {exchange}: {status['error']}")
```

## 🧪 测试指南

### 单元测试

```python
# tests/test_new_exchange.py

import unittest
from cex_tools.new_exchange_future import NewExchangeFuture

class TestNewExchange(unittest.TestCase):

    def setUp(self):
        self.exchange = NewExchangeFuture(testnet=True)

    def test_convert_symbol(self):
        """测试交易对转换"""
        self.assertEqual(self.exchange.convert_symbol("BTC"), "BTCUSDT")
        self.assertEqual(self.exchange.convert_symbol("BTCUSDT"), "BTCUSDT")

    def test_get_tick_price(self):
        """测试价格获取"""
        price = self.exchange.get_tick_price("BTCUSDT")
        self.assertIsInstance(price, (int, float))
        self.assertGreater(price, 0)

    def test_get_orderbook(self):
        """测试订单簿获取"""
        orderbook = self.exchange.get_orderbook("BTCUSDT")
        self.assertIsNotNone(orderbook)
        self.assertGreater(len(orderbook.bids), 0)
        self.assertGreater(len(orderbook.asks), 0)

    def test_parameter_validation(self):
        """测试参数验证"""
        # 这些测试会自动使用基类的验证逻辑
        with self.assertRaises(ValueError):
            self.exchange._validate_order_params("", "BUY", "LIMIT", 1, 1000)

        with self.assertRaises(ValueError):
            self.exchange._validate_order_params("BTC", "INVALID", "LIMIT", 1, 1000)

if __name__ == '__main__':
    unittest.main()
```

### 集成测试

```python
# tests/test_exchange_integration.py

from cex_tools.exchange_factory import ExchangeFactory
from cex_tools.exchange_utils import ExchangeUtils

def test_multi_exchange_price_comparison():
    """测试多交易所价格比较"""
    exchanges = [
        ExchangeFactory.create_exchange(ExchangeEnum.BYBIT, testnet=True),
        ExchangeFactory.create_exchange(ExchangeEnum.BINANCE, testnet=True),
    ]

    prices = ExchangeUtils.compare_prices(exchanges, "BTCUSDT")

    # 验证所有交易所都返回了价格
    assert all(price > 0 for price in prices.values())

    # 验证价格差异在合理范围内
    price_values = list(prices.values())
    price_diff = max(price_values) - min(price_values)
    assert price_diff / min(price_values) < 0.05  # 5%以内

def test_arbitrage_opportunity_detection():
    """测试套利机会检测"""
    exchanges = [ExchangeFactory.create_exchange(ExchangeEnum.BYBIT, testnet=True)]

    opportunity = ExchangeUtils.find_arbitrage_opportunity(
        exchanges, "BTCUSDT", min_profit_rate=0.001
    )

    # 套利机会可能存在也可能不存在，都是正常的
    if opportunity:
        assert opportunity['profit_rate'] >= 0.001
```

## 📚 最佳实践

### 1. 错误处理

```python
def get_tick_price(self, symbol: str) -> float:
    """获取价格的最佳实践"""
    try:
        # 使用基类的参数验证
        symbol = self._validate_symbol(symbol)

        # 检查缓存
        cached_price = self._get_cached_price(symbol)
        if cached_price is not None:
            return cached_price

        # API 调用
        result = self.client.get_ticker(symbol)
        price = float(result['price'])

        # 更新缓存
        self._update_cache('price', symbol, price)

        return price

    except ValueError as e:
        # 参数错误，直接抛出
        logger.error(f"参数错误: {e}")
        raise
    except Exception as e:
        # API 错误，使用统一处理
        self._handle_api_error(e, "获取价格", symbol)
        return 0
```

### 2. 性能优化

```python
class OptimizedExchange(FutureExchange):
    """性能优化的交易所实现"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 自定义缓存TTL
        self._cache_ttl.update({
            'price': 0.5,      # 价格缓存0.5秒
            'orderbook': 0.2, # 订单簿缓存0.2秒
            'funding': 300    # 资金费率缓存5分钟
        })

        # 批量请求支持
        self._batch_request_size = 10

    async def get_multiple_prices(self, symbols: List[str]) -> Dict[str, float]:
        """批量获取价格（减少API调用）"""
        # 批量请求实现
        pass
```

### 3. 监控和日志

```python
class MonitoredExchange(FutureExchange):
    """带监控的交易所实现"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 性能监控
        self._api_call_times = []
        self._error_counts = {'price': 0, 'orderbook': 0, 'trade': 0}

    def get_tick_price(self, symbol: str) -> float:
        """带性能监控的价格获取"""
        start_time = time.time()

        try:
            price = super().get_tick_price(symbol)

            # 记录成功调用
            elapsed = time.time() - start_time
            self._api_call_times.append(elapsed)

            # 性能告警
            if elapsed > 1.0:
                logger.warning(f"{self.exchange_code} API 响应过慢: {elapsed:.2f}s")

            return price

        except Exception as e:
            # 记录错误
            self._error_counts['price'] += 1
            self._handle_api_error(e, "获取价格", symbol)
            return 0
```

## 🔍 调试指南

### 1. 启用详细日志

```python
import logging
from loguru import logger

# 设置日志级别
logger.remove()
logger.add(sys.stdout, level="DEBUG")

# 创建交易所实例
exchange = ExchangeFactory.create_exchange(ExchangeEnum.BYBIT, testnet=True)
```

### 2. 使用缓存调试

```python
# 查看缓存状态
print("价格缓存:", exchange._price_cache)
print("缓存更新时间:", exchange._last_cache_update)

# 清除缓存
exchange.clear_cache()
```

### 3. 模拟API响应

```python
class MockExchange(FutureExchange):
    """用于测试的模拟交易所"""

    def get_tick_price(self, symbol: str) -> float:
        # 返回模拟价格
        return 50000.0 + hash(symbol) % 1000

    def get_orderbook(self, symbol: str, limit: int = 50):
        # 返回模拟订单簿
        return MockOrderBook(symbol)
```

这个指南为开发新的交易所集成提供了完整的参考，包括代码模式、最佳实践、测试方法等。遵循这些指南可以确保新交易所的实现与现有架构保持一致，并充分利用框架提供的功能。