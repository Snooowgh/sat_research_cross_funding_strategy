# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : exchange_config
@Description : 交易所配置管理
@Time        : 2025/10/05
"""
from typing import Dict, Any
from loguru import logger
from .env_config import env_config


class ExchangeConfig:
    """交易所配置管理类"""

    @staticmethod
    def get_binance_config() -> Dict[str, Any]:
        """获取Binance交易所配置"""
        try:
            return {
                'key': env_config.require_str('BINANCE_API_KEY'),
                'secret': env_config.require_str('BINANCE_SECRET_KEY'),
                'erc20_deposit_addr': env_config.get_str('BINANCE_ERC20_DEPOSIT_ADDR', ''),
                'maker_fee_rate': env_config.get_float('BINANCE_MAKER_FEE_RATE', 0.00018),
                'taker_fee_rate': env_config.get_float('BINANCE_TAKER_FEE_RATE', 0.00045),
                'recvWindow': env_config.get_int('RECV_WINDOW', 5000),
                'timeout': env_config.get_int('DEFAULT_TIMEOUT', 10000)
            }
        except ValueError as e:
            logger.error(f"获取Binance配置失败: {e}")
            return {}

    @staticmethod
    def get_hyperliquid_config() -> Dict[str, Any]:
        """获取HyperLiquid交易所配置"""
        try:
            return {
                'private_key': env_config.require_str('HYPERLIQUID_PRIVATE_KEY'),
                'public_key': env_config.get_str('HYPERLIQUID_PUBLIC_KEY'),
                'deposit_private_key': env_config.get_str('HYPERLIQUID_DEPOSIT_PRIVATE_KEY'),
                'maker_fee_rate': env_config.get_float('HYPERLIQUID_MAKER_FEE_RATE', 0.00013),
                'taker_fee_rate': env_config.get_float('HYPERLIQUID_TAKER_FEE_RATE', 0.00039),
                'max_leverage': env_config.get_int('HYPERLIQUID_MAX_LEVERAGE', 10),
                'jump_fund_collect_borrow_amount': env_config.get_float('JUMP_FUND_COLLECT_BORROW_AMOUNT', 8000)
            }
        except ValueError as e:
            logger.error(f"获取HyperLiquid配置失败: {e}")
            return {}

    @staticmethod
    def get_lighter_config() -> Dict[str, Any]:
        """获取Lighter交易所配置"""
        try:
            return {
                'l1_addr': env_config.require_str('LIGHTER_L1_ADDR'),
                'api_private_key': env_config.require_str('LIGHTER_API_PRIVATE_KEY'),
                'account_index': env_config.require_int('LIGHTER_ACCOUNT_INDEX'),
                'api_key_index': env_config.require_int('LIGHTER_API_KEY_INDEX'),
                'base_url': env_config.get_str('LIGHTER_BASE_URL', 'https://mainnet.zklighter.elliot.ai'),
                'account_tier': env_config.get_str('LIGHTER_ACCOUNT_TIER', 'premium'),
                'maker_fee_rate': env_config.get_float('LIGHTER_MAKER_FEE_RATE', 0.00015),
                'taker_fee_rate': env_config.get_float('LIGHTER_TAKER_FEE_RATE', 0.0004),
            }
        except ValueError as e:
            logger.error(f"获取Lighter配置失败: {e}")
            return {}

    @staticmethod
    def get_aster_config() -> Dict[str, Any]:
        """获取Aster Finance交易所配置"""
        try:
            return {
                'key': env_config.require_str('ASTER_API_KEY'),
                'secret': env_config.require_str('ASTER_SECRET_KEY'),
                'erc20_deposit_addr': env_config.get_str('ASTER_ERC20_DEPOSIT_ADDR', ''),
                'base_url': env_config.get_str('ASTER_BASE_URL', 'https://fapi.asterdex.com'),
                'maker_fee_rate': env_config.get_float('ASTER_MAKER_FEE_RATE', 0.00018),
                'taker_fee_rate': env_config.get_float('ASTER_TAKER_FEE_RATE', 0.00045),
                'recvWindow': env_config.get_int('RECV_WINDOW', 5000),
                'timeout': env_config.get_int('DEFAULT_TIMEOUT', 10000),
                'max_leverage': env_config.get_int('ASTER_MAX_LEVERAGE', 10),
            }
        except ValueError as e:
            logger.error(f"获取Aster配置失败: {e}")
            return {}

    @staticmethod
    def get_bybit_config() -> Dict[str, Any]:
        """获取Bybit交易所配置"""
        try:
            return {
                'api_key': env_config.get_str('BYBIT_API_KEY', 'your_bybit_api_key'),
                'secret_key': env_config.get_str('BYBIT_SECRET_KEY', 'your_bybit_secret_key'),
                'erc20_deposit_addr': env_config.get_str('BYBIT_ERC20_DEPOSIT_ADDR', ''),
                'testnet': env_config.get_bool('BYBIT_TESTNET', False),
                'maker_fee_rate': env_config.get_float('BYBIT_MAKER_FEE_RATE', 0.0001),
                'taker_fee_rate': env_config.get_float('BYBIT_TAKER_FEE_RATE', 0.0006),
                'max_leverage': env_config.get_int('BYBIT_MAX_LEVERAGE', 100)
            }
        except ValueError as e:
            logger.error(f"获取Bybit配置失败: {e}")
            return {}

    @staticmethod
    def get_okx_config() -> Dict[str, Any]:
        """获取OKX交易所配置"""
        try:
            return {
                'api_key': env_config.get_str('OKX_API_KEY', 'your_okx_api_key'),
                'secret_key': env_config.get_str('OKX_SECRET_KEY', 'your_okx_secret_key'),
                'passphrase': env_config.get_str('OKX_PASSPHRASE', 'your_okx_passphrase'),
                'erc20_deposit_addr': env_config.get_str('OKX_ERC20_DEPOSIT_ADDR', ''),
                'sandbox': env_config.get_bool('OKX_SANDBOX', False),
                'maker_fee_rate': env_config.get_float('OKX_MAKER_FEE_RATE', 0.00015),
                'taker_fee_rate': env_config.get_float('OKX_TAKER_FEE_RATE', 0.0005),
                'max_leverage': env_config.get_int('OKX_MAX_LEVERAGE', 20)
            }
        except ValueError as e:
            logger.error(f"获取OKX配置失败: {e}")
            return {}

    @staticmethod
    def get_general_config() -> Dict[str, Any]:
        """获取通用配置"""
        return {
            'default_leverage': env_config.get_int('DEFAULT_LEVERAGE', 10),
            'danger_leverage': env_config.get_int('DANGER_LEVERAGE', 14),
            'min_order_amount_usd': env_config.get_float('MIN_ORDER_AMOUNT_USD', 50),
            'default_timeout': env_config.get_int('DEFAULT_TIMEOUT', 10000),
            'recv_window': env_config.get_int('RECV_WINDOW', 5000)
        }

    @staticmethod
    def get_risk_hedge_config() -> Dict[str, Any]:
        """获取实时对冲引擎默认配置"""
        return {
            'min_amount': env_config.get_float('RH_DEFAULT_MIN_AMOUNT', 0.001),
            'max_amount': env_config.get_float('RH_DEFAULT_MAX_AMOUNT', 1.0),
            'amount_step': env_config.get_float('RH_DEFAULT_AMOUNT_STEP', 0.001),
            'min_yield_rate': env_config.get_float('RH_DEFAULT_MIN_YIELD_RATE', 0.001),
            'max_spread': env_config.get_float('RH_DEFAULT_MAX_SPREAD', 0.005),
            'orderbook_freshness': env_config.get_float('RH_DEFAULT_ORDERBOOK_FRESHNESS', 1.0),
            'min_liquidity': env_config.get_float('RH_DEFAULT_MIN_LIQUIDITY', 1000),
            'no_trade_timeout': env_config.get_int('RH_DEFAULT_NO_TRADE_TIMEOUT', 300),
            'engine_timeout': env_config.get_int('RH_DEFAULT_ENGINE_TIMEOUT', 3600)
        }

    @staticmethod
    def get_notification_config() -> Dict[str, Any]:
        """获取通知配置"""
        return {
            'slack_webhook_url': env_config.get_str('SLACK_WEBHOOK_URL', ''),
            'telegram_bot_token': env_config.get_str('TELEGRAM_BOT_TOKEN', ''),
            'telegram_chat_id': env_config.get_str('TELEGRAM_CHAT_ID', '')
        }

    @staticmethod
    def get_log_config() -> Dict[str, Any]:
        """获取日志配置"""
        return {
            'log_level': env_config.get_str('LOG_LEVEL', 'INFO'),
            'log_file_path': env_config.get_str('LOG_FILE_PATH', './logs/'),
            'log_console_enabled': env_config.get_bool('LOG_CONSOLE_ENABLED', True)
        }


    @staticmethod
    def get_binance_ws_config() -> Dict[str, Any]:
        """获取Binance交易所配置"""
        try:
            return {
                'api_key': env_config.require_str('BINANCE_API_KEY'),
                'secret': env_config.require_str('BINANCE_SECRET_KEY'),
            }
        except ValueError as e:
            logger.error(f"获取Binance配置失败: {e}")
            return {}

    @staticmethod
    def get_hyperliquid_ws_config() -> Dict[str, Any]:
        """获取HyperLiquid交易所配置"""
        try:
            return {
                'address': env_config.get_str('HYPERLIQUID_PUBLIC_KEY'),
            }
        except ValueError as e:
            logger.error(f"获取HyperLiquid配置失败: {e}")
            return {}

    @staticmethod
    def get_lighter_ws_config() -> Dict[str, Any]:
        """获取Lighter交易所配置"""
        try:
            return {
                'api_key': env_config.require_int('LIGHTER_API_KEY_INDEX'),
                'secret': env_config.require_str('LIGHTER_API_PRIVATE_KEY'),
                "account_id": env_config.require_int('LIGHTER_ACCOUNT_INDEX'),
            }
        except ValueError as e:
            logger.error(f"获取Lighter配置失败: {e}")
            return {}

    @staticmethod
    def get_aster_ws_config() -> Dict[str, Any]:
        """获取Aster Finance交易所配置"""
        try:
            return {
                'api_key': env_config.require_str('ASTER_API_KEY'),
                'secret': env_config.require_str('ASTER_SECRET_KEY'),
            }
        except ValueError as e:
            logger.error(f"获取Aster配置失败: {e}")
            return {}

    @staticmethod
    def get_bybit_ws_config() -> Dict[str, Any]:
        """获取Bybit交易所配置"""
        try:
            return {
                'api_key': env_config.get_str('BYBIT_API_KEY', 'your_bybit_api_key'),
                'secret': env_config.get_str('BYBIT_SECRET_KEY', 'your_bybit_secret_key'),
            }
        except ValueError as e:
            logger.error(f"获取Bybit配置失败: {e}")
            return {}

    @staticmethod
    def get_okx_ws_config() -> Dict[str, Any]:
        """获取OKX交易所配置"""
        try:
            return {
                'api_key': env_config.get_str('OKX_API_KEY', 'your_okx_api_key'),
                'secret': env_config.get_str('OKX_SECRET_KEY', 'your_okx_secret_key'),
                'passphrase': env_config.get_str('OKX_PASSPHRASE', 'your_okx_passphrase'),
            }
        except ValueError as e:
            logger.error(f"获取OKX配置失败: {e}")
            return {}
