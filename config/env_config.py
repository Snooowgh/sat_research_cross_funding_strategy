# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : env_config
@Description : 环境变量配置读取
@Time        : 2025/10/05
"""
import os
from pathlib import Path
from typing import Optional, Union
from loguru import logger


class EnvConfig:
    """环境变量配置管理类"""

    def __init__(self, env_file: str = ".env"):
        """
        初始化环境配置

        Args:
            env_file: 环境变量文件路径
        """
        self.env_file = Path(env_file)
        self._load_env()

    def _load_env(self):
        """加载环境变量文件"""
        try:
            # 如果没有安装python-dotenv，则手动读取
            try:
                from dotenv import load_dotenv
                load_dotenv(self.env_file)
                logger.debug(f"已使用dotenv加载环境变量文件: {self.env_file}")
            except ImportError:
                self._manual_load_env()
                logger.debug(f"已手动加载环境变量文件: {self.env_file}")
        except Exception as e:
            logger.warning(f"加载环境变量文件失败 {self.env_file}: {e}")

    def _manual_load_env(self):
        """手动加载环境变量文件"""
        if not self.env_file.exists():
            logger.warning(f"环境变量文件不存在: {self.env_file}")
            return

        with open(self.env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    # 移除引号
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    os.environ[key] = value

    def get(self, key: str, default: Optional[Union[str, int, float, bool]] = None,
            value_type: type = str) -> Union[str, int, float, bool]:
        """
        获取环境变量值

        Args:
            key: 环境变量键
            default: 默认值
            value_type: 值类型 (str, int, float, bool)

        Returns:
            环境变量值
        """
        value = os.getenv(key)

        if value is None:
            return default

        try:
            if value_type == bool:
                return value.lower() in ('true', '1', 'yes', 'on')
            elif value_type == int:
                return int(value)
            elif value_type == float:
                return float(value)
            else:
                return value
        except (ValueError, TypeError) as e:
            logger.warning(f"转换环境变量类型失败 {key}={value} -> {value_type}: {e}")
            return default

    def get_str(self, key: str, default: Optional[str] = None) -> str:
        """获取字符串类型环境变量"""
        return self.get(key, default, str)

    def get_int(self, key: str, default: Optional[int] = None) -> int:
        """获取整数类型环境变量"""
        return self.get(key, default, int)

    def get_float(self, key: str, default: Optional[float] = None) -> float:
        """获取浮点数类型环境变量"""
        return self.get(key, default, float)

    def get_bool(self, key: str, default: Optional[bool] = None) -> bool:
        """获取布尔类型环境变量"""
        return self.get(key, default, bool)

    def require(self, key: str, value_type: type = str) -> Union[str, int, float, bool]:
        """
        获取必需的环境变量，如果不存在则抛出异常

        Args:
            key: 环境变量键
            value_type: 值类型

        Returns:
            环境变量值

        Raises:
            ValueError: 当环境变量不存在时
        """
        value = self.get(key, None, value_type)
        if value is None:
            raise ValueError(f"必需的环境变量 {key} 未设置")
        return value

    def require_str(self, key: str) -> str:
        """获取必需的字符串类型环境变量"""
        return self.require(key, str)

    def require_int(self, key: str) -> int:
        """获取必需的整数类型环境变量"""
        return self.require(key, int)

    def require_float(self, key: str) -> float:
        """获取必需的浮点数类型环境变量"""
        return self.require(key, float)

    def require_bool(self, key: str) -> bool:
        """获取必需的布尔类型环境变量"""
        return self.require(key, bool)


# 全局配置实例
if os.getenv("DARWIN_ENV") == "TEST":
    env_config = EnvConfig(env_file=".env.test")
    logger.debug("DARWIN_ENV=TEST, 使用本地测试环境变量配置 (.env.test)")
else:
    env_config = EnvConfig()
