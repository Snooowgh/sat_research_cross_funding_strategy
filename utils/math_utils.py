# coding=utf-8
"""
@Project     : darwin_light
@Author      : Arson
@File Name   : math_utils
@Description :
@Time        : 2025/10/9 20:57
"""
from decimal import Decimal, ROUND_HALF_UP


def align_with_decimal(a, b):
    """
    使用decimal模块将浮点数a的精度调整为与b相同

    参数:
    a (float): 要调整精度的浮点数。
    b (float): 目标精度的浮点数。

    返回:
    float: 精度调整后的浮点数a。
    """
    # 将a和b转换为Decimal对象，注意使用字符串以避免引入初始误差
    dec_a = Decimal(str(a))
    dec_b = Decimal(str(b))

    # 获取b的精度（小数位数）
    # 使用Decimal的as_tuple()方法，exponent表示小数位数的负数
    decimals_b = -dec_b.as_tuple().exponent

    # 构造量化模板，例如小数位数为2则模板为'0.01'
    quantize_str = '0.' + '0' * decimals_b
    quantizer = Decimal(quantize_str)

    # 使用ROUND_HALF_UP模式（四舍五入）进行量化
    rounded_dec_a = dec_a.quantize(quantizer, rounding=ROUND_HALF_UP)

    # 将Decimal对象转换回浮点数
    return float(rounded_dec_a)
    # 输出: True (但注意浮点数比较本身的风险)
