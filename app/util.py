
# -*- coding: utf-8 -*-
"""
Upbit 호가단위(틱) 반올림/내림 유틸리티
- KRW 마켓 기준
"""
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Optional


CUSTOM_MARKET_TICK = {
    "KRW-XRP": Decimal("1"),
    "KRW-SOL": Decimal("100"),
    "KRW-BTC": Decimal("1000"),
    "KRW-ETH": Decimal("1000"),
}


def krw_tick_size(price: float) -> Decimal:
    p = Decimal(str(price))
    if p < Decimal('10'):
        return Decimal('0.01')
    elif p < Decimal('100'):
        return Decimal('0.1')
    elif p < Decimal('1000'):
        return Decimal('1')
    elif p < Decimal('10000'):
        return Decimal('5')
    elif p < Decimal('100000'):
        return Decimal('10')
    elif p < Decimal('500000'):
        return Decimal('50')
    elif p < Decimal('1000000'):
        return Decimal('100')
    elif p < Decimal('2000000'):
        return Decimal('500')
    else:
        return Decimal('1000')


def round_price_to_tick(price: float, method: str = 'up', market: Optional[str] = None) -> float:
    """
    :param price: 희망 가격
    :param method: 'up' -> 올림, 'down' -> 내림
    :param market: 특정 마켓에 대한 별도 호가단위를 적용 (예: KRW-SOL 등)
    """
    if market and market in CUSTOM_MARKET_TICK:
        t = CUSTOM_MARKET_TICK[market]
    else:
        t = krw_tick_size(price)
    p = Decimal(str(price))
    if method == 'down':
        q = (p / t).to_integral_value(rounding=ROUND_DOWN)
    else:
        q = (p / t).to_integral_value(rounding=ROUND_UP)
    return float(q * t)


def round_volume(v: float, digits: int = 8) -> float:
    # 업비트는 보통 8자리 소수 지원
    return float(Decimal(str(v)).quantize(Decimal('1.' + '0' * digits), rounding=ROUND_DOWN))
