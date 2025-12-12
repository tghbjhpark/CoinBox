
# -*- coding: utf-8 -*-
import time
from typing import Any, Dict

try:
    import pyupbit  # type: ignore
except Exception as e:
    pyupbit = None

class UpbitClient:
    """
    pyupbit 래퍼. 간단한 기능만 사용합니다.
    """
    def __init__(self, access_key: str, secret_key: str, dry_run: bool = False):
        self.dry_run = dry_run
        self._upbit = None
        if not dry_run:
            if pyupbit is None:
                raise RuntimeError("pyupbit 모듈이 필요합니다. requirements.txt로 설치해 주세요.")
            self._upbit = pyupbit.Upbit(access_key, secret_key)

    def get_current_price(self, market: str) -> float:
        if self.dry_run:
            # 드라이런: 단순한 모의 가격 (시간 변동)
            base = 100_000.0
            return base + (time.time() % 60)  # 초에 따라 약간 변동
        assert pyupbit is not None
        p = pyupbit.get_current_price(market)
        if p is None:
            raise RuntimeError(f"현재가 조회 실패: {market}")
        return float(p)

    def get_krw_balance(self) -> float:
        if self.dry_run:
            return 1_000_000.0
        assert self._upbit is not None
        balances = self._upbit.get_balances()
        for b in balances:
            if b.get('currency') == 'KRW':
                return float(b.get('balance') or 0.0)
        return 0.0

    def buy_market(self, market: str, krw: float) -> Dict[str, Any]:
        if self.dry_run:
            return {"uuid": f"dry-{time.time()}", "side": "bid", "market": market, "krw": krw}
        assert self._upbit is not None
        return self._upbit.buy_market_order(market, krw)

    def sell_limit(self, market: str, volume: float, price: float) -> Dict[str, Any]:
        if self.dry_run:
            return {"uuid": f"dry-{time.time()}", "side": "ask", "market": market, "price": price, "volume": volume}
        assert self._upbit is not None
        return self._upbit.sell_limit_order(market, price, volume)

    def get_order(self, uuid: str) -> Dict[str, Any]:
        if self.dry_run:
            # 드라이런에서는 즉시 체결로 가정
            return {
                "uuid": uuid,
                "state": "done",
                "executed_volume": "0",
                "trades": [],
            }
        assert self._upbit is not None
        return self._upbit.get_order(uuid)

    def cancel_order(self, uuid: str) -> Dict[str, Any]:
        if self.dry_run:
            return {"uuid": uuid}
        assert self._upbit is not None
        return self._upbit.cancel_order(uuid)
