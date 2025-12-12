
# -*- coding: utf-8 -*-
from dataclasses import dataclass
import os


@dataclass
class Settings:
    access_key: str
    secret_key: str
    market: list[str]
    krw: float
    interval_sec: int
    tp_ratio: float  # 1.0 == +1%, 0.5 == +0.5%
    firestore_credential_path: str

    dry_run: bool = False
    min_krw_balance: float = 5000.0  # 업비트 최소주문금액 기본값
    timezone: str = os.getenv("TZ", "Asia/Seoul")
    skip_buy_within_ratio: float = 0.3  # 이전 매수가 대비 X% 이내면 매수 스킵
    buy_fill_timeout_sec: float = 30.0  # 매수 주문 체결 대기 타임아웃
    max_order_count: int = 10 # 매도 최대 갯수

    @staticmethod
    def from_env_and_args(args) -> "Settings":
        access = os.getenv("UPBIT_ACCESS_KEY", "")
        secret = os.getenv("UPBIT_SECRET_KEY", "")
        if not args.dry_run and (not access or not secret):
            raise SystemExit("실거래 모드에서 UPBIT_ACCESS_KEY / UPBIT_SECRET_KEY 환경변수를 설정해 주세요. (또는 --dry-run 사용)")

        return Settings(
            access_key=access,
            secret_key=secret,
            market=args.market,
            krw=float(args.krw),
            interval_sec=int(args.interval),
            tp_ratio=float(args.tp),
            firestore_credential_path=args.firestore_credential,
            dry_run=bool(args.dry_run),
            min_krw_balance=float(args.min_krw_balance),
            skip_buy_within_ratio=float(args.skip_buy_within),
            buy_fill_timeout_sec=float(args.fill_timeout),
            max_order_count=int(args.max_order_count),
        )
