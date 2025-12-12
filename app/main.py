
# -*- coding: utf-8 -*-
import argparse
from .config import Settings
from .trade import run_loop
from .firestore_trade_db import FirestoreTradeDB, FirestoreCache


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="업비트 자동 매수/익절 매도 봇")
    p.add_argument("--market", required=True, nargs='+', help="예: KRW-BTC KRW-ETH")
    p.add_argument("--krw", type=float, required=True, help="주기마다 시장가로 매수할 KRW 금액 (예: 10000)")
    p.add_argument("--tp", type=float, required=True, help="매도조건: +X%% 익절 (예: 1.0 => +1%%)")
    p.add_argument("--interval", type=int, default=60, help="동작 주기(초)")
    p.add_argument("--firestore-credential", type=str, default="serviceAccountKey.json", help="Firestore 서비스 계정 키 파일 경로")
    p.add_argument("--dry-run", action="store_true", help="실거래 대신 모의 주문만 수행")
    p.add_argument("--min-krw-balance", type=float, default=5000.0, help="최소 주문 금액 (기본 5000 KRW)")
    p.add_argument(
        "--skip-buy-within",
        type=float,
        default=0.3,
        help="이전 매수가 대비 X%% 이내면 이번 주기는 매수 스킵 (0이면 항상 매수)",
    )
    p.add_argument(
        "--fill-timeout",
        type=float,
        default=30.0,
        help="매수 주문 체결 대기 타임아웃(초). 0 이하이면 즉시 종료.",
    )
    p.add_argument(
        "--max-order-count",
        type=int,
        default=10,
        help="매도 최대 대기 갯수"
    )
    return p


def main():
    args = build_parser().parse_args()
    cfg = Settings.from_env_and_args(args)
    db = FirestoreTradeDB(credential_path=cfg.firestore_credential_path)
    
    # 캐시 초기화
    cache = FirestoreCache(db)
    cache.load_all_pending()

    run_loop(cfg, cache)


if __name__ == "__main__":
    main()
