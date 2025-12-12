
# -*- coding: utf-8 -*-
import logging
import time
from typing import Any, Dict, Optional, Tuple

from .config import Settings
from .upbit_client import UpbitClient
from .util import round_price_to_tick, round_volume
from .firestore_trade_db import FirestoreTradeDB, FirestoreCache

log = logging.getLogger("trade")


def run_once(cfg: Settings, client: UpbitClient, db: "FirestoreCache", market: str, last_buy_price: Optional[float] = None) -> Optional[float]:
    auto_price_mode = cfg.krw == 0
    all_order_count = db.get_waiting_trade_count_all_market()

    skip_buy_within_ratio = 0.25
    tp_ratio = 1.2
    if auto_price_mode:
        log.warning(f"자동 거래 금액 모드 입니다. 현재 대기중인 주문수는 {all_order_count}개 입니다.")
        if all_order_count < 10:
            skip_buy_within_ratio = 0.2
            tp_ratio = 2.0
        elif all_order_count < 30:
            skip_buy_within_ratio = 0.25
            tp_ratio = 1.2
        elif all_order_count < 60:
            skip_buy_within_ratio = 0.5
            tp_ratio = 1.0
        elif all_order_count < 80:
            skip_buy_within_ratio = 1.0
            tp_ratio = 1.5
        else:
            skip_buy_within_ratio = 1.5
            tp_ratio = 2.0

    check_pending_sell_orders(cfg, client, db, market)
    all_pending_count = db.get_waiting_trade_count_all_market()
    log.info(f"현재 대기중 전체 거래 갯수 {all_pending_count}")
    
    price = client.get_current_price(market)
    log.info(f"[{market}] 현재가: {price:.8f} KRW")
    
    # Firestore에서 대기중인 가장 낮은 매수가를 가져와 비교
    waiting_count = db.get_waiting_trades_count_by_market(market)
    if waiting_count < 15:
        _modify_loss_order(cfg, client, db, market)

    if waiting_count > 0:
        if skip_buy_within_ratio > 0:
            min_price_trade = db.get_min_price_waiting_trade(market)
            log.info(f"Firestore 최저가 거래 정보: {min_price_trade}")
            if min_price_trade:
                lowest_buy_price = min_price_trade.get('buy_price')
                lowest_sell_price = min_price_trade.get('sell_price')
                log.info(f"buy_price: {lowest_buy_price}, sell_price: {lowest_sell_price}")
                if lowest_buy_price and lowest_buy_price > 0:
                    diff_ratio = abs(price - lowest_sell_price) / lowest_sell_price * 100.0
                    if waiting_count > 1:
                        if diff_ratio <= skip_buy_within_ratio + tp_ratio:
                            log.info(
                                f"현재가({price:.8f})가 Firestore의 최저 매수가({lowest_buy_price:.8f}) 대비 "
                                f"변동 {diff_ratio:.4f}% <= {skip_buy_within_ratio:.4f}% 이므로 매수를 건너뜁니다."
                            )
                            return last_buy_price
                    if waiting_count == 1:
                        if diff_ratio <= skip_buy_within_ratio + tp_ratio and diff_ratio > skip_buy_within_ratio:
                            log.info(
                                f"현재가({price:.8f})가 Firestore의 최저 매수가({lowest_buy_price:.8f}) 대비 "
                                f"변동 {diff_ratio:.4f}% <= {skip_buy_within_ratio:.4f}% 이므로 매수를 건너뜁니다."
                            )
                            return last_buy_price

    # 최소 주문금액 체크
    krw_balance = client.get_krw_balance()
    log.info(f"보유 KRW: {krw_balance:.8f} KRW")

    if auto_price_mode:
        if krw_balance < 10000:
            log.warning(f"보유 KRW({krw_balance:.0f}) 이므로 매수를 건너뛰고, 기존 주문 변경을 시도합니다.")
            _modify_highest_price_order(cfg, client, db, market, price)
            return last_buy_price
    else:
        if krw_balance < cfg.krw or waiting_count > cfg.max_order_count:
            log.warning(f"보유 KRW({krw_balance:.0f}) < 구매금액({cfg.krw:.0f}) 또는 최대 주문 개수 초과({waiting_count} > {cfg.max_order_count})이므로 매수를 건너뛰고, 기존 주문 변경을 시도합니다.")
            _modify_highest_price_order(cfg, client, db, market, price)
            return last_buy_price

    # 1) 시장가 매수

    order_price = 10000
    if auto_price_mode:
        #all_order_count = db.get_waiting_trade_count_all_market()
        log.warning(f"자동 거래 금액 모드 입니다. 현재 대기중인 주문수는 {all_order_count}개 입니다.")
        if all_order_count >= 100:
            order_price = krw_balance // 10000 * 10000
            log.warning(f"대기중인 주문이 90개 이상으로, 남은 잔액 {order_price} 만큰 주문합니다.")
        else:
            if all_order_count < 10:
                order_price = (krw_balance / (100 - all_order_count)) // 10000 * 10000
                log.warning(f"남은 잔액 ({krw_balance})과 가능한 주문수 ${100 -all_order_count}개 비례하여 {order_price} 만큼 주문합니다.")
            elif all_order_count < 30:
                order_price = (krw_balance / (70 - all_order_count)) // 10000 * 10000
                log.warning(f"남은 잔액 ({krw_balance})과 가능한 주문수 ${100 -all_order_count}개 비례하여 {order_price} 만큼 주문합니다.")
            elif all_order_count < 60:
                order_price = (krw_balance / (80 - all_order_count)) // 10000 * 10000
                log.warning(f"남은 잔액 ({krw_balance})과 가능한 주문수 ${100 -all_order_count}개 비례하여 {order_price} 만큼 주문합니다.")
            elif all_order_count < 80:
                order_price = (krw_balance / (90 - all_order_count)) // 10000 * 10000
                log.warning(f"남은 잔액 ({krw_balance})과 가능한 주문수 ${100 -all_order_count}개 비례하여 {order_price} 만큼 주문합니다.")
            else:
                order_price = (krw_balance / (100 - all_order_count)) // 10000 * 10000
                log.warning(f"남은 잔액 ({krw_balance})과 가능한 주문수 ${100 -all_order_count}개 비례하여 {order_price} 만큼 주문합니다.")
    else:
        order_price = cfg.krw

    buy_res = client.buy_market(market, order_price)
    log.info(f"시장가 매수 요청: {buy_res}")
    buy_uuid = buy_res.get("uuid")
    if not buy_uuid:
        log.error("매수 주문 응답에 uuid가 없어 매도를 진행할 수 없습니다: %s", buy_res)
        return last_buy_price

    # 1-1) 체결 결과 대기 (시장가 주문이므로 보통 즉시 완료되지만 부분체결 고려)
    executed_volume: Optional[float]
    avg_buy_price: Optional[float]
    buy_amount: Optional[float]
    if cfg.dry_run:
        executed_volume = round_volume(cfg.krw / price, 8)
        avg_buy_price = price
        buy_amount = cfg.krw
    else:
        executed_volume, avg_buy_price, buy_amount = wait_for_buy_fill(cfg, client, buy_uuid)
        if executed_volume is None or executed_volume <= 0:
            log.warning("매수 주문 체결 정보를 가져오지 못해 매도를 건너뜁니다. uuid=%s", buy_uuid)
            return last_buy_price

    log.info(
        "매수 체결 결과: volume=%.8f, avg_price=%.8f, buy_amount=%.0f",
        executed_volume,
        avg_buy_price if avg_buy_price is not None else 0,
        buy_amount if buy_amount is not None else 0,
    )

    volume = round_volume(executed_volume, 8)
    # 2) 목표가 계산 및 지정가 매도
    base_price_for_tp = avg_buy_price if avg_buy_price is not None else price
    target_price = round_price_to_tick(
        base_price_for_tp * (1.0 + tp_ratio * 0.01),
        method='up',
        market=market,
    )
    sell_res = client.sell_limit(market, volume, target_price)
    log.info(f"익절 지정가 매도 요청: price={target_price}, volume={volume} -> {sell_res}")

    # 매수/매도 거래 정보를 Firestore에 기록
    trade_data = {
        'buy_uuid': buy_uuid,
        'buy_price': int(round(avg_buy_price)) if avg_buy_price is not None else 0,
        'buy_quantity': executed_volume,
        'buy_amount': round(buy_amount, 2) if buy_amount is not None else 0.0,
        'buy_create_time': int(time.time()),
        'sell_uuid': sell_res.get('uuid'),
        'sell_price': target_price,
        'sell_amount': None,
        'sell_complete_time': None,
        'state': 'waiting',  # Firestore에 저장할 통일된 상태값
        'market': market,
    }
    db.upsert_trade(trade_data)
    log.info(f"Firestore에 거래 정보 업데이트: buy_uuid={buy_uuid}")

    return avg_buy_price if avg_buy_price is not None else price


def wait_for_buy_fill(cfg: Settings, client: UpbitClient, uuid: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """매수 주문이 완전히 체결될 때까지 대기하고 (체결 수량, 평단, 총 비용)을 반환."""
    timeout_sec = max(cfg.buy_fill_timeout_sec, 0.0)
    poll_interval = 1.0
    deadline = time.time() + timeout_sec if timeout_sec > 0 else time.time()
    last_state = None
    last_executed_volume = 0.0
    last_avg_price: Optional[float] = None
    last_total_cost: Optional[float] = None

    while True:
        order = client.get_order(uuid)
        if not order:
            log.warning("주문 정보를 조회하지 못했습니다. uuid=%s", uuid)
            return None, None, None

        state = order.get("state")
        executed_volume_str = order.get("executed_volume") or "0"
        try:
            executed_volume = float(executed_volume_str)
        except (TypeError, ValueError):
            executed_volume = 0.0
        avg_price, total_cost = compute_order_details(order)

        if state != last_state:
            log.info(f"매수 체결 상태: state={state}, executed_volume={executed_volume}")
            last_state = state

        last_executed_volume = executed_volume or last_executed_volume
        last_avg_price = avg_price or last_avg_price
        last_total_cost = total_cost or last_total_cost

        if state == "done":
            return executed_volume, avg_price, total_cost
        if state in {"cancel", "error"}:
            if executed_volume > 0:
                log.info("주문 상태=%s이지만 부분 체결된 수량을 사용합니다. uuid=%s", state, uuid)
                return executed_volume, avg_price, total_cost
            log.warning("주문이 정상 체결되지 않았습니다. state=%s, uuid=%s", state, uuid)
            return None, None, None

        if timeout_sec > 0 and time.time() > deadline:
            if last_executed_volume > 0:
                log.warning(
                    "체결 대기 타임아웃 이후 부분 체결된 수량을 사용합니다. executed_volume=%.8f, uuid=%s",
                    last_executed_volume,
                    uuid,
                )
                return last_executed_volume, last_avg_price, last_total_cost
            log.warning(
                "주문 체결을 기다리는 동안 타임아웃이 발생했습니다. state=%s, executed_volume=%s, uuid=%s",
                state,
                executed_volume,
                uuid,
            )
            return None, None, None

        time.sleep(poll_interval)


def compute_order_details(order: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """주문 응답에서 (체결 평단, 수수료를 포함한 총 금액)을 계산."""
    trades = order.get("trades") or []
    if not trades:
        return None, None

    total_funds = 0.0
    total_volume = 0.0
    for trade in trades:
        try:
            price = float(trade.get("price") or 0.0)
            volume = float(trade.get("volume") or 0.0)
            funds = float(trade.get("funds") or (price * volume))
        except (TypeError, ValueError):
            continue
        total_funds += funds
        total_volume += volume

    if total_volume == 0:
        return None, None

    avg_price = total_funds / total_volume
    
    try:
        paid_fee = float(order.get('paid_fee') or 0.0)
    except (TypeError, ValueError):
        paid_fee = 0.0

    side = order.get('side')
    final_amount = 0.0
    if side == 'bid':  # 매수
        final_amount = total_funds + paid_fee
    elif side == 'ask':  # 매도
        final_amount = total_funds - paid_fee

    return avg_price, final_amount


def check_pending_sell_orders(cfg: Settings, client: UpbitClient, db: "FirestoreCache", market: str):
    """
    시작시 대기중인 미체결 매도 주문들을 확인하고 상태를 업데이트합니다.
    """
    log.info(f"--- [{market}] 대기중인 매도 주문 확인 시작 ---")
    # 1. 시작시 현재 대기중인 거래들이 있는지 목록을 가져온다.
    pending_trades = db.get_waiting_trades_by_market(market)

    # 2. 목록이 하나 이상이라면 다음 동작들을 수행한다.
    if not pending_trades:
        log.info("대기중인 매도 주문이 없습니다.")
        return

    log.info(f"{len(pending_trades)}개의 대기중인 매도 주문을 확인합니다.")
    
    # 3. 목록을 sell_price가 낮은 순서대로 정렬한다.
    # Firestore에서 'sell_price' 필드가 있다고 가정합니다.
    try:
        pending_trades.sort(key=lambda t: t.get('sell_price', float('inf')))
    except (TypeError, ValueError):
        log.error("Firestore에 'sell_price' 필드가 없거나 숫자 형식이 아니어서 정렬에 실패했습니다.")
        return

    for trade in pending_trades:
        sell_uuid = trade.get('sell_uuid')
        if not sell_uuid:
            log.warning(f"거래에 sell_uuid가 없어 상태를 확인할 수 없습니다: {trade.get('buy_uuid')}")
            continue

        # 4. sell_uuid로 upbit에서 체결 상태를 확인한다.
        log.info(f"주문 확인: sell_uuid={sell_uuid}")
        order = client.get_order(sell_uuid)
        if not order:
            log.warning(f"Upbit에서 주문 정보를 가져오지 못했습니다: {sell_uuid}")
            continue

        state = order.get('state')
        log.info(f"  -> 현재 상태: {state}")

        # 5. 체결이 done 또는 cancel이 되면 목록을 업데이트 한다.
        if state in {'done', 'cancel'}:
            _, sell_amount = compute_order_details(order)
            trade['state'] = state
            trade['sell_amount'] = round(sell_amount, 2) if sell_amount is not None else 0.0
            trade['sell_complete_time'] = int(time.time())
            db.upsert_trade(trade)
            log.info(f"  -> Firestore 상태 업데이트: {state}, sell_amount: {sell_amount}")
        
        # 6. 체결이 waiting 이라면 확인을 중단한다.
        #    (가장 낮은 가격의 매도 주문이 아직 대기중이므로, 더 비싼 주문들은 확인할 필요가 없음)
        elif state == 'wait':
            log.info("가장 낮은 가격의 매도 주문이 아직 대기중이므로 확인을 중단합니다.")
            break
    log.info("--- 대기중인 매도 주문 확인 완료 ---")


def _modify_highest_price_order(cfg: Settings, client: UpbitClient, db: "FirestoreCache", market: str, current_price: float):
    """보유 KRW가 부족할 때 가장 높은 가격의 매도 주문을 현재가 기준으로 변경"""
    log.info(f"[{market}] 기존 주문 변경을 시도합니다.")
    
    # 1. 가장 높은 가격의 매도 주문 가져오기
    trade_to_modify = db.get_min_price_waiting_trade(market)
    if not trade_to_modify:
        log.info("변경할 대기 중인 매도 주문이 없습니다.")
        return

    old_sell_uuid = trade_to_modify.get('sell_uuid')
    buy_quantity = trade_to_modify.get('buy_quantity')

    if not old_sell_uuid or not buy_quantity:
        log.error(f"주문 변경에 필요한 정보(sell_uuid, buy_quantity)가 부족합니다: {trade_to_modify.get('buy_uuid')}")
        return

    # 2. 기존 주문 취소
    try:
        log.info(f"기존 매도 주문 취소를 시도합니다: {old_sell_uuid}")
        cancel_res = client.cancel_order(old_sell_uuid)
        log.info(f"주문 취소 완료: {cancel_res.get('uuid')}")
    except Exception as e:
        # 이미 체결되었거나 취소된 경우 오류가 발생할 수 있음
        log.error(f"주문 취소 중 오류 발생 (이미 처리되었을 수 있음): {e}")
        return

    # 3. 현재가 기준으로 새 매도가 계산 및 재주문
    new_sell_price = round_price_to_tick(
        current_price * (1.0 + cfg.tp_ratio * 0.01),
        method='up',
        market=market,
    )
    
    log.info(f"새로운 매도 주문을 시도합니다. 가격: {new_sell_price}")
    new_sell_res = client.sell_limit(market, buy_quantity, new_sell_price)
    new_sell_uuid = new_sell_res.get('uuid')
    if not new_sell_uuid:
        log.error("새로운 매도 주문에 실패했습니다.")
        return
    
    # 4. 캐시 및 Firestore 정보 업데이트
    trade_to_modify['sell_uuid'] = new_sell_uuid
    trade_to_modify['sell_price'] = new_sell_price
    db.upsert_trade(trade_to_modify)
    
    log.info(f"주문 변경 완료: {old_sell_uuid} -> {new_sell_uuid} (새로운 가격: {new_sell_price})")

def _modify_loss_order(cfg: Settings, client: UpbitClient, db: "FirestoreCache", market: str):
    """보유 KRW가 부족할 때 가장 높은 가격의 매도 주문을 현재가 기준으로 변경"""
    log.info(f"[{market}] 기존 주문 변경을 시도합니다.")
    
    # 1. 가장 높은 가격의 매도 주문 가져오기
    trades_to_modify = db.get_waiting_loss_trades_by_market(market)
    if not trades_to_modify:
        log.info("변경할 대기 중인 매도 주문이 없습니다.")
        return
    
    for trade_to_modify in trades_to_modify:

        old_sell_uuid = trade_to_modify.get('sell_uuid')
        buy_quantity = trade_to_modify.get('buy_quantity')

        if not old_sell_uuid or not buy_quantity:
            log.error(f"주문 변경에 필요한 정보(sell_uuid, buy_quantity)가 부족합니다: {trade_to_modify.get('buy_uuid')}")
            continue

        # 2. 기존 주문 취소
        try:
            log.info(f"기존 매도 주문 취소를 시도합니다: {old_sell_uuid}")
            cancel_res = client.cancel_order(old_sell_uuid)
            log.info(f"주문 취소 완료: {cancel_res.get('uuid')}")
        except Exception as e:
            # 이미 체결되었거나 취소된 경우 오류가 발생할 수 있음
            log.error(f"주문 취소 중 오류 발생 (이미 처리되었을 수 있음): {e}")
            return

        # 3. 현재가 기준으로 새 매도가 계산 및 재주문
        new_sell_price = round_price_to_tick(
            trade_to_modify.get('buy_price') * (1.0 + cfg.tp_ratio * 0.01),
            method='up',
            market=market,
        )
        
        log.info(f"새로운 매도 주문을 시도합니다. 가격: {new_sell_price}")
        new_sell_res = client.sell_limit(market, buy_quantity, new_sell_price)
        new_sell_uuid = new_sell_res.get('uuid')
        if not new_sell_uuid:
            log.error("새로운 매도 주문에 실패했습니다.")
            return
        
        # 4. 캐시 및 Firestore 정보 업데이트
        trade_to_modify['sell_uuid'] = new_sell_uuid
        trade_to_modify['sell_price'] = new_sell_price
        db.upsert_trade(trade_to_modify)
        
        log.info(f"주문 변경 완료: {old_sell_uuid} -> {new_sell_uuid} (새로운 가격: {new_sell_price})")

def run_loop(cfg: Settings, db: "FirestoreCache") -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    log.info("=== 업비트 자동 매수/익절 매도 루프 시작 ===")
    log.info(
        f"설정: market={','.join(cfg.market)}, krw={cfg.krw}, interval={cfg.interval_sec}s, "
        f"tp={cfg.tp_ratio}% skip_within={cfg.skip_buy_within_ratio}% fill_timeout={cfg.buy_fill_timeout_sec}s dry_run={cfg.dry_run}"
    )
    client = UpbitClient(cfg.access_key, cfg.secret_key, dry_run=cfg.dry_run)
    
    last_buy_prices: Dict[str, Optional[float]] = {m: None for m in cfg.market}

    try:
        while True:
            for market in cfg.market:
                try:
                    time.sleep(5)
                    last_buy_prices[market] = run_once(cfg, client, db, market, last_buy_prices.get(market))
                except Exception as e:
                    log.exception(f"[{market}] 사이클 오류: {e}")
            time.sleep(cfg.interval_sec)
    except KeyboardInterrupt:
        log.info("종료 신호를 받아 루프를 종료합니다.")
