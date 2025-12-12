import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter
import time
import os

class FirestoreCache:
    """Firestore 읽기 요청을 줄이기 위한 로컬 인메모리 캐시"""
    def __init__(self, db_instance):
        self.db = db_instance
        self._cache = {}

    def load_all_pending(self):
        """시작 시 'done'이 아닌 모든 거래를 로드하여 캐시를 채웁니다."""
        print("Firestore에서 모든 미완료 거래를 로드하여 캐시를 초기화합니다...")
        pending_trades = self.db.get_all_pending_trades()
        for trade in pending_trades:
            buy_uuid = trade.get('buy_uuid')
            if buy_uuid:
                self._cache[buy_uuid] = trade
        print(f"{len(self._cache)}개의 미완료 거래가 캐시되었습니다.")

    def upsert_trade(self, data: dict):
        """캐시와 Firestore에 모두 데이터를 업데이트/삽입합니다."""
        buy_uuid = data.get('buy_uuid')
        if not buy_uuid:
            return False
        
        # 1. 로컬 캐시 업데이트
        self._cache[buy_uuid] = data
        
        # 2. Firestore에 업데이트 (write-through)
        return self.db.upsert_trade(data)

    def get_waiting_trades_by_market(self, market: str) -> list[dict]:
        """캐시에서 특정 market의 'waiting' 상태인 모든 거래를 조회합니다."""
        results = []
        for trade in self._cache.values():
            if trade.get('market') == market and trade.get('state') == 'waiting':
                results.append(trade)
        return results
    
    def get_waiting_loss_trades_by_market(self, market: str) -> list[dict]:
        """캐시에서 특정 market의 'waiting' 상태인 모든 거래를 조회합니다."""
        results = []
        for trade in self._cache.values():
            if trade.get('market') == market and trade.get('state') == 'waiting' and trade.get('buy_price') > trade.get('sell_price'):
                results.append(trade)
        return results

    def get_min_price_waiting_trade(self, market: str) -> dict | None:
        """캐시에서 특정 market의 'waiting' 상태인 거래 중 가장 낮은 매수가를 가진 거래를 조회합니다."""
        waiting_trades = self.get_waiting_trades_by_market(market)
        if not waiting_trades:
            return None
        
        # buy_price가 가장 낮은 항목 찾기
        min_price_trade = min(waiting_trades, key=lambda t: t.get('sell_price', float('inf')))
        return min_price_trade
    
    def get_waiting_trade_count_all_market(self) -> int:
        results = []
        for trade in self._cache.values():
            if trade.get('state') == 'waiting':
                results.append(trade)
        return len(results)

    def get_waiting_trades_count_by_market(self, market: str) -> int:
        """캐시에서 특정 market의 'waiting' 상태인 거래의 개수를 조회합니다."""
        waiting_trades = self.get_waiting_trades_by_market(market)
        return len(waiting_trades)

    def get_max_price_waiting_trade(self, market: str) -> dict | None:
        """캐시에서 특정 market의 'waiting' 상태인 거래 중 가장 높은 매도가를 가진 거래를 조회합니다."""
        waiting_trades = self.get_waiting_trades_by_market(market)
        if not waiting_trades:
            return None
        
        # sell_price가 가장 높은 항목 찾기
        max_price_trade = max(waiting_trades, key=lambda t: t.get('sell_price', 0))
        return max_price_trade

class FirestoreTradeDB:
    
    def __init__(self, credential_path: str, collection_name: str = "trades"):
        """
        Firebase Admin SDK를 초기화하고 Firestore 클라이언트를 준비합니다.

        :param credential_path: 다운로드한 서비스 계정 JSON 키 파일 경로
        :param collection_name: 사용할 Firestore 컬렉션 이름 (예: 'trades')
        """
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(credential_path)
                firebase_admin.initialize_app(cred)
                print("Firebase Admin SDK가 초기화되었습니다.")
            
            self.db = firestore.client()
            self.trades_ref = self.db.collection(collection_name)
            print(f"Firestore 컬렉션 '{collection_name}'에 연결되었습니다.")
            
        except FileNotFoundError:
            print(f"오류: 서비스 계정 키 파일을 찾을 수 없습니다. 경로: {credential_path}")
            raise
        except Exception as e:
            print(f"Firebase 초기화 오류: {e}")
            raise

    def upsert_trade(self, data: dict):
        """
        데이터를 삽입(INSERT) 또는 업데이트(UPDATE)합니다.
        'buy_uuid'를 Firestore 문서(Document) ID로 사용합니다.
        :param data: 모든 필드 값이 담긴 딕셔너리
        """
        try:
            doc_id = data.get('buy_uuid')
            if not doc_id:
                print("오류: 'buy_uuid'가 데이터에 포함되어야 합니다.")
                return False
            self.trades_ref.document(doc_id).set(data, merge=True)
            return True
        except Exception as e:
            print(f"Firestore Upsert 오류 ({data.get('buy_uuid')}): {e}")
            return False

    def get_all_pending_trades(self) -> list[dict]:
        """'done' 상태가 아닌 모든 거래 내역을 리스트로 반환합니다."""
        try:
            query = self.trades_ref.where(filter=FieldFilter('state', '!=', 'done'))
            results = query.stream()
            return [doc.to_dict() for doc in results]
        except Exception as e:
            print(f"Firestore '전체 미완료 목록 조회' 오류: {e}")
            print("[알림] 이 쿼리는 Firestore 색인이 필요할 수 있습니다. 오류 메시지의 URL을 확인하세요.")
            return []

    def get_waiting_trades_by_market(self, market: str) -> list[dict]:
        """
        [캐시로 대체됨] 특정 market의 'state'가 'waiting'인 모든 거래 내역을 리스트로 반환합니다.
        """
        # 이 함수는 이제 FirestoreCache에 의해 처리되므로 직접 호출되지 않아야 합니다.
        print("경고: get_waiting_trades_by_market이 DB에서 직접 호출되었습니다. 캐시를 사용하세요.")
        return []

    def get_min_price_waiting_trade(self, market: str) -> dict | None:
        """
        [캐시로 대체됨] 특정 market의 'state'가 'waiting'인 항목 중 가장 낮은 매수가를 가진 거래를 조회합니다.
        """
        # 이 함수는 이제 FirestoreCache에 의해 처리되므로 직접 호출되지 않아야 합니다.
        print("경고: get_min_price_waiting_trade가 DB에서 직접 호출되었습니다. 캐시를 사용하세요.")
        return None