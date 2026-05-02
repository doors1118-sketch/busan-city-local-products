import pytest
from fastapi.testclient import TestClient
import api_server

@pytest.fixture
def client():
    # 실제 DB 및 api_cache.json이 존재하는 통합 환경
    return TestClient(api_server.app)

def test_integration_dashboard_ranking(client):
    """기존 대시보드 API 회귀 테스트 (충돌 없음 확인)"""
    resp = client.get("/api/ranking")
    assert resp.status_code == 200
    data = resp.json()
    # 기존 API 키 유지 여부
    assert "전체" in data
    assert "분야별" in data
    
def test_integration_debug_db_status(client):
    """DB_PATH 누락 패치 후 debug API 정상 동작 여부 통합 테스트"""
    resp = client.get("/api/debug/db-status")
    assert resp.status_code == 200
    data = resp.json()
    # 실제 procurement_contracts.db 기반으로 응답하는지
    assert "용역" in data
    assert "공사" in data
