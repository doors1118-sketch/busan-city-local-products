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
    

