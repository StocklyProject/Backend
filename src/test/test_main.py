from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

# CI 테스트를 위한 코드
def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "메인페이지입니다"}
