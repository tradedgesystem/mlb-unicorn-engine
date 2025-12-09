from fastapi.testclient import TestClient

from backend.app.api.main import app

client = TestClient(app)


def test_top50():
    response = client.get("/top50/2025-03-27")
    print("Status:", response.status_code)
    print("JSON:", response.json())


if __name__ == "__main__":
    test_top50()
