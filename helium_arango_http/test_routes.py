from fastapi.testclient import TestClient
from server import app


TEST_ACCOUNT = '13aSUnDYLkJRWBPSwqFPDvbZ9pvF9g5KByq7msrbGTeE9EezsZB'
TEST_HOTSPOT = '112nUEtrKPrgWtczpbira7aDU5271qduDfJm8Y2WSXjWwpkAHdgJ'

client = TestClient(app)


def test_routes():
    response = client.get(f'/payments/{TEST_ACCOUNT}/from')
    assert response.status_code == 200

    response = client.get(f'/payments/{TEST_ACCOUNT}/to')
    assert response.status_code == 200

    response = client.get(f'/payments/totals', params={'limit': 10})
    assert response.status_code == 200
    assert len(response.json()) == 10

    response = client.get(f'/payments/counts', params={'limit': 10})
    assert response.status_code == 200
    assert len(response.json()) == 10

    response = client.get(f'/payments/payers', params={'limit': 10})
    assert response.status_code == 200
    assert len(response.json()) == 10

    response = client.get(f'/payments/payees', params={'limit': 10})
    assert response.status_code == 200
    assert len(response.json()) == 10

    response = client.get(f'/payments/payers/graph', params={'limit': 10})
    assert response.status_code == 200
    try:
        data = response.json()
        nodes, edges = data['nodes'], data['edges']
    except KeyError:
        raise AssertionError

    response = client.get(f'/payments/payees/graph', params={'limit': 10})
    assert response.status_code == 200
    try:
        data = response.json()
        nodes, edges = data['nodes'], data['edges']
    except KeyError:
        raise AssertionError

    response = client.get(f'/hotspots/coords/graph', params={'lat': 40.689306, 'lon': -74.044500})
    assert response.status_code == 200
    try:
        data = response.json()
        nodes, edges = data['nodes'], data['edges']
    except KeyError:
        raise AssertionError

    response = client.get(f'/hotspots/hex/graph', params={'hex': '862a84707ffffff'})
    assert response.status_code == 200
    try:
        data = response.json()
        nodes, edges = data['nodes'], data['edges']
    except KeyError:
        raise AssertionError

    response = client.get(f'/hotspots/hex/graph', params={'hex': 'not a hex!'})
    assert response.status_code == 400

    response = client.get(f'/hotspots/{TEST_HOTSPOT}/outbound')
    assert response.status_code == 200

    response = client.get(f'/hotspots/{TEST_HOTSPOT}/inbound')
    assert response.status_code == 200

    response = client.get(f'/hotspots/receipts', params={'limit': 50})
    assert response.status_code == 200

    response = client.get(f'/hotspots/receipts', params={'address': TEST_HOTSPOT, 'limit': 50})
    assert response.status_code == 200











