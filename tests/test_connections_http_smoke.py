def test_connections_catalog_reachable(auth_client):
    r = auth_client.get("/api/connections/catalog")
    assert r.status_code == 200, r.text
    assert len(r.json()["connectors"]) >= 10

def test_keypay_status_route_registered(auth_client):
    r = auth_client.get("/api/keypay/status", params={"venue_id": "demo-venue"})
    assert r.status_code == 200, r.text

def test_direct_upload_route_registered(auth_client):
    import base64
    csv = base64.b64encode(b"date,covers\n2026-06-22,50\n").decode()
    r = auth_client.post("/api/reservations/direct/upload", json={"venue_id": "demo-venue", "csv_data": csv})
    assert r.status_code == 200, r.text


def test_generic_custom_connect_via_http(auth_client):
    # create a venue for this owner
    auth_client.post("/venues", json={"id": "hubv", "name": "HubV", "state": "vic",
                                       "max_labour_pct": 30, "tanda_org_id": "h1",
                                       "created_at": "2026-06-20T00:00:00"})
    r = auth_client.post("/api/connections/custom/connect",
                         json={"venue_id": "hubv", "api_key": "k-123", "base_url": "https://api.example.com"})
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "success"
    # status now reflects connected
    s = auth_client.get("/api/connections/venue/hubv")
    custom = [c for c in s.json()["connectors"] if c["key"] == "custom"][0]
    assert custom["status"] == "connected"
