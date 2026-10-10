"""Punches come from a venue's time clock device, not a person's login: tests
that clock people in and out set one up first, the way a manager does."""


def kiosk_headers(client, manager_headers, venue_id, name=""):
    r = client.post("/api/kiosk/devices", json={"venue_id": venue_id, "name": name},
                    headers=manager_headers)
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}
