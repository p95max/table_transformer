import datetime
from fastapi.testclient import TestClient

import api.app as app_module
from api.app import app

client = TestClient(app)


class FakeCursor:
    def __init__(self, rows=None, count=None):
        self._rows = rows or []

        self._count = count if count is not None else len(self._rows)
        self._executed = []

    def execute(self, q, params=None):
        try:
            q_str = str(q)
        except Exception:
            q_str = repr(q)
        self._executed.append((q_str, params))

    def fetchall(self):
        return self._rows

    def fetchone(self):
        last_sql = self._executed[-1][0] if self._executed else ""
        last_sql_up = last_sql.upper()

        if "COUNT(" in last_sql_up:
            return (self._count,)

        if self._rows:
            return self._rows[0]

        return None

    def close(self):
        pass


class FakeConn:
    def __init__(self, rows=None, count=None):
        self._cursor = FakeCursor(rows=rows, count=count)

    def cursor(self):
        return self._cursor


class FakePool:
    def __init__(self, rows=None, count=None):
        self._rows = rows
        self._count = count

    def getconn(self):
        return FakeConn(rows=self._rows, count=self._count)

    def putconn(self, conn):
        pass

    def closeall(self):
        pass


def test_root_redirects_to_docs():
    r = client.get("/")
    assert r.status_code in (200, 307, 302)
    if r.status_code in (307, 302):
        assert "/docs" in r.headers["location"]


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_features_geojson_with_fake_pool(monkeypatch):
    geom_json = '{"type":"Point","coordinates":[12.34,56.78]}'
    row = (1, datetime.date(2025, 10, 17), "Reg", "City", 12.34, 56.78, geom_json)
    fake_pool = FakePool(rows=[row], count=1)
    monkeypatch.setattr(app_module, "pool", fake_pool)

    r = client.get("/features.geojson")
    assert r.status_code == 200
    data = r.json()
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 1
    assert data["features"][0]["properties"]["id"] == 1
    assert data["meta"]["total"] == 1


def test_feature_not_found(monkeypatch):
    fake_pool = FakePool(rows=[], count=0)
    monkeypatch.setattr(app_module, "pool", fake_pool)
    r = client.get("/feature/9999")
    assert r.status_code == 404


def test_feature_found(monkeypatch):
    geom_json = '{"type":"Point","coordinates":[13.0,57.0]}'
    row = (5, datetime.date(2025, 10, 18), "R2", "C2", 13.0, 57.0, geom_json)
    fake_pool = FakePool(rows=[row], count=1)
    monkeypatch.setattr(app_module, "pool", fake_pool)
    r = client.get("/feature/5")
    assert r.status_code == 200
    data = r.json()
    assert data["properties"]["id"] == 5
    assert data["geometry"]["coordinates"] == [13.0, 57.0]
