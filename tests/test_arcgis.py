import requests
import json
from utils.arcgis_rest import upload_features_via_rest


class DummyResp:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body or {"addResults": [{"success": True}]}

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise requests.HTTPError(f"status {self.status_code}")

    def json(self):
        return self._body


def test_upload_features_via_rest_monkeypatch(monkeypatch):
    called = {}

    def fake_post(url, data=None, headers=None, timeout=None):
        called["url"] = url
        called["data"] = data
        return DummyResp()

    monkeypatch.setattr(requests, "post", fake_post)

    features = [
        {"geometry": {"x": 1.0, "y": 2.0, "spatialReference": {"wkid": 4326}}, "attributes": {"d_date": "2025-10-17"}}
    ]
    res = upload_features_via_rest(features, "https://example.com/FeatureServer/0", token="T", batch_size=10)
    assert res["success"] is True
    assert len(res["batches"]) == 1
    assert "response" in res["batches"][0]
