import json
import time
import requests
from typing import List, Dict, Optional


def upload_features_via_rest(
    features: List[Dict],
    feature_layer_url: str,
    token: Optional[str] = None,
    batch_size: int = 200,
    sleep_between_batches: float = 0.5,
    timeout: int = 60,
) -> Dict:
    """
    Upload features to ArcGIS Feature Layer via REST addFeatures.
    """
    url = feature_layer_url.rstrip("/") + "/addFeatures"
    headers = {"Accept": "application/json"}
    results = {"success": True, "batches": []}

    for i in range(0, len(features), batch_size):
        chunk = features[i : i + batch_size]
        payload = {
            "f": "json",
            "features": json.dumps(chunk, ensure_ascii=False),
        }
        if token:
            payload["token"] = token

        try:
            resp = requests.post(url, data=payload, headers=headers, timeout=timeout)
            resp.raise_for_status()
            j = resp.json()
        except Exception as e:
            results["success"] = False
            results["batches"].append({"index": i // batch_size, "ok": False, "error": str(e)})
            break

        results["batches"].append({"index": i // batch_size, "ok": True, "response": j})
        time.sleep(sleep_between_batches)

    return results
