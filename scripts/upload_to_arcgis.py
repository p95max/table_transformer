from __future__ import annotations
import os
import json
import time
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime
import math

try:
    from arcgis.gis import GIS
    from arcgis.features import FeatureLayer
except Exception as exc:
    raise SystemExit("arcgis package is required: pip install arcgis") from exc


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--features", required=True, help="Path to prepared JSON features (features have 'attributes' and 'wkt' or long/lat)")
    p.add_argument("--item-id", help="ArcGIS item id (portal item with layers). If given, uses item.layers[layer_index]")
    p.add_argument("--layer-index", type=int, default=0, help="Index of layer inside item (default 0)")
    p.add_argument("--feature-layer-url", help="Direct FeatureLayer URL (alternative to --item-id)")
    p.add_argument("--gis-url", default="https://www.arcgis.com", help="Portal/ArcGIS Online URL")
    p.add_argument("--batch", type=int, default=200, help="Batch size for adds")
    p.add_argument("--sleep", type=float, default=0.3, help="Seconds to sleep between batches")
    p.add_argument("--dry-run", action="store_true", help="Do not upload — just print summary and first batch")
    return p.parse_args()


def auth_gis(gis_url: str) -> GIS:
    """Authenticate to GIS using env vars: ARCGIS_API_KEY or ARCGIS_USERNAME + ARCGIS_PASSWORD"""
    api_key = os.getenv("ARCGIS_API_KEY")
    username = os.getenv("ARCGIS_USERNAME")
    password = os.getenv("ARCGIS_PASSWORD")

    if api_key:
        print("Authenticating with ARCGIS_API_KEY")
        return GIS(gis_url, api_key=api_key)
    if username and password:
        print(f"Authenticating as {username}")
        return GIS(gis_url, username, password)
    raise RuntimeError("No ArcGIS credentials found. Set ARCGIS_API_KEY or ARCGIS_USERNAME & ARCGIS_PASSWORD.")


def load_features(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise RuntimeError("Features JSON must be a list of features")
    return data


def convert_to_arcgis_features(features: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    out = []
    for f in features:
        attrs = dict(f.get("attributes", {}))
        lon = attrs.get("long")
        lat = attrs.get("lat")
        if lon is None or lat is None:
            # try parse WKT
            wkt = f.get("wkt") or f.get("geometry_wkt")
            if wkt and wkt.upper().startswith("POINT"):
                try:
                    inner = wkt[wkt.find("(") + 1 : wkt.find(")")]
                    parts = inner.strip().split()
                    lon = float(parts[0])
                    lat = float(parts[1])
                except Exception:
                    lon = lat = None
        try:
            lon = float(lon) if lon is not None and str(lon) != "" else None
            lat = float(lat) if lat is not None and str(lat) != "" else None
        except Exception:
            lon = lat = None

        d_date = attrs.get("d_date")
        if d_date:
            if isinstance(d_date, str):

                for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"):
                    try:
                        dt = datetime.strptime(d_date, fmt)
                        # epoch ms
                        epoch_ms = int(dt.timestamp() * 1000)
                        attrs["d_date"] = epoch_ms
                        break
                    except Exception:
                        continue

        geometry = None
        if lon is not None and lat is not None and not (math.isnan(lon) or math.isnan(lat)):
            geometry = {"x": lon, "y": lat, "spatialReference": {"wkid": 4326}}
        else:
            # skip features without geometry
            print("Skipping feature with missing geometry:", attrs)
            continue

        out.append({"attributes": attrs, "geometry": geometry})
    return out


def get_feature_layer(gis: GIS, item_id: Optional[str], layer_index: int, feature_layer_url: Optional[str]) -> FeatureLayer:
    if feature_layer_url:
        print("Using provided feature layer URL")
        return FeatureLayer(feature_layer_url, gis=gis)
    if item_id:
        item = gis.content.get(item_id)
        if item is None:
            raise RuntimeError(f"Item {item_id} not found")
        layers = item.layers
        if not layers:
            raise RuntimeError("Item has no layers")
        if layer_index >= len(layers):
            raise RuntimeError(f"Layer index {layer_index} out of range (0..{len(layers)-1})")
        print(f"Using layer {layer_index} of item {item_id}")
        return layers[layer_index]
    raise RuntimeError("Either item_id or feature_layer_url must be provided")


def upload_batches(fl: FeatureLayer, arcgis_features: List[Dict[str, Any]], batch: int, sleep_between: float, dry_run: bool = False):
    total = len(arcgis_features)
    print(f"Uploading {total} features in batches of {batch} ... dry_run={dry_run}")
    results = {"batches": [], "total": total}
    for i in range(0, total, batch):
        chunk = arcgis_features[i:i+batch]
        if dry_run:
            print(f"[dry-run] batch {i//batch} size {len(chunk)} preview element:", chunk[0] if chunk else None)
            results["batches"].append({"index": i//batch, "ok": True, "count": len(chunk), "dry_run": True})
            continue

        try:
            resp = fl.edit_features(adds=chunk)
            results["batches"].append({"index": i//batch, "ok": True, "count": len(chunk), "response": resp})
            print(f"Batch {i//batch} uploaded, response summary keys: {list(resp.keys()) if isinstance(resp, dict) else type(resp)}")
        except Exception as e:
            print(f"Error uploading batch {i//batch}: {e}")
            results["batches"].append({"index": i//batch, "ok": False, "error": str(e)})
            break
        time.sleep(sleep_between)
    return results


def main():
    args = parse_args()
    gis = auth_gis(args.gis_url)
    raw = load_features(args.features)
    arcgis_feats = convert_to_arcgis_features(raw)
    if not arcgis_feats:
        print("No features to upload after conversion.")
        return

    fl = get_feature_layer(gis, args.item_id, args.layer_index, args.feature_layer_url)

    print("Preview attributes sample:", arcgis_feats[0]["attributes"])
    res = upload_batches(fl, arcgis_feats, batch=args.batch, sleep_between=args.sleep, dry_run=args.dry_run)
    print("Upload summary:", json.dumps(res, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
