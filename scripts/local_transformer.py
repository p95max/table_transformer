from __future__ import annotations
import argparse
import json
import os
import re
import time
import shutil
from typing import List, Dict, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------
# Utility helpers
# ---------------------------
def normalize_number_str(s):
    """Normalize numeric-like strings: replace comma->dot, remove spaces/thousands separators."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return s
    s = str(s).strip()
    if s == "":
        return None
    s = s.replace("\u00A0", "")
    s = s.replace(" ", "")
    s = s.replace("'", "").replace("’", "").replace("`", "")
    s = s.replace(",", ".")
    try:
        if re.search(r"[.eE]", s):
            return float(s)
        return int(s)
    except Exception:
        try:
            return float(s)
        except Exception:
            return s

def find_col_like(cols: List[str], candidates: List[str]) -> Optional[str]:
    """Find column name in cols matching any candidate (exact ignore-case or substring)."""
    lower_map = {c.strip().lower(): c for c in cols}
    for cand in candidates:
        key = cand.strip().lower()
        if key in lower_map:
            return lower_map[key]
    # substring fallback
    for cand in candidates:
        k = cand.strip().lower()
        for col in cols:
            if k in col.strip().lower():
                return col
    return None

def detect_value_columns(cols: List[str]) -> Optional[List[str]]:
    """Detect 10 value columns (1..10). Return list in order or None."""
    value_cols = []
    for i in range(1, 11):
        patterns = [
            f"Value {i}", f"Value_{i}", f"Value{i}", f"Значення {i}", f"Значение {i}",
            f"Знач_{i}", f"Val {i}", f"V{i}", f"v{i}", f"i_value_{i}", str(i)
        ]
        col = find_col_like(cols, patterns)
        if not col:
            for c in cols:
                if str(i) in c and (("value" in c.lower()) or ("знач" in c.lower()) or ("val" in c.lower())):
                    col = c
                    break
        if not col:
            return None
        value_cols.append(col)
    return value_cols

# ---------------------------
# Reading functions
# ---------------------------
def download_public_csv(sheet_id: str, gid: int = 0, out_path: str = "._download.csv", timeout: int = 30) -> str:
    """Download public Google Sheet CSV export to out_path."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    resp = requests.get(url, allow_redirects=True, timeout=timeout)
    resp.raise_for_status()
    with open(out_path, "wb") as fh:
        fh.write(resp.content)
    return out_path

def read_sheet_via_service_account(service_account_json: str, sheet_id: str, worksheet_name: Optional[str] = None) -> pd.DataFrame:
    """Read Google Sheet via gspread and service account JSON into DataFrame (all cells as strings)."""
    import gspread
    gc = gspread.service_account(filename=service_account_json)
    sh = gc.open_by_key(sheet_id)
    if worksheet_name:
        ws = sh.worksheet(worksheet_name)
    else:
        ws = sh.get_worksheet(0)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    df = pd.DataFrame(values[1:], columns=values[0])
    return df.astype(str)

# ---------------------------
# Transformation
# ---------------------------
def prepare_features_from_df(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict], Dict]:
    """Transform DataFrame into ArcGIS features and preview rows. Returns (features, preview_rows, meta)."""
    cols = list(df.columns)
    cols = [str(c).strip() for c in cols]
    df.columns = cols

    date_col = find_col_like(cols, ["Дата", "date", "d_date", "Date"])
    region_col = find_col_like(cols, ["Область", "region", "t_region", "Region"])
    city_col = find_col_like(cols, ["Місто", "city", "t_city", "City", "місто"])
    lon_col = find_col_like(cols, ["long", "longitude", "lon", "Long", "Longitude"])
    lat_col = find_col_like(cols, ["lat", "latitude", "Lat", "LAT", "Latitude"])
    value_cols = detect_value_columns(cols)
    if value_cols is None:
        raise RuntimeError(f"Could not detect all value columns 1..10. Available columns: {cols}")

    features = []
    preview_rows = []
    for idx, row in df.iterrows():
        counts = []
        for c in value_cols:
            raw = row.get(c, None)
            nval = normalize_number_str(raw)
            if nval is None:
                n = 0
            else:
                try:
                    n = int(float(nval))
                except Exception:
                    n = 0
            counts.append(max(0, n))
        max_n = max(counts) if counts else 0
        if max_n == 0:
            continue

        raw_lon = row.get(lon_col, None)
        raw_lat = row.get(lat_col, None)
        x_raw = normalize_number_str(raw_lon)
        y_raw = normalize_number_str(raw_lat)
        try:
            x = float(x_raw)
            y = float(y_raw)
        except Exception:
            continue

        d_raw = row.get(date_col, None)
        d_date = None
        if d_raw is not None and str(d_raw).strip() != "":
            try:
                parsed = pd.to_datetime(d_raw, dayfirst=True, errors='coerce')
                if pd.isna(parsed):
                    d_date = str(d_raw)
                else:
                    d_date = parsed.strftime("%Y-%m-%d")
            except Exception:
                d_date = str(d_raw)

        for i in range(max_n):
            attrs = {
                "d_date": d_date,
                "t_region": row.get(region_col),
                "t_city": row.get(city_col),
                "long": x,
                "lat": y,
            }
            for k in range(len(counts)):
                attrs[f"i_value_{k+1}"] = 1 if counts[k] > i else 0
            feature = {
                "geometry": {"x": x, "y": y, "spatialReference": {"wkid": 4326}},
                "attributes": attrs
            }
            features.append(feature)
            preview_rows.append(attrs.copy())

    meta = {
        "date_col": date_col,
        "region_col": region_col,
        "city_col": city_col,
        "lon_col": lon_col,
        "lat_col": lat_col,
        "value_cols": value_cols
    }
    return features, preview_rows, meta

# ---------------------------
# ArcGIS REST uploader
# ---------------------------
def upload_features_via_rest(
    features: List[Dict],
    feature_layer_url: str,
    token: Optional[str] = None,
    batch_size: int = 200,
    sleep_between: float = 0.5,
    max_retries: int = 3,
    timeout: int = 60
) -> Dict:
    """
    Upload features via addFeatures. Returns dict with success status and per-batch responses.
    token parameter is passed as 'token' form param. If None, tries without token.
    """
    url = feature_layer_url.rstrip("/") + "/addFeatures"
    headers = {"Accept": "application/json"}
    results = {"success": True, "batches": []}

    for i in range(0, len(features), batch_size):
        chunk = features[i:i+batch_size]
        payload = {"f": "json", "features": json.dumps(chunk, ensure_ascii=False)}
        if token:
            payload["token"] = token

        attempt = 0
        while attempt <= max_retries:
            try:
                resp = requests.post(url, data=payload, headers=headers, timeout=timeout)
                resp.raise_for_status()
                j = resp.json()
                results["batches"].append({"index": i//batch_size, "ok": True, "response": j})
                break
            except Exception as e:
                attempt += 1
                if attempt > max_retries:
                    results["success"] = False
                    results["batches"].append({"index": i//batch_size, "ok": False, "error": str(e)})
                    return results
                time.sleep(1.0 * attempt)
        time.sleep(sleep_between)
    return results

# ---------------------------
# I/O and orchestration
# ---------------------------
def ensure_results_dir(dirpath: str = "results") -> str:
    os.makedirs(dirpath, exist_ok=True)
    return dirpath

def save_results(features: List[Dict], preview_rows: List[Dict], out_dir: str = "results", base_name: str = "prepared_features"):
    out_dir = ensure_results_dir(out_dir)
    json_path = os.path.join(out_dir, f"{base_name}.json")
    preview_path = os.path.join(out_dir, f"{base_name}_preview.csv")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(features, fh, ensure_ascii=False, indent=2)
    if preview_rows:
        pd.DataFrame(preview_rows).to_csv(preview_path, index=False, encoding='utf-8-sig')
    return json_path, preview_path

def move_tmp_csv(tmp_path: Optional[str], keep: bool = False):
    if tmp_path and os.path.exists(tmp_path) and not keep:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", help="Local CSV file")
    p.add_argument("--sheet-id", help="Google Sheet ID")
    p.add_argument("--gid", type=int, default=0, help="gid for public download (default 0)")
    p.add_argument("--download", action="store_true", help="Download public CSV export from Google Sheets")
    p.add_argument("--service-account", help="Path to service_account.json to read private sheet")
    p.add_argument("--worksheet-name", help="Worksheet name for service account reading (optional)")
    p.add_argument("--output-dir", default="results", help="Directory to save outputs")
    p.add_argument("--base-name", default="prepared_features", help="Base file name (prepared_features.json)")
    p.add_argument("--upload", action="store_true", help="Upload prepared features to ArcGIS")
    p.add_argument("--feature-layer-url", help="ArcGIS Feature Layer URL (FeatureServer/<layerIndex>)")
    p.add_argument("--token", help="ArcGIS token or API key (if required). If not provided, will check env ARCGIS_TOKEN")
    p.add_argument("--batch", type=int, default=int(os.getenv("UPLOAD_BATCH_SIZE", "250")), help="Batch size for uploads")
    p.add_argument("--dry-run", action="store_true", help="Dry run: do not upload, only save files")
    p.add_argument("--keep-tmp", action="store_true", help="Keep temporary downloaded CSV file")
    p.add_argument("--max-preview", type=int, default=10, help="How many preview features to print on console")
    args = p.parse_args()

    tmp_csv = None
    if args.input:
        csv_path = args.input
        if not os.path.exists(csv_path):
            raise SystemExit(f"Input file not found: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str, engine='python', sep=None)
    elif args.sheet_id and args.download:
        print("Downloading public CSV export...")
        tmp_csv = "._download.csv"
        try:
            download_public_csv(args.sheet_id, gid=args.gid, out_path=tmp_csv)
        except Exception as e:
            raise SystemExit(f"Failed to download public CSV: {e}")
        df = pd.read_csv(tmp_csv, dtype=str, engine='python', sep=None)
    elif args.sheet_id and args.service_account:
        print("Reading sheet via service account...")
        try:
            df = read_sheet_via_service_account(args.service_account, args.sheet_id, args.worksheet_name)
        except Exception as e:
            raise SystemExit(f"Failed to read sheet via service account: {e}")
    else:
        raise SystemExit("Provide --input or (--sheet-id with --download) or (--sheet-id with --service-account)")

    df.columns = [str(c).strip() for c in df.columns]
    print(f"Read rows: {len(df)}")
    try:
        features, preview_rows, meta = prepare_features_from_df(df)
    except Exception as e:
        snap = {"columns": df.columns.tolist(), "error": str(e)}
        os.makedirs(args.output_dir, exist_ok=True)
        with open(os.path.join(args.output_dir, "processing_error_meta.json"), "w", encoding="utf-8") as fh:
            json.dump(snap, fh, ensure_ascii=False, indent=2)
        raise SystemExit(f"Error preparing features: {e}. Metadata saved to {args.output_dir}/processing_error_meta.json")

    json_path, preview_path = save_results(features, preview_rows, out_dir=args.output_dir, base_name=args.base_name)
    print(f"Prepared features: {len(features)}")
    print(f"Saved JSON: {json_path}")
    print(f"Saved preview CSV: {preview_path}")

    if preview_rows:
        print(f"Preview (first {args.max_preview} rows):")
        for r in preview_rows[:args.max_preview]:
            print(r)

    if args.upload and not args.dry_run:
        fl_url = args.feature_layer_url or os.getenv("ARCGIS_FEATURE_LAYER_URL")
        token = args.token or os.getenv("ARCGIS_TOKEN")
        if not fl_url:
            raise SystemExit("Feature layer URL must be provided via --feature-layer-url or ARCGIS_FEATURE_LAYER_URL env var")
        print(f"Starting upload to {fl_url} (batch {args.batch}) ...")
        upload_res = upload_features_via_rest(features, fl_url, token=token, batch_size=args.batch)
        print("Upload result summary:")
        print(json.dumps(upload_res, ensure_ascii=False, indent=2))
    elif args.upload and args.dry_run:
        print("Dry-run: upload requested but --dry-run set -> skipping actual upload.")

    if tmp_csv and not args.keep_tmp:
        try:
            os.remove(tmp_csv)
        except Exception:
            pass

if __name__ == "__main__":
    main()
