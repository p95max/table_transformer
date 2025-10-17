import os
import json
from dotenv import load_dotenv
import pandas as pd

from utils.gsheets_reader import read_sheet_to_df
from utils.arcgis_rest import upload_features_via_rest

load_dotenv()

SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
GSHEET_ID = os.getenv("GSHEET_ID")
GSHEET_WORKSHEET_NAME = os.getenv("GSHEET_WORKSHEET_NAME", "Sheet1")
FEATURE_LAYER_URL = os.getenv("ARCGIS_FEATURE_LAYER_URL")
TOKEN = os.getenv("ARCGIS_TOKEN")
BATCH_SIZE = int(os.getenv("UPLOAD_BATCH_SIZE", "250"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes")


def prepare_features(df: pd.DataFrame) -> list:
    """
    Prepare ArcGIS features list from input DataFrame.
    """
    cols = list(df.columns)

    def find_col_like(candidates):
        for c in candidates:
            for col in cols:
                if col.strip().lower() == c.strip().lower():
                    return col
        return None

    date_col = find_col_like(["Дата", "date", "d_date"])
    region_col = find_col_like(["Область", "region", "t_region"])
    city_col = find_col_like(["Місто", "city", "t_city"])
    lon_col = find_col_like(["long", "longitude", "lon"])
    lat_col = find_col_like(["lat", "latitude"])

    value_cols = []
    for i in range(1, 11):
        cand = find_col_like([f"Значення {i}", f"Value {i}", f"Value_{i}", f"Значение {i}", f"Знач_{i}"])
        if not cand:
            for col in cols:
                if str(i) in col and ("знач" in col.lower() or "value" in col.lower()):
                    cand = col
                    break
        if not cand:
            raise RuntimeError(f"Не знайдено колонку для Value {i}")
        value_cols.append(cand)

    features = []
    for _, row in df.iterrows():
        try:
            counts = []
            for c in value_cols:
                v = row.get(c, 0)
                try:
                    n = int(float(v)) if pd.notna(v) and v != "" else 0
                except Exception:
                    n = 0
                counts.append(max(0, n))
            max_n = max(counts) if counts else 0
            if max_n == 0:
                continue

            lon = row.get(lon_col)
            lat = row.get(lat_col)
            try:
                x = float(lon)
                y = float(lat)
            except Exception:
                continue

            d_date_raw = row.get(date_col)
            try:
                d_date = pd.to_datetime(d_date_raw).strftime("%Y-%m-%d") if pd.notna(d_date_raw) and d_date_raw != "" else None
            except Exception:
                d_date = str(d_date_raw) if d_date_raw else None

            for i in range(max_n):
                attrs = {
                    "d_date": d_date,
                    "t_region": row.get(region_col),
                    "t_city": row.get(city_col),
                    "long": x,
                    "lat": y,
                }
                for k, _ in enumerate(counts):
                    attrs[f"i_value_{k+1}"] = 1 if counts[k] > i else 0

                feature = {
                    "geometry": {"x": x, "y": y, "spatialReference": {"wkid": 4326}},
                    "attributes": attrs
                }
                features.append(feature)
        except Exception:
            continue
    return features


def main():
    if not SERVICE_ACCOUNT_JSON or not GSHEET_ID or not FEATURE_LAYER_URL:
        raise SystemExit("Set GOOGLE_SERVICE_ACCOUNT_JSON, GSHEET_ID and ARCGIS_FEATURE_LAYER_URL in .env")

    df = read_sheet_to_df(SERVICE_ACCOUNT_JSON, GSHEET_ID, GSHEET_WORKSHEET_NAME)
    print(f"Read {len(df)} rows from sheet")

    features = prepare_features(df)
    print(f"Prepared {len(features)} features")

    if DRY_RUN:
        with open("prepared_features.json", "w", encoding="utf-8") as fh:
            json.dump(features, fh, ensure_ascii=False, indent=2)
        print("Dry-run enabled: wrote prepared_features.json")
        return

    res = upload_features_via_rest(features, FEATURE_LAYER_URL, token=TOKEN, batch_size=BATCH_SIZE)
    print("Upload result:", json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
