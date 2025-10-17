"""
    Скрипт читає Google Sheet через service account, нормалізує дані
    (особливо перетворює десяткові коми у long/lat на крапки), зберігає
    очищений CSV у папку results/, та опційно запускає трансформацію у PostGIS
"""
from __future__ import annotations
import argparse
import os
import sys
import subprocess
from typing import List, Optional

import pandas as pd

try:
    import gspread
except Exception as e:
    raise SystemExit("gspread is required: poetry add gspread") from e

# helpers
def find_col_like(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.strip().lower(): c for c in cols}
    for cand in candidates:
        key = cand.strip().lower()
        if key in lower_map:
            return lower_map[key]
    for cand in candidates:
        k = cand.strip().lower()
        for col in cols:
            if k in col.strip().lower():
                return col
    return None

def detect_value_columns(cols: List[str]) -> List[str]:
    found = []
    for i in range(1, 11):
        patterns = [f"Значення {i}", f"Значение {i}", f"Value {i}", f"Value_{i}", f"Value{i}", f"Знач_{i}", str(i)]
        col = find_col_like(cols, patterns)
        if col:
            found.append(col)
        else:
            for c in cols:
                if str(i) in c and (("знач" in c.lower()) or ("value" in c.lower()) or ("val" in c.lower())):
                    found.append(c)
                    break
    return found

def normalize_decimal_str_series(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.replace(r"[ \u00A0]", "", regex=True).str.replace(",", ".", regex=False).replace({"": None})

# main
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sheet-id", required=True, help="Google Sheet ID")
    p.add_argument("--service-account", required=True, help="Path to service_account.json")
    p.add_argument("--worksheet-name", default=None, help="Worksheet name (optional)")
# result file path
    p.add_argument("--out", default="results/from_gsheet.csv", help="Output CSV path (default: results/from_gsheet.csv)")

    p.add_argument("--run-transform", action="store_true", help="If set, call transform_to_postgis after saving CSV")
    p.add_argument("--table", default="my_features", help="Table name to pass to transform script when --run-transform")
    p.add_argument("--batch", type=int, default=500, help="Batch size to pass to transform script")
    args = p.parse_args()

    out_dir = os.path.dirname(args.out) or "results"
    os.makedirs(out_dir, exist_ok=True)

    gc = gspread.service_account(filename=args.service_account)
    sh = gc.open_by_key(args.sheet_id)
    if args.worksheet_name:
        ws = sh.worksheet(args.worksheet_name)
    else:
        ws = sh.get_worksheet(0)
    values = ws.get_all_values()
    if not values:
        raise SystemExit("Sheet is empty or could not be read.")
    df = pd.DataFrame(values[1:], columns=values[0])
    df.columns = [str(c).strip() for c in df.columns]

    cols = df.columns.tolist()
    lon_col = find_col_like(cols, ["long", "longitude", "lon", "lng", "Long", "Longitude"])
    lat_col = find_col_like(cols, ["lat", "latitude", "Lat", "LAT", "Latitude"])
    value_cols = detect_value_columns(cols)

    if not lon_col or not lat_col:
        tail = cols[-4:]
        cand = []
        for c in tail:
            sample = df[c].dropna().astype(str)
            if sample.shape[0] == 0:
                continue
            if sample.str.contains(r"[0-9]+[,\.][0-9]+").any():
                cand.append(c)
        if len(cand) >= 2:
            lon_col, lat_col = cand[0], cand[1]

    if lon_col:
        df[lon_col] = normalize_decimal_str_series(df[lon_col])
    if lat_col:
        df[lat_col] = normalize_decimal_str_series(df[lat_col])

    for vc in value_cols:
        if vc in df.columns:
            ser = df[vc].astype(str).str.replace(r"[ \u00A0]", "", regex=True).str.replace(",", ".", regex=False)
            df[vc] = pd.to_numeric(ser, errors="coerce").fillna(0).astype(int)

    df.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"Saved cleaned CSV to {args.out}")
    print("Detected columns:")
    print(" lon:", lon_col)
    print(" lat:", lat_col)
    print(" value columns (detected):", value_cols)

    if args.run_transform:
        print("Running transform_to_postgis on saved CSV (outputs -> results/)...")
        cmd = [
            sys.executable, "-m", "scripts.transform_to_postgis",
            "--input", args.out,
            "--table", args.table,
            "--batch", str(args.batch),
            "--output-dir", "results"
        ]
        proc = subprocess.run(cmd)
        if proc.returncode != 0:
            raise SystemExit(f"transform_to_postgis exited with code {proc.returncode}")
        print("transform_to_postgis finished successfully.")

if __name__ == "__main__":
    main()
