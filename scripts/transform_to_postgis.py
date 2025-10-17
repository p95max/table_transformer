from __future__ import annotations
from typing import List, Dict, Tuple, Optional
from dotenv import load_dotenv
from psycopg2 import sql
import argparse
import os
import json
import re
import sys
import pandas as pd

load_dotenv()

try:
    import gspread
except Exception:
    gspread = None
try:
    import requests
except Exception:
    requests = None

try:
    import psycopg2
    import psycopg2.extras as extras
except Exception:
    psycopg2 = None
    extras = None


# Helpers for data ingestion

def normalize_number_str(s):
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
            return None

def find_col_like(cols, candidates):
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

def detect_value_columns(cols):
    value_cols = []
    for i in range(1, 11):
        patterns = [f"Value {i}", f"Value_{i}", f"Value{i}",
                    f"Значення {i}", f"Значение {i}", f"Знач_{i}",
                    f"Val {i}", f"V{i}", f"v{i}", f"i_value_{i}", str(i)]
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

# Readers
def read_local_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, engine='python', sep=None)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def download_public_csv(sheet_id: str, gid: int = 0, out_path: str = "._download.csv") -> str:
    if requests is None:
        raise RuntimeError("requests is required to download public sheet (install requests).")
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    r = requests.get(url, allow_redirects=True, timeout=30)
    r.raise_for_status()
    with open(out_path, "wb") as fh:
        fh.write(r.content)
    return out_path

def read_sheet_via_service_account(service_account_json: str, sheet_id: str, worksheet_name: Optional[str] = None) -> pd.DataFrame:
    if gspread is None:
        raise RuntimeError("gspread is required to read private sheets via service account. Install gspread.")
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
    df = df.astype(str)
    df.columns = [str(c).strip() for c in df.columns]
    return df

# Transformation logic
def prepare_features_from_df(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict], Dict]:
    cols = list(df.columns)
    cols = [str(c).strip() for c in cols]
    df.columns = cols

    date_col = find_col_like(cols, ["Дата", "date", "d_date", "Date"])
    region_col = find_col_like(cols, ["Область", "region", "t_region", "Region"])
    city_col = find_col_like(cols, ["Місто", "city", "t_city", "City"])
    lon_col = find_col_like(cols, ["long", "longitude", "lon", "Long", "Longitude"])
    lat_col = find_col_like(cols, ["lat", "latitude", "Lat", "LAT", "Latitude"])
    value_cols = detect_value_columns(cols)
    if value_cols is None:
        raise RuntimeError(f"Could not detect all value columns 1..10. Available columns: {cols}")

    features = []
    preview_rows = []
    for _, row in df.iterrows():
        counts = []
        for c in value_cols:
            raw = row.get(c, None)
            num = normalize_number_str(raw)
            if num is None:
                n = 0
            else:
                try:
                    n = int(float(num))
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
                d_date = pd.to_datetime(d_raw, dayfirst=True, errors='coerce')
                if pd.isna(d_date):
                    d_date = str(d_raw)
                else:
                    d_date = d_date.strftime("%Y-%m-%d")
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
            geom_wkt = f"POINT({x} {y})"
            feature = {"attributes": attrs, "wkt": geom_wkt}
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

# PostGIS operations
def get_db_conn(db_url: Optional[str] = None):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required to write to PostGIS (install psycopg2-binary).")
    if db_url:
        return psycopg2.connect(db_url)
    # try env vars
    conn_info = {}
    for k in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        v = os.getenv(k)
        if v:
            conn_info[k.lower()] = v
    if conn_info:
        return psycopg2.connect(
            host=conn_info.get("pghost"),
            port=conn_info.get("pgport") or 5432,
            dbname=conn_info.get("pgdatabase"),
            user=conn_info.get("pguser"),
            password=conn_info.get("pgpassword"),
        )
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)
    raise RuntimeError("No DB connection info found. Provide --db-url or set PGHOST/PGUSER/PGPASSWORD or DATABASE_URL")

def ensure_postgis_and_table(conn, table_name: str):
    cur = conn.cursor()
    # enable PostGIS if not exists (requires superuser in some setups; otherwise skip)
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    except Exception:
        conn.rollback()
    # create table if not exists (use psycopg2.sql for safe identifier substitution)
    create_sql = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {tbl} (
      id SERIAL PRIMARY KEY,
      d_date DATE,
      t_region TEXT,
      t_city TEXT,
      long DOUBLE PRECISION,
      lat DOUBLE PRECISION,
      i_value_1 INTEGER, i_value_2 INTEGER, i_value_3 INTEGER, i_value_4 INTEGER,
      i_value_5 INTEGER, i_value_6 INTEGER, i_value_7 INTEGER, i_value_8 INTEGER,
      i_value_9 INTEGER, i_value_10 INTEGER,
      geom geometry(Point,4326)
    );
    """).format(tbl=sql.Identifier(table_name))
    try:
        cur.execute(create_sql)
    except Exception:
        conn.rollback()
        raise
    # create gist index safely
    idx_name = f"{table_name}_geom_gist"
    try:
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {tbl} USING GIST (geom);")
            .format(idx=sql.Identifier(idx_name), tbl=sql.Identifier(table_name))
        )
    except Exception:
        conn.rollback()
    conn.commit()
    cur.close()


def truncate_table(conn, table_name: str):
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {extras.Identifier(table_name).string};")
    conn.commit()
    cur.close()

def insert_features_bulk(conn, table_name: str, features: List[Dict], batch_size: int = 500) -> Dict:
    results = {"inserted": 0, "batches": []}
    if not features:
        return results

    cur = conn.cursor()
    cols = [
        "d_date", "t_region", "t_city", "long", "lat",
        "i_value_1","i_value_2","i_value_3","i_value_4","i_value_5",
        "i_value_6","i_value_7","i_value_8","i_value_9","i_value_10"
    ]
    cols_sql = ", ".join(cols) + ", geom"
    # template for each row; last placeholder is WKT for ST_GeomFromText
    vals_template = "(" + ",".join(["%s"] * len(cols)) + ", ST_GeomFromText(%s, 4326))"

    # Build parameterized INSERT statement with safe table identifier
    insert_sql_composed = sql.SQL("INSERT INTO {tbl} ({cols}) VALUES %s").format(
        tbl=sql.Identifier(table_name),
        cols=sql.SQL(cols_sql)
    )
    # convert to string with proper quoting using connection
    insert_sql = insert_sql_composed.as_string(conn)

    rows = []
    for f in features:
        a = f["attributes"]
        row = [
            a.get("d_date"),
            a.get("t_region"),
            a.get("t_city"),
            a.get("long"),
            a.get("lat"),
            a.get("i_value_1"), a.get("i_value_2"), a.get("i_value_3"), a.get("i_value_4"), a.get("i_value_5"),
            a.get("i_value_6"), a.get("i_value_7"), a.get("i_value_8"), a.get("i_value_9"), a.get("i_value_10"),
            f.get("wkt")
        ]
        rows.append(tuple(row))

    from psycopg2.extras import execute_values
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        try:
            execute_values(cur, insert_sql, chunk, template=vals_template)
            conn.commit()
            results["inserted"] += len(chunk)
            results["batches"].append({"index": i//batch_size, "ok": True, "count": len(chunk)})
        except Exception as e:
            conn.rollback()
            results["batches"].append({"index": i//batch_size, "ok": False, "error": str(e)})
            break
    cur.close()
    return results


# Orchestration / CLI
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", help="Local CSV input file")
    p.add_argument("--sheet-id", help="Google Sheet ID")
    p.add_argument("--gid", type=int, default=0, help="gid for public download")
    p.add_argument("--download", action="store_true", help="Download public CSV export")
    p.add_argument("--service-account", help="Path to service_account.json for private sheets")
    p.add_argument("--worksheet-name", help="Worksheet name for service account reading (optional)")
    p.add_argument("--table", default="transformed_features", help="Target PostGIS table name")
    p.add_argument("--db-url", help="Postgres connection URL (psycopg2)")
    p.add_argument("--batch", type=int, default=500, help="Batch size for DB inserts")
    p.add_argument("--dry-run", action="store_true", help="Prepare files but do not write to DB")
    p.add_argument("--output-dir", default="results", help="Dir for prepared JSON/preview CSV")
    p.add_argument("--truncate-before-insert", action="store_true", help="TRUNCATE table before insert")
    args = p.parse_args()

    tmp_csv = None
    if args.input:
        df = read_local_csv(args.input)
    elif args.sheet_id and args.download:
        tmp_csv = download_public_csv(args.sheet_id, gid=args.gid, out_path="._download.csv")
        df = read_local_csv(tmp_csv)
    elif args.sheet_id and args.service_account:
        df = read_sheet_via_service_account(args.service_account, args.sheet_id, args.worksheet_name)
    else:
        print("Provide --input or (--sheet-id with --download) or (--sheet-id with --service-account)")
        sys.exit(1)

    print(f"Read {len(df)} rows from source")

    features, preview_rows, meta = prepare_features_from_df(df)
    print(f"Prepared {len(features)} features")

    os.makedirs(args.output_dir, exist_ok=True)
    json_out = os.path.join(args.output_dir, f"{args.table}.json")
    preview_out = os.path.join(args.output_dir, f"{args.table}_preview.csv")
    with open(json_out, "w", encoding="utf-8") as fh:
        json.dump(features, fh, ensure_ascii=False, indent=2)
    if preview_rows:
        pd.DataFrame(preview_rows).to_csv(preview_out, index=False, encoding='utf-8-sig')
    print(f"Wrote prepared JSON: {json_out}")
    print(f"Wrote preview CSV: {preview_out}")

    if args.dry_run:
        print("Dry-run: skipping DB write")
        if tmp_csv and not args.download:
            pass
        sys.exit(0)

    conn = get_db_conn(args.db_url)
    try:
        ensure_postgis_and_table(conn, args.table)
        if args.truncate_before_insert:
            truncate_table(conn, args.table)
        res = insert_features_bulk(conn, args.table, features, batch_size=args.batch)
        print("Insert result:", json.dumps(res, ensure_ascii=False, indent=2))
    finally:
        conn.close()
    if tmp_csv and not args.download:
        try:
            os.remove(tmp_csv)
        except Exception:
            pass

if __name__ == "__main__":
    main()
