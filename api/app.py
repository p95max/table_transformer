"""
    API для публікації даних з PostGIS у форматі GeoJSON
    Простий REST API (FastAPI) для вибірки ознак з PostGIS
"""
from __future__ import annotations
import os
import json
from typing import Optional, Tuple
from urllib.parse import quote_plus
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2 import sql
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
from fastapi.responses import RedirectResponse

load_dotenv()

DEFAULT_MIN_POOL = 1
DEFAULT_MAX_POOL = 10
APP_PORT = int(os.getenv("API_PORT", 8080))
DATABASE_URL = os.getenv("DATABASE_URL")

PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")

if not DATABASE_URL:
    if not (PGHOST and PGDATABASE and PGUSER and PGPASSWORD):
        pass
    else:

        DATABASE_URL = f"postgresql://{quote_plus(PGUSER)}:{quote_plus(PGPASSWORD)}@{PGHOST}:{PGPORT}/{PGDATABASE}"

app = FastAPI(title="PostGIS → GeoJSON API", version="0.1", swagger_ui_parameters={"deepLinking": True} )

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(",") if os.getenv("CORS_ALLOW_ORIGINS") else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pool: Optional[SimpleConnectionPool] = None
TABLE_NAME = os.getenv("API_TABLE", "my_features")


def get_conn_params():
    """Return a dict of connection params for psycopg2.connect"""
    if DATABASE_URL:
        return {"dsn": DATABASE_URL}
    if PGHOST and PGDATABASE and PGUSER and PGPASSWORD:
        return {"host": PGHOST, "port": PGPORT, "dbname": PGDATABASE, "user": PGUSER, "password": PGPASSWORD}
    raise RuntimeError("Database connection info not found. Set DATABASE_URL or PGHOST/PGUSER/PGPASSWORD.")


@app.on_event("startup")
def startup():
    global pool
    try:
        params = get_conn_params()
    except RuntimeError as e:
        raise RuntimeError(str(e))
    minconn = int(os.getenv("PG_POOL_MIN", DEFAULT_MIN_POOL))
    maxconn = int(os.getenv("PG_POOL_MAX", DEFAULT_MAX_POOL))
    if "dsn" in params:
        pool = SimpleConnectionPool(minconn, maxconn, dsn=params["dsn"])
    else:
        pool = SimpleConnectionPool(minconn, maxconn,
                                    host=params["host"],
                                    port=params["port"],
                                    dbname=params["dbname"],
                                    user=params["user"],
                                    password=params["password"])
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
    finally:
        pool.putconn(conn)


@app.on_event("shutdown")
def shutdown():
    global pool
    if pool:
        pool.closeall()

def parse_bbox(bbox_str: str) -> Tuple[float, float, float, float]:
    parts = bbox_str.split(",")
    if len(parts) != 4:
        raise ValueError("bbox must be 'minx,miny,maxx,maxy'")
    try:
        return tuple(float(p) for p in parts)
    except Exception:
        raise ValueError("bbox coordinates must be numbers")


def build_filters(bbox: Optional[str], region: Optional[str], date_from: Optional[str], date_to: Optional[str]):
    """
    Returns tuple (where_clauses_list, params_list)
    """
    where = []
    params = []
    if region:
        where.append(sql.SQL("t_region ILIKE %s"))
        params.append(f"%{region}%")
    if date_from:
        where.append(sql.SQL("d_date >= %s"))
        params.append(date_from)
    if date_to:
        where.append(sql.SQL("d_date <= %s"))
        params.append(date_to)
    if bbox:
        minx, miny, maxx, maxy = parse_bbox(bbox)
        where.append(sql.SQL("ST_Intersects(geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326))"))
        params.extend([minx, miny, maxx, maxy])
    return where, params


# ручки

@app.get("/")
def root():
    return RedirectResponse(url="/docs")

@app.get("/health",
         summary="Show service health.")
def health_status():
    return {"status": "ok"}


@app.get("/features.geojson",
         response_class=JSONResponse,
         summary="Get features by params.")
def features_geojson(
    bbox: Optional[str] = Query(None, description="bbox=minx,miny,maxx,maxy"),
    region: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    global pool
    if pool is None:
        raise HTTPException(status_code=500, detail="DB pool not initialized")

    where_clauses, params = build_filters(bbox, region, date_from, date_to)
    where_sql = sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses) if where_clauses else sql.SQL("")
    q = sql.SQL("SELECT id, d_date, t_region, t_city, long, lat, ST_AsGeoJSON(geom) AS geom_json FROM {tbl} {where} ORDER BY id LIMIT %s OFFSET %s").format(
        tbl=sql.Identifier(TABLE_NAME),
        where=where_sql
    )
    params_with_paging = params + [limit, offset]

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(q, params_with_paging)
        rows = cur.fetchall()
        features = []
        for row in rows:
            fid, d_date, region_v, city, lon, lat, geom_json = row
            geom = json.loads(geom_json) if geom_json else None
            props = {
                "id": fid,
                "d_date": d_date.isoformat() if getattr(d_date, "isoformat", None) else d_date,
                "t_region": region_v,
                "t_city": city,
                "long": lon,
                "lat": lat,
            }
            features.append({"type": "Feature", "geometry": geom, "properties": props})
        total = None
        if offset == 0:
            count_q = sql.SQL("SELECT COUNT(*) FROM {tbl} {where}").format(tbl=sql.Identifier(TABLE_NAME), where=where_sql)
            cur.execute(count_q, params)
            total = cur.fetchone()[0]
        cur.close()
    finally:
        pool.putconn(conn)

    result = {"type": "FeatureCollection", "features": features, "meta": {"limit": limit, "offset": offset, "total": total}}
    return JSONResponse(content=result)


@app.get("/feature/{fid}",
         response_class=JSONResponse,
         summary="Get features by ID.")
def get_feature(fid: int):
    global pool
    if pool is None:
        raise HTTPException(status_code=500, detail="DB pool not initialized")
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        q = sql.SQL("SELECT id, d_date, t_region, t_city, long, lat, ST_AsGeoJSON(geom) FROM {tbl} WHERE id = %s").format(tbl=sql.Identifier(TABLE_NAME))
        cur.execute(q, (fid,))
        row = cur.fetchone()
        cur.close()
    finally:
        pool.putconn(conn)
    if not row:
        raise HTTPException(status_code=404, detail="Feature not found")
    fid, d_date, region_v, city, lon, lat, geom_json = row
    geom = json.loads(geom_json) if geom_json else None
    props = {"id": fid, "d_date": d_date.isoformat() if getattr(d_date, "isoformat", None) else d_date,
             "t_region": region_v, "t_city": city, "long": lon, "lat": lat}
    return JSONResponse({"type": "Feature", "geometry": geom, "properties": props})


@app.get("/download/gpkg", summary="Download GeoPackage.")
def download_gpkg():
    gpkg_path = os.path.join("results", "my_features.gpkg")
    if os.path.exists(gpkg_path):
        return FileResponse(gpkg_path, media_type="application/geopackage+sqlite", filename="my_features.gpkg")
    raise HTTPException(status_code=404, detail="GPKG not found")