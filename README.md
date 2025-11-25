# Service Workflow and Core Technologies

The service reads tabular data (local CSV or Google Sheets), normalizes fields (coordinates, dates, values), and transforms each row into one or more point features with attributes and geometry. It stores the prepared results as files (.json, preview CSV) and/or inserts them into PostGIS. If needed, the prepared features are sent in batches to a Hosted Feature Layer in ArcGIS via REST (addFeatures) or served through a simple FastAPI API in GeoJSON format for map visualization.

**Core Technologies:**

- Python 3.12 — implementation language for CLI scripts and API.
- pandas, gspread, google-auth — reading and processing tables and Google Sheets.
- psycopg2 / PostGIS (PostgreSQL + PostGIS) — storing spatial data (geometry tables).
- ArcGIS API / REST (addFeatures) — uploading features into ArcGIS Hosted Feature Layer (optional).
- FastAPI + Uvicorn — lightweight web API for serving GeoJSON and exporting (GPKG).
- Docker / docker-compose — containerization and local PostGIS stack for testing.
- ogr2ogr (GDAL) — export/import to formats like GeoPackage.

**Key Files**

* **README.md** — general usage instructions, quick start, command examples, function descriptions.
* **pyproject.toml** — package and dependencies (Poetry).
* **.env.example** — environment variable examples (Postgres, Google service account, ArcGIS, etc.).
* **Dockerfile** — build instructions (Python 3.12-slim).
* **docker-compose.yml** — local stack for testing: `app` + `db (PostGIS)`.
* **scripts/fetch_gs.py** — CLI for reading Google Sheets using a service account and saving cleaned CSV.
* **scripts/transform_to_postgis.py** — core CLI: transforms rows into spatial features, saves JSON/preview, inserts into PostGIS.
* **scripts/upload_to_arcgis.py** — CLI for preparing and uploading features to ArcGIS Hosted Feature Layer.
* **api/app.py** — FastAPI server for serving GeoJSON from PostGIS and endpoints (`/features.geojson`, `/feature/{id}`, `/download/gpkg`).
* **utils/arcgis_rest.py** — utility for uploading features to ArcGIS via REST (`addFeatures`).
* **utils/gsheets_reader.py** — helper for reading Google Sheets into pandas.DataFrame (service account).
* **data/main_data.csv** — sample input data (columns/coordinate formats).
* **results/** — output directory for prepared `{table}.json`, `{table}_preview.csv`, GeoPackage, etc.

---

## 1. General Workflow

1. Create or use an existing project in Google Cloud Console.
2. Enable Google Sheets API (and optionally Google Drive API).
3. Create a service account → generate JSON key (`service_account.json`).
4. Share the Google Sheet with the service account’s email.
5. Place the `service_account.json` file into the project and reference it via env vars or command arguments.

`service_account.json` is the private key for a Google Cloud service account used by scripts (e.g., `gspread.service_account`) to read private Google Sheets.  
A Google API Key is a short token used for public API access (for public CSV export or other services).

---

## 2. Where to Get `service_account.json`

1. Go to Google Cloud Console: https://console.cloud.google.com  
2. Select or create a project.  
3. Go to **APIs & Services → Library**.  
   - Enable **Google Sheets API**.  
   - Optionally enable **Google Drive API**.  
4. Go to **APIs & Services → Credentials**.  
5. Click **Create Credentials → Service account**.  
   - Name it (`table-transformer-sa`) → Create.  
   - Roles: not required for reading Sheets.  
6. After creation:  
   - Open the service account  
   - **Keys → Add Key → Create new key → JSON**  
   - Download the JSON file — this is your `service_account.json`.

Note: The service account has an email like `name@project-id.iam.gserviceaccount.com`. You must share your Google Sheet with this email.

---

## 3. Where to Get a Google API Key

1. In Google Cloud Console → **APIs & Services → Credentials → Create Credentials → API key**.  
2. Copy the generated string — this is your `GOOGLE_API_KEY`.  
3. Recommended:  
   - Restrict by IP or HTTP referrer  
   - Restrict by API (Google Sheets API)

API Key does **not** provide access to private sheets — only `service_account.json` does.

---

## 4. How to Grant Sheet Access to the Service Account

1. Open the Google Sheet.  
2. Click **Share**.  
3. Add the service account email (`...@project-id.iam.gserviceaccount.com`) as Viewer/Editor.  
4. Save.

---

## 5. Where to Put Files & How to Set Env Vars

1. Place `service_account.json` in project root or `secrets/`.  
2. Ensure `.gitignore` includes it.  
3. Configure `.env` or use shell exports.

Example `.env`:

```
GOOGLE_SERVICE_ACCOUNT_JSON=./service_account.json
GSHEET_ID=1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI
ARCGIS_API_KEY=YOUR_ARCGIS_API_KEY
PGHOST=localhost
PGPORT=5432
PGDATABASE=transformer
PGUSER=user
PGPASSWORD=1111
```

Example shell exports:

```
export GOOGLE_SERVICE_ACCOUNT_JSON=./service_account.json
export ARCGIS_API_KEY="abc..."
```

---

## 6. Main Commands

Download Google Sheet and save CSV:

```
poetry run python -m scripts.fetch_gs   --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI   --service-account ./service_account.json   --out results/from_gsheet.csv
```

Fetch + transform + insert into PostGIS:

```
poetry run python -m scripts.fetch_gs   --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI   --service-account ./service_account.json   --run-transform --table transformed_features --batch 200
```

Dry-run upload to ArcGIS:

```
python -m scripts.upload_to_arcgis   --features results/transformed_features.json   --item-id <ARC_ITEM_ID>   --batch 200 --dry-run
```

Real ArcGIS upload:

```
export ARCGIS_API_KEY="YOUR_KEY"
python -m scripts.upload_to_arcgis   --features results/transformed_features.json   --item-id <ARC_ITEM_ID>   --batch 200
```

---

## 7. Common Issues

1. **`gspread.service_account` cannot find the file**  
   - Check path or env var.

2. **403 reading Google Sheet**  
   - Ensure the sheet is shared with service account email.  
   - Ensure correct Sheet ID.

3. **Parsing errors (comma decimal)**  
   - Clean values or ensure scripts normalize decimal commas.

4. **ArcGIS upload permission errors**  
   - API key must have edit rights on the Hosted Feature Layer.

---

Author: `maxx_PC` (m.petrykin@gmx.de)
