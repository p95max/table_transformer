# google sheets

### download table only
```bash
poetry run python -m scripts.fetch_gs \
    --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI \ 
    --service-account ./service_account.json
```    
### download and edit table
```bash
PGHOST=localhost PGPORT=5432 PGDATABASE=transformer PGUSER=user PGPASSWORD=1111 \
poetry run python -m scripts.fetch_gs \
  --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI \
  --service-account ./service_account.json \
  --run-transform \
  --table my_features \
  --batch 500
```  
# GeoPackage export for FastAPI server
###
```bash
mkdir -p results
ogr2ogr -f GPKG results/exported_table.gpkg \
  PG:"host=localhost port=5432 dbname=transformer user=user password=1111" \
  -sql "SELECT * FROM my_features"
```    
### check package
```bash
ogrinfo results/exported_table.gpkg -so
```  

# 3. Run API server
## Run server
```bash
poetry run uvicorn api.app:app --host 0.0.0.0 --port 8080 --workers 2
``` 
## Open in browser
http://0.0.0.0:8080/docs#/




# dry-run local .csv file
```bash
poetry run python -m scripts.transform_to_postgis \
  --input data/test_input.csv \
  --table edited_table \
  --dry-run \
  --output-dir results
``` 

# import data from Google Sheets
export GOOGLE_SERVICE_ACCOUNT_JSON=./service_account.json
poetry run python -m scripts.fetch_gs \
  --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI \
  --service-account ./service_account.json \
  --run-transform \
  --table transformed_features \
  --batch 200 \
  --out results/from_gsheet_ready.csv


# send data to arcgis
python -m scripts.upload_to_arcgis \
  --features results/transformed_features.json \
  --item-id 90094b605df94754987b27d4b12877f9 \
  --batch 200 > results/upload_full_response.json 2>&1

https://www.arcgis.com/home/webmap/viewer.html?url=https://services8.arcgis.com/KEfMfAbvB81PuRGi/arcgis/rest/services/%D0%A2%D0%B5%D1%81%D1%82%D0%BE%D0%B2%D0%B8%D0%B9_%D1%88%D0%B0%D1%80_3/FeatureServer/0




