# google sheets

## 1.
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
# 2. GeoPackage export
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

# 3. Run test API server
## Run server
```bash
poetry run uvicorn api.app:app --host 0.0.0.0 --port 8080 --workers 2
``` 
## Open in browser
http://0.0.0.0:8080/docs#/









# dry-run local .csv file
poetry run python -m scripts.transform_to_postgis \
  --input data/test_input.csv \
  --table edited_table \
  --dry-run \
  --output-dir results



