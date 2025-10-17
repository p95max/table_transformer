# google sheets

## download table only
poetry run python -m scripts.fetch_gs \
    --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI \ 
    --service-account ./service_account.json
## download and edit table
PGHOST=localhost PGPORT=5432 PGDATABASE=transformer PGUSER=user PGPASSWORD=1111 \
poetry run python -m scripts.fetch_gs \
  --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI \
  --service-account ./service_account.json \
  --run-transform \
  --table my_features \
  --batch 500








# dry-run local .csv file
poetry run python -m scripts.transform_to_postgis \
  --input data/test_input.csv \
  --table edited_table \
  --dry-run \
  --output-dir results



