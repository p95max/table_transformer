# local 
poetry run python -m scripts.local_transformer --input data/test_input.csv --output-dir results --base-name result_table

# google sheets
poetry run python -m scripts.gs_transformer \
  --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI \
  --service-account ./service_account.json \
  --worksheet-name Sheet1

