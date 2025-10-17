Проєкт: transform_and_upload

Опис:
    Універсальний скрипт для:
      - зчитування даних з локального CSV, публічної Google-таблиці (експорт CSV) або приватної через Service Account,
      - трансформації рядків за логікою ТЗ (N = max(Value1..Value10) => створюємо N записів),
      - підготовки features у форматі ArcGIS (geometry + attributes),
      - збереження результатів у папку results/ (prepared_features.json, prepared_features_preview.csv),
      - опційної відправки features у ArcGIS Hosted Feature Layer через REST addFeatures батчами.

Підтримка:
    - локальний CSV: --input / -i
    - публічний Google Sheet: --sheet-id <id> --gid <gid> --download
    - приватна таблиця через service account JSON: --sheet-id <id> --service-account <path> [--worksheet-name NAME]
    - завантаження у ArcGIS: --upload --feature-layer-url URL --token TOKEN (або встановіть ARCGIS_FEATURE_LAYER_URL/ARCGIS_TOKEN у .env)
    - dry-run: --dry-run (записує тільки файли у results/, не відправляє)

Приклади:
    # локальний CSV
    python scripts/transform_and_upload.py --input data/my_sheet.csv

    # скачати публічну таблицю і обробити
    python scripts/transform_and_upload.py --sheet-id 1aSc... --gid 0 --download

    # прочитати приватну таблицю через service account
    python scripts/transform_and_upload.py --sheet-id 1aSc... --service-account ./service_account.json

    # обробити і завантажити у ArcGIS
    python scripts/transform_and_upload.py --input data/my_sheet.csv --upload --feature-layer-url "https://.../FeatureServer/0" --token "MYTOKEN" --batch 200

Примітка:
    Скрипт не використовує пакет `arcgis` — робота з ArcGIS виконується через REST (requests).