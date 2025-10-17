# table_transformer— трансформатор табличних даних → GeoJSON / PostGIS / ArcGIS

## Короткий опис

`table_transformer` — невеликий універсальний інструмент на Python для автоматичної обробки табличних даних (локальний CSV або Google Sheets), перетворення кожного рядка в набір просторових об’єктів (point features) згідно з правилами ТЗ, підготовки preview-файлів та опційного завантаження у Hosted Feature Layer ArcGIS через REST (`addFeatures`). Результати також можна записати в PostGIS (PostGIS таблиця) або у файлові формати в папці `results/`.

---

## Основні можливості

* Читання даних із:

  * локального CSV (`--input / -i`),
  * публічної Google-таблиці (CSV export) (`--sheet-id` + `--gid` + `--download`),
  * приватної Google-таблиці через service account (`--sheet-id` + `--service-account`).
  * Трансформація рядків за правилом: `N = max(Value1..Value10)` → створити `N` записів(рядків) з одиничними індикаторами `i_value_1..i_value_10` (1/0).
  * Підготовка `features` у форматі для PostGIS (geom WKT) та JSON для подальшого завантаження.
  * Запис файлів у папку `results/`:

    * `{table}.json` — повний список підготовлених features (attributes + wkt),
    * `{table}_preview.csv` — preview атрибутів.
  * Опційне завантаження у ArcGIS Hosted Feature Layer через REST (по батчах).
  * CLI-скрипти для завантаження з Google Sheets, трансформації і записи у PostGIS.
  * Простий FastAPI-сервіс для видачі результатів у форматі GeoJSON (ендпоінт `/features.geojson`, `/feature/{id}`, `/download/gpkg`).

---

## Швидкий старт

1. Клонувати репозиторій та перейти в папку проекту.

2. Встановити залежності (рекомендовано через Poetry):

```bash
poetry install
```

3. Підготувати `.env` (див. `.env.example`) або встановити відповідні змінні оточення.

4. Запустити трансформацію локального CSV (dry-run — тільки файли в `results/`):

```bash
poetry run python -m scripts.transform_to_postgis \
  --input data/test_input.csv \
  --table transformed_features \
  --dry-run \
  --output-dir results
```

5. Щоб записати результат в PostGIS, вкажіть параметри підключення через `DATABASE_URL` або `PGHOST/PGUSER/PGPASSWORD/PGDATABASE` і запустіть без `--dry-run`:

```bash
poetry run python -m scripts.transform_to_postgis \
  --input data/test_input.csv \
  --table my_features \
  --batch 500 \
  --output-dir results
```

6. Щоб скачати публічну Google-таблицю і обробити її:

```bash
poetry run python -m scripts.transform_to_postgis \
  --sheet-id <SHEET_ID> --gid 0 --download --table my_features
```

7. Для приватної Google-таблиці через service account:

```bash
poetry run python -m scripts.fetch_gs --sheet-id <SHEET_ID> --service-account ./service_account.json --out results/from_gsheet.csv
# або відразу з --run-transform (викличе transform_to_postgis):
poetry run python -m scripts.fetch_gs --sheet-id <SHEET_ID> --service-account ./service_account.json --run-transform --table my_features --batch 500
```

---

## Завантаження в ArcGIS (REST)

* Завантаження виконується через `utils/arcgis_rest.py` — `upload_features_via_rest(features, feature_layer_url, token, batch_size)`.
* Зауваження: скрипт НЕ використовує пакет `arcgis` для відправки через REST; якщо потрібна робота з ArcGIS API for Python — її можна додати окремо.
* Приклади використання в CLI/скриптах надаються у прикладах.

---

## API (FastAPI)

У проекті є простий FastAPI-сервіс (`api.app`) для видачі даних з PostGIS у форматі GeoJSON:

* `/` → редирект на `/docs` (swagger)
* `/health` → статус сервісу
* `/features.geojson` → повертає FeatureCollection; параметри: `bbox`, `region`, `date_from`, `date_to`, `limit`, `offset`
* `/feature/{id}` → одиночний Feature
* `/download/gpkg` → повертає `results/my_features.gpkg`, якщо такий експорт існує

Запуск сервера:

```bash
poetry run uvicorn api.app:app --host 0.0.0.0 --port 8080 --workers 2
# тоді відкрити: http://0.0.0.0:8080/docs#/
```

---

## Docker

`Dockerfile` та `docker-compose.yml` в репозиторії показують базову конфігурацію для контейнеризації (образ з Python 3.12-slim, PostGIS контейнер `postgis/postgis:15-3.4` у `docker-compose`).

**Порада:** Docker-збірка видаляє build-інструменти після встановлення Python-залежностей — переконайтесь, що всі нативні розширення збираються під час етапу встановлення.

---

## Структура репозиторію (важливі файли)

* `scripts/transform_to_postgis.py` — основний CLI: читання джерела, трансформація, запис JSON/preview, вставка в PostGIS
* `scripts/fetch_gs.py` — помічник для зчитування Google Sheets (service account)
* `utils/arcgis_rest.py` — відправка features у ArcGIS REST `addFeatures`
* `api/` — FastAPI-додаток для видачі GeoJSON
* `data/` — приклади вхідних CSV (наприклад `data/test_input.csv`)
* `results/` — каталог для вихідних файлів (створюється автоматично під час запуску)
* `Dockerfile`, `docker-compose.yml` — для локального тестування із PostGIS
* `.env.example` — приклад змінних середовища

---

## Формат трансформації (правило)

1. Для кожного рядка беремо колонки `Value 1..Value 10` (або локалізовані назви `Значення 1..10`).
2. Обчислюємо `N = max(Value1..Value10)` (негативні/порожні значення трактуються як 0).
3. Якщо `N == 0` — рядок пропускається (нічого не додаємо).
4. Ітеруємо `i` від `0` до `N-1`, і для кожної ітерації створюємо новий feature з атрибутами `i_value_1..i_value_10`, де `i_value_k = 1` якщо `Value_k > i` інакше `0`.
5. До атрибутів також додаються: `d_date`, `t_region`, `t_city`, `long`, `lat`.
6. Геометрія формується як `POINT(long lat)` (WKT / PostGIS `ST_GeomFromText`), SRID = 4326.

Це правило відповідає тестовому завданню ГІС‑розробника.

---

## Нотатки по даним та очищенню

* Скрипти нормалізують десяткові коми у координатах (кома → крапка) та усувають небажані пробіли.
* Виконується пошук колонок `long`, `lat`, `date`, `region`, `city` по набору кандидатів із стійкою логікою "find_col_like" — скрипт намагається автоматично знайти відповідні назви колонок у різних мовних варіантах.

---

## Тестування

Запуск тестiв:
```bash
poetry run pytest -q
``` 

---

## Автор та контакти

Автор: `maxx_PC` — [m.petrykin@gmx.de](mailto:m.petrykin@gmx.de)

---

