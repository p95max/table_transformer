# Принцип роботи сервісу та основні технології

Сервіс читає табличні дані (локальний CSV або Google Sheets), нормалізує поля (координати, дати, значення), за правилом трансформації розбиває кожний рядок
на одну або кілька точок (point features) з атрибутами та геометрією, зберігає підготовлені результати у файли (.json, preview CSV) та/або вставляє їх у PostGIS.
За потреби підготовлені features батчами відправляються
у Hosted Feature Layer ArcGIS через REST (addFeatures) або подаються через просте FastAPI-API у форматі GeoJSON для візуалізації на карті.

**Основні технології:**

- Python 3.12 — мова реалізації CLI-скриптів і API.
- pandas, gspread, google-auth — для читання та обробки таблиць і Google Sheets.
- psycopg2 / PostGIS (PostgreSQL + PostGIS) — зберігання просторових даних (таблиці з геометрією).
- ArcGIS API / REST (addFeatures) — завантаження даних у Hosted Feature Layer ArcGIS (опціонально).
- FastAPI + Uvicorn — простий веб-API для видачі GeoJSON та експорту (GPKG).
- Docker / docker-compose — контейнеризація сервісу і локальний PostGIS для тестування.
- ogr2ogr (GDAL) — експорт/імпорт у формати, наприклад GeoPackage.

**Ключові файли**

* **README.md** — загальна інструкція з використання проекту: швидкий старт, приклади команд, опис функцій.
* **pyproject.toml** — опис пакета та залежностей (Poetry).
* **.env.example** — приклади змінних оточення (Postgres, Google service account, ArcGIS і т.д.).
* **Dockerfile** — інструкції для створення Docker-образу (Python 3.12-slim).
* **docker-compose.yml** — локальний стек для тестування: `app` + `db (PostGIS)`.
* **scripts/fetch_gs.py** — CLI для зчитування Google Sheets через service account і збереження очищеного CSV.
* **scripts/transform_to_postgis.py** — основний CLI: трансформація рядків у spatial features, збереження JSON/preview, вставка у PostGIS.
* **scripts/upload_to_arcgis.py** — CLI для підготовки та завантаження features у Hosted Feature Layer ArcGIS (через arcgis або REST).
* **api/app.py** — FastAPI-сервер для видачі GeoJSON з PostGIS та кінцевих точок (`/features.geojson`, `/feature/{id}`, `/download/gpkg`).
* **utils/arcgis_rest.py** — утиліта для завантаження features у ArcGIS Feature Layer через REST (`addFeatures`).
* **utils/gsheets_reader.py** — простий helper для читання Google Sheet у pandas.DataFrame (service account).
* **data/main_data.csv** — приклад вхідних табличних даних (шаблон колонок/формат координат).
* **results/** — каталог для вихідних файлів: підготовлені `{table}.json`, `{table}_preview.csv`, GeoPackage тощо.

---

Файл призначений для швидкого орієнтування у репозиторії — можна додати як `KEY_FILES.md` у корінь проєкту.



## 1. Загальна послідовність роботи:

1. Створити або використати проєкт у Google Cloud Console.
2. Увімкнути Google Sheets API (і, при потребі, Google Drive API).
3. Створити service account → згенерувати JSON ключ (`service_account.json`).
4. Поділитися Google Sheet з email сервісного акаунту (щоб скрипт мав доступ).
5. Підкладати `service_account.json` у проєкт і вказувати шлях у змінних оточення / команді.

**`service_account.json`** — це приватний ключ сервісного облікового запису Google Cloud, який дозволяє скриптам (наприклад, `gspread.service_account`)
читати приватні Google Sheets. Google API Key — це короткий ключ (рядок) для доступу до публічних API (корисно для public CSV export або деяких інших сервісів).

---

## 2. Де взяти `service_account.json`

1. Відкрийте Google Cloud Console: [https://console.cloud.google.com](https://console.cloud.google.com)
2. Створіть новий проєкт або виберіть чинний (гору зліва — вибір проєкту).
3. У меню зліва перейдіть **APIs & Services → Library**.

   * Знайдіть і **Enable** (увімкніть) `Google Sheets API`.
   * Рекомендується також увімкнути `Google Drive API`, якщо потрібно доступ до Sheets, що не є публічними за URL.
4. Перейдіть у **APIs & Services → Credentials**.
5. Натисніть **Create Credentials → Service account**.

   * Введіть назву (наприклад `table-transformer-sa`) та опис → **Create**.
   * Ролі: для читання Google Sheets ролі в проєкті зазвичай не потрібні (ви не працюєте з GCP ресурсами). Можна пропустити добавлення ролей.
6. Після створення service account в списку натисніть на нього → **Keys → Add Key → Create new key**.

   * Тип ключа: **JSON** → Create.
   * Файл автоматично буде завантажений у ваш браузер — це і є `service_account.json`. **Збережіть цей файл у безпечному місці**

**Примітка:** Один service account має email виду `some-name@project-id.iam.gserviceaccount.com` — цей email треба використати при наданні доступу до Google Sheet.

---

## 3. Де взяти Google API Key

API Key корисний, коли ви хочете робити запити до публічних Google API (наприклад, якщо використовуєте `export?format=csv` без авторизації) або коли бібліотека чи сервіс підтримують API key. Процедура:

1. В `Google Cloud Console` → **APIs & Services → Credentials** → **Create Credentials → API key**.
2. Система згенерує ключ у вигляді рядка. Скопіюйте його (це і є `GOOGLE_API_KEY`).
3. **Обмеження ключа (recommended):** натисніть «Restrict key» і встановіть:

   * Application restrictions — HTTP referrers (web sites) або IP addresses (для сервера). Якщо використовуєте тільки локальні скрипти — можна тимчасово не ставити обмеження, але у production — обов'язково.
   * API restrictions — оберіть Google Sheets API (або інші, які ви використовуєте).

> Зверніть увагу: API Key не дає доступ до приватних Google Sheets — для цього потрібен `service_account.json` і спільний доступ файлу.

---

## 4. Як надати доступ service account до Google Sheet

1. Відкрийте Google Sheet у браузері.
2. Клікніть **Share** (Поділитися).
3. В полі додавання користувачів вставте email сервісного акаунту (виглядає як `name@project-id.iam.gserviceaccount.com`). Дайте роль **Viewer** або **Editor** (для читання достатньо Viewer).
4. Зберегти — тепер service account може читати аркуш через `gspread.service_account`.

---

## 5. Куди покласти файли і як задати змінні оточення

1. Покладіть `service_account.json` у директорію проєкту - наприклад у корінь проєкту або у `secrets/`.
2. У `.gitignore` має бути `service_account.json` (цей репозиторій має це вже).
3. Налаштуйте файл `.env` або експортуйте змінні перед запуском.

**Приклад `.env` (коротко):**

```
GOOGLE_SERVICE_ACCOUNT_JSON=./service_account.json
GSHEET_ID=1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI
ARCGIS_API_KEY=ВАШ_ARCGIS_API_KEY
PGHOST=localhost
PGPORT=5432
PGDATABASE=transformer
PGUSER=user
PGPASSWORD=1111
```

**Або експортувати в shell:**

```bash
export GOOGLE_SERVICE_ACCOUNT_JSON=./service_account.json
export ARCGIS_API_KEY="abc..."
```

У скриптах в проєкті ви зазвичай передаєте `--service-account ./service_account.json` або робите `gspread.service_account(filename=...)`.

---

## 6. Основні команди

**Скачати оригінал Google Sheet (через service account) та зберегти CSV:**

```bash
poetry run python -m scripts.fetch_gs \
  --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI \
  --service-account ./service_account.json \
  --out results/from_gsheet.csv
```

**Ті ж кроки + запустити трансформацію в таблицю PostGIS:**

```bash
poetry run python -m scripts.fetch_gs \
  --sheet-id 1aScZXHhADfX8JW22Qr1KaBymLyDeIP2T0dt-lXkAJkI \
  --service-account ./service_account.json \
  --run-transform --table transformed_features --batch 200
```

**Dry-run завантаження у ArcGIS (перевірка):**

```bash
python -m scripts.upload_to_arcgis \
  --features results/transformed_features.json \
  --item-id <ARC_ITEM_ID> \
  --batch 200 --dry-run
```

**Реальне завантаження в ArcGIS (перед цим встановіть ARCGIS_API_KEY):**

```bash
export ARCGIS_API_KEY="ВАШ_КЛЮЧ"
python -m scripts.upload_to_arcgis \
  --features results/transformed_features.json \
  --item-id <ARC_ITEM_ID> \
  --batch 200
```

## 7. Часті проблеми й рішення

**1) `gspread.service_account` не знаходить файл**

* Перевірте шлях, або передавайте абсолютний шлях. Також переконайтесь, що `GOOGLE_SERVICE_ACCOUNT_JSON` встановлено.

**2) Service account отримує 403 при читанні Sheet**

* Перевірте, чи правильно поділились аркушем з email service account (viewer/editor).
* Переконайтесь, що ви відкриваєте саме той Sheet ID і Worksheet.

**3) Дані не парсяться (коми у координатах)**

* Скрипти в проєкті нормалізують десяткові коми — але якщо є лапки/неочікувані символи - виправте в Sheets або вручну перед трансформацією.

**4) ArcGIS: недостатньо прав для редагування шару**

* Переконайтесь, що API key має права для редагування відповідного Hosted Feature Layer. Якщо використовуєте організацію ArcGIS Online, можливо,
потрібно поділитися елементом або додати права користувачу.

---

Автор: `maxx_PC` [m.petrykin@gmx.de]

