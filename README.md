Проєкт: transform_and_upload

Опис:
    Скрипт читає дані з Google Sheets, трансформує їх відповідно до технічного завдання
    та завантажує як об'єкти (features) до Hosted Feature Layer в ArcGIS Online.

Що робить скрипт:
    1. Зчитує Google-таблицю (через Service Account).
    2. Нормалізує назви колонок і витягує основні поля:
       - дата (d_date)
       - область (t_region)
       - місто (t_city)
       - значення/лічильники i_value_1 .. i_value_10
       - координати (long, lat)
    3. Для кожного рядка обчислює N = max(i_value_1..i_value_10). 
       Потім створює N нових записів: у записі номер i для кожної k-ї колонки i_value_k
       ставиться 1 якщо початкове значення для k > i, інакше 0.
    4. Формує для кожного запису geometry (x=long, y=lat, wkid=4326) та attributes:
       d_date, t_region, t_city, i_value_1..i_value_10, long, lat
    5. Завантажує підготовлені features у шар ArcGIS (батчами).

Конфігурація (.env):
    GOOGLE_SERVICE_ACCOUNT_JSON=./service_account.json
    GSHEET_ID=<id_таблиці>
    GSHEET_WORKSHEET_NAME=Sheet1
    ARCGIS_API_KEY=<api_key>  # або ARCGIS_USERNAME + ARCGIS_PASSWORD
    ARCGIS_FEATURE_ITEM_ID=<item_id_шару_ArcGIS>
    ARCGIS_FEATURE_LAYER_URL=<опціонально_прямий_URL_шару>

Запуск:
    1. Створити віртуальне середовище та встановити залежності:
       python -m venv venv
       source venv/bin/activate
       pip install -r requirements.txt
    2. Заповнити .env (скопіювати .env.example -> .env) та покласти service_account.json
    3. Запустити:
       python transform_and_upload.py

Рекомендації:
    - Спершу запускати в режимі "dry-run" (генерувати prepared CSV), щоб перевірити трансформацію.
    - Використовувати невеликі батчі при завантаженні (наприклад 200–500) через обмеження API.
    - Якщо потрібно перезаписати шар — робити це обережно (backup/експорт перед видаленням).
    - Поля дати в шарі ArcGIS можуть бути типу Date; у такому разі передавати ISO-формат або datetime.
    - Service Account в Google Sheets має бути доданий як редактор до таблиці.

Структура модулів:
    - gsheets_reader.py: читання таблиці і повернення pandas.DataFrame
    - arcgis_client.py: автентифікація до ArcGIS та функції для edit_features
    - transform_and_upload.py: оркестратор — використовує утиліти, перетворює дані і вантажить в ArcGIS

Примітки:
    - Файл service_account.json не додавати в git.
    - Логування рекомендується налаштувати через стандартний модуль logging.
    - Тести повинні перевіряти: коректність мапінгу колонок, логіку розгортання N рядків, та підготовку geometry.
"""

import os

def main():
    # реалізація скрипта знаходиться в модульній структурі (utils/*.py)
    # тут — точка входу, яка імпортує і викликає потрібні функції
    pass

if __name__ == "__main__":
    main()
