FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN apt-get update && apt-get install -y gcc libpq-dev build-essential \
    && pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction \
    && apt-get remove -y gcc build-essential \
    && apt-get autoremove -y && apt-get clean

COPY . .

CMD ["python", "transformer.py"]
