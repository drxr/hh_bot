# HH Parser — ETL + Telegram‑бот для поиска вакансий HeadHunter (MVP)

Проект собирает вакансии с HeadHunter, сохраняет "сырые" данные, строит аналитические
витрины (DuckDB / Postgres) и предоставляет простой Telegram‑бот для поиска и вывода
статистики (топ‑городов, топ‑навыков, зарплатные агрегаты).

Файл‑ссылки

- Основной запуск бота: [main.py](main.py)
- Telegram handlers: [bot/bot.py](bot/bot.py)
- HH client: [extract/hh_api.py](extract/hh_api.py)
- Сохранение сырых данных: [extract/hh_raw.py](extract/hh_raw.py)
- Трансформации / витрины: [transform/vitrine.py](transform/vitrine.py)
- DuckDB helper: [load/duckdb_store.py](load/duckdb_store.py)
- Конфиг: [config.py](config.py)
- Тесты: [test/test_hh_parser.py](test/test_hh_parser.py)

Особенности

- Поддержка двух способов авторизации HH: статический `HH_API_TOKEN` и OAuth client credentials.
- Глобальная семафора для защиты от избытка одновременных запросов к HH (throttling).
- Сохранение сырых ответов (JSONL / Parquet) и построение витрин в DuckDB.
- Telegram‑бот: поиск, постраничный вывод, прогресс загрузки и статистика (топ‑навыков/городов).

Требования

- Python 3.11+ (проект тестируется на 3.11 и 3.13)
- Установленные зависимости: смотрите `requirements.txt`.

Быстрый старт (локально)

1. Склонируйте репозиторий и создайте виртуальное окружение:

```bash
git clone <repo-url>
cd hh_parser
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

2. Создайте файл `.env` (можете скопировать `.env.example` если он есть) и заполните
   минимальные переменные:

```env
BOT_TOKEN=your_telegram_bot_token
HH_API_TOKEN=your_hh_api_token        # или использовать client_id/secret
HH_CLIENT_ID=your_client_id
HH_CLIENT_SECRET=your_client_secret
DUCKDB_PATH=./data/hh.duckdb
RAW_STORAGE_DIR=./data/raw
```

3. Запустите бота (polling):

```bash
source .venv/bin/activate
python main.py
```

Запуск ETL (локально)

```bash
python cli.py extract "data analyst" --area 113 --pages 2 --last 7
python cli.py build_vitrines --use-duckdb
```

Тесты

```bash
source .venv/bin/activate
pytest -q
```

CI / GitHub Actions

В проект был добавлен workflow для GitHub Actions: `.github/workflows/python-ci.yml`.
Он запускает тесты на push/PR в ветки `main`/`master` и использует matrix для Python 3.11/3.13.

Рекомендации по деплою

- Для продакшн‑деплоя Telegram‑бота удобно использовать Docker / systemd или
  managed host (Heroku, VPS). При деплое учтите хранение секретов (GH Secrets / env).
- Если планируете длительные фоновые задачи (более 30s) — выносите тяжёлые ETL‑операции
  в фоновые worker'ы (Celery / RQ / systemd timers).
