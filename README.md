### HH Parser — ETL‑система и Telegram‑бот для анализа вакансий HeadHunter (MVP)

Проект реализует конвейер сбора данных с HeadHunter, их хранения и аналитики, а также предоставляет Telegram‑бота для поиска вакансий и просмотра статистики.

**Возможности**
- Сбор вакансий с HH: поддержка двух способов авторизации — статический HH_API_TOKEN и OAuth (client credentials).
- Контроль нагрузки: глобальная семафора для ограничения числа одновременных запросов к API HH (throttling).
- Хранение сырых данных: сохранение ответов в форматах JSONL и Parquet.
- Аналитические витрины: построение витрин данных в DuckDB (с возможностью выгрузки в Postgres).

**Telegram‑бот:**
- поиск вакансий по запросам;
- постраничный вывод результатов;
- отображение прогресса загрузки данных;
- статистика: топ городов, топ навыков, зарплатные агрегаты.

**Структура проекта**
```
main.py — точка входа, запуск бота.
bot/bot.py — обработчики команд Telegram‑бота.
extract/hh_api.py — клиент для работы с API HeadHunter.
extract/hh_raw.py — логика сохранения сырых данных.
transform/vitrine.py — трансформации данных и построение витрин.
load/duckdb_store.py — вспомогательные функции для работы с DuckDB.
config.py — конфигурация приложения.
test/test_hh_parser.py — тесты.
cli.py — интерфейс командной строки для запуска ETL‑задач.
```

**Требования**
- Python версии 3.11 или выше (проект протестирован на 3.11 и 3.13).
- Зависимости, указанные в файле requirements.txt.
---

**Быстрый старт (локальный запуск)**

Склонируйте репозиторий:
```bash
git clone https://github.com/drxr/hh_bot.git
cd hh_parser
```
Создайте и активируйте виртуальное окружение:
```bash
python -m venv .venv
source .venv/bin/activate  # Для Windows: .venv\Scripts\activate
```
Обновите pip и установите зависимости:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```
Настройте переменные окружения:
- Создайте файл .env (можно скопировать шаблон .env.example, если он есть).
- Заполните необходимые переменные:
```env
BOT_TOKEN=your_telegram_bot_token
HH_API_TOKEN=your_hh_api_token          # Либо используйте OAuth: HH_CLIENT_ID и HH_CLIENT_SECRET
HH_CLIENT_ID=your_client_id
HH_CLIENT_SECRET=your_client_secret
DUCKDB_PATH=./data/hh.duckdb
RAW_STORAGE_DIR=./data/raw
```
Запустите Telegram‑бота (polling):
```bash
python main.py
```
**Запуск ETL‑процессов (локально)**

Используйте CLI для запуска этапов извлечения и построения витрин:

Извлечение данных:
```bash
python cli.py extract "data analyst" --area 113 --pages 2 --last 7
```
Построение витрин:
```bash
python cli.py build_vitrines --use-duckdb
```
**Тестирование**

Для запуска тестов активируйте виртуальное окружение и выполните:
```bash
pytest -q
```
**CI / GitHub Actions**

В проекте настроен workflow (.github/workflows/python-ci.yml), который:
- автоматически запускает тесты при push и pull request в ветки main/master;
- использует матрицу версий Python (3.11, 3.13) для проверки совместимости.

Рекомендации по деплою

Для продакшн‑развёртывания Telegram‑бота удобно использовать Docker, systemd или управляемые хостинги (VPS, облачные платформы).
Секреты (токены, ключи) храните в безопасном хранилище (GitHub Secrets, переменные окружения сервера).
Длительные фоновые задачи (ETL‑процессы продолжительностью более 30 секунд) рекомендуется выносить в отдельные воркеры (Celery, RQ) либо запускать через планировщик (systemd timers).
