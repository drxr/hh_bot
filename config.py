"""Настройки проекта, загруженные из environment (.env).

Этот модуль централизует небольшие значения конфигурации времени выполнения, которые читаются
при импорте. Значения представляют собой простые строки (или целые числа) и безопасны для
импорта из других модулей.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

load_dotenv()

BASE_DIR: Final[Path] = Path(__file__).resolve().parent

def _clean_env(key: str, default: str = "") -> str:
    
	"""
    Чтение переменной окружения, удаление пробелов и окружающих кавычек.
	"""
	v = os.getenv(key, default)
	if v is None:
		return default
	s = str(v).strip()
	# remove surrounding single/double quotes if present
	if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
		s = s[1:-1].strip()
	return s


BOT_TOKEN: str = _clean_env("BOT_TOKEN")
HH_API_TOKEN: str = _clean_env("HH_API_TOKEN") or _clean_env("HH_ACCESS_TOKEN")
HH_CLIENT_ID: str = _clean_env("HH_CLIENT_ID")
HH_CLIENT_SECRET: str = _clean_env("HH_CLIENT_SECRET")
HH_API_URL: str = _clean_env("HH_API_URL", "https://api.hh.ru")
HH_AREA: str = _clean_env("HH_AREA", "113")
DEFAULT_PER_PAGE: int = int(_clean_env("DEFAULT_PER_PAGE", "5"))
RAW_STORAGE_DIR: str = _clean_env("RAW_STORAGE_DIR", str(BASE_DIR / "data" / "raw"))
DUCKDB_PATH: str = _clean_env("DUCKDB_PATH", str(BASE_DIR / "data" / "hh.duckdb"))
POSTGRES_URL: str = _clean_env("POSTGRES_URL", "")
TELEGRAM_PROXY: str = _clean_env("TELEGRAM_PROXY", "")
HH_PROXY: str = _clean_env("HH_PROXY", "")
HH_API_MAX_RESULTS: int = int(_clean_env("HH_API_MAX_RESULTS", "2000"))
HH_RETRIES: int = int(_clean_env("HH_RETRIES", "4"))
HH_RETRY_BACKOFF_BASE: float = float(_clean_env("HH_RETRY_BACKOFF_BASE", "1.0"))
HH_MAX_CONCURRENT_REQUESTS: int = int(_clean_env("HH_MAX_CONCURRENT_REQUESTS", "3"))

# Проверка наличия директорий для хранения данных и DuckDB
Path(RAW_STORAGE_DIR).mkdir(parents=True, exist_ok=True)
Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
