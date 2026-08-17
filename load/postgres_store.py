"""Сохраняет DataFrames в базу данных Postgres с использованием SQLAlchemy."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import create_engine

from config import POSTGRES_URL


def save_dataframe_to_postgres(table_name: str, df: pd.DataFrame) -> None:
    
    """
    Добавляет DataFrame в таблицу Postgres.

    Функция требует, чтобы `POSTGRES_URL` был установлен в конфигурации.
    """
    
    if not POSTGRES_URL:
        raise ValueError("POSTGRES_URL is not set")

    engine = create_engine(POSTGRES_URL)
    df.to_sql(table_name, engine, if_exists="append", index=False)
    engine.dispose()
