from __future__ import annotations

from typing import Iterable

import pandas as pd


def normalize_vacancy(item: dict) -> dict:
    
    """Нормализует объект вакансии HH в плоский словарь.

    Возвращаемая структура подходит для построения DataFrame.
    """
    employer = item.get("employer") or {}
    salary = item.get("salary") or {}
    area = item.get("area") or {}
    snippet = item.get("snippet") or {}

    return {
        "id": item.get("id"),
        "title": item.get("name"),
        "company": employer.get("name"),
        "city": area.get("name"),
        "salary_from": salary.get("from"),
        "salary_to": salary.get("to"),
        "salary_currency": salary.get("currency"),
        "salary_gross": salary.get("gross"),
        "published_at": item.get("published_at"),
        "url": item.get("alternate_url"),
        "snippet": snippet.get("requirement") or snippet.get("responsibility") or "",
        "raw": item,
    }


def build_vitrine(items: Iterable[dict], profession: str) -> pd.DataFrame:
    
    """Создает датафрейм (витрину) для профессии из необработанных элементов."""
    
    ows = [normalize_vacancy(item) for item in items]
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame = frame.drop_duplicates(subset=["id"]).reset_index(drop=True)
    frame["profession"] = profession
    return frame
