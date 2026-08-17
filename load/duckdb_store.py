"""
Утилиты для сохранения DataFrames в локальный файл DuckDB.

DuckDB является опциональным; если он не установлен, вызовы вызовут явную ошибку.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from config import DUCKDB_PATH

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None


def _normalize_work_type(item: dict) -> str:
    
    """Нормализует формат работы HH / расписание в категории офис, гибрид, удаленно."""
    
    work_formats: list[str] = []
    raw_formats = item.get("work_format")
    if raw_formats is None:
        raw_formats = []
    elif hasattr(raw_formats, "tolist"):
        raw_formats = raw_formats.tolist()
    if isinstance(raw_formats, dict):
        raw_formats = [raw_formats]
    elif raw_formats is None:
        raw_formats = []
    else:
        raw_formats = list(raw_formats)

    for value in raw_formats:
        if isinstance(value, dict):
            value = value.get("id") or value.get("name") or ""
        if value:
            work_formats.append(str(value))

    if not work_formats and isinstance(item.get("schedule"), dict):
        schedule_id = item["schedule"].get("id") or item["schedule"].get("name")
        if schedule_id:
            work_formats.append(str(schedule_id))

    text = "|".join(work_formats).lower()
    if "remote" in text:
        return "remote"
    if "hybrid" in text:
        return "hybrid"
    if "office" in text or "full_day" in text or "full" in text:
        return "office"
    if "on_site" in text or "onsite" in text:
        return "office"
    return "unknown"


def _flatten_vacancy(item: dict) -> dict:
    
    """Выравнивает сырой элемент HH для использования в аналитических таблицах."""
    
    salary = item.get("salary") or {}
    area = item.get("area") or {}
    employer = item.get("employer") or {}
    schedule = item.get("schedule") or {}

    # нормализует навыки в чистый список строк (в нижнем регистре)
    def _iter_skill_names(val):
        if val is None:
            return
        try:
            import numpy as _np
        except Exception:
            _np = None

        if isinstance(val, dict):
            name = val.get("name") or val.get("skill")
            if name:
                yield str(name).strip().lower()
            return

        if _np is not None and isinstance(val, _np.ndarray):
            for e in val.tolist():
                if isinstance(e, dict):
                    n = e.get("name") or e.get("skill")
                    if n:
                        yield str(n).strip().lower()
                else:
                    yield str(e).strip().lower()
            return

        if isinstance(val, (list, tuple, set)):
            for e in val:
                if isinstance(e, dict):
                    n = e.get("name") or e.get("skill")
                    if n:
                        yield str(n).strip().lower()
                else:
                    yield str(e).strip().lower()
            return

        if isinstance(val, str):
            s = val.strip()
            try:
                import json as _json

                parsed = _json.loads(s)
                if isinstance(parsed, (list, tuple)):
                    for e in parsed:
                        yield str(e).strip().lower()
                    return
                if isinstance(parsed, dict):
                    n = parsed.get("name") or parsed.get("skill")
                    if n:
                        yield str(n).strip().lower()
                        return
            except Exception:
                pass
            if s.startswith("[") and s.endswith("]"):
                s = s[1:-1]
            parts = re.split(r"[,;|\\n]+", s)
            for p in parts:
                p = p.strip().strip('\"\'')
                if p:
                    yield p.strip().lower()

    skills = []
    raw = item.get("key_skills")
    if raw is None:
        raw = item.get("key_skill")
    for name in _iter_skill_names(raw):
        if name:
            skills.append(name)

    return {
        "id": item.get("id"),
        "vacancy_name": item.get("name"),
        "company": employer.get("name"),
        "city": area.get("name"),
        "salary_from": salary.get("from"),
        "salary_to": salary.get("to"),
        "currency": salary.get("currency"),
        "published_at": item.get("published_at"),
        "url": item.get("alternate_url"),
        "schedule_id": schedule.get("id") if isinstance(schedule, dict) else None,
        "work_type": _normalize_work_type(item),
        "skills": skills,
    }


def save_dataframe_to_duckdb(table_name: str, df: pd.DataFrame) -> str:
    
    """
    Сохраняет pandas DataFrame в файл DuckDB под именем таблицы `table_name`.

    Возвращает путь к файлу DuckDB.
    """
    if duckdb is None:
        raise RuntimeError("DuckDB is not installed. Add duckdb to requirements or skip this step.")

    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.register("tmp_df", df)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tmp_df WHERE 1 = 0")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_df")
    con.unregister("tmp_df")
    con.close()
    return DUCKDB_PATH


def build_vacancy_analytics_tables(parquet_path: str, duckdb_path: str | None = None) -> dict[str, pd.DataFrame]:
    
    """
    Читает данные из parquet и записывает сводные таблицы в DuckDB.

    Возвращает словарь с DataFrames для `vacancies`, `skills_summary`, `salary_summary`,
    `city_summary`, и `work_type_summary`.
    """
    
    if duckdb is None:
        raise RuntimeError("DuckDB is not installed. Add duckdb to requirements or skip this step.")

    raw_df = pd.read_parquet(parquet_path)
    rows: list[dict] = []
    for item in raw_df.to_dict(orient="records"):
        rows.append(_flatten_vacancy(item))
    vacancies_df = pd.DataFrame(rows)
    if vacancies_df.empty:
        vacancies_df = pd.DataFrame(
            columns=["id", "vacancy_name", "company", "city", "salary_from", "salary_to", "currency", "published_at", "url", "schedule_id", "work_type", "skills"]
        )

    vacancies_df["salary_from"] = pd.to_numeric(vacancies_df["salary_from"], errors="coerce")
    vacancies_df["salary_to"] = pd.to_numeric(vacancies_df["salary_to"], errors="coerce")

    skill_rows: list[dict] = []
    for _, row in vacancies_df.iterrows():
        for skill in row.get("skills") or []:
            skill_rows.append({"id": row.get("id"), "skill": skill, "vacancy_name": row.get("vacancy_name")})
    skills_df = pd.DataFrame(skill_rows)

    salary_summary = vacancies_df.groupby("currency", dropna=False).agg(
        vacancies_count=("id", "count"),
        avg_salary_from=("salary_from", "mean"),
        avg_salary_to=("salary_to", "mean"),
    ).reset_index()

    city_summary = vacancies_df.groupby("city", dropna=False).size().reset_index(name="vacancies_count")
    work_type_summary = vacancies_df.groupby("work_type", dropna=False).size().reset_index(name="vacancies_count")

    if not skills_df.empty:
        skills_summary = skills_df.groupby("skill", dropna=False).size().reset_index(name="count")
        skills_summary = skills_summary.sort_values(["count", "skill"], ascending=[False, False]).reset_index(drop=True)
    else:
        skills_summary = pd.DataFrame(columns=["skill", "count"])

    output_path = duckdb_path or DUCKDB_PATH
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    con = duckdb.connect(output_path)
    con.register("vacancies_df", vacancies_df)
    con.register("skills_summary_df", skills_summary)
    con.register("salary_summary_df", salary_summary)
    con.register("city_summary_df", city_summary)
    con.register("work_type_summary_df", work_type_summary)

    con.execute("CREATE TABLE IF NOT EXISTS vacancies AS SELECT * FROM vacancies_df")
    con.execute("CREATE TABLE IF NOT EXISTS skills_summary AS SELECT * FROM skills_summary_df")
    con.execute("CREATE TABLE IF NOT EXISTS salary_summary AS SELECT * FROM salary_summary_df")
    con.execute("CREATE TABLE IF NOT EXISTS city_summary AS SELECT * FROM city_summary_df")
    con.execute("CREATE TABLE IF NOT EXISTS work_type_summary AS SELECT * FROM work_type_summary_df")

    tables = {
        "vacancies": con.execute("SELECT * FROM vacancies").fetch_df(),
        "skills_summary": con.execute("SELECT * FROM skills_summary ORDER BY count DESC").fetch_df(),
        "salary_summary": con.execute("SELECT * FROM salary_summary ORDER BY avg_salary_from DESC").fetch_df(),
        "city_summary": con.execute("SELECT * FROM city_summary ORDER BY vacancies_count DESC").fetch_df(),
        "work_type_summary": con.execute("SELECT * FROM work_type_summary ORDER BY vacancies_count DESC").fetch_df(),
    }
    con.close()
    return tables
