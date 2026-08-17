#!/usr/bin/env python3
"""
Command-line interface для локальных ETL операций (development use).

    python cli.py extract "data analyst" --last 7

CLI понимает команды `extract` и `build_vitrines`.
"""
from __future__ import annotations

import argparse
from typing import List

from extract.hh_api import HHClient
from extract.hh_raw import save_raw_batch
from transform.professions import PROFESSIONS
from transform.vitrine import build_vitrine
from load.postgres_store import save_dataframe_to_postgres
from load.duckdb_store import save_dataframe_to_duckdb


def cmd_extract(args: argparse.Namespace) -> None:
    
    """
    Извлекает соответствующие вакансии из HH и сохраняет необработанный JSONL.

    Пространство имен `args` ожидает содержать `query`, `area`, `pages`,
    `per_page`, `date_from`, `date_to`, и опционально `last_days`.
    """
    
    client = HHClient()
    items: List[dict] = []
    for page in range(args.pages):
        # compute date range if --last provided
        date_from = args.date_from
        date_to = args.date_to
        if getattr(args, "last_days", None):
            from datetime import date, timedelta

            d = int(args.last_days)
            today = date.today()
            date_to = today.strftime("%Y-%m-%d")
            date_from = (today - timedelta(days=d)).strftime("%Y-%m-%d")

        payload = client.search(
            query=args.query, area=args.area, per_page=args.per_page, page=page, date_from=date_from, date_to=date_to
        )
        page_items = payload.get("items", [])
        items.extend(page_items)
    path = save_raw_batch(items, source="hh", query=args.query, area=args.area)
    print(f"Fetched {len(items)} items and saved raw to {path}")


def cmd_build_vitrines(args: argparse.Namespace) -> None:
    
    """Строит витрины по каждой профессии и сохраняет их в DuckDB/Postgres.

    Пространство имен `args` ожидает содержать `area`, `pages`, `per_page`,
    `date_from`, `date_to`, `last_days`, `use_duckdb`, и `use_postgres`.
    """
    
    client = HHClient()
    for prof in PROFESSIONS:
        items: List[dict] = []
        for page in range(args.pages):
            date_from = args.date_from
            date_to = args.date_to
            if getattr(args, "last_days", None):
                from datetime import date, timedelta

                d = int(args.last_days)
                today = date.today()
                date_to = today.strftime("%Y-%m-%d")
                date_from = (today - timedelta(days=d)).strftime("%Y-%m-%d")

            payload = client.search(query=prof, area=args.area, per_page=args.per_page, page=page, date_from=date_from, date_to=date_to)
            items.extend(payload.get("items", []))

        df = build_vitrine(items, prof)
        if df.empty:
            print(f"No rows for {prof}")
            continue

        if args.use_duckdb:
            try:
                save_dataframe_to_duckdb(f"vitrine_{prof.replace(' ', '_')}", df)
                print(f"Saved vitrine_{prof} to DuckDB")
            except Exception as exc:
                print(f"DuckDB save failed: {exc}")

        if args.use_postgres:
            try:
                save_dataframe_to_postgres(f"vitrine_{prof.replace(' ', '_')}", df)
                print(f"Saved vitrine_{prof} to Postgres")
            except Exception as exc:
                print(f"Postgres save failed: {exc}")


def main() -> None:
    
    """Главная точка входа CLI, которая парсит аргументы и вызывает соответствующую команду."""
    
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")

    e = sub.add_parser("extract")
    e.add_argument("query")
    e.add_argument("--area", default=None)
    e.add_argument("--pages", type=int, default=1)
    e.add_argument("--per-page", type=int, default=50)
    e.add_argument("--date-from", dest="date_from", default=None)
    e.add_argument("--date-to", dest="date_to", default=None)
    e.add_argument("--last", dest="last_days", choices=["1", "3", "7", "15", "30"], help="Relative period in days: 1,3,7,15,30")
    e.set_defaults(func=cmd_extract)

    b = sub.add_parser("build_vitrines")
    b.add_argument("--area", default=None)
    b.add_argument("--pages", type=int, default=1)
    b.add_argument("--per-page", type=int, default=50)
    b.add_argument("--date-from", dest="date_from", default=None)
    b.add_argument("--date-to", dest="date_to", default=None)
    b.add_argument("--last", dest="last_days", choices=["1", "3", "7", "15", "30"], help="Relative period in days: 1,3,7,15,30")
    b.add_argument("--use-duckdb", action="store_true")
    b.add_argument("--use-postgres", action="store_true")
    b.set_defaults(func=cmd_build_vitrines)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
