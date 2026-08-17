#!/usr/bin/env python3
"""Точка входа для запуска Telegram бота.

Этот модуль просто вызывает `bot.main()` при выполнении как скрипта.
"""

from __future__ import annotations


try:
    from bot.bot import main as run_bot
except ModuleNotFoundError as exc:  # pragma: no cover - helpful runtime message
    missing = exc.name
    raise SystemExit(
        f"Missing dependency: {missing}.\n"
        "Activate the virtualenv and install requirements:\n"
        "  source .venv/bin/activate\n"
        "  pip install -r requirements.txt\n"
    ) from exc


def main() -> None:
    
    """Запуск Telegram бота.

    Эта функция предоставлена, чтобы модуль можно было импортировать и запустить бот
    программно в тестах или других точках входа.
    """
    run_bot()


if __name__ == "__main__":
    main()
