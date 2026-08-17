"""Telegram бот для поиска вакансий по анализу данных на HeadHunter.

Бот поддерживает простые `/search` команды и поиск без текста. 
Параметры: вакансия, регион, зп от, зп до и период поиска.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

try:
    from telegram.request import Request as TgRequest
except Exception:
    TgRequest = None

from config import BOT_TOKEN, DEFAULT_PER_PAGE, TELEGRAM_PROXY, HH_MAX_CONCURRENT_REQUESTS, HH_API_MAX_RESULTS
from extract.hh_api import HHClient
from extract.hh_raw import save_raw_batch_parquet
from load.duckdb_store import build_vacancy_analytics_tables
from transform.professions import PROFESSIONS
from config import HH_API_TOKEN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# глобальный семафор для ограничения одновременных вызовов HH API между обработчиками
HH_SEMAPHORE = asyncio.Semaphore(int(HH_MAX_CONCURRENT_REQUESTS or 3))

async def _safe_answer(q) -> None:
    
    """Безопасный ответ на CallbackQuery, логирующий но не вызывающий исключения при таймаутах или сетевых ошибках."""
    
    if q is None:
        return
    try:
        await q.answer()
    except (TimedOut, BadRequest) as exc:
        msg = str(exc)
        if "Query is too old" in msg or "query id is invalid" in msg.lower() or "response timeout expired" in msg.lower():
            logger.info("Ignoring stale callback query: %s", msg)
            return
        logger.warning("Telegram callback answer failed: %s", exc)
    except Exception:
        logger.exception("Failed to answer callback query (ignored)")


async def _safe_message_reply(message_obj, text: str, **kwargs) -> None:
    
    """Безопасная отправка сообщения, игнорирующая устаревшие запросы или ошибки времени ожидания от Telegram."""
    
    if message_obj is None:
        return
    try:
        await message_obj.reply_text(text, **kwargs)
    except (TimedOut, BadRequest) as exc:
        msg = str(exc)
        if "Query is too old" in msg or "query id is invalid" in msg.lower() or "response timeout expired" in msg.lower():
            logger.info("Skipping reply for stale Telegram callback: %s", msg)
            return
        logger.warning("Telegram reply failed: %s", exc)
    except Exception:
        logger.exception("Failed to send message to user")


async def _safe_reply(obj, text: str, **kwargs) -> None:
    
    """Безопасная отправка сообщения, игнорирующая устаревшие запросы или ошибки времени ожидания от Telegram."""
    
    try:
        # obj is expected to be a `telegram.Message` or similar with `reply_text`
        await obj.reply_text(text, **kwargs)
    except TimedOut:
        logger.warning("Timed out while sending reply; continuing")
    except Exception:
        logger.exception("Failed to send message to user")
    

async def _menu_send_or_edit(update: Update, text: str, keyboard: InlineKeyboardMarkup | None = None) -> None:
    
    """Отправка меню в зависимости от того, находимся ли мы в flow обратного вызова."""
    
    if update.callback_query:
        q = update.callback_query
        await _safe_answer(q)
        try:
            if keyboard is not None:
                await q.message.edit_text(text, reply_markup=keyboard)
            else:
                await q.message.edit_text(text)
            return
        except Exception:
            # fallback to sending a new message
            pass
    # prefer message -> effective_message
    target = update.message if getattr(update, 'message', None) else update.effective_message
    await _safe_reply(target, text, reply_markup=keyboard)
    

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Отправка приветственного сообщения и представление меню быстрого выбора."""
    
    keyboard = []
    # create buttons for preset professions (use PROFESSIONS from transform)
    row = []
    for i, prof in enumerate(PROFESSIONS[:4], start=1):
        row.append(InlineKeyboardButton(prof, callback_data=f"profession:{prof}"))
        if i % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("Своя вакансия", callback_data="profession:__custom__")])

    text = (
        "Привет! Я бот для поиска вакансий на HeadHunter.\n\n"
        "Выберите одну из предустановленных профессий или нажмите 'Своя вакансия'.\n"
        "Далее вы сможете выбрать регион, город и период (1/3/7/15/30 дней или задать дату)."
    )
    await _menu_send_or_edit(update, text, InlineKeyboardMarkup(keyboard))


async def _reply_text_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    
    """Помощник отправки текстового запроса при ожидании следующего ввода пользователя."""
    
    if update.callback_query:
        await _safe_answer(update.callback_query)
        await _safe_reply(update.effective_message, text)
    else:
        await _safe_reply(update.message, text)


def _get_session_token(context: ContextTypes.DEFAULT_TYPE) -> str | None:
    # preference: session token (user input) -> env token -> None
    return context.user_data.get("session_hh_token") or HH_API_TOKEN or None


def _get_session_creds(context: ContextTypes.DEFAULT_TYPE) -> tuple[str | None, str | None]:
    return (
        context.user_data.get("session_hh_client_id"),
        context.user_data.get("session_hh_client_secret"),
    )


async def set_credentials_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Запуск интерактивного потока для установки client_id и client_secret для этой сессии.

    Эти учетные данные хранятся только в сессии пользователя (`context.user_data`) и
    используются для последующих попыток обмена токенами. Они не записываются на диск.
    """
    
    context.user_data["awaiting_client_id"] = True
    await _safe_reply(update.message, "Отправьте клиентский id (client_id):")


async def show_session_creds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    cid = context.user_data.get("session_hh_client_id")
    masked = None
    if cid:
        masked = cid[:4] + "..." + cid[-4:]
    await _safe_reply(update.message, f"Session client_id: {masked if masked else 'none'}")


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Определение команды `/search` с аргументами.

    Ожидает текст запроса после команды; перенаправляет в `process_search`.
    """
    
    args = context.args
    if not args:
        await _safe_reply(update.message, "Укажите запрос после команды: /search data analyst")
        return

    query = " ".join(args)
    await process_search(update, query)


async def search_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Обрабатывает сообщения без текста; запускает поиск, если это не команда."""
    
    query = update.message.text.strip()
    if not query or query.startswith("/"):
        return
    await process_search(update, query)


async def profession_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Обработчик команды профессии; обрабатывает нажатия кнопок профессий в начальном меню."""
    
    query = update.callback_query
    await _safe_answer(query)
    data = query.data or ""
    _, val = data.split(":", 1)
    if val == "__custom__":
        # ask user to type custom query
        context.user_data["awaiting_custom_query"] = True
        await _reply_text_prompt(update, context, "Напишите текст вакансии или запрос, который хотите найти:")
        return

    # preset profession chosen
    context.user_data["query"] = val
    # move to region selection
    keyboard = [
        [InlineKeyboardButton("По всей базе (Все регионы)", callback_data="region:all")],
        [InlineKeyboardButton("Выбрать регион вручную", callback_data="region:choose")],
    ]
    await _safe_reply(query.message, "Выберите регион для поиска:", reply_markup=InlineKeyboardMarkup(keyboard))


async def generic_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Обрабатывает свободный текст ответов, используемый во время интерактивного потока (пользовательский запрос, регион, город, дата)."""
    
    # тексты свободных запросов обрабатываются в `search_text` или в интерактивных ожиданиях ниже
    if context.user_data.pop("awaiting_client_id", False):
        cid = update.message.text.strip()
        context.user_data["session_hh_client_id"] = cid
        context.user_data["awaiting_client_secret"] = True
        await _safe_reply(update.message, "Теперь отправьте client_secret:")
        return

    if context.user_data.pop("awaiting_client_secret", False):
        csecret = update.message.text.strip()
        context.user_data["session_hh_client_secret"] = csecret
        await _safe_reply(update.message, "Креденшиалы сохранены в сессии. Нажмите 'Готово (продолжить)' для продолжения.")
        return

    if context.user_data.pop("awaiting_custom_query", False):
        q = update.message.text.strip()
        context.user_data["query"] = q
        # переходим к выбору региона
        keyboard = [
            [InlineKeyboardButton("По всей базе (Все регионы)", callback_data="region:all")],
            [InlineKeyboardButton("Выбрать регион вручную", callback_data="region:choose")],
        ]
        await _safe_reply(update.message, "Выберите регион для поиска:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if context.user_data.pop("awaiting_region_input", False):
        region_text = update.message.text.strip()
        context.user_data["area"] = region_text
        # спрашиваем город (опционально)
        context.user_data["awaiting_city_input"] = True
        await _safe_reply(update.message, "Введите город (или 'Все' для всех городов в регионе):")
        return

    if context.user_data.pop("awaiting_city_input", False):
        city_text = update.message.text.strip()
        context.user_data["city"] = city_text
        # переходим к выбору периода
        await _send_period_choices(update, context)
        return

    if context.user_data.pop("awaiting_date_input", False):
        # ожидаем период в формате YYYY-MM-DD
        text = update.message.text.strip()
        try:
            datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            await _safe_reply(update.message, "Неправильный формат даты. Ожидается YYYY-MM-DD")
            return
        context.user_data["date_from"] = text
        context.user_data["date_to"] = date.today().strftime("%Y-%m-%d")
        # continue to summary
        await _finalize_search_prep(update, context)
        return

    # ввод зарплаты (от/до) обрабатывается в отдельной функции
    if context.user_data.get("awaiting_salary_min") or context.user_data.get("awaiting_salary_max"):
        await _apply_salary_input(update, context, update.message.text.strip())
        return
    if context.user_data.pop("awaiting_token_input", False):
        token = update.message.text.strip()
        # базовая проверка длины
        if not token or len(token) < 20:
            await _safe_reply(update.message, "Похоже, это невалидный токен. Убедитесь, что вставили правильный HH API token.")
            return
        context.user_data["session_hh_token"] = token
        await _safe_reply(update.message, "Токен сохранён в сессии. Нажмите 'Готово (продолжить)' чтобы продолжить поиск.")
        return

    # если не подняты интерактивные флаги, игнорируем или возвращаемся к нормальному поиску
    await search_text(update, context)


async def region_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """обработчик выбора региона; обрабатывает нажатия кнопок выбора региона."""
    
    q = update.callback_query
    await _safe_answer(q)
    data = q.data or ""
    _, val = data.split(":", 1)
    if val == "all":
        context.user_data["area"] = None
        # ask for period next
        await _send_period_choices(update, context)
        return
    if val == "choose":
        # prompt for manual region input
        context.user_data["awaiting_region_input"] = True
        await _reply_text_prompt(update, context, "Введите id или название региона (например: 113 или Москва):")
        return


async def _send_period_choices(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    keyboard = [
        [InlineKeyboardButton("1 день", callback_data="period:1"), InlineKeyboardButton("3 дня", callback_data="period:3")],
        [InlineKeyboardButton("7 дней", callback_data="period:7"), InlineKeyboardButton("15 дней", callback_data="period:15")],
        [InlineKeyboardButton("30 дней", callback_data="period:30")],
        [InlineKeyboardButton("Задать дату начала", callback_data="period:custom")],
    ]
    await _menu_send_or_edit(update, "Выберите период поиска:", InlineKeyboardMarkup(keyboard))


async def period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    q = update.callback_query
    await _safe_answer(q)
    data = q.data or ""
    _, val = data.split(":", 1)
    if val == "custom":
        context.user_data["awaiting_date_input"] = True
        await _safe_reply(q.message, "Отправьте дату начала поиска в формате YYYY-MM-DD")
        return
    # относительные дни
    try:
        days = int(val)
    except Exception:
        days = 7
    today = date.today()
    context.user_data["date_to"] = today.strftime("%Y-%m-%d")
    context.user_data["date_from"] = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    # переходим к итогу и запрашиваем количество вакансий для вывода
    await _finalize_search_prep(update, context)


def _cache_key_for_query(context: ContextTypes.DEFAULT_TYPE) -> tuple:
    
    """Строим стабильный ключ для текущих фильтров поиска, чтобы повторно использовать ранее загруженные вакансии."""
    
    return (
        context.user_data.get("query"),
        context.user_data.get("area"),
        context.user_data.get("date_from"),
        context.user_data.get("date_to"),
        context.user_data.get("city"),
        context.user_data.get("salary_min"),
        context.user_data.get("salary_max"),
    )


async def _ensure_all_results_loaded(update: Update, context: ContextTypes.DEFAULT_TYPE) -> list[dict]:
    
    """Загружаем все соответствующие вакансии один раз, удаляем дубликаты по id и кэшируем набор результатов."""
    
    token = _get_session_token(context)
    cid, csecret = _get_session_creds(context)
    client = HHClient(token=token).with_client_credentials(cid, csecret)
    query_text = context.user_data.get("query")
    area = context.user_data.get("area")
    date_from = context.user_data.get("date_from")
    date_to = context.user_data.get("date_to")
    salary_min = context.user_data.get("salary_min")
    salary_max = context.user_data.get("salary_max")
    city_filter = context.user_data.get("city")
    cache_key = _cache_key_for_query(context)

    cached = context.user_data.get("results_cache")
    if cached and context.user_data.get("results_cache_key") == cache_key:
        return cached

    cache: list[dict] = []
    seen_ids: set[str] = set()
    payload = None
    
    async def _bounded_search(*args):
    
        async with HH_SEMAPHORE:
            return await asyncio.to_thread(client.search, *args)

    try:
        payload = await _bounded_search(query_text, area, 100, 0, date_from, date_to)
    except Exception as exc:
        msg = str(exc)
        if "400" in msg or "Bad Request" in msg or "400 Client Error" in msg:
            await _safe_message_reply(
                update.effective_message,
                "Ошибка запроса к HH: получен 400 Bad Request. Попробуйте сузить период или изменить запрос.",
            )
            return []
        await _safe_message_reply(update.effective_message, f"Не удалось выполнить поиск: {exc}")
        return []

    total = int((payload or {}).get("found") or 0)
    context.user_data["total_found"] = total
    page = 0
    # расчитываем безопасное максимальное количество страниц для итерации на основе сообщенного `total` и ограничения HH API
    per_page = 100
    try:
        per_page = int(per_page)
    except Exception:
        per_page = 100
    max_pages = 50
    try:
        total_found = int((context.user_data.get("total_found") or 0))
        max_pages = math.ceil(min(total_found or 0, HHClient.HH_API_MAX_RESULTS) / per_page) if total_found else 50
    except Exception:
        max_pages = 50

    page = 0
    last_pct = 0
    while True:
        try:
            payload = await _bounded_search(query_text, area, 100, page, date_from, date_to)
        except Exception as exc:
            msg = str(exc)
            if "400" in msg or "Bad Request" in msg or "400 Client Error" in msg:
                await _safe_message_reply(
                    update.effective_message,
                    "HH вернул 400 при загрузке страницы результатов — остановка загрузки. Попробуйте сузить период или уменьшить диапазон страниц.",
                )
                return cache
            await _safe_message_reply(update.effective_message, f"Ошибка при загрузке вакансий: {exc}")
            return cache

        items = payload.get("items", [])
        if not items:
            break

        for item in items:
            vacancy_id = str(item.get("id") or "")
            if not vacancy_id or vacancy_id in seen_ids:
                continue
            seen_ids.add(vacancy_id)

            if city_filter:
                area_name = (item.get("area") or {}).get("name", "")
                if city_filter.lower() not in area_name.lower() and city_filter.lower() != "все":
                    continue

            if salary_min is not None or salary_max is not None:
                s = item.get("salary")
                if not s:
                    continue
                s_from = s.get("from")
                s_to = s.get("to")
                match = True
                if salary_min is not None and s_to is not None and s_to < salary_min:
                    match = False
                if salary_max is not None and s_from is not None and s_from > salary_max:
                    match = False
                if not match:
                    continue

            cache.append(item)

        # отправляем обновления прогресса (сохраняя ограничения HH API)
        try:
            total_cap = min(int(total or 0), int(HH_API_MAX_RESULTS or 2000)) if total is not None else 0
            pct = int((len(cache) / max(total_cap, 1)) * 100) if total_cap else 0
            if pct - last_pct >= 5 or page % 2 == 0:
                last_pct = pct
                await _menu_send_or_edit(update, f"Идёт загрузка... {pct}%")
        except Exception:
            pass

        if total and len(cache) >= total:
            break
        if len(items) < 100:
            break
        page += 1
        if page >= max_pages:
            break

    context.user_data["results_cache"] = cache
    context.user_data["results_cache_key"] = cache_key
    context.user_data["total_found"] = len(cache) if total == 0 else total

    if cache:
        try:
            raw_parquet_path = save_raw_batch_parquet(cache, source="bot", query=query_text or "search", area=area or "all")
            build_vacancy_analytics_tables(str(raw_parquet_path))
            context.user_data["last_raw_parquet"] = str(raw_parquet_path)
        except Exception:
            logger.exception("Failed to persist bot search results to parquet/duckdb")

    return cache


async def _finalize_search_prep(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """запрос к HH для получения общего количества найденных вакансий и запроса о количестве для отображения на странице."""
    
    token = _get_session_token(context)
    cid, csecret = _get_session_creds(context)
    client = HHClient(token=token).with_client_credentials(cid, csecret)
    query_text = context.user_data.get("query")
    area = context.user_data.get("area")
    date_from = context.user_data.get("date_from")
    date_to = context.user_data.get("date_to")
    try:
        payload = await asyncio.to_thread(
            client.search, query_text, area, 1, 0, date_from, date_to
        )
    except Exception as exc:
        msg = f"Не удалось выполнить поиск: {exc}"
        keyboard = [[InlineKeyboardButton("Вставить HH_API_TOKEN", callback_data="token:paste")]]
        await _safe_message_reply(update.effective_message, msg, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    total = payload.get("found") or len(payload.get("items", []))
    context.user_data["total_found"] = int(total)
    await _ensure_all_results_loaded(update, context)
    total_loaded = len(context.user_data.get("results_cache", []))
    keyboard = [
        [InlineKeyboardButton("3", callback_data="perpage:3"), InlineKeyboardButton("5", callback_data="perpage:5")],
        [InlineKeyboardButton("10", callback_data="perpage:10"), InlineKeyboardButton("20", callback_data="perpage:20")],
    ]
    keyboard.append([InlineKeyboardButton("Настроить фильтр", callback_data="filter:menu")])
    await _menu_send_or_edit(update, f"Найдено {total_loaded} вакансий после фильтрации. Сколько показывать за раз?", InlineKeyboardMarkup(keyboard))


async def perpage_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    q = update.callback_query
    await _safe_answer(q)
    data = q.data or ""
    _, val = data.split(":", 1)
    try:
        per_page = int(val)
    except Exception:
        per_page = DEFAULT_PER_PAGE
    context.user_data["per_page_choice"] = per_page
    # acknowledge selection visibly and fetch initial batch
    try:
        await _safe_reply(q.message, f"Выбрано показывать {per_page} вакансии(й) за раз. Загружаю...")
    except Exception:
        # fallback if message not available
        await _safe_message_reply(update.effective_message, f"Выбрано показывать {per_page} вакансии(й) за раз. Загружаю...")
    logger.info("Per-page selected: %s", per_page)
    await _send_batch(update, context, start_index=0)


async def filter_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Отображение опций фильтра: изменить регион или установить диапазон зарплат."""
    
    q = update.callback_query
    await _safe_answer(q)
    keyboard = [
        [InlineKeyboardButton("Изменить регион", callback_data="filter:region")],
        [InlineKeyboardButton("Фильтр по зарплате", callback_data="filter:salary")],
        [InlineKeyboardButton("Готово (продолжить)", callback_data="filter:done")],
    ]
    await _menu_send_or_edit(update, "Настройка фильтров:", InlineKeyboardMarkup(keyboard))


async def token_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Инициирует сессию ввода токена."""
    
    q = update.callback_query
    await _safe_answer(q)
    context.user_data["awaiting_token_input"] = True
    await _safe_reply(q.message, "Отправьте HH API token (вставьте сюда). Этот токен будет использоваться только в текущей сессии.")


async def set_token_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Команда для запуска процесса вставки токена (альтернатива кнопке)."""
    
    context.user_data["awaiting_token_input"] = True
    await _safe_reply(update.message, "Отправьте HH API token (вставьте сюда). Этот токен будет использоваться только в текущей сессии.")


async def _apply_salary_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    # число или "нет" для пропуска
    t = text.strip().lower()
    if t in ("нет", "no", "n", "none", "-"):
        val = None
    else:
        try:
            val = int(text.replace(" ", ""))
        except Exception:
            await _safe_message_reply(update.effective_message, "Пожалуйста, введите число либо 'нет' если не хотите указывать это значение.")
            return
    # определяем, на каком этапе мы находимся
    if context.user_data.pop("awaiting_salary_min", False):
        context.user_data["salary_min"] = val
        context.user_data["awaiting_salary_max"] = True
        await _safe_message_reply(update.effective_message, "Введите максимальную зарплату (числом) или 'нет' для пропуска:")
        return
    if context.user_data.pop("awaiting_salary_max", False):
        context.user_data["salary_max"] = val
        context.user_data["salary_max"] = val
        # подтвердить и предложить кнопку для продолжения, чтобы пользователь мог установить токен, если это необходимо
        keyboard = [[InlineKeyboardButton("Готово (продолжить)", callback_data="filter:done")]]
        await _safe_message_reply(update.effective_message, "Фильтр зарплаты сохранён.", reply_markup=InlineKeyboardMarkup(keyboard))
        return


async def filter_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Управление обратными вызовами фильтров."""
    
    q = update.callback_query
    await _safe_answer(q)
    _, val = (q.data or "").split(":", 1)
    if val == "region":
        context.user_data["awaiting_region_input"] = True
        await _safe_reply(q.message,
            "Введите id или название региона (например: 113 или Москва).\nПример: отправьте `113` для Москвы или `Санкт-Петербург`.",
            reply_markup=None,
        )
        return
    if val == "salary":
        context.user_data["awaiting_salary_min"] = True
        await _safe_reply(q.message,
            "Введите минимальную зарплату (числом) или 'нет' чтобы не указывать минимальное значение.\nПример: `50000`",
            reply_markup=None,
        )
        return
    if val == "done":
        # user finished filters; re-run perpage prompt without changing per_page
        await _finalize_search_prep(update, context)
        return


def _format_vacancy(item: dict, index: int) -> str:
    salary = item.get("salary")
    salary_text = "не указана"
    if salary:
        f = salary.get("from")
        t = salary.get("to")
        cur = salary.get("currency", "")
        if f and t:
            salary_text = f"от {f} до {t} {cur}"
        elif f:
            salary_text = f"от {f} {cur}"
        elif t:
            salary_text = f"до {t} {cur}"
    name = item.get('name', 'Без названия')
    employer = item.get('employer') or {}
    employer_name = employer.get('name', 'Неизвестно') if isinstance(employer, dict) else str(employer)
    area = item.get('area') or {}
    area_name = area.get('name', 'Не указан') if isinstance(area, dict) else str(area)
    return (
        f"{index}. {name}\n"
        f"Компания: {employer_name}\n"
        f"Зарплата: {salary_text}\n"
        f"Город: {area_name}\n"
        f"Ссылка: {item.get('alternate_url','')}\n"
    )


def extract_skill_names(item: dict) -> list[str]:
    
    """Возвращает список названий навыков для элемента вакансии.

    Предпочитает поле `name`, если оно присутствует в записях `key_skills`. Поддерживает
    общие представления (список словарей, строка JSON, строка с разделителями,
    единственный словарь).
    """
    
    names: list[str] = []
    if not item:
        return names

    candidates = []
    # common keys
    for key in ("key_skills", "key_skill", "skills"):
        if key in item and item.get(key) is not None:
            candidates.append(item.get(key))
    # also consider top-level 'key_skills' absent but maybe 'skills' embedded in other fields
    if not candidates and "key_skills" in item:
        candidates.append(item.get("key_skills"))

    def _yield_from_val(val):
        if val is None:
            return
        # handle numpy arrays resulting from parquet roundtrip
        try:
            import numpy as _np
        except Exception:
            _np = None
        if _np is not None and isinstance(val, _np.ndarray):
            for e in val.tolist():
                if e is None:
                    continue
                yield from _yield_from_val(e)
            return
        # dict with name
        if isinstance(val, dict):
            n = val.get("name") or val.get("skill")
            if n:
                yield str(n)
            return
        # list/tuple/set
        if isinstance(val, (list, tuple, set)):
            for e in val:
                if isinstance(e, dict):
                    n = e.get("name") or e.get("skill")
                    if n:
                        yield str(n)
                else:
                    yield str(e)
            return
        # string: try JSON or comma-separated
        if isinstance(val, str):
            s = val.strip()
            try:
                import json as _json

                parsed = _json.loads(s)
                if isinstance(parsed, (list, tuple)):
                    for e in parsed:
                        yield str(e)
                    return
                if isinstance(parsed, dict):
                    n = parsed.get("name") or parsed.get("skill")
                    if n:
                        yield str(n)
                    return
            except Exception:
                pass
            # split by common delimiters
            parts = re.split(r"[,;|\\n]+", s)
            for p in parts:
                p = p.strip().strip('"\'')
                if p:
                    yield p

    for c in candidates:
        for n in _yield_from_val(c):
            if n:
                names.append(str(n).strip())

    return names


def _build_stats_text(cache: list[dict]) -> str:
    
    """Строит из ранее загруженного кэша краткое резюме для Telegram."""
    
    if not cache:
        return "Нет данных для статистики."

    # city_rows stores aggregations and lists for median calculations
    city_rows: dict[str, dict[str, object]] = {}
    # map lowercase skill -> dict(count=int, example=str)
    skills_counter: dict[str, dict[str, object]] = {}
    salary_values: list[int] = []

    for item in cache:
        city = (item.get("area") or {}).get("name", "Не указан")
        salary = item.get("salary") or {}
        s_from = salary.get("from")
        s_to = salary.get("to")
        if isinstance(s_from, (int, float)):
            salary_values.append(int(s_from))
        if city not in city_rows:
            city_rows[city] = {
                "count": 0,
                "sum_from": 0.0,
                "sum_to": 0.0,
                "count_from": 0,
                "count_to": 0,
                "from_values": [],
                "to_values": [],
                "remote_count": 0,
            }
        city_rows[city]["count"] = int(city_rows[city].get("count", 0)) + 1
        if isinstance(s_from, (int, float)):
            city_rows[city]["sum_from"] = float(city_rows[city].get("sum_from", 0.0)) + float(s_from)
            city_rows[city]["count_from"] = int(city_rows[city].get("count_from", 0)) + 1
            city_rows[city]["from_values"].append(float(s_from))
        if isinstance(s_to, (int, float)):
            city_rows[city]["sum_to"] = float(city_rows[city].get("sum_to", 0.0)) + float(s_to)
            city_rows[city]["count_to"] = int(city_rows[city].get("count_to", 0)) + 1
            city_rows[city]["to_values"].append(float(s_to))

        # extract skills using helper that prefers `name` keys
        for name in extract_skill_names(item):
            if name:
                key = str(name).strip().lower()
                example = str(name).strip()
                if not key:
                    continue
                if key not in skills_counter:
                    skills_counter[key] = {"count": 1, "example": example}
                else:
                    skills_counter[key]["count"] = int(skills_counter[key].get("count", 0)) + 1

        # detect remote work indicators
        wf = item.get("work_format") or item.get("workplace_type") or item.get("schedule")
        is_remote = False
        try:
            # list/tuple/set
            if isinstance(wf, (list, tuple, set)):
                for e in wf:
                    if isinstance(e, dict):
                        if str(e.get("id", "")).lower().startswith("remote") or str(e.get("id", "")).lower().find("remote") != -1:
                            is_remote = True
                    else:
                        if str(e).lower().find("remote") != -1:
                            is_remote = True
            elif isinstance(wf, dict):
                if str(wf.get("id", "")).lower().find("remote") != -1:
                    is_remote = True
                if str(wf.get("name", "")).lower().find("remote") != -1:
                    is_remote = True
            elif isinstance(wf, str):
                if wf.lower().find("remote") != -1 or wf.lower().find("удал") != -1:
                    is_remote = True
        except Exception:
            is_remote = False
        if is_remote:
            try:
                city_rows[city]["remote_count"] = int(city_rows[city].get("remote_count", 0)) + 1
            except Exception:
                city_rows[city]["remote_count"] = 1

    top_cities = sorted(city_rows.items(), key=lambda kv: kv[1]["count"], reverse=True)[:5]
    # строит таблицу с медианными значениями и процентом удаленной работы
    city_lines = ["Топ-5 городов:"]
    col_city = 20
    col_count = 8
    col_avg = 10
    col_med = 10
    col_remote = 8
    header = (
        f"{'Город':<{col_city}} | {'Вак':^{col_count}} | {'Ср мин':^{col_avg}} | {'Ср макс':^{col_avg}} | {'Мед мин':^{col_med}} | {'Мед макс':^{col_med}} | {'% Удал':^{col_remote}}"
    )
    sep = (
        f"{('-' * col_city)}-+-{('-' * col_count)}-+-{('-' * col_avg)}-+-{('-' * col_avg)}-+-{('-' * col_med)}-+-{('-' * col_med)}-+-{('-' * col_remote)}"
    )
    city_lines.append("```" + "\n" + header + "\n" + sep)
    import statistics as _stats
    for city, payload in top_cities:
        count = int(payload["count"])
        avg_from = round(float(payload["sum_from"]) / max(payload["count_from"], 1), 0) if payload["count_from"] else 0
        avg_to = round(float(payload["sum_to"]) / max(payload["count_to"], 1), 0) if payload["count_to"] else 0
        med_from = int(_stats.median(payload.get("from_values")) if payload.get("from_values") else 0)
        med_to = int(_stats.median(payload.get("to_values")) if payload.get("to_values") else 0)
        remote_pct = int((payload.get("remote_count", 0) / max(count, 1)) * 100)
        city_lines.append(
            f"{city[:col_city]:<{col_city}} | {count:^{col_count}} | {avg_from:^{col_avg}} | {avg_to:^{col_avg}} | {med_from:^{col_med}} | {med_to:^{col_med}} | {remote_pct:^{col_remote}}"
        )
    city_lines.append("```")

    avg_min = round(sum(salary_values) / len(salary_values), 0) if salary_values else 0
    max_salary_values = [
        int((item.get("salary") or {}).get("to"))
        for item in cache
        if isinstance((item.get("salary") or {}).get("to"), (int, float))
    ]
    avg_max = round(sum(max_salary_values) / len(max_salary_values), 0) if max_salary_values else 0

    # преобразуем в список (ключ, количество, пример) и сортируем
    skills_list = [(k, v.get("count", 0), v.get("example", k)) for k, v in skills_counter.items()]
    top_skills = sorted(skills_list, key=lambda kv: (-kv[1], kv[0]))[:10]
    skill_lines = ["\nТоп навыки:"]
    if top_skills:
        for i, (key, count, example) in enumerate(top_skills, start=1):
            display = example or key
            skill_lines.append(f"{i}. {display} — {count}")
    else:
        skill_lines.append("Нет данных о навыках.")
    city_summary = "\n".join(city_lines)
    stats_lines = [
        "\nОбщая статистика:",
        f"Количество вакансий: {len(cache)}",
        f"Количество городов: {len(city_rows)}",
        f"Средняя минимальная зарплата: {avg_min}",
        f"Средняя максимальная зарплата: {avg_max}",
    ]
    return "\n".join([city_summary, *skill_lines, *stats_lines])


async def stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Показывает краткую статистику по текущему кэшу и предлагает кнопки для просмотра топовых навыков."""
    
    q = update.callback_query
    await _safe_answer(q)
    cache = context.user_data.get("results_cache", [])
    text = _build_stats_text(cache)
    keyboard = [
        [InlineKeyboardButton("Топ 5 навыков", callback_data="stats:top:5"), InlineKeyboardButton("Топ 10 навыков", callback_data="stats:top:10")],
        [InlineKeyboardButton("Топ 20 навыков", callback_data="stats:top:20")],
    ]
    await _safe_message_reply(update.effective_message, text, disable_web_page_preview=True, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def stats_top_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Обработчик inline callback для отображения топ-N навыков для текущего кэша или последнего parquet."""
    
    q = update.callback_query
    await _safe_answer(q)
    data = q.data or ""
    parts = data.split(":")
    try:
        top_n = int(parts[-1])
    except Exception:
        top_n = 10

    cache = context.user_data.get("results_cache")
    if cache:
        skills_counter = {}
        for item in cache:
            for name in extract_skill_names(item):
                if not name:
                    continue
                key = str(name).strip().lower()
                skills_counter[key] = skills_counter.get(key, 0) + 1
        top = sorted(skills_counter.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
        lines = [f"Топ {top_n} навыков:"]
        for name, cnt in top:
            lines.append(f"- {name.title()}: {cnt}")
        await _safe_message_reply(update.effective_message, "\n".join(lines))
        return

    parquet = context.user_data.get("last_raw_parquet")
    if parquet:
        try:
            tables = build_vacancy_analytics_tables(parquet)
            skills_df = tables.get("skills_summary")
            if skills_df is None or skills_df.empty:
                await _safe_message_reply(update.effective_message, "Нет данных о навыках.")
                return
            lines = [f"Топ {top_n} навыков (DuckDB):"]
            for _, row in skills_df.head(top_n).iterrows():
                lines.append(f"- {str(row['skill']).title()}: {int(row['count'])}")
            await _safe_message_reply(update.effective_message, "\n".join(lines))
            return
        except Exception as exc:
            await _safe_message_reply(update.effective_message, f"Ошибка при вычислении статистики: {exc}")
            return

    await _safe_message_reply(update.effective_message, "Нет данных для статистики. Выполните поиск сначала.")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    
    """Команда /stats [N] для отображения топ-N навыков из текущего кэша или последнего parquet."""
    
    top_n = 10
    if context.args:
        try:
            top_n = int(context.args[0])
        except Exception:
            top_n = 10

    cache = context.user_data.get("results_cache")
    if cache:
        skills_counter = {}
        for item in cache:
            for name in extract_skill_names(item):
                if not name:
                    continue
                key = str(name).strip().lower()
                skills_counter[key] = skills_counter.get(key, 0) + 1
        top = sorted(skills_counter.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
        lines = [f"Топ {top_n} навыков:"]
        for name, cnt in top:
            lines.append(f"- {name.title()}: {cnt}")
        await _safe_reply(update.message, "\n".join(lines))
        return

    parquet = context.user_data.get("last_raw_parquet")
    if parquet:
        try:
            tables = build_vacancy_analytics_tables(parquet)
            skills_df = tables.get("skills_summary")
            if skills_df is None or skills_df.empty:
                await _safe_reply(update.message, "Нет данных о навыках.")
                return
            lines = [f"Топ {top_n} навыков (DuckDB):"]
            for _, row in skills_df.head(top_n).iterrows():
                lines.append(f"- {str(row['skill']).title()}: {int(row['count'])}")
            await _safe_reply(update.message, "\n".join(lines))
            return
        except Exception as exc:
            await _safe_reply(update.message, f"Ошибка при вычислении статистики: {exc}")
            return

    await _safe_reply(update.message, "Нет данных для статистики. Выполните поиск сначала.")


async def _send_batch(update: Update, context: ContextTypes.DEFAULT_TYPE, start_index: int = 0) -> None:
    
    """Использует предварительно загруженный, дедуплицированный кэш и отображает фрагмент результатов.

    Мы загружаем все соответствующие вакансии один раз перед шагом выбора по страницам, поэтому
    последующая навигация только извлекает уже загруженные данные вместо повторного запроса HH.
    """
    
    per_page_choice = int(context.user_data.get("per_page_choice", DEFAULT_PER_PAGE))
    total = int(context.user_data.get("total_found", 0))

    cache = await _ensure_all_results_loaded(update, context)
    context.user_data["results_cache"] = cache

    if not cache:
        await _safe_message_reply(update.effective_message, "Нет доступных вакансий для показа.")
        return

    end_index = min(start_index + per_page_choice, len(cache))
    if start_index >= end_index:
        await _safe_message_reply(update.effective_message, "Нет доступных вакансий для показа.")
        return

    parts = [f"Показываю вакансии {start_index+1}-{end_index} из {len(cache)}:\n"]
    for i, item in enumerate(cache[start_index:end_index], start=start_index + 1):
        parts.append(_format_vacancy(item, i))

    text = "\n".join(parts)
    keyboard = []
    if end_index < len(cache):
        keyboard = [[InlineKeyboardButton("Показать ещё", callback_data=f"showmore:{end_index}")]]
    keyboard.append([InlineKeyboardButton("Статистика", callback_data="stats:summary")])
    await _safe_message_reply(update.effective_message, text[:4000], reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None, disable_web_page_preview=True)


async def showmore_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    # answering callback may fail if network/proxy to Telegram times out; don't let that crash the handler
    try:
        await q.answer()
    except TimedOut:
        logger.warning("Timed out while answering callback query (showmore); continuing without ack")
    except Exception:
        logger.exception("Failed to answer callback query (showmore)")

    data = q.data or ""
    _, val = data.split(":", 1)
    try:
        start_index = int(val)
    except Exception:
        start_index = 0

    try:
        await _send_batch(update, context, start_index=start_index)
    except TimedOut:
        # network to Telegram timed out when sending messages
        logger.exception("Timed out while sending next batch of vacancies")
        try:
            await _safe_message_reply(update.effective_message, "Ошибка сети: таймаут при отправке следующей порции вакансий. Попробуйте снова.")
        except Exception:
            logger.exception("Also failed to send fallback timeout message to user")
    except Exception as exc:
        logger.exception("Error while handling showmore callback: %s", exc)
        try:
            await _safe_message_reply(update.effective_message, f"Ошибка при загрузке следующих вакансий: {exc}")
        except Exception:
            logger.exception("Failed to notify user about showmore error")


async def process_search(
    update: Update,
    query: str,
    area: Optional[str] = None,
    per_page: int = DEFAULT_PER_PAGE,
) -> None:
    
    """Парсер inline параметров и запрос HH для вакансий.

    Принимаемые inline параметры: `area:<id>`, `from:<YYYY-MM-DD>`, `to:<YYYY-MM-DD>`,
    и `last:Nd` (N in 1,3,7,15,30) для выбора относительного периода.
    """
    
    # парсер опциональных inline параметров в запросе, например, "data analyst area:113 from:2026-01-01 to:2026-06-01"
    parsed_area = area
    date_from = None
    date_to = None
    # поиск токенов вида area:123 или area=123
    tokens = query.split()
    clean_tokens = []
    for t in tokens:
        m_area = re.match(r'^(?:area|a)[:=](\d+)$', t, flags=re.IGNORECASE)
        m_from = re.match(r'^(?:from|date_from|since)[:=](\d{4}-\d{2}-\d{2})$', t, flags=re.IGNORECASE)
        m_to = re.match(r'^(?:to|date_to|until)[:=](\d{4}-\d{2}-\d{2})$', t, flags=re.IGNORECASE)
        m_last = re.match(r'^(?:last|l)?[:=_-]?(1|3|7|15|30)d$', t, flags=re.IGNORECASE)
        if m_area:
            parsed_area = m_area.group(1)
            continue
        if m_from:
            # валидация формата даты
            try:
                datetime.strptime(m_from.group(1), "%Y-%m-%d")
            except ValueError:
                await _safe_reply(update.message, "Неверный формат даты. Ожидается YYYY-MM-DD для from")
                return
            date_from = m_from.group(1)
            continue
        if m_to:
            try:
                datetime.strptime(m_to.group(1), "%Y-%m-%d")
            except ValueError:
                await _safe_reply(update.message, "Неверный формат даты. Ожидается YYYY-MM-DD для to")
                return
            date_to = m_to.group(1)
            continue
        if m_last:
            days = int(m_last.group(1))
            # вычисляет date_from как сегодня - дней
            today = date.today()
            date_to = today.strftime("%Y-%m-%d")
            date_from = (today - timedelta(days=days)).strftime("%Y-%m-%d")
            continue
        clean_tokens.append(t)

    query_clean = " ".join(clean_tokens)
    token = _get_session_token(context)
    cid, csecret = _get_session_creds(context)
    client = HHClient(token=token).with_client_credentials(cid, csecret)
    try:
        # строит параметры и включает необязательные фильтры дат, если они предоставлены
        payload = client.search(query=query_clean, area=parsed_area, per_page=per_page, page=0, date_from=date_from, date_to=date_to)
        items = payload.get("items", [])
        
    except Exception as exc:
        logger.exception("HH search failed")
        await _safe_reply(update.message, f"Не удалось выполнить поиск: {exc}")
        return

    if not items:
        await _safe_reply(update.message, f"По запросу '{query}' ничего не найдено.")
        return

    response = [f"Найдено {len(items)} вакансий по запросу: {query}\n"]
    for index, item in enumerate(items[:per_page], start=1):
        salary = item.get("salary")
        from_value = salary.get("from") if salary else None
        to_value = salary.get("to") if salary else None
        salary_text = "не указана"
        if from_value and to_value:
            salary_text = f"от {from_value} до {to_value} {salary.get('currency', '')}"
        elif from_value:
            salary_text = f"от {from_value} {salary.get('currency', '')}"
        elif to_value:
            salary_text = f"до {to_value} {salary.get('currency', '')}"

        response.append(
            f"{index}. {item.get('name', 'Без названия')}\n"
            f"Компания: {item.get('employer', {}).get('name', 'Неизвестно')}\n"
            f"Зарплата: {salary_text}\n"
            f"Город: {item.get('area', {}).get('name', 'Не указан')}\n"
            f"Ссылка: {item.get('alternate_url', '')}\n"
        )

    message = "\n".join(response)
    await _safe_reply(update.message, message[:4000], disable_web_page_preview=True)


def main() -> None:
    
    """Основная функция запускает Telegram bot application (long-running)."""
    
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set. Add it to .env")

    # конфигурировает таймауты HTTP клиента для избежания немедленных сбоев на временных
    # сетевых проблемах
    req = None
    if TgRequest is not None:
        req_kwargs = dict(
            connect_timeout=10.0, read_timeout=20.0, pool_timeout=5.0, con_pool_size=10
        )
        if TELEGRAM_PROXY:
            req_kwargs["proxy_url"] = TELEGRAM_PROXY
        try:
            req = TgRequest(**req_kwargs)
        except Exception:
            req = None

    if req is not None:
        application = Application.builder().token(BOT_TOKEN).request(req).build()
    else:
        application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CommandHandler("set_credentials", set_credentials_command))
    application.add_handler(CommandHandler("show_creds", show_session_creds))
    application.add_handler(CommandHandler("set_token", set_token_command))
    application.add_handler(CommandHandler("stats", stats_command))
    # callbacks от inline клавиатур и кнопок
    application.add_handler(CallbackQueryHandler(profession_callback, pattern=r"^profession:"))
    application.add_handler(CallbackQueryHandler(region_callback, pattern=r"^region:"))
    application.add_handler(CallbackQueryHandler(period_callback, pattern=r"^period:"))
    application.add_handler(CallbackQueryHandler(perpage_callback, pattern=r"^perpage:"))
    application.add_handler(CallbackQueryHandler(showmore_callback, pattern=r"^showmore:"))
    application.add_handler(CallbackQueryHandler(filter_menu_callback, pattern=r"^filter:menu$"))
    application.add_handler(CallbackQueryHandler(filter_callback_router, pattern=r"^filter:(region|salary|done)$"))
    application.add_handler(CallbackQueryHandler(token_callback, pattern=r"^token:paste$"))
    application.add_handler(CallbackQueryHandler(stats_callback, pattern=r"^stats:summary$"))
    application.add_handler(CallbackQueryHandler(stats_top_callback, pattern=r"^stats:top:\\d+$"))
    # обработчик сообщений управляет интерактивными ответами (пользовательский запрос, ввод региона, город, дата)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, generic_message_handler))
    try:
        application.run_polling()
    except TimedOut as exc:
        logger.error("Telegram API timed out: %s", exc)
        print(
            "Не удалось подключиться к Telegram API: истекло время ожидания. Проверьте подключение к сети или прокси."
        )
    except Exception as exc:
        logger.exception("Failed to start bot")
        print(f"Не удалось запустить бота из-за ошибки: {exc}")
