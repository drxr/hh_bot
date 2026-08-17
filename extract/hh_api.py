"""Клиент для работы с HeadHunter (HH) API.

Этот модуль обеспечивает простой клиент `HHClient` для работы с HeadHunter (HH) API, 
поддерживающий аутентификацию приложения (client credentials) и простые помощники для поиска вакансий.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional
import math
import time
from requests.exceptions import RequestException

import requests

from config import HH_API_TOKEN, HH_API_URL, HH_AREA
from config import HH_CLIENT_ID, HH_CLIENT_SECRET
from config import HH_PROXY
from config import HH_API_MAX_RESULTS, HH_RETRIES, HH_RETRY_BACKOFF_BASE


class HHAuthError(RuntimeError):
    
    """Вызывается, когда аутентификация HH или обмен токенов не удается."""


class HHClient:
    
    """Minimal HeadHunter API client.

    Параметры
    ----------
    token:
        Опциональный статический токен доступа (имеет приоритет над учетными данными клиента).
    base_url:
        Базовый URL для API HH (по умолчанию - конфигурация).
    """

    def __init__(self, token: str | None = None, base_url: str | None = None) -> None:
        self.token: Optional[str] = token or HH_API_TOKEN
        self.base_url: str = (base_url or HH_API_URL).rstrip("/")
        # опциональные per-instance client credentials (override env)
        self._client_id: Optional[str] = None
        self._client_secret: Optional[str] = None
        self._access_token: Optional[str] = None
        self._token_expiry: float = 0.0

    def with_client_credentials(self, client_id: str | None, client_secret: str | None) -> "HHClient":
        
        """Возвращает self, настроенный для использования этих учетных данных клиента для обмена токенов.

        Это изменяет экземпляр на месте и возвращает его для удобства.
        """
        
        self._client_id = client_id
        self._client_secret = client_secret
        return self

    def _headers(self) -> Dict[str, str]:
        
        """Возвращает стандартные заголовки для запросов HH, включая Authorization при наличии."""
       
        headers: Dict[str, str] = {"Accept": "application/json"}
        tk = self.token or self._ensure_token()
        if tk:
            headers["Authorization"] = f"Bearer {tk}"
        return headers


    def _ensure_token(self) -> Optional[str]:
        
        """Проверяет, доступен ли токен доступа.

        Возвращает кэшированный токен, если он еще действителен, в противном случае пытается получить
        его через поток учетных данных клиента, когда настроены `HH_CLIENT_ID`/`HH_CLIENT_SECRET`.
        Возвращает None, если токен недоступен.
        """
        # предпочитаем явный токен из окружения
        if self.token:
            return self.token

        # если токен получен и он еще действителен, вернуть его
        if self._access_token and time.time() < self._token_expiry - 30:
            return self._access_token

        # пробуем клиентский поток учетных данных: предпочитаем экземплярные учетные данные, возвращаемся к переменным окружения
        client_id = self._client_id or HH_CLIENT_ID
        client_secret = self._client_secret or HH_CLIENT_SECRET
        if client_id and client_secret:
            # endpoint для получения токена находится на hh.ru (не на api.hh.ru)
            token_url = "https://hh.ru/oauth/token"
            data = {"grant_type": "client_credentials", "client_id": HH_CLIENT_ID, "client_secret": HH_CLIENT_SECRET}
            # добавляем общие заголовки, чтобы избежать блокировки защитой от DDoS
            token_headers = {"Accept": "application/json", "User-Agent": "hh_parser/1.0 (contact: dev)"}
            try:
                # сначала без прокси
                data = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
                resp = requests.post(token_url, data=data, headers=token_headers, timeout=10)
                resp.raise_for_status()
                j = resp.json()
                access = j.get("access_token") or j.get("token")
                expires_in = int(j.get("expires_in", 3600))
                if access:
                    self._access_token = access
                    self._token_expiry = time.time() + expires_in
                    return self._access_token
            except Exception:
                # пробуем вариант с Basic auth и включаем текст ответа для отладки
                try:
                    resp = requests.post(token_url, data={"grant_type": "client_credentials"}, auth=(client_id, client_secret), headers=token_headers, timeout=10)
                    resp.raise_for_status()
                    j = resp.json()
                    access = j.get("access_token") or j.get("token")
                    expires_in = int(j.get("expires_in", 3600))
                    if access:
                        self._access_token = access
                        self._token_expiry = time.time() + expires_in
                        return self._access_token
                except Exception as exc:
                    # surface HTTP response body когда доступно
                    try:
                        body = resp.text
                    except Exception:
                        body = "<no response body>"
                    status = getattr(resp, "status_code", "n/a")
                    # если блок и настроен прокси, пробуем снова через прокси
                    if status == 403 and HH_PROXY:
                        try:
                            proxies = {"https": HH_PROXY}
                            # retry body flow via proxy
                            r2 = requests.post(token_url, data=data, headers=token_headers, timeout=10, proxies=proxies)
                            r2.raise_for_status()
                            j = r2.json()
                            access = j.get("access_token") or j.get("token")
                            expires_in = int(j.get("expires_in", 3600))
                            if access:
                                self._access_token = access
                                self._token_expiry = time.time() + expires_in
                                return self._access_token
                        except Exception:
                            # проваливаемся в оригинальную обработку ошибок
                            pass

                    # если блокированы DDoS-Guard или аналогичной защитой, даем практические советы
                    if status == 403 and isinstance(body, str) and "DDoS-Guard" in body:
                        raise HHAuthError(
                            "Access blocked by DDoS protection (403). Try one of: \n"
                            " - use a static HH_API_TOKEN in .env,\n"
                            " - switch your network (mobile hotspot / different ISP), or\n"
                            " - configure HH_PROXY to route requests via a trusted proxy, or\n"
                            " - contact HH support with the request details.\n"
                            f"Server response snippet: {body[:200]}"
                        )
                    raise HHAuthError(
                        f"Failed to obtain access token (status={status}) : {body} ({exc})"
                    )

        return None


    # Настраиваем лимиты
    HH_API_MAX_RESULTS = HH_API_MAX_RESULTS

    def search(
        self,
        query: str,
        area: str | int | None = None,
        per_page: int = 5,
        page: int = 0,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> Dict[str, Any]:
        # ограничиваем per_page в соответствии с документацией HH API (макс. 100)
        per_page = min(int(per_page or 0), 100) or 5
        params = {
            "text": query,
            "area": area or HH_AREA,
            "per_page": per_page,
            "page": page,
        }
        # опциональные фильтры по дате (строки в формате ISO)
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to

        # выполняем запрос с повторными попытками и экспоненциальной задержкой для временных ошибок (429/5xx)
        max_retries = HH_RETRIES
        backoff = HH_RETRY_BACKOFF_BASE
        last_exc: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    f"{self.base_url}/vacancies", params=params, headers=self._headers(), timeout=20
                )
                # считываем статус безопасно (моки могут не иметь status_code)
                status = getattr(response, "status_code", None)
                try:
                    status_val = int(status) if status is not None else None
                except Exception:
                    status_val = None
                # если ошибка 400 - пробуем адаптивное уменьшение per_page перед тем, как завершить с ошибкой
                if status_val == 400:
                    # пытаемся уменьшить per_page и повторить запрос с несколькими меньшими значениями
                    tried_sizes = []
                    current_pp = per_page
                    while current_pp > 5 and len(tried_sizes) < 3:
                        tried_sizes.append(current_pp)
                        current_pp = max(5, current_pp // 2)
                        params["per_page"] = current_pp
                        try:
                            resp2 = requests.get(f"{self.base_url}/vacancies", params=params, headers=self._headers(), timeout=20)
                            status2 = getattr(resp2, "status_code", None)
                            try:
                                status2_val = int(status2) if status2 is not None else None
                            except Exception:
                                status2_val = None
                            if status2_val is not None and status2_val >= 200 and status2_val < 400:
                                resp2.raise_for_status()
                                return resp2.json()
                        except Exception:
                            # fallthrough - will retry outer loop/backoff
                            pass
                    # если адаптивные попытки не удались, вызываем исключение
                    response.raise_for_status()
                if status_val in (401, 403):
                    raise HHAuthError(f"Authentication/authorization failed (status {status_val}): {response.text}")
                if status_val is not None and (500 <= status_val < 600 or status_val == 429):
                    # transient server error or rate limit - retry
                    last_exc = RequestException(f"HTTP {status_val}: {response.text}")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                response.raise_for_status()
                data = response.json()
                # enforce HH max results cap: API может найти большое количество вакансий, но доступны только первые 2000
                if isinstance(data, dict) and "found" in data:
                    found = int(data.get("found") or 0)
                    # если клиент просит страницу за пределами лимита HH API, вызываем исключение
                    max_pages = math.ceil(min(found, self.HH_API_MAX_RESULTS) / per_page) if per_page else 0
                    if page >= max_pages and found > 0:
                        raise RequestException(f"Requested page {page} is out of range (max pages {max_pages})")
                return data
            except HHAuthError:
                raise
            except RequestException as exc:
                last_exc = exc
                time.sleep(backoff)
                backoff *= 2
                continue
            except Exception as exc:
                last_exc = exc
                time.sleep(backoff)
                backoff *= 2
                continue

        # после повторных попыток, вызываем последнее исключение
        if last_exc:
            raise last_exc
        raise RuntimeError("Unknown error during HH API request")

    def get_vacancy(self, vacancy_id: str) -> Dict[str, Any]:
        
        """Получает информацию о конкретной вакансии по её ID.

        Возвращает разобранный JSON-ответ в виде словаря.
        """
        
        response = requests.get(
            f"{self.base_url}/vacancies/{vacancy_id}", headers=self._headers(), timeout=20
        )
        response.raise_for_status()
        return response.json()
