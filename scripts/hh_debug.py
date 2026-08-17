#!/usr/bin/env python3
"""HH API debug helper.

Использование: активируйте виртуальное окружение и запустите `python scripts/hh_debug.py`.
Он попытается получить токен доступа, используя учетные данные клиента (body и basic auth)
и после этого выполнит запрос к API вакансий с использованием токена.
"""

from __future__ import annotations

import sys
import json
from typing import Any, Optional

import requests

from config import HH_CLIENT_ID, HH_CLIENT_SECRET, HH_API_TOKEN, HH_API_URL


def try_body_token() -> Optional[dict]:
    
    """Попытка получить токен доступа, отправив client_id/secret в теле POST-запроса."""
    
    url = f"{HH_API_URL.rstrip('/')}/oauth/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": HH_CLIENT_ID,
        "client_secret": HH_CLIENT_SECRET,
    }
    try:
        # Подготовка запроса, чтобы мы могли напечатать точное тело/заголовки, которые будут отправлены
        sess = requests.Session()
        req = requests.Request("POST", url, data=data)
        preq = sess.prepare_request(req)
        print("Prepared POST (body) -> URL:", preq.url)
        print("Prepared POST (body) -> Headers:", preq.headers)
        print("Prepared POST (body) -> Body:", preq.body)
        r = sess.send(preq, timeout=10)
        print("POST body token request status:", r.status_code)
        print(r.text)
        if r.status_code == 200:
            return r.json()
    except Exception as e:  # pragma: no cover - simple debug helper
        print("body token request error:", e)
    return None


def try_basic_token() -> Optional[dict]:
    
    """Попытка получить токен доступа, используя HTTP Basic auth (client_id:secret)."""
    
    url = f"{HH_API_URL.rstrip('/')}/oauth/token"
    try:
        sess = requests.Session()
        req = requests.Request("POST", url, data={"grant_type": "client_credentials"}, auth=(HH_CLIENT_ID, HH_CLIENT_SECRET))
        preq = sess.prepare_request(req)
        print("Prepared POST (basic) -> URL:", preq.url)
        print("Prepared POST (basic) -> Headers:", preq.headers)
        print("Prepared POST (basic) -> Body:", preq.body)
        r = sess.send(preq, timeout=10)
        print("POST basic token request status:", r.status_code)
        print(r.text)
        if r.status_code == 200:
            return r.json()
    except Exception as e:  # pragma: no cover - simple debug helper
        print("basic token request error:", e)
    return None


def test_vacancies(token: Optional[str]) -> None:
    
    """Выполняет пробный запрос вакансии с использованием предоставленного токена и выводит статус/текст ответа."""
    
    params = {"text": "data analyst", "area": 113, "per_page": 1, "page": 0}
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        r = requests.get(f"{HH_API_URL.rstrip('/')}/vacancies", params=params, headers=headers, timeout=10)
        print("vacancies request status:", r.status_code)
        print(r.text[:2000])
    except Exception as e:  # pragma: no cover - debug helper
        print("vacancies request error:", e)


def main() -> None:
    
    """Запускает отладочный процесс: попытка получить токен, а затем запрос вакансий."""
    
    print("HH API URL:", HH_API_URL)
    if HH_API_TOKEN:
        print("HH_API_TOKEN is set (will try with it).")
    if HH_CLIENT_ID and HH_CLIENT_SECRET:
        print("HH_CLIENT_ID/SECRET are set — trying client credentials flow.")
    else:
        print("No client_id/secret configured.")

    token: Optional[str] = None
    if HH_CLIENT_ID and HH_CLIENT_SECRET:
        print('\nTrying token via request body...')
        j = try_body_token()
        if j:
            token = j.get("access_token") or j.get("token") or j.get("accessToken")
        if not token:
            print('\nTrying token via HTTP Basic auth...')
            j = try_basic_token()
            if j:
                token = j.get("access_token") or j.get("token") or j.get("accessToken")

    if not token and HH_API_TOKEN:
        print('\nFalling back to HH_API_TOKEN from env')
        token = HH_API_TOKEN

    print('\nUsing token:', 'SET' if token else 'NONE')
    test_vacancies(token)

    if not token:
        print('\nNo token could be obtained. Check client id/secret or HH_API_TOKEN in .env.')
        sys.exit(2)


if __name__ == "__main__":
    main()
