import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config import RAW_STORAGE_DIR


def save_raw_batch(
    items: list[dict], source: str, query: str, area: str | int | None = None
) -> Path:
    
    """Сохраняет список JSON-совместимых элементов как файл JSONL с меткой времени.

    Возвращает путь к созданному файлу.
    """
    
    folder = Path(RAW_STORAGE_DIR)
    folder.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    safe_query = query.replace(" ", "_").lower()
    area_name = area or "all"
    file_path = folder / f"{source}_{safe_query}_{area_name}_{timestamp}.jsonl"

    with file_path.open("w", encoding="utf-8") as handle:
        for item in items:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")

    return file_path


def save_raw_batch_parquet(
    items: list[dict], source: str, query: str, area: str | int | None = None, base_dir: str | Path | None = None
) -> Path:
    
    """Сохраняет полные необработанные данные HH в файл parquet.

    Полезная нагрузка сохраняется без изменений на стороне клиента, что позволяет позже
    создавать несколько аналитических таблиц из одного и того же исходного источника.
    """
    
    folder = Path(base_dir) if base_dir is not None else Path(RAW_STORAGE_DIR)
    folder.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_query = str(query).replace(" ", "_").lower()
    area_name = area or "all"
    file_path = folder / f"{source}_{safe_query}_{area_name}_{timestamp}.parquet"

    frame = pd.DataFrame(items)
    if frame.empty:
        frame = pd.DataFrame([{"_empty": True}])
    frame.to_parquet(file_path, index=False)
    return file_path
