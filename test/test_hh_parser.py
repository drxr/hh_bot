from unittest.mock import Mock, patch
import asyncio

import pandas as pd
from telegram.error import BadRequest

from extract.hh_api import HHClient


def test_safe_answer_ignores_stale_callback_query():
    
    from bot.bot import _safe_answer

    q = Mock()
    q.answer.side_effect = BadRequest("Query is too old and response timeout expired or query id is invalid")

    asyncio.run(_safe_answer(q))

    q.answer.assert_called_once()


class TestHHClient:
    @patch("extract.hh_api.requests.get")
    def test_search_calls_hh_api_with_token(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {"items": [{"id": "1", "name": "Data Analyst"}]}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        client = HHClient(token="secret-token")
        result = client.search("data analyst", area=113, per_page=5)

        assert result["items"][0]["name"] == "Data Analyst"
        headers = mock_get.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer secret-token"


    @patch("extract.hh_api.requests.get")
    def test_get_vacancy_uses_token_header(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {"id": "123"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        client = HHClient(token="token-123")
        vac = client.get_vacancy("123")

        assert vac["id"] == "123"
        assert mock_get.call_args.kwargs["headers"]["Authorization"] == "Bearer token-123"


def test_save_raw_parquet_batch_creates_file(tmp_path):
    from extract.hh_raw import save_raw_batch_parquet

    items = [{
        "id": "1",
        "name": "Data Analyst",
        "area": {"name": "Москва"},
        "employer": {"name": "Acme"},
        "salary": {"from": 150000, "to": 220000, "currency": "RUR"},
        "schedule": {"id": "fullDay"},
        "work_format": [{"id": "remote"}],
        "key_skills": [{"name": "SQL"}, {"name": "Python"}],
    }]

    path = save_raw_batch_parquet(items, source="bot", query="data analyst", area=113, base_dir=tmp_path)

    assert path.exists()
    assert path.suffix == ".parquet"
    df = pd.read_parquet(path)
    assert len(df) == 1
    assert df.iloc[0]["id"] == "1"


def test_build_vacancy_analytics_tables_creates_summary_frames(tmp_path):
    from extract.hh_raw import save_raw_batch_parquet
    from load.duckdb_store import build_vacancy_analytics_tables

    items = [
        {
            "id": "1",
            "name": "Data Analyst",
            "area": {"name": "Москва"},
            "employer": {"name": "Acme"},
            "salary": {"from": 150000, "to": 220000, "currency": "RUR"},
            "schedule": {"id": "fullDay"},
            "work_format": [{"id": "remote"}],
            "key_skills": [{"name": "SQL"}, {"name": "Python"}],
        },
        {
            "id": "2",
            "name": "Data Engineer",
            "area": {"name": "Санкт-Петербург"},
            "employer": {"name": "Beta"},
            "salary": {"from": 180000, "to": 250000, "currency": "RUR"},
            "schedule": {"id": "remote"},
            "work_format": [{"id": "hybrid"}],
            "key_skills": [{"name": "SQL"}, {"name": "Python"}, {"name": "Kafka"}],
        },
    ]

    parquet_path = save_raw_batch_parquet(items, source="bot", query="analytics", area=113, base_dir=tmp_path)
    tables = build_vacancy_analytics_tables(parquet_path, duckdb_path=str(tmp_path / "analytics.duckdb"))

    assert set(tables) >= {"vacancies", "skills_summary", "salary_summary", "city_summary", "work_type_summary"}
    assert len(tables["vacancies"]) == 2
    assert tables["skills_summary"].loc[0, "skill"].lower() == "sql"
    assert tables["salary_summary"].loc[0, "avg_salary_from"] >= 150000
    assert tables["city_summary"].shape[0] >= 2
    assert set(tables["work_type_summary"]["work_type"]) >= {"remote", "hybrid"}


def test_build_stats_text_parses_various_skill_formats():
    from bot.bot import _build_stats_text

    cache = [
        {"id": "1", "key_skills": [{"name": "SQL"}, {"name": "Python"}]},
        {"id": "2", "key_skills": "[\"SQL\", \"Kafka\"]"},
        {"id": "3", "key_skills": "SQL, C++, Go"},
        {"id": "4", "key_skills": {"name": "Python"}},
    ]

    text = _build_stats_text(cache)

    assert "SQL" in text
    assert "Python" in text
    assert "Kafka" in text or "Kafka" in text
