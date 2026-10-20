"""多日分时 API 契约。"""

import asyncio
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest
from fastapi import HTTPException

from app.api import kline as kline_api


def _request(repo=None, capset=None):
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                repo=repo or MagicMock(),
                capabilities=capset or MagicMock(),
            )
        )
    )


def test_minute_range_returns_latest_sessions_with_previous_closes():
    repo = MagicMock()
    repo.resolve_asset_type.return_value = "stock"
    # _get_stock_info 走 instruments 内存缓存 (不再走 execute_one DuckDB 查询)
    repo.get_instruments.return_value = pl.DataFrame({
        "symbol": ["600000.SH"],
        "name": ["浦发银行"],
        "total_shares": [1.0],
        "float_shares": [1.0],
    })
    repo.get_minute_range.return_value = pl.DataFrame({
        "symbol": ["600000.SH"] * 3,
        "datetime": [
            datetime(2026, 8, 5, 1, 30),
            datetime(2026, 8, 6, 1, 30),
            datetime(2026, 8, 7, 1, 30),
        ],
        "open": [10.0, 11.0, 12.0],
        "high": [10.2, 11.2, 12.2],
        "low": [9.8, 10.8, 11.8],
        "close": [10.1, 11.1, 12.1],
        "volume": [100.0, 110.0, 120.0],
        "amount": [101_000.0, 122_100.0, 145_200.0],
    })
    repo.get_daily_asset.return_value = pl.DataFrame({
        "date": [
            date(2026, 8, 4),
            date(2026, 8, 5),
            date(2026, 8, 6),
            date(2026, 8, 7),
        ],
        "close": [9.9, 10.1, 11.1, 12.1],
    })

    result = kline_api.get_minute_range(_request(repo), "600000.SH", 2)

    assert result["name"] == "浦发银行"
    assert result["requested_days"] == 2
    assert result["source"] == "local"
    assert [session["date"] for session in result["sessions"]] == [
        "2026-08-06",
        "2026-08-07",
    ]
    assert [session["prev_close"] for session in result["sessions"]] == [
        10.1,
        11.1,
    ]
    assert result["sessions"][0]["rows"][0]["close"] == 11.1


def test_minute_range_does_not_read_stock_store_for_index():
    repo = MagicMock()
    repo.resolve_asset_type.return_value = "index"
    repo.get_instruments_asset.return_value = pl.DataFrame()

    result = kline_api.get_minute_range(_request(repo), "000001.SH", 10)

    assert result["asset_type"] == "index"
    assert result["sessions"] == []
    repo.get_minute_range.assert_not_called()


def test_sync_minute_single_uses_requested_days(monkeypatch):
    repo = MagicMock()
    repo.resolve_asset_type.return_value = "stock"
    capset = MagicMock()
    sync = MagicMock(return_value=2400)
    refresh = MagicMock()
    monkeypatch.setattr(kline_api, "_minute_allowed", lambda _: True)
    monkeypatch.setattr(kline_api.kline_sync, "sync_and_persist_minute", sync)
    monkeypatch.setattr("app.jobs.daily_pipeline._refresh_single_view", refresh)

    result = asyncio.run(kline_api.sync_minute_single(
        _request(repo, capset),
        {"symbol": "600000.SH", "days": 10},
    ))

    assert result["rows"] == 2400
    sync.assert_called_once_with(["600000.SH"], repo, capset, days=10, force_full_days=True)
    refresh.assert_called_once_with(repo, "kline_minute")


def test_sync_minute_single_rejects_invalid_days():
    with pytest.raises(HTTPException, match="days 必须在 1 到 30 之间"):
        asyncio.run(kline_api.sync_minute_single(
            _request(),
            {"symbol": "600000.SH", "days": 0},
        ))


@pytest.mark.parametrize("force", [False, True])
def test_market_minute_sync_forwards_backfill_mode(monkeypatch, tmp_path, force):
    from app.services import pipeline_jobs

    repo = MagicMock()
    repo.store.data_dir = tmp_path
    repo.get_index_symbol_set.return_value = set()
    sync = MagicMock(return_value=2400)
    jobs = MagicMock()
    jobs.create.return_value = ("test-job", True)
    monkeypatch.setattr(pipeline_jobs, "job_store", jobs)
    monkeypatch.setattr(pipeline_jobs, "try_acquire_run_slot", lambda _: True)
    monkeypatch.setattr(pipeline_jobs, "release_run_slot", lambda _: None)
    monkeypatch.setattr(pipeline_jobs, "run_with_capacity", lambda _, fn: fn())
    monkeypatch.setattr("app.tickflow.pools.get_pool", lambda _: ["600000.SH"])
    monkeypatch.setattr("app.jobs.daily_pipeline._refresh_single_view", MagicMock())
    monkeypatch.setattr("app.api.data.invalidate_storage_cache", MagicMock())
    monkeypatch.setattr(kline_api, "_minute_allowed", lambda _: True)
    monkeypatch.setattr(kline_api.kline_sync, "sync_and_persist_minute", sync)
    pending = []
    monkeypatch.setattr(asyncio, "create_task", pending.append)
    request = _request(repo)
    request.json = AsyncMock(return_value={"days": 12, "force_full_days": force})

    async def run():
        result = await kline_api.sync_minute(request)
        assert result == {"status": "started", "job_id": "test-job"}
        await pending.pop()

    asyncio.run(run())
    assert sync.call_args.kwargs["force_full_days"] is force
    assert sync.call_args.kwargs["days"] == 12
    assert sync.call_args.kwargs["extend_backward"] is False
    jobs.succeed.assert_called_once()


@pytest.mark.parametrize("body", [
    {"force_full_days": "true"},
    {"force_full_days": True, "extend": True},
    {"force_full_days": True, "days": 365},
    {"force_full_days": True, "days": 0},
])
def test_market_minute_backfill_rejects_invalid_options(body, monkeypatch):
    from app.services import pipeline_jobs

    jobs = MagicMock()
    jobs.create.side_effect = AssertionError("Invalid options must not create a job")
    monkeypatch.setattr(pipeline_jobs, "job_store", jobs)
    request = _request()
    request.json = AsyncMock(return_value=body)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(kline_api.sync_minute(request))
    assert exc.value.status_code == 400
