from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from app.plugins.ageman.provider import AgemanProvider, normalize_day


def frame():
    df = pd.DataFrame([dict(symbol="000001.SZ", datetime=datetime(2026, 10, 16, 9, 31,
        tzinfo=ZoneInfo("Asia/Shanghai")), open=10., high=11., low=9., close=10.,
        volume=12345., amount=123450.)])
    df.attrs = dict(complete=True, date_verified=True, source_date=datetime(2026, 10, 16).date(),
                    units={"volume": "share", "amount": "CNY"})
    return df


def test_normalization_preserves_units_and_beijing_time():
    out = normalize_day(frame(), "000001.SZ", datetime(2026, 10, 16).date())
    assert out["datetime"][0] == datetime(2026, 10, 16, 9, 31)
    assert out["volume"][0] == 123.45
    assert out["amount"][0] == 123450.


@pytest.mark.parametrize("field,value", [("complete", False), ("date_verified", False),
                                          ("source_date", None)])
def test_rejects_unverified_or_partial_window(field, value):
    df = frame()
    df.attrs[field] = value
    with pytest.raises(ValueError):
        normalize_day(df, "000001.SZ", datetime(2026, 10, 16).date())


def test_index_unknown_volume_stays_null():
    df = frame()
    df["symbol"] = "000001.SH"
    df["volume"] = None
    df.attrs["units"].pop("volume")
    assert normalize_day(df, "000001.SH", datetime(2026, 10, 16).date())["volume"][0] is None


def test_cdr_cli_checks_window_and_preserves_receipt_units(monkeypatch):
    import json

    from app.plugins.ageman import provider as adapter

    payload = {"ok": True, "meta": {"protocol": "mac", "period": "1MIN", "adjust": "NONE",
        "coverage": {"first": "2026-10-15T09:31:00", "duplicates": 0}},
        "data": [{**frame().iloc[0].to_dict(), "symbol": "689009.SH",
                  "datetime": "2026-10-16T09:31:00"}]}
    monkeypatch.setattr(adapter.subprocess, "run", lambda *a, **k:
                        SimpleNamespace(stdout=json.dumps(payload)))
    day = datetime(2026, 10, 16).date()
    out = normalize_day(adapter._read_cdr_day("689009.SH", day), "689009.SH", day)
    assert out["volume"][0] == 123.45
    payload["meta"]["coverage"]["first"] = "2026-10-16T10:31:00"
    with pytest.raises(ValueError, match="window"):
        adapter._read_cdr_day("689009.SH", day)


def test_unsupported_asset_fails_explicitly():
    with pytest.raises(ValueError, match="asset"):
        list(AgemanProvider().iter_minute(["510300.SH"], None, None, asset_type="etf"))


def test_full_market_refresh_uses_only_current_session(monkeypatch):
    import app.plugins.ageman.provider as adapter
    now = datetime(2026, 10, 16, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    monkeypatch.setattr(adapter, "cn_now", lambda: now)
    provider = AgemanProvider()
    calls = []
    monkeypatch.setattr(provider, "get_minute", lambda *a, **kw: calls.append((a, kw)))
    provider.get_intraday_batch(["000001.SZ"])
    assert calls[0][1]["start_time"] == now.replace(hour=0, minute=0)
    assert calls[0][1]["end_time"] == now


def test_collect_limit_closes_iterator_without_materializing_history(monkeypatch):
    from app.plugins.ageman import provider as adapter

    monkeypatch.setattr(adapter, "_MAX_COLLECTED_ROWS", 1)
    chunk = normalize_day(frame(), "000001.SZ", datetime(2026, 10, 16).date())
    closed = []
    def iterator(*args):
        try:
            yield chunk
            yield chunk
            pytest.fail("must stop before requesting more history")
        finally:
            closed.append(True)
    provider = AgemanProvider()
    monkeypatch.setattr(provider, "iter_minute", iterator)
    with pytest.raises(ValueError, match="streaming"):
        provider.get_minute(["000001.SZ"], None, None)
    assert closed == [True]


def test_stream_persists_before_requesting_next_chunk(monkeypatch):
    from app.services import kline_sync
    seen = []
    chunk = normalize_day(frame(), "000001.SZ", datetime(2026, 10, 16).date())
    def iterator(*args, **kwargs):
        yield chunk
        assert seen == [1]
        raise RuntimeError("source failed")
    provider = SimpleNamespace(iter_minute=iterator)
    monkeypatch.setattr(kline_sync, "_resolve_minute_provider", lambda _: (provider, False, None))
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "ageman")
    with pytest.raises(RuntimeError, match="source failed"):
        kline_sync.sync_minute_batch(["000001.SZ"], on_segment=lambda df: seen.append(df.height))
    assert seen == [1]


def test_stream_closes_connection_when_persistence_fails(monkeypatch):
    from app.services import kline_sync

    closed = []
    chunk = normalize_day(frame(), "000001.SZ", datetime(2026, 10, 16).date())
    def iterator(*args, **kwargs):
        try:
            yield chunk
        finally:
            closed.append(True)
    monkeypatch.setattr(kline_sync, "_resolve_minute_provider",
                        lambda _: (SimpleNamespace(iter_minute=iterator), False, None))
    def persist(df):
        raise OSError("disk failure")
    with pytest.raises(OSError, match="disk failure"):
        kline_sync.sync_minute_batch(["000001.SZ"], on_segment=persist)
    assert closed == [True]


def test_global_sync_replays_latest_session_instead_of_only_last_bar(monkeypatch, tmp_path):
    from app.services import kline_sync
    from app.tickflow.capabilities import CapabilitySet

    monkeypatch.setattr(kline_sync, "_resolve_minute_provider", lambda _: (object(), False, None))
    monkeypatch.setattr(kline_sync, "_cleanup_null_datetime_minute", lambda repo: None)
    monkeypatch.setattr(kline_sync, "_migrate_symbol_to_date_partition", lambda repo: None)
    monkeypatch.setattr(kline_sync, "_latest_minute_datetime",
                        lambda repo: datetime(2026, 10, 16, 15))
    captured = []
    monkeypatch.setattr(kline_sync, "sync_minute_batch", lambda *a, **kw: captured.append(kw))
    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    assert kline_sync.sync_and_persist_minute(["000001.SZ"], repo, CapabilitySet()) == 0
    assert captured[0]["start_time"] == datetime(2026, 10, 16)


def test_iterator_filters_observed_sessions_and_closes_client(monkeypatch, tmp_path):
    import sys

    import polars as pl

    import app.plugins.ageman.provider as adapter
    from app.config import settings

    day = datetime(2026, 10, 16).date()
    monkeypatch.setattr(settings, "data_dir", tmp_path)
    monkeypatch.setattr(adapter, "_days", lambda start, end: [day])
    path = tmp_path / "kline_daily" / f"date={day}"
    path.mkdir(parents=True)
    pl.DataFrame({"symbol": ["000001.SZ", "000002.SZ"], "volume": [100., 0.]}).write_parquet(
        path / "part.parquet")
    calls, closed = [], []

    class Client:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            closed.append(True)

        def read_minute_bars(self, symbol, **kwargs):
            calls.append((symbol, kwargs))
            return frame()

    monkeypatch.setitem(sys.modules, "ageman.sdk", SimpleNamespace(
        MarketClient=Client, MarketSDKError=RuntimeError))
    result = AgemanProvider().get_minute(["000001.SZ", "000002.SZ"],
        datetime(2026, 10, 16), datetime(2026, 10, 16, 15))
    assert result.height == 1
    assert calls == [("000001.SZ", {"date": day, "limit": 500})]
    assert closed == [True]
