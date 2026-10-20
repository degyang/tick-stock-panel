"""Public ageman SDK adapter; connections belong to a single read operation."""
import json
import logging
import subprocess
import sys
from contextlib import closing
from datetime import date, timedelta
from types import SimpleNamespace

import polars as pl

from app.market_time import CN_TZ, cn_now

logger = logging.getLogger(__name__)
_MAX_COLLECTED_ROWS = 2_000_000

SCHEMA = {"symbol": pl.String, "datetime": pl.Datetime("us"),
          **{key: pl.Float64 for key in ("open", "high", "low", "close", "volume", "amount")}}


def availability():
    try:
        from ageman.sdk import MarketClient
        contract = MarketClient().describe()
        if contract["contract_version"] != "1.6":
            return False, "需要已验证的 ageman 行情合同 1.6"
        return True, "ok"
    except Exception:  # noqa: BLE001 — optional plugin failures must not break startup
        return False, "未安装兼容的 ageman SDK（Python 3.12+）"


def _wallclock(value):
    return value.astimezone(CN_TZ).replace(tzinfo=None) if value.tzinfo else value


def normalize_day(frame, symbol: str, day: date) -> pl.DataFrame:
    metadata = frame.attrs
    if (metadata.get("complete") is not True or metadata.get("date_verified") is not True
            or metadata.get("source_date") != day):
        raise ValueError(f"ageman incomplete or unverified minute window: {symbol} {day}")
    if frame.empty:
        return pl.DataFrame(schema=SCHEMA)
    units = metadata.get("units", {})
    volume_unit = "receipt" if symbol == "689009.SH" else "share"
    if units.get("amount") != "CNY" or units.get("volume") not in (None, volume_unit):
        raise ValueError("ageman unexpected minute units")
    rows = []
    for row in frame.to_dict("records"):
        stamp = row["datetime"]
        if stamp.tzinfo is None or _wallclock(stamp).date() != day or row["symbol"] != symbol:
            raise ValueError("ageman unexpected minute identity or timestamp")
        volume = row.get("volume")
        if volume is not None and units.get("volume") != volume_unit:
            raise ValueError("ageman unknown volume unit")
        rows.append({**{key: row.get(key) for key in SCHEMA},
                     "datetime": _wallclock(stamp),
                     "volume": float(volume) / 100 if volume is not None else None})
    return pl.DataFrame(rows, schema=SCHEMA)


def _read_cdr_day(symbol, day):
    """The SDK excludes this CDR; use ageman's public, bounded history CLI."""
    import pandas as pd

    result = subprocess.run([
        sys.executable, "-m", "tdx2man", "--format", "json", "--timeout", "8",
        "--max-pages", "16", "market", "history", "--symbol", symbol,
        "--period", "1MIN", "--start", (day - timedelta(days=1)).isoformat(),
        "--end", day.isoformat(), "--adjust", "NONE", "--no-cache",
    ], capture_output=True, text=True, timeout=45, check=True)
    payload = json.loads(result.stdout)
    meta = payload.get("meta", {})
    coverage = meta.get("coverage", {})
    if (payload.get("ok") is not True or meta.get("protocol") != "mac"
            or meta.get("period") != "1MIN" or meta.get("adjust") != "NONE"
            or coverage.get("duplicates") != 0
            or not coverage.get("first") or coverage["first"][:10] >= day.isoformat()):
        raise ValueError("ageman CDR history window not verified")
    rows = [r for r in payload["data"] if r["datetime"][:10] == day.isoformat()]
    if not rows:
        raise ValueError(f"ageman CDR minute not returned: {symbol} {day}")
    frame = pd.DataFrame(rows)
    frame["datetime"] = pd.to_datetime(frame["datetime"]).dt.tz_localize(CN_TZ)
    frame.attrs = {"complete": True, "date_verified": True, "source_date": day,
                   "units": {"volume": "receipt", "amount": "CNY"}}
    return frame


def _days(start, end):
    # Use observed sessions, never treat an empty source response as a holiday.
    from app.config import settings
    from app.services.trading_day import is_trading_day
    days = set()
    for path in (settings.data_dir / "kline_daily").glob("date=*"):
        if (path / "part.parquet").exists():
            day = date.fromisoformat(path.name[5:])
            if start.date() <= day <= end.date():
                days.add(day)
    now = cn_now()
    if start.date() <= now.date() <= end.date() and is_trading_day(now) is True:
        days.add(now.date())
    if not days:
        raise ValueError("ageman minute sync requires observed trading sessions; sync daily data first")
    return sorted(days)


class AgemanProvider:
    name = "ageman"
    builtin = True

    def __init__(self):
        self.config = SimpleNamespace(datasets={"minute": {}, "full_minute": {}})

    def close(self):
        pass  # Each iterator closes its own SDK connection, including cancellation.

    def iter_minute(self, symbols, start_time, end_time, asset_type="stock", freq="1m",
                    on_chunk_done=None):
        if asset_type not in {"stock", "index"} or freq != "1m":
            raise ValueError("ageman unsupported minute asset or frequency")
        if not symbols:
            return
        from ageman.sdk import MarketClient, MarketSDKError

        from app.config import settings
        end = _wallclock(end_time or cn_now())
        start = _wallclock(start_time) if start_time else end - timedelta(days=7)
        days = _days(start, end)
        total = len(days) * len(symbols)
        done = 0
        with MarketClient(timeout=10) as client:
            for day in days:
                parts = []
                daily_path = settings.data_dir / "kline_daily" / f"date={day}" / "part.parquet"
                eligible = None
                if asset_type == "stock" and day < cn_now().date() and daily_path.exists():
                    eligible = set(pl.read_parquet(daily_path, columns=["symbol", "volume"])
                                   .filter(pl.col("volume") > 0)["symbol"].to_list())
                for index, symbol in enumerate(symbols):
                    df = pl.DataFrame(schema=SCHEMA)
                    if eligible is None or symbol in eligible:
                        try:
                            frame = (_read_cdr_day(symbol, day) if symbol == "689009.SH"
                                     else client.read_minute_bars(symbol, date=day, limit=500))
                            df = normalize_day(frame, symbol, day).filter(
                                pl.col("datetime").is_between(start, end))
                        except MarketSDKError as exc:
                            # Live universes include halted/unlisted symbols. Only an empty
                            # live response may be skipped; historical traded gaps must fail.
                            if exc.code != "INCOMPLETE_RESULT" or day != cn_now().date():
                                raise
                            logger.warning("ageman live minute not returned: %s %s", symbol, day)
                    if not df.is_empty():
                        parts.append(df)
                    done += 1
                    if on_chunk_done:
                        on_chunk_done(done, total)
                    if parts and (len(parts) >= 100 or index == len(symbols) - 1):
                        yield pl.concat(parts)
                        parts.clear()

    def get_minute(self, symbols, start_time, end_time, asset_type="stock", freq="1m",
                   on_chunk_done=None):
        parts, rows = [], 0
        with closing(self.iter_minute(symbols, start_time, end_time, asset_type, freq,
                                      on_chunk_done)) as chunks:
            for chunk in chunks:
                rows += chunk.height
                if rows > _MAX_COLLECTED_ROWS:
                    raise ValueError("ageman minute result too large; use streaming persistence")
                parts.append(chunk)
        return pl.concat(parts) if parts else pl.DataFrame(schema=SCHEMA)

    def get_intraday_batch(self, symbols, count=300, asset_type="stock"):
        # The existing refresh scheduler serializes rounds; retain only one day.
        end = cn_now()
        return self.get_minute(symbols,
            start_time=end.replace(hour=0, minute=0, second=0, microsecond=0),
            end_time=end, asset_type=asset_type)
