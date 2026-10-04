import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest


# gm 不参与测试。set_token 在假模块里直接失败，查询前把 inited 设为真。
class _Gm:
    frame: pd.DataFrame = pd.DataFrame()
    calls: list[dict[str, object]] = []


def _set_token(token: str) -> None:
    raise AssertionError("set_token")


def _history(
    symbol: str,
    frequency: str,
    start_time: datetime,
    end_time: datetime,
    fields: list[str],
    adjust: object,
    df: bool,
) -> pd.DataFrame:
    _Gm.calls.append({
        "symbol": symbol,
        "frequency": frequency,
        "start_time": start_time,
        "end_time": end_time,
        "fields": fields,
        "adjust": adjust,
        "df": df,
    })
    return _Gm.frame


_ADJUST_PREV: str = "ADJUST_PREV"
_gm: ModuleType = ModuleType("gm")
_gm_api: ModuleType = ModuleType("gm.api")
_gm_enum: ModuleType = ModuleType("gm.enum")
_gm_api.set_token = _set_token  # type: ignore[attr-defined]
_gm_api.history = _history  # type: ignore[attr-defined]
_gm_enum.ADJUST_PREV = _ADJUST_PREV  # type: ignore[attr-defined]
_gm.__path__ = []  # type: ignore[attr-defined]
sys.modules["gm"] = _gm
sys.modules["gm.api"] = _gm_api
sys.modules["gm.enum"] = _gm_enum

from vnpy_gm.gm_datafeed import ADJUST_PREV, GmDatafeed, to_gm_symbol  # noqa: E402


_BAR_DT: datetime = datetime(2024, 1, 15, 10, 0)
_FRAME: pd.DataFrame = pd.DataFrame([{
    "open": 100.5,
    "close": 105.0,
    "low": 90.25,
    "high": 110.0,
    "volume": 12.0,
    "amount": 1300.0,
    "position": 33.0,
    "bob": pd.Timestamp("2024-01-15 10:00:00"),
}])


def _request(symbol: str, exchange: Exchange, interval: Interval) -> HistoryRequest:
    return HistoryRequest(
        symbol=symbol,
        exchange=exchange,
        start=datetime(2024, 1, 15, 9, 0),
        end=datetime(2024, 1, 15, 15, 0),
        interval=interval,
    )


def _feed() -> GmDatafeed:
    _Gm.calls.clear()
    feed: GmDatafeed = GmDatafeed()
    feed.password = "offline-token"
    feed.inited = True
    return feed


def test_to_gm_symbol() -> None:
    assert Path(sys.modules["vnpy_gm.gm_datafeed"].__file__ or "").resolve().is_relative_to(
        Path(__file__).resolve().parents[1]
    )
    assert to_gm_symbol("rb2410", Exchange.SHFE) == "SHFE.rb2410"
    assert to_gm_symbol("600000", Exchange.SSE) == "SSE.600000"
    assert to_gm_symbol("IF2410", Exchange.CFFEX) == "CFFEX.IF2410"
    assert to_gm_symbol("sc2501", Exchange.INE) == "INE.sc2501"
    assert to_gm_symbol("si2501", Exchange.GFEX) == "GFEX.si2501"
    assert to_gm_symbol("TA501", Exchange.CZCE) == "CZCE.TA501"


@pytest.mark.parametrize(
    ("interval", "frequency"),
    [
        (Interval.MINUTE, "60s"),
        (Interval.HOUR, "3600s"),
        (Interval.DAILY, "1d"),
    ],
)
def test_query_bar_history(interval: Interval, frequency: str) -> None:
    _Gm.frame = _FRAME
    feed: GmDatafeed = _feed()
    logs: list[str] = []
    bars: list[BarData] = feed.query_bar_history(
        _request("rb2410", Exchange.SHFE, interval),
        output=logs.append,
    )

    assert logs == []
    assert _Gm.calls[0]["symbol"] == "SHFE.rb2410"
    assert _Gm.calls[0]["frequency"] == frequency
    assert _Gm.calls[0]["adjust"] == ADJUST_PREV
    assert len(bars) == 1
    bar: BarData = bars[0]
    assert bar.symbol == "rb2410"
    assert bar.exchange == Exchange.SHFE
    assert bar.interval == interval
    assert bar.datetime == _BAR_DT
    assert bar.open_price == 100.5
    assert bar.high_price == 110.0
    assert bar.low_price == 90.25
    assert bar.close_price == 105.0
    assert bar.volume == 12.0
