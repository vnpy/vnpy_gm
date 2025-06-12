from datetime import datetime, timedelta
from collections.abc import Callable

import pandas as pd
from gm.api import set_token, history
from gm.enum import ADJUST_PREV

from vnpy.trader.utility import ZoneInfo
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.setting import SETTINGS


INTERVAL_VT2GM = {
    Interval.MINUTE: "60s",
    Interval.HOUR: "3600s",
    Interval.DAILY: "1d",
}

CHINA_TZ = ZoneInfo("Asia/Shanghai")


def to_gm_symbol(symbol: str, exchange: Exchange) -> str:
    """
    将交易所代码转换为掘金代码（交易所大写.合约名称小写）
    """
    gm_symbol: str = f"{exchange.value.upper()}.{symbol.lower()}"
    return gm_symbol


class GmDatafeed(BaseDatafeed):
    """掘金GMData数据服务接口"""

    def __init__(self) -> None:
        """"""
        # 加载配置
        self.password: str = SETTINGS["datafeed.password"]

        self.inited: bool = False

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        if self.inited:
            return True

        if not self.password:
            output("GMData数据服务初始化失败：密码为空！")
            return False

        try:
            set_token(self.password)

        except Exception as ex:
            output(f"GMData数据服务初始化失败：{ex}")
            return False

        self.inited = True
        return True

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData]:
        """查询K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end

        gm_symbol: str = to_gm_symbol(symbol, exchange)

        gm_interval: str | None = INTERVAL_VT2GM.get(interval)
        if not gm_interval:
            output(f"GMData查询K线数据失败：不支持的时间周期{req.interval.value}")
            return []

        if start > end:
            return []

        fields: list = ["open", "close", "low", "high", "volume", "amount", "position", "bob"]

        try:
            df: pd.DataFrame = history(
                symbol=gm_symbol,
                frequency=gm_interval,
                start_time=start,
                end_time=end,
                fields=fields,
                adjust=ADJUST_PREV,
                df=True
            )

        except Exception as ex:
            output(f"GMData查询K线数据失败：{ex}")
            return []

        bars: list[BarData] = []

        if df is not None:
            for _ix, row in df.iterrows():
                dt: datetime = row["bob"].to_pydatetime()

                bar: BarData = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    open_interest= row["position"],
                    volume=row["volume"],
                    turnover= row['amount'],
                    gateway_name="GM"
                )
                bars.append(bar)

        return bars

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> list[TickData]:
        """查询Tick数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        start: datetime = req.start
        end: datetime = req.end

        # 股票期权不添加交易所后缀
        gm_symbol: str = to_gm_symbol(symbol, exchange)

        fields: list = [
            "symbol", "open", "high", "low", "price",
            "cum_volume", "cum_amount", "iopv", "last_amount", "last_volume",
            "quotes", "cum_position", "trade_type", "flag", "created_at"
        ]

        if (end > start + timedelta(days=180)):
            output("GMData查询Tick数据失败：查询时间过长")
            return []

        try:
            history_data : list = history(
                symbol=gm_symbol,
                frequency='tick',
                start_time=start,
                end_time = end,
                fields=fields,
                adjust=ADJUST_PREV,
                df=False
            )
        except Exception as ex:
            output(f"GMData查询Tick数据失败：{ex}")
            return []

        ticks: list[TickData] = []

        for td in history_data:
            dt: datetime = td["created_at"]

            tick : TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                open_price=td["open"],
                high_price=td["high"],
                low_price=td["low"],
                last_price=td["price"],
                turnover=td["cum_amount"],
                volume=td["cum_volume"],
                bid_price_1=td["quotes"][0]["bid_p"],
                bid_volume_1=td["quotes"][0]["bid_v"],
                ask_price_1=td["quotes"][0]["ask_p"],
                ask_volume_1=td["quotes"][0]["ask_v"],
                open_interest=td["cum_position"],
                gateway_name="GM"
            )
            ticks.append(tick)

        print(ticks[0], ticks[-1])

        return ticks
