from threading import Thread
from datetime import datetime, timedelta
from time import sleep
from typing import Dict, Tuple, List
from pandas import DataFrame

import tushare as ts
from tushare.pro.client import DataApi
from google.protobuf.timestamp_pb2 import Timestamp
from gmtrade.api import (
    set_token,
    order_volume,
    order_cancel,
    get_cash,
    get_positions,
    get_orders,
    get_execution_reports,
    login,
    set_endpoint,
    account
)
from gmtrade.api.storage import ctx
from gmtrade.api.callback import callback_controller
from gmtrade.csdk.c_sdk import (
    c_status_fail,
    py_gmi_set_data_callback,
    py_gmi_start,
    py_gmi_stop
)

from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    ContractData,
    TickData
)
from vnpy.trader.constant import (
    OrderType,
    Direction,
    Exchange,
    Status,
    Offset,
    Product
)
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import ZoneInfo


# 委托状态映射
STATUS_GM2VT: Dict[int, Status] = {
    1: Status.NOTTRADED,
    2: Status.PARTTRADED,
    3: Status.ALLTRADED,
    5: Status.CANCELLED,
    6: Status.CANCELLED,
    8: Status.REJECTED,
    9: Status.CANCELLED,
    10: Status.SUBMITTING,
    12: Status.CANCELLED
}

# 多空方向映射
DIRECTION_VT2GM: Dict[Direction, int] = {
    Direction.LONG: 1,
    Direction.SHORT: 2,
}
DIRECTION_GM2VT: Dict[int, Direction] = {v: k for k, v in DIRECTION_VT2GM.items()}

# 委托类型映射
ORDERTYPE_GM2VT: Dict[int, Tuple] = {
    1: OrderType.LIMIT,
    2: OrderType.MARKET,
}
ORDERTYPE_VT2GM: Dict[Tuple, int] = {v: k for k, v in ORDERTYPE_GM2VT.items()}

# 交易所映射
EXCHANGE_GM2VT: Dict[str, Exchange] = {
    "SHSE": Exchange.SSE,
    "SZSE": Exchange.SZSE,
}
EXCHANGE_VT2GM: Dict[Exchange, str] = {v: k for k, v in EXCHANGE_GM2VT.items()}

MDEXCHANGE_GM2VT: Dict[str, Exchange] = {
    "SSE": Exchange.SSE,
    "SZSE": Exchange.SZSE,
}

# 多空方向映射
POSITIONEFFECT_VT2GM: Dict[Offset, int] = {
    Offset.OPEN: 1,
    Offset.CLOSE: 2,
    Offset.CLOSETODAY: 3,
    Offset.CLOSEYESTERDAY: 4,
}
POSITIONEFFECT_GM2VT: Dict[int, Offset] = {v: k for k, v in POSITIONEFFECT_VT2GM.items()}

# 其他常量
CHINA_TZ = ZoneInfo("Asia/Shanghai")       # 中国时区

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


class GmGateway(BaseGateway):
    """
    VeighNa用于对接掘金量化终端的交易接口。
    """

    default_name: str = "GM"

    default_setting: Dict[str, str] = {
        "Token": "",
        "账户ID": ""
    }

    exchanges: List[str] = list(EXCHANGE_GM2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.md_api: "GmMdApi" = GmMdApi(self)
        self.td_api: "GmTdApi" = GmTdApi(self)

        self.run_timer: Thread = Thread(target=self.process_timer_event)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        token: str = setting["Token"]
        accountid: str = setting["账户ID"]

        self.md_api.init()
        self.td_api.connect(token, accountid)
        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        self.td_api.query_position()

    def process_timer_event(self) -> None:
        """定时事件处理"""
        while self.td_api._active and self.md_api._active:
            sleep(3)
            self.query_position()
            self.query_account()
            self.md_api.query_realtime_quotes()

    def init_query(self) -> None:
        """初始化查询任务"""
        self.run_timer.start()

    def close(self) -> None:
        """关闭接口"""
        self.md_api.close()
        self.td_api.close()


class GmMdApi:

    def __init__(self, gateway: GmGateway) -> None:
        """构造函数"""
        self.gateway: GmGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

        self._active: bool = False
        self.subscribed: set = set()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if req.symbol in symbol_contract_map:
            self.subscribed.add(req.symbol)

    def init(self) -> None:
        """初始化"""
        ts.set_token(self.password)

        self.pro: DataApi = ts.pro_api()
        self.query_contract()

        self._active = True
        self.gateway.write_log("数据服务初始化完成")

    def query_contract(self) -> None:
        """查询合约"""
        try:
            sh_data: DataFrame = self.pro.query("stock_basic", exchange="SSE", list_status="L", fields="symbol,name,exchange")
            sz_data: DataFrame = self.pro.query("stock_basic", exchange="SZSE", list_status="L", fields="symbol,name,exchange")
        except IOError:
            return

        data: DataFrame = sh_data.append(sz_data, ignore_index=True)

        if data is not None:
            for ix, row in data.iterrows():
                contract: ContractData = ContractData(
                    symbol=row["symbol"],
                    exchange=MDEXCHANGE_GM2VT[row["exchange"]],
                    name=row["name"],
                    product=Product.EQUITY,
                    size=1,
                    pricetick=0.01,
                    gateway_name=self.gateway_name
                )
                self.gateway.on_contract(contract)
                symbol_contract_map[row["symbol"]] = contract

        self.gateway.write_log("合约信息查询成功")

    def query_realtime_quotes(self) -> None:
        """查询行情数据"""
        try:
            df: DataFrame = ts.get_realtime_quotes(self.subscribed)
        except IOError:
            return

        if df is not None:
            # 处理原始数据中的NaN值
            df.fillna(0, inplace=True)

            for ix, row in df.iterrows():
                dt: str = row["date"].replace("-", "") + " " + row["time"].replace(":", "")
                contract: ContractData = symbol_contract_map[row["code"]]

                tick: tick = TickData(
                    symbol=row["code"],
                    exchange=contract.exchange,
                    datetime=generate_datetime(dt),
                    name=contract.name,
                    open_price=process_data(row["open"]),
                    high_price=process_data(row["high"]),
                    low_price=process_data(row["low"]),
                    pre_close=process_data(row["pre_close"]),
                    last_price=process_data(row["price"]),
                    volume=process_data(row["volume"]),
                    turnover=process_data(row["amount"]),
                    bid_price_1=process_data(row["b1_p"]),
                    bid_price_2=process_data(row["b2_p"]),
                    bid_price_3=process_data(row["b3_p"]),
                    bid_price_4=process_data(row["b4_p"]),
                    bid_price_5=process_data(row["b5_p"]),
                    bid_volume_1=process_data(row["b1_v"]) * 100,
                    bid_volume_2=process_data(row["b2_v"]) * 100,
                    bid_volume_3=process_data(row["b3_v"]) * 100,
                    bid_volume_4=process_data(row["b4_v"]) * 100,
                    bid_volume_5=process_data(row["b5_v"]) * 100,
                    ask_price_1=process_data(row["a1_p"]),
                    ask_price_2=process_data(row["a2_p"]),
                    ask_price_3=process_data(row["a3_p"]),
                    ask_price_4=process_data(row["a4_p"]),
                    ask_price_5=process_data(row["a5_p"]),
                    ask_volume_1=process_data(row["a1_v"]) * 100,
                    ask_volume_2=process_data(row["a2_v"]) * 100,
                    ask_volume_3=process_data(row["a3_v"]) * 100,
                    ask_volume_4=process_data(row["a4_v"]) * 100,
                    ask_volume_5=process_data(row["a5_v"]) * 100,
                    gateway_name=self.gateway_name
                )
                self.gateway.on_tick(tick)

    def close(self) -> None:
        """关闭连接"""
        if self._active:
            self._active = False


class GmTdApi:

    def __init__(self, gateway: GmGateway):
        """构造函数"""
        super().__init__()

        self.gateway: GmGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.inited: bool = False
        self._active: bool = False

    def onconnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("交易服务器连接成功")

    def ondisconnected(self) -> None:
        """服务器连接断开回报"""
        self.gateway.write_log("交易服务器连接断开")

    def onRtnOrder(self, order) -> None:
        """生成时间戳"""
        type: OrderType = ORDERTYPE_GM2VT.get(order.order_type, None)
        if type is None:
            return

        exchange, symbol = order.symbol.split(".")
        order_data: OrderData = OrderData(
            symbol=symbol,
            exchange=EXCHANGE_GM2VT[exchange],
            orderid=order.cl_ord_id,
            type=type,
            direction=DIRECTION_GM2VT[order.side],
            offset=POSITIONEFFECT_GM2VT[order.position_effect],
            price=round(order.price, 2),
            volume=order.volume,
            traded=order.filled_volume,
            status=STATUS_GM2VT[order.status],
            datetime=generate_datetime1(order.updated_at),
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order_data)

        if order.ord_rej_reason_detail:
            self.gateway.write_log(f"委托拒单：{order.ord_rej_reason_detail}")

    def onRtnTrade(self, rpt) -> None:
        """生成时间戳"""
        if rpt.exec_type != 15:
            if rpt.ord_rej_reason_detail:
                self.gateway.write_log(rpt.ord_rej_reason_detail)
            return

        exchange, symbol = rpt.symbol.split(".")
        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=EXCHANGE_GM2VT[exchange],
            orderid=rpt.cl_ord_id,
            tradeid=rpt.exec_id,
            direction=DIRECTION_GM2VT[rpt.side],
            price=round(rpt.price, 2),
            volume=rpt.volume,
            datetime=generate_datetime1(rpt.created_at),
            gateway_name=self.gateway_name
        )
        self.gateway.on_trade(trade)

    def on_error(self, code, info) -> None:
        """输出错误信息"""
        self.gateway.write_log(f"错误代码：{code}，信息：{info}")

    def connect(self, token: str, accountid: str) -> None:
        """连接交易接口"""
        if not self.inited:
            self.inited = True

            set_token(token)

            set_endpoint()
            login(account(accountid))
            err: int = self.init_callback()
            if err:
                self.gateway.write_log(f"交易服务器登陆失败，错误码{err}")
                return

            self.query_order()
            self.query_trade()

        else:
            self.gateway.write_log("已经初始化，请勿重复操作")

    def init_callback(self) -> int:
        """注册回调"""
        ctx.inside_file_module = self

        ctx.on_execution_report_fun = self.onRtnTrade
        ctx.on_order_status_fun = self.onRtnOrder
        ctx.on_trade_data_connected_fun = self.onconnected
        ctx.on_trade_data_disconnected_fun = self.ondisconnected

        ctx.on_error_fun = self.on_error

        py_gmi_set_data_callback(callback_controller)  # 设置事件处理的回调函数

        status: int = py_gmi_start()  # type: int
        if c_status_fail(status, 'gmi_start'):
            self._active = False
            return status
        else:
            self._active = True
        return status

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if req.offset not in POSITIONEFFECT_VT2GM:
            self.gateway.write_log("请选择开平方向")
            return ""

        type: int = ORDERTYPE_VT2GM.get(req.type, None)
        if type is None:
            self.gateway.write_log(f"不支持的委托类型: {req.type.value}")
            return ""

        exchange: str = EXCHANGE_VT2GM.get(req.exchange, None)

        if exchange is None:
            self.gateway.write_log(f"不支持的交易所: {req.exchange.value}")
            return ""

        symbol: str = exchange + "." + req.symbol

        order_data: list = order_volume(
            symbol=symbol,
            volume=int(req.volume),
            side=DIRECTION_VT2GM[req.direction],
            order_type=type,
            price=req.price,
            position_effect=POSITIONEFFECT_VT2GM[req.offset],
        )
        orderid: str = order_data[0].cl_ord_id

        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        cancel_order: dict = {"cl_ord_id": req.orderid}

        order_cancel(wait_cancel_orders=cancel_order)

    def query_position(self) -> None:
        """查询持仓"""
        data: list = get_positions()

        for d in data:
            exchange_, symbol = d.symbol.split(".")
            exchange: Exchange = EXCHANGE_GM2VT.get(exchange_, None)
            if not exchange:
                continue

            position: PositionData = PositionData(
                symbol=symbol,
                exchange=exchange,
                direction=DIRECTION_GM2VT[d.side],
                volume=d.volume,
                frozen=d.order_frozen,
                price=round(d.vwap, 2),
                pnl=round(d.fpnl, 2),
                yd_volume=round(d.volume - d.volume_today, 2),
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)

    def query_account(self) -> None:
        """查询账户资金"""
        data = get_cash()
        if not data:
            self.gateway.write_log("请检查accountid")
            return

        account: AccountData = AccountData(
            accountid=data.account_id,
            balance=round(data.nav, 2),
            frozen=round(data.frozen, 2),
            gateway_name=self.gateway_name
        )
        account.available = round(data.available, 2)
        self.gateway.on_account(account)

    def query_order(self) -> None:
        """查询委托信息"""
        data: list = get_orders()

        for d in data:
            type: OrderType = ORDERTYPE_GM2VT.get(d.order_type, None)
            exchange_, symbol = d.symbol.split(".")
            exchange: Exchange = EXCHANGE_GM2VT.get(exchange_, None)

            if not type or not exchange:
                continue

            order: OrderData = OrderData(
                symbol=symbol,
                exchange=exchange,
                orderid=d.cl_ord_id,
                type=ORDERTYPE_GM2VT[d.order_type],
                direction=DIRECTION_GM2VT[d.side],
                offset=POSITIONEFFECT_GM2VT[d.position_effect],
                price=round(d.price, 2),
                volume=d.volume,
                traded=d.filled_volume,
                status=STATUS_GM2VT[d.status],
                datetime=generate_datetime1(d.updated_at),
                gateway_name=self.gateway_name
            )
            self.gateway.on_order(order)

        self.gateway.write_log("委托信息查询成功")

    def query_trade(self) -> None:
        """查询成交信息"""
        data: list = get_execution_reports()

        for d in data:
            exchange_, symbol = d.symbol.split(".")
            exchange: Exchange = EXCHANGE_GM2VT.get(exchange_, None)
            if not exchange:
                continue

            trade: TradeData = TradeData(
                symbol=symbol,
                exchange=exchange,
                orderid=d.cl_ord_id,
                tradeid=d.exec_id,
                direction=DIRECTION_GM2VT[d.side],
                price=round(d.price, 2),
                volume=d.volume,
                datetime=generate_datetime1(d.created_at),
                gateway_name=self.gateway_name
            )
            self.gateway.on_trade(trade)

        self.gateway.write_log("成交信息查询成功")

    def close(self) -> None:
        """关闭连接"""
        if self.inited:
            self._active = False
            py_gmi_stop()


def generate_datetime(timestamp: str) -> datetime:
    """生成时间"""
    dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H%M%S")
    dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    return dt


def generate_datetime1(timestamp: Timestamp) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp.seconds)
    dt: datetime = dt + timedelta(microseconds=timestamp.nanos / 1000)
    dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    return dt


def process_data(data: str) -> float:
    """处理空字符"""
    if data == "":
        d = 0
    else:
        d = float(data)
    return d
