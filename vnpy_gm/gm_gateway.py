from operator import sub
from threading import Thread
from time import sleep
from datetime import datetime
from typing import Dict, Tuple, List
from importlib import import_module
from gm.api import (
    set_token,
    run,
    stop,
    order_volume,
    order_cancel,
    ## get_cash,
    ## get_positions,
    get_orders,
    get_execution_reports,
    get_instruments,
    subscribe
)
from gm.model.storage import context

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
    ContractData
)
from vnpy.trader.constant import (
    OrderType,
    Direction,
    Exchange,
    Status,
    Offset,
    Product
)
from vnpy.trader.utility import ZoneInfo


# 委托状态映射
STATUS_GM2VT: Dict[int, Status] = {
    1: Status.NOTTRADED,
    2: Status.PARTTRADED,
    3: Status.ALLTRADED,
    5: Status.CANCELLED,
    6: Status.CANCELLED,
    8: Status.REJECTED,
    6: Status.CANCELLED,
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
    0: OrderType.LIMIT,
    2: OrderType.MARKET,
}
ORDERTYPE_VT2GM: Dict[Tuple, int] = {v: k for k, v in ORDERTYPE_GM2VT.items()}

# 交易所映射
EXCHANGE_GM2VT: Dict[str, Exchange] = {
    "SHSE": Exchange.SSE,
    "SZSE": Exchange.SZSE,
}
EXCHANGE_VT2GM: Dict[Exchange, str] = {v: k for k, v in EXCHANGE_GM2VT.items()}

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
    VeighNa用于对接掘金的交易接口。
    """

    default_name: str = "GM"

    default_setting: Dict[str, str] = {
        "token": "",
        "地址": "",
        "账户": ""
    }

    exchanges: List[str] = list(EXCHANGE_GM2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.td_api: "GmTdApi" = GmTdApi(self)

        self.run_timer: Thread = Thread(target=self.process_timer_event)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        token: str = setting["token"]
        endpoint: str = setting["地址"]
        accountid: str = setting["账户"]

        self.td_api.connect(token, endpoint, accountid)
        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.td_api.subscribe(req)

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
        while self.td_api._active:
            sleep(3)
            self.query_position()
            self.query_account()

    def init_query(self) -> None:
        """初始化查询任务"""
        self.run_timer.start()

    def close(self) -> None:
        """关闭接口"""
        self.td_api.close()


class GmTdApi:

    def __init__(self, gateway: GmGateway):
        """构造函数"""
        super().__init__()

        self.gateway: GmGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.inited: bool = False
        self._active: bool = False

        self.accountid: str = ""
        self.token: str = ""
        self.order_ref: int = 0

        self.subscribed: set = set()
        self.run: Thread = Thread(target=self.init_callback)

    def init_callback(self) -> None:
        run(filename="vnpy_gm.gm_gateway", mode=1, token="e34de55059c6156dd35aa4f3c8c630cdfd509d39")

    def connect(self, token: str, endpoint: str, accountid: str) -> None:
        """初始化"""
        self.accountid = accountid
        self.token = token

        if not self.inited:
            self.inited = True
            set_token(token)

            ## set_endpoint(endpoint)
            ## login(account(accountid))
            ## err: int = start("vnpy_gm.gm_gateway")
            ## if err:
            ##    self.gateway.write_log(f"交易服务器登陆失败，错误码{err}")
            ##    return

            # run(filename="vnpy_gm.gm_gateway",mode=1, token=token)
  
            self.run.start()

            self.gateway.write_log("GM接口初始化完成")
            self._active = True

            self.query_contract()
            self.query_order()
            self.query_trade()


        else:
            self.gateway.write_log("已经初始化，请勿重复操作")

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        contract: ContractData = symbol_contract_map.get(req.symbol, None)
        if not contract or req.symbol in self.subscribed:
            return

        symbol: str = EXCHANGE_VT2GM[req.exchange] + "." + req.symbol
        subscribe(symbols=symbol, frequency='tick')
        self.subscribed.add(req.symbol)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
#        contract: ContractData = symbol_contract_map.get(req.symbol, None)
#        if not contract:
#            self.gateway.write_log(f"找不到该合约{req.symbol}")
#            return ""
#
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
            account=self.accountid
        )
        print(order_data)
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
        ## data = get_positions()
        data = context.account(self.accountid).positions()

        for d in data:
            exchange_, symbol = d.symbol.split(".")
            exchange: Exchange = EXCHANGE_GM2VT.get(exchange_, None)
            if not exchange:
                continue
        
            position: PositionData = PositionData(
                symbol=symbol,
                exchange=exchange,
                direction=Direction.NET,
                volume=d.volume,
                frozen=d.order_frozen,
                price=d.vwap,
                pnl=d.fpnl,
                yd_volume=d.volume - d.volume_today,
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)

    def query_account(self) -> None:
        """查询账户资金"""
        ## data = get_cash()
        data = context.account(self.accountid).cash

        account: AccountData = AccountData(
            accountid=self.accountid,
            balance=data.nav,
            frozen=data.frozen,
            gateway_name=self.gateway_name
        )
        account.available = data.available
        self.gateway.on_account(account)

    def query_contract(self) -> None:
        """查询合约信息"""
        ## data = get_cash()
        data = get_instruments(exchanges=["SHSE", "SZSE"])

        for d in data:
            if d["sec_type"] != 1:
                continue

            exchange, symbol = d["symbol"].split(".")

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=EXCHANGE_GM2VT[exchange],
                name=d["sec_name"],
                product=Product.EQUITY,
                size=1,
                pricetick=d["price_tick"],
                gateway_name=self.gateway_name
            )
            self.gateway.on_contract(contract)
            symbol_contract_map[symbol] = contract

        self.gateway.write_log("合约信息查询成功")

    def query_order(self) -> None:
        """查询委托信息"""
        data = get_orders()
        print("query_order", data)

        for d in data:          
            exchange, symbol = d.symbol.split(".")
            order: OrderData = OrderData(
                symbol=symbol,
                exchange=EXCHANGE_GM2VT[exchange],
                orderid=d.cl_ord_id,
                type=ORDERTYPE_GM2VT[d.order_type],
                direction=DIRECTION_GM2VT[d.side],
                offset=POSITIONEFFECT_GM2VT[d.position_effect],
                price=d.price,
                volume=d.volume,
                traded=d.filled_volume,
                status=STATUS_GM2VT[d.status],
                datetime=d.updated_at.replace(tzinfo=CHINA_TZ),
                gateway_name=self.gateway_name
            )
            self.gateway.on_order(order)
            
        self.gateway.write_log("委托信息查询成功")

    def query_trade(self) -> None:
        """查询成交信息"""
        data = get_execution_reports()
        print("query_trade", data)

        for d in data:
            exchange, symbol = d.symbol.split(".")
            trade: TradeData = TradeData(
                symbol=symbol,
                exchange=EXCHANGE_GM2VT[exchange],
                orderid=d.cl_ord_id,
                tradeid=d.exec_id,
                direction=DIRECTION_GM2VT[d.side],
                price=d.price,
                volume=d.volume,
                datetime=d.created_at.replace(tzinfo=CHINA_TZ),
                gateway_name=self.gateway_name
            )
            self.gateway.on_trade(trade)

        self.gateway.write_log("成交信息查询成功")

    def close(self) -> None:
        """关闭连接"""
        if self.inited:
            self._active = False
            stop()


def on_trade_data_connected(context):
    # subscribe(symbols='SZSE.000333', frequency='tick')
    print('已连接交易服务.................')
        
# 回报到达时触发
def on_execution_report(rpt):
    print(f'exec_rpt_count={rpt}')

# 委托状态变化时触发
def on_order_status(order):
    print(f'order_stats_count={order}')
    
# 交易服务断开后触发
def on_trade_data_disconnected():
    print('已断开交易服务.................')

def on_tick(context, tick):
    print(tick)
