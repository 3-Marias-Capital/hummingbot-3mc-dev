from typing import NamedTuple

class TradeType(NamedTuple):
    timestamp: int
    datetime: str
    symbol: str
    price: float
    qty: float
    trade_type: str
