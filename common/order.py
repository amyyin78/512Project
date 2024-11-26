from dataclasses import dataclass
from enum import Enum
from datetime import datetime


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"


@dataclass
class Order:
    order_id: str
    symbol: str
    side: Side
    price: float
    quantity: float
    remaining_quantity: float
    status: OrderStatus
    timestamp: datetime
    client_id: str
    engine_origin_addr: str  # Add this line

    def pretty_print(self) -> str:
        if self.side == Side.SELL:
            return f"SELL {self.quantity} {self.symbol} @{self.price}"
        else:
            return f"BUY {self.quantity} {self.symbol} @{self.price}"

def pretty_print_OrderRequest(order) -> str:
    if order.side == "SELL":
        return f"SELL {order.quantity} {order.symbol} @{order.price}"
    else:
        return f"BUY {order.quantity} {order.symbol} @{order.price}"

@dataclass
class Fill:
    fill_id: str
    order_id: str
    symbol: str
    side: Side
    price: float
    quantity: float
    remaining_quantity: float
    timestamp: datetime
    buyer_id: str
    seller_id: str
    engine_destination_addr: str

    def pretty_print(self) -> str:
        if self.side == Side.SELL:
            return f"{self.seller_id} SOLD {self.quantity} {self.symbol} @{self.price} to {self.buyer_id}"
        else:
            return f"{self.buyer_id} BOUGHT {self.quantity} {self.symbol} @{self.price} from {self.seller_id}"


def pretty_print_FillResponse(fill) -> str:
    if fill.side == "SELL":
        return f"{fill.seller_id} SOLD {fill.quantity} {fill.symbol} @{fill.price} to {fill.buyer_id}"
    else:
        return f"{fill.buyer_id} BOUGHT {fill.quantity} {fill.symbol} @{fill.price} from {fill.seller_id}"
