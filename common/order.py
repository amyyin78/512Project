from dataclasses import dataclass
from enum import Enum
from datetime import datetime
from typing import Optional

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
    user_id: str
    engine_id: str
    
# common/orderbook.py
from typing import Dict, List
from collections import defaultdict
from .order import Order, Side

class OrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[float, List[Order]] = defaultdict(list)
        self.asks: Dict[float, List[Order]] = defaultdict(list)
        
    def add_order(self, order: Order) -> List[Order]:
        """Add order to book and return list of fills"""
        fills = []
        if order.side == Side.BUY:
            # Match against asks
            for price in sorted(self.asks.keys()):
                if price > order.price or order.remaining_quantity <= 0:
                    break
                fills.extend(self._match_order_at_price(order, price))
        else:
            # Match against bids
            for price in sorted(self.bids.keys(), reverse=True):
                if price < order.price or order.remaining_quantity <= 0:
                    break
                fills.extend(self._match_order_at_price(order, price))
                
        # Add remaining quantity to book
        if order.remaining_quantity > 0:
            if order.side == Side.BUY:
                self.bids[order.price].append(order)
            else:
                self.asks[order.price].append(order)
                
        return fills
    
    def _match_order_at_price(self, incoming_order: Order, price: float) -> List[Order]:
        fills = []
        orders = self.bids[price] if incoming_order.side == Side.SELL else self.asks[price]
        
        for resting_order in orders[:]:
            fill_qty = min(incoming_order.remaining_quantity, resting_order.remaining_quantity)
            if fill_qty <= 0:
                continue
                
            # Update quantities
            incoming_order.remaining_quantity -= fill_qty
            resting_order.remaining_quantity -= fill_qty
            
            # Create fill records
            fills.append(Order(
                order_id=f"fill_{incoming_order.order_id}_{resting_order.order_id}",
                symbol=incoming_order.symbol,
                side=incoming_order.side,
                price=price,
                quantity=fill_qty,
                remaining_quantity=0,
                status=OrderStatus.FILLED,
                timestamp=datetime.now(),
                user_id=incoming_order.user_id,
                engine_id=incoming_order.engine_id
            ))
            
            # Remove filled orders
            if resting_order.remaining_quantity <= 0:
                orders.remove(resting_order)
                
        return fills