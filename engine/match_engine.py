import uuid
from datetime import datetime
from typing import Dict, List, Optional
from common.order import Order, OrderStatus
from common.orderbook import OrderBook

class MatchEngine:
    def __init__(self, engine_id: str):
        self.engine_id = engine_id
        self.orderbooks: Dict[str, OrderBook] = {}
        self.orders: Dict[str, Order] = {}
        
    def create_orderbook(self, symbol: str) -> None:
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = OrderBook(symbol)
            
    def submit_order(self, order: Order) -> List[Order]:
        """Submit new order to matching engine"""
        order.engine_id = self.engine_id
        self.orders[order.order_id] = order
        
        if order.symbol not in self.orderbooks:
            self.create_orderbook(order.symbol)
            
        fills = self.orderbooks[order.symbol].add_order(order)
        return fills
        
    def cancel_order(self, order_id: str) -> Optional[Order]:
        """Cancel existing order"""
        if order_id not in self.orders:
            return None
            
        order = self.orders[order_id]
        if order.status != OrderStatus.CANCELLED:
            order.status = OrderStatus.CANCELLED
            return order
        return None
