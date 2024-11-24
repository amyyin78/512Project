from datetime import datetime
from typing import Dict, List
from collections import defaultdict
from .order import Order, Side, OrderStatus

class OrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[float, List[Order]] = defaultdict(list)  # Buy orders, sorted descending by price
        self.asks: Dict[float, List[Order]] = defaultdict(list)  # Sell orders, sorted ascending by price
    
    def add_order(self, order: Order) -> List[Order]:
        """
        Add an order to the order book and attempt to match it.
        """
        fills = []

        if order.side == Side.BUY:
            # Match with asks
            while order.remaining_quantity > 0 and self.asks:
                best_ask_price = min(self.asks.keys())  # Lowest ask price
                if order.price < best_ask_price:
                    break  # No match possible

                # Match with the best ask orders
                best_ask_orders = self.asks[best_ask_price]
                for ask_order in best_ask_orders[:]:  # Copy to avoid modification during iteration
                    match_quantity = min(order.remaining_quantity, ask_order.remaining_quantity)

                    # Record the fill
                    fills.append(Order(
                        order_id=ask_order.order_id,
                        symbol=order.symbol,
                        side="SELL",
                        price=best_ask_price,
                        quantity=match_quantity,
                        remaining_quantity=ask_order.remaining_quantity - match_quantity,
                        user_id=ask_order.user_id,
                        status=OrderStatus.FILLED if ask_order.remaining_quantity == match_quantity else OrderStatus.PARTIALLY_FILLED,
                        timestamp=datetime.now(),
                        engine_id=ask_order.engine_id
                    ))

                    # Update quantities
                    order.remaining_quantity -= match_quantity
                    ask_order.remaining_quantity -= match_quantity

                    # Remove fully filled ask orders
                    if ask_order.remaining_quantity == 0:
                        best_ask_orders.remove(ask_order)

                    if order.remaining_quantity == 0:
                        break

                # Remove the price level if all orders are filled
                if not best_ask_orders:
                    del self.asks[best_ask_price]

        elif order.side == Side.SELL:
            # Match with bids
            while order.remaining_quantity > 0 and self.bids:
                best_bid_price = max(self.bids.keys())  # Highest bid price
                if order.price > best_bid_price:
                    break  # No match possible

                # Match with the best bid orders
                best_bid_orders = self.bids[best_bid_price]
                for bid_order in best_bid_orders[:]:
                    match_quantity = min(order.remaining_quantity, bid_order.remaining_quantity)

                    # Record the fill
                    fills.append(Order(
                        order_id=bid_order.order_id,
                        symbol=order.symbol,
                        side="BUY",
                        price=best_bid_price,
                        quantity=match_quantity,
                        remaining_quantity=bid_order.remaining_quantity - match_quantity,
                        user_id=bid_order.user_id,
                        status=OrderStatus.FILLED if bid_order.remaining_quantity == match_quantity else OrderStatus.PARTIALLY_FILLED,
                        timestamp=datetime.now(),
                        engine_id=bid_order.engine_id
                    ))

                    # Update quantities
                    order.remaining_quantity -= match_quantity
                    bid_order.remaining_quantity -= match_quantity

                    # Remove fully filled bid orders
                    if bid_order.remaining_quantity == 0:
                        best_bid_orders.remove(bid_order)

                    if order.remaining_quantity == 0:
                        break

                # Remove the price level if all orders are filled
                if not best_bid_orders:
                    del self.bids[best_bid_price]

        # Add the remaining unmatched portion of the order to the book
        if order.remaining_quantity > 0:
            target_book = self.bids if order.side == Side.BUY else self.asks
            if order.price not in target_book:
                target_book[order.price] = []
            target_book[order.price].append(order)
            # print(f"Order {order.order_id} added to {'bids' if order.side == Side.BUY else Side.SELL} at price {order.price}. Remaining quantity: {order.remaining_quantity}")

        # Debug: Print current order book state
        # print(f"Current bids: {self._get_order_book_summary(self.bids)}")
        # print(f"Current asks: {self._get_order_book_summary(self.asks)}")

        return fills

    def _get_order_book_summary(self, book: Dict[float, List[Order]]) -> Dict[float, int]:
        """
        Summarizes the order book with price levels and total quantities for debugging.
        """
        return {price: sum(order.remaining_quantity for order in orders) for price, orders in book.items()}
