import uuid
import asyncio
from datetime import datetime
from typing import Dict, List, Optional
from common.order import Order, OrderStatus,Side
from common.orderbook import OrderBook
from engine.synchronizer import OrderBookSynchronizer  
from proto import matching_service_pb2 as pb2
import grpc.aio

class MatchEngine:
    def __init__(self, engine_id: str, synchronizer: OrderBookSynchronizer):
        self.engine_id = engine_id
        self.orderbooks: Dict[str, OrderBook] = {}
        self.orders: Dict[str, Order] = {}
        self.synchronizer = synchronizer 
        
    def create_orderbook(self, symbol: str) -> None:
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = OrderBook(symbol)
            
    async def submit_order(self, order: Order) -> (bool, str, List[Order]):
        """Submit a new order to the matching engine or reroute based on global best price."""
        # Assign the engine ID to the order
        order.engine_id = self.engine_id
        self.orders[order.order_id] = order
        # Ensure the orderbook exists for the symbol
        if order.symbol not in self.orderbooks:
            self.create_orderbook(order.symbol)

        # Get global best prices from the synchronizer
        global_best = self.synchronizer.global_best_prices  
        order_symbol = order.symbol  
        # print('checking')
        # print(global_best)

        if order_symbol in global_best:
            data = global_best[order_symbol]
            best_bid = data.get('best_bid', {})
            best_ask = data.get('best_ask', {})
            global_best_bid = best_bid.get('price', 'N/A')
            global_best_bid_id = best_bid.get('engine_id', 'N/A')
            global_best_ask = best_ask.get('price', 'N/A')
            global_best_ask_id = best_ask.get('engine_id', 'N/A')
        else:
            global_best_bid = None
            global_best_ask = None

        local_orderbook = self.orderbooks[order.symbol]
        local_best_bid = max(local_orderbook.bids.keys(), default=None)
        local_best_ask = min(local_orderbook.asks.keys(), default=None)

        # Log local and global best prices
        # print(f"Local best bid: {local_best_bid}, Local best ask: {local_best_ask}")
        # print(f"Global best bid: {global_best_bid}, Global best ask: {global_best_ask}")

        # Decide whether to process locally or reroute

        local_best_bid = local_best_bid if local_best_bid is not None else 0  
        local_best_ask = local_best_ask if local_best_ask is not None else 9999

        if order.side == Side.SELL and global_best_bid is not None and global_best_bid > order.price and global_best_bid > local_best_bid:
            # Global best bid is better
            # print(f"should reroute SELL order {order.order_id} for {order.symbol} to global best bid {global_best_bid}")
            # await self.route_order_to_global(order, global_best_bid, "bid", global_best_bid_id)
            # return []
            return False, global_best_bid_id, []
        elif order.side == Side.BUY and global_best_ask is not None and global_best_ask < order.price and global_best_ask < local_best_ask:
            # Global best ask is better
            # print(f"should reroute BUY order {order.order_id} for {order.symbol} to global best ask {global_best_ask}")
            # await self.route_order_to_global(order, global_best_ask, "ask", global_best_ask_id)
            return False, global_best_ask_id, []

        # Process locally if local best price is better or matches
        # print(f"Processing order {order.order_id} locally for {order.symbol}")
        fills = local_orderbook.add_order(order)

        # Gather order book data for synchronization
        bids = [(price, sum(o.remaining_quantity for o in orders), len(orders))
                for price, orders in local_orderbook.bids.items()]
        asks = [(price, sum(o.remaining_quantity for o in orders), len(orders))
                for price, orders in local_orderbook.asks.items()]

        # Log the updated bids and asks before publishing
        # print(f"Order book after processing order {order.order_id}: Bids={bids}, Asks={asks}")

        # Publish updates to peers
        # print(f"Publishing update for {order.symbol} after order submission")
        await self.synchronizer.publish_update(order.symbol, bids, asks)

        return True, 0, fills


    def cancel_order(self, order_id: str) -> Optional[Order]:
        """Cancel existing order"""
        if order_id not in self.orders:
            return None
            
        order = self.orders[order_id]
        if order.status != OrderStatus.CANCELLED:
            order.status = OrderStatus.CANCELLED
            # Remove the order from bids or asks in the order book
            local_orderbook = self.orderbooks[order.symbol]
            if order.side == Side.BUY and order.price in local_orderbook.bids:
                local_orderbook.bids[order.price].remove(order)
                if not local_orderbook.bids[order.price]:
                    del local_orderbook.bids[order.price]
            elif order.side == Side.SELL and order.price in local_orderbook.asks:
                local_orderbook.asks[order.price].remove(order)
                if not local_orderbook.asks[order.price]:
                    del local_orderbook.asks[order.price]

            print(f"Order {order_id} cancelled. Updated order book: Bids={local_orderbook.bids}, Asks={local_orderbook.asks}")
            return order
        return None
    
    async def get_peer_orderbooks(self) -> Dict[str, OrderBook]:
        """
        Fetch the orderbooks from all connected peer engines.
        """
        peer_orderbooks = {}
        for address, stub in self.peer_stubs.items():
            try:
                # Send a request to the peer to get the orderbook
                request = pb2.GetOrderBookRequest()
                response = await stub.GetOrderBook(request)

                # Convert the protobuf response to an OrderBook object
                orderbook = OrderBook(symbol=response.symbol)
                
                for bid in response.bids:
                    orderbook.bids[bid.price] = [
                        Order(
                            order_id=str(uuid.uuid4()),
                            symbol=response.symbol,
                            side=Side.BUY,
                            price=bid.price,
                            quantity=bid.quantity,
                            remaining_quantity=bid.quantity,
                            status=OrderStatus.NEW,
                            timestamp=datetime.now().timestamp(),
                            user_id="peer",
                            engine_id=address,
                        )
                        for _ in range(bid.order_count)
                    ]

                for ask in response.asks:
                    orderbook.asks[ask.price] = [
                        Order(
                            order_id=str(uuid.uuid4()),
                            symbol=response.symbol,
                            side=Side.SELL,
                            price=ask.price,
                            quantity=ask.quantity,
                            remaining_quantity=ask.quantity,
                            status=OrderStatus.NEW,
                            timestamp=datetime.now().timestamp(),
                            user_id="peer",
                            engine_id=address,
                        )
                        for _ in range(ask.order_count)
                    ]

                peer_orderbooks[address] = orderbook

            except Exception as e:
                print(f"Error fetching orderbook from peer {address}: {e}")

        return peer_orderbooks

