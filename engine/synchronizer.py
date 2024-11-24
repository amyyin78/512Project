import asyncio
import time
from typing import Dict, List, Set, Optional
import grpc
import grpc.aio

from common.order import Order, OrderStatus, Side
from common.orderbook import OrderBook
import proto.matching_service_pb2 as pb2
import proto.matching_service_pb2_grpc as pb2_grpc

class OrderBookSynchronizer:
    def __init__(self, engine_id: str, peer_addresses: List[str]):
        self.engine_id = engine_id
        self.peer_addresses = peer_addresses
        self.sequence_number = 0
        self.update_queue = asyncio.Queue()
        self.peer_stubs: Dict[str, pb2_grpc.MatchingServiceStub] = {}
        self.known_orders: Set[str] = set()
        self.running = False
        self.lock = asyncio.Lock()
        self.global_best_prices: Dict[str, Dict[str, Optional[float]]] = {}

    async def start(self):
        """Start the synchronizer"""
        await self._connect_to_peers()
        self.running = True
        asyncio.create_task(self._sync_loop())
        print(f"Synchronizer {self.engine_id} started")

    async def stop(self):
        """Stop the synchronizer"""
        self.running = False
        # Close all gRPC channels
        for stub in self.peer_stubs.values():
            if hasattr(stub, '_channel'):
                await stub._channel.close()

    async def _connect_to_peers(self):
        """Establish async gRPC connections to peer engines"""
        for address in self.peer_addresses:
            try:
                channel = grpc.aio.insecure_channel(address)
                self.peer_stubs[address] = pb2_grpc.MatchingServiceStub(channel)
            except Exception as e:
                print(f"Failed to connect to peer at {address}: {e}")

    async def _sync_loop(self):
        """Main synchronization loop"""
        while self.running:
            try:
                # Get next update from queue
                update = await self.update_queue.get()

                try:
                    await self._broadcast_update(update)
                    self.update_queue.task_done()
                except Exception as e:
                    print(f"Error broadcasting update: {e}")
                    self.update_queue.task_done()

                try:
                    await self._process_peer_updates()
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"Error processing peer updates: {e}")
                
            except Exception as e:
                print(f"Sync error: {e}")
                await asyncio.sleep(1)

    async def _broadcast_update(self, update: dict):
        """Broadcast update to all peer engines"""
        pb_update = pb2.OrderBookUpdate(
            symbol=update['symbol'],
            sequence_number=self.sequence_number,
            engine_id=self.engine_id,
            bids=[pb2.PriceLevel(
                price=price,
                quantity=qty,
                order_count=count
            ) for price, qty, count in update['bids']],
            asks=[pb2.PriceLevel(
                price=price,
                quantity=qty,
                order_count=count
            ) for price, qty, count in update['asks']]
        )

        # Broadcast to all peers
        tasks = []
        for address, stub in self.peer_stubs.items():
            try:
                tasks.append(stub.SyncOrderBook(pb_update))
            except Exception as e:
                print(f"Error creating broadcast task for {address}: {e}")

        if tasks:
            # Wait for all broadcasts to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    print(f"Broadcast error: {result}")

    async def _process_peer_updates(self):
        """Process incoming updates from peer engines"""
        async with self.lock:
            for address, stub in self.peer_stubs.items():
                try:
                    # Get updates from peer
                    request = pb2.GetOrderBookRequest()
                    updates = await stub.GetOrderBook(request)
                    
                    # Process each update
                    if hasattr(updates, 'sequence_number') and updates.sequence_number > self.sequence_number:
                        await self._apply_update(updates)
                            
                except Exception as e:
                    print(f"Error processing updates from {address}: {e}")

    async def _apply_update(self, update):
        """Apply an order book update from a peer"""
        async with self.lock:
            # Update sequence number
            self.sequence_number = max(self.sequence_number, update.sequence_number)
            
            # Add to known orders if it's an order update
            if hasattr(update, 'order_id') and update.order_id not in self.known_orders:
                self.known_orders.add(update.order_id)
                
                # Convert protobuf update to internal format
                order = Order(
                    order_id=update.order_id,
                    symbol=update.symbol,
                    side=update.side,
                    price=update.price,
                    quantity=update.quantity,
                    remaining_quantity=update.quantity,
                    status=OrderStatus.NEW,
                    timestamp=time.time(),
                    user_id=update.user_id,
                    engine_id=update.engine_id
                )
                
                return order

     

    
    async def publish_update(self, symbol: str, bids: List[tuple], asks: List[tuple]):
        """
        Publish an order book update to peers.
        Only prices with >0 volume are considered for best bid and ask.
        """
        # Filter bids and asks to include only those with volume > 0
        valid_bids = [(price, quantity, count) for price, quantity, count in bids if quantity > 0]
        valid_asks = [(price, quantity, count) for price, quantity, count in asks if quantity > 0]

        # Calculate best bid and ask from valid prices
        best_bid = max((price for price, _, _ in valid_bids), default=None)
        best_ask = min((price for price, _, _ in valid_asks), default=None)

        # Log filtered bids, asks, and calculated best prices
        # print(f"current global Best Bid: {best_bid}, Best Ask: {best_ask}")

        # Proceed with publishing the update if there are valid prices
        update = {
            'symbol': symbol,
            'bids': valid_bids,
            'asks': valid_asks,
            'timestamp': time.time()
        }
        await self.update_queue.put(update)
        # print(f"Publishing update for {symbol} with {len(valid_bids)} valid bids and {len(valid_asks)} valid asks")

        # Update sequence number
        self.sequence_number += 1

        # Update global best prices
        await self.update_global_best_prices(symbol, best_bid, best_ask)

    async def broadcast_best_prices(self, symbol: str, best_bid: float, best_ask: float):
        """Broadcast global best bid and ask prices to peers"""
        pb_update = pb2.GlobalBestPriceUpdate(
            symbol=symbol,
            best_bid=best_bid,
            best_ask=best_ask,
            engine_id=self.engine_id
        )
        tasks = []
        for address, stub in self.peer_stubs.items():
            try:
                tasks.append(stub.SyncGlobalBestPrice(pb_update))
            except Exception as e:
                print(f"Error broadcasting best prices to {address}: {e}")
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    print(f"Broadcast error: {result}")

    def extract_engine_id(address: str) -> int:
        """
        Extract the engine ID from the given address.

        The engine ID is determined as the last digit of the port minus 1.

        Args:
            address (str): The peer address in the format 'IP:PORT'.

        Returns:
            int: The calculated engine ID.
        """
        try:
            # Split the address to extract the port
            port = address.split(":")[-1]

            # Get the last digit of the port
            last_digit = int(port[-1])

            # Compute the engine ID
            engine_id = last_digit - 1
            return engine_id
        except (ValueError, IndexError) as e:
            print(f"Error extracting engine ID from address {address}: {e}")
            return None


    
    async def update_global_best_prices(self, symbol: str, highest_bid: float, lowest_ask: float):
        """
        Update the global best prices for a symbol and include engine IDs.

        Args:
            symbol (str): The trading symbol.
            highest_bid (float): The current highest bid price.
            lowest_ask (float): The current lowest ask price.
        """
        highest_bid_engine = None
        lowest_ask_engine = None

        for address, stub in self.peer_stubs.items():
            try:
                request = pb2.GetOrderBookRequest(symbol=symbol)
                response = await stub.GetOrderBook(request)

                valid_bids = [price_level for price_level in response.bids if price_level.quantity > 0]
                valid_asks = [price_level for price_level in response.asks if price_level.quantity > 0]

                peer_highest_bid = max((price_level.price for price_level in valid_bids), default=None)
                peer_lowest_ask = min((price_level.price for price_level in valid_asks), default=None)

                # Get engine ID from address
                # engine_id = extract_engine_id(address)

                if peer_highest_bid is not None and (highest_bid is None or peer_highest_bid > highest_bid):
                    highest_bid = peer_highest_bid
                    highest_bid_engine = address

                if peer_lowest_ask is not None and (lowest_ask is None or peer_lowest_ask < lowest_ask):
                    lowest_ask = peer_lowest_ask
                    lowest_ask_engine = address

            except Exception as e:
                print(f"Failed to fetch order book from {address} for {symbol}: {e}")

        # Save the updated global best prices
        self.global_best_prices[symbol] = {
            'best_bid': {'price': highest_bid, 'engine_id': highest_bid_engine},
            'best_ask': {'price': lowest_ask, 'engine_id': lowest_ask_engine},
        }





    def print_global_best_prices(self):
        """Print the global best bid and ask for all symbols."""
        print("\nGlobal Best Prices:")
        for symbol, prices in self.global_best_prices.items():
            best_bid = prices['best_bid']
            best_ask = prices['best_ask']
            print(f"Symbol: {symbol}")
            print(f"  Best Bid: {best_bid if best_bid is not None else 'None'}")
            print(f"  Best Ask: {best_ask if best_ask is not None else 'None'}")

    async def get_peer_orderbooks(self, symbol: str):
        """
        Fetch and maintain order books from all peer engines.
        This method retrieves the current order books from all peers and updates
        the local state to reflect the latest data from each engine.
        """
        peer_orderbooks = {}

        for address, stub in self.peer_stubs.items():
            try:
                # Create a request for the order book
                request = pb2.GetOrderBookRequest(symbol=symbol)
                response = await stub.GetOrderBook(request)

                # Parse response into a structured format
                peer_orderbooks[address] = {
                    'bids': [
                        {'price': level.price, 'quantity': level.quantity, 'order_count': level.order_count}
                        for level in response.bids if level.quantity > 0
                    ],
                    'asks': [
                        {'price': level.price, 'quantity': level.quantity, 'order_count': level.order_count}
                        for level in response.asks if level.quantity > 0
                    ],
                    'sequence_number': response.sequence_number
                }
            except Exception as e:
                print(f"Failed to fetch order book from {address}: {e}")

        return peer_orderbooks
    
    async def fetch_peer_order_book(self, symbol: str, engine_address: str) -> Optional[OrderBook]:
        """
        Fetch the order book for a symbol from a peer engine.

        Args:
            symbol (str): The trading symbol.
            engine_address (str): The address of the peer engine.

        Returns:
            Optional[OrderBook]: The retrieved order book in internal format, or None if unavailable.
        """
        stub = self.peer_stubs.get(engine_address)
        if not stub:
            print(f"Error: No gRPC stub found for engine address {engine_address}")
            return None

        try:
            # Request order book from the peer
            request = pb2.GetOrderBookRequest(symbol=symbol)
            response = await stub.GetOrderBook(request)

            # Convert the protobuf response into an internal OrderBook object
            orderbook = OrderBook(symbol=symbol)
            for bid in response.bids:
                orderbook.bids[bid.price] = [
                    Order(
                        order_id=str(uuid.uuid4()),
                        symbol=symbol,
                        side=Side.BUY,
                        price=bid.price,
                        quantity=bid.quantity,
                        remaining_quantity=bid.quantity,
                        status=OrderStatus.NEW,
                        timestamp=datetime.now().timestamp(),
                        user_id="peer",
                        engine_id=engine_address,
                    )
                    for _ in range(bid.order_count)
                ]
            for ask in response.asks:
                orderbook.asks[ask.price] = [
                    Order(
                        order_id=str(uuid.uuid4()),
                        symbol=symbol,
                        side=Side.SELL,
                        price=ask.price,
                        quantity=ask.quantity,
                        remaining_quantity=ask.quantity,
                        status=OrderStatus.NEW,
                        timestamp=datetime.now().timestamp(),
                        user_id="peer",
                        engine_id=engine_address,
                    )
                    for _ in range(ask.order_count)
                ]
            return orderbook

        except grpc.RpcError as e:
            print(f"Failed to fetch order book from engine at {engine_address}: {e.details()}")
            return None

        







