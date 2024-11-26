import asyncio
import time
from typing import Dict, List, Set
import grpc
import grpc.aio
import pytz
import datetime
import uuid

from common.order import Order, OrderStatus
import proto.matching_service_pb2 as pb2
import proto.matching_service_pb2_grpc as pb2_grpc


import asyncio
import time
from typing import Dict, List, Set, Optional
import grpc
import grpc.aio
import os

from common.order import Order, OrderStatus, Side
from common.orderbook import OrderBook
from client.custom_formatter import LogFactory

class OrderBookSynchronizer:
    def __init__(self, engine_id: str, engine_addr: str, peer_addresses: List[str]):
        self.engine_id = engine_id
        self.engine_addr = engine_addr
        self.peer_addresses = peer_addresses
        self.sequence_number = 0
        self.update_queue = asyncio.Queue()
        self.peer_stubs: Dict[str, pb2_grpc.MatchingServiceStub] = {}
        self.known_orders: Set[str] = set()
        self.running = False
        self.lock = asyncio.Lock()
        self.global_best_prices: Dict[str, Dict[str, Optional[float]]] = {}

        self.log_directory = os.getcwd() + "/logs/synchronizer_logs/"
        self.logger = LogFactory(
            name=f"Synchronizer-ME {self.engine_id}", 
            log_directory=self.log_directory
        ).get_logger()

        # Include itself in peer_stubs
        self.stub = pb2_grpc.MatchingServiceStub(grpc.aio.insecure_channel(self.engine_addr))

    async def start(self):
        """Start the synchronizer"""
        await self._connect_to_peers()
        self.running = True
        asyncio.create_task(self._sync_loop())
        self.logger.info(f"Synchronizer {self.engine_id} started")

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
                self.logger.error(f"Failed to connect to peer at {address}: {e}")

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

#                try:
#                    await self._process_peer_updates()
#                    await asyncio.sleep(0.1)
#                except Exception as e:
#                    print(f"Error processing peer updates: {e}")
                
            except Exception as e:
                print(f"Sync error: {e}")
                await asyncio.sleep(1)

            await asyncio.sleep(0.1) # return control

    async def _broadcast_update(self, update: dict):
        """Broadcast update to all peer engines"""
        pb_update = pb2.BroadcastOrderbookRequest(
            symbol=update['symbol'],
            sequence_number=self.sequence_number,
            originating_engine_id=self.engine_id,
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
                tasks.append(stub.BroadcastOrderbook(pb_update))
            except Exception as e:
                print(f"Error creating broadcast task for {address}: {e}")

        if tasks:
            # Wait for all broadcasts to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    print(f"Broadcast error: {result}")

    async def process_peer_update(self, request):
        """Process incoming updates from peer engines"""
                    
        try:
            if request.sequence_number > self.sequence_number:
                await self._apply_update(request)
            else:
                self.logger.warning(f"request {request} has sequence number smaller than sequence number ({self.sequence_number}) of synchronizer")
                            
        except Exception as e:
           self.logger.error(f"Error processing updates from {request.originating_engine_addr}: {e}")

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
                    client_id=update.user_id,
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

    async def route_fill(self, fill, client_id, me_addr): 
        stub = self.peer_stubs[me_addr]

        self.logger.debug(f"route_fill fill: {fill}")
        eastern = pytz.timezone('US/Eastern')

        fill_dict = {
            'fill_id' : str(fill.fill_id),
            'order_id' : str(fill.order_id),
            'symbol' : str(fill.symbol),
            'side' : str(fill.side),
            'price' : float(fill.price),
            'quantity' : int(fill.quantity),
            'remaining_quantity' : int(fill.remaining_quantity),
            'timestamp' : (int(fill.timestamp.astimezone(eastern).timestamp() * 10 ** 9)),
            'buyer_id' : str(fill.buyer_id),
            'seller_id' : str(fill.seller_id),
            'engine_destination_addr' : str(fill.engine_destination_addr),
        }
        fill_response = await stub.PutFill(pb2.PutFillRequest(
            client_id=str(client_id),
            fill=fill_dict,
        ))
        self.logger.debug(f"route fill {self.engine_addr} -> {me_addr} return status {fill_response.status}")


    async def route_order(self, order, me_addr):
        stub = self.peer_stubs[me_addr]
        order_response = await stub.SubmitOrder(order)
        self.logger.debug(f"route order {order.engine_origin_addr} -> {me_addr} return status {order_response.status}")


    async def lookup_bbo_engine(self, order):
        """Returns the address of the engine with the best bid/ask for a symbol"""
        side = order.side
        symbol = order.symbol
        best_bids_asks = await self.get_global_best_bids_asks([symbol])
        if side == Side.BUY:
            # find best ask available
            if best_bids_asks[1][symbol][0] is None or order.price < best_bids_asks[1][symbol][0]:
                self.logger.info(f"Best ask for {symbol} is on {best_bids_asks[1][symbol][1]} but order is below, not rerouting")
                return self.engine_addr
            return best_bids_asks[1][symbol][1]  
        else:
            # find best bid available
            if best_bids_asks[0][symbol][0] is None or order.price > best_bids_asks[0][symbol][0]:
                self.logger.info(f"Best bid for {symbol} is on {best_bids_asks[0][symbol][1]} but order is above, not rerouting")
                return self.engine_addr
            return best_bids_asks[0][symbol][1]

    async def get_global_best_bids_asks(self, symbols: List[str]):
        """Fetch and log global best bids and asks across engines"""
        self.logger.info(f"Fetching global best bids and asks for {symbols}")
        global_best_bids = {}
        global_best_asks = {}

        for symbol in symbols:
            # Initialize with local best bid and ask
            local_orderbook = await self._get_local_orderbook(symbol)
            best_bid = max((level.price for level in local_orderbook.bids if level.quantity > 0), default=float('-inf'))
            best_ask = min((level.price for level in local_orderbook.asks if level.quantity > 0), default=float('inf'))

            if best_bid is None:
                self.logger.info(f"No local best bid found for {symbol}")
            if best_ask is None:
                self.logger.info(f"No local best ask found for {symbol}")

            global_best_bids[symbol] = (best_bid, self.engine_addr)
            global_best_asks[symbol] = (best_ask, self.engine_addr)

            for address in self.peer_stubs.keys():
                try:
                    stub = self.peer_stubs[address]
                    request = pb2.GetOrderbookRequest(symbol=symbol)
                    update = await stub.GetOrderBook(request)

                    best_bid = max((level.price for level in update.bids if level.quantity > 0), default=None)
                    best_ask = min((level.price for level in update.asks if level.quantity > 0), default=None)

                    if best_bid is not None:
                        if symbol not in global_best_bids or best_bid > global_best_bids[symbol][0]:
                            global_best_bids[symbol] = (best_bid, address)

                    if best_ask is not None:
                        if symbol not in global_best_asks or best_ask < global_best_asks[symbol][0]:
                            global_best_asks[symbol] = (best_ask, address)

                except Exception as e:
                    self.logger.error(f"Error fetching order book from {address} for {symbol}: {e}")
        self.logger.info(f"Global best bids: {global_best_bids}")
        self.logger.info(f"Global best asks: {global_best_asks}")

        return global_best_bids, global_best_asks

    async def _get_local_orderbook(self, symbol: str):
        """Fetch the local order book for a symbol"""
        request = pb2.GetOrderbookRequest(symbol=symbol)
        response = await self.stub.GetOrderBook(request)
        return response

    def get_best_bid(self, symbol: str) -> Optional[float]:
        """Get the best bid price for a symbol in the local order book"""
        if symbol in self.global_best_prices and 'bid' in self.global_best_prices[symbol]:
            return self.global_best_prices[symbol]['bid']
        return None

    def get_best_ask(self, symbol: str) -> Optional[float]:
        """Get the best ask price for a symbol in the local order book"""
        if symbol in self.global_best_prices and 'ask' in self.global_best_prices[symbol]:
            return self.global_best_prices[symbol]['ask']
        return None






