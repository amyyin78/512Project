import asyncio
import time
from typing import Dict, List, Set
import grpc
import grpc.aio

from common.order import Order, OrderStatus
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
        """Publish an order book update to peers"""
        update = {
            'symbol': symbol,
            'bids': bids,
            'asks': asks,
            'timestamp': time.time()
        }
        await self.update_queue.put(update)
        self.sequence_number += 1