import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import random
import time
from typing import List
import uuid
import grpc
import grpc.aio

from engine.match_engine import MatchEngine
from engine.synchronizer import OrderBookSynchronizer
from common.order import Order, Side, OrderStatus
from network.grpc_server import serve

class MatchingSystemSimulator:
    def __init__(self, num_engines: int = 3, base_port: int = 50051):
        self.num_engines = num_engines
        self.base_port = base_port
        self.engines = []
        self.synchronizers = []
        self.servers = []
        
    async def setup(self):
        """Set up matching engines and synchronizers"""
        # Create engines
        for i in range(self.num_engines):
            engine = MatchEngine(f"engine_{i}")
            self.engines.append(engine)
            
            # Create peer address list for each engine
            peer_addresses = [
                f"127.0.0.1:{self.base_port + j}"
                for j in range(self.num_engines)
                if j != i
            ]
            
            # Create and start synchronizer
            synchronizer = OrderBookSynchronizer(
                engine_id=f"engine_{i}",
                peer_addresses=peer_addresses
            )
            await synchronizer.start()  # Start the synchronizer
            self.synchronizers.append(synchronizer)
            
            # Start gRPC server
            try:
                server = await serve(
                    engine,
                    f"127.0.0.1:{self.base_port + i}"
                )
                self.servers.append(server)
                print(f"Started server {i} on port {self.base_port + i}")
            except Exception as e:
                print(f"Failed to start server {i}: {e}")
                raise
            
        # Wait for servers to start
        await asyncio.sleep(2)
        
    async def cleanup(self):
        """Cleanup resources"""
        # Stop synchronizers
        for synchronizer in self.synchronizers:
            await synchronizer.stop()
        
        # Stop servers
        for server in self.servers:
            await server.stop(grace=None)
        
    async def run_simulation(self, num_orders: int = 1000, symbols: List[str] = None):
        """Run trading simulation"""
        if symbols is None:
            symbols = ["BTC-USD", "DOGE-BTC", "DUCK-DOGE"]
            
        print("Starting simulation...")
        start_time = time.time()
        
        try:
            # Generate and submit orders
            for i in range(num_orders):
                order = self._generate_random_order(symbols)
                
                # Select random engine and its synchronizer
                engine_idx = random.randrange(len(self.engines))
                engine = self.engines[engine_idx]
                synchronizer = self.synchronizers[engine_idx]
                
                # Submit order and measure latency
                submit_time = time.time()
                fills = engine.submit_order(order)
                
                # Publish the update to peers
                if fills:
                    orderbook = engine.orderbooks.get(order.symbol)
                    if orderbook:
                        bids = [(price, sum(o.remaining_quantity for o in orders), len(orders)) 
                               for price, orders in orderbook.bids.items()]
                        asks = [(price, sum(o.remaining_quantity for o in orders), len(orders)) 
                               for price, orders in orderbook.asks.items()]
                        await synchronizer.publish_update(order.symbol, bids, asks)
                
                latency = time.time() - submit_time
                print(f"Order {i+1}: {order.order_id} executed in {latency*1000:.2f}ms with {len(fills)} fills")
                
                # Small delay between orders
                await asyncio.sleep(0.1)
                
            total_time = time.time() - start_time
            print(f"\nSimulation completed:")
            print(f"Processed {num_orders} orders in {total_time:.2f} seconds")
            print(f"Average latency: {(total_time/num_orders)*1000:.2f}ms per order")
            
            # Print final order book state
            await self._print_order_books(symbols)
            
        except Exception as e:
            print(f"Simulation error: {e}")
            raise
            
    async def _print_order_books(self, symbols: List[str]):
        """Print final state of all order books"""
        for symbol in symbols:
            print(f"\nOrder book for {symbol}:")
            for engine in self.engines:
                if symbol in engine.orderbooks:
                    book = engine.orderbooks[symbol]
                    print(f"\nEngine {engine.engine_id}:")
                    print("Bids:")
                    for price in sorted(book.bids.keys(), reverse=True)[:5]:
                        print(f"  {price}: {sum(o.remaining_quantity for o in book.bids[price])}")
                    print("Asks:")
                    for price in sorted(book.asks.keys())[:5]:
                        print(f"  {price}: {sum(o.remaining_quantity for o in book.asks[price])}")

    def _generate_random_order(self, symbols: List[str]) -> Order:
        """Generate a random order"""
        return Order(
            order_id=str(uuid.uuid4()),
            symbol=random.choice(symbols),
            side=random.choice([Side.BUY, Side.SELL]),
            price=round(random.uniform(90, 110), 2),
            quantity=random.randint(1, 100),
            remaining_quantity=random.randint(1, 100),
            status=OrderStatus.NEW,
            timestamp=time.time(),
            user_id=f"user_{random.randint(1, 10)}",
            engine_id=""
        )

async def main():
    # Initialize gRPC (non-async call)
    grpc.aio.init_grpc_aio()
    
    # Create simulator with desired configuration
    simulator = MatchingSystemSimulator(
        num_engines=3,
        base_port=50051
    )
    
    try:
        # Set up the system
        await simulator.setup()
        
        # Run simulation
        await simulator.run_simulation(
            num_orders=100
        )
    except Exception as e:
        print(f"Simulation failed: {e}")
    finally:
        # Cleanup
        await simulator.cleanup()

if __name__ == "__main__":
    asyncio.run(main())