import logging
from client.custom_formatter import CustomFormatter

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.getcwd())

import asyncio
import random
import time
from typing import List, Optional

from engine.match_engine import MatchEngine
from engine.synchronizer import OrderBookSynchronizer
from network.grpc_server import serve
from client.client import Client


class MatchingSystemSimulator:
    def __init__(
        self, num_engines: int = 3, num_clients: int = 3, base_port: int = 50051
    ):
        self.name = "Simulator"
        self.num_engines = num_engines
        self.num_clients = num_clients
        self.base_port = base_port
        self.engines = []
        self.synchronizers = []
        self.servers = []
        self.clients = []

    async def setup(self):
        """Set up matching engines, synchronizers, clients, and logging"""

        # setup logging

        self._setup_logging()

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
                engine_id=f"engine_{i}", peer_addresses=peer_addresses
            )
            await synchronizer.start()  # Start the synchronizer
            self.synchronizers.append(synchronizer)

            # Start gRPC server
            try:
                server = await serve(engine, f"127.0.0.1:{self.base_port + i}")
                self.servers.append(server)
                self.logger.info(f"Started server {i} on port {self.base_port + i}")
            except Exception as e:
                self.logger.info(f"Failed to start server {i}: {e}")
                raise

        # Wait for servers to start
        await asyncio.sleep(2)

        # Create clients
        for i in range(self.num_clients):
            client = Client(f"Client_{i}")
            self.clients.append(client)
            self._assign_client(client)

    async def cleanup(self):
        """Cleanup resources"""
        # Stop synchronizers
        for synchronizer in self.synchronizers:
            await synchronizer.stop()

        # Stop servers
        for server in self.servers:
            await server.stop(grace=None)

        # Disconnect clients

        for client in self.clients:
            client.disconnect()

    async def run_simulation(
        self, num_orders: int = 1000, symbols: Optional[List[str]] = None
    ):
        """Run trading simulation"""
        if symbols is None:
            symbols = ["BTC-USD", "DOGE-BTC", "DUCK-DOGE"]

        self.logger.info("Starting simulation...")
        start_time = time.time()

        try:
            # Generate and submit orders
            for i in range(num_orders):
                # Select random client

                client_idx = random.randrange(len(self.clients))
                client = self.clients[client_idx]
                engine = self.engines[client.engine_index]
                synchronizer = self.synchronizers[client.engine_index]

                # Create random order

                order = client._generate_random_order(symbols)

                # Submit order and measure latency
                submit_time = time.time()
                fills = await client.submit_order(order)

                # Publish the update to peers
                if fills:
                    orderbook = engine.orderbooks.get(order.symbol)
                    if orderbook:
                        bids = [
                            (
                                price,
                                sum(o.remaining_quantity for o in orders),
                                len(orders),
                            )
                            for price, orders in orderbook.bids.items()
                        ]
                        asks = [
                            (
                                price,
                                sum(o.remaining_quantity for o in orders),
                                len(orders),
                            )
                            for price, orders in orderbook.asks.items()
                        ]
                        await synchronizer.publish_update(order.symbol, bids, asks)

                    latency = time.time() - submit_time
                    self.logger.info(
                        f"Order {i+1}: {order.order_id} executed in {latency*1000:.2f}ms with {len(fills)} fills"
                    )

                # Small delay between orders
                # await asyncio.sleep(0.1)

            total_time = time.time() - start_time
            self.logger.info("\nSimulation completed:")
            self.logger.info(
                f"Processed {num_orders} orders in {total_time:.2f} seconds"
            )
            self.logger.info(
                f"Average latency: {(total_time/num_orders)*1000:.2f}ms per order"
            )

            for client in self.clients:
                self.logger.info(
                    f"{client.name} average latency: {client.mean_latency() * 1_000_000:.3f} us"
                )

            # Print final order book state
            await self._print_order_books(symbols)

        except Exception as e:
            self.logger.info(f"Simulation error: {e}")
            raise

    async def _print_order_books(self, symbols: List[str]):
        """Print final state of all order books"""
        for symbol in symbols:
            self.logger.info(f"\nOrder book for {symbol}:")
            for engine in self.engines:
                if symbol in engine.orderbooks:
                    book = engine.orderbooks[symbol]
                    self.logger.info(f"\nEngine {engine.engine_id}:")
                    self.logger.info("Asks:")
                    for price in sorted(book.asks.keys(), reverse=True)[:5]:
                        if not (
                            sum(o.remaining_quantity for o in book.asks[price]) == 0
                        ):
                            self.logger.info(
                                f"  {price}: {sum(o.remaining_quantity for o in book.asks[price])}"
                            )
                    self.logger.info("Bids:")
                    for price in sorted(book.bids.keys(), reverse=True)[:5]:
                        if not (
                            sum(o.remaining_quantity for o in book.bids[price]) == 0
                        ):
                            self.logger.info(
                                f"  {price}: {sum(o.remaining_quantity for o in book.bids[price])}"
                            )

    def _assign_client(self, client):
        if not self.engines:
            return

        # assign clients randomly

        index = random.randint(0, len(self.engines) - 1)
        client.connect_to_engine(self.engines[index])
        client.set_engine_index(index)

        self.logger.info(f"assigned {client.name} to ME {index}")

        return

    def _setup_logging(self):
        self.log_directory = os.getcwd() + "/logs/simulator_logs/"
        self.log_file = os.getcwd() + "/logs/simulator_logs/" + self.name

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(CustomFormatter())
        self.logger.addHandler(ch)

        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)

        with open(self.log_file, "w") as file:
            file.write("")

        fh = logging.FileHandler(self.log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(CustomFormatter())
        self.logger.addHandler(fh)

        self.logger.info(
            f"started logging for simulator {self.name} at time {time.time()}"
        )
