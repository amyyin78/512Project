import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import grpc
import grpc.aio

from engine.exchange import Exchange
from client.client import Client
from simulation.simulation import MatchingSystemSimulator


async def matching_simulation():
    # Initialize gRPC (non-async call)
    grpc.aio.init_grpc_aio()

    # Create simulator with desired configuration
    simulator = MatchingSystemSimulator(num_engines=3, num_clients=6, base_port=50051)

    try:
        # Set up the system
        await simulator.setup()

        # Run simulation
        await simulator.run_simulation(num_orders=100)
    except Exception as e:
        print(f"Simulation failed: {e}")
    finally:
        # Cleanup
        await simulator.cleanup()


async def exchange_simulation():
    # Initialize gRPC (non-async call)
    grpc.aio.init_grpc_aio()
    symbol_list = ["AAPL", "TSLA"]

    exchange = Exchange(num_engines=3, base_port=50051, symbols=symbol_list)

    # specify number of clients
    clients = []
    for i in range(5):
        clients.append(
            Client(name=f"Client {i}", symbols=symbol_list, delay_factor=0.1)
        )

    await exchange.setup()
    for client in clients:
        exchange.add_client(client)

    client_tasks = []

    # start running clients
    for client in clients:
        await client.run()

    # run sim for 10 seconds
    await asyncio.sleep(5)

    # get client positions
    for client in clients:
        client.log_positions()

    # stop clients 
    for client in clients:
        await client.stop()

    await exchange.cleanup()


async def main():
    # await matching_simulation()
    await exchange_simulation()


if __name__ == "__main__":
    asyncio.run(main())
