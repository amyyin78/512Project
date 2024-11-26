import asyncio
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.getcwd())

from engine.match_engine import MatchEngine
from engine.exchange import Exchange
from engine.synchronizer import OrderBookSynchronizer
from network.grpc_server import MatchingServicer, serve_ME

from client.custom_formatter import LogFactory
from client.client import Client



async def main():
    symbol_list = ["AAPL"]
    DELAY_FACTOR = 1
    SIM_DURATION = 10 # in seconds
    EXCHANGE_ADDR = "127.0.0.1:50050"
    client_names = [
        "Adam",
        "Betsy",
        "Charlie",
        "Diana",
        "Eric",
        "Fred",
        "Geoffrey",
        "Harry",
        "Ian",
    ]
    client_assignments = {
        "Adam": "engine_0",
        "Betsy": "engine_1",
        "Charlie": "engine_2",
        "Diana": "engine_0",
        "Eric": "engine_1",
        "Fred": "engine_2",
        "Geoffrey": "engine_0",
        "Harry": "engine_1",
        "Ian": "engine_2"
    }
    me_data = {
        "engine_0": "127.0.0.1:50051",
        "engine_1": "127.0.0.1:50052",
        "engine_2": "127.0.0.1:50053"
    }
    clients = []

    for client_name in client_names:
        assigned_engine = client_assignments[client_name]
        clients.append(Client(
            name=client_name,
            authentication_key="password",
            symbols=symbol_list,
            delay_factor=DELAY_FACTOR,
            exchange_addr=EXCHANGE_ADDR,
            me_addr=me_data[assigned_engine],
            direct_connect=True
        ))

    for client in clients:
        await client.run()

    await asyncio.sleep(SIM_DURATION)

    for client in clients:
        asyncio.create_task(client.stop())

    await asyncio.sleep(1) # give time to log positions

    for client in clients:
        client.log_positions() # get final positions



if __name__ == "__main__":
    asyncio.run(main())

