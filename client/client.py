import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import random
from datetime import datetime as dt
from datetime import timezone
import pytz

import time
import uuid

from client.custom_formatter import LogFactory
from common.order import Order, Side, OrderStatus, Fill, pretty_print_FillResponse, pretty_print_OrderRequest

import grpc

import proto.matching_service_pb2 as pb2
import proto.matching_service_pb2_grpc as pb2_grpc

class Client: 
    """ gRPC client for order submission
    """
    def __init__(
        self,
        name: str,
        authentication_key: str,
        balance: int = 0,
        positions: dict = {},
        location: tuple = (0, 0),
        symbols: list = [],
        delay_factor: float = 1.0,
        exchange_addr: str = "127.0.0.1:50050",
        me_addr: str = "",
        direct_connect: bool = False
    ):
        self.name = name
        self.authentication_key = authentication_key
        self.log_directory = os.getcwd() + "/logs/client_logs/"
        self.log_file = os.getcwd() + "/logs/client_logs/" + name
        self.balance = balance
        self.positions = positions.copy()
        self.location = location
        self.connected_engine = None
        self.latencies = []
        self.symbols = symbols
        self.delay_factor = delay_factor
        self.exchange_addr = exchange_addr
        self.me_addr = me_addr
        self.connected_to_me = False
        self.direct_connect = direct_connect

        self.running = False
        self.order_running = False
        self.fill_running = False

        self.logger = LogFactory(self.name, self.log_directory).get_logger()

        if not symbols:
            self.symbols = ["BTC-USD", "DOGE-BTC", "DUCK-DOGE"]
        else:
            self.symbols = symbols

    async def submit_order(self, order: Order):
        if not self.connected_to_me:
            self.logger.error("No matching engine recorded")
        else:
            send_time = time.time()
            order = self._generate_random_order()
            # self.logger.info(f"{self.name} submitted order with ID: {order.order_id} at time {send_time}")
            self.logger.info(f"{self.name}: {order.pretty_print()}")

            eastern = pytz.timezone('US/Eastern')
            order_msg = pb2.OrderRequest(
                order_id=str(order.order_id),
                symbol=str(order.symbol),
                side=str(order.side.name),
                price=float(order.price),
                quantity=int(order.quantity),
                remaining_quantity=int(order.quantity),
                client_id=str(order.client_id),
                engine_origin_addr=str(self.me_addr),
                timestamp=(int(order.timestamp.astimezone(eastern).timestamp() * 10 ** 9))
            )
            self.logger.debug(f"Sent OrderRequest: {order_msg}")
            response = await self.stub.SubmitOrder(order_msg)

            if response.status == "ERROR":
                self.logger.error(f"Received response to order {order.pretty_print()}: {response.status} {response.error_message}")

            receive_time = time.time()
            self.latencies.append(receive_time - send_time)

    async def get_fills_and_update(self):
        fills = []
        if not self.connected_to_me:
            self.logger.error("No matching engine recorded")
        else:
            fill_stream = self.stub.GetFills(pb2.FillRequest(
                client_id=self.name,
                engine_destination_addr=self.me_addr, # NOTE: Unused
                timeout=1_000 # NOTE: Unused
            ))


            fill = await fill_stream.read()
            while (fill):
                self.logger.info(f"FILLED: {pretty_print_FillResponse(fill)}")
                self.update_positions(fill)
                fill = await fill_stream.read()

    async def register(self):
        if not self.direct_connect:
            try:
                self.exchange_channel = grpc.aio.insecure_channel(self.exchange_addr)
                self.connected_to_exchange = True
                self.logger.info(f"connected to exchange at address {self.exchange_addr}")

                self.exchange_stub = pb2_grpc.MatchingServiceStub(self.exchange_channel)
                response = await self.exchange_stub.RegisterClient(pb2.ClientRegistrationRequest(
                    client_id=self.name,
                    client_authentication=self.authentication_key,
                    client_x=0,
                    client_y=0,
                ))
                self.logger.info(f"Received registration response from exchange: {response}")
                self.me_addr = response.match_engine_address
            except Exception as e:
                self.logger.error(f"exchange registration error: {e}")


        try:
            self.me_channel = grpc.aio.insecure_channel(self.me_addr)
            self.stub = pb2_grpc.MatchingServiceStub(self.me_channel)
            response = await self.stub.RegisterClient(pb2.ClientRegistrationRequest(
                client_id=self.name,
                client_authentication=self.authentication_key,
                client_x=0,
                client_y=0,
            ))

            self.connected_to_me = True
            return response
        except Exception as e:
            self.logger.error(f"match engine registration error: {e}")

        return None







    async def run(self):
        self.logger.info(f"started runnning {self.name}")
        self.running = True
        self.order_running = True
        self.fill_running = True
        registration_response = await self.register()
        if (registration_response):
            if ("SUCCESSFUL" in registration_response.status):
                # TODO: Modify self.me_addr to have the address given in the response
                asyncio.create_task(self.run_loop())
            else:
                self.logger.error(f"Registration failed for client {self.name} with response status {registration_response.status}")

    async def run_loop(self):
        with grpc.insecure_channel(self.me_addr) as channel:
            stub = pb2_grpc.MatchingServiceStub(channel)
            while self.running:
                await asyncio.sleep(random.random() * self.delay_factor)
                order = self._generate_random_order()
                if self.fill_running:
                    await self.get_fills_and_update()
                if self.order_running:
                    await self.submit_order(order)

    async def stop(self):
        self.logger.info("Stopping run")
        self.order_running = False

        await self.get_fills_and_update()
        self.fill_running = False
        self.running = False


        # TODO: cancel all orders

    def update_positions(self, fill: Fill):
        if fill.symbol not in self.positions.keys():
            self.positions.update({fill.symbol: 0})
        if fill.buyer_id == self.name:
            self.balance -= fill.quantity * fill.price
            self.positions[fill.symbol] += fill.quantity
        if fill.seller_id == self.name:
            self.balance += fill.quantity * fill.price
            self.positions[fill.symbol] -= fill.quantity

    def log_positions(self):
        self.logger.info(f"Name: {self.name}")
        self.logger.info(f"Balance: {round(self.balance, 2)}")
        self.logger.info(f"Positions: \n {self.positions}")

    def mean_latency(self):
        return sum(self.latencies) / len(self.latencies)

    def _generate_random_order(self, symbols: list = []) -> Order:
        """Generate a random order"""

        order_symbols = []
        if symbols:
            order_symbols = symbols
        else:
            order_symbols = self.symbols

        gen_quantity = random.randint(1, 100)

        if gen_quantity % 2 == 0:
            gen_side = Side.SELL
            gen_price = round(random.uniform(95, 105), 2)
        else:
            gen_side = Side.BUY
            gen_price = round(random.uniform(90, 100), 2)

        return Order(
            order_id=str(uuid.uuid4()),
            symbol=random.choice(order_symbols),
            side=gen_side,
            price=gen_price,
            quantity=gen_quantity,
            remaining_quantity=gen_quantity,
            status=OrderStatus.NEW,
            timestamp=dt.now(),
            client_id=self.name,
            engine_origin_addr=self.me_addr  # Set the engine origin address
        )
