import os
import queue
from typing import Dict, Optional
import asyncio
from common.order import Order, OrderStatus
from common.orderbook import OrderBook
from client.custom_formatter import LogFactory
from engine.synchronizer import OrderBookSynchronizer


class MatchEngine:
    def __init__(self, engine_id: str, engine_addr: str, synchronizer: OrderBookSynchronizer, authentication_key: str = "password"):
        self.engine_id = engine_id
        self.address = engine_addr
        self.orderbooks: Dict[str, OrderBook] = {}
        self.orders: Dict[str, Order] = {}
        self.clients = []
        self.fill_queues = {}

        self.log_directory = os.getcwd() + "/logs/engine_logs/"
        self.logger = LogFactory(
            f"ME {self.engine_id}", self.log_directory
        ).get_logger()

        self.num_orders = 0
        self.num_fills = 0
        self.authentication_key = authentication_key
        self.synchronizer = synchronizer
        self.fill_routing_table = {}

        self.symbol_bbo_lookup = {}

    async def start_synchronizer(self):
        await self.synchronizer.start()

    def create_orderbook(self, symbol: str) -> None:
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = OrderBook(symbol)

    async def submit_order(self, order):
        """Submit new order to matching engine"""

        self.logger.debug(f"received order: {order}")

        # First check if the best price for this symbol is on another engine
        best_me_addr = await self.synchronizer.lookup_bbo_engine(order)
        self.logger.info(f"Order {order.order_id} original address: {order.engine_origin_addr}")
        self.logger.info(f"Order {order.order_id} best address: {best_me_addr}")

        if best_me_addr != self.address and order.engine_origin_addr == self.address:
            # route the order at most once
            self.logger.info(f"routing order from {self.address} -> {best_me_addr}")
            await self.synchronizer.route_order(order, best_me_addr)
            self.logger.info(f"Order {order.order_id} routed to {best_me_addr}")
            return {'incoming_fills' : [], 'resting_fills' : []}
        else:
            if order.engine_origin_addr != self.address:
                self.logger.info(f"order {order.order_id} has been rerouted once, will process locally")
            else:
                self.logger.info(f"Order {order.order_id} processed locally")

        self.orders[order.order_id] = order

        # update fill routing table
        self.fill_routing_table[order.client_id] = order.engine_origin_addr
        self.logger.debug(f"registered {order.client_id} wanting fills on engine address {order.engine_origin_addr}")

        # validate order
        self.validate_order(order)
        self.logger.debug(f"order validated")

        # add the order
        fills = self.orderbooks[order.symbol].add_order(order)
        self.logger.debug(f"order added")
        self.num_orders += 1

        # NOTE: Turn on this to see order book state after each order
        self.logger.debug(f"order book state for {order.symbol} on engine {self.address}:\n" + str(self.orderbooks[order.symbol]))

        # Add fills to queues
        if fills:
            self.logger.debug(f"clients registered with ME {self.address}: {self.clients}")
            for client_id, fill in fills['incoming_fills'] + fills['resting_fills']:
                if client_id in self.clients:
                    self.fill_queues[client_id].put(fill)
                    self.logger.debug(f"put to {client_id}")
                    self.logger.debug(f"put: {fill}")
                    self.num_fills += 1
                else: 
                    # the filled order was a routed order
                    if client_id in self.fill_routing_table.keys():
                        me_dst_addr = self.fill_routing_table[client_id]
                        self.logger.debug(f"routing fill for client {client_id} to address {me_dst_addr}")
                        # push a fill 
                        await self.synchronizer.route_fill(fill, client_id, me_dst_addr)
                    else:
                        self.logger.error(f"{client_id} not a registered client of {self.engine_id} and is not registered in the routing table")

        return fills

    def validate_order(self, order):
        if order.symbol not in self.orderbooks:
            self.create_orderbook(order.symbol)

        if order.quantity != order.remaining_quantity:
            self.logger.warning(
                f"order from {order.client_id} with properties {order.pretty_print()} is malformed\nReason: quantity is not equal to remaining_quantity"
            )
            order.remaining_quantity = order.quantity

    def register_client(self, client_name):
        if client_name not in self.clients:
            self.clients.append(client_name)
            self.fill_queues.update({client_name : queue.Queue()})
            self.logger.info(f"Registered client {client_name}")
        else:
            self.logger.warning(f"Attempted duplicate registration of client {client_name}")
            # TODO: Maybe prevent connection here?


    def authenticate(self, client_id : str, client_authentication : str):
        # TODO: Add a proper authentication system (simple)
        if (client_authentication == self.authentication_key):
            return True
        else:
            self.logger.error(f"client {client_id} failed to authenticate on matching engine {self.engine_id} with password {client_authentication}")
            return False

    def cancel_order(self, order_id: str) -> Optional[Order]:
        """Cancel existing order"""
        if order_id not in self.orders:
            return None

        order = self.orders[order_id]
        if order.status != OrderStatus.CANCELLED:
            order.status = OrderStatus.CANCELLED
            return order
        return None

    def log_orderbooks(self):
        for orderbook in self.orderbooks:
            self.logger.info(orderbook)
