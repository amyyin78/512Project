from grpc import aio

from proto import matching_service_pb2 as pb2
from proto import matching_service_pb2_grpc as pb2_grpc
from engine.match_engine import MatchEngine
from engine.synchronizer import OrderBookSynchronizer

class MatchingServicer(pb2_grpc.MatchingServiceServicer):
    def __init__(self, engine, synchronizer):
        self.engine = engine
        self.synchronizer = synchronizer

    async def SubmitOrder(self, request, context):
        try:
            # Await the asynchronous submit_order method
            await self.engine.submit_order(request)
            return pb2.SubmitOrderResponse(
                order_id=request.order_id,
                status="SUCCESS"
            )
        except Exception as e:
            return pb2.SubmitOrderResponse(
                order_id=request.order_id,
                status="ERROR",
                error_message=str(e)
            )

    async def CancelOrder(self, request, context):
        try:
            result = self.engine.cancel_order(request.order_id)
            return pb2.CancelOrderResponse(
                order_id=request.order_id,
                status="SUCCESS" if result else "NOT_FOUND"
            )
        except Exception as e:
            return pb2.CancelOrderResponse(
                order_id=request.order_id,
                status="ERROR",
                error_message=str(e)
            )

    async def SyncOrderBook(self, request, context):
        """
        Synchronize order book by providing the current state of the symbol's bids and asks.
        """
        symbol = request.symbol

        # Ensure the requested symbol exists in the local engine
        if symbol not in self.engine.orderbooks:
            print(f"Symbol {symbol} not found in local order books.")
            return pb2.OrderBookUpdate(
                symbol=symbol,
                engine_id=self.engine.engine_id,
                bids=[],  # Empty bids
                asks=[]   # Empty asks
            )

        # Retrieve the local order book for the symbol
        orderbook = self.engine.orderbooks[symbol]

        # Construct the response with bids and asks
        response = pb2.OrderBookUpdate(
            symbol=symbol,
            engine_id=self.engine.engine_id,
            bids=[
                pb2.PriceLevel(
                    price=price,
                    quantity=sum(o.remaining_quantity for o in orders),
                    order_count=len(orders)
                )
                for price, orders in orderbook.bids.items()
            ],
            asks=[
                pb2.PriceLevel(
                    price=price,
                    quantity=sum(o.remaining_quantity for o in orders),
                    order_count=len(orders)
                )
                for price, orders in orderbook.asks.items()
            ]
        )

        print(f"SyncOrderBook response for {symbol}: Bids={response.bids}, Asks={response.asks}")
        return response


    async def GetOrderBook(self, request, context):
        symbol = request.symbol
        if symbol in self.engine.orderbooks:
            orderbook = self.engine.orderbooks[symbol]
            # print(f"Server processing GetOrderBook for {symbol}")
            # print(f"Bids: {orderbook.bids}")
            # print(f"Asks: {orderbook.asks}")
            response = pb2.OrderBook(
                symbol=symbol,
                bids=[
                    pb2.PriceLevel(price=price, quantity=sum(o.remaining_quantity for o in orders), order_count=len(orders))
                    for price, orders in orderbook.bids.items()
                ],
                asks=[
                    pb2.PriceLevel(price=price, quantity=sum(o.remaining_quantity for o in orders), order_count=len(orders))
                    for price, orders in orderbook.asks.items()
                ]
            )
            # print(f"Response to be sent: {response}")
            return response

        # print(f"Symbol {symbol} not found in order books")
        return pb2.OrderBook(symbol=symbol)

    async def SyncGlobalBestPrice(self, request, context):
        """Handle incoming global best price updates from peers"""
        symbol = request.symbol
        best_bid = request.best_bid
        best_ask = request.best_ask
        
        # Debugging log for incoming global best price updates
        # print(f"Received global best price update for {symbol}: "
              # f"Best Bid={best_bid}, Best Ask={best_ask}")

        # Update the global best prices in the synchronizer
        await self.synchronizer.update_global_best_prices(symbol, best_bid, best_ask)
        return pb2.Empty()
    
async def serve(engine: MatchEngine, synchronizer: OrderBookSynchronizer, address: str) -> aio.Server:
    """Start gRPC server"""
    # Create server using aio specifically
    server = aio.server()
    
    # Add the service with engine and synchronizer
    service = MatchingServicer(engine, synchronizer)
    pb2_grpc.add_MatchingServiceServicer_to_server(service, server)
    
    try:
        # Add the port
        server.add_insecure_port(address)
        
        # Start the server
        await server.start()
        print(f"Server started on {address}")
        
        return server
        
    except Exception as e:
        print(f"Error starting server: {e}")
        await server.stop(0)
        raise
