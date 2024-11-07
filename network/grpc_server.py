from grpc import aio

from proto import matching_service_pb2 as pb2
from proto import matching_service_pb2_grpc as pb2_grpc
from engine.match_engine import MatchEngine

class MatchingServicer(pb2_grpc.MatchingServiceServicer):
    def __init__(self, engine: MatchEngine):
        self.engine = engine

    async def SubmitOrder(self, request, context):
        try:
            order = self.engine.submit_order(request)
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
        # TODO - rn just return empty update
        return pb2.OrderBookUpdate(
            symbol=request.symbol,
            engine_id=self.engine.engine_id
        )

    async def GetOrderBook(self, request, context):
        # TODO - rn just return empty order book
        return pb2.OrderBook(symbol=request.symbol)

async def serve(engine: MatchEngine, address: str) -> aio.Server:
    """Start gRPC server"""
    # Create server using aio specifically
    server = aio.server()
    
    # Add the service
    service = MatchingServicer(engine)
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