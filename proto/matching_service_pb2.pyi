from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OrderRequest(_message.Message):
    __slots__ = ("order_id", "symbol", "side", "price", "quantity", "remaining_quantity", "client_id", "engine_origin_addr", "timestamp")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    REMAINING_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    ENGINE_ORIGIN_ADDR_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    symbol: str
    side: str
    price: float
    quantity: int
    remaining_quantity: int
    client_id: str
    engine_origin_addr: str
    timestamp: int
    def __init__(self, order_id: _Optional[str] = ..., symbol: _Optional[str] = ..., side: _Optional[str] = ..., price: _Optional[float] = ..., quantity: _Optional[int] = ..., remaining_quantity: _Optional[int] = ..., client_id: _Optional[str] = ..., engine_origin_addr: _Optional[str] = ..., timestamp: _Optional[int] = ...) -> None: ...

class SubmitOrderResponse(_message.Message):
    __slots__ = ("order_id", "status", "error_message")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    status: str
    error_message: str
    def __init__(self, order_id: _Optional[str] = ..., status: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class FillRequest(_message.Message):
    __slots__ = ("client_id", "engine_destination_addr", "timeout")
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    ENGINE_DESTINATION_ADDR_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    client_id: str
    engine_destination_addr: str
    timeout: int
    def __init__(self, client_id: _Optional[str] = ..., engine_destination_addr: _Optional[str] = ..., timeout: _Optional[int] = ...) -> None: ...

class Fill(_message.Message):
    __slots__ = ("fill_id", "order_id", "symbol", "side", "price", "quantity", "remaining_quantity", "timestamp", "buyer_id", "seller_id", "engine_destination_addr")
    FILL_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    REMAINING_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    BUYER_ID_FIELD_NUMBER: _ClassVar[int]
    SELLER_ID_FIELD_NUMBER: _ClassVar[int]
    ENGINE_DESTINATION_ADDR_FIELD_NUMBER: _ClassVar[int]
    fill_id: str
    order_id: str
    symbol: str
    side: str
    price: float
    quantity: int
    remaining_quantity: int
    timestamp: int
    buyer_id: str
    seller_id: str
    engine_destination_addr: str
    def __init__(self, fill_id: _Optional[str] = ..., order_id: _Optional[str] = ..., symbol: _Optional[str] = ..., side: _Optional[str] = ..., price: _Optional[float] = ..., quantity: _Optional[int] = ..., remaining_quantity: _Optional[int] = ..., timestamp: _Optional[int] = ..., buyer_id: _Optional[str] = ..., seller_id: _Optional[str] = ..., engine_destination_addr: _Optional[str] = ...) -> None: ...

class PutFillRequest(_message.Message):
    __slots__ = ("client_id", "fill")
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    FILL_FIELD_NUMBER: _ClassVar[int]
    client_id: str
    fill: Fill
    def __init__(self, client_id: _Optional[str] = ..., fill: _Optional[_Union[Fill, _Mapping]] = ...) -> None: ...

class PutFillResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class CancelOrderRequest(_message.Message):
    __slots__ = ("order_id", "client_id")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    client_id: str
    def __init__(self, order_id: _Optional[str] = ..., client_id: _Optional[str] = ...) -> None: ...

class CancelOrderResponse(_message.Message):
    __slots__ = ("order_id", "status", "error_message")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    status: str
    error_message: str
    def __init__(self, order_id: _Optional[str] = ..., status: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class PriceLevel(_message.Message):
    __slots__ = ("price", "quantity", "order_count")
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    ORDER_COUNT_FIELD_NUMBER: _ClassVar[int]
    price: float
    quantity: int
    order_count: int
    def __init__(self, price: _Optional[float] = ..., quantity: _Optional[int] = ..., order_count: _Optional[int] = ...) -> None: ...

class SyncRequest(_message.Message):
    __slots__ = ("symbol", "engine_id", "num_levels")
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    ENGINE_ID_FIELD_NUMBER: _ClassVar[int]
    NUM_LEVELS_FIELD_NUMBER: _ClassVar[int]
    symbol: str
    engine_id: str
    num_levels: int
    def __init__(self, symbol: _Optional[str] = ..., engine_id: _Optional[str] = ..., num_levels: _Optional[int] = ...) -> None: ...

class SyncResponse(_message.Message):
    __slots__ = ("symbol", "bids", "asks", "sequence_number", "engine_id")
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    BIDS_FIELD_NUMBER: _ClassVar[int]
    ASKS_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    ENGINE_ID_FIELD_NUMBER: _ClassVar[int]
    symbol: str
    bids: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    asks: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    sequence_number: int
    engine_id: str
    def __init__(self, symbol: _Optional[str] = ..., bids: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., asks: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., sequence_number: _Optional[int] = ..., engine_id: _Optional[str] = ...) -> None: ...

class BroadcastOrderbookRequest(_message.Message):
    __slots__ = ("symbol", "originating_engine_id", "num_levels", "bids", "asks", "sequence_number")
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    ORIGINATING_ENGINE_ID_FIELD_NUMBER: _ClassVar[int]
    NUM_LEVELS_FIELD_NUMBER: _ClassVar[int]
    BIDS_FIELD_NUMBER: _ClassVar[int]
    ASKS_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    symbol: str
    originating_engine_id: str
    num_levels: int
    bids: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    asks: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    sequence_number: int
    def __init__(self, symbol: _Optional[str] = ..., originating_engine_id: _Optional[str] = ..., num_levels: _Optional[int] = ..., bids: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., asks: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., sequence_number: _Optional[int] = ...) -> None: ...

class BroadcastOrderbookResponse(_message.Message):
    __slots__ = ("symbol", "receiving_engine_id", "status")
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    RECEIVING_ENGINE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    symbol: str
    receiving_engine_id: str
    status: str
    def __init__(self, symbol: _Optional[str] = ..., receiving_engine_id: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...

class GetOrderbookRequest(_message.Message):
    __slots__ = ("symbol",)
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    symbol: str
    def __init__(self, symbol: _Optional[str] = ...) -> None: ...

class GetOrderbookResponse(_message.Message):
    __slots__ = ("symbol", "bids", "asks", "timestamp")
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    BIDS_FIELD_NUMBER: _ClassVar[int]
    ASKS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    symbol: str
    bids: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    asks: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    timestamp: int
    def __init__(self, symbol: _Optional[str] = ..., bids: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., asks: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., timestamp: _Optional[int] = ...) -> None: ...

class ClientRegistrationRequest(_message.Message):
    __slots__ = ("client_id", "client_authentication", "client_x", "client_y")
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    CLIENT_AUTHENTICATION_FIELD_NUMBER: _ClassVar[int]
    CLIENT_X_FIELD_NUMBER: _ClassVar[int]
    CLIENT_Y_FIELD_NUMBER: _ClassVar[int]
    client_id: str
    client_authentication: str
    client_x: int
    client_y: int
    def __init__(self, client_id: _Optional[str] = ..., client_authentication: _Optional[str] = ..., client_x: _Optional[int] = ..., client_y: _Optional[int] = ...) -> None: ...

class ClientRegistrationResponse(_message.Message):
    __slots__ = ("status", "match_engine_address")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MATCH_ENGINE_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    status: str
    match_engine_address: str
    def __init__(self, status: _Optional[str] = ..., match_engine_address: _Optional[str] = ...) -> None: ...
