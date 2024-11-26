# Distributed Order Matching Engine

A simple distributed order matching system that demonstrates how multiple matching engines can synchronize order books across different nodes. 

Final Project for CS512 Distsys 2024 Fall

## Project Structure
```
.
├── client
│   ├── client.py
├── common
│   ├── orderbook.py
│   ├── order.py
├── engine
│   ├── exchange.py
│   ├── match_engine.py
│   └── synchronizer.py
├── logs
│   ├── ...
├── main.py
├── network
│   ├── grpc_server.py
├── proto
│   ├── matching_service_pb2_grpc.py
│   ├── matching_service_pb2.py
│   ├── matching_service.proto
├── README.md
└── simulation
    └── simulation.py
```

## Features
- Multiple matching engines running on different ports to simulate a distributed exchange system
- Real-time order book synchronization(most basic logic now)
- Simple price-time priority matching
- Support for multiple trading pairs
- Random order simulation for testing

## Assumptions
- Each matching machine has an orderbook and its own synchronizer to communicate with other engines
- Price is handled with a naive event-driven sync (on fills)


## Implementation Details
- A .proto file defines the structure of the messages and services that will be used for communication. It uses Google's language-agnostic data serialization format used with gRPC(protobuf). To generate gRPC code(you don't need to):
    ```bash
    python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/matching_service.proto
    ```
- python gRPC package is outdated, make sure you install the latest version of **grpcio**, dependencies in `requirements.txt`
- Run simulator from main.py
  - adjust the number of engines, trading pairs, and other simulation parameters

## TODOs
- There seems to be some broadcasting issue after fills
- More robust syncing methods
  - Event-driven: immediate sync on fills or price changes or large orders
  - Time-based: periodic full sync to ensure consistency across all machines
  - Recovery-based: full sync when a peer reconnects or joins
- Maybe we want to implement a "Exchange Computer System 100" in Figure 1 of the patern as the master file of all machines, traders, and trading data
- Maybe we want to have a separate "shared memory" as designed in patent somewhere -> maybe a centralized cache?
- Right now updates are sending over complete orderbook, perhaps we should send incrementally
- No handling of network issue reconnect/ retry/ any recovery method
- No handling of new machine joining the network
- Latency Optimizations: 
  - parallelizing gRPC calls when broadscasting updates to different peers
  - batching multiple updates tgt instead of independently?
- Something fun fun
- Test entry


