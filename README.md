# Distributed Order Matching Engine
A simple distributed order matching system that demonstrates how multiple matching engines can synchronize order books across different nodes.

Final Project for CS512 Distsys 2024 Fall

## Project Structure
512Project/
├── proto/
│   └── matching_service.proto
├── common/
│   ├── __init__.py
│   ├── order.py
│   └── orderbook.py
├── engine/
│   ├── __init__.py
│   ├── match_engine.py
│   └── synchronizer.py
├── network/
│   ├── __init__.py
│   └── grpc_server.py
└── main.py

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
- A .proto file defines the structure of the messages and services that will be used for communication. It uses Google's language-agnostic data serialization format used with gRPC(protobuf). To generate gRPC code:
    ```bash
    python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/matching_service.proto
    ```
- gRPC package is outdated, make sure you install the latest version of grpcio, see requirements.txt
- Run simulator from main.py

## TODOs
- More robust syncing methods
  - Event-driven: immediate sync on fills
  - Time-based: periodic full sync to ensure consistency across all machines
  - Recovery-based: full sync when a peer reconnects or joins
- Maybe we want to implement a "Exchange Computer System 100" in Figure 1 of the patern as the master file of all machines, traders, and trading data
- No handling of network issue reconnect/ any recovery method
- No handling of new machine joining the network
- Something fun fun
