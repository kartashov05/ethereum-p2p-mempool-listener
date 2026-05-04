---

# Ethereum P2P Mempool Listener

A high-performance service written in Rust that connects directly to the Ethereum peer-to-peer network and streams raw mempool transactions for further processing.

---

## Overview

This project implements a low-latency pipeline for ingesting Ethereum transactions without relying on JSON-RPC endpoints. Instead, it connects directly to the devp2p network, receives gossip transactions, performs in-memory deduplication, and forwards raw transaction data to Redis.

A minimal Python script is included as an example consumer. In practice, Redis can be consumed by any downstream system or processed further within Rust.

---

## Features

* Direct connection to Ethereum devp2p network (no RPC dependency)
* Real-time ingestion of raw EIP-2718 encoded transactions
* In-memory deduplication at the Rust layer
* Storage of full raw transactions in Redis (not just hashes)
* Simple and extensible architecture for downstream processing

---

## Architecture

```text
Ethereum P2P Network
        ↓
Rust listener (reth-based)
        ↓
In-memory deduplication
        ↓
Redis (queue: txs)
        ↓
Consumer (Python example / any language / Rust)
```

---

## Rust Component

The core service is built on top of reth and is responsible for:

* Establishing peer connections over devp2p
* Receiving mempool transactions via gossip
* Computing transaction hashes (keccak)
* Filtering duplicates using an in-memory cache
* Encoding transactions in EIP-2718 format
* Pushing raw transactions into Redis

Example:

```rust
let raw_tx = tx.encoded_2718();
conn.rpush("txs", raw_tx).await?;
```

---

## Python Consumer (Example)

A minimal Python script demonstrates how to read and decode transactions from Redis. This component is optional and serves only as an example.

* Uses Redis `BLPOP` for streaming consumption
* Decodes raw transactions using `rlp` and `eth-account`
* Can be replaced with any other language or processing pipeline

---

## Configuration

Configuration is shared via a `config.toml` file

Used by both Rust and Python components.

---

## Design Decisions

* **No RPC usage**: avoids latency and rate limits
* **Raw transaction storage**: preserves full data for flexible downstream processing
* **Rust-based deduplication**: reduces load on Redis and consumers
* **Loose coupling via Redis**: enables scalable and language-agnostic processing

---

## Use Cases

* Mempool monitoring
* Transaction analytics
* MEV research and strategy development
* Real-time contract interaction tracking
* Event-driven pipelines (e.g. Kafka, ClickHouse integration)

---

## Extensibility

The system is designed to be easily extended:

* Replace Redis with Kafka or another message broker
* Add filtering logic directly in Rust
* Implement persistent or distributed deduplication
* Support multiple peers and horizontal scaling
* Integrate with analytics or alerting systems

---

## Requirements

Before running the project, make sure you have:

- Rust (with Cargo)
- Python 3.12.8
- Redis
- Git

---

## Installation

Clone the repository:

```bash
git clone https://github.com/kartashov05/ethereum-p2p-mempool-listener.git
cd ethereum-p2p-mempool-listener
````

---

## Setup

### 1. Start Redis

```bash
redis-server
```

---

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

---

## Run

### Start the Rust mempool listener

```bash
RUST_LOG=info cargo run
```

This will connect to Ethereum peers, receive transactions, deduplicate them, and push raw transactions to Redis.

---

### Start the Python consumer (example)

In a separate terminal:

```bash
python src/reader.py
```

---

## Notes

* The Python script is provided for demonstration purposes only
* The pipeline can be fully implemented in Rust if needed
* Raw transactions are stored in EIP-2718 format and can be decoded by standard Ethereum tooling

---

## Acknowledgements

This project was inspired by the article:
https://medium.com/@suyashnyn1/observing-ethereums-mempool-directly-with-reth-d404919cae79

The implementation was independently developed and adapted for a different architecture and use case.

---
