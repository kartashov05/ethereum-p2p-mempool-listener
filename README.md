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
* Prometheus-compatible metrics for P2P connectivity, ingestion throughput, deduplication, Redis latency, and queue backpressure

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
* Exposing runtime metrics for observability

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

Configuration is shared via a `config.toml` file.

Used by both Rust and Python components.

Example:

```toml
p2p_listen_addr = "0.0.0.0:30313"
discv4_listen_addr = "0.0.0.0:30314"

max_peers_outbound = 32
max_peers_inbound = 16

redis_url = "redis://127.0.0.1:6379/0"

metrics_listen_addr = "0.0.0.0:9100"
```

---

## Design Decisions

* **No RPC usage**: avoids latency and rate limits
* **Raw transaction storage**: preserves full data for flexible downstream processing
* **Rust-based deduplication**: reduces load on Redis and consumers
* **Loose coupling via Redis**: enables scalable and language-agnostic processing
* **Observable ingestion pipeline**: exposes Prometheus metrics for peer connectivity, transaction throughput, deduplication efficiency, Redis delivery latency, and downstream backpressure

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
* Optionally add Prometheus scraping, Grafana dashboards, or alerting rules

---

## Requirements

Before running the project, make sure you have:

* Rust with Cargo
* Python 3.12.8
* Redis
* Git

---

## Installation

Clone the repository:

```bash
git clone https://github.com/kartashov05/ethereum-p2p-mempool-listener.git
cd ethereum-p2p-mempool-listener
```

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

### Start the Python consumer example

In a separate terminal:

```bash
python src/reader.py
```

---

## Observability

The listener exposes Prometheus-compatible metrics for monitoring the full ingestion pipeline:

```bash
curl http://127.0.0.1:9100/metrics
```

The metrics endpoint is configurable via `config.toml`:

```toml
metrics_listen_addr = "0.0.0.0:9100"
```

These metrics make the service observable across the full path:

```text
Ethereum P2P peers
        ↓
Mempool transaction ingestion
        ↓
Rust-layer deduplication
        ↓
Redis delivery
        ↓
Downstream queue / consumer
```

---

## Metrics

| Metric | Type | Description |
|---|---|---|
| `p2p_peers_connected` | Gauge | Current number of active Ethereum P2P peer connections |
| `mempool_tx_seen_total` | Counter | Total transactions seen before deduplication |
| `mempool_tx_unique_total` | Counter | Total unique transactions after Rust-layer deduplication |
| `mempool_tx_duplicates_total` | Counter | Total duplicate transactions skipped by the deduplication layer |
| `dedup_ratio` | Gauge | Current duplicate ratio: `duplicates / seen` |
| `redis_push_total` | Counter | Total transactions successfully pushed to Redis |
| `redis_push_latency_ms` | Histogram | Redis `RPUSH` latency in milliseconds |
| `redis_queue_depth` | Gauge | Current Redis queue depth for the `txs` list |
| `pipeline_errors_total` | Counter | Processing errors grouped by pipeline stage |

---

## Example Runtime Snapshot

Example metrics from a local run:

```text
p2p_peers_connected 2
mempool_tx_seen_total 13011
mempool_tx_unique_total 13011
mempool_tx_duplicates_total 0
dedup_ratio 0
redis_push_total 13011
redis_queue_depth 2
redis_push_latency_ms_sum 891.9520009999984
redis_push_latency_ms_count 13011
```

Summary:

| Metric | Value |
|---|---:|
| Connected P2P peers | `2` |
| Transactions seen | `13,011` |
| Unique transactions | `13,011` |
| Duplicate transactions | `0` |
| Redis pushes | `13,011` |
| Redis queue depth | `2` |
| Redis latency samples | `13,011` |
| Average Redis push latency | `~0.07 ms` |

The average Redis push latency is calculated from:

```text
redis_push_latency_ms_sum / redis_push_latency_ms_count
```

Example:

```text
891.952 ms / 13,011 ≈ 0.07 ms
```

This shows that the listener is not only receiving Ethereum P2P mempool traffic, but also deduplicating transactions and delivering them to Redis with measurable latency and backpressure visibility.

---

## Optional Prometheus Queries

For dashboards, counters should usually be visualized as rates over a time window.

Recommended Prometheus scrape interval:

```yaml
scrape_interval: 5s
```

The project does not require Prometheus or Grafana. The following queries are examples for users who want to connect the `/metrics` endpoint to Prometheus later.

| Panel | PromQL |
|---|---|
| Connected peers | `p2p_peers_connected` |
| Raw ingestion rate | `rate(mempool_tx_seen_total[5m])` |
| Unique transaction throughput | `rate(mempool_tx_unique_total[5m])` |
| Duplicate transaction rate | `rate(mempool_tx_duplicates_total[5m])` |
| Deduplication ratio | `rate(mempool_tx_duplicates_total[5m]) / rate(mempool_tx_seen_total[5m])` |
| Redis delivery rate | `rate(redis_push_total[5m])` |
| Redis p95 push latency | `histogram_quantile(0.95, rate(redis_push_latency_ms_bucket[5m]))` |
| Redis queue depth | `redis_queue_depth` |
| Processing errors by stage | `sum by (stage) (rate(pipeline_errors_total[5m]))` |

For local demos, the `[5m]` window can be replaced with `[1m]` to make metric changes visible faster.

---

## Notes

* The Python script is provided for demonstration purposes only
* The pipeline can be fully implemented in Rust if needed
* Raw transactions are stored in EIP-2718 format and can be decoded by standard Ethereum tooling
* P2P peer counts can fluctuate because Ethereum peers may disconnect due to peer limits, usefulness scoring, timeouts, or normal network churn
* `redis_queue_depth` is useful for detecting downstream backpressure: if the value grows continuously, the consumer is not keeping up with ingestion

---

## Acknowledgements

This project was inspired by the article:

https://medium.com/@suyashnyn1/observing-ethereums-mempool-directly-with-reth-d404919cae79

The implementation was independently developed and adapted for a different architecture and use case.

---