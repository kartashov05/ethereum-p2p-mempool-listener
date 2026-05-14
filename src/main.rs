mod config;
mod network;
mod metrics;
use crate::{
    config::{load_config, load_or_generate_key, parse_bootnodes},
    network::EthP2PHandler,
};
use dashmap::DashMap;
use anyhow::Result;
use futures_util::StreamExt;
use reth::chainspec::{ChainSpec, MAINNET};
use reth::network::transactions::NetworkTransactionEvent;
use reth::revm::revm::primitives::alloy_primitives::{B256, B512};
use reth_discv4::{Discv4ConfigBuilder, NatResolver, NodeRecord};
use reth_network::{
    EthNetworkPrimitives, NetworkConfigBuilder, NetworkEventListenerProvider, NetworkManager,
    PeersConfig, PeersInfo, config::SecretKey as RethSecretKey,
};
use reth_network_api::PeerId;
use reth_primitives::TransactionSigned;
use reth_provider::noop::NoopProvider;
use reth_tasks::TaskManager;
use secp256k1::Secp256k1;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{
    signal,
    sync::{RwLock, mpsc::{self}},
};
use tracing::{error, info, trace, warn};
use redis::AsyncCommands;
use reth_eth_wire::Encodable2718;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let app_config = load_config()?;
    metrics::init();
    info!("Starting Ethereum P2P Mempool Listener");
    println!("Loaded configuration: {:?}", app_config);

    let redis_client = redis::Client::open(app_config.redis_url.clone())?;
    let secret_key: RethSecretKey = load_or_generate_key(app_config.node_key_file.clone())?;
    let secp = Secp256k1::new();
    let public_key = secret_key.public_key(&secp);
    let serialized_pk_bytes = public_key.serialize_uncompressed();
    let our_peer_id: PeerId = B512::from_slice(&serialized_pk_bytes[1..65]);
    info!("Our Peer ID: {}", our_peer_id);

    let chain_spec: Arc<ChainSpec> = MAINNET.clone();
    info!("Using Chain Spec: {}", chain_spec.chain);

    let bootnodes: Vec<NodeRecord> = parse_bootnodes(app_config.bootnodes.clone())?;
    if bootnodes.is_empty() {
        warn!("No bootnodes specified or found! Peer discovery might fail.");
    } else {
        info!("Using {} bootnodes", bootnodes.len());
    }

    let tokio_handle = tokio::runtime::Handle::current();
    let task_manager = TaskManager::new(tokio_handle);
    let executor = task_manager.executor();
    info!("Task executor created.");

    let (tx_event_sender, mut tx_event_receiver) =
        mpsc::unbounded_channel::<NetworkTransactionEvent>();
    let (decoded_tx_sender, mut decoded_tx_receiver) =
        mpsc::unbounded_channel::<Arc<TransactionSigned>>();

    // Rust-level duplicate filter: tx hash is used only as an in-memory key.
    // Redis receives the full encoded transaction bytes, not the hash.
    let public_tx_cache = Arc::new(RwLock::new(HashMap::<B256, Instant>::new()));
    let public_tx_order = Arc::new(RwLock::new(VecDeque::<B256>::new()));
    let peers = Arc::new(DashMap::new());

    let mut discv4_builder = Discv4ConfigBuilder::default();
    discv4_builder.add_boot_nodes(bootnodes.clone());
    info!("Discv4 behaviour configured.");

    let peers_config = PeersConfig::default()
        .with_max_outbound(app_config.max_peers_outbound)
        .with_max_inbound(app_config.max_peers_inbound);

    let config_builder: NetworkConfigBuilder<EthNetworkPrimitives> =
        NetworkConfigBuilder::new(secret_key)
            .listener_addr(app_config.p2p_listen_addr)
            .discovery_addr(app_config.discv4_listen_addr)
            .discovery(discv4_builder)
            .boot_nodes(bootnodes)
            .add_nat(Some(NatResolver::Upnp))
            .peer_config(peers_config);

    let client = NoopProvider::<ChainSpec>::new(chain_spec.clone());
    let network_config = config_builder.build(client);

    info!(
        "Network configured. RLPx TCP listening on {}. Discovery UDP listening on {}. Attempting UPnP NAT.",
        app_config.p2p_listen_addr, app_config.discv4_listen_addr
    );

    let mut network_manager = NetworkManager::new(network_config).await?;
    network_manager.set_transactions(tx_event_sender);
    let network_handle = network_manager.handle().clone();
    info!(
        "Network Manager created. Initial peer count: {}",
        network_handle.num_connected_peers()
    );

    let mut events = network_handle.event_listener();

    let event_handler = EthP2PHandler::new(
        network_handle.clone(),
        peers.clone(),
        decoded_tx_sender.clone(),
    );
    let handler_arc = Arc::new(event_handler);

    let task_executor = &executor;

    let metrics_addr = app_config.metrics_listen_addr;

    task_executor.spawn(Box::pin(async move {
        info!(
            target: "listener::metrics",
            %metrics_addr,
            "Starting Prometheus metrics endpoint"
        );

        if let Err(err) = metrics::serve(metrics_addr).await {
            error!(
                target: "listener::metrics",
                "Metrics endpoint failed: {err}"
            );
        }
    }));

    let handler_clone_for_events = Arc::clone(&handler_arc);
    task_executor.spawn(Box::pin(async move {
        info!(target: "listener::events", "EVENT HANDLER TASK STARTED");
        loop {
            if let Some(event) = events.next().await {
                trace!(target: "listener::events", ?event, "Received network event object.");
                if let Err(e) = handler_clone_for_events
                    .handle_network_event_wrapper(event)
                    .await
                {
                    error!(target: "listener::events", "Error handling network event: {}", e);
                }
            } else {
                warn!(target: "listener::events", "Network event stream finished unexpectedly!");
                break;
            }
        }
    }));
    info!("Spawned Peer Event Handler task.");

    let handler_clone_for_tx = Arc::clone(&handler_arc);
    task_executor.spawn(Box::pin(async move {
        info!(target: "listener::tx", "TX HANDLER TASK STARTED");
        loop {
            if let Some(event) = tx_event_receiver.recv().await {
                if let Err(e) = handler_clone_for_tx.handle_transaction_event(event).await {
                    error!(target: "listener::tx", "Error handling transaction event: {}", e);
                }
            } else {
                warn!(target: "listener::tx", "Transaction event stream finished unexpectedly!");
                break;
            }
        }
    }));
    info!("Spawned Transaction Event Handler task.");

    let conn = redis_client.get_multiplexed_async_connection().await?;

    let cache_clone = public_tx_cache.clone();
    let order_clone = public_tx_order.clone();
    task_executor.spawn(Box::pin(async move {
        let mut conn = conn;
        const TX_DEDUP_TTL: Duration = Duration::from_secs(10 * 60);
        const TX_DEDUP_MAX: usize = 500_000;

        info!(target: "listener::processor", "Starting decoded transaction processor task...");
        while let Some(tx_signed_arc) = decoded_tx_receiver.recv().await {
            metrics::MEMPOOL_TX_SEEN_TOTAL.inc();

            let tx = tx_signed_arc.as_ref();
            let hash = *tx.hash();
            let now = Instant::now();

            // Skip duplicates before any Redis write.
            {
                let mut cache = cache_clone.write().await;
                if cache.contains_key(&hash) {
                    metrics::MEMPOOL_TX_DUPLICATES_TOTAL.inc();
                    metrics::update_dedup_ratio();
                    trace!(target: "listener::processor", tx_hash=%hash, "Duplicate tx skipped in Rust");
                    continue;
                }

                cache.insert(hash, now);
                metrics::MEMPOOL_TX_UNIQUE_TOTAL.inc();
                metrics::update_dedup_ratio();

                let mut order = order_clone.write().await;
                order.push_back(hash);

                // Keep the in-memory dedup cache bounded by size and time.
                while order.len() > TX_DEDUP_MAX {
                    if let Some(old_hash) = order.pop_front() {
                        cache.remove(&old_hash);
                    }
                }
                while let Some(old_hash) = order.front().copied() {
                    let expired = cache
                        .get(&old_hash)
                        .map(|seen_at| seen_at.elapsed() > TX_DEDUP_TTL)
                        .unwrap_or(true);
                    if !expired {
                        break;
                    }
                    order.pop_front();
                    cache.remove(&old_hash);
                }
            }

            let raw_tx = tx.encoded_2718();
            let redis_push_started = Instant::now();

            match conn.rpush::<_, _, ()>("txs", raw_tx).await {
                Ok(()) => {
                    metrics::REDIS_PUSH_TOTAL.inc();

                    metrics::REDIS_PUSH_LATENCY_MS.observe(
                        redis_push_started.elapsed().as_secs_f64() * 1000.0
                    );

                    match conn.llen::<_, i64>("txs").await {
                        Ok(depth) => {
                            metrics::REDIS_QUEUE_DEPTH.set(depth);
                        }
                        Err(err) => {
                            metrics::PIPELINE_ERRORS_TOTAL
                                .with_label_values(&["redis_queue_depth"])
                                .inc();

                            warn!(
                                target: "listener::processor",
                                "Redis LLEN txs error: {err}"
                            );
                        }
                    }
                }
                Err(err) => {
                    metrics::PIPELINE_ERRORS_TOTAL
                        .with_label_values(&["redis_push"])
                        .inc();

                    error!(
                        target: "listener::processor",
                        "Redis RPUSH tx error: {err}"
                    );
                }
            }
        }
    }));
    info!("Spawned Decoded Transaction Processor task.");

    let network_manager_handle = task_executor.spawn(Box::pin(async move {
        info!(target: "listener::netmgr", "Starting core network task...");
        network_manager.await;
        error!(target: "listener::netmgr", "Core network task finished unexpectedly!");
    }));
    info!("Spawned Core Network task.");

    signal::ctrl_c().await?;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    drop(task_manager);

    let _ = tokio::join!(network_manager_handle);
    Ok(())
}