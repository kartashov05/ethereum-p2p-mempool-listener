use std::net::SocketAddr;

use axum::{
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use once_cell::sync::Lazy;
use prometheus::{
    default_registry, Encoder, Gauge, Histogram, HistogramOpts, IntCounter, IntCounterVec,
    IntGauge, TextEncoder,
    register_gauge, register_int_counter, register_int_counter_vec, register_int_gauge,
};

pub static P2P_PEERS_CONNECTED: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "p2p_peers_connected",
        "Number of currently connected Ethereum P2P peers."
    )
    .unwrap()
});

pub static MEMPOOL_TX_SEEN_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "mempool_tx_seen_total",
        "Total number of mempool transactions seen before deduplication."
    )
    .unwrap()
});

pub static MEMPOOL_TX_UNIQUE_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "mempool_tx_unique_total",
        "Total number of unique mempool transactions after Rust-layer deduplication."
    )
    .unwrap()
});

pub static MEMPOOL_TX_DUPLICATES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "mempool_tx_duplicates_total",
        "Total number of duplicate mempool transactions skipped by Rust-layer deduplication."
    )
    .unwrap()
});

pub static DEDUP_RATIO: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "dedup_ratio",
        "Current duplicate ratio: mempool_tx_duplicates_total / mempool_tx_seen_total."
    )
    .unwrap()
});

pub static REDIS_PUSH_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "redis_push_total",
        "Total number of transactions successfully pushed to Redis."
    )
    .unwrap()
});

pub static REDIS_PUSH_LATENCY_MS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "redis_push_latency_ms",
        "Redis RPUSH latency in milliseconds from ingestion processor to downstream queue.",
    )
    .buckets(vec![
        0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
    ]);

    let histogram = Histogram::with_opts(opts).unwrap();
    default_registry()
        .register(Box::new(histogram.clone()))
        .unwrap();
    histogram
});

pub static REDIS_QUEUE_DEPTH: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "redis_queue_depth",
        "Current Redis queue depth for the txs list."
    )
    .unwrap()
});

pub static PIPELINE_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pipeline_errors_total",
        "Total number of transaction decode, encode, or processing errors.",
        &["stage"]
    )
    .unwrap()
});

pub fn init() {
    Lazy::force(&P2P_PEERS_CONNECTED);
    Lazy::force(&MEMPOOL_TX_SEEN_TOTAL);
    Lazy::force(&MEMPOOL_TX_UNIQUE_TOTAL);
    Lazy::force(&MEMPOOL_TX_DUPLICATES_TOTAL);
    Lazy::force(&DEDUP_RATIO);
    Lazy::force(&REDIS_PUSH_TOTAL);
    Lazy::force(&REDIS_PUSH_LATENCY_MS);
    Lazy::force(&REDIS_QUEUE_DEPTH);
    Lazy::force(&PIPELINE_ERRORS_TOTAL);

    PIPELINE_ERRORS_TOTAL.with_label_values(&["tx_forward"]);
    PIPELINE_ERRORS_TOTAL.with_label_values(&["pool_request"]);
    PIPELINE_ERRORS_TOTAL.with_label_values(&["hash_mismatch"]);
    PIPELINE_ERRORS_TOTAL.with_label_values(&["redis_push"]);
    PIPELINE_ERRORS_TOTAL.with_label_values(&["redis_queue_depth"]);
}

pub fn update_dedup_ratio() {
    let seen = MEMPOOL_TX_SEEN_TOTAL.get();

    if seen == 0 {
        DEDUP_RATIO.set(0.0);
        return;
    }

    let duplicates = MEMPOOL_TX_DUPLICATES_TOTAL.get();
    DEDUP_RATIO.set(duplicates as f64 / seen as f64);
}

async fn metrics_handler() -> Response {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();

    let mut buffer = Vec::new();

    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
            );

            (headers, buffer).into_response()
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

pub async fn serve(addr: SocketAddr) -> anyhow::Result<()> {
    let app = Router::new().route("/metrics", get(metrics_handler));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}