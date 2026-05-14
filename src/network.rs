use crate::metrics;
use anyhow::Result;
use dashmap::DashMap;
use reth::primitives::{PooledTransaction, TransactionSigned};
use reth::revm::revm::primitives::B256;
use reth_eth_wire::{GetPooledTransactions, NewPooledTransactionHashes, PooledTransactions};
use reth_network::p2p::error::RequestError;
use reth_network::transactions::NetworkTransactionEvent;
use reth_network::{NetworkHandle, PeerRequest};
use reth_network_api::{
    NetworkEvent, PeerId,
    events::{PeerEvent, SessionInfo},
};
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tracing::{debug, error, info, trace, warn};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PeerSessionInfo {
    #[allow(dead_code)]
    session_info: Arc<SessionInfo>,
}

#[derive(Debug)]
pub struct EthP2PHandler {
    network_handle: NetworkHandle,
    pub peers: Arc<DashMap<PeerId, PeerSessionInfo>>,
    decoded_tx_sender: UnboundedSender<Arc<TransactionSigned>>,
}

impl EthP2PHandler {
    pub fn new(
        network_handle: NetworkHandle,
        peers: Arc<DashMap<PeerId, PeerSessionInfo>>,
        decoded_tx_sender: UnboundedSender<Arc<TransactionSigned>>,
    ) -> Self {
        info!(target: "listener::network", "Initializing EthP2PHandler.");
        Self {
            network_handle,
            peers,
            decoded_tx_sender,
        }
    }

    fn on_session_established(&self, session_info: Arc<SessionInfo>) {
        let peer_id = session_info.peer_id;
        info!(target: "listener::network", %peer_id, client=%session_info.client_version, "Session established...");

        let peer_info_struct = PeerSessionInfo {
            session_info: Arc::clone(&session_info),
        };
        self.peers.insert(peer_id, peer_info_struct);
        metrics::P2P_PEERS_CONNECTED.set(self.peers.len() as i64);

        println!("[DEBUG] EthP2PHandler: Peer added! New peer count: {}", self.peers.len());
    }

    pub async fn handle_network_event_wrapper(&self, event: NetworkEvent) -> Result<()> {
        trace!(target: "listener::handler", "Received NetworkEvent: {:?}", event);

        match event {
            NetworkEvent::Peer(peer_event) => {
                self.handle_peer_event(peer_event).await?;
            }
            NetworkEvent::ActivePeerSession { info, .. } => {
                self.on_session_established(info.into());
            }
        }
        Ok(())
    }

    pub async fn handle_peer_event(&self, event: PeerEvent) -> Result<()> {
        match event {
            PeerEvent::SessionEstablished(session_info) => {
                self.on_session_established(Arc::new(session_info));
            }
            PeerEvent::SessionClosed { peer_id, reason } => {
                info!(target: "listener::network", %peer_id, ?reason, "Session closed");
                self.peers.remove(&peer_id);
                metrics::P2P_PEERS_CONNECTED.set(self.peers.len() as i64);
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn handle_transaction_event(&self, event: NetworkTransactionEvent) -> Result<()> {
        match event {
            NetworkTransactionEvent::IncomingTransactions { peer_id, msg } => {
                let signed_transactions = msg.0;
                info!(target: "listener::mempool", %peer_id, count = signed_transactions.len(), "Received full SIGNED transactions broadcast");
                let sender_clone = self.decoded_tx_sender.clone();
                for tx_signed_arc in signed_transactions.into_iter() {
                    trace!(target: "listener::tx", tx_hash=%tx_signed_arc.hash(), "Processing directly received TransactionSigned.");
                    let tx_to_send = tx_signed_arc.clone();
                    if let Err(e) = sender_clone.send(tx_to_send.into()) {
                        metrics::PIPELINE_ERRORS_TOTAL
                            .with_label_values(&["tx_forward"])
                            .inc();
                        error!(target: "listener::tx", %peer_id, "Failed to send DIRECTLY received TransactionSigned: {}. Receiver likely dropped.", e);
                    } else {
                        debug!(target: "listener::tx", %peer_id, tx_hash=%tx_signed_arc.hash(), "Successfully forwarded DIRECTLY received TransactionSigned to processor task.");
                    }
                }
            }
            NetworkTransactionEvent::IncomingPooledTransactionHashes { peer_id, msg } => {
                let hashes: Vec<B256> = match msg {
                    NewPooledTransactionHashes::Eth66(h) => h.0,
                    NewPooledTransactionHashes::Eth68(h) => h.hashes,
                };
                info!(target: "listener::mempool", %peer_id, count = hashes.len(), "Received transaction hashes broadcast");
                if !hashes.is_empty() {
                    let request_payload = GetPooledTransactions(hashes.clone());
                    let (response_tx, response_rx) =
                        oneshot::channel::<Result<PooledTransactions, RequestError>>();
                    let peer_request = PeerRequest::GetPooledTransactions {
                        request: request_payload,
                        response: response_tx,
                    };
                    self.network_handle.send_request(peer_id, peer_request);
                    let sender_clone = self.decoded_tx_sender.clone();
                    spawn(async move {
                        match response_rx.await {
                            Ok(Ok(response_msg)) => {
                                let received_pooled_txs = response_msg.0;
                                info!(target: "listener::mempool", %peer_id, count = received_pooled_txs.len(), "Received PooledTransactions RESPONSE");
                                for pooled_tx_arc in received_pooled_txs.into_iter() {
                                    let received_hash = pooled_tx_arc.hash();
                                    let pooled_tx_ref: &PooledTransaction = &pooled_tx_arc;
                                    let pooled_tx: PooledTransaction = pooled_tx_ref.clone();
                                    let tx_signed: TransactionSigned = pooled_tx.into();
                                    if tx_signed.hash() != received_hash {
                                        metrics::PIPELINE_ERRORS_TOTAL
                                            .with_label_values(&["hash_mismatch"])
                                            .inc();
                                        warn!(target: "listener::tx", received_hash=%received_hash, computed_hash=%tx_signed.hash(), "Hash mismatch on requested tx!");
                                    }
                                    let tx_signed_arc = Arc::new(tx_signed);
                                    if let Err(e) = sender_clone.send(tx_signed_arc) {
                                        error!(target: "listener::tx", %peer_id, "Failed to send REQUESTED tx: {}. Receiver likely dropped.", e);
                                    } else {
                                        debug!(target: "listener::tx", %peer_id, tx_hash=%received_hash, "Forwarded REQUESTED tx to processor.");
                                    }
                                }
                            }
                            Ok(Err(req_err)) => {
                                metrics::PIPELINE_ERRORS_TOTAL
                                    .with_label_values(&["pool_request"])
                                    .inc();
                                warn!(target: "listener::network", %peer_id, ?req_err, "GetPooledTransactions request failed")
                            }
                            Err(recv_err) => {
                                metrics::PIPELINE_ERRORS_TOTAL
                                    .with_label_values(&["pool_request"])
                                    .inc();
                                warn!(target: "listener::network", %peer_id, %recv_err, "Failed to receive GetPooledTransactions response")
                            }
                        }
                    });
                }
            }
            NetworkTransactionEvent::GetPooledTransactions {
                peer_id,
                request,
                response,
            } => {
                debug!(target: "listener::network", %peer_id, count = request.0.len(), "Ignoring incoming GetPooledTransactions request");
                let _ = response.send(Ok(PooledTransactions(vec![])));
            }
            _ => {
                debug!(target: "listener::network", "Unhandled NetworkTransactionEvent: {:?}", event);
            }
        }
        Ok(())
    }
}