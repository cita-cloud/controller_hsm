// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use prost::Message;
use std::{cmp::Ordering, sync::Arc, time::Duration};
use tokio::{sync::RwLock, time};

use cita_cloud_proto::{
    blockchain::{Block, CompactBlock, RawTransaction, RawTransactions},
    client::NetworkClientTrait,
    common::{
        Address, ConsensusConfiguration, Hash, Hashes, NodeNetInfo, NodeStatus, PeerStatus, Proof,
        ProposalInner, StateRoot,
    },
    controller::{BlockNumber, CrossChainProof},
    network::NetworkMsg,
    status_code::StatusCodeEnum,
    storage::Regions,
};
use cloud_util::{
    clean_0x,
    common::{get_tx_hash, h160_address_check},
    storage::load_data,
    unix_now,
};

use crate::{
    config::ControllerConfig,
    core::{
        auditor::Auditor,
        chain::Chain,
        genesis::GenesisBlock,
        pool::{Pool, PoolError},
        system_config::SystemConfig,
    },
    crypto::{crypto_check_async, crypto_check_batch_async, hash_data},
    grpc_client::{
        consensus::reconfigure,
        executor::get_receipt_proof,
        network::{get_network_status, get_peers_info},
        network_client,
        storage::{
            db_get_tx, get_compact_block, get_full_block, get_height_by_block_hash, get_proof,
            get_state_root, load_tx_info, store_data,
        },
        storage_client,
    },
    inner_health_check::InnerHealthCheck,
    protocol::{
        controller_msg_type::ControllerMsgType,
        node_manager::{
            chain_status_respond::Respond, ChainStatus, ChainStatusInit, ChainStatusRespond,
            NodeAddress, NodeManager,
        },
        sync_manager::{
            sync_block_respond, SyncBlockRequest, SyncBlockRespond, SyncBlocks, SyncManager,
            SyncTxRequest, SyncTxRespond,
        },
    },
    util::*,
    {impl_broadcast, impl_unicast},
};

use super::state_machine::{Event, State, Superstate};

#[derive(Clone)]
pub struct Controller {
    pub(crate) config: ControllerConfig,

    pub(crate) auditor: Arc<RwLock<Auditor>>,

    pool: Arc<RwLock<Pool>>,

    pub(crate) chain: Arc<RwLock<Chain>>,

    pub(crate) local_address: Address,

    pub(crate) validator_address: Address,

    current_status: Arc<RwLock<ChainStatus>>,

    global_status: Arc<RwLock<(NodeAddress, ChainStatus)>>,

    pub(crate) node_manager: NodeManager,

    pub(crate) sync_manager: SyncManager,

    pub(crate) event_sender: flume::Sender<Event>,

    pub(crate) forward_pool: Arc<RwLock<RawTransactions>>,

    pub initial_sys_config: SystemConfig,

    pub init_block_number: u64,

    pub private_key_path: String,

    pub inner_health_check: Arc<RwLock<InnerHealthCheck>>,
}

impl Controller {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        config: ControllerConfig,
        current_block_number: u64,
        current_block_hash: Vec<u8>,
        sys_config: SystemConfig,
        genesis: GenesisBlock,
        event_sender: flume::Sender<Event>,
        initial_sys_config: SystemConfig,
        private_key_path: String,
    ) -> Self {
        let node_address = hex::decode(clean_0x(&config.node_address)).unwrap();
        info!("node address: {}", &config.node_address);
        //
        let validator_address = hex::decode(&clean_0x(&config.validator_address)[..40]).unwrap();
        info!("validator address: {}", &config.validator_address[..40]);

        h160_address_check(Some(&Address {
            address: node_address.clone(),
        }))
        .unwrap();

        let own_status = ChainStatus {
            version: sys_config.version,
            chain_id: sys_config.chain_id.clone(),
            height: 0,
            hash: Some(Hash {
                hash: current_block_hash.clone(),
            }),
            address: Some(Address {
                address: node_address.clone(),
            }),
        };

        let pool = Arc::new(RwLock::new(Pool::new(
            sys_config.block_limit,
            sys_config.quota_limit,
        )));
        let auditor = Arc::new(RwLock::new(Auditor::new(sys_config)));
        let chain = Arc::new(RwLock::new(Chain::new(
            current_block_number,
            current_block_hash,
            pool.clone(),
            auditor.clone(),
            genesis,
        )));

        Controller {
            sync_manager: SyncManager::new(&config),
            node_manager: NodeManager::default(),

            config,
            auditor,
            pool,
            chain,
            local_address: Address {
                address: node_address,
            },
            validator_address: Address {
                address: validator_address,
            },
            current_status: Arc::new(RwLock::new(own_status)),
            global_status: Arc::new(RwLock::new((NodeAddress(0), ChainStatus::default()))),
            event_sender,
            forward_pool: Arc::new(RwLock::new(RawTransactions { body: vec![] })),
            initial_sys_config,
            init_block_number: current_block_number,
            private_key_path,
            inner_health_check: Arc::new(RwLock::new(InnerHealthCheck::default())),
        }
    }

    pub async fn init(&self, init_block_number: u64, sys_config: SystemConfig) {
        let sys_config_clone = sys_config.clone();
        let consensus_config = ConsensusConfiguration {
            height: init_block_number,
            block_interval: sys_config_clone.block_interval,
            validators: sys_config_clone.validators,
        };
        {
            let chain = self.chain.read().await;
            chain
                .init(init_block_number, self.config.server_retry_interval)
                .await;
            chain.init_auditor(init_block_number).await;
            let status = self
                .init_status(init_block_number, sys_config)
                .await
                .unwrap();
            self.set_status(status.clone()).await;

            if self.config.tx_persistence {
                self.pool.write().await.init(self.auditor.clone()).await;
            }
        }
        // send configuration to consensus
        let mut server_retry_interval =
            time::interval(Duration::from_secs(self.config.server_retry_interval));
        // don't block at here
        tokio::spawn(async move {
            loop {
                server_retry_interval.tick().await;
                {
                    if reconfigure(consensus_config.clone())
                        .await
                        .is_success()
                        .is_ok()
                    {
                        info!("consensus service ready");
                        break;
                    } else {
                        warn!("consensus service not ready: retrying...")
                    }
                }
            }
        });
    }

    pub async fn rpc_get_block_number(&self, _is_pending: bool) -> Result<u64, String> {
        let block_number = self.get_status().await.height;
        Ok(block_number)
    }

    pub async fn rpc_send_raw_transaction(
        &self,
        raw_tx: RawTransaction,
        broadcast: bool,
    ) -> Result<Vec<u8>, StatusCodeEnum> {
        let tx_hash = get_tx_hash(&raw_tx)?.to_vec();

        {
            let auditor = self.auditor.read().await;
            auditor.auditor_check(&raw_tx)?;
        }

        crypto_check_async(Arc::new(raw_tx.clone())).await?;

        let res = {
            let mut pool = self.pool.write().await;
            pool.insert(raw_tx.clone())
        };
        match res {
            Ok(_) => {
                if broadcast {
                    let mut f_pool = self.forward_pool.write().await;
                    f_pool.body.push(raw_tx.clone());
                    if f_pool.body.len() > self.config.count_per_batch {
                        self.broadcast_send_txs(f_pool.clone()).await;
                        f_pool.body.clear();
                    }
                }
                // send to storage
                if self.config.tx_persistence {
                    tokio::spawn(async move {
                        let raw_txs = RawTransactions { body: vec![raw_tx] };
                        let mut raw_tx_bytes = Vec::new();
                        match raw_txs.encode(&mut raw_tx_bytes) {
                            Ok(_) => {
                                if store_data(
                                    Regions::TransactionsPool as u32,
                                    vec![0; 8],
                                    raw_tx_bytes,
                                )
                                .await
                                .is_success()
                                .is_err()
                                {
                                    warn!("store raw tx failed");
                                }
                            }
                            Err(_) => warn!("encode raw tx failed"),
                        }
                    });
                }
                Ok(tx_hash)
            }
            Err(e) => {
                warn!(
                    "rpc send raw transaction failed: {e:?}. hash: 0x{}",
                    hex::encode(&tx_hash)
                );
                match e {
                    PoolError::TooManyRequests => Err(StatusCodeEnum::TooManyRequests),
                    PoolError::DupTransaction => Err(StatusCodeEnum::DupTransaction),
                }
            }
        }
    }

    pub async fn batch_transactions(
        &self,
        raw_txs: RawTransactions,
        broadcast: bool,
    ) -> Result<Hashes, StatusCodeEnum> {
        crypto_check_batch_async(Arc::new(raw_txs.clone())).await?;

        let mut hashes = Vec::new();
        {
            let auditor = self.auditor.read().await;
            let mut pool = self.pool.write().await;
            auditor.auditor_check_batch(&raw_txs)?;
            for raw_tx in raw_txs.body.clone() {
                let hash = get_tx_hash(&raw_tx)?.to_vec();
                if pool.insert(raw_tx).is_ok() {
                    hashes.push(Hash { hash })
                }
            }
        }
        if broadcast && !hashes.is_empty() {
            self.broadcast_send_txs(raw_txs.clone()).await;
        }
        // send to storage
        if self.config.tx_persistence {
            tokio::spawn(async move {
                let mut raw_tx_bytes = Vec::new();
                match raw_txs.encode(&mut raw_tx_bytes) {
                    Ok(_) => {
                        if store_data(Regions::TransactionsPool as u32, vec![0; 8], raw_tx_bytes)
                            .await
                            .is_success()
                            .is_err()
                        {
                            warn!("store raw tx failed");
                        }
                    }
                    Err(_) => warn!("encode raw tx failed"),
                }
            });
        }

        Ok(Hashes { hashes })
    }

    pub async fn rpc_get_block_hash(&self, block_number: u64) -> Result<Vec<u8>, StatusCodeEnum> {
        load_data(
            storage_client(),
            Regions::BlockHash as u32,
            block_number.to_be_bytes().to_vec(),
        )
        .await
        .map_err(|e| {
            warn!(
                "rpc get block({}) hash failed: {}",
                block_number,
                e.to_string()
            );
            StatusCodeEnum::NoBlockHeight
        })
    }

    pub async fn rpc_get_tx_block_number(&self, tx_hash: Vec<u8>) -> Result<u64, StatusCodeEnum> {
        load_tx_info(&tx_hash).await.map(|t| t.0)
    }

    pub async fn rpc_get_tx_index(&self, tx_hash: Vec<u8>) -> Result<u64, StatusCodeEnum> {
        load_tx_info(&tx_hash).await.map(|t| t.1)
    }

    pub async fn rpc_get_height_by_hash(
        &self,
        hash: Vec<u8>,
    ) -> Result<BlockNumber, StatusCodeEnum> {
        get_height_by_block_hash(hash).await
    }

    pub async fn rpc_get_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<CompactBlock, StatusCodeEnum> {
        get_compact_block(block_number).await
    }

    pub async fn rpc_get_block_by_hash(
        &self,
        hash: Vec<u8>,
    ) -> Result<CompactBlock, StatusCodeEnum> {
        let block_number = load_data(
            storage_client(),
            Regions::BlockHash2blockHeight as u32,
            hash.clone(),
        )
        .await
        .map_err(|e| {
            warn!(
                "rpc get block height failed: {}. hash: 0x{}",
                e.to_string(),
                hex::encode(&hash),
            );
            StatusCodeEnum::NoBlockHeight
        })
        .map(u64_decode)?;
        self.rpc_get_block_by_number(block_number).await
    }

    pub async fn rpc_get_state_root_by_number(
        &self,
        block_number: u64,
    ) -> Result<StateRoot, StatusCodeEnum> {
        get_state_root(block_number).await
    }

    pub async fn rpc_get_proof_by_number(
        &self,
        block_number: u64,
    ) -> Result<Proof, StatusCodeEnum> {
        get_proof(block_number).await
    }

    pub async fn rpc_get_block_detail_by_number(
        &self,
        block_number: u64,
    ) -> Result<Block, StatusCodeEnum> {
        get_full_block(block_number).await
    }

    pub async fn rpc_get_transaction(
        &self,
        tx_hash: Vec<u8>,
    ) -> Result<RawTransaction, StatusCodeEnum> {
        match db_get_tx(&tx_hash).await {
            Ok(tx) => Ok(tx),
            Err(e) => {
                let pool = self.pool.read().await;
                match pool.pool_get_tx(&tx_hash) {
                    Some(tx) => Ok(tx),
                    None => Err(e),
                }
            }
        }
    }

    pub async fn rpc_get_system_config(&self) -> Result<SystemConfig, StatusCodeEnum> {
        let auditor = self.auditor.read().await;
        let sys_config = auditor.get_system_config();
        Ok(sys_config)
    }

    pub async fn rpc_add_node(&self, info: NodeNetInfo) -> cita_cloud_proto::common::StatusCode {
        let res = network_client().add_node(info).await.unwrap_or_else(|e| {
            warn!("rpc add node failed: {}", e.to_string());
            StatusCodeEnum::NetworkServerNotReady.into()
        });

        let controller_for_add = self.clone();
        let code = StatusCodeEnum::from(res.clone());
        if code == StatusCodeEnum::Success || code == StatusCodeEnum::AddExistedPeer {
            tokio::spawn(async move {
                time::sleep(Duration::from_secs(
                    controller_for_add.config.server_retry_interval,
                ))
                .await;
                let _ = controller_for_add
                    .event_sender
                    .send(Event::BroadcastCSI)
                    .map_err(|e| warn!("rpc_add_node: send broadcast csi event failed: {}", e));
            });
        }
        res
    }

    pub async fn rpc_get_node_status(&self, state: &State) -> Result<NodeStatus, StatusCodeEnum> {
        let peers_count = get_network_status().await?.peer_count;
        let peers_net_info = get_peers_info().await?;
        let mut peers_status = vec![];
        for p in peers_net_info.nodes {
            let na = NodeAddress(p.origin);
            let (address, height) = self
                .node_manager
                .nodes
                .read()
                .await
                .get(&na)
                .map_or((vec![], 0), |c| {
                    (c.address.clone().unwrap().address, c.height)
                });
            peers_status.push(PeerStatus {
                height,
                address,
                node_net_info: Some(p),
            });
        }
        let chain_status = self.get_status().await;
        let self_status = Some(PeerStatus {
            height: chain_status.height,
            address: chain_status.address.unwrap().address,
            node_net_info: None,
        });

        let node_status = NodeStatus {
            is_sync: state.matches_super(&Superstate::Sync {}),
            version: env!("CARGO_PKG_VERSION").to_string(),
            self_status,
            peers_count,
            peers_status,
            is_danger: self.config.is_danger,
            init_block_number: self.init_block_number,
        };
        Ok(node_status)
    }

    pub async fn rpc_get_cross_chain_proof(
        &self,
        hash: Hash,
    ) -> Result<CrossChainProof, StatusCodeEnum> {
        let receipt_proof = get_receipt_proof(hash).await?;
        if receipt_proof.receipt.is_empty() || receipt_proof.roots_info.is_none() {
            warn!("not get receipt or roots_info");
            return Err(StatusCodeEnum::NoReceiptProof);
        }
        let height = receipt_proof.roots_info.clone().unwrap().height;
        let compact_block = get_compact_block(height).await?;
        let sys_con = self.rpc_get_system_config().await?;
        let pre_state_root = get_state_root(height - 1).await?.state_root;
        let proposal = ProposalInner {
            pre_state_root,
            proposal: Some(compact_block),
        };
        Ok(CrossChainProof {
            version: sys_con.version,
            chain_id: sys_con.chain_id,
            proposal: Some(proposal),
            receipt_proof: Some(receipt_proof),
            proof: get_proof(height).await?.proof,
            state_root: get_state_root(height).await?.state_root,
        })
    }

    pub async fn chain_get_proposal(
        &self,
        state: &State,
    ) -> Result<(u64, Vec<u8>), StatusCodeEnum> {
        if matches!(state, State::Syncing {}) {
            return Err(StatusCodeEnum::NodeInSyncMode);
        }

        let mut chain = self.chain.write().await;
        chain
            .add_proposal(self.validator_address.address.clone())
            .await?;
        chain.get_proposal().await
    }

    pub async fn chain_check_proposal(
        &self,
        proposal_height: u64,
        data: &[u8],
    ) -> Result<(), StatusCodeEnum> {
        let proposal_inner = ProposalInner::decode(data).map_err(|_| {
            warn!("check proposal failed: decode ProposalInner failed");
            StatusCodeEnum::DecodeError
        })?;

        let block = &proposal_inner
            .proposal
            .ok_or(StatusCodeEnum::NoneProposal)?;
        let header = block
            .header
            .as_ref()
            .ok_or(StatusCodeEnum::NoneBlockHeader)?;
        let block_hash = get_block_hash(block.header.as_ref())?;
        let block_height = header.height;

        //check height is consistent
        if block_height != proposal_height {
            warn!(
                "check proposal({}) failed: proposal_height: {}, block_height: {}",
                proposal_height, proposal_height, block_height,
            );
            return Err(StatusCodeEnum::ProposalCheckError);
        }

        let ret = {
            let chain = self.chain.read().await;
            //if proposal is own, skip check_proposal
            if chain.is_own(data) && chain.is_candidate(&block_hash) {
                info!(
                    "check own proposal({}): skip check. hash: 0x{}",
                    block_height,
                    hex::encode(&block_hash)
                );
                return Ok(());
            } else {
                info!(
                    "check remote proposal({}): start check. hash: 0x{}",
                    block_height,
                    hex::encode(&block_hash)
                );
            }
            //check height
            chain.check_proposal(block_height).await
        };

        match ret {
            Ok(_) => {
                let sys_config = self.rpc_get_system_config().await?;
                let pre_height_bytes = (block_height - 1).to_be_bytes().to_vec();

                //check pre_state_root in proposal
                let pre_state_root = load_data(
                    storage_client(),
                    Regions::Result as u32,
                    pre_height_bytes.clone(),
                )
                .await?;
                if proposal_inner.pre_state_root != pre_state_root {
                    warn!(
                            "check proposal({}) failed: pre_state_root: 0x{}, local pre_state_root: 0x{}",
                            block_height,
                            hex::encode(&proposal_inner.pre_state_root),
                            hex::encode(&pre_state_root),
                        );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }

                //check proposer in block header
                let proposer = header.proposer.as_slice();
                if sys_config.validators.iter().all(|v| &v[..20] != proposer) {
                    warn!(
                        "check proposal({}) failed: proposer: {} not in validators {:?}",
                        block_height,
                        hex::encode(proposer),
                        sys_config
                            .validators
                            .iter()
                            .map(hex::encode)
                            .collect::<Vec<String>>(),
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }
                //check timestamp in block header
                let pre_compact_block_bytes = load_data(
                    storage_client(),
                    Regions::CompactBlock as u32,
                    pre_height_bytes.clone(),
                )
                .await?;
                let pre_header = CompactBlock::decode(pre_compact_block_bytes.as_slice())
                    .map_err(|_| {
                        warn!(
                            "check proposal({}) failed: decode CompactBlock failed",
                            block_height
                        );
                        StatusCodeEnum::DecodeError
                    })?
                    .header
                    .ok_or(StatusCodeEnum::NoneBlockHeader)?;
                let timestamp = header.timestamp;
                let left_bounds = pre_header.timestamp;
                let right_bounds = unix_now() + sys_config.block_interval as u64 * 1000;
                if timestamp < left_bounds || timestamp > right_bounds {
                    warn!(
                        "check proposal({}) failed: timestamp: {} must be in range of {} - {}",
                        block_height, timestamp, left_bounds, right_bounds,
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }

                //check quota and transaction_root
                let mut total_quota = 0;
                let tx_hashes = &block
                    .body
                    .as_ref()
                    .ok_or(StatusCodeEnum::NoneBlockBody)?
                    .tx_hashes;
                let tx_count = tx_hashes.len();
                let mut transaction_data = Vec::new();
                let mut miss_tx_hash_list = Vec::new();
                for tx_hash in tx_hashes {
                    if let Some(tx) = self.pool.read().await.pool_get_tx(tx_hash) {
                        total_quota += get_tx_quota(&tx)?;
                        if total_quota > sys_config.quota_limit {
                            return Err(StatusCodeEnum::QuotaUsedExceed);
                        }
                        transaction_data.extend_from_slice(tx_hash);
                    } else {
                        miss_tx_hash_list.push(tx_hash);
                    }
                }

                if !miss_tx_hash_list.is_empty() {
                    let addr = Address {
                        address: proposer.to_vec(),
                    };
                    let origin = NodeAddress::from(&addr).0;
                    for tx_hash in miss_tx_hash_list {
                        self.unicast_sync_tx(
                            origin,
                            SyncTxRequest {
                                tx_hash: tx_hash.to_vec(),
                            },
                        )
                        .await;
                    }
                    return Err(StatusCodeEnum::NoneRawTx);
                }

                let transactions_root = hash_data(&transaction_data);
                if transactions_root != header.transactions_root {
                    warn!(
                        "check proposal({}) failed: header transactions_root: {}, controller calculate: {}",
                        block_height, hex::encode(&header.transactions_root), hex::encode(&transactions_root),
                    );
                    return Err(StatusCodeEnum::ProposalCheckError);
                }

                // add remote proposal
                {
                    let mut chain = self.chain.write().await;
                    chain.add_remote_proposal(&block_hash).await;
                }
                info!(
                    "check proposal({}) success: tx count: {}, quota: {}, block_hash: 0x{}",
                    block_height,
                    tx_count,
                    total_quota,
                    hex::encode(&block_hash)
                );
            }
            Err(StatusCodeEnum::ProposalTooHigh) => {
                self.broadcast_chain_status(self.get_status().await).await;
                {
                    let mut wr = self.chain.write().await;
                    wr.clear_candidate();
                }
                let _ = self.event_sender.send(Event::TrySyncBlock).map_err(|e| {
                    warn!(
                        "rpc_get_cross_chain_proof: send Event::TrySyncBlock failed: {:?}",
                        e
                    )
                });
            }
            _ => {}
        }

        ret
    }

    #[instrument(skip_all)]
    pub async fn chain_commit_block(
        &self,
        height: u64,
        proposal: &[u8],
        proof: &[u8],
    ) -> Result<ConsensusConfiguration, StatusCodeEnum> {
        let status = self.get_status().await;

        if status.height >= height {
            let config = self.rpc_get_system_config().await?;
            return Ok(ConsensusConfiguration {
                height,
                block_interval: config.block_interval,
                validators: config.validators,
            });
        }

        let res = {
            let mut chain = self.chain.write().await;
            chain.commit_block(height, proposal, proof).await
        };

        match res {
            Ok((config, mut status)) => {
                status.address = Some(self.local_address.clone());
                self.set_status(status.clone()).await;
                self.broadcast_chain_status(status).await;
                let _ = self.event_sender.send(Event::TrySyncBlock).map_err(|e| {
                    warn!(
                        "chain_commit_block: send Event::TrySyncBlock failed: {:?}",
                        e
                    )
                });
                Ok(config)
            }
            Err(StatusCodeEnum::ProposalTooHigh) => {
                self.broadcast_chain_status(self.get_status().await).await;
                {
                    let mut wr = self.chain.write().await;
                    wr.clear_candidate();
                }
                let _ = self.event_sender.send(Event::TrySyncBlock).map_err(|e| {
                    warn!(
                        "chain_commit_block: send Event::TrySyncBlock failed: {:?}",
                        e
                    )
                });
                Err(StatusCodeEnum::ProposalTooHigh)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn process_network_msg(&self, msg: NetworkMsg) -> Result<(), StatusCodeEnum> {
        debug!("get network msg: {}", msg.r#type);
        match ControllerMsgType::from(msg.r#type.as_str()) {
            ControllerMsgType::ChainStatusInitType => {
                let chain_status_init =
                    ChainStatusInit::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process ChainStatusInitType failed: decode failed");
                        StatusCodeEnum::DecodeError
                    })?;

                let own_status = self.get_status().await;
                match chain_status_init.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            StatusCodeEnum::VersionOrIdCheckError
                            | StatusCodeEnum::HashCheckError => {
                                self.unicast_chain_status_respond(
                                    msg.origin,
                                    ChainStatusRespond {
                                        respond: Some(Respond::NotSameChain(
                                            self.local_address.clone(),
                                        )),
                                    },
                                )
                                .await;
                                let node = chain_status_init
                                    .chain_status
                                    .clone()
                                    .ok_or(StatusCodeEnum::NoneChainStatus)?
                                    .address
                                    .ok_or(StatusCodeEnum::NoProvideAddress)?;
                                self.delete_global_status(&NodeAddress::from(&node)).await;
                                self.node_manager
                                    .set_ban_node(&NodeAddress::from(&node))
                                    .await?;
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                }

                let status = chain_status_init
                    .chain_status
                    .ok_or(StatusCodeEnum::NoneChainStatus)?;
                let node = status
                    .address
                    .clone()
                    .ok_or(StatusCodeEnum::NoProvideAddress)?;
                let node_origin = NodeAddress::from(&node);

                match self
                    .node_manager
                    .set_node(&node_origin, status.clone())
                    .await
                {
                    Ok(None) => {
                        let chain_status_init = self.make_csi(own_status).await?;
                        self.unicast_chain_status_init(msg.origin, chain_status_init)
                            .await;
                    }
                    Ok(Some(_)) => {
                        if own_status.height > status.height {
                            self.unicast_chain_status(msg.origin, own_status).await;
                        }
                    }
                    Err(status_code) => {
                        if status_code == StatusCodeEnum::EarlyStatus
                            && own_status.height < status.height
                        {
                            {
                                let mut chain = self.chain.write().await;
                                chain.clear_candidate();
                            }
                            let chain_status_init = self.make_csi(own_status).await?;
                            self.unicast_chain_status_init(msg.origin, chain_status_init)
                                .await;
                            let _ = self
                                .event_sender
                                .send(Event::TryUpdateGlobalStatus(node_origin, status))
                                .map_err(|e|
                                    warn!("process_network_msg: send Event::TryUpdateGlobalStatus failed: {:?}", e)
                                );
                        }
                        return Err(status_code);
                    }
                }
                let _ = self
                    .event_sender
                    .send(Event::TryUpdateGlobalStatus(node_origin, status))
                    .map_err(|e| {
                        warn!(
                            "process_network_msg: send Event::TryUpdateGlobalStatus failed: {:?}",
                            e
                        )
                    });
            }
            ControllerMsgType::ChainStatusInitRequestType => {
                let chain_status_init = self.make_csi(self.get_status().await).await?;

                self.unicast_chain_status_init(msg.origin, chain_status_init)
                    .await;
            }
            ControllerMsgType::ChainStatusType => {
                let chain_status = ChainStatus::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process ChainStatusType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                let own_status = self.get_status().await;
                let node = chain_status.address.clone().unwrap();
                let node_origin = NodeAddress::from(&node);
                match chain_status.check(&own_status).await {
                    Ok(()) => {}
                    Err(e) => {
                        match e {
                            StatusCodeEnum::VersionOrIdCheckError
                            | StatusCodeEnum::HashCheckError => {
                                self.unicast_chain_status_respond(
                                    msg.origin,
                                    ChainStatusRespond {
                                        respond: Some(Respond::NotSameChain(
                                            self.local_address.clone(),
                                        )),
                                    },
                                )
                                .await;
                                self.delete_global_status(&node_origin).await;
                                self.node_manager.set_ban_node(&node_origin).await?;
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                }

                match self
                    .node_manager
                    .check_address_origin(&node_origin, NodeAddress(msg.origin))
                    .await
                {
                    Ok(true) => {
                        self.node_manager
                            .set_node(&node_origin, chain_status.clone())
                            .await?;
                        let _ = self
                            .event_sender
                            .send(Event::TryUpdateGlobalStatus(node_origin, chain_status))
                            .map_err(|e|
                                warn!("process_network_msg: send Event::TryUpdateGlobalStatus failed: {:?}", e)
                            );
                    }
                    // give Ok or Err for process_network_msg is same
                    Err(StatusCodeEnum::AddressOriginCheckError) | Ok(false) => {
                        self.unicast_chain_status_init_req(msg.origin, own_status)
                            .await;
                    }
                    Err(e) => return Err(e),
                }
            }

            ControllerMsgType::ChainStatusRespondType => {
                let chain_status_respond =
                    ChainStatusRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process ChainStatusRespondType failed: decode failed");
                        StatusCodeEnum::DecodeError
                    })?;

                if let Some(respond) = chain_status_respond.respond {
                    match respond {
                        Respond::NotSameChain(node) => {
                            h160_address_check(Some(&node))?;
                            let node_origin = NodeAddress::from(&node);
                            warn!(
                                "process ChainStatusRespondType failed: remote check chain_status failed: NotSameChain. ban remote node. origin: {}", node_origin
                            );
                            self.delete_global_status(&node_origin).await;
                            self.node_manager.set_ban_node(&node_origin).await?;
                        }
                    }
                }
            }

            ControllerMsgType::SyncBlockType => {
                let sync_block_request =
                    SyncBlockRequest::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process SyncBlockType failed: decode failed",);
                        StatusCodeEnum::DecodeError
                    })?;

                info!(
                    "get SyncBlockRequest: from origin: {:x}, height: {} - {}",
                    msg.origin, sync_block_request.start_height, sync_block_request.end_height
                );
                let _ = self
                    .event_sender
                    .send(Event::SyncBlockReq(sync_block_request, msg.origin))
                    .map_err(|e| {
                        warn!(
                            "process_network_msg: send Event::SyncBlockReq failed: {:?}",
                            e
                        )
                    });
            }

            ControllerMsgType::SyncBlockRespondType => {
                let sync_block_respond =
                    SyncBlockRespond::decode(msg.msg.as_slice()).map_err(|_| {
                        warn!("process SyncBlockRespondType failed: decode failed");
                        StatusCodeEnum::DecodeError
                    })?;

                let controller_clone = self.clone();

                use crate::protocol::sync_manager::sync_block_respond::Respond;

                tokio::spawn(async move {
                    match sync_block_respond.respond {
                        // todo check origin
                        Some(Respond::MissBlock(node)) => {
                            let node_origin = NodeAddress::from(&node);
                            warn!("misbehavior: MissBlock({})", node_origin);
                            controller_clone.delete_global_status(&node_origin).await;
                            let _ = controller_clone
                                .node_manager
                                .set_misbehavior_node(&node_origin)
                                .await;
                        }
                        Some(Respond::Ok(sync_blocks)) => {
                            // todo handle error
                            match controller_clone
                                .handle_sync_blocks(sync_blocks.clone())
                                .await
                            {
                                Ok(_) => {
                                    if controller_clone
                                        .sync_manager
                                        .contains_block(
                                            controller_clone.get_status().await.height + 1,
                                        )
                                        .await
                                    {
                                        let _ = controller_clone
                                            .event_sender
                                            .send(Event::SyncBlock)
                                            .map_err(|e|
                                                warn!("process_network_msg: send Event::SyncBlock failed: {:?}", e)
                                            );
                                    }
                                }
                                Err(StatusCodeEnum::ProvideAddressError)
                                | Err(StatusCodeEnum::NoProvideAddress) => {
                                    warn!(
                                        "process SyncBlockRespondType failed: message address error. origin: {:x}",
                                        msg.origin
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "process SyncBlockRespondType failed: {}. origin: {:x}",
                                        e.to_string(),
                                        msg.origin
                                    );
                                    let node = sync_blocks.address.as_ref().unwrap();
                                    let node_origin = NodeAddress::from(node);
                                    let _ = controller_clone
                                        .node_manager
                                        .set_misbehavior_node(&node_origin)
                                        .await;
                                    controller_clone.delete_global_status(&node_origin).await;
                                }
                            }
                        }
                        None => {}
                    }
                });
            }

            ControllerMsgType::SyncTxType => {
                let sync_tx = SyncTxRequest::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SyncTxType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                let controller_clone = self.clone();

                use crate::protocol::sync_manager::sync_tx_respond::Respond;
                tokio::spawn(async move {
                    if let Ok(raw_tx) = {
                        let rd = controller_clone.chain.read().await;
                        rd.chain_get_tx(&sync_tx.tx_hash).await
                    } {
                        controller_clone
                            .unicast_sync_tx_respond(
                                msg.origin,
                                SyncTxRespond {
                                    respond: Some(Respond::Ok(raw_tx)),
                                },
                            )
                            .await;
                    }
                });
            }

            ControllerMsgType::SyncTxRespondType => {
                let sync_tx_respond = SyncTxRespond::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SyncTxRespondType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                use crate::protocol::sync_manager::sync_tx_respond::Respond;
                match sync_tx_respond.respond {
                    Some(Respond::MissTx(node)) => {
                        let node_origin = NodeAddress::from(&node);
                        warn!("misbehavior: MissTx({})", node_origin);
                        self.node_manager.set_misbehavior_node(&node_origin).await?;
                        self.delete_global_status(&node_origin).await;
                    }
                    Some(Respond::Ok(raw_tx)) => {
                        self.rpc_send_raw_transaction(raw_tx, false).await?;
                    }
                    None => {}
                }
            }

            ControllerMsgType::SendTxType => {
                let send_tx = RawTransaction::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SendTxType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                self.rpc_send_raw_transaction(send_tx, false).await?;
            }

            ControllerMsgType::SendTxsType => {
                let body = RawTransactions::decode(msg.msg.as_slice()).map_err(|_| {
                    warn!("process SendTxsType failed: decode failed");
                    StatusCodeEnum::DecodeError
                })?;

                self.batch_transactions(body, true).await?;
            }

            ControllerMsgType::Noop => {
                warn!("process Noop failed: unexpected");
                self.delete_global_status(&NodeAddress(msg.origin)).await;
                self.node_manager
                    .set_ban_node(&NodeAddress(msg.origin))
                    .await?;
            }
        }

        Ok(())
    }

    impl_broadcast!(
        broadcast_chain_status_init,
        ChainStatusInit,
        "chain_status_init"
    );
    impl_broadcast!(broadcast_chain_status, ChainStatus, "chain_status");

    // impl_multicast!(multicast_chain_status, ChainStatus, "chain_status");
    // impl_multicast!(multicast_send_tx, RawTransaction, "send_tx");
    // impl_multicast!(multicast_send_txs, RawTransactions, "send_txs");
    impl_broadcast!(broadcast_send_txs, RawTransactions, "send_txs");
    // impl_multicast!(multicast_sync_tx, SyncTxRequest, "sync_tx");
    // impl_multicast!(multicast_sync_block, SyncBlockRequest, "sync_block");

    impl_unicast!(unicast_chain_status, ChainStatus, "chain_status");
    impl_unicast!(
        unicast_chain_status_init_req,
        ChainStatus,
        "chain_status_init_req"
    );
    impl_unicast!(
        unicast_chain_status_init,
        ChainStatusInit,
        "chain_status_init"
    );
    impl_unicast!(unicast_sync_block, SyncBlockRequest, "sync_block");
    impl_unicast!(
        unicast_sync_block_respond,
        SyncBlockRespond,
        "sync_block_respond"
    );
    impl_unicast!(unicast_sync_tx, SyncTxRequest, "sync_tx");
    impl_unicast!(unicast_sync_tx_respond, SyncTxRespond, "sync_tx_respond");
    impl_unicast!(
        unicast_chain_status_respond,
        ChainStatusRespond,
        "chain_status_respond"
    );

    pub async fn get_global_status(&self) -> (NodeAddress, ChainStatus) {
        let rd = self.global_status.read().await;
        rd.clone()
    }

    pub async fn update_global_status(&self, node: NodeAddress, status: ChainStatus) {
        let mut wr = self.global_status.write().await;
        *wr = (node, status);
    }

    async fn delete_global_status(&self, node: &NodeAddress) -> bool {
        let res = {
            let rd = self.global_status.read().await;
            let gs = rd.clone();
            &gs.0 == node
        };
        if res {
            let mut wr = self.global_status.write().await;
            *wr = (NodeAddress(0), ChainStatus::default());
            true
        } else {
            false
        }
    }

    pub async fn try_update_global_status(
        &self,
        node: &NodeAddress,
        status: ChainStatus,
        in_sync: bool,
    ) -> Result<bool, StatusCodeEnum> {
        let old_status = self.get_global_status().await;
        let own_status = self.get_status().await;
        let global_height = status.height;
        if global_height > old_status.1.height && global_height >= own_status.height {
            info!(
                "update global status: origin: {}, height: {}, hash: 0x{}",
                node,
                global_height,
                hex::encode(status.hash.clone().unwrap().hash)
            );
            self.update_global_status(node.to_owned(), status).await;
            if global_height > own_status.height {
                let _ = self.event_sender.send(Event::TrySyncBlock).map_err(|e| {
                    warn!(
                        "try_update_global_status: send TrySyncBlock event failed: {}",
                        e
                    )
                });
            }
            if (!in_sync || global_height % self.config.force_sync_epoch == 0)
                && self
                    .sync_manager
                    .contains_block(own_status.height + 1)
                    .await
            {
                let _ = self.event_sender.send(Event::SyncBlock).map_err(|e| {
                    warn!(
                        "try_update_global_status: send SyncBlock event failed: {}",
                        e
                    )
                });
            }

            return Ok(true);
        }

        // request block if own height behind remote's
        if global_height > own_status.height {
            let _ = self.event_sender.send(Event::TrySyncBlock).map_err(|e| {
                warn!(
                    "try_update_global_status: send TrySyncBlock event failed: {}",
                    e
                )
            });
        }

        Ok(false)
    }

    async fn init_status(
        &self,
        height: u64,
        config: SystemConfig,
    ) -> Result<ChainStatus, StatusCodeEnum> {
        let compact_block = get_compact_block(height).await?;

        Ok(ChainStatus {
            version: config.version,
            chain_id: config.chain_id,
            height,
            hash: Some(Hash {
                hash: get_block_hash(compact_block.header.as_ref())?,
            }),
            address: Some(self.local_address.clone()),
        })
    }

    pub async fn get_status(&self) -> ChainStatus {
        let rd = self.current_status.read().await;
        rd.clone()
    }

    pub async fn set_status(&self, mut status: ChainStatus) {
        if h160_address_check(status.address.as_ref()).is_err() {
            status.address = Some(self.local_address.clone())
        }

        let mut wr = self.current_status.write().await;
        *wr = status;
    }

    async fn handle_sync_blocks(&self, sync_blocks: SyncBlocks) -> Result<usize, StatusCodeEnum> {
        h160_address_check(sync_blocks.address.as_ref())?;

        let own_height = self.get_status().await.height;
        Ok(self
            .sync_manager
            .insert_blocks(
                sync_blocks
                    .address
                    .ok_or(StatusCodeEnum::NoProvideAddress)?,
                sync_blocks.sync_blocks,
                own_height,
            )
            .await)
    }

    pub async fn make_csi(
        &self,
        own_status: ChainStatus,
    ) -> Result<ChainStatusInit, StatusCodeEnum> {
        let mut chain_status_bytes = Vec::new();
        own_status.encode(&mut chain_status_bytes).map_err(|_| {
            warn!("make csi failed: encode ChainStatus failed");
            StatusCodeEnum::EncodeError
        })?;
        let msg_hash = hash_data(&chain_status_bytes);

        cfg_if::cfg_if! {
            if #[cfg(feature = "sm")] {
                let crypto = crypto_sm::crypto::Crypto::new(&self.private_key_path);
            } else if #[cfg(feature = "eth")] {
                let crypto = crypto_eth::crypto::Crypto::new(&self.private_key_path);
            }
        }

        let signature = crypto.sign_message(&msg_hash)?;

        Ok(ChainStatusInit {
            chain_status: Some(own_status),
            signature,
        })
    }

    pub async fn handle_record_all_node(&self) {
        let nodes = self.node_manager.nodes.read().await.clone();
        for (na, current_cs) in nodes.iter() {
            if let Some(old_cs) = self.node_manager.nodes_pre_status.read().await.get(na) {
                match old_cs.height.cmp(&current_cs.height) {
                    Ordering::Greater => {
                        error!(
                            "node status rollback: old height: {}, current height: {}. set it misbehavior. origin: {}",
                            old_cs.height,
                            current_cs.height,
                            &na
                        );
                        let _ = self.node_manager.set_misbehavior_node(na).await;
                    }
                    Ordering::Equal => {
                        warn!(
                            "node status stale: height: {}. delete it. origin: {}",
                            old_cs.height, &na
                        );
                        if self.node_manager.in_node(na).await {
                            self.node_manager.delete_node(na).await;
                        }
                    }
                    Ordering::Less => {
                        // update node in old status
                        self.node_manager
                            .nodes_pre_status
                            .write()
                            .await
                            .insert(*na, current_cs.clone());
                    }
                }
            } else {
                self.node_manager
                    .nodes_pre_status
                    .write()
                    .await
                    .insert(*na, current_cs.clone());
            }
        }
    }

    pub async fn handle_broadcast_csi(&self) -> Result<(), StatusCodeEnum> {
        info!("receive BroadCastCSI event");
        let status = self.get_status().await;

        let mut chain_status_bytes = Vec::new();
        status.encode(&mut chain_status_bytes).map_err(|_| {
            warn!("handle_broadcast_csi failed: encode ChainStatus failed");
            StatusCodeEnum::EncodeError
        })?;
        let msg_hash = hash_data(&chain_status_bytes);

        cfg_if::cfg_if! {
            if #[cfg(feature = "sm")] {
                let crypto = crypto_sm::crypto::Crypto::new(&self.private_key_path);
            } else if #[cfg(feature = "eth")] {
                let crypto = crypto_eth::crypto::Crypto::new(&self.private_key_path);
            }
        }

        let signature = crypto.sign_message(&msg_hash)?;

        self.broadcast_chain_status_init(ChainStatusInit {
            chain_status: Some(status),
            signature,
        })
        .await
        .await
        .map_err(|_| {
            warn!("handle_broadcast_csi: broadcast ChainStatusInit failed");
            StatusCodeEnum::FatalError
        })?;
        Ok(())
    }

    pub async fn syncing_block(&self) -> Result<(), StatusCodeEnum> {
        let (global_address, _) = self.get_global_status().await;
        let mut own_status = self.get_status().await;
        let mut syncing = false;
        {
            let mut chain = self.chain.write().await;
            while let Some((addr, block)) =
                self.sync_manager.remove_block(own_status.height + 1).await
            {
                chain.clear_candidate();
                match chain.process_block(block).await {
                    Ok((consensus_config, mut status)) => {
                        reconfigure(consensus_config).await.is_success()?;
                        status.address = Some(self.local_address.clone());
                        self.set_status(status.clone()).await;
                        own_status = status.clone();
                        if status.height % self.config.send_chain_status_interval_sync == 0 {
                            self.broadcast_chain_status(status).await;
                        }
                        syncing = true;
                    }
                    Err(e) => {
                        if (e as u64) % 100 == 0 {
                            warn!("sync block failed: {}", e.to_string());
                            continue;
                        }
                        warn!(
                            "sync block failed: {}. set remote misbehavior. origin: {}",
                            NodeAddress::from(&addr),
                            e.to_string()
                        );
                        let del_node_addr = NodeAddress::from(&addr);
                        let _ = self.node_manager.set_misbehavior_node(&del_node_addr).await;
                        if global_address == del_node_addr {
                            let (ex_addr, ex_status) = self.node_manager.pick_node().await;
                            self.update_global_status(ex_addr, ex_status).await;
                        }
                        if let Some(range_heights) =
                            self.sync_manager.clear_node_block(&addr, &own_status).await
                        {
                            let (global_address, global_status) = self.get_global_status().await;
                            if global_address.0 != 0 {
                                for range_height in range_heights {
                                    if let Some(reqs) = self
                                        .sync_manager
                                        .re_sync_block_req(range_height, &global_status)
                                    {
                                        for req in reqs {
                                            self.unicast_sync_block(global_address.0, req).await;
                                        }
                                    }
                                }
                            }
                        } else {
                            syncing = true;
                        }
                    }
                }
            }
        }
        if syncing {
            let _ = self
                .event_sender
                .send(Event::TrySyncBlock)
                .map_err(|_| warn!("syncing_block: send TrySyncBlock event failed"));
        }
        Ok(())
    }

    pub async fn handle_sync_block_req(&self, req: &SyncBlockRequest, origin: &u64) {
        let mut block_vec = Vec::new();

        for h in req.start_height..=req.end_height {
            if let Ok(block) = get_full_block(h).await {
                block_vec.push(block);
            } else {
                warn!("handle SyncBlockReq failed: get block({}) failed", h);
                break;
            }
        }

        if block_vec.len() as u64 != req.end_height - req.start_height + 1 {
            let sync_block_respond = SyncBlockRespond {
                respond: Some(sync_block_respond::Respond::MissBlock(
                    self.local_address.clone(),
                )),
            };
            self.unicast_sync_block_respond(*origin, sync_block_respond)
                .await;
        } else {
            info!(
                "send SyncBlockRespond: to origin: {:x}, height: {} - {}",
                origin, req.start_height, req.end_height
            );
            let sync_block = SyncBlocks {
                address: Some(self.local_address.clone()),
                sync_blocks: block_vec,
            };
            let sync_block_respond = SyncBlockRespond {
                respond: Some(sync_block_respond::Respond::Ok(sync_block)),
            };
            self.unicast_sync_block_respond(*origin, sync_block_respond)
                .await;
        }
    }

    pub async fn inner_health_check(&self) {
        let mut inner_health_check = self.inner_health_check.write().await;
        inner_health_check.tick += 1;
        if inner_health_check.current_height == u64::MAX {
            inner_health_check.current_height = self.get_status().await.height;
            inner_health_check.tick = 0;
        } else if self.get_status().await.height == inner_health_check.current_height
            && inner_health_check.tick >= inner_health_check.retry_limit
        {
            info!(
                "inner healthy check: broadcast csi: height: {}, {}th time",
                inner_health_check.current_height, inner_health_check.tick
            );
            if self.get_global_status().await.1.height > inner_health_check.current_height {
                self.chain.write().await.clear_candidate();
            }
            let _ = self
                .event_sender
                .send(Event::BroadcastCSI)
                .map_err(|_| warn!("inner_health_check: send BroadcastCSI event failed"));
            inner_health_check.retry_limit += inner_health_check.tick;
            inner_health_check.tick = 0;
        } else if self.get_status().await.height < inner_health_check.current_height {
            unreachable!()
        } else if self.get_status().await.height > inner_health_check.current_height {
            // update current height
            inner_health_check.current_height = self.get_status().await.height;
            inner_health_check.tick = 0;
            inner_health_check.retry_limit = 0;
        }
    }

    pub async fn retransmission_tx(&self) {
        let mut f_pool = self.forward_pool.write().await;
        if !f_pool.body.is_empty() {
            self.broadcast_send_txs(f_pool.clone()).await;
            f_pool.body.clear();
        }
    }
}
