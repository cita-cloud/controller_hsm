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

use std::{
    borrow::Borrow,
    cmp::{Eq, PartialEq},
    collections::HashSet,
    hash::{Hash, Hasher},
    sync::Arc,
};

use cita_cloud_proto::blockchain::{
    raw_transaction::Tx, RawTransaction, UnverifiedUtxoTransaction,
};
use indexmap::IndexSet;
use tokio::sync::RwLock;

use crate::{grpc_client::storage::reload_transactions_pool, util::get_tx_quota};

use super::{
    auditor::Auditor,
    system_config::{SystemConfig, LOCK_ID_BUTTON, LOCK_ID_VERSION},
};

// wrapper type for Hash
#[derive(Clone)]
struct Txn(RawTransaction);

impl Borrow<[u8]> for Txn {
    fn borrow(&self) -> &[u8] {
        get_raw_tx_hash(&self.0)
    }
}

impl PartialEq for Txn {
    fn eq(&self, other: &Self) -> bool {
        get_raw_tx_hash(&self.0) == get_raw_tx_hash(&other.0)
    }
}

impl Eq for Txn {}

impl Hash for Txn {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(get_raw_tx_hash(&self.0), state);
    }
}

fn get_raw_tx_hash(raw_tx: &RawTransaction) -> &[u8] {
    match raw_tx.tx.as_ref() {
        Some(Tx::NormalTx(tx)) => &tx.transaction_hash,
        Some(Tx::UtxoTx(utxo)) => &utxo.transaction_hash,
        None => &[],
    }
}

pub struct Pool {
    txns: IndexSet<Txn>,
    pool_quota: u64,
    sys_config: SystemConfig,
}

impl Pool {
    pub fn new(sys_config: SystemConfig) -> Self {
        Pool {
            txns: IndexSet::new(),
            pool_quota: 0,
            sys_config,
        }
    }

    pub async fn init(&mut self, auditor: Arc<RwLock<Auditor>>, init_block_number: u64) {
        let txns = reload_transactions_pool()
            .await
            .map_or_else(|_| vec![], |txns| txns.body);
        info!("pool init start: txns({})", txns.len());
        let history_hashes_set: HashSet<_> = auditor
            .read()
            .await
            .history_hashes
            .iter()
            .flat_map(|(_, hashes)| hashes.clone())
            .collect();

        let system_config = &self.sys_config;
        let next_height = init_block_number + 1;
        txns.iter().for_each(|txn| {
            if !history_hashes_set.contains(get_raw_tx_hash(txn))
                && tx_is_valid(system_config, txn, next_height)
            {
                let tx_quota = get_tx_quota(txn).unwrap();
                self.txns.insert(Txn(txn.clone()));
                self.pool_quota += tx_quota;
            }
        });

        info!(
            "pool init finished: txns({}), pool_quota({})",
            self.txns.len(),
            self.pool_quota
        );
    }

    pub fn insert(&mut self, raw_tx: RawTransaction) -> bool {
        let tx_quota = get_tx_quota(&raw_tx).unwrap();
        let ret = self.txns.insert(Txn(raw_tx));
        if ret {
            self.pool_quota += tx_quota;
        }
        ret
    }

    pub fn remove(&mut self, tx_hash_list: &[Vec<u8>], height: u64) {
        let system_config = &self.sys_config;
        let next_height = height + 1;
        self.txns.retain(|txn| {
            let tx_is_valid = !tx_hash_list.contains(&get_raw_tx_hash(&txn.0).to_vec())
                && tx_is_valid(system_config, &txn.0, next_height);
            if !tx_is_valid {
                let tx_quota = get_tx_quota(&txn.0).unwrap();
                self.pool_quota -= tx_quota;
            }
            tx_is_valid
        });
    }

    pub fn package(&self) -> (Vec<Vec<u8>>, u64) {
        let system_config = &self.sys_config;
        let mut quota_limit = system_config.quota_limit;
        let mut pack_tx = vec![];
        for txn in self.txns.iter().cloned() {
            let tx_quota = get_tx_quota(&txn.0).unwrap();
            if quota_limit >= tx_quota {
                pack_tx.push(get_raw_tx_hash(&txn.0).to_vec());
                quota_limit -= tx_quota;
                // 21000 is a basic tx quota, but the utxo's quota spend is 0
                if quota_limit < 21000 {
                    break;
                }
            }
        }
        (pack_tx, system_config.quota_limit - quota_limit)
    }

    pub fn pool_status(&self) -> (usize, u64) {
        (self.txns.len(), self.pool_quota)
    }

    pub fn pool_get_tx(&self, tx_hash: &[u8]) -> Option<RawTransaction> {
        self.txns.get(tx_hash).cloned().map(|txn| txn.0)
    }

    pub fn waiting_block(&self) -> u64 {
        self.pool_quota / self.sys_config.quota_limit
    }

    pub fn update_system_config(&mut self, tx: &UnverifiedUtxoTransaction) -> bool {
        self.sys_config.update(tx, false)
    }
}

fn tx_is_valid(sys_config: &SystemConfig, raw_tx: &RawTransaction, height: u64) -> bool {
    match &raw_tx.tx {
        Some(Tx::NormalTx(ref normal_tx)) => match normal_tx.transaction {
            Some(ref tx) => {
                height <= tx.valid_until_block
                    && tx.valid_until_block < height + sys_config.block_limit
            }
            None => false,
        },
        Some(Tx::UtxoTx(utxo_tx)) => {
            // check utxo tx sender
            if utxo_tx.witnesses[0].sender != sys_config.admin {
                return false;
            }

            if let Some(utxo_tx) = utxo_tx.transaction.as_ref() {
                if utxo_tx.version != sys_config.version {
                    return false;
                }
                let lock_id = utxo_tx.lock_id;
                if !(LOCK_ID_VERSION..LOCK_ID_BUTTON).contains(&lock_id) {
                    return false;
                }
                let hash = sys_config.utxo_tx_hashes.get(&lock_id).cloned().unwrap();
                hash == utxo_tx.pre_tx_hash
            } else {
                false
            }
        }
        None => false,
    }
}
