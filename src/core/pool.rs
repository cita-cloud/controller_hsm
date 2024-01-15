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

use cita_cloud_proto::blockchain::{raw_transaction::Tx, RawTransaction};
use indexmap::IndexSet;
use tokio::sync::RwLock;

use crate::{grpc_client::storage::reload_transactions_pool, util::get_tx_quota};

use super::auditor::Auditor;

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
    block_limit: u64,
    quota_limit: u64,
    warn_quota: u64,
    busy_quota: u64,
    in_busy: bool,
}

#[derive(Debug)]
pub enum PoolError {
    DupTransaction,
    TooManyRequests,
}

impl Pool {
    pub fn new(block_limit: u64, quota_limit: u64) -> Self {
        Pool {
            txns: IndexSet::new(),
            pool_quota: 0,
            block_limit,
            quota_limit,
            warn_quota: quota_limit * 30,
            busy_quota: quota_limit * 50,
            in_busy: false,
        }
    }

    pub async fn init(&mut self, auditor: Arc<RwLock<Auditor>>) {
        let mut txns = reload_transactions_pool()
            .await
            .map_or_else(|_| vec![], |txns| txns.body);
        info!("pool init start: txns({})", txns.len());
        {
            let auditor = auditor.read().await;
            let history_hashes_set: HashSet<_> = auditor
                .history_hashes
                .iter()
                .flat_map(|(_, hashes)| hashes)
                .collect();

            txns.retain(|txn| !history_hashes_set.contains(&get_raw_tx_hash(txn).to_vec()));
        }
        for raw_tx in txns {
            let tx_quota = get_tx_quota(&raw_tx).unwrap();
            self.txns.insert(Txn(raw_tx));
            self.pool_quota += tx_quota;
        }

        info!(
            "pool init finished: txns({}), pool_quota({})",
            self.txns.len(),
            self.pool_quota
        );
    }

    pub fn insert(&mut self, raw_tx: RawTransaction) -> Result<(), PoolError> {
        if self.in_busy {
            Err(PoolError::TooManyRequests)
        } else {
            let tx_quota = get_tx_quota(&raw_tx).unwrap();
            if self.pool_quota + tx_quota > self.busy_quota {
                self.in_busy = true;
                Err(PoolError::TooManyRequests)
            } else {
                let ret = self.txns.insert(Txn(raw_tx));
                if ret {
                    self.pool_quota += tx_quota;
                    Ok(())
                } else {
                    Err(PoolError::DupTransaction)
                }
            }
        }
    }

    pub fn remove(&mut self, tx_hash_list: &[Vec<u8>]) {
        for tx_hash in tx_hash_list {
            let tx_quota = self
                .txns
                .get(tx_hash.as_slice())
                .map(|txn| get_tx_quota(&txn.0).unwrap());
            if let Some(tx_quota) = tx_quota {
                self.pool_quota -= tx_quota;
            }
            self.txns.remove(tx_hash.as_slice());
        }
        if self.pool_quota < self.warn_quota {
            self.in_busy = false;
        }
    }

    pub fn package(&mut self, height: u64) -> (Vec<Vec<u8>>, u64) {
        let block_limit = self.block_limit;
        self.txns.retain(|txn| {
            let tx_is_valid = tx_is_valid(&txn.0, height, block_limit);
            if !tx_is_valid {
                let tx_quota = get_tx_quota(&txn.0).unwrap();
                self.pool_quota -= tx_quota;
            }
            tx_is_valid
        });
        if self.pool_quota < self.warn_quota {
            self.in_busy = false;
        }
        let mut quota_limit = self.quota_limit;
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
        (pack_tx, self.quota_limit - quota_limit)
    }

    pub fn pool_status(&self) -> (usize, u64) {
        (self.txns.len(), self.pool_quota)
    }

    pub fn pool_get_tx(&self, tx_hash: &[u8]) -> Option<RawTransaction> {
        self.txns.get(tx_hash).cloned().map(|txn| txn.0)
    }

    pub fn set_block_limit(&mut self, block_limit: u64) {
        self.block_limit = block_limit;
    }

    pub fn set_quota_limit(&mut self, quota_limit: u64) {
        self.quota_limit = quota_limit;
    }
}

fn tx_is_valid(raw_tx: &RawTransaction, height: u64, block_limit: u64) -> bool {
    match raw_tx.tx {
        Some(Tx::NormalTx(ref normal_tx)) => match normal_tx.transaction {
            Some(ref tx) => {
                height < tx.valid_until_block && tx.valid_until_block <= height + block_limit
            }
            None => false,
        },
        Some(Tx::UtxoTx(_)) => true,
        None => false,
    }
}
