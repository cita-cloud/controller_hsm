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

use statig::prelude::*;
use std::fmt::Debug;

use crate::{
    core::chain::ChainStep,
    protocol::{
        node_manager::{ChainStatus, NodeAddress},
        sync_manager::SyncBlockRequest,
    },
};

use super::controller::Controller;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Event {
    // chain status respond
    SyncBlockReq(SyncBlockRequest, u64),
    // multicast sync block request
    SyncBlock,
    // broadcast chain status init
    BroadcastCSI,
    // record all node chain status
    RecordAllNode,
    // inner health check
    InnerHealthCheck,

    TryUpdateGlobalStatus(NodeAddress, ChainStatus),
    TrySyncBlock,
}

pub struct ControllerStateMachine;

#[state_machine(
    initial = "State::participate_in_consensus()",
    state(derive(Debug, Clone, PartialEq, Eq)),
    superstate(derive(Debug, Clone, PartialEq, Eq)),
    on_transition = "Self::on_transition",
    on_dispatch = "Self::on_dispatch"
)]
impl ControllerStateMachine {
    #[state]
    async fn offline(&self) -> Response<State> {
        Handled
    }

    #[superstate]
    async fn online(&self, context: &Controller, event: &Event) -> Response<State> {
        debug!("online: `{event:?}`");
        match event {
            Event::SyncBlockReq(req, origin) => {
                context.handle_sync_block_req(req, origin).await;
                Handled
            }
            Event::SyncBlock => handle_sync_block(context).await,
            Event::BroadcastCSI => {
                let _ = context.handle_broadcast_csi().await;
                Handled
            }
            Event::RecordAllNode => {
                context.handle_record_all_node().await;
                Handled
            }
            Event::InnerHealthCheck => {
                context.inner_health_check().await;
                Handled
            }
            Event::TrySyncBlock => try_sync_block(context).await,
            _ => Super,
        }
    }

    #[state(superstate = "online")]
    async fn participate_in_consensus(
        &self,
        context: &mut Controller,
        event: &Event,
    ) -> Response<State> {
        debug!("participate_in_consensus: `{event:?}`");
        match event {
            Event::TryUpdateGlobalStatus(node, status) => {
                let _ = context
                    .try_update_global_status(node, status.clone(), false)
                    .await;
                Handled
            }
            _ => Super,
        }
    }

    #[superstate(superstate = "online", exit_action = "exit_sync")]
    async fn sync(&self, context: &mut Controller, event: &Event) -> Response<State> {
        debug!("sync: `{event:?}`");
        match event {
            Event::TryUpdateGlobalStatus(node, status) => {
                let _ = context
                    .try_update_global_status(node, status.clone(), true)
                    .await;
                Handled
            }
            _ => Super,
        }
    }

    #[state(superstate = "sync")]
    async fn prepare_sync(&self, event: &Event) -> Response<State> {
        debug!("prepare_sync: `{event:?}`");
        Super
    }

    #[state(superstate = "sync", entry_action = "enter_syncing")]
    async fn syncing(&self, event: &Event) -> Response<State> {
        debug!("syncing: `{event:?}`");
        Super
    }

    #[action]
    async fn enter_syncing(&self, context: &mut Controller) {
        if let Err(e) = context.syncing_block().await {
            warn!("syncing block error: {:?}", e)
        }
    }

    #[action]
    async fn exit_sync(&self, context: &mut Controller) {
        context.sync_manager.clear().await;
    }
}

impl ControllerStateMachine {
    fn on_transition(&mut self, source: &State, target: &State) {
        if source != target {
            info!("transitioned from `{source:?}` to `{target:?}`");
        }
    }

    fn on_dispatch(&mut self, state: StateOrSuperstate<Self>, event: &Event) {
        debug!("dispatching `{event:?}` to `{state:?}`");
    }
}

async fn handle_sync_block(context: &Controller) -> statig::Response<State> {
    debug!("receive SyncBlock event");
    let (_, global_status) = context.get_global_status().await;
    let res = {
        let chain = context.chain.read().await;
        chain.next_step(&global_status)
    };
    match res {
        ChainStep::SyncStep => Transition(State::syncing()),
        ChainStep::OnlineStep => Transition(State::participate_in_consensus()),
        ChainStep::BusyState => Handled,
    }
}

async fn try_sync_block(context: &Controller) -> statig::Response<State> {
    let (global_address, global_status) = context.get_global_status().await;

    let current_height = context.get_status().await.height;

    // try read chain state, if can't get chain default online state
    let res = {
        if let Ok(chain) = context.chain.try_read() {
            chain.next_step(&global_status)
        } else {
            ChainStep::BusyState
        }
    };

    match res {
        ChainStep::SyncStep => {
            if let Some(sync_req) = context
                .sync_manager
                .get_sync_block_req(current_height, &global_status)
                .await
            {
                let _ = context
                    .unicast_sync_block(global_address.0, sync_req.clone())
                    .await
                    .await
                    .map_err(|e| warn!("try_sync_block: unicast_sync_block error: {:?}", e));
            }
            Transition(State::prepare_sync())
        }
        ChainStep::OnlineStep => Transition(State::participate_in_consensus()),
        ChainStep::BusyState => Handled,
    }
}

impl State {
    pub fn matches_super(&self, other: &Superstate) -> bool {
        !matches!(
            (self, other),
            (State::Offline {}, Superstate::Sync {})
                | (State::Offline {}, Superstate::Online {})
                | (State::ParticipateInConsensus {}, Superstate::Sync {})
        )
    }
}

#[test]
fn test_state() {
    assert!(State::ParticipateInConsensus {}.matches_super(&Superstate::Online {}));
    assert!(!State::ParticipateInConsensus {}.matches_super(&Superstate::Sync {}));
}

#[tokio::test]
async fn test() {
    use crate::{config::ControllerConfig, GenesisBlock, SystemConfig};

    let config = ControllerConfig::new("example/config.toml");
    let genesis = GenesisBlock::new("example/config.toml");
    let sys_config = SystemConfig::new("example/config.toml");
    let (event_sender, _) = flume::unbounded();
    let mut controller = Controller::new(
        config,
        0,
        genesis.genesis_block_hash().await,
        sys_config.clone(),
        genesis,
        event_sender.clone(),
        sys_config.clone(),
        "example/private_key".to_owned(),
    )
    .await;

    let mut state_machine = ControllerStateMachine
        .uninitialized_state_machine()
        .init_with_context(&mut controller)
        .await;

    state_machine
        .handle_with_context(&Event::SyncBlock, &mut controller)
        .await;
    println!("{:?}", state_machine.state());
}
