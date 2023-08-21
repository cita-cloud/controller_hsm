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

mod config;
mod protocol;
#[macro_use]
mod util;
mod core;
mod crypto;
mod grpc_client;
mod grpc_server;
mod inner_health_check;

#[macro_use]
extern crate tracing as logger;

use clap::Parser;
use flume::unbounded;
use statig::awaitable::IntoStateMachineExt;
use std::{sync::Arc, time::Duration};
use tokio::{sync::RwLock, time};

use cita_cloud_proto::{network::RegisterInfo, status_code::StatusCodeEnum, storage::Regions};
use cloud_util::{network::register_network_msg_handler, storage::load_data};

use crate::{
    config::ControllerConfig,
    core::{
        controller::Controller,
        genesis::GenesisBlock,
        state_machine::{ControllerStateMachine, Event},
        system_config::SystemConfig,
    },
    grpc_client::{
        init_grpc_client, network_client, storage::load_data_maybe_empty, storage_client,
    },
    grpc_server::grpc_serve,
    util::{clap_about, u64_decode},
};

#[derive(Parser)]
#[clap(version, about = clap_about())]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
enum SubCommand {
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Parser)]
struct RunOpts {
    /// Chain config path
    #[clap(short = 'c', long = "config", default_value = "config.toml")]
    config_path: String,
    /// private key path
    #[clap(short = 'p', long = "private_key_path", default_value = "private_key")]
    private_key_path: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::Run(opts) => {
            let fin = run(opts);
            if let Err(e) = fin {
                warn!("unreachable: {:?}", e);
            }
        }
    }
}

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), StatusCodeEnum> {
    let rx_signal = cloud_util::graceful_shutdown::graceful_shutdown();

    // read consensus-config.toml
    let config = ControllerConfig::new(&opts.config_path);

    // init tracer
    cloud_util::tracer::init_tracer(config.domain.clone(), &config.log_config)
        .map_err(|e| println!("tracer init err: {e}"))
        .unwrap();

    init_grpc_client(&config);

    let grpc_port = config.controller_port.to_string();

    info!("controller grpc port: {}", grpc_port);

    info!("health check timeout: {}", config.health_check_timeout);

    let mut server_retry_interval =
        time::interval(Duration::from_secs(config.server_retry_interval));
    loop {
        server_retry_interval.tick().await;

        // register endpoint
        let request = RegisterInfo {
            module_name: "controller".to_owned(),
            hostname: "127.0.0.1".to_owned(),
            port: grpc_port.clone(),
        };

        match register_network_msg_handler(network_client(), request).await {
            StatusCodeEnum::Success => {
                info!("network service ready");
                break;
            }
            status => warn!(
                "network service not ready: {}. retrying...",
                status.to_string()
            ),
        }
    }

    // load sys_config
    info!("load system config");
    let genesis = GenesisBlock::new(&opts.config_path);
    let current_block_number;
    let current_block_hash;
    let mut server_retry_interval =
        time::interval(Duration::from_secs(config.server_retry_interval));
    loop {
        server_retry_interval.tick().await;
        {
            match load_data_maybe_empty(Regions::Global as u32, 0u64.to_be_bytes().to_vec()).await {
                Ok(current_block_number_bytes) => {
                    info!("storage service ready, get current height success");
                    if current_block_number_bytes.is_empty() {
                        info!("this is a new chain");
                        current_block_number = 0u64;
                        current_block_hash = genesis.genesis_block_hash().await;
                    } else {
                        info!("this is an old chain");
                        current_block_number = u64_decode(current_block_number_bytes);
                        current_block_hash = load_data(
                            storage_client(),
                            Regions::Global as u32,
                            1u64.to_be_bytes().to_vec(),
                        )
                        .await?;
                    }
                    break;
                }
                Err(e) => warn!("get current height failed: {}", e.to_string()),
            }
        }
        warn!("storage service not ready: retrying...");
    }
    info!(
        "init height: {}, init block hash: 0x{}",
        current_block_number,
        hex::encode(&current_block_hash)
    );

    let mut sys_config = SystemConfig::new(&opts.config_path);
    let initial_sys_config = sys_config.clone();
    sys_config.init(&config).await?;

    let (event_sender, event_receiver) = unbounded();

    let mut controller = Controller::new(
        config.clone(),
        current_block_number,
        current_block_hash,
        sys_config.clone(),
        genesis,
        event_sender.clone(),
        initial_sys_config,
        opts.private_key_path.clone(),
    )
    .await;

    config.clone().set_global();
    controller.init(current_block_number, sys_config).await;

    let controller_state_machine = Arc::new(RwLock::new(
        ControllerStateMachine
            .uninitialized_state_machine()
            .init_with_context(&mut controller)
            .await,
    ));

    let mut grpc_join_handle = tokio::spawn(grpc_serve(
        controller.clone(),
        controller_state_machine.clone(),
        config.clone(),
        rx_signal.clone(),
    ));
    let mut restart_num = 0;

    let mut reconnect_interval =
        time::interval(Duration::from_secs(config.origin_node_reconnect_interval));

    let mut inner_health_check_interval = time::interval(Duration::from_secs(
        config.inner_block_growth_check_interval,
    ));
    let mut forward_interval = time::interval(Duration::from_micros(config.buffer_duration));

    loop {
        tokio::select! {
            _ = reconnect_interval.tick() => {
                event_sender
                    .send(Event::BroadcastCSI)
                    .map_err(|e| {
                        warn!("send broadcast csi event failed: {}", e);
                        StatusCodeEnum::FatalError
                    })?;
                event_sender
                    .send(Event::RecordAllNode)
                    .map_err(|e| {
                        warn!("send record all node event failed: {}", e);
                        StatusCodeEnum::FatalError
                    })?;
            },
            _ = inner_health_check_interval.tick() => {
                event_sender
                    .send(Event::InnerHealthCheck)
                    .map_err(|e| {
                        warn!("send inner health check event failed: {}", e);
                        StatusCodeEnum::FatalError
                    })?;
            },
            _ = forward_interval.tick() => {
                controller
                    .retransmission_tx()
                    .await;
            },
            Ok(event) = event_receiver.recv_async() => {
                controller_state_machine.write().await.handle_with_context(&event, &mut controller).await;
            },
            _ = rx_signal.recv_async() => {
                info!("controller task exit!");
                break;
            },
            else => {
                debug!("controller task exit!");
                break;
            }
        }
        if grpc_join_handle.is_finished() {
            if restart_num < 10 {
                info!(
                    "controller grpc server has exited, try to start again({})...",
                    restart_num
                );
                tokio::time::sleep(Duration::from_secs(config.server_retry_interval)).await;
                grpc_join_handle = tokio::spawn(grpc_serve(
                    controller.clone(),
                    controller_state_machine.clone(),
                    config.clone(),
                    rx_signal.clone(),
                ));
                restart_num += 1;
            } else {
                info!("controller grpc server has exited, and restart failed, exit!",);
                break;
            }
        }
    }
    Ok(())
}
