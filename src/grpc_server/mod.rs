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

pub(crate) mod consensus_server;
pub(crate) mod health_check_server;
pub(crate) mod network_server;
pub(crate) mod rpc_server;

use statig::awaitable::InitializedStateMachine;
use std::{net::AddrParseError, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;

use cita_cloud_proto::{
    controller::{
        consensus2_controller_service_server::Consensus2ControllerServiceServer,
        rpc_service_server::RpcServiceServer,
    },
    health_check::health_server::HealthServer,
    network::network_msg_handler_service_server::NetworkMsgHandlerServiceServer,
    status_code::StatusCodeEnum,
    CONTROLLER_DESCRIPTOR_SET,
};
use cloud_util::metrics::{run_metrics_exporter, MiddlewareLayer};

use crate::{
    config::ControllerConfig,
    core::{controller::Controller, state_machine::ControllerStateMachine},
    grpc_server::{
        consensus_server::Consensus2ControllerServer, health_check_server::HealthCheckServer,
        network_server::NetworkMsgHandlerServer, rpc_server::RPCServer,
    },
};

pub(crate) async fn grpc_serve(
    controller: Controller,
    controller_state_machine: Arc<RwLock<InitializedStateMachine<ControllerStateMachine>>>,
    config: ControllerConfig,
    rx_signal: flume::Receiver<()>,
) -> Result<(), StatusCodeEnum> {
    let grpc_port = config.controller_port.to_string();
    let addr_str = format!("0.0.0.0:{grpc_port}");
    let addr = addr_str.parse().map_err(|e: AddrParseError| {
        warn!("parse grpc listen address failed: {:?} ", e);
        StatusCodeEnum::FatalError
    })?;

    let layer = if config.enable_metrics {
        Some((
            tower::ServiceBuilder::new()
                .layer(MiddlewareLayer::new(config.metrics_buckets))
                .into_inner(),
            tokio::spawn(run_metrics_exporter(config.metrics_port)),
        ))
    } else {
        None
    };

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(CONTROLLER_DESCRIPTOR_SET)
        .build()
        .map_err(|e| {
            warn!("register grpc reflection failed: {:?} ", e);
            StatusCodeEnum::FatalError
        })?;

    info!("start controller grpc server");
    let http2_keepalive_interval = config.http2_keepalive_interval;
    let http2_keepalive_timeout = config.http2_keepalive_timeout;
    let tcp_keepalive = config.tcp_keepalive;
    if let Some((layer, metrics_exporter_join_handle)) = layer {
        Server::builder()
            .accept_http1(true)
            .http2_keepalive_interval(Some(Duration::from_secs(http2_keepalive_interval)))
            .http2_keepalive_timeout(Some(Duration::from_secs(http2_keepalive_timeout)))
            .tcp_keepalive(Some(Duration::from_secs(tcp_keepalive)))
            .layer(layer)
            .layer(GrpcWebLayer::new())
            .add_service(reflection)
            .add_service(
                RpcServiceServer::new(RPCServer::new(
                    controller.clone(),
                    controller_state_machine.clone(),
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                Consensus2ControllerServiceServer::new(Consensus2ControllerServer::new(
                    controller.clone(),
                    controller_state_machine,
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                NetworkMsgHandlerServiceServer::new(NetworkMsgHandlerServer::new(
                    controller.clone(),
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(HealthServer::new(HealthCheckServer::new(
                controller,
                config.health_check_timeout,
            )))
            .serve_with_shutdown(
                addr,
                cloud_util::graceful_shutdown::grpc_serve_listen_term(rx_signal),
            )
            .await
            .map_err(|e| {
                warn!("start controller grpc server failed: {:?} ", e);
                metrics_exporter_join_handle.abort();
                StatusCodeEnum::FatalError
            })?;
    } else {
        Server::builder()
            .accept_http1(true)
            .http2_keepalive_interval(Some(Duration::from_secs(http2_keepalive_interval)))
            .http2_keepalive_timeout(Some(Duration::from_secs(http2_keepalive_timeout)))
            .tcp_keepalive(Some(Duration::from_secs(tcp_keepalive)))
            .layer(GrpcWebLayer::new())
            .add_service(reflection)
            .add_service(
                RpcServiceServer::new(RPCServer::new(
                    controller.clone(),
                    controller_state_machine.clone(),
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                Consensus2ControllerServiceServer::new(Consensus2ControllerServer::new(
                    controller.clone(),
                    controller_state_machine,
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(
                NetworkMsgHandlerServiceServer::new(NetworkMsgHandlerServer::new(
                    controller.clone(),
                ))
                .max_decoding_message_size(usize::MAX),
            )
            .add_service(HealthServer::new(HealthCheckServer::new(
                controller,
                config.health_check_timeout,
            )))
            .serve_with_shutdown(
                addr,
                cloud_util::graceful_shutdown::grpc_serve_listen_term(rx_signal),
            )
            .await
            .map_err(|e| {
                warn!("start controller grpc server failed: {:?} ", e);
                StatusCodeEnum::FatalError
            })?;
    }
    Ok(())
}
