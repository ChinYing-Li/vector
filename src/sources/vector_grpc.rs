use crate::{
    config::SourceContext,
    config::{DataType, GenerateConfig, Resource, SourceConfig, SourceDescription},
    event::Event,
    proto::vector as proto,
    shutdown::{ShutdownSignal, ShutdownSignalToken},
    sources::Source,
    tls::TlsConfig,
    Pipeline,
};

use futures::{FutureExt, SinkExt, TryFutureExt};
use getset::Setters;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Debug, Clone)]
pub struct Service {
    pipeline: Pipeline,
    healthy: Arc<AtomicBool>,
}

impl Service {
    // TODO: might not need this if there's no way to determine internal Vector
    // health status (or if we don't care about it in this source).
    #[allow(dead_code)]
    fn set_healthy(&self, healthy: bool) {
        loop {
            match self.healthy.compare_exchange_weak(
                !healthy,
                healthy,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(_) => std::hint::spin_loop(),
            }
        }
    }
}

#[tonic::async_trait]
impl proto::Service for Service {
    async fn push_events(
        &self,
        request: Request<proto::EventRequest>,
    ) -> Result<Response<proto::EventAck>, Status> {
        let event = request
            .into_inner()
            .message
            .map(Event::from)
            .ok_or(Status::invalid_argument("missing event"))?;

        let response = Response::new(proto::EventAck {
            // TODO: There is no need for any body in the ack.
            message: "success".to_owned(),
        });

        self.pipeline
            .clone()
            .send(event)
            .await
            .map(|_| response)
            .map_err(|err| Status::unavailable(err.to_string()))
    }

    // TODO: figure out a way to determine if the current Vector instance is "healthy".
    async fn health_check(
        &self,
        _: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        let status = if self.healthy.load(Ordering::Relaxed) {
            proto::ServingStatus::Serving
        } else {
            proto::ServingStatus::NotServing
        };

        let message = proto::HealthCheckResponse {
            status: status.into(),
        };

        Ok(Response::new(message))
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Setters)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub address: SocketAddr,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[set = "pub"]
    tls: Option<TlsConfig>,
}

fn default_shutdown_timeout_secs() -> u64 {
    30
}

inventory::submit! {
    SourceDescription::new::<Config>("vector_grpc")
}

impl GenerateConfig for Config {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            address: "0.0.0.0:80".parse().unwrap(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            tls: None,
        })
        .unwrap()
    }
}

#[tonic::async_trait]
#[typetag::serde(name = "vector_grpc")]
impl SourceConfig for Config {
    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        let SourceContext { shutdown, out, .. } = cx;

        let source = run(self.address, out, shutdown).map_err(|error| {
            error!(message = "Source future failed.", %error);
        });

        Ok(Box::pin(source))
    }

    fn output_type(&self) -> DataType {
        DataType::Any
    }

    fn source_type(&self) -> &'static str {
        "vector_grpc"
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::tcp(self.address)]
    }
}

async fn run(address: SocketAddr, out: Pipeline, shutdown: ShutdownSignal) -> crate::Result<()> {
    let _span = crate::trace::current_span();

    let healthy = Arc::new(AtomicBool::new(true));
    let service = proto::Server::new(Service {
        pipeline: out,
        healthy,
    });

    let (tx, rx) = tokio::sync::oneshot::channel::<ShutdownSignalToken>();

    Server::builder()
        .add_service(service)
        .serve_with_shutdown(address, shutdown.map(|token| tx.send(token).unwrap()))
        .await?;

    drop(rx.await);

    Ok(())
}
