use crate::{
    config::SourceContext,
    config::{DataType, GenerateConfig, GlobalOptions, Resource, SourceConfig, SourceDescription},
    event::proto as event,
    event::Event,
    shutdown::{ShutdownSignal, ShutdownSignalToken},
    sources::Source,
    tls::TlsConfig,
    Pipeline,
};

use futures::{FutureExt, TryFutureExt};
use getset::Setters;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tonic::{transport::Server, Request, Response, Status};

// TODO: duplicated for sink/source, should move to util.
mod proto {
    pub use vector_server::{Vector, VectorServer as Server};

    tonic::include_proto!("vector");
}

#[derive(Debug, Clone)]
pub struct Service {
    pipeline: Pipeline,
}

#[tonic::async_trait]
impl proto::Vector for Service {
    async fn push_events(
        &self,
        request: Request<proto::EventRequest>,
    ) -> Result<Response<proto::EventAck>, Status> {
        println!("GetEvents = {:?}", request);

        let event: Event = match request.into_inner().message {
            None => panic!("TODO"),
            Some(wrapper) => wrapper.into(),
        };

        dbg!(event);

        let status = Status::invalid_argument("name is invalid");
        Err(status)

        // let reply = vector_proto::EventAck {
        //     message: "nonsense".to_owned(),
        // };
        // // Get event from request
        // // event_from_req(request.body)
        // // Then send event to pipeline
        // self.out.send() //'send' `Event`
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
    let service = proto::Server::new(Service { pipeline: out });

    let (tx, rx) = tokio::sync::oneshot::channel::<ShutdownSignalToken>();
    Server::builder()
        .add_service(service)
        .serve_with_shutdown(address, shutdown.map(|token| tx.send(token).unwrap()))
        .await?;

    drop(rx.await);

    Ok(())
}

// fn build_event(body: Bytes) -> Option<Event> {
//     match event::EventWrapper::decode(body).map(Event::from) {
//         Ok(event) => Some(event),
//         Err(..) => None,
//     }
// }
