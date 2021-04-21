use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    event::proto as event,
    event::Event,
    sinks::util::{
        retries::RetryLogic, sink, BatchConfig, BatchSettings, BatchSink, EncodedLength,
        TowerRequestConfig, VecBuffer,
    },
    sinks::{Healthcheck, VectorSink},
};
use futures::{
    future::{self, BoxFuture},
    stream, SinkExt, StreamExt, TryFutureExt,
};
use getset::Setters;
use lazy_static::lazy_static;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::task::{Context, Poll};
use tonic::{transport::Endpoint, IntoRequest};
use tower::ServiceBuilder;

// TODO: duplicated for sink/source, should move to util.
mod proto {
    use tonic::include_proto;
    pub use vector_client::VectorClient as Client;

    include_proto!("vector");
}

type Client = proto::Client<tonic::transport::Channel>;
type Request = tonic::Request<proto::EventRequest>;
type Response = Result<tonic::Response<proto::EventAck>, tonic::Status>;

#[derive(Deserialize, Serialize, Debug, Setters)]
#[serde(deny_unknown_fields)]
pub struct VectorSinkConfig {
    address: String,
    #[serde(default)]
    pub batch: BatchConfig,
    #[serde(default)]
    pub request: TowerRequestConfig<Option<usize>>,
}

inventory::submit! {
    SinkDescription::new::<VectorSinkConfig>("vector_grpc")
}

impl GenerateConfig for VectorSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(default_config("127.0.0.1:6000")).unwrap()
    }
}

fn default_config(address: &str) -> VectorSinkConfig {
    VectorSinkConfig {
        address: address.to_owned(),
        batch: BatchConfig::default(),
        request: TowerRequestConfig::default(),
    }
}

lazy_static! {
    static ref REQUEST_DEFAULTS: TowerRequestConfig<Option<usize>> = TowerRequestConfig {
        ..Default::default()
    };
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector_grpc")]
impl SinkConfig for VectorSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let endpoint = Endpoint::from_shared(self.address.clone()).expect("TODO");
        let channel = endpoint.connect_lazy().expect("TODO");
        let batch = BatchSettings::default()
            .bytes(1300)
            .events(1000)
            .timeout(1)
            .parse_config(self.batch)?;
        let request = self.request.unwrap_with(&REQUEST_DEFAULTS);

        let client = proto::Client::new(channel);
        let svc = ServiceBuilder::new()
            .concurrency_limit(request.concurrency.unwrap())
            // .retry(request.retry_policy(VectorGrpcRetryLogic))
            // .rate_limit(request.rate_limit_num, request.rate_limit_duration)
            // .timeout(request.timeout)
            .service(client);

        let buffer = VecBuffer::new(batch.size);
        let sink = BatchSink::new(svc, buffer, batch.timeout, cx.acker())
            .sink_map_err(|error| error!(message = "Fatal grpc sink error.", %error))
            .with_flat_map(move |event| stream::iter(encode_event(event)).map(Ok));

        // TODO
        let healthcheck = async move { Ok(()) };

        Ok((VectorSink::Sink(Box::new(sink)), Box::pin(healthcheck)))
    }

    fn input_type(&self) -> DataType {
        DataType::Any
    }

    fn sink_type(&self) -> &'static str {
        "vector_grpc"
    }
}

fn encode_event(event: Event) -> Option<Request> {
    let request = proto::EventRequest {
        message: Some(event.into()),
    };

    Some(request.into_request())
}

impl EncodedLength for Request {
    fn encoded_length(&self) -> usize {
        self.get_ref().encoded_len()
    }
}

impl tower::Service<Vec<Request>> for Client {
    type Response = ();
    type Error = String;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, requests: Vec<Request>) -> Self::Future {
        let mut futures = Vec::with_capacity(requests.len());

        for request in requests {
            let mut client = self.clone();
            futures.push(async move {
                client
                    .push_events(request)
                    .map_err(|err| err.to_string())
                    .await
            })
        }

        Box::pin(async move {
            future::join_all(futures)
                .await
                .into_iter()
                .map(|v| match v {
                    Ok(..) => Ok(()),
                    Err(e) => Err(e.to_string()),
                })
                .collect::<Result<_, _>>()
        })
    }
}

impl sink::Response for Response {
    fn is_successful(&self) -> bool {
        self.is_ok()
    }
}

#[derive(Debug, Snafu)]
pub enum Error {}

#[derive(Debug, Clone)]
struct VectorGrpcRetryLogic;

impl RetryLogic for VectorGrpcRetryLogic {
    type Error = Error;
    type Response = ();

    fn is_retriable_error(&self, _: &Self::Error) -> bool {
        // TODO
        true
    }
}
