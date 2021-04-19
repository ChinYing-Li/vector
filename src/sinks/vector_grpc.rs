use crate::sinks::util::{BatchConfig, BatchSettings, BatchSink, EncodedLength, VecBuffer};
use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    event::proto as event,
    sinks::{util::sink, Healthcheck, VectorSink},
    event::Event,
};
use futures::{
    future::{self, BoxFuture},
    stream, SinkExt, StreamExt, TryFutureExt,
};
use getset::Setters;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::task::{Context, Poll};
use tonic::{transport::Endpoint, IntoRequest};

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
pub struct VectorConfig {
    address: String,
    // TODO
    #[serde(default)]
    pub batch: BatchConfig,
    // #[serde(default)]
    // pub request: RequestConfig,
    // pub tls: Option<TlsOptions>,
}

impl VectorConfig {
    pub fn new(address: String) -> Self {
        Self {
            address,
            batch: BatchConfig::default(),
        }
    }
}

inventory::submit! {
    SinkDescription::new::<VectorConfig>("vector_grpc")
}

impl GenerateConfig for VectorConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::new("127.0.0.1:6000".to_string())).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector_grpc")]
impl SinkConfig for VectorConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let endpoint = Endpoint::from_shared(self.address.clone()).expect("TODO");
        let channel = endpoint.connect_lazy().expect("TODO");
        let client = proto::Client::new(channel);

        let batch = BatchSettings::default()
            .bytes(1300)
            .events(1000)
            .timeout(1)
            .parse_config(self.batch)?;

        let sink = BatchSink::new(
            client,
            VecBuffer::new(batch.size),
            batch.timeout,
            cx.acker(),
        )
        .sink_map_err(|error| error!(message = "Fatal GRPC sink error.", %error))
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
