use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    event::Event,
    proto::vector as proto,
    sinks::util::{
        retries::RetryLogic, sink, BatchConfig, BatchSettings, BatchSink, EncodedLength,
        ServiceBuilderExt, TowerRequestConfig, VecBuffer,
    },
    sinks::{Healthcheck, VectorSink},
};
use futures::{
    future::{self, BoxFuture},
    stream, SinkExt, StreamExt,
};
use http::uri::Uri;
use lazy_static::lazy_static;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::path::PathBuf;
use std::task::{Context, Poll};
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
    IntoRequest,
};
use tower::ServiceBuilder;

type Client = proto::Client<Channel>;
type Response = Result<tonic::Response<proto::EventAck>, tonic::Status>;

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct VectorSinkConfig {
    address: String,
    #[serde(default)]
    pub batch: BatchConfig,
    #[serde(default)]
    pub request: TowerRequestConfig,
    #[serde(default)]
    pub tls: Option<GrpcTlsConfig>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct GrpcTlsConfig {
    ca_file: PathBuf,
    crt_file: PathBuf,
    key_file: PathBuf,
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
        tls: None,
    }
}

lazy_static! {
    static ref REQUEST_DEFAULTS: TowerRequestConfig = TowerRequestConfig {
        ..Default::default()
    };
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector_grpc")]
impl SinkConfig for VectorSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let endpoint = Endpoint::from_shared(self.address.clone())?;
        let endpoint = match &self.tls {
            Some(tls) => {
                let host = get_authority(&self.address)?;
                let ca = Certificate::from_pem(tokio::fs::read(&tls.ca_file).await?);
                let crt = tokio::fs::read(&tls.crt_file).await?;
                let key = tokio::fs::read(&tls.key_file).await?;
                let identity = Identity::from_pem(crt, key);

                let tls_config = ClientTlsConfig::new()
                    .identity(identity)
                    .ca_certificate(ca)
                    .domain_name(host);

                endpoint.tls_config(tls_config)?
            }
            None => endpoint,
        };

        let client = proto::Client::new(endpoint.connect_lazy()?);
        let healthcheck = healthcheck(client.clone());

        let request = self.request.unwrap_with(&REQUEST_DEFAULTS);
        let batch = BatchSettings::default()
            .bytes(1300)
            .events(1000)
            .timeout(1)
            .parse_config(self.batch)?;

        let svc = ServiceBuilder::new()
            .settings(request, VectorGrpcRetryLogic)
            .service(client);

        let buffer = VecBuffer::new(batch.size);
        let sink = BatchSink::new(svc, buffer, batch.timeout, cx.acker())
            .sink_map_err(|error| error!(message = "Fatal Vector GRPC sink error.", %error))
            .with_flat_map(move |event| stream::iter(encode_event(event)).map(Ok));

        Ok((VectorSink::Sink(Box::new(sink)), Box::pin(healthcheck)))
    }

    fn input_type(&self) -> DataType {
        DataType::Any
    }

    fn sink_type(&self) -> &'static str {
        "vector_grpc"
    }
}

/// Check to see if the remote service accepts new events.
async fn healthcheck(mut client: Client) -> crate::Result<()> {
    let request = client.health_check(proto::HealthCheckRequest {});

    if let Ok(response) = request.await {
        let status = proto::ServingStatus::from_i32(response.into_inner().status);

        if let Some(proto::ServingStatus::Serving) = status {
            return Ok(());
        }
    }

    Err(Box::new(Error::Health))
}

fn get_authority(url: &str) -> Result<String, Error> {
    url.parse::<Uri>()
        .ok()
        .and_then(|uri| uri.authority().map(ToString::to_string))
        .ok_or(Error::NoHost)
}

impl tower::Service<Vec<proto::EventRequest>> for Client {
    type Response = ();
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Readiness check of the client is done through the `push_events()`
        // call happening inside `call()`. That check blocks until the client is
        // ready to perform another request.
        //
        // See: <https://docs.rs/tonic/0.4.2/tonic/client/struct.Grpc.html#method.ready>
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, requests: Vec<proto::EventRequest>) -> Self::Future {
        let mut futures = Vec::with_capacity(requests.len());

        // TODO: Instead of firing off multiple requests, have the server accept
        // more than one event per request (i.e. bulk endpoint).
        for request in requests {
            let mut client = self.clone();
            futures.push(async move { client.push_events(request.into_request()).await })
        }

        Box::pin(async move {
            future::join_all(futures)
                .await
                .into_iter()
                .map(|v| match v {
                    Ok(..) => Ok(()),
                    Err(err) => Err(Error::Request { source: err }),
                })
                .collect::<Result<_, _>>()
        })
    }
}

fn encode_event(event: Event) -> Option<proto::EventRequest> {
    Some(proto::EventRequest {
        message: Some(event.into()),
    })
}

impl EncodedLength for proto::EventRequest {
    fn encoded_length(&self) -> usize {
        self.encoded_len()
    }
}

impl sink::Response for Response {
    fn is_successful(&self) -> bool {
        self.is_ok()
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Request failed: {}", source))]
    Request { source: tonic::Status },

    #[snafu(display("Vector source unhealthy"))]
    Health,

    #[snafu(display("URL has no host."))]
    NoHost,
}

#[derive(Debug, Clone)]
struct VectorGrpcRetryLogic;

impl RetryLogic for VectorGrpcRetryLogic {
    type Error = Error;
    type Response = ();

    fn is_retriable_error(&self, err: &Self::Error) -> bool {
        if let Error::Request { source } = err {
            if let tonic::Code::Unknown = source.code() {
                return false;
            }
        }

        true
    }
}
