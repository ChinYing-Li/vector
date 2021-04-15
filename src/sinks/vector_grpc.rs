use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    event::proto as event,
    event::Event,
    sinks::{Healthcheck, Sink, VectorSink},
};
use futures::{Future, FutureExt};
use getset::Setters;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::{
    client::GrpcService,
    transport::{Channel, Endpoint},
};

// TODO: duplicated for sink/source, should move to util.
mod proto {
    use tonic::include_proto;
    pub use vector_client::VectorClient as Client;

    include_proto!("vector");
}

#[derive(Deserialize, Serialize, Debug, Setters)]
#[serde(deny_unknown_fields)]
pub struct VectorConfig {
    address: String,
    // TODO
    // #[serde(default)]
    // pub compression: Compression,
    // pub encoding: EncodingConfig<Encoding>,
    // #[serde(default)]
    // pub batch: BatchConfig,
    // #[serde(default)]
    // pub request: RequestConfig,
    // pub tls: Option<TlsOptions>,
}

impl VectorConfig {
    pub fn new(address: String) -> Self {
        Self { address }
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
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let endpoint = Endpoint::from_shared(self.address.clone()).expect("TODO");
        let channel = endpoint.connect_lazy().expect("TODO");
        let sink = GrpcSink {
            channel,
            future: None,
        };

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

struct GrpcSink {
    channel: Channel,
    future: Option<
        Pin<
            Box<
                dyn Future<Output = Result<tonic::Response<proto::EventAck>, tonic::Status>> + Send,
            >,
        >,
    >,
}

impl Sink<Event> for GrpcSink {
    type Error = ();

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.future.is_some() {
            return Poll::Pending;
        }

        self.channel.poll_ready(cx).map_err(|_| ())
    }

    fn start_send(mut self: Pin<&mut Self>, item: Event) -> Result<(), Self::Error> {
        let channel = self.channel.clone();
        let future = Box::pin(async move {
            let mut client = proto::Client::new(channel);
            let request = proto::EventRequest {
                message: Some(item.into()),
            };

            client.push_events(request).await
        });

        self.future = Some(future);

        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.future.take() {
            Some(mut fut) => match fut.poll_unpin(cx) {
                Poll::Pending => {
                    self.future.replace(fut);
                    return Poll::Pending;
                }
                _ => {}
            },
            None => {}
        };

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
