use crate::{
    config::{DataType, GenerateConfig, SinkConfig, SinkContext, SinkDescription},
    event::proto as event,
    event::Event,
    sinks::{Healthcheck, Sink, VectorSink},
};
use futures::Future;
use getset::Setters;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::pin::Pin;
use std::task::{Context, Poll};

// TODO: duplicated for sink/source, should move to util.
mod proto {
    pub use vector_client::VectorClient as Client;

    tonic::include_proto!("vector");
}

#[derive(Deserialize, Serialize, Debug, Setters)]
#[serde(deny_unknown_fields)]
pub struct Config {
    address: String,
}

impl Config {
    pub fn new(address: String) -> Self {
        Self { address }
    }
}

inventory::submit! {
    SinkDescription::new::<Config>("vector_grpc")
}

impl GenerateConfig for Config {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::new("127.0.0.1:6000".to_string())).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector_grpc")]
impl SinkConfig for Config {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let endpoint = tonic::transport::Endpoint::try_from(self.address.to_owned()).expect("TODO");
        let client = proto::Client::connect(endpoint).await.expect("TODO");
        let sink = GrpcSink {
            client,
            request: None,
        };

        Ok((
            VectorSink::Sink(Box::new(sink)),
            Box::pin(async move { Ok(()) }),
        ))

        // let sink_config = TcpSinkConfig::new(
        //     self.address.clone(),
        //     self.keepalive,
        //     self.tls.clone(),
        //     self.send_buffer_bytes,
        // );

        // sink_config.build(cx, |event| Some(encode_event(event)))
    }

    fn input_type(&self) -> DataType {
        DataType::Any
    }

    fn sink_type(&self) -> &'static str {
        "vector_grpc"
    }
}

struct GrpcSink {
    client: proto::Client<tonic::transport::Channel>,
    request: Option<
        Pin<
            Box<
                dyn Future<Output = Result<tonic::Response<proto::EventAck>, tonic::Status>>
                    + Send
                    + 'static,
            >,
        >,
    >,
}

impl Sink<Event> for GrpcSink {
    type Error = ();

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Event) -> Result<(), Self::Error> {
        let request = proto::EventRequest {
            message: Some(item.into()),
        };

        // let client = self.client.clone();
        // let response = self.client.clone().push_events(request);

        self.request = Some(Box::pin(
            async move { self.client.push_events(request).await },
        ));

        // async move {
        //     self.request = Some(Box::pin(response));
        // };

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO
        // self.client.push_events(request);

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
