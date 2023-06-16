use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use error::NodeError;
use futures::Future;
use message::Message;
use miette::IntoDiagnostic;

use node::Node;
use serde_json::Value;
use tracing_subscriber::{prelude::*, EnvFilter};

pub mod error;
pub mod io;
pub mod message;
pub mod node;
pub mod service;

pub fn init_tracing() -> miette::Result<()> {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .install_batch(opentelemetry::runtime::Tokio)
        .into_diagnostic()?;

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .pretty(),
        )
        .init();

    Ok(())
}

pub struct Main<Fut>
where
    Fut: Future<Output = miette::Result<()>>,
{
    pub node: Arc<Node>,
    pub fut: Fut,
}

impl<Fut> Future for Main<Fut>
where
    Fut: Future<Output = miette::Result<()>>,
{
    type Output = miette::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|s| &mut s.fut) }.poll(cx)
    }
}

pub fn main_loop<F, FutF>(
    func: F,
) -> Result<Main<impl Future<Output = miette::Result<()>>>, NodeError>
where
    F: Fn(Message<Value>, Arc<Node>) -> FutF + Send + Sync + Clone + 'static,
    FutF: Future<Output = Result<(), NodeError>> + Send + Sync,
{
    let (node, mut rx) = {
        let (node, rx) = Node::new()?;
        (Arc::new(node), rx)
    };
    let n = node.clone();

    let (c_tx, mut c_rx) = tokio::sync::mpsc::channel(1);

    let res = async move {
        let res = loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(msg) => {
                        let node = node.clone();
                        let c_tx = c_tx.clone();
                        let func = func.clone();

                        tokio::spawn(async move {
                            if let Err(e) = func(msg, node).await {
                                _ = c_tx.send(e).await;
                            }
                        });
                    },
                    None => break Ok(())
                },
                err = c_rx.recv() => if let Some(err) = err {
                    break Err(err);
                }
            }
        };

        opentelemetry::global::shutdown_tracer_provider();
        Ok(res?)
    };

    Ok(Main { node: n, fut: res })
}
