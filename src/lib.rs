use miette::IntoDiagnostic;

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
