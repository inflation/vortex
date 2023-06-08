pub mod error;
pub mod io;
pub mod message;
pub mod node;
pub mod service;

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(std::io::stderr)
        .pretty()
        .init();
}
