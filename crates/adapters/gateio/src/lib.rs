//! NautilusTrader adapter for Gate.io Spot and USDT-margined perpetual markets.

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod common;
pub mod config;
pub mod data;
pub mod execution;
pub mod factories;
pub mod http;
pub mod websocket;

#[cfg(feature = "python")]
pub mod python;

pub use common::enums::GateioProductType;
pub use config::{GateioDataClientConfig, GateioExecClientConfig};
pub use data::{GateioFuturesDataClient, GateioSpotDataClient};
pub use execution::{GateioFuturesExecutionClient, GateioSpotExecutionClient};
pub use factories::{GateioDataClientFactory, GateioExecutionClientFactory};
pub use http::{
    client::{GateioHttpClient, GateioRawHttpClient},
    error::GateioHttpError,
};
pub use websocket::{GateioWebSocketClient, GateioWsError};
