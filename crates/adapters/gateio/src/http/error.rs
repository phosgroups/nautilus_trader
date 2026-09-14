use nautilus_network::http::HttpClientError;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum GateioHttpError {
    #[error("missing credentials for authenticated Gate.io request")]
    MissingCredentials,
    #[error("Gate.io error {label}: {message}")]
    GateioError { label: String, message: String },
    #[error("JSON error: {0}")]
    Json(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("unexpected HTTP status {status}: {body}")]
    UnexpectedStatus { status: u16, body: String },
    #[error("network error: {0}")]
    Network(String),
}

impl From<HttpClientError> for GateioHttpError {
    fn from(value: HttpClientError) -> Self {
        Self::Network(value.to_string())
    }
}

impl From<serde_json::Error> for GateioHttpError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value.to_string())
    }
}

impl From<String> for GateioHttpError {
    fn from(value: String) -> Self {
        Self::Validation(value)
    }
}
