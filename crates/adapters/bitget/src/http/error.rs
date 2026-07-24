// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Error types for the Bitget adapter.

use nautilus_network::http::HttpClientError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Represents the JSON structure of an error response returned by Bitget.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BitgetErrorResponse {
    /// Bitget response code.
    pub code: String,
    /// Human-readable error message.
    pub msg: String,
}

/// A typed error enumeration for the Bitget HTTP client.
#[derive(Debug, Clone, Error)]
pub enum BitgetHttpError {
    /// Error variant when credentials are missing but the request is authenticated.
    #[error("Missing credentials for authenticated Bitget request")]
    MissingCredentials,
    /// Errors returned directly by Bitget.
    #[error("Bitget error {code}: {message}")]
    BitgetError {
        /// Bitget error code.
        code: String,
        /// Bitget error message.
        message: String,
    },
    /// Failure during JSON serialization/deserialization.
    #[error("JSON error: {0}")]
    JsonError(String),
    /// Parameter validation error.
    #[error("Parameter validation error: {0}")]
    ValidationError(String),
    /// Generic network error.
    #[error("Network error: {0}")]
    NetworkError(String),
    /// Any unknown HTTP status or unexpected response from Bitget.
    #[error("Unexpected HTTP status code {status}: {body}")]
    UnexpectedStatus {
        /// HTTP status code.
        status: u16,
        /// Response body.
        body: String,
    },
}

impl From<HttpClientError> for BitgetHttpError {
    fn from(error: HttpClientError) -> Self {
        Self::NetworkError(error.to_string())
    }
}

impl From<serde_json::Error> for BitgetHttpError {
    fn from(error: serde_json::Error) -> Self {
        Self::JsonError(error.to_string())
    }
}

impl From<String> for BitgetHttpError {
    fn from(error: String) -> Self {
        Self::ValidationError(error)
    }
}
