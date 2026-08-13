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

//! Bitget API credential storage and signing helpers.

use std::fmt::Debug;

use aws_lc_rs::hmac;
use base64::{Engine, engine::general_purpose};
use nautilus_core::{env::get_or_env_var_opt, string::secret::REDACTED};
use zeroize::ZeroizeOnDrop;

/// Returns the Bitget environment variable names for credentials.
#[must_use]
pub const fn credential_env_vars() -> (&'static str, &'static str, &'static str) {
    (
        "BITGET_API_KEY",
        "BITGET_API_SECRET",
        "BITGET_API_PASSPHRASE",
    )
}

/// API credentials required for signing Bitget REST and private WebSocket requests.
#[derive(Clone, ZeroizeOnDrop)]
pub struct Credential {
    api_key: Box<str>,
    api_secret: Box<[u8]>,
    api_passphrase: Box<str>,
}

impl Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Credential))
            .field("api_key", &self.api_key)
            .field("api_secret", &REDACTED)
            .field("api_passphrase", &REDACTED)
            .finish()
    }
}

impl Credential {
    /// Resolves credentials from provided values or Bitget environment variables.
    #[must_use]
    pub fn resolve(
        api_key: Option<String>,
        api_secret: Option<String>,
        api_passphrase: Option<String>,
    ) -> Option<Self> {
        let (key_var, secret_var, passphrase_var) = credential_env_vars();
        let key = get_or_env_var_opt(api_key, key_var);
        let secret = get_or_env_var_opt(api_secret, secret_var);
        let passphrase = get_or_env_var_opt(api_passphrase, passphrase_var);

        match (key, secret, passphrase) {
            (Some(k), Some(s), Some(p)) => Some(Self::new(k, s, p)),
            _ => None,
        }
    }

    /// Creates a new [`Credential`] instance.
    #[must_use]
    pub fn new(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        api_passphrase: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into().into_boxed_str(),
            api_secret: api_secret.into().into_bytes().into_boxed_slice(),
            api_passphrase: api_passphrase.into().into_boxed_str(),
        }
    }

    /// Returns the API key.
    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Returns the API passphrase.
    #[must_use]
    pub fn api_passphrase(&self) -> &str {
        &self.api_passphrase
    }

    /// Produces the Bitget HMAC-SHA256 Base64 signature.
    ///
    /// The message is `timestamp + method + request_path + query_string + body`.
    #[must_use]
    pub fn sign(
        &self,
        timestamp: &str,
        method: &str,
        request_path: &str,
        query_string: Option<&str>,
        body: Option<&str>,
    ) -> String {
        let query = query_string.unwrap_or_default();
        let body = body.unwrap_or_default();
        let mut message = String::with_capacity(
            timestamp.len() + method.len() + request_path.len() + query.len() + body.len(),
        );
        message.push_str(timestamp);
        message.push_str(method);
        message.push_str(request_path);
        message.push_str(query);
        message.push_str(body);

        let key = hmac::Key::new(hmac::HMAC_SHA256, &self.api_secret);
        let tag = hmac::sign(&key, message.as_bytes());
        general_purpose::STANDARD.encode(tag.as_ref())
    }

    /// Produces the private WebSocket login signature.
    ///
    /// Bitget signs WebSocket login over `timestamp + GET + /user/verify`.
    #[must_use]
    pub fn sign_websocket_login(&self, timestamp: &str) -> String {
        self.sign(timestamp, "GET", "/user/verify", None, None)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn sign_get_with_query_is_stable() {
        let credential = Credential::new("key", "secret", "passphrase");

        let signature = credential.sign(
            "1700000000000",
            "GET",
            "/api/v3/market/instruments",
            Some("?category=USDT-FUTURES"),
            None,
        );

        assert_eq!(signature, "rzhObUOoLh7FK+WLJfYrk/BfldkvNvDB1mEeiADbwT0=");
    }

    #[rstest]
    fn sign_post_with_body_is_stable() {
        let credential = Credential::new("key", "secret", "passphrase");

        let signature = credential.sign(
            "1700000000000",
            "POST",
            "/api/v3/trade/place-order",
            None,
            Some(r#"{"category":"SPOT","symbol":"BTCUSDT","side":"buy"}"#),
        );

        assert_eq!(signature, "MeBp/ZKxm9NifLSnZrJwLxBiLJUrDVcus8s6bJ2KhTQ=");
    }

    #[rstest]
    fn sign_websocket_login_uses_user_verify_prehash() {
        let credential = Credential::new("key", "secret", "passphrase");

        assert_eq!(
            credential.sign_websocket_login("1700000000000"),
            credential.sign("1700000000000", "GET", "/user/verify", None, None),
        );
    }

    #[rstest]
    fn resolve_accepts_empty_passphrase() {
        let credential = Credential::resolve(
            Some("key".to_string()),
            Some("secret".to_string()),
            Some(String::new()),
        )
        .unwrap();

        assert_eq!(credential.api_key(), "key");
        assert_eq!(credential.api_passphrase(), "");
    }
}
