use std::fmt::Debug;

use aws_lc_rs::hmac;
use nautilus_core::{env::get_or_env_var_opt, string::secret::REDACTED};
use zeroize::ZeroizeOnDrop;

pub const fn credential_env_vars() -> (&'static str, &'static str) {
    ("GATEIO_API_KEY", "GATEIO_API_SECRET")
}

#[derive(Clone, ZeroizeOnDrop)]
pub struct Credential {
    api_key: Box<str>,
    api_secret: Box<[u8]>,
}

impl Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("api_key", &self.api_key)
            .field("api_secret", &REDACTED)
            .finish()
    }
}

impl Credential {
    #[must_use]
    pub fn resolve(api_key: Option<String>, api_secret: Option<String>) -> Option<Self> {
        let (key_var, secret_var) = credential_env_vars();
        match (
            get_or_env_var_opt(api_key, key_var),
            get_or_env_var_opt(api_secret, secret_var),
        ) {
            (Some(key), Some(secret)) if !key.trim().is_empty() && !secret.trim().is_empty() => {
                Some(Self::new(key, secret))
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn new(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into().into_boxed_str(),
            api_secret: api_secret.into().into_bytes().into_boxed_slice(),
        }
    }

    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    #[must_use]
    pub fn sign_rest(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: &str,
        timestamp: &str,
    ) -> String {
        let body_hash = sha512_hex(body.as_bytes());
        let payload = format!(
            "{}\n{}\n{}\n{}\n{}",
            method.to_ascii_uppercase(),
            path,
            query,
            body_hash,
            timestamp
        );
        hmac_sha512_hex(&self.api_secret, payload.as_bytes())
    }

    #[must_use]
    pub fn sign_ws(&self, channel: &str, event: &str, timestamp: &str) -> String {
        let payload = format!("channel={channel}&event={event}&time={timestamp}");
        hmac_sha512_hex(&self.api_secret, payload.as_bytes())
    }
}

fn hmac_sha512_hex(key: &[u8], message: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA512, key);
    let tag = hmac::sign(&key, message);
    tag.as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sha512_hex(value: &[u8]) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA512, value);
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_signature_is_stable() {
        let credential = Credential::new("key", "secret");
        assert_eq!(
            credential.sign_rest(
                "GET",
                "/api/v4/spot/currency_pairs",
                "currency_pair=BTC_USDT",
                "",
                "1700000000"
            ),
            "27e117de64c048929828ca684185b252d22f29e7e3215563a09ceea4c8b2e3c542a7a6317858041db1398b96255131d82ade981999dcf9c7ff3ff5894f934dc2"
        );
    }

    #[test]
    fn websocket_signature_is_hex() {
        let signature = Credential::new("key", "secret").sign_ws("spot.orders", "subscribe", "1");
        assert_eq!(signature.len(), 128);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
