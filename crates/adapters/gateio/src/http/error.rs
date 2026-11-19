use nautilus_network::http::HttpClientError;
use thiserror::Error;

use super::models::GateioErrorResponse;

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

impl GateioHttpError {
    fn matches_business_label(&self, predicate: impl FnOnce(&str) -> bool) -> bool {
        match self {
            Self::GateioError { label, .. } => predicate(label),
            Self::UnexpectedStatus { status, body }
                if (400..500).contains(status) && *status != 408 =>
            {
                serde_json::from_str::<GateioErrorResponse>(body)
                    .is_ok_and(|error| predicate(&error.label))
            }
            _ => false,
        }
    }

    pub(crate) fn is_order_not_found(&self) -> bool {
        self.matches_business_label(|label| {
            matches!(label, "ORDER_NOT_FOUND" | "CLIENT_ID_NOT_FOUND")
        })
    }

    pub(crate) fn is_definitive_submit_rejection(&self) -> bool {
        // These local errors occur before sending the request.
        if matches!(self, Self::MissingCredentials | Self::Validation(_)) {
            return true;
        }

        // Unknown labels and duplicate submissions need reconciliation by order ID.
        self.matches_business_label(|label| {
            matches!(
                label,
                "BAD_REQUEST"
                    | "INVALID_PARAM_VALUE"
                    | "INVALID_ARGUMENT"
                    | "INVALID_PROTOCOL"
                    | "INVALID_CONTENT_TYPE"
                    | "INVALID_REQUEST_BODY"
                    | "MISSING_REQUIRED_PARAM"
                    | "MISSING_REQUIRED_HEADER"
                    | "METHOD_NOT_ALLOWED"
                    | "NOT_ACCEPTABLE"
                    | "INVALID_CREDENTIALS"
                    | "INVALID_KEY"
                    | "INVALID_SIGNATURE"
                    | "IP_FORBIDDEN"
                    | "READ_ONLY"
                    | "FORBIDDEN"
                    | "ACCOUNT_LOCKED"
                    | "REQUEST_EXPIRED"
                    | "INVALID_CLIENT_ORDER_ID"
                    | "TOO_MANY_REQUESTS"
                    | "TOO_MANY_ORDERS"
                    | "INVALID_CURRENCY_PAIR"
                    | "INVALID_CURRENCY"
                    | "INVALID_PRECISION"
                    | "POC_FILL_IMMEDIATELY"
                    | "FOK_NOT_FILL"
                    | "QUANTITY_NOT_ENOUGH"
                    | "BALANCE_NOT_ENOUGH"
                    | "AMOUNT_TOO_LITTLE"
                    | "AMOUNT_TOO_MUCH"
                    | "TRADE_RESTRICTED"
                    | "ORDER_BOOK_NOT_FOUND"
                    | "USER_NOT_FOUND"
                    | "CONTRACT_NOT_FOUND"
                    | "CONTRACT_NO_COUNTER"
                    | "CONTRACT_IN_DELISTING"
                    | "INSUFFICIENT_AVAILABLE"
                    | "RISK_LIMIT_EXCEEDED"
                    | "LIQUIDATE_IMMEDIATELY"
                    | "POSITION_IN_LIQUIDATION"
                    | "POSITION_IN_CLOSE"
                    | "POSITION_EMPTY"
                    | "SIZE_TOO_LARGE"
                    | "SIZE_TOO_SMALL"
                    | "PRICE_TOO_DEVIATED"
                    | "PRICE_OVER_LIQUIDATION"
                    | "PRICE_OVER_BANKRUPT"
                    | "REDUCE_EXCEEDED"
                    | "INCREASE_POSITION"
                    | "ORDER_POC_IMMEDIATE"
                    | "ORDER_FOK"
            )
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn status_error(status: u16, label: &str) -> GateioHttpError {
        GateioHttpError::UnexpectedStatus {
            status,
            body: serde_json::json!({ "label": label }).to_string(),
        }
    }

    #[test]
    fn submit_rejection_requires_a_definitive_outcome() {
        for (label, rejected) in [
            ("BALANCE_NOT_ENOUGH", true),
            ("ORDER_POC_IMMEDIATE", true),
            ("INVALID_PARAM_VALUE", true),
            ("REPEATED_CREATION", false),
            ("ORDER_EXISTS", false),
            ("SERVER_ERROR", false),
            ("UNKNOWN_LABEL", false),
        ] {
            let business_error = GateioHttpError::GateioError {
                label: label.to_string(),
                message: String::new(),
            };
            assert_eq!(
                business_error.is_definitive_submit_rejection(),
                rejected,
                "{label}"
            );
            assert_eq!(
                status_error(400, label).is_definitive_submit_rejection(),
                rejected,
                "{label}"
            );
        }

        for error in [
            status_error(408, "BALANCE_NOT_ENOUGH"),
            status_error(503, "BALANCE_NOT_ENOUGH"),
            GateioHttpError::UnexpectedStatus {
                status: 400,
                body: "Bad Request".into(),
            },
            GateioHttpError::Network("connection closed".into()),
            GateioHttpError::Json("invalid acknowledgment".into()),
        ] {
            assert!(!error.is_definitive_submit_rejection(), "{error}");
        }
        assert!(GateioHttpError::MissingCredentials.is_definitive_submit_rejection());
    }

    #[test]
    fn order_lookup_miss_requires_an_order_specific_label() {
        assert!(status_error(404, "ORDER_NOT_FOUND").is_order_not_found());
        assert!(status_error(404, "CLIENT_ID_NOT_FOUND").is_order_not_found());
        assert!(!status_error(404, "NOT_FOUND").is_order_not_found());
        assert!(!status_error(503, "ORDER_NOT_FOUND").is_order_not_found());
    }
}
