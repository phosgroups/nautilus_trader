use crate::common::{
    consts::{
        GATEIO_FUTURES_TESTNET_REST_URL, GATEIO_FUTURES_TESTNET_WS_URL, GATEIO_FUTURES_WS_URL,
        GATEIO_REST_URL, GATEIO_SPOT_TESTNET_REST_URL, GATEIO_SPOT_TESTNET_WS_URL,
        GATEIO_SPOT_WS_URL,
    },
    enums::{GateioEnvironment, GateioProductType},
};

#[must_use]
pub fn http_base_url(value: Option<&str>) -> String {
    let value = value
        .unwrap_or(GATEIO_REST_URL)
        .trim_end_matches('/')
        .to_string();
    if value.ends_with("/api/v4") {
        value
    } else {
        format!("{value}/api/v4")
    }
}

#[must_use]
pub fn http_base_url_for(
    value: Option<&str>,
    product_type: GateioProductType,
    environment: GateioEnvironment,
) -> String {
    if value.is_some() {
        return http_base_url(value);
    }
    match environment {
        GateioEnvironment::Live => http_base_url(None),
        GateioEnvironment::Testnet => http_base_url(Some(match product_type {
            GateioProductType::Spot => GATEIO_SPOT_TESTNET_REST_URL,
            GateioProductType::UsdtPerpetual => GATEIO_FUTURES_TESTNET_REST_URL,
        })),
    }
}

#[must_use]
pub const fn ws_url(
    product_type: GateioProductType,
    environment: GateioEnvironment,
) -> &'static str {
    match (product_type, environment) {
        (GateioProductType::Spot, GateioEnvironment::Live) => GATEIO_SPOT_WS_URL,
        (GateioProductType::Spot, GateioEnvironment::Testnet) => GATEIO_SPOT_TESTNET_WS_URL,
        (GateioProductType::UsdtPerpetual, GateioEnvironment::Live) => GATEIO_FUTURES_WS_URL,
        (GateioProductType::UsdtPerpetual, GateioEnvironment::Testnet) => {
            GATEIO_FUTURES_TESTNET_WS_URL
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_live_and_testnet_urls() {
        assert_eq!(
            http_base_url_for(None, GateioProductType::Spot, GateioEnvironment::Live),
            "https://api.gateio.ws/api/v4"
        );
        assert_eq!(
            http_base_url_for(
                None,
                GateioProductType::UsdtPerpetual,
                GateioEnvironment::Testnet
            ),
            "https://api-testnet.gateapi.io/api/v4"
        );
        assert_eq!(
            http_base_url_for(None, GateioProductType::Spot, GateioEnvironment::Testnet),
            "https://api-testnet.gateapi.io/api/v4"
        );
        assert_eq!(
            ws_url(GateioProductType::Spot, GateioEnvironment::Testnet),
            "wss://ws-testnet.gate.com/v4/ws/spot"
        );
        assert_eq!(
            ws_url(GateioProductType::UsdtPerpetual, GateioEnvironment::Testnet),
            "wss://ws-testnet.gate.com/v4/ws/futures/usdt"
        );
    }

    #[test]
    fn preserves_explicit_url_over_environment() {
        assert_eq!(
            http_base_url_for(
                Some("https://localhost:9000"),
                GateioProductType::Spot,
                GateioEnvironment::Testnet
            ),
            "https://localhost:9000/api/v4"
        );
    }
}
