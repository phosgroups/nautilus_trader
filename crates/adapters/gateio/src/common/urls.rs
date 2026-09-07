use crate::common::{
    consts::{GATEIO_FUTURES_WS_URL, GATEIO_REST_URL, GATEIO_SPOT_WS_URL},
    enums::GateioProductType,
};

#[must_use]
pub fn http_base_url(value: Option<&str>) -> String {
    value
        .unwrap_or(GATEIO_REST_URL)
        .trim_end_matches('/')
        .to_string()
}

#[must_use]
pub const fn ws_url(product_type: GateioProductType) -> &'static str {
    match product_type {
        GateioProductType::Spot => GATEIO_SPOT_WS_URL,
        GateioProductType::UsdtPerpetual => GATEIO_FUTURES_WS_URL,
    }
}
