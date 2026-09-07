use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

#[derive(
    Copy, Clone, Debug, Display, PartialEq, Eq, Hash, AsRefStr, EnumString, Serialize, Deserialize,
)]
#[serde(rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase", ascii_case_insensitive)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.core.nautilus_pyo3.gateio",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.gateio")
)]
pub enum GateioProductType {
    Spot,
    #[serde(rename = "UsdtPerpetual")]
    #[strum(serialize = "USDT-PERPETUAL", serialize = "usdt-perpetual")]
    UsdtPerpetual,
}

impl GateioProductType {
    #[must_use]
    pub const fn is_derivative(self) -> bool {
        matches!(self, Self::UsdtPerpetual)
    }

    #[must_use]
    pub const fn suffix(self) -> &'static str {
        match self {
            Self::Spot => "",
            Self::UsdtPerpetual => "-PERP",
        }
    }

    #[must_use]
    pub fn from_symbol(symbol: &str) -> Self {
        let symbol = symbol.strip_suffix(".GATEIO").unwrap_or(symbol);
        if symbol.ends_with("-PERP") {
            Self::UsdtPerpetual
        } else {
            Self::Spot
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_product_type_from_symbol_and_instrument_id() {
        assert_eq!(
            GateioProductType::from_symbol("BTC_USDT"),
            GateioProductType::Spot
        );
        assert_eq!(
            GateioProductType::from_symbol("BTC_USDT.GATEIO"),
            GateioProductType::Spot
        );
        assert_eq!(
            GateioProductType::from_symbol("BTC_USDT-PERP"),
            GateioProductType::UsdtPerpetual
        );
        assert_eq!(
            GateioProductType::from_symbol("BTC_USDT-PERP.GATEIO"),
            GateioProductType::UsdtPerpetual
        );
    }
}
