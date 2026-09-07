use nautilus_model::identifiers::{InstrumentId, Symbol};

use crate::common::{consts::GATEIO_VENUE, enums::GateioProductType};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GateioSymbol(String);

impl GateioSymbol {
    pub fn new(raw: impl AsRef<str>, product_type: GateioProductType) -> anyhow::Result<Self> {
        let raw = raw.as_ref().trim();
        anyhow::ensure!(!raw.is_empty(), "Gate.io symbol cannot be empty");
        let raw = raw.trim_end_matches(".GATEIO").trim_end_matches("-PERP");
        Ok(Self(format!(
            "{}{suffix}",
            raw,
            suffix = product_type.suffix()
        )))
    }

    pub fn spot(raw: impl AsRef<str>) -> anyhow::Result<Self> {
        Self::new(raw, GateioProductType::Spot)
    }

    pub fn usdt_perpetual(raw: impl AsRef<str>) -> anyhow::Result<Self> {
        Self::new(raw, GateioProductType::UsdtPerpetual)
    }

    #[must_use]
    pub fn from_instrument_id(instrument_id: InstrumentId) -> Self {
        let raw = instrument_id.symbol.as_str();
        Self(format!("{raw}"))
    }

    #[must_use]
    pub fn raw_symbol(&self) -> &str {
        self.0.trim_end_matches("-PERP")
    }

    #[must_use]
    pub fn product_type(&self) -> GateioProductType {
        GateioProductType::from_symbol(&self.0)
    }

    #[must_use]
    pub fn instrument_id(&self) -> InstrumentId {
        InstrumentId::new(Symbol::new(&self.0), *GATEIO_VENUE)
    }
}

#[must_use]
pub fn raw_symbol(instrument_id: InstrumentId) -> String {
    instrument_id
        .symbol
        .as_str()
        .trim_end_matches("-PERP")
        .to_string()
}

#[must_use]
pub fn extract_raw_symbol(value: &str) -> &str {
    value.trim_end_matches(".GATEIO").trim_end_matches("-PERP")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_spot_and_perpetual_symbols() {
        assert_eq!(
            GateioSymbol::spot("BTC_USDT")
                .unwrap()
                .instrument_id()
                .to_string(),
            "BTC_USDT.GATEIO"
        );
        assert_eq!(
            GateioSymbol::usdt_perpetual("BTC_USDT")
                .unwrap()
                .instrument_id()
                .to_string(),
            "BTC_USDT-PERP.GATEIO"
        );
    }
}
