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

//! Symbol helpers for Bitget.

use nautilus_model::identifiers::{InstrumentId, Symbol};
use ustr::Ustr;

use crate::common::{consts::BITGET_VENUE, enums::BitgetProductType};

/// Bitget symbol wrapper that knows how to map raw and Nautilus symbols.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BitgetSymbol {
    value: Ustr,
}

impl BitgetSymbol {
    /// Creates a new [`BitgetSymbol`].
    ///
    /// # Errors
    ///
    /// Returns an error if the symbol is empty.
    pub fn new(value: impl AsRef<str>) -> anyhow::Result<Self> {
        let value = value.as_ref();
        anyhow::ensure!(!value.is_empty(), "Bitget symbol cannot be empty");
        Ok(Self {
            value: Ustr::from(value),
        })
    }

    /// Creates a Spot Nautilus symbol from a raw Bitget symbol.
    pub fn spot(raw_symbol: impl AsRef<str>) -> anyhow::Result<Self> {
        Self::new(raw_symbol)
    }

    /// Creates a USDT-FUTURES perpetual Nautilus symbol from a raw Bitget symbol.
    pub fn usdt_perp(raw_symbol: impl AsRef<str>) -> anyhow::Result<Self> {
        Self::new(format!("{}-PERP", raw_symbol.as_ref()))
    }

    /// Returns the raw Bitget symbol.
    #[must_use]
    pub fn raw_symbol(&self) -> &str {
        extract_raw_symbol(self.value.as_str())
    }

    /// Returns the inferred product type.
    #[must_use]
    pub fn product_type(&self) -> BitgetProductType {
        BitgetProductType::from_symbol(self.value.as_str())
    }

    /// Returns the Nautilus instrument ID for this Bitget symbol.
    #[must_use]
    pub fn to_instrument_id(&self) -> InstrumentId {
        InstrumentId::new(Symbol::new(self.value.as_str()), *BITGET_VENUE)
    }
}

impl AsRef<str> for BitgetSymbol {
    fn as_ref(&self) -> &str {
        self.value.as_str()
    }
}

/// Extracts the raw Bitget symbol from a Nautilus Bitget symbol.
#[must_use]
pub fn extract_raw_symbol(symbol: &str) -> &str {
    symbol.strip_suffix("-PERP").unwrap_or(symbol)
}

/// Constructs a Nautilus symbol from a raw Bitget symbol and product type.
#[must_use]
pub fn make_bitget_symbol(raw_symbol: impl AsRef<str>, product_type: BitgetProductType) -> Ustr {
    let raw = raw_symbol.as_ref();
    Ustr::from(&format!("{raw}{}", product_type.suffix()))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn spot_symbol_maps_to_raw_and_instrument_id() {
        let symbol = BitgetSymbol::spot("BTCUSDT").unwrap();

        assert_eq!(symbol.raw_symbol(), "BTCUSDT");
        assert_eq!(symbol.product_type(), BitgetProductType::Spot);
        assert_eq!(symbol.to_instrument_id().to_string(), "BTCUSDT.BITGET");
    }

    #[rstest]
    fn usdt_perp_symbol_maps_to_raw_and_instrument_id() {
        let symbol = BitgetSymbol::usdt_perp("BTCUSDT").unwrap();

        assert_eq!(symbol.raw_symbol(), "BTCUSDT");
        assert_eq!(symbol.product_type(), BitgetProductType::UsdtFutures);
        assert_eq!(symbol.to_instrument_id().to_string(), "BTCUSDT-PERP.BITGET");
    }
}
