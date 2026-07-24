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

//! Enumerations used by the Bitget adapter.

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter, EnumString};

/// Bitget environments supported by this adapter.
#[derive(
    Copy,
    Clone,
    Debug,
    strum::Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.core.nautilus_pyo3.bitget",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetEnvironment {
    /// Bitget mainnet.
    Mainnet,
}

/// Bitget Classic product types supported by this adapter.
#[derive(
    Copy,
    Clone,
    Debug,
    strum::Display,
    PartialEq,
    Eq,
    Hash,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
#[strum(serialize_all = "SCREAMING-KEBAB-CASE", ascii_case_insensitive)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        eq,
        eq_int,
        rename_all = "SCREAMING_SNAKE_CASE",
        module = "nautilus_trader.core.nautilus_pyo3.bitget",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.bitget")
)]
pub enum BitgetProductType {
    /// Bitget Spot.
    Spot,
    /// Bitget USDT-margined futures/perpetuals.
    #[serde(rename = "USDT-FUTURES")]
    #[strum(serialize = "USDT-FUTURES", serialize = "usdt-futures")]
    UsdtFutures,
}

impl BitgetProductType {
    /// Returns the Bitget REST API `category` string.
    #[must_use]
    pub const fn as_api_str(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
            Self::UsdtFutures => "USDT-FUTURES",
        }
    }

    /// Returns the Bitget UTA public WebSocket `instType` string.
    #[must_use]
    pub const fn as_ws_public_inst_type(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::UsdtFutures => "usdt-futures",
        }
    }

    /// Parses a Bitget API `category`/`instType` string.
    #[must_use]
    pub fn from_api_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "SPOT" => Some(Self::Spot),
            "USDT-FUTURES" | "USDT_FUTURES" => Some(Self::UsdtFutures),
            _ => None,
        }
    }

    /// Returns the Nautilus symbol suffix for this product type.
    #[must_use]
    pub const fn suffix(self) -> &'static str {
        match self {
            Self::Spot => "",
            Self::UsdtFutures => "-PERP",
        }
    }

    /// Returns `true` if the product is a derivatives market.
    #[must_use]
    pub const fn is_derivative(self) -> bool {
        matches!(self, Self::UsdtFutures)
    }

    /// Infers a product type from a Nautilus symbol.
    #[must_use]
    pub fn from_symbol(symbol: &str) -> Self {
        if symbol.ends_with("-PERP") {
            Self::UsdtFutures
        } else {
            Self::Spot
        }
    }
}
