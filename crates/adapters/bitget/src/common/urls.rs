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

//! URL helpers for Bitget API endpoints.

use crate::common::{
    consts::{BITGET_HTTP_URL, BITGET_WS_PRIVATE_URL, BITGET_WS_PUBLIC_URL},
    enums::BitgetEnvironment,
};

/// Returns the REST base URL for the selected Bitget environment.
#[must_use]
pub const fn bitget_http_base_url(environment: BitgetEnvironment) -> &'static str {
    match environment {
        BitgetEnvironment::Mainnet => BITGET_HTTP_URL,
    }
}

/// Returns the public WebSocket URL for the selected Bitget environment.
#[must_use]
pub const fn bitget_ws_public_url(environment: BitgetEnvironment) -> &'static str {
    match environment {
        BitgetEnvironment::Mainnet => BITGET_WS_PUBLIC_URL,
    }
}

/// Returns the private WebSocket URL for the selected Bitget environment.
#[must_use]
pub const fn bitget_ws_private_url(environment: BitgetEnvironment) -> &'static str {
    match environment {
        BitgetEnvironment::Mainnet => BITGET_WS_PRIVATE_URL,
    }
}
