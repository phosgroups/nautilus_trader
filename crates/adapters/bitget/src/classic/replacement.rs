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

//! Identity tracking for Classic futures cancel-replace amendments.
//!
//! The original Nautilus client ID survives replacements. The venue client ID carries
//! that ID plus a unique suffix so REST reconciliation can recover it after a restart.

use std::collections::{HashMap, HashSet};

use nautilus_core::UnixNanos;
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    orders::{Order, OrderAny},
    reports::OrderStatusReport,
    types::{Price, Quantity},
};

use crate::http::models::{BitgetFill, BitgetOrderStatus};

const MARKER: &str = "-NTR-";

pub(crate) fn original_client_id(value: &str) -> &str {
    match value.rsplit_once(MARKER) {
        Some((original, suffix))
            if suffix.len() == 16 && suffix.bytes().all(|b| b.is_ascii_hexdigit()) =>
        {
            original
        }
        _ => value,
    }
}

#[derive(Debug)]
struct Replacement {
    order: OrderAny,
    client_oid: String,
    old_venue_id: String,
    new_venue_id: Option<String>,
    promoted: bool,
    is_plan: bool,
    previous: Option<Box<Self>>,
    quantity: Quantity,
    price: Option<Price>,
}

#[derive(Debug, Default)]
pub(crate) struct Replacements {
    pending: HashMap<String, Replacement>,
    superseded: HashSet<String>,
    restored: HashMap<String, OrderAny>,
}

impl Replacements {
    pub(crate) fn restore(&mut self, orders: Vec<OrderAny>) {
        for order in orders {
            for id in order.venue_order_ids() {
                if Some(*id) != order.venue_order_id() {
                    self.superseded.insert(id.to_string());
                }
            }
            if order.trigger_price().is_some()
                && let Some(id) = order.venue_order_id()
            {
                self.register_plan(order.clone(), id.as_str());
            }
            self.restored
                .insert(order.client_order_id().to_string(), order);
        }
    }

    fn recover(&mut self, client: &str, venue: Option<&str>) {
        let original = original_client_id(client);
        if original == client
            || self.pending.contains_key(original)
            || venue.is_some_and(|id| self.superseded.contains(id))
        {
            return;
        }
        let Some(order) = self.restored.get(original).cloned() else {
            return;
        };
        let Some(old) = order.venue_order_id() else {
            return;
        };
        self.pending.insert(
            original.to_owned(),
            Replacement {
                quantity: order.quantity(),
                price: order.price(),
                order,
                client_oid: client.to_owned(),
                old_venue_id: old.to_string(),
                new_venue_id: venue.map(str::to_owned),
                promoted: venue == Some(old.as_str()),
                is_plan: false,
                previous: None,
            },
        );
    }

    pub(crate) fn query_identity(
        &self,
        client: Option<&str>,
        venue: Option<&str>,
    ) -> (Option<String>, Option<String>) {
        if let Some(row) = client.and_then(|id| self.pending.get(id)) {
            return (row.new_venue_id.clone(), Some(row.client_oid.clone()));
        }
        if let Some(order) = client.and_then(|id| self.restored.get(id)) {
            return (
                order
                    .venue_order_id()
                    .map(|id| id.to_string())
                    .or_else(|| venue.map(str::to_owned)),
                client.map(str::to_owned),
            );
        }
        (venue.map(str::to_owned), client.map(str::to_owned))
    }

    pub(crate) fn register_plan(&mut self, order: OrderAny, id: &str) {
        let client = order.client_order_id().to_string();
        self.pending
            .entry(client.clone())
            .or_insert_with(|| Replacement {
                quantity: order.quantity(),
                price: order.price(),
                order,
                client_oid: client,
                old_venue_id: id.to_owned(),
                new_venue_id: Some(id.to_owned()),
                promoted: true,
                is_plan: true,
                previous: None,
            });
    }

    pub(crate) fn begin(
        &mut self,
        order: OrderAny,
        nonce: u64,
        quantity: Option<Quantity>,
        price: Option<Price>,
    ) -> anyhow::Result<String> {
        let original = order.client_order_id().to_string();
        anyhow::ensure!(
            !self.pending.get(&original).is_some_and(|r| !r.promoted),
            "Classic order already has an unconfirmed replacement"
        );
        let old_venue_id = order
            .venue_order_id()
            .ok_or_else(|| anyhow::anyhow!("Classic modify requires an accepted venue order ID"))?
            .to_string();
        let client_oid = format!("{original}{MARKER}{nonce:016x}");
        anyhow::ensure!(
            client_oid.len() <= 64,
            "Classic replacement clientOid exceeds 64 bytes"
        );
        let previous = self.pending.remove(&original).map(Box::new);
        self.pending.insert(
            original,
            Replacement {
                quantity: quantity.unwrap_or(order.quantity()),
                price: price.or(order.price()),
                order,
                client_oid: client_oid.clone(),
                old_venue_id,
                new_venue_id: None,
                promoted: false,
                is_plan: false,
                previous,
            },
        );
        Ok(client_oid)
    }

    pub(crate) fn abort(&mut self, original: &str) {
        if self.pending.get(original).is_some_and(|r| !r.promoted)
            && let Some(row) = self.pending.remove(original)
            && let Some(previous) = row.previous
        {
            self.pending.insert(original.to_owned(), *previous);
        }
    }

    /// Resolve commands carrying an old venue ID to the latest confirmed replacement.
    /// While acceptance is unknown, reject rather than cancel the wrong leg.
    pub(crate) fn resolve(
        &self,
        client: Option<&str>,
        venue: Option<&str>,
    ) -> anyhow::Result<(Option<String>, Option<String>)> {
        let row = client.and_then(|id| self.pending.get(id)).or_else(|| {
            self.pending
                .values()
                .find(|r| venue == Some(r.old_venue_id.as_str()))
        });
        if let Some(row) = row {
            anyhow::ensure!(
                row.promoted,
                "Classic replacement is awaiting venue confirmation"
            );
            return Ok((row.new_venue_id.clone(), Some(row.client_oid.clone())));
        }
        Ok((venue.map(str::to_owned), client.map(str::to_owned)))
    }

    /// An asynchronous replacement rejection leaves the old leg canceled at the venue.
    pub(crate) fn handle_rejection(
        &mut self,
        status: &mut BitgetOrderStatus,
        emitter: &ExecutionEventEmitter,
        ts: UnixNanos,
    ) {
        if !matches!(
            status.status.as_deref(),
            Some("rejected" | "failed" | "fail")
        ) {
            return;
        }
        let Some(client) = status.client_oid.as_deref() else {
            return;
        };
        let original = original_client_id(client).to_owned();
        if !self
            .pending
            .get(&original)
            .is_some_and(|r| !r.promoted && !r.is_plan && r.client_oid == client)
        {
            return;
        }
        let Some(row) = self.pending.remove(&original) else {
            return;
        };
        emitter.emit_order_modify_rejected_event(
            row.order.strategy_id(),
            row.order.instrument_id(),
            row.order.client_order_id(),
            Some(row.old_venue_id.as_str().into()),
            "Classic replacement rejected after the old order was canceled",
            ts,
        );
        status.order_id = Some(row.old_venue_id);
        status.client_oid = Some(original);
        status.status = Some("canceled".to_string());
    }

    /// Suppress only the cancel leg owned by this adapter, retaining fills/rejections.
    pub(crate) fn normalize(&mut self, row: &mut BitgetOrderStatus) -> bool {
        if let Some(client) = row.client_oid.as_deref() {
            self.recover(client, row.order_id.as_deref());
            let original = original_client_id(client);
            if let Some(replacement) = self.pending.get_mut(original)
                && replacement.client_oid == client
            {
                if replacement.is_plan
                    && replacement.new_venue_id.as_deref()
                        != Some(replacement.old_venue_id.as_str())
                    && row.order_id.as_deref() == Some(replacement.old_venue_id.as_str())
                {
                    return false;
                }
                if replacement.is_plan && replacement.new_venue_id != row.order_id {
                    replacement.promoted = false;
                }
                replacement.new_venue_id.clone_from(&row.order_id);
            }
            row.client_oid = Some(original.to_owned());
        }
        let canceled = matches!(row.status.as_deref(), Some("canceled" | "cancelled"));
        if canceled && let Some(id) = row.order_id.as_deref() {
            return !self.superseded.contains(id)
                && !self.pending.values().any(|r| {
                    r.old_venue_id == id && (!r.is_plan || r.new_venue_id.as_deref() != Some(id))
                });
        }
        true
    }

    pub(crate) fn normalize_fill(
        &mut self,
        fill: &mut BitgetFill,
        emitter: &ExecutionEventEmitter,
        ts: UnixNanos,
    ) {
        let Some(client) = fill.client_oid.as_deref() else {
            return;
        };
        self.recover(client, fill.order_id.as_deref());
        let original = original_client_id(client).to_owned();
        if let Some(row) = self.pending.get_mut(&original)
            && row.client_oid == client
            && let Some(id) = fill.order_id.as_deref()
            && (!row.promoted || row.is_plan && row.new_venue_id.as_deref() != Some(id))
        {
            emitter.emit_order_updated(
                &row.order,
                id.into(),
                row.quantity,
                row.price,
                None,
                None,
                ts,
            );
            row.new_venue_id = Some(id.to_owned());
            row.promoted = true;
            row.previous = None;
            self.superseded.insert(row.old_venue_id.clone());
        }
        fill.client_oid = Some(original);
    }

    pub(crate) fn promote(
        &mut self,
        report: &OrderStatusReport,
        emitter: &ExecutionEventEmitter,
        ts: UnixNanos,
    ) {
        let Some(client) = report.client_order_id else {
            return;
        };
        let Some(row) = self.pending.get_mut(client.as_str()) else {
            return;
        };
        if row.promoted || row.new_venue_id.as_deref() != Some(report.venue_order_id.as_str()) {
            return;
        }
        // OrderUpdated promotes the new venue ID before any fill/status report is delivered.
        emitter.emit_order_updated(
            &row.order,
            report.venue_order_id,
            report.quantity,
            report.price,
            report.trigger_price,
            None,
            ts,
        );
        row.promoted = true;
        row.previous = None;
        self.superseded.insert(row.old_venue_id.clone());
    }
}
