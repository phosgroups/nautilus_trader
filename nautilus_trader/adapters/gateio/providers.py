"""Gate instruments, using the v1 provider and native model objects."""

from decimal import Decimal

from nautilus_trader.adapters.gateio.config import GateioProductType
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def parse_instrument(row, now):
    symbol = row["id"]
    if symbol != row["base"] + "_" + row["quote"]:
        raise ValueError("Gate instrument symbol does not match its currencies")
    pp, qp = int(row["precision"]), int(row["amount_precision"])
    base, quote = Currency.from_str(row["base"]), Currency.from_str(row["quote"])
    max_base = row.get("max_base_amount")
    max_quote = row.get("max_quote_amount")
    amount_point = row.get("amount_point")
    step = Decimal(10) ** -qp if amount_point in (None, "") else Decimal(str(amount_point))
    if not step.is_finite() or step <= 0 or step != step.quantize(Decimal(10) ** -qp):
        raise ValueError("Invalid Gate amount_point for the configured amount precision")
    fee = Decimal(str(row["fee"])) / 100 if row.get("fee") not in (None, "") else None
    return CurrencyPair(
        instrument_id=InstrumentId.from_str(symbol + ".GATEIO"),
        raw_symbol=Symbol(symbol),
        base_currency=base,
        quote_currency=quote,
        price_precision=pp,
        size_precision=qp,
        price_increment=Price.from_str(f"{Decimal(10) ** -pp:.{pp}f}"),
        size_increment=Quantity.from_str(f"{step:.{qp}f}"),
        maker_fee=fee,
        taker_fee=fee,
        min_quantity=Quantity.from_str(str(row.get("min_base_amount") or "0")),
        min_notional=Money(str(row.get("min_quote_amount") or "0"), quote),
        max_quantity=Quantity.from_str(str(max_base)) if max_base else None,
        max_notional=Money(str(max_quote), quote) if max_quote else None,
        ts_event=now,
        ts_init=now,
        info=row,
    )


def parse_perpetual(row, now):
    symbol = row["name"]
    base, separator, quote = symbol.rpartition("_")
    if not base or not separator or quote != "USDT" or row.get("type", "direct") != "direct":
        raise ValueError(f"Expected a direct Gate USDT perpetual contract, got {symbol}")
    currency = Currency.from_str(quote)
    price_step = Price.from_str(format(Decimal(str(row["order_price_round"])).normalize(), "f"))
    size_step = Quantity.from_str(format(Decimal(str(row["order_size_min"])).normalize(), "f"))
    return CryptoPerpetual(
        instrument_id=InstrumentId.from_str(f"{symbol}-PERP.GATEIO"), raw_symbol=Symbol(symbol),
        base_currency=Currency.from_str(base), quote_currency=currency, settlement_currency=currency,
        is_inverse=False, price_precision=price_step.precision, size_precision=size_step.precision,
        price_increment=price_step, size_increment=size_step, min_quantity=size_step, lot_size=size_step,
        max_quantity=Quantity.from_str(str(row["order_size_max"])),
        multiplier=Quantity.from_str(str(row["quanto_multiplier"])),
        maker_fee=Decimal(str(row["maker_fee_rate"])), taker_fee=Decimal(str(row["taker_fee_rate"])),
        margin_maint=Decimal(str(row["maintenance_rate"])) if row.get("maintenance_rate") else None,
        ts_event=now, ts_init=now, info=row,
    )


class GateioInstrumentProvider(InstrumentProvider):
    def __init__(self, client, clock, config, product_type=GateioProductType.SPOT):
        super().__init__(config=config)
        self.client, self.clock = client, clock
        self.perpetual = GateioProductType(product_type) == GateioProductType.USDT_PERPETUAL
        self._path = "/futures/usdt/contracts" if self.perpetual else "/spot/currency_pairs"

    def _listed(self, row):
        if self.perpetual:
            return (row["name"].endswith("_USDT") and (row.get("status") or "trading").lower() == "trading"
                    and not row.get("in_delisting", False))
        return row.get("trade_status") == "tradable"

    def _parse(self, row, now):
        return parse_perpetual(row, now) if self.perpetual else parse_instrument(row, now)

    async def load_all_async(self, filters=None):
        rows = await self.client.request("GET", self._path)
        now = self.clock.timestamp_ns()
        instruments = [self._parse(row, now) for row in rows if self._listed(row)]
        self._instruments = {instrument.id: instrument for instrument in instruments}

    async def load_ids_async(self, instrument_ids, filters=None):
        for instrument_id in instrument_ids:
            await self.load_async(instrument_id, filters)

    async def load_async(self, instrument_id, filters=None):
        symbol = instrument_id.symbol.value
        if instrument_id.venue.value != "GATEIO" or symbol.endswith("-PERP") != self.perpetual:
            raise ValueError(f"Instrument {instrument_id} does not belong to this Gate product")
        row = await self.client.request("GET", f"{self._path}/{symbol.removesuffix('-PERP')}")
        if not self._listed(row):
            raise ValueError("Configured Gate instrument is not tradable")
        instrument = self._parse(row, self.clock.timestamp_ns())
        if instrument.id != instrument_id:
            raise ValueError("Gate returned a different instrument")
        self.add(instrument)
