"""Gate spot instruments, using the v1 provider and native model objects."""

from decimal import Decimal

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
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


class GateioInstrumentProvider(InstrumentProvider):
    def __init__(self, client, clock, config):
        super().__init__(config=config)
        self.client, self.clock = client, clock

    async def load_all_async(self, filters=None):
        rows = await self.client.request("GET", "/spot/currency_pairs")
        now = self.clock.timestamp_ns()
        instruments = [parse_instrument(row, now) for row in rows if row.get("trade_status") == "tradable"]
        self._instruments = {instrument.id: instrument for instrument in instruments}

    async def load_ids_async(self, instrument_ids, filters=None):
        for instrument_id in instrument_ids:
            await self.load_async(instrument_id, filters)

    async def load_async(self, instrument_id, filters=None):
        if instrument_id.venue.value != "GATEIO" or "-PERP" in instrument_id.symbol.value:
            raise ValueError("Gate adapter currently supports GATEIO spot instruments only")
        row = await self.client.request("GET", "/spot/currency_pairs/" + instrument_id.symbol.value)
        if row.get("trade_status") != "tradable":
            raise ValueError("Configured Gate instrument is not tradable")
        instrument = parse_instrument(row, self.clock.timestamp_ns())
        if instrument.id != instrument_id:
            raise ValueError("Gate returned a different instrument")
        self.add(instrument)
