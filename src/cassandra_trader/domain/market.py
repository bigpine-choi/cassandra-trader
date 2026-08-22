"""Market-domain value objects."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TradingPair:
    """Exchange-independent representation of a trading pair."""

    code: str
    korean_name: str
    english_name: str
    is_warning: bool

    def __post_init__(self) -> None:
        quote, separator, base = self.code.partition("-")

        if not separator or not quote or not base:
            raise ValueError(f"Invalid trading-pair code: {self.code}")

    @property
    def quote_currency(self) -> str:
        """Return the quote currency, such as KRW."""

        return self.code.split("-", maxsplit=1)[0]

    @property
    def base_currency(self) -> str:
        """Return the traded asset, such as BTC."""

        return self.code.split("-", maxsplit=1)[1]
