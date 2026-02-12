from __future__ import annotations

import re

UPPER_TOKENS = {
    "BMLL",
    "MIC",
    "OPOL",
    "ISO",
    "ISIN",
    "FIGI",
    "RIC",
    "VWAP",
    "OHLC",
    "TZ",
    "NBBO",
    "BBO",
    "USD",
    "EUR",
    "CFI",
    "NBB",
    "NBO",
    "L1",
    "L2",
    "L3",
}

DATASET_ALIASES = {
    "trades-plus": "trades_plus",
    "tradesplus": "trades_plus",
    "trades_plus": "trades_plus",
    "marketstate": "market_state",
    "market-state": "market_state",
    "market_state": "market_state",
    "cbbo": "cbbo_equity",
    "cbbo_equity": "cbbo_equity",
}


def _normalize(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def _snake_nat_to_pascal(nat_name: str) -> str:
    parts = [p for p in str(nat_name).split("_") if p]
    out = []
    for p in parts:
        token = p.upper()
        if token == "ID":
            out.append("Id")
        elif token in UPPER_TOKENS:
            out.append(token)
        elif re.fullmatch(r"\d+[A-Z]+", token):
            out.append(token[:-2] + token[-2:].lower() if token.endswith("MS") else token)
        elif token.isdigit():
            out.append(token)
        else:
            out.append(token[:1] + token[1:].lower())
    return "".join(out)


def _pascal_to_nat(name: str) -> str:
    s = str(name)
    s = s.replace("Id", "_ID_")
    for token in sorted(UPPER_TOKENS, key=len, reverse=True):
        s = s.replace(token, f"_{token}_")
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"([A-Za-z])(\d)", r"\1_\2", s)
    s = re.sub(r"_(\d+)_([A-Z])(?=_|$)", r"_\1\2", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s.upper()


def resolve_schema_dataset(name: str) -> str | None:
    raw = str(name).strip()
    if not raw:
        return None
    normalized_text = re.sub(r"[\s\-]+", "_", raw).lower()
    key = _normalize(raw)
    for alias, dataset in DATASET_ALIASES.items():
        if key == _normalize(alias):
            return dataset
    return normalized_text


def db_to_pascal_column(dataset_name: str, nat_name: str) -> str:
    _ = resolve_schema_dataset(dataset_name)
    return _snake_nat_to_pascal(str(nat_name).strip())


def pascal_to_db_column(dataset_name: str, name: str) -> str:
    _ = resolve_schema_dataset(dataset_name)
    return _pascal_to_nat(str(name).strip())
