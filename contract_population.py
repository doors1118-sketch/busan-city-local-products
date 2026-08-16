"""Versioned canonical contract identities shared by locality consumers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import re
import sqlite3
from typing import Any, Iterator, Mapping
from zoneinfo import ZoneInfo


SEOUL = ZoneInfo("Asia/Seoul")
ITERATOR_VERSION = "canonical_contract_v1"

_SECTORS = {
    "공사": ("공사", "cnstwk_cntrct", False),
    "cnstwk": ("공사", "cnstwk_cntrct", False),
    "cnstwk_cntrct": ("공사", "cnstwk_cntrct", False),
    "construction": ("공사", "cnstwk_cntrct", False),
    "용역": ("용역", "servc_cntrct", False),
    "servc": ("용역", "servc_cntrct", False),
    "servc_cntrct": ("용역", "servc_cntrct", False),
    "service": ("용역", "servc_cntrct", False),
    "물품": ("물품", "thng_cntrct", False),
    "thng": ("물품", "thng_cntrct", False),
    "thng_cntrct": ("물품", "thng_cntrct", False),
    "goods": ("물품", "thng_cntrct", False),
    "쇼핑몰": ("쇼핑몰", "shopping_cntrct", True),
    "shopping": ("쇼핑몰", "shopping_cntrct", True),
    "shopping_cntrct": ("쇼핑몰", "shopping_cntrct", True),
}


class CanonicalContractError(RuntimeError):
    pass


class MissingContractIdentity(CanonicalContractError):
    pass


class MissingSupplierIdentity(CanonicalContractError):
    pass


class CanonicalContractCollision(CanonicalContractError):
    def __init__(self, sector: str, contract_key: str, revision: str, fingerprints: list[str]):
        self.sector = sector
        self.contract_key = contract_key
        self.contract_revision = revision
        self.fingerprints = tuple(sorted(fingerprints))
        super().__init__(
            f"canonical contract collision for {sector}/{contract_key}/{revision}: "
            + ",".join(self.fingerprints)
        )


@dataclass(frozen=True, order=True)
class CanonicalSupplier:
    bizno: str
    share_pct: float


@dataclass(frozen=True)
class CanonicalContract:
    sector: str
    contract_key: str
    contract_revision: str
    agency: str
    amount_won: int
    contract_date: str
    suppliers: tuple[CanonicalSupplier, ...]
    source_order: tuple[str, ...] = field(default=(), compare=False, repr=False)
    source_row: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @property
    def identity(self) -> tuple[str, str, str]:
        return self.sector, self.contract_key, self.contract_revision


def normalize_contract_key(value: Any) -> str:
    text = _text(value)
    return re.sub(r"[-\s]", "", text)


def _text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "nat"} else text


def _first_text(*values: Any) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _normalize_bizno(value: Any) -> str:
    return "".join(character for character in _text(value) if character.isdigit())


def _decimal(value: Any) -> Decimal:
    text = _text(value).replace(",", "")
    if not text:
        return Decimal(0)
    try:
        number = Decimal(text)
    except InvalidOperation as error:
        raise CanonicalContractError(f"invalid contract number: {value!r}") from error
    if not number.is_finite():
        return Decimal(0)
    return number


def _amount_won(row: Mapping[str, Any], shopping: bool) -> int:
    if shopping:
        amount = _decimal(row.get("prdctAmt"))
    else:
        amount = _decimal(row.get("thtmCntrctAmt"))
        if amount == 0:
            amount = _decimal(row.get("totCntrctAmt"))
    return int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _normalize_date(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    if len(text) == 8 and text.isdigit():
        try:
            return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return text
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        try:
            return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return text
    formats = ("%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y-%m-%d %H:%M:%S")
    parsed = None
    for format_string in formats:
        try:
            parsed = datetime.strptime(text, format_string).replace(tzinfo=SEOUL)
            break
        except ValueError:
            pass
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=SEOUL)
        else:
            parsed = parsed.astimezone(SEOUL)
    return parsed.isoformat(timespec="seconds")


def _governing_date(row: Mapping[str, Any], shopping: bool) -> str:
    if shopping:
        return _normalize_date(row.get("dlvrReqRcptDate"))
    return _normalize_date(_first_text(row.get("cntrctCnclsDate"), row.get("cntrctDate")))


def _parse_suppliers(row: Mapping[str, Any], shopping: bool, sector: str) -> tuple[CanonicalSupplier, ...]:
    if shopping:
        bizno = _normalize_bizno(row.get("cntrctCorpBizno"))
        if not bizno:
            raise MissingSupplierIdentity(f"{sector} contract has no normalized supplier business number")
        return (CanonicalSupplier(bizno, 100.0),)

    aggregated: dict[str, Decimal] = {}
    raw = _text(row.get("corpList"))
    for chunk in raw.split("[")[1:]:
        parts = chunk.split("]", 1)[0].split("^")
        if len(parts) < 10:
            continue
        bizno = _normalize_bizno(parts[9])
        if not bizno:
            raise MissingSupplierIdentity(f"{sector} contract has no normalized supplier business number")
        share = _decimal(parts[6]) if _text(parts[6]) else Decimal(0)
        aggregated[bizno] = aggregated.get(bizno, Decimal(0)) + share
    if not aggregated:
        raise MissingSupplierIdentity(f"{sector} contract has no supplier entries")
    total = sum(aggregated.values(), Decimal(0))
    if total == 0:
        equal = Decimal(100) / Decimal(len(aggregated))
        aggregated = {bizno: equal for bizno in aggregated}
    elif total > Decimal("100.1"):
        aggregated = {bizno: share / total * Decimal(100) for bizno, share in aggregated.items()}
    return tuple(
        CanonicalSupplier(bizno, float(share))
        for bizno, share in sorted(aggregated.items())
    )


def _shopping_revision(value: Any) -> str:
    text = _text(value)
    if not text:
        return "0"
    number = _decimal(text)
    integral = number.to_integral_value()
    if number != integral:
        raise MissingContractIdentity(f"shopping change order is not an integer: {value!r}")
    return str(int(integral))


def canonical_contract_from_row(row: Mapping[str, Any], sector: str) -> CanonicalContract:
    try:
        canonical_sector, _, shopping = _SECTORS[_text(sector).lower() if _text(sector).isascii() else _text(sector)]
    except KeyError as error:
        raise ValueError(f"unknown contract sector: {sector!r}") from error
    values = dict(row)
    if shopping:
        delivery = normalize_contract_key(values.get("dlvrReqNo"))
        product = normalize_contract_key(values.get("prdctSno"))
        if not delivery or not product:
            raise MissingContractIdentity(f"{canonical_sector} contract has a missing delivery or product key")
        contract_key = f"{delivery}:{product}"
        revision = _shopping_revision(values.get("dlvrReqChgOrd"))
    else:
        decision = normalize_contract_key(values.get("dcsnCntrctNo"))
        if decision:
            contract_key, revision = decision[:-2], decision[-2:]
            if not contract_key:
                raise MissingContractIdentity(
                    f"{canonical_sector} contract decision number has no family key"
                )
        else:
            contract_key = normalize_contract_key(values.get("untyCntrctNo"))
            revision = ""
        if not contract_key:
            raise MissingContractIdentity(f"{canonical_sector} contract has no canonical contract key")
    agency = normalize_contract_key(_first_text(values.get("dminsttCd"), values.get("cntrctInsttCd")))
    contract_date = _governing_date(values, shopping)
    suppliers = _parse_suppliers(values, shopping, canonical_sector)
    source_order = tuple(
        _normalize_date(values.get(name))
        for name in ("lastUpdtDt", "updDt", "chgDt", "rgstDt", "cntrctDate")
    ) + (contract_date, revision)
    return CanonicalContract(
        canonical_sector,
        contract_key,
        revision,
        agency,
        _amount_won(values, shopping),
        contract_date,
        suppliers,
        source_order,
        values,
    )


def _share_micros(share_pct: float) -> int:
    return int((Decimal(str(share_pct)) * Decimal(1_000_000)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def content_fingerprint(contract: CanonicalContract) -> str:
    payload = {
        "agency": contract.agency,
        "amount_won": contract.amount_won,
        "contract_date": contract.contract_date,
        "suppliers": [[supplier.bizno, _share_micros(supplier.share_pct)] for supplier in sorted(contract.suppliers)],
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _revision_rank(revision: str) -> tuple[int, int | str]:
    return (1, int(revision)) if revision.isdigit() else (0, revision)


def _in_date_range(contract_date: str, date_range: tuple[Any, Any] | None) -> bool:
    if date_range is None:
        return True
    if len(date_range) != 2:
        raise ValueError("date_range must contain inclusive start and end values")
    if not contract_date:
        return False
    day = contract_date[:10]
    start = _normalize_date(date_range[0])[:10]
    end = _normalize_date(date_range[1])[:10]
    return (not start or day >= start) and (not end or day <= end)


def iter_canonical_contracts(
    conn: sqlite3.Connection,
    sector: str,
    date_range: tuple[Any, Any] | None = None,
) -> Iterator[CanonicalContract]:
    """Yield the one latest, collision-free canonical revision per contract family."""
    key = _text(sector)
    lookup = key.lower() if key.isascii() else key
    try:
        canonical_sector, table, _ = _SECTORS[lookup]
    except KeyError as error:
        raise ValueError(f"unknown contract sector: {sector!r}") from error
    cursor = conn.execute(f"SELECT * FROM [{table}]")
    columns = [description[0] for description in cursor.description]
    by_identity: dict[tuple[str, str, str], list[CanonicalContract]] = {}
    for source in cursor:
        contract = canonical_contract_from_row(dict(zip(columns, source)), canonical_sector)
        if not _in_date_range(contract.contract_date, date_range):
            continue
        by_identity.setdefault(contract.identity, []).append(contract)

    canonical_revisions: list[CanonicalContract] = []
    for identity, candidates in sorted(by_identity.items()):
        by_fingerprint: dict[str, list[CanonicalContract]] = {}
        for candidate in candidates:
            by_fingerprint.setdefault(content_fingerprint(candidate), []).append(candidate)
        if len(by_fingerprint) != 1:
            raise CanonicalContractCollision(identity[0], identity[1], identity[2], list(by_fingerprint))
        canonical_revisions.append(max(candidates, key=lambda row: (row.source_order, content_fingerprint(row))))

    by_family: dict[tuple[str, str], list[CanonicalContract]] = {}
    for contract in canonical_revisions:
        by_family.setdefault((contract.sector, contract.contract_key), []).append(contract)
    selected = [
        max(revisions, key=lambda row: (_revision_rank(row.contract_revision), row.source_order, content_fingerprint(row)))
        for revisions in by_family.values()
    ]
    yield from sorted(selected, key=lambda row: (row.sector, row.contract_key, _revision_rank(row.contract_revision)))
