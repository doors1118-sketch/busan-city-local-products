"""Shared locality resolver configuration for contract-rate consumers."""

from __future__ import annotations

from collections.abc import Collection, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
import sqlite3
from types import MappingProxyType
from typing import Any

from contract_population import CanonicalContract
from locality_snapshot import (
    MissingHistoricalSnapshot,
    REQUIRED_SECTORS,
    SnapshotContentMismatch,
    SnapshotResolver,
    VALID_MODES,
)


LEGACY_CUTOVER = "1970-01-01 00:00:00+09:00"


@dataclass(frozen=True)
class LocalityRateConfig:
    mode: str
    cutover_at: str
    generation_id: str
    baseline_id: str | None

    def __post_init__(self) -> None:
        if self.mode not in VALID_MODES:
            raise ValueError("locality mode must be legacy, shadow, or snapshot")
        if not self.cutover_at:
            raise ValueError("LOCALITY_CUTOVER_AT is required")
        if not self.generation_id:
            raise ValueError("LOCALITY_GENERATION_ID is required")
        if self.mode == "snapshot" and not self.baseline_id:
            raise ValueError("LOCALITY_BASELINE_ID is required in snapshot mode")


@dataclass(frozen=True)
class ActiveLocalityBinding:
    generation_id: str
    baseline_id: str | None


@dataclass(frozen=True)
class LocalitySharedContext:
    mode: str
    cutover_at: str
    generation_id: str
    selected_baseline_id: str | None
    procurement_conn: sqlite3.Connection
    company_conn: sqlite3.Connection
    eligible_agency_codes: frozenset[str]
    read_only: bool
    active_binding: ActiveLocalityBinding | None


def read_locality_config(environment: Mapping[str, str] | None = None) -> LocalityRateConfig:
    """Read command locality settings, including LOCALITY_MODE, exactly once."""
    source = os.environ if environment is None else environment
    mode = source.get("LOCALITY_MODE", "legacy")
    if mode not in VALID_MODES:
        raise ValueError("locality mode must be legacy, shadow, or snapshot")
    cutover_at = source.get("LOCALITY_CUTOVER_AT", "")
    generation_id = source.get("LOCALITY_GENERATION_ID", "")
    baseline_id = source.get("LOCALITY_BASELINE_ID", "") or None
    if mode == "legacy":
        cutover_at = cutover_at or LEGACY_CUTOVER
        generation_id = generation_id or "legacy"
    return LocalityRateConfig(mode, cutover_at, generation_id, baseline_id)


class SectorSnapshotResolver(SnapshotResolver):
    """A SnapshotResolver that accepts contracts for exactly one sector."""

    _PINNED_CONTEXT_FIELDS = frozenset(
        {
            "sector",
            "read_only",
            "mode",
            "cutover_at",
            "_cutover",
            "generation_id",
            "selected_baseline_id",
            "procurement_conn",
            "company_conn",
            "eligible_agency_codes",
        }
    )

    def __init__(self, *args: Any, sector: str, read_only: bool = False, **kwargs: Any) -> None:
        if sector not in REQUIRED_SECTORS:
            raise ValueError(f"unknown locality sector: {sector!r}")
        object.__setattr__(self, "_context_fields_pinned", False)
        self.sector = sector
        self.read_only = bool(read_only)
        super().__init__(*args, **kwargs)
        object.__setattr__(self, "_context_fields_pinned", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if (
            name in self._PINNED_CONTEXT_FIELDS
            and getattr(self, "_context_fields_pinned", False)
        ):
            raise AttributeError(f"resolver context field is immutable: {name}")
        super().__setattr__(name, value)

    def _contract(self, row: CanonicalContract | Mapping[str, Any]) -> CanonicalContract:
        contract = super()._contract(row)
        if contract.sector != self.sector:
            raise SnapshotContentMismatch(
                f"{self.sector} resolver cannot resolve {contract.sector} contracts"
            )
        return contract

    def _stage(self, *args: Any, **kwargs: Any) -> None:
        if self.read_only:
            raise MissingHistoricalSnapshot(
                "read-only locality resolver cannot stage a missing snapshot"
            )
        super()._stage(*args, **kwargs)

    def flush(self) -> int:
        if self.read_only:
            raise MissingHistoricalSnapshot(
                "read-only locality resolver cannot flush snapshots"
            )
        return super().flush()


class LocalityResolverSet(Mapping[str, SectorSnapshotResolver]):
    def __init__(
        self,
        config: LocalityRateConfig,
        resolvers: Mapping[str, SectorSnapshotResolver],
        *,
        active_binding: ActiveLocalityBinding | None = None,
        owned_connections: tuple[sqlite3.Connection, sqlite3.Connection] | None = None,
    ) -> None:
        if set(resolvers) != set(REQUIRED_SECTORS) or len(resolvers) != len(REQUIRED_SECTORS):
            raise ValueError("all four locality sectors are required")
        ordered = {sector: resolvers[sector] for sector in REQUIRED_SECTORS}
        first = ordered[REQUIRED_SECTORS[0]]
        for sector, resolver in ordered.items():
            if not isinstance(resolver, SectorSnapshotResolver):
                raise ValueError(f"{sector} resolver has an unsupported type")
            if resolver.sector != sector:
                raise ValueError(f"{sector} resolver sector does not match its mapping key")
            if resolver.mode != config.mode:
                raise ValueError(f"{sector} resolver mode does not match configuration")
            if resolver.cutover_at != config.cutover_at:
                raise ValueError(f"{sector} resolver cutover does not match configuration")
            if resolver.generation_id != config.generation_id:
                raise ValueError(f"{sector} resolver generation does not match configuration")
            if resolver.selected_baseline_id != config.baseline_id:
                raise ValueError(f"{sector} resolver baseline does not match configuration")
            if resolver.procurement_conn is not first.procurement_conn:
                raise ValueError(f"{sector} resolver procurement connection is not shared")
            if resolver.company_conn is not first.company_conn:
                raise ValueError(f"{sector} resolver company connection is not shared")
            if resolver.read_only != first.read_only:
                raise ValueError(f"{sector} resolver read-only policy is not uniform")
            if resolver.eligible_agency_codes != first.eligible_agency_codes:
                raise ValueError(f"{sector} resolver eligible agency codes are not shared")
        if active_binding is not None and active_binding != ActiveLocalityBinding(
            config.generation_id,
            config.baseline_id,
        ):
            raise ValueError("active generation/baseline binding does not match configuration")
        self._config = config
        self._resolvers = MappingProxyType(ordered)
        self._shared_context = LocalitySharedContext(
            mode=config.mode,
            cutover_at=config.cutover_at,
            generation_id=config.generation_id,
            selected_baseline_id=config.baseline_id,
            procurement_conn=first.procurement_conn,
            company_conn=first.company_conn,
            eligible_agency_codes=frozenset(first.eligible_agency_codes or ()),
            read_only=first.read_only,
            active_binding=active_binding,
        )
        self._owned_connections = owned_connections

    def __getitem__(self, sector: str) -> SectorSnapshotResolver:
        try:
            return self._resolvers[sector]
        except KeyError as error:
            raise ValueError(f"unknown locality sector: {sector!r}") from error

    def __iter__(self) -> Iterator[str]:
        return iter(REQUIRED_SECTORS)

    def __len__(self) -> int:
        return len(REQUIRED_SECTORS)

    @property
    def mode(self) -> str:
        return self.config.mode

    @property
    def config(self) -> LocalityRateConfig:
        return self._config

    @property
    def shared_context(self) -> LocalitySharedContext:
        return self._shared_context

    def require_config(self, config: LocalityRateConfig) -> None:
        if self.config != config:
            raise ValueError("locality resolver context does not match configuration")

    def require_active_read_only(self, config: LocalityRateConfig) -> None:
        self.require_config(config)
        if not self.shared_context.read_only:
            raise ValueError("report/export locality resolvers must be uniformly read-only")
        expected = ActiveLocalityBinding(config.generation_id, config.baseline_id)
        if self.shared_context.active_binding != expected:
            raise ValueError(
                "report/export locality resolvers must be bound to the requested active generation/baseline"
            )

    def close(self) -> None:
        if self._owned_connections is None:
            return
        procurement_conn, company_conn = self._owned_connections
        self._owned_connections = None
        procurement_conn.close()
        company_conn.close()


def build_locality_resolver(
    procurement_conn: sqlite3.Connection,
    company_conn: sqlite3.Connection,
    mode: str,
    sector: str,
    *,
    cutover_at: str,
    generation_id: str,
    selected_baseline_id: str | None,
    eligible_agency_codes: Collection[str],
    read_only: bool = False,
) -> SectorSnapshotResolver:
    return SectorSnapshotResolver(
        procurement_conn,
        company_conn,
        mode=mode,
        cutover_at=cutover_at,
        generation_id=generation_id,
        selected_baseline_id=selected_baseline_id,
        eligible_agency_codes=eligible_agency_codes,
        sector=sector,
        read_only=read_only,
    )


def build_locality_resolvers(
    procurement_conn: sqlite3.Connection,
    company_conn: sqlite3.Connection,
    config: LocalityRateConfig,
    *,
    eligible_agency_codes: Collection[str],
    read_only: bool = False,
    active_binding: ActiveLocalityBinding | None = None,
    owned_connections: tuple[sqlite3.Connection, sqlite3.Connection] | None = None,
) -> LocalityResolverSet:
    agencies = frozenset(str(code).strip() for code in eligible_agency_codes if str(code).strip())
    if not agencies:
        raise ValueError("eligible agency codes are required")
    resolvers = {
        sector: build_locality_resolver(
            procurement_conn,
            company_conn,
            config.mode,
            sector,
            cutover_at=config.cutover_at,
            generation_id=config.generation_id,
            selected_baseline_id=config.baseline_id,
            eligible_agency_codes=agencies,
            read_only=read_only,
        )
        for sector in REQUIRED_SECTORS
    }
    return LocalityResolverSet(
        config,
        resolvers,
        active_binding=active_binding,
        owned_connections=owned_connections,
    )


def _connect(path: str | Path, *, read_only: bool) -> sqlite3.Connection:
    if not read_only:
        return sqlite3.connect(str(path))
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


@contextmanager
def open_locality_resolvers(
    procurement_path: str | Path,
    company_path: str | Path,
    config: LocalityRateConfig,
    *,
    eligible_agency_codes: Collection[str],
    read_only: bool = False,
    active_binding: ActiveLocalityBinding | None = None,
) -> Iterator[LocalityResolverSet]:
    procurement_conn = _connect(procurement_path, read_only=read_only)
    try:
        company_conn = _connect(company_path, read_only=read_only)
    except Exception:
        procurement_conn.close()
        raise
    try:
        resolver_set = build_locality_resolvers(
            procurement_conn,
            company_conn,
            config,
            eligible_agency_codes=eligible_agency_codes,
            read_only=read_only,
            active_binding=active_binding,
            owned_connections=(procurement_conn, company_conn),
        )
    except Exception:
        procurement_conn.close()
        company_conn.close()
        raise
    try:
        yield resolver_set
    finally:
        resolver_set.close()
