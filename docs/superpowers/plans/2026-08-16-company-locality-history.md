# Company Locality History Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the current Busan supplier population accurate for inbound, outbound, and head-office changes while preserving every already-published contract locality decision and proving rate and disk impact before activation.

**Architecture:** Add a focused lifecycle module to the existing supplier SQLite database, a contract-supplier snapshot module to the procurement database, and strict supplier-specific synchronization state. Run legacy and snapshot calculations side by side, generate rate/capacity audit reports, and activate snapshot reads only after exact baseline and seven-day shadow gates pass.

**Tech Stack:** Python 3, SQLite, pandas/numpy already used by the repository, stdlib `unittest`, existing NCP backup and cron infrastructure.

## Global Constraints

- Existing contract locality decisions remain fixed after baseline creation.
- A supplier moving into Busan is local only from the confirmed inbound effective time; pre-inbound contracts remain non-local.
- A supplier moving outside Busan or becoming a branch remains local for frozen historical contracts but is not current-local for future contracts.
- Do not change the prefix/address/branch classification policy in this feature; isolate lifecycle impact from policy impact.
- Modes are exactly `legacy`, `shadow`, and `snapshot`; rollout defaults to `shadow`.
- No partial supplier API batch may be committed.
- Historical baseline coverage must be exactly 100% by canonical revision, normalized supplier, aggregated share, amount, and content fingerprint; pre-cutover fallback and unknown amount are forbidden.
- Baseline overall/sector order and local amounts must match exactly after rounding, with zero canonical-contract count difference.
- Block activation above 0.1 percentage-point overall difference, 0.3 percentage-point sector difference, 0.1% unknown denominator, or unexplained 1.0 percentage-point agency difference.
- Block migration at projected filesystem usage of 80% or more, or post-migration free space below 2 GiB or 20%, whichever requires more free space.
- Do not store full API payloads in audit tables or generated Git-tracked files.
- Normalize source and contract times to Asia/Seoul; same-day date-only transition boundaries are unknown until explicitly resolved.
- All mutation, cache publication, and backup paths use one maintenance lock and generation protocol.
- After snapshot activation, rollback keeps snapshot reads and last-known-good caches while pausing writes; it never returns to legacy membership reads.
- Visibility changes only through one fsynced active-generation pointer after an immutable two-cache bundle and prepared snapshots are complete; a partial generation never becomes active.
- Every database writer is inventory-classified and either uses the guarded write session, fails closed after cutover, or is explicitly not deployed.
- Cache manifests pin source `data_generation` clocks only; activation, audit, and output bookkeeping use a separate `control_revision` and cannot stale prepared data.
- Use `apply_patch` for manual source edits and preserve unrelated worktree changes.

## File Map

- Create `company_locality.py`: supplier status schema, transitions, events, job state, and rolling bucket selection.
- Create `maintenance_lock.py`: cross-process lock, guarded write sessions, persisted write fence, generation stamps, SQLite transaction/checkpoint helpers.
- Create `locality_quiesce.py`: maintenance-window process/file-handle verification and dual-exclusive transition helpers.
- Create `locality_generation.py`: prepared/active/abandoned generation lifecycle, immutable cache bundles, pointer activation, and crash recovery.
- Create `company_locality_admin.py`: audited conflict resolution, snapshot correction, and fence commands.
- Create `locality_writer_inventory.json`: classification of every script that writes either production database.
- Create `locality_consumer_inventory.json`: classification of every direct supplier-master reader.
- Create `company_sync.py`: strict paginated API collection with dependency injection for tests.
- Create `contract_population.py`: canonical identities, deterministic row selection, supplier aggregation, and content fingerprints.
- Create `locality_snapshot.py`: canonical contract keys, frozen decisions, mode-aware resolver, and snapshot coverage.
- Create `company_locality_audit.py`: capacity preflight, staging baseline, legacy/snapshot comparison, and activation gates.
- Modify `daily_pipeline_sync.py`: delegate supplier synchronization and keep supplier catch-up independent from contract success.
- Modify `core_calc.py`: allow a bound locality resolver while preserving legacy defaults.
- Modify `build_api_cache.py`, `build_monthly_cache.py`, `rate_calc_db.py`, and `export_excel.py`: construct and pass the same resolver.
- Modify `bootstrap_master_data.py` and current-supplier cache/recommendation loaders: filter `active_local` without changing frozen rate snapshots.
- Modify `api_server.py`, `dashboard.py`, and deployed `server_sync` equivalents: load one active generation per request and use the shared current-local relation.
- Modify or disable all production/manual database writers identified by the inventory, including `collect_busan_awards.py`, `update_servc_site.py`, and `import_manual_contracts.py`.
- Modify `monitoring_regression_check.py`: supplier sync, snapshot coverage, and storage checks.
- Modify `HANDOVER.md` and `docs/REMOTE_MAINTENANCE.md`: cron, shadow rollout, reports, and rollback.
- Create focused `unittest` modules listed in each task.

---

### Task 1: Maintenance Coordination, Supplier Schema, and Ordered State Transitions

**Files:**
- Create: `company_locality.py`
- Create: `maintenance_lock.py`
- Create: `locality_quiesce.py`
- Create: `company_locality_admin.py`
- Test: `test_company_locality.py`
- Test: `test_maintenance_lock.py`
- Test: `test_locality_quiesce.py`
- Test: `test_company_locality_admin.py`

**Interfaces:**
- Produces: `ensure_locality_schema(conn: sqlite3.Connection) -> None`
- Produces: `apply_company_changes(conn, items, source_date, job_id, verified_at) -> ChangeSummary`
- Produces: `active_local_biznos(conn: sqlite3.Connection) -> set[str]`
- Produces: `status_at(conn, bizno, effective_at) -> str | None`
- Produces: `resolve_company_conflict(conn, event_ids, selected_status, effective_at, operator, reason, evidence) -> Resolution`
- Produces: `start_sync_job`, `finish_sync_job`, and `fail_sync_job`
- Produces: `maintenance_lock(path, timeout_seconds)`, `guarded_write_session`, `set_write_fence`, `read_data_generation`, `read_control_revision`, and `checkpoint_wal`
- Produces: `assert_databases_quiesced(paths, process_inspector) -> None` and `dual_exclusive_transition(company_conn, proc_conn)`.

- [ ] **Step 1: Write transition tests**

Create `test_company_locality.py` with temporary SQLite fixtures covering current-local bootstrap, outbound move, head-office-to-branch change, inbound insertion, re-entry after outbound, replay idempotence, retrograde events, equal-time divergent locality fields, descriptive-only equal-time changes, and same-day date-only boundaries. Create `test_maintenance_lock.py` covering serialization, absolute lock-path agreement, timeout, generation changes, failed checkpoints, persisted write-fence restart behavior, and guard-trigger coverage. Create `test_locality_quiesce.py` with injectable process/file-handle inspection and pre-opened connection/transaction fixtures. Create `test_company_locality_admin.py` for audited conflict resolution and deterministic replay.

```python
class CompanyLocalityTransitionTests(unittest.TestCase):
    def test_outbound_supplier_is_retained_but_inactivated(self):
        apply_company_changes(self.conn, [source_item("1234567890", "경남", "본사", "202608160900")], "20260816", "job-1", NOW)
        row = self.conn.execute("SELECT status, inactive_reason FROM company_locality_status WHERE bizno='1234567890'").fetchone()
        self.assertEqual(row, ("moved_out", "region_changed"))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM company_master WHERE bizno='1234567890'").fetchone()[0], 1)

    def test_inbound_supplier_becomes_active_from_source_change_time(self):
        apply_company_changes(self.conn, [source_item("2222222222", "부산", "본사", "202608161015")], "20260816", "job-2", NOW)
        self.assertEqual(status_at(self.conn, "2222222222", "2026-08-16 10:15:01"), "active_local")
        self.assertIsNone(status_at(self.conn, "2222222222", "2026-08-15 23:59:59"))

    def test_same_day_date_only_boundary_is_unknown(self):
        apply_company_changes(self.conn, [source_item("2222222222", "부산", "본사", "202608161015")], "20260816", "job-3", NOW)
        self.assertIsNone(status_at(self.conn, "2222222222", "2026-08-16"))
```

- [ ] **Step 2: Run tests and confirm the module is missing**

Run: `py -3.13 -m unittest test_company_locality test_maintenance_lock test_locality_quiesce test_company_locality_admin -v`

Expected: import failure for `company_locality`.

- [ ] **Step 3: Implement schemas and normalized state transitions**

Create `company_locality.py` with `normalize_bizno`, immutable `ChangeSummary`, schema migration, and transactional transition handling. Use these table shapes:

```sql
CREATE TABLE IF NOT EXISTS company_locality_status (
    bizno TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('active_local','moved_out','branch_changed','unverified')),
    source_rgn_nm TEXT NOT NULL DEFAULT '',
    source_hdoffce_div_nm TEXT NOT NULL DEFAULT '',
    source_effective_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    inactive_at TEXT,
    inactive_reason TEXT,
    last_verified_at TEXT NOT NULL,
    source_chg_dt TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS company_locality_event (
    id INTEGER PRIMARY KEY,
    bizno TEXT NOT NULL,
    previous_status TEXT,
    new_status TEXT NOT NULL,
    source_effective_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source_chg_dt TEXT NOT NULL DEFAULT '',
    locality_hash TEXT NOT NULL,
    descriptive_hash TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    job_id TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK(disposition IN ('applied','duplicate','quarantined_retrograde','quarantined_conflict','quarantined_invalid_time')),
    UNIQUE(bizno, new_status, source_chg_dt, locality_hash)
);

CREATE TABLE IF NOT EXISTS company_sync_job_log (
    job_name TEXT NOT NULL,
    source_date TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','success','failed')),
    expected_rows INTEGER,
    received_rows INTEGER,
    page_count INTEGER,
    retry_count INTEGER NOT NULL DEFAULT 0,
    call_count INTEGER NOT NULL DEFAULT 0,
    call_budget INTEGER NOT NULL DEFAULT 0,
    circuit_state TEXT NOT NULL DEFAULT 'closed',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error_detail TEXT,
    PRIMARY KEY(job_name, source_date)
);

CREATE TABLE IF NOT EXISTS company_sync_response_metric (
    job_name TEXT NOT NULL,
    source_date TEXT NOT NULL,
    response_class TEXT NOT NULL,
    response_count INTEGER NOT NULL,
    PRIMARY KEY(job_name, source_date, response_class)
);

CREATE TABLE IF NOT EXISTS company_locality_resolution (
    id INTEGER PRIMARY KEY,
    bizno TEXT NOT NULL,
    event_ids_json TEXT NOT NULL,
    before_status TEXT,
    selected_status TEXT NOT NULL,
    selected_effective_at TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    operator TEXT NOT NULL,
    reason TEXT NOT NULL,
    generation_id TEXT NOT NULL,
    resolved_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS locality_activation_state (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    writes_enabled INTEGER NOT NULL CHECK(writes_enabled IN (0,1)),
    active_generation_id TEXT,
    ever_snapshot_activated INTEGER NOT NULL CHECK(ever_snapshot_activated IN (0,1)),
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS locality_fence_audit (
    id INTEGER PRIMARY KEY,
    writes_enabled INTEGER NOT NULL,
    operator TEXT NOT NULL,
    reason TEXT NOT NULL,
    changed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS locality_generation_clock (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    data_generation INTEGER NOT NULL,
    control_revision INTEGER NOT NULL
);
```

Classify status with this precedence:

```python
def current_status(rgn_nm: str, head_office: str) -> tuple[str, str | None]:
    if "부산" not in (rgn_nm or ""):
        return "moved_out", "region_changed"
    if (head_office or "").strip() != "본사":
        return "branch_changed", "head_office_changed"
    return "active_local", None
```

Ignore new non-Busan suppliers after staging, but apply a non-local transition when the business number already exists in `company_master` or `company_locality_status`. Bootstrap existing master rows as `active_local` with `source_effective_at='1900-01-01 00:00:00+09:00'`, `observed_at` set to migration time, and `source_chg_dt` from the row when available.

Parse `chgDt` forms into timezone-aware Asia/Seoul values. Apply only strictly newer valid source-effective events. Record but do not apply older events. Hash normalized business number, region, head-office division, and effective time separately from descriptive fields. Equal-time events with a different locality hash are quarantined and make `status_at` unknown at and after the conflict until an audited resolution selects evidence; descriptive-only changes do not block. Preserve the last confirmed status on transport failures.

Implement the maintenance lock with `fcntl.flock` on Linux and an injectable fallback for Windows tests. Every caller uses the same resolved absolute lock path and acquires locks in this order: maintenance lock, company DB, procurement DB. Configure WAL, bounded `busy_timeout`, explicit transactions, and verified `PRAGMA wal_checkpoint(TRUNCATE)`.

`guarded_write_session` checks an fsynced activation journal, `locality_writes_paused` marker, both persisted activation rows, and pointer/DB agreement before opening the write transaction. Classify every protected table as `cache_input`, `derived_output`, or `control`. Transactional triggers on cache-input tables bump only `data_generation`; control tables bump only `control_revision`; derived snapshot/cache-generation rows do not change the source tuple. Supplier status/effective-time resolution, contract/evidence data, and pending correction/collision requests are cache inputs. Activation/fence/generation bookkeeping, job metrics, backup records, and post-pointer correction lifecycle events are control-plane. Rollback removes trigger increments automatically.

Tests prove activation-state, fence-audit, generation-row, snapshot-output, and correction-`applied` commits leave the manifest's source tuple unchanged, while a genuine company status, contract/evidence, or pending-correction write invalidates a prepared generation. Monitoring and backup use the same clock classification.

Fence and first-snapshot activation commands require an explicit maintenance window: stop all cron/systemd/manual DB users, inspect OS file handles for both DB/WAL/SHM paths, and abort unless no process has them open. Then acquire the maintenance lock and `BEGIN EXCLUSIVE` on both databases before state changes. Pause creates/fsyncs the journal and marker, disables both DB rows while both locks are held, and commits one only while the other remains exclusively locked. Resume keeps the marker, enables both rows under the same dual-exclusive protocol, and removes/fsyncs the marker last. Disabled legacy writers refuse permanently once either verified state shows `ever_snapshot_activated=1`.

Recovery never resumes automatically. It repeats quiescence, holds both exclusive transactions, reconciles both DB rows to the authoritative pointer, verifies trigger/backup/generation gates, and requires the operator command to remove the marker. Tests first prove any pre-opened DB handle makes quiescence abort with no state change. After successful quiescence, crash injection covers journal creation, each database commit, pointer replacement, bookkeeping reconciliation, and marker removal; newly started migrated and disabled-legacy writer attempts remain blocked at every incomplete state.

- [ ] **Step 4: Run transition tests**

Run: `py -3.13 -m unittest test_company_locality test_maintenance_lock test_locality_quiesce test_company_locality_admin -v`

Expected: all tests pass, including exactly one event after replaying the same source row.

- [ ] **Step 5: Commit the state layer**

```bash
git add company_locality.py maintenance_lock.py locality_quiesce.py company_locality_admin.py test_company_locality.py test_maintenance_lock.py test_locality_quiesce.py test_company_locality_admin.py
git commit -m "feat: coordinate ordered supplier locality transitions"
```

---

### Task 2: Strict Supplier Change Collection and Independent Recovery

**Files:**
- Create: `company_sync.py`
- Modify: `daily_pipeline_sync.py:319-408`
- Modify: `daily_pipeline_sync.py:1182-1207`
- Modify: `daily_pipeline_sync.py:1488-1556`
- Test: `test_company_sync.py`

**Interfaces:**
- Consumes: Task 1 job-state and transition functions.
- Produces: `fetch_complete_change_batch(source_date, fetch_page, rows_per_page=999) -> CompanyBatch`
- Produces: `pending_supplier_dates(conn, through_date) -> list[str]`
- Produces: `sync_company_change_date(source_date, fetch_page, company_db_path) -> ChangeSummary`

- [ ] **Step 1: Write completeness and recovery tests**

```python
def test_missing_second_page_raises_and_applies_nothing(self):
    fetch = FakePages({1: ([item("1")], 1000), 2: RuntimeError("timeout")})
    with self.assertRaises(IncompleteCompanyBatch):
        fetch_complete_change_batch("20260816", fetch)

def test_count_mismatch_raises(self):
    fetch = FakePages({1: ([item("1")], 2)})
    with self.assertRaisesRegex(IncompleteCompanyBatch, "expected=2 received=1"):
        fetch_complete_change_batch("20260816", fetch)

def test_duplicate_source_identity_raises_even_when_count_matches(self):
    fetch = FakePages({1: ([item("1"), item("1")], 2)})
    with self.assertRaisesRegex(IncompleteCompanyBatch, "duplicate source identity"):
        fetch_complete_change_batch("20260816", fetch)

def test_supplier_failure_is_pending_even_when_contract_date_succeeded(self):
    fail_sync_job(self.conn, "company_changes", "20260815", "page 2 timeout", NOW)
    self.assertEqual(pending_supplier_dates(self.conn, "20260816"), ["20260815", "20260816"])
```

- [ ] **Step 2: Run the new tests and confirm failure**

Run: `py -3.13 -m unittest test_company_sync -v`

Expected: import or symbol failures.

- [ ] **Step 3: Implement strict pagination**

In `company_sync.py`, use default TLS verification and calculate pages with `math.ceil(total_count / rows_per_page)`. Retry each page three times with exponential backoff, jitter, and `Retry-After`; enforce configurable QPS and daily-call budgets plus a circuit breaker. Raise `IncompleteCompanyBatch` when a page fails, page metadata or `totalCount` drifts, required fields/schema are invalid, a normalized source identity repeats, or final received count differs from the stable `totalCount`. Return an immutable batch only after all pages pass.

```python
@dataclass(frozen=True)
class CompanyBatch:
    items: tuple[dict, ...]
    total_count: int
    page_count: int
    retry_count: int
```

Do not return `([], 0)` for an exhausted retry.

Treat empty/not-found direct responses as non-authoritative unless the response contains the API's explicit not-found result code. Persist response-class counts, call count, call-budget ceiling, and circuit state in the job/metric tables, including failed and budget-exhausted runs.

- [ ] **Step 4: Replace the inline updater with orchestration**

Keep the existing API URL and request parameters, but pass the verified-TLS HTTP page reader into `sync_company_change_date`. Record `company_changes` success independently. In `main()`, collect every unresolved supplier date separately from the contract `sync_log` and retry oldest first; do not expire failures after the nominal lookback. Add an explicit overlap-window job. A supplier failure may allow contract collection to continue, but cannot create a supplier success row.

- [ ] **Step 5: Run focused and existing recovery tests**

Run: `py -3.13 -m unittest test_company_sync test_public_api_recovery -v`

Expected: strict supplier tests and existing contract catch-up tests pass.

- [ ] **Step 6: Commit strict synchronization**

```bash
git add company_sync.py daily_pipeline_sync.py test_company_sync.py
git commit -m "fix: make supplier synchronization recoverable"
```

---

### Task 3: Rolling Full-Population Revalidation

**Files:**
- Create: `company_reconcile.py`
- Modify: `company_locality.py`
- Test: `test_company_reconcile.py`
- Test: `test_company_lookup_contract.py`

**Interfaces:**
- Consumes: Task 1 `apply_company_changes` and job-state APIs.
- Produces: `bucket_for_bizno(bizno: str, bucket_count: int = 30) -> int`
- Produces: `biznos_for_bucket(conn, bucket, bucket_count=30) -> list[str]`
- Produces: `revalidate_bucket(conn, source_client, run_date, bucket_count=30, workers=8) -> RevalidationSummary`
- Produces: `drain_revalidation_queue(conn, source_client, run_at, request_budget) -> RevalidationSummary`

- [ ] **Step 1: Write deterministic bucket and inbound/outbound tests**

```python
def test_bucket_assignment_is_stable(self):
    self.assertEqual(bucket_for_bizno("1234567890"), bucket_for_bizno("123-45-67890"))

def test_revalidation_reactivates_returning_supplier_without_changing_old_event(self):
    source = FakeCompanyClient({"1234567890": source_item("1234567890", "부산", "본사", "202608160900")})
    result = revalidate_bucket(self.conn, source, date(2026, 8, 16), bucket_count=1, workers=1)
    self.assertEqual(result.activated, 1)
    self.assertEqual(status_at(self.conn, "1234567890", "2026-08-16 09:00:00"), "active_local")
```

- [ ] **Step 2: Run and confirm the tests fail**

Run: `py -3.13 -m unittest test_company_reconcile -v`

Expected: missing reconciliation symbols.

- [ ] **Step 3: Implement bounded rolling verification**

Assign buckets with the first eight hex characters of SHA-256 modulo `bucket_count`. Before the day's bucket, drain due rows from `company_revalidation_queue`; then re-query one bucket per day by normalized business number. Use at most eight workers by default, three retries, configurable QPS/daily-call ceilings, exponential backoff with jitter and `Retry-After`, response schema/size validation, and a circuit breaker. Acquire the maintenance lock and apply authoritative rows in a guarded database transaction.

Persist one queue row per business number with attempt count, response class, next-attempt time, and `pending|deferred_budget|failed|complete` state. When a direct lookup fails, returns malformed data, exhausts budget, or returns an unauthoritative empty response, retain the last confirmed status, leave `last_verified_at` unchanged, and schedule bounded-backoff recovery instead of waiting for the next 30-day bucket. Use `unverified` only when the business number has no prior confirmed status. Never infer `moved_out` from an empty or failed response. Record request-budget, call-count, circuit-state, and response-class metrics in `company_revalidation` job state.

```sql
CREATE TABLE IF NOT EXISTS company_revalidation_queue (
    bizno TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('pending','deferred_budget','failed','complete')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_response_class TEXT,
    next_attempt_at TEXT,
    last_attempt_at TEXT,
    last_success_at TEXT,
    error_detail TEXT
);
```

`test_company_lookup_contract.py` captures the exact deployed business-number request parameter names, normalized value, pagination fields, success code, and authoritative not-found code from a fixture response. The source client must fail closed when the response contract differs.

- [ ] **Step 4: Add CLI dry-run and apply modes**

Support these commands without embedding credentials:

```bash
python3 company_reconcile.py --date 2026-08-16 --bucket-count 30 --dry-run
python3 company_reconcile.py --date 2026-08-16 --bucket-count 30 --apply
```

Dry-run prints counts only. Apply writes status/events and a `company_revalidation` job row.

- [ ] **Step 5: Run reconciliation tests**

Run: `py -3.13 -m unittest test_company_reconcile test_company_lookup_contract test_company_locality -v`

Expected: inbound, outbound, branch, empty-response, and idempotence tests pass.

- [ ] **Step 6: Commit rolling verification**

```bash
git add company_reconcile.py company_locality.py test_company_reconcile.py test_company_lookup_contract.py
git commit -m "feat: reconcile supplier locality on a rolling cycle"
```

---

### Task 4: Canonical Contract Population and Frozen Locality Baseline

**Files:**
- Create: `contract_population.py`
- Create: `locality_snapshot.py`
- Modify: `core_calc.py:587-658`
- Test: `test_contract_population.py`
- Test: `test_locality_snapshot.py`

**Interfaces:**
- Produces: `iter_canonical_contracts(conn, sector, date_range=None) -> Iterator[CanonicalContract]`
- Produces: `content_fingerprint(contract: CanonicalContract) -> str`
- Produces: `ensure_snapshot_schema(conn: sqlite3.Connection) -> None`
- Produces: `create_baseline_manifest(conn, canonical_rows, baseline_id) -> BaselineManifest`
- Produces: `verify_baseline_manifest(conn, baseline_id) -> BaselineCoverage`
- Produces: `SnapshotResolver.seed(row, bizno, share_pct, is_busan, basis, baseline_id) -> None`
- Produces: `SnapshotResolver.resolve(row, bizno, share_pct, legacy_is_local) -> bool`
- Produces: `SnapshotResolver.flush() -> int`
- `process_contract_row` gains optional `locality_resolver=None` and `sector=None`; both default to the exact legacy path.

- [ ] **Step 1: Write identity, collision, and snapshot invariance tests**

Build fixtures for reused `untyCntrctNo`, `dcsnCntrctNo` families/revisions, same-revision divergent corrections, missing keys, duplicate supplier entries, unordered source rows, and shopping numeric/string change-order representations.

```python
def test_outbound_move_does_not_change_frozen_historical_contract(self):
    resolver = SnapshotResolver(self.proc_conn, self.company_conn, mode="snapshot", now=NOW, cutover_at=CUTOVER)
    resolver.seed(self.old_contract, "1234567890", 100.0, True, "legacy_baseline_v1", BASELINE_ID)
    apply_company_changes(self.company_conn, [source_item("1234567890", "경남", "본사", "202608160900")], "20260816", "job-outbound", NOW)
    self.assertTrue(resolver.resolve(self.old_contract, "1234567890", 100.0, False))

def test_pre_inbound_contract_remains_non_local(self):
    resolver.seed(self.old_contract, "2222222222", 100.0, False, "legacy_baseline_v1", BASELINE_ID)
    apply_company_changes(self.company_conn, [source_item("2222222222", "부산", "본사", "202608161015")], "20260816", "job-inbound", NOW)
    self.assertFalse(resolver.resolve(self.old_contract, "2222222222", 100.0, True))
    self.assertTrue(resolver.resolve(self.new_contract, "2222222222", 100.0, True))

def test_pre_cutover_manifest_miss_is_a_hard_failure(self):
    resolver = SnapshotResolver(self.proc_conn, self.company_conn, mode="snapshot", now=NOW, cutover_at=CUTOVER)
    with self.assertRaisesRegex(MissingHistoricalSnapshot, "pre-cutover"):
        resolver.resolve(self.unmanifested_old_contract, "3333333333", 100.0, True)
```

- [ ] **Step 2: Run and confirm tests fail**

Run: `py -3.13 -m unittest test_contract_population test_locality_snapshot -v`

Expected: missing canonical population and snapshot symbols.

- [ ] **Step 3: Implement one versioned canonical iterator**

Use one identity algorithm for every consumer:

- Non-shopping: sector namespace plus normalized `dcsnCntrctNo[:-2]` family and final-two-character revision. If absent, use normalized `untyCntrctNo` family and empty revision.
- Shopping: normalized `dlvrReqNo`, normalized `prdctSno`, and integer-normalized `dlvrReqChgOrd`.
- Aggregate repeated supplier entries by normalized business number before calculating shares.
- Deterministically order by explicit source update/date/revision fields and normalized content fingerprint; unordered `keep='last'` is prohibited.
- Fingerprint normalized agency, amount, governing date, and sorted supplier/share pairs.
- Quarantine and abort when one identity maps to divergent content. Reject missing keys during historical baseline.

- [ ] **Step 4: Implement snapshot and manifest schemas**

```sql
CREATE TABLE IF NOT EXISTS contract_supplier_locality (
    sector TEXT NOT NULL,
    contract_key TEXT NOT NULL,
    contract_revision TEXT NOT NULL DEFAULT '',
    bizno TEXT NOT NULL,
    share_pct REAL NOT NULL,
    is_busan INTEGER,
    basis TEXT NOT NULL,
    contract_date TEXT NOT NULL DEFAULT '',
    classified_at TEXT NOT NULL,
    classifier_version TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    baseline_id TEXT,
    introduced_generation_id TEXT NOT NULL,
    PRIMARY KEY(sector, contract_key, contract_revision, bizno)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS locality_baseline_manifest (
    baseline_id TEXT PRIMARY KEY,
    cutover_at TEXT NOT NULL,
    classifier_version TEXT NOT NULL,
    iterator_version TEXT NOT NULL,
    cache_generation_id TEXT NOT NULL,
    manifest_fingerprint TEXT NOT NULL,
    expected_contracts INTEGER NOT NULL,
    expected_suppliers INTEGER NOT NULL,
    expected_share_micros INTEGER NOT NULL,
    expected_amount_won INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('building','complete','failed')),
    created_at TEXT NOT NULL
);

```

Baseline verification reconciles every canonical revision, normalized supplier, aggregated share, rounded amount, and content fingerprint. Coverage must equal 100% and historical unknown amount must equal zero.

- [ ] **Step 5: Add resolver support with fail-closed historical behavior**

In `process_contract_row`, preserve the unchanged legacy expression when no resolver is supplied. Legacy mode returns that value. Shadow mode records a candidate snapshot but returns legacy output. Snapshot mode returns the frozen decision; a pre-cutover manifest miss raises `MissingHistoricalSnapshot` and aborts the build. Only post-cutover identities may create a decision from ordered status history.

For a new revision, inherit unchanged suppliers' prior decisions and classify only newly introduced suppliers at the revision's governing date. Construction/service/goods use `cntrctCnclsDate`; shopping uses `dlvrReqRcptDate`. A date-only contract on the same Asia/Seoul date as a status transition is unknown and blocks publication.

- [ ] **Step 6: Run canonical and snapshot tests**

Run: `py -3.13 -m unittest test_contract_population test_locality_snapshot test_company_locality -v`

Expected: identity/collision fixtures, exact manifest coverage, legacy invariance, transition inheritance, and fail-closed historical tests pass.

- [ ] **Step 7: Commit canonical snapshot primitives**

```bash
git add contract_population.py locality_snapshot.py core_calc.py test_contract_population.py test_locality_snapshot.py
git commit -m "feat: canonicalize and freeze contract locality"
```

---

### Task 5: Use One Resolver and One Generation Across All Consumers

**Files:**
- Create: `locality_generation.py`
- Create: `build_locality_generation.py`
- Create: `db_writer_inventory.py`
- Create: `locality_writer_inventory.json`
- Create: `db_consumer_inventory.py`
- Create: `locality_consumer_inventory.json`
- Modify: `build_api_cache.py`
- Modify: `build_monthly_cache.py`
- Modify: `rate_calc_db.py`
- Modify: `export_excel.py`
- Modify: `api_server.py`
- Modify: `dashboard.py`
- Modify: `server_sync/api_server.py`
- Modify: `alert_check.py`
- Modify: `server_sync/alert_check.py`
- Modify: `nts_batch_sync.py`
- Modify: `server_sync/nts_batch_sync.py`
- Modify: `server_sync/bootstrap_master_data.py`
- Modify: `rate_calc.py`
- Modify: `bootstrap_master_data.py`
- Modify: `daily_pipeline_sync.py`
- Modify: `collect_busan_awards.py`
- Modify: `update_servc_site.py`
- Modify: `import_manual_contracts.py`
- Modify: `migrate_chatbot_db.py`
- Modify: `import_company_industry.py`
- Modify: `run_vendor_recommendation_1000_qa_20260611.py`
- Test: `test_locality_rate_integration.py`
- Test: `test_locality_consumer_integration.py`
- Test: `test_locality_generation.py`
- Test: `test_locality_writer_inventory.py`
- Test: `test_locality_consumer_inventory.py`

**Interfaces:**
- Consumes: Task 4 `SnapshotResolver` and new `process_contract_row` arguments.
- Produces: `build_locality_resolver(proc_conn, company_conn, mode, sector) -> SnapshotResolver`
- Produces: `prepare_generation`, `activate_generation`, `load_active_generation`, and `recover_generations`.
- Produces: `build_locality_generation(mode, paths) -> GenerationManifest` as the only snapshot/cache writer.
- Produces: SQL view `current_local_company` for non-rate supplier views.
- Produces: `scan_writer_inventory(root, inventory_path) -> InventoryReport`.
- Produces: `scan_consumer_inventory(root, inventory_path) -> InventoryReport`.
- Produces identical legacy output in `legacy` and `shadow` modes.

- [ ] **Step 1: Write four-sector, consumer, and publication fixtures**

Build temporary agency, company, and procurement databases with one local and one non-local supplier in construction, service, goods, and shopping. Include a joint contract, a contract revision, a shopping change order, an outbound supplier retained in `company_master`, and an inbound supplier. Assert legacy and shadow totals are identical, snapshot coverage contains only canonical rows, and every current-supplier view excludes inactive rows without changing historical rates.

```python
self.assertEqual(legacy["overall"], shadow["overall"])
self.assertEqual(legacy["sectors"], shadow["sectors"])
self.assertEqual(snapshot_count, expected_canonical_supplier_count)
```

Add crash-injection tests before/after snapshot prepare, each cache write, directory fsync, active-pointer replace, and generation finalization. Change a source `data_generation` during a build and force one required sector to raise. The old active generation must remain readable and no incomplete generation may become active. Also prove control-only commits do not stale the source tuple. Recovery finalizes a pointer-matching prepared generation and abandons any other prepared generation deterministically.

- [ ] **Step 2: Run and confirm integration failure**

Run: `py -3.13 -m unittest test_locality_rate_integration test_locality_consumer_integration test_locality_generation test_locality_writer_inventory test_locality_consumer_inventory -v`

Expected: builders do not yet create/pass a resolver and current-supplier consumers still include inactive rows.

- [ ] **Step 3: Thread the resolver through every calculation path**

Read `LOCALITY_MODE` once per command, validate it against `legacy|shadow|snapshot`, create sector-bound resolvers, and pass `sector` plus `locality_resolver` to every `process_contract_row` call in the four files. Add missing contract date and canonical identity fields to SELECT lists, including weekly and daily queries and shopping receipt dates. Replace direct `bizno in biznos`, raw prefix checks, and ad hoc address membership decisions with the resolver; retain raw company rows only for non-locality metadata.

Required-sector exceptions propagate and fail the build; broad `except: pass` is forbidden around locality calculations. `build_api_cache.py` and `build_monthly_cache.py` become pure generation builders called by `build_locality_generation.py`; they no longer publish independently. `rate_calc_db.py` and `export_excel.py` bind a read-only resolver to the active generation and must never create snapshots during an HTTP or ad hoc request. In `shadow`, publish legacy rate values in the generation bundle while recording complete candidate decisions.

- [ ] **Step 4: Make current-supplier views lifecycle-aware**

Create the `current_local_company` SQL view joining `company_master` to `company_locality_status.status='active_local'`. Use it for API license/product/manufacturer endpoints, dashboard searches/downloads, alert candidate selection, NTS/revalidation input, deployed and `server_sync` bootstrap paths, legacy rate entrypoints, company counts/names, chatbot migration/search inputs, industry joins, and recommendation candidates. Bootstrap/import commands may enrich retained rows but must not reactivate an inactive supplier unless they carry an authoritative newer locality event.

Generate and review `locality_consumer_inventory.json`; every direct `company_master` reader declares deployment status and `current_local|historical_metadata|not_deployed`. The AST/SQL scan fails on an unclassified reader or a deployed current-local consumer that does not use the view. Add API/dashboard endpoint tests plus alert, NTS input, bootstrap, and legacy rate-entry tests proving inactive suppliers cannot re-enter through non-HTTP paths.

Contract-rate consumers continue using frozen snapshots and must never join current `active_local` state for a historical decision.

- [ ] **Step 5: Inventory and guard every database writer**

Generate `locality_writer_inventory.json` from every Python file containing SQLite `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, DDL, or backup/restore operations. Each entry declares database paths/tables, deployment status, and exactly one disposition: `migrated`, `disabled_after_cutover`, or `not_deployed`. Cross-check deployed entries against `HANDOVER.md`, cron, systemd, and `server_sync` copies.

Every migrated writer uses `guarded_write_session`. Every disabled writer calls `assert_legacy_writer_allowed` before connecting and fails once `ever_snapshot_activated` is true. The static test rejects unclassified writers, a migrated writer lacking the helper, a disabled writer lacking the startup guard, or a deployed `not_deployed` entry. Explicitly cover `daily_pipeline_sync.py`, `collect_busan_awards.py`, `update_servc_site.py`, `import_manual_contracts.py`, bootstrap/import jobs, and production backup/restore paths.

- [ ] **Step 6: Coordinate immutable generation-set activation**

Acquire the shared maintenance lock before opening write transactions. A contract refresh `DELETE` plus replacement `INSERT` runs in one guarded cache-input transaction and changes the procurement data clock atomically; it must complete before generation preparation captures its source tuple. The orchestrator then records company and procurement `data_generation` values before calculation, checkpoints both WALs, and verifies the same values before activation.

Create a `locality_generation` row with parent ID, both source data generations, mode, baseline ID, manifest hash, cache directory, and `prepared|active|abandoned` status. Store newly prepared snapshot rows with `introduced_generation_id`; resolvers expose only the ancestry named by the active pointer, regardless of whether post-pointer bookkeeping has already changed the row from `prepared` to `active`. Write `api_cache.json`, `monthly_cache.json`, and `manifest.json` beneath immutable `cache_generations/<id>/`, `flush`/`fsync` every file and directory, and validate all hashes/metadata.

```sql
CREATE TABLE IF NOT EXISTS locality_generation (
    generation_id TEXT PRIMARY KEY,
    parent_generation_id TEXT,
    company_data_generation INTEGER NOT NULL,
    procurement_data_generation INTEGER NOT NULL,
    mode TEXT NOT NULL CHECK(mode IN ('legacy','shadow','snapshot')),
    baseline_id TEXT,
    cache_directory TEXT NOT NULL,
    manifest_hash TEXT NOT NULL,
    correction_set_hash TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK(status IN ('prepared','active','abandoned')),
    prepared_at TEXT NOT NULL,
    activated_at TEXT
);
```

After every artifact is durable, atomically replace and fsync the single `active_locality_generation.json` pointer containing generation ID, previous generation ID, and manifest hash. That pointer is the sole visibility boundary. Readers load it once per request and use only its directory and snapshot ancestry; a valid pointer to a still-`prepared` row is usable and recovery finalizes its bookkeeping. A mismatch retains the prior in-memory generation or, after restart, validates and loads the persisted previous generation, then emits a critical check. Nonmatching prepared rows are abandoned and remain invisible until verified cleanup. Never promise one transaction across SQLite and filesystem operations.

Every forward activation and predecessor rollback uses the Task 1 journal/marker and dual-exclusive state machine: pause both DB control rows; verify the target bundle and unchanged source data tuple; replace the pointer; reconcile repairable `active_generation_id` in both DBs; verify agreement; then resume guarded writes only after all gates pass. The first snapshot transition additionally sets `ever_snapshot_activated=1` and requires full process/file-handle quiescence. Later transitions need not stop read-only services because legacy writers are permanently disabled and supported writers honor the maintenance lock/marker.

Control-plane commits in this sequence change only `control_revision`; they cannot stale the prepared source tuple. `guarded_write_session` rejects any pointer/DB mismatch. A command reports success only after both DB rows agree and the marker is removed. Recovery rolls both bookkeeping rows forward to the pointer before writes can resume and never changes the pointer to follow one partially committed DB row. Extend crash injection through both pre-pointer DB commits, pointer replacement, both post-pointer bookkeeping commits, correction lifecycle events, forward successor activation, predecessor rollback, recovery, and writer attempts in every state.

- [ ] **Step 7: Verify all calculation, consumer, writer, and recovery surfaces agree**

Run: `py -3.13 -m unittest test_locality_rate_integration test_locality_consumer_integration test_locality_generation test_locality_writer_inventory test_locality_consumer_inventory test_locality_snapshot test_public_api_recovery -v`

Expected: API cache, monthly cache, report, and export fixtures agree for the same active generation; current supplier outputs reflect inbound/outbound state; historical snapshots do not change; every writer is classified/guarded; interrupted and mixed-generation builds preserve the last-known-good active generation.

- [ ] **Step 8: Commit resolver and generation integration**

```bash
git add locality_generation.py build_locality_generation.py db_writer_inventory.py locality_writer_inventory.json db_consumer_inventory.py locality_consumer_inventory.json build_api_cache.py build_monthly_cache.py rate_calc_db.py export_excel.py api_server.py dashboard.py server_sync/api_server.py alert_check.py server_sync/alert_check.py nts_batch_sync.py server_sync/nts_batch_sync.py server_sync/bootstrap_master_data.py rate_calc.py bootstrap_master_data.py daily_pipeline_sync.py collect_busan_awards.py update_servc_site.py import_manual_contracts.py migrate_chatbot_db.py import_company_industry.py run_vendor_recommendation_1000_qa_20260611.py test_locality_rate_integration.py test_locality_consumer_integration.py test_locality_generation.py test_locality_writer_inventory.py test_locality_consumer_inventory.py
git commit -m "feat: coordinate locality consumers and cache generations"
```

---

### Task 6: Rate and Capacity Audit CLI

**Files:**
- Create: `company_locality_audit.py`
- Modify: `company_locality_admin.py`
- Test: `test_company_locality_audit.py`

**Interfaces:**
- Consumes: Tasks 1-5 schemas and resolver.
- Produces: `collect_capacity_report(paths, api_encoded_samples, staging_page_metrics, eligible_supplier_rows) -> dict`
- Produces: `evaluate_capacity_gates(report) -> list[GateFailure]`
- Produces: `compare_rate_results(legacy, snapshot) -> dict`
- Produces CLI commands `capacity`, `baseline`, and `compare`; admin commands create pending snapshot-correction and canonical-collision-resolution requests.

- [ ] **Step 1: Write capacity and activation-gate tests**

```python
def test_capacity_blocks_projected_peak_at_eighty_percent(self):
    failures = evaluate_capacity_gates({"projected_peak_pct": 80.0, "post_free_bytes": 10 * GIB, "post_free_pct": 40.0})
    self.assertIn("projected_peak_pct", {f.code for f in failures})

def test_baseline_blocks_one_won_local_amount_difference(self):
    report = compare_rate_results(result(local=100), result(local=99))
    self.assertFalse(report["baseline_gate_passed"])

def test_shadow_blocks_large_sector_delta(self):
    report = compare_rate_results(result(rate=50.0), result(rate=50.4), sector="용역")
    self.assertIn("sector_rate_delta", report["gate_failures"])

def test_baseline_requires_exact_manifest_and_zero_unknown(self):
    report = baseline_report(expected_contracts=10, matched_contracts=9, unknown_amount=0)
    self.assertIn("baseline_coverage", report["gate_failures"])

def test_capacity_counts_simultaneous_wal_staging_backup_and_gzip_temp(self):
    report = collect_capacity_report(self.paths, api_encoded_samples=[b"x" * 400], staging_page_metrics={"page_count": 12, "page_size": 4096}, eligible_supplier_rows=2500)
    self.assertGreaterEqual(report["devices"][self.staging_device]["api_staging_bytes"], 12 * 4096)
    self.assertGreater(report["total_projected_peak_bytes"], report["database_bytes"] * 2)
```

- [ ] **Step 2: Run and confirm audit tests fail**

Run: `py -3.13 -m unittest test_company_locality_audit -v`

Expected: missing audit functions.

- [ ] **Step 3: Implement exact preflight and measured staging reports**

Use `Path.stat()`, `shutil.disk_usage`, SQLite `page_count`, `page_size`, table counts, parsed supplier-share counts, measured encoded API sample sizes with a conservative multiplier, and actual staging SQLite pages. API rows-per-page is telemetry only and never treated as bytes. Model the simultaneous peak, not independent file peaks: both main DBs, both `-wal`/`-shm` companions, measured plus conservative production-write WAL growth, API staging, baseline staging DB and indexes, retained gzip, new raw backup, gzip temporary output, generated reports/caches, and the existing 256 MiB margin.

Resolve the filesystem device for every database, WAL/SHM, staging directory, cache directory, backup directory, and compression temp path. Evaluate peak/free-space gates independently per device. Run capacity inspection while holding the maintenance lock after verified WAL checkpoints. Persist measured inputs, formulas, path-to-device assignments, both data-generation/control-revision values, active pointer ID, and safety margins. Reject stale or cross-generation measurements.

Write reports atomically to configurable paths under `sync_log/`:

```bash
python3 company_locality_audit.py capacity --output sync_log/locality_capacity.json
python3 company_locality_audit.py baseline --staging-db /opt/busan/tmp/locality-baseline.db --output sync_log/locality_baseline.json
python3 company_locality_audit.py compare --output sync_log/locality_impact.json
```

The baseline command uses Task 4's canonical iterator and manifest verification. It exits nonzero unless contract, revision, normalized supplier, aggregated share, rounded amount, content fingerprint, local amount, and sector/overall ordering all match exactly with 100% coverage and zero historical unknown amount. Exit nonzero on any failed gate. Never modify `api_cache.json` from audit commands.

- [ ] **Step 4: Implement controlled snapshot correction**

Extend `company_locality_admin.py request-snapshot-correction` to require `--operator`, `--reason`, contract identity, business number, selected source evidence, and explicit old/new value. Print the impact preview and require `--apply`. Append a pending request; never overwrite the original snapshot row or active generation. Reject a second unsuperseded request for the same identity.

Add `resolve-canonical-collision` requiring the quarantined identity, all candidate fingerprints, selected source fingerprint/evidence, operator, and reason. It appends a pending audited resolution and rejects incomplete candidate sets or concurrent unsuperseded resolutions. The canonical iterator may consume only a resolution whose candidate-set hash exactly matches the current collision.

Only `build_locality_generation.py` consumes pending correction/resolution requests. It applies them deterministically to a prepared successor, records sorted request IDs and a correction-set hash in `manifest.json`, rebuilds both caches, and exposes them only by pointer activation. Post-pointer bookkeeping appends an `applied` lifecycle event; visibility is always derived from the active manifest. Pointer rollback restores the predecessor decisions. Emit no supplier PII beyond the normalized internal business number required for the operator command.

```sql
CREATE TABLE IF NOT EXISTS contract_supplier_locality_correction_request (
    id INTEGER PRIMARY KEY,
    sector TEXT NOT NULL,
    contract_key TEXT NOT NULL,
    contract_revision TEXT NOT NULL,
    bizno TEXT NOT NULL,
    old_is_busan INTEGER,
    new_is_busan INTEGER NOT NULL,
    evidence_json TEXT NOT NULL,
    impact_fingerprint TEXT NOT NULL,
    operator TEXT NOT NULL,
    reason TEXT NOT NULL,
    requested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_collision_resolution (
    id INTEGER PRIMARY KEY,
    sector TEXT NOT NULL,
    canonical_identity TEXT NOT NULL,
    candidate_set_hash TEXT NOT NULL,
    selected_fingerprint TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    operator TEXT NOT NULL,
    reason TEXT NOT NULL,
    requested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS locality_correction_request_event (
    id INTEGER PRIMARY KEY,
    request_type TEXT NOT NULL CHECK(request_type IN ('snapshot','canonical_collision')),
    request_id INTEGER NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN ('superseded','rejected','applied')),
    generation_id TEXT,
    related_request_id INTEGER,
    operator TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
```

Creation and supersession use `BEGIN IMMEDIATE`, derive open requests from lifecycle events, and reject a second open request unless the operator atomically appends a `superseded` event naming it. Request and event rows are never updated or deleted.

- [ ] **Step 5: Run audit tests**

Run: `py -3.13 -m unittest test_company_locality_audit test_locality_rate_integration -v`

Expected: exact manifest baseline, rate thresholds, simultaneous-peak capacity thresholds, generation checks, atomic report writing, and correction audit tests pass.

- [ ] **Step 6: Commit audit tooling**

```bash
git add company_locality_audit.py company_locality_admin.py test_company_locality_audit.py
git commit -m "feat: audit locality rate and storage impact"
```

---

### Task 7: Operational Monitoring and Backup Verification

**Files:**
- Modify: `monitoring_regression_check.py`
- Modify: `backup_db.py`
- Test: `test_locality_monitoring.py`
- Modify: `test_backup_db.py`

**Interfaces:**
- Consumes: locality status/job/snapshot schemas and audit gate functions.
- Produces regression records for supplier freshness, failed dates, mode/generation/baseline consistency, canonical coverage, event quarantine, database growth, WAL size, and projected backup peak.
- Produces: `BackupManifest` with both DB data generations/control revisions, active pointer, sizes, checksums, restore checks, local/remote state, and completion time.

- [ ] **Step 1: Write monitoring failure tests**

Cover an unresolved supplier failed date, verification older than 31 days, snapshot coverage below 100%, pre-cutover fallback, manifest fingerprint mismatch, cache/DB generation mismatch, wrong mode, identity collision, quarantined event conflict, unresolved governing date, filesystem usage at 75%, abnormal week-over-week table growth, failed checkpoint, failed backup verification, and a backup projected above its capacity ceiling. Call the checker twice in one process and assert results do not leak between invocations.

- [ ] **Step 2: Run and confirm monitoring tests fail**

Run: `py -3.13 -m unittest test_locality_monitoring test_backup_db -v`

Expected: missing locality checks.

- [ ] **Step 3: Add locality operational checks**

Reset module-level `CHECKS` state at each invocation. Acquire the maintenance lock for one consistent observation; if lock-free diagnostic mode is explicitly requested, read both data-generation/control-revision pairs before and after and retry the entire observation on change. Use the same cache-input/control table classification as the build. Record critical failures for incomplete supplier jobs, revalidation backlog beyond its SLA, any canonical snapshot coverage below 100%, pre-cutover fallback, identity collisions, event conflicts, unresolved transition/contract dates, wrong locality mode, active pointer/cache/snapshot ancestry mismatch, baseline/manifest mismatch, failed checkpoints, failed or stale backup verification, and any partial required-sector build. Record warnings for stale verification and 75% filesystem use. Read sizes without mutating application data. Include `procurement_contracts.db`, `busan_companies_master.db`, WAL/SHM companions, staging files, immutable cache generations, reports, and backup directory totals.

- [ ] **Step 4: Revalidate backup capacity against enlarged databases**

Replace per-file `source_size * 2` checks with the Task 6 per-device simultaneous-peak model. Acquire the maintenance lock, checkpoint WAL, capture both DB data generations/control revisions and active pointer, create generation-stamped raw backups, compress them, restore into temporary SQLite files, run integrity checks, verify SHA-256 and logical sizes, and atomically persist a backup manifest with local/remote upload states and completion time. Cleanup is allowed only after a verified manifest for the exact active/baseline generation exists.

Define gates precisely: baseline cleanup and snapshot activation require an exact-generation verified backup; ordinary ongoing writes require a non-stale last-known-good verified backup plus passing projected capacity; each newly active generation must receive a verified backup before any previous generation or backup is removed. Remote upload failure preserves local verified artifacts and blocks retention cleanup/activation according to policy rather than being recorded as mere process success. Add tests that enlarged company/procurement databases are counted together and that no locality migration bypasses `assert_capacity`.

- [ ] **Step 5: Run monitoring and backup tests**

Run: `py -3.13 -m unittest test_locality_monitoring test_backup_db -v`

Expected: all checks pass, every critical condition fails closed, repeated invocation is isolated, and no test accesses the real server filesystem.

- [ ] **Step 6: Commit operational checks**

```bash
git add monitoring_regression_check.py backup_db.py test_locality_monitoring.py test_backup_db.py
git commit -m "feat: monitor locality sync and storage capacity"
```

---

### Task 8: End-to-End Shadow Verification and Operations Documentation

**Files:**
- Create: `test_company_locality_end_to_end.py`
- Modify: `HANDOVER.md`
- Modify: `docs/REMOTE_MAINTENANCE.md`

**Interfaces:**
- Consumes all prior tasks.
- Produces a documented, guarded `legacy -> shadow -> snapshot` runbook and a safe post-activation pause procedure.

- [ ] **Step 1: Write an end-to-end lifecycle test**

The fixture sequence must be:

1. Baseline an old Busan contract and an old non-Busan contract.
2. Move the first supplier out of Busan and the second supplier into Busan.
3. Rebuild in shadow mode and assert published totals are unchanged.
4. Add one post-outbound and one post-inbound contract.
5. Rebuild in snapshot mode and assert only the new contracts use the new statuses.
6. Simulate a supplier page failure and assert no partial status or snapshot changes.
7. Resolve an equal-time supplier conflict, a snapshot correction, and a canonical identity collision through audited pending requests; assert none affects the active generation until both caches are rebuilt and a successor pointer activates, and pointer rollback restores prior decisions.
8. Prove a pre-opened DB handle aborts first-activation quiescence, then crash at every forward/rollback transition phase including each DB commit and immediately before/after pointer replacement; assert no incomplete generation is visible, control commits preserve the source tuple, real input writes stale it, new writer attempts remain blocked, and restart recovery reconciles DB bookkeeping to the pointer before writes resume.
9. Run per-device capacity and exact-generation verified-backup gates and assert the fixture remains below thresholds.
10. Pause writes with the persisted fence, restart simulated cron/manual writers, and assert protected writes fail while API/dashboard reads continue from the active generation; legacy membership rollback is forbidden.

- [ ] **Step 2: Run the end-to-end test and fix only discovered integration defects**

Run: `py -3.13 -m unittest test_company_locality_end_to_end -v`

Expected: the complete transition, invariance, failure, and capacity sequence passes.

- [ ] **Step 3: Run the focused regression suite**

Run:

```bash
py -3.13 -m unittest \
  test_company_locality \
  test_company_locality_admin \
  test_maintenance_lock \
  test_locality_quiesce \
  test_company_sync \
  test_company_reconcile \
  test_company_lookup_contract \
  test_contract_population \
  test_locality_snapshot \
  test_locality_rate_integration \
  test_locality_consumer_integration \
  test_locality_generation \
  test_locality_writer_inventory \
  test_locality_consumer_inventory \
  test_company_locality_audit \
  test_locality_monitoring \
  test_company_locality_end_to_end \
  test_public_api_recovery \
  test_backup_db \
  test_alert_dates -v
```

Expected: all tests pass.

- [ ] **Step 4: Document cron and guarded rollout**

Document exact server commands using `/opt/busan/venv/bin/python3`, but no credentials. Add:

- Daily supplier change retry and durable per-supplier revalidation queue before the generation build.
- Daily rolling bucket command with `--dry-run` first and `--apply` after approval.
- One `build_locality_generation.py` command replacing separate API/monthly publication commands.
- Capacity, baseline, and comparison report locations.
- Seven-day shadow checklist requiring exact historical coverage, zero unknown historical amount, stable generations, no quarantined conflicts/collisions, and all rate gates.
- Maintenance-window activation commands: stop cron/systemd/manual DB users, verify no DB/WAL/SHM handles, create the fail-closed journal/marker, run the ordered dual-DB/pointer state machine, restart read services, and explicitly resume guarded writers only after verification.
- Activation values and the explicit pre-activation rollback path.
- Post-activation failure procedure: run the audited fence command, keep `LOCALITY_MODE=snapshot`, serve the last-known-good active pointer, repair/replay, verify backup and generation gates, then explicitly re-enable writes. Never switch historical rate reads back to current `company_master` membership.
- Verification that a same-generation backup, restore check, WAL checkpoint, and measured simultaneous peak all pass after DB growth.
- Privacy guidance limiting reports to aggregate counts/rates/capacity and excluding supplier identifiers.

- [ ] **Step 5: Commit documentation and end-to-end coverage**

```bash
git add test_company_locality_end_to_end.py HANDOVER.md docs/REMOTE_MAINTENANCE.md
git commit -m "docs: add guarded supplier locality rollout"
```

- [ ] **Step 6: Push code without activating production mode**

```bash
git push origin feature/company-locality-history
```

Expected: the review branch contains all code and tests; production remains `LOCALITY_MODE=legacy` until merge, server capacity, verified backup, exact baseline, and seven-day shadow gates pass.

- [ ] **Step 7: Run server preflight and report, not activation**

On the server, run the capacity and staging baseline commands from the runbook. Capture only aggregate rate, storage, coverage, and gate results. Do not publish supplier identifiers in the maintenance report. Activation is a separate explicit operation after seven successful shadow days.
