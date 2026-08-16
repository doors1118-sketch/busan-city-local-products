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
- Baseline overall/sector order and local amounts must match exactly after rounding, with zero canonical-contract count difference.
- Block activation above 0.1 percentage-point overall difference, 0.3 percentage-point sector difference, 0.1% unknown denominator, or unexplained 1.0 percentage-point agency difference.
- Block migration at projected filesystem usage of 80% or more, or post-migration free space below 2 GiB or 20%, whichever requires more free space.
- Do not store full API payloads in audit tables or generated Git-tracked files.
- Use `apply_patch` for manual source edits and preserve unrelated worktree changes.

## File Map

- Create `company_locality.py`: supplier status schema, transitions, events, job state, and rolling bucket selection.
- Create `company_sync.py`: strict paginated API collection with dependency injection for tests.
- Create `locality_snapshot.py`: canonical contract keys, frozen decisions, mode-aware resolver, and snapshot coverage.
- Create `company_locality_audit.py`: capacity preflight, staging baseline, legacy/snapshot comparison, and activation gates.
- Modify `daily_pipeline_sync.py`: delegate supplier synchronization and keep supplier catch-up independent from contract success.
- Modify `core_calc.py`: allow a bound locality resolver while preserving legacy defaults.
- Modify `build_api_cache.py`, `build_monthly_cache.py`, `rate_calc_db.py`, and `export_excel.py`: construct and pass the same resolver.
- Modify `monitoring_regression_check.py`: supplier sync, snapshot coverage, and storage checks.
- Modify `HANDOVER.md` and `docs/REMOTE_MAINTENANCE.md`: cron, shadow rollout, reports, and rollback.
- Create focused `unittest` modules listed in each task.

---

### Task 1: Supplier Locality Schema and State Transitions

**Files:**
- Create: `company_locality.py`
- Test: `test_company_locality.py`

**Interfaces:**
- Produces: `ensure_locality_schema(conn: sqlite3.Connection) -> None`
- Produces: `apply_company_changes(conn, items, source_date, job_id, verified_at) -> ChangeSummary`
- Produces: `active_local_biznos(conn: sqlite3.Connection) -> set[str]`
- Produces: `status_at(conn, bizno, effective_at) -> str | None`
- Produces: `start_sync_job`, `finish_sync_job`, and `fail_sync_job`

- [ ] **Step 1: Write transition tests**

Create `test_company_locality.py` with temporary SQLite fixtures covering current-local bootstrap, outbound move, head-office-to-branch change, inbound insertion, re-entry after outbound, and replay idempotence.

```python
class CompanyLocalityTransitionTests(unittest.TestCase):
    def test_outbound_supplier_is_retained_but_inactivated(self):
        apply_company_changes(self.conn, [source_item("1234567890", "경남", "본사", "202608160900")], "20260816", "job-1", NOW)
        row = self.conn.execute("SELECT status, inactive_reason FROM company_locality_status WHERE bizno='1234567890'").fetchone()
        self.assertEqual(row, ("moved_out", "region_changed"))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM company_master WHERE bizno='1234567890'").fetchone()[0], 1)

    def test_inbound_supplier_becomes_active_from_source_change_time(self):
        apply_company_changes(self.conn, [source_item("2222222222", "부산", "본사", "202608161015")], "20260816", "job-2", NOW)
        self.assertEqual(status_at(self.conn, "2222222222", "2026-08-16 10:15:00"), "active_local")
        self.assertIsNone(status_at(self.conn, "2222222222", "2026-08-15 23:59:59"))
```

- [ ] **Step 2: Run tests and confirm the module is missing**

Run: `py -3.13 -m unittest test_company_locality -v`

Expected: import failure for `company_locality`.

- [ ] **Step 3: Implement schemas and normalized state transitions**

Create `company_locality.py` with `normalize_bizno`, immutable `ChangeSummary`, schema migration, and transactional transition handling. Use these table shapes:

```sql
CREATE TABLE IF NOT EXISTS company_locality_status (
    bizno TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('active_local','moved_out','branch_changed','unverified')),
    source_rgn_nm TEXT NOT NULL DEFAULT '',
    source_hdoffce_div_nm TEXT NOT NULL DEFAULT '',
    effective_at TEXT NOT NULL,
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
    effective_at TEXT NOT NULL,
    source_chg_dt TEXT NOT NULL DEFAULT '',
    payload_hash TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    job_id TEXT NOT NULL,
    UNIQUE(bizno, new_status, source_chg_dt, payload_hash)
);

CREATE TABLE IF NOT EXISTS company_sync_job_log (
    job_name TEXT NOT NULL,
    source_date TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','success','failed')),
    expected_rows INTEGER,
    received_rows INTEGER,
    page_count INTEGER,
    retry_count INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error_detail TEXT,
    PRIMARY KEY(job_name, source_date)
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

Ignore new non-Busan suppliers after staging, but apply a non-local transition when the business number already exists in `company_master` or `company_locality_status`. Bootstrap existing master rows as `active_local` with `effective_at='1900-01-01 00:00:00'` and `source_chg_dt` from the row when available.

- [ ] **Step 4: Run transition tests**

Run: `py -3.13 -m unittest test_company_locality -v`

Expected: all tests pass, including exactly one event after replaying the same source row.

- [ ] **Step 5: Commit the state layer**

```bash
git add company_locality.py test_company_locality.py
git commit -m "feat: track supplier locality transitions"
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
- Produces: `pending_supplier_dates(conn, through_date, lookback_days=30) -> list[str]`
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

def test_supplier_failure_is_pending_even_when_contract_date_succeeded(self):
    fail_sync_job(self.conn, "company_changes", "20260815", "page 2 timeout", NOW)
    self.assertEqual(pending_supplier_dates(self.conn, "20260816"), ["20260815", "20260816"])
```

- [ ] **Step 2: Run the new tests and confirm failure**

Run: `py -3.13 -m unittest test_company_sync -v`

Expected: import or symbol failures.

- [ ] **Step 3: Implement strict pagination**

In `company_sync.py`, calculate pages with `math.ceil(total_count / rows_per_page)`, retry each page three times, and raise `IncompleteCompanyBatch` when a page fails or final received count differs from `totalCount`. Return an immutable batch only after all pages pass.

```python
@dataclass(frozen=True)
class CompanyBatch:
    items: tuple[dict, ...]
    total_count: int
    page_count: int
    retry_count: int
```

Do not return `([], 0)` for an exhausted retry.

- [ ] **Step 4: Replace the inline updater with orchestration**

Keep the existing API URL and request parameters, but pass the HTTP page reader into `sync_company_change_date`. Record `company_changes` success independently. In `main()`, collect pending supplier dates separately from the contract `sync_log` and retry oldest first. A supplier failure may allow contract collection to continue, but cannot create a supplier success row.

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

**Interfaces:**
- Consumes: Task 1 `apply_company_changes` and job-state APIs.
- Produces: `bucket_for_bizno(bizno: str, bucket_count: int = 30) -> int`
- Produces: `biznos_for_bucket(conn, bucket, bucket_count=30) -> list[str]`
- Produces: `revalidate_bucket(conn, source_client, run_date, bucket_count=30, workers=8) -> RevalidationSummary`

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

Assign buckets with the first eight hex characters of SHA-256 modulo `bucket_count`. Re-query one bucket per day by normalized business number. Use at most eight workers by default, three retries, and configurable delay. Apply successful rows in one database transaction after all responses complete.

When a direct lookup fails, retain the last confirmed status and leave `last_verified_at` unchanged so monitoring marks it stale. Use `unverified` only when the business number has no prior confirmed status. Never infer `moved_out` from an empty or failed response.

- [ ] **Step 4: Add CLI dry-run and apply modes**

Support these commands without embedding credentials:

```bash
python3 company_reconcile.py --date 2026-08-16 --bucket-count 30 --dry-run
python3 company_reconcile.py --date 2026-08-16 --bucket-count 30 --apply
```

Dry-run prints counts only. Apply writes status/events and a `company_revalidation` job row.

- [ ] **Step 5: Run reconciliation tests**

Run: `py -3.13 -m unittest test_company_reconcile test_company_locality -v`

Expected: inbound, outbound, branch, empty-response, and idempotence tests pass.

- [ ] **Step 6: Commit rolling verification**

```bash
git add company_reconcile.py company_locality.py test_company_reconcile.py
git commit -m "feat: reconcile supplier locality on a rolling cycle"
```

---

### Task 4: Frozen Contract-Supplier Locality Snapshots

**Files:**
- Create: `locality_snapshot.py`
- Modify: `core_calc.py:587-658`
- Test: `test_locality_snapshot.py`

**Interfaces:**
- Produces: `ensure_snapshot_schema(conn: sqlite3.Connection) -> None`
- Produces: `canonical_contract_identity(row, sector, is_shopping=False) -> ContractIdentity`
- Produces: `SnapshotResolver.seed(row, bizno, share_pct, is_busan, basis) -> None`
- Produces: `SnapshotResolver.resolve(row, bizno, share_pct, legacy_is_local) -> bool`
- Produces: `SnapshotResolver.flush() -> int`
- `process_contract_row` gains optional `locality_resolver=None` and `sector=None`; both default to the exact legacy path.

- [ ] **Step 1: Write snapshot invariance tests**

```python
def test_outbound_move_does_not_change_frozen_historical_contract(self):
    resolver = SnapshotResolver(self.proc_conn, self.company_conn, mode="snapshot", now=NOW)
    resolver.seed(self.old_contract, "1234567890", 100.0, True, "legacy_baseline_v1")
    before = resolver.resolve(self.old_contract, "1234567890", 100.0, True)
    apply_company_changes(
        self.company_conn,
        [source_item("1234567890", "경남", "본사", "202608160900")],
        "20260816", "job-outbound", NOW,
    )
    after = resolver.resolve(self.old_contract, "1234567890", 100.0, False)
    self.assertTrue(before)
    self.assertTrue(after)

def test_pre_inbound_contract_remains_non_local(self):
    resolver.seed(self.old_contract, "2222222222", 100.0, False, "legacy_baseline_v1")
    apply_company_changes(
        self.company_conn,
        [source_item("2222222222", "부산", "본사", "202608161015")],
        "20260816", "job-inbound", NOW,
    )
    self.assertFalse(resolver.resolve(self.old_contract, "2222222222", 100.0, True))
    self.assertTrue(resolver.resolve(self.new_contract, "2222222222", 100.0, True))
```

- [ ] **Step 2: Run and confirm tests fail**

Run: `py -3.13 -m unittest test_locality_snapshot -v`

Expected: missing snapshot module or resolver argument.

- [ ] **Step 3: Implement the compact snapshot table**

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
    corrected_at TEXT,
    correction_reason TEXT,
    PRIMARY KEY(sector, contract_key, contract_revision, bizno)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_contract_locality_bizno_date
ON contract_supplier_locality(bizno, contract_date);
```

Use normalized `dcsnCntrctNo` when present, otherwise `untyCntrctNo`; use `dlvrReqNo:prdctSno` and numeric `dlvrReqChgOrd` for shopping. Reject rows without a stable key during baseline and report their amount as unknown rather than inventing a key.

- [ ] **Step 4: Add resolver support without changing legacy behavior**

In `process_contract_row`, compute `legacy_is_local` with the unchanged current expression. Call the bound resolver only when provided:

```python
legacy_is_local = bno in biznos or (len(bno) >= 3 and bno[:3] in BUSAN_BIZNO_PREFIXES)
is_local = locality_resolver.resolve(row, bno, share, legacy_is_local) if locality_resolver else legacy_is_local
if is_local:
    loc_amt += amt * (share / 100.0)
```

Legacy mode returns the legacy value. Shadow mode records the candidate snapshot but returns the legacy value. Snapshot mode returns the frozen value, or creates a new decision using effective status and existing classification policy.

- [ ] **Step 5: Run core and snapshot tests**

Run: `py -3.13 -m unittest test_locality_snapshot test_company_locality -v`

Expected: legacy calculation is byte-for-byte unchanged when no resolver is supplied; snapshot tests pass.

- [ ] **Step 6: Commit snapshot primitives**

```bash
git add locality_snapshot.py core_calc.py test_locality_snapshot.py
git commit -m "feat: freeze contract-time supplier locality"
```

---

### Task 5: Use One Resolver Across All Rate Outputs

**Files:**
- Modify: `build_api_cache.py`
- Modify: `build_monthly_cache.py`
- Modify: `rate_calc_db.py`
- Modify: `export_excel.py`
- Test: `test_locality_rate_integration.py`

**Interfaces:**
- Consumes: Task 4 `SnapshotResolver` and new `process_contract_row` arguments.
- Produces: `build_locality_resolver(proc_conn, company_conn, mode, sector) -> SnapshotResolver`
- Produces identical legacy output in `legacy` and `shadow` modes.

- [ ] **Step 1: Write four-sector integration fixtures**

Build temporary agency, company, and procurement databases with one local and one non-local supplier in construction, service, goods, and shopping. Include a joint contract, a contract revision, and a shopping change order. Assert legacy and shadow totals are identical and snapshot coverage contains only canonical rows.

```python
self.assertEqual(legacy["overall"], shadow["overall"])
self.assertEqual(legacy["sectors"], shadow["sectors"])
self.assertEqual(snapshot_count, expected_canonical_supplier_count)
```

- [ ] **Step 2: Run and confirm integration failure**

Run: `py -3.13 -m unittest test_locality_rate_integration -v`

Expected: builders do not yet create/pass a resolver.

- [ ] **Step 3: Thread the resolver through every calculation path**

Read `LOCALITY_MODE` once per command, validate it against `legacy|shadow|snapshot`, create sector-bound resolvers, and pass `sector` plus `locality_resolver` to every `process_contract_row` call in the four files. Add missing contract date and stable identifier columns to SELECT lists, including weekly and daily queries and shopping receipt dates.

Flush pending snapshots only after a successful complete build. In `shadow`, continue writing the legacy `api_cache.json` values.

- [ ] **Step 4: Verify all calculation surfaces agree**

Run: `py -3.13 -m unittest test_locality_rate_integration test_locality_snapshot -v`

Expected: API cache, monthly cache, report, and export fixtures agree for the same mode and historical snapshots do not change after supplier transitions.

- [ ] **Step 5: Commit resolver integration**

```bash
git add build_api_cache.py build_monthly_cache.py rate_calc_db.py export_excel.py test_locality_rate_integration.py
git commit -m "feat: apply locality snapshots across rate outputs"
```

---

### Task 6: Rate and Capacity Audit CLI

**Files:**
- Create: `company_locality_audit.py`
- Test: `test_company_locality_audit.py`

**Interfaces:**
- Consumes: Tasks 1-5 schemas and resolver.
- Produces: `collect_capacity_report(paths, eligible_supplier_rows) -> dict`
- Produces: `evaluate_capacity_gates(report) -> list[GateFailure]`
- Produces: `compare_rate_results(legacy, snapshot) -> dict`
- Produces CLI commands `capacity`, `baseline`, `compare`, and `correct`.

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
```

- [ ] **Step 2: Run and confirm audit tests fail**

Run: `py -3.13 -m unittest test_company_locality_audit -v`

Expected: missing audit functions.

- [ ] **Step 3: Implement exact preflight and measured staging reports**

Use `Path.stat()`, `shutil.disk_usage`, SQLite `page_count`, `page_size`, table counts, and parsed supplier-share counts. Include main DB, `-wal`, `-shm`, cache, local backup, estimated indexes, one raw backup, one worst-case gzip, and the existing 256 MiB margin.

Write reports atomically to configurable paths under `sync_log/`:

```bash
python3 company_locality_audit.py capacity --output sync_log/locality_capacity.json
python3 company_locality_audit.py baseline --staging-db /opt/busan/tmp/locality-baseline.db --output sync_log/locality_baseline.json
python3 company_locality_audit.py compare --output sync_log/locality_impact.json
```

Exit nonzero on any failed gate. Never modify `api_cache.json` from audit commands.

- [ ] **Step 4: Implement controlled snapshot correction**

Require `--operator`, `--reason`, contract identity, business number, and explicit old/new value. Print impact and require `--apply` for mutation. Store `corrected_at` and `correction_reason`; emit no supplier PII beyond the normalized internal business number already required for the operator command.

- [ ] **Step 5: Run audit tests**

Run: `py -3.13 -m unittest test_company_locality_audit test_locality_rate_integration -v`

Expected: exact baseline, rate thresholds, capacity thresholds, atomic report writing, and correction audit tests pass.

- [ ] **Step 6: Commit audit tooling**

```bash
git add company_locality_audit.py test_company_locality_audit.py
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
- Produces regression records for supplier freshness, failed dates, coverage, database growth, WAL size, and projected backup peak.

- [ ] **Step 1: Write monitoring failure tests**

Cover an unresolved supplier failed date, verification older than 31 days, snapshot coverage below 99.9%, filesystem usage at 75%, abnormal week-over-week table growth, and a backup projected above its capacity ceiling.

- [ ] **Step 2: Run and confirm monitoring tests fail**

Run: `py -3.13 -m unittest test_locality_monitoring test_backup_db -v`

Expected: missing locality checks.

- [ ] **Step 3: Add locality operational checks**

Record critical failures for incomplete supplier jobs and snapshot enforcement coverage gaps. Record warnings for stale verification and 75% filesystem use. Read sizes without writing to the databases. Include `procurement_contracts.db`, `busan_companies_master.db`, WAL/SHM companions, and backup directory totals.

- [ ] **Step 4: Revalidate backup capacity against enlarged databases**

Keep the existing `source_size * 2 + 256 MiB` worst-case formula. Add a test that enlarged company/procurement source sizes are each checked independently and that no locality migration bypasses `assert_capacity`.

- [ ] **Step 5: Run monitoring and backup tests**

Run: `py -3.13 -m unittest test_locality_monitoring test_backup_db -v`

Expected: all checks pass and no test accesses the real server filesystem.

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
- Produces a documented, reversible `legacy -> shadow -> snapshot` runbook.

- [ ] **Step 1: Write an end-to-end lifecycle test**

The fixture sequence must be:

1. Baseline an old Busan contract and an old non-Busan contract.
2. Move the first supplier out of Busan and the second supplier into Busan.
3. Rebuild in shadow mode and assert published totals are unchanged.
4. Add one post-outbound and one post-inbound contract.
5. Rebuild in snapshot mode and assert only the new contracts use the new statuses.
6. Simulate a supplier page failure and assert no partial status or snapshot changes.
7. Run capacity gates and assert the fixture remains below thresholds.

- [ ] **Step 2: Run the end-to-end test and fix only discovered integration defects**

Run: `py -3.13 -m unittest test_company_locality_end_to_end -v`

Expected: the complete transition, invariance, failure, and capacity sequence passes.

- [ ] **Step 3: Run the focused regression suite**

Run:

```bash
py -3.13 -m unittest \
  test_company_locality \
  test_company_sync \
  test_company_reconcile \
  test_locality_snapshot \
  test_locality_rate_integration \
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

- Daily supplier change retry before cache build.
- Daily rolling bucket command with `--dry-run` first and `--apply` after approval.
- Capacity, baseline, and comparison report locations.
- Seven-day shadow checklist.
- Activation and rollback environment values.
- Verification that backups still pass after measured DB growth.

- [ ] **Step 5: Commit documentation and end-to-end coverage**

```bash
git add test_company_locality_end_to_end.py HANDOVER.md docs/REMOTE_MAINTENANCE.md
git commit -m "docs: add guarded supplier locality rollout"
```

- [ ] **Step 6: Push code without activating production mode**

```bash
git push origin main
```

Expected: GitHub `main` contains all code and tests; production remains `LOCALITY_MODE=legacy` until server capacity and exact baseline gates pass.

- [ ] **Step 7: Run server preflight and report, not activation**

On the server, run the capacity and staging baseline commands from the runbook. Capture only aggregate rate, storage, coverage, and gate results. Do not publish supplier identifiers in the maintenance report. Activation is a separate explicit operation after seven successful shadow days.
