# Company Locality History and Reconciliation Design

## Status

- Date: 2026-08-16
- Scope: `busan_companies_master.db` lifecycle management and procurement-rate locality preservation
- Decision: A supplier's locality for an existing contract remains fixed at the contract-time determination, even if the supplier later moves outside Busan.

## Goal

Keep the current Busan supplier master accurate without retroactively changing historical local-award results.

The design must solve both current gaps:

1. A supplier that moves outside Busan or changes from head office to branch must stop appearing as a current local supplier.
2. A missed daily change feed must be detected and recovered without depending on the contract pipeline's success state.

It must also prove the impact on the published award rate before the new behavior can be enabled.

## Non-Goals

- This change does not redefine the legal or policy meaning of a Busan supplier.
- This change does not remove the existing business-number-prefix fallback or address heuristics. Those require a separate policy change and impact review.
- Historical locality snapshots are not silently rewritten after publication. Corrections require an explicit audited operation.

## Current Risks

`daily_pipeline_sync.py` currently fetches nationwide changed suppliers but filters to `Busan + head office` before writing. A previously stored supplier that moves outside Busan therefore receives no update and remains in the local master.

API page failures return an empty result, which can be reported as "no changes." Supplier-step failures are non-critical to the contract job, so the shared daily `sync_log` can still record the date as successful and prevent supplier-specific catch-up.

Award-rate calculation reads a current set of local business numbers. Removing a moved supplier from that set before preserving contract-time decisions would retroactively reduce historical local-award amounts.

## Recommended Architecture

Use six coordinated controls:

1. Current supplier status instead of hard deletion.
2. Immutable contract-supplier locality snapshots for published historical calculations.
3. Supplier-specific incremental job state and strict page completeness checks.
4. Rolling revalidation of the complete current supplier population.
5. One versioned canonical-contract iterator shared by baseline, caches, reports, and exports.
6. One cross-process maintenance lock and generation protocol for both SQLite databases, cache publication, migration, and backup.

The rollout is shadow-first. No published calculation changes until a comparison report passes the acceptance gates.

## Data Model

### `company_locality_status`

Stored in `busan_companies_master.db` and keyed by normalized business number.

| Column | Purpose |
| --- | --- |
| `bizno` | Primary key |
| `status` | `active_local`, `moved_out`, `branch_changed`, or `unverified` |
| `source_rgn_nm` | Latest source region |
| `source_hdoffce_div_nm` | Latest source head-office division |
| `source_effective_at` | Normalized G2B source change time used for ordering |
| `observed_at` | Time this system received the source row |
| `inactive_at` | Time the supplier stopped qualifying as current local |
| `inactive_reason` | Auditable status reason |
| `last_verified_at` | Last successful direct or change-feed verification |
| `source_chg_dt` | Latest G2B source change timestamp |

`company_master` remains the descriptive supplier record. Consumers that need the current local population use `company_locality_status.status = 'active_local'`. Rows are retained rather than deleted.

### `company_locality_event`

Append-only audit history of status transitions. It stores previous status, new status, a locality-governing hash, a separate descriptive-payload hash, normalized source-effective time, observed/processed time, job identifier, and event disposition. The locality hash includes only normalized business number, region, head-office division, and source-effective time, so harmless descriptive changes do not create locality conflicts. Retrograde events and equal-time divergent locality hashes are quarantined and cannot replace current state.

`company_locality_resolution` is an append-only operator audit for resolving a quarantined locality conflict. It records the event IDs and source evidence considered, selected before/after status and effective time, operator, reason, generation, and resolution time. Replay remains deterministic after resolution.

### `contract_supplier_locality`

Stored with the procurement data and keyed by sector, canonical contract key, contract revision, and supplier business number.

| Column | Purpose |
| --- | --- |
| `sector` | Construction, service, goods, or shopping |
| `contract_key` | Stable normalized contract identifier |
| `contract_revision` | Contract or delivery-request change order |
| `bizno` | Supplier business number |
| `share_pct` | Supplier share used by the calculation |
| `is_busan` | Frozen contract-time locality result; nullable when unresolved |
| `basis` | Master, contract address, award address, branch rule, or legacy baseline |
| `contract_date` | Date used for the historical decision |
| `classified_at` | Classification time |
| `classifier_version` | Reproducibility marker |
| `content_fingerprint` | Hash of the canonical amount/date/agency/sorted supplier-share content |
| `baseline_id` | Manifest generation that owns a historical snapshot |
| `introduced_generation_id` | Prepared/active generation that first made this immutable decision visible |

The award-rate numerator reads this snapshot for contracts that have one. A later supplier move changes only current supplier status and future contract classifications. A pre-cutover contract without a manifest-owned snapshot is a hard build failure in snapshot mode; it never falls back to current status. Prepared-generation rows are invisible unless their generation is in the ancestry named by the active pointer.

`contract_supplier_locality_correction_request` is append-only and records an operator's requested correction instead of mutating the original decision. It stores operator, reason, old/new value, source evidence, impact preview fingerprint, and request time. A request becomes visible only when the publication orchestrator consumes it into a prepared successor generation, records its ID/hash in that generation manifest, rebuilds both caches, and activates the pointer. Rolling the pointer back naturally excludes the correction.

`canonical_collision_resolution` is an append-only audited selection of one source fingerprint for a quarantined canonical identity. It follows the same pending-input and successor-generation publication rule. At most one unsuperseded request may exist per snapshot identity or collision fingerprint; conflicting requests block generation preparation.

### `locality_baseline_manifest`

Records the cutover timestamp, classifier version, canonical-iterator version, per-sector contract/supplier/share/amount counts, full content fingerprint, cache generation ID, and completion status. Production baseline coverage is exactly 100% by canonical contract revision, normalized supplier, aggregated share, and rounded amount across every published rate surface.

### `company_sync_job_log`

Supplier synchronization has an independent job log keyed by source date and job type. It records expected rows, received rows, page count, retry/call counts, call-budget ceiling, circuit-breaker state, status, and error details. A child response-metric table stores response class and count. Contract collection success cannot mark supplier synchronization successful.

`company_revalidation_queue` stores one durable row per business number with attempt count, last response class, next-attempt time, and `pending`, `deferred_budget`, `failed`, or `complete` state. Pending work is processed before the next daily bucket and never expires merely because a lookback window elapsed.

### Generation, activation, and backup manifests

`locality_generation` records a parent generation, both source DB generation IDs, mode, baseline ID, immutable cache-directory name, `prepared|active|abandoned` status, manifest hash, and timestamps. Only one generation may be prepared while holding the maintenance lock.

`locality_activation_state` is persisted in both databases and contains `active_generation_id`, `ever_snapshot_activated`, and `writes_enabled`. An fsynced activation journal and `locality_writes_paused` marker coordinate the two databases. Fence or first-snapshot activation is allowed only in a maintenance window after cron/systemd/manual database users are stopped and an OS file-handle inspection proves no process has either DB, WAL, or SHM open. Both databases are then opened with exclusive write transactions before state changes begin.

Pause creates the marker and journal before either commit, sets both databases fail-closed while holding both exclusive transactions, and commits one only while the other remains exclusively held. Resume keeps the marker present, enables both databases under exclusive transactions, and removes/fsyncs the marker only after both commits and all repair/backup checks. The writer inventory guarantees that any process started afterward either honors the marker/maintenance lock or is permanently disabled after cutover. Guard triggers reject direct protected-table writes. Recovery reads the journal and pointer, reacquires quiescence and both exclusive transactions, keeps both DBs trigger-disabled until they agree, and only then may resume writes. Every crash point is roll-forward recoverable; there is no automatic unfencing.

Each database has a separate generation clock with `data_generation` and `control_revision`. Source tables that can change locality or rate results, including contract/evidence data, supplier status, effective-time resolutions, and pending correction/collision requests, bump `data_generation` transactionally through classified triggers. Output and control-plane tables, including snapshots, generation rows, activation state, fence audit, job metrics, backup manifests, and post-activation correction lifecycle events, bump only `control_revision` or neither when stored outside the DB. Cache manifests record only the two source `data_generation` values plus their input/correction hashes. Activation and recovery bookkeeping therefore cannot stale a prepared cache tuple, while any genuine input mutation does.

Each verified backup has a durable manifest containing both databases' data-generation and control-revision values, active pointer ID, file and logical sizes, SHA-256 checksums, SQLite integrity/restore-check results, local and remote upload state, and completion time.

### Canonical contract identity

All calculation paths consume one canonical iterator.

- Construction/service/goods namespace: sector plus normalized `dcsnCntrctNo` family (all but the final two revision characters) and explicit final-two-character revision. If `dcsnCntrctNo` is absent, use normalized `untyCntrctNo` as the family and an empty revision.
- Shopping namespace: normalized `dlvrReqNo`, `prdctSno`, and numeric `dlvrReqChgOrd`.
- Aggregate duplicate supplier entries by normalized business number before fingerprinting.
- Select rows with explicit deterministic ordering; unordered `keep='last'` is prohibited.
- One identity mapping to divergent content is quarantined and blocks baseline/cache publication until an audited correction resolves it.

## Data Flow

### Baseline Before Any Status Cleanup

1. Back up both SQLite databases with the existing atomic backup mechanism.
2. Recompute the currently published population with the existing classifier.
3. Run the shared canonical iterator across every published rate surface and create `contract_supplier_locality` rows for every normalized supplier share, with `basis = 'legacy_baseline_v1'`.
4. Persist a complete manifest with per-row fingerprints and reconcile the manifest back to every canonical contract-supplier row.
5. Recalculate the rate from snapshots.
6. Require 100% historical row/share/amount coverage and exact old/snapshot totals by sector and agency before proceeding. Unknown historical amount, unstable identity, or offsetting aggregate differences block cleanup.

Status cleanup must not run before this baseline completes. This ordering prevents an immediate historical-rate drop.

### Daily Change Feed

1. Fetch all nationwide changed rows for the source date into memory or a staging table using normal TLS certificate verification.
2. Retry each page up to three times with exponential backoff, jitter, `Retry-After`, configurable QPS/daily-call ceilings, and a circuit breaker.
3. Require stable page metadata/`totalCount`, required response fields, unique source record identities, and exact final completeness. Any page drift, duplicate identity, malformed response, or failure makes the whole date fail; no partial batch is applied.
4. In one transaction, process every changed row before applying the Busan filter:
   - Busan head office: insert or update descriptive data and set `active_local`.
   - A supplier moving from another region into Busan: insert it or reactivate it as `active_local` from the source effective time.
   - Existing local supplier now outside Busan: retain the row and set `moved_out`.
   - Existing local supplier now a branch: retain the row and set `branch_changed`.
   - New non-Busan supplier: ignore after staging.
5. Append status transitions to `company_locality_event`.
6. Mark only the supplier job date successful.

The daily query uses an explicit overlap-window job so a late source update can be seen again. Unresolved dates remain pending without a 30-day expiry. Upserts and events must be idempotent.

All timestamps normalize to Asia/Seoul. `chgDt` is the source-effective time and receipt time is stored separately. Older events never replace newer state. Equal-time divergent payloads and invalid/missing change times are quarantined as ambiguous.

### Rolling Revalidation

Partition current and unverified suppliers into 30 stable buckets by business-number hash. Each day, drain due rows from the durable revalidation queue before directly querying the day's bucket by business number with the same request budget, backoff, schema validation, and circuit breaker as the change feed. Persist each supplier outcome and enqueue budget-deferred or failed lookups with bounded backoff. This rechecks the complete population within 30 days while recovering individual misses independently of the bucket cycle.

Failures remain `unverified` or retain the last confirmed status with a stale flag; they are not automatically treated as non-local. The dashboard and monitoring report expose verification age.

### New Contract Classification

For a new contract or a newly observed revision:

1. Reuse an existing snapshot when present.
2. If the identity is a new revision of a known contract family, inherit each unchanged supplier's prior decision; classify only newly introduced suppliers at the revision effective date.
3. Otherwise classify each supplier using the existing policy and available contract-time evidence.
4. Store the result and evidence in `contract_supplier_locality` before aggregation.
5. Do not overwrite a frozen result during ordinary cache rebuilds.

A corrected source record can create a new contract revision snapshot. Changing a published snapshot requires a separate command with operator identity, reason, before/after values, and an impact preview.

Construction/service/goods use `cntrctCnclsDate`; shopping uses `dlvrReqRcptDate`. Dates are interpreted in Asia/Seoul. When a contract has date-only precision and a status transition occurs on the same local date, the new decision is unknown and blocks publication until evidence or an audited correction resolves the boundary. An inbound supplier is local only after the confirmed inbound boundary. Existing pre-inbound contract snapshots remain non-local and are not changed retroactively.

### Concurrency and generation-set publication

All migration, supplier apply, reconciliation, baseline, snapshot write, cache build, and backup commands acquire the same absolute-path maintenance lock. Lock order is maintenance lock, company DB, then procurement DB. Connections use WAL, bounded `busy_timeout`, explicit transactions, verified checkpoints, and one write-session helper that checks the persisted fence and bumps the relevant generation on commit.

A reviewed writer inventory classifies every script that can mutate either production database as `migrated`, `disabled_after_cutover`, or `not_deployed`. Production/manual writers in the first class use the write-session helper. Disabled scripts fail closed after first snapshot activation. A static inventory test scans SQL-writing Python files and fails when an unclassified writer is introduced. Read-only report/export commands never create snapshot rows.

One publication orchestrator builds API and monthly caches together. It reads stable source generation IDs, creates a `prepared` locality generation, performs every required sector without broad exception suppression, and stores any new immutable snapshot decisions under that prepared generation. It writes all cache files and a manifest beneath an immutable `cache_generations/<generation_id>/` directory, fsyncs each file and the directory, and verifies source generations again.

Visibility changes only by atomically replacing and fsyncing one small `active_locality_generation.json` pointer containing the new generation, previous generation, and manifest hash. API/dashboard readers load that pointer once per request, then use only the named cache directory and snapshot generation ancestry; a valid pointer to a still-`prepared` row is usable because the pointer is authoritative. Readers retain the prior in-memory generation on failure or, after restart, validate and load the persisted previous generation. A crash before pointer replacement leaves an invisible prepared generation. Recovery marks a pointer-matching prepared generation active for bookkeeping, otherwise marks it abandoned and removes its unreferenced rows/files only after a verified backup. Contract replacement delete and insert occur in one transaction.

Every forward or rollback pointer transition uses this ordered state machine: (1) create/fsync a transition journal and pause marker; (2) set `writes_enabled=0` in both databases under the dual-exclusive protocol, also setting `ever_snapshot_activated=1` for the first snapshot transition; (3) verify both control rows and the target generation bundle/source tuple; (4) replace/fsync the pointer; (5) reconcile repairable `active_generation_id` bookkeeping in both databases to the pointer; (6) verify pointer/DB agreement; (7) enable guarded writes in both databases and remove the marker last only when all gates pass. `guarded_write_session` rejects pointer/DB disagreement.

The first snapshot transition additionally requires the full process/file-handle quiescence described above. Later successor activation and predecessor rollback still acquire the maintenance lock, marker, and both exclusive transactions, but need not stop read-only services because all legacy writers are permanently disabled and every supported writer honors the lock/marker. A transition command reports success only after both DB rows agree and the marker is removed. Any crash leaves writes paused; recovery derives read visibility from the pointer, rolls both control rows forward to that pointer, and completes or keeps the transition fenced. It never moves the pointer to match one partially committed database row.

## Shadow Impact Analysis

Add an audit command that runs the legacy and snapshot paths against the same database without changing `api_cache.json`.

The report contains:

- Overall and sector totals, local amounts, rates, and percentage-point differences.
- Agency-level differences, sorted by absolute local-amount impact.
- Suppliers changing current status and their affected contract counts.
- Contract-level before/after locality and share evidence.
- Unknown locality amount and percentage of the denominator.
- API completeness, stale verification counts, and oldest verification age.

Write machine-readable JSON and a concise CSV summary under `sync_log/`; do not commit generated reports or supplier identifiers to Git.

## Activation Gates

### Baseline gate

Before current-status reconciliation starts, the snapshot calculation must match the published legacy calculation:

- Historical manifest coverage: exactly 100%; no fallback attempt and no unknown historical amount.
- Rounded overall and sector order amounts: exact match.
- Rounded overall and sector local amounts: exact match.
- Displayed rates: exact match at one decimal place.
- Contract count differences: zero after applying the same deduplication rules.

### Enforcement gate

Run at least seven successful shadow days. Do not enable snapshot enforcement when any condition is true:

- Supplier job has an incomplete page or unresolved failed date.
- Unknown locality for post-cutover contracts exceeds 0.1% of total order amount. Historical unknown amount is always zero.
- Overall rate difference exceeds 0.1 percentage points.
- Any sector rate difference exceeds 0.3 percentage points.
- A single agency changes by more than 1.0 percentage point without an explained contract list.

An expected difference caused solely by new contracts after a confirmed move is permitted only after the report identifies those contracts and their effective dates.

## Expected Award-Rate Impact

Immediate published impact should be zero because all already-counted contracts are baselined before current supplier statuses change. Current supplier search and recommendation results may change when stale moved or branch records are deactivated.

Future award-rate differences should be limited to contracts first classified after a supplier's confirmed locality transition. The shadow report quantifies these differences before enforcement. A large immediate historical change indicates an ordering, snapshot-coverage, or contract-key defect and blocks deployment.

## Storage and Peak-Capacity Assessment

Capacity validation is a deployment gate, not a post-deployment observation. Measure both steady-state growth and temporary peak usage.

### Data kept small by design

- Store one locality snapshot only for each supplier in the canonical contract revision that is eligible for the rate calculation. Do not snapshot discarded duplicate revisions or create a new row on every cache build.
- Store source fields and a payload hash in locality events, not complete API payloads.
- Keep only indexes used by snapshot lookup, current-status lookup, and audit reporting.
- Generated shadow JSON/CSV reports use bounded retention and never contain full raw supplier payloads.

### Preflight estimate

Before creating tables or copying data, the audit command reports:

- Main DB, WAL, SHM, cache, log, and local-backup sizes.
- Filesystem total, used, available, and percentage used independently for every device containing a database, WAL/SHM, cache generation, staging path, temporary compression output, or backup destination.
- Eligible contract count and parsed supplier-share count by sector.
- Estimated snapshot, index, status, event, and report sizes using a conservative per-row allowance.
- Projected simultaneous migration peak per device including logical SQLite page sizes, measured plus worst-case baseline-write WAL growth, staging DB and indexes, new rows, retained backup, one verified raw SQLite backup, one worst-case gzip output plus gzip temp, reports, and the existing 256 MiB backup safety margin.
- API staging bytes measured from encoded response samples with a conservative multiplier or from actual staging SQLite pages. API rows-per-page is telemetry, not a byte estimate.

The estimate uses actual server row counts. Repository documentation counts are not accepted as a deployment basis.

### Measured staging run

Create the baseline first in a disposable staging database after the preflight guard passes. Record SQLite `page_count * page_size` before and after table creation, index creation, and baseline insertion. Use the measured delta to replace the estimate before touching production data.

The staging database is deleted only after its report and checksum have been preserved. Production baseline creation uses bounded transactions and checkpoints the WAL after completion.

### Capacity gates

Block migration or activation when any condition is true:

- Projected migration or backup peak would use 80% or more of the filesystem.
- Projected post-migration free space is below 2 GiB or 20% of the filesystem, whichever requires more free space.
- Measured snapshot storage exceeds 1 GiB or 30% of the current procurement database without an explicit design review.
- The existing backup capacity guard would reject any database after the new tables are included.
- WAL cannot be checkpointed or temporary migration files remain after the staging run.

The existing backup implementation's `source_size * 2` estimate is not sufficient for this rollout. Capacity uses logical page size plus WAL/SHM and all simultaneous staging/backup files on each actual device. A verified, restorable, same-generation backup of both databases is required before baseline cleanup or activation. Ongoing writes require a non-stale last-known-good verified backup and enough projected capacity; each newly active generation must be backed up and verified before any older generation or backup is removed.

### Ongoing monitoring

`monitoring_regression_check.py` acquires the maintenance lock for one consistent observation, or retries when before/after generation IDs differ. It verifies the active pointer, cache hashes, snapshot ancestry, and durable backup manifest in addition to reporting database, WAL, backup, and locality-table sizes. During the first 30-day revalidation cycle, retain daily size history and alert on:

- Filesystem usage at or above 75%.
- More than 15% week-over-week locality-table growth after baseline creation.
- Snapshot rows growing faster than eligible contract-supplier rows.
- Event growth without corresponding source changes.
- Old shadow reports or temporary files exceeding their retention period.

After the first cycle, set the long-term threshold from the observed contract and event growth rather than a guessed fixed row count.

## Failure Handling and Rollback

- Feature mode: `legacy`, `shadow`, or `snapshot`.
- Default during rollout: `shadow`.
- After snapshot activation, rollback sets the persisted write fence under the maintenance lock, leaves the active generation pointer unchanged, and serves last-known-good snapshot caches. Every protected-table trigger and supported writer rejects mutation while fenced. It must not return to legacy membership reads.
- A supplier batch applies atomically only after completeness validation.
- Existing descriptive rows and historical snapshots are never hard-deleted by synchronization.
- If snapshot cache generation fails, preserve the last known-good cache and report the error. Do not silently rebuild from a partially updated supplier set.
- Coordinated database-and-cache restoration is disaster recovery and requires verified same-generation backups. `legacy` is available only before first snapshot activation.
- Pausing or resuming writes through the fence is an audited operator command with a required reason. Restarted cron and manual commands read the same persisted state before opening a write transaction.

## Test Strategy

### Unit tests

- Busan head office remains active.
- Existing supplier moving outside Busan becomes `moved_out` without deletion.
- Existing supplier becoming a branch becomes `branch_changed`.
- A new or returning supplier moving into Busan becomes `active_local` from the inbound effective time.
- Historical contract snapshot remains local after either transition.
- A pre-inbound historical contract remains non-local after the supplier moves into Busan.
- A contract after the transition is classified from the new status.
- Replayed change rows are idempotent.
- Retrograde and equal-time divergent events are quarantined.
- Descriptive-only equal-time changes do not create locality conflicts; locality conflicts require an audited resolution before publication.
- Same-day date-only transition boundaries remain unknown.
- Any failed page or total-count mismatch prevents a transaction commit.
- Supplier failed dates are retried even when contract collection succeeded.
- Snapshot correction requires a reason and records before/after values.
- Revalidation failures and budget deferrals persist per supplier and are retried before the next bucket.
- A write fence survives restart and blocks cron, manual, supplier, snapshot, and protected input-table writes while reads continue.

### Integration tests

- Build a fixture with all four sectors, joint shares, contract revisions, and shopping change orders.
- Compare legacy and baseline snapshot rows, shares, amounts, fingerprints, and totals exactly at 100% coverage.
- Test identity collisions, reused unified numbers, same-revision corrections, missing keys, duplicate suppliers, and shopping change-order encodings.
- Simulate a move, rerun the cache, and prove historical totals remain unchanged.
- Add a new post-move contract and prove only that contract is non-local.
- Crash at each generation-build phase and prove no incomplete generation becomes active; recover pointer-matching prepared state deterministically.
- Verify API and dashboard requests load one active pointer and reject mixed cache/snapshot generations.
- Verify `monitoring_regression_check.py` fails on incomplete supplier sync, stale revalidation, or snapshot coverage gaps.

## Planned Code Boundaries

- `maintenance_lock.py`: shared process lock, write sessions, lock ordering, generation stamps, write fence, guard-trigger coverage, and checkpoint helpers.
- `locality_generation.py`: immutable generation directories, prepared/active/abandoned lifecycle, single-pointer activation, and crash recovery.
- `company_locality.py`: schema, status transitions, ordered event history, direct verification, and job-state APIs.
- `company_locality_admin.py`: audited supplier-conflict resolution, snapshot correction, and write-fence commands.
- `contract_population.py`: versioned identity and one canonical iterator for every calculation surface.
- `daily_pipeline_sync.py`: orchestration and source fetching; delegates lifecycle decisions.
- `core_calc.py`: centralized snapshot-aware supplier locality lookup; no pre-cutover fallback is allowed in snapshot mode.
- `company_locality_audit.py`: shadow comparison and activation-gate report.
- `monitoring_regression_check.py`: operational checks for sync completeness and snapshot coverage.
- `locality_writer_inventory.json`: every database writer classified as migrated, disabled after cutover, or not deployed; CI fails on an unclassified writer.
- `locality_consumer_inventory.json`: every direct `company_master` reader classified as current-local, historical metadata, or not deployed; CI fails on an unclassified reader.
- Focused unit and integration test modules for lifecycle, retry, and rate invariance.
- Current-supplier consumers, including `api_server.py`, `dashboard.py`, `alert_check.py`, `nts_batch_sync.py`, `bootstrap_master_data.py`, `rate_calc.py`, deployed `server_sync` copies, cache supplier counts/names, chatbot migration/search, recommendations, and industry joins, use one shared active-local relation; frozen rate snapshots remain independent.

## Operational Risks

- G2B change time may differ from the legal effective time of a relocation. The source change time is used unless a better effective date exists.
- Directly checking roughly 1/30 of the population daily adds API traffic and runtime. Concurrency and rate limits must be configurable.
- Business numbers with complex branch relationships still follow the existing policy in this scope.
- Baseline snapshots preserve the current published decision, including any existing classifier errors. Correcting those errors is a separate audited policy migration.
- Snapshot tables increase database size and backup time; deployment verification must measure both before activation.
- Baseline creation can temporarily consume more space than the final tables because of WAL, staging, and backup files. Capacity gates cover the combined peak rather than only the final DB size.

## Deployment Sequence

1. Deploy audit tooling in `legacy` mode and run the capacity preflight.
2. Create a measured staging baseline and pass all capacity gates.
3. Deploy schema, back up databases, and create the production historical baseline.
4. Pass the exact rate baseline gate and re-run the backup capacity guard.
5. Enable daily status reconciliation and rolling verification in `shadow` mode.
6. Observe at least seven successful days and review rate and storage reports.
7. Enable `snapshot` mode only after all gates pass.
8. Keep snapshot reads available with writes paused for rollback through the first full 30-day revalidation cycle; keep same-generation database/cache backups for disaster recovery.
