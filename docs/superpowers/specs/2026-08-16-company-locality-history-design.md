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

Use four coordinated controls:

1. Current supplier status instead of hard deletion.
2. Immutable contract-supplier locality snapshots for published historical calculations.
3. Supplier-specific incremental job state and strict page completeness checks.
4. Rolling revalidation of the complete current supplier population.

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
| `effective_at` | Source change time used for the status transition |
| `inactive_at` | Time the supplier stopped qualifying as current local |
| `inactive_reason` | Auditable status reason |
| `last_verified_at` | Last successful direct or change-feed verification |
| `source_chg_dt` | Latest G2B source change timestamp |

`company_master` remains the descriptive supplier record. Consumers that need the current local population use `company_locality_status.status = 'active_local'`. Rows are retained rather than deleted.

### `company_locality_event`

Append-only audit history of status transitions. It stores previous status, new status, source payload hash, source change time, processing time, and job identifier.

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
| `corrected_at` | Set only by an audited correction |
| `correction_reason` | Required for a correction |

The award-rate numerator reads this snapshot for contracts that have one. A later supplier move changes only current supplier status and future contract classifications.

### `company_sync_job_log`

Supplier synchronization has an independent job log keyed by source date and job type. It records expected rows, received rows, page count, retry count, status, and error details. Contract collection success cannot mark supplier synchronization successful.

## Data Flow

### Baseline Before Any Status Cleanup

1. Back up both SQLite databases with the existing atomic backup mechanism.
2. Recompute the currently published population with the existing classifier.
3. Create `contract_supplier_locality` rows for every supplier share used by the current calculation, with `basis = 'legacy_baseline_v1'`.
4. Recalculate the rate from snapshots.
5. Require the old and snapshot totals to match by sector and agency before proceeding.

Status cleanup must not run before this baseline completes. This ordering prevents an immediate historical-rate drop.

### Daily Change Feed

1. Fetch all nationwide changed rows for the source date into memory or a staging table.
2. Retry each page up to three times.
3. Treat any page failure or `received_rows != totalCount` as a failed supplier job. Do not partially apply the batch.
4. In one transaction, process every changed row before applying the Busan filter:
   - Busan head office: insert or update descriptive data and set `active_local`.
   - Existing local supplier now outside Busan: retain the row and set `moved_out`.
   - Existing local supplier now a branch: retain the row and set `branch_changed`.
   - New non-Busan supplier: ignore after staging.
5. Append status transitions to `company_locality_event`.
6. Mark only the supplier job date successful.

The daily query uses an overlap window so a late source update can be seen again. Upserts and events must be idempotent.

### Rolling Revalidation

Partition current and unverified suppliers into 30 stable buckets by business-number hash. Each day, directly re-query one bucket by business number with rate limiting and bounded concurrency. This rechecks the complete population within 30 days and repairs misses that predate the new job log.

Failures remain `unverified` or retain the last confirmed status with a stale flag; they are not automatically treated as non-local. The dashboard and monitoring report expose verification age.

### New Contract Classification

For a new contract or a newly observed revision:

1. Reuse an existing snapshot when present.
2. Otherwise classify each supplier using the existing policy and available contract-time evidence.
3. Store the result and evidence in `contract_supplier_locality` before aggregation.
4. Do not overwrite a frozen result during ordinary cache rebuilds.

A corrected source record can create a new contract revision snapshot. Changing a published snapshot requires a separate command with operator identity, reason, before/after values, and an impact preview.

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

- Rounded overall and sector order amounts: exact match.
- Rounded overall and sector local amounts: exact match.
- Displayed rates: exact match at one decimal place.
- Contract count differences: zero after applying the same deduplication rules.

### Enforcement gate

Run at least seven successful shadow days. Do not enable snapshot enforcement when any condition is true:

- Supplier job has an incomplete page or unresolved failed date.
- Unknown locality exceeds 0.1% of total order amount.
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
- Filesystem total, used, available, and percentage used.
- Eligible contract count and parsed supplier-share count by sector.
- Estimated snapshot, index, status, event, and report sizes using a conservative per-row allowance.
- Projected migration peak including new rows, possible WAL growth, one verified raw SQLite backup, one worst-case gzip output, and the existing 256 MiB backup safety margin.

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

The existing backup implementation already budgets a raw online snapshot, a worst-case gzip output, and a 256 MiB margin. The new locality data must be included in its database list and rechecked against that guard.

### Ongoing monitoring

`monitoring_regression_check.py` reports database, WAL, backup, and locality-table sizes. During the first 30-day revalidation cycle, retain daily size history and alert on:

- Filesystem usage at or above 75%.
- More than 15% week-over-week locality-table growth after baseline creation.
- Snapshot rows growing faster than eligible contract-supplier rows.
- Event growth without corresponding source changes.
- Old shadow reports or temporary files exceeding their retention period.

After the first cycle, set the long-term threshold from the observed contract and event growth rather than a guessed fixed row count.

## Failure Handling and Rollback

- Feature mode: `legacy`, `shadow`, or `snapshot`.
- Default during rollout: `shadow`.
- A supplier batch applies atomically only after completeness validation.
- Existing descriptive rows and historical snapshots are never hard-deleted by synchronization.
- If snapshot cache generation fails, preserve the last known-good cache and report the error. Do not silently rebuild from a partially updated supplier set.
- Rollback changes the feature mode to `legacy`; new tables remain for investigation.

## Test Strategy

### Unit tests

- Busan head office remains active.
- Existing supplier moving outside Busan becomes `moved_out` without deletion.
- Existing supplier becoming a branch becomes `branch_changed`.
- Historical contract snapshot remains local after either transition.
- A contract after the transition is classified from the new status.
- Replayed change rows are idempotent.
- Any failed page or total-count mismatch prevents a transaction commit.
- Supplier failed dates are retried even when contract collection succeeded.
- Snapshot correction requires a reason and records before/after values.

### Integration tests

- Build a fixture with all four sectors, joint shares, contract revisions, and shopping change orders.
- Compare legacy and baseline snapshot totals exactly.
- Simulate a move, rerun the cache, and prove historical totals remain unchanged.
- Add a new post-move contract and prove only that contract is non-local.
- Verify `monitoring_regression_check.py` fails on incomplete supplier sync, stale revalidation, or snapshot coverage gaps.

## Planned Code Boundaries

- `company_locality.py`: schema, status transitions, direct verification, and job-state APIs.
- `daily_pipeline_sync.py`: orchestration and source fetching; delegates lifecycle decisions.
- `core_calc.py`: snapshot-aware supplier locality lookup with legacy fallback controlled by feature mode.
- `company_locality_audit.py`: shadow comparison and activation-gate report.
- `monitoring_regression_check.py`: operational checks for sync completeness and snapshot coverage.
- Focused unit and integration test modules for lifecycle, retry, and rate invariance.

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
8. Keep the legacy path available for rollback through the first full 30-day revalidation cycle.
