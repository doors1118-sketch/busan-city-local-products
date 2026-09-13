# Shopping performance recovery and guarded operation — 2026-09-13

Approved scope: import 135 verified missing dates (44,148 Busan rows; nationwide
946,137 received / 946,137 expected), and exclude exactly R25TA0111600801 and
R26TA0192750600 from both numerator and denominator. Preserve raw construction
rows. No global change to company locality, business-number prefixes or rates.

Expected approved snapshot: total KRW 9,295,833,432,256; local KRW 5,587,644,601,704;
60.109129992747%, displayed 60.1%. Shopping total KRW 783,433,852,853 and local
KRW 415,689,171,660. New daily data may subsequently change these figures.

## Scheduling and safety

- Shopping-only collector: 01:30 Asia/Seoul, every day; D-2, latest day first,
  unresolved dates since 2026-01-01 next, then remaining seven-day revisions.
- At most 100 API requests per run; preserve at least 150 reported requests;
  stop writing below 8 GiB available. No cross-service storage migration.
- All-page count, unique source identity and requested-date checks before each
  date's transactional replacement. Zero API results cannot erase existing rows.
- Failed, deferred and unattempted dates yield nonzero status and remain eligible.
- General daily contracts exclude shopping; 03:00 daily and 04:00 cache jobs use
  the same lock as shopping. Cache files are generated/validated before atomic
  publication. API reads fresh JSON on each request; no restart is required.
- Shopping watchdog at 07:45 every day and service OnFailure uses existing
  approved SMS configuration, only on issues. API acceptance is not handset
  delivery confirmation. No healthy-state or test SMS.

## Limits

The 117 legacy dates containing existing data are preserved, not certified as
API-complete merely because rows exist. Nationwide upstream errors, quota
exhaustion and external service downtime cannot be eliminated; the prevention
guarantee is detection, preservation and resumable retry, not zero external errors.
Existing unrelated weekly site-filter differences are not changed by this release.

## Recovery

Before deployment create a SQLite online backup, validate quick_check, and keep
old API/monthly caches, overrides, cron and exact prior Git commit. Use the
timestamped server backup directory recorded in the deployment receipt.
Hold `.procurement_pipeline.lock`, stop the two newly introduced timers, and
restore only changed scripts/configuration and the pre-release cache files.
Do not reset/checkout a dirty checkout, recursively chown services, or overwrite
newer data with a whole-DB restore without assessing writes since the snapshot.
For an immediate data rollback before other writes, use SQLite backup restore
under the lock and verify integrity; otherwise reverse only the imported keys
with a reviewed migration. Preserve the source shadow DB and audit evidence.

No secrets, private keys, environment values or database files belong in Git.
