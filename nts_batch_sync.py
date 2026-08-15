"""
NTS(국세청) 사업자등록상태 야간 배치 갱신 모듈
- 부산업체 전체를 100건씩 배치로 조회하여 company_business_status 테이블 갱신
- 일 1회 야간 실행으로 챗봇 응답 경로에서 NTS API 호출 제거 가능
- 소요 시간: ~46,000업체 기준 약 4~5분
"""
import os
import sys
import sqlite3
import datetime
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("NTSBatchSync")

DB_FILE = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db'))
BATCH_SIZE = 100  # NTS API 1회 최대 100건


def _batch_sleep_seconds() -> float:
    try:
        sleep_seconds = float(os.environ.get("NTS_BATCH_SLEEP_SECONDS", "1.0"))
    except ValueError:
        sleep_seconds = 1.0
    return max(0.0, sleep_seconds)


def _ensure_failure_log_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nts_batch_failure_log (
            failure_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_started_at TEXT NOT NULL,
            batch_no INTEGER NOT NULL,
            failed_count INTEGER NOT NULL,
            error_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _mark_batch_failed(cursor, id_map, bno_list, now_str, error_message):
    error_message = (error_message or "nts_batch_failed")[:500]
    for b_no in bno_list:
        internal_id = id_map.get(b_no)
        if internal_id:
            cursor.execute("""
                INSERT INTO company_business_status
                (company_internal_id, business_status, business_status_freshness,
                 checked_at, business_status_source, retry_count, last_error_message, last_attempt_at)
                VALUES (?, 'api_failed', 'api_failed', ?, 'nts_batch', 1, ?, ?)
                ON CONFLICT(company_internal_id) DO UPDATE SET
                    business_status_freshness='api_failed',
                    business_status_source='nts_batch',
                    retry_count=IFNULL(company_business_status.retry_count, 0) + 1,
                    last_error_message=excluded.last_error_message,
                    last_attempt_at=excluded.last_attempt_at
            """, (internal_id, now_str, error_message, now_str))


def run_batch_sync(
    dry_run=False,
    probe=False,
    limit=None,
    only_failed=False,
    stale_days=None,
    abort_after_consecutive_failures=None,
):
    """
    부산업체 전체의 휴폐업 상태를 NTS API 배치 호출로 갱신.
    probe=True: 1배치(100건)만 테스트
    limit=N: 최대 N건만 처리
    only_failed=True: 직전 NTS 실패분만 재검증
    stale_days=N: 실패/미검증/마지막 검증일이 N일보다 오래된 업체만 재검증
    """
    import nts_business_status_client

    service_key = os.environ.get("NTS_SERVICE_KEY") or os.environ.get("SERVICE_KEY") or os.environ.get("SHOPPING_MALL_PRDCT_SERVICE_KEY")
    if not service_key:
        logger.error("NTS_SERVICE_KEY 또는 SERVICE_KEY 환경변수가 설정되지 않았습니다.")
        return False

    # NTS 클라이언트가 환경변수에서 직접 읽으므로 설정
    if not os.environ.get("NTS_SERVICE_KEY"):
        os.environ["NTS_SERVICE_KEY"] = service_key

    logger.info(
        "Starting NTS Batch Sync. dry_run=%s, probe=%s, limit=%s, only_failed=%s, stale_days=%s",
        dry_run, probe, limit, only_failed, stale_days
    )

    conn = sqlite3.connect(DB_FILE, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    if not dry_run:
        _ensure_failure_log_table(conn)

    # 부산업체 중 폐업 확정(fresh)된 업체는 제외.
    # full: 전체 검증, only_failed: 실패분만, stale_days: 실패/미검증/오래된 검증분 중심.
    target_filter = ""
    query_params = []
    if only_failed:
        target_filter = "AND cbs.business_status_freshness = 'api_failed'"
    elif stale_days is not None:
        cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=max(0, int(stale_days)))).strftime('%Y-%m-%d')
        target_filter = """
          AND (
              cbs.company_internal_id IS NULL
              OR cbs.business_status_freshness = 'api_failed'
              OR cbs.checked_at IS NULL
              OR SUBSTR(cbs.checked_at, 1, 10) < ?
          )
        """
        query_params.append(cutoff_date)
    rows = cursor.execute(f"""
        SELECT m.company_internal_id, i.canonical_business_no
        FROM company_master m
        JOIN company_identity i ON m.company_internal_id = i.company_internal_id
        LEFT JOIN company_business_status cbs ON m.company_internal_id = cbs.company_internal_id
        WHERE m.is_busan_company = 1
          AND i.canonical_business_no IS NOT NULL
          AND IFNULL(cbs.business_status, '') != 'closed'
          {target_filter}
        ORDER BY
          CASE
            WHEN cbs.business_status_freshness = 'api_failed' THEN 0
            WHEN cbs.company_internal_id IS NULL THEN 1
            WHEN cbs.checked_at IS NULL THEN 2
            ELSE 3
          END,
          cbs.checked_at
    """, query_params).fetchall()

    bno_pairs = [(row['company_internal_id'], row['canonical_business_no']) for row in rows]
    total_companies = len(bno_pairs)

    if probe:
        bno_pairs = bno_pairs[:BATCH_SIZE]
    elif limit:
        bno_pairs = bno_pairs[:limit]

    logger.info(f"Target companies: {len(bno_pairs)} (total Busan: {total_companies})")

    if (only_failed or stale_days is not None) and not bno_pairs:
        logger.info("No NTS rows to retry.")
        conn.close()
        print("NTS 재검증 대상 없음")
        return True

    start_time = datetime.datetime.now()
    now_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    sleep_seconds = _batch_sleep_seconds()

    updated_count = 0
    failed_count = 0
    batch_count = 0
    failed_batches = []
    consecutive_failed_batches = 0
    if abort_after_consecutive_failures is None:
        try:
            abort_after_consecutive_failures = int(os.environ.get("NTS_ABORT_AFTER_CONSECUTIVE_FAILURES", "8"))
        except ValueError:
            abort_after_consecutive_failures = 8
    abort_after_consecutive_failures = max(0, abort_after_consecutive_failures)

    # 배치 단위로 처리
    for i in range(0, len(bno_pairs), BATCH_SIZE):
        batch = bno_pairs[i:i + BATCH_SIZE]
        batch_count += 1
        bno_list = [pair[1] for pair in batch]
        id_map = {pair[1]: pair[0] for pair in batch}

        if dry_run:
            logger.info(f"  Batch {batch_count}: {len(batch)} companies (dry-run, skip API)")
            updated_count += len(batch)
            continue

        try:
            res = nts_business_status_client.check_business_status(bno_list)

            if not res.get("success"):
                err = res.get("error") or "nts_api_failed"
                attempts = res.get("attempts")
                err_label = f"{err}, attempts={attempts}" if attempts else err
                logger.warning(f"  Batch {batch_count}: API 실패 - {err_label}")
                failed_count += len(batch)
                failed_batches.append((batch_count, len(batch), err_label))
                consecutive_failed_batches += 1
                _mark_batch_failed(cursor, id_map, bno_list, now_str, err_label)
                cursor.execute("""
                    INSERT INTO nts_batch_failure_log
                    (run_started_at, batch_no, failed_count, error_message)
                    VALUES (?, ?, ?, ?)
                """, (now_str, batch_count, len(batch), err_label[:500]))
                conn.commit()
                if abort_after_consecutive_failures and consecutive_failed_batches >= abort_after_consecutive_failures:
                    logger.error("Abort NTS batch: consecutive API failures reached %s", consecutive_failed_batches)
                    print(f"NTS API 연속 실패 {consecutive_failed_batches}개 배치로 중단")
                    break
                continue

            results = res.get("results", {})
            consecutive_failed_batches = 0
            for b_no, status_info in results.items():
                internal_id = id_map.get(b_no)
                if not internal_id:
                    continue

                cursor.execute("""
                    INSERT INTO company_business_status
                    (company_internal_id, business_status, business_status_freshness, tax_type, closed_at, api_result_code, checked_at, business_status_source)
                    VALUES (?, ?, 'fresh', ?, ?, ?, ?, 'nts_batch')
                    ON CONFLICT(company_internal_id) DO UPDATE SET
                        business_status=excluded.business_status,
                        business_status_freshness='fresh',
                        tax_type=excluded.tax_type,
                        closed_at=excluded.closed_at,
                        api_result_code=excluded.api_result_code,
                        checked_at=excluded.checked_at,
                        business_status_source='nts_batch',
                        retry_count=0,
                        last_error_message=NULL,
                        last_attempt_at=NULL
                """, (
                    internal_id,
                    status_info["business_status"],
                    status_info.get("tax_type"),
                    status_info.get("closed_at"),
                    status_info.get("api_result_code"),
                    now_str
                ))
                updated_count += 1

            logger.info(f"  Batch {batch_count}: {len(results)}/{len(batch)} updated")

            conn.commit()

            time.sleep(sleep_seconds)  # Rate limiting

        except Exception as e:
            err_label = f"{type(e).__name__}: {e}"
            logger.error(f"  Batch {batch_count}: Exception - {err_label}")
            failed_count += len(batch)
            failed_batches.append((batch_count, len(batch), err_label))
            consecutive_failed_batches += 1
            try:
                _mark_batch_failed(cursor, id_map, bno_list, now_str, err_label)
                cursor.execute("""
                    INSERT INTO nts_batch_failure_log
                    (run_started_at, batch_no, failed_count, error_message)
                    VALUES (?, ?, ?, ?)
                """, (now_str, batch_count, len(batch), err_label[:500]))
                conn.commit()
            except Exception as mark_error:
                logger.error(f"  Batch {batch_count}: failure marker write failed - {mark_error}")
                try:
                    conn.rollback()
                except Exception:
                    pass
            if abort_after_consecutive_failures and consecutive_failed_batches >= abort_after_consecutive_failures:
                logger.error("Abort NTS batch: consecutive exceptions reached %s", consecutive_failed_batches)
                print(f"NTS API 연속 예외 {consecutive_failed_batches}개 배치로 중단")
                break

    # ETL 로그
    if not dry_run:
        end_time = datetime.datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        if only_failed:
            source_name = "nts_api_batch_retry_failed"
        elif stale_days is not None:
            source_name = "nts_api_batch_incremental"
        else:
            source_name = "nts_api_batch_full"

        summary_error = None
        if failed_batches:
            summary_error = "; ".join(
                f"batch={batch_no},count={count},error={err}" for batch_no, count, err in failed_batches[:5]
            )
            if len(failed_batches) > 5:
                summary_error += f"; ... {len(failed_batches) - 5} more"

        cursor.execute("""
            INSERT INTO etl_job_log (
                job_name, source_name, started_at, finished_at, status,
                input_row_count, inserted_count, skipped_count, error_count, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "nts_batch_sync", source_name, now_str,
            end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "success" if failed_count == 0 else "partial",
            len(bno_pairs), updated_count, 0, failed_count, summary_error
        ))

        cursor.execute("""
            INSERT INTO source_manifest (source_name, source_type, row_count, source_refreshed_at, status)
            VALUES ('nts_batch', 'api_batch', ?, ?, ?)
            ON CONFLICT(source_name) DO UPDATE SET
                row_count=excluded.row_count,
                source_refreshed_at=excluded.source_refreshed_at,
                status=excluded.status
        """, (updated_count, now_str, "success" if failed_count == 0 else "partial"))

        conn.commit()
        conn.close()

        logger.info(f"NTS Batch Sync 완료. "
                     f"처리: {len(bno_pairs)}건, 갱신: {updated_count}건, 실패: {failed_count}건, "
                     f"소요: {elapsed:.1f}초 ({batch_count} batches)")
        print(f"NTS Batch Sync 완료: 처리 {len(bno_pairs)}건, 갱신 {updated_count}건, 실패 {failed_count}건, "
              f"상태 {'success' if failed_count == 0 else 'partial'}, 소요 {elapsed:.1f}초")
        if failed_batches:
            first = failed_batches[0]
            print(f"NTS 실패 배치: {len(failed_batches)}개, 첫 실패 batch={first[0]}, count={first[1]}, error={first[2]}")
    else:
        conn.close()
        logger.info(f"NTS Batch Sync (dry-run). 대상: {len(bno_pairs)}건, 배치: {batch_count}개")

    if probe:
        print(f"\n=== NTS Batch Probe Result ===")
        print(f"대상: {len(bno_pairs)}건 (전체 {total_companies}건 중)")
        print(f"갱신: {updated_count}건")
        print(f"실패: {failed_count}건")

    return failed_count == 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="NTS 사업자등록상태 야간 배치 갱신")
    parser.add_argument("--dry-run", action="store_true", help="DB 기록 없이 대상 건수만 확인")
    parser.add_argument("--probe", action="store_true", help="1배치(100건)만 테스트")
    parser.add_argument("--limit", type=int, help="최대 처리 건수 제한")
    parser.add_argument("--only-failed", action="store_true", help="직전 NTS 실패분(api_failed)만 재검증")
    parser.add_argument("--stale-days", type=int, help="미검증/실패/마지막 검증일이 지정 일수보다 오래된 업체만 재검증")
    parser.add_argument("--abort-after-consecutive-failures", type=int, help="연속 실패 배치가 지정 횟수에 도달하면 조기 중단")
    args = parser.parse_args()

    success = run_batch_sync(
        dry_run=args.dry_run,
        probe=args.probe,
        limit=args.limit,
        only_failed=args.only_failed,
        stale_days=args.stale_days,
        abort_after_consecutive_failures=args.abort_after_consecutive_failures,
    )
    if not success:
        sys.exit(1)
