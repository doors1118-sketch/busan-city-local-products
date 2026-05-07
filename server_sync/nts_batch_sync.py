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


def run_batch_sync(dry_run=False, probe=False, limit=None):
    """
    부산업체 전체의 휴폐업 상태를 NTS API 배치 호출로 갱신.
    probe=True: 1배치(100건)만 테스트
    limit=N: 최대 N건만 처리
    """
    import nts_business_status_client

    service_key = os.environ.get("NTS_SERVICE_KEY") or os.environ.get("SERVICE_KEY") or os.environ.get("SHOPPING_MALL_PRDCT_SERVICE_KEY")
    if not service_key:
        logger.error("NTS_SERVICE_KEY 또는 SERVICE_KEY 환경변수가 설정되지 않았습니다.")
        return False

    # NTS 클라이언트가 환경변수에서 직접 읽으므로 설정
    if not os.environ.get("NTS_SERVICE_KEY"):
        os.environ["NTS_SERVICE_KEY"] = service_key

    logger.info(f"Starting NTS Batch Sync. dry_run={dry_run}, probe={probe}, limit={limit}")

    conn = sqlite3.connect(DB_FILE, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 부산업체 중 폐업 확정(fresh)된 업체는 제외
    rows = cursor.execute("""
        SELECT m.company_internal_id, i.canonical_business_no
        FROM company_master m
        JOIN company_identity i ON m.company_internal_id = i.company_internal_id
        LEFT JOIN company_business_status cbs ON m.company_internal_id = cbs.company_internal_id
        WHERE m.is_busan_company = 1
          AND i.canonical_business_no IS NOT NULL
          AND IFNULL(cbs.business_status, '') != 'closed'
    """).fetchall()

    bno_pairs = [(row['company_internal_id'], row['canonical_business_no']) for row in rows]
    total_companies = len(bno_pairs)

    if probe:
        bno_pairs = bno_pairs[:BATCH_SIZE]
    elif limit:
        bno_pairs = bno_pairs[:limit]

    logger.info(f"Target companies: {len(bno_pairs)} (total Busan: {total_companies})")

    start_time = datetime.datetime.now()
    now_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

    updated_count = 0
    failed_count = 0
    batch_count = 0

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
                logger.warning(f"  Batch {batch_count}: API 실패 - {res.get('error')}")
                failed_count += len(batch)
                # 실패 시에도 기록
                for b_no in bno_list:
                    internal_id = id_map.get(b_no)
                    if internal_id:
                        cursor.execute("""
                            INSERT INTO company_business_status
                            (company_internal_id, business_status, business_status_freshness, checked_at, business_status_source)
                            VALUES (?, 'api_failed', 'api_failed', ?, 'nts_batch')
                            ON CONFLICT(company_internal_id) DO UPDATE SET
                                business_status_freshness='api_failed',
                                checked_at=excluded.checked_at,
                                business_status_source='nts_batch'
                        """, (internal_id, now_str))
                continue

            results = res.get("results", {})
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
                        business_status_source='nts_batch'
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

            # 커밋은 10배치마다 (1,000건 단위)
            if batch_count % 10 == 0:
                conn.commit()

            time.sleep(0.3)  # Rate limiting

        except Exception as e:
            logger.error(f"  Batch {batch_count}: Exception - {e}")
            failed_count += len(batch)

    # ETL 로그
    if not dry_run:
        end_time = datetime.datetime.now()
        elapsed = (end_time - start_time).total_seconds()

        cursor.execute("""
            INSERT INTO etl_job_log (
                job_name, source_name, started_at, finished_at, status,
                input_row_count, inserted_count, skipped_count, error_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "nts_batch_sync", "nts_api_batch", now_str,
            end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "success" if failed_count == 0 else "partial",
            len(bno_pairs), updated_count, 0, failed_count
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
    args = parser.parse_args()

    success = run_batch_sync(dry_run=args.dry_run, probe=args.probe, limit=args.limit)
    if not success:
        sys.exit(1)
