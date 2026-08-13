import os
import requests
import logging
import time

logger = logging.getLogger("NTSClient")

NTS_API_URL = "https://api.odcloud.kr/api/nts-businessman/v1/status"

def _retry_settings() -> tuple[int, float]:
    try:
        max_retries = int(os.environ.get("NTS_MAX_RETRIES", "3"))
    except ValueError:
        max_retries = 3
    try:
        backoff_seconds = float(os.environ.get("NTS_RETRY_BACKOFF_SECONDS", "2.0"))
    except ValueError:
        backoff_seconds = 2.0
    return max(1, max_retries), max(0.0, backoff_seconds)


def _request_timeout_seconds() -> float:
    try:
        timeout_seconds = float(os.environ.get("NTS_TIMEOUT_SECONDS", "10"))
    except ValueError:
        timeout_seconds = 10.0
    return max(1.0, timeout_seconds)


def check_business_status(bno_list: list) -> dict:
    service_key = os.environ.get("NTS_SERVICE_KEY")
    if not service_key:
        logger.warning("NTS_SERVICE_KEY not found in environment.")
        return {"success": False, "error": "NTS_SERVICE_KEY not set"}
        
    params = {"serviceKey": service_key}
    headers = {"Content-Type": "application/json"}
    
    # NTS API는 하이픈 없는 사업자번호를 배열로 받음
    cleaned_bno_list = [b.replace("-", "") for b in bno_list]
    payload = {"b_no": cleaned_bno_list}

    max_retries, backoff_seconds = _retry_settings()
    timeout_seconds = _request_timeout_seconds()
    last_error = "nts_api_failed"

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(NTS_API_URL, params=params, json=payload, headers=headers, timeout=timeout_seconds)
            if resp.status_code >= 400:
                last_error = f"nts_http_{resp.status_code}"
                if resp.status_code < 500 and resp.status_code != 429:
                    logger.error("NTS API non-retryable http_error status=%s", resp.status_code)
                    return {"success": False, "error": last_error, "attempts": attempt}
                resp.raise_for_status()

            data = resp.json()

            results = {}
            for item in data.get("data", []):
                b_no = item.get("b_no")
                if not b_no:
                    continue

                b_stt_cd = item.get("b_stt_cd")
                # 01: 계속사업자, 02: 휴업자, 03: 폐업자
                if b_stt_cd == "01":
                    status = "active"
                elif b_stt_cd == "02":
                    status = "suspended"
                elif b_stt_cd == "03":
                    status = "closed"
                else:
                    status = "unknown"

                tax_type = item.get("tax_type", "")
                end_dt = item.get("end_dt", "") # YYYYMMDD

                results[b_no] = {
                    "business_status": status,
                    "tax_type": tax_type,
                    "closed_at": end_dt if status == "closed" else None,
                    "api_result_code": b_stt_cd
                }

            return {"success": True, "results": results, "attempts": attempt}
        except requests.exceptions.Timeout:
            last_error = "nts_timeout"
            logger.warning("NTS API timeout attempt=%s/%s", attempt, max_retries)
        except requests.exceptions.RequestException as exc:
            last_error = "nts_http_error"
            logger.warning("NTS API http_error attempt=%s/%s error=%s", attempt, max_retries, type(exc).__name__)
        except ValueError:
            last_error = "nts_invalid_json"
            logger.warning("NTS API invalid_json attempt=%s/%s", attempt, max_retries)
        except Exception as exc:
            last_error = "nts_api_failed"
            logger.warning("NTS API failed attempt=%s/%s error=%s", attempt, max_retries, type(exc).__name__)

        if attempt < max_retries:
            time.sleep(backoff_seconds * attempt)

    return {"success": False, "error": last_error, "attempts": max_retries}
