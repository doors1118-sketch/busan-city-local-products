import os
import requests
import logging

logger = logging.getLogger("NTSClient")

NTS_API_URL = "https://api.odcloud.kr/api/nts-businessman/v1/status"

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
    
    try:
        resp = requests.post(NTS_API_URL, params=params, json=payload, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        results = {}
        for item in data.get("data", []):
            b_no = item.get("b_no")
            if not b_no: continue
            
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
            
        return {"success": True, "results": results}
    except requests.exceptions.Timeout:
        logger.error("NTS API timeout")
        return {"success": False, "error": "nts_timeout"}
    except requests.exceptions.RequestException:
        logger.error("NTS API http_error")
        return {"success": False, "error": "nts_http_error"}
    except Exception:
        logger.error("NTS API failed")
        return {"success": False, "error": "nts_api_failed"}
