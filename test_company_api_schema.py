import sys
import os
sys.path.append(os.path.abspath('app'))
import company_api
import pandas as pd
import io

print("--- 테스트 시작 ---")

# 1. 에러 스키마 반환 테스트
err_res = company_api._get_standard_error()
assert err_res["company_search_status"] == "failed"
assert err_res["error"] == "업체 후보 조회 실패"
assert isinstance(err_res["candidates"], list)
print("[O] _api_get() 실패 시 표준 실패 스키마 반환 정상")

# 2. meta + candidates 구조 정규화 테스트 (list 입력 시)
raw_list = [{"businessNo": "123", "업체명": "A", "company_id": "C01"}]
res_list = company_api._normalize_response(raw_list)
assert res_list["company_search_status"] == "success"
assert "businessNo" not in res_list["candidates"][0]
assert res_list["candidates"][0]["company_id"] == "C01"
print("[O] 리스트 입력 시 정규화 정상 (businessNo 제거, company_id 보존)")

# 3. candidates 우선 파싱 및 dict 입력 처리 테스트
raw_dict = {
    "candidates": [{"business_no": "456", "업체명": "B", "email": "test@test.com", "company_id": "C02"}],
    "data": [{"업체명": "잘못된데이터"}],
    "meta": {"page": 1}
}
res_dict = company_api._normalize_response(raw_dict)
assert res_dict["meta"] == {"page": 1}
assert "business_no" not in res_dict["candidates"][0]
assert "email" not in res_dict["candidates"][0]
assert res_dict["candidates"][0]["company_id"] == "C02"
print("[O] 딕셔너리 입력 시 candidates 필드 우선 파싱 및 정규화 정상")

# 4. _sanitize_candidate 후보 row 단위 적용 및 민감정보 삭제 확인
cand = {
    "businessNo": "A", "business_no": "B", "biz_no": "C", "사업자등록번호": "D",
    "internal_join_key": "E", "serviceKey": "F", "token": "G", "email": "H",
    "company_id": "I", "업체명": "J"
}
sanitized = company_api._sanitize_candidate(cand)
assert "businessNo" not in sanitized
assert "internal_join_key" not in sanitized
assert "token" not in sanitized
assert "company_id" in sanitized
assert sanitized["업체명"] == "J"
print("[O] _sanitize_candidate() 필터 정상 (company_id 보존, 그 외 삭제)")

# 5. format_company_results() 테스트 (company_id, 연락처 미출력)
fmt_res_dict = company_api.format_company_results(res_dict)
assert "1. B" in fmt_res_dict
assert "C02" not in fmt_res_dict  # company_id 노출 불가
fmt_res_list = company_api.format_company_results(res_list["candidates"])
assert "1. A" in fmt_res_list
print("[O] format_company_results() dict/list 양방향 지원 정상")

# 6. results_to_excel() 테스트 (허용 컬럼만 추출)
excel_bytes = company_api.results_to_excel(res_dict)
df = pd.read_excel(io.BytesIO(excel_bytes))
assert "업체명" in df.columns
assert "company_id" not in df.columns
assert "사업자등록번호" not in df.columns
print("[O] results_to_excel() 민감/내부 필드 배제 정상")

print("--- 모든 테스트 성공 ---")
