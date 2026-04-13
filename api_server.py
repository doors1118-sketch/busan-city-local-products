"""
부산 조달 모니터링 REST API 서버
=================================
캐시 파일(api_cache.json)을 읽어서 즉시 응답

실행: python api_server.py
문서: http://localhost:8000/docs
"""
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import json, sys, math, os, sqlite3
from typing import Optional

sys.stdout.reconfigure(encoding='utf-8')

# NaN-safe JSON encoder: NaN/Inf → null (FastAPI 500 방지)
class NaNSafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return super().default(obj)

class NaNSafeResponse(JSONResponse):
    def render(self, content):
        return json.dumps(content, ensure_ascii=False, cls=NaNSafeEncoder).encode('utf-8')

app = FastAPI(title="부산 조달 모니터링 API", version="1.0", default_response_class=NaNSafeResponse)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

CACHE_FILE = 'api_cache.json'
DB_COMPANIES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'busan_companies_master.db')

def _get_company_db():
    """부산 업체 마스터 DB 연결 (읽기 전용)"""
    conn = sqlite3.connect(f"file:{DB_COMPANIES}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn

def load_cache():
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] 캐시 로드 실패: {e}")
        return {"error": f"캐시 파일 로드 실패: {str(e)}"}

@app.get("/", include_in_schema=False)
def root():
    """루트 접속 시 API 문서로 이동"""
    return RedirectResponse(url="/docs")

@app.get("/api/summary", tags=["대시보드"])
def get_summary():
    """종합 수주율 (전체/분야별/그룹별/그룹별×분야별)"""
    cache = load_cache()
    return {k: v for k, v in cache.items() if not k.startswith("5_")}

@app.get("/api/ranking", tags=["대시보드"])
def get_ranking():
    """비교단위 수주율 랭킹 (전체+분야별, 그룹별 상/하위 10)"""
    cache = load_cache()
    return {
        "generated_at": cache.get("generated_at"),
        "전체": cache.get("5_기관랭킹_전체", {}),
        "분야별": cache.get("5_기관랭킹_분야별", {}),
        "소그룹": cache.get("5_기관랭킹_소그룹", {}),
    }

@app.get("/api/ranking/{sector}", tags=["대시보드"])
def get_ranking_by_sector(sector: str):
    """특정 분야 수주율 랭킹 (공사/용역/물품/쇼핑몰)"""
    cache = load_cache()
    data = cache.get("5_기관랭킹_분야별", {}).get(sector)
    if not data:
        return {"error": f"'{sector}' 분야를 찾을 수 없습니다. (공사/용역/물품/쇼핑몰)"}
    return {"generated_at": cache.get("generated_at"), "분야": sector, "랭킹": data}

@app.get("/api/leakage", tags=["유출 분석"])
def get_leakage():
    """유출 분석 전체: 쇼핑몰 유출품목 + 공사/용역/물품 유출계약"""
    cache = load_cache()
    return {
        "generated_at": cache.get("generated_at"),
        "쇼핑몰_유출품목": cache.get("6_유출품목_쇼핑몰", []),
        "유출계약": cache.get("7_유출계약_주요", []),
    }

@app.get("/api/leakage/shopping", tags=["유출 분석"])
def get_leakage_shopping():
    """쇼핑몰 유출품목 Top 10 (비부산 업체 유출액 기준)"""
    cache = load_cache()
    return {
        "generated_at": cache.get("generated_at"),
        "유출품목": cache.get("6_유출품목_쇼핑몰", []),
    }

@app.get("/api/leakage/contracts", tags=["유출 분석"])
def get_leakage_contracts():
    """공사/용역/물품 주요 유출계약 Top 10 (유출액 기준, 필터 적용)"""
    cache = load_cache()
    return {
        "generated_at": cache.get("generated_at"),
        "유출계약": cache.get("7_유출계약_주요", []),
    }

@app.get("/api/protection", tags=["보호제도"])
def get_protection():
    """지역업체 보호제도 적용 현황 + 미적용 Top 10"""
    cache = load_cache()
    return {
        "generated_at": cache.get("generated_at"),
        "현황": cache.get("8_보호제도_현황", {}),
        "미적용_건": cache.get("8_보호제도_미적용", []),
        "기관별_미적용": cache.get("8_보호제도_기관별", []),
    }

@app.get("/api/private-contract", tags=["수의계약"])
def get_private_contract():
    """수의계약 지역업체 수주율 (공사/물품/용역, 국가/부산시 2그룹)"""
    cache = load_cache()
    return {
        "generated_at": cache.get("generated_at"),
        "수의계약": cache.get("9_수의계약", {}),
        "유출_수의계약": cache.get("9_수의계약_유출", []),
        "유출_기관별": cache.get("9_수의계약_유출_기관별", []),
    }

@app.get("/api/local-companies", tags=["지역업체"])
def get_local_companies():
    """지역업체 현황표 (전체/분야별 업체수, 물품 대분류, 공사/용역 업종)"""
    cache = load_cache()
    return {
        "generated_at": cache.get("generated_at"),
        "현황": cache.get("10_지역업체현황", {}),
    }

@app.get("/api/economic-impact", tags=["경제효과"])
def get_economic_impact():
    """지역상품 구매에 따른 지역생산부가가치 및 지역고용기여도 (한국은행 2020 지역산업연관표 부산 계수)"""
    cache = load_cache()
    return {
        "generated_at": cache.get("generated_at"),
        "경제효과": cache.get("11_경제효과", {}),
    }

@app.get("/api/agency/search", tags=["기관 검색"])
def search_agency(q: str = Query(..., min_length=1, description="검색할 기관명")):
    """특정 수요기관의 총괄 수주율, 금액, 주요 유출계약 정보 검색"""
    cache = load_cache()
    agency_details = cache.get("12_기관별_상세", {})
    
    results = {}
    q_clean = q.strip()
    for unit, details in agency_details.items():
        if q_clean in unit:
            results[unit] = details
            
    return {
        "generated_at": cache.get("generated_at"),
        "검색어": q_clean,
        "검색결과": results
    }

@app.get("/api/agency/suui-search", tags=["기관 검색"])
def search_agency_suui(q: str = Query(..., min_length=1, description="검색할 기관명")):
    """특정 수요기관의 수의계약 유출 현황 검색 (분야별 발주/수주, 유출계약 목록)"""
    cache = load_cache()
    suui_details = cache.get("13_수의계약_기관별_상세", {})
    
    results = {}
    q_clean = q.strip()
    for unit, details in suui_details.items():
        if q_clean in unit:
            results[unit] = details
            
    return {
        "generated_at": cache.get("generated_at"),
        "검색어": q_clean,
        "검색결과": results
    }

@app.get("/api/shopping-contract", tags=["종합쇼핑몰"])
def get_shopping_contract():
    """종합쇼핑몰 유출 현황 (유출 기관별, 유출 계약별)"""
    cache = load_cache()
    return {
        "generated_at": cache.get("generated_at"),
        "유출_쇼핑몰": cache.get("14_쇼핑몰_유출", []),
        "유출_기관별": cache.get("14_쇼핑몰_유출_기관별", []),
        "구군_상세": cache.get("15_쇼핑몰_구군_상세", {}),
        "유형별": cache.get("16_쇼핑몰_유형별", {}),
    }

@app.get("/api/agency/shop-search", tags=["기관 검색"])
def search_agency_shop(q: str = Query(..., min_length=1, description="검색할 기관명")):
    """특정 수요기관의 쇼핑몰 유출 현황 검색"""
    cache = load_cache()
    shop_details = cache.get("14_쇼핑몰_기관별_상세", {})
    
    results = {}
    q_clean = q.strip()
    for unit, details in shop_details.items():
        if q_clean in unit:
            results[unit] = details
            
    return {
        "generated_at": cache.get("generated_at"),
        "검색어": q_clean,
        "검색결과": results
    }


# ════════════════════════════════════════════
#   업체 검색 API (busan_companies_master.db 직접 쿼리)
# ════════════════════════════════════════════

@app.get("/api/company/license-list", tags=["업체 검색"])
def get_license_list(q: Optional[str] = Query(None, description="업종명 검색어 (포함 검색)")):
    """면허업종 목록 + 업체수 (업체수 내림차순). q 파라미터로 필터링 가능"""
    try:
        conn = _get_company_db()
        if q and q.strip():
            rows = conn.execute(
                "SELECT indstrytyNm, COUNT(DISTINCT bizno) cnt FROM company_industry "
                "WHERE indstrytyNm LIKE ? GROUP BY indstrytyNm ORDER BY cnt DESC",
                (f"%{q.strip()}%",)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT indstrytyNm, COUNT(DISTINCT bizno) cnt FROM company_industry "
                "GROUP BY indstrytyNm ORDER BY cnt DESC"
            ).fetchall()
        conn.close()
        return {
            "총업종수": len(rows),
            "업종목록": [{"업종명": r["indstrytyNm"], "업체수": r["cnt"]} for r in rows]
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/company/product-list", tags=["업체 검색"])
def get_product_list(q: Optional[str] = Query(None, description="품명 검색어 (포함 검색)")):
    """대표품명 목록 + 업체수 (업체수 내림차순). q 파라미터로 필터링 가능"""
    try:
        conn = _get_company_db()
        if q and q.strip():
            rows = conn.execute(
                "SELECT rprsntDtlPrdnm, COUNT(*) cnt FROM company_master "
                "WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != '' "
                "AND rprsntDtlPrdnm LIKE ? "
                "GROUP BY rprsntDtlPrdnm ORDER BY cnt DESC",
                (f"%{q.strip()}%",)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT rprsntDtlPrdnm, COUNT(*) cnt FROM company_master "
                "WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != '' "
                "GROUP BY rprsntDtlPrdnm ORDER BY cnt DESC"
            ).fetchall()
        conn.close()
        return {
            "총품명수": len(rows),
            "품명목록": [{"품명": r["rprsntDtlPrdnm"], "업체수": r["cnt"]} for r in rows]
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/company/license-search", tags=["업체 검색"])
def search_by_license(
    q: str = Query(..., min_length=1, description="면허업종명 (정확 매칭 또는 포함 검색)"),
    exact: bool = Query(False, description="True면 정확 매칭, False면 포함 검색"),
    limit: int = Query(200, ge=1, le=5000, description="최대 반환 건수"),
):
    """면허업종으로 업체 검색 → 업체명/대표자/소재지/주소/본사구분/개업일"""
    try:
        conn = _get_company_db()
        q_clean = q.strip()
        where = "i.indstrytyNm = ?" if exact else "i.indstrytyNm LIKE ?"
        param = q_clean if exact else f"%{q_clean}%"
        rows = conn.execute(f"""
            SELECT DISTINCT c.corpNm, c.bizno, c.ceoNm, c.rgnNm, c.adrs, c.dtlAdrs,
                   c.hdoffceDivNm, c.corpBsnsDivNm, c.opbizDt, c.rgstDt,
                   c.rprsntDtlPrdnm, i.indstrytyNm, i.rprsntIndstrytyYn
            FROM company_industry i
            JOIN company_master c ON i.bizno = c.bizno
            WHERE {where}
            ORDER BY c.corpNm
            LIMIT ?
        """, (param, limit)).fetchall()
        conn.close()
        return {
            "검색어": q_clean,
            "검색결과수": len(rows),
            "업체목록": [{
                "업체명": r["corpNm"], "사업자번호": r["bizno"],
                "대표자": r["ceoNm"], "소재지": r["rgnNm"],
                "주소": r["adrs"], "상세주소": r["dtlAdrs"],
                "본사구분": r["hdoffceDivNm"], "업체구분": r["corpBsnsDivNm"],
                "대표품명": r["rprsntDtlPrdnm"],
                "면허업종": r["indstrytyNm"], "대표업종여부": r["rprsntIndstrytyYn"],
                "개업일": r["opbizDt"], "등록일": r["rgstDt"],
            } for r in rows]
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/company/product-search", tags=["업체 검색"])
def search_by_product(
    q: str = Query(..., min_length=1, description="대표품명 검색어 (포함 검색)"),
    exact: bool = Query(False, description="True면 정확 매칭, False면 포함 검색"),
    limit: int = Query(200, ge=1, le=5000, description="최대 반환 건수"),
):
    """대표품명(세부품명)으로 업체 검색 → 업체명/대표자/소재지/대표품명"""
    try:
        conn = _get_company_db()
        q_clean = q.strip()
        where = "rprsntDtlPrdnm = ?" if exact else "rprsntDtlPrdnm LIKE ?"
        param = q_clean if exact else f"%{q_clean}%"
        rows = conn.execute(f"""
            SELECT corpNm, bizno, ceoNm, rgnNm, adrs, dtlAdrs,
                   hdoffceDivNm, corpBsnsDivNm, rprsntDtlPrdnm, opbizDt, rgstDt
            FROM company_master
            WHERE {where}
            ORDER BY corpNm
            LIMIT ?
        """, (param, limit)).fetchall()
        conn.close()
        return {
            "검색어": q_clean,
            "검색결과수": len(rows),
            "업체목록": [{
                "업체명": r["corpNm"], "사업자번호": r["bizno"],
                "대표자": r["ceoNm"], "소재지": r["rgnNm"],
                "주소": r["adrs"], "상세주소": r["dtlAdrs"],
                "본사구분": r["hdoffceDivNm"], "업체구분": r["corpBsnsDivNm"],
                "대표품명": r["rprsntDtlPrdnm"],
                "개업일": r["opbizDt"], "등록일": r["rgstDt"],
            } for r in rows]
        }
    except Exception as e:
        return {"error": str(e)}

# ── 물품 대분류 코드 ↔ 공식 분류명 매핑 (조달청 UNSPSC 기준) ──
UNSPSC_CATEGORIES = {
    "10": "농축수산물", "11": "광물/금속/비금속", "12": "화학약품",
    "14": "종이/고무/플라스틱원재료", "15": "연료/윤활유",
    "20": "광업/유전/가스장비", "21": "농림어업장비",
    "22": "건설/건물유지관리장비", "23": "산업생산/제조장비",
    "24": "산업취급/보관장비", "25": "차량/수송장비",
    "26": "동력/발전장비", "27": "공구/일반기계",
    "30": "구조물/건축자재", "31": "배관/난방자재", "32": "배선/통신자재",
    "39": "전기/조명장비", "40": "냉난방/공조/환기",
    "41": "실험/측정/관측장비", "42": "의료/보건장비",
    "43": "정보통신/방송장비", "44": "사무용기기/용품",
    "46": "안전/방호/소방", "47": "환경/수처리장비",
    "48": "세정/위생장비", "49": "체육/레저/여행",
    "50": "식품/음료/담배", "51": "약품/의약품",
    "52": "가정주방/세탁/가전", "53": "피복/섬유/개인용품",
    "54": "시계/보석/귀금속", "55": "인쇄/출판/광고",
    "56": "가구/인테리어", "60": "악기/게임/완구",
    "70": "서비스(임대/관리)", "72": "건설/유지보수서비스",
    "73": "산업생산/유지보수서비스", "76": "환경/산업청소서비스",
    "77": "교육/훈련서비스", "78": "운송/보관서비스",
    "80": "경영/마케팅서비스", "81": "정보시스템서비스",
    "82": "디자인/엔지니어링서비스", "83": "공공서비스/행정서비스",
    "84": "금융/보험서비스", "85": "보건의료서비스",
    "86": "교육/문화/예술서비스",
    "90": "국방/공공질서", "92": "소방/구조서비스",
    "93": "정치/시민활동",
}
# 역매핑: 분류명 → 코드 (포함검색용)
_CAT_NAME_TO_CODES = {}
for _code, _name in UNSPSC_CATEGORIES.items():
    for _part in _name.replace("/", " ").split():
        _CAT_NAME_TO_CODES.setdefault(_part, []).append(_code)

@app.get("/api/company/category-list", tags=["업체 검색"])
def get_category_list(q: Optional[str] = Query(None, description="분류코드(숫자) 또는 분류명(한글) 검색어")):
    """물품 대분류 목록 + 공식분류명 + 업체수. q로 코드/이름 필터링 가능"""
    try:
        conn = _get_company_db()
        rows = conn.execute("""
            SELECT SUBSTR(rprsntDtlPrdnmNo, 1, 2) AS cat2, COUNT(*) AS cnt
            FROM company_master
            WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != ''
            GROUP BY cat2 ORDER BY cnt DESC
        """).fetchall()
        conn.close()

        result = []
        qc = q.strip() if q else None
        for r in rows:
            code = r["cat2"]
            cat_name = UNSPSC_CATEGORIES.get(code, "기타")
            if qc:
                if qc.isdigit():
                    if not code.startswith(qc):
                        continue
                else:
                    if qc not in cat_name:
                        continue
            result.append({"분류코드": code, "분류명": cat_name, "업체수": r["cnt"]})
        return {"총분류수": len(result), "분류목록": result}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/company/category-search", tags=["업체 검색"])
def search_by_category(
    q: str = Query(..., min_length=1, description="분류코드(예:50) / 분류명(예:식품) / 대표품명(예:우유)"),
    exact: bool = Query(False, description="True면 정확 매칭, False면 포함 검색"),
    limit: int = Query(500, ge=1, le=5000, description="최대 반환 건수"),
):
    """물품 분류코드 / 분류명 / 대표품명으로 업체 검색 (자동 판별)

    - 숫자 → 분류코드 검색 (rprsntDtlPrdnmNo 앞자리 매칭)
    - 분류명 매칭 → 해당 코드의 업체 전체 반환 (예: '식품' → 코드50 전체)
    - 그 외 → 대표품명 키워드 검색 (예: '우유', '컴퓨터')
    """
    try:
        conn = _get_company_db()
        q_clean = q.strip()

        # 1) 숫자 → 분류코드 검색
        if q_clean.isdigit():
            search_mode = "분류코드"
            if exact:
                where = "SUBSTR(rprsntDtlPrdnmNo, 1, 2) = ?"
                param = q_clean
            else:
                where = "rprsntDtlPrdnmNo LIKE ?"
                param = f"{q_clean}%"
        else:
            # 2) 분류명 매칭 시도 (예: '식품' → 코드50, '음료' → 코드50)
            matched_codes = set()
            for part in q_clean.replace("/", " ").split():
                for cat_code, cat_name in UNSPSC_CATEGORIES.items():
                    if part in cat_name:
                        matched_codes.add(cat_code)

            if matched_codes:
                search_mode = "분류명"
                placeholders = ",".join("?" * len(matched_codes))
                where = f"SUBSTR(rprsntDtlPrdnmNo, 1, 2) IN ({placeholders})"
                param = tuple(sorted(matched_codes))
            else:
                # 3) 대표품명 키워드 검색
                search_mode = "대표품명"
                if exact:
                    where = "rprsntDtlPrdnm = ?"
                    param = q_clean
                else:
                    where = "rprsntDtlPrdnm LIKE ?"
                    param = f"%{q_clean}%"

        # 분류명 검색은 IN 절이므로 tuple, 나머지는 단일 param
        if search_mode == "분류명":
            rows = conn.execute(f"""
                SELECT corpNm, bizno, ceoNm, rgnNm, adrs, dtlAdrs,
                       hdoffceDivNm, corpBsnsDivNm, mnfctDivNm,
                       rprsntDtlPrdnmNo, rprsntDtlPrdnm, opbizDt, rgstDt
                FROM company_master
                WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != ''
                  AND {where}
                ORDER BY corpNm
                LIMIT ?
            """, (*param, limit)).fetchall()
            matched_names = [f"{c}({UNSPSC_CATEGORIES[c]})" for c in sorted(matched_codes)]
        else:
            rows = conn.execute(f"""
                SELECT corpNm, bizno, ceoNm, rgnNm, adrs, dtlAdrs,
                       hdoffceDivNm, corpBsnsDivNm, mnfctDivNm,
                       rprsntDtlPrdnmNo, rprsntDtlPrdnm, opbizDt, rgstDt
                FROM company_master
                WHERE rprsntDtlPrdnm IS NOT NULL AND rprsntDtlPrdnm != ''
                  AND {where}
                ORDER BY corpNm
                LIMIT ?
            """, (param, limit)).fetchall()
            matched_names = None
        conn.close()

        resp = {
            "검색어": q_clean,
            "검색방식": search_mode,
            "검색결과수": len(rows),
        }
        if matched_names:
            resp["매칭분류"] = matched_names
        resp["업체목록"] = [{
            "업체명": r["corpNm"], "사업자번호": r["bizno"],
            "대표자": r["ceoNm"], "소재지": r["rgnNm"],
            "주소": r["adrs"], "상세주소": r["dtlAdrs"],
            "본사구분": r["hdoffceDivNm"], "업체구분": r["corpBsnsDivNm"],
            "제조구분": r["mnfctDivNm"],
            "분류코드": r["rprsntDtlPrdnmNo"],
            "분류명": UNSPSC_CATEGORIES.get((r["rprsntDtlPrdnmNo"] or "")[:2], ""),
            "대표품명": r["rprsntDtlPrdnm"],
            "개업일": r["opbizDt"], "등록일": r["rgstDt"],
        } for r in rows]
        return resp
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/company/manufacturers", tags=["업체 검색"])
def get_manufacturers(
    limit: int = Query(5000, ge=1, le=10000, description="최대 반환 건수"),
    format: Optional[str] = Query(None, description="'csv'면 CSV 파일 다운로드, 미지정 시 JSON"),
):
    """제조업체 전체 목록 (mnfctDivNm='제조'). format=csv로 CSV 다운로드 가능"""
    try:
        conn = _get_company_db()
        rows = conn.execute("""
            SELECT c.corpNm, c.bizno, c.ceoNm, c.rgnNm, c.adrs, c.dtlAdrs,
                   c.hdoffceDivNm, c.corpBsnsDivNm, c.mnfctDivNm,
                   c.rprsntDtlPrdnmNo, c.rprsntDtlPrdnm,
                   c.rprsntIndstrytyNm, c.opbizDt, c.rgstDt
            FROM company_master c
            WHERE c.mnfctDivNm = '제조'
            ORDER BY c.corpNm
            LIMIT ?
        """, (limit,)).fetchall()
        conn.close()

        # CSV 다운로드
        if format and format.strip().lower() == "csv":
            import csv, io
            output = io.StringIO()
            writer = csv.writer(output)
            headers = ["업체명","사업자번호","대표자","소재지","주소","상세주소",
                       "본사구분","업체구분","제조구분","분류코드","대표품명","대표업종","개업일","등록일"]
            writer.writerow(headers)
            for r in rows:
                writer.writerow([
                    r["corpNm"], r["bizno"], r["ceoNm"], r["rgnNm"],
                    r["adrs"], r["dtlAdrs"], r["hdoffceDivNm"], r["corpBsnsDivNm"],
                    r["mnfctDivNm"], r["rprsntDtlPrdnmNo"], r["rprsntDtlPrdnm"],
                    r["rprsntIndstrytyNm"], r["opbizDt"], r["rgstDt"],
                ])
            from starlette.responses import StreamingResponse
            csv_bytes = output.getvalue().encode("utf-8-sig")
            return StreamingResponse(
                iter([csv_bytes]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=busan_manufacturers.csv"}
            )

        return {
            "총제조업체수": len(rows),
            "업체목록": [{
                "업체명": r["corpNm"], "사업자번호": r["bizno"],
                "대표자": r["ceoNm"], "소재지": r["rgnNm"],
                "주소": r["adrs"], "상세주소": r["dtlAdrs"],
                "본사구분": r["hdoffceDivNm"], "업체구분": r["corpBsnsDivNm"],
                "제조구분": r["mnfctDivNm"],
                "분류코드": r["rprsntDtlPrdnmNo"], "대표품명": r["rprsntDtlPrdnm"],
                "대표업종": r["rprsntIndstrytyNm"],
                "개업일": r["opbizDt"], "등록일": r["rgstDt"],
            } for r in rows]
        }
    except Exception as e:
        return {"error": str(e)}

# ── 월별 추이 ──
MONTHLY_CACHE_FILE = 'monthly_cache.json'

def load_monthly_cache():
    try:
        with open(MONTHLY_CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] 월별 캐시 로드 실패: {e}")
        return {"error": f"월별 캐시 파일 로드 실패: {str(e)}"}

@app.get("/api/monthly-trend", tags=["종합분석"])
def get_monthly_trend():
    """월별 누계/단월 수주율 추이 (전체, 그룹별, 분야별) + 변동 원인"""
    mc = load_monthly_cache()
    ac = load_cache()
    # 소그룹 누계의 최종 값을 API 캐시 기준으로 보정 (데이터 정합성)
    누계_소그룹 = mc.get("누계_소그룹", {})
    누계_소그룹분야 = mc.get("누계_소그룹분야", {})
    sg_ranking = ac.get("5_기관랭킹_소그룹", {})
    for sg_key, sg_data in sg_ranking.items():
        # 합계 보정
        if sg_key in 누계_소그룹 and 누계_소그룹[sg_key]:
            last = 누계_소그룹[sg_key][-1]
            last['발주액'] = sg_data.get('발주액', last['발주액'])
            last['수주액'] = sg_data.get('수주액', last['수주액'])
            last['수주율'] = sg_data.get('수주율', last['수주율'])
        # 분야별 보정
        api_sec = sg_data.get('분야별', {})
        if sg_key in 누계_소그룹분야 and api_sec:
            for sec_name, sec_vals in api_sec.items():
                sec_list = 누계_소그룹분야[sg_key].get(sec_name, [])
                if sec_list:
                    sec_last = sec_list[-1]
                    sec_last['발주액'] = sec_vals.get('발주액', sec_last['발주액'])
                    sec_last['수주액'] = sec_vals.get('수주액', sec_last['수주액'])
                    sec_last['수주율'] = sec_vals.get('수주율', sec_last['수주율'])
    return {
        "generated_at": mc.get("generated_at"),
        "year": mc.get("year"),
        "months": mc.get("months", []),
        "누계_그룹": mc.get("누계_그룹", {}),
        "누계_분야": mc.get("누계_분야", {}),
        "월간_그룹": mc.get("월간_그룹", {}),
        "월간_분야": mc.get("월간_분야", {}),
        "변동분석": mc.get("변동분석", {}),
        "분야변동": mc.get("분야변동", {}),
        "누계_소그룹": 누계_소그룹,
        "월간_소그룹": mc.get("월간_소그룹", {}),
        "누계_소그룹분야": 누계_소그룹분야,
        "월간_소그룹분야": mc.get("월간_소그룹분야", {}),
        "소그룹_분야변동": mc.get("소그룹_분야변동", {}),
        "소그룹_변동분석": mc.get("소그룹_변동분석", {}),
    }

@app.get("/api/monthly-trend/agency", tags=["종합분석"])
def get_monthly_trend_agency(q: str = Query(..., min_length=1, description="검색할 기관명")):
    """특정 기관의 월별 누계/단월 수주율 추이"""
    mc = load_monthly_cache()
    기관별 = mc.get("기관별", {})
    
    results = {}
    q_clean = q.strip()
    for unit, details in 기관별.items():
        if q_clean in unit:
            results[unit] = details
            
    return {
        "generated_at": mc.get("generated_at"),
        "months": mc.get("months", []),
        "검색어": q_clean,
        "검색결과": results
    }

if __name__ == '__main__':
    import uvicorn
    print("[API] 부산 조달 모니터링 API 서버 시작")
    print("   http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)

