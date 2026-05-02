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


# ════════════════════════════════════════════
#   챗봇 업체 검색 API (chatbot_company.db)
# ════════════════════════════════════════════

import logging
import datetime
from typing import Literal
from fastapi import Request

logger = logging.getLogger("chatbot_api")

CHATBOT_DB = os.environ.get("CHATBOT_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'chatbot_company.db'))

def _get_chatbot_db():
    conn = sqlite3.connect(CHATBOT_DB)
    conn.row_factory = sqlite3.Row
    return conn

def _get_status_filter_sql(status_filter: str) -> str:
    if status_filter == "all":
        return ""
    elif status_filter == "active_only":
        return " AND IFNULL(cbs.business_status, 'unknown') = 'active' "
    elif status_filter == "needs_check":
        return " AND (IFNULL(cbs.business_status, 'unknown') IN ('unknown', 'api_failed', 'quota_exceeded') OR IFNULL(cbs.business_status_freshness, 'not_checked') != 'fresh') "
    return " AND NOT (IFNULL(cbs.business_status, 'unknown') IN ('closed', 'suspended') AND IFNULL(cbs.business_status_freshness, 'not_checked') = 'fresh') "

def _build_chatbot_response(rows, meta=None, error=None, validity_filter="valid_only"):
    if error:
        return {
            "meta": {},
            "candidates": [],
            "company_source_status": "api_failed",
            "company_cache_mode": "none",
            "company_cache_used": False,
            "company_search_status": "failed",
            "error": error
        }

    candidates = []
    counts_by_type = {}
    latest_refresh = {}

    for r in rows:
        c_raw = dict(r)

        for list_field in ["license_or_business_type", "main_products"]:
            if c_raw.get(list_field):
                c_raw[list_field] = c_raw[list_field].split("|")
            else:
                c_raw[list_field] = []

        for json_field in ["candidate_types", "source_refs"]:
            if c_raw.get(json_field):
                try:
                    c_raw[json_field] = json.loads(c_raw[json_field])
                except Exception:
                    c_raw[json_field] = []
            else:
                c_raw[json_field] = []

        raw_policies = c_raw.get("policy_subtypes_raw")
        if "policy_subtypes_raw" in c_raw:
            del c_raw["policy_subtypes_raw"]
            
        c_raw["sme_competition_product"] = bool(c_raw.get("is_sme_competition_product", 0))
        if "is_sme_competition_product" in c_raw:
            del c_raw["is_sme_competition_product"]

        c_raw["policy_subtypes"] = []
        c_raw["policy_validity_summary"] = {}

        if raw_policies:
            for cert in set(raw_policies.split("|")):
                if ":" in cert:
                    subtype, status = cert.split(":")
                    c_raw["policy_validity_summary"][subtype] = status
                    if status == "valid":
                        c_raw["policy_subtypes"].append(subtype)
                else:
                    c_raw["policy_subtypes"].append(cert)

            if c_raw["policy_subtypes"] or c_raw["policy_validity_summary"]:
                if "policy_company_certification" not in c_raw["source_refs"]:
                    c_raw["source_refs"].append("policy_company_certification")

            if c_raw["policy_subtypes"] and "policy_company" not in c_raw["candidate_types"]:
                c_raw["candidate_types"].append("policy_company")

        raw_certified_types = c_raw.get("certified_product_types_raw")
        raw_certified_summary = c_raw.get("certified_product_summary_raw")
        
        if "certified_product_types_raw" in c_raw:
            del c_raw["certified_product_types_raw"]
        if "certified_product_summary_raw" in c_raw:
            del c_raw["certified_product_summary_raw"]

        c_raw["certified_product_types"] = []
        c_raw["certified_product_summary"] = []

        if raw_certified_types:
            for cert in set(raw_certified_types.split("|")):
                if not cert: continue
                parts = cert.split(":")
                if len(parts) == 5:
                    ctype, is_priority, is_innov, is_exc, status = parts
                    if status == "valid":
                        c_raw["certified_product_types"].append(ctype)
                        if is_priority == '1' and "priority_purchase_product" not in c_raw["candidate_types"]:
                            c_raw["candidate_types"].append("priority_purchase_product")
                        if is_innov == '1' and "innovation_product" not in c_raw["candidate_types"]:
                            c_raw["candidate_types"].append("innovation_product")
                        if is_exc == '1' and "excellent_procurement_product" not in c_raw["candidate_types"]:
                            c_raw["candidate_types"].append("excellent_procurement_product")

        if raw_certified_summary:
            items = []
            for cert in raw_certified_summary.split("|||"):
                if not cert: continue
                parts = cert.split("^^")
                if len(parts) == 5:
                    ctype, pname, status, exp_date, src = parts
                    # validity_filter에 따른 필터링 로직 추가
                    if validity_filter == "valid_only" and status != "valid":
                        continue
                    if validity_filter == "include_unknown" and status not in ("valid", "unknown"):
                        continue
                        
                    items.append({
                        "certification_type": ctype,
                        "product_name": pname,
                        "validity_status": status,
                        "expiration_date": exp_date if exp_date else None,
                        "source_name": src
                    })
            # Sort valid first
            items.sort(key=lambda x: (x["validity_status"] != "valid", x["expiration_date"] or ""))
            c_raw["certified_product_summary"] = items[:5] # Max 5
            
            if items and "certified_product" not in c_raw["source_refs"]:
                c_raw["source_refs"].append("certified_product")

        raw_shopping_flags = c_raw.get("shopping_mall_flags_raw")
        raw_mas_summary = c_raw.get("mas_product_summary_raw")
        
        if "shopping_mall_flags_raw" in c_raw:
            del c_raw["shopping_mall_flags_raw"]
        if "mas_product_summary_raw" in c_raw:
            del c_raw["mas_product_summary_raw"]

        c_raw["shopping_mall_flags"] = []
        c_raw["mas_product_summary"] = []

        if raw_shopping_flags == "mas_registered":
            c_raw["shopping_mall_flags"].append("mas_registered")
            if "shopping_mall_supplier" not in c_raw["candidate_types"]:
                c_raw["candidate_types"].append("shopping_mall_supplier")
                
        if raw_mas_summary:
            items = []
            for mas in raw_mas_summary.split("|||"):
                if not mas: continue
                parts = mas.split("^^")
                if len(parts) == 7:
                    pname, dcode, status, end_date, price, unit, src = parts
                    # 뷰에서 이미 active만 제한하여 반환하므로 필터 로직 불필요
                    items.append({
                        "product_name": pname,
                        "detail_product_code": dcode if dcode else None,
                        "contract_status": status,
                        "contract_end_date": end_date if end_date else None,
                        "price_amount": float(price) if price else None,
                        "price_unit": unit if unit else None,
                        "source_name": src
                    })
            c_raw["mas_product_summary"] = items
            if items and "mas_product" not in c_raw["source_refs"]:
                c_raw["source_refs"].append("mas_product")

        # Phase 6-D-2: 업체/정책 속성 파싱
        raw_procurement_attrs = c_raw.get("procurement_attributes_raw")
        if "procurement_attributes_raw" in c_raw:
            del c_raw["procurement_attributes_raw"]
        c_raw["procurement_attributes"] = []
        if raw_procurement_attrs:
            c_raw["procurement_attributes"] = [a for a in raw_procurement_attrs.split("|") if a]

        # Phase 6-D-2: 일반 인증/기타 파싱
        raw_general_certs = c_raw.get("general_certifications_raw")
        if "general_certifications_raw" in c_raw:
            del c_raw["general_certifications_raw"]
        c_raw["general_certifications"] = []
        if raw_general_certs:
            c_raw["general_certifications"] = [g for g in raw_general_certs.split("|") if g]

        # Phase 6-D-2: source_refs 보강
        if c_raw["procurement_attributes"] and "company_procurement_attribute" not in c_raw.get("source_refs", []):
            c_raw.setdefault("source_refs", []).append("company_procurement_attribute")
        if c_raw["general_certifications"] and "product_general_certification" not in c_raw.get("source_refs", []):
            c_raw.setdefault("source_refs", []).append("product_general_certification")

        for ctype in c_raw.get("candidate_types", []):
            counts_by_type[ctype] = counts_by_type.get(ctype, 0) + 1

        for ref in c_raw.get("source_refs", []):
            t = c_raw.get("source_refreshed_at")
            if t:
                if ref not in latest_refresh or t > latest_refresh[ref]:
                    latest_refresh[ref] = t

        ALLOWED_CANDIDATE_FIELDS = [
            "company_id", "company_name", "representative_name", "corporate_phone",
            "location", "detail_address", "is_busan_company", "is_headquarters",
            "license_or_business_type", "main_products", "candidate_types",
            "primary_candidate_type", "manufacturer_type", "business_status",
            "business_status_freshness", "business_status_checked_at", "business_status_source", "display_status",
            "contract_possible_auto_promoted", "source_refs", "source_refreshed_at",
            "policy_subtypes", "policy_validity_summary",
            "certified_product_types", "certified_product_summary",
            "shopping_mall_flags", "mas_product_summary", "sme_competition_product",
            "procurement_attributes", "general_certifications"
        ]

        filtered = {k: v for k, v in c_raw.items() if k in ALLOWED_CANDIDATE_FIELDS}

        if "actual_business_status" in c_raw and c_raw["actual_business_status"] is not None:
            filtered["business_status"] = c_raw["actual_business_status"]
        if "actual_business_status_freshness" in c_raw and c_raw["actual_business_status_freshness"] is not None:
            filtered["business_status_freshness"] = c_raw["actual_business_status_freshness"]

        candidates.append(filtered)

    final_meta = meta or {}
    final_meta["candidate_counts_by_type"] = counts_by_type
    if "source_refreshed_at" not in final_meta:
        final_meta["source_refreshed_at"] = latest_refresh

    return {
        "meta": final_meta,
        "candidates": candidates,
        "company_source_status": "success",
        "company_search_status": "success",
        "company_cache_used": True,
        "company_cache_mode": "database"
    }

ChatbotStatusFilter = Literal["exclude_closed", "all", "active_only", "needs_check"]
ChatbotValidityFilter = Literal["valid_only", "include_unknown", "all"]
ChatbotPolicySubtype = Literal[
    "women_company", "disabled_company", "social_enterprise",
    "preliminary_social_enterprise", "severe_disabled_production"
]
ChatbotCertificationType = Literal[
    "performance_certification", "excellent_procurement_product", "nep_product",
    "gs_certified_product", "net_certified_product", "innovation_product",
    "excellent_rnd_innovation_product", "innovation_prototype_product",
    "other_innovation_product", "disaster_safety_certified_product",
    "green_technology_product", "industrial_convergence_new_product",
    "excellent_procurement_joint_brand"
]

@app.get("/api/chatbot/company/license-list", tags=["챗봇"])
def get_chatbot_license_list(limit: int = Query(50, ge=1, le=50), offset: int = Query(0, ge=0)):
    try:
        conn = _get_chatbot_db()
        query = f'''
            SELECT cl.license_name, COUNT(DISTINCT m.company_internal_id) as candidate_count
            FROM company_license cl
            JOIN company_master m ON cl.company_internal_id = m.company_internal_id
            LEFT JOIN company_business_status cbs ON m.company_internal_id = cbs.company_internal_id
            WHERE m.is_busan_company = 1
            {_get_status_filter_sql("exclude_closed")}
            GROUP BY cl.license_name
            ORDER BY candidate_count DESC
            LIMIT ? OFFSET ?
        '''
        rows = conn.execute(query, (limit, offset)).fetchall()
        conn.close()
        return {
            "meta": {},
            "candidates": [dict(r) for r in rows],
            "company_source_status": "success",
            "company_search_status": "success",
            "company_cache_used": True,
            "company_cache_mode": "database"
        }
    except Exception:
        logger.exception("챗봇 API 오류: license-list")
        return _build_chatbot_response([], error="업체 목록 조회 실패")

@app.get("/api/chatbot/company/product-list", tags=["챗봇"])
def get_chatbot_product_list(limit: int = Query(50, ge=1, le=50), offset: int = Query(0, ge=0)):
    try:
        conn = _get_chatbot_db()
        query = f'''
            SELECT cp.product_name, COUNT(DISTINCT m.company_internal_id) as candidate_count
            FROM company_product cp
            JOIN company_master m ON cp.company_internal_id = m.company_internal_id
            LEFT JOIN company_business_status cbs ON m.company_internal_id = cbs.company_internal_id
            WHERE m.is_busan_company = 1
            {_get_status_filter_sql("exclude_closed")}
            GROUP BY cp.product_name
            ORDER BY candidate_count DESC
            LIMIT ? OFFSET ?
        '''
        rows = conn.execute(query, (limit, offset)).fetchall()
        conn.close()
        return {
            "meta": {},
            "candidates": [dict(r) for r in rows],
            "company_source_status": "success",
            "company_search_status": "success",
            "company_cache_used": True,
            "company_cache_mode": "database"
        }
    except Exception:
        logger.exception("챗봇 API 오류: product-list")
        return _build_chatbot_response([], error="업체 목록 조회 실패")

@app.get("/api/chatbot/company/category-list", tags=["챗봇"])
def get_chatbot_category_list(limit: int = Query(50, ge=1, le=50), offset: int = Query(0, ge=0)):
    try:
        conn = _get_chatbot_db()
        query = f'''
            SELECT g.category_code, MAX(g.category_name) as category_name, COUNT(DISTINCT m.company_internal_id) as candidate_count
            FROM company_product cp
            JOIN g2b_product_category g ON cp.g2b_category_code = g.category_code
            JOIN company_master m ON cp.company_internal_id = m.company_internal_id
            LEFT JOIN company_business_status cbs ON m.company_internal_id = cbs.company_internal_id
            WHERE m.is_busan_company = 1
            {_get_status_filter_sql("exclude_closed")}
            GROUP BY g.category_code
            ORDER BY candidate_count DESC
            LIMIT ? OFFSET ?
        '''
        rows = conn.execute(query, (limit, offset)).fetchall()
        conn.close()
        return {
            "meta": {},
            "candidates": [dict(r) for r in rows],
            "company_source_status": "success",
            "company_search_status": "success",
            "company_cache_used": True,
            "company_cache_mode": "database"
        }
    except Exception:
        logger.exception("챗봇 API 오류: category-list")
        return _build_chatbot_response([], error="업체 목록 조회 실패")

@app.get("/api/chatbot/company/manufacturers", tags=["챗봇"])
def get_chatbot_manufacturers(limit: int = Query(50, ge=1, le=50), offset: int = Query(0, ge=0)):
    try:
        conn = _get_chatbot_db()
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE v.manufacturer_type != 'unknown' AND v.is_busan_company = 1
            {_get_status_filter_sql("exclude_closed")}
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        rows = conn.execute(query, (limit, offset)).fetchall()
        conn.close()
        return _build_chatbot_response(rows, meta={"query": {"limit": limit, "offset": offset}})
    except Exception:
        logger.exception("챗봇 API 오류: manufacturers")
        return _build_chatbot_response([], error="업체 목록 조회 실패")

@app.get("/api/chatbot/company/license-search", tags=["챗봇"])
def get_chatbot_license_search(license_name: str, status_filter: ChatbotStatusFilter = "exclude_closed", limit: int = Query(50, ge=1, le=50), offset: int = Query(0, ge=0)):
    try:
        conn = _get_chatbot_db()
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            JOIN company_license cl ON i.company_internal_id = cl.company_internal_id
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE (cl.license_name LIKE ? OR cl.license_name_normalized LIKE ?) AND v.is_busan_company = 1
            {_get_status_filter_sql(status_filter)}
            GROUP BY v.company_id
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        p = f"%{license_name}%"
        rows = conn.execute(query, (p, p, limit, offset)).fetchall()
        conn.close()
        return _build_chatbot_response(rows, meta={"query": {"keyword": license_name, "limit": limit, "offset": offset, "status_filter": status_filter}})
    except Exception:
        logger.exception("챗봇 API 오류: license-search")
        return _build_chatbot_response([], error="업체 목록 조회 실패")

@app.get("/api/chatbot/company/product-search", tags=["챗봇"])
def get_chatbot_product_search(product_name: str, status_filter: ChatbotStatusFilter = "exclude_closed", limit: int = Query(50, ge=1, le=50), offset: int = Query(0, ge=0)):
    try:
        conn = _get_chatbot_db()
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            JOIN company_product cp ON i.company_internal_id = cp.company_internal_id
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE (cp.product_name LIKE ? OR cp.product_name_normalized LIKE ?) AND v.is_busan_company = 1
            {_get_status_filter_sql(status_filter)}
            GROUP BY v.company_id
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        p = f"%{product_name}%"
        rows = conn.execute(query, (p, p, limit, offset)).fetchall()
        conn.close()
        return _build_chatbot_response(rows, meta={"query": {"keyword": product_name, "limit": limit, "offset": offset, "status_filter": status_filter}})
    except Exception:
        logger.exception("챗봇 API 오류: product-search")
        return _build_chatbot_response([], error="업체 목록 조회 실패")

@app.get("/api/chatbot/company/category-search", tags=["챗봇"])
def get_chatbot_category_search(category_name: str, status_filter: ChatbotStatusFilter = "exclude_closed", limit: int = Query(50, ge=1, le=50), offset: int = Query(0, ge=0)):
    try:
        conn = _get_chatbot_db()
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            JOIN company_product cp ON i.company_internal_id = cp.company_internal_id
            JOIN g2b_product_category g ON cp.g2b_category_code = g.category_code
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE (g.category_name LIKE ? OR g.category_code = ?) AND v.is_busan_company = 1
            {_get_status_filter_sql(status_filter)}
            GROUP BY v.company_id
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        p = f"%{category_name}%"
        rows = conn.execute(query, (p, category_name, limit, offset)).fetchall()
        conn.close()
        return _build_chatbot_response(rows, meta={"query": {"keyword": category_name, "limit": limit, "offset": offset, "status_filter": status_filter}})
    except Exception:
        logger.exception("챗봇 API 오류: category-search")
        return _build_chatbot_response([], error="업체 목록 조회 실패")

@app.get("/api/chatbot/company/detail", tags=["챗봇"])
def get_chatbot_company_detail(company_id: str, request: Request):
    try:
        conn = _get_chatbot_db()
        row = conn.execute("SELECT company_internal_id FROM company_identity WHERE company_id = ?", (company_id,)).fetchone()
        if not row:
            conn.close()
            return _build_chatbot_response([], error="유효하지 않거나 만료된 업체 식별자입니다.")

        internal_id = row["company_internal_id"]

        cache_row = conn.execute("SELECT business_status, checked_at, business_status_source, business_status_freshness FROM company_business_status WHERE company_internal_id = ?", (internal_id,)).fetchone()

        now = datetime.datetime.now()
        should_fetch = False
        if not cache_row:
            should_fetch = True
        else:
            checked_at_str = cache_row["checked_at"]
            if not checked_at_str or cache_row["business_status"] in ("unknown", "api_failed"):
                should_fetch = True
            else:
                try:
                    checked_at = datetime.datetime.strptime(checked_at_str, "%Y-%m-%d %H:%M:%S")
                    if (now - checked_at).days >= 7:
                        should_fetch = True
                except Exception:
                    should_fetch = True

        if should_fetch:
            import nts_business_status_client
            b_row = conn.execute("SELECT canonical_business_no FROM company_identity WHERE company_internal_id = ?", (internal_id,)).fetchone()
            if b_row and b_row["canonical_business_no"]:
                b_no = b_row["canonical_business_no"]
                res = nts_business_status_client.check_business_status([b_no])
                now_str = now.strftime("%Y-%m-%d %H:%M:%S")
                if res.get("success") and res["results"].get(b_no):
                    r = res["results"][b_no]
                    conn.execute('''
                        INSERT INTO company_business_status
                        (company_internal_id, business_status, business_status_freshness, tax_type, closed_at, api_result_code, checked_at, business_status_source)
                        VALUES (?, ?, 'fresh', ?, ?, ?, ?, 'nts_api')
                        ON CONFLICT(company_internal_id) DO UPDATE SET
                            business_status=excluded.business_status,
                            business_status_freshness='fresh',
                            tax_type=excluded.tax_type,
                            closed_at=excluded.closed_at,
                            api_result_code=excluded.api_result_code,
                            checked_at=excluded.checked_at,
                            business_status_source='nts_api',
                            updated_at=CURRENT_TIMESTAMP
                    ''', (internal_id, r["business_status"], r.get("tax_type"), r.get("closed_at"), r.get("api_result_code"), now_str))
                    conn.commit()
                else:
                    conn.execute('''
                        INSERT INTO company_business_status
                        (company_internal_id, business_status, business_status_freshness, checked_at, business_status_source)
                        VALUES (?, 'unknown', 'api_failed', ?, 'nts_api')
                        ON CONFLICT(company_internal_id) DO UPDATE SET
                            business_status_freshness='api_failed',
                            checked_at=excluded.checked_at,
                            updated_at=CURRENT_TIMESTAMP
                    ''', (internal_id, now_str))
                    conn.commit()

        query = '''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE v.company_id = ?
        '''
        rows = conn.execute(query, (company_id,)).fetchall()
        conn.close()

        resp = _build_chatbot_response(rows, meta={"query": {"company_id": company_id}})
        if resp["candidates"]:
            resp["candidates"][0]["representative_name"] = None
            resp["candidates"][0]["corporate_phone"] = None

        return resp
    except Exception:
        logger.exception("챗봇 API 오류: company-detail")
        return _build_chatbot_response([], error="업체 목록 조회 실패")

@app.get("/api/chatbot/company/policy-search", tags=["챗봇"])
def get_chatbot_policy_search(policy_subtype: ChatbotPolicySubtype = None, status_filter: ChatbotStatusFilter = "exclude_closed", validity_filter: ChatbotValidityFilter = "valid_only", limit: int = Query(50, ge=1, le=50), offset: int = Query(0, ge=0)):
    try:
        conn = _get_chatbot_db()
        if policy_subtype:
            query = f'''
                SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
                FROM chatbot_company_candidate_view v
                JOIN company_identity i ON v.company_id = i.company_id
                JOIN policy_company_certification pcc ON i.company_internal_id = pcc.company_internal_id
                LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
                WHERE pcc.policy_subtype = ? AND pcc.validity_status = 'valid' AND v.is_busan_company = 1
                {_get_status_filter_sql(status_filter)}
                GROUP BY v.company_id
                ORDER BY v.company_id
                LIMIT ? OFFSET ?
            '''
            rows = conn.execute(query, (policy_subtype, limit, offset)).fetchall()
        else:
            query = f'''
                SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
                FROM chatbot_company_candidate_view v
                JOIN company_identity i ON v.company_id = i.company_id
                JOIN policy_company_certification pcc ON i.company_internal_id = pcc.company_internal_id
                LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
                WHERE pcc.validity_status = 'valid' AND v.is_busan_company = 1
                {_get_status_filter_sql(status_filter)}
                GROUP BY v.company_id
                ORDER BY v.company_id
                LIMIT ? OFFSET ?
            '''
            rows = conn.execute(query, (limit, offset)).fetchall()

        conn.close()
        resp = _build_chatbot_response(rows, meta={"query": {"policy_subtype": policy_subtype, "limit": limit, "offset": offset}}, validity_filter=validity_filter)
        for c in resp["candidates"]:
            c["primary_candidate_type"] = "policy_company"
        return resp
    except Exception:
        logger.exception("챗봇 API 오류: policy-search")
        return _build_chatbot_response([], error="정책기업 조회 실패")

@app.get("/api/chatbot/company/policy-list", tags=["챗봇"])
def get_chatbot_policy_list():
    try:
        conn = _get_chatbot_db()
        query = '''
            SELECT
                pcc.policy_subtype,
                COUNT(DISTINCT pcc.company_internal_id) as candidate_count,
                SUM(CASE WHEN pcc.validity_status = 'valid' THEN 1 ELSE 0 END) as valid_count,
                SUM(CASE WHEN pcc.validity_status = 'expired' THEN 1 ELSE 0 END) as expired_count,
                MAX(pcc.source_refreshed_at) as refreshed_at
            FROM policy_company_certification pcc
            JOIN company_master m ON pcc.company_internal_id = m.company_internal_id
            WHERE m.is_busan_company = 1
            GROUP BY pcc.policy_subtype
        '''
        rows = conn.execute(query).fetchall()
        conn.close()

        candidates = []
        latest = None
        for r in rows:
            candidates.append({
                "policy_subtype": r["policy_subtype"],
                "candidate_count": r["candidate_count"],
                "valid_count": r["valid_count"],
                "expired_count": r["expired_count"]
            })
            if r["refreshed_at"]:
                if latest is None or r["refreshed_at"] > latest:
                    latest = r["refreshed_at"]

        return {
            "meta": {
                "source_refreshed_at": {"policy_company_certification": latest} if latest else {}
            },
            "candidates": candidates,
            "company_source_status": "success",
            "company_search_status": "success",
            "company_cache_used": False,
            "company_cache_mode": "none"
        }
    except Exception:
        logger.exception("챗봇 API 오류: policy-list")
        return _build_chatbot_response([], error="정책기업 조회 실패")

# ==========================================
# Phase 5: 인증제품 연동 API
# ==========================================

@app.get("/api/chatbot/product/certified-search", tags=["챗봇"])
def get_chatbot_certified_search(
    certification_type: Optional[ChatbotCertificationType] = None,
    product_name: Optional[str] = None,
    company_keyword: Optional[str] = None,
    status_filter: ChatbotStatusFilter = "exclude_closed",
    validity_filter: ChatbotValidityFilter = "valid_only",
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    try:
        conn = _get_chatbot_db()
        where_clauses = ["v.is_busan_company = 1"]
        params = []
        
        if certification_type:
            where_clauses.append("cp.certification_type = ?")
            params.append(certification_type)
        if product_name:
            where_clauses.append("(cp.product_name LIKE ? OR cp.product_name_normalized LIKE ?)")
            params.extend([f"%{product_name}%", f"%{product_name}%"])
        if company_keyword:
            where_clauses.append("(v.company_name LIKE ? OR v.company_id = ?)")
            params.extend([f"%{company_keyword}%", company_keyword])
            
        if validity_filter == "valid_only":
            where_clauses.append("cp.validity_status = 'valid'")
        elif validity_filter == "include_unknown":
            where_clauses.append("cp.validity_status IN ('valid', 'unknown')")
            
        status_sql = _get_status_filter_sql(status_filter)
        where_sql = " AND ".join(where_clauses)
        
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            JOIN certified_product cp ON i.company_internal_id = cp.company_internal_id
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE {where_sql} {status_sql}
            GROUP BY v.company_id
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        params.extend([limit, offset])
        rows = conn.execute(query, params).fetchall()
        conn.close()
        
        return _build_chatbot_response(rows, meta={
            "query": {
                "certification_type": certification_type,
                "product_name": product_name,
                "company_keyword": company_keyword,
                "limit": limit,
                "offset": offset
            }
        }, validity_filter=validity_filter)
    except Exception:
        logger.exception("챗봇 API 오류: certified-search")
        return _build_chatbot_response([], error="인증제품 조회 실패")

@app.get("/api/chatbot/product/innovation-search", tags=["챗봇"])
def get_chatbot_innovation_search(
    product_name: Optional[str] = None,
    innovation_type: Literal["all", "excellent_rnd", "prototype", "other"] = "all",
    status_filter: ChatbotStatusFilter = "exclude_closed",
    validity_filter: ChatbotValidityFilter = "valid_only",
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    try:
        conn = _get_chatbot_db()
        where_clauses = ["v.is_busan_company = 1"]
        params = []
        
        type_in = []
        if innovation_type == "all":
            type_in = ["innovation_product", "excellent_rnd_innovation_product", "innovation_prototype_product", "other_innovation_product"]
        elif innovation_type == "excellent_rnd":
            type_in = ["excellent_rnd_innovation_product"]
        elif innovation_type == "prototype":
            type_in = ["innovation_prototype_product"]
        else:
            type_in = ["other_innovation_product"]
            
        placeholders = ",".join(["?"] * len(type_in))
        where_clauses.append(f"cp.certification_type IN ({placeholders})")
        params.extend(type_in)
        
        if product_name:
            where_clauses.append("(cp.product_name LIKE ? OR cp.product_name_normalized LIKE ?)")
            params.extend([f"%{product_name}%", f"%{product_name}%"])
            
        if validity_filter == "valid_only":
            where_clauses.append("cp.validity_status = 'valid'")
        elif validity_filter == "include_unknown":
            where_clauses.append("cp.validity_status IN ('valid', 'unknown')")
            
        status_sql = _get_status_filter_sql(status_filter)
        where_sql = " AND ".join(where_clauses)
        
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            JOIN certified_product cp ON i.company_internal_id = cp.company_internal_id
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE {where_sql} {status_sql}
            GROUP BY v.company_id
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        params.extend([limit, offset])
        rows = conn.execute(query, params).fetchall()
        conn.close()
        
        return _build_chatbot_response(rows, meta={
            "query": {
                "product_name": product_name,
                "innovation_type": innovation_type,
                "limit": limit,
                "offset": offset
            }
        }, validity_filter=validity_filter)
    except Exception:
        logger.exception("챗봇 API 오류: innovation-search")
        return _build_chatbot_response([], error="혁신제품 조회 실패")

@app.get("/api/chatbot/product/priority-purchase-search", tags=["챗봇"])
def get_chatbot_priority_purchase_search(
    product_name: Optional[str] = None,
    certification_type: Optional[ChatbotCertificationType] = None,
    status_filter: ChatbotStatusFilter = "exclude_closed",
    validity_filter: ChatbotValidityFilter = "valid_only",
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    try:
        conn = _get_chatbot_db()
        where_clauses = ["v.is_busan_company = 1", "map.is_priority_purchase_product = 1"]
        params = []
        
        if certification_type:
            where_clauses.append("cp.certification_type = ?")
            params.append(certification_type)
        if product_name:
            where_clauses.append("(cp.product_name LIKE ? OR cp.product_name_normalized LIKE ?)")
            params.extend([f"%{product_name}%", f"%{product_name}%"])
            
        if validity_filter == "valid_only":
            where_clauses.append("cp.validity_status = 'valid'")
        elif validity_filter == "include_unknown":
            where_clauses.append("cp.validity_status IN ('valid', 'unknown')")
            
        status_sql = _get_status_filter_sql(status_filter)
        where_sql = " AND ".join(where_clauses)
        
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            JOIN certified_product cp ON i.company_internal_id = cp.company_internal_id
            JOIN certified_product_type_map map ON cp.certification_type = map.normalized_certification_type
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE {where_sql} {status_sql}
            GROUP BY v.company_id
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        params.extend([limit, offset])
        rows = conn.execute(query, params).fetchall()
        conn.close()
        
        return _build_chatbot_response(rows, meta={
            "query": {
                "product_name": product_name,
                "certification_type": certification_type,
                "limit": limit,
                "offset": offset
            }
        }, validity_filter=validity_filter)
    except Exception:
        logger.exception("챗봇 API 오류: priority-purchase-search")
        return _build_chatbot_response([], error="기술개발제품(우선구매) 조회 실패")

@app.get("/api/chatbot/product/excellent-procurement-search", tags=["챗봇"])
def get_chatbot_excellent_procurement_search(
    product_name: Optional[str] = None,
    status_filter: ChatbotStatusFilter = "exclude_closed",
    validity_filter: ChatbotValidityFilter = "valid_only",
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    try:
        conn = _get_chatbot_db()
        where_clauses = ["v.is_busan_company = 1", "cp.certification_type = 'excellent_procurement_product'"]
        params = []
        
        if product_name:
            where_clauses.append("(cp.product_name LIKE ? OR cp.product_name_normalized LIKE ?)")
            params.extend([f"%{product_name}%", f"%{product_name}%"])
            
        if validity_filter == "valid_only":
            where_clauses.append("cp.validity_status = 'valid'")
        elif validity_filter == "include_unknown":
            where_clauses.append("cp.validity_status IN ('valid', 'unknown')")
            
        status_sql = _get_status_filter_sql(status_filter)
        where_sql = " AND ".join(where_clauses)
        
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            JOIN certified_product cp ON i.company_internal_id = cp.company_internal_id
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE {where_sql} {status_sql}
            GROUP BY v.company_id
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        params.extend([limit, offset])
        rows = conn.execute(query, params).fetchall()
        conn.close()
        
        return _build_chatbot_response(rows, meta={
            "query": {
                "product_name": product_name,
                "limit": limit,
                "offset": offset
            }
        }, validity_filter=validity_filter)
    except Exception:
        logger.exception("챗봇 API 오류: excellent-procurement-search")
        return _build_chatbot_response([], error="우수조달물품 조회 실패")

@app.get("/api/chatbot/product/certified-list", tags=["챗봇"])
def get_chatbot_certified_list():
    try:
        conn = _get_chatbot_db()
        query = '''
            SELECT
                cp.certification_type,
                COUNT(DISTINCT cp.company_internal_id) as candidate_count,
                SUM(CASE WHEN cp.validity_status = 'valid' THEN 1 ELSE 0 END) as valid_count,
                SUM(CASE WHEN cp.validity_status = 'expired' THEN 1 ELSE 0 END) as expired_count,
                MAX(cp.source_refreshed_at) as refreshed_at
            FROM certified_product cp
            JOIN company_master m ON cp.company_internal_id = m.company_internal_id
            WHERE m.is_busan_company = 1
            GROUP BY cp.certification_type
        '''
        rows = conn.execute(query).fetchall()
        conn.close()

        candidates = []
        latest = None
        for r in rows:
            candidates.append({
                "certification_type": r["certification_type"],
                "candidate_count": r["candidate_count"],
                "valid_count": r["valid_count"],
                "expired_count": r["expired_count"]
            })
            if r["refreshed_at"]:
                if latest is None or r["refreshed_at"] > latest:
                    latest = r["refreshed_at"]

        return {
            "meta": {
                "source_refreshed_at": {"certified_product": latest} if latest else {}
            },
            "candidates": candidates,
            "company_source_status": "success",
            "company_search_status": "success",
            "company_cache_used": False,
            "company_cache_mode": "none",
            "error": None
        }
    except Exception:
        logger.exception("챗봇 API 오류: certified-list")
        return _build_chatbot_response([], error="인증제품 목록 조회 실패")

# ==========================================
# Phase 6-C: MAS 쇼핑몰 연동 API
# ==========================================

MasContractStatusFilter = Literal["active_only", "include_unknown", "all"]

def _get_mas_status_filter_sql(status_filter: str) -> str:
    if status_filter == "all":
        return ""
    elif status_filter == "include_unknown":
        return " AND mp.contract_status IN ('active', 'unknown') "
    return " AND mp.contract_status = 'active' "

@app.get("/api/chatbot/mas/search", tags=["챗봇"])
def get_chatbot_mas_search(
    product_name: Optional[str] = None,
    detail_product_code: Optional[str] = None,
    company_keyword: Optional[str] = None,
    contract_status_filter: MasContractStatusFilter = "active_only",
    status_filter: ChatbotStatusFilter = "exclude_closed",
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    """
    MAS 종합 검색. 
    주의: active_only는 종합쇼핑몰에서의 당장 구매 가능을 의미하지 않습니다.
    """
    try:
        conn = _get_chatbot_db()
        where_clauses = ["v.is_busan_company = 1"]
        params = []
        
        if product_name:
            where_clauses.append("(mp.product_name LIKE ? OR mp.product_name_normalized LIKE ? OR mp.detail_product_name LIKE ?)")
            params.extend([f"%{product_name}%", f"%{product_name}%", f"%{product_name}%"])
        if detail_product_code:
            where_clauses.append("mp.detail_product_code = ?")
            params.append(detail_product_code)
        if company_keyword:
            where_clauses.append("(v.company_name LIKE ? OR v.company_id = ?)")
            params.extend([f"%{company_keyword}%", company_keyword])
            
        where_clauses.append(_get_mas_status_filter_sql(contract_status_filter).strip(" AND"))
        
        status_sql = _get_status_filter_sql(status_filter)
        where_sql = " AND ".join([w for w in where_clauses if w])
        
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            JOIN mas_product mp ON i.company_internal_id = mp.company_internal_id
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE {where_sql} {status_sql}
            GROUP BY v.company_id
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        params.extend([limit, offset])
        rows = conn.execute(query, params).fetchall()
        conn.close()
        
        # Note: 뷰가 반환하는 mas_product_summary는 active만 포함하지만,
        # 이 MAS 전용 API에서는 뷰 결과를 그대로 제공하되, 향후 필요시 직접 조인 결과로 오버라이드할 수 있습니다.
        # 현재 요구사항인 "include_unknown/all에서는 mas_product_summary에만 표시할 수 있다"에 따라,
        # mas_product_summary를 다시 쿼리하여 오버라이드합니다.
        
        resp = _build_chatbot_response(rows, meta={
            "query": {
                "product_name": product_name,
                "detail_product_code": detail_product_code,
                "company_keyword": company_keyword,
                "contract_status_filter": contract_status_filter,
                "limit": limit,
                "offset": offset
            }
        })
        
        if contract_status_filter != "active_only" and resp["candidates"]:
            # Re-fetch mas products for the candidates to include non-active ones
            cids = [c["company_id"] for c in resp["candidates"]]
            conn = _get_chatbot_db()
            placeholders = ",".join("?" * len(cids))
            
            mas_q = f'''
                SELECT i.company_id, mp.product_name, mp.detail_product_code, mp.contract_status, mp.contract_end_date, mp.price_amount, mp.price_unit, mp.source_name
                FROM mas_product mp
                JOIN company_identity i ON mp.company_internal_id = i.company_internal_id
                WHERE i.company_id IN ({placeholders}) {_get_mas_status_filter_sql(contract_status_filter)}
                ORDER BY mp.contract_end_date DESC
            '''
            mas_rows = conn.execute(mas_q, cids).fetchall()
            conn.close()
            
            mas_map = {}
            for r in mas_rows:
                cid = r["company_id"]
                if cid not in mas_map:
                    mas_map[cid] = []
                mas_map[cid].append({
                    "product_name": r["product_name"],
                    "detail_product_code": r["detail_product_code"],
                    "contract_status": r["contract_status"],
                    "contract_end_date": r["contract_end_date"],
                    "price_amount": float(r["price_amount"]) if r["price_amount"] is not None else None,
                    "price_unit": r["price_unit"],
                    "source_name": r["source_name"]
                })
            
            for c in resp["candidates"]:
                c["mas_product_summary"] = mas_map.get(c["company_id"], [])[:5] # Limit to 5
                
        return resp
    except Exception:
        logger.exception("챗봇 API 오류: mas-search")
        return _build_chatbot_response([], error="MAS 제품 조회 실패")


@app.get("/api/chatbot/mas/product-search", tags=["챗봇"])
def get_chatbot_mas_product_search(
    product_name: str,
    detail_product_code: Optional[str] = None,
    contract_status_filter: MasContractStatusFilter = "active_only",
    status_filter: ChatbotStatusFilter = "exclude_closed",
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    """
    MAS 제품 검색.
    """
    return get_chatbot_mas_search(
        product_name=product_name,
        detail_product_code=detail_product_code,
        contract_status_filter=contract_status_filter,
        status_filter=status_filter,
        limit=limit,
        offset=offset
    )


@app.get("/api/chatbot/mas/supplier-search", tags=["챗봇"])
def get_chatbot_mas_supplier_search(
    company_keyword: Optional[str] = None,
    is_busan_company: bool = True,
    contract_status_filter: MasContractStatusFilter = "active_only",
    status_filter: ChatbotStatusFilter = "exclude_closed",
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0)
):
    """
    MAS 공급업체 검색.
    """
    try:
        conn = _get_chatbot_db()
        where_clauses = []
        if is_busan_company:
            where_clauses.append("v.is_busan_company = 1")
            
        params = []
        if company_keyword:
            where_clauses.append("(v.company_name LIKE ? OR v.company_id = ?)")
            params.extend([f"%{company_keyword}%", company_keyword])
            
        where_clauses.append(_get_mas_status_filter_sql(contract_status_filter).strip(" AND"))
        
        status_sql = _get_status_filter_sql(status_filter)
        where_sql = " AND ".join([w for w in where_clauses if w])
        
        query = f'''
            SELECT v.*, cbs.business_status as actual_business_status, cbs.business_status_freshness as actual_business_status_freshness, cbs.checked_at as business_status_checked_at, cbs.business_status_source
            FROM chatbot_company_candidate_view v
            JOIN company_identity i ON v.company_id = i.company_id
            JOIN mas_product mp ON i.company_internal_id = mp.company_internal_id
            LEFT JOIN company_business_status cbs ON i.company_internal_id = cbs.company_internal_id
            WHERE {where_sql} {status_sql}
            GROUP BY v.company_id
            ORDER BY v.company_id
            LIMIT ? OFFSET ?
        '''
        params.extend([limit, offset])
        rows = conn.execute(query, params).fetchall()
        conn.close()
        
        resp = _build_chatbot_response(rows, meta={
            "query": {
                "company_keyword": company_keyword,
                "is_busan_company": is_busan_company,
                "contract_status_filter": contract_status_filter,
                "limit": limit,
                "offset": offset
            }
        })
        return resp
    except Exception:
        logger.exception("챗봇 API 오류: mas-supplier-search")
        return _build_chatbot_response([], error="MAS 공급업체 조회 실패")


@app.get("/api/chatbot/mas/list", tags=["챗봇"])
def get_chatbot_mas_list():
    """
    MAS 통계/집계 목록.
    """
    try:
        conn = _get_chatbot_db()
        query = '''
            SELECT
                mp.detail_product_code,
                MAX(mp.product_name) as product_name,
                COUNT(DISTINCT mp.company_internal_id) as supplier_count,
                SUM(CASE WHEN mp.contract_status = 'active' THEN 1 ELSE 0 END) as active_contract_count,
                SUM(CASE WHEN mp.contract_status = 'expired' THEN 1 ELSE 0 END) as expired_contract_count,
                MAX(mp.source_refreshed_at) as refreshed_at
            FROM mas_product mp
            JOIN company_master m ON mp.company_internal_id = m.company_internal_id
            WHERE m.is_busan_company = 1
            GROUP BY mp.detail_product_code
        '''
        rows = conn.execute(query).fetchall()
        conn.close()

        candidates = []
        latest = None
        for r in rows:
            candidates.append({
                "detail_product_code": r["detail_product_code"],
                "product_name": r["product_name"],
                "supplier_count": r["supplier_count"],
                "active_contract_count": r["active_contract_count"],
                "expired_contract_count": r["expired_contract_count"]
            })
            if r["refreshed_at"]:
                if latest is None or r["refreshed_at"] > latest:
                    latest = r["refreshed_at"]

        return {
            "meta": {
                "source_refreshed_at": {"mas_product": latest} if latest else {}
            },
            "candidates": candidates,
            "company_source_status": "live_company_lookup",
            "company_search_status": "success",
            "company_cache_used": False,
            "company_cache_mode": "live_only",
            "error": None
        }
    except Exception:
        logger.exception("챗봇 API 오류: mas-list")
        return _build_chatbot_response([], error="MAS 목록 조회 실패")

if __name__ == '__main__':
    import uvicorn
    print("[API] 부산 조달 모니터링 API 서버 시작")
    print("   http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)

