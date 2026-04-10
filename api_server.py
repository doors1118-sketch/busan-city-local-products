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

@app.get("/api/company/license-search", tags=["업체 검색"])
def search_by_license(
    q: str = Query(..., min_length=1, description="면허업종명 (정확 매칭 또는 포함 검색)"),
    exact: bool = Query(False, description="True면 정확 매칭, False면 포함 검색"),
    limit: int = Query(200, ge=1, le=1000, description="최대 반환 건수"),
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
    limit: int = Query(200, ge=1, le=1000, description="최대 반환 건수"),
):
    """대표품명(세부품명)으로 업체 검색 → 업체명/대표자/소재지/대표품명"""
    try:
        conn = _get_company_db()
        q_clean = q.strip()
        rows = conn.execute("""
            SELECT corpNm, bizno, ceoNm, rgnNm, adrs, dtlAdrs,
                   hdoffceDivNm, corpBsnsDivNm, rprsntDtlPrdnm, opbizDt, rgstDt
            FROM company_master
            WHERE rprsntDtlPrdnm LIKE ?
            ORDER BY corpNm
            LIMIT ?
        """, (f"%{q_clean}%", limit)).fetchall()
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

if __name__ == '__main__':
    import uvicorn
    print("[API] 부산 조달 모니터링 API 서버 시작")
    print("   http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)
