from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DB_FILE = os.environ.get("CHATBOT_DB", str(Path(__file__).resolve().parents[1] / "chatbot_company.db"))
SERVICE_KEY = (
    os.environ.get("PROCUREMENT_PRODUCT_SERVICE_KEY")
    or os.environ.get("G2B_PRODUCT_LIST_SERVICE_KEY")
    or os.environ.get("SERVICE_KEY")
)
API_BASE_URL = os.environ.get(
    "PROCUREMENT_PRODUCT_API_URL",
    "https://apis.data.go.kr/1230000/ao/ThngListInfoService",
)
SOURCE_NAME = "g2b_procurement_product_list_api_20250106"

UNIT_ENDPOINTS = {
    2: "getPrdctClsfcNoUnit2Info",
    4: "getPrdctClsfcNoUnit4Info",
    6: "getPrdctClsfcNoUnit6Info",
    8: "getPrdctClsfcNoUnit8Info",
    10: "getPrdctClsfcNoUnit10Info",
}

COMPUTER_ALIAS_GROUPS = {
    "computer_equipment": {
        "컴퓨터": ["데스크톱컴퓨터", "노트북컴퓨터", "컴퓨터서버", "일체형컴퓨터", "특수목적컴퓨터"],
        "pc": ["데스크톱컴퓨터", "노트북컴퓨터"],
        "피씨": ["데스크톱컴퓨터", "노트북컴퓨터"],
        "피시": ["데스크톱컴퓨터", "노트북컴퓨터"],
        "전산장비": ["데스크톱컴퓨터", "노트북컴퓨터", "컴퓨터서버", "일체형컴퓨터"],
        "랩탑": ["노트북컴퓨터"],
        "랩톱": ["노트북컴퓨터"],
        "laptop": ["노트북컴퓨터"],
        "노트북": ["노트북컴퓨터"],
        "노트북컴퓨터": ["노트북컴퓨터"],
        "데스크톱": ["데스크톱컴퓨터"],
        "데스크탑": ["데스크톱컴퓨터"],
        "데탑": ["데스크톱컴퓨터"],
        "데스트톱": ["데스크톱컴퓨터"],
        "데스트탑": ["데스크톱컴퓨터"],
        "데스크톱컴퓨터": ["데스크톱컴퓨터"],
        "데스크탑컴퓨터": ["데스크톱컴퓨터"],
        "데스트톱컴퓨터": ["데스크톱컴퓨터"],
        "데스트탑컴퓨터": ["데스크톱컴퓨터"],
        "서버": ["컴퓨터서버"],
        "컴퓨터서버": ["컴퓨터서버"],
        "일체형컴퓨터": ["일체형컴퓨터"],
        "특수목적컴퓨터": ["특수목적컴퓨터"],
        "태블릿": ["태블릿컴퓨터"],
        "태블릿컴퓨터": ["태블릿컴퓨터"],
    }
}

GOODS_ALIAS_GROUPS = {
    "air_conditioning_equipment": {
        "냉난방기": ["냉난방기"],
        "냉난방": ["냉난방기"],
        "냉방기": ["냉난방기"],
        "난방기": ["냉난방기"],
        "냉온풍기": ["냉난방기"],
        "온풍기": ["냉난방기"],
        "시스템에어컨": ["냉난방기"],
        "에어컨": ["냉난방기"],
        "냉난방장치": ["냉난방기"],
        "냉난방설비": ["냉난방기"],
        "공기청정기": ["공기청정기"],
        "공기청정": ["공기청정기"],
        "공청기": ["공기청정기"],
        "공기정화기": ["공기청정기"],
        "공기살균기": ["공기살균기"],
        "공기살균장치": ["공기살균기"],
    },
    "security_camera_equipment": {
        "cctv": ["보안용카메라", "영상감시장치"],
        "CCTV": ["보안용카메라", "영상감시장치"],
        "씨씨티비": ["보안용카메라", "영상감시장치"],
        "시시티비": ["보안용카메라", "영상감시장치"],
        "폐쇄회로텔레비전": ["보안용카메라", "영상감시장치"],
        "보안카메라": ["보안용카메라"],
        "보안용카메라": ["보안용카메라"],
        "감시카메라": ["보안용카메라"],
        "방범카메라": ["보안용카메라"],
        "영상감시장치": ["영상감시장치"],
        "영상감시": ["영상감시장치"],
        "영상감시시스템": ["영상감시장치"],
        "감시장치": ["영상감시장치"],
    },
    "printer_office_equipment": {
        "프린터": ["레이저프린터", "다기능프린터"],
        "레이저프린터": ["레이저프린터"],
        "컬러프린터": ["레이저프린터", "다기능프린터"],
        "흑백프린터": ["레이저프린터"],
        "복합기": ["다기능프린터", "다기능복사기", "전자복사기"],
        "다기능복합기": ["다기능프린터", "다기능복사기"],
        "복합프린터": ["다기능프린터"],
        "mfp": ["다기능프린터", "다기능복사기"],
        "복사기": ["전자복사기", "다기능복사기"],
        "복사장비": ["전자복사기", "다기능복사기"],
        "문서세단기": ["문서세단기"],
        "세단기": ["문서세단기"],
        "문서파쇄기": ["문서세단기"],
        "파쇄기": ["문서세단기"],
    },
    "office_furniture": {
        "책상": ["책상", "회의용탁자", "학생용책상"],
        "사무책상": ["책상"],
        "학생책상": ["학생용책상"],
        "의자": ["작업용의자", "고정식연결의자"],
        "사무의자": ["작업용의자"],
        "작업의자": ["작업용의자"],
        "연결의자": ["고정식연결의자"],
        "사무용가구": ["책상", "작업용의자", "캐비닛"],
        "가구": ["책상", "작업용의자", "캐비닛"],
        "캐비닛": ["캐비닛", "파일링캐비닛"],
        "캐비넷": ["캐비닛", "파일링캐비닛"],
        "파일캐비닛": ["파일링캐비닛", "캐비닛"],
        "서류함": ["파일링캐비닛", "캐비닛"],
        "회의용탁자": ["회의용탁자"],
        "회의탁자": ["회의용탁자"],
        "회의테이블": ["회의용탁자"],
    },
    "presentation_broadcast_equipment": {
        "프로젝터": ["비디오프로젝터"],
        "빔프로젝터": ["비디오프로젝터"],
        "빔": ["비디오프로젝터"],
        "영상프로젝터": ["비디오프로젝터"],
        "비디오프로젝터": ["비디오프로젝터"],
        "방송장비": ["구내방송장치", "무선마이크장치", "스피커"],
        "음향장비": ["구내방송장치", "무선마이크장치", "스피커"],
        "구내방송": ["구내방송장치"],
        "구내방송장치": ["구내방송장치"],
        "구내방송설비": ["구내방송장치"],
        "무선마이크": ["무선마이크장치"],
        "마이크": ["무선마이크장치"],
        "스피커": ["스피커"],
    },
    "printed_signage": {
        "현수막": ["현수막"],
        "현수막제작": ["현수막"],
        "배너": ["현수막"],
        "인쇄물": ["기타인쇄물", "디지털인쇄물제작서비스"],
        "인쇄": ["기타인쇄물", "디지털인쇄물제작서비스"],
        "실사출력": ["디지털인쇄물제작서비스", "기타인쇄물"],
    },
    "electrical_lighting_equipment": {
        "LED조명": ["LED실내조명등", "LED경관조명기구", "LED보안등기구"],
        "LED등": ["LED실내조명등"],
        "LED실내조명": ["LED실내조명등"],
        "실내조명": ["LED실내조명등"],
        "조명": ["LED실내조명등", "경관조명기구", "보안등기구"],
        "전등": ["LED실내조명등"],
        "경관조명": ["경관조명기구", "LED경관조명기구"],
        "경관등": ["경관조명기구", "LED경관조명기구"],
        "보안등": ["보안등기구", "LED보안등기구"],
        "가로등": ["가로등기구"],
        "배전반": ["폐쇄형배전반", "분전반"],
        "수배전반": ["폐쇄형배전반", "분전반"],
        "분전반": ["분전반"],
    },
    "cleaning_hygiene_equipment": {
        "청소기": ["진공청소기", "건습식진공청소기", "바닥청소기"],
        "진공청소기": ["진공청소기", "건습식진공청소기"],
        "업소용청소기": ["건습식진공청소기", "진공청소기"],
        "청소장비": ["진공청소기", "건습식진공청소기", "바닥청소기"],
        "바닥청소기": ["바닥청소기", "건식바닥청소기"],
        "방역기": ["방역용소독기"],
        "방역소독기": ["방역용소독기"],
        "방역용소독기": ["방역용소독기"],
        "소독기": ["방역용소독기"],
        "소독제": ["손소독제", "감염병예방용방역살균소독제", "외피용살균소독제"],
        "살균소독제": ["감염병예방용방역살균소독제", "외피용살균소독제"],
        "방역약품": ["감염병예방용방역살균소독제"],
        "손소독제": ["손소독제"],
        "손세정제": ["손소독제"],
        "마스크": ["보건용마스크", "방진마스크", "일반마스크"],
        "보건용마스크": ["보건용마스크"],
        "kf마스크": ["보건용마스크"],
    },
    "education_equipment": {
        "전자칠판": ["전자칠판"],
        "스마트칠판": ["전자칠판"],
        "전자보드": ["전자칠판"],
        "화이트보드": ["칠판", "전자칠판"],
        "칠판": ["칠판", "전자칠판"],
        "학습교구": ["자석판학습교구"],
        "과학교구": ["자석판학습교구"],
        "교구": ["자석판학습교구", "유아용교구장"],
        "실험대": ["실험대"],
        "과학실험대": ["실험대"],
        "교구장": ["유아용교구장"],
    },
    "kitchen_catering_equipment": {
        "냉장고": ["냉장고", "대형냉장고"],
        "업소용냉장고": ["대형냉장고", "냉장고"],
        "급식냉장고": ["대형냉장고", "냉장고"],
        "식기세척기": ["상업용식기세척기", "가정용식기세척기"],
        "식세기": ["상업용식기세척기", "가정용식기세척기"],
        "세척기": ["상업용식기세척기"],
        "조리대": ["상업용조리대"],
        "급식조리대": ["상업용조리대"],
        "배식대": ["배식대"],
        "오븐": ["상업용오븐"],
        "상업용오븐": ["상업용오븐"],
        "전기오븐": ["상업용오븐"],
        "컨벡션오븐": ["상업용오븐"],
        "취반기": ["취반기"],
        "밥솥": ["취반기"],
        "급식기구": ["상업용식기세척기", "상업용조리대", "배식대", "취반기"],
    },
    "network_equipment": {
        "네트워크스위치": ["네트워크스위치"],
        "네트워크장비": ["네트워크스위치", "무선랜액세스포인트", "라우터", "방화벽장치"],
        "스위칭허브": ["네트워크스위치"],
        "스위치허브": ["네트워크스위치"],
        "L2스위치": ["네트워크스위치"],
        "L3스위치": ["네트워크스위치"],
        "무선AP": ["무선랜액세스포인트"],
        "AP": ["무선랜액세스포인트"],
        "와이파이AP": ["무선랜액세스포인트"],
        "와이파이": ["무선랜액세스포인트"],
        "무선랜": ["무선랜액세스포인트"],
        "라우터": ["라우터"],
        "방화벽": ["방화벽장치"],
        "방화벽장치": ["방화벽장치"],
        "디스크어레이": ["디스크어레이"],
        "저장장치": ["SSD저장장치", "레이드저장장치", "디스크어레이"],
    },
    "fire_safety_equipment": {
        "소방용품": ["수동식소화기", "연기감지기", "유도등"],
        "소방안전용품": ["수동식소화기", "연기감지기", "유도등"],
        "소화기": ["수동식소화기", "자동식소화기", "캐비닛형소화기"],
        "분말소화기": ["수동식소화기"],
        "자동확산소화기": ["자동식소화기"],
        "수동식소화기": ["수동식소화기"],
        "자동식소화기": ["자동식소화기"],
        "화재감지기": ["연기감지기", "열감지기", "복합형화재감지기", "단독경보형감지기"],
        "감지기": ["연기감지기", "열감지기", "복합형화재감지기", "단독경보형감지기"],
        "연기감지기": ["연기감지기"],
        "열감지기": ["열감지기"],
        "유도등": ["유도등"],
    },
    "personal_safety_equipment": {
        "안전용품": ["안전화", "안전조끼", "구명조끼"],
        "안전화": ["안전화"],
        "작업화": ["안전화"],
        "안전조끼": ["안전조끼"],
        "안전vest": ["안전조끼"],
        "구명조끼": ["구명조끼"],
    },
    "road_traffic_equipment": {
        "도로시설물": ["볼라드", "차선분리대", "도로표지", "과속방지턱"],
        "교통시설물": ["도로표지", "교통신호등", "차량번호판독기", "무인교통감시장치"],
        "볼라드": ["볼라드"],
        "차선규제봉": ["차선규제봉", "차선분리대", "차선오뚝이"],
        "차선분리대": ["차선분리대"],
        "도로표지": ["도로표지"],
        "교통표지": ["도로표지", "안전표지판"],
        "도로표지판": ["도로표지", "안전표지판"],
        "과속방지턱": ["과속방지턱"],
        "교통신호등": ["교통신호등"],
        "신호등": ["교통신호등"],
        "차량번호판독기": ["차량번호판독기"],
        "번호인식기": ["차량번호판독기"],
        "차량번호인식기": ["차량번호판독기"],
        "LPR": ["차량번호판독기"],
        "주차관제": ["차량차단기", "주차관제주변기기", "주차관제장치"],
        "주차관제장치": ["차량차단기", "주차관제주변기기", "주차관제장치"],
        "차량차단기": ["차량차단기"],
        "주차차단기": ["차량차단기"],
        "무인교통감시장치": ["무인교통감시장치"],
    },
    "waste_environment_equipment": {
        "분리수거함": ["쓰레기통", "재활용품자동회수기"],
        "분리배출함": ["쓰레기통", "재활용품자동회수기"],
        "재활용수거함": ["쓰레기통", "재활용품자동회수기"],
        "재활용분리수거함": ["쓰레기통", "재활용품자동회수기"],
        "쓰레기통": ["쓰레기통"],
        "쓰레기수거함": ["쓰레기통"],
        "음식물수거함": ["음식물쓰레기처리통"],
        "음식물쓰레기통": ["음식물쓰레기처리통"],
        "음식물쓰레기처리통": ["음식물쓰레기처리통"],
        "음식물쓰레기종량기": ["음식물쓰레기종량기"],
        "음식물쓰레기처리기": ["일반용음식물쓰레기처리기", "가정용음식물쓰레기처리기"],
        "재활용품자동회수기": ["재활용품자동회수기"],
        "재활용품압축기": ["재활용품압축기"],
    },
    "park_sports_equipment": {
        "야외운동기구": ["야외운동기구", "종합운동기구"],
        "운동기구": ["야외운동기구", "종합운동기구"],
        "체육시설": ["야외운동기구", "종합운동기구", "체육시설탄성포장재"],
        "놀이기구": ["기타놀이기구", "회전놀이기구", "운동장및놀이터용흔들놀이기구"],
        "어린이놀이기구": ["기타놀이기구", "회전놀이기구", "운동장및놀이터용흔들놀이기구"],
        "어린이놀이시설": ["기타놀이기구", "어린이놀이시설탄성포장재"],
        "벤치": ["옥외용벤치", "벤치"],
        "옥외용벤치": ["옥외용벤치"],
        "공원벤치": ["옥외용벤치", "벤치"],
        "야외벤치": ["옥외용벤치", "벤치"],
        "파고라": ["퍼걸러"],
        "퍼걸러": ["퍼걸러"],
        "퍼골라": ["퍼걸러"],
        "정자": ["퍼걸러"],
    },
    "medical_health_equipment": {
        "AED": ["저출력심장충격기"],
        "aed": ["저출력심장충격기"],
        "제세동기": ["저출력심장충격기"],
        "자동제세동기": ["저출력심장충격기"],
        "자동심장충격기": ["저출력심장충격기"],
        "심장충격기": ["저출력심장충격기"],
        "체온계": ["적외선체온계", "전자체온계"],
        "비접촉체온계": ["적외선체온계"],
        "비접촉식체온계": ["적외선체온계"],
        "적외선체온계": ["적외선체온계"],
        "전자체온계": ["전자체온계"],
        "혈압계": ["자동전자혈압계"],
        "자동혈압계": ["자동전자혈압계"],
        "자동혈압측정기": ["자동전자혈압계"],
    },
}

CURATED_ALIAS_GROUPS = {**COMPUTER_ALIAS_GROUPS, **GOODS_ALIAS_GROUPS}


def main() -> int:
    parser = argparse.ArgumentParser(description="Import G2B procurement product classifications into chatbot_company.db")
    parser.add_argument("--db", default=DB_FILE, help="SQLite DB path")
    parser.add_argument("--service-key", default=SERVICE_KEY, help="data.go.kr service key")
    parser.add_argument("--units", default="2,4,6,8,10", help="comma-separated units to import")
    parser.add_argument("--num-rows", type=int, default=999, help="API page size; data.go.kr falls back to 10 when 1000 is used")
    parser.add_argument("--sleep", type=float, default=0.05, help="sleep seconds between API calls")
    parser.add_argument("--seed-only", action="store_true", help="only rebuild curated alias rows from existing unit10 table")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    started_at = now_text()
    try:
        imported_count = 0
        if not args.seed_only:
            if not args.service_key:
                raise RuntimeError("PROCUREMENT_PRODUCT_SERVICE_KEY, G2B_PRODUCT_LIST_SERVICE_KEY, or SERVICE_KEY is required")
            units = parse_units(args.units)
            for unit in units:
                imported_count += import_unit(
                    conn,
                    unit=unit,
                    service_key=args.service_key,
                    num_rows=args.num_rows,
                    sleep_seconds=args.sleep,
                )

        alias_count = seed_curated_aliases(conn)
        insert_job_log(
            conn,
            started_at=started_at,
            status="success",
            imported_count=imported_count,
            alias_count=alias_count,
        )
        print(json.dumps({"status": "success", "imported_count": imported_count, "alias_count": alias_count}, ensure_ascii=False))
        return 0
    except Exception as exc:
        insert_job_log(
            conn,
            started_at=started_at,
            status="failed",
            imported_count=0,
            alias_count=0,
            error_message=str(exc),
        )
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    finally:
        conn.close()


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS procurement_product_classification (
            classification_no TEXT PRIMARY KEY,
            classification_unit INTEGER NOT NULL,
            classification_name TEXT NOT NULL,
            classification_name_normalized TEXT NOT NULL,
            parent_classification_no TEXT NOT NULL DEFAULT '',
            english_name TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            use_yn TEXT NOT NULL DEFAULT '',
            chg_date TEXT NOT NULL DEFAULT '',
            source_name TEXT NOT NULL,
            source_refreshed_at DATETIME NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_procurement_product_classification_name
        ON procurement_product_classification(classification_name_normalized)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_procurement_product_classification_parent
        ON procurement_product_classification(parent_classification_no)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS procurement_product_alias (
            alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
            alias TEXT NOT NULL,
            alias_normalized TEXT NOT NULL,
            canonical_name TEXT NOT NULL,
            canonical_name_normalized TEXT NOT NULL DEFAULT '',
            dtil_prdct_clsfc_no TEXT NOT NULL,
            prdct_clsfc_no TEXT NOT NULL DEFAULT '',
            domain TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT 'curated',
            priority INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            source_refreshed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_procurement_product_alias_unique
        ON procurement_product_alias(alias_normalized, dtil_prdct_clsfc_no, domain)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_procurement_product_alias_lookup
        ON procurement_product_alias(alias_normalized, domain, is_active)
        """
    )
    conn.commit()


def parse_units(value: str) -> list[int]:
    units: list[int] = []
    for part in value.split(","):
        if not part.strip():
            continue
        unit = int(part.strip())
        if unit not in UNIT_ENDPOINTS:
            raise ValueError(f"unsupported unit: {unit}")
        units.append(unit)
    return units or [10]


def import_unit(
    conn: sqlite3.Connection,
    *,
    unit: int,
    service_key: str,
    num_rows: int,
    sleep_seconds: float,
) -> int:
    page = 1
    imported = 0
    endpoint = UNIT_ENDPOINTS[unit]
    num_rows = min(max(1, int(num_rows)), 999)
    while True:
        print(f"fetching unit={unit} page={page} num_rows={num_rows}", flush=True)
        payload = api_get(endpoint, service_key=service_key, page_no=page, num_rows=num_rows)
        body = payload.get("response", {}).get("body", {})
        total_count = int(body.get("totalCount") or 0)
        items = body.get("items") or []
        if isinstance(items, dict):
            items = [items]
        if not items:
            break
        rows = [normalize_classification_item(unit, item) for item in items]
        upsert_classification_rows(conn, rows)
        imported += len(rows)
        print(f"  imported unit={unit} count={imported}/{total_count or '?'}", flush=True)
        if total_count and imported >= total_count:
            break
        page += 1
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return imported


def api_get(endpoint: str, *, service_key: str, page_no: int, num_rows: int) -> dict[str, Any]:
    query = urllib.parse.urlencode(
        {
            "ServiceKey": service_key,
            "pageNo": str(page_no),
            "numOfRows": str(num_rows),
            "type": "json",
        }
    )
    url = f"{API_BASE_URL.rstrip('/')}/{endpoint}?{query}"
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read().decode("utf-8")
    payload = json.loads(body)
    header = payload.get("response", {}).get("header", {})
    if str(header.get("resultCode")) != "00":
        raise RuntimeError(f"API returned {header.get('resultCode')}: {header.get('resultMsg')}")
    return payload


def normalize_classification_item(unit: int, item: dict[str, Any]) -> dict[str, str | int]:
    if unit == 10:
        code = clean(item.get("dtilPrdctClsfcNo"))
        name = clean(item.get("dtilPrdctClsfcNoNm"))
        english_name = clean(item.get("dtilPrdctClsfcNoEngNm"))
        description = clean(item.get("dtilPrdctClsfcNoNmDscrpt"))
        parent = code[:8] if len(code) >= 8 else ""
    else:
        code = clean(item.get("prdctClsfcNo"))
        name = clean(item.get("prdctClsfcNoNm"))
        english_name = clean(item.get("prdctClsfcNoEngNm"))
        description = clean(item.get("prdctClsfcNoNmDscrpt"))
        parent = code[:-2] if len(code) > 2 else ""
    return {
        "classification_no": code,
        "classification_unit": unit,
        "classification_name": name,
        "classification_name_normalized": normalize_text(name),
        "parent_classification_no": parent,
        "english_name": english_name,
        "description": description,
        "use_yn": clean(item.get("useYn")),
        "chg_date": clean(item.get("chgDate")),
        "source_name": SOURCE_NAME,
        "source_refreshed_at": now_text(),
    }


def upsert_classification_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO procurement_product_classification (
            classification_no, classification_unit, classification_name,
            classification_name_normalized, parent_classification_no,
            english_name, description, use_yn, chg_date, source_name,
            source_refreshed_at
        )
        VALUES (
            :classification_no, :classification_unit, :classification_name,
            :classification_name_normalized, :parent_classification_no,
            :english_name, :description, :use_yn, :chg_date, :source_name,
            :source_refreshed_at
        )
        ON CONFLICT(classification_no) DO UPDATE SET
            classification_unit=excluded.classification_unit,
            classification_name=excluded.classification_name,
            classification_name_normalized=excluded.classification_name_normalized,
            parent_classification_no=excluded.parent_classification_no,
            english_name=excluded.english_name,
            description=excluded.description,
            use_yn=excluded.use_yn,
            chg_date=excluded.chg_date,
            source_name=excluded.source_name,
            source_refreshed_at=excluded.source_refreshed_at,
            updated_at=CURRENT_TIMESTAMP
        """,
        rows,
    )
    conn.commit()


def seed_curated_aliases(conn: sqlite3.Connection) -> int:
    now = now_text()
    rows: list[tuple[str, str, str, str, str, str, str, int, int, str]] = []
    for domain, alias_map in CURATED_ALIAS_GROUPS.items():
        for alias, canonical_names in alias_map.items():
            for priority_offset, canonical_name in enumerate(canonical_names):
                standard = find_unit10_by_exact_name(conn, canonical_name)
                if standard is None:
                    continue
                rows.append(
                    (
                        alias,
                        normalize_text(alias),
                        standard["classification_name"],
                        normalize_text(standard["classification_name"]),
                        standard["classification_no"],
                        standard["parent_classification_no"],
                        domain,
                        "curated_g2b_unit10",
                        max(1, 100 - priority_offset),
                        1,
                        now,
                    )
                )

    for name in ["데스크톱컴퓨터", "노트북컴퓨터", "컴퓨터서버", "일체형컴퓨터", "특수목적컴퓨터", "태블릿컴퓨터"]:
        standard = find_unit10_by_exact_name(conn, name)
        if standard is None:
            continue
        rows.append(
            (
                standard["classification_name"],
                normalize_text(standard["classification_name"]),
                standard["classification_name"],
                normalize_text(standard["classification_name"]),
                standard["classification_no"],
                standard["parent_classification_no"],
                "computer_equipment",
                "g2b_unit10_self_alias",
                120,
                1,
                now,
            )
        )

    delete_domains = tuple(CURATED_ALIAS_GROUPS)
    placeholders = ",".join("?" for _ in delete_domains)
    conn.execute(f"DELETE FROM procurement_product_alias WHERE domain IN ({placeholders})", delete_domains)
    if rows:
        conn.executemany(
            """
            INSERT INTO procurement_product_alias (
                alias, alias_normalized, canonical_name, canonical_name_normalized,
                dtil_prdct_clsfc_no, prdct_clsfc_no, domain, source,
                priority, is_active, source_refreshed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(alias_normalized, dtil_prdct_clsfc_no, domain) DO UPDATE SET
                alias=excluded.alias,
                canonical_name=excluded.canonical_name,
                canonical_name_normalized=excluded.canonical_name_normalized,
                prdct_clsfc_no=excluded.prdct_clsfc_no,
                source=excluded.source,
                priority=excluded.priority,
                is_active=excluded.is_active,
                source_refreshed_at=excluded.source_refreshed_at,
                updated_at=CURRENT_TIMESTAMP
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def find_unit10_by_exact_name(conn: sqlite3.Connection, name: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT classification_no, classification_name, parent_classification_no
        FROM procurement_product_classification
        WHERE classification_unit = 10
          AND classification_name_normalized = ?
          AND use_yn = 'Y'
        ORDER BY
          CASE WHEN classification_no LIKE '99%' THEN 1 ELSE 0 END,
          classification_no
        LIMIT 1
        """,
        (normalize_text(name),),
    ).fetchone()


def insert_job_log(
    conn: sqlite3.Connection,
    *,
    started_at: str,
    status: str,
    imported_count: int,
    alias_count: int,
    error_message: str | None = None,
) -> None:
    finished_at = now_text()
    if table_exists(conn, "etl_job_log"):
        conn.execute(
            """
            INSERT INTO etl_job_log (
                job_name, source_name, started_at, finished_at, status,
                input_row_count, inserted_count, skipped_count, error_count, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "import_procurement_product_classification_api",
                SOURCE_NAME,
                started_at,
                finished_at,
                status,
                imported_count,
                alias_count,
                0,
                0 if status == "success" else 1,
                error_message,
            ),
        )
    if table_exists(conn, "source_manifest"):
        conn.execute(
            """
            INSERT INTO source_manifest (
                source_name, source_type, source_url_or_file,
                row_count, source_refreshed_at, status, error_message
            ) VALUES (?, 'api_full', ?, ?, ?, ?, ?)
            ON CONFLICT(source_name) DO UPDATE SET
                source_url_or_file=excluded.source_url_or_file,
                row_count=excluded.row_count,
                source_refreshed_at=excluded.source_refreshed_at,
                status=excluded.status,
                error_message=excluded.error_message,
                updated_at=CURRENT_TIMESTAMP
            """,
            (SOURCE_NAME, API_BASE_URL, imported_count, finished_at, status, error_message),
        )
    conn.commit()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
    except Exception:
        return False
    return row is not None


def normalize_text(value: object) -> str:
    text = clean(value).lower()
    for ch in [" ", "-", "_", "/", "(", ")", "[", "]", ".", ","]:
        text = text.replace(ch, "")
    return text


def clean(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def now_text() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    raise SystemExit(main())
