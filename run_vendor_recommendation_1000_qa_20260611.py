from __future__ import annotations

import argparse
import json
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ITEMS = [
    {"kind": "goods", "name": "LED 조명", "terms": ["led", "조명"], "policy": True, "caps": ["shopping", "mas"], "avoid": []},
    {"kind": "goods", "name": "CCTV 보안카메라", "terms": ["cctv", "카메라", "영상감시"], "policy": True, "caps": ["shopping"], "avoid": []},
    {"kind": "goods", "name": "데스크톱 컴퓨터", "terms": ["컴퓨터", "데스크톱", "pc"], "policy": True, "caps": ["shopping", "mas", "direct"], "avoid": []},
    {"kind": "goods", "name": "노트북", "terms": ["노트북", "컴퓨터"], "policy": True, "caps": ["shopping", "mas"], "avoid": []},
    {"kind": "goods", "name": "서버 컴퓨터", "terms": ["서버", "컴퓨터"], "policy": True, "caps": ["shopping"], "avoid": []},
    {"kind": "goods", "name": "비디오프로젝터", "terms": ["비디오", "프로젝터"], "policy": True, "caps": ["shopping"], "avoid": []},
    {"kind": "goods", "name": "토너 카트리지", "terms": ["토너"], "policy": True, "caps": ["shopping"], "avoid": ["축산", "생육"]},
    {"kind": "goods", "name": "프린터", "terms": ["프린터", "복합기"], "policy": True, "caps": ["shopping", "mas"], "avoid": []},
    {"kind": "goods", "name": "드론", "terms": ["드론"], "policy": True, "caps": ["venture"], "avoid": []},
    {"kind": "goods", "name": "책상", "terms": ["책상"], "policy": True, "caps": ["direct", "sme"], "avoid": []},
    {"kind": "goods", "name": "의자", "terms": ["의자"], "policy": True, "caps": ["direct", "sme"], "avoid": []},
    {"kind": "goods", "name": "캐비닛", "terms": ["캐비닛", "가구"], "policy": True, "caps": ["direct", "sme"], "avoid": []},
    {"kind": "goods", "name": "인쇄물", "terms": ["인쇄"], "policy": True, "caps": ["direct", "sme"], "avoid": []},
    {"kind": "goods", "name": "현수막", "terms": ["현수막"], "policy": True, "caps": ["direct", "sme"], "avoid": []},
    {"kind": "goods", "name": "레미콘", "terms": ["레미콘", "콘크리트"], "policy": True, "caps": ["facility_material"], "avoid": []},
    {"kind": "goods", "name": "아스콘", "terms": ["아스콘", "아스팔트"], "policy": True, "caps": ["facility_material"], "avoid": []},
    {"kind": "goods", "name": "복층유리", "terms": ["복층유리", "유리"], "policy": True, "caps": ["facility_material"], "avoid": []},
    {"kind": "goods", "name": "각재", "terms": ["각재", "목재"], "policy": True, "caps": ["facility_material"], "avoid": []},
    {"kind": "goods", "name": "스텐밴드", "terms": ["스텐밴드"], "policy": True, "caps": ["facility_material"], "avoid": []},
    {"kind": "goods", "name": "소화전", "terms": ["소화전", "소방"], "policy": True, "caps": ["facility_material"], "avoid": []},
    {"kind": "service", "name": "행사 운영 용역", "terms": ["행사", "이벤트"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "번역 용역", "terms": ["번역", "통역"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "청소 용역", "terms": ["청소", "환경미화"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "경비 용역", "terms": ["경비", "시설경비"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "시설관리 용역", "terms": ["시설관리", "건물관리"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "방역 소독 용역", "terms": ["방역", "소독"], "policy": False, "caps": [], "avoid": ["자소엽제"]},
    {"kind": "service", "name": "폐기물 처리 용역", "terms": ["폐기물"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "승강기 유지보수", "terms": ["승강기", "유지보수"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "회계 정산 용역", "terms": ["회계", "정산"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "디자인 용역", "terms": ["디자인"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "영상 제작 용역", "terms": ["영상", "동영상"], "policy": False, "caps": [], "avoid": []},
    {"kind": "service", "name": "정보시스템 유지보수", "terms": ["시스템", "소프트웨어", "유지보수"], "policy": False, "caps": [], "avoid": []},
    {"kind": "works", "name": "전기공사", "terms": ["전기공사"], "policy": False, "caps": ["capacity"], "avoid": []},
    {"kind": "works", "name": "정보통신공사", "terms": ["정보통신"], "policy": False, "caps": ["capacity"], "avoid": []},
    {"kind": "works", "name": "소방시설공사", "terms": ["소방"], "policy": False, "caps": ["capacity"], "avoid": []},
    {"kind": "works", "name": "조경식재공사", "terms": ["조경", "식재"], "policy": False, "caps": ["capacity"], "avoid": []},
    {"kind": "works", "name": "도로 포장공사", "terms": ["포장", "도로"], "policy": False, "caps": ["capacity"], "avoid": []},
    {"kind": "works", "name": "실내건축공사", "terms": ["실내건축", "건축"], "policy": False, "caps": ["capacity"], "avoid": []},
    {"kind": "works", "name": "기계설비공사", "terms": ["기계설비"], "policy": False, "caps": ["capacity"], "avoid": []},
    {"kind": "works", "name": "방수공사", "terms": ["방수"], "policy": False, "caps": ["capacity"], "avoid": []},
    {"kind": "works", "name": "금속창호공사", "terms": ["금속", "창호"], "policy": False, "caps": ["capacity"], "avoid": []},
    {"kind": "works", "name": "상하수도설비공사", "terms": ["상하수도", "설비"], "policy": False, "caps": ["capacity"], "avoid": []},
]

CONTEXTS = [
    "구청 발주 담당자가",
    "공공기관 구매 담당자가",
    "부산시 산하기관에서",
    "학교 계약 담당자가",
    "복지관에서",
    "문화행사 담당부서에서",
    "시설관리 부서에서",
    "정보화사업 담당자가",
]

PURPOSES = [
    "후보군 비교용",
    "조달 등록 근거 확인용",
    "계약 편의성 높은 순서로",
    "정책기업 여부까지 같이",
    "부산 본사 업체 중심으로",
    "엑셀 다운로드 검토용",
    "수의계약 가능성 검토용",
    "공고 전 시장조사용",
    "MAS나 쇼핑몰 등록 여부 포함해서",
    "직접생산 필요 여부 포함해서",
    "중기간경쟁 해당 여부 포함해서",
    "면허 업종 근거가 보이게",
]

BUDGETS = [
    5_000_000,
    8_000_000,
    12_000_000,
    18_000_000,
    25_000_000,
    35_000_000,
    45_000_000,
    55_000_000,
    70_000_000,
    90_000_000,
    120_000_000,
    200_000_000,
    500_000_000,
]

TEMPLATES = [
    "{ctx} {item} 업체 후보를 찾아줘. {purpose}",
    "{item} 예산 {budget_label} 기준으로 부산업체 추천해줘. {purpose}",
    "{item} 계약 가능한 지역업체 목록이 필요해. {purpose}",
    "{ctx} {item} 조달등록 업체를 확인하려고 해. {purpose}",
    "{item} 수의계약 검토 전에 부산 업체를 보고 싶어. {purpose}",
    "{item} 종합쇼핑몰이나 MAS 등록업체도 같이 보여줘. {purpose}",
    "{item} 직접생산 확인이 필요한지와 업체 후보를 같이 보고 싶어. {purpose}",
    "{item} 중기간경쟁제품이면 부산업체 후보를 보여줘. {purpose}",
    "{item} 오타가 있어도 적절한 업체를 찾아줘. {purpose}",
    "{ctx} {item} 관련 면허나 품목을 가진 업체를 보여줘. {purpose}",
]

AMBIGUOUS = [
    "{short} 업체 좀 찾아줘. {purpose}",
    "{short} 되는 부산업체 있어? {purpose}",
    "{short} 관련 조달업체 후보. {purpose}",
    "{short} 구매하려는데 지역업체가 있나? {purpose}",
]

TYPO = {
    "비디오프로젝터": ["빔프로젝터", "비됴프로젝터", "프로젝터"],
    "데스크톱 컴퓨터": ["데스크탑 컴퓨터", "PC", "피씨"],
    "토너 카트리지": ["토너", "토너카트리지"],
    "CCTV 보안카메라": ["씨씨티비", "cctv카메라", "보안캠"],
    "방역 소독 용역": ["소독방역", "방역용역"],
    "정보시스템 유지보수": ["시스템유지보수", "소프트웨어 유지관리"],
    "상하수도설비공사": ["상하수도 공사", "상수도 설비"],
}


def budget_label(krw: int) -> str:
    if krw >= 100_000_000:
        return f"{krw / 100_000_000:g}억원"
    return f"{krw // 10_000:,}만원"


def compact(text: str) -> str:
    return "".join(ch.lower() for ch in str(text or "") if ch.isalnum())


def make_cases(target: int, seed: int = 20260611) -> list[dict[str, Any]]:
    random.seed(seed)
    cases: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(item: dict[str, Any], q: str, tags: list[str], budget: int) -> None:
        if q in seen or len(cases) >= target:
            return
        seen.add(q)
        cases.append(
            {
                "id": f"qa_{len(cases)+1:04d}",
                "q": q,
                "item": item["name"],
                "kind": item["kind"],
                "terms": item["terms"],
                "policy_expected": item["policy"],
                "caps": item.get("caps", []),
                "avoid": item.get("avoid", []),
                "tags": tags,
                "budget_krw": budget,
            }
        )

    seq = 0
    while len(cases) < target:
        item = ITEMS[seq % len(ITEMS)]
        budget = BUDGETS[seq % len(BUDGETS)]
        ctx = CONTEXTS[seq % len(CONTEXTS)]
        purpose = PURPOSES[(seq * 3) % len(PURPOSES)]
        if seq % 7 == 0:
            short = item["name"].replace(" 용역", "").replace("공사", "").replace(" 카트리지", "")
            template = AMBIGUOUS[(seq // 7) % len(AMBIGUOUS)]
            q = template.format(short=short, purpose=purpose)
            tags = ["ambiguous"]
        elif item["name"] in TYPO and seq % 5 == 0:
            typo = TYPO[item["name"]][(seq // 5) % len(TYPO[item["name"]])]
            q = f"{ctx} {typo} 업체 후보를 찾아줘. {purpose}"
            tags = ["typo"]
        elif seq % 11 == 0:
            other = ITEMS[(seq * 5 + 3) % len(ITEMS)]
            q = f"{ctx} {item['name']}와 {other['name']} 둘 다 가능한 업체가 있는지 찾아줘. {purpose}"
            tags = ["mixed"]
        elif seq % 13 == 0:
            policy = ["여성기업", "장애인기업", "벤처기업", "창업기업", "사회적기업"][seq % 5]
            q = f"{policy} 조건으로 {item['name']} 부산업체 후보를 보여줘. {purpose}"
            tags = ["policy"]
        else:
            template = TEMPLATES[seq % len(TEMPLATES)]
            q = template.format(ctx=ctx, item=item["name"], budget_label=budget_label(budget), purpose=purpose)
            tags = ["base"]
        add(item, q, tags, budget)
        seq += 1
        if seq > target * 20:
            raise RuntimeError("failed to generate unique cases")
    return cases


def http_get_json(base_url: str, path: str, params: dict[str, Any], timeout: float) -> tuple[dict[str, Any], int]:
    url = base_url.rstrip("/") + path + "?" + urlencode(params, doseq=True)
    start = time.perf_counter()
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    return json.loads(raw.decode("utf-8")), elapsed_ms


def text_blob(row: dict[str, Any]) -> str:
    fields = [
        "company_name",
        "license_or_business_type",
        "main_products",
        "certified_product_summary",
        "shopping_mall_product_summary",
        "mas_product_summary",
        "direct_production_summary",
        "construction_capacity_summary",
        "venture_nara_product_summary",
        "venture_nara_order_summary",
        "policy_company_labels",
        "contract_review_types",
    ]
    return " ".join(str(row.get(f) or "") for f in fields)


def evaluate_case(base_url: str, case: dict[str, Any], timeout: float, max_latency_ms: int) -> dict[str, Any]:
    try:
        data, elapsed_ms = http_get_json(
            base_url,
            "/vendor-recommendations/search",
            {
                "q": case["q"],
                "region": "부산",
                "limit": 10,
                "budget_krw": case["budget_krw"],
                "include_product_policy": "true",
            },
            timeout,
        )
        rows = data.get("rows") or []
        first = rows[0] if rows else {}
        row_blob = compact(" ".join(text_blob(r) for r in rows[:5]))
        first_blob = compact(text_blob(first))
        checks: dict[str, bool] = {}
        checks["http_ok"] = True
        checks["latency_ok"] = elapsed_ms <= max_latency_ms
        checks["has_rows"] = len(rows) > 0
        checks["has_active_or_display_status"] = bool(first.get("business_status_label") or first.get("business_status"))
        checks["has_review_score"] = bool(str(first.get("review_score") or ""))
        checks["has_recommended_checks"] = bool(first.get("recommended_checks"))
        terms = case.get("terms") or []
        checks["term_match_any_top5"] = any(compact(term) and compact(term) in row_blob for term in terms)
        checks["term_match_top1"] = any(compact(term) and compact(term) in first_blob for term in terms)
        if case.get("policy_expected"):
            summary = data.get("item_policy_summary") or {}
            checks["policy_summary_present"] = bool(summary) and summary.get("status") != "not_requested"
        for cap in case.get("caps") or []:
            if cap == "shopping":
                checks["shopping_or_mas_visible"] = any(
                    str(r.get("shopping_mall_status_label") or "").startswith("종합쇼핑몰")
                    or str(r.get("mas_status_label") or "").startswith("MAS")
                    for r in rows
                )
            elif cap == "mas":
                checks["mas_visible"] = any(str(r.get("mas_status_label") or "").startswith("MAS") for r in rows)
            elif cap == "direct":
                checks["direct_production_visible"] = any(
                    "직접생산" in str(r.get("direct_production_certificate_status") or "")
                    or bool(r.get("direct_production_certificate_products"))
                    for r in rows
                )
            elif cap == "capacity":
                checks["capacity_visible"] = any(
                    "시공능력" in str(r.get("construction_capacity_status_label") or "")
                    or bool(r.get("construction_capacity_summary"))
                    for r in rows
                )
            elif cap == "venture":
                checks["venture_visible"] = any(
                    "벤처나라" in str(r.get("venture_nara_status_label") or "")
                    or bool(r.get("venture_nara_product_summary"))
                    for r in rows
                )
            elif cap == "facility_material":
                policy = data.get("item_policy_summary") or {}
                checks["facility_material_signal_visible"] = (
                    "시설" in json.dumps(policy, ensure_ascii=False)
                    or "가격" in json.dumps(policy, ensure_ascii=False)
                    or "공사용자재" in json.dumps(policy, ensure_ascii=False)
                )
        for avoid in case.get("avoid") or []:
            checks[f"avoid_top1_{avoid}"] = compact(avoid) not in first_blob
        weights = {
            "http_ok": 15,
            "latency_ok": 5,
            "has_rows": 20,
            "has_active_or_display_status": 5,
            "has_review_score": 5,
            "has_recommended_checks": 5,
            "term_match_any_top5": 25,
            "term_match_top1": 10,
            "policy_summary_present": 5,
            "shopping_or_mas_visible": 5,
            "mas_visible": 5,
            "direct_production_visible": 5,
            "capacity_visible": 5,
            "venture_visible": 5,
            "facility_material_signal_visible": 10,
        }
        score = sum(weights.get(k, 3) for k, v in checks.items() if v)
        max_score = sum(weights.get(k, 3) for k in checks)
        score100 = round(score / max_score * 100, 1) if max_score else 0
        if score100 >= 80:
            grade = "good"
        elif score100 >= 60:
            grade = "needs_review"
        else:
            grade = "weak"
        return {
            **case,
            "elapsed_ms": elapsed_ms,
            "row_count": len(rows),
            "score": score100,
            "grade": grade,
            "checks": checks,
            "first_company": first.get("company_name", ""),
            "first_review_score": first.get("review_score", ""),
            "first_match_rank_score": first.get("match_rank_score", ""),
            "first_contract_review_types": first.get("contract_review_types", ""),
            "first_budget_review_hint": first.get("budget_review_hint", ""),
            "item_policy_summary": data.get("item_policy_summary") or {},
            "policy_preference_summary": data.get("policy_preference_summary") or {},
            "search_plan": (data.get("meta") or {}).get("search_plan"),
            "error": "",
        }
    except Exception as exc:
        return {
            **case,
            "elapsed_ms": 0,
            "row_count": 0,
            "score": 0,
            "grade": "weak",
            "checks": {"exception": False},
            "first_company": "",
            "first_review_score": "",
            "first_match_rank_score": "",
            "first_contract_review_types": "",
            "first_budget_review_hint": "",
            "item_policy_summary": {},
            "policy_preference_summary": {},
            "search_plan": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = [r["elapsed_ms"] for r in records if r["elapsed_ms"]]
    scores = [float(r["score"]) for r in records]
    fail_counts: dict[str, int] = {}
    tag_stats: dict[str, dict[str, Any]] = {}
    kind_stats: dict[str, dict[str, Any]] = {}
    for r in records:
        for k, v in (r.get("checks") or {}).items():
            if not v:
                fail_counts[k] = fail_counts.get(k, 0) + 1
        for tag in r.get("tags") or ["untagged"]:
            stat = tag_stats.setdefault(tag, {"count": 0, "scores": [], "weak": 0})
            stat["count"] += 1
            stat["scores"].append(float(r["score"]))
            if r["grade"] != "good":
                stat["weak"] += 1
        kind = r.get("kind") or "unknown"
        kstat = kind_stats.setdefault(kind, {"count": 0, "scores": [], "weak": 0})
        kstat["count"] += 1
        kstat["scores"].append(float(r["score"]))
        if r["grade"] != "good":
            kstat["weak"] += 1
    return {
        "total": len(records),
        "grade_counts": {g: sum(1 for r in records if r["grade"] == g) for g in ["good", "needs_review", "weak"]},
        "average_score": round(statistics.mean(scores), 2) if scores else 0,
        "median_score": round(statistics.median(scores), 2) if scores else 0,
        "average_latency_ms": round(statistics.mean(latencies), 1) if latencies else 0,
        "p95_latency_ms": sorted(latencies)[int(len(latencies) * 0.95) - 1] if latencies else 0,
        "max_latency_ms": max(latencies) if latencies else 0,
        "fail_counts": dict(sorted(fail_counts.items(), key=lambda x: (-x[1], x[0]))),
        "tag_stats": {
            k: {"count": v["count"], "avg_score": round(statistics.mean(v["scores"]), 2), "weak": v["weak"]}
            for k, v in sorted(tag_stats.items())
        },
        "kind_stats": {
            k: {"count": v["count"], "avg_score": round(statistics.mean(v["scores"]), 2), "weak": v["weak"]}
            for k, v in sorted(kind_stats.items())
        },
    }


def write_report(path: Path, records: list[dict[str, Any]], summary: dict[str, Any], base_url: str) -> None:
    lines = [
        "# 업체추천 Q&A 1,000건 평가",
        "",
        f"- generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"- base_url: {base_url}",
        f"- total_cases: {summary['total']}",
        f"- grade_counts: {summary['grade_counts']}",
        f"- average_score: {summary['average_score']}",
        f"- median_score: {summary['median_score']}",
        f"- average_latency_ms: {summary['average_latency_ms']}",
        f"- p95_latency_ms: {summary['p95_latency_ms']}",
        f"- max_latency_ms: {summary['max_latency_ms']}",
        "",
        "## 실패 체크 집계",
        "",
        "| check | count |",
        "|---|---:|",
    ]
    for k, v in summary["fail_counts"].items():
        lines.append(f"| {k} | {v} |")
    lines.extend(["", "## 유형별 집계", "", "| type | count | avg_score | weak_or_review |", "|---|---:|---:|---:|"])
    for k, v in summary["kind_stats"].items():
        lines.append(f"| {k} | {v['count']} | {v['avg_score']} | {v['weak']} |")
    lines.extend(["", "## 태그별 집계", "", "| tag | count | avg_score | weak_or_review |", "|---|---:|---:|---:|"])
    for k, v in summary["tag_stats"].items():
        lines.append(f"| {k} | {v['count']} | {v['avg_score']} | {v['weak']} |")
    lines.extend(["", "## 낮은 점수 사례 Top 80", "", "| id | score | kind | tags | q | first_company | failed_checks |", "|---|---:|---|---|---|---|---|"])
    for r in sorted(records, key=lambda x: (float(x["score"]), x["elapsed_ms"]))[:80]:
        failed = ",".join(k for k, v in (r.get("checks") or {}).items() if not v)
        q = str(r["q"]).replace("|", "/")
        first = str(r.get("first_company") or "").replace("|", "/")
        lines.append(f"| {r['id']} | {r['score']} | {r['kind']} | {','.join(r.get('tags') or [])} | {q} | {first} | {failed} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8001")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout-sec", type=float, default=20)
    parser.add_argument("--max-latency-ms", type=int, default=5000)
    parser.add_argument("--out-dir", default="artifacts/vendor_recommendation_quality_qa")
    args = parser.parse_args()

    cases = make_cases(args.count)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    stem = f"vendor_recommendation_1000_qa_{stamp}"
    records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(args.workers, 16))) as executor:
        futures = {
            executor.submit(evaluate_case, args.base_url, case, args.timeout_sec, args.max_latency_ms): idx
            for idx, case in enumerate(cases)
        }
        by_index: dict[int, dict[str, Any]] = {}
        for future in as_completed(futures):
            idx = futures[future]
            by_index[idx] = future.result()
        records = [by_index[i] for i in range(len(cases))]

    summary = summarize(records)
    jsonl_path = out_dir / f"{stem}.jsonl"
    summary_path = out_dir / f"{stem}.summary.json"
    report_path = out_dir / f"{stem}.md"
    jsonl_path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n", encoding="utf-8")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(report_path, records, summary, args.base_url)
    print(json.dumps({"jsonl": str(jsonl_path), "summary": str(summary_path), "report": str(report_path), **summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
