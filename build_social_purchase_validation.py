import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, "/opt/busan")
from core_calc import (  # noqa: E402
    dedup_by_dcsn,
    filter_cnstwk_by_site,
    filter_servc_by_site,
    filter_shopping_by_site,
    load_award_sets,
    load_bid_dict,
    parse_corp_shares,
    process_contract_row,
)

DB_PROC = "/opt/busan/procurement_contracts.db"
DB_AG = "/opt/busan/busan_agencies_master.db"
DB_CHATBOT = "/opt/busan/chatbot_company.db"
API_CACHE = "/opt/busan/api_cache.json"
OUT_JSON = "/opt/busan/social_purchase_cache.json"
OUT_XLSX = "/opt/busan/social_purchase_cache.xlsx"
PERIOD_START = "2026-01-01"
PERIOD_END = "2026-12-31"

COMPARE_UNITS = {
    "게임물관리위원회": "게임물관리위원회",
    "국립수산물품질관리원": "국립수산물품질관리원",
    "국립해양조사원": "국립해양조사원",
    "영상물등급위원회": "영상물등급위원회",
    "영화진흥위원회": "영화진흥위원회",
    "주택도시보증공사": "주택도시보증공사",
    "한국남부발전": "한국남부발전",
    "한국예탁결제원": "한국예탁결제원",
    "한국자산관리공사": "한국자산관리공사",
    "한국주택금융공사": "한국주택금융공사",
    "한국청소년상담복지개발원": "한국청소년상담복지개발원",
    "한국해양과학기술원": "한국해양과학기술원",
    "한국해양수산개발원": "한국해양수산개발원",
}

DETAIL_TARGETS = {
    "해양수산부 본부": {"해양수산부 본청"},
    "동남지방데이터청": {"동남지방데이터청"},
    "부산지방국세청": {"부산지방국세청"},
    "부산지방국토관리청": {"부산지방국토관리청"},
    "부산지방보훈청": {"부산지방보훈청"},
    "부산지방식약청": {"부산지방식품의약품안전청"},
    "부산지방해양수산청": {"부산지방해양수산청", "부산지방해운항만청"},
    "부산지방경찰청": {"부산광역시경찰청", "부산광역시지방경찰청"},
}

EXACT_CODE_TARGETS = {"부산항만공사": {"B551220"}}


def month_of(value):
    s = str(value or "").strip()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 6:
        return f"{digits[:4]}-{digits[4:6]}"
    return ""


def date_key(value):
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def in_period(value):
    key = date_key(value)
    if not key:
        return False
    return PERIOD_START.replace("-", "") <= key <= PERIOD_END.replace("-", "")


def filter_period(df, date_col):
    if date_col not in df.columns:
        return df.iloc[0:0].copy(), len(df)
    mask = df[date_col].map(in_period)
    return df[mask].copy(), int((~mask).sum())


def date_text(value):
    s = str(value or "").strip()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return s[:10]


def pct(num, den):
    return round(num / den * 100, 4) if den else 0.0


def load_agencies():
    conn = sqlite3.connect(DB_AG)
    master = pd.read_sql(
        """
        SELECT dminsttCd, dminsttNm, cate_lrg, cate_mid, cate_sml, cate_dtl, compare_unit
        FROM agency_master
        """,
        conn,
    )
    conn.close()
    master["dminsttCd"] = master["dminsttCd"].astype(str).str.strip()
    inst_dict = master.set_index("dminsttCd")[["cate_lrg", "cate_mid", "cate_sml"]].to_dict("index")

    code_to_label = {}
    for label, unit in COMPARE_UNITS.items():
        for _, row in master[master["compare_unit"] == unit].iterrows():
            if row["cate_lrg"] == "정부 및 국가공공기관":
                code_to_label[str(row["dminsttCd"]).strip()] = label
    for label, details in DETAIL_TARGETS.items():
        for _, row in master[master["cate_dtl"].isin(details)].iterrows():
            if row["cate_lrg"] == "정부 및 국가공공기관":
                code_to_label[str(row["dminsttCd"]).strip()] = label
    for label, codes in EXACT_CODE_TARGETS.items():
        for code in codes:
            code_to_label[code] = label

    target_rows = []
    for _, row in master.iterrows():
        code = str(row["dminsttCd"]).strip()
        if code in code_to_label:
            d = row.to_dict()
            d["target_label"] = code_to_label[code]
            target_rows.append(d)
    return inst_dict, code_to_label, target_rows


def load_social_biznos():
    conn = sqlite3.connect(DB_CHATBOT)
    conn.row_factory = sqlite3.Row
    policy = {
        str(r["bizno"]).replace("-", "").strip()
        for r in conn.execute(
            """
            SELECT DISTINCT ci.canonical_business_no AS bizno
            FROM policy_company_certification p
            JOIN company_identity ci ON ci.company_internal_id = p.company_internal_id
            WHERE p.policy_subtype='social_enterprise'
              AND p.validity_status='valid'
              AND ci.canonical_business_no IS NOT NULL
              AND ci.canonical_business_no <> ''
            """
        )
        if r["bizno"]
    }
    source = {
        str(r["bizno"]).replace("-", "").strip()
        for r in conn.execute(
            """
            SELECT DISTINCT canonical_business_no AS bizno
            FROM social_enterprise_master
            WHERE record_status='active'
              AND (region IN ('부산','부산광역시') OR matched_is_busan_company=1)
              AND canonical_business_no IS NOT NULL
              AND canonical_business_no <> ''
            """
        )
        if r["bizno"]
    }
    conn.close()
    return policy | source, policy, source


def social_vendors_from_corp_list(corp_list, social_biznos):
    vendors = []
    for chunk in str(corp_list or "").split("[")[1:]:
        parts = chunk.split("]")[0].split("^")
        if len(parts) < 10:
            continue
        bno = str(parts[9]).replace("-", "").strip()
        if bno not in social_biznos:
            continue
        name = parts[3].strip() if len(parts) > 3 else ""
        try:
            share = float(parts[6]) if parts[6].strip() else 0.0
        except Exception:
            share = 0.0
        vendors.append({"bizno": bno, "name": name, "share": share})
    if vendors:
        total_share = sum(v["share"] for v in vendors)
        if total_share == 0:
            share = 100.0 / len(vendors)
            for v in vendors:
                v["share"] = share
    return vendors


def add_totals(container, label, month, sector, total, social):
    key = (label, month)
    container[key][f"{sector}_total"] += total
    container[key][f"{sector}_social"] += social
    container[key][f"{sector}_count"] += 1
    if social > 0:
        container[key][f"{sector}_social_count"] += 1


def calculate():
    inst_dict, code_to_label, target_rows = load_agencies()
    social_biznos, policy_biznos, source_biznos = load_social_biznos()
    conn = sqlite3.connect(DB_PROC)
    bid_dict, bid_df = load_bid_dict(conn)
    award_sets = load_award_sets(conn)

    totals = defaultdict(lambda: defaultdict(float))
    records = []
    excluded = defaultdict(int)

    def accept(cd):
        return code_to_label.get(str(cd or "").strip())

    def add_record(label, month, sector, total, social, row, name_col, date_col, vendors):
        add_totals(totals, "전체", month, sector, total, social)
        add_totals(totals, label, month, sector, total, social)
        if social <= 0:
            return
        records.append(
            {
                "기관": label,
                "월": month,
                "분야": sector,
                "계약일": date_text(row.get(date_col, "")),
                "계약명": str(row.get(name_col, "") or ""),
                "계약액": round(total),
                "사회적기업수주액": round(social),
                "사회적기업업체": ", ".join(v["name"] for v in vendors if v.get("name")),
                "사회적기업사업자번호": ", ".join(v["bizno"] for v in vendors if v.get("bizno")),
                "사회적기업지분율": ", ".join(str(round(v["share"], 2)) for v in vendors),
            }
        )

    # Construction: numerator only, but the same Busan-site filter is applied.
    df = pd.read_sql(
        """
        SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
               corpList, ntceNo, dminsttList, cnstwkNm, cntrctInsttOfclTelNo,
               cnstrtsiteRgnNm, cntrctCnclsDate
        FROM cnstwk_cntrct
        """,
        conn,
    )
    df, n_period_drop = filter_period(df, "cntrctCnclsDate")
    excluded["공사_기간외배제"] = int(n_period_drop)
    df.drop_duplicates(subset=["untyCntrctNo"], keep="last", inplace=True)
    df = dedup_by_dcsn(df)
    df, n_site_drop, _ = filter_cnstwk_by_site(df, bid_df)
    excluded["공사_현장배제"] = int(n_site_drop)
    for _, row in df.iterrows():
        res = process_contract_row(row, inst_dict, set(), use_location_filter=True, bid_dict=bid_dict, award_set=award_sets["공사"])
        if not res:
            excluded["공사_process_excluded"] += 1
            continue
        cd, total, _ = res
        label = accept(cd)
        if not label:
            continue
        vendors = social_vendors_from_corp_list(row.get("corpList", ""), social_biznos)
        social = sum(total * v["share"] / 100.0 for v in vendors)
        add_record(label, month_of(row.get("cntrctCnclsDate")), "공사", total, social, row, "cnstwkNm", "cntrctCnclsDate", vendors)

    for table, sector in [("servc_cntrct", "용역"), ("thng_cntrct", "물품")]:
        extra = ", cnstrtsiteRgnNm" if table == "servc_cntrct" else ""
        df = pd.read_sql(
            f"""
            SELECT untyCntrctNo, dcsnCntrctNo, cntrctInsttCd, totCntrctAmt, thtmCntrctAmt,
                   corpList, ntceNo, dminsttList, cntrctNm, cntrctInsttOfclTelNo,
                   cntrctCnclsDate{extra}
            FROM {table}
            """,
            conn,
        )
        df, n_period_drop = filter_period(df, "cntrctCnclsDate")
        excluded[f"{sector}_기간외배제"] = int(n_period_drop)
        df.drop_duplicates(subset=["untyCntrctNo"], keep="last", inplace=True)
        df = dedup_by_dcsn(df)
        if table == "servc_cntrct":
            df, n_site_drop, _ = filter_servc_by_site(df, inst_dict)
            excluded["용역_현장배제"] = int(n_site_drop)
        for _, row in df.iterrows():
            res = process_contract_row(row, inst_dict, set(), use_location_filter=True, bid_dict=bid_dict, award_set=award_sets[sector])
            if not res:
                excluded[f"{sector}_process_excluded"] += 1
                continue
            cd, total, _ = res
            label = accept(cd)
            if not label:
                continue
            vendors = social_vendors_from_corp_list(row.get("corpList", ""), social_biznos)
            social = sum(total * v["share"] / 100.0 for v in vendors)
            add_record(label, month_of(row.get("cntrctCnclsDate")), sector, total, social, row, "cntrctNm", "cntrctCnclsDate", vendors)

    df = pd.read_sql(
        """
        SELECT dlvrReqNo, dlvrReqChgOrd, prdctSno, dminsttCd, prdctAmt,
               cntrctCorpBizno, corpNm, dlvrReqNm, cnstwkMtrlDrctPurchsObjYn, dlvrReqRcptDate
        FROM shopping_cntrct
        """,
        conn,
    )
    df, n_period_drop = filter_period(df, "dlvrReqRcptDate")
    excluded["쇼핑몰_기간외배제"] = int(n_period_drop)
    df["dlvrReqChgOrd"] = pd.to_numeric(df["dlvrReqChgOrd"], errors="coerce").fillna(0)
    df.sort_values("dlvrReqChgOrd", ascending=False, inplace=True)
    df.drop_duplicates(subset=["dlvrReqNo", "prdctSno"], keep="first", inplace=True)
    df, n_site_drop, _ = filter_shopping_by_site(df, conn, set(inst_dict.keys()), inst_dict=inst_dict)
    excluded["쇼핑몰_현장배제"] = int(n_site_drop)
    for _, row in df.iterrows():
        res = process_contract_row(row, inst_dict, set(), is_shopping=True)
        if not res:
            excluded["쇼핑몰_process_excluded"] += 1
            continue
        cd, total, _ = res
        label = accept(cd)
        if not label:
            continue
        bno = str(row.get("cntrctCorpBizno", "") or "").replace("-", "").strip()
        vendors = []
        if bno in social_biznos:
            vendors = [{"bizno": bno, "name": str(row.get("corpNm", "") or ""), "share": 100.0}]
        social = total if vendors else 0.0
        add_record(label, month_of(row.get("dlvrReqRcptDate")), "쇼핑몰", total, social, row, "dlvrReqNm", "dlvrReqRcptDate", vendors)

    conn.close()

    row_keys = sorted(totals.keys(), key=lambda x: (x[0] != "전체", x[0], x[1]))
    monthly_rows = []
    agency_monthly_rows = []
    for label, month in row_keys:
        d = totals[(label, month)]
        denom = d["용역_total"] + d["물품_total"] + d["쇼핑몰_total"]
        denom_no_shop = d["용역_total"] + d["물품_total"]
        social = d["공사_social"] + d["용역_social"] + d["물품_social"] + d["쇼핑몰_social"]
        row = {
            "기관": label,
            "월": month,
            "모수_물품용역쇼핑몰": round(denom),
            "모수_물품용역": round(denom_no_shop),
            "사회적기업수주액": round(social),
            "수주율_쇼핑몰포함": pct(social, denom),
            "수주율_쇼핑몰제외": pct(social, denom_no_shop),
            "공사_사회적기업수주액": round(d["공사_social"]),
            "용역_발주액": round(d["용역_total"]),
            "용역_사회적기업수주액": round(d["용역_social"]),
            "물품_발주액": round(d["물품_total"]),
            "물품_사회적기업수주액": round(d["물품_social"]),
            "쇼핑몰_발주액": round(d["쇼핑몰_total"]),
            "쇼핑몰_사회적기업수주액": round(d["쇼핑몰_social"]),
        }
        if label == "전체":
            monthly_rows.append(row)
        else:
            agency_monthly_rows.append(row)

    def aggregate(label):
        result = defaultdict(float)
        for (lbl, _month), d in totals.items():
            if lbl != label:
                continue
            for k, v in d.items():
                result[k] += v
        denom = result["용역_total"] + result["물품_total"] + result["쇼핑몰_total"]
        denom_no_shop = result["용역_total"] + result["물품_total"]
        social = result["공사_social"] + result["용역_social"] + result["물품_social"] + result["쇼핑몰_social"]
        return {
            "기관": label,
            "모수_물품용역쇼핑몰": round(denom),
            "모수_물품용역": round(denom_no_shop),
            "사회적기업수주액": round(social),
            "수주율_쇼핑몰포함": pct(social, denom),
            "수주율_쇼핑몰제외": pct(social, denom_no_shop),
            "공사_사회적기업수주액": round(result["공사_social"]),
            "용역_발주액": round(result["용역_total"]),
            "용역_사회적기업수주액": round(result["용역_social"]),
            "물품_발주액": round(result["물품_total"]),
            "물품_사회적기업수주액": round(result["물품_social"]),
            "쇼핑몰_발주액": round(result["쇼핑몰_total"]),
            "쇼핑몰_사회적기업수주액": round(result["쇼핑몰_social"]),
        }

    labels = ["전체"] + sorted({r["target_label"] for r in target_rows})
    agency_rows = [aggregate(label) for label in labels]

    with open(API_CACHE, "r", encoding="utf-8") as f:
        cache = json.load(f)
    summary = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cache_generated_at": cache.get("generated_at"),
        "cache_period": cache.get("데이터_기간"),
        "validation_period": f"{PERIOD_START} ~ {PERIOD_END}",
        "target_code_count": len(code_to_label),
        "target_agency_count": len(set(code_to_label.values())),
        "social_bizno_union_count": len(social_biznos),
        "social_policy_bizno_count": len(policy_biznos),
        "social_source_busan_or_master_busan_count": len(source_biznos),
        "formula": "분모=대상기관 부산 현장/소비처 물품+용역+쇼핑몰, 분자=대상기관 부산 현장/소비처 공사+용역+물품+쇼핑몰 사회적기업 수주액",
        "excluded_counts": dict(excluded),
    }
    out = {
        "summary": summary,
        "overall": agency_rows[0],
        "monthly": monthly_rows,
        "agency_rates": agency_rows[1:],
        "agency_monthly": agency_monthly_rows,
        "social_contracts": records,
        "target_agency_codes": target_rows,
    }
    Path(OUT_JSON).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        pd.DataFrame([summary, agency_rows[0]]).to_excel(writer, sheet_name="summary", index=False)
        pd.DataFrame(monthly_rows).to_excel(writer, sheet_name="monthly_overall", index=False)
        pd.DataFrame(agency_rows[1:]).to_excel(writer, sheet_name="agency_rates", index=False)
        pd.DataFrame(agency_monthly_rows).to_excel(writer, sheet_name="agency_monthly", index=False)
        pd.DataFrame(records).to_excel(writer, sheet_name="social_contracts", index=False)
        pd.DataFrame(target_rows).to_excel(writer, sheet_name="target_agency_codes", index=False)

    print(json.dumps({
        "json": OUT_JSON,
        "xlsx": OUT_XLSX,
        "summary": summary,
        "overall": agency_rows[0],
        "monthly_count": len(monthly_rows),
        "agency_rate_count": len(agency_rows) - 1,
        "agency_monthly_count": len(agency_monthly_rows),
        "social_contract_count": len(records),
        "top_agencies_by_social": sorted(agency_rows[1:], key=lambda r: r["사회적기업수주액"], reverse=True)[:10],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    calculate()
