"""
부산 공공계약 모니터링 대시보드
============================
Streamlit 기반 시각화 대시보드 — DashLite 스타일.
API 서버에서 데이터를 받아 사이드바 네비게이션으로 표시.

실행: streamlit run dashboard.py
"""
import streamlit as st
import requests
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
import base64, os, sqlite3

# ─── 설정 ───
# 로컬 테스트용 (서버 배포 시 서버 IP로 변경)
# API_BASE = "http://49.50.133.160:8000"
API_BASE = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="부산 공공계약 모니터링",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── DashLite 테마 색상 ───
COLORS = {
    "primary": "#6576ff",
    "primary_light": "#8091ff",
    "primary_pale": "rgba(101,118,255,0.1)",
    "secondary": "#364a63",
    "success": "#1ee0ac",
    "success_pale": "rgba(30,224,172,0.15)",
    "warning": "#f4bd0e",
    "warning_pale": "rgba(244,189,14,0.15)",
    "danger": "#e85347",
    "danger_pale": "rgba(232,83,71,0.15)",
    "info": "#09c2de",
    "info_pale": "rgba(9,194,222,0.15)",
    "bg": "#f5f6fa",
    "card_bg": "#ffffff",
    "card_border": "#dbdfea",
    "text_dark": "#364a63",
    "text_body": "#526484",
    "text_light": "#8094ae",
    "sidebar_bg": "#1c2b46",
    "sidebar_text": "#b7c2d0",
    "sidebar_active": "#6576ff",
}

# 분야별 색상
SECTOR_COLORS = {
    "공사": "#6576ff",
    "용역": "#8B5CF6",
    "물품": "#1ee0ac",
    "쇼핑몰": "#f4bd0e",
}

GROUP_COLORS = {
    "부산시 및 소관기관": "#09c2de",
    "정부 및 국가공공기관": "#e85347",
}


# ─── DashLite 스타일 CSS ───
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito+Sans:wght@400;600;700;800;900&display=swap');
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard-dynamic-subset.css');

/* 전체 폰트 & 배경 */
html, body, [class*="css"] {{
    font-family: 'Nunito Sans', 'Pretendard', -apple-system, sans-serif !important;
}}
.stApp {{
    background: {COLORS['bg']};
}}

/* ─── 사이드바 (DashLite Dark Navy) ─── */
section[data-testid="stSidebar"] {{
    background: linear-gradient(180deg, {COLORS['sidebar_bg']} 0%, #101924 100%);
    border-right: none;
    min-width: 260px !important;
    max-width: 260px !important;
}}
section[data-testid="stSidebar"] * {{
    color: {COLORS['sidebar_text']} !important;
}}
section[data-testid="stSidebar"] .stRadio label {{
    padding: 10px 20px !important;
    border-radius: 6px !important;
    margin: 2px 8px !important;
    transition: all 0.2s ease !important;
    font-weight: 600 !important;
    font-size: 0.95rem !important;
}}
section[data-testid="stSidebar"] .stRadio label:hover {{
    background: rgba(101,118,255,0.12) !important;
    color: #fff !important;
}}
section[data-testid="stSidebar"] .stRadio [data-checked="true"] + label,
section[data-testid="stSidebar"] .stRadio label[data-checked="true"] {{
    background: rgba(101,118,255,0.18) !important;
    color: #fff !important;
}}
section[data-testid="stSidebar"] .stRadio > div {{
    gap: 0px !important;
}}
section[data-testid="stSidebar"] .stRadio > div > label > div:first-child {{
    display: none !important;
}}
/* 사이드바 제목 */
section[data-testid="stSidebar"] h1, 
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {{
    color: #fff !important;
}}

/* ─── 카드 스타일 ─── */
div[data-testid="stMetric"] {{
    background: {COLORS['card_bg']};
    border: 1px solid {COLORS['card_border']};
    border-radius: 4px;
    padding: 20px 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    transition: box-shadow 0.2s ease;
}}
div[data-testid="stMetric"]:hover {{
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    transform: none;
    border-color: {COLORS['primary']};
}}
div[data-testid="stMetric"] label {{
    color: {COLORS['text_light']} !important;
    font-size: 0.85rem !important;
    font-weight: 700 !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}}
div[data-testid="stMetric"] div[data-testid="stMetricValue"] {{
    color: {COLORS['text_dark']} !important;
    font-size: 2rem !important;
    font-weight: 800 !important;
}}

/* ─── 제목 색상 ─── */
h1 {{ color: {COLORS['text_dark']} !important; font-weight: 800 !important; }}
h2 {{ color: {COLORS['text_dark']} !important; font-weight: 700 !important; font-size: 1.3rem !important; }}
h3 {{ color: {COLORS['secondary']} !important; font-weight: 700 !important; font-size: 1.1rem !important; }}
p, span {{ color: {COLORS['text_body']}; }}

/* ─── 데이터프레임 ─── */
.stDataFrame {{
    border-radius: 4px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    border: 1px solid {COLORS['card_border']};
}}

/* ─── 히어로 카드 + 웨이브 간격 제거 ─── */
[data-testid="column"]:first-child [data-testid="stMarkdownContainer"] + [data-testid="stPlotlyChart"] {{
    margin-top: -16px;
}}

/* ─── 검색 입력 ─── */
.stTextInput input {{
    border: 1px solid {COLORS['card_border']} !important;
    border-radius: 4px !important;
    padding: 10px 16px !important;
    font-size: 0.95rem !important;
    transition: border-color 0.2s !important;
}}
.stTextInput input:focus {{
    border-color: {COLORS['primary']} !important;
    box-shadow: 0 0 0 3px {COLORS['primary_pale']} !important;
}}

/* ─── 셀렉트박스 ─── */
.stSelectbox > div > div {{
    border-radius: 4px !important;
    border-color: {COLORS['card_border']} !important;
}}

/* ─── 구분선 ─── */
hr {{
    border-color: {COLORS['card_border']} !important;
    opacity: 0.5;
}}

/* ─── 링크 ─── */
a {{ color: {COLORS['primary']}; text-decoration: none; }}
a:hover {{ color: {COLORS['primary_light']}; text-decoration: underline; }}

/* 스크롤바 */
::-webkit-scrollbar {{ width: 6px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: #c4c9d4; border-radius: 3px; }}

/* 태블릿 반응형 (768px ~ 1024px) */
@media (max-width: 1024px) {{
    .main .block-container {{ padding: 1rem 1.5rem !important; max-width: 100% !important; }}
    h1 {{ font-size: 1.5rem !important; }}
    h2 {{ font-size: 1.2rem !important; }}
    h3 {{ font-size: 1rem !important; }}
    .stMetric {{ font-size: 0.85rem; }}
    [data-testid="stHorizontalBlock"] {{ flex-wrap: wrap !important; gap: 0.5rem !important; }}
    [data-testid="stHorizontalBlock"] > div {{ min-width: 45% !important; }}
    [data-testid="stDataFrame"] {{ font-size: 0.8rem !important; }}
}}

/* 소형 태블릿 / 모바일 (768px 이하) */
@media (max-width: 768px) {{
    .main .block-container {{ padding: 0.5rem 0.8rem !important; }}
    h1 {{ font-size: 1.3rem !important; }}
    h2 {{ font-size: 1.05rem !important; }}
    h3 {{ font-size: 0.9rem !important; }}
    [data-testid="stHorizontalBlock"] > div {{ min-width: 100% !important; flex-basis: 100% !important; }}
    [data-testid="stDataFrame"] {{ font-size: 0.72rem !important; }}
    .stTabs [data-baseweb="tab"] {{ font-size: 0.78rem !important; padding: 8px 12px !important; }}
}}
</style>
""", unsafe_allow_html=True)


# ─── API 호출 헬퍼 ───
@st.cache_data(ttl=300)
def fetch_api(endpoint):
    try:
        r = requests.get(f"{API_BASE}{endpoint}", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API 연결 실패: {e}")
        return None


def format_억(amt):
    if amt is None or amt == 0:
        return "0"
    abs_amt = abs(amt)
    if abs_amt >= 1e8:
        return f"{amt / 1e8:,.0f}억"
    elif abs_amt >= 1e4:
        return f"{amt / 1e4:,.0f}만원"
    else:
        return f"{amt:,.0f}원"


def format_조(amt):
    if amt is None or amt == 0:
        return "0"
    abs_amt = abs(amt)
    if abs_amt >= 1e12:
        return f"{amt / 1e12:.1f}조"
    elif abs_amt >= 1e8:
        return f"{amt / 1e8:,.0f}억"
    elif abs_amt >= 1e4:
        return f"{amt / 1e4:,.0f}만원"
    else:
        return f"{amt:,.0f}원"


def rate_color(rate):
    if rate >= 70: return COLORS["success"]
    elif rate >= 50: return COLORS["warning"]
    else: return COLORS["danger"]

def classify_agency(name):
    """기관명으로 기관 유형 분류"""
    if name.endswith("구") or name.endswith("군"):
        return "자치구"
    elif "교육" in name:
        return "교육"
    elif "경찰" in name:
        return "경찰"
    elif "소방" in name:
        return "소방"
    elif any(k in name for k in ["공단", "공사", "공단"]):
        return "산하기관"
    elif any(k in name for k in ["벡스코", "의료원", "연구원", "진흥원", "재단"]):
        return "공공기관"
    elif "본청" in name or "광역시" in name or "부산시" in name:
        return "본청"
    elif any(k in name for k in ["청", "원", "부", "처", "관리"]):
        return "정부기관"
    else:
        return "기관"


def get_base_group(k):
    return k.replace("부산광역시 및 소속기관", "부산시 및 소관기관").replace("부산시_지역제한", "부산시 및 소관기관_지역제한")


def format_group_display(k, for_plotly=False, for_html=False):
    grp = get_base_group(k)
    if "부산" in grp:
        label = grp.split("_")[0]
        sub = "(지방계약법)"
        if for_plotly: return f"{label}<br><span style='font-size:11px; color:{COLORS['text_light']};'>{sub}</span>"
        if for_html: return f"{label} <span style='font-size:0.75em; font-weight:normal; color:{COLORS['text_light']};'>{sub}</span>"
        return f"{label} {sub}"
    elif "국가" in grp or "정부" in grp:
        label = grp.split("_")[0]
        sub = "(국가계약법)"
        if for_plotly: return f"{label}<br><span style='font-size:11px; color:{COLORS['text_light']};'>{sub}</span>"
        if for_html: return f"{label} <span style='font-size:0.75em; font-weight:normal; color:{COLORS['text_light']};'>{sub}</span>"
        return f"{label} {sub}"
    return grp


def plotly_layout_base(height=380):
    """DashLite 스타일 Plotly 레이아웃 기본"""
    return dict(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color=COLORS["text_body"], family="Nunito Sans, Pretendard"),
        margin=dict(t=20, b=30, l=10, r=10),
        height=height,
        showlegend=False,
    )


def kpi_card(title, value, sub_text="", trend="", trend_label="vs. last week", icon="", bg_gradient=None):
    """DashLite 스타일 KPI 카드 HTML"""
    trend_html = ""
    if trend:
        trend_color = COLORS["success"] if "+" in trend or "↑" in trend else COLORS["danger"]
        trend_html = f"""
        <div style="text-align:right; margin-top:16px;">
            <span style="font-size:0.9rem; font-weight:700; color:{trend_color};">{trend}</span><br>
            <span style="font-size:0.75rem; color:rgba(255,255,255,0.5);">{trend_label}</span>
        </div>"""
    
    bg_style = f"background: linear-gradient(135deg, {bg_gradient[0]}, {bg_gradient[1]});" if bg_gradient else f"background: {COLORS['card_bg']}; border: 1px solid {COLORS['card_border']};"
    text_color = "#fff" if bg_gradient else COLORS["text_dark"]
    sub_color = "rgba(255,255,255,0.55)" if bg_gradient else COLORS["text_light"]
    title_color = "rgba(255,255,255,0.85)" if bg_gradient else COLORS["text_light"]
    
    return f"""
    <div style="{bg_style} border-radius: 4px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(0,0,0,0.12); height: 100%;">
        <div style="font-size:0.85rem; font-weight:600; color:{title_color}; margin-bottom:12px;">{title}</div>
        <div style="font-size:2.4rem; font-weight:800; color:{text_color}; line-height:1; margin-bottom:8px; font-family:'Nunito Sans',sans-serif;">{value}</div>
        <div style="font-size:0.8rem; color:{sub_color}; margin-bottom:4px;">{sub_text}</div>
        {trend_html}
    </div>
    """


def kpi_card_hero(title, main_value, sub_label, sub_value, trend, trend_label="vs. 전년 동기"):
    """DashLite Total Sales 스타일 — 다크 네이비 히어로 카드"""
    trend_color = COLORS["success"] if "+" in trend or "↑" in trend else COLORS["danger"]
    
    return f"""<div style="background: linear-gradient(135deg, #1c2b46 0%, #253d5b 100%); border-radius: 4px; padding: 28px; box-shadow: 0 4px 16px rgba(0,0,0,0.2); height: 100%;"><div style="font-size:0.9rem; font-weight:600; color:rgba(255,255,255,0.65); margin-bottom:14px;">{title}</div><div style="font-size:2.6rem; font-weight:800; color:#fff; line-height:1; margin-bottom:6px; font-family:Nunito Sans,sans-serif; letter-spacing:-0.02em;">{main_value}</div><div style="margin-top:20px; padding-top:16px; border-top:1px solid rgba(255,255,255,0.08);"><div style="font-size:0.8rem; color:rgba(255,255,255,0.45); margin-bottom:6px;">{sub_label}</div><div style="display:flex; justify-content:space-between; align-items:flex-end;"><div style="font-size:1.6rem; font-weight:700; color:rgba(255,255,255,0.9); font-family:Nunito Sans,sans-serif;">{sub_value}</div><div style="text-align:right;"><span style="font-size:0.9rem; font-weight:700; color:{trend_color};">{trend}</span><br><span style="font-size:0.72rem; color:rgba(255,255,255,0.4);">{trend_label}</span></div></div></div></div>"""


# ─── 사이드바 네비게이션 ───
with st.sidebar:
    # 로고/브랜드
    st.markdown(f"""
    <div style="padding: 20px 16px 10px; text-align: center; border-bottom: 1px solid rgba(255,255,255,0.08); margin-bottom: 16px;">
        <div style="font-size: 1.5rem; font-weight: 900; color: #fff; letter-spacing: -0.02em;">📊 부산이삽니다</div>
        <div style="font-size: 0.75rem; color: {COLORS['text_light']}; margin-top: 4px;">공공계약 모니터링 시스템</div>
    </div>
    """, unsafe_allow_html=True)
    
    # 네비게이션 메뉴
    st.markdown(f'<div style="font-size:0.7rem; font-weight:700; color:{COLORS["text_light"]}; padding:8px 20px; text-transform:uppercase; letter-spacing:0.1em;">메뉴</div>', unsafe_allow_html=True)
    
    page = st.radio(
        "nav",
        ["📊 종합현황", "🏆 기관별 순위", "🔍 기관별 실적 검색", "🔴 유출계약 분석", "🛡️ 지역업체 보호제도", "📝 수의계약", "🛒 종합쇼핑몰", "🏢 지역업체 정보"],
        label_visibility="collapsed",
    )
    
    st.markdown("<br>" * 3, unsafe_allow_html=True)
    
    # 하단 정보
    st.markdown(f"""
    <div style="padding: 16px; border-top: 1px solid rgba(255,255,255,0.08); margin-top: auto;">
        <div style="font-size: 0.7rem; color: {COLORS['text_light']};">
            API: <a href="{API_BASE}/docs" target="_blank" style="color: {COLORS['primary_light']};">Swagger UI</a><br>
            © 2026 부산광역시
        </div>
    </div>
    """, unsafe_allow_html=True)


# ─── 페이지 헤더 ───
page_titles = {
    "📊 종합현황": "종합현황",
    "🏆 기관별 순위": "기관별 지역업체 수주율 순위",
    "🔍 기관별 실적 검색": "기관별 수주현황 검색",
    "🔴 유출계약 분석": "지역외 유출계약 분석",
    "🛡️ 지역업체 보호제도": "지역업체 보호제도(지역제한경쟁 및 의무공동수급 적용여부)",
    "📝 수의계약": "수의계약 분석",
    "🏢 지역업체 정보": "지역업체 정보",
    "🛒 종합쇼핑몰": "종합쇼핑몰 지역외 유출현황",
}
st.markdown(f"""
<div style="background:linear-gradient(90deg, #1a2b6d 0%, #3b4ab8 50%, #1a2b6d 100%); padding:10px 24px; border-radius:6px; margin-bottom:16px; text-align:center;">
    <span style="font-size:1rem; font-weight:800; color:#fff; letter-spacing:0.08em;">발주는 부산기업으로! &nbsp;·&nbsp; 구매는 부산상품으로! &nbsp;·&nbsp; 우리가 살리는 부산경제!</span>
</div>
""", unsafe_allow_html=True)
st.markdown(f"""
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid {COLORS['card_border']};">
    <h1 style="margin:0; font-size:1.6rem;">{page_titles.get(page, "")}</h1>
    <div style="font-size:0.8rem; color:{COLORS['text_light']};">
        {datetime.now().strftime('%Y년 %m월 %d일')}
    </div>
</div>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════
# PAGE: 종합현황
# ════════════════════════════════════════════
if page == "📊 종합현황":
    data = fetch_api("/api/summary")
    if data:
        gen_at = data.get("generated_at", "")
        st.caption(f"📅 데이터 기간: {data.get('데이터_기간', '')} | 생성: {gen_at}")

        total = data.get("1_전체", {})
        발주액 = total.get("발주액", 0)
        수주액 = total.get("수주액", 0)
        수주율 = total.get("수주율", 0)
        
        # 분야별 발주액 (금액)
        분야별 = data.get("2_분야별", {})
        분야_items = list(분야별.items())
        # 순서: 공사, 용역, 물품, 쇼핑몰
        amt_공사 = 분야_items[0][1].get("발주액", 0) if len(분야_items) > 0 else 0
        amt_용역 = 분야_items[1][1].get("발주액", 0) if len(분야_items) > 1 else 0
        amt_물품 = 분야_items[2][1].get("발주액", 0) if len(분야_items) > 2 else 0
        amt_쇼핑 = 분야_items[3][1].get("발주액", 0) if len(분야_items) > 3 else 0
        
        sub_info = f"공사 {round(amt_공사/발주액*100,1) if 발주액>0 else 0}% · 용역 {round(amt_용역/발주액*100,1) if 발주액>0 else 0}% · 물품 {round(amt_물품/발주액*100,1) if 발주액>0 else 0}% · 쇼핑몰 {round(amt_쇼핑/발주액*100,1) if 발주액>0 else 0}%"
        
        # ── 수요기관 수 (DB에서 — 분류별) ──
        try:
            db_path = os.path.join(os.path.dirname(__file__), "busan_agencies_master.db")
            _conn = sqlite3.connect(db_path)
            n_부산 = _conn.execute(
                "SELECT COUNT(*) FROM agency_master WHERE cate_lrg LIKE '%부산%'"
            ).fetchone()[0]
            n_정부 = _conn.execute(
                "SELECT COUNT(*) FROM agency_master WHERE cate_lrg LIKE '%정부%'"
            ).fetchone()[0]
            n_기관 = n_부산 + n_정부
            _conn.close()
        except Exception:
            n_부산 = 0
            n_정부 = 0
            n_기관 = 0
        agency_label = f"부산광역시 수요기관 ({n_기관:,}개)" if n_기관 else "부산광역시 수요기관"
        
        # ── DashLite Total Sales 스타일 — 통합 히어로 카드 + 우측 분야별 ──
        with st.container(border=True):
            col_hero, col_side = st.columns([5, 5])
        
            with col_hero:
                sc = COLORS["success"]
                # 주간 수주율 증감 (누계기준: 7일 평균 누계 수주율 vs 현재)
                _weekly = data.get("13_주간데이터", {})
                _cum = _weekly.get("누계비교", {}).get("전체", {})
                _wk_change = _cum.get("증감", 0)
                _wk_arrow = "↑" if _wk_change >= 0 else "↓"
                _wk_color = COLORS["success"] if _wk_change >= 0 else COLORS["danger"]
                _wk_label = f"{_wk_arrow} {abs(_wk_change):.1f}%p"
                st.markdown(f"""<div style="background: linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius: 8px; padding: 20px 28px 20px; box-shadow: 0 4px 20px rgba(35,46,122,0.35);"><div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;"><span style="font-size:0.9rem; font-weight:700; color:rgba(255,255,255,0.85);">총 계약액</span><span style="font-size:0.78rem; color:rgba(255,255,255,0.55); font-weight:600;">{agency_label}</span></div><div style="font-size:2.4rem; font-weight:800; color:#fff; line-height:1; font-family:Nunito Sans,sans-serif; letter-spacing:-0.02em;">{format_조(발주액)}</div><div style="font-size:0.68rem; color:rgba(255,255,255,0.45); margin-top:6px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{sub_info}</div><div style="font-size:0.9rem; font-weight:700; color:rgba(255,255,255,0.85); margin-top:16px;">지역업체 수주액 (수주율)</div><div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:6px;"><div style="font-size:1.5rem; font-weight:700; color:rgba(255,255,255,0.92); font-family:Nunito Sans,sans-serif; line-height:1; letter-spacing:-0.02em;">{format_조(수주액)} <span style="color:{sc};">({수주율}%)</span></div><div style="text-align:right;"><span style="font-size:0.85rem; font-weight:700; color:{_wk_color};">{_wk_label}</span><br><span style="font-size:0.7rem; color:rgba(255,255,255,0.4);">vs. 지난주</span></div></div></div>""", unsafe_allow_html=True)
                
                # 웨이브 스파크라인 (Plotly 미니 area chart)
                import random
                random.seed(42)
                wave_y = [30 + 15 * __import__('math').sin(i * 0.5) + random.uniform(-5, 5) for i in range(30)]
                fig_wave = go.Figure()
                fig_wave.add_trace(go.Scatter(
                    y=wave_y, mode='lines', fill='tozeroy',
                    line=dict(color='rgba(255,255,255,0.25)', width=2, shape='spline'),
                    fillcolor='rgba(255,255,255,0.06)',
                ))
                fig_wave.update_layout(
                    plot_bgcolor='rgba(35,46,122,1)', paper_bgcolor='rgba(35,46,122,1)',
                    margin=dict(t=0, b=0, l=0, r=0), height=70,
                    showlegend=False,
                    xaxis=dict(visible=False), yaxis=dict(visible=False),
                )
                st.plotly_chart(fig_wave, use_container_width=True, config={"displayModeBar": False})
                
                # 이번주 부가가치 / 고용 기여도 (키 이름으로 안전 접근)
                econ_data = data.get("11_경제효과", {})
                econ_total = econ_data.get("전체", {})
                econ_meta = econ_data.get("계수", {})
                부가가치 = econ_total.get("지역생산부가가치", 0)
                고용기여 = econ_total.get("지역고용기여도_명", 0)
                수주액_total = econ_total.get("지역업체수주액", 0)
                
                # 이번주 수주액에서 이번주 부가가치/고용 계산 (키 이름으로 안전 접근)
                weekly_data = data.get("13_주간데이터", {})
                weekly_period = weekly_data.get("이번주_기간", "")
                부산_avg = econ_meta.get("부산_전산업평균", {})
                유발계수 = 부산_avg.get("부가가치유발계수", 0.467) if isinstance(부산_avg, dict) else 0.467
                취업유발 = 부산_avg.get("취업유발계수", 6.6) if isinstance(부산_avg, dict) else 6.6
                
                # 이번주 그룹별 수주액
                전체_주간 = weekly_data.get("전체", {})
                이번주_수주 = 전체_주간.get("이번주_수주액", 0) if isinstance(전체_주간, dict) else 0
                
                이번주_부가 = int(이번주_수주 * 유발계수) if 이번주_수주 else 0
                이번주_고용 = round(이번주_수주 / 1e9 * 취업유발, 1) if 이번주_수주 else 0
                
                if 부가가치 or 고용기여:
                    week_label = f"이번주 ({weekly_period})" if weekly_period else "이번주"
                    st.markdown(f'''<div style="display:flex; gap:16px; margin-top:-8px; margin-bottom:8px;">
<div style="flex:1; background:linear-gradient(135deg, #fef9ef 0%, #fdf0d5 100%); border:1px solid #f0d9a0; border-radius:6px; padding:14px 18px;">
<div style="display:flex; justify-content:space-between; align-items:flex-start;">
<div>
<div style="font-size:0.78rem; color:#8a6d3b; font-weight:600; white-space:nowrap;">지역총생산 부가가치증가</div>
<div style="font-size:1.35rem; font-weight:800; color:#5a4520; font-family:Nunito Sans,sans-serif;">{format_억(부가가치)}</div>
</div>
<div style="text-align:right;">
<div style="font-size:0.65rem; color:#8a6d3b;">{week_label}</div>
<div style="font-size:1rem; font-weight:700; color:#c08b30; font-family:Nunito Sans,sans-serif;">{format_억(이번주_부가)}</div>
</div>
</div>
</div>
<div style="flex:1; background:linear-gradient(135deg, #eef8f6 0%, #d4efea 100%); border:1px solid #a3d5cb; border-radius:6px; padding:14px 18px;">
<div style="display:flex; justify-content:space-between; align-items:flex-start;">
<div>
<div style="font-size:0.78rem; color:#2d7a6c; font-weight:600;">지역고용 기여도</div>
<div style="font-size:1.35rem; font-weight:800; color:#1a5248; font-family:Nunito Sans,sans-serif;">{고용기여:,.0f}명</div>
</div>
<div style="text-align:right;">
<div style="font-size:0.65rem; color:#2d7a6c;">{week_label}</div>
<div style="font-size:1rem; font-weight:700; color:#1a8a6e; font-family:Nunito Sans,sans-serif;">{이번주_고용:,.0f}명</div>
</div>
</div>
</div>
</div>
<div style="margin-top:6px; padding:8px 12px; background:transparent;">
<div style="font-size:0.65rem; font-weight:700; color:#364a63; margin-bottom:3px;">📊 산출근거 (추정치)</div>
<div style="font-size:0.58rem; color:#8094ae; line-height:1.7;">
• 부가가치 = 지역업체 수주액 × 부가가치유발계수(0.467)<br>
• 고용기여 = 지역업체 수주액(10억) × 취업유발계수(6.6명)<br>
<span style="color:#999; font-size:0.55rem;">※ 한국은행 2020년 지역산업연관표(2025년 발행) 부산 전산업 평균 계수 활용 추정치</span>
</div>
</div>''', unsafe_allow_html=True)
                
                # ── 수주율 변동 원인 분석 ──
                _wk_data = data.get("13_주간데이터", {})
                _wk_전체 = _wk_data.get("전체", {})
                _wk_증감 = _wk_전체.get("수주율_증감", 0) if isinstance(_wk_전체, dict) else 0
                _wk_기간 = _wk_data.get("이번주_기간", "")
                _leak_top = _wk_data.get("이번주_주요유출", [])
                _local_top = _wk_data.get("이번주_주요수주", [])
                
                if _leak_top or _local_top:
                    _chg_icon = "📉" if _wk_증감 < 0 else ("📈" if _wk_증감 > 0 else "➡️")
                    _chg_txt = f"수주율 {abs(_wk_증감):+.1f}%p" if _wk_증감 != 0 else "수주율 변동 없음"
                    with st.expander(f"{_chg_icon} 이번주({_wk_기간}) 수주율 변동 원인 분석 ({_chg_txt})", expanded=False):
                        # 하락 원인 (유출 Top 5)
                        if _leak_top:
                            _leak_title = "주요 유출계약 (수주율 하락 요인)"
                            _lk_hdr = f'<div style="display:flex; justify-content:space-between; align-items:center; padding:10px 14px; background:linear-gradient(135deg, #e85347 0%, #ff7b6b 100%); border-radius:6px 6px 0 0;"><span style="font-size:0.78rem; font-weight:700; color:#fff;">📉 {_leak_title}</span></div>'
                            _lk_col = f'<div style="display:flex; padding:5px 14px; border-bottom:1px solid #f0f1f5; background:#f8f9fc;"><div style="flex:0.4; font-size:0.6rem; color:#8094ae; font-weight:600;">분야</div><div style="flex:1; font-size:0.6rem; color:#8094ae; font-weight:600;">기관</div><div style="flex:1.8; font-size:0.6rem; color:#8094ae; font-weight:600;">계약명</div><div style="flex:1; font-size:0.6rem; color:#8094ae; font-weight:600;">수주업체</div><div style="flex:0.5; font-size:0.6rem; color:#8094ae; font-weight:600;">지역</div><div style="flex:0.6; text-align:right; font-size:0.6rem; color:#8094ae; font-weight:600;">유출액</div></div>'
                            _lk_rows = ""
                            for _li, _lk in enumerate(_leak_top):
                                _rbg = "#fafbfe" if _li % 2 == 1 else "#fff"
                                _fc = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}.get(_lk.get("분야",""), "#999")
                                _corp_nm = _lk.get("수주업체","") or "-"
                                _rgn = _lk.get("지역","") or "-"
                                _lk_rows += f'<div style="display:flex; align-items:center; padding:7px 14px; border-bottom:1px solid #f0f1f5; background:{_rbg};"><div style="flex:0.4;"><span style="background:{_fc}; color:#fff; padding:1px 6px; border-radius:8px; font-size:0.55rem; font-weight:600;">{_lk.get("분야","")}</span></div><div style="flex:1; font-size:0.68rem; font-weight:600; color:#364a63; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{_lk.get("기관","")}</div><div style="flex:1.8; font-size:0.65rem; color:#526484; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{_lk.get("계약명","")[:25]}</div><div style="flex:1; font-size:0.65rem; color:#364a63; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{_corp_nm}</div><div style="flex:0.5; font-size:0.65rem; color:#8094ae;">{_rgn}</div><div style="flex:0.6; text-align:right; font-size:0.7rem; font-weight:700; color:#e85347; font-family:Nunito Sans,sans-serif;">{format_억(_lk.get("유출액",0))}</div></div>'
                            st.markdown(f'<div style="background:#fff; border:1px solid #f0f1f5; border-radius:6px; overflow:hidden;">{_lk_hdr}{_lk_col}{_lk_rows}</div>', unsafe_allow_html=True)
                        
                        # 상승 원인 (수주 Top 5)
                        if _local_top:
                            _loc_title = "주요 지역수주 (수주율 상승 요인)"
                            _lo_hdr = f'<div style="display:flex; justify-content:space-between; align-items:center; padding:10px 14px; background:linear-gradient(135deg, #1ee0ac 0%, #59e8c4 100%); border-radius:6px 6px 0 0;"><span style="font-size:0.78rem; font-weight:700; color:#fff;">📈 {_loc_title}</span></div>'
                            _lo_col = f'<div style="display:flex; padding:5px 14px; border-bottom:1px solid #f0f1f5; background:#f8f9fc;"><div style="flex:0.4; font-size:0.6rem; color:#8094ae; font-weight:600;">분야</div><div style="flex:1; font-size:0.6rem; color:#8094ae; font-weight:600;">기관</div><div style="flex:1.8; font-size:0.6rem; color:#8094ae; font-weight:600;">계약명</div><div style="flex:1; font-size:0.6rem; color:#8094ae; font-weight:600;">수주업체</div><div style="flex:0.5; font-size:0.6rem; color:#8094ae; font-weight:600;">지역</div><div style="flex:0.6; text-align:right; font-size:0.6rem; color:#8094ae; font-weight:600;">수주액</div></div>'
                            _lo_rows = ""
                            for _li, _lo in enumerate(_local_top):
                                _rbg = "#fafbfe" if _li % 2 == 1 else "#fff"
                                _fc = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}.get(_lo.get("분야",""), "#999")
                                _corp_nm = _lo.get("수주업체","") or "-"
                                _rgn = _lo.get("지역","") or "-"
                                _lo_rows += f'<div style="display:flex; align-items:center; padding:7px 14px; border-bottom:1px solid #f0f1f5; background:{_rbg};"><div style="flex:0.4;"><span style="background:{_fc}; color:#fff; padding:1px 6px; border-radius:8px; font-size:0.55rem; font-weight:600;">{_lo.get("분야","")}</span></div><div style="flex:1; font-size:0.68rem; font-weight:600; color:#364a63; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{_lo.get("기관","")}</div><div style="flex:1.8; font-size:0.65rem; color:#526484; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{_lo.get("계약명","")[:25]}</div><div style="flex:1; font-size:0.65rem; color:#364a63; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{_corp_nm}</div><div style="flex:0.5; font-size:0.65rem; color:#8094ae;">{_rgn}</div><div style="flex:0.6; text-align:right; font-size:0.7rem; font-weight:700; color:#1ee0ac; font-family:Nunito Sans,sans-serif;">{format_억(_lo.get("수주액",0))}</div></div>'
                            st.markdown(f'<div style="background:#fff; border:1px solid #f0f1f5; border-radius:6px; overflow:hidden; margin-top:8px;">{_lo_hdr}{_lo_col}{_lo_rows}</div>', unsafe_allow_html=True)
        
            with col_side:
                # 상단: 수요기관 분류 (인디고 배경 + 하얀 텍스트)
                if n_부산 or n_정부:
                    st.markdown(f"""<div style="background: linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius:6px; padding:14px 14px; box-shadow:0 2px 10px rgba(35,46,122,0.25); margin-bottom:6px;"><div style="display:flex; justify-content:space-between; align-items:center;"><div><div style="font-size:0.68rem; font-weight:600; color:rgba(255,255,255,0.65);">부산시 및 소관기관 <span style="font-size:0.58rem;">(지방계약법 적용)</span></div><div style="font-size:1.15rem; font-weight:800; color:#fff; font-family:Nunito Sans,sans-serif; margin-top:2px;">{n_부산:,}개 기관</div></div><div><div style="font-size:0.68rem; font-weight:600; color:rgba(255,255,255,0.65);">정부 및 국가공공기관 <span style="font-size:0.58rem;">(국가계약법 적용)</span></div><div style="font-size:1.15rem; font-weight:800; color:#fff; font-family:Nunito Sans,sans-serif; margin-top:2px;">{n_정부:,}개 기관</div></div></div></div>""", unsafe_allow_html=True)
                
                # 우측: 2x2 그리드 — DashLite Average Order 스타일
                sc = COLORS["success"]
                분야_데이터 = [
                    ("공사계약액", amt_공사, 분야_items[0][1] if len(분야_items) > 0 else {}),
                    ("용역계약액", amt_용역, 분야_items[1][1] if len(분야_items) > 1 else {}),
                    ("물품계약액", amt_물품, 분야_items[2][1] if len(분야_items) > 2 else {}),
                    ("종합쇼핑몰계약액", amt_쇼핑, 분야_items[3][1] if len(분야_items) > 3 else {}),
                ]
                dot_colors = ["#6576ff", "#1ee0ac", "#e85347", "#f4bd0e"]
                # 누계비교 데이터 기반 증감 값
                _cum_sectors = _weekly.get("누계비교", {})
                _sector_keys = ['공사', '용역', '물품', '쇼핑몰']
                trends = []
                trend_colors = []
                for _sk in _sector_keys:
                    _schg = _cum_sectors.get(_sk, {}).get('증감', 0)
                    _arr = '↑' if _schg >= 0 else '↓'
                    trends.append(f"{_arr} {abs(_schg):.1f}%p")
                    trend_colors.append(COLORS['success'] if _schg >= 0 else COLORS['danger'])
                bar_sets = [
                    [40, 55, 35, 60, 45, 70, 80],
                    [60, 45, 50, 35, 55, 40, 65],
                    [30, 50, 65, 45, 70, 55, 75],
                    [45, 35, 55, 40, 50, 60, 70],
                ]
                
                def _mini_card(idx):
                    name, amt, detail = 분야_데이터[idx]
                    수주 = detail.get("수주액", 0)
                    율 = detail.get("수주율", 0)
                    비중 = round(amt / 발주액 * 100, 1) if 발주액 > 0 else 0
                    tc = trend_colors[idx]
                    dc = dot_colors[idx]
                    bars = ""
                    for j, h in enumerate(bar_sets[idx]):
                        op = "0.3" if j < 6 else "1"
                        bars += f'<div style="width:6px; height:{h}%; background:{dc}; opacity:{op}; border-radius:1px;"></div>'
                    st.markdown(f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px; padding:22px 18px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
<div style="display:flex; justify-content:space-between; align-items:flex-start;">
<div style="flex:1;">
<div style="font-size:0.8rem; font-weight:700; color:{COLORS['text_dark']}; margin-bottom:8px;">{name}</div>
<div style="font-size:1.3rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif; line-height:1;">{format_억(amt)}</div>
<div style="margin-top:10px;">
<div style="font-size:0.65rem; font-weight:600; color:{COLORS['text_light']}; letter-spacing:0.03em;">지역업체 수주액</div>
<div style="font-size:1.05rem; font-weight:800; color:{COLORS['text_dark']}; margin-top:2px; font-family:Nunito Sans,sans-serif;">{format_억(수주)} <span style="color:{COLORS['primary']};">({율}%)</span></div>
</div>
</div>
<div style="text-align:right; display:flex; flex-direction:column; align-items:flex-end;">
<span style="width:8px; height:8px; border-radius:50%; background:{dc}; display:inline-block; margin-bottom:6px;"></span>
<div style="font-size:0.75rem; font-weight:700; color:{tc};">{trends[idx]}</div>
<div style="font-size:0.6rem; color:{COLORS['text_light']}; margin-top:1px;">vs. 지난주</div>
<div style="display:flex; align-items:flex-end; gap:2px; height:40px; margin-top:8px;">{bars}</div>
</div>
</div>
</div>""", unsafe_allow_html=True)
                
                
                # 1행: 공사, 용역
                r1c1, r1c2 = st.columns(2)
                with r1c1:
                    _mini_card(0)
                with r1c2:
                    _mini_card(1)
                
                st.markdown('<div style="margin-top:12px;"></div>', unsafe_allow_html=True)
                
                # 2행: 물품, 종합쇼핑몰
                r2c1, r2c2 = st.columns(2)
                with r2c1:
                    _mini_card(2)
                with r2c2:
                    _mini_card(3)
                
                st.markdown(f'''<div style="margin-top:10px; text-align:center; padding:8px 10px; background:#f8f9fc; border-radius:5px;">
<span style="font-size:0.58rem; color:#8094ae; font-weight:600;">📋 국가계약법 및 지방계약법 적용을 받는 국가·지자체·교육청·출자출연기관·공공기관·대학 등 부산지역 조달청 등록 수요기관의 전체 계약 데이터를 활용하여 수주액·수주율 산출</span>
</div>''', unsafe_allow_html=True)
                


        st.markdown('<div style="margin-top:20px;"></div>', unsafe_allow_html=True)
        
        # ── 부산광역시 수요기관 지역업체 수주현황 (지방계약법 적용) ──
        # 그룹별 데이터
        groups_data = data.get("3_그룹별", {})
        gxs = data.get("4_그룹별_분야별", {})
        부산_grp = groups_data.get("부산광역시 및 소속기관", {})
        부산_분야 = gxs.get("부산광역시 및 소속기관", {})
        
        부산_발주 = 부산_grp.get("발주액", 0)
        부산_수주 = 부산_grp.get("수주액", 0)
        부산_율 = 부산_grp.get("수주율", 0)
        부산_외지 = round(100 - 부산_율, 1) if 부산_율 else 0
        부산_외지액 = 부산_발주 - 부산_수주 if 부산_발주 > 부산_수주 else 0
        
        with st.container(border=True):
            col_left, col_right = st.columns(2)

            with col_left:
                st.markdown(f"""<div style="padding:20px 0 8px;"><h2 style="margin:0; font-size:1.6rem; font-weight:700; color:{COLORS['text_dark']};">부산광역시 수요기관 지역업체 수주현황</h2><span style="font-size:0.75rem; color:{COLORS['text_light']};">(지방계약법 적용)</span></div>""", unsafe_allow_html=True)
                
                dc1, dc2 = st.columns([3, 4])
                with dc1:
                    fig_donut1 = go.Figure(go.Pie(
                        labels=["지역업체", "지역외업체"],
                        values=[부산_율, 부산_외지],
                        hole=0.65,
                        marker=dict(colors=["#6576ff", "#e4e7ff"]),
                        textinfo="none",
                        hovertemplate="%{label}: %{value}%<extra></extra>",
                    ))
                    fig_donut1.update_layout(
                        showlegend=False,
                        margin=dict(t=5, b=5, l=5, r=5), height=220,
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        annotations=[dict(
                            text=f"<b style='font-size:1.4rem; color:{COLORS['text_dark']};'>{부산_율}%</b>",
                            x=0.5, y=0.5, showarrow=False, font=dict(size=14, family="Nunito Sans"),
                        )],
                    )
                    st.plotly_chart(fig_donut1, use_container_width=True, config={"displayModeBar": False})
                with dc2:
                    st.markdown(f'<div style="display:flex; flex-direction:column; justify-content:center; height:220px; gap:16px; padding-left:8px;"><div><div style="font-size:0.7rem; font-weight:600; color:{COLORS["text_light"]};">총 계약액</div><div style="font-size:1.2rem; font-weight:800; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif; margin-top:2px;">{format_조(부산_발주)}</div></div><div style="display:flex; align-items:center; gap:8px;"><span style="width:10px; height:10px; border-radius:50%; background:#6576ff; display:inline-block;"></span><div><div style="font-size:0.7rem; font-weight:600; color:{COLORS["text_light"]};">지역업체 수주액</div><div style="font-size:1rem; font-weight:800; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif; margin-top:2px;">{format_조(부산_수주)} <span style="color:#6576ff;">({부산_율}%)</span></div></div></div><div style="display:flex; align-items:center; gap:8px;"><span style="width:10px; height:10px; border-radius:50%; background:#e4e7ff; display:inline-block;"></span><div><div style="font-size:0.7rem; font-weight:600; color:{COLORS["text_light"]};">지역외업체 수주액</div><div style="font-size:1rem; font-weight:800; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif; margin-top:2px;">{format_조(부산_외지액)} <span style="color:#aab0c6;">({부산_외지}%)</span></div></div></div></div>', unsafe_allow_html=True)

                # 이번주 계약액 / 이번주 지역업체 수주액 (실제 데이터)
                _w_부산 = _weekly.get('부산광역시 및 소속기관', {})
                w_부산_발주 = _w_부산.get('이번주_계약액', 0)
                w_부산_수주 = _w_부산.get('이번주_수주액', 0)
                _w부산_증감 = _w_부산.get('수주율_증감', 0)
                _w부산_arrow = '↑' if _w부산_증감 >= 0 else '↓'
                _w부산_color = COLORS['success'] if _w부산_증감 >= 0 else COLORS['danger']
                # 이번주 계약액 증감 계산
                _w부산_지난주_계약 = _w_부산.get('지난주_계약액', 0)
                _w부산_계약증감 = round(((w_부산_발주 - _w부산_지난주_계약) / _w부산_지난주_계약 * 100), 1) if _w부산_지난주_계약 > 0 else 0
                _w부산_계약arrow = '↑' if _w부산_계약증감 >= 0 else '↓'
                _w부산_계약color = COLORS['success'] if _w부산_계약증감 >= 0 else COLORS['danger']
                st.markdown(f"""<div style="display:flex; gap:0; border-top:1px solid {COLORS['card_border']}; margin-top:4px;">
<div style="flex:1; padding:8px 14px; border-right:1px solid {COLORS['card_border']};">
<div style="font-size:0.72rem; font-weight:700; color:{COLORS['text_dark']};">이번주 계약액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:2px;">
<div style="font-size:1.05rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(w_부산_발주)}</div>
<div style="text-align:right;"><span style="font-size:0.65rem; font-weight:700; color:{_w부산_계약color};">{_w부산_계약arrow} {abs(_w부산_계약증감):.1f}%</span><br><span style="font-size:0.52rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
</div>
</div>
<div style="flex:1; padding:8px 14px;">
<div style="font-size:0.72rem; font-weight:700; color:{COLORS['text_dark']};">이번주 지역업체 수주액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:2px;">
<div style="font-size:1.05rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(w_부산_수주)}</div>
<div style="text-align:right;"><span style="font-size:0.65rem; font-weight:700; color:{_w부산_color};">{_w부산_arrow} {abs(_w부산_증감):.1f}%p</span><br><span style="font-size:0.52rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
</div>
</div>
</div>""", unsafe_allow_html=True)

            with col_right:
                st.markdown('<div style="padding:20px 0 8px;"></div>', unsafe_allow_html=True)
                
                if 부산_분야:
                    분야_목록 = [
                        ("공사", 부산_분야.get("공사", {}), "#6576ff"),
                        ("용역", 부산_분야.get("용역", {}), "#9cabff"),
                        ("물품", 부산_분야.get("물품", {}), "#1ee0ac"),
                        ("쇼핑몰", 부산_분야.get("쇼핑몰", 부산_분야.get("종합쇼핑몰", {})), "#f4bd0e"),
                    ]
                    # 누계비교 데이터로 전주대비 계산 (부산 그룹×분야)
                    _cum_all = _weekly.get("누계비교", {})
                    trends_r = []
                    trend_c = []
                    for _sn in ['공사','용역','물품','쇼핑몰']:
                        _sc = _cum_all.get(f"부산광역시 및 소속기관_{_sn}", _cum_all.get(_sn, {}))
                        _chg = _sc.get('증감', 0)
                        _arr = '↑' if _chg >= 0 else '↓'
                        trends_r.append(f"{abs(_chg):.1f}%p {_arr}")
                        trend_c.append(COLORS['success'] if _chg >= 0 else COLORS['danger'])
                    spark_data = [
                        [65, 72, 68, 74, 70, 73, 72],
                        [55, 50, 53, 48, 52, 50, 52],
                        [58, 62, 55, 60, 57, 61, 59],
                        [40, 35, 38, 32, 37, 34, 37],
                    ]
                    
                    def make_svg_wave(pts, color):
                        w, h = 80, 28
                        max_v = max(pts) if max(pts) > 0 else 1
                        min_v = min(pts)
                        rng = max_v - min_v if max_v - min_v > 0 else 1
                        coords = []
                        for j, v in enumerate(pts):
                            x = j * (w / (len(pts) - 1))
                            y = h - ((v - min_v) / rng * (h - 4)) - 2
                            coords.append(f"{x:.1f},{y:.1f}")
                        path = "M" + "L".join(coords)
                        return f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" style="display:block;"><path d="{path}" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>'
                    
                    th = f'font-size:0.75rem; font-weight:600; color:{COLORS["text_light"]}; text-transform:uppercase; letter-spacing:0.04em; white-space:nowrap;'
                    header = f'<div style="display:flex; align-items:center; padding:14px 0; border-bottom:1px solid {COLORS["card_border"]};"><div style="flex:2; {th}">분야</div><div style="flex:1.5; {th}">총계약액</div><div style="flex:1.5; {th}">지역업체 수주액</div><div style="flex:1; {th}">비중</div><div style="flex:1.2; {th} text-align:center;">전주대비</div><div style="flex:1.5; {th} text-align:right;">주간추이</div></div>'
                    
                    rows = ""
                    for i, (nm, vals, clr) in enumerate(분야_목록):
                        계약 = vals.get("발주액", 0)
                        수주_v = vals.get("수주액", 0)
                        율_v = vals.get("수주율", 0)
                        td = f'font-size:1rem; font-weight:700; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;'
                        svg = make_svg_wave(spark_data[i], clr)
                        rows += f'<div style="display:flex; align-items:center; padding:26px 0; border-bottom:1px solid {COLORS["card_border"]};"><div style="flex:2; display:flex; align-items:center; gap:6px;"><span style="width:8px; height:8px; border-radius:50%; background:{clr}; display:inline-block;"></span><span style="{td}">{nm}</span></div><div style="flex:1.5; {td}">{format_억(계약)}</div><div style="flex:1.5; {td}">{format_억(수주_v)}</div><div style="flex:1; {td} color:{COLORS["primary"]};">​{율_v}%</div><div style="flex:1.2; text-align:center; font-size:0.9rem; font-weight:600; color:{trend_c[i]};">{trends_r[i]}</div><div style="flex:1.5; text-align:right;">{svg}</div></div>'
                    
                    st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; padding:4px 16px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">{header}{rows}</div>', unsafe_allow_html=True)
                
                st.markdown('<div style="padding:4px 0;"></div>', unsafe_allow_html=True)

        # ── 정부 및 국가공공기관 지역업체 수주현황 (국가계약법 적용) ──
        st.markdown('<div style="margin-top:20px;"></div>', unsafe_allow_html=True)
        
        정부_grp = groups_data.get("정부 및 국가공공기관", {})
        정부_분야 = gxs.get("정부 및 국가공공기관", {})
        
        정부_발주 = 정부_grp.get("발주액", 0)
        정부_수주 = 정부_grp.get("수주액", 0)
        정부_율 = 정부_grp.get("수주율", 0)
        정부_외지 = round(100 - 정부_율, 1) if 정부_율 else 0
        정부_외지액 = 정부_발주 - 정부_수주 if 정부_발주 > 정부_수주 else 0
        
        with st.container(border=True):
            col_left2, col_right2 = st.columns(2)

            with col_left2:
                st.markdown(f"""<div style="padding:20px 0 8px;"><h2 style="margin:0; font-size:1.6rem; font-weight:700; color:{COLORS['text_dark']};">정부 및 국가공공기관 지역업체 수주현황</h2><span style="font-size:0.75rem; color:{COLORS['text_light']};">(국가계약법 적용)</span></div>""", unsafe_allow_html=True)
                
                gc1, gc2 = st.columns([3, 4])
                with gc1:
                    fig_donut_gov = go.Figure(go.Pie(
                        labels=["지역업체", "지역외업체"],
                        values=[정부_율, 정부_외지],
                        hole=0.65,
                        marker=dict(colors=["#ff63a5", "#ffe4ef"]),
                        textinfo="none",
                        hovertemplate="%{label}: %{value}%<extra></extra>",
                    ))
                    fig_donut_gov.update_layout(
                        showlegend=False,
                        margin=dict(t=5, b=5, l=5, r=5), height=220,
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        annotations=[dict(
                            text=f"<b style='font-size:1.4rem; color:{COLORS['text_dark']};'>{정부_율}%</b>",
                            x=0.5, y=0.5, showarrow=False, font=dict(size=14, family="Nunito Sans"),
                        )],
                    )
                    st.plotly_chart(fig_donut_gov, use_container_width=True, config={"displayModeBar": False})
                with gc2:
                    st.markdown(f'<div style="display:flex; flex-direction:column; justify-content:center; height:220px; gap:16px; padding-left:8px;"><div><div style="font-size:0.7rem; font-weight:600; color:{COLORS["text_light"]};">총 계약액</div><div style="font-size:1.2rem; font-weight:800; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif; margin-top:2px;">{format_조(정부_발주)}</div></div><div style="display:flex; align-items:center; gap:8px;"><span style="width:10px; height:10px; border-radius:50%; background:#ff63a5; display:inline-block;"></span><div><div style="font-size:0.7rem; font-weight:600; color:{COLORS["text_light"]};">지역업체 수주액</div><div style="font-size:1rem; font-weight:800; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif; margin-top:2px;">{format_조(정부_수주)} <span style="color:#ff63a5;">({정부_율}%)</span></div></div></div><div style="display:flex; align-items:center; gap:8px;"><span style="width:10px; height:10px; border-radius:50%; background:#ffe4ef; display:inline-block;"></span><div><div style="font-size:0.7rem; font-weight:600; color:{COLORS["text_light"]};">지역외업체 수주액</div><div style="font-size:1rem; font-weight:800; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif; margin-top:2px;">{format_조(정부_외지액)} <span style="color:#aab0c6;">({정부_외지}%)</span></div></div></div></div>', unsafe_allow_html=True)

                # 이번주 계약액 / 이번주 지역업체 수주액 (실제 데이터)
                _w_정부 = _weekly.get('정부 및 국가공공기관', {})
                w_정부_발주 = _w_정부.get('이번주_계약액', 0)
                w_정부_수주 = _w_정부.get('이번주_수주액', 0)
                _w정부_증감 = _w_정부.get('수주율_증감', 0)
                _w정부_arrow = '↑' if _w정부_증감 >= 0 else '↓'
                _w정부_color = COLORS['success'] if _w정부_증감 >= 0 else COLORS['danger']
                # 이번주 계약액 증감 계산
                _w정부_지난주_계약 = _w_정부.get('지난주_계약액', 0)
                _w정부_계약증감 = round(((w_정부_발주 - _w정부_지난주_계약) / _w정부_지난주_계약 * 100), 1) if _w정부_지난주_계약 > 0 else 0
                _w정부_계약arrow = '↑' if _w정부_계약증감 >= 0 else '↓'
                _w정부_계약color = COLORS['success'] if _w정부_계약증감 >= 0 else COLORS['danger']
                st.markdown(f"""<div style="display:flex; gap:0; border-top:1px solid {COLORS['card_border']}; margin-top:4px;">
<div style="flex:1; padding:8px 14px; border-right:1px solid {COLORS['card_border']};">
<div style="font-size:0.72rem; font-weight:700; color:{COLORS['text_dark']};">이번주 계약액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:2px;">
<div style="font-size:1.05rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(w_정부_발주)}</div>
<div style="text-align:right;"><span style="font-size:0.65rem; font-weight:700; color:{_w정부_계약color};">{_w정부_계약arrow} {abs(_w정부_계약증감):.1f}%</span><br><span style="font-size:0.52rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
</div>
</div>
<div style="flex:1; padding:8px 14px;">
<div style="font-size:0.72rem; font-weight:700; color:{COLORS['text_dark']};">이번주 지역업체 수주액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:2px;">
<div style="font-size:1.05rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(w_정부_수주)}</div>
<div style="text-align:right;"><span style="font-size:0.65rem; font-weight:700; color:{_w정부_color};">{_w정부_arrow} {abs(_w정부_증감):.1f}%p</span><br><span style="font-size:0.52rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
</div>
</div>
</div>""", unsafe_allow_html=True)

            with col_right2:
                st.markdown('<div style="padding:20px 0 8px;"></div>', unsafe_allow_html=True)
                
                if 정부_분야:
                    gov_목록 = [
                        ("공사", 정부_분야.get("공사", {}), "#ff63a5"),
                        ("용역", 정부_분야.get("용역", {}), "#b98dff"),
                        ("물품", 정부_분야.get("물품", {}), "#1ee0ac"),
                        ("쇼핑몰", 정부_분야.get("쇼핑몰", 정부_분야.get("종합쇼핑몰", {})), "#f4bd0e"),
                    ]
                    gov_trends = []
                    gov_tc = []
                    for _sn in ['공사','용역','물품','쇼핑몰']:
                        _sc = _cum_all.get(f"정부 및 국가공공기관_{_sn}", _cum_all.get(_sn, {}))
                        _chg = _sc.get('증감', 0)
                        _arr = '↑' if _chg >= 0 else '↓'
                        gov_trends.append(f"{abs(_chg):.1f}%p {_arr}")
                        gov_tc.append(COLORS['success'] if _chg >= 0 else COLORS['danger'])
                    gov_spark = [
                        [60, 65, 58, 63, 61, 64, 62],
                        [22, 18, 20, 16, 19, 17, 20],
                        [60, 64, 58, 63, 60, 65, 61],
                        [28, 22, 25, 20, 24, 21, 24],
                    ]
                    
                    def make_svg_wave_g(pts, color):
                        w, h = 80, 28
                        max_v = max(pts) if max(pts) > 0 else 1
                        min_v = min(pts)
                        rng = max_v - min_v if max_v - min_v > 0 else 1
                        coords = []
                        for j, v in enumerate(pts):
                            x = j * (w / (len(pts) - 1))
                            y = h - ((v - min_v) / rng * (h - 4)) - 2
                            coords.append(f"{x:.1f},{y:.1f}")
                        path = "M" + "L".join(coords)
                        return f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" style="display:block;"><path d="{path}" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>'
                    
                    th_g = f'font-size:0.75rem; font-weight:600; color:{COLORS["text_light"]}; text-transform:uppercase; letter-spacing:0.04em; white-space:nowrap;'
                    header_g = f'<div style="display:flex; align-items:center; padding:14px 0; border-bottom:1px solid {COLORS["card_border"]};"><div style="flex:2; {th_g}">분야</div><div style="flex:1.5; {th_g}">총계약액</div><div style="flex:1.5; {th_g}">지역업체 수주액</div><div style="flex:1; {th_g}">비중</div><div style="flex:1.2; {th_g} text-align:center;">전주대비</div><div style="flex:1.5; {th_g} text-align:right;">주간추이</div></div>'
                    
                    rows_g = ""
                    for i, (nm, vals, clr) in enumerate(gov_목록):
                        계약_g = vals.get("발주액", 0)
                        수주_g = vals.get("수주액", 0)
                        율_g = vals.get("수주율", 0)
                        td_g = f'font-size:1rem; font-weight:700; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;'
                        svg_g = make_svg_wave_g(gov_spark[i], clr)
                        rows_g += f'<div style="display:flex; align-items:center; padding:26px 0; border-bottom:1px solid {COLORS["card_border"]};"><div style="flex:2; display:flex; align-items:center; gap:6px;"><span style="width:8px; height:8px; border-radius:50%; background:{clr}; display:inline-block;"></span><span style="{td_g}">{nm}</span></div><div style="flex:1.5; {td_g}">{format_억(계약_g)}</div><div style="flex:1.5; {td_g}">{format_억(수주_g)}</div><div style="flex:1; {td_g} color:{COLORS["primary"]};">​{율_g}%</div><div style="flex:1.2; text-align:center; font-size:0.9rem; font-weight:600; color:{gov_tc[i]};">{gov_trends[i]}</div><div style="flex:1.5; text-align:right;">{svg_g}</div></div>'
                    
                    st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; padding:4px 16px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">{header_g}{rows_g}</div>', unsafe_allow_html=True)
                
                st.markdown('<div style="padding:4px 0;"></div>', unsafe_allow_html=True)


# ════════════════════════════════════════════
# PAGE: 기관별 순위
# ════════════════════════════════════════════
elif page == "🏆 기관별 순위":
    data_rank = fetch_api("/api/ranking")
    if data_rank:
        st.caption(f"📅 생성: {data_rank.get('generated_at', '')}")
        st.markdown(f'<div style="font-size:0.78rem; color:{COLORS["text_light"]}; margin-top:-8px; margin-bottom:12px;">※ 발주액 50억 이상 기관 대상으로 적용</div>', unsafe_allow_html=True)

        sector_opt = st.selectbox("📂 분야 선택", ["전체", "공사", "용역", "물품", "쇼핑몰"], key="rank_sector")

        if sector_opt == "전체":
            rank_data = data_rank.get("전체", {})
        else:
            sector_data = fetch_api(f"/api/ranking/{sector_opt}")
            rank_data = sector_data.get("랭킹", {}) if sector_data else {}

        # ── 수요기관 수 (DB에서 — 분류별) ──
        try:
            import sqlite3, os
            db_path = os.path.join(os.path.dirname(__file__), "busan_agencies_master.db")
            _conn = sqlite3.connect(db_path)
            n_부산 = _conn.execute("SELECT COUNT(*) FROM agency_master WHERE cate_lrg LIKE '%부산%'").fetchone()[0]
            n_정부 = _conn.execute("SELECT COUNT(*) FROM agency_master WHERE cate_lrg LIKE '%정부%'").fetchone()[0]
            _conn.close()
        except Exception:
            n_부산 = 1934
            n_정부 = 480
            
        # 상/하위 순위 — DashLite Invest 스타일
        for grp_name in ["부산광역시 및 소속기관", "정부 및 국가공공기관"]:
            grp_data = rank_data.get(grp_name, {})
            icon = '<img src="https://www.busan.go.kr/humanframe/global/assets/img/common/busan_logo.svg" style="height:26px; width:26px; object-fit:cover; object-position:left; vertical-align:middle; margin-right:8px;">' if "부산" in grp_name else '<img src="https://www.mois.go.kr/frt2022/main/img/common/logo.png" style="height:26px; width:26px; object-fit:cover; object-position:left; vertical-align:middle; margin-right:8px;">'
            grp_label = "부산시 및 소관기관" if "부산" in grp_name else "정부 및 국가공공기관"
            법적용 = "지방계약법 적용" if "부산" in grp_name else "국가계약법 적용"
            기관수 = n_부산 if "부산" in grp_name else n_정부
            
            # 그룹 헤더 (상단에 한 번만)
            st.markdown(f'<div style="padding:20px 0 8px;"><span style="font-size:1.15rem; font-weight:700; color:{COLORS["text_dark"]};">{icon} {grp_label}</span> <span style="font-size:0.78rem; color:{COLORS["text_light"]};">({법적용} {기관수:,}개 기관)</span></div>', unsafe_allow_html=True)
            
            th_s = f'font-size:0.82rem; font-weight:700; color:{COLORS["text_dark"]}; letter-spacing:0.03em; padding:10px 0;'
            
            col_top, col_bot = st.columns(2)
            
            with col_top:
                top_list = grp_data.get("상위", [])
                header_html = f'<div style="display:flex; justify-content:space-between; align-items:center; padding:14px 20px; background:linear-gradient(135deg, #6576ff 0%, #8a9bff 100%); border-radius:6px 6px 0 0;"><div style="font-size:0.95rem; font-weight:700; color:#fff;">🔝 상위 10개 기관</div><div style="font-size:0.72rem; font-weight:600; color:rgba(255,255,255,0.7);">수주율 높은 순</div></div>'
                col_header = f'<div style="display:flex; align-items:center; padding:10px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:#f8f9fc;"><div style="flex:0.5; {th_s}">순위</div><div style="flex:3; {th_s}">수요기관명</div><div style="flex:1.5; {th_s} text-align:right;">총 발주액</div><div style="flex:1.5; {th_s} text-align:right;">지역업체 수주액</div><div style="flex:1.2; {th_s} text-align:right;">수주율</div></div>'
                rows_html = ""
                medal_icons = {1: "👑", 2: "🥈", 3: "🥉"}
                top_bg = {1: "linear-gradient(135deg, #FFF8E1 0%, #FFECB3 50%, #FFE082 100%)", 2: "linear-gradient(135deg, #F5F5F5 0%, #E8EAF6 50%, #E0E0E0 100%)", 3: "linear-gradient(135deg, #FFF3E0 0%, #FFE0B2 50%, #FFCC80 100%)"}
                top_border = {1: "4px solid #FFD700", 2: "4px solid #B0BEC5", 3: "4px solid #CD7F32"}
                for i, item in enumerate(top_list[:10]):
                    name = item.get("비교단위", "")
                    rate = item.get("수주율", 0)
                    발주 = item.get("발주액", 0)
                    수주 = item.get("수주액", 0)
                    rc = rate_color(rate)
                    rank_num = i + 1
                    badge_bg = "#6576ff" if rank_num <= 3 else "#e3e7fe"
                    badge_fg = "#fff" if rank_num <= 3 else "#6576ff"
                    bg = top_bg.get(rank_num, "transparent")
                    bl = top_border.get(rank_num, "none")
                    medal = f'<span style="font-size:1rem; margin-left:4px;">{medal_icons[rank_num]}</span>' if rank_num in medal_icons else ""
                    rows_html += f'''<div style="display:flex; align-items:center; padding:14px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:{bg}; border-left:{bl};">
<div style="flex:0.5;"><span style="display:inline-flex; align-items:center; justify-content:center; width:28px; height:28px; border-radius:50%; background:{badge_bg}; color:{badge_fg}; font-size:0.72rem; font-weight:700;">{rank_num}</span></div>
<div style="flex:3; display:flex; align-items:center;"><span style="font-size:0.88rem; font-weight:600; color:{COLORS["text_dark"]};">{name}</span>{medal}</div>
<div style="flex:1.5; text-align:right; font-size:0.85rem; font-weight:600; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;">{format_억(발주)}</div>
<div style="flex:1.5; text-align:right; font-size:0.85rem; font-weight:600; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;">{format_억(수주)}</div>
<div style="flex:1.2; text-align:right; font-size:0.88rem; font-weight:700; color:{rc};">{rate}%</div>
</div>'''
                
                st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; box-shadow:0 1px 3px rgba(0,0,0,0.04); overflow:hidden;">{header_html}{col_header}{rows_html}</div>', unsafe_allow_html=True)
                top_names = [item.get("비교단위", "") for item in top_list[:10] if item.get("비교단위")]
                if top_names:
                    sel_top = st.selectbox("🔍 기관 상세 조회", ["선택하세요"] + top_names, key=f"rank_top_{grp_name}")
                    if sel_top and sel_top != "선택하세요":
                        res = fetch_api(f"/api/agency/search?q={sel_top}")
                        if res and "검색결과" in res and res["검색결과"]:
                            for u, det in res["검색결과"].items():
                                r = det.get("총수주율", 0)
                                rc2 = rate_color(r)
                                발주_d = det.get("총발주액", 0)
                                수주_d = det.get("총수주액", 0)
                                분야별_d = det.get("분야별", {})
                                그룹_d = str(det.get("그룹", ""))
                                법_d = "지방계약법" if "부산" in 그룹_d else "국가계약법"
                                sub_ps = [f"{sn}({format_억(sv.get('발주액',0))})" for sn in ["공사","용역","물품","쇼핑몰"] for sv in [분야별_d.get(sn,{})] if sv.get("발주액",0)>0]
                                sub_i = " · ".join(sub_ps)
                                st.markdown(f"""<div style="background: linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius: 8px; padding: 22px 28px 18px; box-shadow: 0 4px 20px rgba(35,46,122,0.35); margin-top:12px;">
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
<span style="font-size:1.2rem; font-weight:800; color:#fff;">{u}</span>
<span style="font-size:0.75rem; color:rgba(255,255,255,0.55); font-weight:600;">({법_d})</span>
</div>
<div style="font-size:0.8rem; font-weight:600; color:rgba(255,255,255,0.7);">총 계약액</div>
<div style="font-size:2.2rem; font-weight:800; color:#fff; line-height:1; font-family:Nunito Sans,sans-serif; margin-top:4px;">{format_억(발주_d)}</div>
<div style="font-size:0.7rem; color:rgba(255,255,255,0.4); margin-top:4px;">{sub_i}</div>
<div style="font-size:0.8rem; font-weight:600; color:rgba(255,255,255,0.7); margin-top:14px;">지역업체 수주액 (수주율)</div>
<div style="font-size:1.5rem; font-weight:800; color:rgba(255,255,255,0.95); font-family:Nunito Sans,sans-serif; margin-top:4px;">{format_억(수주_d)} <span style="color:{rc2};">({r}%)</span></div>
</div>""", unsafe_allow_html=True)
                                # 분야별 미니 카드
                                dot_c_r = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}
                                bs_r = [[40,55,35,60,45,70,80],[60,45,50,35,55,40,65],[30,50,65,45,70,55,75],[45,35,55,40,50,60,70]]
                                분_ls = ["공사","용역","물품","쇼핑몰"]
                                for ri in range(2):
                                    mc_1, mc_2 = st.columns(2)
                                    for cj, cw in enumerate([mc_1, mc_2]):
                                        sx = ri*2+cj
                                        if sx >= 4: break
                                        sn = 분_ls[sx]
                                        sv = 분야별_d.get(sn, {})
                                        dc = dot_c_r.get(sn, "#aaa")
                                        brs = "".join([f'<div style="width:5px; height:{h}%; background:{dc}; opacity:{"0.3" if j<6 else "1"}; border-radius:1px;"></div>' for j,h in enumerate(bs_r[sx])])
                                        with cw:
                                            s_율_v = sv.get("수주율", 0)
                                            st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; padding:12px 14px; margin-top:8px;"><div style="display:flex; justify-content:space-between;"><div><div style="font-size:0.7rem; font-weight:700; color:{COLORS["text_dark"]}; margin-bottom:4px;">{sn}계약액</div><div style="font-size:1rem; font-weight:800; font-family:Nunito Sans,sans-serif;">{format_억(sv.get("발주액",0))}</div><div style="font-size:0.55rem; font-weight:600; color:{COLORS["text_light"]}; margin-top:6px;">지역업체 수주액</div><div style="font-size:0.82rem; font-weight:800; font-family:Nunito Sans,sans-serif; margin-top:1px;">{format_억(sv.get("수주액",0))} <span style="color:{COLORS["primary"]};">({s_율_v}%)</span></div></div><div style="display:flex; flex-direction:column; align-items:flex-end;"><span style="width:7px; height:7px; border-radius:50%; background:{dc}; display:inline-block;"></span><div style="display:flex; align-items:flex-end; gap:2px; height:30px; margin-top:6px;">{brs}</div></div></div></div>', unsafe_allow_html=True)
                                # 유출계약
                                leaks_all_t = det.get("유출계약", [])
                                if leaks_all_t:
                                    leaks_t = leaks_all_t[:50]
                                    limit_txt_t = " (상위 50건)" if len(leaks_all_t) > 50 else ""
                                    th_lr = f'font-size:0.82rem; font-weight:700; color:{COLORS["text_dark"]}; padding:10px 0;'
                                    lk_hd = f'<div style="display:flex; justify-content:space-between; padding:10px 16px; background:linear-gradient(135deg, #e85347 0%, #ff7b6b 100%); border-radius:6px 6px 0 0;"><div style="font-size:0.85rem; font-weight:700; color:#fff;">🔴 {u} 유출 계약{limit_txt_t}</div><div style="font-size:0.7rem; color:rgba(255,255,255,0.7);">총 {len(leaks_all_t):,}건 중 상위 {len(leaks_t)}건</div></div>'
                                    ch_r = f'<div style="display:flex; padding:6px 16px; border-bottom:1px solid {COLORS["card_border"]}; background:#f8f9fc;"><div style="flex:0.8; {th_lr}">분야</div><div style="flex:2.5; {th_lr}">계약명</div><div style="flex:1; {th_lr} text-align:right;">계약액</div><div style="flex:1; {th_lr} text-align:right;">유출액</div><div style="flex:0.8; {th_lr} text-align:right;">유출율</div><div style="flex:1.8; {th_lr} padding-left:12px;">수주업체</div><div style="flex:2; {th_lr} padding-left:8px;">비고</div></div>'
                                    rws = ""
                                    for li, lk in enumerate(leaks_t):
                                        fc = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}.get(lk.get("분야",""), COLORS["text_light"])
                                        ul = lk.get("유출율",0)
                                        uc = COLORS['danger'] if ul>=80 else (COLORS['warning'] if ul>=50 else COLORS['text_dark'])
                                        rb = "#fafbfe" if li%2==1 else COLORS["card_bg"]
                                        _bigo = lk.get("비고", "")
                                        _bc = '#e85347' if '비정상' in _bigo or '미적용' in _bigo else (COLORS['text_light'] if '정상' in _bigo or '장기' in _bigo else COLORS['text_dark'])
                                        rws += f'<div style="display:flex; align-items:center; padding:8px 16px; border-bottom:1px solid {COLORS["card_border"]}; background:{rb};"><div style="flex:0.8;"><span style="background:{fc}; color:#fff; padding:2px 6px; border-radius:10px; font-size:0.6rem; font-weight:600;">{lk.get("분야","")}</span></div><div style="flex:2.5; font-size:0.75rem; font-weight:600; color:{COLORS["text_dark"]}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{lk.get("계약명","")[:35]}</div><div style="flex:1; text-align:right; font-size:0.78rem; font-weight:600; font-family:Nunito Sans,sans-serif;">{format_억(lk.get("계약액",0))}</div><div style="flex:1; text-align:right; font-size:0.78rem; font-weight:700; color:{COLORS["danger"]}; font-family:Nunito Sans,sans-serif;">{format_억(lk.get("유출액",0))}</div><div style="flex:0.8; text-align:right; font-size:0.78rem; font-weight:700; color:{uc};">{ul}%</div><div style="flex:1.8; font-size:0.7rem; color:{COLORS["text_light"]}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-left:12px;">{lk.get("수주업체","")}</div><div style="flex:2; font-size:0.65rem; font-weight:600; color:{_bc}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-left:8px;">{_bigo}</div></div>'
                                    st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; overflow:hidden; margin-top:8px;">{lk_hd}{ch_r}{rws}</div>', unsafe_allow_html=True)
                                    
                                    # Excel 다운로드
                                    df_dl_t = pd.DataFrame(leaks_all_t)
                                    cols_dl = ["분야", "계약명", "계약액", "유출액", "유출율", "수주업체", "비고"]
                                    if not df_dl_t.empty:
                                        df_dl_t = df_dl_t[[c for c in cols_dl if c in df_dl_t.columns]].copy()
                                        import io
                                        buf_t = io.BytesIO()
                                        df_dl_t.to_excel(buf_t, index=False, engine='openpyxl')
                                        st.download_button(
                                            label=f"📥 {u} 지역외 유출계약 전체 다운로드",
                                            data=buf_t.getvalue(),
                                            file_name=f"{u}_유출계약(상세).xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key=f"dl_leak_top_{u}"
                                        )
                                else:
                                    st.info(f"{u}의 주요 유출 계약이 없습니다.")

            with col_bot:
                bot_list = grp_data.get("하위", [])
                header_html_b = f'<div style="display:flex; justify-content:space-between; align-items:center; padding:14px 20px; background:linear-gradient(135deg, #e85347 0%, #ff7b6b 100%); border-radius:6px 6px 0 0;"><div style="font-size:0.95rem; font-weight:700; color:#fff;">🔻 하위 10개 기관</div><div style="font-size:0.72rem; font-weight:600; color:rgba(255,255,255,0.7);">수주율 낮은 순</div></div>'
                col_header_b = f'<div style="display:flex; align-items:center; padding:10px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:#f8f9fc;"><div style="flex:0.5; {th_s}">순위</div><div style="flex:3; {th_s}">수요기관명</div><div style="flex:1.5; {th_s} text-align:right;">총 발주액</div><div style="flex:1.5; {th_s} text-align:right;">지역업체 수주액</div><div style="flex:1.2; {th_s} text-align:right;">수주율</div></div>'
                rows_html_b = ""
                for i, item in enumerate(bot_list[:10]):
                    name = item.get("비교단위", "")
                    rate = item.get("수주율", 0)
                    발주 = item.get("발주액", 0)
                    수주 = item.get("수주액", 0)
                    rc = rate_color(rate)
                    rank_num = i + 1
                    badge_bg = "#e85347" if rank_num <= 3 else "#fce4e4"
                    badge_fg = "#fff" if rank_num <= 3 else "#e85347"
                    rows_html_b += f'''<div style="display:flex; align-items:center; padding:14px 20px; border-bottom:1px solid {COLORS["card_border"]};">
<div style="flex:0.5;"><span style="display:inline-flex; align-items:center; justify-content:center; width:28px; height:28px; border-radius:50%; background:{badge_bg}; color:{badge_fg}; font-size:0.72rem; font-weight:700;">{rank_num}</span></div>
<div style="flex:3;"><span style="font-size:0.88rem; font-weight:600; color:{COLORS["text_dark"]};">{name}</span></div>
<div style="flex:1.5; text-align:right; font-size:0.85rem; font-weight:600; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;">{format_억(발주)}</div>
<div style="flex:1.5; text-align:right; font-size:0.85rem; font-weight:600; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;">{format_억(수주)}</div>
<div style="flex:1.2; text-align:right; font-size:0.88rem; font-weight:700; color:{rc};">{rate}%</div>
</div>'''
                
                st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; box-shadow:0 1px 3px rgba(0,0,0,0.04); overflow:hidden;">{header_html_b}{col_header_b}{rows_html_b}</div>', unsafe_allow_html=True)
                bot_names = [item.get("비교단위", "") for item in bot_list[:10] if item.get("비교단위")]
                if bot_names:
                    sel_bot = st.selectbox("🔍 기관 상세 조회", ["선택하세요"] + bot_names, key=f"rank_bot_{grp_name}")
                    if sel_bot and sel_bot != "선택하세요":
                        res_b = fetch_api(f"/api/agency/search?q={sel_bot}")
                        if res_b and "검색결과" in res_b and res_b["검색결과"]:
                            for u, det in res_b["검색결과"].items():
                                r = det.get("총수주율", 0)
                                rc2 = rate_color(r)
                                발주_d = det.get("총발주액", 0)
                                수주_d = det.get("총수주액", 0)
                                분야별_d = det.get("분야별", {})
                                그룹_d = str(det.get("그룹", ""))
                                법_d = "지방계약법" if "부산" in 그룹_d else "국가계약법"
                                sub_ps = [f"{sn}({format_억(sv.get('발주액',0))})" for sn in ["공사","용역","물품","쇼핑몰"] for sv in [분야별_d.get(sn,{})] if sv.get("발주액",0)>0]
                                sub_i = " · ".join(sub_ps)
                                st.markdown(f"""<div style="background: linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius: 8px; padding: 22px 28px 18px; box-shadow: 0 4px 20px rgba(35,46,122,0.35); margin-top:12px;">
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
<span style="font-size:1.2rem; font-weight:800; color:#fff;">{u}</span>
<span style="font-size:0.75rem; color:rgba(255,255,255,0.55); font-weight:600;">({법_d})</span>
</div>
<div style="font-size:0.8rem; font-weight:600; color:rgba(255,255,255,0.7);">총 계약액</div>
<div style="font-size:2.2rem; font-weight:800; color:#fff; line-height:1; font-family:Nunito Sans,sans-serif; margin-top:4px;">{format_억(발주_d)}</div>
<div style="font-size:0.7rem; color:rgba(255,255,255,0.4); margin-top:4px;">{sub_i}</div>
<div style="font-size:0.8rem; font-weight:600; color:rgba(255,255,255,0.7); margin-top:14px;">지역업체 수주액 (수주율)</div>
<div style="font-size:1.5rem; font-weight:800; color:rgba(255,255,255,0.95); font-family:Nunito Sans,sans-serif; margin-top:4px;">{format_억(수주_d)} <span style="color:{rc2};">({r}%)</span></div>
</div>""", unsafe_allow_html=True)
                                # 분야별 미니 카드
                                dot_c_r = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}
                                bs_r = [[40,55,35,60,45,70,80],[60,45,50,35,55,40,65],[30,50,65,45,70,55,75],[45,35,55,40,50,60,70]]
                                분_ls = ["공사","용역","물품","쇼핑몰"]
                                for ri in range(2):
                                    mc_1, mc_2 = st.columns(2)
                                    for cj, cw in enumerate([mc_1, mc_2]):
                                        sx = ri*2+cj
                                        if sx >= 4: break
                                        sn = 분_ls[sx]
                                        sv = 분야별_d.get(sn, {})
                                        dc = dot_c_r.get(sn, "#aaa")
                                        brs = "".join([f'<div style="width:5px; height:{h}%; background:{dc}; opacity:{"0.3" if j<6 else "1"}; border-radius:1px;"></div>' for j,h in enumerate(bs_r[sx])])
                                        with cw:
                                            s_율_v = sv.get("수주율", 0)
                                            st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; padding:12px 14px; margin-top:8px;"><div style="display:flex; justify-content:space-between;"><div><div style="font-size:0.7rem; font-weight:700; color:{COLORS["text_dark"]}; margin-bottom:4px;">{sn}계약액</div><div style="font-size:1rem; font-weight:800; font-family:Nunito Sans,sans-serif;">{format_억(sv.get("발주액",0))}</div><div style="font-size:0.55rem; font-weight:600; color:{COLORS["text_light"]}; margin-top:6px;">지역업체 수주액</div><div style="font-size:0.82rem; font-weight:800; font-family:Nunito Sans,sans-serif; margin-top:1px;">{format_억(sv.get("수주액",0))} <span style="color:{COLORS["primary"]};">({s_율_v}%)</span></div></div><div style="display:flex; flex-direction:column; align-items:flex-end;"><span style="width:7px; height:7px; border-radius:50%; background:{dc}; display:inline-block;"></span><div style="display:flex; align-items:flex-end; gap:2px; height:30px; margin-top:6px;">{brs}</div></div></div></div>', unsafe_allow_html=True)
                                # 유출계약
                                leaks_all_b = det.get("유출계약", [])
                                if leaks_all_b:
                                    leaks_b = leaks_all_b[:50]
                                    limit_txt_b = " (상위 50건)" if len(leaks_all_b) > 50 else ""
                                    th_lr = f'font-size:0.82rem; font-weight:700; color:{COLORS["text_dark"]}; padding:10px 0;'
                                    lk_hd = f'<div style="display:flex; justify-content:space-between; padding:10px 16px; background:linear-gradient(135deg, #e85347 0%, #ff7b6b 100%); border-radius:6px 6px 0 0;"><div style="font-size:0.85rem; font-weight:700; color:#fff;">🔴 {u} 유출 계약{limit_txt_b}</div><div style="font-size:0.7rem; color:rgba(255,255,255,0.7);">총 {len(leaks_all_b):,}건 중 상위 {len(leaks_b)}건</div></div>'
                                    ch_r = f'<div style="display:flex; padding:6px 16px; border-bottom:1px solid {COLORS["card_border"]}; background:#f8f9fc;"><div style="flex:0.8; {th_lr}">분야</div><div style="flex:2.5; {th_lr}">계약명</div><div style="flex:1; {th_lr} text-align:right;">계약액</div><div style="flex:1; {th_lr} text-align:right;">유출액</div><div style="flex:0.8; {th_lr} text-align:right;">유출율</div><div style="flex:1.8; {th_lr} padding-left:12px;">수주업체</div><div style="flex:2; {th_lr} padding-left:8px;">비고</div></div>'
                                    rws = ""
                                    for li, lk in enumerate(leaks_b):
                                        fc = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}.get(lk.get("분야",""), COLORS["text_light"])
                                        ul = lk.get("유출율",0)
                                        uc = COLORS['danger'] if ul>=80 else (COLORS['warning'] if ul>=50 else COLORS['text_dark'])
                                        rb = "#fafbfe" if li%2==1 else COLORS["card_bg"]
                                        _bigo = lk.get("비고", "")
                                        _bc = '#e85347' if '비정상' in _bigo or '미적용' in _bigo else (COLORS['text_light'] if '정상' in _bigo or '장기' in _bigo else COLORS['text_dark'])
                                        rws += f'<div style="display:flex; align-items:center; padding:8px 16px; border-bottom:1px solid {COLORS["card_border"]}; background:{rb};"><div style="flex:0.8;"><span style="background:{fc}; color:#fff; padding:2px 6px; border-radius:10px; font-size:0.6rem; font-weight:600;">{lk.get("분야","")}</span></div><div style="flex:2.5; font-size:0.75rem; font-weight:600; color:{COLORS["text_dark"]}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{lk.get("계약명","")[:35]}</div><div style="flex:1; text-align:right; font-size:0.78rem; font-weight:600; font-family:Nunito Sans,sans-serif;">{format_억(lk.get("계약액",0))}</div><div style="flex:1; text-align:right; font-size:0.78rem; font-weight:700; color:{COLORS["danger"]}; font-family:Nunito Sans,sans-serif;">{format_억(lk.get("유출액",0))}</div><div style="flex:0.8; text-align:right; font-size:0.78rem; font-weight:700; color:{uc};">{ul}%</div><div style="flex:1.8; font-size:0.7rem; color:{COLORS["text_light"]}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-left:12px;">{lk.get("수주업체","")}</div><div style="flex:2; font-size:0.65rem; font-weight:600; color:{_bc}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-left:8px;">{_bigo}</div></div>'
                                    st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; overflow:hidden; margin-top:8px;">{lk_hd}{ch_r}{rws}</div>', unsafe_allow_html=True)
                                    
                                    # Excel 다운로드
                                    df_dl_b = pd.DataFrame(leaks_all_b)
                                    cols_dl = ["분야", "계약명", "계약액", "유출액", "유출율", "수주업체", "비고"]
                                    if not df_dl_b.empty:
                                        df_dl_b = df_dl_b[[c for c in cols_dl if c in df_dl_b.columns]].copy()
                                        import io
                                        buf_b = io.BytesIO()
                                        df_dl_b.to_excel(buf_b, index=False, engine='openpyxl')
                                        st.download_button(
                                            label=f"📥 {u} 지역외 유출계약 전체 다운로드",
                                            data=buf_b.getvalue(),
                                            file_name=f"{u}_유출계약(상세).xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key=f"dl_leak_bot_{u}"
                                        )
                                else:
                                    st.info(f"{u}의 주요 유출 계약이 없습니다.")
            
            st.markdown('<div style="margin-top:20px;"></div>', unsafe_allow_html=True)




# ════════════════════════════════════════════
# PAGE: 기관검색
# ════════════════════════════════════════════
elif page == "🔍 기관별 실적 검색":
    search_org = st.text_input("🔍 기관 검색", key="search_org", placeholder="기관명을 입력하세요 (예: 해운대구, 부산교육청)")

    if search_org and search_org.strip():
        st.markdown(f"### 🔍 '{search_org}' 검색 결과")
        found = False
        search_api_res = fetch_api(f"/api/agency/search?q={search_org.strip()}")
        if search_api_res and "검색결과" in search_api_res and search_api_res["검색결과"]:
            found = True
            for u, details in search_api_res["검색결과"].items():
                rate = details.get("총수주율", 0)
                rc = rate_color(rate)
                발주_t = details.get("총발주액", 0)
                수주_t = details.get("총수주액", 0)
                외지율 = round(100 - rate, 1) if rate else 0
                외지액 = 발주_t - 수주_t if 발주_t > 수주_t else 0
                그룹 = str(details.get("그룹", ""))
                법적용 = "지방계약법" if "부산" in 그룹 else "국가계약법"
                분야별 = details.get("분야별", {})
                
                with st.container(border=True):
                    col_hero, col_side = st.columns([6, 4])
                    
                    with col_hero:
                        # 인디고 히어로 카드
                        sub_parts = []
                        for sn in ["공사", "용역", "물품", "쇼핑몰"]:
                            sv = 분야별.get(sn, {})
                            if sv.get("발주액", 0) > 0:
                                sub_parts.append(f"{sn}({format_억(sv.get('발주액',0))})")
                        sub_info = " · ".join(sub_parts) if sub_parts else ""
                        
                        st.markdown(f"""<div style="background: linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius: 8px; padding: 28px 32px 22px; box-shadow: 0 4px 20px rgba(35,46,122,0.35);">
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
<span style="font-size:1.3rem; font-weight:800; color:#fff;">{u}</span>
<span style="font-size:0.78rem; color:rgba(255,255,255,0.55); font-weight:600;">({법적용})</span>
</div>
<div style="font-size:0.85rem; font-weight:600; color:rgba(255,255,255,0.7);">총 계약액</div>
<div style="font-size:2.6rem; font-weight:800; color:#fff; line-height:1; font-family:Nunito Sans,sans-serif; letter-spacing:-0.02em; margin-top:4px;">{format_억(발주_t)}</div>
<div style="font-size:0.72rem; color:rgba(255,255,255,0.4); margin-top:6px;">{sub_info}</div>
<div style="font-size:0.85rem; font-weight:600; color:rgba(255,255,255,0.7); margin-top:18px;">지역업체 수주액 (수주율)</div>
<div style="font-size:1.8rem; font-weight:800; color:rgba(255,255,255,0.95); font-family:Nunito Sans,sans-serif; line-height:1; letter-spacing:-0.02em; margin-top:6px;">{format_억(수주_t)} <span style="color:{rc};">({rate}%)</span></div>
</div>""", unsafe_allow_html=True)

                    
                    with col_side:
                        # 2x2 분야별 미니 카드
                        dot_colors_s = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}
                        bar_sets_s = [
                            [40,55,35,60,45,70,80], [60,45,50,35,55,40,65],
                            [30,50,65,45,70,55,75], [45,35,55,40,50,60,70],
                        ]
                        분야_list = ["공사","용역","물품","쇼핑몰"]
                        
                        for row_idx in range(2):
                            mc1, mc2 = st.columns(2)
                            for ci, col_wgt in enumerate([mc1, mc2]):
                                si = row_idx * 2 + ci
                                if si >= len(분야_list):
                                    break
                                sn = 분야_list[si]
                                sv = 분야별.get(sn, {})
                                s_발주 = sv.get("발주액", 0)
                                s_수주 = sv.get("수주액", 0)
                                s_율 = sv.get("수주율", 0)
                                dc = dot_colors_s.get(sn, "#aaa")
                                bars = ""
                                for j, h in enumerate(bar_sets_s[si]):
                                    op = "0.3" if j < 6 else "1"
                                    bars += f'<div style="width:6px; height:{h}%; background:{dc}; opacity:{op}; border-radius:1px;"></div>'
                                with col_wgt:
                                    st.markdown(f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px; padding:14px 16px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
<div style="display:flex; justify-content:space-between; align-items:flex-start;">
<div style="flex:1;">
<div style="font-size:0.75rem; font-weight:700; color:{COLORS['text_dark']}; margin-bottom:6px;">{sn}계약액</div>
<div style="font-size:1.15rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif; line-height:1;">{format_억(s_발주)}</div>
<div style="margin-top:8px;">
<div style="font-size:0.6rem; font-weight:600; color:{COLORS['text_light']}; letter-spacing:0.03em;">지역업체 수주액</div>
<div style="font-size:0.9rem; font-weight:800; color:{COLORS['text_dark']}; margin-top:2px; font-family:Nunito Sans,sans-serif;">{format_억(s_수주)} <span style="color:{COLORS['primary']};">({s_율}%)</span></div>
</div>
</div>
<div style="text-align:right; display:flex; flex-direction:column; align-items:flex-end;">
<span style="width:8px; height:8px; border-radius:50%; background:{dc}; display:inline-block; margin-bottom:6px;"></span>
<div style="display:flex; align-items:flex-end; gap:2px; height:36px; margin-top:6px;">{bars}</div>
</div>
</div>
</div>""", unsafe_allow_html=True)
                            if row_idx == 0:
                                st.markdown('<div style="margin-top:10px;"></div>', unsafe_allow_html=True)

                leaks_all = details.get("유출계약", [])
                if leaks_all:
                    leaks = leaks_all[:50]
                    limit_txt = " (상위 50건)" if len(leaks_all) > 50 else ""
                    th_lk = f'font-size:0.72rem; font-weight:600; color:{COLORS["text_light"]}; letter-spacing:0.03em; padding:10px 0;'
                    leak_header = f"""<div style="display:flex; justify-content:space-between; align-items:center; padding:12px 20px; background:linear-gradient(135deg, #e85347 0%, #ff7b6b 100%); border-radius:6px 6px 0 0;">
<div style="font-size:0.9rem; font-weight:700; color:#fff;">🔴 {u} 주요 지역외 유출 계약{limit_txt}</div>
<div style="font-size:0.72rem; color:rgba(255,255,255,0.7);">총 {len(leaks_all):,}건 중 상위 {len(leaks)}건</div>
</div>"""
                    col_hdr = f'<div style="display:flex; align-items:center; padding:8px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:#f8f9fc;"><div style="flex:0.8; {th_lk}">분야</div><div style="flex:2.5; {th_lk}">계약명</div><div style="flex:1; {th_lk} text-align:right;">계약액</div><div style="flex:1; {th_lk} text-align:right;">유출액</div><div style="flex:0.8; {th_lk} text-align:right;">유출율</div><div style="flex:2; {th_lk} padding-left:12px;">수주업체</div><div style="flex:2; {th_lk} padding-left:8px;">비고</div></div>'
                    leak_rows = ""
                    for li, lk in enumerate(leaks):
                        분야_l = lk.get("분야", "")
                        계약명_l = lk.get("계약명", "")[:40]
                        계약액_l = format_억(lk.get("계약액", 0))
                        유출액_l = format_억(lk.get("유출액", 0))
                        유출율_l = lk.get("유출율", 0)
                        수주업체_l = lk.get("수주업체", "")
                        row_bg = "#fafbfe" if li % 2 == 1 else COLORS["card_bg"]
                        율_clr = COLORS['danger'] if 유출율_l >= 80 else (COLORS['warning'] if 유출율_l >= 50 else COLORS['text_dark'])
                        분야_clr = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}.get(분야_l, COLORS["text_light"])
                        비고_l = lk.get("비고", "")
                        비고_clr = '#e85347' if '비정상' in 비고_l or '미적용' in 비고_l else (COLORS['text_light'] if '정상' in 비고_l or '장기' in 비고_l else COLORS['text_dark'])
                        leak_rows += f'<div style="display:flex; align-items:center; padding:10px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:{row_bg};"><div style="flex:0.8;"><span style="background:{분야_clr}; color:#fff; padding:2px 8px; border-radius:10px; font-size:0.65rem; font-weight:600;">{분야_l}</span></div><div style="flex:2.5; font-size:0.8rem; font-weight:600; color:{COLORS["text_dark"]}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{계약명_l}</div><div style="flex:1; text-align:right; font-size:0.82rem; font-weight:600; font-family:Nunito Sans,sans-serif;">{계약액_l}</div><div style="flex:1; text-align:right; font-size:0.82rem; font-weight:700; color:{COLORS["danger"]}; font-family:Nunito Sans,sans-serif;">{유출액_l}</div><div style="flex:0.8; text-align:right; font-size:0.82rem; font-weight:700; color:{율_clr};">{유출율_l}%</div><div style="flex:2; font-size:0.75rem; color:{COLORS["text_light"]}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-left:12px;">{수주업체_l}</div><div style="flex:2; font-size:0.68rem; font-weight:600; color:{비고_clr}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-left:8px;">{비고_l}</div></div>'
                    st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; box-shadow:0 1px 3px rgba(0,0,0,0.04); overflow:hidden; margin-top:8px;">{leak_header}{col_hdr}{leak_rows}</div>', unsafe_allow_html=True)
                    # Excel 다운로드
                    df_dl = pd.DataFrame(leaks_all)
                    cols_dl = ["분야", "계약명", "계약액", "유출액", "유출율", "수주업체", "비고"]
                    df_dl = df_dl[[c for c in cols_dl if c in df_dl.columns]].copy()
                    import io
                    buf = io.BytesIO()
                    df_dl.to_excel(buf, index=False, engine='openpyxl')
                    
                    st.markdown("---")
                    col_dl1, col_dl2 = st.columns(2)
                    with col_dl1:
                        st.download_button(
                            label=f"📥 {u} 유출계약 엑셀 다운로드",
                            data=buf.getvalue(),
                            file_name=f"{u}_유출계약.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_leak_{u}"
                        )
                        
                    with col_dl2:
                        state_key = f"gen_excel_{u}"
                        if st.button(f"📥 {u} 전체 계약 생성 (지역업체 포함)", key=f"btn_gen_{u}"):
                            st.session_state[state_key] = True
                            
                        if st.session_state.get(state_key):
                            with st.spinner("DB 데이터 조회 및 필터링 중..."):
                                from export_excel import generate_agency_excel
                                excel_buf = generate_agency_excel(u)
                                if excel_buf:
                                    st.download_button(
                                        label=f"✅ 준비 완료! 클릭하여 저장",
                                        data=excel_buf.getvalue(),
                                        file_name=f"{u}_전체계약내역.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key=f"dl_all_{u}"
                                    )
                                else:
                                    st.error("데이터 생성 실패 또는 데이터 없음")
                    st.info(f"{u}의 주요 지역외 유출 계약(기준 충족 건)이 없습니다.")

        if not found:
            st.info(f"'{search_org}' 기관 관련 데이터를 찾을 수 없습니다.")
    else:
        st.info("검색어를 입력하면 해당 기관의 수주현황을 확인할 수 있습니다.")


# ════════════════════════════════════════════
# PAGE: 유출 분석
# ════════════════════════════════════════════
elif page == "🔴 유출계약 분석":
    data_leak = fetch_api("/api/leakage")
    if data_leak:
        st.caption(f"📅 생성: {data_leak.get('generated_at', '')}")

        # ── 종합쇼핑몰 유출품목 테이블 ──
        shop_items_data = data_leak.get("쇼핑몰_유출품목", [])
        if shop_items_data:
            st.markdown(f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px 6px 0 0; padding:16px 20px;">
<span style="font-size:1rem; font-weight:700; color:{COLORS['text_dark']};">🛒 종합쇼핑몰 유출품목</span>
<span style="font-size:0.78rem; color:{COLORS['text_light']}; margin-left:8px;">유출액 기준 상위 10개</span>
</div>""", unsafe_allow_html=True)

            th_s = f'font-size:0.82rem; font-weight:700; color:{COLORS["text_dark"]}; letter-spacing:0.03em; padding:12px 0;'
            t_header = f'''<div style="display:flex; align-items:center; padding:0 20px; border-bottom:1px solid {COLORS["card_border"]};">
<div style="flex:0.3; {th_s} text-align:center;">순위</div>
<div style="flex:2.5; {th_s}">품목명</div>
<div style="flex:1.5; {th_s}">주요 수요기관</div>
<div style="flex:1.2; {th_s} text-align:right;">계약액</div>
<div style="flex:1.2; {th_s} text-align:right;">지역외 수주액</div>
<div style="flex:0.7; {th_s} text-align:right;">유출 비중</div>
<div style="flex:0.7; {th_s} text-align:right;">부산업체</div>
</div>'''
            t_rows = ""
            for idx, item in enumerate(shop_items_data):
                nm = item.get("품목명", "")
                agency = item.get("주요수요기관", "")
                tot = item.get("총액", 0)
                leak = item.get("유출액", 0)
                rate = item.get("유출율", 0)
                supplier = item.get("부산공급업체", 0)
                row_bg = "#fafbfe" if idx % 2 == 1 else COLORS["card_bg"]
                rc = COLORS['danger'] if rate >= 80 else (COLORS['warning'] if rate >= 50 else COLORS['text_dark'])
                sc_txt = f'<span style="color:{COLORS["danger"]}; font-weight:700;">0</span>' if supplier == 0 else f'{supplier}'
                rank_bg = "#e85347" if idx < 3 else COLORS["text_light"]
                t_rows += f'''<div style="display:flex; align-items:center; padding:13px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:{row_bg}; transition:background 0.15s;" onmouseover="this.style.background='#f0f2ff'" onmouseout="this.style.background='{row_bg}'">
<div style="flex:0.3; text-align:center;"><span style="display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; border-radius:50%; background:{rank_bg}; color:#fff; font-size:0.68rem; font-weight:700;">{idx+1}</span></div>
<div style="flex:2.5; font-size:0.85rem; font-weight:600; color:{COLORS['text_dark']}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{nm}</div>
<div style="flex:1.5; font-size:0.85rem; font-weight:600; color:{COLORS['text_dark']}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-family:Nunito Sans,sans-serif;">{agency}</div>
<div style="flex:1.2; text-align:right; font-size:0.85rem; font-weight:600; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(tot)}</div>
<div style="flex:1.2; text-align:right; font-size:0.85rem; font-weight:700; color:#e85347; font-family:Nunito Sans,sans-serif;">{format_억(leak)}</div>
<div style="flex:0.7; text-align:right; font-size:0.85rem; font-weight:700; color:{rc};">{rate}%</div>
<div style="flex:0.7; text-align:right; font-size:0.85rem; font-weight:500; color:{COLORS['primary']};">{sc_txt}개</div>
</div>'''
            st.markdown(f'''<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:0 0 6px 6px; box-shadow:0 1px 3px rgba(0,0,0,0.04); overflow:hidden;">
{t_header}{t_rows}
</div>''', unsafe_allow_html=True)

            # ── 품목 선택 → 부산업체 리스트 ──
            item_names = [i.get("품목명", "") for i in shop_items_data]
            selected = st.selectbox("🏢 부산 공급업체 조회", ["선택하세요"] + item_names, key="leak_item_select")
            if selected and selected != "선택하세요":
                matched = [i for i in shop_items_data if i.get("품목명") == selected]
                if matched:
                    item_info = matched[0]
                    names = item_info.get("부산업체명", [])
                    if names:
                        # names는 dict 리스트 또는 문자열 리스트 (하위호환)
                        display_names = [n.get("업체명", n) if isinstance(n, dict) else n for n in names]
                        chips = " ".join([f'<span style="display:inline-block; padding:4px 10px; margin:3px; border-radius:4px; background:#e8eaff; color:#6576ff; font-size:0.78rem; font-weight:600;">{n}</span>' for n in display_names])
                        st.markdown(f'<div style="padding:12px; background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px;">{chips}</div>', unsafe_allow_html=True)
                        
                        # Excel 다운로드 (상세 정보 포함)
                        import io
                        if names and isinstance(names[0], dict):
                            df_suppliers = pd.DataFrame([{
                                "순번": i + 1,
                                "업체명": n.get("업체명", ""),
                                "대표자": n.get("대표자", ""),
                                "주소": n.get("주소", ""),
                                "대표품명": n.get("대표품명", ""),
                                "대표업종": n.get("대표업종", ""),
                                "본사/지사": n.get("본사구분", ""),
                                "개업일": n.get("개업일", ""),
                            } for i, n in enumerate(names)])
                        else:
                            df_suppliers = pd.DataFrame({
                                "순번": range(1, len(names) + 1),
                                "부산 공급업체명": display_names,
                                "품목": [selected] * len(names),
                            })
                        # 요약 시트용 데이터
                        df_summary = pd.DataFrame([{
                            "품목명": selected,
                            "총 계약액": item_info.get("총액", 0),
                            "유출액": item_info.get("유출액", 0),
                            "유출율(%)": item_info.get("유출율", 0),
                            "유출건수": item_info.get("유출건수", 0),
                            "주요 수요기관": item_info.get("주요수요기관", ""),
                            "부산 공급업체 수": item_info.get("부산공급업체", len(names)),
                        }])
                        buf = io.BytesIO()
                        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                            df_suppliers.to_excel(writer, sheet_name='부산공급업체', index=False)
                            df_summary.to_excel(writer, sheet_name='품목요약', index=False)
                        st.download_button(
                            label=f"📥 {selected} 부산업체 목록 다운로드 ({len(names)}개사)",
                            data=buf.getvalue(),
                            file_name=f"부산공급업체_{selected}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="leak_item_download"
                        )
                    else:
                        st.info(f"'{selected}' 품목에 등록된 부산 공급업체가 없습니다.")

        # ── 하단: 주요 유출계약 ──
        st.markdown(f'<div style="margin-top:28px;"></div>', unsafe_allow_html=True)
        st.markdown(f"""<div style="font-size:1.05rem; font-weight:700; color:{COLORS['text_dark']}; margin-bottom:14px;">🔴 주요 지역외 유출계약</div>""", unsafe_allow_html=True)
        contracts = data_leak.get("유출계약", [])
        if contracts:
            df_ct = pd.DataFrame(contracts)
            type_cfg = {
                "공사": {"icon": "🔧", "color": "#6576ff", "bg": "#e8eaff"},
                "용역": {"icon": "📋", "color": "#8B5CF6", "bg": "#efe5ff"},
                "물품": {"icon": "📦", "color": "#1ee0ac", "bg": "#e0fff5"},
                "쇼핑몰": {"icon": "🛒", "color": "#f4bd0e", "bg": "#fff8e0"},
            }
            group_cfg = {
                "부산광역시 및 소속기관": {"icon": "🏛️", "label": "부산시 및 소관기관", "gradient": "linear-gradient(135deg, #6576ff 0%, #8B5CF6 100%)"},
                "정부 및 국가공공기관": {"icon": "🇰🇷", "label": "정부 및 국가공공기관", "gradient": "linear-gradient(135deg, #364a63 0%, #526484 100%)"},
            }
            g_col1, g_col2 = st.columns(2)
            for g_name, col in [("부산광역시 및 소속기관", g_col1), ("정부 및 국가공공기관", g_col2)]:
                with col:
                    gc = group_cfg.get(g_name, {})
                    st.markdown(f'''<div style="background:{gc.get('gradient','')}; border-radius:6px 6px 0 0; padding:14px 20px; display:flex; align-items:center; gap:10px;">
<span style="font-size:1.2rem;">{gc.get('icon','')}</span>
<span style="font-size:0.95rem; font-weight:700; color:#fff;">{gc.get('label','')}</span>
</div>''', unsafe_allow_html=True)
                    df_g = df_ct[df_ct["그룹"] == g_name] if "그룹" in df_ct.columns else df_ct
                    if not df_g.empty:
                        df_g = df_g.sort_values("유출액", ascending=False).head(10)
                        th_c = f'font-size:0.68rem; font-weight:600; color:{COLORS["text_light"]}; padding:10px 0; letter-spacing:0.02em;'
                        ct_header = f'''<div style="display:flex; align-items:center; padding:0 16px; border-bottom:1px solid {COLORS['card_border']}; background:#fafbfe;">
<div style="flex:0.6; {th_c}">유형</div>
<div style="flex:3; {th_c}">계약명</div>
<div style="flex:1.2; {th_c}">발주기관</div>
<div style="flex:1; {th_c} text-align:right;">계약금액</div>
<div style="flex:1; {th_c} text-align:right;">지역외 수주액</div>
</div>'''
                        ct_rows = ""
                        for ci, (_, cr) in enumerate(df_g.iterrows()):
                            s = cr.get("분야", "")
                            tc = type_cfg.get(s, {"icon":"📄","color":"#6576ff","bg":"#e8eaff"})
                            rbg = "#fafbfe" if ci % 2 == 1 else COLORS["card_bg"]
                            amt = cr.get("계약액", 0)
                            leak = cr.get("유출액", 0)
                            agency = cr.get("수요기관", "")
                            ct_rows += f'''<div style="display:flex; align-items:center; padding:12px 16px; border-bottom:1px solid {COLORS['card_border']}; background:{rbg}; transition:background 0.15s;" onmouseover="this.style.background='#f0f2ff'" onmouseout="this.style.background='{rbg}'">
<div style="flex:0.6; display:flex; align-items:center; gap:5px;">
<span style="display:inline-flex; align-items:center; justify-content:center; width:26px; height:26px; border-radius:50%; background:{tc['bg']}; font-size:0.75rem;">{tc['icon']}</span>
<span style="font-size:0.68rem; font-weight:700; color:{tc['color']};">{s}</span>
</div>
<div style="flex:3; font-size:0.8rem; font-weight:600; color:{COLORS['text_dark']}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-right:8px;">{cr.get('계약명','')}</div>
<div style="flex:1.2; font-size:0.75rem; color:{COLORS['text_light']}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{agency}</div>
<div style="flex:1; text-align:right; font-size:0.8rem; font-weight:600; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(amt)}</div>
<div style="flex:1; text-align:right; font-size:0.8rem; font-weight:700; color:#e85347; font-family:Nunito Sans,sans-serif;">{format_억(leak)}</div>
</div>'''
                        st.markdown(f'''<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:0 0 6px 6px; overflow:hidden; box-shadow:0 2px 6px rgba(0,0,0,0.06); max-height:520px; overflow-y:auto;">
{ct_header}{ct_rows}
</div>''', unsafe_allow_html=True)
                    else:
                        st.markdown(f'''<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:0 0 6px 6px; padding:40px; text-align:center; color:{COLORS['text_light']};">
유출계약 데이터가 없습니다.
</div>''', unsafe_allow_html=True)


# ════════════════════════════════════════════
# PAGE: 보호제도
# ════════════════════════════════════════════
elif page == "🛡️ 지역업체 보호제도":
    data_prot = fetch_api("/api/protection")
    if data_prot:
        st.caption(f"📅 생성: {data_prot.get('generated_at', '')}")
        현황 = data_prot.get("현황", {})

        def make_donut_card(title, icon, gradient, data_dict, criteria=""):
            if not data_dict:
                return
            st.markdown(f'''<div style="background:{gradient}; border-radius:6px 6px 0 0; padding:14px 20px; display:flex; align-items:center; gap:10px;">
<span style="font-size:1.2rem;">{icon}</span>
<span style="font-size:0.95rem; font-weight:700; color:#fff;">{title}</span>
</div>''', unsafe_allow_html=True)
            total_under = sum(v.get("기준이하", 0) for v in data_dict.values())
            total_applied = sum(v.get("지역제한", 0) for v in data_dict.values()) + sum(v.get("의무공동", 0) for v in data_dict.values())
            total_unapplied = sum(v.get("미적용", 0) for v in data_dict.values())
            total_unapplied_amt = sum(v.get("미적용액", 0) for v in data_dict.values())
            unapply_rate = round(total_unapplied / total_under * 100, 1) if total_under > 0 else 0
            fig = go.Figure(data=[go.Pie(
                labels=["지역제한경쟁입찰 가능 계약", "미적용"],
                values=[total_applied, total_unapplied],
                hole=0.65,
                marker=dict(colors=["#6576ff", "#e85347"]),
                textinfo='none',
                hovertemplate='%{label}: %{value}건 (%{percent})<extra></extra>',
            )])
            fig.update_layout(
                showlegend=False, margin=dict(t=10, b=10, l=10, r=10), height=220,
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                annotations=[dict(
                    text=f'<b style="font-size:1.3rem;color:#e85347">{unapply_rate}%</b><br><span style="font-size:0.65rem;color:{COLORS["text_light"]}">미적용 비율</span>',
                    x=0.5, y=0.5, font_size=12, showarrow=False,
                )]
            )
            c_crit, c_left, c_right = st.columns([2, 3, 5])
            with c_crit:
                st.markdown('<div style="margin-top:24px;"></div>', unsafe_allow_html=True)
                if criteria:
                    st.markdown(f'<div style="margin-top:60px; padding:12px 14px; background:#f8f9fc; border-radius:6px; border-left:3px solid {COLORS["primary"]};">{criteria}</div>', unsafe_allow_html=True)
            with c_left:
                st.markdown('<div style="margin-top:24px;"></div>', unsafe_allow_html=True)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                total_items = total_applied + total_unapplied
                pct_a = round(total_applied / total_items * 100, 1) if total_items > 0 else 0
                pct_u = round(total_unapplied / total_items * 100, 1) if total_items > 0 else 0
                st.markdown(f'''<div style="padding:0 8px;"><div style="display:flex; gap:20px; flex-wrap:wrap;">
<div style="display:flex; align-items:center; gap:8px;"><span style="display:inline-block; width:10px; height:10px; border-radius:50%; background:#6576ff;"></span><div>
<div style="font-size:0.75rem; color:{COLORS['text_light']};">지역제한 적용</div>
<div style="font-size:1rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{total_applied:,} <span style="font-size:0.78rem; font-weight:500; color:{COLORS['text_light']};">{pct_a}%</span></div>
</div></div>
<div style="display:flex; align-items:center; gap:8px;"><span style="display:inline-block; width:10px; height:10px; border-radius:50%; background:#e85347;"></span><div>
<div style="font-size:0.75rem; color:{COLORS['text_light']};">미적용</div>
<div style="font-size:1rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{total_unapplied:,} <span style="font-size:0.78rem; font-weight:500; color:{COLORS['text_light']};">{pct_u}%</span></div>
</div></div>
</div><div style="margin-top:8px; font-size:0.75rem; color:{COLORS['text_light']};">미적용 금액: <span style="font-weight:700; color:#e85347;">{format_억(total_unapplied_amt)}</span></div>
</div>''', unsafe_allow_html=True)
            with c_right:
                st.markdown('<div style="margin-top:24px;"></div>', unsafe_allow_html=True)
                th_c = f'font-size:0.82rem; font-weight:600; color:{COLORS["text_light"]}; padding:12px 0; letter-spacing:0.02em;'
                t_header = f'''<div style="display:flex; align-items:center; padding:0 16px; border-bottom:1px solid {COLORS['card_border']}; background:#fafbfe;">
<div style="flex:1.2; {th_c}">분야</div>
<div style="flex:1.2; {th_c} text-align:right;">적용가능 계약</div>
<div style="flex:1; {th_c} text-align:right;">지역제한적용</div>
<div style="flex:1; {th_c} text-align:right;">지역제한 미적용</div>
<div style="flex:1; {th_c} text-align:right;">적용률</div>
</div>'''
                t_rows = ""
                sec_colors = {"종합공사": "#6576ff", "전문공사": "#8B5CF6", "용역": "#1ee0ac"}
                sec_bg = {"종합공사": "#e8eaff", "전문공사": "#efe5ff", "용역": "#e0fff5"}
                for ci, (typ, vals) in enumerate(data_dict.items()):
                    under = vals.get("기준이하", 0)
                    applied = vals.get("지역제한", 0) + vals.get("의무공동", 0)
                    unapplied = vals.get("미적용", 0)
                    r = round(applied / under * 100, 1) if under > 0 else 0
                    rbg = "#fafbfe" if ci % 2 == 1 else COLORS["card_bg"]
                    sc = sec_colors.get(typ, "#6576ff")
                    sb = sec_bg.get(typ, "#e8eaff")
                    rc = COLORS['success'] if r >= 80 else (COLORS['warning'] if r >= 50 else COLORS['danger'])
                    t_rows += f'''<div style="display:flex; align-items:center; padding:14px 16px; border-bottom:1px solid {COLORS['card_border']}; background:{rbg};">
<div style="flex:1.2;"><span style="display:inline-block; padding:4px 12px; border-radius:3px; background:{sb}; color:{sc}; font-size:0.85rem; font-weight:700;">{typ}</span></div>
<div style="flex:1; text-align:right; font-size:1rem; font-weight:600; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{under:,}</div>
<div style="flex:1; text-align:right; font-size:1rem; font-weight:700; color:{COLORS['primary']}; font-family:Nunito Sans,sans-serif;">{applied:,}</div>
<div style="flex:1; text-align:right; font-size:1rem; font-weight:700; color:#e85347; font-family:Nunito Sans,sans-serif;">{unapplied:,}</div>
<div style="flex:1; text-align:right; font-size:1rem; font-weight:700; color:{rc};">{r}%</div>
</div>'''
                st.markdown(f'''<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:0 0 6px 6px; overflow:hidden; box-shadow:0 2px 6px rgba(0,0,0,0.06);">
{t_header}{t_rows}
</div>''', unsafe_allow_html=True)
                return title

        국가 = 현황.get("정부 및 국가공공기관", {})
        gov_criteria = f'<div style="font-size:0.88rem; font-weight:700; color:{COLORS["text_dark"]}; margin-bottom:6px;">국가계약법 지역제한 기준</div><div style="font-size:0.82rem; color:{COLORS["text_dark"]}; line-height:1.8;">· 종합공사: 추정가격 <b>88억</b> 이하<br>· 전문공사: 추정가격 <b>10억</b> 이하<br>· 용역: 추정가격 <b>2.2억</b> 이하</div>'
        gov_card_title = make_donut_card("정부 및 국가공공기관", '<img src="https://www.mois.go.kr/frt2022/main/img/common/logo.png" style="height:22px; width:22px; object-fit:cover; object-position:left;">', "linear-gradient(135deg, #364a63 0%, #526484 100%)", 국가, gov_criteria)

        # ── 정부 미적용 계약 상세 조회 ──
        주요미적용_all = data_prot.get("미적용_건", [])
        if 주요미적용_all and 국가:
            gov_미적용 = []
            for c in 주요미적용_all:
                vals = list(c.values())
                grp_val = str(vals[4]) if len(vals) > 4 else ""
                if "정부" in grp_val or "국가" in grp_val:
                    gov_미적용.append(vals)
            gov_미적용.sort(key=lambda v: v[3] if len(v) > 3 else 0, reverse=True)
            if gov_미적용:
                with st.expander(f"🔍 정부 및 국가공공기관 미적용 계약 상세 ({len(gov_미적용)}건)", expanded=False):
                    for vi, vals in enumerate(gov_미적용[:30]):
                        ct_name = str(vals[2]) if len(vals) > 2 else ""
                        ct_amt = vals[3] if len(vals) > 3 else 0
                        ct_agency = str(vals[6]) if len(vals) > 6 else (str(vals[5]) if len(vals) > 5 else "")
                        ct_sector = str(vals[0]) if len(vals) > 0 else ""
                        ct_method = str(vals[1]) if len(vals) > 1 else ""
                        rbg = "#fafbfe" if vi % 2 == 1 else COLORS["card_bg"]
                        st.markdown(f'''<div style="display:flex; align-items:center; padding:12px 16px; border-bottom:1px solid {COLORS['card_border']}; background:{rbg};">
<div style="flex:0.3; font-size:0.82rem; font-weight:600; color:{COLORS['text_light']};">{vi+1}</div>
<div style="flex:3; font-size:0.88rem; font-weight:600; color:{COLORS['text_dark']};">{ct_name}</div>
<div style="flex:1.2; font-size:0.82rem; font-weight:600; color:{COLORS['text_dark']};">{ct_agency}</div>
<div style="flex:0.6;"><span style="padding:2px 8px; border-radius:3px; background:#e9ecef; color:#364a63; font-size:0.75rem; font-weight:600;">{ct_sector}</span></div>
<div style="flex:1; text-align:right; font-size:0.95rem; font-weight:700; color:#e85347; font-family:Nunito Sans,sans-serif;">{format_억(ct_amt)}</div>
</div>''', unsafe_allow_html=True)

        st.markdown('<div style="margin-top:24px;"></div>', unsafe_allow_html=True)
        부산 = 현황.get("부산시 및 소관기관_지역제한", {})
        busan_criteria = f'<div style="font-size:0.88rem; font-weight:700; color:{COLORS["text_dark"]}; margin-bottom:6px;">지방계약법 지역제한 기준</div><div style="font-size:0.82rem; color:{COLORS["text_dark"]}; line-height:1.8;">· 종합공사: 추정가격 <b>100억</b> 이하<br>· 전문공사: 추정가격 <b>10억</b> 이하<br>· 용역: 추정가격 <b>3.3억</b> 이하</div>'
        busan_card_title = make_donut_card("부산시 및 소관기관", '<img src="https://www.busan.go.kr/humanframe/global/assets/img/common/busan_logo.svg" style="height:22px; width:22px; object-fit:cover; object-position:left;">', "linear-gradient(135deg, #6576ff 0%, #8B5CF6 100%)", 부산, busan_criteria)

        # ── 부산시 미적용 계약 상세 조회 ──
        if 주요미적용_all and 부산:
            busan_미적용 = []
            for c in 주요미적용_all:
                vals = list(c.values())
                grp_val = str(vals[4]) if len(vals) > 4 else ""
                if "부산" in grp_val:
                    busan_미적용.append(vals)
            busan_미적용.sort(key=lambda v: v[3] if len(v) > 3 else 0, reverse=True)
            if busan_미적용:
                with st.expander(f"🔍 부산시 및 소관기관 미적용 계약 상세 ({len(busan_미적용)}건)", expanded=False):
                    for vi, vals in enumerate(busan_미적용[:30]):
                        ct_name = str(vals[2]) if len(vals) > 2 else ""
                        ct_amt = vals[3] if len(vals) > 3 else 0
                        ct_agency = str(vals[6]) if len(vals) > 6 else (str(vals[5]) if len(vals) > 5 else "")
                        ct_sector = str(vals[0]) if len(vals) > 0 else ""
                        ct_method = str(vals[1]) if len(vals) > 1 else ""
                        rbg = "#fafbfe" if vi % 2 == 1 else COLORS["card_bg"]
                        st.markdown(f'''<div style="display:flex; align-items:center; padding:12px 16px; border-bottom:1px solid {COLORS['card_border']}; background:{rbg};">
<div style="flex:0.3; font-size:0.82rem; font-weight:600; color:{COLORS['text_light']};">{vi+1}</div>
<div style="flex:3; font-size:0.88rem; font-weight:600; color:{COLORS['text_dark']};">{ct_name}</div>
<div style="flex:1.2; font-size:0.82rem; font-weight:600; color:{COLORS['text_dark']};">{ct_agency}</div>
<div style="flex:0.6;"><span style="padding:2px 8px; border-radius:3px; background:#e8eaff; color:#6576ff; font-size:0.75rem; font-weight:600;">{ct_sector}</span></div>
<div style="flex:1; text-align:right; font-size:0.95rem; font-weight:700; color:#e85347; font-family:Nunito Sans,sans-serif;">{format_억(ct_amt)}</div>
</div>''', unsafe_allow_html=True)

        st.markdown('<div style="margin-top:24px;"></div>', unsafe_allow_html=True)

        st.markdown(f'''<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px 6px 0 0; padding:16px 20px; display:flex; justify-content:space-between; align-items:center;">
<span style="font-size:1.05rem; font-weight:700; color:{COLORS['text_dark']};">⚠️ 보호제도 미적용 기관</span>
<span style="font-size:0.75rem; color:{COLORS['text_light']};">미적용률 높은 순</span>
</div>''', unsafe_allow_html=True)
        기관별_전체 = data_prot.get("기관별_미적용", [])
        기관별_유효 = [c for c in 기관별_전체 if (c.get("미적용", 0) if isinstance(c, dict) else (c[4] if len(c)>4 else 0)) > 0]
        기관별 = 기관별_유효[:20]
        if 기관별:
            df_org = pd.DataFrame(기관별)
            if len(df_org.columns) >= 7:
                df_org = df_org.sort_values(df_org.columns[6], ascending=False)
            avatar_colors = ["#6576ff", "#8B5CF6", "#1ee0ac", "#e85347", "#f4bd0e", "#364a63", "#09c2de", "#ff63a5"]
            th_s = f'font-size:0.82rem; font-weight:700; color:{COLORS["text_dark"]}; padding:12px 0; letter-spacing:0.02em;'
            org_header = f'''<div style="display:flex; align-items:center; padding:0 20px; border-bottom:1px solid {COLORS['card_border']}; background:#fafbfe;">
<div style="flex:2; {th_s}">수요기관명</div>
<div style="flex:1; {th_s}">적용법규</div>
<div style="flex:1; {th_s} text-align:right;">적용가능(모수)</div>
<div style="flex:1; {th_s} text-align:right;">적용</div>
<div style="flex:1; {th_s} text-align:right;">미적용</div>
<div style="flex:1.2; {th_s} text-align:right;">미적용 금액</div>
<div style="flex:1; {th_s} text-align:right;">미적용률</div>
</div>'''
            # 그룹 컬럼 찾기 (encoding mismatch 방지)
            grp_col = df_org.columns[1] if len(df_org.columns) > 1 else None
            org_rows = ""
            for idx, (_, row) in enumerate(df_org.iterrows()):
                name = row.iloc[0] if len(row) > 0 else ""
                grp = str(row[grp_col]) if grp_col else ""
                total = int(row.iloc[2]) if len(row) > 2 else 0
                applied = int(row.iloc[3]) if len(row) > 3 else 0
                unapplied = int(row.iloc[4]) if len(row) > 4 else 0
                amt = row.iloc[5] if len(row) > 5 else 0
                rate = row.iloc[6] if len(row) > 6 else 0
                initials = name[:2] if len(name) >= 2 else name
                ac = avatar_colors[idx % len(avatar_colors)]
                rbg = "#fafbfe" if idx % 2 == 1 else COLORS["card_bg"]
                grp_short = "지방계약법" if "부산" in grp else "국가계약법"
                grp_color = "#6576ff" if "부산" in grp else "#364a63"
                grp_bg = "#e8eaff" if "부산" in grp else "#e9ecef"
                rate_color = COLORS['danger'] if rate >= 50 else (COLORS['warning'] if rate >= 30 else COLORS['success'])
                status_label = "위험" if rate >= 50 else ("주의" if rate >= 30 else "양호")
                status_dot_color = rate_color
                org_rows += f'''<div style="display:flex; align-items:center; padding:14px 20px; border-bottom:1px solid {COLORS['card_border']}; background:{rbg}; transition:background 0.15s;" onmouseover="this.style.background='#f0f2ff'" onmouseout="this.style.background='{rbg}'">
<div style="flex:2; font-size:0.88rem; font-weight:700; color:{COLORS['text_dark']};">{name}</div>
<div style="flex:1;"><span style="display:inline-block; padding:2px 8px; border-radius:3px; background:{grp_bg}; color:{grp_color}; font-size:0.65rem; font-weight:600;">{grp_short}</span></div>
<div style="flex:1; text-align:right; font-size:0.85rem; font-weight:600; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{total:,}</div>
<div style="flex:1; text-align:right; font-size:0.85rem; font-weight:700; color:{COLORS['primary']}; font-family:Nunito Sans,sans-serif;">{applied:,}</div>
<div style="flex:1; text-align:right; font-size:0.85rem; font-weight:700; color:#e85347; font-family:Nunito Sans,sans-serif;">{unapplied:,}</div>
<div style="flex:1.2; text-align:right; font-size:0.85rem; font-weight:600; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(amt)}</div>
<div style="flex:1; text-align:right;">
<span style="display:inline-flex; align-items:center; gap:5px; padding:3px 10px; border-radius:3px; background:{rate_color}15; font-size:0.78rem; font-weight:700; color:{rate_color};">
<span style="display:inline-block; width:6px; height:6px; border-radius:50%; background:{status_dot_color};"></span>{rate}%
</span>
</div>
</div>'''
            st.markdown(f'''<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:0 0 6px 6px; overflow:hidden; box-shadow:0 2px 6px rgba(0,0,0,0.06); max-height:680px; overflow-y:auto;">
{org_header}{org_rows}
</div>''', unsafe_allow_html=True)

        # ── 기관별 세부 위반내역 검색 및 다운로드 ──
        st.markdown('<div style="margin-top:24px;"></div>', unsafe_allow_html=True)
        st.markdown(f'''<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px 6px 0 0; padding:16px 20px; display:flex; justify-content:space-between; align-items:center;">
<span style="font-size:1.05rem; font-weight:700; color:{COLORS['text_dark']};">🔍 기관별 상세 검색 및 다운로드</span>
</div>''', unsafe_allow_html=True)
        
        with st.container():
            search_kw_prot = st.text_input("검색할 수요기관명을 입력하세요. (예: 부산교통공사)", key="search_kw_prot").strip()
            
            if search_kw_prot:
                # 1. 수요기관 보호제도 적용 스탯 요약카드 표시
                matched_org_stats = []
                for o in 기관별_전체:
                    org_name = str(o.get('기관', '')) if isinstance(o, dict) else str(o[0] if len(o)>0 else '')
                    if search_kw_prot in org_name:
                        matched_org_stats.append(o)
                
                if matched_org_stats:
                    st.markdown(f"#### 🏢 '{search_kw_prot}' 관련 기관 현황 요약 ({len(matched_org_stats)}개 기관)")
                    for stat in matched_org_stats:
                        if isinstance(stat, dict):
                            org_name = stat.get('기관', '')
                            total = stat.get('기준이하', 0)
                            applied = stat.get('적용', 0)
                            unapplied = stat.get('미적용', 0)
                        else:
                            org_name = stat[0] if len(stat)>0 else ''
                            total = stat[2] if len(stat)>2 else 0
                            applied = stat[3] if len(stat)>3 else 0
                            unapplied = stat[4] if len(stat)>4 else 0
                            
                        st.markdown(f'''<div style="display:flex; gap:12px; margin-bottom:16px;">
<div style="flex:1.5; padding:14px; background:#f8f9fa; border:1px solid #e9ecef; border-radius:6px;">
<div style="font-size:0.8rem; color:#6c757d; margin-bottom:4px; font-weight:600;">수요기관</div>
<div style="font-size:1.05rem; font-weight:700; color:#343a40;">{org_name}</div>
</div>
<div style="flex:1; padding:14px; background:#f8f9fa; border:1px solid #e9ecef; border-radius:6px; text-align:right;">
<div style="font-size:0.8rem; color:#6c757d; margin-bottom:4px; font-weight:600;">적용가능(모수)</div>
<div style="font-size:1.2rem; font-weight:700; color:#495057;">{total:,}건</div>
</div>
<div style="flex:1; padding:14px; background:#e0fff5; border:1px solid #1ee0ac; border-radius:6px; text-align:right;">
<div style="font-size:0.8rem; color:#10b981; margin-bottom:4px; font-weight:600;">제도 적용(준수)</div>
<div style="font-size:1.2rem; font-weight:700; color:#059669;">{applied:,}건</div>
</div>
<div style="flex:1; padding:14px; background:#ffebeb; border:1px solid #ffb8b8; border-radius:6px; text-align:right;">
<div style="font-size:0.8rem; color:#e85347; margin-bottom:4px; font-weight:600;">미적용(위반)</div>
<div style="font-size:1.2rem; font-weight:700; color:#dc2626;">{unapplied:,}건</div>
</div>
</div>''', unsafe_allow_html=True)
                else:
                    st.info(f"'{search_kw_prot}'에 해당하는 기관 실적 데이터가 없습니다.")

                # 2. 계약 상세내역 및 다운로드
                all_violations = data_prot.get("미적용_건", [])
                filtered_dicts = []
                for c in all_violations:
                    if isinstance(c, dict):
                        agency_val = str(c.get("수요기관", ""))
                        unit_val = str(c.get("비교단위", ""))
                        if search_kw_prot in agency_val or search_kw_prot in unit_val:
                            filtered_dicts.append(c)
                    else:
                        agency_val = str(c[5]) if len(c) > 5 else ""
                        unit_val = str(c[6]) if len(c) > 6 else ""
                        if search_kw_prot in agency_val or search_kw_prot in unit_val:
                            cols = ["분야", "계약방식", "공고명", "추정가격", "기관그룹", "수요기관", "비교단위", "수주업체", "비고"]
                            filtered_dicts.append(dict(zip(cols, c)))
                
                if filtered_dicts:
                    df_filtered_prot = pd.DataFrame(filtered_dicts)
                    st.markdown(f"**상세 유출(미적용) 계약 내역 ({len(filtered_dicts)}건)**")
                    st.dataframe(df_filtered_prot, use_container_width=True)
                    
                    import io
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        df_filtered_prot.to_excel(writer, index=False, sheet_name='위반계약_목록')
                    st.download_button(
                        label="📥 검색결과 엑셀 다운로드",
                        data=excel_buffer.getvalue(),
                        file_name=f"보호제도_위반계약_{search_kw_prot}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:
                    st.info("해당 기관의 상세 위반(미적용) 계약 내역이 없습니다.")

# ════════════════════════════════════════════
# PAGE: 수의계약
# ════════════════════════════════════════════
elif page == "📝 수의계약":
    data_pvt = fetch_api("/api/private-contract")
    # 유출 데이터는 /api/summary에서 직접 가져오기 (encoding 문제 우회)
    _summary_pvt = fetch_api("/api/summary")
    _summary_vals = list(_summary_pvt.values()) if _summary_pvt else []
    _유출_계약 = _summary_vals[12] if len(_summary_vals) > 12 and isinstance(_summary_vals[12], list) else []
    _유출_기관 = _summary_vals[13] if len(_summary_vals) > 13 and isinstance(_summary_vals[13], list) else []
    if data_pvt:
        st.caption(f"📅 생성: {data_pvt.get('generated_at', '')}")

        수의 = data_pvt.get("수의계약", {})
        if 수의:
            # 분야별 합산 (그룹 합계)
            sector_totals = {}
            for key, vals in 수의.items():
                kv = list(vals.values()) if isinstance(vals, dict) else []
                parts = key.split("_")
                sector = parts[1] if len(parts) >= 2 else ""
                if sector not in sector_totals:
                    sector_totals[sector] = {"total": 0, "busan": 0, "non_busan": 0, "busan_amt": 0, "non_busan_amt": 0}
                sector_totals[sector]["total"] += vals.get("total", 0)
                sector_totals[sector]["busan"] += vals.get("busan", 0)
                sector_totals[sector]["non_busan"] += vals.get("non_busan", 0)
                sector_totals[sector]["busan_amt"] += vals.get("busan_amt", 0)
                sector_totals[sector]["non_busan_amt"] += vals.get("non_busan_amt", 0)
            
            # 전체 합계
            grand_total_amt = sum(s["busan_amt"] + s["non_busan_amt"] for s in sector_totals.values())
            grand_busan_amt = sum(s["busan_amt"] for s in sector_totals.values())
            grand_rate = round(grand_busan_amt / grand_total_amt * 100, 1) if grand_total_amt > 0 else 0
            grand_건수 = sum(s["total"] for s in sector_totals.values())
            grand_부산건 = sum(s["busan"] for s in sector_totals.values())
            
            sec_labels = {"공사": "공사 수의계약액", "용역": "용역 수의계약액", "물품": "물품 수의계약액"}
            
            with st.container(border=True):
                col_hero, col_side = st.columns([5, 5])
                
                with col_hero:
                    sc = COLORS["success"]
                    sub_items = " · ".join(f"{l}({format_억(sector_totals.get(s,{}).get('busan_amt',0)+sector_totals.get(s,{}).get('non_busan_amt',0))})" for s, l in [("공사","공사"),("용역","용역"),("물품","물품")] if s in sector_totals)
                    st.markdown(f"""<div style="background: linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius: 8px 8px 0 0; padding: 24px 28px 18px; box-shadow: 0 4px 20px rgba(35,46,122,0.35);">
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
<span style="font-size:0.9rem; font-weight:700; color:rgba(255,255,255,0.85);">총 수의계약액</span>
<span style="font-size:0.78rem; color:rgba(255,255,255,0.55); font-weight:600;">총 {grand_건수:,}건</span>
</div>
<div style="font-size:2.6rem; font-weight:800; color:#fff; line-height:1; font-family:Nunito Sans,sans-serif; letter-spacing:-0.02em;">{format_조(grand_total_amt)}</div>
<div style="font-size:0.78rem; color:rgba(255,255,255,0.45); margin-top:8px;">{sub_items}</div>
<div style="font-size:0.9rem; font-weight:700; color:rgba(255,255,255,0.85); margin-top:20px;">지역업체 수주액 (수주율)</div>
<div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:8px;">
<div style="font-size:1.6rem; font-weight:700; color:rgba(255,255,255,0.92); font-family:Nunito Sans,sans-serif; line-height:1; letter-spacing:-0.02em;">{format_조(grand_busan_amt)} <span style="color:{sc};">({grand_rate}%)</span></div>
<div style="text-align:right;"><span style="font-size:0.78rem; color:rgba(255,255,255,0.55);">부산업체 {grand_부산건:,}건 / {grand_건수:,}건</span></div>
</div>
</div>""", unsafe_allow_html=True)
                    
                    # 웨이브 스파크라인 (Plotly 미니 area chart)
                    import random, math
                    random.seed(77)
                    wave_y = [30 + 15 * math.sin(i * 0.5) + random.uniform(-5, 5) for i in range(30)]
                    fig_wave = go.Figure()
                    fig_wave.add_trace(go.Scatter(
                        y=wave_y, mode='lines', fill='tozeroy',
                        line=dict(color='rgba(255,255,255,0.25)', width=2, shape='spline'),
                        fillcolor='rgba(255,255,255,0.06)',
                    ))
                    fig_wave.update_layout(
                        height=80, margin=dict(l=0,r=0,t=0,b=0),
                        paper_bgcolor='#3444a8', plot_bgcolor='#3444a8',
                        showlegend=False,
                        xaxis=dict(visible=False), yaxis=dict(visible=False),
                    )
                    st.plotly_chart(fig_wave, use_container_width=True, config={"displayModeBar": False})
                
                with col_side:
                    sc = COLORS["success"]
                    dot_colors = ["#6576ff", "#1ee0ac", "#e85347"]
                    sector_list = [("공사", "공사 수의계약액"), ("용역", "용역 수의계약액"), ("물품", "물품 수의계약액")]
                    bar_sets = [
                        [40, 55, 35, 60, 45, 70, 80],
                        [60, 45, 50, 35, 55, 40, 65],
                        [50, 60, 40, 55, 65, 45, 70],
                    ]
                    cols_2x2 = st.columns(2)
                    for i, (sec_key, sec_label) in enumerate(sector_list):
                        s_data = sector_totals.get(sec_key, {})
                        s_total = s_data.get("busan_amt", 0) + s_data.get("non_busan_amt", 0)
                        s_busan = s_data.get("busan_amt", 0)
                        s_rate = round(s_busan / s_total * 100, 1) if s_total > 0 else 0
                        dc = dot_colors[i % len(dot_colors)]
                        bars = bar_sets[i % len(bar_sets)]
                        bar_html = "".join(f'<div style="width:4px; height:{h}%; background:{dc}; border-radius:2px; opacity:0.7;"></div>' for h in bars)
                        with cols_2x2[i % 2]:
                            st.markdown(f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px; padding:14px 16px; box-shadow:0 1px 3px rgba(0,0,0,0.04); margin-bottom:10px;">
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
<div style="display:flex; align-items:center; gap:6px;">
<span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:{dc};"></span>
<span style="font-size:0.78rem; font-weight:700; color:{COLORS['text_dark']};">{sec_label}</span>
</div>
</div>
<div style="display:flex; justify-content:space-between; align-items:flex-end;">
<div>
<div style="font-size:1.35rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif; line-height:1;">{format_억(s_total)}</div>
<div style="font-size:0.68rem; color:{COLORS['text_light']}; margin-top:6px;">지역업체 수주액</div>
<div style="font-size:0.95rem; font-weight:700; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(s_busan)} ({s_rate}%)</div>
</div>
<div style="display:flex; align-items:flex-end; gap:3px; height:45px;">
{bar_html}
</div>
</div>
</div>""", unsafe_allow_html=True)

            # ── 그룹별 수의계약 현황 (부산시 / 정부) ──
            # 그룹별로 데이터 분리
            group_data = {}
            for key, vals in 수의.items():
                parts = key.split("_")
                if len(parts) < 2:
                    continue
                raw_grp = parts[0]
                sector = parts[1]
                grp_key = "부산" if "부산" in raw_grp else "정부"
                if grp_key not in group_data:
                    group_data[grp_key] = {"total_amt": 0, "busan_amt": 0, "sectors": {}}
                total_a = vals.get("busan_amt", 0) + vals.get("non_busan_amt", 0)
                busan_a = vals.get("busan_amt", 0)
                group_data[grp_key]["total_amt"] += total_a
                group_data[grp_key]["busan_amt"] += busan_a
                group_data[grp_key]["sectors"][sector] = {
                    "total_amt": total_a, "busan_amt": busan_a,
                    "total_cnt": vals.get("total", 0), "busan_cnt": vals.get("busan", 0),
                }
            
            grp_configs = [
                ("부산", "부산시 및 소관기관 수의계약 수주현황", "(지방계약법 적용)", "#6576ff", "#e4e7ff"),
                ("정부", "정부 및 국가공공기관 수의계약 수주현황", "(국가계약법 적용)", "#f4bd0e", "#fef5d5"),
            ]
            
            def make_svg_wave_pvt(pts, color):
                w, h = 80, 28
                max_v = max(pts) if max(pts) > 0 else 1
                min_v = min(pts)
                rng = max_v - min_v if max_v - min_v > 0 else 1
                coords = []
                for j, v in enumerate(pts):
                    x = j * (w / (len(pts) - 1))
                    y = h - ((v - min_v) / rng * (h - 4)) - 2
                    coords.append(f"{x:.1f},{y:.1f}")
                path = "M" + "L".join(coords)
                return f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" style="display:block;"><path d="{path}" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>'
            
            spark_sets = {
                "공사": [65, 72, 68, 74, 70, 73, 72],
                "용역": [55, 50, 53, 48, 52, 50, 52],
                "물품": [58, 62, 55, 60, 57, 61, 59],
            }
            sector_colors = {"공사": "#6576ff", "용역": "#9cabff", "물품": "#1ee0ac"}
            
            for grp_key, title, subtitle, main_color, sub_color in grp_configs:
                gd = group_data.get(grp_key)
                if not gd:
                    continue
                g_total = gd["total_amt"]
                g_busan = gd["busan_amt"]
                g_rate = round(g_busan / g_total * 100, 1) if g_total > 0 else 0
                g_외지 = round(100 - g_rate, 1)
                g_외지액 = g_total - g_busan
                
                st.markdown('<div style="margin-top:20px;"></div>', unsafe_allow_html=True)
                
                with st.container(border=True):
                    col_left, col_right = st.columns(2)
                    
                    with col_left:
                        st.markdown(f"""<div style="padding:20px 0 8px;"><h2 style="margin:0; font-size:1.4rem; font-weight:700; color:{COLORS['text_dark']};">{title}</h2><span style="font-size:0.75rem; color:{COLORS['text_light']};">{subtitle}</span></div>""", unsafe_allow_html=True)
                        
                        dc1, dc2 = st.columns([3, 4])
                        with dc1:
                            fig_donut = go.Figure(go.Pie(
                                labels=["지역업체", "지역외업체"],
                                values=[g_rate, g_외지],
                                hole=0.65,
                                marker=dict(colors=[main_color, sub_color]),
                                textinfo="none",
                                hovertemplate="%{label}: %{value}%<extra></extra>",
                            ))
                            fig_donut.update_layout(
                                showlegend=False,
                                margin=dict(t=5, b=5, l=5, r=5), height=220,
                                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                annotations=[dict(
                                    text=f"<b style='font-size:1.4rem; color:{COLORS['text_dark']};'>{g_rate}%</b>",
                                    x=0.5, y=0.5, showarrow=False, font=dict(size=14, family="Nunito Sans"),
                                )],
                            )
                            st.plotly_chart(fig_donut, use_container_width=True, config={"displayModeBar": False})
                        with dc2:
                            st.markdown(f'<div style="display:flex; flex-direction:column; justify-content:center; height:220px; gap:16px; padding-left:8px;"><div><div style="font-size:0.7rem; font-weight:600; color:{COLORS["text_light"]};">총 수의계약액</div><div style="font-size:1.2rem; font-weight:800; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif; margin-top:2px;">{format_조(g_total)}</div></div><div style="display:flex; align-items:center; gap:8px;"><span style="width:10px; height:10px; border-radius:50%; background:{main_color}; display:inline-block;"></span><div><div style="font-size:0.7rem; font-weight:600; color:{COLORS["text_light"]};">지역업체 수주액</div><div style="font-size:1rem; font-weight:800; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif; margin-top:2px;">{format_조(g_busan)} <span style="color:{main_color};">({g_rate}%)</span></div></div></div><div style="display:flex; align-items:center; gap:8px;"><span style="width:10px; height:10px; border-radius:50%; background:{sub_color}; display:inline-block;"></span><div><div style="font-size:0.7rem; font-weight:600; color:{COLORS["text_light"]};">지역외업체 수주액</div><div style="font-size:1rem; font-weight:800; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif; margin-top:2px;">{format_조(g_외지액)} <span style="color:#aab0c6;">({g_외지}%)</span></div></div></div></div>', unsafe_allow_html=True)
                    
                    with col_right:
                        st.markdown('<div style="padding:20px 0 8px;"></div>', unsafe_allow_html=True)
                        
                        th = f'font-size:0.75rem; font-weight:600; color:{COLORS["text_light"]}; text-transform:uppercase; letter-spacing:0.04em;'
                        header = f'<div style="display:flex; align-items:center; padding:14px 0; border-bottom:1px solid {COLORS["card_border"]};"><div style="flex:2; {th}">분야</div><div style="flex:1.5; {th}">총계약액</div><div style="flex:1.5; {th}">지역업체 수주액</div><div style="flex:1; {th}">비중</div><div style="flex:1.5; {th} text-align:right;">주간추이</div></div>'
                        
                        rows_html = ""
                        for sec_name in ["공사", "용역", "물품"]:
                            sec = gd["sectors"].get(sec_name, {})
                            sec_total = sec.get("total_amt", 0)
                            sec_busan = sec.get("busan_amt", 0)
                            sec_rate = round(sec_busan / sec_total * 100, 1) if sec_total > 0 else 0
                            clr = sector_colors.get(sec_name, "#6576ff")
                            svg = make_svg_wave_pvt(spark_sets.get(sec_name, [50]*7), clr)
                            td = f'font-size:1rem; font-weight:700; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;'
                            rows_html += f'<div style="display:flex; align-items:center; padding:26px 0; border-bottom:1px solid {COLORS["card_border"]};"><div style="flex:2; display:flex; align-items:center; gap:6px;"><span style="width:8px; height:8px; border-radius:50%; background:{clr}; display:inline-block;"></span><span style="{td}">{sec_name}</span></div><div style="flex:1.5; {td}">{format_억(sec_total)}</div><div style="flex:1.5; {td}">{format_억(sec_busan)}</div><div style="flex:1; {td} color:{COLORS["primary"]};">​{sec_rate}%</div><div style="flex:1.5; text-align:right;">{svg}</div></div>'
                        
                        st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; padding:4px 16px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">{header}{rows_html}</div>', unsafe_allow_html=True)

            # ── 주요 유출 수의계약 현황 (좌: 기관별, 우: 계약별) ──
            st.markdown('<div style="margin-top:20px;"></div>', unsafe_allow_html=True)
            유출_list = (_유출_계약 if _유출_계약 else data_pvt.get("유출_수의계약", []))[:10]
            유출_기관 = (_유출_기관 if _유출_기관 else data_pvt.get("유출_기관별", []))[:10]
            
            if 유출_기관 or 유출_list:
                with st.container(border=True):
                    col_ag, col_ct = st.columns(2)
                    
                    # ── 좌측: 기관별 유출액 (DashLite 스타일 progress bars) ──
                    with col_ag:
                        st.markdown(f"""<div style="padding:16px 0 12px;">
<h3 style="margin:0; font-size:1.1rem; font-weight:700; color:{COLORS['text_dark']};">유출액 상위 기관</h3>
<span style="font-size:0.7rem; color:{COLORS['text_light']};">수의계약 비부산업체 수주 금액 기준</span>
</div>""", unsafe_allow_html=True)
                        
                        if 유출_기관:
                            # 최대 금액 (progress bar 비율 계산용)
                            ag_items = []
                            for item in 유출_기관:
                                iv = list(item.values()) if isinstance(item, dict) else []
                                if len(iv) >= 4:
                                    ag_items.append({"name": iv[0], "amt": iv[1], "cnt": iv[2], "grp": iv[3]})
                            
                            max_amt = ag_items[0]["amt"] if ag_items else 1
                            bar_colors = ["#6576ff", "#1ee0ac", "#f4bd0e", "#e85347", "#9cabff",
                                          "#6576ff", "#1ee0ac", "#f4bd0e", "#e85347", "#9cabff"]
                            
                            rows_ag = ""
                            for i, ag in enumerate(ag_items):
                                pct = round(ag["amt"] / max_amt * 100) if max_amt > 0 else 0
                                bc = bar_colors[i % len(bar_colors)]
                                rows_ag += f'''<div style="padding:8px 0; border-bottom:1px solid {COLORS["card_border"]};">
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
<span style="font-size:0.82rem; font-weight:600; color:{COLORS["text_dark"]};">{ag["name"]}</span>
<span style="font-size:0.82rem; font-weight:700; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;">{format_억(ag["amt"])}</span>
</div>
<div style="width:100%; height:5px; background:{COLORS["card_border"]}; border-radius:3px; overflow:hidden;">
<div style="width:{pct}%; height:100%; background:{bc}; border-radius:3px;"></div>
</div>
<div style="font-size:0.6rem; color:{COLORS["text_light"]}; margin-top:2px;">{ag["cnt"]}건 · {ag["grp"]}</div>
</div>'''
                            
                            st.markdown(f'<div style="padding:0 4px;">{rows_ag}</div>', unsafe_allow_html=True)
                            
                            # 기관 선택 드롭다운
                            ag_names = [ag["name"] for ag in ag_items]
                            sel_ag = st.selectbox("🔍 기관 상세 조회", ["선택하세요"] + ag_names, key="suui_leak_agency_sel")
                            if sel_ag and sel_ag != "선택하세요":
                                res_s = fetch_api(f"/api/agency/suui-search?q={sel_ag}")
                                if res_s and "검색결과" in res_s and res_s["검색결과"]:
                                    for u_s, det_s in res_s["검색결과"].items():
                                        r_s = det_s.get("총수주율", 0)
                                        rc_s = rate_color(r_s)
                                        발주_s = det_s.get("총발주액", 0)
                                        수주_s = det_s.get("총수주액", 0)
                                        그룹_s = str(det_s.get("그룹", ""))
                                        법_s = "지방계약법" if "부산" in 그룹_s else "국가계약법"
                                        분야별_s = det_s.get("분야별", {})
                                        sub_ps = [f"{sn}({format_억(sv.get('발주액',0))})" for sn in ["공사","용역","물품","쇼핑몰"] for sv in [분야별_s.get(sn,{})] if sv.get("발주액",0)>0]
                                        st.markdown(f"""<div style="background:linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius:8px; padding:20px 24px 16px; margin-top:8px;">
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
<span style="font-size:1.1rem; font-weight:800; color:#fff;">{u_s} (수의계약)</span>
<span style="font-size:0.72rem; color:rgba(255,255,255,0.55);">({법_s})</span>
</div>
<div style="font-size:0.75rem; font-weight:700; color:rgba(255,255,255,0.85);">총 수의계약액</div>
<div style="font-size:1.8rem; font-weight:800; color:#fff; line-height:1; font-family:Nunito Sans,sans-serif; margin-top:4px;">{format_억(발주_s)}</div>
<div style="font-size:0.65rem; color:rgba(255,255,255,0.45); margin-top:4px;">{' · '.join(sub_ps)}</div>
<div style="font-size:0.75rem; font-weight:700; color:rgba(255,255,255,0.85); margin-top:12px;">지역업체 수주액 (수주율)</div>
<div style="font-size:1.3rem; font-weight:700; color:rgba(255,255,255,0.92); line-height:1; margin-top:4px;">{format_억(수주_s)} <span style="color:{COLORS['success']};">({r_s}%)</span></div>
</div>""", unsafe_allow_html=True)
                                        # 유출계약 리스트
                                        leaks_s = det_s.get("유출계약", {})
                                        all_leaks = []
                                        for sec_name, sec_list in leaks_s.items():
                                            if isinstance(sec_list, list):
                                                all_leaks.extend(sec_list)
                                        all_leaks.sort(key=lambda x: x.get("유출액", 0), reverse=True)
                                        if all_leaks:
                                            disp = all_leaks[:30]
                                            th_sl = f'font-size:0.82rem; font-weight:700; color:{COLORS["text_dark"]}; padding:10px 0;'
                                            lk_hd_s = f'<div style="display:flex; justify-content:space-between; padding:10px 16px; background:linear-gradient(135deg, #e85347 0%, #ff7b6b 100%); border-radius:6px 6px 0 0; margin-top:8px;"><div style="font-size:0.85rem; font-weight:700; color:#fff;">🔴 {u_s} 수의계약 유출 (상위 {len(disp)}건)</div><div style="font-size:0.7rem; color:rgba(255,255,255,0.7);">총 {len(all_leaks)}건</div></div>'
                                            ch_s = f'<div style="display:flex; padding:6px 12px; border-bottom:1px solid {COLORS["card_border"]}; background:#f8f9fc;"><div style="flex:0.6; {th_sl}">분야</div><div style="flex:3; {th_sl}">계약명</div><div style="flex:1; {th_sl} text-align:right;">계약액</div><div style="flex:1; {th_sl} text-align:right;">유출액</div><div style="flex:2; {th_sl} padding-left:8px;">비고</div></div>'
                                            rws_s = ""
                                            for li, lk in enumerate(disp):
                                                fc = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}.get(lk.get("분야",""), COLORS["text_light"])
                                                rb = "#fafbfe" if li%2==1 else COLORS["card_bg"]
                                                _bg = lk.get("비고", "")
                                                _bgc = '#e85347' if '비정상' in _bg or '미적용' in _bg else COLORS['text_dark']
                                                rws_s += f'<div style="display:flex; align-items:center; padding:7px 12px; border-bottom:1px solid {COLORS["card_border"]}; background:{rb};"><div style="flex:0.6;"><span style="background:{fc}; color:#fff; padding:2px 6px; border-radius:10px; font-size:0.6rem; font-weight:600;">{lk.get("분야","")}</span></div><div style="flex:3; font-size:0.75rem; font-weight:600; color:{COLORS["text_dark"]}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{lk.get("계약명","")[:30]}</div><div style="flex:1; text-align:right; font-size:0.78rem; font-weight:600; font-family:Nunito Sans,sans-serif;">{format_억(lk.get("계약액",0))}</div><div style="flex:1; text-align:right; font-size:0.78rem; font-weight:700; color:{COLORS["danger"]}; font-family:Nunito Sans,sans-serif;">{format_억(lk.get("유출액",0))}</div><div style="flex:2; font-size:0.65rem; font-weight:600; color:{_bgc}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-left:8px;">{_bg}</div></div>'
                                            st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; overflow:hidden;">{lk_hd_s}{ch_s}{rws_s}</div>', unsafe_allow_html=True)
                    
                    # ── 우측: 유출 계약 top 10 ──
                    with col_ct:
                        st.markdown(f"""<div style="padding:16px 0 12px;">
<h3 style="margin:0; font-size:1.1rem; font-weight:700; color:{COLORS['text_dark']};">유출 수의계약 상위</h3>
<span style="font-size:0.7rem; color:{COLORS['text_light']};">비부산 업체 수주 금액 상위 10건</span>
</div>""", unsafe_allow_html=True)
                        
                        if 유출_list:
                            분야_colors = {"공사": "#6576ff", "용역": "#9cabff", "물품": "#1ee0ac", "쇼핑몰": "#f4bd0e"}
                            rows_ct = ""
                            for idx, item in enumerate(유출_list):
                                분야 = item.get("분야", "")
                                계약명 = item.get("계약명", "")
                                금액 = item.get("유출액", 0)
                                기관 = item.get("수요기관", "")
                                분야_clr = 분야_colors.get(분야, COLORS["text_light"])
                                rank_clr = "#e85347" if idx < 3 else COLORS["text_light"]
                                rows_ct += f'''<div style="padding:13px 0; border-bottom:1px solid {COLORS["card_border"]};">
<div style="display:flex; justify-content:space-between; align-items:flex-start;">
<div style="flex:1; min-width:0;">
<div style="display:flex; align-items:center; gap:6px; margin-bottom:4px;">
<span style="font-size:0.8rem; font-weight:800; color:{rank_clr}; font-family:Nunito Sans,sans-serif; min-width:16px;">{idx+1}</span>
<span style="font-size:0.8rem; font-weight:600; color:{COLORS["text_dark"]}; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{계약명[:35]}</span>
</div>
<div style="display:flex; align-items:center; gap:6px; padding-left:22px;">
<span style="background:{분야_clr}; color:#fff; padding:1px 6px; border-radius:8px; font-size:0.6rem; font-weight:600;">{분야}</span>
<span style="font-size:0.65rem; color:{COLORS["text_light"]};">{기관}</span>
</div>
</div>
<div style="font-size:0.88rem; font-weight:700; color:#e85347; font-family:Nunito Sans,sans-serif; white-space:nowrap; padding-left:8px;">{format_억(금액)}</div>
</div>
</div>'''
                            
                            st.markdown(f'<div style="padding:0 4px;">{rows_ct}</div>', unsafe_allow_html=True)

        # ── 기관별 수의계약 유출 조회 ──
        st.markdown("---")
        st.markdown(f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px; padding:16px 20px; margin-bottom:12px;">
<span style="font-size:1rem; font-weight:700; color:{COLORS['text_dark']};">🏛️ 기관별 수의계약 유출 조회</span>
<span style="font-size:0.78rem; color:{COLORS['text_light']}; margin-left:8px;">기관 선택 시 해당 기관의 수의계약 유출 내역을 확인</span>
</div>""", unsafe_allow_html=True)

        search_suui = st.text_input("🔍 수의계약 대상 기관 검색", key="suui_agency_search", placeholder="기관명을 입력하세요 (예: 해운대구, 부산교육청)")
        
        if search_suui and search_suui.strip():
            st.markdown(f"### 🔍 '{search_suui}' 수의계약 유출현황 검색 결과")
            found = False
            search_api_res = fetch_api(f"/api/agency/suui-search?q={search_suui.strip()}")
            if search_api_res and "검색결과" in search_api_res and search_api_res["검색결과"]:
                found = True
                for u, details in search_api_res["검색결과"].items():
                    rate = details.get("총수주율", 0)
                    rc = rate_color(rate)
                    발주_t = details.get("총발주액", 0)
                    수주_t = details.get("총수주액", 0)
                    그룹 = str(details.get("그룹", ""))
                    법적용 = "지방계약법" if "부산" in 그룹 else "국가계약법"
                    분야별 = details.get("분야별", {})
                    
                    with st.container(border=True):
                        col_hero, col_side = st.columns([6, 4])
                        
                        with col_hero:
                            sub_parts = []
                            for sn in ["공사", "용역", "물품", "쇼핑몰"]:
                                sv = 분야별.get(sn, {})
                                if sv.get("발주액", 0) > 0:
                                    sub_parts.append(f"{sn}({format_억(sv.get('발주액',0))})")
                            sub_info = " · ".join(sub_parts) if sub_parts else ""
                            
                            st.markdown(f"""<div style="background: linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius: 8px; padding: 28px 32px 22px; box-shadow: 0 4px 20px rgba(35,46,122,0.35);">
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
<span style="font-size:1.3rem; font-weight:800; color:#fff;">{u} (수의계약 전체)</span>
<span style="font-size:0.78rem; color:rgba(255,255,255,0.55); font-weight:600;">({법적용})</span>
</div>
<div style="font-size:0.85rem; font-weight:600; color:rgba(255,255,255,0.7);">총 수의계약액</div>
<div style="font-size:2.6rem; font-weight:800; color:#fff; line-height:1; font-family:Nunito Sans,sans-serif; letter-spacing:-0.02em; margin-top:4px;">{format_억(발주_t)}</div>
<div style="font-size:0.72rem; color:rgba(255,255,255,0.4); margin-top:6px;">{sub_info}</div>
<div style="font-size:0.85rem; font-weight:600; color:rgba(255,255,255,0.7); margin-top:18px;">지역업체 수주액 (수주율)</div>
<div style="font-size:1.8rem; font-weight:800; color:rgba(255,255,255,0.95); font-family:Nunito Sans,sans-serif; line-height:1; letter-spacing:-0.02em; margin-top:6px;">{format_억(수주_t)} <span style="color:{rc};">({rate}%)</span></div>
</div>""", unsafe_allow_html=True)

                        with col_side:
                            dot_colors_s = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}
                            bar_sets_s = [
                                [40,55,35,60,45,70,80], [60,45,50,35,55,40,65],
                                [30,50,65,45,70,55,75], [45,35,55,40,50,60,70],
                            ]
                            분야_list = ["공사","용역","물품","쇼핑몰"]
                            
                            for row_idx in range(2):
                                mc1, mc2 = st.columns(2)
                                for ci, col_wgt in enumerate([mc1, mc2]):
                                    si = row_idx * 2 + ci
                                    if si >= len(분야_list): break
                                    sn = 분야_list[si]
                                    sv = 분야별.get(sn, {})
                                    s_발주 = sv.get("발주액", 0)
                                    s_수주 = sv.get("수주액", 0)
                                    s_율 = sv.get("수주율", 0)
                                    dc = dot_colors_s.get(sn, "#aaa")
                                    bars = ""
                                    for j, h in enumerate(bar_sets_s[si]):
                                        op = "0.3" if j < 6 else "1"
                                        bars += f'<div style="width:6px; height:{h}%; background:{dc}; opacity:{op}; border-radius:1px;"></div>'
                                    with col_wgt:
                                        st.markdown(f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px; padding:14px 16px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
<div style="display:flex; justify-content:space-between; align-items:flex-start;">
<div style="flex:1;">
<div style="font-size:0.75rem; font-weight:700; color:{COLORS['text_dark']}; margin-bottom:6px;">{sn}계약액</div>
<div style="font-size:1.15rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif; line-height:1;">{format_억(s_발주)}</div>
<div style="margin-top:8px;">
<div style="font-size:0.6rem; font-weight:600; color:{COLORS['text_light']}; letter-spacing:0.03em;">지역업체 수주액</div>
<div style="font-size:0.9rem; font-weight:800; color:{COLORS['text_dark']}; margin-top:2px; font-family:Nunito Sans,sans-serif;">{format_억(s_수주)} <span style="color:{COLORS['primary']};">({s_율}%)</span></div>
</div>
</div>
<div style="text-align:right; display:flex; flex-direction:column; align-items:flex-end;">
<span style="width:8px; height:8px; border-radius:50%; background:{dc}; display:inline-block; margin-bottom:6px;"></span>
<div style="display:flex; align-items:flex-end; gap:2px; height:36px; margin-top:6px;">{bars}</div>
</div>
</div>
</div>""", unsafe_allow_html=True)
                                if row_idx == 0:
                                    st.markdown('<div style="margin-top:10px;"></div>', unsafe_allow_html=True)

                    leaks_all = []
                    for s, leak_list in details.get("유출계약", {}).items():
                        leaks_all.extend(leak_list)
                    leaks_all.sort(key=lambda x: x["유출액"], reverse=True)
                    
                    if leaks_all:
                        leaks = leaks_all[:50]
                        limit_txt = " (상위 50건)" if len(leaks_all) > 50 else ""
                        th_lk = f'font-size:0.72rem; font-weight:600; color:{COLORS["text_light"]}; letter-spacing:0.03em; padding:10px 0;'
                        leak_header = f"""<div style="display:flex; justify-content:space-between; align-items:center; padding:12px 20px; background:linear-gradient(135deg, #e85347 0%, #ff7b6b 100%); border-radius:6px 6px 0 0;">
<div style="font-size:0.9rem; font-weight:700; color:#fff;">🔴 {u} 주요 지역외 유출 수의계약{limit_txt}</div>
<div style="font-size:0.72rem; color:rgba(255,255,255,0.7);">총 {len(leaks_all):,}건 중 상위 {len(leaks)}건</div>
</div>"""
                        col_hdr = f'<div style="display:flex; align-items:center; padding:8px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:#f8f9fc;"><div style="flex:0.8; {th_lk}">분야</div><div style="flex:3; {th_lk}">계약명</div><div style="flex:1.2; {th_lk} text-align:right;">계약액</div><div style="flex:1.2; {th_lk} text-align:right;">유출액</div><div style="flex:1; {th_lk} text-align:right;">유출율</div><div style="flex:2.5; {th_lk} padding-left:12px;">수주업체</div></div>'
                        leak_rows = ""
                        for li, lk in enumerate(leaks):
                            분야_l = lk.get("분야", "")
                            계약명_l = lk.get("계약명", "")[:40]
                            계약액_l = format_억(lk.get("계약액", 0))
                            유출액_l = format_억(lk.get("유출액", 0))
                            유출율_l = lk.get("유출율", 0)
                            수주업체_l = lk.get("수주업체", "")
                            row_bg = "#fafbfe" if li % 2 == 1 else COLORS["card_bg"]
                            율_clr = COLORS['danger'] if 유출율_l >= 80 else (COLORS['warning'] if 유출율_l >= 50 else COLORS['text_dark'])
                            분야_clr = {"공사":"#6576ff","용역":"#1ee0ac","물품":"#f4bd0e","쇼핑몰":"#ff63a5"}.get(분야_l, COLORS["text_light"])
                            leak_rows += f'<div style="display:flex; align-items:center; padding:10px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:{row_bg};"><div style="flex:0.8;"><span style="background:{분야_clr}; color:#fff; padding:2px 8px; border-radius:10px; font-size:0.65rem; font-weight:600;">{분야_l}</span></div><div style="flex:3; font-size:0.8rem; font-weight:600; color:{COLORS["text_dark"]}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{계약명_l}</div><div style="flex:1.2; text-align:right; font-size:0.82rem; font-weight:600; font-family:Nunito Sans,sans-serif;">{계약액_l}</div><div style="flex:1.2; text-align:right; font-size:0.82rem; font-weight:700; color:{COLORS["danger"]}; font-family:Nunito Sans,sans-serif;">{유출액_l}</div><div style="flex:1; text-align:right; font-size:0.82rem; font-weight:700; color:{율_clr};">{유출율_l}%</div><div style="flex:2.5; font-size:0.75rem; color:{COLORS["text_light"]}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; padding-left:12px;">{수주업체_l}</div></div>'
                        st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; box-shadow:0 1px 3px rgba(0,0,0,0.04); overflow:hidden; margin-top:8px;">{leak_header}{col_hdr}{leak_rows}</div>', unsafe_allow_html=True)
                        df_dl = pd.DataFrame(leaks_all)
                        cols_dl = ["분야", "수요기관", "계약명", "계약액", "유출액", "유출율", "수주업체", "그룹"]
                        df_dl = df_dl[[c for c in cols_dl if c in df_dl.columns]].copy()
                        import io
                        buf = io.BytesIO()
                        df_dl.to_excel(buf, index=False, engine='openpyxl')
                        st.download_button(
                            label=f"📥 {u} 수의계약 유출내역 전체 다운로드",
                            data=buf.getvalue(),
                            file_name=f"{u}_수의계약_유출.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_leak_suui_{u}"
                        )
                    else:
                        st.info(f"{u}의 주요 지역외 유출 수의계약이 없습니다.")

            if not found:
                st.info(f"'{search_suui}' 기관 관련 데이터를 찾을 수 없습니다.")
        else:
            st.info("검색어를 입력하면 해당 기관의 수의계약 유출현황을 확인할 수 있습니다.")
# ════════════════════════════════════════════
# PAGE: 종합쇼핑몰
# ════════════════════════════════════════════
elif page == "🛒 종합쇼핑몰":
    data_shop = fetch_api("/api/shopping-contract")
    _summary_shop = fetch_api("/api/summary")

    if data_shop:
        유출_기관_shop = data_shop.get("유출_기관별", [])
        유출_list_shop = data_shop.get("유출_쇼핑몰", [])

        from datetime import datetime as _dt
        _now = _dt.now()
        st.markdown(f"""<div style="margin-bottom:12px;">

<p style="margin:0; font-size:0.78rem; color:{COLORS['text_light']};">📅 데이터 기간: 2026-01-01 ~ 현재 | 생성: {_now.strftime("%Y-%m-%d %H:%M")}</p>
<p style="margin:2px 0 0; font-size:0.78rem; color:{COLORS['text_light']};">※ 나라장터 종합쇼핑몰 구매 현황 · 경쟁입찰 없이 직접 구매</p>
</div>""", unsafe_allow_html=True)

        # --- 쇼핑몰 요약 4개 카드 (Hero Cards) ---
        if _summary_shop and data_shop:
            분야별_shop = _summary_shop.get("2_분야별", {})
            쇼핑몰_전체 = 분야별_shop.get("쇼핑몰", {})
            
            그룹별_분야_shop = _summary_shop.get("4_그룹별_분야별", {})
            쇼핑몰_부산 = 그룹별_분야_shop.get("부산광역시 및 소속기관", {}).get("쇼핑몰", {})
            쇼핑몰_국가 = 그룹별_분야_shop.get("정부 및 국가공공기관", {}).get("쇼핑몰", {})
            쇼핑몰_구군 = data_shop.get("구군_상세", {})
            유형별 = data_shop.get("유형별", {})
            
            def render_hero_card(title, data, type_data=None, subtitle=""):
                amt = data.get("발주액", 0)
                loc = data.get("수주액", 0)
                rate = data.get("수주율", 0)

                관급 = type_data.get("관급자재", {}) if type_data else {}
                일반 = type_data.get("일반물품", {}) if type_data else {}
                관급액 = 관급.get("발주액", 0)
                일반액 = 일반.get("발주액", 0)
                sub_text = ""
                if 관급액 or 일반액:
                    sub_text = f"관급자재({format_억(관급액)}) · 일반물품({format_억(일반액)})"
                right_html = ""
                if type_data:
                    mini_data = [
                        ("🔧 관급자재", 관급, "#6576ff", [40,55,35,60,45,70,80]),
                        ("📦 일반물품", 일반, "#1ee0ac", [60,45,50,35,55,40,65]),
                    ]
                    for m_title, m_d, m_clr, m_bars in mini_data:
                        m_amt = m_d.get("발주액", 0)
                        m_loc = m_d.get("수주액", 0)
                        m_rate = m_d.get("수주율", 0)
                        bars_html = ""
                        for j, h in enumerate(m_bars):
                            op = "0.3" if j < 6 else "1"
                            bars_html += f'<div style="width:6px; height:{h}%; background:{m_clr}; opacity:{op}; border-radius:1px;"></div>'
                        right_html += f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px; padding:16px 16px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
<div style="display:flex; justify-content:space-between; align-items:flex-start;">
<div style="flex:1;">
<div style="font-size:0.78rem; font-weight:700; color:{COLORS['text_dark']}; margin-bottom:6px;">{m_title}</div>
<div style="font-size:1.2rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif; line-height:1;">{format_억(m_amt)}</div>
<div style="margin-top:8px;">
<div style="font-size:0.62rem; font-weight:600; color:{COLORS['text_light']}; letter-spacing:0.03em;">지역업체 수주액</div>
<div style="font-size:0.95rem; font-weight:800; color:{COLORS['text_dark']}; margin-top:2px; font-family:Nunito Sans,sans-serif;">{format_억(m_loc)} <span style="color:{COLORS['primary']};">({m_rate}%)</span></div>
</div>
</div>
<div style="text-align:right; display:flex; flex-direction:column; align-items:flex-end;">
<span style="width:8px; height:8px; border-radius:50%; background:{m_clr}; display:inline-block; margin-bottom:6px;"></span>
<div style="display:flex; align-items:flex-end; gap:2px; height:40px; margin-top:8px;">{bars_html}</div>
</div>
</div>
</div>"""
                return f"""<div style="display:flex; gap:16px; margin-bottom:16px;">
<div style="flex:1; background:linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius:10px; padding:24px 28px; color:#fff; box-shadow:0 4px 15px rgba(35,46,122,0.25); position:relative; overflow:hidden;">
<div style="font-size:1.1rem; font-weight:800; margin-bottom:2px;">{title}</div>
{f'<div style="font-size:0.72rem; opacity:0.65;">({subtitle})</div>' if subtitle else ''}
<div style="margin-top:12px;">
<div style="font-size:0.72rem; font-weight:700; color:rgba(255,255,255,0.85);">총 계약액</div>
<div style="font-size:2.2rem; font-weight:800; letter-spacing:-0.02em; line-height:1.1;">{format_억(amt)}</div>
</div>
{f'<div style="font-size:0.68rem; color:rgba(255,255,255,0.45); margin-top:4px;">{sub_text}</div>' if sub_text else ''}
<div style="margin-top:16px;">
<div style="font-size:0.72rem; font-weight:700; color:rgba(255,255,255,0.85);">지역업체 수주액 (수주율)</div>
<div style="font-size:1.6rem; font-weight:700; color:rgba(255,255,255,0.92); line-height:1.1; margin-top:4px;">{format_억(loc)} <span style="color:{COLORS['success']};">({rate}%)</span></div>
</div>
<div style="position:absolute; bottom:0; left:0; width:100%; height:35%; background:linear-gradient(0deg, rgba(255,255,255,0.06) 0%, transparent 100%); border-radius:0 0 10px 10px; pointer-events:none;"></div>
</div>
<div style="flex:1; display:flex; flex-direction:column; gap:12px;">
{right_html if right_html else f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; padding:40px 20px; text-align:center; color:{COLORS["text_light"]}; font-size:0.8rem; height:100%; display:flex; align-items:center; justify-content:center;">유형별 상세 데이터 없음</div>'}
</div>
</div>"""



            row1_c1, row1_c2 = st.columns(2)
            with row1_c1:
                st.markdown(render_hero_card("부산지역 전체", 쇼핑몰_전체, 유형별.get("전체")), unsafe_allow_html=True)
            with row1_c2:
                st.markdown(render_hero_card("부산광역시 및 소관기관", 쇼핑몰_부산, 유형별.get("부산광역시 및 소속기관"), "지방계약법 적용"), unsafe_allow_html=True)
            row2_c1, row2_c2 = st.columns(2)
            with row2_c1:
                st.markdown(render_hero_card("정부 및 국가공공기관", 쇼핑몰_국가, 유형별.get("정부 및 국가공공기관"), "국가계약법 적용"), unsafe_allow_html=True)
            with row2_c2:
                st.markdown(render_hero_card("자치구·군", 쇼핑몰_구군, 유형별.get("구군")), unsafe_allow_html=True)
        # --- 16개 자치구·군 종합쇼핑몰 구매 현황 ---
        st.markdown('<div style="margin-top:30px;"></div>', unsafe_allow_html=True)
        
        DISTRICTS_LIST = ["중구", "서구", "동구", "영도구", "부산진구", "동래구", "남구", "북구", "해운대구", "사하구", "금정구", "강서구", "연제구", "수영구", "사상구", "기장군"]
        
        # 구군별 데이터 조합

        # ── 검색 결과 렌더링 헬퍼 ──
        def _render_shop_result(search_q, suffix=""):
            search_api_res = fetch_api(f"/api/agency/shop-search?q={search_q.strip()}")
            if not search_api_res: 
                st.info(f"'{search_q}' 관련 데이터를 확인할 수 없습니다.")
                return
            results_shop = search_api_res.get("검색결과", {})
            if not results_shop:
                st.info(f"'{search_q}' 관련 데이터를 확인할 수 없습니다.")
                return
            for u, details in results_shop.items():
                type_key = None
                for prefix in [f"구군_{u}", f"부산기관_{u}"]:
                    if 유형별 and prefix in 유형별: type_key = prefix; break
                if type_key:
                    관급_d = 유형별[type_key].get("관급자재", {}); 일반_d = 유형별[type_key].get("일반물품", {})
                    관급_총 = 관급_d.get("발주액",0); 관급_수주 = 관급_d.get("수주액",0); 관급_율 = 관급_d.get("수주율",0)
                    일반_총 = 일반_d.get("발주액",0); 일반_수주 = 일반_d.get("수주액",0); 일반_율 = 일반_d.get("수주율",0)
                    발주 = 관급_총 + 일반_총; 수주 = 관급_수주 + 일반_수주
                    율 = round(수주/발주*100,1) if 발주>0 else 0
                else:
                    ac = details.get("유출계약", [])
                    관급_총 = sum(c["계약액"] for c in ac if c.get("관급자재여부")=="Y")
                    관급_수주 = sum(c["계약액"]-c["유출액"] for c in ac if c.get("관급자재여부")=="Y")
                    일반_총 = sum(c["계약액"] for c in ac if c.get("관급자재여부")!="Y")
                    일반_수주 = sum(c["계약액"]-c["유출액"] for c in ac if c.get("관급자재여부")!="Y")
                    관급_율 = round(관급_수주/관급_총*100,1) if 관급_총>0 else 0
                    일반_율 = round(일반_수주/일반_총*100,1) if 일반_총>0 else 0
                    발주 = details.get("총발주액",0); 수주 = details.get("총수주액",0); 율 = details.get("총수주율",0)
                st.markdown(f"""<div style="display:flex; gap:16px; margin:20px 0 12px;"><div style="flex:1; background:linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius:10px; padding:22px 26px; color:#fff; position:relative; overflow:hidden;"><div style="font-size:1.2rem; font-weight:800; margin-bottom:12px;">{u}</div><div style="font-size:0.7rem; font-weight:700; color:rgba(255,255,255,0.85);">총 계약액</div><div style="font-size:2rem; font-weight:800; letter-spacing:-0.02em; line-height:1.1;">{format_억(발주)}</div><div style="font-size:0.65rem; color:rgba(255,255,255,0.45); margin-top:3px;">관급자재({format_억(관급_총)}) · 일반물품({format_억(일반_총)})</div><div style="margin-top:14px;"><div style="font-size:0.7rem; font-weight:700; color:rgba(255,255,255,0.85);">지역업체 수주액 (수주율)</div><div style="font-size:1.5rem; font-weight:700; color:rgba(255,255,255,0.92); line-height:1.1; margin-top:4px;">{format_억(수주)} <span style="color:{COLORS['success']};">({율}%)</span></div></div><div style="position:absolute; bottom:0; left:0; width:100%; height:35%; background:linear-gradient(0deg, rgba(255,255,255,0.06) 0%, transparent 100%); pointer-events:none;"></div></div><div style="flex:1; display:flex; flex-direction:column; gap:10px;"><div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px; padding:14px 16px; flex:1;"><div style="font-size:0.75rem; font-weight:700; color:{COLORS['text_dark']}; margin-bottom:4px;">🔧 관급자재</div><div style="font-size:1.1rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(관급_총)}</div><div style="font-size:0.6rem; color:{COLORS['text_light']}; margin-top:6px;">지역업체 수주액</div><div style="font-size:0.9rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(관급_수주)} <span style="color:{COLORS['primary']};">({관급_율}%)</span></div></div><div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px; padding:14px 16px; flex:1;"><div style="font-size:0.75rem; font-weight:700; color:{COLORS['text_dark']}; margin-bottom:4px;">📦 일반물품</div><div style="font-size:1.1rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(일반_총)}</div><div style="font-size:0.6rem; color:{COLORS['text_light']}; margin-top:6px;">지역업체 수주액</div><div style="font-size:0.9rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(일반_수주)} <span style="color:{COLORS['primary']};">({일반_율}%)</span></div></div></div></div>""", unsafe_allow_html=True)
                leaks_shop = details.get("유출계약", [])
                if leaks_shop:
                    leaks_disp = leaks_shop[:50]
                    th_lk = f'font-size:0.72rem; font-weight:600; color:{COLORS["text_light"]}; letter-spacing:0.02em; padding:10px 0;'
                    from datetime import datetime
                    now_str = datetime.now().strftime("%Y년 %m월 %d일")
                    leak_header = f"""<div style="padding:20px 0 8px;">
<div style="display:flex; justify-content:space-between; align-items:center; border-bottom:2px solid {COLORS['card_border']}; padding-bottom:14px; margin-bottom:8px;">
<div style="font-size:1.1rem; font-weight:800; color:{COLORS['text_dark']};">📋 {u} 유출 구매내역</div>
<div style="font-size:0.78rem; color:{COLORS['text_light']};">{now_str}</div>
</div>
<div style="font-size:0.72rem; color:{COLORS['text_light']}; margin-bottom:4px;">📅 생성: {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
<div style="font-size:0.72rem; color:{COLORS['text_light']}; margin-bottom:12px;">※ 유출건 {len(leaks_shop):,}건 중 상위 {len(leaks_disp)}건 (계약액순)</div>
</div>
<div style="display:flex; align-items:center; padding:0 20px; border-bottom:1px solid {COLORS['card_border']}; background:#fafbfe;"><div style="flex:0.7; {th_lk}">분야</div><div style="flex:2.5; {th_lk}">계약명(품목)</div><div style="flex:1; text-align:right; {th_lk}">계약액</div><div style="flex:1; text-align:right; {th_lk}">유출액</div><div style="flex:0.7; text-align:right; {th_lk}">유출율</div><div style="flex:1.8; padding-left:12px; {th_lk}">수주업체</div><div style="flex:1; {th_lk}">비고</div></div>"""
                    leak_rows = ""
                    for li, lk in enumerate(leaks_disp):
                        is_관급 = lk.get("관급자재여부", "")
                        if is_관급 == "Y": 분야_tag, 분야_clr, 분야_bg = "관급자재", "#e85347", "#fff0ef"
                        else: 분야_tag, 분야_clr, 분야_bg = "일반물품", "#6576ff", "#f0f2ff"
                        계약명_l = lk.get("계약명","")[:40]; 계약액_l = format_억(lk.get("계약액",0))
                        유출액_l = format_억(lk.get("유출액",0)); 유출율_l = lk.get("유출율",0)
                        수주업체_l = lk.get("수주업체","")[:18]; 비고_l = lk.get("비고","직접구매")
                        row_bg = "#fafbfe" if li%2==1 else COLORS["card_bg"]
                        율_clr = COLORS['danger'] if 유출율_l>=80 else COLORS['warning'] if 유출율_l>=50 else COLORS['text_dark']
                        leak_rows += f"""<div style="display:flex; align-items:center; padding:10px 20px; border-bottom:1px solid {COLORS['card_border']}; background:{row_bg};"><div style="flex:0.7;"><span style="display:inline-block; font-size:0.65rem; font-weight:700; color:{분야_clr}; background:{분야_bg}; padding:3px 8px; border-radius:10px; white-space:nowrap;">{분야_tag}</span></div><div style="flex:2.5; font-size:0.8rem; font-weight:600; color:{COLORS['text_dark']}; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{계약명_l}</div><div style="flex:1; text-align:right; font-size:0.8rem; font-weight:600; font-family:Nunito Sans,sans-serif; color:{COLORS['text_dark']};">{계약액_l}</div><div style="flex:1; text-align:right; font-size:0.8rem; font-weight:800; color:{COLORS['danger']}; font-family:Nunito Sans,sans-serif;">{유출액_l}</div><div style="flex:0.7; text-align:right; font-size:0.8rem; font-weight:800; color:{율_clr};">{유출율_l}%</div><div style="flex:1.8; font-size:0.75rem; color:{COLORS['text_light']}; padding-left:12px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">{수주업체_l}</div><div style="flex:1; font-size:0.68rem; color:{COLORS['text_light']};">{비고_l}</div></div>"""
                    st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; box-shadow:0 1px 3px rgba(0,0,0,0.04); overflow:hidden; margin-top:8px;">{leak_header}{leak_rows}</div>', unsafe_allow_html=True)
                    import io
                    df_dl = pd.DataFrame(leaks_shop)
                    if not df_dl.empty:
                        cols_dl = ["분야","수요기관","계약명","계약액","유출액","유출율","수주업체","관급자재여부","그룹"]
                        df_dl = df_dl[[c for c in cols_dl if c in df_dl.columns]].copy()
                        buf = io.BytesIO(); df_dl.to_excel(buf, index=False, engine='openpyxl')
                        
                        col_dl1, col_dl2 = st.columns(2)
                        with col_dl1:
                            st.download_button(label=f"📥 {u} 쇼핑몰 유출계약 엑셀 다운로드", data=buf.getvalue(), file_name=f"{u}_쇼핑몰_유출.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"dl_shop_{u}{suffix}")
                            
                        with col_dl2:
                            state_key_shop = f"gen_excel_shop_{u}{suffix}"
                            if st.button(f"📥 {u} 전체 계약 생성 (모든 분야)", key=f"btn_gen_shop_{u}{suffix}"):
                                st.session_state[state_key_shop] = True
                                
                            if st.session_state.get(state_key_shop):
                                with st.spinner("DB 데이터 조회 및 필터링 중..."):
                                    from export_excel import generate_agency_excel
                                    excel_buf = generate_agency_excel(u)
                                    if excel_buf:
                                        st.download_button(
                                            label=f"✅ 준비 완료! 클릭하여 저장",
                                            data=excel_buf.getvalue(),
                                            file_name=f"{u}_전체계약내역.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key=f"dl_all_shop_{u}{suffix}"
                                        )
                                    else:
                                        st.error("데이터 생성 실패 또는 데이터 없음")
                                        
        district_rows = []
        if '쇼핑몰' in (_summary_shop.get("12_기관별_상세", {}) if _summary_shop else {}):
            pass  # fallback below
        # unit_data from API
        unit_shop = {}
        raw_summary = _summary_shop or {}
        기관별 = raw_summary.get("12_기관별_상세", {})
        if isinstance(기관별, dict) and "쇼핑몰" in 기관별:
            unit_shop = 기관별["쇼핑몰"]
        else:
            # Try from data_shop directly via agency list
            pass
        
        for dname in DISTRICTS_LIST:
            # 총발주/수주 from 유출_기관_shop or unit_shop
            d_total = 0
            d_local = 0
            # First try from unit_shop (most accurate - includes ALL contracts)
            if dname in unit_shop:
                d_total = unit_shop[dname].get("total", 0)
                d_local = unit_shop[dname].get("local", 0)
            else:
                # Fallback from 유출_기관_shop
                for ag in 유출_기관_shop:
                    if ag.get("기관") == dname:
                        d_total = ag.get("발주액", 0)
                        d_local = ag.get("수주액", 0)
                        break
            
            d_rate = round(d_local / d_total * 100, 1) if d_total > 0 else 0
            
            # 관급자재/일반물품 from 유형별
            관급 = 유형별.get(f"구군_{dname}", {}).get("관급자재", {}) if 유형별 else {}
            일반 = 유형별.get(f"구군_{dname}", {}).get("일반물품", {}) if 유형별 else {}
            
            # 총계약액 = 관급자재 + 일반물품 (정합성 보장)
            d_total = 관급.get("발주액", 0) + 일반.get("발주액", 0)
            d_local = 관급.get("수주액", 0) + 일반.get("수주액", 0)
            d_rate = round(d_local / d_total * 100, 1) if d_total > 0 else 0
            
            district_rows.append({
                "name": dname,
                "total": d_total, "local": d_local, "rate": d_rate,
                "관급_발주": 관급.get("발주액", 0), "관급_수주": 관급.get("수주액", 0), "관급_율": 관급.get("수주율", 0),
                "일반_발주": 일반.get("발주액", 0), "일반_수주": 일반.get("수주액", 0), "일반_율": 일반.get("수주율", 0),
            })
        
        # 지역업체 수주율 기준 정렬 (낮은 순 = 유출 심각한 순)
        district_rows.sort(key=lambda x: x["rate"], reverse=True)
        
        th_d = f'font-size:0.82rem; font-weight:700; color:{COLORS["text_dark"]}; padding:12px 6px; border-bottom:2px solid {COLORS["card_border"]}; background:#fafbfe; text-align:center; white-space:nowrap;'
        
        header_html = f"""<div style="background:linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius:8px 8px 0 0; padding:16px 20px;">
<div style="font-size:1.05rem; font-weight:800; color:#fff;">📊 16개 자치구·군 종합쇼핑몰 구매현황</div>
<div style="font-size:0.72rem; color:rgba(255,255,255,0.7); margin-top:4px;">지역업체 구매율 기준 순위 (관급자재/일반물품 구분)</div>
</div>"""
        
        # 컬럼 헤더: 순위 | 구·군 | 총계약액 | 지역업체(비중) | 관급자재 | 관급지역(비중) | 일반물품 | 일반지역(비중)
        col_header = f"""<div style="display:flex; align-items:stretch; background:#f0f2ff;">
<div style="flex:0.4; {th_d}">순위</div>
<div style="flex:1.2; {th_d} text-align:left; padding-left:12px;">구·군</div>
<div style="flex:1; {th_d}">총 계약액</div>
<div style="flex:1.4; {th_d}">지역업체 수주액<br><span style="font-size:0.6rem; opacity:0.7;">(비중)</span></div>
<div style="flex:1; {th_d} border-left:2px solid {COLORS['card_border']};">🔧관급자재</div>
<div style="flex:1.4; {th_d}">관급 지역업체<br><span style="font-size:0.6rem; opacity:0.7;">(비중)</span></div>
<div style="flex:1; {th_d} border-left:2px solid {COLORS['card_border']};">📦일반물품</div>
<div style="flex:1.4; {th_d}">일반 지역업체<br><span style="font-size:0.6rem; opacity:0.7;">(비중)</span></div>
</div>"""

        rows_html = ""
        medal_map = {1: "🥇", 2: "🥈", 3: "🥉"}
        for idx, dr in enumerate(district_rows):
            rank = idx + 1
            bg = "#fffbcc" if rank <= 3 else ("#fff" if rank % 2 == 0 else "#fafbfe")
            medal = medal_map.get(rank, "")
            rc = COLORS['danger'] if dr["rate"] < 30 else (COLORS['warning'] if dr["rate"] < 50 else COLORS['success'])
            관급rc = COLORS['danger'] if dr["관급_율"] < 30 else (COLORS['warning'] if dr["관급_율"] < 50 else COLORS['success'])
            일반rc = COLORS['danger'] if dr["일반_율"] < 30 else (COLORS['warning'] if dr["일반_율"] < 50 else COLORS['success'])
            
            td = f'font-size:0.82rem; font-weight:600; color:{COLORS["text_dark"]}; padding:10px 6px; text-align:center; font-family:Nunito Sans,sans-serif;'
            
            rows_html += f"""<div style="display:flex; align-items:center; border-bottom:1px solid {COLORS['card_border']}; background:{bg}; min-height:44px;">
<div style="flex:0.4; {td}"><span style="display:inline-flex; width:22px; height:22px; border-radius:50%; background:#6576ff; color:#fff; font-size:0.7rem; font-weight:800; align-items:center; justify-content:center;">{rank}</span></div>
<div style="flex:1.2; {td} text-align:left; padding-left:12px; font-weight:700;">{dr["name"]} {medal}</div>
<div style="flex:1; {td}">{format_억(dr["total"])}</div>
<div style="flex:1.4; {td}">{format_억(dr["local"])} <span style="color:{rc}; font-weight:800;">({dr["rate"]}%)</span></div>
<div style="flex:1; {td} border-left:2px solid {COLORS['card_border']};">{format_억(dr["관급_발주"])}</div>
<div style="flex:1.4; {td}">{format_억(dr["관급_수주"])} <span style="color:{관급rc}; font-weight:800;">({dr["관급_율"]}%)</span></div>
<div style="flex:1; {td} border-left:2px solid {COLORS['card_border']};">{format_억(dr["일반_발주"])}</div>
<div style="flex:1.4; {td}">{format_억(dr["일반_수주"])} <span style="color:{일반rc}; font-weight:800;">({dr["일반_율"]}%)</span></div>
</div>"""

        st.markdown(f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,0.06); overflow:hidden;">
{header_html}{col_header}{rows_html}
</div>""", unsafe_allow_html=True)
        # 구·군 상세 조회 드롭다운 (테이블 바로 아래)
        st.markdown(f'<div style="font-size:0.78rem; font-weight:600; color:{COLORS["text_light"]}; margin-top:12px; margin-bottom:4px;">🔍 자치구·군 상세 조회</div>', unsafe_allow_html=True)
        district_options = ["선택하세요"] + [dr["name"] for dr in district_rows]
        sel_district = st.selectbox("구·군 선택", district_options, key="shop_sel_district", label_visibility="collapsed")


        # 구·군 선택 시 바로 아래 결과
        if sel_district and sel_district != "선택하세요":
            _render_shop_result(sel_district, "_d")

        # --- 부산광역시 사업소/출자출연/공사공단 종합쇼핑몰 현황 ---
        st.markdown('<div style="margin-top:24px;"></div>', unsafe_allow_html=True)
        
        # 부산기관_ 접두어로 된 유형별 데이터에서 기관 목록 추출
        busan_agency_names = []
        sel_agency = None
        if 유형별:
            for k in 유형별.keys():
                if k.startswith("부산기관_"):
                    busan_agency_names.append(k.replace("부산기관_", ""))
        
        if busan_agency_names:
            busan_agency_rows = []
            for aname in busan_agency_names:
                관급_a = 유형별.get(f"부산기관_{aname}", {}).get("관급자재", {})
                일반_a = 유형별.get(f"부산기관_{aname}", {}).get("일반물품", {})
                a_total = 관급_a.get("발주액", 0) + 일반_a.get("발주액", 0)
                a_local = 관급_a.get("수주액", 0) + 일반_a.get("수주액", 0)
                a_rate = round(a_local / a_total * 100, 1) if a_total > 0 else 0
                busan_agency_rows.append({
                    "name": aname,
                    "total": a_total, "local": a_local, "rate": a_rate,
                    "관급_발주": 관급_a.get("발주액", 0), "관급_수주": 관급_a.get("수주액", 0), "관급_율": 관급_a.get("수주율", 0),
                    "일반_발주": 일반_a.get("발주액", 0), "일반_수주": 일반_a.get("수주액", 0), "일반_율": 일반_a.get("수주율", 0),
                })
            
            busan_agency_rows = [r for r in busan_agency_rows if r["total"] >= 5e8]
            busan_agency_rows.sort(key=lambda x: x["rate"], reverse=True)
            
            ba_header = f"""<div style="background:linear-gradient(135deg, #0d5c80 0%, #147c94 100%); border-radius:8px 8px 0 0; padding:16px 20px;">
<div style="font-size:1.05rem; font-weight:800; color:#fff;">🏢 부산광역시 사업소·출자출연·공사공단 종합쇼핑몰 구매현황</div>
<div style="font-size:0.72rem; color:rgba(255,255,255,0.7); margin-top:4px;">지역업체 구매율 기준 순위 · 총계약액 5억 이상 ({len(busan_agency_rows)}개 기관)</div>
</div>"""
            
            ba_col_header = f"""<div style="display:flex; align-items:stretch; background:#f0f2ff;">
<div style="flex:0.4; {th_d}">순위</div>
<div style="flex:1.6; {th_d} text-align:left; padding-left:12px;">기관명</div>
<div style="flex:1; {th_d}">총 계약액</div>
<div style="flex:1.4; {th_d}">지역업체 수주액<br><span style="font-size:0.6rem; opacity:0.7;">(비중)</span></div>
<div style="flex:1; {th_d} border-left:2px solid {COLORS['card_border']};">🔧관급자재</div>
<div style="flex:1.4; {th_d}">관급 지역업체<br><span style="font-size:0.6rem; opacity:0.7;">(비중)</span></div>
<div style="flex:1; {th_d} border-left:2px solid {COLORS['card_border']};">📦일반물품</div>
<div style="flex:1.4; {th_d}">일반 지역업체<br><span style="font-size:0.6rem; opacity:0.7;">(비중)</span></div>
</div>"""

            ba_rows_html = ""
            for idx, dr in enumerate(busan_agency_rows):
                rank = idx + 1
                bg = "#fffbcc" if rank <= 3 else ("#fff" if rank % 2 == 0 else "#fafbfe")
                medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, "")
                rc = COLORS['danger'] if dr["rate"] < 30 else (COLORS['warning'] if dr["rate"] < 50 else COLORS['success'])
                관급rc = COLORS['danger'] if dr["관급_율"] < 30 else (COLORS['warning'] if dr["관급_율"] < 50 else COLORS['success'])
                일반rc = COLORS['danger'] if dr["일반_율"] < 30 else (COLORS['warning'] if dr["일반_율"] < 50 else COLORS['success'])
                td_b = f'font-size:0.82rem; font-weight:600; color:{COLORS["text_dark"]}; padding:10px 6px; text-align:center; font-family:Nunito Sans,sans-serif;'
                
                ba_rows_html += f"""<div style="display:flex; align-items:center; border-bottom:1px solid {COLORS['card_border']}; background:{bg}; min-height:44px;">
<div style="flex:0.4; {td_b}"><span style="display:inline-flex; width:22px; height:22px; border-radius:50%; background:#147c94; color:#fff; font-size:0.7rem; font-weight:800; align-items:center; justify-content:center;">{rank}</span></div>
<div style="flex:1.6; {td_b} text-align:left; padding-left:12px; font-weight:700;">{dr["name"]} {medal}</div>
<div style="flex:1; {td_b}">{format_억(dr["total"])}</div>
<div style="flex:1.4; {td_b}">{format_억(dr["local"])} <span style="color:{rc}; font-weight:800;">({dr["rate"]}%)</span></div>
<div style="flex:1; {td_b} border-left:2px solid {COLORS['card_border']};">{format_억(dr["관급_발주"])}</div>
<div style="flex:1.4; {td_b}">{format_억(dr["관급_수주"])} <span style="color:{관급rc}; font-weight:800;">({dr["관급_율"]}%)</span></div>
<div style="flex:1; {td_b} border-left:2px solid {COLORS['card_border']};">{format_억(dr["일반_발주"])}</div>
<div style="flex:1.4; {td_b}">{format_억(dr["일반_수주"])} <span style="color:{일반rc}; font-weight:800;">({dr["일반_율"]}%)</span></div>
</div>"""

            st.markdown(f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,0.06); overflow:hidden;">
{ba_header}{ba_col_header}{ba_rows_html}
</div>""", unsafe_allow_html=True)

            # 부산기관 상세 조회 드롭다운 (테이블 바로 아래)
            st.markdown(f'<div style="font-size:0.78rem; font-weight:600; color:{COLORS["text_light"]}; margin-top:12px; margin-bottom:4px;">🔍 부산시 기관 상세 조회</div>', unsafe_allow_html=True)
            agency_options = ["선택하세요"] + [dr["name"] for dr in busan_agency_rows]
            sel_agency = st.selectbox("기관 선택", agency_options, key="shop_sel_agency", label_visibility="collapsed")


            # 부산기관 선택 시 바로 아래 결과
            if sel_agency and sel_agency != "선택하세요":
                _render_shop_result(sel_agency, "_a")



        # ── 텍스트 검색 (맨 하단) ──
        st.markdown('<div style="margin-top:30px;"></div>', unsafe_allow_html=True)
        st.markdown(f"""<div style="margin-bottom:12px;"><span style="font-size:1.1rem; font-weight:800; color:{COLORS['text_dark']};"><span style="font-size:1.2rem;">🔍</span> 기관명 직접 검색</span></div>""", unsafe_allow_html=True)
        search_txt = st.text_input("기관명을 입력하세요 (예: 부산대학교병원, 해양경찰청)", key="shop_agency_search")
        if search_txt and search_txt.strip():
            _render_shop_result(search_txt.strip(), "_t")

# ════════════════════════════════════════════
# PAGE: 지역업체 정보
# ════════════════════════════════════════════
elif page == "🏢 지역업체 정보":
    data_comp = fetch_api("/api/local-companies")
    if data_comp:
        현황_comp = data_comp.get("현황", {})
        전체 = 현황_comp.get("전체", 0)
        
        분야목록 = [("물품", "#6576ff"), ("용역", "#9cabff"), ("공사", "#1ee0ac"), ("제조", "#f4bd0e"), ("공급", "#e85347")]
        분야데이터 = [(f, 현황_comp.get(f, 0), c) for f, c in 분야목록 if 현황_comp.get(f, 0)]
        
        with st.container(border=True):
            st.markdown(f"""<div style="padding:16px 0 8px;">
<h3 style="margin:0; font-size:1.15rem; font-weight:700; color:{COLORS['text_dark']};">조달 등록 부산업체</h3>
<span style="font-size:0.72rem; color:{COLORS['text_light']};">나라장터 등록 부산 소재 업체 현황</span>
</div>""", unsafe_allow_html=True)
            
            if 분야데이터:
                max_val = max(v for _, v, _ in 분야데이터)
                
                rows_bar = ""
                for name, count, color in 분야데이터:
                    pct = round(count / max_val * 100) if max_val > 0 else 0
                    lighter = color + "30"
                    rows_bar += f'''<div style="display:flex; align-items:center; gap:12px; margin-bottom:16px;">
<div style="flex:1;">
<div style="display:flex; position:relative; height:8px; border-radius:4px; overflow:hidden; background:{COLORS['card_border']};">
<div style="width:{pct}%; height:100%; background:{color}; border-radius:4px; position:relative;">
<div style="position:absolute; right:0; top:0; width:30%; height:100%; background:{lighter}; border-radius:0 4px 4px 0;"></div>
</div>
</div>
</div>
<div style="display:flex; align-items:center; gap:6px; min-width:120px;">
<span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:{color};"></span>
<span style="font-size:0.82rem; font-weight:600; color:{COLORS['text_dark']};">{name}</span>
<span style="font-size:0.78rem; font-weight:700; color:{COLORS['text_light']}; font-family:Nunito Sans,sans-serif; margin-left:auto;">{count:,}개</span>
</div>
</div>'''
                
                st.markdown(f"""<div style="padding:12px 4px;">
<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:20px;">
<div>
<div style="font-size:2rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif; line-height:1;">{전체:,}개</div>
<div style="font-size:0.72rem; color:{COLORS['text_light']}; margin-top:4px;">총 등록업체</div>
</div>
</div>
{rows_bar}
</div>""", unsafe_allow_html=True)

        # ── 물품 / 공사 / 용역 분류별 도넛차트 ──
        st.markdown('<div style="margin-top:20px;"></div>', unsafe_allow_html=True)
        
        # 데이터 준비 (positional access)
        현황_vals = list(현황_comp.values()) if 현황_comp else []
        물품_분류 = 현황_vals[6] if len(현황_vals) > 6 and isinstance(현황_vals[6], list) else []
        공사_업종 = 현황_vals[7] if len(현황_vals) > 7 and isinstance(현황_vals[7], list) else []
        용역_업종 = 현황_vals[8] if len(현황_vals) > 8 and isinstance(현황_vals[8], list) else []
        
        donut_colors = ["#6576ff", "#1ee0ac", "#f4bd0e", "#e85347", "#9cabff", "#816bff"]
        
        def make_donut_section(title, total_count, items, top_n=5):
            """DashLite Room Booking 스타일 도넛차트 생성"""
            if not items:
                return
            
            # 상위 N개 + 기타
            top_items = items[:top_n]
            etc_count = total_count - sum(
                (list(it.values())[1] if len(list(it.values())) > 1 else 0) for it in top_items
            ) if total_count > 0 else 0
            
            labels = []
            values = []
            for it in top_items:
                iv = list(it.values())
                nm = iv[0] if len(iv) > 0 else "?"
                cnt = iv[1] if len(iv) > 1 else 0
                labels.append(str(nm))
                values.append(cnt)
            if etc_count > 0:
                labels.append("기타")
                values.append(etc_count)
            
            colors = donut_colors[:len(labels)]
            if len(labels) > len(donut_colors):
                colors.extend(["#ccc"] * (len(labels) - len(donut_colors)))
            
            with st.container(border=True):
                st.markdown(f"""<div style="padding:16px 0 8px;">
<h3 style="margin:0; font-size:1.1rem; font-weight:700; color:{COLORS['text_dark']};">{title}</h3>
</div>""", unsafe_allow_html=True)
                
                fig = go.Figure(go.Pie(
                    labels=labels, values=values, hole=0.6,
                    marker=dict(colors=colors),
                    textinfo="none",
                    hovertemplate="%{label}: %{value:,}개 (%{percent})<extra></extra>",
                ))
                fig.update_layout(
                    showlegend=False,
                    margin=dict(t=10, b=10, l=10, r=10), height=220,
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                
                # 하단 범례 (2열)
                total_sum = sum(values)
                cols_per_row = 2
                legend_items = ""
                for i, (lbl, val) in enumerate(zip(labels, values)):
                    pct = round(val / total_sum * 100, 1) if total_sum > 0 else 0
                    clr = colors[i] if i < len(colors) else "#ccc"
                    legend_items += f'''<div style="flex:0 0 50%; display:flex; align-items:center; gap:8px; padding:6px 0;">
<span style="display:inline-block; width:10px; height:10px; border-radius:50%; background:{clr}; flex-shrink:0;"></span>
<div>
<div style="font-size:0.72rem; color:{COLORS['text_light']};">{lbl}</div>
<div style="font-size:1rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif; line-height:1.2;">{val:,} <span style="font-size:0.75rem; font-weight:500; color:{COLORS['text_light']};">{pct}%</span></div>
</div>
</div>'''
                
                st.markdown(f'<div style="display:flex; flex-wrap:wrap; padding:4px 8px; border-top:1px solid {COLORS["card_border"]};">{legend_items}</div>', unsafe_allow_html=True)
        
        col_d1, col_d2, col_d3 = st.columns(3)
        
        def make_search_donut(col, title, total_count, all_items, search_key, top_n=5):
            """검색 기능 포함 도넛 차트"""
            with col:
                with st.container(border=True):
                    st.markdown(f"""<div style="padding:12px 0 4px;">
<h3 style="margin:0; font-size:1.1rem; font-weight:700; color:{COLORS['text_dark']};">{title}</h3>
<span style="font-size:0.68rem; color:{COLORS['text_light']};">총 {total_count:,}개 업체</span>
</div>""", unsafe_allow_html=True)
                    
                    query = st.text_input("🔍 검색", key=search_key, placeholder="업종/분류명 검색...", label_visibility="collapsed")
                    
                    if not all_items:
                        return
                    
                    # 검색 필터
                    if query:
                        filtered = [it for it in all_items if query.lower() in str(list(it.values())[0]).lower()]
                    else:
                        filtered = all_items[:top_n]
                    
                    display = filtered[:top_n]
                    
                    # 도넛 데이터
                    labels, values = [], []
                    for it in display:
                        iv = list(it.values())
                        labels.append(str(iv[0]) if iv else "?")
                        values.append(iv[1] if len(iv) > 1 else 0)
                    
                    if not query:
                        etc = total_count - sum(values)
                        if etc > 0:
                            labels.append("기타")
                            values.append(etc)
                    
                    if values:
                        colors = donut_colors[:len(labels)]
                        if len(labels) > len(donut_colors):
                            colors += ["#ccc"] * (len(labels) - len(donut_colors))
                        
                        fig = go.Figure(go.Pie(
                            labels=labels, values=values, hole=0.6,
                            marker=dict(colors=colors),
                            textinfo="none",
                            hovertemplate="%{label}: %{value:,}개 (%{percent})<extra></extra>",
                        ))
                        fig.update_layout(
                            showlegend=False,
                            margin=dict(t=10, b=10, l=10, r=10), height=200,
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        )
                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                        
                        # 하단 범례
                        total_sum = sum(values)
                        legend_items = ""
                        for i, (lbl, val) in enumerate(zip(labels, values)):
                            pct = round(val / total_sum * 100, 1) if total_sum > 0 else 0
                            clr = colors[i] if i < len(colors) else "#ccc"
                            legend_items += f'''<div style="flex:0 0 50%; display:flex; align-items:center; gap:8px; padding:5px 0;">
<span style="display:inline-block; width:10px; height:10px; border-radius:50%; background:{clr}; flex-shrink:0;"></span>
<div>
<div style="font-size:0.68rem; color:{COLORS['text_light']};">{lbl}</div>
<div style="font-size:0.92rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif; line-height:1.2;">{val:,} <span style="font-size:0.7rem; font-weight:500; color:{COLORS['text_light']};">{pct}%</span></div>
</div>
</div>'''
                        
                        st.markdown(f'<div style="display:flex; flex-wrap:wrap; padding:4px 8px; border-top:1px solid {COLORS["card_border"]};">{legend_items}</div>', unsafe_allow_html=True)
                    
                    # 검색 결과 목록 (검색어 있을 때 전체 결과 표시)
                    if query and len(filtered) > top_n:
                        more_html = ""
                        for it in filtered[top_n:20]:
                            iv = list(it.values())
                            nm = str(iv[0]) if iv else "?"
                            cnt = iv[1] if len(iv) > 1 else 0
                            more_html += f'<div style="display:flex; justify-content:space-between; padding:5px 0; border-bottom:1px solid {COLORS["card_border"]}; font-size:0.78rem;"><span style="color:{COLORS["text_dark"]};">{nm}</span><span style="font-weight:700; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;">{cnt:,}</span></div>'
                        if more_html:
                            st.markdown(f'<div style="max-height:150px; overflow-y:auto; padding:4px 8px; border-top:1px solid {COLORS["card_border"]};">{more_html}</div>', unsafe_allow_html=True)
                    
                    if query:
                        st.caption(f"검색결과: {len(filtered)}건")
                    
                    # ── 업종 drill-down: 업체명 보기 ──
                    all_names = [str(list(it.values())[0]) for it in (filtered if query else all_items[:20])]
                    if all_names:
                        selected = st.selectbox("📋 업종 선택 → 업체 보기", ["선택하세요"] + all_names, key=f"drill_{search_key}", label_visibility="collapsed")
                        if selected and selected != "선택하세요":
                            try:
                                DB_PATH = os.path.join(os.path.dirname(__file__), "busan_companies_master.db")
                                conn = sqlite3.connect(DB_PATH)
                                cur = conn.cursor()
                                if "물품" in search_key:
                                    # 물품: 분류명을 "/" 로 분할하여 키워드별 OR 검색
                                    keywords = [k.strip() for k in selected.replace("/", " ").split() if len(k.strip()) >= 2]
                                    if keywords:
                                        where_parts = []
                                        params = []
                                        for kw in keywords:
                                            where_parts.append("(c.rprsntDtlPrdnm LIKE ? OR i.indstrytyNm LIKE ?)")
                                            params.extend([f"%{kw}%", f"%{kw}%"])
                                        sql = (
                                            "SELECT DISTINCT c.corpNm, c.ceoNm, c.rgnNm, c.adrs, c.dtlAdrs, c.opbizDt, c.rgstDt "
                                            "FROM company_master c "
                                            "LEFT JOIN company_industry i ON c.bizno = i.bizno "
                                            f"WHERE ({' OR '.join(where_parts)}) ORDER BY c.corpNm"
                                        )
                                        companies = cur.execute(sql, params).fetchall()
                                    else:
                                        companies = []
                                else:
                                    # 공사/용역: 업종명으로 검색
                                    companies = cur.execute(
                                        "SELECT DISTINCT c.corpNm, c.ceoNm, c.rgnNm, c.adrs, c.dtlAdrs, c.opbizDt, c.rgstDt "
                                        "FROM company_industry i "
                                        "JOIN company_master c ON i.bizno = c.bizno "
                                        "WHERE i.indstrytyNm LIKE ? ORDER BY c.corpNm",
                                        (f"%{selected}%",)
                                    ).fetchall()
                                conn.close()
                                if companies:
                                    comp_html = ""
                                    for ci, row in enumerate(companies[:50]):
                                        nm, rgn = row[0], row[2]
                                        comp_html += f'<div style="display:flex; justify-content:space-between; padding:4px 0; border-bottom:1px solid {COLORS["card_border"]}; font-size:0.75rem;"><span style="font-weight:600; color:{COLORS["text_dark"]};">{nm}</span><span style="color:{COLORS["text_light"]};">{rgn or ""}</span></div>'
                                    st.markdown(f'<div style="max-height:200px; overflow-y:auto; padding:4px 8px; background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:4px;">{comp_html}</div>', unsafe_allow_html=True)
                                    
                                    import pandas as pd
                                    import io
                                    df = pd.DataFrame(companies, columns=['업체명', '대표자명', '구군', '주소', '상세주소', '개업일', '등록일'])
                                    buffer = io.BytesIO()
                                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                        df.to_excel(writer, index=False, sheet_name='업체목록')
                                        
                                    st.caption(f"총 {len(companies):,}개 통계 (화면에는 상위 50건만 표시됩니다)")
                                    st.download_button(
                                        label="📥 전체 다운로드 (Excel)",
                                        data=buffer.getvalue(),
                                        file_name=f"{selected.replace('/', '_')}_업체목록.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key=f"dl_{search_key}_{selected}"
                                    )
                                else:
                                    st.caption("해당 업종 업체 없음")
                            except Exception as e:
                                st.caption(f"조회 실패: {e}")
        
        # 물품 분류 데이터 변환 (분류명 + 업체수)
        물품_items = []
        for it in 물품_분류:
            iv = list(it.values())
            물품_items.append({"분류": iv[1] if len(iv) > 1 else "?", "업체수": iv[2] if len(iv) > 2 else 0})
        
        물품_total = 현황_comp.get("물품", list(현황_comp.values())[1] if len(현황_comp) > 1 else 0)
        공사_total = 현황_comp.get("공사", list(현황_comp.values())[3] if len(현황_comp) > 3 else 0)
        용역_total = 현황_comp.get("용역", list(현황_comp.values())[2] if len(현황_comp) > 2 else 0)
        
        make_search_donut(col_d1, "물품 분류별", 물품_total, 물품_items, "search_물품")
        make_search_donut(col_d2, "공사 업종별", 공사_total, 공사_업종, "search_공사")
        make_search_donut(col_d3, "용역 업종별", 용역_total, 용역_업종, "search_용역")
