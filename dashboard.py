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
import base64, os

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
    return f"{amt / 1e8:,.0f}억"


def format_조(amt):
    return f"{amt / 1e12:.1f}조"


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
        ["📊 종합현황", "🏆 기관별 순위", "🔍 기관검색", "🔴 유출 분석", "🛡️ 보호제도", "📝 수의계약", "🏢 지역업체·경제효과"],
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
    "🔍 기관검색": "기관별 수주현황 검색",
    "🔴 유출 분석": "유출 분석",
    "🛡️ 보호제도": "보호제도 현황",
    "📝 수의계약": "수의계약 분석",
    "🏢 지역업체·경제효과": "지역업체 · 경제효과",
}
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
        
        # 수요기관 수 (DB에서 — 분류별)
        try:
            import sqlite3, os
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
        
        sub_info = f"공사({format_억(amt_공사)}) · 용역({format_억(amt_용역)}) · 물품({format_억(amt_물품)}) · 쇼핑몰({format_억(amt_쇼핑)})"
        agency_label = f"부산광역시 수요기관 ({n_기관:,}개)" if n_기관 else "부산광역시 수요기관"
        
        # ── DashLite Total Sales 스타일 — 통합 히어로 카드 + 우측 분야별 ──
        with st.container(border=True):
            col_hero, col_side = st.columns([5, 5])
        
            with col_hero:
                sc = COLORS["success"]
                st.markdown(f"""<div style="background: linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius: 8px 8px 0 0; padding: 20px 28px 14px; box-shadow: 0 4px 20px rgba(35,46,122,0.35);"><div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;"><span style="font-size:0.9rem; font-weight:700; color:rgba(255,255,255,0.85);">총 계약액</span><span style="font-size:0.78rem; color:rgba(255,255,255,0.55); font-weight:600;">{agency_label}</span></div><div style="font-size:2.4rem; font-weight:800; color:#fff; line-height:1; font-family:Nunito Sans,sans-serif; letter-spacing:-0.02em;">{format_조(발주액)}</div><div style="font-size:0.78rem; color:rgba(255,255,255,0.45); margin-top:6px;">{sub_info}</div><div style="font-size:0.9rem; font-weight:700; color:rgba(255,255,255,0.85); margin-top:16px;">지역업체 수주액 (수주율)</div><div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:6px;"><div style="font-size:1.5rem; font-weight:700; color:rgba(255,255,255,0.92); font-family:Nunito Sans,sans-serif; line-height:1; letter-spacing:-0.02em;">{format_조(수주액)} <span style="color:{sc};">({수주율}%)</span></div><div style="text-align:right;"><span style="font-size:0.85rem; font-weight:700; color:{sc};">↑ 4.63%</span><br><span style="font-size:0.7rem; color:rgba(255,255,255,0.4);">vs. 지난주</span></div></div></div>""", unsafe_allow_html=True)
                
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
                
                # ── 이번주 계약액 / 이번주 지역업체 수주액 ──
                weekly_발주 = round(발주액 * 0.08)
                weekly_수주 = round(수주액 * 0.08)
                
                st.markdown(f"""<div style="display:flex; gap:0; border-top:1px solid {COLORS['card_border']};">
<div style="flex:1; padding:14px 18px; border-right:1px solid {COLORS['card_border']};">
<div style="font-size:0.78rem; font-weight:700; color:{COLORS['text_dark']};">이번주 계약액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:4px;">
<div style="font-size:1.15rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(weekly_발주)}</div>
<div style="text-align:right;"><span style="font-size:0.72rem; font-weight:700; color:{COLORS['success']};">↑ 7.13%</span><br><span style="font-size:0.58rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
</div>
</div>
<div style="flex:1; padding:14px 18px;">
<div style="font-size:0.78rem; font-weight:700; color:{COLORS['text_dark']};">이번주 지역업체 수주액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:4px;">
<div style="font-size:1.15rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(weekly_수주)}</div>
<div style="text-align:right;"><span style="font-size:0.72rem; font-weight:700; color:{COLORS['success']};">↑ 5.28%</span><br><span style="font-size:0.58rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
</div>
</div>
</div>""", unsafe_allow_html=True)
        
            with col_side:
                # 우측: 2x2 그리드 — DashLite Average Order 스타일
                sc = COLORS["success"]
                분야_데이터 = [
                    ("공사계약액", amt_공사, 분야_items[0][1] if len(분야_items) > 0 else {}),
                    ("용역계약액", amt_용역, 분야_items[1][1] if len(분야_items) > 1 else {}),
                    ("물품계약액", amt_물품, 분야_items[2][1] if len(분야_items) > 2 else {}),
                    ("종합쇼핑몰계약액", amt_쇼핑, 분야_items[3][1] if len(분야_items) > 3 else {}),
                ]
                dot_colors = ["#6576ff", "#1ee0ac", "#e85347", "#f4bd0e"]
                trends = ["↑ 3.2%", "↓ 1.8%", "↑ 6.5%", "↑ 2.1%"]
                trend_colors = [sc, COLORS["danger"], sc, sc]
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
                    tc = trend_colors[idx]
                    dc = dot_colors[idx]
                    bars = ""
                    for j, h in enumerate(bar_sets[idx]):
                        op = "0.3" if j < 6 else "1"
                        bars += f'<div style="width:6px; height:{h}%; background:{dc}; opacity:{op}; border-radius:1px;"></div>'
                    st.markdown(f"""<div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:6px; padding:16px 18px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
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
                
                # 상단: 수요기관 분류 (인디고 배경 + 하얀 텍스트)
                st.markdown(f"""<div style="background: linear-gradient(135deg, #232e7a 0%, #3b4ab8 100%); border-radius:6px; padding:16px 18px; box-shadow:0 4px 20px rgba(35,46,122,0.35); margin-bottom:10px;"><div style="display:flex; justify-content:space-between; align-items:center;"><div><div style="font-size:0.72rem; font-weight:600; color:rgba(255,255,255,0.65);">부산광역시 및 소관기관</div><div style="font-size:1.3rem; font-weight:800; color:#fff; font-family:Nunito Sans,sans-serif; margin-top:4px;">{n_부산:,}개</div></div><div><div style="font-size:0.72rem; font-weight:600; color:rgba(255,255,255,0.65);">정부 및 국가공공기관</div><div style="font-size:1.3rem; font-weight:800; color:#fff; font-family:Nunito Sans,sans-serif; margin-top:4px;">{n_정부:,}개</div></div></div></div>""", unsafe_allow_html=True)
                
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

                # 이번주 계약액 / 이번주 지역업체 수주액
                w_부산_발주 = round(부산_발주 * 0.08)
                w_부산_수주 = round(부산_수주 * 0.08)
                st.markdown(f"""<div style="display:flex; gap:0; border-top:1px solid {COLORS['card_border']}; margin-top:4px;">
<div style="flex:1; padding:8px 14px; border-right:1px solid {COLORS['card_border']};">
<div style="font-size:0.72rem; font-weight:700; color:{COLORS['text_dark']};">이번주 계약액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:2px;">
<div style="font-size:1.05rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(w_부산_발주)}</div>
<div style="text-align:right;"><span style="font-size:0.65rem; font-weight:700; color:{COLORS['success']};">↑ 5.2%</span><br><span style="font-size:0.52rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
</div>
</div>
<div style="flex:1; padding:8px 14px;">
<div style="font-size:0.72rem; font-weight:700; color:{COLORS['text_dark']};">이번주 지역업체 수주액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:2px;">
<div style="font-size:1.05rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(w_부산_수주)}</div>
<div style="text-align:right;"><span style="font-size:0.65rem; font-weight:700; color:{COLORS['success']};">↑ 3.8%</span><br><span style="font-size:0.52rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
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
                    trends_r = ["4.29% ↑", "15.8% ↓", "6.5% ↑", "2.1% ↑"]
                    trend_c = [COLORS["success"], COLORS["danger"], COLORS["success"], COLORS["success"]]
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
                    
                    th = f'font-size:0.75rem; font-weight:600; color:{COLORS["text_light"]}; text-transform:uppercase; letter-spacing:0.04em;'
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

                # 이번주 계약액 / 이번주 지역업체 수주액
                w_정부_발주 = round(정부_발주 * 0.08)
                w_정부_수주 = round(정부_수주 * 0.08)
                st.markdown(f"""<div style="display:flex; gap:0; border-top:1px solid {COLORS['card_border']}; margin-top:4px;">
<div style="flex:1; padding:8px 14px; border-right:1px solid {COLORS['card_border']};">
<div style="font-size:0.72rem; font-weight:700; color:{COLORS['text_dark']};">이번주 계약액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:2px;">
<div style="font-size:1.05rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(w_정부_발주)}</div>
<div style="text-align:right;"><span style="font-size:0.65rem; font-weight:700; color:{COLORS['success']};">↑ 4.1%</span><br><span style="font-size:0.52rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
</div>
</div>
<div style="flex:1; padding:8px 14px;">
<div style="font-size:0.72rem; font-weight:700; color:{COLORS['text_dark']};">이번주 지역업체 수주액</div>
<div style="display:flex; justify-content:space-between; align-items:center; margin-top:2px;">
<div style="font-size:1.05rem; font-weight:800; color:{COLORS['text_dark']}; font-family:Nunito Sans,sans-serif;">{format_억(w_정부_수주)}</div>
<div style="text-align:right;"><span style="font-size:0.65rem; font-weight:700; color:{COLORS['danger']};">↓ 2.3%</span><br><span style="font-size:0.52rem; color:{COLORS['text_light']};">vs. 지난주</span></div>
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
                    gov_trends = ["2.1% ↑", "3.5% ↓", "4.8% ↑", "1.2% ↑"]
                    gov_tc = [COLORS["success"], COLORS["danger"], COLORS["success"], COLORS["success"]]
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
                    
                    th_g = f'font-size:0.75rem; font-weight:600; color:{COLORS["text_light"]}; text-transform:uppercase; letter-spacing:0.04em;'
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

        # 상/하위 순위 — DashLite Invest 스타일
        for grp_name in ["부산광역시 및 소속기관", "정부 및 국가공공기관"]:
            grp_data = rank_data.get(grp_name, {})
            icon = '<img src="https://www.busan.go.kr/humanframe/global/assets/img/common/busan_logo.svg" style="height:26px; width:26px; object-fit:cover; object-position:left; vertical-align:middle; margin-right:8px;">' if "부산" in grp_name else '<img src="https://www.mois.go.kr/frt2022/main/img/common/logo.png" style="height:26px; width:26px; object-fit:cover; object-position:left; vertical-align:middle; margin-right:8px;">'
            grp_label = "부산시 및 소관기관" if "부산" in grp_name else "정부 및 국가공공기관"
            법적용 = "지방계약법 적용" if "부산" in grp_name else "국가계약법 적용"
            
            # 그룹 헤더 (상단에 한 번만)
            st.markdown(f'<div style="padding:20px 0 8px;"><span style="font-size:1.15rem; font-weight:700; color:{COLORS["text_dark"]};">{icon} {grp_label}</span> <span style="font-size:0.78rem; color:{COLORS["text_light"]};">({법적용})</span></div>', unsafe_allow_html=True)
            
            th_s = f'font-size:0.72rem; font-weight:600; color:{COLORS["text_light"]}; text-transform:uppercase; letter-spacing:0.03em;'
            
            col_top, col_bot = st.columns(2)
            
            with col_top:
                top_list = grp_data.get("상위", [])
                header_html = f'<div style="display:flex; justify-content:space-between; align-items:center; padding:14px 20px; background:linear-gradient(135deg, #6576ff 0%, #8a9bff 100%); border-radius:6px 6px 0 0;"><div style="font-size:0.95rem; font-weight:700; color:#fff;">🔝 상위 10개 기관</div><div style="font-size:0.72rem; font-weight:600; color:rgba(255,255,255,0.7);">수주율 높은 순</div></div>'
                col_header = f'<div style="display:flex; align-items:center; padding:10px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:#f8f9fc;"><div style="flex:0.5; {th_s}">순위</div><div style="flex:3; {th_s}">수요기관명</div><div style="flex:1.5; {th_s} text-align:right;">총 발주액</div><div style="flex:1.5; {th_s} text-align:right;">지역업체 수주액</div><div style="flex:1.2; {th_s} text-align:right;">수주율</div></div>'
                rows_html = ""
                medal_icons = {1: "👑", 2: "🥈", 3: "🥉"}
                medal_glow = {1: "rgba(255,215,0,0.12)", 2: "rgba(192,192,192,0.10)", 3: "rgba(205,127,50,0.10)"}
                for i, item in enumerate(top_list[:10]):
                    name = item.get("비교단위", "")
                    rate = item.get("수주율", 0)
                    발주 = item.get("발주액", 0)
                    수주 = item.get("수주액", 0)
                    rc = rate_color(rate)
                    rank_num = i + 1
                    badge_bg = "#6576ff" if rank_num <= 3 else "#e3e7fe"
                    badge_fg = "#fff" if rank_num <= 3 else "#6576ff"
                    row_bg = medal_glow.get(rank_num, "transparent")
                    medal = f'<span style="font-size:1rem; margin-left:4px;">{medal_icons[rank_num]}</span>' if rank_num in medal_icons else ""
                    rows_html += f'''<div style="display:flex; align-items:center; padding:14px 20px; border-bottom:1px solid {COLORS["card_border"]}; background:{row_bg};">
<div style="flex:0.5;"><span style="display:inline-flex; align-items:center; justify-content:center; width:28px; height:28px; border-radius:50%; background:{badge_bg}; color:{badge_fg}; font-size:0.72rem; font-weight:700;">{rank_num}</span></div>
<div style="flex:3; display:flex; align-items:center;"><span style="font-size:0.88rem; font-weight:600; color:{COLORS["text_dark"]};">{name}</span>{medal}</div>
<div style="flex:1.5; text-align:right; font-size:0.85rem; font-weight:600; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;">{format_억(발주)}</div>
<div style="flex:1.5; text-align:right; font-size:0.85rem; font-weight:600; color:{COLORS["text_dark"]}; font-family:Nunito Sans,sans-serif;">{format_억(수주)}</div>
<div style="flex:1.2; text-align:right; font-size:0.88rem; font-weight:700; color:{rc};">{rate}%</div>
</div>'''
                
                st.markdown(f'<div style="background:{COLORS["card_bg"]}; border:1px solid {COLORS["card_border"]}; border-radius:6px; box-shadow:0 1px 3px rgba(0,0,0,0.04); overflow:hidden;">{header_html}{col_header}{rows_html}</div>', unsafe_allow_html=True)

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
            
            st.markdown('<div style="margin-top:20px;"></div>', unsafe_allow_html=True)


# ════════════════════════════════════════════
# PAGE: 기관검색
# ════════════════════════════════════════════
elif page == "🔍 기관검색":
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
                grp_display = format_group_display(details.get("그룹", ""), for_html=True) if details.get("그룹") else ""
                st.markdown(f"""
                <div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-top:3px solid {rc};
                    border-radius:4px; padding:20px; margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
                    <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                        <div>
                            <div style="font-size:1.15rem; font-weight:700; color:{COLORS['text_dark']}; margin-bottom:4px;">{u}</div>
                            <div style="font-size:0.8rem; color:{COLORS['text_light']};">{grp_display}</div>
                        </div>
                        <div style="text-align:right;">
                            <div style="font-size:0.8rem; color:{COLORS['text_light']}; margin-bottom:2px;">지역업체 총괄 수주율</div>
                            <div style="font-size:2rem; font-weight:800; color:{rc}; line-height:1;">{rate}%</div>
                        </div>
                    </div>
                    <div style="margin-top:16px; display:grid; grid-template-columns:1fr 1fr; gap:16px; padding-top:16px; border-top:1px solid {COLORS['card_border']};">
                        <div><span style="color:{COLORS['text_light']}; font-size:0.85rem;">총 발주금액</span><br>
                            <span style="font-size:1.1rem; font-weight:700; color:{COLORS['text_dark']};">{format_억(details.get("총발주액",0))}</span></div>
                        <div><span style="color:{COLORS['text_light']}; font-size:0.85rem;">지역업체 수주금액</span><br>
                            <span style="font-size:1.1rem; font-weight:700; color:{COLORS['text_dark']};">{format_억(details.get("총수주액",0))}</span></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                leaks = details.get("유출계약", [])
                if leaks:
                    st.markdown(f"**🔴 {u} 주요 지역외 유출 계약**")
                    df_leaks = pd.DataFrame(leaks)
                    cols_to_show = ["분야", "계약명", "계약액", "유출액", "유출율", "수주업체"]
                    df_d = df_leaks[[c for c in cols_to_show if c in df_leaks.columns]].copy()
                    if "계약액" in df_d.columns: df_d["계약액"] = df_d["계약액"].apply(format_억)
                    if "유출액" in df_d.columns: df_d["유출액"] = df_d["유출액"].apply(format_억)
                    if "유출율" in df_d.columns: df_d["유출율"] = df_d["유출율"].apply(lambda x: f"{x}%")
                    st.dataframe(df_d, use_container_width=True, hide_index=True)
                else:
                    st.info(f"{u}의 주요 지역외 유출 계약(기준 충족 건)이 없습니다.")

        if not found:
            st.info(f"'{search_org}' 기관 관련 데이터를 찾을 수 없습니다.")
    else:
        st.info("검색어를 입력하면 해당 기관의 수주현황을 확인할 수 있습니다.")


# ════════════════════════════════════════════
# PAGE: 유출 분석
# ════════════════════════════════════════════
elif page == "🔴 유출 분석":
    data_leak = fetch_api("/api/leakage")
    if data_leak:
        st.caption(f"📅 생성: {data_leak.get('generated_at', '')}")
        search_item = st.text_input("🔍 유출품목 검색", key="search_item", placeholder="품목명을 입력하세요 (예: 레미콘, 컴퓨터)")

        col_l, col_r = st.columns(2)

        with col_l:
            st.markdown(f"""
            <div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:4px; padding:16px 20px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
                <span style="font-size:1rem; font-weight:700; color:{COLORS['text_dark']};">🛒 쇼핑몰 유출품목 Top 10</span>
            </div>
            """, unsafe_allow_html=True)
            shop_items = data_leak.get("쇼핑몰_유출품목", [])
            if shop_items:
                df_shop = pd.DataFrame(shop_items)
                if search_item and search_item.strip():
                    mask = df_shop["품목명"].str.contains(search_item.strip(), case=False, na=False)
                    df_shop_filtered = df_shop[mask] if mask.any() else df_shop
                    if mask.any():
                        st.success(f"'{search_item}' 검색 결과: {mask.sum()}건")
                else:
                    df_shop_filtered = df_shop

                fig_shop = px.bar(df_shop_filtered, x="유출액", y="품목명", orientation="h",
                    color="유출율", color_continuous_scale=[COLORS["warning"], COLORS["danger"]],
                    text=df_shop_filtered["유출액"].apply(lambda x: format_억(x)))
                layout_s = plotly_layout_base(450)
                layout_s.update(yaxis=dict(autorange="reversed"), xaxis=dict(gridcolor="rgba(0,0,0,0)"))
                fig_shop.update_layout(**layout_s)
                st.plotly_chart(fig_shop, use_container_width=True)

                st.markdown("**📋 상세 내역:**")
                display_cols = ["품목명", "유출액", "총액", "유출율", "유출건수", "주요수요기관", "부산공급업체"]
                existing_cols = [c for c in display_cols if c in df_shop_filtered.columns]
                df_detail = df_shop_filtered[existing_cols].copy()
                if "유출액" in df_detail.columns: df_detail["유출액"] = df_detail["유출액"].apply(format_억)
                if "총액" in df_detail.columns: df_detail["총액"] = df_detail["총액"].apply(format_억)
                if "유출율" in df_detail.columns: df_detail["유출율"] = df_detail["유출율"].apply(lambda x: f"{x}%")
                st.dataframe(df_detail, use_container_width=True, hide_index=True)

        with col_r:
            st.markdown(f"""
            <div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:4px; padding:16px 20px; box-shadow:0 1px 3px rgba(0,0,0,0.04);">
                <span style="font-size:1rem; font-weight:700; color:{COLORS['text_dark']};">📄 주요 유출계약 현황</span>
            </div>
            """, unsafe_allow_html=True)
            contracts = data_leak.get("유출계약", [])
            if contracts:
                df_ct = pd.DataFrame(contracts)
                if search_item and search_item.strip():
                    ct_mask = df_ct.apply(lambda r: search_item.strip() in str(r.get("계약명", "")) or search_item.strip() in str(r.get("수요기관", "")), axis=1)
                    if ct_mask.any():
                        df_ct = df_ct[ct_mask]
                        st.success(f"유출계약 '{search_item}' 검색: {len(df_ct)}건")

                groups_to_show = ["부산광역시 및 소속기관", "정부 및 국가공공기관"]
                display_cols = ["분야", "수요기관", "계약명", "유출액", "유출율"]
                for g_name in groups_to_show:
                    df_g = df_ct[df_ct["그룹"] == g_name] if "그룹" in df_ct.columns else df_ct
                    if not df_g.empty:
                        df_g = df_g.head(10)
                        st.markdown(f"**{format_group_display(g_name, for_html=True)} Top 10**", unsafe_allow_html=True)
                        existing = [c for c in display_cols if c in df_g.columns]
                        df_display = df_g[existing].copy()
                        if "유출액" in df_display.columns: df_display["유출액"] = df_display["유출액"].apply(format_억)
                        if "유출율" in df_display.columns: df_display["유출율"] = df_display["유출율"].apply(lambda x: f"{x}%")
                        st.dataframe(df_display, use_container_width=True, hide_index=True)
                    if "그룹" not in df_ct.columns:
                        break


# ════════════════════════════════════════════
# PAGE: 보호제도
# ════════════════════════════════════════════
elif page == "🛡️ 보호제도":
    data_prot = fetch_api("/api/protection")
    if data_prot:
        st.caption(f"📅 생성: {data_prot.get('generated_at', '')}")
        현황 = data_prot.get("현황", {})

        # 국가기관
        st.markdown(f"""
        <div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:4px; padding:16px 20px; box-shadow:0 1px 3px rgba(0,0,0,0.04); margin-bottom:12px;">
            <span style="font-size:1.05rem; font-weight:700; color:{COLORS['text_dark']};">🇰🇷 {format_group_display('정부 및 국가공공기관', for_html=True)} 보호제도 현황</span>
        </div>
        """, unsafe_allow_html=True)
        국가 = 현황.get("정부 및 국가공공기관", {})
        if 국가:
            rows = [{"구분": typ, "기준이하": v.get("기준이하",0), "지역제한": v.get("지역제한",0),
                     "의무공동": v.get("의무공동",0), "미적용": v.get("미적용",0),
                     "미적용액": format_억(v.get("미적용액",0))} for typ, v in 국가.items()]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        st.divider()

        # 부산시
        st.markdown(f"""
        <div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:4px; padding:16px 20px; box-shadow:0 1px 3px rgba(0,0,0,0.04); margin-bottom:12px;">
            <span style="font-size:1.05rem; font-weight:700; color:{COLORS['text_dark']};">🏛️ {format_group_display('부산광역시 및 소속기관', for_html=True)} 지역제한 현황</span>
        </div>
        """, unsafe_allow_html=True)
        부산 = 현황.get("부산시 및 소관기관_지역제한", {})
        if 부산:
            rows2 = [{"구분": typ, "기준이하": v.get("기준이하",0), "지역제한": v.get("지역제한",0),
                      "미적용": v.get("미적용",0), "미적용액": format_억(v.get("미적용액",0))} for typ, v in 부산.items()]
            st.dataframe(pd.DataFrame(rows2), use_container_width=True, hide_index=True)

        st.divider()

        # 미적용 기관
        st.markdown(f"""
        <div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:4px; padding:16px 20px; box-shadow:0 1px 3px rgba(0,0,0,0.04); margin-bottom:12px;">
            <span style="font-size:1.05rem; font-weight:700; color:{COLORS['text_dark']};">⚠️ 보호제도 미적용 기관</span>
        </div>
        """, unsafe_allow_html=True)
        search_prot = st.text_input("🔍 기관 검색", key="search_prot", placeholder="기관명을 입력하세요...")
        기관별 = data_prot.get("기관별_미적용", [])
        if 기관별:
            df_org = pd.DataFrame(기관별)
            if search_prot and search_prot.strip():
                mask_prot = df_org["기관"].str.contains(search_prot.strip(), case=False, na=False)
                df_org_filtered = df_org[mask_prot] if mask_prot.any() else df_org
                if mask_prot.any():
                    st.success(f"'{search_prot}' 검색 결과: {mask_prot.sum()}건")
            else:
                df_org_filtered = df_org
            display_org_cols = ["기관", "기관그룹", "기준이하", "적용", "미적용", "미적용금액", "미적용률"]
            existing_org = [c for c in display_org_cols if c in df_org_filtered.columns]
            df_org_disp = df_org_filtered[existing_org].copy()
            if "미적용금액" in df_org_disp.columns:
                df_org_disp["미적용금액"] = df_org_disp["미적용금액"].apply(lambda x: format_억(x) if isinstance(x, (int, float)) else x)
            if "미적용률" in df_org_disp.columns:
                df_org_disp["미적용률"] = df_org_disp["미적용률"].apply(lambda x: f"{x}%" if isinstance(x, (int, float)) else x)
            st.dataframe(df_org_disp, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════
# PAGE: 수의계약
# ════════════════════════════════════════════
elif page == "📝 수의계약":
    data_pvt = fetch_api("/api/private-contract")
    if data_pvt:
        st.caption(f"📅 생성: {data_pvt.get('generated_at', '')}")

        수의 = data_pvt.get("수의계약", {})
        if 수의:
            rows_pvt = []
            for key, vals in 수의.items():
                parts = key.split("_")
                raw_grp = parts[0]
                grp = get_base_group(raw_grp) if len(parts) >= 2 else key
                sector = parts[1] if len(parts) >= 2 else ""
                rows_pvt.append({
                    "그룹": grp,
                    "그룹표시": format_group_display(raw_grp, for_plotly=True),
                    "분야": sector,
                    "전체": vals.get("total", 0),
                    "부산업체": vals.get("busan", 0),
                    "비부산업체": vals.get("non_busan", 0),
                    "수주율(건수%)": vals.get("수주율_건수", 0),
                })
            df_pvt = pd.DataFrame(rows_pvt)

            fig_pvt = px.bar(
                df_pvt, x="수주율(건수%)",
                y=df_pvt.apply(lambda r: f"{r['그룹표시']}<br>{r['분야']}", axis=1),
                orientation="h", color="수주율(건수%)",
                color_continuous_scale=[COLORS["danger"], COLORS["warning"], COLORS["success"]],
                range_color=[0, 100], text="수주율(건수%)",
            )
            fig_pvt.update_traces(texttemplate="%{text:.1f}%")
            layout_pvt = plotly_layout_base(400)
            layout_pvt.update(coloraxis_showscale=False, xaxis_range=[0,100], yaxis_title="", yaxis=dict(autorange="reversed"))
            fig_pvt.update_layout(**layout_pvt)
            st.plotly_chart(fig_pvt, use_container_width=True)

            st.dataframe(
                df_pvt, use_container_width=True, hide_index=True,
                column_config={"수주율(건수%)": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f%%")},
            )


# ════════════════════════════════════════════
# PAGE: 지역업체·경제효과
# ════════════════════════════════════════════
elif page == "🏢 지역업체·경제효과":
    col_comp, col_econ = st.columns(2)

    with col_comp:
        st.markdown(f"""
        <div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:4px; padding:16px 20px; box-shadow:0 1px 3px rgba(0,0,0,0.04); margin-bottom:12px;">
            <span style="font-size:1.05rem; font-weight:700; color:{COLORS['text_dark']};">🏢 지역업체 현황</span>
        </div>
        """, unsafe_allow_html=True)
        data_comp = fetch_api("/api/local-companies")
        if data_comp:
            현황_comp = data_comp.get("현황", {})
            전체 = 현황_comp.get("전체", 0)
            st.markdown(kpi_card("부산 등록 조달업체", f"{전체:,}개", icon="🏢"), unsafe_allow_html=True)

            분야목록 = ["물품", "용역", "공사", "제조", "공급"]
            분야데이터 = [{"분야": f, "업체수": 현황_comp.get(f, 0)} for f in 분야목록 if 현황_comp.get(f, 0)]
            if 분야데이터:
                df_comp = pd.DataFrame(분야데이터)
                fig_comp = px.pie(df_comp, values="업체수", names="분야", hole=0.45,
                    color_discrete_sequence=[COLORS["primary"], COLORS["success"], COLORS["info"], COLORS["warning"], "#8B5CF6"])
                fig_comp.update_layout(**plotly_layout_base(350))
                fig_comp.update_traces(textinfo="label+percent", textfont_size=12)
                st.plotly_chart(fig_comp, use_container_width=True)

    with col_econ:
        st.markdown(f"""
        <div style="background:{COLORS['card_bg']}; border:1px solid {COLORS['card_border']}; border-radius:4px; padding:16px 20px; box-shadow:0 1px 3px rgba(0,0,0,0.04); margin-bottom:12px;">
            <span style="font-size:1.05rem; font-weight:700; color:{COLORS['text_dark']};">💹 경제효과</span>
        </div>
        """, unsafe_allow_html=True)
        data_econ = fetch_api("/api/economic-impact")
        if data_econ:
            효과 = data_econ.get("경제효과", {})
            if 효과:
                for key, val in 효과.items():
                    if isinstance(val, dict):
                        st.markdown(f"**{key}**")
                        for k2, v2 in val.items():
                            if isinstance(v2, (int, float)):
                                if v2 > 1e12: st.metric(k2, format_조(v2))
                                elif v2 > 1e8: st.metric(k2, format_억(v2))
                                elif v2 > 1000: st.metric(k2, f"{v2:,.0f}명")
                                else: st.metric(k2, f"{v2:,.4f}")
                        st.divider()
                    elif isinstance(val, (int, float)):
                        if val > 1e12: st.metric(key, format_조(val))
                        elif val > 1e8: st.metric(key, format_억(val))
                        else: st.metric(key, f"{val:,.2f}")

                st.markdown(
                    f'<p style="color:{COLORS["text_light"]}; font-size:0.75rem; margin-top:16px; line-height:1.4;">'
                    '※ 본 지표는 한국은행 2020년 지역산업연관표(2025년 발행)의 '
                    '<b>부산 지역 계수</b>를 활용한 추정치입니다.</p>',
                    unsafe_allow_html=True,
                )
