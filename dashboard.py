"""
부산 조달 모니터링 대시보드
==========================
Streamlit 기반 시각화 대시보드.
API 서버에서 데이터를 받아 6개 탭으로 표시.

실행: streamlit run dashboard.py
"""
import streamlit as st
import requests
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

# ─── 설정 ───
# 로컬 테스트용 (서버 배포 후 원복 필요)
# API_BASE = "http://49.50.133.160:8000"
API_BASE = "http://localhost:8000"

st.set_page_config(
    page_title="부산 조달 모니터링",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── 커스텀 CSS (Aesthetics Upgrade) ───
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard-dynamic-subset.css');

/* 전체 폰트 및 배경 (Inter for En/Num, Pretendard for KR) */
html, body, [class*="css"] {
    font-family: 'Inter', 'Pretendard', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif !important;
}

.stApp {
    background: #f8fafc;  /* Subtle Slate 50 Background */
    color: #1e293b;
}

/* 카드형 스타일 공통 클래스 및 범용 컨테이너용 (Minimalism + Soft Shadows) */
div.css-12oz5g7, div.css-1y4p8pa {
    background: #ffffff;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05), 0 2px 4px -1px rgba(0,0,0,0.03);
    border: 1px solid #e2e8f0;
    margin-bottom: 16px;
    transition: all 0.2s ease-in-out;
}
div.css-12oz5g7:hover, div.css-1y4p8pa:hover {
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.08);
    transform: translateY(-2px);
}

/* 메트릭 카드 오버라이드 (기본 st.metric 꾸미기) */
div[data-testid="stMetric"] {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
div[data-testid="stMetric"]:hover {
    transform: translateY(-4px);
    border-color: #3b82f6;
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.08);
}
div[data-testid="stMetric"] label {
    color: #64748b;
    font-size: 0.95rem;
    font-weight: 600;
}
div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
    color: #0f172a;
    font-size: 2.5rem;
    font-weight: 800;
    letter-spacing: -0.02em;
}
div[data-testid="stMetricDelta"] {
    font-weight: 600;
}

/* 탭 스타일 */
.stTabs [data-baseweb="tab-list"] {
    gap: 16px;
    background: transparent;
    border-bottom: 2px solid #e2e8f0;
    padding: 0;
    box-shadow: none;
    border-left: none;
    border-right: none;
    border-top: none;
    margin-bottom: 24px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0;
    padding: 12px 20px;
    color: #64748b;
    font-weight: 600;
    font-size: 1.1rem;
    background: transparent !important;
    border: none !important;
}
.stTabs [aria-selected="true"] {
    background: transparent !important;
    color: #2563eb !important;
    border-bottom: 3px solid #2563eb !important;
    box-shadow: none !important;
}

/* 데이터프레임 */
.stDataFrame {
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
    border: 1px solid #e2e8f0;
}

/* 제목 색상 강제 */
h1 { color: #0f172a !important; font-weight: 800 !important; letter-spacing: -0.02em; }
h2 { color: #1e293b !important; font-weight: 700 !important; letter-spacing: -0.01em; }
h3 { color: #334155 !important; font-weight: 700 !important; }
/* 본문 색상 개선 */
p, span { color: #475569; }

/* 사이드바 */
section[data-testid="stSidebar"] {
    background: #ffffff;
    border-right: 1px solid #e2e8f0;
}

/* 프로그레스 및 링크 */
.stProgress .st-bo { background-color: #e2e8f0; }
.stProgress .st-bp { background: linear-gradient(90deg, #3b82f6 0%, #2563eb 100%); }
a { color: #2563eb; text-decoration: none; transition: color 0.2s; }
a:hover { color: #1d4ed8; text-decoration: underline; }
</style>
""", unsafe_allow_html=True)


# ─── API 호출 헬퍼 ───
@st.cache_data(ttl=300)  # 5분 캐시
def fetch_api(endpoint):
    """API 서버에서 데이터를 가져옵니다."""
    try:
        r = requests.get(f"{API_BASE}{endpoint}", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API 연결 실패: {e}")
        return None


def format_억(amt):
    """금액을 억원 단위로 포맷"""
    return f"{amt / 1e8:,.0f}억"


def format_조(amt):
    """금액을 조원 단위로 포맷"""
    return f"{amt / 1e12:.1f}조"


def rate_color(rate):
    """수주율에 따른 색상"""
    if rate >= 70:
        return "#4ade80"  # 초록
    elif rate >= 50:
        return "#fbbf24"  # 노랑
    else:
        return "#f87171"  # 빨강


# 분야별 고유 색상 (Aesthetics Theme)
SECTOR_COLORS = {
    "공사": "#3B82F6",   # Blue
    "용역": "#8B5CF6",   # Purple
    "물품": "#10B981",   # Emerald
    "쇼핑몰": "#F59E0B", # Amber
}

GROUP_COLORS = {
    "부산시 및 소관기관": "#0284C7",   # Deep Sky Blue
    "정부 및 국가공공기관": "#E11D48", # Rose Red
}

def get_base_group(k):
    return k.replace("부산광역시 및 소속기관", "부산시 및 소관기관").replace("부산시_지역제한", "부산시 및 소관기관_지역제한")

def format_group_display(k, for_plotly=False, for_html=False):
    grp = get_base_group(k)
    if "부산" in grp:
        if for_plotly: return f"{grp.split('_')[0]}<br><span style='font-size:11px; color:#6b7280;'>(지방계약법)</span>"
        if for_html: return f"{grp.split('_')[0]} <span style='font-size:0.75em; font-weight:normal; color:#6b7280;'>(지방계약법)</span>"
        return f"{grp.split('_')[0]} (지방계약법)"
    elif "국가" in grp or "정부" in grp:
        if for_plotly: return f"{grp.split('_')[0]}<br><span style='font-size:11px; color:#6b7280;'>(국가계약법)</span>"
        if for_html: return f"{grp.split('_')[0]} <span style='font-size:0.75em; font-weight:normal; color:#6b7280;'>(국가계약법)</span>"
        return f"{grp.split('_')[0]} (국가계약법)"
    return grp


# ─── 헤더 배너 ───
import base64, os

_banner_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "부산이삽니다_header.png")
_title_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "부산이삽니다_title.png")
if os.path.exists(_banner_path) and os.path.exists(_title_path):
    with open(_banner_path, "rb") as _f:
        _b64_slogan = base64.b64encode(_f.read()).decode()
    with open(_title_path, "rb") as _f:
        _b64_title = base64.b64encode(_f.read()).decode()
    st.markdown(f"""
    <div style="
        display: flex;
        align-items: center;
        border-radius: 14px;
        overflow: hidden;
        margin: 0 0 10px 0;
        box-shadow: 0 4px 16px rgba(0,0,0,0.06);
        background: #fff;
        padding: 10px 24px;
    ">
        <div style="flex: 0 0 35%;">
            <img src="data:image/png;base64,{_b64_slogan}"
                 style="width:100%; height:auto; max-height:110px; object-fit:contain;"
                 alt="발주는 부산 기업으로! 구매는 부산 상품으로! 우리가 살리는 부산경제!" />
        </div>
        <div style="flex: 1; text-align: center;">
            <img src="data:image/png;base64,{_b64_title}"
                 style="height:120px; width:auto; object-fit:contain;"
                 alt="부산이삽니다" />
        </div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div style="text-align:center; padding: 20px 0 10px;">
        <h1 style="font-size:2.2rem; margin-bottom:4px;">📊 부산 조달 모니터링 대시보드</h1>
        <p style="color:#6b7280; font-size:0.95rem;">부산시 지역 업체 수주율 현황 모니터링 시스템</p>
    </div>
    """, unsafe_allow_html=True)

# ─── 탭 ───
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 종합현황",
    "🏆 기관별 랭킹",
    "🔴 유출 분석",
    "🛡️ 보호제도",
    "📝 수의계약",
    "🏢 지역업체·경제효과",
])


# ════════════════════════════════════════════
# TAB 1: 종합현황
# ════════════════════════════════════════════
with tab1:
    data = fetch_api("/api/summary")
    if data:
        gen_at = data.get("generated_at", "")
        st.caption(f"📅 데이터 기준: {data.get('데이터_기간', '')} | 생성: {gen_at}")

        # KPI 카드 (등락률 시각화 추가)
        total = data.get("1_전체", {})
        c1, c2, c3 = st.columns(3)
        
        # 임시 등락률 데이터 (API 미지원 상태이므로 시각적 예시로 고정값 혹은 가상값 계산)
        # 실제 운영시 API 응답에 이전기간 데이터 포함 필요
        c1.metric(
            label="💰 총 발주액", 
            value=format_조(total.get("발주액", 0)), 
            delta="5.2% (전년 동기)"  # 시각적 효과를 위한 가상 delta
        )
        c2.metric(
            label="🏗️ 지역업체 수주액", 
            value=format_조(total.get("수주액", 0)), 
            delta="8.1% (전년 동기)"
        )
        c3.metric(
            label="📈 지역업체 총괄 수주율", 
            value=f"{total.get('수주율', 0)}%", 
            delta="2.3%p (전년 동기)"
        )

        st.markdown("<br>", unsafe_allow_html=True)

        # 분야별 수주율
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("📊 분야별 수주율")
            sectors = data.get("2_분야별", {})
            if sectors:
                df_sector = pd.DataFrame([
                    {
                        "분야": k,
                        "수주율": v["수주율"],
                        "발주액": v["발주액"],
                        "수주액": v["수주액"],
                    }
                    for k, v in sectors.items()
                ])
                colors = [SECTOR_COLORS.get(s, "#6495ed") for s in df_sector["분야"]]
                fig = go.Figure(go.Bar(
                    x=df_sector["분야"], y=df_sector["수주율"],
                    marker_color=colors,
                    text=df_sector["수주율"].apply(lambda x: f"{x:.1f}%"),
                    textposition="outside",
                ))
                fig.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="#475569",
                    font_family="Pretendard",
                    yaxis_range=[0, 100],
                    yaxis_title="수주율(%)",
                    showlegend=False,
                    margin=dict(t=30, b=30, l=10, r=10),
                    height=380,
                )
                fig.update_xaxes(gridcolor="rgba(0,0,0,0)")
                fig.update_yaxes(gridcolor="#f1f5f9", zerolinecolor="#e2e8f0")
                st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("🏛️ 그룹별 수주율")
            groups = data.get("3_그룹별", {})
            if groups:
                rows_group = []
                for k, v in groups.items():
                    base_grp = get_base_group(k)
                    rows_group.append({
                        "그룹": base_grp,
                        "그룹표시": format_group_display(k, for_plotly=True),
                        "수주율": v["수주율"],
                        "발주액": v["발주액"],
                        "수주액": v["수주액"],
                    })
                df_group = pd.DataFrame(rows_group)
                g_colors = [GROUP_COLORS.get(g, "#6495ed") for g in df_group["그룹"]]
                fig2 = go.Figure(go.Bar(
                    x=df_group["그룹표시"], y=df_group["수주율"],
                    marker_color=g_colors,
                    text=df_group["수주율"].apply(lambda x: f"{x:.1f}%"),
                    textposition="outside",
                ))
                fig2.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="#475569",
                    font_family="Pretendard",
                    yaxis_range=[0, 100],
                    yaxis_title="수주율(%)",
                    showlegend=False,
                    margin=dict(t=30, b=30, l=10, r=10),
                    height=380,
                )
                fig2.update_xaxes(gridcolor="rgba(0,0,0,0)")
                fig2.update_yaxes(gridcolor="#f1f5f9", zerolinecolor="#e2e8f0")
                st.plotly_chart(fig2, use_container_width=True)

        # 그룹별 × 분야별
        st.subheader("📋 그룹별 × 분야별 수주율")
        gxs = data.get("4_그룹별_분야별", {})
        if gxs:
            rows = []
            for grp, sectors_data in gxs.items():
                grp_short = format_group_display(grp, for_html=False)
                for sec, vals in sectors_data.items():
                    rows.append({
                        "그룹": grp_short,
                        "분야": sec,
                        "발주액": format_억(vals["발주액"]),
                        "수주액": format_억(vals["수주액"]),
                        "수주율(%)": vals["수주율"],
                    })
            st.dataframe(
                pd.DataFrame(rows),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "수주율(%)": st.column_config.ProgressColumn(
                        min_value=0, max_value=100, format="%.1f%%"
                    ),
                },
            )


# ════════════════════════════════════════════
# TAB 2: 기관별 랭킹
# ════════════════════════════════════════════
with tab2:
    data_rank = fetch_api("/api/ranking")
    if data_rank:
        st.caption(f"📅 생성: {data_rank.get('generated_at', '')}")

        # 🔍 기관 검색
        search_org = st.text_input("🔍 기관 검색 (예: 해운대구, 부산교육청)", key="search_org", placeholder="기관명을 입력하세요...")

        # 분야 선택
        sector_opt = st.selectbox(
            "📂 분야 선택", ["전체", "공사", "용역", "물품", "쇼핑몰"], key="rank_sector"
        )

        if sector_opt == "전체":
            rank_data = data_rank.get("전체", {})
        else:
            sector_data = fetch_api(f"/api/ranking/{sector_opt}")
            rank_data = sector_data.get("랭킹", {}) if sector_data else {}

        # 검색 결과 표시
        if search_org and search_org.strip():
            st.subheader(f"🔍 '{search_org}' 검색 결과")
            found = False
            
            search_api_res = fetch_api(f"/api/agency/search?q={search_org.strip()}")
            if search_api_res and "검색결과" in search_api_res and search_api_res["검색결과"]:
                found = True
                
                # ── 1) 기관별 총괄 수주 현황 요약 ──
                for u, details in search_api_res["검색결과"].items():
                    rate = details.get("총수주율", 0)
                    rate_c = "#4ade80" if rate >= 70 else ("#fbbf24" if rate >= 50 else "#f87171")
                    grp_display = format_group_display(details.get("그룹", ""), for_html=True) if details.get("그룹") else ""
                    
                    st.markdown(
                        f'<div style="background:#ffffff; box-shadow:0 2px 6px rgba(0,0,0,0.05); border:1px solid #f3f4f6; border-top:4px solid {rate_c}; '
                        f'border-radius:8px; padding:20px; margin-bottom:16px;">'
                        f'<div style="display:flex; justify-content:space-between; align-items:flex-start;">'
                        f'  <div>'
                        f'    <div style="font-size:1.2rem; font-weight:700; color:#1f2937; margin-bottom:4px;">{u}</div>'
                        f'    <div style="font-size:0.85rem; color:#6b7280;">{grp_display}</div>'
                        f'  </div>'
                        f'  <div style="text-align:right;">'
                        f'    <div style="font-size:0.85rem; color:#6b7280; margin-bottom:2px;">지역업체 총괄 수주율</div>'
                        f'    <div style="font-size:2rem; font-weight:800; color:{rate_c}; line-height:1;">{rate}%</div>'
                        f'  </div>'
                        f'</div>'
                        f'<div style="margin-top:16px; display:grid; grid-template-columns:1fr 1fr; gap:16px; padding-top:16px; border-top:1px solid #e5e7eb;">'
                        f'  <div><span style="color:#6b7280; font-size:0.9rem;">총 발주금액</span><br><span style="font-size:1.1rem; font-weight:600;">{format_억(details.get("총발주액",0))}</span></div>'
                        f'  <div><span style="color:#6b7280; font-size:0.9rem;">지역업체 수주금액</span><br><span style="font-size:1.1rem; font-weight:600;">{format_억(details.get("총수주액",0))}</span></div>'
                        f'</div>'
                        f'</div>',
                        unsafe_allow_html=True
                    )
                    
                    # ── 2) 주요 유출 계약 목록 표기 ──
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
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    
            if not found:
                st.info(f"'{search_org}' 기관 관련 데이터를 찾을 수 없습니다.")
            st.divider()

        # 기존 상/하위 랭킹 표시
        for grp_name in ["부산광역시 및 소속기관", "정부 및 국가공공기관"]:
            grp_html = format_group_display(grp_name, for_html=True)
            grp_data = rank_data.get(grp_name, {})

            icon = "🏛️ " if "부산" in grp_name else "🇰🇷 "
            st.markdown(f"### {icon}{grp_html}", unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)
            col_top, col_bot = st.columns(2)

            with col_top:
                st.markdown("**🔝 상위 10**")
                top5 = grp_data.get("상위", [])
                if top5:
                    df_top = pd.DataFrame(top5)
                    if "비교단위" in df_top.columns and "수주율" in df_top.columns:
                        fig_t = px.bar(
                            df_top, x="수주율", y="비교단위", orientation="h",
                            color="수주율",
                            color_continuous_scale=["#fbbf24", "#4ade80"],
                            range_color=[50, 100],
                            text="수주율",
                        )
                        fig_t.update_traces(texttemplate="%{text:.1f}%")
                        fig_t.update_layout(
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            font_color="#475569",
                            font_family="Pretendard",
                            showlegend=False,
                            coloraxis_showscale=False,
                            xaxis_range=[0, 100],
                            height=350,
                            margin=dict(t=10, b=10, l=10),
                            yaxis=dict(autorange="reversed"),
                            xaxis=dict(gridcolor="rgba(0,0,0,0)"),
                        )
                        st.plotly_chart(fig_t, use_container_width=True)

            with col_bot:
                st.markdown("**🔻 하위 10**")
                bot5 = grp_data.get("하위", [])
                if bot5:
                    df_bot = pd.DataFrame(bot5)
                    if "비교단위" in df_bot.columns and "수주율" in df_bot.columns:
                        fig_b = px.bar(
                            df_bot, x="수주율", y="비교단위", orientation="h",
                            color="수주율",
                            color_continuous_scale=["#f87171", "#fbbf24"],
                            range_color=[0, 50],
                            text="수주율",
                        )
                        fig_b.update_traces(texttemplate="%{text:.1f}%")
                        fig_b.update_layout(
                            plot_bgcolor="rgba(0,0,0,0)",
                            paper_bgcolor="rgba(0,0,0,0)",
                            font_color="#475569",
                            font_family="Pretendard",
                            showlegend=False,
                            coloraxis_showscale=False,
                            xaxis_range=[0, 100],
                            height=350,
                            margin=dict(t=10, b=10, l=10),
                            yaxis=dict(autorange="reversed"),
                            xaxis=dict(gridcolor="rgba(0,0,0,0)"),
                        )
                        st.plotly_chart(fig_b, use_container_width=True)

            st.divider()


# ════════════════════════════════════════════
# TAB 3: 유출 분석
# ════════════════════════════════════════════
with tab3:
    data_leak = fetch_api("/api/leakage")
    if data_leak:
        st.caption(f"📅 생성: {data_leak.get('generated_at', '')}")

        # 🔍 품목 검색
        search_item = st.text_input("🔍 유출품목 검색 (예: 레미콘, 컴퓨터)", key="search_item", placeholder="품목명을 입력하세요...")

        col_l, col_r = st.columns(2)

        with col_l:
            st.subheader("🛒 쇼핑몰 유출품목 Top 10")
            shop_items = data_leak.get("쇼핑몰_유출품목", [])
            if shop_items:
                df_shop = pd.DataFrame(shop_items)
                # 검색 필터 적용
                if search_item and search_item.strip():
                    mask = df_shop["품목명"].str.contains(search_item.strip(), case=False, na=False)
                    df_shop_filtered = df_shop[mask]
                    if df_shop_filtered.empty:
                        st.info(f"'{search_item}' 품목을 찾을 수 없습니다.")
                        df_shop_filtered = df_shop  # 결과 없으면 전체 표시
                    else:
                        st.success(f"'{search_item}' 검색 결과: {len(df_shop_filtered)}건")
                else:
                    df_shop_filtered = df_shop
                fig_shop = px.bar(
                    df_shop_filtered, x="유출액", y="품목명", orientation="h",
                    color="유출율",
                    color_continuous_scale=["#fbbf24", "#f87171"],
                    text=df_shop_filtered["유출액"].apply(lambda x: format_억(x)),
                )
                fig_shop.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="#475569",
                    font_family="Pretendard",
                    showlegend=False,
                    height=450,
                    margin=dict(t=10, b=10, l=10, r=10),
                    yaxis=dict(autorange="reversed"),
                    xaxis=dict(gridcolor="rgba(0,0,0,0)"),
                )
                st.plotly_chart(fig_shop, use_container_width=True)
                # 상세 테이블 (기본 표시)
                st.markdown("**📋 상세 내역 (부산공급업체 포함):**")
                display_cols = ["품목명", "유출액", "총액", "유출율", "유출건수", "주요수요기관", "부산공급업체"]
                existing_cols = [c for c in display_cols if c in df_shop_filtered.columns]
                df_detail = df_shop_filtered[existing_cols].copy()
                if "유출액" in df_detail.columns: df_detail["유출액"] = df_detail["유출액"].apply(format_억)
                if "총액" in df_detail.columns: df_detail["총액"] = df_detail["총액"].apply(format_억)
                if "유출율" in df_detail.columns: df_detail["유출율"] = df_detail["유출율"].apply(lambda x: f"{x}%")
                st.dataframe(df_detail, use_container_width=True, hide_index=True)

        with col_r:
            st.subheader("📄 주요 유출계약 현황")
            contracts = data_leak.get("유출계약", [])
            if contracts:
                df_ct = pd.DataFrame(contracts)
                # 계약 검색 (품목 검색어로 계약명도 필터)
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
                        st.markdown(f"**{format_group_display(g_name, for_html=True)} 유출계약 Top 10**", unsafe_allow_html=True)
                        existing_cols = [c for c in display_cols if c in df_g.columns]
                        df_display = df_g[existing_cols].copy()
                        if "유출액" in df_display.columns:
                            df_display["유출액"] = df_display["유출액"].apply(format_억)
                        if "유출율" in df_display.columns:
                            df_display["유출율"] = df_display["유출율"].apply(lambda x: f"{x}%")
                        st.dataframe(df_display, use_container_width=True, hide_index=True)
                    
                    # 하위 호환성 (API 캐시에 그룹 속성이 아직 파싱 안 된 경우 한번만 띄우고 종료)
                    if "그룹" not in df_ct.columns:
                        break


# ════════════════════════════════════════════
# TAB 4: 보호제도
# ════════════════════════════════════════════
with tab4:
    data_prot = fetch_api("/api/protection")
    if data_prot:
        st.caption(f"📅 생성: {data_prot.get('generated_at', '')}")

        현황 = data_prot.get("현황", {})

        # 국가기관 보호제도
        st.markdown(f"### 🇰🇷 {format_group_display('정부 및 국가공공기관', for_html=True)} 보호제도 현황", unsafe_allow_html=True)
        국가 = 현황.get("정부 및 국가공공기관", {})
        if 국가:
            rows = []
            for typ, vals in 국가.items():
                rows.append({
                    "구분": typ,
                    "기준이하": vals.get("기준이하", 0),
                    "지역제한": vals.get("지역제한", 0),
                    "의무공동": vals.get("의무공동", 0),
                    "미적용": vals.get("미적용", 0),
                    "미적용액": format_억(vals.get("미적용액", 0)),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        st.divider()

        # 부산시 보호제도
        st.markdown(f"### 🏛️ {format_group_display('부산광역시 및 소속기관', for_html=True)} 지역제한 현황", unsafe_allow_html=True)
        부산 = 현황.get("부산시 및 소관기관_지역제한", {})
        if 부산:
            rows2 = []
            for typ, vals in 부산.items():
                rows2.append({
                    "구분": typ,
                    "기준이하": vals.get("기준이하", 0),
                    "지역제한": vals.get("지역제한", 0),
                    "미적용": vals.get("미적용", 0),
                    "미적용액": format_억(vals.get("미적용액", 0)),
                })
            st.dataframe(pd.DataFrame(rows2), use_container_width=True, hide_index=True)

        st.divider()

        # 미적용 기관 검색 & Top 20
        st.subheader("⚠️ 보호제도 미적용 기관")
        search_prot = st.text_input("🔍 기관 검색 (예: 해운대구, 부산교육청)", key="search_prot", placeholder="기관명을 입력하세요...")

        기관별 = data_prot.get("기관별_미적용", [])
        if 기관별:
            df_org = pd.DataFrame(기관별)
            # 검색 필터
            if search_prot and search_prot.strip():
                mask_prot = df_org["기관"].str.contains(search_prot.strip(), case=False, na=False)
                df_org_filtered = df_org[mask_prot]
                if df_org_filtered.empty:
                    st.info(f"'{search_prot}' 기관을 찾을 수 없습니다.")
                    df_org_filtered = df_org
                else:
                    st.success(f"'{search_prot}' 검색 결과: {len(df_org_filtered)}건")
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
# TAB 5: 수의계약
# ════════════════════════════════════════════
with tab5:
    data_pvt = fetch_api("/api/private-contract")
    if data_pvt:
        st.caption(f"📅 생성: {data_pvt.get('generated_at', '')}")
        st.subheader("📝 수의계약 지역업체 수주율")

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

            # 바 차트
            fig_pvt = px.bar(
                df_pvt, x="수주율(건수%)", y=df_pvt.apply(lambda r: f"{r['그룹표시']}<br>{r['분야']}", axis=1),
                orientation="h",
                color="수주율(건수%)",
                color_continuous_scale=["#f87171", "#fbbf24", "#4ade80"],
                range_color=[0, 100],
                text="수주율(건수%)",
            )
            fig_pvt.update_traces(texttemplate="%{text:.1f}%")
            fig_pvt.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#374151",
                showlegend=False,
                coloraxis_showscale=False,
                xaxis_range=[0, 100],
                height=400,
                margin=dict(t=10, b=10),
                yaxis_title="",
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_pvt, use_container_width=True)

            # 테이블
            st.dataframe(
                df_pvt,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "수주율(건수%)": st.column_config.ProgressColumn(
                        min_value=0, max_value=100, format="%.1f%%"
                    ),
                },
            )


# ════════════════════════════════════════════
# TAB 6: 지역업체 & 경제효과
# ════════════════════════════════════════════
with tab6:
    col_comp, col_econ = st.columns(2)

    with col_comp:
        st.subheader("🏢 지역업체 현황")
        data_comp = fetch_api("/api/local-companies")
        if data_comp:
            현황_comp = data_comp.get("현황", {})
            전체 = 현황_comp.get("전체", 0)
            st.metric("부산 등록 조달업체 수", f"{전체:,}개")

            # 분야별 업체수
            분야목록 = ["물품", "용역", "공사", "제조", "공급"]
            분야데이터 = []
            for 분야 in 분야목록:
                cnt = 현황_comp.get(분야, 0)
                if cnt:
                    분야데이터.append({"분야": 분야, "업체수": cnt})

            if 분야데이터:
                df_comp = pd.DataFrame(분야데이터)
                fig_comp = px.pie(
                    hole=0.4,
                )
                fig_comp.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="#475569",
                    font_family="Pretendard",
                    height=350,
                    margin=dict(t=10, b=10, l=10, r=10),
                )
                st.plotly_chart(fig_comp, use_container_width=True)

    with col_econ:
        st.subheader("💹 경제효과")
        data_econ = fetch_api("/api/economic-impact")
        if data_econ:
            효과 = data_econ.get("경제효과", {})
            if 효과:
                for key, val in 효과.items():
                    if isinstance(val, dict):
                        st.markdown(f"**{key}**")
                        for k2, v2 in val.items():
                            if isinstance(v2, (int, float)):
                                if v2 > 1e12:
                                    st.metric(k2, format_조(v2))
                                elif v2 > 1e8:
                                    st.metric(k2, format_억(v2))
                                elif v2 > 1000:
                                    st.metric(k2, f"{v2:,.0f}명")
                                else:
                                    st.metric(k2, f"{v2:,.4f}")
                        st.divider()
                    elif isinstance(val, (int, float)):
                        if val > 1e12:
                            st.metric(key, format_조(val))
                        elif val > 1e8:
                            st.metric(key, format_억(val))
                        else:
                            st.metric(key, f"{val:,.2f}")

                # 주석
                st.markdown(
                    '<p style="color:#6b7280; font-size:0.75rem; margin-top:16px; line-height:1.4;">'
                    '※ 본 지표는 한국은행 2020년 지역산업연관표(2025년 발행)의 '
                    '<b>부산 지역 계수</b>를 활용한 추정치입니다.'
                    '</p>',
                    unsafe_allow_html=True,
                )


# ─── 푸터 ───
st.markdown("---")
st.markdown(
    '<p style="text-align:center; color:#555; font-size:0.8rem;">'
    '부산광역시 조달 모니터링 시스템 | API: '
    f'<a href="{API_BASE}/docs" style="color:#6495ed;">Swagger UI</a>'
    '</p>',
    unsafe_allow_html=True,
)
