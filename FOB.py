# FOB.py (v0.2)
# Streamlit 웹앱: 이물(FO) 분석/알림 "에이스"
# v0.2 변경:
#  - 필터 4열 그리드, imported/origin/severity 필터·피벗 차원 추가
#  - 날짜 기본값: 시작=오늘-1년, 종료=오늘
#  - 피벗 지표: 이물수준 / 중대이물 수준 / 일반이물 수준 (분자합/분모합)
#  - 액션 템플릿: flag == "정상" 제외, |z| 내림차순 상위 N

import io
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# -----------------------------
# 전역 설정 및 상수
# -----------------------------
st.set_page_config(page_title="포케스트 - 이물 분석·알림", layout="wide", initial_sidebar_state="expanded")
APP_TITLE = "포케스트 (FOCast) – 이물 분석·알림 웹앱"
SECRET_CODE = "cj123456"  # 주기적 변경 예정

# v4 스키마 반영 (stage 제거, 신규 열 추가)
REQUIRED_COLUMNS = [
    "dt","plant","line",
    "material_code","material_name",
    "supplier_code","supplier_name",
    "contam_type","color_tags",
    "count","unit","lot_no","severity",
    "photo_url","notes",
    "origin","imported",
    "selection_amount_kg",
    "이물수준","중대이물 수준","일반이물 수준"
]

DEFAULT_RECENT_DAYS = 7
DEFAULT_BASELINE_DAYS = 180
SURGE_Z_THRESHOLD = 3.0  # z >= 3 상승, z <= -3 하락

# -----------------------------
# 인증
# -----------------------------
def auth_gate():
    st.markdown("### 🔐 보안코드 입력")
    with st.form("auth_form", clear_on_submit=False):
        code = st.text_input("보안코드", type="password", help="접속 보안코드가 필요합니다.")
        ok = st.form_submit_button("접속")
        if ok:
            if code == SECRET_CODE:
                st.session_state["_authed"] = True
                st.success("접속 허용되었습니다.")
            else:
                st.session_state["_authed"] = False
                st.error("보안코드가 올바르지 않습니다.")

if "_authed" not in st.session_state:
    st.session_state["_authed"] = False

st.title(APP_TITLE)

if not st.session_state["_authed"]:
    auth_gate()
    st.stop()

# -----------------------------
# 유틸 & 전처리
# -----------------------------
@st.cache_data(show_spinner=False)
def load_file(uploaded_file, sheet_name=None) -> pd.DataFrame:
    """CSV/Excel 파일 로드"""
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    elif name.endswith(".xlsx") or name.endswith(".xls"):
        df = pd.read_excel(uploaded_file, sheet_name=sheet_name if sheet_name else None, engine="openpyxl")
    else:
        raise ValueError("지원하지 않는 파일 형식입니다. CSV 또는 Excel(.xlsx/.xls)만 업로드하세요.")
    return df

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """필수 컬럼/타입 보정 (v4 스키마 대응)"""
    # 누락 컬럼 보강
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    # dt (날짜) 파싱: v4는 날짜까지만 존재
    try:
        df["dt"] = pd.to_datetime(df["dt"]).dt.date
    except Exception:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date

    # 숫자형
    for c in ["count", "selection_amount_kg", "이물수준", "중대이물 수준", "일반이물 수준"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["count"] = df["count"].fillna(0).astype(int)
    df["selection_amount_kg"] = df["selection_amount_kg"].fillna(0).astype(int)
    for c in ["이물수준","중대이물 수준","일반이물 수준"]:
        df[c] = df[c].fillna(0.0).astype(float)

    # 문자열형
    str_cols = [
        "plant","line","material_code","material_name",
        "supplier_code","supplier_name","contam_type",
        "color_tags","unit","lot_no","severity",
        "photo_url","notes","origin","imported"
    ]
    for c in str_cols:
        df[c] = df[c].fillna("").astype(str)

    return df

def split_tags(s: str):
    if not isinstance(s, str):
        return []
    return [t.strip() for t in s.split(";") if t.strip()]

def tag_filter_mask(series_tags: pd.Series, selected_tags, mode="ANY"):
    if not selected_tags:
        return pd.Series([True]*len(series_tags), index=series_tags.index)
    row_tags = series_tags.apply(split_tags)
    if mode == "ALL":
        mask = row_tags.apply(lambda lst: all(t in lst for t in selected_tags))
    else:
        mask = row_tags.apply(lambda lst: any(t in lst for t in selected_tags))
    return mask

def flatten_index(idx):
    if hasattr(idx, "names") and isinstance(idx, pd.MultiIndex):
        return [" | ".join(map(str, tup)) for tup in idx.to_list()]
    return [str(x) for x in idx]

# -----------------------------
# 분석 함수
# -----------------------------
def detect_novel_types(df: pd.DataFrame,
                       key_cols=("supplier_code","material_code"),
                       type_col="contam_type",
                       time_col="dt") -> pd.DataFrame:
    f = df[[*key_cols, type_col, time_col]].copy()
    f = f.sort_values(time_col)
    seen, flags = {}, []
    for _, row in f.iterrows():
        key = tuple(row[c] for c in key_cols)
        t = row[type_col]
        if key not in seen:
            seen[key] = set()
        novel = t not in seen[key]
        flags.append(novel)
        seen[key].add(t)
    f["is_novel_type"] = flags
    return f

def rate_change_flag(df: pd.DataFrame,
                     key_cols=("supplier_code","material_code","contam_type"),
                     count_col="count",
                     time_col="dt",
                     recent_days=DEFAULT_RECENT_DAYS,
                     baseline_days=DEFAULT_BASELINE_DAYS) -> pd.DataFrame:
    # dt가 date 형식이므로 그대로 사용
    g = df[[*key_cols, count_col, time_col]].copy()
    g["date"] = g[time_col]  # already date
    if g["date"].isna().all():
        return pd.DataFrame()

    today = max([d for d in g["date"] if pd.notna(d)], default=None)
    if pd.isna(today) or today is None:
        return pd.DataFrame()

    recent_start = today - timedelta(days=recent_days-1)
    base_end = recent_start - timedelta(days=1)
    base_start = base_end - timedelta(days=baseline_days-1)

    recent = g[(g["date"]>=recent_start) & (g["date"]<=today)]
    base   = g[(g["date"]>=base_start) & (g["date"]<=base_end)]

    if recent.empty and base.empty:
        return pd.DataFrame()

    r = recent.groupby(list(key_cols))[count_col].sum().rename("x").reset_index()
    b = base.groupby(list(key_cols))[count_col].sum().rename("base_count").reset_index()

    merged = pd.merge(r, b, on=list(key_cols), how="outer").fillna(0)
    daily_base = merged["base_count"] / max(baseline_days, 1)
    E = daily_base * recent_days
    z = (merged["x"] - E) / np.sqrt(E + 1e-6)
    merged["expected_recent"] = E
    merged["z"] = z
    merged["flag"] = np.select([z >= SURGE_Z_THRESHOLD, z <= -SURGE_Z_THRESHOLD], ["상승","하락"], default="정상")
    merged = merged.sort_values("z", ascending=False)
    return merged

# -----------------------------
# 사이드바: 업로드
# -----------------------------
with st.sidebar:
    st.header("① 데이터 업로드")
    uploaded = st.file_uploader("엑셀/CSV 업로드", type=["csv","xlsx","xls"])
    sheet_name = st.text_input("엑셀 시트명(옵션)", value="")
    st.header("② 태그 매칭")
    tag_mode = st.radio("태그 모드", ["ANY(하나라도 일치)","ALL(모두 포함)"], index=0)
    st.caption("💡 업로드 후 상단 탭에서 피벗/경보/액션/내보내기를 사용하세요.")

if uploaded is None:
    st.info("왼쪽에서 CSV 또는 엑셀 파일을 업로드하세요. (v4 스키마 권장)")
    st.stop()

try:
    df_raw = load_file(uploaded, sheet_name=sheet_name if sheet_name.strip() else None)
except Exception as e:
    st.error(f"파일을 읽는 중 오류: {e}")
    st.stop()

df = ensure_columns(df_raw)

min_dt = pd.to_datetime(df["dt"]).min()
max_dt = pd.to_datetime(df["dt"]).max()

# -----------------------------
# 상단 KPI
# -----------------------------
k1,k2,k3,k4 = st.columns(4)
with k1:
    st.metric("총 건수", f"{len(df):,}")
with k2:
    st.metric("고유 원료코드", df["material_code"].nunique())
with k3:
    st.metric("공급사 수", df["supplier_code"].nunique())
with k4:
    st.metric("기간 범위", f"{min_dt} ~ {max_dt}" if pd.notna(min_dt) else "-")

# 탭 상태
st.session_state.setdefault("pivot_df", None)
st.session_state.setdefault("alerts_novel", None)
st.session_state.setdefault("alerts_surge", None)
st.session_state.setdefault("filtered_df", None)

# -----------------------------
# 탭 구성
# -----------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "① 피벗/필터 검색", "② 경보 보드", "③ 액션 템플릿", "④ 내보내기"
])

# -----------------------------
# ① 피벗/필터 검색
# -----------------------------
with tab1:
    st.subheader("피벗/필터 검색")

    # ---- 필터 (4개씩 배치) ----
    today_d = date.today()
    default_start = today_d - timedelta(days=365)
    default_end = today_d

    # 1행
    c1,c2,c3,c4 = st.columns(4)
    with c1:
        plants = st.multiselect("공장(plant)", sorted([p for p in df["plant"].unique() if p!=""]))
    with c2:
        lines = st.multiselect("라인(line)", sorted([p for p in df["line"].unique() if p!=""]))
    with c3:
        suppliers = st.multiselect("공급사 코드(supplier_code)", sorted([p for p in df["supplier_code"].unique() if p!=""]), key="supplier_select")
    with c4:
        # suppliers에 따라 material 후보 제한
        if suppliers:
            mat_opts = sorted(df[df["supplier_code"].isin(suppliers)]["material_code"].dropna().unique())
        else:
            mat_opts = sorted(df["material_code"].dropna().unique())
        # 이전 선택 유지
        prev_selected = st.session_state.get("material_select", [])
        valid_prev = [m for m in prev_selected if m in mat_opts]
        materials = st.multiselect("원료 코드(material_code)", mat_opts, default=valid_prev, key="material_select")

    # 2행
    c5,c6,c7,c8 = st.columns(4)
    with c5:
        fo_types = st.multiselect("이물 유형(contam_type)", sorted([p for p in df["contam_type"].unique() if p!=""]))
    with c6:
        severities = st.multiselect("중대/일반(severity)", ["중대","일반"])
    with c7:
        origins = st.multiselect("원산지(origin)", sorted([p for p in df["origin"].unique() if p!=""]))
    with c8:
        imported = st.multiselect("수입여부(imported)", sorted([p for p in df["imported"].unique() if p!=""]))

    # 3행
    c9,c10,c11,c12 = st.columns(4)
    with c9:
        unique_tags = sorted({t for row in df["color_tags"] for t in split_tags(row)})
        tags = st.multiselect("태그(color_tags)", unique_tags)
    with c10:
        date_from = st.date_input("시작일", value=default_start)
    with c11:
        date_to = st.date_input("종료일", value=default_end)
    with c12:
        st.write("")  # 자리맞춤

    # ---- 필터 적용 ----
    f = df.copy()
    if plants:    f = f[f["plant"].isin(plants)]
    if lines:     f = f[f["line"].isin(lines)]
    if suppliers: f = f[f["supplier_code"].isin(suppliers)]
    if materials: f = f[f["material_code"].isin(materials)]
    if fo_types:  f = f[f["contam_type"].isin(fo_types)]
    if severities:f = f[f["severity"].isin(severities)]
    if origins:   f = f[f["origin"].isin(origins)]
    if imported:  f = f[f["imported"].isin(imported)]
    # 날짜
    f = f[(pd.to_datetime(f["dt"]) >= pd.to_datetime(date_from)) & (pd.to_datetime(f["dt"]) <= pd.to_datetime(date_to))]
    # 태그
    mode = "ALL" if tag_mode.startswith("ALL") else "ANY"
    f = f[tag_filter_mask(f["color_tags"], selected_tags=tags, mode=mode)]

    st.session_state["filtered_df"] = f

    st.write(f"필터 결과: **{len(f):,}건**")
    st.dataframe(f.head(200), use_container_width=True)

    # ---- 피벗 ----
    st.markdown("#### 피벗 테이블")
    pv_c1, pv_c2, pv_c3, pv_c4 = st.columns([1.4,1,1,1])
    with pv_c1:
        row_opts = ["plant","line","supplier_code","supplier_name","material_code","material_name","contam_type","severity","origin","imported"]
        rows = st.multiselect("행(다중 선택)", row_opts)
    with pv_c2:
        col_opts = ["plant","line","supplier_code","material_code","contam_type","severity","origin","imported"]
        cols = st.multiselect("열(선택)", col_opts)
    with pv_c3:
        agg_metric = st.selectbox("지표", [
            "count 합계 (건수)","레코드 수",
            "이물수준 (분자합/선별량합)",
            "중대이물 수준 (분자합/선별량합)",
            "일반이물 수준 (분자합/선별량합)"
        ])
    with pv_c4:
        chart_type = st.selectbox("차트 유형", ["막대(bar)","선(line)","영역(area)"])

    def pivot_rates(frame, rows, cols, which="all"):
        """which: 'all'|'sev'|'norm' -> (sum count)/sum selection_amount_kg"""
        grp = rows + (cols if cols else [])
        denom = frame.groupby(grp)["selection_amount_kg"].sum()
        if which == "all":
            num = frame.groupby(grp)["count"].sum()
        elif which == "sev":
            num = frame.assign(_num=np.where(frame["severity"]=="중대", frame["count"], 0)).groupby(grp)["_num"].sum()
        else:  # 'norm'
            num = frame.assign(_num=np.where(frame["severity"]=="일반", frame["count"], 0)).groupby(grp)["_num"].sum()
        rate = (num / denom.replace(0, np.nan)).fillna(0.0)
        if cols:
            return rate.unstack(cols).fillna(0.0)
        else:
            return rate.to_frame("value")

    pt = None
    if rows:
        g = f.copy()
        if agg_metric.startswith("count"):
            if agg_metric.startswith("count 합계"):
                values = "count"; aggfunc = "sum"
            else:
                g["__one__"] = 1; values = "__one__"; aggfunc = "sum"
            if cols:
                pt = pd.pivot_table(g, index=rows, columns=cols, values=values, aggfunc=aggfunc, fill_value=0)
            else:
                pt = g.groupby(rows)[values].sum().to_frame("value")
        else:
            if "이물수준" in agg_metric:
                pt = pivot_rates(g, rows, cols, which="all")
            elif "중대이물" in agg_metric:
                pt = pivot_rates(g, rows, cols, which="sev")
            else:
                pt = pivot_rates(g, rows, cols, which="norm")

        st.session_state["pivot_df"] = pt
        st.dataframe(pt, use_container_width=True)

        # ---- 피벗 차트 ----
        st.markdown("##### 피벗 차트")
        chart_df = pt.copy()
        if isinstance(chart_df, pd.Series):
            chart_df = chart_df.to_frame("value")
        if isinstance(chart_df.index, pd.MultiIndex):
            chart_df.index = flatten_index(chart_df.index)
        if isinstance(chart_df.columns, pd.MultiIndex):
            chart_df.columns = flatten_index(chart_df.columns)
        if chart_df.shape[0] > 50:
            st.caption("⚠️ 차트 성능을 위해 상위 50행만 표시합니다.")
            chart_df = chart_df.head(50)

        if chart_type.startswith("막대"):
            st.bar_chart(chart_df, use_container_width=True)
        elif chart_type.startswith("선"):
            st.line_chart(chart_df, use_container_width=True)
        else:
            st.area_chart(chart_df, use_container_width=True)
    else:
        st.info("행 차원을 1개 이상 선택하면 피벗이 생성됩니다.")

# -----------------------------
# ② 경보 보드
# -----------------------------
with tab2:
    st.subheader("신규 이물 / 급증 경보 보드")

    # 신규 이물
    with st.expander("신규 이물 발생 (조합: 공급사+원료)", expanded=True):
        nov_df = detect_novel_types(st.session_state["filtered_df"])
        nov_view = nov_df[nov_df["is_novel_type"]].sort_values("dt", ascending=False)
        st.session_state["alerts_novel"] = nov_view
        st.write(f"신규 유형 발생 건수: **{len(nov_view):,}**")
        st.dataframe(nov_view.head(200), use_container_width=True)

    # 급증/하락
    with st.expander(f"급증/하락 탐지 (최근 {DEFAULT_RECENT_DAYS}일 vs 과거 {DEFAULT_BASELINE_DAYS}일, z≥±{SURGE_Z_THRESHOLD})", expanded=True):
        surge_df = rate_change_flag(
            st.session_state["filtered_df"],
            recent_days=int(DEFAULT_RECENT_DAYS),
            baseline_days=int(DEFAULT_BASELINE_DAYS),
        )
        st.session_state["alerts_surge"] = surge_df
        if surge_df is not None and not surge_df.empty:
            st.write(f"분석 대상 조합 수: **{len(surge_df):,}**")
            st.dataframe(surge_df.head(200), use_container_width=True)

            s1, s2, s3 = st.columns(3)
            with s1: st.metric("상승 경보", int((surge_df["flag"]=="상승").sum()))
            with s2: st.metric("하락 감지", int((surge_df["flag"]=="하락").sum()))
            with s3: st.metric("정상", int((surge_df["flag"]=="정상").sum()))

            st.markdown("##### 선택 항목 그래프")
            view_df = surge_df.head(200).copy()
            view_df["key"] = view_df["supplier_code"] + " | " + view_df["material_code"] + " | " + view_df["contam_type"]
            sel = st.selectbox("항목 선택 (공급사 | 원료 | 유형)", options=view_df["key"].tolist())
            sel_row = view_df[view_df["key"]==sel].iloc[0]

            compare_df = pd.DataFrame({
                "지표": ["최근 실측(x)", "최근 기대(expected)", f"기준선 합({DEFAULT_BASELINE_DAYS}일)"],
                "값": [float(sel_row.get("x",0)), float(sel_row.get("expected_recent",0)), float(sel_row.get("base_count",0))]
            }).set_index("지표")
            st.bar_chart(compare_df, use_container_width=True)
            st.caption("• 실측(x): 최근 창의 실제 건수  • 기대(expected): 기준선을 바탕으로 최근 창에서 기대되는 건수  • 기준선 합: 기준선 기간 전체 합계")
        else:
            st.info("유효한 기간 데이터가 부족하거나 결과가 없습니다.")

# -----------------------------
# ③ 액션 템플릿 (화면 출력 + 복사 + txt)
# -----------------------------
with tab3:
    st.subheader("액션 템플릿 생성")

    surge_all = st.session_state.get("alerts_surge", pd.DataFrame())
    novel_view = st.session_state.get("alerts_novel", pd.DataFrame())

    if surge_all is None or surge_all.empty:
        st.info("경보 보드에서 결과가 생성된 후 사용 가능합니다.")
    else:
        # 정상 제외 + |z| 내림차순
        non_normal = surge_all[surge_all["flag"]!="정상"].copy()
        if non_normal.empty:
            st.info("상승/하락 경보가 없습니다.")
        else:
            non_normal["abs_z"] = non_normal["z"].abs()
            top_n = st.slider("알림 상위 N(|z| 기준)", min_value=5, max_value=100, value=20, step=5)
            top_df = non_normal.sort_values("abs_z", ascending=False).head(top_n)

            today_str = datetime.now().strftime("%Y-%m-%d")
            intro = f"[자동생성] 이물 급증/하락·신규 유형 모니터링 알림 – {today_str}\n"

            lines_out = []
            for _, r in top_df.iterrows():
                key = f"{r.get('supplier_code','')}-{r.get('material_code','')}-{r.get('contam_type','')}"
                lines_out.append(f"• {key}: 최근={int(r.get('x',0))}건, 기대={r.get('expected_recent',0):.1f}건, z={r.get('z',0):.2f}, 판정={r.get('flag','')}")
            summary = "\n".join(lines_out[:200])

            novel_lines = []
            if novel_view is not None and not novel_view.empty:
                for _, r in novel_view.head(20).iterrows():
                    key = f"{r.get('supplier_code','')}-{r.get('material_code','')}"
                    novel_lines.append(f"• [신규] {key}에서 '{r.get('contam_type','')}' 최초 발생 @ {r.get('dt')}")
            novel_text = "\n".join(novel_lines)

            guidance = (
                "\n[권고 액션]\n"
                "- 공정 선별강도 상향 및 해당 LOT 추가검사\n"
                "- 공급사 원인점검 요청(사진/증빙 첨부)\n"
                "- (임계 초과 시) 원료 LOT Hold 및 관련 제품 LOT 출고중지 검토\n"
                "- CAPA 등록 및 재발방지 추적"
            )

            email_text = intro + "\n[급증·하락 상위 요약]\n" + summary + ("\n\n[신규 이물 감지]\n" + novel_text if novel_text else "") + guidance

            st.markdown("#### 📣 발송/공유용 본문 미리보기")
            st.text_area("본문", value=email_text, height=300)

            # 복사 버튼
            components.html(
                f"""
                <button onclick="navigator.clipboard.writeText({email_text!r});
                                 const s=this; s.innerText='복사됨!'; setTimeout(()=>s.innerText='클립보드로 복사',1200);"
                        style="padding:8px 14px; border-radius:8px; border:1px solid #ddd; cursor:pointer;">
                    클립보드로 복사
                </button>
                """,
                height=60
            )
            st.download_button("본문 .txt 다운로드", data=email_text.encode("utf-8-sig"), file_name="alert_message.txt")

# -----------------------------
# ④ 내보내기 (CSV/XLSX)
# -----------------------------
with tab4:
    st.subheader("결과 보고서 내보내기")

    f = st.session_state.get("filtered_df", pd.DataFrame())
    pv = st.session_state.get("pivot_df", None)
    nov = st.session_state.get("alerts_novel", pd.DataFrame())
    surge = st.session_state.get("alerts_surge", pd.DataFrame())

    if not f.empty:
        st.download_button("필터 결과 CSV 다운로드", data=f.to_csv(index=False).encode("utf-8-sig"), file_name="filtered_incidents.csv")
    else:
        st.info("필터 결과가 없습니다. (탭①에서 조건을 조정하세요)")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if not f.empty:
            f.to_excel(writer, sheet_name="FilteredData", index=False)
            writer.sheets["FilteredData"].freeze_panes(1,0)
        if pv is not None:
            pv_out = pv.copy()
            if isinstance(pv_out, pd.Series):
                pv_out = pv_out.to_frame("value")
            if isinstance(pv_out.index, pd.MultiIndex):
                pv_out.index = [' | '.join(map(str, t)) for t in pv_out.index]
            if isinstance(pv_out.columns, pd.MultiIndex):
                pv_out.columns = [' | '.join(map(str, t)) for t in pv_out.columns]
            pv_out.to_excel(writer, sheet_name="Pivot", merge_cells=False)
            writer.sheets["Pivot"].freeze_panes(1,1)
        if nov is not None and not nov.empty:
            nov.to_excel(writer, sheet_name="NovelAlerts", index=False)
            writer.sheets["NovelAlerts"].freeze_panes(1,0)
        if surge is not None and not surge.empty:
            surge.to_excel(writer, sheet_name="SurgeAlerts", index=False)
            writer.sheets["SurgeAlerts"].freeze_panes(1,0)

    st.download_button("엑셀 보고서(XLSX) 다운로드", data=output.getvalue(), file_name="ACE_report.xlsx")

st.caption("※ 고도화: 분모(선별량) 기반 임계 경보, LOT↔제품 트레이스, 자동 메일/Teams 전송(Graph API) 등 확장 가능.")
