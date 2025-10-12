# FOB.py (v0.3) — FOCast web app
# V5 schema + rate-based alerts + dependent filters + chart axis toggle + XLSX engine fallback

import io
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# 시각화(축 전환/레이어링)를 위해 Altair 사용
import altair as alt

# -----------------------------
# 전역 설정 및 상수
# -----------------------------
st.set_page_config(page_title="FOCast - 이물 분석·알림", layout="wide", initial_sidebar_state="expanded")
APP_TITLE = "FOCast – 이물 분석·알림 웹앱"
SECRET_CODE = "cj123456"  # 주기적 변경 예정

# V5 스키마 (stage 제거, material_type 추가)
REQUIRED_COLUMNS = [
    "dt","plant","line",
    "material_type",          # NEW in V5
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
def load_file(uploaded_file, sheet_name: str | None = None) -> pd.DataFrame:
    """CSV/Excel 파일 로드 (엑셀은 기본적으로 '첫 번째 시트' 또는 'Incidents' 시트 선택)"""
    name = uploaded_file.name.lower()

    # CSV 처리: 인코딩 자동 폴백
    if name.endswith(".csv"):
        try:
            return pd.read_csv(uploaded_file)  # 기본 UTF-8
        except UnicodeDecodeError:
            uploaded_file.seek(0)
            return pd.read_csv(uploaded_file, encoding="cp949")

    # Excel 처리
    if name.endswith((".xlsx", ".xls")):
        import openpyxl  # ensure installed
        # 1) 사용자가 시트명을 지정한 경우
        if sheet_name and str(sheet_name).strip():
            return pd.read_excel(uploaded_file, sheet_name=str(sheet_name).strip(), engine="openpyxl")

        # 2) 미지정 시 → 첫 시트 또는 'Incidents' 우선 선택
        xls = pd.ExcelFile(uploaded_file, engine="openpyxl")
        preferred = [s for s in xls.sheet_names if s.lower() in ("incidents", "data", "sheet1")]
        pick = preferred[0] if preferred else xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=pick)
        # 선택된 시트명을 화면에 표시(디버깅/가이드용)
        st.caption(f"엑셀 시트 자동 선택: **{pick}** (파일 내 시트: {', '.join(xls.sheet_names)})")
        return df

    raise ValueError("지원하지 않는 파일 형식입니다. CSV 또는 Excel(.xlsx/.xls)만 업로드하세요.")

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """필수 컬럼/타입 보정 (V5)"""
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    # 날짜 파싱: V5는 날짜까지만 존재
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
        "plant","line","material_type","material_code","material_name",
        "supplier_code","supplier_name","contam_type","color_tags",
        "unit","lot_no","severity","photo_url","notes","origin","imported"
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
# 분석 함수 (V5: rate 기반)
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

def rate_change_flag_v5(df: pd.DataFrame,
                        key_cols=("supplier_code","material_code","contam_type"),
                        count_col="count",
                        exposure_col="selection_amount_kg",
                        time_col="dt",
                        recent_days=DEFAULT_RECENT_DAYS,
                        baseline_days=DEFAULT_BASELINE_DAYS) -> pd.DataFrame:
    """이물수준(=count/exposure) 기반 급증/하락 탐지.
       z = (r_recent - r_base) / sqrt(r_base / recent_exposure)
       (Poisson with exposure 근사, small-sample 안정화 포함)
    """
    g = df[[*key_cols, count_col, exposure_col, time_col]].copy()
    g["date"] = g[time_col]
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

    # 집계: 분자/분모 따로 합산
    r = recent.groupby(list(key_cols))[[count_col, exposure_col]].sum().rename(columns={
        count_col: "x_cnt", exposure_col: "x_exp"
    }).reset_index()
    b = base.groupby(list(key_cols))[[count_col, exposure_col]].sum().rename(columns={
        count_col: "b_cnt", exposure_col: "b_exp"
    }).reset_index()

    merged = pd.merge(r, b, on=list(key_cols), how="outer").fillna(0)

    # rate 계산
    merged["x_rate"] = np.where(merged["x_exp"]>0, merged["x_cnt"] / merged["x_exp"], 0.0)
    merged["b_rate"] = np.where(merged["b_exp"]>0, merged["b_cnt"] / merged["b_exp"], 0.0)

    # 최근 기대 rate = b_rate (기준선)
    merged["expected_recent_rate"] = merged["b_rate"]

    # z-score (안정화: base_rate 최소값 바닥치)
    eps = 1e-9
    denom = np.sqrt((merged["b_rate"] + eps) / (merged["x_exp"] + eps))
    merged["z"] = (merged["x_rate"] - merged["b_rate"]) / denom.replace(0, np.nan)
    merged["z"] = merged["z"].replace([np.inf, -np.inf], 0).fillna(0)

    merged["flag"] = np.select(
        [merged["z"] >= SURGE_Z_THRESHOLD, merged["z"] <= -SURGE_Z_THRESHOLD],
        ["상승","하락"], default="정상"
    )

    # 참고용 컬럼(기존표현 유지)
    merged["x"] = merged["x_cnt"]
    merged["base_count"] = merged["b_cnt"]
    merged["expected_recent"] = merged["expected_recent_rate"] * merged["x_exp"]

    # 정렬
    merged = merged.sort_values("z", ascending=False)
    return merged

# -----------------------------
# 사이드바: 업로드 (교체)
# -----------------------------
with st.sidebar:
    st.header("① 데이터 업로드")
    uploaded = st.file_uploader("엑셀/CSV 업로드", type=["csv","xlsx","xls"])

    # 사용자가 직접 입력해 강제 지정할 수 있는 텍스트 입력(옵션)
    sheet_name_input = st.text_input("엑셀 시트명(옵션)", value="")

    # 업로드된 파일이 엑셀이라면: 시트 목록 안내 + 선택 박스 제공
    sheet_choice = None
    if uploaded and uploaded.name.lower().endswith((".xlsx", ".xls")):
        try:
            import openpyxl  # ensure installed
            # 업로더 스트림을 한번 읽으면 포인터가 이동하므로, 사용 후 반드시 seek(0) 복구
            xls = pd.ExcelFile(uploaded, engine="openpyxl")
            # 목록 안내
            st.caption("이 파일의 시트: " + ", ".join(xls.sheet_names))

            # 추천 기본 시트(있으면 incidents/data/sheet1 → 없으면 첫 시트)
            preferred = [s for s in xls.sheet_names if s.lower() in ("incidents", "data", "sheet1")]
            default_sheet = preferred[0] if preferred else xls.sheet_names[0]
            default_idx = xls.sheet_names.index(default_sheet)

            # 선택 UI
            sheet_choice = st.selectbox("시트 선택(자동 감지)", options=xls.sheet_names, index=default_idx)

        except Exception as e:
            st.warning(f"시트 목록을 읽는 중 문제가 발생했습니다: {e}")
        finally:
            # 이후 실제 로딩을 위해 파일 포인터 복구
            try:
                uploaded.seek(0)
            except Exception:
                pass

    st.header("② 태그 매칭")
    tag_mode = st.radio("태그 모드", ["ANY(하나라도 일치)","ALL(모두 포함)"], index=0)
    st.caption("💡 업로드 후 상단 탭에서 피벗/경보/액션/내보내기를 사용하세요.")

if uploaded is None:
    st.info("왼쪽에서 CSV 또는 엑셀 파일을 업로드하세요. (V5 스키마 권장)")
    st.stop()

# 텍스트 입력이 우선, 없으면 선택박스 값 사용
chosen_sheet = sheet_name_input.strip() or sheet_choice

try:
    df_raw = load_file(uploaded, sheet_name=chosen_sheet if chosen_sheet else None)
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

    # material_type→ material_name/code 의존을 위한 맵
    map_type_to_materials = (
        df.groupby("material_type")[["material_code","material_name"]]
          .apply(lambda g: g.drop_duplicates().to_dict("records"))
          .to_dict()
    )

    # 1행
    c1,c2,c3,c4 = st.columns(4)
    with c1:
        plants = st.multiselect("공장(plant)", sorted([p for p in df["plant"].unique() if p!=""]))
    with c2:
        lines = st.multiselect("라인(line)", sorted([p for p in df["line"].unique() if p!=""]))
    with c3:
        suppliers = st.multiselect("공급사 코드(supplier_code)", sorted([p for p in df["supplier_code"].unique() if p!=""]), key="supplier_select")
    with c4:
        supplier_names = st.multiselect("공급사명(supplier_name)", sorted([p for p in df["supplier_name"].unique() if p!=""]))

    # 2행
    c5,c6,c7,c8 = st.columns(4)
    with c5:
        mat_types = st.multiselect("원료대분류(material_type)", sorted([p for p in df["material_type"].unique() if p!=""]), key="mat_type_select")
    with c6:
        # material_type & supplier 교집합으로 material 후보 제한
        if mat_types:
            subset = df[df["material_type"].isin(mat_types)]
        else:
            subset = df
        if suppliers or supplier_names:
            subset = subset[
                subset["supplier_code"].isin(suppliers) if suppliers else subset.index.isin(subset.index)
            ]
            if supplier_names:
                subset = subset[subset["supplier_name"].isin(supplier_names)]
        mat_name_opts = sorted(subset["material_name"].dropna().unique())
        material_names = st.multiselect("원료명(material_name)", mat_name_opts, key="material_name_select")
    with c7:
        # material_name을 재차 반영해 code 후보 제한
        subset2 = subset[subset["material_name"].isin(material_names)] if material_names else subset
        mat_code_opts = sorted(subset2["material_code"].dropna().unique())
        materials = st.multiselect("원료코드(material_code)", mat_code_opts, key="material_code_select")
    with c8:
        fo_types = st.multiselect("이물 유형(contam_type)", sorted([p for p in df["contam_type"].unique() if p!=""]))

    # 3행
    c9,c10,c11,c12 = st.columns(4)
    with c9:
        severities = st.multiselect("중대/일반(severity)", ["중대","일반"])
    with c10:
        origins = st.multiselect("원산지(origin)", sorted([p for p in df["origin"].unique() if p!=""]))
    with c11:
        imported = st.multiselect("수입여부(imported)", sorted([p for p in df["imported"].unique() if p!=""]))
    with c12:
        unique_tags = sorted({t for row in df["color_tags"] for t in split_tags(row)})
        tags = st.multiselect("태그(color_tags)", unique_tags)

    # 4행 (기간)
    c13,c14,c15,c16 = st.columns(4)
    with c13:
        date_from = st.date_input("시작일", value=default_start)
    with c14:
        date_to = st.date_input("종료일", value=default_end)
    with c15:
        st.write("")  # 자리맞춤
    with c16:
        st.write("")  # 자리맞춤

    # ---- 필터 적용 ----
    f = df.copy()
    if plants:         f = f[f["plant"].isin(plants)]
    if lines:          f = f[f["line"].isin(lines)]
    if suppliers:      f = f[f["supplier_code"].isin(suppliers)]
    if supplier_names: f = f[f["supplier_name"].isin(supplier_names)]
    if mat_types:      f = f[f["material_type"].isin(mat_types)]
    if material_names: f = f[f["material_name"].isin(material_names)]
    if materials:      f = f[f["material_code"].isin(materials)]
    if fo_types:       f = f[f["contam_type"].isin(fo_types)]
    if severities:     f = f[f["severity"].isin(severities)]
    if origins:        f = f[f["origin"].isin(origins)]
    if imported:       f = f[f["imported"].isin(imported)]
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
        row_opts = ["plant","line","supplier_code","supplier_name","material_type","material_code","material_name","contam_type","severity","origin","imported"]
        rows = st.multiselect("행(다중 선택)", row_opts)
    with pv_c2:
        col_opts = ["plant","line","supplier_code","material_type","material_code","contam_type","severity","origin","imported"]
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

    # (탭① 피벗/필터 내부) 축 전환 토글 라인 대체
    axis_toggle = st.toggle("차트 가로/세로축 전환 (기본=가로형)", value=True)  # False=세로형, True=가로형 기본


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

        # ---- 피벗 차트 (Altair + 축 전환) ----
        st.markdown("##### 피벗 차트")
        chart_df = pt.copy()
        if isinstance(chart_df, pd.Series):
            chart_df = chart_df.to_frame("value")
        if isinstance(chart_df.index, pd.MultiIndex):
            chart_df.index = flatten_index(chart_df.index)
        if isinstance(chart_df.columns, pd.MultiIndex):
            chart_df.columns = flatten_index(chart_df.columns)
        chart_df = chart_df.reset_index().rename(columns={"index":"row"})
        # 폭 제한
        if chart_df.shape[0] > 2000:
            st.caption("⚠️ 차트 성능을 위해 상위 2000 셀만 표시합니다.")
            chart_df = chart_df.head(2000)

        # wide->long
        chart_long = chart_df.melt(id_vars=chart_df.columns[0], var_name="col", value_name="val")
        row_field = chart_df.columns[0]

        base = alt.Chart(chart_long).transform_filter(alt.datum.val != None)
        if axis_toggle:
            enc = base.mark_bar().encode(
                x=alt.X("val:Q", title="값"),
                y=alt.Y(f"{row_field}:N", title="행"),
                color=alt.Color("col:N", title="열", legend=alt.Legend(columns=2))
            )
        else:
            enc = base.mark_bar().encode(
                x=alt.X(f"{row_field}:N", title="행"),
                y=alt.Y("val:Q", title="값"),
                color=alt.Color("col:N", title="열", legend=alt.Legend(columns=2))
            )

        if chart_type.startswith("선"):
            enc = enc.mark_line(point=True)
        elif chart_type.startswith("영역"):
            enc = enc.mark_area(opacity=0.6)

        st.altair_chart(enc.properties(height=420), use_container_width=True)
    else:
        st.info("행 차원을 1개 이상 선택하면 피벗이 생성됩니다.")

# -----------------------------
# ② 경보 보드 (V5: rate 기반)
# -----------------------------
with tab2:
    st.subheader("신규 이물 / 급증 경보 보드 (이물수준 기반)")

    # 신규 이물
    with st.expander("신규 이물 발생 (조합: 공급사+원료)", expanded=True):
        nov_df = detect_novel_types(st.session_state["filtered_df"])
        nov_view = nov_df[nov_df["is_novel_type"]].sort_values("dt", ascending=False)
        st.session_state["alerts_novel"] = nov_view
        st.write(f"신규 유형 발생 건수: **{len(nov_view):,}**")
        st.dataframe(nov_view.head(200), use_container_width=True)

    # 급증/하락 (rate-based)
    with st.expander(f"급증/하락 탐지 (최근 {DEFAULT_RECENT_DAYS}일 vs 과거 {DEFAULT_BASELINE_DAYS}일, z≥±{SURGE_Z_THRESHOLD})", expanded=True):
        surge_df = rate_change_flag_v5(
            st.session_state["filtered_df"],
            recent_days=int(DEFAULT_RECENT_DAYS),
            baseline_days=int(DEFAULT_BASELINE_DAYS),
        )
        st.session_state["alerts_surge"] = surge_df
        if surge_df is not None and not surge_df.empty:
            st.write(f"분석 대상 조합 수: **{len(surge_df):,}**")
            st.dataframe(
                surge_df[["supplier_code","material_code","contam_type","x_cnt","x_exp","x_rate","b_cnt","b_exp","b_rate","expected_recent_rate","z","flag"]].head(200),
                use_container_width=True
            )

            s1, s2, s3 = st.columns(3)
            with s1: st.metric("상승 경보", int((surge_df["flag"]=="상승").sum()))
            with s2: st.metric("하락 감지", int((surge_df["flag"]=="하락").sum()))
            with s3: st.metric("정상", int((surge_df["flag"]=="정상").sum()))

        # ----- 여기부터 교체: 선택 항목 그래프 (최근 180일 '이물수준' 시계열) -----
        st.markdown("##### 선택 항목 그래프 (최근 180일 일일 이물수준 + b/expected/x rate 선)")

        view_df = surge_df.head(200).copy()
        view_df["key"] = view_df["supplier_code"] + " | " + view_df["material_code"] + " | " + view_df["contam_type"]
        sel = st.selectbox("항목 선택 (공급사 | 원료 | 유형)", options=view_df["key"].tolist())
        srow = view_df[view_df["key"]==sel].iloc[0]

        # 최근 180일 범위
        f2 = st.session_state["filtered_df"].copy()
        today = pd.to_datetime(f2["dt"]).max().date()
        base_start = today - timedelta(days=DEFAULT_BASELINE_DAYS-1)

        # 해당 조합 데이터 필터
        mask = (
            (f2["supplier_code"] == srow["supplier_code"]) &
            (f2["material_code"]  == srow["material_code"]) &
            (f2["contam_type"]    == srow["contam_type"]) &
            (pd.to_datetime(f2["dt"]).dt.date >= base_start) &
            (pd.to_datetime(f2["dt"]).dt.date <= today)
)

        ts = f2.loc[mask, ["dt","count","selection_amount_kg"]].copy()
        ts["dt"] = pd.to_datetime(ts["dt"]).dt.date

        # 일일 합산 (분자/분모)
        daily = (
        ts.groupby("dt")[["count","selection_amount_kg"]]
            .sum()
              .reindex([base_start + timedelta(days=i) for i in range(DEFAULT_BASELINE_DAYS)], fill_value=0)
            .reset_index()
            .rename(columns={"index":"dt"})
)
        # 일일 이물수준 = sum(count)/sum(kg)
        daily["daily_rate"] = np.where(daily["selection_amount_kg"]>0,
                                       daily["count"]/daily["selection_amount_kg"], 0.0)

        # 기준선/기대/최근 rate (전기간 동일 값의 '수평선'을 시계열로 표현)
        b_rate  = float(srow.get("b_rate", 0.0))
        exp_rate = float(srow.get("expected_recent_rate", b_rate))
        x_rate  = float(srow.get("x_rate", 0.0))

        lines_df = pd.DataFrame({
            "dt": list(daily["dt"])*3,
            "value": [b_rate]*len(daily) + [exp_rate]*len(daily) + [x_rate]*len(daily),
            "type": (["기준선 b_rate"]*len(daily)) + (["최근 기대 expected_rate"]*len(daily)) + (["최근 실측 x_rate"]*len(daily))
})

        # 최근 7일 하이라이트 밴드
        recent_start = today - timedelta(days=DEFAULT_RECENT_DAYS-1)
        band = alt.Chart(pd.DataFrame({"start":[recent_start], "end":[today]})).mark_rect(
            opacity=0.08, color="#E53935"
        ).encode(x="start:T", x2="end:T")

        # 산점도(일일 이물수준)
        points = alt.Chart(daily).mark_circle(size=40, opacity=0.65).encode(
            x=alt.X("dt:T", title="일자"),
            y=alt.Y("daily_rate:Q", title="일일 이물수준 (count/kg)", axis=alt.Axis(format=".4f"))
)

        # 선(b_rate / expected_recent_rate / x_rate)
        lines = alt.Chart(lines_df).mark_line(size=2).encode(
            x="dt:T",
            y=alt.Y("value:Q", title="일일 이물수준 (count/kg)", axis=alt.Axis(format=".4f")),
            color=alt.Color("type:N", title=None)
)

        st.altair_chart((band + points + lines).properties(height=360), use_container_width=True)
        st.caption("• 점: 최근 180일의 일일 이물수준(분자합/선별량합)  • 선: b_rate / expected_recent_rate / x_rate (기간 전체 동일 값)")

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
            intro = f"[자동생성] 이물수준 급증/하락·신규 유형 모니터링 알림 – {today_str}\n"

            lines_out = []
            for _, r in top_df.iterrows():
                key = f"{r.get('supplier_code','')}-{r.get('material_code','')}-{r.get('contam_type','')}"
                lines_out.append(
                    f"• {key}: 최근 rate={r.get('x_rate',0):.4f}, 기준선 rate={r.get('b_rate',0):.4f}, z={r.get('z',0):.2f}, 판정={r.get('flag','')}"
                )
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

    # XLSX 엔진 폴백 (XlsxWriter -> openpyxl)
    try:
        import xlsxwriter  # noqa
        engine = "xlsxwriter"
    except Exception:
        engine = "openpyxl"

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine=engine) as writer:
        if not f.empty:
            f.to_excel(writer, sheet_name="FilteredData", index=False)
            try:
                writer.sheets["FilteredData"].freeze_panes(1,0)
            except Exception:
                pass
        if pv is not None:
            pv_out = pv.copy()
            if isinstance(pv_out, pd.Series):
                pv_out = pv_out.to_frame("value")
            if isinstance(pv_out.index, pd.MultiIndex):
                pv_out.index = [' | '.join(map(str, t)) for t in pv_out.index]
            if isinstance(pv_out.columns, pd.MultiIndex):
                pv_out.columns = [' | '.join(map(str, t)) for t in pv_out.columns]
            pv_out.to_excel(writer, sheet_name="Pivot", merge_cells=False)
            try:
                writer.sheets["Pivot"].freeze_panes(1,1)
            except Exception:
                pass
        if nov is not None and not nov.empty:
            nov.to_excel(writer, sheet_name="NovelAlerts", index=False)
            try:
                writer.sheets["NovelAlerts"].freeze_panes(1,0)
            except Exception:
                pass
        if surge is not None and not surge.empty:
            surge.to_excel(writer, sheet_name="SurgeAlerts", index=False)
            try:
                writer.sheets["SurgeAlerts"].freeze_panes(1,0)
            except Exception:
                pass

    st.download_button("엑셀 보고서(XLSX) 다운로드", data=output.getvalue(), file_name="FOCast_report.xlsx")

st.caption("※ 고도화: rate 임계치 정책/가중, LOT↔제품 트레이스, 자동 메일/Teams 전송(Graph API) 등 확장 가능.")
