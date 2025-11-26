# FOB.py (v0.3) — FOCast web app
# V5 schema + rate-based alerts + dependent filters + chart axis toggle + XLSX engine fallback

import io
import json
import re
from datetime import datetime, timedelta, date
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import pydeck as pdk

# 시각화(축 전환/레이어링)를 위해 Altair 사용
import altair as alt

# -----------------------------
# 전역 설정 및 상수
# -----------------------------
st.set_page_config(page_title="FOCast - 이물 분석·알림", layout="wide", initial_sidebar_state="expanded")
APP_TITLE = "FOCast – 이물 분석·알림 웹앱"
HIGH_RISK_STATE_PATH = Path(".high_risk_state.json")

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

COUNTRY_CENTROIDS = {
    "China": (35.0, 103.0),
    "South Korea": (36.5, 127.8),
    "Republic of Korea": (36.5, 127.8),
    "Korea": (36.5, 127.8),
    "United States": (39.5, -98.35),
    "USA": (39.5, -98.35),
    "United States of America": (39.5, -98.35),
    "Japan": (36.2, 138.3),
    "Vietnam": (14.0, 108.0),
    "Thailand": (15.8, 101.0),
    "Indonesia": (-2.5, 118.0),
    "India": (22.0, 79.0),
    "Canada": (56.0, -106.0),
    "Brazil": (-10.0, -55.0),
    "Mexico": (23.5, -102.0),
    "Germany": (51.0, 10.0),
    "France": (46.2, 2.2),
    "United Kingdom": (55.0, -2.0),
    "U.K.": (55.0, -2.0),
    "UK": (55.0, -2.0),
    "Spain": (40.0, -4.0),
    "Italy": (42.5, 12.5),
    "Australia": (-25.0, 133.0),
    "New Zealand": (-41.0, 174.0),
    "Russia": (60.0, 90.0),
    "Türkiye": (39.0, 35.0),
    "Turkey": (39.0, 35.0),
    "Philippines": (12.5, 122.5),
    "Malaysia": (4.5, 102.0),
    "Taiwan": (23.7, 121.0),
    "Hong Kong": (22.3, 114.2),
    "Singapore": (1.35, 103.8),
    "Argentina": (-34.0, -64.0),
    "Chile": (-30.0, -71.0),
}

st.title(APP_TITLE)

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


CONTAM_WIDE_PATTERN = re.compile(r"^contam_type_(중대|일반)_(.+)_count$")


def reshape_wide_contam(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """신규 wide schema(contam_type_*_count) → long 변환."""
    contam_cols = [c for c in df.columns if isinstance(c, str) and CONTAM_WIDE_PATTERN.match(c)]
    if not contam_cols:
        return df, False

    base_cols = [c for c in df.columns if c not in contam_cols]
    work = df.reset_index(drop=True).copy()
    work["_base_id"] = work.index

    melted = work.melt(
        id_vars=base_cols + ["_base_id"],
        value_vars=contam_cols,
        var_name="_contam_key",
        value_name="count",
    )

    extracted = melted["_contam_key"].str.extract(CONTAM_WIDE_PATTERN)
    melted["severity"] = extracted[0].fillna("")
    melted["contam_type"] = extracted[1].fillna("")
    melted.drop(columns=["_contam_key"], inplace=True)

    melted["count"] = pd.to_numeric(melted["count"], errors="coerce").fillna(0).astype(int)
    if "selection_amount_kg" in melted.columns:
        melted["selection_amount_kg"] = pd.to_numeric(
            melted["selection_amount_kg"], errors="coerce"
        ).fillna(0.0)
    else:
        melted["selection_amount_kg"] = 0.0

    melted["selection_amount_kg_unique"] = 0.0
    if "selection_amount_kg" in melted.columns:
        first_idx = melted.groupby("_base_id").head(1).index
        melted.loc[first_idx, "selection_amount_kg_unique"] = melted.loc[first_idx, "selection_amount_kg"]

    return melted, True


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """필수 컬럼/타입 보정 (V5)"""
    df, _ = reshape_wide_contam(df)

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    # 날짜 파싱: V5는 날짜까지만 존재
    try:
        df["dt"] = pd.to_datetime(df["dt"]).dt.date
    except Exception:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date

    # 숫자형
    numeric_targets = ["count", "selection_amount_kg", "이물수준", "중대이물 수준", "일반이물 수준"]
    for c in numeric_targets:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["count"] = df["count"].fillna(0).astype(int)
    df["selection_amount_kg"] = df["selection_amount_kg"].fillna(0.0).astype(float)
    for c in ["이물수준","중대이물 수준","일반이물 수준"]:
        df[c] = df[c].fillna(0.0).astype(float)

    if "selection_amount_kg_unique" not in df.columns:
        df["selection_amount_kg_unique"] = df["selection_amount_kg"]
    else:
        df["selection_amount_kg_unique"] = pd.to_numeric(
            df["selection_amount_kg_unique"], errors="coerce"
        ).fillna(0.0)
    df["selection_amount_kg_unique"] = df["selection_amount_kg_unique"].astype(float)

    if "_base_id" not in df.columns:
        df["_base_id"] = np.arange(len(df))

    # 문자열형
    str_cols = [
        "plant","line","material_type","material_code","material_name",
        "supplier_code","supplier_name","contam_type","color_tags",
        "unit","lot_no","severity","photo_url","notes","origin","imported"
    ]
    for c in str_cols:
        df[c] = df[c].fillna("").astype(str)

    return df


def normalize_origin_name(origin: str) -> str:
    """원산지 문자열을 글로벌 표준 국호로 정규화."""
    if origin is None:
        return ""
    raw = str(origin).strip()
    if not raw:
        return ""

    # remove brackets/separators then normalize tokens
    cleaned = re.sub(r"[()\\[\\]]", " ", raw)
    tokens = [t for t in re.split(r"[/,;]|\s+", cleaned) if t]
    candidate_tokens = [cleaned, raw] + tokens

    lower_map = {k.lower(): v for k, v in ORIGIN_ALIASES.items()}
    iso_map = {k.lower(): v for k, v in ISO_COUNTRY_CODES.items()}

    for cand in candidate_tokens:
        if cand in ORIGIN_ALIASES:
            return ORIGIN_ALIASES[cand]
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
        if cand.upper() in ISO_COUNTRY_CODES:
            return ISO_COUNTRY_CODES[cand.upper()]
        if cand.lower() in iso_map:
            return iso_map[cand.lower()]
        if cand in COUNTRY_CENTROIDS:
            return cand

    centroid_lower = {k.lower(): k for k in COUNTRY_CENTROIDS.keys()}
    if raw.lower() in centroid_lower:
        return centroid_lower[raw.lower()]

    return raw

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
st.session_state.setdefault("high_risk_df", pd.DataFrame())
st.session_state.setdefault("high_risk_options", {})
st.session_state.setdefault("high_risk_loaded_from_disk", False)


def load_high_risk_state_from_disk():
    if st.session_state.get("high_risk_loaded_from_disk"):
        return
    if HIGH_RISK_STATE_PATH.exists():
        try:
            data = json.loads(HIGH_RISK_STATE_PATH.read_text(encoding="utf-8"))
            items = data.get("items", [])
            opts = data.get("options", {})
            st.session_state["high_risk_df"] = pd.DataFrame(items)
            st.session_state["high_risk_options"] = opts
            st.success("저장된 고위험 리스트 상태를 자동 복원했습니다.")
        except Exception:
            st.warning("서버 측 저장된 고위험 상태를 읽는 중 문제가 발생했습니다. 새로 구성해주세요.")
    st.session_state["high_risk_loaded_from_disk"] = True


load_high_risk_state_from_disk()


def persist_high_risk_state(df: pd.DataFrame, options: dict):
    try:
        payload = {
            "items": df.to_dict("records"),
            "options": options or {},
        }
        HIGH_RISK_STATE_PATH.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return True
    except Exception:
        return False

# -----------------------------
# 탭 구성
# -----------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "① 피벗/필터 검색", "② 경보 보드", "③ 액션 템플릿", "④ 내보내기", "⑤ 고위험·지도"
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
    hide_cols = [c for c in f.columns if str(c).startswith("_")]
    st.dataframe(f.head(200).drop(columns=hide_cols, errors="ignore"), use_container_width=True)

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
        grp = list(dict.fromkeys(grp))

        if "_base_id" in frame.columns:
            dedup_cols = grp + ["_base_id", "selection_amount_kg"]
            dedup = frame[dedup_cols].drop_duplicates(subset=grp + ["_base_id"])
            denom = dedup.groupby(grp)["selection_amount_kg"].sum()
        else:
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

with tab2:
    st.subheader("신규 이물 / 급증 경보 보드 (이물수준 기반)")

    # -----------------------------
    # 공통: 컬럼 정규화 + 타입 보정
    # -----------------------------
    def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        base = {c: c.strip().lower() for c in df.columns}
        df.rename(columns=base, inplace=True)
        # 동의어 매핑
        mapping = {
            "date": "dt", "datetime": "dt", "time": "dt",
            "factory": "plant", "site": "plant", "plantname": "plant",
            "linename": "line", "line_id": "line",
            "materialtype": "material_type", "mat_type": "material_type",
            "material": "material_code", "material_cd": "material_code", "item_code": "material_code", "sku": "material_code",
            "supplier": "supplier_code", "vendor": "supplier_code", "vendor_code": "supplier_code",
            "contam": "contam_type", "defect_type": "contam_type", "foreign_matter_type": "contam_type",
            "qty_kg": "selection_amount_kg", "amount_kg": "selection_amount_kg", "selection_kg": "selection_amount_kg",
            "counts": "count", "count_num": "count",
        }
        for src, dst in mapping.items():
            if src in df.columns and dst not in df.columns:
                df.rename(columns={src: dst}, inplace=True)
        # 타입
        if "dt" in df.columns:
            df["dt"] = pd.to_datetime(df["dt"]).dt.date
        df["count"] = pd.to_numeric(df.get("count", 0), errors="coerce").fillna(0)
        df["selection_amount_kg"] = pd.to_numeric(df.get("selection_amount_kg", 0), errors="coerce").fillna(0)
        if "selection_amount_kg_unique" in df.columns:
            df["selection_amount_kg_unique"] = pd.to_numeric(
                df.get("selection_amount_kg_unique", 0), errors="coerce"
            ).fillna(0)
        # 키 누락 방지용 빈 컬럼
        for k in ["plant","line","material_type","material_code","supplier_code","contam_type"]:
            if k not in df.columns:
                df[k] = ""
        return df

    fdf = normalize_columns(st.session_state["filtered_df"])
    if fdf.empty:
        st.info("표시할 데이터가 없습니다.")
        st.stop()

    # -----------------------------
    # 파라미터/기간
    # -----------------------------
    TODAY = fdf["dt"].max()
    RECENT_DAYS = int(DEFAULT_RECENT_DAYS)
    BASE_DAYS   = int(DEFAULT_BASELINE_DAYS)
    SURGE_Z     = float(SURGE_Z_THRESHOLD)
    EPS         = 1e-9

    recent_start  = TODAY - timedelta(days=RECENT_DAYS - 1)
    baseline_end  = recent_start - timedelta(days=1)
    baseline_start= baseline_end - timedelta(days=BASE_DAYS - 1)

    KEY7 = ["plant","line","material_type","material_code","supplier_code","contam_type"]

    # -----------------------------
    # 1) 급증/하락 (rate 기반 z-점수, 키=7개)
    # -----------------------------
    def rate_change_flag_v5_full(df: pd.DataFrame,
                                 recent_days: int,
                                 baseline_days: int) -> pd.DataFrame:
        df = df.copy()

        # 윈도우 분할
        mask_recent   = (df["dt"] >= recent_start) & (df["dt"] <= TODAY)
        mask_baseline = (df["dt"] >= baseline_start) & (df["dt"] <= baseline_end)

        # 일일 합산 (동일 7키 + dt 기준으로 분자/유일 분모 합)
        grp_cols = KEY7 + ["dt"]
        key6 = ["plant","line","material_type","material_code","supplier_code"]

        den_source_col = (
            "selection_amount_kg_unique"
            if "selection_amount_kg_unique" in df.columns
            else "selection_amount_kg"
        )
        df["_selection_amount_unique"] = pd.to_numeric(
            df.get(den_source_col, 0), errors="coerce"
        ).fillna(0.0)

        def build_daily(mask):
            counts = (
                df.loc[mask, grp_cols + ["count"]]
                  .groupby(grp_cols, as_index=False)["count"].sum()
            )

            lot_den = (
                df.loc[mask, key6 + ["lot_no","dt","_selection_amount_unique"]]
                  .groupby(key6 + ["lot_no","dt"], as_index=False)["_selection_amount_unique"].sum()
            )
            day_den = (
                lot_den.groupby(key6 + ["dt"], as_index=False)["_selection_amount_unique"].sum()
                       .rename(columns={"_selection_amount_unique": "selection_amount_kg_unique"})
            )

            return counts.merge(day_den, on=key6 + ["dt"], how="left").fillna({"selection_amount_kg_unique": 0.0})

        recent_daily = build_daily(mask_recent)
        base_daily = build_daily(mask_baseline)

        # 최근/기준 기간 합계 (키=7개)
        key7_only = KEY7.copy()
        recent_sum = (
            recent_daily.groupby(key7_only, as_index=False)
                        .agg(x_cnt=("count","sum"), x_den_unique=("selection_amount_kg_unique","sum"))
        )
        base_sum = (
            base_daily.groupby(key7_only, as_index=False)
                      .agg(b_cnt=("count","sum"), b_den_unique=("selection_amount_kg_unique","sum"))
        )

        # 결합
        merged = recent_sum.merge(base_sum, on=key7_only, how="outer").fillna(0)

        # rate 계산 (유일 노출량 기준)
        merged["x_rate"] = np.where(
            merged["x_den_unique"] > 0, merged["x_cnt"] / merged["x_den_unique"], 0.0
        )
        merged["b_rate"] = np.where(
            merged["b_den_unique"] > 0, merged["b_cnt"] / merged["b_den_unique"], 0.0
        )

        # 기대값 E = baseline_rate * recent_den
        merged["x_exp"] = merged["b_rate"] * merged["x_den_unique"]

        # z-score (포아송 근사)
        merged["z"] = np.where(merged["x_exp"] > 0,
                               (merged["x_cnt"] - merged["x_exp"]) / np.sqrt(merged["x_exp"] + EPS),
                               0.0)

        merged["expected_recent_rate"] = np.where(
            merged["x_den_unique"] > 0,
            merged["x_exp"] / merged["x_den_unique"],
            0.0,
        )

        merged["flag"] = np.select(
            [merged["z"] >= SURGE_Z, merged["z"] <= -SURGE_Z],
            ["상승","하락"], default="정상"
        )

        # 표시 순서/컬럼 정리
        cols = key7_only + [
            "x_cnt","x_den_unique","x_rate",
            "b_cnt","b_den_unique","b_rate",
            "expected_recent_rate","z","flag",
        ]
        return merged[cols].sort_values("z", ascending=False)

    with st.expander(f"급증/하락 탐지 (최근 {RECENT_DAYS}일 vs 과거 {BASE_DAYS}일, z≥±{SURGE_Z})", expanded=True):
        surge_df = rate_change_flag_v5_full(fdf, RECENT_DAYS, BASE_DAYS)
        st.session_state["alerts_surge"] = surge_df
        if surge_df is not None and not surge_df.empty:
            st.write(f"분석 대상 조합 수: **{len(surge_df):,}**")
            st.dataframe(
                surge_df[KEY7 + ["x_cnt","x_den_unique","x_rate","b_cnt","b_den_unique","b_rate","expected_recent_rate","z","flag"]].head(200),
                use_container_width=True
            )
            st.caption("노출량(x/b_den_unique)은 LOT·원료 단위로 중복 제거된 selection_amount_kg_unique 합계이며, 기존 selection_amount_kg 기반 집계와 다를 수 있습니다.")
            s1, s2, s3 = st.columns(3)
            with s1: st.metric("상승 경보", int((surge_df["flag"]=="상승").sum()))
            with s2: st.metric("하락 감지", int((surge_df["flag"]=="하락").sum()))
            with s3: st.metric("정상", int((surge_df["flag"]=="정상").sum()))
        else:
            st.info("급증/하락 분석 대상 데이터가 없습니다.")

    def render_alert_details():
        # ----- 여기부터 교체: 선택 항목 그래프 (최근 180일 '이물수준' 시계열 + SPC) -----
        st.markdown("##### 선택 항목 그래프 (최근 180일 일일 이물수준 + b/expected/x rate 선)")

        def as_str(v):
            return "" if pd.isna(v) else str(v)

        view_df = surge_df.head(1000).copy()
        view_df["key"] = (
            view_df["plant"].map(as_str) + " | " +
            view_df["line"].map(as_str) + " | " +
            view_df["material_type"].map(as_str) + " | " +
            view_df["supplier_code"].map(as_str) + " | " +
            view_df["material_code"].map(as_str) + " | " +
            view_df["contam_type"].map(as_str)
        )

        sel = st.selectbox(
            "항목 선택 (plant | line | material_type | supplier | material | contam)",
            options=view_df["key"].tolist(),
        )
        srow = view_df[view_df["key"] == sel].iloc[0]

        base_start = TODAY - timedelta(days=BASE_DAYS - 1)

        mask = (
            (fdf["dt"] >= base_start) & (fdf["dt"] <= TODAY) &
            (fdf["plant"] == srow["plant"]) &
            (fdf["line"] == srow["line"]) &
            (fdf["material_type"] == srow["material_type"]) &
            (fdf["material_code"] == srow["material_code"]) &
            (fdf["supplier_code"] == srow["supplier_code"]) &
            (fdf["contam_type"] == srow["contam_type"])
        )
        ts = fdf.loc[mask, ["dt", "count", "selection_amount_kg", "selection_amount_kg_unique", "lot_no", "plant", "line", "material_type", "material_code", "supplier_code"]].copy()

        ts["_detail_den"] = pd.to_numeric(
            ts["selection_amount_kg_unique"] if "selection_amount_kg_unique" in ts else ts.get("selection_amount_kg", 0),
            errors="coerce",
        ).fillna(0.0)

        calendar = pd.DataFrame({"dt": [base_start + timedelta(days=i) for i in range(BASE_DAYS)]})

        count_daily = ts.groupby("dt", as_index=False)["count"].sum()
        den_lot = (
            ts.groupby(["plant","line","material_type","material_code","supplier_code","lot_no","dt"], as_index=False)["_detail_den"].sum()
        )
        den_daily = den_lot.groupby("dt", as_index=False)["_detail_den"].sum()

        daily = (
            count_daily.merge(den_daily, on="dt", how="outer")
                       .merge(calendar, on="dt", how="right")
                       .fillna({"count": 0, "_detail_den": 0.0})
                       .sort_values("dt")
        )
        daily = daily.rename(columns={"_detail_den": "selection_amount_kg_unique"})
        daily["has_selection"] = daily["selection_amount_kg_unique"] > 0
        daily["daily_rate"] = np.where(
            daily["selection_amount_kg_unique"] > 0,
            daily["count"] / daily["selection_amount_kg_unique"],
            0.0,
        )

        b_rate = float(srow.get("b_rate", 0.0)) if "b_rate" in srow else 0.0
        exp_rate = float(srow.get("expected_recent_rate", b_rate)) if "expected_recent_rate" in srow else b_rate
        x_rate = float(srow.get("x_rate", 0.0)) if "x_rate" in srow else 0.0

        lines_df = pd.DataFrame({
            "dt": list(daily["dt"]) * 3,
            "value": [b_rate] * len(daily) + [exp_rate] * len(daily) + [x_rate] * len(daily),
            "type": (["기준선 b_rate"] * len(daily)) +
                    (["최근 기대 expected_rate"] * len(daily)) +
                    (["최근 실측 x_rate"] * len(daily)),
        })

        recent_start = TODAY - timedelta(days=RECENT_DAYS - 1)
        band = alt.Chart(pd.DataFrame({"start": [recent_start], "end": [TODAY]})).mark_rect(
            opacity=0.08, color="#E53935"
        ).encode(x="start:T", x2="end:T")

        points_sel = alt.Chart(daily[daily["has_selection"]]).mark_circle(size=55, opacity=0.75).encode(
            x=alt.X("dt:T", title="일자"),
            y=alt.Y("daily_rate:Q", title="일일 이물수준 (count/kg)", axis=alt.Axis(format=".4f")),
            color=alt.value("#1E88E5"),
            shape=alt.value("circle"),
            tooltip=["dt:T", "count:Q", "selection_amount_kg_unique:Q", "daily_rate:Q"],
        )

        points_nosel = alt.Chart(daily[~daily["has_selection"]]).mark_square(size=45, opacity=0.45).encode(
            x=alt.X("dt:T"),
            y=alt.Y("daily_rate:Q"),
            color=alt.value("#9E9E9E"),
            shape=alt.value("square"),
            tooltip=["dt:T", alt.Tooltip("selection_amount_kg_unique:Q", title="selection_kg_unique")],
        )

        lines = alt.Chart(lines_df).mark_line(size=2).encode(
            x="dt:T",
            y=alt.Y("value:Q", title="일일 이물수준 (count/kg)", axis=alt.Axis(format=".4f")),
            color=alt.Color("type:N", title=None),
        )

        st.altair_chart((band + points_nosel + points_sel + lines).properties(height=360), use_container_width=True)
        st.caption("• 원형=선별 有, 회색 사각형=선별 無  • 선: b_rate / expected_recent_rate / x_rate (기간 전체 동일 값)")
        st.markdown("###### 업체 품질 수준 비교 (최대 3개)")
        key_to_row = view_df.set_index("key")

        def _build_daily_from_row(row):
            comp_mask = (
                (fdf["dt"] >= base_start) & (fdf["dt"] <= TODAY) &
                (fdf["plant"] == row["plant"]) &
                (fdf["line"] == row["line"]) &
                (fdf["material_type"] == row["material_type"]) &
                (fdf["material_code"] == row["material_code"]) &
                (fdf["supplier_code"] == row["supplier_code"]) &
                (fdf["contam_type"] == row["contam_type"])
            )
            comp_ts = fdf.loc[comp_mask, [
                "dt",
                "plant",
                "line",
                "material_type",
                "material_code",
                "supplier_code",
                "lot_no",
                "count",
                "selection_amount_kg",
                "selection_amount_kg_unique",
            ]].copy()
            comp_ts["_detail_den"] = pd.to_numeric(
                comp_ts["selection_amount_kg_unique"] if "selection_amount_kg_unique" in comp_ts else comp_ts.get("selection_amount_kg", 0),
                errors="coerce",
            ).fillna(0.0)

            calendar_comp = pd.DataFrame({"dt": [base_start + timedelta(days=i) for i in range(BASE_DAYS)]})
            comp_count_daily = comp_ts.groupby("dt", as_index=False)["count"].sum()
            comp_den_lot = (
                comp_ts.groupby(
                    ["plant","line","material_type","material_code","supplier_code","lot_no","dt"],
                    as_index=False,
                )["_detail_den"].sum()
            )
            comp_den_daily = comp_den_lot.groupby("dt", as_index=False)["_detail_den"].sum()

            comp_daily = (
                comp_count_daily.merge(comp_den_daily, on="dt", how="outer")
                                .merge(calendar_comp, on="dt", how="right")
                                .fillna({"count": 0, "_detail_den": 0.0})
                                .sort_values("dt")
            )
            comp_daily = comp_daily.rename(columns={"_detail_den": "selection_amount_kg_unique"})
            comp_daily["has_selection"] = comp_daily["selection_amount_kg_unique"] > 0
            comp_daily["daily_rate"] = np.where(
                comp_daily["selection_amount_kg_unique"] > 0,
                comp_daily["count"] / comp_daily["selection_amount_kg_unique"],
                0.0,
            )
            return comp_daily

        compare_keys = st.multiselect(
            "업체/원료 조합 선택 (최대 3개, 현재 선택 항목 포함 추천)",
            options=view_df["key"].tolist(),
            default=[sel],
            max_selections=3,
            help="선택 항목들 중 최대 3개를 골라 평균/산포/관리도 이탈 기반으로 비교합니다.",
        )

        quality_rows = []
        for ck in compare_keys:
            if ck not in key_to_row.index:
                continue
            crow = key_to_row.loc[ck]
            daily_comp = _build_daily_from_row(crow)
            den_sum = daily_comp["selection_amount_kg_unique"].sum()
            cnt_sum = daily_comp["count"].sum()
            has_data = daily_comp[daily_comp["has_selection"]]

            if den_sum <= 0 or has_data.empty:
                quality_rows.append({
                    "대상": ck,
                    "평균 이물률(count/kg)": 0.0,
                    "산포(표준편차)": 0.0,
                    "CV(%)": 0.0,
                    "SPC 이탈(3σ 기준)": 0,
                    "이물건수": int(cnt_sum),
                    "선별량(kg)": float(den_sum),
                    "활성일수": int(has_data["dt"].nunique()),
                })
                continue

            center_rate = cnt_sum / max(den_sum, 1e-9)
            std_rate = float(has_data["daily_rate"].std(ddof=0))
            cv_pct = float(std_rate / center_rate * 100) if center_rate > 0 else 0.0

            sigma_per_day = np.sqrt(np.maximum(center_rate, 0) / has_data["selection_amount_kg_unique"])
            ucl = center_rate + 3 * sigma_per_day
            spc_breach = int((has_data["daily_rate"] > ucl).sum())

            sup_label = f"{crow.get('supplier_name','') or crow.get('supplier_code','')}".strip() or crow.get("supplier_code", "")
            mat_label = f"{crow.get('material_name','') or crow.get('material_code','')}".strip() or crow.get("material_code", "")
            contam_label = str(crow.get("contam_type", ""))
            short_label = f"{sup_label} | {mat_label} | {contam_label}"

            quality_rows.append({
                "대상": short_label,
                "평균 이물률(count/kg)": center_rate,
                "산포(표준편차)": std_rate,
                "CV(%)": cv_pct,
                "SPC 이탈(3σ 기준)": spc_breach,
                "이물건수": int(cnt_sum),
                "선별량(kg)": float(den_sum),
                "활성일수": int(has_data["dt"].nunique()),
            })

        if quality_rows:
            quality_df = pd.DataFrame(quality_rows)
            st.dataframe(quality_df, use_container_width=True)

            rate_chart = alt.Chart(quality_df).mark_bar(size=40).encode(
                x=alt.X("대상:N", title="업체/원료"),
                y=alt.Y("평균 이물률(count/kg):Q", title="평균 이물률"),
                color=alt.Color("대상:N", legend=None),
                tooltip=list(quality_df.columns),
            )
            spc_chart = alt.Chart(quality_df).mark_bar(size=40).encode(
                x=alt.X("대상:N", title="업체/원료"),
                y=alt.Y("SPC 이탈(3σ 기준):Q", title="관리도 이탈 횟수"),
                color=alt.Color("대상:N", legend=None),
                tooltip=list(quality_df.columns),
            )
            st.altair_chart(rate_chart.properties(height=260), use_container_width=True)
            st.altair_chart(spc_chart.properties(height=220), use_container_width=True)
        else:
            st.info("비교 대상을 선택하면 평균/산포/관리도 이탈 기반 품질 수준을 보여줍니다.")

        st.markdown("###### ▷ 업체 SPC 관리도(u-chart) (선별일수 ≥ 20일일 때 표시)")

        sup_mask = (
            (fdf["dt"] >= base_start) & (fdf["dt"] <= TODAY) &
            (fdf["supplier_code"] == srow["supplier_code"]) &
            (fdf["material_code"] == srow["material_code"]) &
            (fdf["contam_type"] == srow["contam_type"])
        )
        sup_ts = fdf.loc[sup_mask, ["dt", "count", "selection_amount_kg"]].copy()

        sup_daily = (
            sup_ts.groupby("dt", as_index=False)
                  .agg(count=("count", "sum"), kg=("selection_amount_kg", "sum"))
                  .sort_values("dt")
        )

        if len(sup_daily) < 20:
            st.info(
                f"SPC 표시 보류: 선택 조합 "
                f"(supplier={srow['supplier_code']}, material={srow['material_code']}, contam={srow['contam_type']}) "
                f"선별일 수가 {len(sup_daily)}일입니다. (≥ 20일 필요)"
            )
        else:
            def _recommend_chart_type(df_daily: pd.DataFrame) -> tuple[str, str]:
                exposure_ratio = (df_daily["kg"] > 0).mean()
                has_exposure = exposure_ratio >= 0.6
                reason_parts = []
                if has_exposure:
                    kg_pos = df_daily[df_daily["kg"] > 0]["kg"]
                    cv_kg = kg_pos.std() / kg_pos.mean() if not kg_pos.empty and kg_pos.mean() > 0 else 0
                    avg_rate = df_daily["count"].sum() / max(df_daily["kg"].sum(), 1e-9)
                    if cv_kg > 0.3:
                        reason_parts.append("일일 노출량 변동이 큰 편")
                        return "u", " / ".join(reason_parts + ["가변 표본 → u-chart 추천"])
                    if avg_rate < 0.05:
                        reason_parts.append("결함률이 낮고 표본이 비교적 일정")
                        return "p", " / ".join(reason_parts + ["이상비율 관리(p-chart)"])
                    reason_parts.append("노출량이 존재하며 가변성 낮음")
                    return "u", " / ".join(reason_parts + ["결점률 관리(u-chart)"])
                avg_cnt = df_daily["count"].mean()
                if avg_cnt <= 3:
                    return "np", "노출량이 없어 건수 자체를 추적(np-chart)"
                return "c", "노출량이 없어 결점 건수 자체 관리(c-chart)"

            def _build_chart_df(df_daily: pd.DataFrame, chart_type: str) -> pd.DataFrame:
                work = df_daily.copy()
                eps = 1e-9
                if chart_type == "u":
                    work["metric"] = np.where(work["kg"] > 0, work["count"] / work["kg"], np.nan)
                    center = work["count"].sum() / max(work["kg"].sum(), eps)
                    work["sigma"] = np.where(work["kg"] > 0, np.sqrt(np.maximum(center, 0) / work["kg"]), np.nan)
                    work["cl"] = center
                    work["ucl"] = center + 3 * work["sigma"]
                    work["chart_label"] = "u-chart (count/kg)"
                elif chart_type == "p":
                    work["sample"] = work["kg"].replace(0, np.nan)
                    center = work["count"].sum() / max(work["sample"].sum(), eps)
                    work["metric"] = np.where(work["sample"] > 0, work["count"] / work["sample"], np.nan)
                    work["sigma"] = np.where(
                        work["sample"] > 0,
                        np.sqrt(np.maximum(center * (1 - center), 0) / work["sample"]),
                        np.nan,
                    )
                    work["cl"] = center
                    work["ucl"] = center + 3 * work["sigma"]
                    work["chart_label"] = "p-chart (불량비율)"
                elif chart_type == "np":
                    n_est = work["kg"].replace(0, np.nan).mean()
                    n_est = n_est if pd.notna(n_est) and n_est > 0 else max(work["count"].mean(), 1)
                    center_rate = work["count"].sum() / (n_est * max(len(work), 1))
                    work["metric"] = work["count"]
                    work["sigma"] = np.sqrt(np.maximum(center_rate * (1 - center_rate), 0)) * n_est
                    work["cl"] = center_rate * n_est
                    work["ucl"] = work["cl"] + 3 * work["sigma"]
                    work["chart_label"] = "np-chart (불량개수)"
                elif chart_type == "I-MR":
                    work["metric"] = np.where(work["kg"] > 0, work["count"] / work["kg"], work["count"])
                    center = work["metric"].mean()
                    mr = work["metric"].diff().abs()
                    mr_bar = mr[1:].mean()
                    d2 = 1.128
                    sigma = mr_bar / d2 if d2 > 0 else 0
                    work["sigma"] = sigma
                    work["cl"] = center
                    work["ucl"] = center + 3 * sigma
                    work["chart_label"] = "I-MR (개별값)"
                else:  # c-chart
                    work["metric"] = work["count"]
                    center = work["metric"].mean()
                    sigma = np.sqrt(np.maximum(center, 0))
                    work["sigma"] = sigma
                    work["cl"] = center
                    work["ucl"] = center + 3 * sigma
                    work["chart_label"] = "c-chart (결점건수)"
                work["sigma"] = work["sigma"].replace(0, np.nan)
                work["z"] = (work["metric"] - work["cl"]) / work["sigma"]
                return work

            def _detect_rules(chart_df: pd.DataFrame, selected_rules: list[str]) -> tuple[pd.DataFrame, list[dict]]:
                z = chart_df["z"].fillna(0)
                z_pos = z.clip(lower=0)  # 하단 이탈은 무시하고 상단만 관리
                labels = [[] for _ in range(len(chart_df))]
                violations = []

                def _mark(idx_list, rule_name, description):
                    for i in idx_list:
                        labels[i].append(rule_name)
                    for i in idx_list:
                        violations.append({
                            "dt": chart_df.iloc[i]["dt"],
                            "rule": rule_name,
                            "설명": description,
                            "값": chart_df.iloc[i]["metric"],
                        })

                if "3시그마" in selected_rules:
                    breach = chart_df.index[(z_pos > 3)].tolist()
                    _mark(breach, "3시그마", "관리한계 상단(UCL) 초과")

                if "8점 한쪽" in selected_rules:
                    side = np.sign(z_pos.replace(0, np.nan)).fillna(0)
                    run = 0
                    last = 0
                    run_idx = []
                    for i, sgn in enumerate(side):
                        if sgn != 0 and sgn == last:
                            run += 1
                        else:
                            run = 1 if sgn != 0 else 0
                        last = sgn
                        run_idx.append(run)
                    breach = [i for i, r in enumerate(run_idx) if r >= 8]
                    _mark(breach, "8점 한쪽", "연속 8점이 중심선 위쪽")

                if "추세 6점" in selected_rules:
                    inc = dec = 0
                    trend_idx = []
                    for i in range(len(chart_df)):
                        if i == 0:
                            inc = dec = 1
                        else:
                            inc = inc + 1 if chart_df.iloc[i]["metric"] > chart_df.iloc[i-1]["metric"] else 1
                            dec = dec + 1 if chart_df.iloc[i]["metric"] < chart_df.iloc[i-1]["metric"] else 1
                        trend_idx.append(max(inc, dec))
                    breach = [i for i, r in enumerate(trend_idx) if r >= 6 and chart_df.iloc[i]["metric"] >= chart_df["cl"].iloc[0]]
                    _mark(breach, "추세 6점", "연속 6점 상승/하락(상단)")

                if "2/3점 2시그마 이상" in selected_rules:
                    sigma2 = (z_pos >= 2)
                    for i in range(len(chart_df) - 2):
                        window = sigma2.iloc[i:i+3]
                        if window.sum() >= 2:
                            _mark(range(i, i+3), "2/3점 2시그마 이상", "최근 3점 중 2점이 +2σ 이상")

                if "4/5점 1시그마 이상" in selected_rules:
                    sigma1 = (z_pos >= 1)
                    for i in range(len(chart_df) - 4):
                        window = sigma1.iloc[i:i+5]
                        if window.sum() >= 4:
                            _mark(range(i, i+5), "4/5점 1시그마 이상", "최근 5점 중 4점이 +1σ 이상")

                label_series = ["; ".join(sorted(set(l))) if l else "정상" for l in labels]
                chart_df = chart_df.copy()
                chart_df["violation"] = label_series
                return chart_df, violations

            rec_chart, rec_reason = _recommend_chart_type(sup_daily)
            chart_options = ["자동 추천", "u", "c", "p", "np", "I-MR"]
            chart_labels = {
                "u": "u-chart", "c": "c-chart", "p": "p-chart", "np": "np-chart", "I-MR": "I-MR"
            }
            default_idx = chart_options.index(rec_chart) if rec_chart in chart_options else 0
            sel_chart = st.radio(
                "관리도 유형 선택 (자동 추천 포함)",
                chart_options,
                index=default_idx,
                format_func=lambda x: "자동 추천(" + chart_labels.get(rec_chart, "u-chart") + ")" if x == "자동 추천" else chart_labels.get(x, x)
            )
            chosen_chart = rec_chart if sel_chart == "자동 추천" else sel_chart
            st.caption(f"추천 이유: {rec_reason}")

            rule_choices = ["3시그마", "8점 한쪽", "추세 6점", "2/3점 2시그마 이상", "4/5점 1시그마 이상"]
            selected_rules = st.multiselect(
                "Western/Nelson 규칙 적용", rule_choices, default=["3시그마", "8점 한쪽", "2/3점 2시그마 이상"]
            )

            chart_df = _build_chart_df(sup_daily, chosen_chart)
            if chart_df["metric"].isna().all():
                st.warning("선택한 관리도에 필요한 노출량/표본 정보가 부족합니다.")
            else:
                chart_df, violation_rows = _detect_rules(chart_df, selected_rules)

                base_line = alt.Chart(chart_df).mark_rule(color="#00897B", strokeDash=[6, 4]).encode(
                    x="dt:T", y=alt.datum(float(chart_df["cl"].iloc[0]))
                )
                limit_band = alt.Chart(chart_df).mark_area(opacity=0.08, color="#FFCDD2").encode(
                    x="dt:T", y="cl:Q", y2="ucl:Q"
                )
                line = alt.Chart(chart_df).mark_line(color="#3949AB").encode(
                    x="dt:T", y=alt.Y("metric:Q", title=chart_df["chart_label"].iloc[0], axis=alt.Axis(format=".4f"))
                )
                ucl_line = alt.Chart(chart_df).mark_line(color="#E53935", strokeDash=[4, 3]).encode(
                    x="dt:T", y="ucl:Q"
                )
                pts = alt.Chart(chart_df).mark_circle(size=55).encode(
                    x="dt:T", y="metric:Q",
                    color=alt.condition(
                        alt.datum.violation != "정상",
                        alt.value("#E53935"),
                        alt.value("#43A047"),
                    ),
                    tooltip=[
                        "dt:T", "count:Q", "kg:Q", "metric:Q", "ucl:Q", "violation:N"
                    ],
                )

                st.markdown(
                    f"**{chart_df['chart_label'].iloc[0]} | 규칙:** {', '.join(selected_rules)}"
                )
                st.altair_chart((limit_band + base_line + ucl_line + line + pts).properties(height=320),
                                use_container_width=True)

                if violation_rows:
                    viol_df = pd.DataFrame(violation_rows)
                    st.markdown("**규칙 위반 내역**")
                    st.dataframe(viol_df, use_container_width=True)
                else:
                    st.info("선택한 규칙 위반이 없습니다.")

                n = len(chart_df)
                out_rate = (chart_df["violation"] != "정상").mean()
                z_abs_max = float(np.abs(chart_df["z"].fillna(0)).max())

                var_obs = float(np.var(chart_df["count"] - chart_df["kg"] * chart_df["metric"].fillna(0), ddof=1)) if "kg" in chart_df else 0.0
                var_exp = float(np.mean(chart_df.get("kg", pd.Series([0])) * chart_df["metric"].fillna(0)))
                overdisp = var_obs > 1.5 * var_exp and var_exp > 0

                verdict = []
                if out_rate >= 0.05 or z_abs_max >= 3.5:
                    verdict.append("**관리불량(경보 수준)**: 규칙 위반율이 높거나 극단치가 큽니다.")
                elif out_rate >= 0.02 or z_abs_max >= 3.0:
                    verdict.append("**주의 필요**: 변동성이 커지고 있음.")
                else:
                    verdict.append("**관리양호**: 통계적으로 안정적인 수준.")
                if overdisp:
                    verdict.append("**과산포 의심**: 단순 포아송 가정보다 산포가 큽니다.")

                actions = [
                    "- **자석·체·금속검출기** 점검 주기 단축 및 감도 재검증",
                    "- **LOT별 이물 이력** 사전심사(입고검사 강화), 고위험 LOT 선별 우선",
                    "- **설비 청결/세척 SOP** 강화, 교대/작업자 편차 모니터링",
                    "- **선별량/속도 최적화**로 과부하 구간 제거",
                ]
                st.markdown("**선택 규칙/해석:** " + ", ".join(selected_rules) + " → " + " ".join(verdict))
                st.markdown("**개선 제안:**")
                st.markdown("\n".join([f"  {a}" for a in actions]))

        st.markdown("#### 🔎 최근 2일 치명적 이물 원료 추적 & 교차공장 사용 이력")

        def _crit_key(x):
            s = str(x).strip().lower()
            if any(k in s for k in ["금속", "metal"]):
                return "metal"
            if any(k in s for k in ["유리", "glass"]):
                return "glass"
            return None

        last2_start = TODAY - timedelta(days=1)
        mask_last2_crit = (
            (fdf["dt"] >= last2_start) & (fdf["dt"] <= TODAY) &
            (fdf["count"] > 0) &
            fdf["contam_type"].apply(lambda v: _crit_key(v) is not None)
        )

        cols_needed = [
            "dt", "plant", "line", "lot_no", "contam_type", "count", "selection_amount_kg",
            "material_code", "material_name", "supplier_code", "supplier_name", "material_type",
        ]
        for c in cols_needed:
            if c not in fdf.columns:
                fdf[c] = "" if c not in ["count", "selection_amount_kg"] else 0

        crit_last2_raw = fdf.loc[mask_last2_crit, cols_needed].copy()
        crit_last2_raw["crit_key"] = crit_last2_raw["contam_type"].map(_crit_key)

        if crit_last2_raw.empty:
            st.info("최근 2일 내 치명적 이물(금속/유리) 발생 데이터가 없습니다.")
            return

        grp_cols = ["plant", "line", "dt", "lot_no", "contam_type", "material_code", "supplier_code"]
        crit_last2 = (
            crit_last2_raw
            .groupby(grp_cols, as_index=False)
            .agg(
                발생건수=("count", "sum"),
                selection_amount_kg=("selection_amount_kg", "sum"),
                material_name=("material_name", "first"),
                supplier_name=("supplier_name", "first"),
                material_type=("material_type", "first"),
                crit_key=("crit_key", "first"),
            )
            .sort_values(["dt", "plant", "line"], ascending=[False, True, True])
        )

        st.markdown("##### ① 최근 2일 치명적 이물 발생 목록")
        st.dataframe(
            crit_last2[[
                "plant", "line", "dt", "lot_no", "contam_type", "발생건수", "selection_amount_kg",
                "material_code", "supplier_code", "material_name", "supplier_name", "material_type",
            ]],
            use_container_width=True,
        )

        def _lab(r):
            return (
                f"{r['dt']} | {r['plant']} | {r['line']} | lot_no={r['lot_no']} | "
                f"{r['contam_type']} | {r['material_code']} | {r['supplier_code']}"
            )

        crit_last2["label"] = crit_last2.apply(_lab, axis=1)

        sel_label = st.selectbox(
            "원료 선택 (→ 동일 원료의 타 공장 사용 이력 조회)",
            options=crit_last2["label"].tolist(),
        )
        sel = crit_last2[crit_last2["label"] == sel_label].iloc[0]

        sel_mat = sel["material_code"]
        sel_sup = sel["supplier_code"]
        sel_lot = str(sel["lot_no"]) if pd.notna(sel["lot_no"]) else ""
        sel_plant = sel["plant"]
        sel_line = sel["line"]
        sel_dt = sel["dt"]
        sel_contam = sel["contam_type"]
        sel_crit = sel["crit_key"]
        sel_cnt = int(sel["발생건수"])
        sel_kg = float(sel["selection_amount_kg"])
        sel_mname = sel["material_name"]
        sel_sname = sel["supplier_name"]

        search_start = baseline_start
        base180 = fdf[
            (fdf["dt"] >= search_start) & (fdf["dt"] <= TODAY) &
            (fdf["material_code"] == sel_mat) &
            (fdf["supplier_code"] == sel_sup) &
            (fdf["plant"] != sel_plant)
        ].copy()
        base180["crit_key"] = base180["contam_type"].map(_crit_key)

        if base180.empty:
            st.info("최근 180일 동안 동일 원료(코드+업체)의 타 공장 사용 실적이 없습니다.")
            usage = pd.DataFrame()
        else:
            base180["same_lot"] = False
            if sel_lot.strip():
                base180["same_lot"] = base180["lot_no"].astype(str).eq(sel_lot)

            unique_usage_col = "selection_amount_kg_unique" if "selection_amount_kg_unique" in base180.columns else "selection_amount_kg"

            usage_base = (
                base180.groupby(["plant", "line", "dt", "lot_no"], as_index=False)
                       .agg(usage_kg=(unique_usage_col, "sum"))
            )

            samecrit = base180[base180["crit_key"] == sel_crit]
            samecrit_cnt = (
                samecrit.groupby(["plant", "line", "dt", "lot_no"], as_index=False)
                        .agg(same_critical_count=("count", "sum"))
            )

            same_lot_flag = (
                base180.groupby(["plant", "line", "dt", "lot_no"], as_index=False)
                       .agg(same_lot=("same_lot", "max"))
            )

            usage = (
                usage_base
                .merge(samecrit_cnt, on=["plant", "line", "dt", "lot_no"], how="left")
                .merge(same_lot_flag, on=["plant", "line", "dt", "lot_no"], how="left")
                .fillna({"same_critical_count": 0, "same_lot": False})
                .sort_values(["same_lot", "dt"], ascending=[False, False])
            )

            st.markdown("##### ② 동일 원료(코드+업체)의 타 공장 사용 실적 (최근 180일)")
            show_usage = usage.rename(columns={
                "plant": "사업장", "line": "선별라인", "dt": "선별일자", "same_lot": "same_lot",
            })[["사업장", "선별라인", "선별일자", "lot_no", "usage_kg", "same_critical_count", "same_lot"]]

            show_usage["⚠️"] = np.where(show_usage["same_lot"], "⚠️ 동일 LOT 사용", "")
            st.dataframe(
                show_usage[["⚠️", "사업장", "선별라인", "선별일자", "lot_no", "usage_kg", "same_critical_count"]],
                use_container_width=True,
            )
            st.download_button(
                "② 사용 실적 CSV 다운로드",
                data=show_usage.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"cross_plant_usage_{sel_mat}_{sel_sup}.csv",
            )

        st.markdown("##### ③ 자동 경보 메시지")

        lines_to = []
        lines_to.append("[자동경보] 치명적 이물 발생(금속/유리) – 동일 원료 사용 주의")
        lines_to.append(
            f"- 원료: {sel_mname} (코드 {sel_mat}), 업체: {sel_sname} (코드 {sel_sup}), LOT: {sel_lot or '(미기재)'}"
        )
        lines_to.append(
            f"- 발생: {sel_dt} @ {sel_plant}/{sel_line}, 이물={sel_contam}, 건수={sel_cnt}, 당일 선별량={int(sel_kg)}kg"
        )
        lines_to.append("- 타 공장 사용/발생 요약(최근 180일):")
        if not base180.empty and not usage.empty:
            for _, r in usage.sort_values("dt", ascending=False).head(20).iterrows():
                lot_tag = " ⚠️동일LOT" if r.get("same_lot", False) else ""
                lines_to.append(
                    f"  · {r['plant']} / {r['line']} @ {r['dt']} | lot_no={r['lot_no']} | 사용량={int(r['usage_kg'])}kg | "
                    f"동일이물발생건수={int(r['same_critical_count'])}{lot_tag}"
                )
        else:
            lines_to.append("  · 동일 원료의 타 공장 사용 이력이 없거나 집계 데이터가 없습니다.")

        lines_to.append("- 조치 요청:")
        lines_to.append("  1) 해당 원료(가능 시 동일 LOT) **즉시 사용 중지(Hold)**")
        lines_to.append("  2) 창고/라인 **재고 및 사용 이력 확인**, 동일 LOT 사용 여부 점검")
        lines_to.append("  3) 금속검출/이물선별 **보강 검사** 시행")
        lines_to.append("  4) 결과 회신 및 조치 완료 보고")

        msg_to_plants = "\n".join(lines_to)
        st.text_area("타 공장 경보문", value=msg_to_plants, height=280)
        st.download_button(
            "타 공장 경보문 .txt",
            data=msg_to_plants.encode("utf-8-sig"),
            file_name=f"alert_to_plants_{sel_mat}_{sel_sup}_{sel_lot or 'nolot'}.txt",
        )

        lines_v = []
        lines_v.append("[요청] 치명적 이물(금속/유리) 발생 관련 원인조사 및 CAPA 제출")
        lines_v.append(f"- 원료명/코드: {sel_mname} / {sel_mat}")
        lines_v.append(f"- 업체명/코드: {sel_sname} / {sel_sup}")
        lines_v.append(f"- LOT: {sel_lot or '(미기재)'}")
        lines_v.append(
            f"- 발생정보: {sel_dt} @ {sel_plant}/{sel_line}, 이물={sel_contam}, 건수={sel_cnt}, 당일 선별량={int(sel_kg)}kg"
        )
        lines_v.append("- 요청사항:")
        lines_v.append("  1) 해당 LOT 포함 출하분 **전량 출하정지(Hold)** 및 재고 격리")
        lines_v.append("  2) **원인 분석**(공정/원자재/설비/인력/세척/자석·체 분리장치 점검)")
        lines_v.append("  3) **근본대책(CAPA)** 수립 및 예방조치 계획(기한 포함)")
        lines_v.append("  4) **동일 LOT/동일 설비** 생산분의 추적자료 및 검사성적서(COA) 제출")
        lines_v.append("  5) 회신 기한: 영업일 기준 3일 내 1차 회신, 10일 내 최종 보고")
        msg_to_vendor = "\n".join(lines_v)

        st.text_area("벤더/제조업체 통지문", value=msg_to_vendor, height=260)
        st.download_button(
            "벤더 통지문 .txt",
            data=msg_to_vendor.encode("utf-8-sig"),
            file_name=f"notice_to_vendor_{sel_mat}_{sel_sup}_{sel_lot or 'nolot'}.txt",
        )

    if surge_df is None or surge_df.empty:
        st.info("표시할 조합이 없습니다.")
    else:
        render_alert_details()

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

# -----------------------------
# ⑤ 고위험 리스트 & 지도 시각화
# -----------------------------
with tab5:
    st.subheader("고위험 원료 선정 · 원산지 지표맵")

    base_candidates = st.session_state.get("filtered_df", df).copy()
    if base_candidates.empty:
        st.info("탭① 필터에서 데이터를 만든 뒤 고위험 후보를 선택하세요.")
    else:
        search_kw = st.text_input("검색(원료명/코드/공급사)", key="high_risk_search")
        origin_pool = sorted([o for o in base_candidates["origin"].unique() if str(o).strip()])
        origin_filter = st.multiselect("원산지 필터", origin_pool, key="high_risk_origin")

        candidate = base_candidates.copy()
        if search_kw:
            pattern = search_kw.strip()
            mask = candidate[["material_name", "material_code", "supplier_name"]].apply(
                lambda s: s.str.contains(pattern, case=False, na=False)
            ).any(axis=1)
            candidate = candidate[mask]
        if origin_filter:
            candidate = candidate[candidate["origin"].isin(origin_filter)]

        option_map = {}
        for _, row in (
            candidate[["material_code", "material_name", "supplier_name", "origin"]]
            .dropna(subset=["material_code"])
            .drop_duplicates()
            .iterrows()
        ):
            label = f"{row.material_name} ({row.material_code}) / {row.supplier_name} / {row.origin or '원산지 미기재'}"
            option_map[label] = row.material_code

        st.caption("검색과 원산지 필터를 이용해 후보를 줄인 뒤, 다중 선택으로 고위험 리스트를 정의하세요.")
        selected_labels = st.multiselect("고위험 후보(다중 선택)", list(option_map.keys()), key="high_risk_candidates")
        selected_codes = [option_map[lbl] for lbl in selected_labels]
        selected_df = candidate[candidate["material_code"].isin(selected_codes)]

        col_add, col_replace = st.columns(2)
        if col_add.button("선택 항목을 고위험 리스트에 추가/병합", use_container_width=True):
            merged = pd.concat([st.session_state.get("high_risk_df", pd.DataFrame()), selected_df], ignore_index=True)
            if not merged.empty:
                merged = merged.drop_duplicates(subset=["material_code", "supplier_code", "origin"], keep="first")
            st.session_state["high_risk_df"] = merged
            st.success(f"{len(selected_df)}개 항목을 고위험 리스트에 병합했습니다.")
        if col_replace.button("선택만으로 고위험 리스트 덮어쓰기", use_container_width=True):
            st.session_state["high_risk_df"] = selected_df.copy()
            st.success("선택 항목으로 고위험 리스트를 덮어썼습니다.")

        st.dataframe(selected_df.head(200), use_container_width=True)

        st.markdown("#### 알림/경보 강화 옵션")
        opts_prev = st.session_state.get("high_risk_options", {}) or {}
        opt1 = st.selectbox(
            "경보 강도", ["표준", "강화", "매우 강화"],
            index={"표준": 0, "강화": 1, "매우 강화": 2}.get(opts_prev.get("alert_level", "강화"), 1),
            key="high_risk_alert_level",
        )
        opt2 = st.checkbox("반복 발생 시 재알림", value=opts_prev.get("repeat", True), key="high_risk_repeat")
        opt3 = st.checkbox("임계 초과 시 즉시 경보/푸시", value=opts_prev.get("escalate", True), key="high_risk_escalate")
        st.session_state["high_risk_options"] = {
            "alert_level": opt1,
            "repeat": opt2,
            "escalate": opt3,
        }

        st.markdown("#### 로컬 저장/불러오기 · 상태 복원")
        risk_df = st.session_state.get("high_risk_df", pd.DataFrame())
        with st.expander("고위험 리스트 저장/불러오기"):
            if not risk_df.empty:
                st.download_button(
                    "CSV로 저장", data=risk_df.to_csv(index=False).encode("utf-8-sig"), file_name="high_risk_list.csv"
                )
                st.download_button(
                    "JSON으로 저장", data=risk_df.to_json(orient="records", force_ascii=False, indent=2), file_name="high_risk_list.json"
                )
                if st.button("서버 측 자동복원 파일로 저장", use_container_width=True):
                    ok = persist_high_risk_state(risk_df, st.session_state.get("high_risk_options", {}))
                    if ok:
                        st.success("서버 측에 저장 완료 – 다음 접속 시 자동 복원됩니다.")
                    else:
                        st.error("서버 측에 저장하지 못했습니다. 권한을 확인하세요.")
            loader = st.file_uploader("저장한 고위험 리스트 불러오기(csv/json)", type=["csv", "json"], key="high_risk_loader")
            if loader:
                try:
                    if loader.name.lower().endswith(".json"):
                        loaded_items = json.load(loader)
                        loaded_df = pd.DataFrame(loaded_items)
                    else:
                        loaded_df = pd.read_csv(loader)
                    if not loaded_df.empty:
                        loaded_df = ensure_columns(loaded_df)
                        st.session_state["high_risk_df"] = loaded_df
                        st.success(f"{len(loaded_df)}개 항목을 불러와 고위험 리스트를 복원했습니다.")
                    else:
                        st.warning("불러온 파일에 데이터가 없습니다.")
                except Exception as e:
                    st.error(f"불러오기 실패: {e}")

        st.markdown("#### 원산지별 이물 건/kg 지표 및 지도")
        risk_df = st.session_state.get("high_risk_df", pd.DataFrame())
        if risk_df.empty:
            st.info("고위험 리스트가 비어 있습니다. 후보를 추가해주세요.")
        else:
            origin_metrics = (
                risk_df.groupby("origin", dropna=False)
                .agg(count_sum=("count", "sum"), kg_sum=("selection_amount_kg", "sum"))
                .reset_index()
            )
            origin_metrics["origin"].fillna("(미기재)", inplace=True)
            origin_metrics["rate_per_kg"] = np.where(
                origin_metrics["kg_sum"] > 0,
                origin_metrics["count_sum"] / origin_metrics["kg_sum"],
                0.0,
            )
            st.dataframe(origin_metrics, use_container_width=True)

            max_rate = float(origin_metrics["rate_per_kg"].max()) if not origin_metrics.empty else 0.0
            max_rate = max(max_rate, 1e-6)
            world = alt.topo_feature("https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json", "countries")
            map_chart = (
                alt.Chart(world)
                .mark_geoshape(stroke="#f5f5f5", strokeWidth=0.5)
                .transform_lookup(
                    lookup="properties.name",
                    from_=alt.LookupData(origin_metrics, "origin", ["rate_per_kg", "count_sum", "kg_sum", "origin"]),
                )
                .encode(
                    color=alt.Color(
                        "rate_per_kg:Q",
                        title="건/선별kg",
                        scale=alt.Scale(scheme="orangered", domain=[0, max_rate]),
                    ),
                    tooltip=["origin:N", "count_sum:Q", "kg_sum:Q", alt.Tooltip("rate_per_kg:Q", format=".4f")],
                )
                .project(type="equalEarth")
                .properties(height=380)
            )
            st.altair_chart(map_chart, use_container_width=True)

            geo_ready = origin_metrics.copy()
            geo_ready[["lat", "lon"]] = geo_ready["origin"].apply(
                lambda origin: pd.Series(
                    COUNTRY_CENTROIDS.get(origin, (np.nan, np.nan)), index=["lat", "lon"]
                )
            )
            geo_ready = geo_ready.dropna(subset=["lat", "lon"])
            if not geo_ready.empty:
                geo_ready["rate_scaled"] = geo_ready["rate_per_kg"] * 1e6
                geo_ready["rate_scaled"] = geo_ready["rate_scaled"].clip(upper=1e6)
                column_layer = pdk.Layer(
                    "ColumnLayer",
                    data=geo_ready,
                    get_position="[lon, lat]",
                    get_elevation="rate_scaled",
                    elevation_scale=1,
                    radius=150000,
                    get_fill_color="[255, 87, 34, 180]",
                    pickable=True,
                )
                view_state = pdk.ViewState(latitude=20, longitude=0, zoom=0.8, pitch=20)
                st.pydeck_chart(
                    pdk.Deck(
                        layers=[column_layer],
                        initial_view_state=view_state,
                        tooltip={
                            "text": "{origin}\n건수: {count_sum}\n선별kg: {kg_sum}\n건/선별kg: {rate_per_kg}"}
                    )
                )
            else:
                st.info("지도 좌표와 매칭되는 원산지가 없어 3D 레이어를 표시하지 않습니다.")

            report_lines = [
                "[고위험 리스트 보고서]",
                f"- 경보 강도: {st.session_state['high_risk_options'].get('alert_level', '강화')}",
                f"- 반복 알림: {'ON' if st.session_state['high_risk_options'].get('repeat', True) else 'OFF'}",
                f"- 즉시 경보: {'ON' if st.session_state['high_risk_options'].get('escalate', True) else 'OFF'}",
                f"- 대상 원료 수: {len(risk_df)}",
                f"- 원산지 수: {origin_metrics['origin'].nunique()}",
                "",
                "[원산지별 요약]",
            ]
            for _, r in origin_metrics.iterrows():
                report_lines.append(
                    f"• {r['origin']}: 건수 {int(r['count_sum'])}, 선별kg {r['kg_sum']:.2f}, 건/선별kg {r['rate_per_kg']:.6f}"
                )
            report_text = "\n".join(report_lines)
            st.download_button(
                "고위험 보고서(.txt) 다운로드",
                data=report_text.encode("utf-8-sig"),
                file_name="high_risk_report.txt",
            )

st.caption("※ 고도화: rate 임계치 정책/가중, LOT↔제품 트레이스, 자동 메일/Teams 전송(Graph API) 등 확장 가능.")



