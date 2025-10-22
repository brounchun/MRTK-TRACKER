# app.py
import streamlit as st
import pandas as pd
import numpy as np
from scraper import MyResultScraper
from utils import parse_hhmmss_to_seconds

# ---------------------------------------------------------
# 기본 설정
# ---------------------------------------------------------
st.set_page_config(page_title="MRTK 2025춘천마라톤 Tracker", layout="wide")
st.title("🏃 MRTK 2025춘천마라톤 Tracker")

# ---------------------------------------------------------
# 하드코딩된 테스트 데이터 (레이스/참가자)
# ---------------------------------------------------------
race_id = "132"


runner_details_text = """
1051, 42.195
1342, 42.195
1139, 42.195
1198, 42.195
3073, 42.195
8632, 42.195
2391, 42.195
2051, 42.195
2598, 42.195
2004, 42.1954
110, 42.195
7026, 42.195
6135, 42.195
10313, 42.195
7081, 42.195
4211, 42.195
13114, 42.195
13215, 42.195
7342, 42.195
7196, 42.195
4109, 42.195
37256, 10"
"""

# ---------------------------------------------------------
# 헬퍼 함수
# ---------------------------------------------------------
def seconds_to_hhmmss(seconds: float) -> str:
    if pd.isna(seconds) or seconds <= 0:
        return "-"
    s = int(seconds)
    h = s // 3600
    s %= 3600
    m = s // 60
    s %= 60
    return f"{h:02d}:{m:02d}:{s:02d}"
def format_km(km):
    # 주요 거리 값은 정밀 표시
    if abs(km - 42.195) < 0.001:
        return "42.195"
    if abs(km - 21.0975) < 0.001:
        return "21.0975"
    if km.is_integer():
        return f"{int(km)}"
    return f"{km:.2f}"

def parse_distance_input(text: str) -> float:
    text = str(text).strip().lower()
    if 'full' in text or '풀' in text or '42.195' in text:
        return 42.195
    if 'half' in text or '하프' in text or '21.0975' in text or '21.1' in text:
        return 21.0975
    if '10' in text and not text.startswith(('21', '42')):
        return 10.0
    try:
        return float(text)
    except ValueError:
        return 0.0

# ---------------------------------------------------------
# 데이터 가져오기
# ---------------------------------------------------------
@st.cache_data(show_spinner=False)
def fetch_many(race_id_int: int, ids: list[int]):
    try:
        scraper = MyResultScraper()
        return scraper.get_many(race_id_int, ids, limit=5)
    except NameError:
        st.warning("⚠️ 'MyResultScraper'를 찾을 수 없습니다. (scraper.py 확인 필요)")
        return [{"error": "Missing Scraper"}] * len(ids)
    except Exception as e:
        st.error(f"데이터 크롤링 중 오류 발생: {e}")
        return [{"error": f"Error: {e}"}] * len(ids)

def normalize_to_rows(one: dict) -> list[dict]:
    rows = []
    for sec in one.get("sections", []):
        rows.append({
            "runner_id": one.get("runner_id"),
            "name": one.get("name", ""),
            "gender": one.get("gender", ""),
            "bib_no": one.get("bib_no", ""),
            "section": sec.get("section", ""),
            "pass_time": sec.get("pass_time", ""),
            "split_time": sec.get("split_time", ""),
            "total_time": sec.get("total_time", "")
        })
    return rows

# ---------------------------------------------------------
# 입력 파싱 및 데이터 로드
# ---------------------------------------------------------
try:
    race_id_int = int(race_id.strip())
    runner_inputs = {
        int(p[0]): parse_distance_input(p[1])
        for line in runner_details_text.strip().split('\n')
        if (p := [x.strip() for x in line.split(',')]) and len(p) >= 2 and p[0].isdigit()
    }
    runner_ids = list(runner_inputs.keys())
except Exception:
    st.error("❌ 입력 형식 오류 (예: 100, 42.195)")
    st.stop()

with st.spinner("데이터 수집 중..."):
    data_list = fetch_many(race_id_int, runner_ids)

oks = [d for d in data_list if not d.get("error")]
all_rows = [r for d in oks for r in normalize_to_rows(d)]
if not all_rows:
    st.error("데이터를 가져오지 못했습니다. 참가자 ID를 확인해주세요.")
    st.stop()

df = pd.DataFrame(all_rows)

# ---------------------------------------------------------
# pace 계산용 데이터 정리
# ---------------------------------------------------------
def parse_hhmmss_to_seconds(hhmmss_str):
    if not hhmmss_str or pd.isna(hhmmss_str):
        return np.nan
    parts = str(hhmmss_str).split(':')
    if len(parts) == 3:
        return int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
    elif len(parts) == 2:
        return int(parts[0])*60 + int(parts[1])
    return np.nan

df["total_seconds"] = df["total_time"].fillna('').astype(str).apply(parse_hhmmss_to_seconds).astype(float)
df["split_seconds"] = df["split_time"].fillna('').astype(str).apply(parse_hhmmss_to_seconds).astype(float)

# ---------------------------------------------------------
# 참가자 요약 데이터 구성
# ---------------------------------------------------------
runner_properties = []
for rid, sub in df.groupby("runner_id"):
    if sub.empty:
        continue
    user_target_km = runner_inputs.get(rid, 0.0)
    total_course_km = user_target_km or 42.195
    finish_records = sub[sub["section"].str.contains("도착|Finish", case=False, na=False)]
    is_finished = not finish_records.empty
    finish_time_sec = finish_records["total_seconds"].max() if is_finished else np.inf
    if is_finished:
        max_known_distance = total_course_km
    else:
        # 반만 통과했을 경우 대략 절반 거리로 추정 (혹은 실제 기록 기반으로 조정 가능)
        known_sections = sub[sub["total_time"].notna()]
        if not known_sections.empty:
            # 섹션 이름에 숫자(km)가 있으면 가장 큰 숫자로 계산
            max_known_distance = max([
                float(s.replace('K', '').replace('k', '')) 
                for s in known_sections["section"] if any(ch.isdigit() for ch in s)
            ] + [0])
        else:
            max_known_distance = 0.0
    runner_properties.append({
        'runner_id': rid,
        'total_course_km': total_course_km,
        'is_finished': is_finished,
        'finish_time_seconds': finish_time_sec,
        'max_known_distance': max_known_distance
    })
props_df = pd.DataFrame(runner_properties)
df = df.merge(props_df, on='runner_id', how='left')

# ---------------------------------------------------------
# 코스별 트랙 시각화 함수
# ---------------------------------------------------------
def render_course_track(course_name: str, total_distance: float, runners_data: pd.DataFrame):
    """세로 트랙 형태로 각 러너의 위치를 시각화 — 완주자 겹침 자동 분산"""
    track_height = 450
    if total_distance > 30:
        track_height = 850
    elif total_distance > 15:
        track_height = 650

    css = f"""
    <style>
    .track-container {{
        position: relative;
        margin: 25px auto;
        padding: 20px 0;
        width: 280px;
        min-height: {track_height}px;
    }}
    .track-line {{
        position: absolute;
        left: 50%;
        top: 0;
        bottom: 0;
        width: 4px;
        background-color: #e0e0e0;
        transform: translateX(-50%);
    }}
    .checkpoint {{
        position: absolute;
        left: 50%;
        width: 14px;
        height: 14px;
        border-radius: 50%;
        background: #333;
        transform: translate(-50%, -50%);
        z-index: 10;
    }}
    .checkpoint-label {{
        position: absolute;
        left: calc(50% + 20px);
        transform: translateY(-50%);
        background: #f9f9f9;
        border: 1px solid #ccc;
        padding: 2px 6px;
        border-radius: 4px;
        font-size: 12px;
        white-space: nowrap;
    }}
    .runner-marker {{
        position: absolute;
        transform: translateY(-50%);
        padding: 4px 8px;
        border-radius: 6px;
        font-size: 0.8em;
        font-weight: bold;
        white-space: nowrap;
        color: white;
        box-shadow: 0 2px 5px rgba(0,0,0,0.3);
        z-index: 20;
        cursor: default;
    }}
    .runner-finished {{ background: #00A389; }}
    .runner-progress {{ background: #FF5733; }}
    .finish-dot {{ background: #DC3545 !important; }}
    </style>
    """
    html = css + "<div class='track-container'><div class='track-line'></div>"

    checkpoints = [0, 5, 10, 20, 30, 40, 42.195] if total_distance > 30 else [0, 5, 10, 15, 21.0975]
    for km in checkpoints:
        if km > total_distance:
            continue
        top_percent = (km / total_distance) * 100
        label = "START" if km == 0 else ("FINISH" if abs(km - total_distance) < 0.1 else f"{km:.1f}km")
        cls = "checkpoint finish-dot" if label == "FINISH" else "checkpoint"
        html += f"<div class='{cls}' style='top:{top_percent}%;'></div>"
        html += f"<div class='checkpoint-label' style='top:{top_percent}%;'>{label}</div>"

    runners_data = runners_data.copy()
    runners_data['progress_ratio'] = runners_data.apply(
        lambda r: 1.0 if r['is_finished'] else (r['max_known_distance'] / total_distance) if total_distance > 0 else 0.0,
        axis=1
    )
    runners_data['group_key'] = runners_data['progress_ratio'].round(4)

    def assign_offsets(group):
        group = group.sort_values(by='finish_time_seconds', ascending=True)
        offsets = []
        for i in range(len(group)):
            if i % 2 == 0:
                offsets.append((i // 2) * 60)
            else:
                offsets.append(-((i // 2 + 1) * 60))
        group['offset_px'] = offsets
        return group

    runners_data = runners_data.groupby('group_key', group_keys=False).apply(assign_offsets,include_groups=False)

    for _, r in runners_data.iterrows():
        ratio = min(max(r['progress_ratio'], 0), 1)
        top_percent = ratio * 100
        offset = r['offset_px']
        color_class = "runner-finished" if r["is_finished"] else "runner-progress"
        label = f"{r['name']}"
        html += f"<div class='runner-marker {color_class}' style='top:{top_percent}%; left: calc(50% + {offset}px);'>{label}</div>"

    html += "</div>"
    st.html(html)

# ---------------------------------------------------------
# UI 구성
# ---------------------------------------------------------
tab_individual, tab_overall = st.tabs(["개별 참가자 기록 카드", "전체 코스별 예상 위치"])

# ============ 카드형 상세보기 ============
with tab_individual:
    st.subheader("개별 참가자 기록 카드 (클릭하여 상세 기록 확인)")

    st.markdown("""
    <style>
    div[data-testid="stButton"] > button {
        text-align: left !important;
        display: block !important;
        width: 100% !important;
        padding: 14px 18px !important;
        border-radius: 10px !important;
        border: 1px solid #e0e0e0 !important;
        background: #ffffff !important;
        box-shadow: 0 2px 6px rgba(0,0,0,0.05) !important;
        transition: all .2s ease-in-out;
        line-height: 1.28;
        white-space: pre-line;
    }
    div[data-testid="stButton"] > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.12) !important;
        border-color: #00A389 !important;
        background: #f9fdfb !important;
    }
    div[data-active-card="true"] > div[data-testid="stButton"] > button {
        border-color: #00A389 !important;
        box-shadow: 0 4px 14px rgba(0,163,137,0.18) !important;
        background: #f7fffc !important;
    }
    </style>
    """, unsafe_allow_html=True)

    if "active_card" not in st.session_state:
        st.session_state.active_card = None

    def toggle_card(runner_id):
        st.session_state.active_card = None if st.session_state.active_card == runner_id else runner_id

    for rid, sub in df.groupby("runner_id"):
        name, gender, bib = sub[["name", "gender", "bib_no"]].iloc[0]
        total_course_km, max_known_distance, is_finished = sub[["total_course_km", "max_known_distance", "is_finished"]].iloc[0]
        total_sec = sub["total_seconds"].dropna().max()

        pace_str = "-"
        if total_sec > 0 and max_known_distance > 0:
            pace_min = (total_sec / max_known_distance) / 60
            pace_str = f"{int(pace_min):02d}:{int((pace_min % 1)*60):02d} 분/km"

        is_open = st.session_state.active_card == rid
        icon = "▼" if is_open else "▶"
        label_line1 = f"{icon} {name} ({gender}) #{bib}"
        label_line2 = f"풀 마라톤 ({total_course_km:.3f}km) | 페이스: {pace_str}"
        button_label = f"{label_line1}\n{label_line2}"

        with st.container():
            st.markdown(f"<div data-active-card={'true' if is_open else 'false'}></div>", unsafe_allow_html=True)
            if st.button(button_label, key=f"card_btn_{rid}", on_click=toggle_card, args=(rid,), use_container_width=True):
                pass

            if is_open:
                if is_finished:
                    st.success(f"✅ 최종 기록: {seconds_to_hhmmss(total_sec)}")
                else:
                    st.info(f"⏳ 진행 중 - 거리: {max_known_distance:.1f} km")
                st.progress(max_known_distance / total_course_km, text=f"{format_km(max_known_distance)} / {format_km(total_course_km)} km")
                st.dataframe(sub[["section", "pass_time", "split_time", "total_time"]], use_container_width=True, hide_index=True)

# ============ 전체 트랙 시각화 ============
with tab_overall:
    st.header("📍 전체 코스별 실시간 진행 상황 트랙")

    valid_runners_df = df[df["total_course_km"] > 0.1]
    for course_name, course_group in valid_runners_df.groupby("total_course_km"):
        st.markdown(f"### 🏁 코스 거리: {format_km(course_name)} km")
        unique_runners = course_group.drop_duplicates(subset=['runner_id']).reset_index(drop=True)
        if not unique_runners.empty:
            render_course_track(str(course_name), unique_runners["total_course_km"].iloc[0], unique_runners)
        else:
            st.info("이 코스에는 표시할 참가자가 없습니다.")
