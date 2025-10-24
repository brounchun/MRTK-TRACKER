import os
import streamlit as st
import pandas as pd
import numpy as np
import subprocess
import json
import sys
from google.cloud import storage
from utils import parse_hhmmss_to_seconds
import time
import re # 정규 표현식 모듈 추가

# ---------------------------------------------------------
# GCS 인증 자동 설정 (로컬 + 배포 환경 공통)
# ---------------------------------------------------------
BUCKET_NAME = "mrtk-tracker-data-2025"
FILE_NAME = "runner_list.txt"
LOCAL_GCS_KEY_PATH = os.path.join(os.path.dirname(__file__), "gcs_key.json")


if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    if os.path.exists(LOCAL_GCS_KEY_PATH):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = LOCAL_GCS_KEY_PATH
        print(f"[INFO] GOOGLE_APPLICATION_CREDENTIALS 설정됨 → {LOCAL_GCS_KEY_PATH}")
    else:
        print("[WARN] GCS 키 파일(gcs_key.json)을 찾을 수 없습니다. 환경 변수로 수동 설정 필요.")
else:
    print(f"[INFO] 기존 환경 변수 GOOGLE_APPLICATION_CREDENTIALS 사용 중: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")

# ---------------------------------------------------------
# 기본 설정
# ---------------------------------------------------------
st.set_page_config(page_title="MRTK 2025춘천마라톤 Tracker", layout="wide")
st.title("🏃 MRTK 2025춘천마라톤 Tracker")

race_id = "132"

# ---------------------------------------------------------
# GCS에서 runner_list.txt 읽기
# ---------------------------------------------------------
def load_runner_text_from_gcs(force_refresh: bool = False):
    """
    GCS에서 runner_list.txt를 읽어오되, 
    Streamlit rerun 시에는 캐시 유지하고,
    '수동 새로고침' 버튼을 눌렀을 때만 다시 다운로드함.
    """
    BUCKET_NAME = "mrtk-tracker-data-2025"
    FILE_NAME = "runner_list.txt"
    print(f"[DEBUG] GCS Client created by: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'default_service_account')}")
    # 세션 캐시 초기화
    if "gcs_cache" not in st.session_state:
        st.session_state.gcs_cache = {}

    cache_key = f"{BUCKET_NAME}/{FILE_NAME}"

    # 👉 캐시가 있고, 강제 새로고침이 아니면 기존 데이터 사용
    if not force_refresh and cache_key in st.session_state.gcs_cache:
        print("[INFO] GCS 캐시 사용 중 (수동 새로고침 시 갱신)")
        return st.session_state.gcs_cache[cache_key]

    # 🚀 강제 새로고침 or 최초 실행 시 GCS 다운로드
    print(f"load_runner_text_from_gcs 접속 중...")
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(FILE_NAME)
        text = blob.download_as_text(encoding="utf-8")
        st.session_state.gcs_cache[cache_key] = text
        print("[INFO] GCS 파일 다운로드 성공 ✅")
        return text
    except Exception as e:
        st.error(f"GCS에서 runner_list.txt 읽기 실패: {e}")
        print(f"[ERROR] GCS 접근 실패: {e}")
        return ""

st.sidebar.markdown("### 📦 데이터 관리")
if st.sidebar.button("🔄 GCS 데이터 새로고침"):
    runner_details_text = load_runner_text_from_gcs(force_refresh=True)
    st.success("✅ GCS 데이터가 새로 다운로드되었습니다.")
else:
    runner_details_text = load_runner_text_from_gcs(force_refresh=False)
st.markdown("""
    <style>
        [data-testid="stSidebar"] {
            display: none;
        }
    </style>
""", unsafe_allow_html=True)
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
    if abs(km - 42.195) < 0.001:
        return "42.195"
    if abs(km - 21.0975) < 0.001:
        return "21.0975"
    if isinstance(km, (int, float)) and float(km).is_integer():
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
# ✅ subprocess 기반 데이터 크롤링 실행
# ---------------------------------------------------------
#@st.cache_data(show_spinner=False)
def fetch_many(race_id_int: int, ids: list[int]):
    """Playwright 실행을 Streamlit 외부 프로세스로 분리 (세션 단위 캐싱용)"""
    start_time3 = time.time()
    try:
        cmd = [
            sys.executable,
            "scraper_runner.py",
            str(race_id_int),
            ",".join(map(str, ids))
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        json_output = []
        # 실시간 stderr 로깅 (필요하면 st.empty() 사용 가능)
        while True:
            stderr_line = process.stderr.readline()
            stdout_line = process.stdout.readline()

            if stderr_line:
                print(stderr_line.strip(), flush=True)
            if stdout_line:
                json_output.append(stdout_line.strip())

            if process.poll() is not None:
                break

        process.wait()
        raw_output = "".join(json_output).strip()
        end_time3 = time.time()
        elapsed3 = end_time3 - start_time3

        if not raw_output:
            st.warning("⚠️ scraper_runner.py에서 JSON 데이터가 반환되지 않았습니다.")
            return []

        try:
            data = json.loads(raw_output)
        except json.JSONDecodeError as e:
            st.error(f"⚠️ JSON 파싱 실패: {e}")
            st.text(raw_output)
            return []

        if isinstance(data, dict) and "error" in data:
            st.error(f"❌ 스크래퍼 실행 오류: {data['error']}")
            if "trace" in data:
                st.text(data["trace"])
            return []

        st.success(f"✅ 데이터 갱신 완료! (소요 {elapsed3:.2f}초)")
        return data

    except Exception as e:
        st.error(f"크롤링 실행 실패: {e}")
        print(f"[ERROR] {e}", file=sys.stderr, flush=True)
        return []


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

if "data_list" not in st.session_state:
    with st.spinner("데이터 수집 중..."):
        st.session_state.data_list = fetch_many(race_id_int, runner_ids)
else:
    st.info("💾 세션 캐시 데이터 사용 중")

data_list = st.session_state.data_list

oks = [d for d in data_list if not d.get("error")]
all_rows = [r for d in oks for r in normalize_to_rows(d)]
if not all_rows:
    st.error("데이터를 가져오지 못했습니다. 참가자 ID를 확인해주세요.")
    st.stop()

df = pd.DataFrame(all_rows)

# ---------------------------------------------------------
# pace 계산용 데이터 정리
# ---------------------------------------------------------
# utils.py의 parse_hhmmss_to_seconds는 파싱 실패 시 None을 반환합니다.
df["total_seconds"] = df["total_time"].fillna('').astype(str).apply(parse_hhmmss_to_seconds).fillna(0.0).astype(float)
df["split_seconds"] = df["split_time"].fillna('').astype(str).apply(parse_hhmmss_to_seconds).fillna(0.0).astype(float)

# ---------------------------------------------------------
# ⭐ 참가자 요약 데이터 구성 (max_known_distance 및 페이스 계산 로직 수정)
# ---------------------------------------------------------
runner_properties = []
for rid, sub in df.groupby("runner_id"):
    if sub.empty:
        continue
    user_target_km = runner_inputs.get(rid, 0.0)
    total_course_km = user_target_km or 42.195
    
    # ⭐ 1. is_finished 로직 수정: '도착|Finish' 섹션이 존재하고, total_seconds 값이 유효해야 완주로 간주
    # total_seconds > 0 조건 추가
    finish_records = sub[sub["section"].str.contains("도착|Finish", case=False, na=False) & (sub["total_seconds"] > 0)]
    is_finished = not finish_records.empty
    
    # 🏃‍♂️ 최종 기록
    finish_time_sec = finish_records["total_seconds"].max() if is_finished else np.inf
    
    # ⭐ 2. max_known_distance 계산 (진행률 표시용)
    if is_finished:
        max_known_distance = total_course_km
    else:
        # 미완주자는 유효한 숫자가 포함된 섹션 중 최대 거리만 사용
        # ⭐ total_seconds가 0보다 큰 섹션만 유효한 것으로 간주 (기록이 ?나 -인 경우 제외)
        known_sections = sub[sub["total_seconds"] > 0]
        
        valid_distances = []
        for s in known_sections["section"]:
            s_clean = s.replace('K', '').replace('k', '').strip()
            # '도착', 'Start' 등의 텍스트는 float 변환 시 오류 발생 -> 숫자만 통과
            try:
                distance = float(s_clean)
                valid_distances.append(distance)
            except ValueError:
                pass 
        
        # ⚠️ Start 지점 기록이 유효해도 0km로 간주해야 하므로, max_known_distance는 0이 최소값임
        max_known_distance = max(valid_distances) if valid_distances else 0.0

    # ⭐ 3. 페이스 계산 기준 설정
    pace_calc_distance = total_course_km if is_finished else max_known_distance
    # 미완주자는 현재까지 통과한 가장 늦은 시간, 완주자는 최종 시간
    pace_calc_seconds = finish_time_sec if is_finished else sub["total_seconds"].max() if max_known_distance > 0 else 0.0
    
    runner_properties.append({
        'runner_id': rid,
        'name': sub["name"].iloc[0], # 이름 추가
        'gender': sub["gender"].iloc[0], # 성별 추가
        'bib_no': sub["bib_no"].iloc[0], # 등번호 추가
        'total_course_km': total_course_km,
        'is_finished': is_finished,
        'finish_time_seconds': finish_time_sec,
        'max_known_distance': max_known_distance,
        'pace_calc_distance': pace_calc_distance, # 페이스 계산 기준 거리
        'pace_calc_seconds': pace_calc_seconds    # 페이스 계산 기준 시간
    })

props_df = pd.DataFrame(runner_properties)
df = df.merge(props_df[['runner_id', 'total_course_km', 'is_finished', 'finish_time_seconds', 
                        'max_known_distance', 'pace_calc_distance', 'pace_calc_seconds']], on='runner_id', how='left')

# ---------------------------------------------------------
# 그룹화된 데이터 준비 (KeyError 방지 및 효율 개선)
# ---------------------------------------------------------
runner_groups = {rid: group for rid, group in df.groupby("runner_id")}


# ---------------------------------------------------------
# 코스별 트랙 시각화 함수 (동일)
# ---------------------------------------------------------
def render_course_track(course_name: str, total_distance: float, runners_data: pd.DataFrame):
    track_height = 450
    if total_distance > 30:
        track_height = 850
    elif total_distance > 15:
        track_height = 650

    css = f"""
    <style>
    /* CSS 스타일은 이전과 동일 */
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
        line-height: 1.2; 
    }}
    .runner-finished {{ background: #00A389; }}
    .runner-progress {{ background: #FF5733; }}
    .finish-dot {{ background: #DC3545 !important; }}
    </style>
    """
    html = css + "<div class='track-container'><div class='track-line'></div>"

    checkpoints = [0, 5, 10, 20, 30, 40, 42.195] if total_distance > 30 else [0, 5, 10, 15, 21.0975]
    if total_distance <= 10.0:
        checkpoints = [0, 5, 10]
        
    checkpoints = sorted(list(set([c for c in checkpoints if c <= total_distance])))
    if total_distance > 0 and total_distance not in checkpoints:
        checkpoints.append(total_distance)
    checkpoints = sorted(list(set(checkpoints)))


    for km in checkpoints:
        if km > total_distance * 1.001:
            continue
        top_percent = (km / total_distance) * 100 if total_distance > 0 else 0
        label = "START" if km == 0 else ("FINISH" if abs(km - total_distance) < 0.1 else f"{format_km(km)}km")
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
            offsets.append((i // 2) * 60 if i % 2 == 0 else -((i // 2 + 1) * 60))
        group['offset_px'] = offsets
        return group

    runners_data = runners_data.groupby('group_key', group_keys=False).apply(assign_offsets, include_groups=False)

    for _, r in runners_data.iterrows():
        ratio = min(max(r['progress_ratio'], 0), 1)
        top_percent = ratio * 100
        offset = r['offset_px']
        
        # ⭐ 이모지로 완주자와 진행 중인 주자 구분
        if r["is_finished"]:
            color_class = "runner-finished"
            marker_emoji = "✅"
        else:
            color_class = "runner-progress"
            marker_emoji = "🏃"

        label = f"{marker_emoji} {r['name']}" # 이름 앞에 이모지 추가
        
        html += f"<div class='runner-marker {color_class}' style='top:{top_percent}%; left: calc(50% + {offset}px);'>{label}</div>"

    html += "</div>"
    st.html(html)

# ---------------------------------------------------------
# 개별 카드 렌더링 함수 (재사용)
# ---------------------------------------------------------
# ⭐ runner_groups를 사용하여 KeyError 방지
def render_runner_card(rid, sub_df: pd.DataFrame, is_open):
    # sub_df는 이미 한 주자의 모든 기록을 포함
    name, gender, bib = sub_df[["name", "gender", "bib_no"]].iloc[0]
    total_course_km, max_known_distance, is_finished, pace_calc_distance, pace_calc_seconds = sub_df[[
        "total_course_km", "max_known_distance", "is_finished", 
        "pace_calc_distance", "pace_calc_seconds"
    ]].iloc[0]
    
    # UI 표시용 최종 기록
    total_sec_display = pace_calc_seconds if is_finished else sub_df["total_seconds"].dropna().max() 

    pace_str = "-"
    # ⭐ 페이스 계산에 새로운 필드 사용
    if pace_calc_seconds > 0 and pace_calc_distance > 0:
        pace_min = (pace_calc_seconds / pace_calc_distance) / 60
        pace_str = f"{int(pace_min):02d}:{int((pace_min % 1)*60):02d} 분/km"
    
    
    # ⭐ 모바일 최적화: 아이콘 및 텍스트 구성
    toggle_icon = "▼" if is_open else "▶"
    status_emoji = "✅" if is_finished else "🏃"
    gender_emoji = "♂️" if gender == "남자" else "♀️"
    
    # Line 1: 토글, 상태, 이름, 등번호, 성별
    line1 = f"{toggle_icon} {status_emoji} **{name}** (#{bib}) {gender_emoji}"
    # Line 2: 현재 위치/총 거리, 현재 페이스
    line2 = f"📍 {format_km(max_known_distance)} / {format_km(total_course_km)} km | ⏱️ {pace_str}" 

    # 최종 버튼 라벨 (두 줄로 압축)
    compact_label = f"{line1}\n{line2}"

    def toggle_card(runner_id):
        # 함수 내부에서 session_state에 접근
        if 'active_card' not in st.session_state:
            st.session_state.active_card = None
        st.session_state.active_card = None if st.session_state.active_card == runner_id else runner_id
    
    with st.container(border=False):
        # 활성화된 카드 테두리 및 그림자 효과를 위한 컨테이너
        st.markdown(
            f"<div class='runner-card-container' data-active-card={'true' if is_open else 'false'}>", 
            unsafe_allow_html=True
        )
        
        # 컴팩트 라벨을 가진 버튼 (토글 기능)
        if st.button(compact_label, key=f"card_btn_{rid}", on_click=toggle_card, args=(rid,), use_container_width=True):
            pass

        # 상세 정보 (카드가 열렸을 때) - 기능은 유지
        if is_open:
            st.markdown("<div style='padding: 10px 15px 15px;'>", unsafe_allow_html=True) # 상세 내용에 패딩 추가
            if is_finished:
                # ⭐ 최종 기록 표시
                st.success(f"✅ 최종 기록: {seconds_to_hhmmss(total_sec_display)}")
            else:
                st.info(f"⏳ 진행 중 - 거리: {max_known_distance:.1f} km")

            # ⭐ max_known_distance가 total_course_km보다 클 수 없도록 보호
            progress_value = min(max_known_distance / total_course_km, 1.0) 
            
            st.progress(
                progress_value,
                text=f"{format_km(max_known_distance)} / {format_km(total_course_km)} km"
            )

            st.dataframe(
                sub_df[["section", "pass_time", "split_time", "total_time"]],
                use_container_width=True,
                hide_index=True
            )
            st.markdown("</div>", unsafe_allow_html=True)
        
        # 카드 컨테이너 닫기
        st.markdown("</div>", unsafe_allow_html=True)

# ---------------------------------------------------------
# UI 구성
# ---------------------------------------------------------
# ⭐ 탭 구조 변경: 진행 중 명단, 완주자 명단, 전체 트랙으로 분리
tab_progress, tab_finished, tab_overall = st.tabs(["🏃 진행 중 명단", "✅ 완주자 명단", "📍 전체 코스별 예상 위치"])


# =================== CSS 스타일 (유지) ===================
st.markdown("""
<style>
/* ... 이전 CSS 스타일 유지 ... */
.runner-card-container {
    border: 1px solid #e0e0e0;
    border-radius: 10px;
    margin-bottom: 10px;
    transition: box-shadow 0.2s ease-in-out;
}
.runner-card-container[data-active-card="true"] {
    box-shadow: 0 4px 14px rgba(0,163,137,0.18) !important;
    border-color: #00A389 !important;
}
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
.stButton > button > div > p {
    font-size: 1.0em;
    margin: 0 !important;
    padding: 0 !important;
}
.stButton > button > div > p > strong {
    font-size: 1.1em;
}
</style>
""", unsafe_allow_html=True)


if "active_card" not in st.session_state:
    st.session_state.active_card = None

# =================== 진행 중 명단 탭 ===================
with tab_progress:
    st.subheader("🏃 현재 레이스 진행 중인 참가자 명단")
    
    # props_df를 사용하여 완주하지 않은 주자 ID 목록을 가져옵니다.
    progress_runners_ids = props_df.query('is_finished == False')['runner_id'].tolist()

    if not progress_runners_ids:
        st.info("현재 진행 중인 참가자가 없습니다.")
    else:
        # 진행 중인 주자는 이름/등번호 순으로 정렬하는 것이 일반적이므로, props_df를 사용하여 정렬
        progress_runners_sorted = props_df.query('is_finished == False').sort_values(['name', 'bib_no'])
        
        for rid in progress_runners_sorted['runner_id']:
            # ⭐ 그룹핑된 딕셔너리에서 데이터프레임을 가져옵니다.
            sub = runner_groups.get(rid)
            if sub is not None:
                is_open = st.session_state.active_card == rid
                render_runner_card(rid, sub, is_open)

# =================== 완주자 명단 탭 ===================
with tab_finished:
    st.subheader("✅ 레이스를 완주한 참가자 명단 (최종 기록 기준)")

    # props_df를 사용하여 완주한 주자들을 최종 기록 순으로 정렬
    finished_runners_sorted = props_df.query('is_finished == True').sort_values('finish_time_seconds')

    if finished_runners_sorted.empty:
        st.info("아직 완주한 참가자가 없습니다.")
    else:
        for rid in finished_runners_sorted['runner_id']:
            # ⭐ 그룹핑된 딕셔너리에서 데이터프레임을 가져옵니다.
            sub = runner_groups.get(rid)
            if sub is not None:
                is_open = st.session_state.active_card == rid
                render_runner_card(rid, sub, is_open)

# =================== 전체 트랙 탭 ===================
with tab_overall:
    st.header("📍 전체 코스별 실시간 진행 상황 트랙")

    valid_runners_df = df[df["total_course_km"] > 0.1]
    for course_name, course_group in valid_runners_df.groupby("total_course_km"):
        st.markdown(f"### 🏁 코스 거리: {format_km(course_name)} km")
        # 트랙 렌더링에 필요한 정보는 중복을 제거한 데이터만 사용 (KeyError와 무관)
        unique_runners = course_group.drop_duplicates(subset=['runner_id']).reset_index(drop=True)
        if not unique_runners.empty:
            render_course_track(str(course_name), unique_runners["total_course_km"].iloc[0], unique_runners)
        else:
            st.info("이 코스에는 표시할 참가자가 없습니다.")