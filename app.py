import os
import streamlit as st
import pandas as pd
import numpy as np
import subprocess
import json
import sys
from google.cloud import storage
from utils import parse_hhmmss_to_seconds # utils.py에서 import
import time

# ---------------------------------------------------------
# GCS 인증 자동 설정 (기존과 동일)
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
# GCS에서 runner_list.txt 읽기 (D 전략 적용 - st.cache_data)
# ---------------------------------------------------------
@st.cache_data(show_spinner=False) # ✨ D 전략: Streamlit 캐시 적용
def load_runner_text_from_gcs(force_refresh: bool = False):
    """
    GCS에서 runner_list.txt를 읽어오되, 
    Streamlit rerun 시에는 캐시 유지하고,
    '수동 새로고침' 버튼을 눌렀을 때(force_refresh=True)만 캐시를 무효화하고 다시 다운로드함.
    """
    # force_refresh 인자는 캐시의 키가 되어 캐시 무효화에 사용됨
    
    # 🚀 GCS 다운로드
    print(f"load_runner_text_from_gcs 접속 중...")
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(FILE_NAME)
        text = blob.download_as_text(encoding="utf-8")
        print("[INFO] GCS 파일 다운로드 성공 ✅")
        return text
    except Exception as e:
        st.error(f"GCS에서 runner_list.txt 읽기 실패: {e}")
        print(f"[ERROR] GCS 접근 실패: {e}")
        return ""

st.sidebar.markdown("### 📦 데이터 관리")
# force_refresh=True로 호출하면 cache_data는 다른 키로 인식하여 재실행됨
if st.sidebar.button("🔄 GCS 데이터 새로고침"):
    load_runner_text_from_gcs.clear() # 캐시를 명시적으로 비웁니다.
    runner_details_text = load_runner_text_from_gcs(force_refresh=True)
    st.success("✅ GCS 데이터가 새로 다운로드되었습니다.")
else:
    # force_refresh=False로 호출하면 동일한 캐시 키를 사용하여 캐시된 값을 반환합니다.
    runner_details_text = load_runner_text_from_gcs(force_refresh=False)

# UI 숨김 로직은 기존과 동일
st.markdown("""
    <style>
        [data-testid="stSidebar"] {
            display: none;
        }
    </style>
""", unsafe_allow_html=True)
# ---------------------------------------------------------
# 헬퍼 함수 (기존과 동일)
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
    if isinstance(km, (int, float)) and km.is_integer():
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
# ✅ subprocess 기반 데이터 크롤링 실행 (C 전략 적용 - st.cache_data)
# ---------------------------------------------------------
@st.cache_data(show_spinner=False) # ✨ C 전략: Streamlit 캐시 적용
def fetch_many(race_id_int: int, ids: list[int]):
    """Playwright 실행을 Streamlit 외부 프로세스로 분리 (캐싱용)"""
    start_time3 = time.time()
    
    # 캐싱 일관성을 위해 ids를 정렬하여 사용
    sorted_ids = sorted(ids) 
    
    try:
        cmd = [
            sys.executable,
            "scraper_runner.py",
            str(race_id_int),
            ",".join(map(str, sorted_ids)) # 정렬된 ID 사용
        ]

        # 이하 subprocess 로직은 기존과 동일
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        json_output = []
        # 실시간 stderr 로깅
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
    # 기존과 동일
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
    # runner_list.txt (소스 1) 로드
    runner_inputs = {
        int(p[0]): parse_distance_input(p[1])
        for line in runner_details_text.strip().split('\n')
        if (p := [x.strip() for x in line.split(',')]) and len(p) >= 2 and p[0].isdigit()
    }
    # 캐싱을 위해 정렬된 리스트를 사용
    runner_ids = sorted(list(runner_inputs.keys())) 
except Exception:
    st.error("❌ 입력 형식 오류 (예: 100, 42.195)")
    st.stop()

# st.session_state 로직 제거, st.cache_data로 대체
with st.spinner("데이터 수집 중..."):
    data_list = fetch_many(race_id_int, runner_ids)


oks = [d for d in data_list if not d.get("error")]
all_rows = [r for d in oks for r in normalize_to_rows(d)]
if not all_rows:
    st.error("데이터를 가져오지 못했습니다. 참가자 ID를 확인해주세요.")
    st.stop()

df = pd.DataFrame(all_rows)

# ---------------------------------------------------------
# pace 계산용 데이터 정리 (기존과 동일)
# ---------------------------------------------------------
df["total_seconds"] = df["total_time"].fillna('').astype(str).apply(parse_hhmmss_to_seconds).astype(float)
df["split_seconds"] = df["split_time"].fillna('').astype(str).apply(parse_hhmmss_to_seconds).astype(float)

# ---------------------------------------------------------
# 참가자 요약 데이터 구성 (기존과 동일)
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
        known_sections = sub[sub["total_time"].notna()]
        if not known_sections.empty:
            max_known_distance = max([
                float(s.replace('K', '').replace('k', '').replace('m', ''))
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
# 코스별 트랙 시각화 함수 (기존과 동일)
# ---------------------------------------------------------
def render_course_track(course_name: str, total_distance: float, runners_data: pd.DataFrame):
    # ... (기존 render_course_track 함수 내용 유지)
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

    checkpoints = [0, 5, 10, 20, 30, 40, 42.195] if total_distance > 30 else ([0, 5, 10, 15, 21.0975] if total_distance > 15 else [0, 5, 10, total_distance])
    checkpoints = sorted(list(set([c for c in checkpoints if c <= total_distance])))
    
    # 코스 거리와 체크포인트 보정
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
        color_class = "runner-finished" if r["is_finished"] else "runner-progress"
        label = f"{r['name']}"
        html += f"<div class='runner-marker {color_class}' style='top:{top_percent}%; left: calc(50% + {offset}px);'>{label}</div>"

    html += "</div>"
    st.html(html)


# ---------------------------------------------------------
# UI 구성 (개편된 st.expander 적용)
# ---------------------------------------------------------
tab_individual, tab_overall = st.tabs(["개별 참가자 기록 카드", "전체 코스별 예상 위치"])

# =================== 개별 카드 (수정된 부분) ===================
with tab_individual:
    st.subheader("개별 참가자 기록 카드")
    
    # ⭐ 기존의 커스텀 CSS 및 수동 토글 로직(st.button + st.session_state) 제거
    # 대신 st.expander를 사용하여 리로드 없는 확장/축소 기능 구현

    for rid, sub in df.groupby("runner_id"):
        # 카드에 표시할 기본 정보 추출
        name, gender, bib = sub[["name", "gender", "bib_no"]].iloc[0]
        total_course_km, max_known_distance, is_finished = sub[["total_course_km", "max_known_distance", "is_finished"]].iloc[0]
        total_sec = sub["total_seconds"].dropna().max()

        pace_str = "-"
        if total_sec > 0 and max_known_distance > 0:
            pace_min = (total_sec / max_known_distance) / 60
            pace_str = f"{int(pace_min):02d}:{int((pace_min % 1)*60):02d} 분/km"
        
        # st.expander의 헤더(label) 구성
        label_line1 = f"🏃 **{name}** ({gender}) | 등번호 **#{bib}**"
        label_line2 = f"코스: {format_km(total_course_km)}km | 현재 페이스: {pace_str}"
        expander_label = f"{label_line1} \t \t {label_line2}"
        
        # ⭐ st.expander 사용: 토글 시 Streamlit 재실행(rerun)이 발생하지 않음
        with st.expander(label=expander_label):
            # 확장되었을 때 보이는 상세 내용
            if is_finished:
                st.success(f"✅ **최종 기록**: {seconds_to_hhmmss(total_sec)}")
            else:
                st.info(f"⏳ **진행 중** - 현재 거리: {max_known_distance:.1f} km")

            st.progress(
                max_known_distance / total_course_km,
                text=f"{format_km(max_known_distance)} / {format_km(total_course_km)} km"
            )
            
            st.markdown("---")
            st.subheader("구간별 상세 기록")
            st.dataframe(
                sub[["section", "pass_time", "split_time", "total_time"]].fillna('-'),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "section": st.column_config.TextColumn("구간", width="small"),
                    "pass_time": st.column_config.TextColumn("통과 시각", width="small"),
                    "split_time": st.column_config.TextColumn("구간 기록", width="small"),
                    "total_time": st.column_config.TextColumn("누적 기록", width="small"),
                }
            )

# =================== 전체 트랙 (기존과 동일) ===================
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