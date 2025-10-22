import streamlit as st
import pandas as pd
from scraper import MyResultScraper
from utils import parse_hhmmss_to_seconds
import numpy as np

# ----------------------------------------------------------------------
# Helper Functions and Constants
# ----------------------------------------------------------------------

# 초(seconds)를 HH:MM:SS 문자열로 변환하는 헬퍼 함수
def seconds_to_hhmmss(seconds: float) -> str:
    """초 단위를 HH:MM:SS 형식의 문자열로 변환합니다."""
    if pd.isna(seconds) or seconds <= 0:
        return "-"
    s = int(seconds)
    h = s // 3600
    s %= 3600
    m = s // 60
    s %= 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def parse_distance_input(text: str) -> float:
    """사용자가 입력한 거리 문자열 (예: 'full', 'half', '42.195')을 km 숫자로 변환합니다."""
    text = str(text).strip().lower()
    if 'full' in text or '풀' in text or '42.195' in text:
        return 42.195
    if 'half' in text or '하프' in text or '21.0975' in text or '21.1' in text:
        return 21.0975
    if '10' in text and not text.startswith(('21', '42')): # 10km을 의도한 경우
        return 10.0
    try:
        # 숫자를 직접 파싱
        return float(text)
    except ValueError:
        return 0.0 # 유효하지 않은 입력

# 주요 섹션 이름과 실제 거리(km)를 매핑하는 딕셔너리
SECTION_DISTANCES_KM = {
    "출발": 0.0,
    "Start": 0.0,
    "5K": 5.0, "10K": 10.0, "15K": 15.0, "20K": 20.0,
    "Half": 21.0975, "25K": 25.0, "30K": 30.0, "35K": 35.0,
    "40K": 40.0,
    "도착": 42.195, # '도착' 섹션을 명시적으로 42.195km로 매핑
    "Finish": 42.195, # 'Finish' 섹션도 42.195km로 매핑
}

# 코스별 체크포인트 정의 (트랙 시각화에 사용)
COURSE_CHECKPOINTS = {
    "10km 레이스": {
        "distance": 10.0,
        "points": [0, 5, 10]
    },
    "하프 마라톤 (21.0975km)": {
        "distance": 21.0975,
        "points": [0, 5, 10, 15, 20, 21.0975]
    },
    "풀 마라톤 (42.195km)": {
        "distance": 42.195,
        "points": [0, 10, 20, 21.0975, 30, 40, 42.195] # 42.195km도 명시
    }
}

def extract_distance_from_section(section_name: str) -> float:
    """섹션 이름에서 숫자 거리를 추출하거나 매핑된 거리를 반환합니다."""

    # 대소문자 무시를 위해 모두 대문자로 변환하여 SECTION_DISTANCES_KM 매핑 확인
    upper_name = str(section_name).strip().upper().replace("KM", "K").replace("HALF", "Half")

    # SECTION_DISTANCES_KM에서 명시적으로 정의된 거리를 먼저 확인
    if upper_name in SECTION_DISTANCES_KM:
        return SECTION_DISTANCES_KM[upper_name]

    # '출발'은 0km로 고정
    if str(section_name).strip().lower() in ["출발", "start"]:
        return 0.0

    try:
        # '5km' 등의 명칭에서 숫자를 추출
        if 'km' in str(section_name).lower():
            num_str = ''.join(c for c in str(section_name) if c.isdigit() or c == '.')
            return float(num_str) if num_str else 0.0
    except:
        pass

    return 0.0

# ----------------------------------------------------------------------
# Track Visualization Function
# ----------------------------------------------------------------------
def render_course_track(course_name: str, total_distance: float, runners_data: pd.DataFrame):
    """코스별 참가자들의 현재 위치를 세로 트랙 형태로 시각화합니다."""

    course_key = next((key for key, val in COURSE_CHECKPOINTS.items()
                       if abs(val["distance"] - total_distance) < 0.1), None)

    checkpoint_kms = COURSE_CHECKPOINTS.get(course_key, {"points": [0, total_distance]})["points"]

    # 🚨 코스 거리에 따라 트랙 높이를 동적으로 설정 (풀코스를 더 길게)
    track_height = 450  # 기본 높이 (10km)
    if total_distance > 30: # 풀 마라톤
        track_height = 850
    elif total_distance > 15: # 하프 마라톤
        track_height = 650

    # 🚨 CSS에 동적 높이 적용
    css = f"""
    <style>
    .track-container {{ 
        position: relative; 
        padding-top: 10px; 
        padding-bottom: 20px; 
        margin: 20px auto; 
        min-height: {track_height}px; 
        max-width: 300px; 
        font-family: Arial, sans-serif; 
    }}
    .track-line {{ position: absolute; left: 50%; top: 0; bottom: 0; width: 4px; background-color: #e0e0e0; transform: translateX(-50%); }}
    .checkpoint {{ position: absolute; left: 50%; transform: translateX(-50%); width: 14px; height: 14px; background-color: #333; border-radius: 50%; z-index: 10; box-shadow: 0 0 0 4px #fff; }}
    .checkpoint-label {{ position: absolute; left: calc(50% + 15px); top: 50%; transform: translateY(-50%); white-space: nowrap; font-weight: 500; background: #f0f0f0; padding: 2px 6px; border-radius: 4px; font-size: 0.85em; border: 1px solid #ddd; }}
    .runner-marker {{ position: absolute; transform: translateY(-50%); padding: 4px 8px; border-radius: 6px; font-size: 0.85em; font-weight: bold; white-space: nowrap; z-index: 20; color: white; box-shadow: 0 2px 5px rgba(0,0,0,0.3); cursor: default; }}
    .runner-left {{ right: calc(50% + 18px); left: auto; background-color: #FF5733; }}
    .runner-right {{ left: calc(50% + 18px); background-color: #00A389; }}
    .runner-dispersed {{ background-color: #00A389; z-index: 30; }}
    .finish-marker-dot {{ background-color: #DC3545 !important; }}
    .start-label {{ background: #28a745; color: white; font-weight: bold; border-color: #28a745; }}
    .finish-label {{ background: #DC3545; color: white; font-weight: bold; border-color: #DC3545; }}
    </style>
    """
    html = css + "<div class='track-container'><div class='track-line'></div>"

    if 'finish_time_seconds' not in runners_data.columns:
        st.error("오류: 데이터에 'finish_time_seconds' 열이 없어 트랙 시각화를 건너뜁니다.")
        return

    runners_data['progress_ratio'] = runners_data.apply(
        lambda r: 1.0 if r['is_finished'] else (r['max_known_distance'] / total_distance) if total_distance > 0 else 0.0,
        axis=1
    )
    runners_data['grouping_key'] = runners_data['progress_ratio'].round(4)

    def assign_dispersal_index(group):
        # 'finish_time_seconds'가 없을 경우를 대비해 예외 처리
        if 'finish_time_seconds' not in group.columns:
            group['dispersal_index'] = 0
            return group

        group = group.sort_values(by='finish_time_seconds', ascending=True)
        # 지그재그 인덱스 생성 (0, -1, 1, -2, 2, ...)
        indices = [(i // 2) if i % 2 == 0 else -(i // 2 + 1) for i in range(len(group))]
        group['dispersal_index'] = indices
        return group

    runners_data = runners_data.groupby('grouping_key', group_keys=False).apply(assign_dispersal_index)
    
    
    # 정렬 시 'finish_time_seconds' 컬럼 존재 여부 확인
    sort_cols = ['is_finished', 'progress_ratio']
    if 'finish_time_seconds' in runners_data.columns:
        sort_cols.append('finish_time_seconds')

    sorted_runners = runners_data.sort_values(
        by=sort_cols,
        ascending=[False, False] + ([True] if 'finish_time_seconds' in runners_data.columns else []),
    ).reset_index(drop=True)

    for km in checkpoint_kms:
        if total_distance <= 0: continue
        progress_ratio = km / total_distance
        top_percent = progress_ratio * 100
        if abs(km - 42.195) < 0.001: label = f"{km:.3f}km"
        elif abs(km - 21.0975) < 0.0001: label = f"{km:.4f}km"
        else: label = f"{km:.1f}km"
        checkpoint_class, label_class = "checkpoint", "checkpoint-label"
        if km == 0:
            label, label_class = "START", label_class + " start-label"
        elif abs(km - total_distance) < 0.1:
            label, checkpoint_class, label_class = "FINISH", checkpoint_class + " finish-marker-dot", label_class + " finish-label"
        html += f"<div class='{checkpoint_class}' style='top: {top_percent}%;'></div><div class='{label_class}' style='top: {top_percent}%;'>{label}</div>"

    OFFSET_STEP_PX = 100
    for idx, runner in sorted_runners.iterrows():
        max_dist, name = runner["max_known_distance"], runner["name"]
        if total_distance <= 0 or (max_dist <= 0 and not runner["is_finished"]): continue
        progress_ratio, offset_index = runner['progress_ratio'], runner['dispersal_index']
        offset_px = offset_index * OFFSET_STEP_PX
        marker_class, marker_pos_class, left_pos, label_text = "runner-marker", "", "", ""
        is_stacked = abs(offset_index) > 0
        if runner["is_finished"]:
            marker_class += " runner-dispersed"
            left_pos = f"left: 50%; transform: translate(calc(-50% + {offset_px}px), -50%);"
            label_text = f"🎉 {name} (완주)"
        elif is_stacked:
            marker_class += " runner-dispersed"
            left_pos = f"left: 50%; transform: translate(calc(-50% + {offset_px}px), -50%);"
            label_text = f"{name} ({max_dist:.1f}km)"
        else:
            marker_pos_class = "runner-left" if idx % 2 == 0 else "runner-right"
            label_text = f"{name} ({max_dist:.1f}km)"
        top_percent = progress_ratio * 100
        html += f"<div class='{marker_class} {marker_pos_class}' style='top: {top_percent}%; {left_pos}'>{label_text}</div>"
    html += "</div>"
    st.html(html)

# ----------------------------------------------------------------------
# Streamlit App Layout
# ----------------------------------------------------------------------

st.set_page_config(page_title="MyResult 다중 참가자 구간기록 뷰어", layout="wide")
st.title("MyResult 다중 참가자 구간기록 뷰어 (Python-only)")

with st.expander("사용법", expanded=False):
    st.markdown("""
    1) 레이스 ID를 입력합니다. 예) 레이스: **132**
    2) **참가자 ID와 목표 거리**를 쉼표로 구분하여 입력하고, 참가자별로 줄바꿈합니다.
        - **형식**: `[배번ID], [거리(km)]`
        - **거리 예시**: `42.195` 또는 `full`, `21.0975` 또는 `half`, `10`
        - 예) `220, 42.195` (풀코스), `14100, half` (하프코스)
    3) [크롤링]을 누르면 데이터를 가져옵니다.
    """)

race_id = st.text_input("레이스 ID", value="132")
runner_details_text = st.text_area(
    "참가자 상세 정보 (배번ID, 목표거리(km))",
    value="220, 42.195\n14100, 42.195\n39074, 10",
    height=100
)
start = st.button("크롤링")

@st.cache_data(show_spinner=False)
def fetch_many(race_id_int: int, ids: list[int]):
    scraper = MyResultScraper()    
    return [scraper.get_runner(race_id_int, rid) for rid in ids]

def normalize_to_rows(one: dict) -> list[dict]:
    rows = []
    runner_id = one.get("runner_id")
    # 'sections' 키가 없는 경우 빈 리스트를 순회하여 에러 방지
    for sec in one.get("sections", []): 
        rows.append({
            "runner_id": runner_id, "name": one.get("name", ""), "gender": one.get("gender", ""),
            "bib_no": one.get("bib_no", ""), "section": sec.get("section", ""),
            "pass_time": sec.get("pass_time", ""), "split_time": sec.get("split_time", ""),
            "total_time": sec.get("total_time", "")
        })
    return rows

df = st.session_state.get("df_data", None)

if start:
    try:
        race_id_int = int(race_id.strip())
        runner_inputs = {int(p[0]): parse_distance_input(p[1]) for line in runner_details_text.strip().split('\n') if (p := [x.strip() for x in line.split(',')]) and len(p) >= 2 and p[0].isdigit()}
        runner_ids = list(runner_inputs.keys())
    except Exception:
        st.error("입력 형식을 확인해주세요. (예: 100, 42.195)")
        st.stop()

    if not runner_ids:
        st.error("유효한 참가자 ID를 입력해주세요.")
        st.stop()

    with st.spinner("데이터 수집 중..."):
        data_list = fetch_many(race_id_int, runner_ids)

    oks = [d for d in data_list if not d.get("error")]
    all_rows = [r for d in oks for r in normalize_to_rows(d)]
    

    if not all_rows:
        st.error("데이터를 가져오지 못했습니다. 참가자 ID를 확인해주세요.")
        if "df_data" in st.session_state: del st.session_state["df_data"]
        st.stop()

    df = pd.DataFrame(all_rows)
    # total_time, split_time 값이 NaN 또는 None일 경우를 대비하여 fillna('') 후 처리
    df["total_seconds"] = df["total_time"].fillna('').astype(str).apply(parse_hhmmss_to_seconds).astype(float)
    df["split_seconds"] = df["split_time"].fillna('').astype(str).apply(parse_hhmmss_to_seconds).astype(float)

    runner_properties = []
    for rid, sub in df.groupby("runner_id"):
        # sub DataFrame이 비어있을 경우 건너뛰기
        if sub.empty:
            continue

        user_target_km = runner_inputs.get(rid, 0.0)
        total_course_km = user_target_km
        if total_course_km <= 0.1:
            sections_upper = [str(s).strip().upper().replace("KM", "K") for s in sub["section"].values]
            if any(sec in sections_upper for sec in ["30K", "35K", "40K", "FINISH", "도착"]): total_course_km = 42.195
            elif any(sec in sections_upper for sec in ["HALF", "20K", "21.1K"]): total_course_km = 21.0975
            else: total_course_km = 10.0

        if abs(total_course_km - 42.195) < 0.001: course_name = f"풀 마라톤 ({total_course_km:.3f}km)"
        elif abs(total_course_km - 21.0975) < 0.0001: course_name = f"하프 마라톤 ({total_course_km:.4f}km)"
        else: course_name = f"{total_course_km:.1f}km 레이스"

        max_known_distance = max((extract_distance_from_section(s) for s in sub["section"]), default=0.0)
        
        is_finished = False
        finish_time_sec = np.inf
        # 'total_seconds'가 이미 float으로 변환되었으므로, 여기서 문제가 생길 가능성은 낮음
        finish_records = sub[sub["section"].str.contains("도착|Finish", case=False, na=False)]
        
        # 'total_seconds' 컬럼이 있고, 기록이 있는 경우에만 처리
        if 'total_seconds' in finish_records.columns and total_course_km > 0 and not finish_records.empty and finish_records["total_seconds"].max() > 0:
            is_finished = True
            max_known_distance = total_course_km
            finish_time_sec = finish_records["total_seconds"].max()

        runner_properties.append({
            'runner_id': rid, 'max_known_distance': max_known_distance, 'total_course_km': total_course_km,
            'course_name': course_name, 'is_finished': is_finished, 'finish_time_seconds': finish_time_sec,
        })

    props_df = pd.DataFrame(runner_properties)
    df = df.merge(props_df, on='runner_id', how='left')
    st.session_state["df_data"] = df

if df is not None:
    tab_individual, tab_overall = st.tabs(["개별 참가자 기록 카드", "전체 코스별 예상 위치"])

    with tab_individual:
        st.subheader("개별 참가자 기록 카드 (클릭하여 상세 기록 확인)")
        
        # 🚨 아코디언 기능을 위한 세션 상태 초기화
        if 'active_card' not in st.session_state:
            st.session_state.active_card = None

        # 🚨 카드 클릭 시 호출될 콜백 함수
        def toggle_card(runner_id):
            if st.session_state.active_card == runner_id:
                st.session_state.active_card = None  # 이미 열려있으면 닫기
            else:
                st.session_state.active_card = runner_id # 새로 열기

        # 'runner_id'로 그룹화하기 전에 데이터가 충분한지 확인
        if df.empty or 'runner_id' not in df.columns:
            st.warning("표시할 유효한 참가자 데이터가 없습니다.")
            st.stop()
            
        grouped_runners = list(df.groupby("runner_id"))
        COLUMNS_PER_ROW_INDIVIDUAL = 3

        def format_distance(km):
            if abs(km - 42.195) < 0.001: return f"{km:.3f}km"
            if abs(km - 21.0975) < 0.0001: return f"{km:.4f}km"
            return f"{km:.1f}km"

        for i, (rid, sub) in enumerate(grouped_runners):
            
            # 그룹이 비어있거나 필수 정보가 없는 경우 방어적 코드
            if sub.empty or 'total_course_km' not in sub.columns:
                 continue
                 
            if i % COLUMNS_PER_ROW_INDIVIDUAL == 0:
                cols = st.columns(COLUMNS_PER_ROW_INDIVIDUAL)
            
            with cols[i % COLUMNS_PER_ROW_INDIVIDUAL]:
                name, gender, bib = sub[["name", "gender", "bib_no"]].iloc[0]
                total_course_km, max_known_distance, course_name, is_finished = sub[["total_course_km", "max_known_distance", "course_name", "is_finished"]].iloc[0]
                
                if total_course_km <= 0.1:
                    st.warning(f"🏃 {name} ({gender}) #{bib}: 코스 거리가 유효하지 않습니다.")
                    continue

                # 'total_seconds'가 없을 경우를 대비해 기본값 설정
                total_sec = 0
                if 'total_seconds' in sub.columns:
                    valid_total_seconds = sub["total_seconds"].dropna()
                    total_sec = valid_total_seconds[valid_total_seconds > 0].max() if not valid_total_seconds.empty else 0
                
                pace_calc_dist = total_course_km if is_finished else max_known_distance
                pace_str = "-"
                if total_sec > 0 and pace_calc_dist > 0:
                    avg_pace_min_per_km = (total_sec / pace_calc_dist) / 60
                    minutes, seconds = int(avg_pace_min_per_km), int((avg_pace_min_per_km % 1) * 60)
                    pace_str = f"{minutes:02d}:{seconds:02d} 분/km"
                
                # 🚨 st.expander를 st.container와 st.button으로 대체
                is_open = st.session_state.active_card == rid
                icon = "▼" if is_open else "▶"
                button_title = f"{icon} {name} ({gender}) #{bib} | {course_name} | 페이스: {pace_str}"

                with st.container(border=True):
                    st.button(
                        button_title,
                        key=f"card_btn_{rid}",
                        on_click=toggle_card,
                        args=(rid,),
                        use_container_width=True
                    )
                    
                    # 🚨 현재 카드가 활성화된 경우에만 내용 표시
                    if is_open:
                        if total_sec > 0 and not is_finished:
                            remaining_distance = total_course_km - max_known_distance
                            if remaining_distance > 0 and max_known_distance > 0:
                                current_avg_pace_sec_per_km = total_sec / max_known_distance
                                predicted_arrival_seconds = total_sec + (remaining_distance * current_avg_pace_sec_per_km)
                                st.markdown(f"**➡️ 예상 도착**: <span style='color: #1E90FF;'>{seconds_to_hhmmss(predicted_arrival_seconds)}</span>", unsafe_allow_html=True)
                        elif is_finished:
                            st.success(f"**✅ 최종 기록**: {seconds_to_hhmmss(total_sec)}")
                        
                        progress_ratio = 1.0 if is_finished else min(max_known_distance / total_course_km, 1.0) if total_course_km > 0 else 0.0
                        st.progress(progress_ratio, text=f"{progress_ratio*100:.1f}% ({format_distance(max_known_distance)})")

                        # 'total_seconds'가 있는 경우에만 정렬 시도
                        sort_by_cols = ["section", "pass_time", "split_time", "total_time"]
                        if 'total_seconds' in sub.columns:
                             sorted_sub = sub.sort_values(by='total_seconds', ascending=True)
                        else:
                             sorted_sub = sub
                             
                        st.dataframe(
                            sorted_sub[sort_by_cols],
                            use_container_width=True, hide_index=True
                        )

    with tab_overall:
        st.header("코스별 실시간 진행 상황 트랙")
        valid_runners_df = df[df["total_course_km"] > 0.1]
        for course_name, course_group in valid_runners_df.groupby("course_name"):
            st.markdown(f"## 🏃 {course_name} 트랙")
            
            sort_cols = ['is_finished']
            if 'finish_time_seconds' in course_group.columns:
                 sort_cols.append('finish_time_seconds')

            unique_runners = course_group.drop_duplicates(subset=['runner_id']).sort_values(
                by=sort_cols, ascending=[False] + ([True] if 'finish_time_seconds' in course_group.columns else []),
            ).reset_index(drop=True)
            
            if not unique_runners.empty:
                render_course_track(course_name, unique_runners["total_course_km"].iloc[0], unique_runners)
            else:
                st.info("이 코스에는 표시할 유효한 참가자가 없습니다.")

else:
    st.info("먼저 크롤링 버튼을 눌러 데이터를 불러와주세요.")
st.caption("주의: 사이트 구조나 정책 변경 시 파싱이 실패할 수 있습니다.")
