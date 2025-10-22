import asyncio
# On Windows, this line handles specific event loop requirements for Playwright.
# On Linux/macOS, it can often be omitted or set differently.
try:
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
except AttributeError:
    pass # Ignore if policy is not available (e.g., non-Windows)

import time
import sys # <-- 시스템 로깅을 위해 추가
from typing import Dict, Any, List, Optional

# Playwright and BeautifulSoup are imported inside the class methods 
# or assumed to be available globally in this runnable script.
from bs4 import BeautifulSoup
# The playwright import is intentionally left inside fetch_html 
# as provided in the original source, but the library must be installed.


# --- Original MyResultScraper Class ---
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MyRunnerViewer/1.0; +https://example.com)"
}

class MyResultScraper:
    """
    MyResultScraper 클래스는 Playwright를 사용하여 동적으로 로드된 
    웹 페이지에서 달리기 경주 결과를 스크랩하고 파싱합니다.
    """
    def __init__(self, base: str = "https://www.myresult.co.kr", delay_sec: float = 0.8, timeout: int = 15):
        self.base = base.rstrip("/")
        self.delay_sec = delay_sec
        self.timeout = timeout

    def fetch_html(self, race_id: int, runner_id: int) -> Optional[str]:
        """Playwright로 렌더링된 HTML 가져오기"""
        url = f"{self.base}/{race_id}/{runner_id}"
        print(f"[{race_id}/{runner_id}] URL 접속 시도: {url}", file=sys.stderr) # 로그 강화
        
        try:
            from playwright.sync_api import sync_playwright
            
            with sync_playwright() as p:
                # 🚨 Docker 환경 실행을 위해 --no-sandbox 옵션 추가
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox'] # <-- 핵심 수정
                )
                page = browser.new_page()
                page.goto(url, timeout=self.timeout * 1000)
                
                # JS 데이터 로드 대기: 
                print(f"[{race_id}/{runner_id}] 데이터 셀렉터 대기 중...", file=sys.stderr)
                # 데이터가 로드된 테이블 행이 나타날 때까지 기다립니다. (최대 8초)
                page.wait_for_selector("div.table-row.ant-row", timeout=8000)
                
                html = page.content()
                browser.close()
                print(f"[{race_id}/{runner_id}] HTML 가져오기 성공.", file=sys.stderr)
                return html
        except Exception as e:
            # 🚨 실패 시 에러를 sys.stderr로 출력하여 로그 확인이 용이하도록 함
            print(f"[{race_id}/{runner_id}] [Playwright 오류] HTML 가져오기 실패: {e}", file=sys.stderr)
            return None

    def parse_runner(self, html: str) -> Dict[str, Any]:
        """가져온 HTML을 분석하여 주자 정보를 추출합니다."""
        soup = BeautifulSoup(html, "lxml")
        rows = soup.select("div.table-row.ant-row")
        sections: List[Dict[str, Any]] = []

        # 섹션별 기록 추출 (중간 기록)
        for row in rows:
            # 모든 데이터를 ant-col-6 클래스에서 가져옵니다.
            cols = [div.get_text(strip=True) for div in row.select("div.ant-col.ant-col-6")]
            if not cols:
                continue
            
            # 컬럼 인덱스 확인 및 할당
            sec = {
                "section": cols[0] if len(cols) > 0 else "", # 구간 이름
                "pass_time": cols[1] if len(cols) > 1 else "", # 통과 시간
                "split_time": cols[2] if len(cols) > 2 else "", # 구간 기록
                "total_time": cols[3] if len(cols) > 3 else "", # 누적 시간
            }
            sections.append(sec)

        # -----------------------------
        # 이름, 성별, 배번 추출 (페이지 상단 메타 정보)
        # -----------------------------
        name = ""
        gender = ""
        bib_no = ""
        try:
            # 이름: div.ant-card-meta-title
            name_tag = soup.select_one("div.ant-card-meta-detail > div.ant-card-meta-title") 
            if name_tag:
                name = name_tag.get_text(strip=True)

            # 성별/번호: div.ant-card-meta-description (예: 남자 | #1060)
            desc_tag = soup.select_one("div.ant-card-meta-detail > div.ant-card-meta-description")
            if desc_tag:
                desc_text = desc_tag.get_text(strip=True)
                parts = [p.strip() for p in desc_text.split("|")]
                if len(parts) >= 1:
                    gender = parts[0].strip()
                if len(parts) >= 2:
                    bib_no = parts[1].replace("#", "").strip()
        except Exception as e:
            # 메타 정보 추출 중 발생한 오류는 무시하고 기본값 사용
            print(f"파싱 중 메타데이터 추출 오류: {e}")
            pass

        # 대회명 추출
        event_name = ""
        try:
            event_tag = soup.find("div", class_="ant-card-head-title")
            if event_tag:
                event_name = event_tag.get_text(strip=True)
        except Exception:
            pass
        
        return {
            "name": name or "이름없음",
            "gender": gender,
            "bib_no": bib_no,
            "event_name": event_name,
            "sections": sections,
        }
        
    def get_runner(self, race_id: int, runner_id: int) -> Dict[str, Any]:
        """단일 주자 기록을 가져와 파싱된 결과를 반환합니다."""
        # 지연 시간 적용
        time.sleep(self.delay_sec) 
        
        html = self.fetch_html(race_id, runner_id)
        if not html:
            return {"runner_id": runner_id, "error": "fetch_failed"}
        
        parsed = self.parse_runner(html)
        parsed["runner_id"] = runner_id
        return parsed

# ------------------------------------
# 데모 실행 로직 (변경 없음)
# ------------------------------------
if __name__ == "__main__":
    # --- 주의 ---
    # 실제 존재하는 Race ID와 Runner ID를 사용해야 데이터를 가져올 수 있습니다.
    # 아래 ID는 예시이므로, 스크래퍼가 정상 작동하는지 확인하려면 
    # 실제 MyResult 페이지에서 유효한 ID를 찾아 입력해야 합니다.
    EXAMPLE_RACE_ID = 1111 # 여기에 실제 레이스 ID를 입력하세요.
    EXAMPLE_RUNNER_ID = 1001 # 여기에 실제 주자 ID를 입력하세요.

    # 1. Scraper 인스턴스 생성 (기본 0.8초 딜레이 적용)
    scraper = MyResultScraper(
        base="https://www.myresult.co.kr", 
        delay_sec=0.8, 
        timeout=15
    )

    print("--- 스크래핑 시작 ---")
    
    # 2. 데이터 가져오기
    runner_data = scraper.get_runner(
        race_id=EXAMPLE_RACE_ID, 
        runner_id=EXAMPLE_RUNNER_ID
    )

    print("\n--- 스크래핑 결과 ---")
    
    # 3. 결과 출력
    if "error" in runner_data:
        print(f"오류 발생: {runner_data['error']}. ID ({EXAMPLE_RACE_ID}/{EXAMPLE_RUNNER_ID})를 확인해주세요.")
    else:
        print(f"대회명: {runner_data['event_name']}")
        print(f"이름: {runner_data['name']} ({runner_data['gender']}, 배번 #{runner_data['bib_no']})")
        print("-" * 30)
        
        if runner_data['sections']:
            print(f"{'구간명':<10} | {'통과 시간':<10} | {'구간 기록':<10} | {'누적 시간':<10}")
            print("-" * 30)
            for sec in runner_data['sections']:
                print(
                    f"{sec['section']:<10} | {sec['pass_time']:<10} | "
                    f"{sec['split_time']:<10} | {sec['total_time']:<10}"
                )
        else:
            print("섹션별 기록을 찾을 수 없습니다.")

    print("\n--- 스크래핑 완료 ---")
