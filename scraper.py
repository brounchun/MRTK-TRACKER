import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

class MyResultScraper:
    """Playwright(sync) 기반 — Windows/Linux 완전 호환 버전"""

    def __init__(self, base="https://www.myresult.co.kr", timeout=15):
        self.base = base.rstrip("/")
        self.timeout = timeout

    def fetch_html_sync(self, race_id: int, runner_id: int):
        """Playwright를 사용해 HTML을 가져오는 동기 버전"""
        url = f"{self.base}/{race_id}/{runner_id}"
        print(f"[sync] {race_id}/{runner_id} 접속 중...", file=sys.stderr)
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                )
                page = browser.new_page()
                page.goto(url, timeout=self.timeout * 1000)
                page.wait_for_selector("div.table-row.ant-row", timeout=8000)
                html = page.content()
                browser.close()

            parsed = self.parse_runner(html)
            parsed["runner_id"] = runner_id
            print(f"[sync] {race_id}/{runner_id} 완료", file=sys.stderr)
            return parsed

        except PlaywrightTimeoutError:
            print(f"[timeout] {race_id}/{runner_id} 시간 초과", file=sys.stderr)
        except Exception as e:
            print(f"[error] {race_id}/{runner_id} 실패: {e}", file=sys.stderr)
        return {"runner_id": runner_id, "error": "fetch_failed"}

    def parse_runner(self, html: str) -> Dict[str, Any]:
        """HTML 파싱 — card-player 카드에서 이름/성별/번호 추출 (이벤트 카드와 구분)"""
        soup = BeautifulSoup(html, "lxml")

        # 1) 섹션 테이블
        rows = soup.select("div.table-row.ant-row")
        sections = []
        for row in rows:
            cols = [div.get_text(strip=True) for div in row.select("div.ant-col.ant-col-6")]
            if not cols:
                continue
            sections.append({
                "section": cols[0] if len(cols) > 0 else "",
                "pass_time": cols[1] if len(cols) > 1 else "",
                "split_time": cols[2] if len(cols) > 2 else "",
                "total_time": cols[3] if len(cols) > 3 else "",
            })

        # 2) 플레이어 카드 범위 한정 (이벤트 카드와 구분되는 핵심)
        #    <div class="card-player ant-card ..."> 내부의 메타 영역만 사용
        player = soup.select_one("div.card-player.ant-card") or soup.select_one("div.card-player")
        name = gender = bib_no = ""
        event_name = ""

        try:
            # 이벤트명은 (있다면) 첫번째 카드의 메타 타이틀에서 가져오되, player 내부는 제외
            event_title = soup.select_one("div.ant-card:not(.card-player) .ant-card-meta-title")
            if event_title:
                event_name = event_title.get_text(strip=True)

            # 플레이어 카드에서 이름/성별/배번 추출
            if player:
                name_tag = player.select_one(".ant-card-meta-title")
                desc_tag = player.select_one(".ant-card-meta-description")
            else:
                # fallback (이전 방식)
                name_tag = soup.select_one("div.ant-card-meta-title")
                desc_tag = soup.select_one("div.ant-card-meta-description")

            if name_tag:
                name = name_tag.get_text(strip=True)

            if desc_tag:
                # 예: "남자 | #110" 또는 "여자 | #220"
                parts = [p.strip() for p in desc_tag.get_text(strip=True).split("|")]
                if parts:
                    # 첫 요소가 성별일 가능성 높음
                    if any(k in parts[0] for k in ("남", "여", "남자", "여자", "M", "F")):
                        gender = parts[0]
                    # 두 번째 요소가 배번
                    if len(parts) >= 2:
                        bib_no = parts[1].replace("#", "").strip()

            # 최후 보정: 이름이 비었고 desc에 이름이 섞여 있는 구조일 때
            if not name and desc_tag:
                # "김태인 | 남자 | #110" 같은 변형 대응
                chunks = [c.strip() for c in desc_tag.get_text(strip=True).split("|")]
                if chunks:
                    name = chunks[0]

        except Exception as e:
            print(f"[파싱오류] {e}", file=sys.stderr)

        return {
            "name": name or "이름없음",
            "gender": gender,
            "bib_no": bib_no,
            "event_name": event_name,
            "sections": sections,
        }


    def get_many(self, race_id: int, runner_ids: list[int], limit=4) -> List[Dict[str, Any]]:
        """ThreadPoolExecutor 기반 병렬 크롤링"""
        print(f"[🚀] 병렬 크롤링 시작 (최대 동시 {limit}명)", file=sys.stderr)
        results = []
        with ThreadPoolExecutor(max_workers=limit) as executor:
            future_map = {executor.submit(self.fetch_html_sync, race_id, rid): rid for rid in runner_ids}
            for fut in as_completed(future_map):
                result = fut.result()
                if result:
                    results.append(result)
        print("[🧹] 브라우저 정상 종료", file=sys.stderr)
        return results
