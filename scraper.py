import asyncio
import time
import sys
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import platform # 🚨 platform 모듈 추가

if platform.system() == "Windows": # 🚨 Windows에서만 실행하도록 조건 추가
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except AttributeError:
        pass

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MyRunnerViewer/1.0; +https://example.com)"
}
class MyResultScraper:
    """
    MyResultScraper 클래스는 Playwright를 사용하여
    동적으로 로드된 웹 페이지에서 달리기 경주 결과를 스크랩합니다.
    """

    def __init__(self, base: str = "https://www.myresult.co.kr", delay_sec: float = 0.6, timeout: int = 15):
        self.base = base.rstrip("/")
        self.delay_sec = delay_sec
        self.timeout = timeout

    # -----------------------------
    # 기존 단일 fetch 함수 (수정 없음)
    # -----------------------------
    def fetch_html(self, race_id: int, runner_id: int) -> Optional[str]:
        from playwright.sync_api import sync_playwright
        url = f"{self.base}/{race_id}/{runner_id}"
        print(f"[{race_id}/{runner_id}] 접속 시도: {url}", file=sys.stderr)

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
                page = browser.new_page()
                page.goto(url, timeout=self.timeout * 1000)
                page.wait_for_selector("div.table-row.ant-row", timeout=8000)
                html = page.content()
                browser.close()
                print(f"[{race_id}/{runner_id}] HTML OK", file=sys.stderr)
                return html
        except Exception as e:
            print(f"[{race_id}/{runner_id}] [오류] HTML 실패: {e}", file=sys.stderr)
            return None

    # -----------------------------
    # 기존 파서 (수정 없음)
    # -----------------------------
    def parse_runner(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "lxml")
        rows = soup.select("div.table-row.ant-row")
        sections = []

        for row in rows:
            cols = [div.get_text(strip=True) for div in row.select("div.ant-col.ant-col-6")]
            if not cols:
                continue
            sec = {
                "section": cols[0] if len(cols) > 0 else "",
                "pass_time": cols[1] if len(cols) > 1 else "",
                "split_time": cols[2] if len(cols) > 2 else "",
                "total_time": cols[3] if len(cols) > 3 else "",
            }
            sections.append(sec)

        name, gender, bib_no, event_name = "", "", "", ""

        try:
            name_tag = soup.select_one("div.ant-card-meta-detail > div.ant-card-meta-title")
            if name_tag:
                name = name_tag.get_text(strip=True)
            desc_tag = soup.select_one("div.ant-card-meta-detail > div.ant-card-meta-description")
            if desc_tag:
                parts = [p.strip() for p in desc_tag.get_text(strip=True).split("|")]
                if len(parts) >= 1:
                    gender = parts[0].strip()
                if len(parts) >= 2:
                    bib_no = parts[1].replace("#", "").strip()
            event_tag = soup.find("div", class_="ant-card-head-title")
            if event_tag:
                event_name = event_tag.get_text(strip=True)
        except Exception as e:
            print(f"메타데이터 추출 오류: {e}", file=sys.stderr)

        return {
            "name": name or "이름없음",
            "gender": gender,
            "bib_no": bib_no,
            "event_name": event_name,
            "sections": sections,
        }

    def get_runner(self, race_id: int, runner_id: int) -> Dict[str, Any]:
        """단일 주자 데이터"""
        time.sleep(self.delay_sec)
        html = self.fetch_html(race_id, runner_id)
        if not html:
            return {"runner_id": runner_id, "error": "fetch_failed"}
        parsed = self.parse_runner(html)
        parsed["runner_id"] = runner_id
        return parsed

    # -----------------------------
    # ✅ 병렬 버전 추가
    # -----------------------------
    async def fetch_html_async(self, race_id: int, runner_id: int):
        """Playwright async 버전"""
        from playwright.async_api import async_playwright
        url = f"{self.base}/{race_id}/{runner_id}"
        print(f"[async] {race_id}/{runner_id} 접속 중...", file=sys.stderr)
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
                page = await browser.new_page()
                await page.goto(url, timeout=self.timeout * 1000)
                await page.wait_for_selector("div.table-row.ant-row", timeout=8000)
                html = await page.content()
                await browser.close()
                return runner_id, html
        except Exception as e:
            print(f"[async] {race_id}/{runner_id} 실패: {e}", file=sys.stderr)
            return runner_id, None

    async def _get_many_async(self, race_id: int, runner_ids: list[int], limit: int = 4):
        """내부 async 실행기 — 동시에 limit개씩 실행"""
        sem = asyncio.Semaphore(limit)
        results = []

        async def sem_task(runner_id):
            async with sem:
                _, html = await self.fetch_html_async(race_id, runner_id)
                if html:
                    parsed = self.parse_runner(html)
                    parsed["runner_id"] = runner_id
                    results.append(parsed)
                else:
                    results.append({"runner_id": runner_id, "error": "fetch_failed"})

        await asyncio.gather(*(sem_task(rid) for rid in runner_ids))
        return results

    def get_many(self, race_id: int, runner_ids: list[int], limit: int = 5):
        """비동기 병렬 실행 (외부에서 호출용)"""
        return asyncio.run(self._get_many_async(race_id, runner_ids, limit))
