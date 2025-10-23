import asyncio
import sys
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

class MyResultScraper:
    """Playwright(async) 기반 고속 병렬 버전 — 기존 구조와 동일 출력 유지"""

    def __init__(self, base="https://www.myresult.co.kr", timeout=15):
        self.base = base.rstrip("/")
        self.timeout = timeout

    # ---------------------------------------------------------
    # 🔹 HTML 파싱 (기존 그대로 유지)
    # ---------------------------------------------------------
    def parse_runner(self, html: str) -> Dict[str, Any]:
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

        # 2) 플레이어 카드 영역
        player = soup.select_one("div.card-player.ant-card") or soup.select_one("div.card-player")
        name = gender = bib_no = ""
        event_name = ""

        try:
            event_title = soup.select_one("div.ant-card:not(.card-player) .ant-card-meta-title")
            if event_title:
                event_name = event_title.get_text(strip=True)

            if player:
                name_tag = player.select_one(".ant-card-meta-title")
                desc_tag = player.select_one(".ant-card-meta-description")
            else:
                name_tag = soup.select_one("div.ant-card-meta-title")
                desc_tag = soup.select_one("div.ant-card-meta-description")

            if name_tag:
                name = name_tag.get_text(strip=True)

            if desc_tag:
                parts = [p.strip() for p in desc_tag.get_text(strip=True).split("|")]
                if parts:
                    if any(k in parts[0] for k in ("남", "여", "남자", "여자", "M", "F")):
                        gender = parts[0]
                    if len(parts) >= 2:
                        bib_no = parts[1].replace("#", "").strip()

            if not name and desc_tag:
                chunks = [c.strip() for c in desc_tag.get_text(strip=True).split("|")]
                if chunks:
                    name = chunks[0]

        except Exception as e:
            print(f"[파싱오류] {e}", file=sys.stderr, flush=True)

        return {
            "name": name or "이름없음",
            "gender": gender,
            "bib_no": bib_no,
            "event_name": event_name,
            "sections": sections,
        }

    # ---------------------------------------------------------
    # 🔹 참가자 한 명 처리 (비동기)
    # ---------------------------------------------------------
    async def fetch_runner(self, page, race_id: int, runner_id: int) -> Dict[str, Any]:
        url = f"{self.base}/{race_id}/{runner_id}"
        print(f"[async] {race_id}/{runner_id} 접속 중...", file=sys.stderr, flush=True)
        try:
            await page.goto(url, timeout=self.timeout * 1000)
            await page.wait_for_selector("div.table-row.ant-row", timeout=8000)
            html = await page.content()
            parsed = self.parse_runner(html)
            parsed["runner_id"] = runner_id
            print(f"[async] {race_id}/{runner_id} 완료", file=sys.stderr, flush=True)
            return parsed

        except PlaywrightTimeoutError:
            print(f"[timeout] {race_id}/{runner_id} 시간 초과", file=sys.stderr, flush=True)
            return {"runner_id": runner_id, "error": "timeout"}
        except Exception as e:
            print(f"[error] {race_id}/{runner_id} 실패: {e}", file=sys.stderr, flush=True)
            return {"runner_id": runner_id, "error": str(e)}

    # ---------------------------------------------------------
    # 🔹 병렬 크롤링 (asyncio + 단일 브라우저)
    # ---------------------------------------------------------
    async def get_many_async(self, race_id: int, runner_ids: List[int], limit: int = 10) -> List[Dict[str, Any]]:
        print(f"[🚀] Async 병렬 크롤링 시작 (최대 동시 {limit}명)", file=sys.stderr, flush=True)
        results = []
        sem = asyncio.Semaphore(limit)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = await browser.new_context()

            async def worker(runner_id: int):
                async with sem:
                    page = await context.new_page()
                    res = await self.fetch_runner(page, race_id, runner_id)
                    await page.close()
                    return res

            # gather를 이용해 동시 실행
            results = await asyncio.gather(*(worker(rid) for rid in runner_ids))
            await browser.close()

        print("[🧹] 브라우저 정상 종료", file=sys.stderr)
        return results

    # ---------------------------------------------------------
    # 🔹 외부 호출용 (기존과 동일한 인터페이스)
    # ---------------------------------------------------------
    def get_many(self, race_id: int, runner_ids: List[int], limit: int = 10) -> List[Dict[str, Any]]:
        try:
            return asyncio.run(self.get_many_async(race_id, runner_ids, limit))
        except Exception as e:
            print(f"[FATAL] 병렬 크롤링 실패: {e}", file=sys.stderr, flush=True)
            return []
