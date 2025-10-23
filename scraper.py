import asyncio
import os
import sys
import time
import multiprocessing
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


class MyResultScraper:
    """Playwright(async) 기반 고속 + Cloud Run 환경 적응형 버전"""

    def __init__(self, base="https://www.myresult.co.kr", timeout=12):
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.is_cloudrun = "K_SERVICE" in os.environ  # Cloud Run 환경 감지
        self.cpu_count = multiprocessing.cpu_count()

        # Cloud Run이면 vCPU가 2개 이하일 가능성 높음
        # 자동 limit 계산
        if self.is_cloudrun:
            if self.cpu_count <= 1:
                self.limit = 6
            elif self.cpu_count == 2:
                self.limit = 8
            else:
                self.limit = 10
        else:
            # 로컬은 더 넉넉하게
            self.limit = min(12, self.cpu_count * 2)

        print(
            f"[⚙️ 환경 감지] {'Cloud Run' if self.is_cloudrun else 'Local'} 모드 | "
            f"CPU {self.cpu_count}개 | 동시 처리 제한: {self.limit}명", file=sys.stderr, flush=True
        )

    # ---------------------------------------------------------
    # HTML 파싱 (기존 동일)
    # ---------------------------------------------------------
    def parse_runner(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "lxml")
        rows = soup.select("div.table-row.ant-row")
        sections = []
        for row in rows:
            cols = [div.get_text(strip=True) for div in row.select("div.ant-col.ant-col-6")]
            if cols:
                sections.append({
                    "section": cols[0] if len(cols) > 0 else "",
                    "pass_time": cols[1] if len(cols) > 1 else "",
                    "split_time": cols[2] if len(cols) > 2 else "",
                    "total_time": cols[3] if len(cols) > 3 else "",
                })

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
    # 참가자 개별 처리
    # ---------------------------------------------------------
    async def fetch_runner(self, page, race_id: int, runner_id: int) -> Dict[str, Any]:
        url = f"{self.base}/{race_id}/{runner_id}"
        try:
            await page.goto(url, timeout=self.timeout * 1000, wait_until="domcontentloaded")
            try:
                await page.wait_for_selector("div.table-row.ant-row", timeout=7000)
            except:
                await asyncio.sleep(0.8)
            html = await page.content()
            parsed = self.parse_runner(html)
            parsed["runner_id"] = runner_id
            return parsed
        except PlaywrightTimeoutError:
            return {"runner_id": runner_id, "error": "timeout"}
        except Exception as e:
            return {"runner_id": runner_id, "error": str(e)}

    # ---------------------------------------------------------
    # 적응형 병렬 크롤링
    # ---------------------------------------------------------
    async def get_many_async(self, race_id: int, runner_ids: List[int]) -> List[Dict[str, Any]]:
        total = len(runner_ids)
        done = 0
        start_time = time.time()
        results = []

        print(f"[🚀] 병렬 크롤링 시작 (총 {total}명, 동시 {self.limit}명)", file=sys.stderr, flush=True)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                channel="chrome",
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-software-rasterizer",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--disable-renderer-backgrounding",
                    "--disable-ipc-flooding-protection",
                    "--hide-scrollbars",
                    "--mute-audio",
                    "--headless=new",
                    "--window-position=-2000,-2000"
                ],
            )
            context = await browser.new_context()

            page_pool = [await context.new_page() for _ in range(self.limit)]
            sem = asyncio.Semaphore(self.limit)
            lock = asyncio.Lock()

            async def worker(runner_id: int):
                nonlocal done
                async with sem:
                    async with lock:
                        page = page_pool.pop() if page_pool else await context.new_page()
                    t0 = time.time()
                    res = await self.fetch_runner(page, race_id, runner_id)
                    await page.goto("about:blank")
                    async with lock:
                        page_pool.append(page)
                    done += 1
                    elapsed = time.time() - start_time
                    avg = elapsed / done
                    remain = total - done
                    eta = remain * avg
                    pct = (done / total) * 100
                    print(
                        f"[{pct:5.1f}%] {done}/{total} 완료 - ID {runner_id} "
                        f"(평균 {avg:.2f}s, ETA {eta:.1f}s)",
                        file=sys.stderr, flush=True
                    )
                    return res

            # adaptive batch execution
            batch_size = self.limit if self.is_cloudrun else self.limit * 2
            results = []
            for i in range(0, total, batch_size):
                batch = runner_ids[i:i + batch_size]
                print(f"[⚡] Batch {i//batch_size+1} 실행 ({len(batch)}명)", file=sys.stderr, flush=True)
                part = await asyncio.gather(*(worker(r) for r in batch))
                results.extend(part)

            await browser.close()

        total_time = time.time() - start_time
        print(f"[✅] 전체 완료 ({done}/{total}) 총 소요 {total_time:.2f}s", file=sys.stderr, flush=True)
        print("[🧹] 브라우저 정상 종료", file=sys.stderr, flush=True)
        return results

    # ---------------------------------------------------------
    # 외부 호출용 wrapper
    # ---------------------------------------------------------
    def get_many(self, race_id: int, runner_ids: List[int]) -> List[Dict[str, Any]]:
        try:
            return asyncio.run(self.get_many_async(race_id, runner_ids))
        except Exception as e:
            print(f"[FATAL] 병렬 크롤링 실패: {e}", file=sys.stderr, flush=True)
            return []
