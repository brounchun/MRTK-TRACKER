import sys
import json
import traceback
import asyncio
import os
import time
import multiprocessing
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright._impl._errors import TargetClosedError


# ============================================================
# MyResultScraper Class
# ============================================================
class MyResultScraper:
    def __init__(self, base="https://www.myresult.co.kr", timeout=10):
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.is_cloudrun = "K_SERVICE" in os.environ
        self.cpu_count = multiprocessing.cpu_count()
        self.limit = min(self.cpu_count * 2, 8)

        print(
            f"[⚙️ 환경 감지] {'Cloud Run' if self.is_cloudrun else 'Local'} 모드 | "
            f"CPU {self.cpu_count}개 | 동시 처리 제한: {self.limit}명",
            file=sys.stderr,
            flush=True,
        )

    # ---------------------------------------------------------
    # HTML 파싱
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
        player = soup.select_one("div.card-player.ant-card")
        name = gender = bib_no = ""
        event_name = ""
        try:
            event_title = soup.select_one("div.ant-card:not(.card-player) .ant-card-meta-title")
            if event_title:
                event_name = event_title.get_text(strip=True)
            if player:
                name_tag = player.select_one(".ant-card-meta-title")
                desc_tag = player.select_one(".ant-card-meta-description")
                if name_tag:
                    name = name_tag.get_text(strip=True)
                if desc_tag:
                    parts = [p.strip() for p in desc_tag.get_text(strip=True).split("|")]
                    if len(parts) >= 1:
                        gender = parts[0]
                    if len(parts) >= 2:
                        bib_no = parts[1].replace("#", "").strip()
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
    # 개별 참가자 처리
    # ---------------------------------------------------------
    async def fetch_runner(self, page, race_id: int, runner_id: int) -> Dict[str, Any]:
        url = f"{self.base}/{race_id}/{runner_id}"
        try:
            await page.goto(url, timeout=self.timeout * 1000, wait_until="domcontentloaded")
            try:
                await page.wait_for_selector("div.table-row.ant-row", timeout=6000)
            except PlaywrightTimeoutError:
                await asyncio.sleep(0.5)
            html = await page.content()
            parsed = self.parse_runner(html)
            parsed["runner_id"] = runner_id
            return parsed
        except PlaywrightTimeoutError:
            print(f"[timeout] {race_id}/{runner_id}", file=sys.stderr, flush=True)
            return {"runner_id": runner_id, "error": "timeout"}
        except TargetClosedError as e:
            print(f"[TargetClosedError] {race_id}/{runner_id} 실패: {e}", file=sys.stderr, flush=True)
            return {"runner_id": runner_id, "error": "TargetClosed"}
        except Exception as e:
            print(f"[error] {race_id}/{runner_id} 실패: {e}", file=sys.stderr, flush=True)
            return {"runner_id": runner_id, "error": str(e)}

    # ---------------------------------------------------------
    # 병렬 크롤링 (단일 브라우저, 다중 페이지)
    # ---------------------------------------------------------
    async def get_many_async(self, race_id: int, runner_ids: List[int]) -> List[Dict[str, Any]]:
        total = len(runner_ids)
        done = 0
        start_time = time.time()

        print(f"[🚀] 병렬 크롤링 시작 (총 {total}명, 동시 {self.limit}명)", file=sys.stderr, flush=True)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--no-zygote",
                    "--headless=new",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-renderer-backgrounding",
                    "--disable-ipc-flooding-protection",
                ],
            )
            context = await browser.new_context(ignore_https_errors=True)
            sem = asyncio.Semaphore(self.limit)

            async def worker(runner_id: int):
                nonlocal done
                async with sem:
                    page = await context.new_page()
                    res = await self.fetch_runner(page, race_id, runner_id)
                    await page.close()
                    done += 1
                    pct = (done / total) * 100
                    elapsed = time.time() - start_time
                    avg = elapsed / done
                    eta = (total - done) * avg
                    print(f"[{pct:5.1f}%] {done}/{total} 완료 - {runner_id} (평균 {avg:.2f}s, ETA {eta:.1f}s)", file=sys.stderr, flush=True)
                    return res

            tasks = [worker(r) for r in runner_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            await browser.close()

        total_time = time.time() - start_time
        print(f"[✅] 전체 완료 ({done}/{total}) 총 소요 {total_time:.2f}s", file=sys.stderr, flush=True)
        return results

    # ---------------------------------------------------------
    # 외부 호출용 (동기 Wrapper)
    # ---------------------------------------------------------
    def get_many(self, race_id: int, runner_ids: List[int]) -> List[Dict[str, Any]]:
        try:
            return asyncio.run(self.get_many_async(race_id, runner_ids))
        except Exception as e:
            print(f"[FATAL] 병렬 크롤링 실패: {e}", file=sys.stderr, flush=True)
            return []


# ============================================================
# 실행부 (총 29명 자동 분할 실행)
# ============================================================
if __name__ == "__main__":
    try:
        if len(sys.argv) < 3:
            raise ValueError("인자 부족: race_id, runner_ids 필요")

        race_id = int(sys.argv[1])
        runner_ids = [int(x) for x in sys.argv[2].split(",") if x.strip().isdigit()]
        if not runner_ids:
            raise ValueError("runner_ids가 비어 있습니다.")

        scraper = MyResultScraper()

        # ✅ 29명 자동 2배치 분할 (성능 균형)
        batches = [runner_ids[i:i + 15] for i in range(0, len(runner_ids), 15)]
        all_results = []

        for i, batch in enumerate(batches, start=1):
            print(f"\n[Batch {i}/{len(batches)}] ▶ {len(batch)}명 처리 중...\n", file=sys.stderr, flush=True)
            res = scraper.get_many(race_id, batch)
            all_results.extend(res)

        print(json.dumps(all_results, ensure_ascii=False), flush=True)

    except Exception as e:
        err = {"error": str(e), "trace": traceback.format_exc(limit=2)}
        print(json.dumps(err, ensure_ascii=False), flush=True)
        sys.exit(1)
