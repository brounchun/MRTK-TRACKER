import asyncio
import os
import sys
import time
import multiprocessing
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright._impl._errors import TargetClosedError


class MyResultScraper:
    """Playwright(async) 기반 고속 + Cloud Run 환경 적응형 버전 (최종 극단적 안정화)"""

    def __init__(self, base="https://www.myresult.co.kr", timeout=12):
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.is_cloudrun = "K_SERVICE" in os.environ  # Cloud Run 환경 감지
        self.cpu_count = multiprocessing.cpu_count()

        # ⭐⭐⭐ 핵심 수정: 동시 처리 제한을 8명으로 극단적으로 낮춰 안정성 최대화 ⭐⭐⭐
        self.limit = 12

        print(
            f"[⚙️ 환경 감지] {'Cloud Run' if self.is_cloudrun else 'Local'} 모드 | "
            f"CPU {self.cpu_count}개 | 동시 처리 제한: {self.limit}명", file=sys.stderr, flush=True
        )

    # ---------------------------------------------------------
    # HTML 파싱 (기존과 동일)
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
    # 참가자 개별 처리 (예외 포획 강화)
    # ---------------------------------------------------------
    async def fetch_runner(self, page, race_id: int, runner_id: int) -> Dict[str, Any]:
        url = f"{self.base}/{race_id}/{runner_id}"
        try:
            await page.goto(url, timeout=self.timeout * 1000, wait_until="domcontentloaded")
            try:
                await page.wait_for_selector("div.table-row.ant-row", timeout=7000)
            except PlaywrightTimeoutError:
                await asyncio.sleep(0.8) 
            
            html = await page.content()
            parsed = self.parse_runner(html)
            parsed["runner_id"] = runner_id
            return parsed
        except PlaywrightTimeoutError:
            print(f"[timeout] {race_id}/{runner_id} 시간 초과", file=sys.stderr, flush=True)
            return {"runner_id": runner_id, "error": "timeout"}
        except TargetClosedError as e:
            print(f"[error-TargetClosed] {race_id}/{runner_id} 실패: {e}", file=sys.stderr, flush=True)
            return {"runner_id": runner_id, "error": "TargetClosedError"}
        except Exception as e:
            print(f"[error] {race_id}/{runner_id} 실패: {e}", file=sys.stderr, flush=True) 
            return {"runner_id": runner_id, "error": str(e)}

    # ---------------------------------------------------------
    # 일괄 병렬 크롤링 (최종 안정화 버전)
    # ---------------------------------------------------------
    async def get_many_async(self, race_id: int, runner_ids: List[int]) -> List[Dict[str, Any]]:
        total = len(runner_ids)
        done = 0
        start_time = time.time()

        print(f"[🚀] 병렬 크롤링 시작 (총 {total}명, 브라우저 병렬 {self.limit}개)", file=sys.stderr, flush=True)

        sem = asyncio.Semaphore(self.limit)

        async def worker(runner_id: int):
            nonlocal done
            async with sem:
                async with async_playwright() as p:
                    # 개별 브라우저 프로세스 (각 runner 완전 분리)
                    browser = await p.chromium.launch(
                        headless=True,
                        args=[
                            "--no-sandbox",
                            "--disable-dev-shm-usage",
                            "--disable-gpu",
                            "--no-zygote",
                            "--headless=new",
                            "--disable-extensions",
                            "--disable-software-rasterizer",
                            "--disable-background-networking",
                            "--disable-renderer-backgrounding",
                            "--disable-web-security",
                            "--hide-scrollbars",
                            "--mute-audio",
                        ],
                    )
                    context = await browser.new_context()
                    page = await context.new_page()
                    res = await self.fetch_runner(page, race_id, runner_id)
                    await page.close()
                    await browser.close()

                    done += 1
                    elapsed = time.time() - start_time
                    avg = elapsed / done
                    eta = (total - done) * avg
                    pct = (done / total) * 100
                    print(f"[{pct:5.1f}%] {done}/{total} 완료 - ID {runner_id} (평균 {avg:.2f}s, ETA {eta:.1f}s)", file=sys.stderr, flush=True)
                    return res

        # return_exceptions=True로 중단 방지
        tasks = [worker(r) for r in runner_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_time = time.time() - start_time
        print(f"[✅] 전체 완료 ({done}/{total}) 총 소요 {total_time:.2f}s", file=sys.stderr, flush=True)
        print("[🧹] 모든 브라우저 종료 완료", file=sys.stderr, flush=True)
        return results


    # ---------------------------------------------------------
    # 외부 호출용 wrapper (기존과 동일)
    # ---------------------------------------------------------
    def get_many(self, race_id: int, runner_ids: List[int]) -> List[Dict[str, Any]]:
        try:
            return asyncio.run(self.get_many_async(race_id, runner_ids))
        except Exception as e:
            print(f"[FATAL] 병렬 크롤링 실패: {e}", file=sys.stderr, flush=True)
            return []