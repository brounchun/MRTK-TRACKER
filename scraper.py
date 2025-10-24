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

        # Cloud Run 환경에서 동시 처리 제한 조정
        if self.is_cloudrun:
            if self.cpu_count <= 1:
                self.limit = 8
            elif self.cpu_count == 2:
                self.limit = 14 # ✨ A 전략: 2 vCPU 환경에서 동시 처리 제한을 14로 상향
            else:
                self.limit = 20
        else:
            # 로컬은 더 넉넉하게
            self.limit = min(12, self.cpu_count * 2)

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
    # 참가자 개별 처리 (기존과 동일)
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
            print(html[:500], file=sys.stderr, flush=True)
            parsed = self.parse_runner(html)
            parsed["runner_id"] = runner_id
            return parsed
        except PlaywrightTimeoutError:
            return {"runner_id": runner_id, "error": "timeout"}
        except Exception as e:
            return {"runner_id": runner_id, "error": str(e)}

    # ---------------------------------------------------------
    # 일괄 병렬 크롤링 (B 전략 적용: 적응형 배치 제거)
    # ---------------------------------------------------------
    async def get_many_async(self, race_id: int, runner_ids: List[int]) -> List[Dict[str, Any]]:
        total = len(runner_ids)
        done = 0
        start_time = time.time()
        
        print(f"[🚀] 병렬 크롤링 시작 (총 {total}명, 동시 {self.limit}명)", file=sys.stderr, flush=True)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                channel="chrome",
                headless=True,
                args=[
                    "--no-sandbox",
                    "--single-process", # ✨ Cloud Run 환경에서 single-process 추가 (안정성)
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--no-zygote", # ✨ Cloud Run 환경에서 no-zygote 추가 (안정성)
                    "--disable-extensions",
                    "--disable-software-rasterizer",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--disable-renderer-backgrounding",
                    "--disable-ipc-flooding-protection",
                    "--disable-web-security",
                    "--hide-scrollbars",
                    "--mute-audio",
                    "--headless=new",
                    "--window-position=-2000,-2000"
                ],
            )
            context = await browser.new_context()

            # 페이지 풀 생성
            page_pool = [await context.new_page() for _ in range(self.limit)]
            sem = asyncio.Semaphore(self.limit)
            lock = asyncio.Lock()

            async def worker(runner_id: int):
                nonlocal done
                
                # 락을 이용해 페이지 객체를 풀에서 꺼냄
                async with sem:
                    async with lock:
                        page = page_pool.pop(0) if page_pool else await context.new_page() 
                    print(f"[DEBUG] 접속 시도: {url}", file=sys.stderr, flush=True)
                    res = await self.fetch_runner(page, race_id, runner_id)
                    await page.goto("about:blank") # 페이지 상태 클린

                    # 락을 이용해 페이지 객체를 다시 풀에 넣음
                    async with lock:
                        page_pool.append(page)
                    
                    done += 1
                    elapsed = time.time() - start_time
                    avg = elapsed / done
                    remain = total - done
                    eta = remain * avg
                    pct = (done / total) * 100
                    
                    # 진행 상황 출력 (stderr)
                    print(
                        f"[{pct:5.1f}%] {done}/{total} 완료 - ID {runner_id} "
                        f"(평균 {avg:.2f}s, ETA {eta:.1f}s)",
                        file=sys.stderr, flush=True
                    )
                    return res

            # ✨ B 전략: 적응형 배치 실행 대신, 모든 작업을 asyncio.gather로 한 번에 실행
            tasks = [worker(r) for r in runner_ids]
            results = await asyncio.gather(*tasks)

            await browser.close()

        total_time = time.time() - start_time
        print(f"[✅] 전체 완료 ({done}/{total}) 총 소요 {total_time:.2f}s", file=sys.stderr, flush=True)
        print("[🧹] 브라우저 정상 종료", file=sys.stderr, flush=True)
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