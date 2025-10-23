import sys
import json
import traceback
from scraper import MyResultScraper

if __name__ == "__main__":
    try:
        # ✅ 명령줄 인자 파싱
        if len(sys.argv) < 3:
            raise ValueError("인자 부족: race_id, runner_ids 필요")

        race_id = int(sys.argv[1])
        runner_ids = [int(x) for x in sys.argv[2].split(",") if x.strip().isdigit()]

        if not runner_ids:
            raise ValueError("runner_ids가 비어 있습니다.")

        # ✅ 크롤러 실행
        scraper = MyResultScraper()
        result = scraper.get_many(race_id, runner_ids, limit=4)

        # ✅ 표준출력으로 JSON 결과 반환
        print(json.dumps(result, ensure_ascii=False))

    except Exception as e:
        err_msg = {
            "error": str(e),
            "trace": traceback.format_exc(limit=1)
        }
        # ❗ Streamlit에서 subprocess 결과를 받을 때 stderr로 넘기면 로깅에 안 잡히므로, stdout으로 보냄
        print(json.dumps(err_msg, ensure_ascii=False))
        sys.exit(1)
