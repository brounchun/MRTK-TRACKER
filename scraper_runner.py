import sys
import json
import traceback
from scraper import MyResultScraper

if __name__ == "__main__":
    try:
        if len(sys.argv) < 3:
            raise ValueError("인자 부족: race_id, runner_ids 필요")

        race_id = int(sys.argv[1])
        runner_ids = [int(x) for x in sys.argv[2].split(",") if x.strip().isdigit()]

        if not runner_ids:
            raise ValueError("runner_ids가 비어 있습니다.")

        scraper = MyResultScraper()
        result = scraper.get_many(race_id, runner_ids) 

        # ✅ JSON은 오직 stdout으로만 (정상 종료)
        print(json.dumps(result, ensure_ascii=False), flush=True)

    except Exception as e:
        err_msg = {
            "error": str(e),
            "trace": traceback.format_exc(limit=2)
        }
        print(json.dumps(err_msg, ensure_ascii=False), flush=True)
        sys.exit(1)
