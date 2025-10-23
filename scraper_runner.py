import sys
import json
from scraper import MyResultScraper

if __name__ == "__main__":
    try:
        race_id = int(sys.argv[1])
        runner_ids = [int(x) for x in sys.argv[2].split(",")]
    except Exception:
        print(json.dumps({"error": "잘못된 입력 인자"}, ensure_ascii=False))
        sys.exit(1)

    scraper = MyResultScraper()
    result = scraper.get_many(race_id, runner_ids, limit=4)
    print(json.dumps(result, ensure_ascii=False))
