# MyResult Multi-Runner Viewer (Python-only)

이 프로젝트는 `https://www.myresult.co.kr/{race_id}/{runner_id}` 형태의 페이지에서
여러 참가자의 구간기록(구간명, 통과시간, 누적기록)을 수집해 한 화면에서 보여주는 **파이썬 단독(Streamlit)** 앱입니다.

> 참고: 대상 사이트 구조가 변경되거나 자바스크립트로만 렌더링될 경우 `requests` 만으로는 안 보일 수 있습니다.
> 이 경우 `Playwright` 추가 설치로 쉽게 전환할 수 있도록 코드에 훅을 포함했습니다.

## 빠른 시작

1) Python 3.10+ 설치
2) 가상환경 생성 및 활성화
```bash
python -m venv .venv
# Windows
.\.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```
3) 의존성 설치
```bash
pip install -r requirements.txt
```
4) 앱 실행
```bash
streamlit run app.py
```
5) 브라우저에서 열리는 UI에 `레이스ID`(예: 129), `참가자ID들`(예: 1060,1061,1062)을 입력하고 [크롤링] 버튼을 누르세요.

## 파일 구성
- `app.py` : Streamlit UI + 그래프, CSV 내보내기
- `scraper.py` : requests + BeautifulSoup 크롤러 (필요시 Playwright 훅 제공)
- `utils.py` : 공통 유틸(시간파싱 등)
- `requirements.txt` : 필수 패키지 목록
- `runner_ids_example.txt` : 예시 참가자 ID 목록

## 법적/윤리적 유의사항
- robots.txt 및 서비스 약관(TOS)을 확인하고 허용 범위 내에서 사용하세요.
- 요청 사이에 지연을 두고, 과도한 트래픽을 유발하지 마세요.
- 로그인/개인정보가 필요한 영역은 수집하지 마세요.

