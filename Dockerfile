# ▶ 베이스 이미지: Playwright 사전 설치 (브라우저 포함)
FROM mcr.microsoft.com/playwright/python:v1.45.0-focal

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH="/root/.local/bin:$PATH"

WORKDIR /app

# ▶ 시스템 유틸 (선택)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates fonts-liberation libasound2 && \
    rm -rf /var/lib/apt/lists/*

# ▶ Python 의존성
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ▶ 앱 소스 복사
COPY . .

# ▶ Streamlit 환경 설정
RUN mkdir -p ~/.streamlit && echo "\
[server]\n\
headless = true\n\
port = 8080\n\
enableCORS = false\n\
enableXsrfProtection = false\n\
" > ~/.streamlit/config.toml

# ▶ Cloud Run 실행 명령
CMD ["bash", "-lc", "streamlit run app.py --server.address=0.0.0.0 --server.port=${PORT:-8080}"]
