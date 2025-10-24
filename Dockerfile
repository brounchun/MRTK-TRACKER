# ▶ 베이스 이미지 최신 버전
FROM python:3.11-slim

# ▶ 환경설정
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PATH="/root/.local/bin:$PATH"

WORKDIR /app

# ▶ 시스템 기본 유틸 및 Playwright 의존성
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates fonts-liberation libnss3 libatk-bridge2.0-0 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

# ▶ Python 의존성
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ▶ Playwright 브라우저 사전설치 (빌드 타임)
RUN python -m playwright install --with-deps chromium

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

# ▶ Cloud Run 기준 포트 바인딩 및 실행
CMD ["bash", "-lc", "streamlit run app.py --server.address=0.0.0.0 --server.port=${PORT:-8080}"]
