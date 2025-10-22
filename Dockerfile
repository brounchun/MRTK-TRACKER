# Dockerfile
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# (선택) 기본 유틸
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install -r requirements.txt

# ▶ Playwright 브라우저 & OS 의존성 설치 (한방)
RUN python -m playwright install --with-deps chromium




# 앱 복사
COPY . .

# Cloud Run: 반드시 0.0.0.0:${PORT} 로 바인딩
CMD ["bash", "-lc", "streamlit run app.py --server.address=0.0.0.0 --server.port=${PORT:-8080} --server.headless=true --server.enableCORS=false --server.enableXsrfProtection=false"]
