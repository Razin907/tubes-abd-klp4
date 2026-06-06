# =====================================================================
# Dockerfile Kustom: Apache Airflow + Google Chrome + Selenium
# =====================================================================
# File ini meracik image Docker Airflow yang sudah dilengkapi dengan
# Google Chrome (Headless) sehingga Selenium dapat berjalan di dalamnya.
# =====================================================================

FROM apache/airflow:2.9.1

USER root

# Install dependensi sistem yang dibutuhkan oleh Google Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libappindicator3-1 \
    libasound2 \
    fonts-liberation \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libx11-xcb1 \
    xdg-utils \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Download dan install Google Chrome Stable
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Kembali ke user airflow yang aman (non-root)
USER airflow

# Copy daftar dependensi dan install semua library Python yang dibutuhkan pipeline
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir magic-pdf[full]
