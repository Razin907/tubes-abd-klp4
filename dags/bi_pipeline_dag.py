from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os

# DAG Pipeline Ekstraksi Data BI dari Web Resmi
# DAG ini mengeksekusi script ektraksi.py menggunakan
# Selenium + Google Chrome Headless di dalam kontainer Docker Airflow.

default_args = {
    'owner': 'Razin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Jika gagal (misal koneksi BI down), otomatis diulang 3x
    'retry_delay': timedelta(minutes=5),
}

# Path skrip di dalam kontainer (di-mount dari folder src/ di lokal)
AIRFLOW_SRC = "/opt/airflow/src"
AIRFLOW_DATA = "/opt/airflow/data"

with DAG(
    'bi_laporan_ekstraksi_medallion',
    default_args=default_args,
    description='Pipeline Ekstraksi Data Kesenjangan Ekonomi Regional (BI)',
    schedule_interval='@monthly',  # Berjalan otomatis sebulan sekali
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tubes', 'medallion', 'bi', 'selenium'],
) as dag:


    # BRONZE LAYER: Pengecekan Website BI
    def check_website_bi():
        import requests
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            )
        }
        try:
            # allow_redirects=False agar kita cukup cek apakah server merespons
            # Status 200 atau 302 sama-sama berarti server aktif
            resp = requests.get(
                "https://www.bi.go.id",
                timeout=15,
                headers=headers,
                allow_redirects=False
            )
            if resp.status_code in [200, 301, 302, 303]:
                logging.info(f"Website Bank Indonesia aktif (status: {resp.status_code}). Siap memulai ekstraksi!")
                return "OK"
            else:
                raise Exception(f"Website BI merespons dengan status tidak dikenal: {resp.status_code}")
        except Exception as e:
            raise Exception(f"Website BI tidak dapat diakses: {e}")

    task_check_web = PythonOperator(
        task_id='check_website_bi',
        python_callable=check_website_bi,
    )


    # BRONZE + SILVER LAYER: Scraping & Ekstraksi
    # Menjalankan skrip ektraksi.py
    # Skrip ini menggunakan Selenium (Chrome Headless) untuk scraping BI,
    # lalu MinerU/AI untuk mengekstrak data angka dari format PDF ke CSV.
    task_scrape_and_extract = BashOperator(
        task_id='scrape_and_extract_to_bronze_silver',
        bash_command=(
            f"cd {AIRFLOW_SRC} && "
            f"PYTHONPATH={AIRFLOW_SRC} "
            f"python {AIRFLOW_SRC}/ektraksi.py"
        ),
        # Timeout 8 jam karena proses ekstraksi 34 provinsi
        execution_timeout=timedelta(hours=8),
    )


    # GOLD LAYER: Transformasi & Penggabungan
    # Menggabungkan 8 CSV di layer Silver menjadi 1 Master Tabel di layer Gold
    task_transform_gold = BashOperator(
        task_id='transform_to_gold_layer',
        bash_command=(
            f"cd {AIRFLOW_SRC} && "
            f"PYTHONPATH={AIRFLOW_SRC} "
            f"python {AIRFLOW_SRC}/transform_gold_layer.py"
        ),
        execution_timeout=timedelta(hours=1),
    )


    # UPLOAD KE MINIO: Sinkronisasi akhir ke Data Lake
    task_upload_minio = BashOperator(
        task_id='upload_gold_to_minio',
        bash_command=(
            f"cd {AIRFLOW_SRC} && "
            f"PYTHONPATH={AIRFLOW_SRC} "
            f"python {AIRFLOW_SRC}/upload_to_minio.py"
        ),
    )


    # CEK KUALITAS DATA (DATA QUALITY CHECK)
    def data_quality_check():
        import pandas as pd
        gold_path = f"{AIRFLOW_DATA}/gold_kesenjangan_regional_master.csv"
        if not os.path.exists(gold_path):
            raise FileNotFoundError(f"File Gold tidak ditemukan: {gold_path}")
        df = pd.read_csv(gold_path)
        logging.info("Data Quality Check:")
        logging.info("  - Total baris: %d", len(df))
        logging.info("  - Total kolom: %d", len(df.columns))
        logging.info("  - Kolom: %s", list(df.columns))
        # Cek nilai null pada kolom penting
        null_count = df['provinsi'].isnull().sum() if 'provinsi' in df.columns else 0
        if null_count > 0:
            logging.warning("  - PERINGATAN: %d baris tanpa nama provinsi!", null_count)
        logging.info("  - Data Quality Check: LULUS!")
        return "Data Valid!"

    task_dq_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
    )


    # ALUR KERJA (DEPENDENCY GRAPH)
    task_check_web >> task_scrape_and_extract >> task_transform_gold >> task_upload_minio >> task_dq_check
