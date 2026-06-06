"""
upload_to_minio.py
==================
Skrip ini mengunggah file-file hasil pipeline ke MinIO Object Storage,
sehingga data dapat dilihat melalui Web UI MinIO di http://localhost:9001.

Fungsi:
  - Mengunggah file PDF & Markdown ke bucket "bronze"
  - Mengunggah 8 file CSV terpisah ke bucket "silver"
  - Mengunggah Master CSV (Gold Layer) ke bucket "gold"

Dependensi:
  pip install minio
"""

import os
import logging
from pathlib import Path
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# Konfigurasi koneksi MinIO
# Di lokal (Windows/WSL): localhost:9000
# Di dalam Docker Airflow: nama kontainer MinIO (minio-datalake:9000)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

# Mapping folder lokal -> bucket MinIO
LAYER_MAP = {
    "bronze": {
        "description": "File mentah (PDF & Markdown)",
        "extensions": [".pdf", ".md"],
        "source_dirs": [],  # Akan diisi dinamis dari folder data/<Provinsi>/
    },
    "silver": {
        "description": "8 file CSV terstruktur",
        "extensions": [".csv"],
        "source_dirs": ["data/hasil_ekstraksi"],
    },
    "gold": {
        "description": "Master CSV gabungan",
        "extensions": [".csv"],
        "source_dirs": ["data"],
    },
}


def get_minio_client() -> Minio:
    """Membuat koneksi ke MinIO server."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,  # Tidak pakai HTTPS untuk lokal
    )
    # Kita comment log ini agar tidak spamming saat real-time upload
    # log.info("Terhubung ke MinIO di %s", MINIO_ENDPOINT)
    return client

# --- FUNGSI UNTUK REAL-TIME UPLOAD ---

_CLIENT = None
def _get_client_singleton():
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = get_minio_client()
        # Pastikan bucket sudah ada
        for bucket in ("bronze", "silver", "gold"):
            if not _CLIENT.bucket_exists(bucket):
                _CLIENT.make_bucket(bucket)
    return _CLIENT

def upload_single_file(bucket: str, object_name: str, file_path: Path):
    """Fungsi pembantu untuk mengunggah satu file ke MinIO secara real-time."""
    if not file_path.exists():
        return
    client = _get_client_singleton()
    try:
        client.fput_object(bucket, object_name, str(file_path))
        log.info("  [MinIO] Tersinkronisasi: %s -> bucket '%s'", object_name, bucket)
    except Exception as e:
        log.warning("  [MinIO] Gagal sinkronisasi %s: %s", object_name, e)


def upload_bronze(client: Minio):
    """Upload file PDF dan Markdown dari setiap folder provinsi ke bucket bronze."""
    data_dir = Path("data")
    count = 0

    for provinsi_dir in sorted(data_dir.iterdir()):
        if not provinsi_dir.is_dir():
            continue
        if provinsi_dir.name in ("hasil_ekstraksi",):
            continue

        for file_path in provinsi_dir.iterdir():
            if file_path.suffix.lower() in (".pdf", ".md"):
                object_name = f"{provinsi_dir.name}/{file_path.name}"
                try:
                    client.fput_object("bronze", object_name, str(file_path))
                    count += 1
                except S3Error as e:
                    log.warning("  Gagal upload %s: %s", object_name, e)

    log.info("BRONZE: %d file berhasil diunggah", count)


def upload_silver(client: Minio):
    """Upload 8 file CSV dari folder hasil_ekstraksi ke bucket silver."""
    silver_dir = Path("data/hasil_ekstraksi")
    count = 0

    if not silver_dir.exists():
        log.warning("Folder %s tidak ditemukan, lewati Silver Layer.", silver_dir)
        return

    for csv_file in sorted(silver_dir.glob("*.csv")):
        object_name = csv_file.name
        try:
            client.fput_object("silver", object_name, str(csv_file))
            count += 1
        except S3Error as e:
            log.warning("  Gagal upload %s: %s", object_name, e)

    log.info("SILVER: %d file CSV berhasil diunggah", count)


def upload_gold(client: Minio):
    """Upload Master CSV Gold Layer ke bucket gold."""
    gold_files = list(Path("data").glob("gold_*.csv"))
    count = 0

    if not gold_files:
        log.warning("File Gold Layer belum ada. Jalankan transform_gold_layer.py terlebih dahulu.")
        return

    for gold_file in gold_files:
        try:
            client.fput_object("gold", gold_file.name, str(gold_file))
            count += 1
        except S3Error as e:
            log.warning("  Gagal upload %s: %s", gold_file.name, e)

    log.info("GOLD: %d file Master CSV berhasil diunggah", count)


def main():
    log.info("=" * 60)
    log.info("UPLOAD DATA KE MINIO (Data Lake)")
    log.info("=" * 60)

    client = get_minio_client()

    # Pastikan bucket sudah ada (seharusnya sudah dibuat oleh minio-init)
    for bucket in ("bronze", "silver", "gold"):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            log.info("Bucket '%s' dibuat.", bucket)

    log.info("")
    log.info("--- Mengunggah Bronze Layer (PDF & Markdown) ---")
    upload_bronze(client)

    log.info("")
    log.info("--- Mengunggah Silver Layer (8 CSV Terstruktur) ---")
    upload_silver(client)

    log.info("")
    log.info("--- Mengunggah Gold Layer (Master CSV) ---")
    upload_gold(client)

    log.info("")
    log.info("=" * 60)
    log.info("SELESAI! Buka http://localhost:9001 untuk melihat data.")
    log.info("Login: minioadmin / minioadmin")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
