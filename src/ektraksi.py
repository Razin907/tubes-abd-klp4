"""
ektraksi.py
===========
Scraper otomatis Laporan Perekonomian Provinsi (LPP) / Kajian Ekonomi Regional
(KER) dari situs Bank Indonesia (bi.go.id).

Arsitektur (berdasarkan inspeksi halaman nyata):
  1. Halaman listing   : https://www.bi.go.id/id/publikasi/laporan/lpp/
                          default.aspx?kategori=<slug-provinsi>&periode=
     -> Berisi daftar link ke halaman DETAIL setiap laporan (/Pages/...)
  2. Halaman detail    : /id/publikasi/laporan/lpp/Pages/<nama>.aspx
     -> Berisi link unduhan PDF yang sebenarnya
  3. Download PDF      : requests + stream, jeda acak anti-bot

Filter Nomenklatur:
  Mendukung: LPP, KEKR, Kajian Ekonomi & Keuangan Regional, Kajian Ekonomi
  Regional, untuk rentang tahun 2015-2024.

Dependensi:
    pip install selenium webdriver-manager requests tqdm

Cara pakai:
    python src/ektraksi.py

Output:
    PDF   -> data/<provinsi>/<nama-file>.pdf
    Log   -> data/log_ekstraksi.csv
"""

import sys
import re
import csv
import time
import random
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, quote_plus

import requests
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from webdriver_manager.chrome import ChromeDriverManager

from mineru_parser import parse_via_mineru
import upload_to_minio

sys.stdout.reconfigure(encoding="utf-8")

# =============================================================================
# KONFIGURASI
# =============================================================================

BASE_URL  = "https://www.bi.go.id"
LIST_URL  = BASE_URL + "/id/publikasi/laporan/lpp/default.aspx"

TAHUN_MULAI   = 2015
TAHUN_SELESAI = 2024

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data"
CSV_DIR    = OUTPUT_DIR / "hasil_ekstraksi"

JEDA_MIN = 8    # detik, jeda antar-unduhan PDF
JEDA_MAX = 15
PAGE_WAIT = 8   # detik tunggu halaman render
TIMEOUT   = 20  # detik tunggu elemen Selenium
MAX_RETRY = 3

# Daftar slug provinsi (sesuai nilai parameter ?kategori= di URL BI)
# Nilai harus lowercase, spasi diganti +/spasi (akan di-encode otomatis)
PROVINSI_MAP = {
    "Aceh":                      "aceh",
    "Sumatera Utara":            "sumatera utara",
    "Sumatera Barat":            "sumatera barat",
    "Riau":                      "riau",
    "Jambi":                     "jambi",
    "Sumatera Selatan":          "sumatera selatan",
    "Bengkulu":                  "bengkulu",
    "Lampung":                   "lampung",
    "Kepulauan Bangka Belitung": "bangka belitung",
    "Kepulauan Riau":            "kepulauan riau",
    "DKI Jakarta":               "dki jakarta",
    "Jawa Barat":                "jawa barat",
    "Jawa Tengah":               "jawa tengah",
    "DI Yogyakarta":             "di yogyakarta",
    "Jawa Timur":                "jawa timur",
    "Banten":                    "banten",
    "Bali":                      "bali",
    "Nusa Tenggara Barat":       "nusa tenggara barat",
    "Nusa Tenggara Timur":       "nusa tenggara timur",
    "Kalimantan Barat":          "kalimantan barat",
    "Kalimantan Tengah":         "kalimantan tengah",
    "Kalimantan Selatan":        "kalimantan selatan",
    "Kalimantan Timur":          "kalimantan timur",
    "Kalimantan Utara":          "kalimantan utara",
    "Sulawesi Utara":            "sulawesi utara",
    "Sulawesi Tengah":           "sulawesi tengah",
    "Sulawesi Selatan":          "sulawesi selatan",
    "Sulawesi Tenggara":         "sulawesi tenggara",
    "Gorontalo":                 "gorontalo",
    "Sulawesi Barat":            "sulawesi barat",
    "Maluku":                    "maluku",
    "Maluku Utara":              "maluku utara",
    "Papua Barat":               "papua barat",
    "Papua":                     "papua",
}

# Pola tahun yang valid
TAHUN_VALID = set(range(TAHUN_MULAI, TAHUN_SELESAI + 1))

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def _setup_file_logging() -> None:
    """Tambahkan FileHandler setelah OUTPUT_DIR sudah ada."""
    fh = logging.FileHandler(OUTPUT_DIR / "log_scraper.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(fh)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def buat_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sanitasi(teks: str) -> str:
    """Hapus karakter ilegal dari nama file/folder."""
    return re.sub(r'[<>:"/\\|?*]', "_", teks).strip()


def tahun_dari_teks(teks: str) -> int | None:
    """Ekstrak tahun 4-digit dari teks; kembalikan None jika tidak ditemukan."""
    m = re.search(r"\b(20\d{2})\b", teks)
    if m:
        y = int(m.group(1))
        if y in TAHUN_VALID:
            return y
    return None


def simpan_log(log_path: Path, baris: dict) -> None:
    kolom = ["timestamp", "provinsi", "tahun", "judul", "detail_url", "pdf_url",
             "status", "path_file"]
    mode = "a" if log_path.exists() else "w"
    with open(log_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=kolom)
        if mode == "w":
            writer.writeheader()
        writer.writerow({k: baris.get(k, "") for k in kolom})


def unduh_pdf(url: str, path_tujuan: Path,
              session: requests.Session, max_retry: int = MAX_RETRY) -> bool:
    """Unduh PDF ke path_tujuan. Return True jika berhasil."""
    for attempt in range(1, max_retry + 1):
        try:
            resp = session.get(url, stream=True, timeout=60)
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "")
            if "html" in ct:
                log.warning("  Response adalah HTML bukan PDF: %s", url)
                return False
            total = int(resp.headers.get("Content-Length", 0))
            with open(path_tujuan, "wb") as f:
                if total:
                    for chunk in tqdm(resp.iter_content(65536),
                                      total=max(1, total // 65536),
                                      unit="KB", desc=path_tujuan.name[:40],
                                      leave=False):
                        f.write(chunk)
                else:
                    f.write(resp.content)
            if path_tujuan.stat().st_size < 1000:
                log.warning("  File terlalu kecil (%d bytes), mungkin error page.",
                            path_tujuan.stat().st_size)
                path_tujuan.unlink(missing_ok=True)
                return False
            log.info("  OK  Berhasil: %s (%.1f KB)",
                     path_tujuan.name, path_tujuan.stat().st_size / 1024)
            return True
        except requests.RequestException as exc:
            log.warning("  Percobaan %d/%d gagal: %s", attempt, max_retry, exc)
            if attempt < max_retry:
                time.sleep(random.uniform(5, 10))
    log.error("  GAGAL mengunduh setelah %d percobaan: %s", max_retry, url)
    return False


# =============================================================================
# SELENIUM SETUP
# =============================================================================

def buat_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    svc = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=svc, options=opts)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"},
    )
    return driver


# =============================================================================
# SCRAPING LOGIK
# =============================================================================

def url_listing(slug: str) -> str:
    """Bangun URL halaman listing untuk satu provinsi."""
    return f"{LIST_URL}?kategori={quote_plus(slug)}&periode="


def ambil_link_detail(driver: webdriver.Chrome, slug: str) -> list[dict]:
    """
    Buka halaman listing provinsi dan kumpulkan semua link halaman detail
    laporan (pola: /Pages/...) beserta tahun yang terdeteksi dari URL/teks.

    Return: list of dict {"judul": ..., "url": ..., "tahun": int}
            diurutkan berdasarkan tahun ascending (2015 -> 2024)
    """
    url = url_listing(slug)
    log.info("  Membuka listing: %s", url)
    driver.get(url)
    time.sleep(PAGE_WAIT)

    hasil = []
    seen = set()

    # Slug provinsi dalam berbagai format untuk validasi
    # Contoh: "aceh" -> ["aceh"], "sumatera utara" -> ["sumatera-utara", "sumatera_utara", "sumatera utara"]
    slug_lower = slug.lower()
    slug_variasi = {
        slug_lower,
        slug_lower.replace(" ", "-"),
        slug_lower.replace(" ", "_"),
        slug_lower.replace(" ", "+"),
    }

    # ------------------------------------------------------------------
    # LANGKAH 1: Isi filter tanggal agar semua tahun (2015-2024) muncul
    # ------------------------------------------------------------------
    try:
        wait = WebDriverWait(driver, TIMEOUT)

        # Tutup popup/cookie banner jika ada (klik tombol accept/close)
        for dismiss_sel in [
            "button[id*='accept']", "button[id*='cookie']", "button[id*='close']",
            ".cookie-accept", ".btn-accept", "#onetrust-accept-btn-handler",
            "button[aria-label='Close']", ".modal-close", "[data-dismiss='modal']",
        ]:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, dismiss_sel)
                if btn.is_displayed():
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(1)
                    log.info("  Popup/overlay ditutup (%s).", dismiss_sel)
                    break
            except (NoSuchElementException, Exception):
                pass

        # Isi TextBoxDateStart
        start_box = wait.until(EC.presence_of_element_located((By.ID, "TextBoxDateStart")))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", start_box)
        time.sleep(0.5)
        start_box.clear()
        start_box.send_keys(f"01/01/{TAHUN_MULAI}")

        # Isi TextBoxDateEnd
        end_box = driver.find_element(By.ID, "TextBoxDateEnd")
        end_box.clear()
        end_box.send_keys(f"31/12/{TAHUN_SELESAI}")

        # Klik tombol Filter via JavaScript (melewati overlay yang menghalangi)
        filter_btn = driver.find_element(
            By.XPATH, "//input[contains(@id,'ButtonFilter') or contains(@name,'ButtonFilter')]"
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", filter_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", filter_btn)
        log.info("  Filter tanggal %d-%d diterapkan, menunggu reload...", TAHUN_MULAI, TAHUN_SELESAI)
        time.sleep(PAGE_WAIT)

    except (TimeoutException, NoSuchElementException) as exc:
        log.warning("  Filter tanggal tidak ditemukan (%s), lanjut tanpa filter.", exc)
    except Exception as exc:
        log.warning("  Filter tanggal gagal (%s), lanjut tanpa filter.", exc)

    # ------------------------------------------------------------------
    # LANGKAH 2: Siapkan variasi slug + helper + loop pagination
    # ------------------------------------------------------------------

    def _kumpulkan_dari_halaman() -> int:
        """Ambil link dari halaman saat ini. Return jumlah link baru."""
        baru = 0
        try:
            elemen = driver.find_elements(
                By.XPATH, "//a[contains(@href,'/Pages/') or contains(@href,'/pages/')]"
            )
        except StaleElementReferenceException:
            time.sleep(2)
            elemen = driver.find_elements(
                By.XPATH, "//a[contains(@href,'/Pages/') or contains(@href,'/pages/')]"
            )

        for el in elemen:
            try:
                href = el.get_attribute("href") or ""
                if href.startswith("/"):
                    href = BASE_URL + href
                if not href or href in seen:
                    continue

                href_lower = href.lower()

                # Filter tipe laporan
                if not any(kw in href_lower for kw in [
                    "laporan-perekonomian", "kajian-ekonomi", "kekr", "ker-"
                ]):
                    continue

                # Filter provinsi: abaikan link yang tidak mengandung slug provinsi ini
                if not any(sv in href_lower for sv in slug_variasi):
                    log.debug("    Skip (bukan %s): %s", slug, href[-80:])
                    continue

                tahun = tahun_dari_teks(href)
                if tahun is None:
                    continue

                judul = el.text.strip()
                if not judul:
                    nama_slug = href.rstrip("/").split("/")[-1].replace(".aspx", "")
                    judul = nama_slug.replace("-", " ")

                seen.add(href)
                hasil.append({"judul": judul, "url": href, "tahun": tahun})
                baru += 1
            except StaleElementReferenceException:
                continue
        return baru

    # ------------------------------------------------------------------
    # LANGKAH 3: Loop semua halaman pagination via ASP.NET DataPager
    # ------------------------------------------------------------------
    page_num = 1
    while True:
        baru = _kumpulkan_dari_halaman()
        log.info("  Halaman listing %d: %d link baru (total: %d)", page_num, baru, len(hasil))

        # Strategi: baca nomor halaman aktif, klik link halaman berikutnya.
        # BI menggunakan ASP.NET DataPager dengan href="javascript:__doPostBack(...)"
        # sehingga harus diklik via JS bukan .click() biasa.
        next_clicked = False
        try:
            # Nomor halaman yang sedang aktif
            active_el = driver.find_element(By.CSS_SELECTOR, ".page-link--custom.active")
            current   = int(active_el.text.strip())
            next_num  = current + 1

            # Cari link halaman berikutnya (class='pagination-list', teks = nomor halaman)
            next_link = driver.find_element(
                By.XPATH,
                f"//a[contains(@class,'pagination-list') and normalize-space(text())='{next_num}']"
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_link)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", next_link)
            log.info("  Navigasi ke halaman %d...", next_num)
            time.sleep(PAGE_WAIT)
            page_num  += 1
            next_clicked = True

        except NoSuchElementException:
            # Tidak ada link nomor berikutnya -> sudah halaman terakhir
            pass
        except ValueError:
            # Teks active page bukan angka -> hentikan
            pass
        except Exception as exc:
            log.warning("  Navigasi pagination error: %s", exc)

        if not next_clicked:
            log.info("  Tidak ada halaman berikutnya. Total halaman: %d", page_num)
            break

    # ------------------------------------------------------------------
    # Urutkan berdasarkan tahun ascending (2015 -> 2024)
    # ------------------------------------------------------------------
    hasil.sort(key=lambda x: x["tahun"])

    log.info("  Total %d link detail ditemukan (tahun %d-%d).",
             len(hasil), TAHUN_MULAI, TAHUN_SELESAI)
    return hasil


def ambil_pdf_dari_detail(driver: webdriver.Chrome, detail_url: str) -> str | None:
    """
    Buka halaman detail laporan dan temukan URL PDF.
    Return URL PDF atau None jika tidak ditemukan.
    """
    log.debug("    Buka detail: %s", detail_url)
    driver.get(detail_url)
    time.sleep(random.uniform(3, 5))

    # Cari link PDF langsung
    for by, selector in [
        (By.XPATH, "//a[contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                   "'abcdefghijklmnopqrstuvwxyz'),'.pdf')]"),
        (By.XPATH, "//a[contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                   "'abcdefghijklmnopqrstuvwxyz'),'download')]"),
        (By.XPATH, "//a[contains(@class,'download') or contains(@class,'btn-download')]"),
    ]:
        try:
            els = driver.find_elements(by, selector)
            for el in els:
                href = el.get_attribute("href") or ""
                if href and not href.startswith("javascript"):
                    if href.startswith("/"):
                        href = BASE_URL + href
                    log.debug("    PDF ditemukan: %s", href)
                    return href
        except Exception:
            continue

    # Fallback: cari di page source via regex
    src = driver.page_source
    # Cari pola URL PDF dalam HTML
    matches = re.findall(
        r'href=["\']([^"\']*\.pdf[^"\']*)["\']', src, re.IGNORECASE
    )
    if matches:
        href = matches[0]
        if href.startswith("/"):
            href = BASE_URL + href
        log.debug("    PDF dari page source: %s", href)
        return href

    # Cari juga link dengan kata "unduh" atau "download" dalam teks
    try:
        unduh_els = driver.find_elements(
            By.XPATH,
            "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
            "'abcdefghijklmnopqrstuvwxyz'),'unduh') or "
            "contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
            "'abcdefghijklmnopqrstuvwxyz'),'download')]"
        )
        for el in unduh_els:
            href = el.get_attribute("href") or ""
            if href and not href.startswith("javascript"):
                if href.startswith("/"):
                    href = BASE_URL + href
                return href
    except Exception:
        pass

    log.warning("    Tidak ditemukan link PDF di: %s", detail_url)
    return None


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    buat_folder(OUTPUT_DIR)
    buat_folder(CSV_DIR)
    _setup_file_logging()

    log_csv = OUTPUT_DIR / "log_ekstraksi.csv"
    log.info("=" * 60)
    log.info("  SCRAPER LPP/KER BANK INDONESIA")
    log.info("  Mulai : %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("  Provinsi : %d | Tahun: %d-%d",
             len(PROVINSI_MAP), TAHUN_MULAI, TAHUN_SELESAI)
    log.info("  Output   : %s", OUTPUT_DIR)
    log.info("=" * 60)

    http = requests.Session()
    http.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/pdf,*/*",
        "Referer": BASE_URL,
    })

    driver = buat_driver(headless=True)  # ganti False untuk debug visual

    stat = {"total": 0, "ok": 0, "gagal": 0, "skip": 0}

    try:
        for nama_prov, slug in PROVINSI_MAP.items():
            folder = OUTPUT_DIR / sanitasi(nama_prov)
            buat_folder(folder)
            log.info("\n>>> Provinsi: %s (slug: %s)", nama_prov, slug)

            # Ambil semua link detail dari halaman listing
            detail_list = ambil_link_detail(driver, slug)

            if not detail_list:
                log.warning("  Tidak ada data untuk %s. Lewati.", nama_prov)
                continue

            for item in detail_list:
                judul    = item["judul"]
                det_url  = item["url"]
                tahun    = item["tahun"]

                stat["total"] += 1

                # Nama file: <provinsi>_<tahun>_<judul-pendek>.pdf
                nama_file = sanitasi(f"{nama_prov}_{tahun}_{judul[:70]}.pdf")
                path_file = folder / nama_file
                md_file = path_file.with_suffix(".md")

                if md_file.exists():
                    log.info("  SKIP (sudah diekstrak): %s", md_file.name)
                    stat["skip"] += 1
                    simpan_log(log_csv, {
                        "timestamp": datetime.now().isoformat(),
                        "provinsi": nama_prov, "tahun": tahun,
                        "judul": judul, "detail_url": det_url,
                        "pdf_url": "", "status": "sudah_diekstrak",
                        "path_file": str(md_file),
                    })
                    continue

                log.info("  [%d] %s | %s", tahun, judul[:60], det_url[-60:])

                pdf_url = ""
                ok = False
                
                if path_file.exists():
                    log.info("  [RETRY] PDF sudah ada, mencoba ekstrak ulang: %s", nama_file)
                    ok = True
                    status = "retry_parse"
                else:
                    # Kunjungi halaman detail untuk ambil link PDF
                    pdf_url = ambil_pdf_dari_detail(driver, det_url)
    
                    if not pdf_url:
                        stat["gagal"] += 1
                        simpan_log(log_csv, {
                            "timestamp": datetime.now().isoformat(),
                            "provinsi": nama_prov, "tahun": tahun,
                            "judul": judul, "detail_url": det_url,
                            "pdf_url": "", "status": "pdf_tidak_ditemukan",
                            "path_file": "",
                        })
                        continue
    
                    # Download PDF
                    ok = unduh_pdf(pdf_url, path_file, http)
                    status = "berhasil" if ok else "gagal_unduh"
                        
                if ok:
                    stat["ok"] += 1

                    # --- PARSE PDF: ekstrak indikator ketimpangan ---
                    try:
                        parse_via_mineru(path_file, nama_prov, tahun, CSV_DIR)
                    except Exception as parse_exc:
                        log.warning("  [PARSE] Error parsing %s: %s",
                                    path_file.name, parse_exc)
                else:
                    stat["gagal"] += 1
                    path_file.unlink(missing_ok=True)

                simpan_log(log_csv, {
                    "timestamp": datetime.now().isoformat(),
                    "provinsi": nama_prov, "tahun": tahun,
                    "judul": judul, "detail_url": det_url,
                    "pdf_url": pdf_url, "status": status,
                    "path_file": str(path_file) if ok else "",
                })

                # Jeda anti-bot
                jeda = random.uniform(JEDA_MIN, JEDA_MAX)
                log.info("  Jeda %.1f detik ...", jeda)
                time.sleep(jeda)

    except KeyboardInterrupt:
        log.info("\nDihentikan oleh pengguna (Ctrl+C).")

    finally:
        driver.quit()
        log.info("\n" + "=" * 60)
        log.info("  RINGKASAN")
        log.info("  Total diproses : %d", stat["total"])
        log.info("  Berhasil       : %d", stat["ok"])
        log.info("  Gagal          : %d", stat["gagal"])
        log.info("  Dilewati       : %d", stat["skip"])
        log.info("  Log CSV        : %s", log_csv)
        log.info("=" * 60)


if __name__ == "__main__":
    main()
