"""
mineru_parser.py
================
Ekstraksi indikator ketimpangan dari PDF Laporan BI menggunakan MinerU API.

Alur:
  1. Start HTTP Server lokal & ekspos via Cloudflare Tunnel
  2. Kirim URL publik ke MinerU API → dapat task_id
  3. Poll sampai selesai → download ZIP hasil
  4. Parse full.md dengan regex
  5. Simpan ke CSV (sama format seperti parser_pdf.py)
"""

import re
import csv
import io
import json
import logging
import time
import zipfile
import threading
import http.server
import subprocess
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests
import upload_to_minio
import urllib3

urllib3.disable_warnings()
log = logging.getLogger(__name__)

# ============================================================================
# KONFIGURASI
# ============================================================================

MINERU_TOKEN = (
    "eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFM1MTIifQ"
    ".eyJqdGkiOiI5OTAwMDU0MyIsInJvbCI6IlJPTEVfUkVHSVNURVIiLCJpc3MiOiJPcGVuWExhYiIsImlhdCI6MTc3ODc3NjU3MiwiY2xpZW50SWQiOiJsa3pkeDU3bnZ5MjJqa3BxOXgydyIsInBob25lIjoiIiwib3BlbklkIjpudWxsLCJ1dWlkIjoiMzE1OTRiNTAtMTI5OS00ZmQyLWI1MmYtMjAzMTY2ZGViNTYwIiwiZW1haWwiOiIiLCJleHAiOjE3ODY1NTI1NzJ9"
    ".7LeTkA52giSE1iStJpeP-HZFeW3C50hLWn_w4ii0PGKvbyjXIsxw0rjlj96Albs6C5A_dnq503Sq0jqlI2dOPA"
)
MINERU_BASE = "https://mineru.net/api/v4"
MINERU_HEADERS = {
    "Authorization": f"Bearer {MINERU_TOKEN}",
    "Content-Type": "application/json",
}
PORT = 8765
POLL_INTERVAL = 10
POLL_TIMEOUT  = 600

# ============================================================================
# TEMPORARY FILE UPLOAD (CATBOX)
# ============================================================================

def upload_ke_catbox(pdf_path: Path) -> str:
    log.info("  [CATBOX] Mengunggah %s ke Catbox.moe...", pdf_path.name)
    with open(pdf_path, 'rb') as f:
        files = {
            'reqtype': (None, 'fileupload'),
            'fileToUpload': (pdf_path.name, f, 'application/pdf')
        }
        r = requests.post("https://catbox.moe/user/api.php", files=files, timeout=300)
        r.raise_for_status()
        url = r.text.strip()
        log.info("  [CATBOX] Public URL: %s", url)
        return url

# ============================================================================
# MINERU API
# ============================================================================

def submit_task(pdf_url: str) -> str:
    r = requests.post(
        f"{MINERU_BASE}/extract/task",
        headers=MINERU_HEADERS,
        json={"url": pdf_url, "model_version": "vlm"},
        timeout=30,
        verify=False,
    )
    r.raise_for_status()
    resp = r.json()
    if resp.get("code") != 0:
        raise RuntimeError(f"MinerU submit gagal: {resp}")
    task_id = resp["data"]["task_id"]
    log.info("  [MINERU] Task submitted: %s", task_id)
    return task_id

def poll_task(task_id: str, timeout: int = 1800) -> str:
    log.info("  [MINERU] Polling task %s...", task_id)
    elapsed = 0
    while elapsed < timeout:
        r = requests.get(
            f"{MINERU_BASE}/extract/task/{task_id}",
            headers=MINERU_HEADERS,
            timeout=15,
            verify=False,
        )
        r.raise_for_status()
        resp = r.json()
        data = resp.get("data", {})
        state = data.get("state", "unknown")
        log.info("  [MINERU] State: %s (%ds elapsed)", state, elapsed)

        if state == "done":
            return data["full_zip_url"]
        elif state in ("failed", "error"):
            raise RuntimeError(f"Task gagal: {data.get('err_msg')}")

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    raise TimeoutError(f"Task {task_id} timeout")

def download_markdown(zip_url: str) -> str:
    log.info("  [MINERU] Downloading result ZIP...")
    r = requests.get(zip_url, timeout=120, verify=False)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        if "full.md" in z.namelist():
            md = z.read("full.md").decode("utf-8", errors="replace")
            log.info("  [MINERU] Markdown OK, %d karakter", len(md))
            return md
        raise RuntimeError(f"full.md tidak ada di ZIP.")

# ============================================================================
# PARSING & CSV
# ============================================================================

def _to_float(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return float(str(s).replace(",", ".").replace(" ", "").strip())
    except (ValueError, TypeError):
        return None

def _find(pattern: re.Pattern, text: str) -> str | None:
    m = pattern.search(text)
    return m.group(1) if m else None

# Regex patterns sama seperti sebelumnya
RE_MD_GINI_PROV = re.compile(r"[Gg]ini\s*[Rr]atio\s+(?:Provinsi|[A-Z][a-zA-Z\s]+)?.{0,120}?(0[,\.]\d{2,3})", re.DOTALL)
RE_MD_GINI_KOTA = re.compile(r"[Gg]ini.{0,30}?[Pp]erkotaan.{0,60}?(0[,\.]\d{2,3})", re.DOTALL)
RE_MD_GINI_DESA = re.compile(r"[Gg]ini.{0,30}?[Pp]er[ds]e[sd]aan.{0,60}?(0[,\.]\d{2,3})", re.DOTALL)

RE_MD_PERSEN = re.compile(r"(?:persentase|tingkat)\s+(?:penduduk\s+)?miskin.{0,80}?(\d+[,\.]\d+)\s*%", re.IGNORECASE | re.DOTALL)
RE_MD_PERSEN2 = re.compile(r"(\d+[,\.]\d+)\s*%\s+(?:penduduk\s+)?miskin", re.IGNORECASE)
RE_MD_JUMLAH = re.compile(r"penduduk\s+miskin.{0,100}?(\d+[,\.]\d+)\s*(?:ribu|juta)\s*(?:orang|jiwa)?", re.IGNORECASE | re.DOTALL)
RE_MD_GARIS = re.compile(r"[Gg]aris\s+[Kk]emiskinan.{0,80}?Rp\s*\.?\s*([\d.]+(?:[,\.]\d+)?)", re.IGNORECASE | re.DOTALL)
RE_MD_P1 = re.compile(r"[Ii]ndeks\s+[Kk]edalaman\s+[Kk]emiskinan\s*(?:\(P1\))?\s*(?:sebesar|:)?\s*([\d,\.]+)", re.IGNORECASE)
RE_MD_P2 = re.compile(r"[Ii]ndeks\s+[Kk]eparahan\s+[Kk]emiskinan\s*(?:\(P2\))?\s*(?:sebesar|:)?\s*([\d,\.]+)", re.IGNORECASE)

RE_MD_IPM = re.compile(r"IPM\s+(?:Provinsi\s+)?(?:[A-Z][a-zA-Z\s]+)?.{0,100}?(?:menjadi|sebesar|mencapai|tercatat)\s*([\d,\.]+)", re.IGNORECASE | re.DOTALL)
RE_MD_IPM2 = re.compile(r"[Ii]ndeks\s+[Pp]embangunan\s+[Mm]anusia.{0,100}?(?:menjadi|sebesar|mencapai|tercatat)\s*([\d,\.]+)", re.IGNORECASE | re.DOTALL)
RE_MD_PENGELUARAN = re.compile(r"pengeluaran\s+(?:riil\s+)?per\s*kapita.{0,60}?Rp\s*\.?\s*([\d.]+(?:[,\.]\d+)?)\s*(?:juta|ribu)", re.IGNORECASE | re.DOTALL)

RE_MD_TPT = re.compile(r"[Tt]ingkat\s+[Pp]engangguran\s+[Tt]erbuka\s*(?:\(TPT\))?.{0,80}?(\d+[,\.]\d+)\s*%", re.IGNORECASE | re.DOTALL)
RE_MD_TPAK = re.compile(r"[Tt]ingkat\s+[Pp]artisipasi\s+[Aa]ngkatan\s+[Kk]erja\s*(?:\(TPAK\))?.{0,80}?(\d+[,\.]\d+)\s*%", re.IGNORECASE | re.DOTALL)
RE_MD_TPT_SHORT = re.compile(r"TPT.{0,30}?(\d+[,\.]\d+)\s*%", re.IGNORECASE)
RE_MD_TPAK_SHORT = re.compile(r"TPAK.{0,30}?(\d+[,\.]\d+)\s*%", re.IGNORECASE)

RE_MD_PANGSA_40 = re.compile(r"40\s*%\s*(?:penduduk\s+)?(?:ber)?pengeluaran\s+terendah.{0,100}?(\d+[,\.]\d+)\s*%", re.IGNORECASE | re.DOTALL)
RE_MD_PANGSA_40B = re.compile(r"(\d+[,\.]\d+)\s*%\s*(?:dari\s+)?(?:total\s+)?pengeluaran.{0,60}?(?:40\s*%\s*(?:penduduk\s+)?(?:ter)?bawah|kelompok\s+bawah)", re.IGNORECASE | re.DOTALL)

RE_MD_PERIODE = re.compile(r"(?:Maret|September|Agustus|Februari)\s+\d{4}", re.IGNORECASE)

# --- Tambahan Makroekonomi ---
RE_MD_INFLASI = re.compile(r"inflasi.{0,60}?(?:sebesar|mencapai|tercatat|yakni).{0,20}?(\d+[,\.]\d+)\s*%", re.IGNORECASE | re.DOTALL)
RE_MD_PDRB = re.compile(r"(?:pertumbuhan\s+ekonomi|perekonomian\s+tumbuh|ekonomi\s+tumbuh).{0,60}?(?:sebesar|mencapai|tercatat|sebanyak)?.{0,20}?(\d+[,\.]\d+)\s*%", re.IGNORECASE | re.DOTALL)
RE_MD_NTP = re.compile(r"(?:Nilai\s+Tukar\s+Petani|NTP).{0,60}?(?:sebesar|menjadi|mencapai|tercatat).{0,20}?(\d{2,3}[,\.]\d+)", re.IGNORECASE | re.DOTALL)

def parse_markdown(md: str, provinsi: str, tahun: int, out_dir: Path, source_file: str) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    ringkasan = {"gini": 0, "kemiskinan": 0, "ipm": 0, "distribusi": 0, "ketenagakerjaan": 0, "inflasi": 0, "pdrb": 0, "ntp": 0}
    ts = datetime.now().isoformat()
    base_meta = {"provinsi": provinsi, "tahun": tahun, "source_file": source_file, "extraction_ts": ts, "page_number": "N/A (MinerU)"}

    m_prd = RE_MD_PERIODE.search(md)
    periode = m_prd.group(0) if m_prd else ""

    gini_kota = _to_float(_find(RE_MD_GINI_KOTA, md))
    gini_desa = _to_float(_find(RE_MD_GINI_DESA, md))
    gini_prov = _to_float(_find(RE_MD_GINI_PROV, md))
    if gini_prov == gini_kota or gini_prov == gini_desa: gini_prov = None
    if gini_kota or gini_desa or gini_prov:
        _append_csv(out_dir / "gini_ratio.csv", "gini_ratio", {**base_meta, "periode": periode, "gini_total": gini_prov, "gini_kota": gini_kota, "gini_desa": gini_desa})
        ringkasan["gini"] = 1

    persen = _to_float(_find(RE_MD_PERSEN, md) or _find(RE_MD_PERSEN2, md))
    if persen and persen >= 30: persen = None
    jumlah = _to_float(_find(RE_MD_JUMLAH, md))
    garis = _to_float(_find(RE_MD_GARIS, md))
    p1 = _to_float(_find(RE_MD_P1, md))
    p2 = _to_float(_find(RE_MD_P2, md))
    if p1 and p1 >= 5: p1 = None
    if p2 and p2 >= 2: p2 = None
    if persen is not None:
        m_prd2 = re.search(r"(?:Maret|September)\s+\d{4}", md, re.IGNORECASE)
        prd_kemiskinan = m_prd2.group(0) if m_prd2 else periode
        _append_csv(out_dir / "kemiskinan.csv", "kemiskinan", {**base_meta, "periode": prd_kemiskinan, "persen_miskin": persen, "jumlah_miskin_ribu": jumlah, "garis_kemiskinan_rp": garis, "p1_kedalaman": p1, "p2_keparahan": p2})
        ringkasan["kemiskinan"] = 1

    ipm_val = _to_float(_find(RE_MD_IPM, md) or _find(RE_MD_IPM2, md))
    if ipm_val and 50 < ipm_val < 95:
        pengeluaran = _to_float(_find(RE_MD_PENGELUARAN, md))
        _append_csv(out_dir / "ipm.csv", "ipm", {**base_meta, "ipm_provinsi": ipm_val, "pengeluaran_per_kapita_juta": pengeluaran})
        ringkasan["ipm"] = 1

    pangsa_40 = _to_float(_find(RE_MD_PANGSA_40, md) or _find(RE_MD_PANGSA_40B, md))
    if pangsa_40 and 5 < pangsa_40 < 50:
        _append_csv(out_dir / "distribusi_pendapatan.csv", "distribusi_pendapatan", {**base_meta, "periode": periode, "pangsa_40_terbawah": pangsa_40, "pangsa_kota": None, "pangsa_desa": None})
        ringkasan["distribusi"] = 1

    tpt = _to_float(_find(RE_MD_TPT, md) or _find(RE_MD_TPT_SHORT, md))
    tpak = _to_float(_find(RE_MD_TPAK, md) or _find(RE_MD_TPAK_SHORT, md))
    if tpt is not None or tpak is not None:
        m_prd3 = re.search(r"(?:Agustus|Februari)\s+\d{4}", md, re.IGNORECASE)
        prd_naker = m_prd3.group(0) if m_prd3 else periode
        _append_csv(out_dir / "ketenagakerjaan.csv", "ketenagakerjaan", {**base_meta, "periode": prd_naker, "tpak": tpak, "tpt": tpt, "tpt_kota": None, "tpt_desa": None})
        ringkasan["ketenagakerjaan"] = 1

    # ---------- MAKROEKONOMI ----------
    inflasi = _to_float(_find(RE_MD_INFLASI, md))
    if inflasi is not None:
        _append_csv(out_dir / "inflasi.csv", "inflasi", {**base_meta, "inflasi_persen": inflasi})
        ringkasan["inflasi"] = 1
        
    pdrb = _to_float(_find(RE_MD_PDRB, md))
    if pdrb is not None:
        _append_csv(out_dir / "pdrb.csv", "pdrb", {**base_meta, "pertumbuhan_ekonomi_persen": pdrb})
        ringkasan["pdrb"] = 1
        
    ntp = _to_float(_find(RE_MD_NTP, md))
    if ntp is not None:
        _append_csv(out_dir / "ntp.csv", "ntp", {**base_meta, "nilai_tukar_petani": ntp})
        ringkasan["ntp"] = 1

    log.info("  [MINERU PARSE] Ringkasan: %s", ringkasan)
    return ringkasan

SCHEMAS = {
    "gini_ratio": ["provinsi", "tahun", "periode", "gini_total", "gini_kota", "gini_desa", "source_file", "page_number", "extraction_ts"],
    "kemiskinan": ["provinsi", "tahun", "periode", "persen_miskin", "jumlah_miskin_ribu", "garis_kemiskinan_rp", "p1_kedalaman", "p2_keparahan", "source_file", "page_number", "extraction_ts"],
    "ipm": ["provinsi", "tahun", "ipm_provinsi", "pengeluaran_per_kapita_juta", "source_file", "page_number", "extraction_ts"],
    "distribusi_pendapatan": ["provinsi", "tahun", "periode", "pangsa_40_terbawah", "pangsa_kota", "pangsa_desa", "source_file", "page_number", "extraction_ts"],
    "ketenagakerjaan": ["provinsi", "tahun", "periode", "tpak", "tpt", "tpt_kota", "tpt_desa", "source_file", "page_number", "extraction_ts"],
    "inflasi": ["provinsi", "tahun", "inflasi_persen", "source_file", "page_number", "extraction_ts"],
    "pdrb": ["provinsi", "tahun", "pertumbuhan_ekonomi_persen", "source_file", "page_number", "extraction_ts"],
    "ntp": ["provinsi", "tahun", "nilai_tukar_petani", "source_file", "page_number", "extraction_ts"],
}

def _append_csv(csv_path: Path, schema_name: str, row: dict) -> None:
    fieldnames = SCHEMAS[schema_name]
    write_header = not csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header: writer.writeheader()
        writer.writerow(row)
    
    # Real-time sync to MinIO Silver Layer
    upload_to_minio.upload_single_file("silver", csv_path.name, csv_path)

# ============================================================================
# MAIN
# ============================================================================

def parse_via_mineru(pdf_path: Path, provinsi: str, tahun: int, out_dir: Path) -> dict:
    try:
        # 1. Upload ke file host sementara
        pdf_url = upload_ke_catbox(pdf_path)

        # 2. Kirim URL ke MinerU
        task_id = submit_task(pdf_url)
        zip_url = poll_task(task_id)
        md = download_markdown(zip_url)
        
        # Save MD backup and delete PDF
        md_file = pdf_path.with_suffix(".md")
        md_file.write_text(md, encoding="utf-8")
        
        # Real-time sync MD to MinIO Bronze Layer
        md_object_name = f"{pdf_path.parent.name}/{md_file.name}"
        upload_to_minio.upload_single_file("bronze", md_object_name, md_file)

        try:
            pdf_path.unlink(missing_ok=True)
            log.info("  [CLEANUP] Deleted original PDF: %s", pdf_path.name)
        except Exception as e:
            log.warning("  [CLEANUP] Failed to delete PDF: %s", e)

        return parse_markdown(md, provinsi, tahun, out_dir, pdf_path.name)
    except Exception as exc:
        log.error("  [MINERU] Gagal parse %s: %s", pdf_path.name, exc)
        return {"gini": 0, "kemiskinan": 0, "ipm": 0, "distribusi": 0, "ketenagakerjaan": 0}

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    if len(sys.argv) < 4:
        print("Usage: python src/mineru_parser.py <pdf_path> <provinsi> <tahun>")
        sys.exit(1)
    
    pdf = Path(sys.argv[1])
    prov = sys.argv[2]
    thn = int(sys.argv[3])
    out = Path("data/hasil_ekstraksi")
    
    res = parse_via_mineru(pdf, prov, thn, out)
    print(f"Result: {res}")
