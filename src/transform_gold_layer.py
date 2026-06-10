"""
transform_gold_layer.py
========================
Skrip ini adalah representasi dari Tahap "Transform" dalam arsitektur ETL (Extract, Transform, Load).
Konsep Medallion Architecture:
- Bronze Layer: Data mentah PDF & Markdown.
- Silver Layer: 8 kepingan file CSV yang sudah kita bersihkan dari teks (seperti ipm.csv, kemiskinan.csv).
- Gold Layer: Penggabungan (JOIN) seluruh file Silver menjadi satu Tabel Master yang siap divisualisasikan oleh BI Tools (Grafana / Tableau).

Skrip ini membaca 8 file CSV di data/hasil_ekstraksi, melakukan OUTER JOIN berdasarkan kunci "provinsi", "tahun", dan "periode", lalu membuang data duplikat agar siap pakai.
"""

import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def transform_to_gold_layer(silver_dir: str, gold_file: str):
    folder = Path(silver_dir)
    if not folder.exists():
        logging.error(f"Folder {silver_dir} tidak ditemukan.")
        return

    csv_files = list(folder.glob("*.csv"))
    if not csv_files:
        logging.warning("Tidak ada file CSV di folder Silver Layer.")
        return

    logging.info(f"Memulai penggabungan (JOIN) {len(csv_files)} file CSV...")

    # Dataframe utama untuk menampung gabungan
    master_df = None

    for file_path in csv_files:
        # Load setiap CSV
        try:
            df = pd.read_csv(file_path)
            logging.info(f" - Membaca {file_path.name} ({len(df)} baris)")
        except pd.errors.EmptyDataError:
            logging.warning(f" - {file_path.name} kosong, melewati.")
            continue

        # Standarisasi kolom join keys agar huruf kecil semua dan bebas spasi berlebih
        if 'provinsi' in df.columns:
            df['provinsi'] = df['provinsi'].str.strip().str.title()
        
        # Kolom yang digunakan sebagai kunci penggabungan (Primary Key gabungan)
        join_keys = ['provinsi', 'tahun']
        if 'periode' in df.columns:
            join_keys.append('periode')
            # Isi NaN periode dengan string kosong agar tidak error saat di-join
            df['periode'] = df['periode'].fillna("Tahunan")
        else:
            # Jika file tidak punya periode (misal IPM yang sifatnya tahunan mutlak), tambahkan kolom periode "Tahunan"
            df['periode'] = "Tahunan"
            join_keys.append('periode')

        # Hapus kolom meta yang tidak perlu ada di Gold Layer secara berulang
        for col in ['source_file', 'page_number', 'extraction_ts']:
            if col in df.columns:
                df = df.drop(columns=[col])

        # Hapus duplikat baris (jika ada file PDF yang ditarik 2x)
        df = df.drop_duplicates(subset=join_keys, keep='last')

        if master_df is None:
            master_df = df
        else:
            # Lakukan FULL OUTER JOIN
            master_df = pd.merge(master_df, df, on=join_keys, how='outer')

    if master_df is not None:
        # Rapihkan susunan kolom (provinsi, tahun, periode di depan)
        cols = master_df.columns.tolist()
        first_cols = ['provinsi', 'tahun', 'periode']
        other_cols = [c for c in cols if c not in first_cols]
        # Wait, fix typo in previous logic, python syntax correction
        other_cols = [c for c in cols if c not in first_cols]
        master_df = master_df[first_cols + other_cols]

        # Urutkan data berdasarkan Provinsi dan Tahun
        master_df = master_df.sort_values(by=['provinsi', 'tahun', 'periode']).reset_index(drop=True)

        # IMPUTASI (Data Aggregation & Cleansing for Gold Layer):
        # Mengisi nilai kosong (NaN) menggunakan metode Forward Fill (ffill) 
        # lalu Backward Fill (bfill) dikelompokkan per provinsi.
        master_df[other_cols] = master_df.groupby('provinsi')[other_cols].ffill()
        master_df[other_cols] = master_df.groupby('provinsi')[other_cols].bfill()
        
        master_df.to_csv(gold_file, index=False)
        logging.info(f"Sukses! Gold Layer berhasil dibuat di {gold_file} dengan total {len(master_df)} baris dan {len(master_df.columns)} kolom indikator.")
    else:
        logging.error("Gagal membuat Gold Layer, tidak ada data yang diproses.")

if __name__ == "__main__":
    silver_folder = "data/hasil_ekstraksi"
    gold_filepath = "data/gold_kesenjangan_regional_master.csv"
    transform_to_gold_layer(silver_folder, gold_filepath)
