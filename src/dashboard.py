import streamlit as st
import pandas as pd
import plotly.express as px
import io
from minio import Minio
from pathlib import Path

# Set konfigurasi halaman
st.set_page_config(
    page_title="Dashboard Kesenjangan Regional BI",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Judul Dashboard
st.title("Dashboard Analisis Kesenjangan Regional Indonesia")
st.markdown("Visualisasi sederhana dari **Gold Layer** (Master Table) yang diambil langsung dari **MinIO Data Lake**.")

# Load Data dari MinIO
@st.cache_data(ttl=600) # Cache selama 10 menit
def load_data():
    try:
        # Konfigurasi Koneksi MinIO
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        
        # Nama bucket dan object
        bucket_name = "gold"
        object_name = "gold_kesenjangan_regional_master.csv"
        
        # Menarik data dari MinIO Object Storage
        response = client.get_object(bucket_name, object_name)
        
        # Membaca bytestream langsung ke Pandas DataFrame tanpa menyimpannya ke disk
        df = pd.read_csv(io.BytesIO(response.read()))
        
        # Bersihkan koneksi
        response.close()
        response.release_conn()
        
        return df

    except Exception as e:
        st.sidebar.error(f"Gagal mengambil dari MinIO: {e}")
        
        # Fallback ke penyimpanan lokal jika server MinIO belum siap
        gold_path = Path("data/gold_kesenjangan_regional_master.csv")
        if gold_path.exists():
            st.sidebar.warning("MinIO tidak terjangkau. Membaca dari data lokal (Fallback).")
            return pd.read_csv(gold_path)
            
        return None

df = load_data()

if df is not None:
    # --- SIDEBAR: Filter Data ---
    st.sidebar.header("Filter Data")
    
    # Filter Provinsi
    all_provinsi = sorted(df['provinsi'].dropna().unique())
    selected_prov = st.sidebar.multiselect(
        "Pilih Provinsi:",
        options=all_provinsi,
        default=all_provinsi[:3] if len(all_provinsi) >= 3 else all_provinsi
    )
    
    # Terapkan Filter
    if not selected_prov:
        st.warning("Silakan pilih setidaknya satu provinsi di sidebar.")
        filtered_df = df
    else:
        filtered_df = df[df['provinsi'].isin(selected_prov)]
    
    # --- TAMPILAN METRIK (Data Keseluruhan Filtered) ---
    st.subheader(f"Ringkasan Data untuk {len(selected_prov)} Provinsi")
    
    # Tampilkan tabel data
    with st.expander("Lihat Data Master Tabel (Gold Layer)", expanded=False):
        st.dataframe(filtered_df, use_container_width=True)
    
    # --- VISUALISASI ---
    st.divider()
    st.subheader("Tren Indikator Ekonomi")
    
    # Pilihan Indikator untuk di-plot
    numeric_cols = filtered_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    if 'tahun' in numeric_cols:
        numeric_cols.remove('tahun')
        
    if not numeric_cols:
         st.info("Tidak ada kolom numerik untuk divisualisasikan.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            selected_indicator1 = st.selectbox("Pilih Indikator 1:", options=numeric_cols, index=0)
            fig1 = px.line(
                filtered_df, x='tahun', y=selected_indicator1, color='provinsi',
                markers=True, title=f"Tren {selected_indicator1.title()} per Tahun"
            )
            st.plotly_chart(fig1, use_container_width=True)
            
        with col2:
            if len(numeric_cols) > 1:
                selected_indicator2 = st.selectbox("Pilih Indikator 2:", options=numeric_cols, index=1)
            else:
                selected_indicator2 = selected_indicator1
            fig2 = px.bar(
                filtered_df, x='provinsi', y=selected_indicator2, color='tahun',
                barmode='group', title=f"Perbandingan {selected_indicator2.title()} Antar Provinsi"
            )
            st.plotly_chart(fig2, use_container_width=True)
            
        # Scatter Plot untuk korelasi
        st.divider()
        st.subheader("Korelasi Antar Indikator")
        col3, col4 = st.columns(2)
        with col3:
            scat_x = st.selectbox("Sumbu X:", options=numeric_cols, index=0)
        with col4:
            scat_y = st.selectbox("Sumbu Y:", options=numeric_cols, index=1 if len(numeric_cols)>1 else 0)
            
        fig_scatter = px.scatter(
            filtered_df, x=scat_x, y=scat_y, color='provinsi', hover_data=['tahun'],
            title=f"Korelasi {scat_x.title()} vs {scat_y.title()}"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

else:
    st.error("File Master Data (Gold Layer) tidak ditemukan!")
    st.info("Pastikan Anda sudah menjalankan Airflow pipeline atau script `transform_gold_layer.py` untuk menghasilkan `data/gold_kesenjangan_regional_master.csv`.")
