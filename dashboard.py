import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Konfigurasi Halaman
st.set_page_config(page_title="Dashboard Ekonomi Regional", page_icon="📊", layout="wide")

# CSS Kustom untuk tampilan premium
st.markdown("""
<style>
    .main {background-color: #f8f9fa;}
    h1, h2, h3 {color: #1f2937; font-weight: 600;}
    .stMetric {background-color: white; padding: 15px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border-left: 5px solid #1f77b4;}
</style>
""", unsafe_allow_html=True)

st.title("📊 Dashboard Kesenjangan Ekonomi Regional Indonesia")
st.markdown("Visualisasi interaktif data *Gold Layer* bersumber dari Laporan Perekonomian Provinsi (LPP) Bank Indonesia (2015-2024).")

@st.cache_data
def load_data():
    file_path = Path('data/gold_kesenjangan_regional_master.csv')
    if not file_path.exists():
        return pd.DataFrame()
    
    df = pd.read_csv(file_path)
    
    # Bersihkan / Rapikan Data (Data Cleaning)
    if 'provinsi' in df.columns:
        df['provinsi'] = df['provinsi'].str.strip().str.title()
        
    return df

# Coba muat data
df = load_data()

if df.empty:
    st.error("🚨 Data Gold Layer tidak ditemukan. Harap jalankan pipeline Airflow terlebih dahulu!")
    st.stop()

if 'provinsi' not in df.columns:
    st.error("🚨 Kolom 'provinsi' tidak ditemukan di dataset Gold Layer. Pastikan data Silver telah dijoin dengan benar.")
    st.stop()

# --- Sidebar Filter ---
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/c/ce/Bank_Indonesia_logo.svg/1200px-Bank_Indonesia_logo.svg.png", width=150)
st.sidebar.header("Penyaringan Data")

tahun_tersedia = sorted(df['tahun'].dropna().unique().tolist())
if not tahun_tersedia:
    st.error("Data tahun tidak tersedia.")
    st.stop()

tahun_pilihan = st.sidebar.slider("Pilih Rentang Tahun", 
                                  min_value=int(min(tahun_tersedia)), 
                                  max_value=int(max(tahun_tersedia)), 
                                  value=(int(min(tahun_tersedia)), int(max(tahun_tersedia))))

provinsi_tersedia = sorted(df['provinsi'].dropna().unique().tolist())
# Default pilihan: 5 provinsi terbesar di Jawa
default_prov = [p for p in ['Dki Jakarta', 'Jawa Barat', 'Jawa Tengah', 'Jawa Timur', 'Banten'] if p in provinsi_tersedia]

provinsi_pilihan = st.sidebar.multiselect("Pilih Provinsi (Bisa lebih dari 1)", 
                                          options=provinsi_tersedia, 
                                          default=default_prov if default_prov else provinsi_tersedia[:5])

# Filter dataset
df_filtered = df[(df['tahun'] >= tahun_pilihan[0]) & (df['tahun'] <= tahun_pilihan[1])]
if provinsi_pilihan:
    df_filtered = df_filtered[df_filtered['provinsi'].isin(provinsi_pilihan)]

if df_filtered.empty:
    st.warning("⚠️ Tidak ada data untuk rentang tahun dan provinsi yang dipilih.")
    st.stop()

# --- Metrics Utama ---
st.markdown("### 📈 Indikator Rata-Rata (Berdasarkan Filter)")
col1, col2, col3, col4 = st.columns(4)
with col1:
    avg_ipm = df_filtered['ipm_provinsi'].mean() if 'ipm_provinsi' in df_filtered.columns else 0
    st.metric("Rata-rata IPM", f"{avg_ipm:.2f}")
with col2:
    avg_miskin = df_filtered['persen_miskin'].mean() if 'persen_miskin' in df_filtered.columns else 0
    st.metric("Rata-rata Penduduk Miskin", f"{avg_miskin:.2f}%")
with col3:
    avg_pdrb = df_filtered['pertumbuhan_ekonomi_persen'].mean() if 'pertumbuhan_ekonomi_persen' in df_filtered.columns else 0
    st.metric("Pertumbuhan Ekonomi", f"{avg_pdrb:.2f}%")
with col4:
    avg_tpt = df_filtered['tpt'].mean() if 'tpt' in df_filtered.columns else 0
    st.metric("Tingkat Pengangguran Terbuka", f"{avg_tpt:.2f}%")

st.markdown("---")

# --- Grafik 1: Tren Kemiskinan ---
st.markdown("### 📉 Tren Kemiskinan Antar Provinsi")
if 'persen_miskin' in df_filtered.columns:
    tren_kemiskinan = df_filtered.groupby(['tahun', 'provinsi'])['persen_miskin'].mean().reset_index()
    fig_trend = px.line(tren_kemiskinan, x='tahun', y='persen_miskin', color='provinsi', markers=True,
                       title="Pergerakan Persentase Penduduk Miskin (2015-2024)",
                       labels={'persen_miskin': 'Persentase Miskin (%)', 'tahun': 'Tahun', 'provinsi': 'Provinsi'},
                       color_discrete_sequence=px.colors.qualitative.Set1)
    fig_trend.update_layout(xaxis=dict(tickmode='linear', dtick=1))
    st.plotly_chart(fig_trend, use_container_width=True)

colA, colB = st.columns(2)

with colA:
    # --- Grafik 2: Bar Chart IPM ---
    st.markdown("### 🏆 Peringkat Indeks Pembangunan Manusia (IPM)")
    if 'ipm_provinsi' in df_filtered.columns:
        df_ipm = df_filtered.groupby('provinsi')['ipm_provinsi'].mean().reset_index().sort_values(by='ipm_provinsi', ascending=False)
        fig_bar = px.bar(df_ipm, x='ipm_provinsi', y='provinsi', orientation='h', color='ipm_provinsi',
                         title="Rata-rata IPM Berdasarkan Filter Provinsi", 
                         color_continuous_scale='Blues')
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

with colB:
    # --- Grafik 3: Scatter Plot Korelasi ---
    st.markdown("### 🔗 Korelasi PDRB vs Kemiskinan")
    if 'pertumbuhan_ekonomi_persen' in df_filtered.columns and 'persen_miskin' in df_filtered.columns:
        fig_scatter = px.scatter(df_filtered, x='pertumbuhan_ekonomi_persen', y='persen_miskin', color='provinsi',
                                hover_data=['tahun', 'periode'], 
                                title="Sebaran PDRB terhadap Kemiskinan",
                                labels={'pertumbuhan_ekonomi_persen': 'Pertumbuhan Ekonomi (%)', 'persen_miskin': 'Kemiskinan (%)'})
        
        # Tambahkan garis tren OLS (Ordinary Least Squares)
        fig_scatter.add_traces(
            px.scatter(df_filtered, x='pertumbuhan_ekonomi_persen', y='persen_miskin', trendline="ols").data[1]
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

st.markdown("---")
st.caption("Dikembangkan untuk Tugas Besar Analisis Big Data - Kelompok 4 (Razin & Hanna)")
