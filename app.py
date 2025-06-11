import streamlit as st
import pandas as pd
import re
from datetime import datetime
from huggingface_hub import HfApi, hf_hub_download, upload_file, list_repo_files
import os
import io

st.set_page_config(page_title="📊 Ringkasan Broker", layout="wide")

st.markdown("<h1 style='text-align:center;'>📊 Ringkasan Aktivitas Broker Saham</h1>", unsafe_allow_html=True)
st.markdown("### 📂 Unggah File Excel (.xlsx) & Simpan ke Hugging Face Dataset")

# === CONFIG HF ===
REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]

# === UPLOAD SECTION ===
uploaded_files = st.file_uploader("⬆️ Upload file Excel (.xlsx)", type=["xlsx"], accept_multiple_files=True)
if uploaded_files:
    for file in uploaded_files:
        try:
            # Save to Hugging Face
            upload_file(
                path_or_fileobj=file,
                path_in_repo=file.name,
                repo_id=REPO_ID,
                repo_type="dataset",
                token=HF_TOKEN
            )
            st.success(f"✅ {file.name} berhasil diunggah ke Hugging Face")
        except Exception as e:
            st.error(f"❌ Gagal upload {file.name}: {e}")

# === LOAD FROM HF ===
@st.cache_data
def list_excel_files_from_repo():
    files = list_repo_files(repo_id=REPO_ID, repo_type="dataset", token=HF_TOKEN)
    return [f for f in files if f.endswith(".xlsx")]

@st.cache_data
def load_excel_from_repo(filename):
    try:
        file_path = hf_hub_download(repo_id=REPO_ID, repo_type="dataset", filename=filename, token=HF_TOKEN)
        df = pd.read_excel(file_path, sheet_name="Sheet1")
        df.columns = df.columns.str.strip()

        # Extract date from filename
        match = re.search(r"(\d{8})", filename)
        file_date = None
        if match:
            try:
                file_date = datetime.strptime(match.group(1), "%Y%m%d")
            except:
                pass

        # Handle Tanggal
        if "Tanggal" in df.columns:
            df["Tanggal"] = pd.to_datetime(df["Tanggal"], errors='coerce')
            if df["Tanggal"].isna().all() and file_date:
                df["Tanggal"] = file_date
        elif file_date:
            df["Tanggal"] = file_date
        else:
            raise KeyError("Kolom 'Tanggal' tidak ditemukan dan tidak bisa deteksi dari nama file.")

        df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
        return df
    except Exception as e:
        st.error(f"Gagal load file {filename}: {e}")
        return pd.DataFrame()

# === SELECT AND DISPLAY FILE ===
available_files = list_excel_files_from_repo()

if available_files:
    selected_file = st.selectbox("📁 Pilih file dari penyimpanan Hugging Face:", available_files)
    df = load_excel_from_repo(selected_file)

    if not df.empty:
        st.markdown("### 📈 Data yang Dimuat")
        st.dataframe(df)
else:
    st.warning("⚠️ Belum ada file .xlsx yang tersedia di penyimpanan Hugging Face.")
