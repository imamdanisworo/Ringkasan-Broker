import streamlit as st
import pandas as pd
import re
from datetime import datetime
import plotly.express as px
import requests
from bs4 import BeautifulSoup

st.set_page_config(page_title="📊 Ringkasan Broker", layout="wide")

st.markdown("<h1 style='text-align:center;'>📊 Ringkasan Aktivitas Broker Saham</h1>", unsafe_allow_html=True)
st.markdown("### 📂 Unggah File & Sinkronisasi ke Google Drive")

# ========== LINK FOLDER GOOGLE DRIVE ==========
FOLDER_ID = "17gDaKfBzTCLGQkGdsUFZ-CXayGjtYlvD"

uploaded_files = st.file_uploader("⬆️ Upload file Excel (.xlsx)", type=["xlsx"], accept_multiple_files=True)
temp_data = []

if uploaded_files:
    for file in uploaded_files:
        try:
            df_uploaded = pd.read_excel(file, sheet_name="Sheet1")
            df_uploaded.columns = df_uploaded.columns.str.strip()

            # Try to extract date from filename
            match = re.search(r"(\d{8})", file.name)
            file_date = None
            if match:
                try:
                    file_date = datetime.strptime(match.group(1), "%Y%m%d")
                except:
                    pass

            # Handle "Tanggal" column
            if "Tanggal" in df_uploaded.columns:
                df_uploaded["Tanggal"] = pd.to_datetime(df_uploaded["Tanggal"], errors='coerce')
                if df_uploaded["Tanggal"].isna().all() and file_date:
                    df_uploaded["Tanggal"] = file_date
            elif file_date:
                df_uploaded["Tanggal"] = file_date
            else:
                raise KeyError("Kolom 'Tanggal' tidak ditemukan dan tidak bisa mendeteksi tanggal dari nama file.")

            df_uploaded["Broker"] = df_uploaded["Kode Perusahaan"] + " / " + df_uploaded["Nama Perusahaan"]
            temp_data.append(df_uploaded)
            st.success(f"✅ {file.name} berhasil diunggah.")
        except Exception as e:
            st.error(f"❌ Gagal membaca {file.name}: {e}")

@st.cache_data
def list_excel_files_in_folder(folder_id):
    url = f"https://drive.google.com/embeddedfolderview?id={folder_id}#list"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    links = soup.find_all('a')
    file_dict = {}
    for link in links:
        href = link.get('href')
        if href and href.startswith("https://drive.google.com/file/d/") and href.endswith("/view"):
            file_id = href.split("/d/")[1].split("/")[0]
            file_name = link.text.strip()
            if file_name.endswith(".xlsx"):
                direct_url = f"https://drive.google.com/uc?id={file_id}&export=download"
                file_dict[file_name] = direct_url
    return file_dict

@st.cache_data
def load_excel_from_url(url, filename):
    try:
        df = pd.read_excel(url, sheet_name="Sheet1")
        df.columns = df.columns.str.strip()

        # Try to extract date from filename
        match = re.search(r"(\d{8})", filename)
        file_date = None
        if match:
            try:
                file_date = datetime.strptime(match.group(1), "%Y%m%d")
            except:
                pass

        # Handle "Tanggal" column
        if "Tanggal" in df.columns:
            df["Tanggal"] = pd.to_datetime(df["Tanggal"], errors='coerce')
            if df["Tanggal"].isna().all() and file_date:
                df["Tanggal"] = file_date
        elif file_date:
            df["Tanggal"] = file_date
        else:
            raise KeyError("Kolom 'Tanggal' tidak ditemukan dan tidak bisa mendeteksi tanggal dari nama file.")

        df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
        return df
    except Exception as e:
        st.error(f"❌ Gagal memuat file: {e}")
        return pd.DataFrame()

excel_links = list_excel_files_in_folder(FOLDER_ID)

df = pd.DataFrame()
if temp_data:
    df = pd.concat(temp_data, ignore_index=True)
elif excel_links:
    selected_file = st.selectbox("📁 Pilih File dari Google Drive:", list(excel_links.keys()))
    df = load_excel_from_url(excel_links[selected_file], selected_file)
    if df.empty:
        st.warning("⚠️ Tidak ada data ditemukan. Pastikan folder Google Drive dapat diakses publik dan berisi file .xlsx.")
