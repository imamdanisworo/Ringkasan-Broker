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
def load_excel_from_url(url):
    try:
        df = pd.read_excel(url, sheet_name="Sheet1")
        df.columns = df.columns.str.strip()
        df["Tanggal"] = pd.to_datetime(df["Tanggal"])
        df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
        return df
    except Exception as e:
        st.error(f"❌ Gagal memuat file: {e}")
        return pd.DataFrame()


excel_links = list_excel_files_in_folder(FOLDER_ID)

if excel_links:
    selected_file = st.selectbox("📁 Pilih File dari Google Drive:", list(excel_links.keys()))
    df = load_excel_from_url(excel_links[selected_file])
else:
    st.warning("⚠️ Tidak ada data ditemukan. Pastikan folder Google Drive dapat diakses publik dan berisi file .xlsx.")
