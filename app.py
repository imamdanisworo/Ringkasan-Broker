import streamlit as st
import pandas as pd
import re
from datetime import datetime
import plotly.express as px
import os
import io
from huggingface_hub import HfApi, hf_hub_download, upload_file
from pathlib import Path

st.set_page_config(page_title="📊 Ringkasan Broker", layout="wide")

st.markdown("<h1 style='text-align:center;'>📊 Ringkasan Aktivitas Broker Saham</h1>", unsafe_allow_html=True)
st.markdown("### 📂 Unggah & Sinkronisasi Data Excel")

if st.button("🔄 Refresh dari Hugging Face"):
    st.cache_data.clear()
    st.rerun()

REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]

def upload_all_excels():
    folder_path = "."
    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(folder_path, filename)
            try:
                upload_file(
                    path_or_fileobj=file_path,
                    path_in_repo=filename,
                    repo_id=REPO_ID,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                st.success(f"✅ Uploaded: {filename}")
            except Exception as e:
                st.error(f"❌ Failed to upload {filename}: {e}")

@st.cache_data
def load_excel_files_from_hf():
    api = HfApi()
    files = api.list_repo_files(REPO_ID, repo_type="dataset")
    xlsx_files = [f for f in files if f.endswith(".xlsx")]
    all_data = []

    for file in xlsx_files:
        try:
            file_path = hf_hub_download(repo_id=REPO_ID, filename=file, repo_type="dataset", token=HF_TOKEN)
            match = re.search(r'(\d{8})', file)
            file_date = datetime.strptime(match.group(1), "%Y%m%d").date() if match else datetime.today().date()
            df = pd.read_excel(file_path, sheet_name="Sheet1")
            df.columns = df.columns.str.strip()

            required_cols = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
            if not required_cols.issubset(df.columns):
                continue

            df["Tanggal"] = file_date
            df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
            all_data.append(df)
        except Exception as e:
            st.warning(f"⚠️ Gagal memuat {file} dari HF: {e}")

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def upload_to_hf(file):
    clean_name = re.sub(r"\s*\(\d+\)", "", Path(file.name).stem) + ".xlsx"
    try:
        upload_file(
            path_or_fileobj=file,
            path_in_repo=clean_name,
            repo_id=REPO_ID,
            repo_type="dataset",
            token=HF_TOKEN
        )
        st.success(f"✅ Uploaded {clean_name} ke Hugging Face")
    except Exception as e:
        st.error(f"❌ Upload gagal: {e}")

uploaded_files = st.file_uploader("📁 Unggah Beberapa File Excel (Sheet1 wajib)", type=["xlsx"], accept_multiple_files=True)
if uploaded_files:
    for file in uploaded_files:
        upload_to_hf(file)

combined_df = load_excel_files_from_hf()
if not combined_df.empty:
    combined_df["Tanggal"] = pd.to_datetime(combined_df["Tanggal"])

    st.markdown("---")
    st.markdown("### 🎛️ Filter & Tampilan Data")

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        selected_brokers = st.multiselect("🏢 Pilih Broker", sorted(combined_df["Broker"].unique()))
    with col2:
        selected_fields = st.multiselect("📈 Pilih Metode", ["Volume", "Nilai", "Frekuensi"])
    with col3:
        min_date, max_date = combined_df["Tanggal"].min().date(), combined_df["Tanggal"].max().date()
        display_mode = st.selectbox("🗓️ Mode Tampilan", ["Daily", "Monthly", "Yearly"])

        if display_mode == "Daily":
            date_from = st.date_input("Dari", min_value=min_date, max_value=max_date, value=min_date)
            date_to = st.date_input("Sampai", min_value=min_date, max_value=max_date, value=max_date)
        elif display_mode == "Monthly":
            months = sorted(combined_df["Tanggal"].dt.to_period("M").unique())
            selected_months = st.multiselect("🗓️ Bulan", months, default=[months[0]])
            if not selected_months:
                selected_months = [months[0]]
            date_from = min(m.to_timestamp() for m in selected_months)
            date_to = max((m + 1).to_timestamp() - pd.Timedelta(days=1) for m in selected_months)
        elif display_mode == "Yearly":
            years = sorted(combined_df["Tanggal"].dt.year.unique())
            selected_years = st.multiselect("📆 Tahun", years, default=[years[0]])
            date_from = datetime(min(selected_years), 1, 1).date()
            date_to = datetime(max(selected_years), 12, 31).date()

    if selected_brokers and selected_fields:
        filtered_df = combined_df[
            (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
            (combined_df["Tanggal"] <= pd.to_datetime(date_to)) &
            (combined_df["Broker"].isin(selected_brokers))
        ]

        if not filtered_df.empty:
            melted_df = filtered_df.melt(
                id_vars=["Tanggal", "Broker"],
                value_vars=selected_fields,
                var_name="Field",
                value_name="Value"
            )

            total_all_df = combined_df.melt(id_vars=["Tanggal", "Broker"], value_vars=selected_fields, var_name="Field", value_name="Value")
            total_all_df = total_all_df.groupby(["Tanggal", "Field"])["Value"].sum().reset_index()
            total_all_df.rename(columns={"Value": "TotalValue"}, inplace=True)

            merged_df = pd.merge(melted_df, total_all_df, on=["Tanggal", "Field"])
            merged_df["Percentage"] = merged_df.apply(
                lambda row: (row["Value"] / row["TotalValue"] * 100) if row["TotalValue"] != 0 else 0,
                axis=1
            )

            display_df = merged_df.copy()

            if display_mode == "Monthly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("M").dt.to_timestamp()
                display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()
            elif display_mode == "Yearly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()

            display_df["Tanggal"] = display_df["Tanggal"].dt.strftime('%d-%b-%y' if display_mode == "Daily" else '%b-%y' if display_mode == "Monthly" else '%Y')
            display_df["Formatted Value"] = display_df["Value"].apply(lambda x: f"{x:,.0f}")
            display_df["Formatted %"] = display_df["Percentage"].apply(lambda x: f"{x:.2f}%")
            display_df = display_df.sort_values(["Tanggal", "Broker", "Field"])

            st.markdown("### 📋 Tabel Ringkasan")
            st.dataframe(display_df[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]], use_container_width=True)

            st.markdown("---")
            st.markdown("### 📊 Grafik Nilai Asli")

            def format_value_short(val):
                if val >= 1_000_000_000_000:
                    return f"{val / 1_000_000_000_000:.1f} T"
                elif val >= 1_000_000_000:
                    return f"{val / 1_000_000_000:.1f} B"
                elif val >= 1_000_000:
                    return f"{val / 1_000_000:.1f} M"
                elif val >= 1_000:
                    return f"{val / 1_000:.1f} K"
                return f"{val:.0f}"

            for field in selected_fields:
                chart_data = merged_df[merged_df["Field"] == field].dropna()

                if display_mode == "Monthly":
                    chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("M").dt.to_timestamp()
                    chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                elif display_mode == "Yearly":
                    chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                    chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()

                chart_data["ValueShort"] = chart_data["Value"].apply(format_value_short)

                fig = px.line(
                    chart_data,
                    x="Tanggal",
                    y="Value",
                    color="Broker",
                    title=f"{field} over Time",
                    markers=True,
                    hover_data={"ValueShort": True, "Broker": True, "Tanggal": True}
                )

                fig.update_traces(
                    hovertemplate="<b>%{x|%d %b %Y}</b><br>Broker: %{customdata[1]}<br>Value: %{customdata[0]}"
                )

                fig.update_layout(
                    yaxis_title=field,
                    yaxis_tickformat="~s",
                    xaxis_title="Tanggal",
                    xaxis_tickformat='%d %b %Y',
                    xaxis=dict(tickmode='array', tickvals=chart_data['Tanggal'].unique())
                )

                st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")
            st.markdown("### 📈 Grafik Kontribusi (%)")

            for field in selected_fields:
                chart_data = merged_df[merged_df["Field"] == field].dropna()

                if display_mode == "Monthly":
                    chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("M").dt.to_timestamp()
                    chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                elif display_mode == "Yearly":
                    chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                    chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()

                fig = px.line(
                    chart_data,
                    x="Tanggal",
                    y="Percentage",
                    color="Broker",
                    title=f"{field} Contribution (%) Over Time",
                    markers=True
                )
                fig.update_layout(
                    yaxis_title="Persentase (%)",
                    xaxis_title="Tanggal",
                    xaxis_tickformat='%d %b %Y',
                    xaxis=dict(tickmode='array', tickvals=chart_data['Tanggal'].unique())
                )
                st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("⚠️ Tidak ditemukan data broker. Silakan unggah file Excel terlebih dahulu.")
