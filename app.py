import streamlit as st
import pandas as pd
import re
from datetime import datetime
import plotly.express as px
import os
import time
from huggingface_hub import HfApi, hf_hub_download, upload_file
from io import BytesIO

st.set_page_config(page_title="Ringkasan Broker", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

# === CONFIG ===
REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]

# === Refresh Button with Progress Bar ===
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    with st.spinner("Refreshing data..."):
        progress_bar = st.progress(0)
        for percent in range(0, 101, 10):
            time.sleep(0.05)
            progress_bar.progress(percent)
    st.rerun()

# === File Upload ===
uploaded_files = st.file_uploader("Upload Excel Files (.xlsx)", type=["xlsx"], accept_multiple_files=True)
if uploaded_files:
    with st.spinner("📤 Uploading files..."):
        for file in uploaded_files:
            try:
                file.seek(0)
                upload_file(
                    path_or_fileobj=file,
                    path_in_repo=file.name,
                    repo_id=REPO_ID,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                st.success(f"✅ Uploaded: {file.name}")
            except Exception as e:
                st.error(f"❌ Upload failed: {e}")
        st.rerun()

# === Load Excel Files from HF with Progress Bar + File Count ===
def load_excel_files_with_progress():
    api = HfApi(token=HF_TOKEN)
    files = api.list_repo_files(REPO_ID, repo_type="dataset")
    xlsx_files = [f for f in files if f.endswith(".xlsx")]
    total_files = len(xlsx_files)
    progress = st.progress(0)
    status = st.empty()
    data = []

    for idx, file in enumerate(xlsx_files):
        status.info(f"📥 Loading file {idx + 1} of {total_files}...")
        try:
            file_path = hf_hub_download(
                repo_id=REPO_ID,
                filename=file,
                repo_type="dataset",
                token=HF_TOKEN
            )
            match = re.search(r"(\d{8})", file)
            file_date = datetime.strptime(match.group(1), "%Y%m%d").date() if match else datetime.today().date()

            df = pd.read_excel(file_path, sheet_name="Sheet1")
            df.columns = df.columns.str.strip()

            if {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}.issubset(df.columns):
                df["Tanggal"] = file_date
                df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
                data.append(df)
            else:
                st.warning(f"⚠️ {file} skipped: missing required columns.")
        except Exception as e:
            st.warning(f"⚠️ Failed to load {file}: {e}")
        progress.progress((idx + 1) / total_files)

    status.success("✅ All files loaded.") if total_files else status.info("📁 No files found.")
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

combined_df = load_excel_files_with_progress()

# === Main UI and Analysis ===
if not combined_df.empty:
    combined_df["Tanggal"] = pd.to_datetime(combined_df["Tanggal"])

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        selected_brokers = st.multiselect("Select Broker(s)", sorted(combined_df["Broker"].unique()))
    with col2:
        selected_fields = st.multiselect("Select Fields", ["Volume", "Nilai", "Frekuensi"])
    with col3:
        min_date, max_date = combined_df["Tanggal"].min().date(), combined_df["Tanggal"].max().date()
        display_mode = st.selectbox("Display Mode", ["Daily", "Monthly", "Yearly"])

        today = datetime.today()
        year_start = datetime(today.year, 1, 1).date()

        if display_mode == "Daily":
            date_from = st.date_input("From", min_value=min_date, max_value=max_date, value=year_start)
            date_to = st.date_input("To", min_value=min_date, max_value=max_date, value=max_date)
        elif display_mode == "Monthly":
            all_months = combined_df["Tanggal"].dt.to_period("M")
            unique_years = sorted(set(m.year for m in all_months.unique()))
            selected_years = st.multiselect("Year(s)", unique_years, default=[today.year])
            months = sorted([m for m in all_months.unique() if m.year in selected_years])
            selected_months = st.multiselect("Month(s)", months, default=months)
            if selected_months:
                date_from = min(m.to_timestamp() for m in selected_months)
                date_to = max((m + 1).to_timestamp() - pd.Timedelta(days=1) for m in selected_months)
            else:
                date_from = date_to = None
        elif display_mode == "Yearly":
            years = sorted(combined_df["Tanggal"].dt.year.unique())
            selected_years = st.multiselect("Year(s)", years, default=[today.year])
            if selected_years:
                date_from = datetime(min(selected_years), 1, 1).date()
                date_to = datetime(max(selected_years), 12, 31).date()
            else:
                date_from = date_to = None

    if not selected_brokers:
        st.warning("❗ Please select at least one broker.")
    elif not selected_fields:
        st.warning("❗ Please select at least one data field.")
    elif not date_from or not date_to:
        st.warning("❗ Please specify a valid date range.")
    else:
        filtered_df = combined_df[
            (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
            (combined_df["Tanggal"] <= pd.to_datetime(date_to)) &
            (combined_df["Broker"].isin(selected_brokers))
        ]

        if not filtered_df.empty:
            melted_df = filtered_df.melt(id_vars=["Tanggal", "Broker"], value_vars=selected_fields,
                                         var_name="Field", value_name="Value")
            total_df = combined_df.melt(id_vars=["Tanggal", "Broker"], value_vars=selected_fields,
                                        var_name="Field", value_name="Value")
            total_df = total_df.groupby(["Tanggal", "Field"])["Value"].sum().reset_index()
            total_df.rename(columns={"Value": "TotalValue"}, inplace=True)

            merged_df = pd.merge(melted_df, total_df, on=["Tanggal", "Field"])
            merged_df["Percentage"] = merged_df.apply(
                lambda row: (row["Value"] / row["TotalValue"] * 100) if row["TotalValue"] != 0 else 0, axis=1)

            display_df = merged_df.copy()

            if display_mode == "Monthly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("M").dt.to_timestamp()
                display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                display_df = display_df[
                    (display_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (display_df["Tanggal"] <= pd.to_datetime(date_to))
                ]
            elif display_mode == "Yearly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                display_df = display_df[
                    (display_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (display_df["Tanggal"] <= pd.to_datetime(date_to))
                ]

            display_df["Formatted Value"] = display_df["Value"].apply(lambda x: f"{x:,.0f}")
            display_df["Formatted %"] = display_df["Percentage"].apply(lambda x: f"{x:.2f}%")

            display_df_for_table = display_df[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]].copy()
            display_df_for_table["Tanggal Display"] = display_df["Tanggal"].dt.strftime(
                '%-d %b %Y' if display_mode == "Daily" else '%b %Y' if display_mode == "Monthly" else '%Y'
            )
            display_df_for_table = display_df_for_table.sort_values("Tanggal")
            st.dataframe(display_df_for_table[["Tanggal Display", "Broker", "Field", "Formatted Value", "Formatted %"]].rename(columns={"Tanggal Display": "Tanggal"}))

            to_download = display_df_for_table[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]].copy()
            to_download.columns = ["Tanggal", "Broker", "Field", "Value", "%"]
            csv = to_download.to_csv(index=False).encode("utf-8")
            st.download_button("📥 Download Table as CSV", data=csv, file_name="broker_summary.csv", mime="text/csv")

            tab1, tab2 = st.tabs(["📈 Original Values", "📊 % Contribution"])

            with tab1:
                for field in selected_fields:
                    chart_data = display_df[display_df["Field"] == field].copy()
                    fig = px.line(
                        chart_data,
                        x="Tanggal",
                        y="Value",
                        color="Broker",
                        title=f"{field} over Time",
                        markers=True
                    )
                    fig.update_layout(
                        yaxis_tickformat=".2s",
                        xaxis_title="Tanggal",
                        hovermode="x unified"
                    )
                    st.plotly_chart(fig, use_container_width=True)

            with tab2:
                for field in selected_fields:
                    chart_data = display_df[display_df["Field"] == field].copy()
                    fig = px.line(
                        chart_data,
                        x="Tanggal",
                        y="Percentage",
                        color="Broker",
                        title=f"{field} Contribution (%) Over Time",
                        markers=True
                    )
                    fig.update_layout(
                        xaxis_title="Tanggal",
                        hovermode="x unified"
                    )
                    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("⬆️ Silakan unggah file Excel terlebih dahulu.")
