import streamlit as st
import pandas as pd
import re
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file

# === CONFIGURATION ===
REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]

st.set_page_config(page_title="📊 Ringkasan Broker Saham", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

# === SIDEBAR ACTIONS ===
with st.sidebar:
    st.header("🔧 Data Controls")

    if st.button("🔄 Refresh Cache"):
        st.cache_data.clear()
        st.rerun()

    uploaded_files = st.file_uploader("📂 Upload Excel Files", type=["xlsx"], accept_multiple_files=True)

# === HANDLE UPLOADS ===
combined_df = pd.DataFrame()

if uploaded_files:
    api = HfApi(token=HF_TOKEN)
    existing_files = api.list_repo_files(REPO_ID, repo_type="dataset")

    with st.spinner("📤 Uploading files..."):
        for file in uploaded_files:
            if file.name in existing_files:
                st.warning(f"{file.name} already exists and will be replaced.")
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
                st.error(f"❌ Failed to upload {file.name}: {e}")

    def read_uploaded_files(files):
        data = []
        for file in files:
            try:
                match = re.search(r"(\d{8})", file.name)
                file_date = datetime.strptime(match.group(1), "%Y%m%d").date() if match else datetime.today().date()
                df = pd.read_excel(file, sheet_name="Sheet1")
                df.columns = df.columns.str.strip()
                if {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}.issubset(df.columns):
                    df["Tanggal"] = file_date
                    df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
                    data.append(df)
            except Exception as e:
                st.warning(f"❗ {file.name} skipped: {e}")
        return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

    combined_df = read_uploaded_files(uploaded_files)

# === LOAD FROM HUGGING FACE ===
@st.cache_data(show_spinner="📥 Loading data from Hugging Face...")
def load_data_from_repo():
    api = HfApi(token=HF_TOKEN)
    files = [f for f in api.list_repo_files(REPO_ID, repo_type="dataset") if f.endswith(".xlsx")]
    data = []

    for i, file in enumerate(files):
        try:
            path = hf_hub_download(repo_id=REPO_ID, filename=file, repo_type="dataset", token=HF_TOKEN)
            match = re.search(r"(\d{8})", file)
            file_date = datetime.strptime(match.group(1), "%Y%m%d").date() if match else datetime.today().date()
            df = pd.read_excel(path, sheet_name="Sheet1")
            df.columns = df.columns.str.strip()
            if {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}.issubset(df.columns):
                df["Tanggal"] = file_date
                df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
                data.append(df)
        except Exception as e:
            st.warning(f"⚠️ Failed loading {file}: {e}")
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

if uploaded_files is None:
    combined_df = load_data_from_repo()

# === UI AND FILTERING ===
if not combined_df.empty:
    combined_df["Tanggal"] = pd.to_datetime(combined_df["Tanggal"])
    min_date, max_date = combined_df["Tanggal"].min().date(), combined_df["Tanggal"].max().date()
    today = datetime.today().date()
    year_start = datetime(today.year, 1, 1).date()

    with st.container():
        st.subheader("🎛️ Filter Data")

        col1, col2, col3 = st.columns([2, 2, 2])
        selected_brokers = col1.multiselect("🧾 Select Broker(s)", sorted(combined_df["Broker"].unique()))
        selected_fields = col2.multiselect("📊 Select Field(s)", ["Volume", "Nilai", "Frekuensi"])
        display_mode = col3.selectbox("🗓️ Display Mode", ["Daily", "Monthly", "Yearly"])

        if display_mode == "Daily":
            date_from = col1.date_input("From", min_value=min_date, max_value=max_date, value=year_start)
            date_to = col2.date_input("To", min_value=min_date, max_value=max_date, value=max_date)
        elif display_mode == "Monthly":
            periods = combined_df["Tanggal"].dt.to_period("M").unique()
            months = sorted(periods, key=lambda p: (p.year, p.month))
            selected_months = col1.multiselect("Month(s)", months, default=months[-3:])
            date_from = min(m.to_timestamp() for m in selected_months) if selected_months else None
            date_to = max((m + 1).to_timestamp() - pd.Timedelta(days=1) for m in selected_months) if selected_months else None
        else:  # Yearly
            years = sorted(combined_df["Tanggal"].dt.year.unique())
            selected_years = col1.multiselect("Year(s)", years, default=[today.year])
            date_from = datetime(min(selected_years), 1, 1).date() if selected_years else None
            date_to = datetime(max(selected_years), 12, 31).date() if selected_years else None

    if not selected_brokers or not selected_fields or not date_from or not date_to:
        st.warning("⚠️ Please complete all filters to view data.")
        st.stop()

    filtered_df = combined_df[
        (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
        (combined_df["Tanggal"] <= pd.to_datetime(date_to)) &
        (combined_df["Broker"].isin(selected_brokers))
    ]

    if filtered_df.empty:
        st.warning("❌ No data found for selected filters.")
    else:
        # === TRANSFORM AND DISPLAY ===
        melted = filtered_df.melt(id_vars=["Tanggal", "Broker"], value_vars=selected_fields,
                                  var_name="Field", value_name="Value")
        total = combined_df.melt(id_vars=["Tanggal"], value_vars=selected_fields,
                                 var_name="Field", value_name="TotalValue")
        total = total.groupby(["Tanggal", "Field"]).sum().reset_index()

        merged = pd.merge(melted, total, on=["Tanggal", "Field"])
        merged["%"] = merged.apply(lambda r: (r["Value"] / r["TotalValue"] * 100) if r["TotalValue"] != 0 else 0, axis=1)

        # Group by mode
        if display_mode == "Monthly":
            merged["Tanggal"] = merged["Tanggal"].dt.to_period("M").dt.to_timestamp()
        elif display_mode == "Yearly":
            merged["Tanggal"] = merged["Tanggal"].dt.to_period("Y").dt.to_timestamp()

        grouped = merged.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "%": "mean"}).reset_index()

        # Format
        grouped["Formatted Value"] = grouped["Value"].apply(lambda x: f"{x:,.0f}")
        grouped["Formatted %"] = grouped["%"].apply(lambda x: f"{x:.2f}%")
        grouped["Tanggal Display"] = grouped["Tanggal"].dt.strftime(
            "%d %b %Y" if display_mode == "Daily" else "%b %Y" if display_mode == "Monthly" else "%Y"
        )

        st.subheader("📋 Data Table")
        st.dataframe(grouped[["Tanggal Display", "Broker", "Field", "Formatted Value", "Formatted %"]]
                     .rename(columns={"Tanggal Display": "Tanggal"}), use_container_width=True)

        csv_export = grouped[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]].copy()
        csv_export.columns = ["Tanggal", "Broker", "Field", "Value", "Percentage"]
        st.download_button("💾 Download CSV", data=csv_export.to_csv(index=False).encode("utf-8"),
                           file_name="broker_summary.csv", mime="text/csv")

        # === CHARTS ===
        tab1, tab2 = st.tabs(["📈 Value Trend", "📊 % Contribution"])

        for field in selected_fields:
            data = grouped[grouped["Field"] == field]
            with tab1:
                fig = px.line(data, x="Tanggal", y="Value", color="Broker", markers=True,
                              title=f"{field} Over Time")
                fig.update_layout(hovermode="x unified", yaxis_tickformat=".2s")
                st.plotly_chart(fig, use_container_width=True)

            with tab2:
                fig = px.line(data, x="Tanggal", y="%", color="Broker", markers=True,
                              title=f"{field} % Contribution Over Time")
                fig.update_layout(hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)
else:
    st.info("📤 Upload Excel files to get started.")

