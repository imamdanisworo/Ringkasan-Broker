import streamlit as st
import pandas as pd
import re
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
from pandas.errors import EmptyDataError
import io
import os  # ★ NEW: needed for filename normalisation

# === CONFIGURATION ===
REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]

st.set_page_config(page_title="📊 Ringkasan Broker Saham", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

# ───────────────────────────────────────────────────────────
# 1️⃣  UNIVERSAL HELPERS
# ───────────────────────────────────────────────────────────

def normalise_name(fname: str) -> str:
    """
    Remove a trailing ' (n)' before the extension so
    'data (1).xlsx' → 'data.xlsx'.  Ensures later uploads
    overwrite previous ones with the same logical name.
    """
    base, ext = os.path.splitext(fname)
    base = re.sub(r"\s*\(\d+\)$", "", base)
    return f"{base}{ext}"

def parse_broker_excel(path_or_buf, file_name: str) -> pd.DataFrame:
    """Read Excel/Parquet and return validated DataFrame."""
    try:
        if str(file_name).lower().endswith(".parquet"):
            df = pd.read_parquet(path_or_buf)
        else:
            df = pd.read_excel(path_or_buf, sheet_name=0)
    except EmptyDataError:
        raise ValueError("File is empty.")
    df.columns = df.columns.str.strip()
    required = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
    if not required.issubset(df.columns):
        raise ValueError("Missing required columns.")
    match = re.search(r"(\d{8})", file_name)
    file_date = datetime.strptime(match.group(1), "%Y%m%d").date() if match else datetime.today().date()
    df["Tanggal"] = file_date
    df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
    return df


# === SIDEBAR ACTIONS ===
with st.sidebar:
    st.header("🔧 Data Controls")

    if st.button("🔄 Refresh Cache"):
        st.cache_data.clear()
        st.rerun()

    uploaded_files = st.file_uploader("📂 Upload Excel Files", type=["xlsx"], accept_multiple_files=True)

# === HANDLE UPLOADS (Fix #5 + #6 + #7 + ★ NEW duplicate-name overwrite)
session_uploads = []

if uploaded_files:
    api = HfApi(token=HF_TOKEN)
    existing_files = api.list_repo_files(REPO_ID, repo_type="dataset")

    total = len(uploaded_files)
    progress = st.progress(0, text=f"Preparing to upload {total} file(s)…")

    for i, file in enumerate(uploaded_files, start=1):
        try:
            df_parsed = parse_broker_excel(file, file.name)
            session_uploads.append(df_parsed)

            # ★ NEW: normalise the target filename
            safe_name = normalise_name(file.name)
            parquet_name = re.sub(r"\.xlsx$", ".parquet", safe_name, flags=re.IGNORECASE)

            buf = io.BytesIO()
            df_parsed.to_parquet(buf, index=False)
            buf.seek(0)

            if parquet_name in existing_files:
                st.warning(f"{parquet_name} already exists and will be replaced.")

            upload_file(
                path_or_fileobj=buf,
                path_in_repo=parquet_name,
                repo_id=REPO_ID,
                repo_type="dataset",
                token=HF_TOKEN,
                commit_message="Add/replace broker file"
            )
            st.success(f"✅ Uploaded: {file.name}")
        except Exception as e:
            st.error(f"❌ Failed to upload {file.name}: {e}")

        progress.progress(i / total, text=f"Uploaded {i}/{total} file(s)")

    progress.empty()

# === LOAD FROM HUGGING FACE (Fix #5) ===
@st.cache_data(show_spinner="📥 Loading data from Hugging Face…")
def load_data_from_repo() -> pd.DataFrame:
    api = HfApi(token=HF_TOKEN)
    files = [f for f in api.list_repo_files(REPO_ID, repo_type="dataset") if f.endswith(".parquet")]
    data = []
    for file in files:
        try:
            path = hf_hub_download(repo_id=REPO_ID, filename=file, repo_type="dataset", token=HF_TOKEN)
            df = parse_broker_excel(path, file)
            data.append(df)
        except Exception as e:
            st.warning(f"⚠️ Failed loading {file}: {e}")
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

# === COMBINE REMOTE + SESSION (Fix #2) ===
remote_df = load_data_from_repo()
session_df = pd.concat(session_uploads, ignore_index=True) if session_uploads else pd.DataFrame()
combined_df = (
    pd.concat([remote_df, session_df], ignore_index=True)
    if not remote_df.empty or not session_df.empty
    else pd.DataFrame()
)

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
        melted = filtered_df.melt(id_vars=["Tanggal", "Broker"], value_vars=selected_fields,
                                  var_name="Field", value_name="Value")

        total = filtered_df.melt(id_vars=["Tanggal"], value_vars=selected_fields,
                                 var_name="Field", value_name="TotalValue") \
                          .groupby(["Tanggal", "Field"]).sum().reset_index()

        merged = pd.merge(melted, total, on=["Tanggal", "Field"])
        merged["%"] = merged.apply(lambda r: (r["Value"] / r["TotalValue"] * 100) if r["TotalValue"] != 0 else 0, axis=1)

        if display_mode == "Monthly":
            merged["Tanggal"] = merged["Tanggal"].dt.to_period("M").dt.to_timestamp()
        elif display_mode == "Yearly":
            merged["Tanggal"] = merged["Tanggal"].dt.to_period("Y").dt.to_timestamp()

        grouped = merged.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "%": "mean"}).reset_index()
        grouped["Formatted Value"] = grouped["Value"].apply(lambda x: f"{x:,.0f}")
        grouped["Formatted %"] = grouped["%"].apply(lambda x: f"{x:.2f}%")

        st.subheader("📋 Data Table")
        display_table = grouped.sort_values("Tanggal", ascending=False).reset_index(drop=True)
        display_table = display_table.iloc[::-1].reset_index(drop=True)  # Fix #1
        display_table["Tanggal"] = display_table["Tanggal"].dt.strftime(
            "%d %b %Y" if display_mode == "Daily" else "%b %Y" if display_mode == "Monthly" else "%Y"
        )
        display_table = display_table[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]]
        st.dataframe(display_table, use_container_width=True)

        csv_export = grouped[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]].copy()
        csv_export.columns = ["Tanggal", "Broker", "Field", "Value", "Percentage"]
        st.download_button("💾 Download CSV", data=csv_export.to_csv(index=False).encode("utf-8"),
                           file_name="broker_summary.csv", mime="text/csv")

        tab1, tab2 = st.tabs(["📈 Value Trend", "📊 % Contribution"])

        for field in selected_fields:
            data = grouped[grouped["Field"] == field]

            with tab1:
                fig = px.line(data, x="Tanggal", y="Value", color="Broker", title=f"{field} Over Time")
                fig.update_traces(mode="lines" if display_mode == "Daily" else "lines+markers")  # Fix #6
                fig.update_layout(hovermode="x unified", yaxis_tickformat=".2s")
                st.plotly_chart(fig, use_container_width=True)

            with tab2:
                fig = px.line(data, x="Tanggal", y="%", color="Broker",
                              title=f"{field} % Contribution Over Time")
                fig.update_traces(mode="lines" if display_mode == "Daily" else "lines+markers")  # Fix #6
                fig.update_layout(hovermode="x unified")
                st.plotly_chart(fig, use_container_width=True)
else:
    st.info("📤 Upload Excel files to get started.")
