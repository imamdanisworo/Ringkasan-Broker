import streamlit as st
import pandas as pd
import re, io, os
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
from pandas.errors import EmptyDataError

# === CONFIGURATION ===
REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]

st.set_page_config(page_title="📊 Ringkasan Broker Saham", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

# ───────────────────────────────────────────────────────────
# 1️⃣  UNIVERSAL HELPERS
# ───────────────────────────────────────────────────────────
def normalise_name(fname: str) -> str:
    """Strip trailing ' (n)' so copies overwrite originals."""
    base, ext = os.path.splitext(fname)
    base = re.sub(r"\s*\(\d+\)$", "", base)
    return f"{base}{ext}"

def parse_broker_excel(path_or_buf, file_name: str) -> pd.DataFrame:
    """Read Excel/Parquet → validated DataFrame."""
    try:
        df = (
            pd.read_parquet(path_or_buf)
            if file_name.lower().endswith(".parquet")
            else pd.read_excel(path_or_buf, sheet_name=0)
        )
    except EmptyDataError:
        raise ValueError("File is empty.")
    df.columns = df.columns.str.strip()
    required = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
    if not required.issubset(df.columns):
        raise ValueError("Missing required columns.")
    m = re.search(r"(\d{8})", file_name)
    df["Tanggal"] = (
        datetime.strptime(m.group(1), "%Y%m%d").date() if m else datetime.today().date()
    )
    df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
    return df

# ───────────────────────────────────────────────────────────
# 2️⃣  UPLOAD HANDLING (NON-BLOCKING)
# ───────────────────────────────────────────────────────────

# Initialize session state for upload management
if "upload_status" not in st.session_state:
    st.session_state.upload_status = {"files": [], "total": 0, "processed": 0, "running": False}

def start_upload_process():
    """Callback to queue files for processing."""
    if st.session_state.file_uploader:
        status = st.session_state.upload_status
        status["files"] = st.session_state.file_uploader
        status["total"] = len(status["files"])
        status["processed"] = 0
        status["running"] = True

def handle_uploads():
    """Processes one file per rerun, keeping the UI responsive."""
    status = st.session_state.upload_status
    if not status["running"]:
        return

    api = HfApi(token=HF_TOKEN)
    progress_bar = st.status(f"Uploading {status['total']} file(s)…", expanded=True)

    if status["files"]:
        up = status["files"].pop(0) # Get one file from the queue
        status["processed"] += 1
        
        progress_text = f"Processing **{up.name}** ({status['processed']}/{status['total']})…"
        progress_value = status["processed"] / status["total"]
        progress_bar.progress(progress_value, text=progress_text)
        
        try:
            df = parse_broker_excel(up, up.name)
            parquet_name = re.sub(
                r"\.xlsx$", ".parquet", normalise_name(up.name), flags=re.IGNORECASE
            )
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            
            upload_file(
                path_or_fileobj=buf,
                path_in_repo=parquet_name,
                repo_id=REPO_ID,
                repo_type="dataset",
                token=HF_TOKEN,
                commit_message=f"Add/replace broker file: {parquet_name}",
            )
        except Exception as e:
            st.error(f"Failed to process {up.name}: {e}")
            # Continue to next file on next rerun
        
        st.rerun() # Rerun to process the next file in the queue

    else:
        # All files are processed
        progress_bar.update(label="✅ Upload finished! Refreshing data...", state="complete")
        status["running"] = False
        st.cache_data.clear() # IMPORTANT: Clear cache to load new data
        st.rerun() # Final rerun to update the dashboard

# === SIDEBAR ACTIONS ===
with st.sidebar:
    st.header("🔧 Data Controls")

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    # The file uploader now uses a callback to trigger the upload process
    st.file_uploader(
        "📂 Upload Excel Files",
        type=["xlsx"],
        accept_multiple_files=True,
        key="file_uploader",
        on_change=start_upload_process, # This starts the upload
    )

# === PROCESS UPLOADS (main page) ===
handle_uploads()

# === LOAD FROM HUGGING FACE (Parquet, cached) ===
@st.cache_data(show_spinner="📥 Loading data from Hugging Face…")
def load_data_from_repo() -> pd.DataFrame:
    api = HfApi(token=HF_TOKEN)
    files = [
        f
        for f in api.list_repo_files(REPO_ID, repo_type="dataset")
        if f.endswith(".parquet")
    ]
    dfs = []
    for f in files:
        try:
            path = hf_hub_download(
                repo_id=REPO_ID, filename=f, repo_type="dataset", token=HF_TOKEN
            )
            dfs.append(parse_broker_excel(path, f))
        except Exception as e:
            st.warning(f"⚠️ Failed to load or parse {f}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# === LOAD DATA (simplified) ===
combined_df = load_data_from_repo()

# === UI AND FILTERING ===
if combined_df.empty:
    st.info("📤 Upload Excel files to get started.")
    st.stop()

combined_df["Tanggal"] = pd.to_datetime(combined_df["Tanggal"])
min_date, max_date = combined_df["Tanggal"].min().date(), combined_df["Tanggal"].max().date()
today = datetime.today().date()
year_start = datetime(today.year, 1, 1).date()

with st.container():
    st.subheader("🎛️ Filter Data")
    col1, col2, col3 = st.columns([2, 2, 2])

    selected_brokers = col1.multiselect(
        "🧾 Select Broker(s)", sorted(combined_df["Broker"].unique())
    )
    selected_fields = col2.multiselect("📊 Select Field(s)", ["Volume", "Nilai", "Frekuensi"])
    display_mode = col3.selectbox("🗓️ Display Mode", ["Daily", "Monthly", "Yearly"])

    if display_mode == "Daily":
        date_from = col1.date_input(
            "From", min_value=min_date, max_value=max_date, value=year_start
        )
        date_to = col2.date_input("To", min_value=min_date, max_value=max_date, value=max_date)
    elif display_mode == "Monthly":
        # Ensure 'Tanggal' is datetime before using .dt accessor
        if pd.api.types.is_datetime64_any_dtype(combined_df["Tanggal"]):
            periods = combined_df["Tanggal"].dt.to_period("M").unique()
            months = sorted(periods, key=lambda p: (p.year, p.month))
            # Ensure default is not empty and exists in the options
            default_months = [m for m in months[-3:] if m in months]
            selected_months = col1.multiselect("Month(s)", months, default=default_months)
            date_from = min(m.to_timestamp() for m in selected_months).date() if selected_months else None
            date_to = (
                max(m.end_time for m in selected_months).date()
                if selected_months
                else None
            )
    else:  # Yearly
        years = sorted(combined_df["Tanggal"].dt.year.unique())
        default_years = [y for y in [today.year] if y in years]
        selected_years = col1.multiselect("Year(s)", years, default=default_years)
        date_from = datetime(min(selected_years), 1, 1).date() if selected_years else None
        date_to = datetime(max(selected_years), 12, 31).date() if selected_years else None


if not selected_brokers or not selected_fields or not date_from or not date_to:
    st.warning("⚠️ Please complete all filters to view data.")
    st.stop()

filtered_df = combined_df[
    (combined_df["Tanggal"] >= pd.to_datetime(date_from))
    & (combined_df["Tanggal"] <= pd.to_datetime(date_to))
    & (combined_df["Broker"].isin(selected_brokers))
]

if filtered_df.empty:
    st.warning("❌ No data found for selected filters.")
    st.stop()


# === DATA TRANSFORMATIONS (No changes here) ===
melted = filtered_df.melt(
    id_vars=["Tanggal", "Broker"],
    value_vars=selected_fields,
    var_name="Field",
    value_name="Value",
)

total = (
    filtered_df.melt(
        id_vars=["Tanggal"],
        value_vars=selected_fields,
        var_name="Field",
        value_name="TotalValue",
    )
    .groupby(["Tanggal", "Field"])['TotalValue']
    .sum()
    .reset_index()
)

merged = pd.merge(melted, total, on=["Tanggal", "Field"])
merged["%"] = merged.apply(
    lambda r: (r["Value"] / r["TotalValue"] * 100) if r["TotalValue"] else 0, axis=1
)

if display_mode == "Monthly":
    merged["Tanggal"] = merged["Tanggal"].dt.to_period("M").dt.to_timestamp()
elif display_mode == "Yearly":
    merged["Tanggal"] = merged["Tanggal"].dt.to_period("Y").dt.to_timestamp()

grouped = (
    merged.groupby(["Tanggal", "Broker", "Field"])
    .agg({"Value": "sum", "%": "mean"})
    .reset_index()
)
grouped["Formatted Value"] = grouped["Value"].apply(lambda x: f"{x:,.0f}")
grouped["Formatted %"] = grouped["%"].apply(lambda x: f"{x:.2f}%")


# === TABLE & CHARTS (No changes here) ===
st.subheader("📋 Data Table")
table = (
    grouped.sort_values("Tanggal", ascending=True)
    .reset_index(drop=True)
)
table_display = table.copy()
table_display["Tanggal"] = table_display["Tanggal"].dt.strftime(
    "%d %b %Y" if display_mode == "Daily" else "%b %Y" if display_mode == "Monthly" else "%Y"
)
st.dataframe(
    table_display[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]],
    use_container_width=True,
    hide_index=True,
)

csv_export = table[["Tanggal", "Broker", "Field", "Value", "%"]].copy()
csv_export.columns = ["Tanggal", "Broker", "Field", "Value", "Percentage"]
st.download_button(
    "💾 Download CSV",
    data=csv_export.to_csv(index=False).encode("utf-8"),
    file_name="broker_summary.csv",
    mime="text/csv",
)

# === CHARTS ===
tab1, tab2 = st.tabs(["📈 Value Trend", "📊 % Contribution"])
for field in selected_fields:
    data = grouped[grouped["Field"] == field]

    with tab1:
        fig = px.line(data, x="Tanggal", y="Value", color="Broker", title=f"{field} Over Time")
        fig.update_traces(mode="lines+markers")
        fig.update_layout(hovermode="x unified", yaxis_tickformat=".2s")
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        fig = px.line(
            data, x="Tanggal", y="%", color="Broker", title=f"{field} % Contribution Over Time"
        )
        fig.update_traces(mode="lines+markers")
        fig.update_layout(hovermode="x unified", yaxis_title_text="Contribution (%)")
        st.plotly_chart(fig, use_container_width=True)
