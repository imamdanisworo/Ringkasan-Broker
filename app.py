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
# 1️⃣  UNIVERSAL HELPERS (No changes)
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
# 2️⃣  REFINED UPLOAD HANDLING
# ───────────────────────────────────────────────────────────

# Initialize a more detailed session state for upload management
if "upload_manager" not in st.session_state:
    st.session_state.upload_manager = {
        "files_to_process": [],
        "total_files": 0,
        "processed_count": 0,
        "status": "idle",  # idle, processing, complete, error
    }

def queue_files_for_upload():
    """Callback to queue files from the uploader into session_state."""
    if st.session_state.file_uploader:
        mgr = st.session_state.upload_manager
        mgr["files_to_process"] = st.session_state.file_uploader
        mgr["total_files"] = len(mgr["files_to_process"])
        mgr["processed_count"] = 0
        mgr["status"] = "processing"
        # Clear the uploader widget itself after queueing
        st.session_state.file_uploader = []


def process_upload_queue():
    """Processes one file from the queue per Streamlit rerun."""
    mgr = st.session_state.upload_manager
    if mgr["status"] != "processing":
        return

    # Create a persistent status box for the entire upload process
    status_box = st.status(f"Uploading {mgr['total_files']} file(s)…", expanded=True)
    
    if mgr["files_to_process"]:
        # Get one file from the top of the queue
        file_to_upload = mgr["files_to_process"].pop(0)
        mgr["processed_count"] += 1
        
        progress_percent = mgr["processed_count"] / mgr["total_files"]
        progress_text = f"Uploading **{file_to_upload.name}** ({mgr['processed_count']}/{mgr['total_files']})..."
        status_box.progress(progress_percent, text=progress_text)

        try:
            api = HfApi(token=HF_TOKEN)
            df = parse_broker_excel(file_to_upload, file_to_upload.name)
            
            # Convert to Parquet
            parquet_name = re.sub(r"\.xlsx$", ".parquet", normalise_name(file_to_upload.name), flags=re.IGNORECASE)
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            
            # Upload
            upload_file(
                path_or_fileobj=buf,
                path_in_repo=parquet_name,
                repo_id=REPO_ID, repo_type="dataset", token=HF_TOKEN,
                commit_message=f"Add/replace: {parquet_name}"
            )
            st.toast(f"✅ Successfully processed {file_to_upload.name}!", icon="🎉")

        except Exception as e:
            mgr["status"] = "error"
            status_box.update(label=f"Error on file {file_to_upload.name}", state="error", expanded=True)
            st.error(f"Could not process {file_to_upload.name}: {e}")
            st.stop() # Stop execution on error

        # IMPORTANT: Trigger a rerun to process the next file
        st.rerun()

    else:
        # This block runs when the queue is empty
        mgr["status"] = "complete"
        status_box.update(label="✅ All files uploaded! Refreshing data...", state="complete")
        st.toast("🚀 All uploads complete!", icon="🚀")
        
        # Reset for the next batch of uploads
        st.session_state.upload_manager = {
            "files_to_process": [], "total_files": 0, "processed_count": 0, "status": "idle"
        }
        
        st.cache_data.clear() # The critical step to force a data refresh
        st.rerun() # The final rerun to display the new data


# === SIDEBAR ACTIONS ===
with st.sidebar:
    st.header("🔧 Data Controls")

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.file_uploader(
        "📂 Upload Excel Files",
        type=["xlsx"],
        accept_multiple_files=True,
        key="file_uploader",
        on_change=queue_files_for_upload,
    )
    
    # === DEBUGGING BOX (Optional) ===
    # This helps see the state. You can comment it out later.
    with st.expander("⚙️ Upload Status (for debugging)"):
        st.json(st.session_state.upload_manager)


# === TRIGGER THE UPLOAD PROCESSOR ===
# This function will run on every rerun and check if there's work to do.
process_upload_queue()


# === LOAD FROM HUGGING FACE (Parquet, cached) ===
@st.cache_data(show_spinner="📥 Loading data from Hugging Face…")
def load_data_from_repo() -> pd.DataFrame:
    # (This function remains unchanged)
    api = HfApi(token=HF_TOKEN)
    files = [
        f for f in api.list_repo_files(REPO_ID, repo_type="dataset") if f.endswith(".parquet")
    ]
    dfs = []
    for f in files:
        try:
            path = hf_hub_download(repo_id=REPO_ID, filename=f, repo_type="dataset", token=HF_TOKEN)
            dfs.append(parse_broker_excel(path, f))
        except Exception as e:
            st.warning(f"⚠️ Failed to load or parse {f}: {e}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

# === MAIN APP LOGIC (No changes below this line) ===
# ... (the rest of your script from "combined_df = load_data_from_repo()" onwards) ...
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
        if pd.api.types.is_datetime64_any_dtype(combined_df["Tanggal"]):
            periods = combined_df["Tanggal"].dt.to_period("M").unique()
            months = sorted(periods, key=lambda p: (p.year, p.month))
            default_months = [m for m in months[-3:] if m in months]
            selected_months = col1.multiselect("Month(s)", months, default=default_months)
            date_from = min(m.to_timestamp() for m in selected_months).date() if selected_months else None
            date_to = (
                max(m.end_time for m in selected_months).date() if selected_months else None
            )
    else:
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

melted = filtered_df.melt(
    id_vars=["Tanggal", "Broker"], value_vars=selected_fields, var_name="Field", value_name="Value",
)

total = (
    filtered_df.melt(
        id_vars=["Tanggal"], value_vars=selected_fields, var_name="Field", value_name="TotalValue",
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
    use_container_width=True, hide_index=True,
)

csv_export = table[["Tanggal", "Broker", "Field", "Value", "%"]].copy()
csv_export.columns = ["Tanggal", "Broker", "Field", "Value", "Percentage"]
st.download_button(
    "💾 Download CSV", data=csv_export.to_csv(index=False).encode("utf-8"),
    file_name="broker_summary.csv", mime="text/csv",
)

tab1, tab2 = st.tabs(["📈 Value Trend", "📊 % Contribution"])
for field in selected_fields:
    data = grouped[grouped["Field"] == field]
    with tab1:
        fig = px.line(data, x="Tanggal", y="Value", color="Broker", title=f"{field} Over Time")
        fig.update_traces(mode="lines+markers")
        fig.update_layout(hovermode="x unified", yaxis_tickformat=".2s")
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        fig = px.line(data, x="Tanggal", y="%", color="Broker", title=f"{field} % Contribution Over Time")
        fig.update_traces(mode="lines+markers")
        fig.update_layout(hovermode="x unified", yaxis_title_text="Contribution (%)")
        st.plotly_chart(fig, use_container_width=True)
