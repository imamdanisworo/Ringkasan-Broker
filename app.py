import streamlit as st
import pandas as pd
import re, io, os
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
from pandas.errors import EmptyDataError

# === CONFIGURATION ===
REPO_ID  = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]          # keep your token in Secrets!

st.set_page_config(page_title="📊 Ringkasan Broker Saham", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

UNIT_MAP = {
    "Original":      (1,   ""),
    "Thousands (K)": (1e3, " K"),
    "Millions (M)":  (1e6, " M"),
    "Billions (B)":  (1e9, " B"),
}

# ── Helper functions ─────────────────────────────────────
def normalise_name(fname: str) -> str:
    base, ext = os.path.splitext(fname)
    base = re.sub(r"\s*\(\d+\)$", "", base)         # remove trailing " (1)"
    return f"{base}{ext}"

def parse_broker_excel(buf_or_path, file_name: str) -> pd.DataFrame:
    """Read Excel or Parquet → DataFrame with required columns."""
    df = (pd.read_parquet(buf_or_path)
          if file_name.lower().endswith(".parquet")
          else pd.read_excel(buf_or_path, sheet_name=0))
    df.columns = df.columns.str.strip()
    needed = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
    if not needed.issubset(df.columns):
        raise ValueError("Missing required columns")
    m = re.search(r"(\d{8})", file_name)
    df["Tanggal"] = datetime.strptime(m.group(1), "%Y%m%d").date() if m else datetime.today().date()
    df["Broker"]  = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
    return df

@st.cache_data(show_spinner="📥 Loading data…")
def load_repo() -> pd.DataFrame:
    """Download all Parquet files from the Hugging Face repo."""
    api = HfApi(token=HF_TOKEN)
    files = [f for f in api.list_repo_files(REPO_ID, repo_type="dataset") if f.endswith(".parquet")]
    frames = []
    for f in files:
        try:
            path = hf_hub_download(REPO_ID, f, repo_type="dataset", token=HF_TOKEN)
            frames.append(parse_broker_excel(path, f))
        except Exception as e:
            st.warning(f"⚠️ {f} skipped: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ── Sidebar ──────────────────────────────────────────────
with st.sidebar:
    st.header("🔧 Controls")
    if st.button("🔄 Refresh Cache"):
        st.cache_data.clear(); st.rerun()

    with st.expander("Upload Excel files (.xlsx)"):
        uploaded_files = st.file_uploader(
            "Choose file(s)",
            type=["xlsx"],
            accept_multiple_files=True,
            key="file_uploader",
        )

# ── Upload loop (simple, no crashing) ───────────────────
if uploaded_files:
    api   = HfApi(token=HF_TOKEN)
    total = len(uploaded_files)

    line_box   = st.empty()            # shows current status text
    progress   = st.progress(0.0)      # progress bar

    for i, up in enumerate(uploaded_files, 1):
        line_box.markdown(f"🔄 **Uploading {up.name} ({i}/{total})…**")
        try:
            df = parse_broker_excel(up, up.name)
            parquet_name = re.sub(r"\.xlsx$", ".parquet", normalise_name(up.name), flags=re.I)

            buf = io.BytesIO(); df.to_parquet(buf, index=False); buf.seek(0)
            upload_file(
                path_or_fileobj = buf,
                path_in_repo    = parquet_name,
                repo_id         = REPO_ID,
                repo_type       = "dataset",
                token           = HF_TOKEN,
                commit_message  = "Add/replace broker file",
            )
            line_box.markdown(f"✅ **{up.name} uploaded**")
        except Exception as e:
            line_box.markdown(f"❌ **{up.name} failed:** {e}")

        progress.progress(i / total)

    line_box.markdown("✅ **All uploads finished**")
    progress.empty()

    # refresh the cache so new files appear immediately
    st.cache_data.clear()
    st.session_state["file_uploader"] = None
    st.experimental_rerun()

# ── Load data from repo ─────────────────────────────────
with st.spinner("🔄 Preparing data…"):
    df = load_repo()
if df.empty:
    st.info("📤 Upload Excel files to get started.")
    st.stop()

# ── Filters ─────────────────────────────────────────────
df["Tanggal"] = pd.to_datetime(df["Tanggal"])
min_d, max_d  = df["Tanggal"].min().date(), df["Tanggal"].max().date()
today         = datetime.today().date()
year_start    = datetime(today.year, 1, 1).date()

st.subheader("🎛️ Filter data")
c1,c2,c3,c4 = st.columns([2,2,2,2])
brokers = c1.multiselect("Broker", sorted(df["Broker"].unique()))
fields  = c2.multiselect("Field",  ["Volume","Nilai","Frekuensi"])
mode    = c3.selectbox("Mode", ["Daily","Monthly","Yearly"])
unit    = c4.selectbox("Display Unit", list(UNIT_MAP.keys()), index=2)  # default Millions

if mode=="Daily":
    d_from = c1.date_input("From", year_start, min_d, max_d)
    d_to   = c2.date_input("To",   max_d,    min_d, max_d)
elif mode=="Monthly":
    months = sorted(df["Tanggal"].dt.to_period("M").unique())
    sel_m  = c1.multiselect("Months", months, default=months[-3:])
    d_from = min(m.to_timestamp() for m in sel_m) if sel_m else None
    d_to   = max((m+1).to_timestamp()-pd.Timedelta(days=1) for m in sel_m) if sel_m else None
else:
    years  = sorted(df["Tanggal"].dt.year.unique())
    sel_y  = c1.multiselect("Years", years, default=[today.year])
    d_from = datetime(min(sel_y),1,1).date() if sel_y else None
    d_to   = datetime(max(sel_y),12,31).date() if sel_y else None

if not brokers or not fields or not d_from or not d_to:
    st.warning("Select broker(s), field(s) and date range.")
    st.stop()

filt = df[(df["Tanggal"].between(pd.to_datetime(d_from), pd.to_datetime(d_to))) &
          (df["Broker"].isin(brokers))]
if filt.empty:
    st.warning("No data for selected filters.")
    st.stop()

# ── Transform & display ─────────────────────────────────
m   = filt.melt(id_vars=["Tanggal","Broker"], value_vars=fields,
                var_name="Field", value_name="Value")
tot = filt.melt(id_vars=["Tanggal"], value_vars=fields,
                var_name="Field", value_name="Total").groupby(
                ["Tanggal","Field"]).sum().reset_index()
mg  = pd.merge(m, tot, on=["Tanggal","Field"])
mg["%"] = (mg["Value"] / mg["Total"]).fillna(0)*100

if mode=="Monthly": mg["Tanggal"] = mg["Tanggal"].dt.to_period("M").dt.to_timestamp()
if mode=="Yearly":  mg["Tanggal"] = mg["Tanggal"].dt.to_period("Y").dt.to_timestamp()

grp = mg.groupby(["Tanggal","Broker","Field"]).agg(Value=("Value","sum"), Pct=("%","mean")).reset_index()

div, suf   = UNIT_MAP[unit]
grp["ScaleVal"] = grp["Value"] / div
grp["Val_fmt"]  = grp["ScaleVal"].apply(lambda x:f"{x:,.0f}{suf}")
grp["Pct_fmt"]  = grp["Pct"].apply(lambda x:f"{x:.2f}%")

st.subheader("📋 Data Table")
tbl = grp.sort_values("Tanggal").copy()
tbl["Tanggal"] = tbl["Tanggal"].dt.strftime("%d %b %Y" if mode=="Daily"
                                            else "%b %Y" if mode=="Monthly" else "%Y")
st.dataframe(tbl[["Tanggal","Broker","Field","Val_fmt","Pct_fmt"]],
             use_container_width=True, hide_index=True)

# download
dl = tbl.rename(columns={"Val_fmt":"Value"+suf.strip(),"Pct_fmt":"Percentage"})
st.download_button("💾 CSV", dl.to_csv(index=False).encode(), "broker_summary.csv")

# ── Charts ───────────────────────────────────────────────
tab1, tab2 = st.tabs(["📈 Value","📊 % Share"])
for f in fields:
    sub = grp[grp["Field"]==f]
    with tab1:
        fig = px.line(sub, x="Tanggal", y="ScaleVal", color="Broker",
                      title=f"{f} ({unit})", template="plotly_white")
        fig.update_traces(mode="lines" if mode=="Daily" else "lines+markers")
        fig.update_layout(hovermode="x unified", yaxis_tickformat=".2s")
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        fig = px.line(sub, x="Tanggal", y="Pct", color="Broker",
                      title=f"{f} % share", template="plotly_white")
        fig.update_traces(mode="lines" if mode=="Daily" else "lines+markers")
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
