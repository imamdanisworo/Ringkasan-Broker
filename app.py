import streamlit as st
import pandas as pd
import re, io, os
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
from pandas.errors import EmptyDataError

# === CONFIGURATION ===
REPO_ID  = "imamdanisworo/broker-storage"      # <- your dataset repo
HF_TOKEN = st.secrets["HF_TOKEN"]              # keep token out of code!

st.set_page_config(page_title="📊 Ringkasan Broker Saham", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

# ╭────────────────────────────────────────────────────────╮
# │  Helpers                                               │
# ╰────────────────────────────────────────────────────────╯
def normalise_name(fname: str) -> str:
    base, ext = os.path.splitext(fname)
    base = re.sub(r"\s*\(\d+\)$", "", base)          # strip trailing “ (1)”
    return f"{base}{ext}"

def parse_broker_excel(buf_or_path, file_name: str) -> pd.DataFrame:
    """Read Excel / Parquet -> validated DataFrame."""
    try:
        df = (pd.read_parquet(buf_or_path)
              if file_name.lower().endswith(".parquet")
              else pd.read_excel(buf_or_path, sheet_name=0))
    except EmptyDataError:
        raise ValueError("file is empty")
    df.columns = df.columns.str.strip()

    needed = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
    if not needed.issubset(df.columns):
        raise ValueError("missing required columns")

    m = re.search(r"(\d{8})", file_name)
    df["Tanggal"] = datetime.strptime(m.group(1), "%Y%m%d").date() if m else datetime.today().date()
    df["Broker"]  = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
    return df

@st.cache_data(show_spinner="📥 Loading data...")
def load_repo() -> pd.DataFrame:
    api   = HfApi(token=HF_TOKEN)
    files = [f for f in api.list_repo_files(REPO_ID, repo_type="dataset") if f.endswith(".parquet")]
    dfs   = []
    for f in files:
        try:
            path = hf_hub_download(REPO_ID, f, repo_type="dataset", token=HF_TOKEN)
            dfs.append(parse_broker_excel(path, f))
        except Exception as e:
            st.warning(f"⚠️ {f} skipped: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# ╭────────────────────────────────────────────────────────╮
# │  Sidebar – upload & controls                           │
# ╰────────────────────────────────────────────────────────╯
with st.sidebar:
    st.header("🔧 Controls")
    if st.button("🔄 Refresh Cache"):
        st.cache_data.clear()
        st.rerun()

    uploaded_files = st.file_uploader(
        "📂 Upload Excel Files",
        type=["xlsx"],
        accept_multiple_files=True,
        key="file_uploader",      # lets us reset widget after upload
    )

# ╭────────────────────────────────────────────────────────╮
# │  Upload loop – **robust** (never crashes)              │
# ╰────────────────────────────────────────────────────────╯
if uploaded_files:
    api    = HfApi(token=HF_TOKEN)
    total  = len(uploaded_files)
    status = st.status(f"Uploading {total} file(s)…", expanded=True)
    bar    = status.progress(0.0)

    for i, up in enumerate(uploaded_files, 1):
        status.write(f"🔄 {up.name}")
        try:
            df    = parse_broker_excel(up, up.name)
            pname = re.sub(r"\.xlsx$", ".parquet", normalise_name(up.name), flags=re.I)

            buf = io.BytesIO()
            df.to_parquet(buf, index=False); buf.seek(0)

            upload_file(
                path_or_fileobj = buf,
                path_in_repo    = pname,
                repo_id         = REPO_ID,
                repo_type       = "dataset",
                token           = HF_TOKEN,
                commit_message  = "Add/replace broker file",
            )
            status.write(f"✅ {up.name} uploaded")
        except Exception as e:
            status.write(f"❌ {up.name} failed: {e}")

        bar.progress(i/total)

    status.update(state="complete", label="Upload finished", expanded=False)
    bar.empty()

    # refresh cache so next run pulls the fresh files
    st.cache_data.clear()
    st.session_state["file_uploader"] = None
    st.experimental_rerun()

# ╭────────────────────────────────────────────────────────╮
# │  Load repo data                                        │
# ╰────────────────────────────────────────────────────────╯
df = load_repo()
if df.empty:
    st.info("📤 Upload Excel files to get started.")
    st.stop()

# ╭────────────────────────────────────────────────────────╮
# │  Filters                                               │
# ╰────────────────────────────────────────────────────────╯
df["Tanggal"] = pd.to_datetime(df["Tanggal"])
min_d, max_d  = df["Tanggal"].min().date(), df["Tanggal"].max().date()
today         = datetime.today().date()
year_start    = datetime(today.year, 1, 1).date()

st.subheader("🎛️ Filter data")
c1, c2, c3 = st.columns([2,2,2])
brokers  = c1.multiselect("Broker", sorted(df["Broker"].unique()))
fields   = c2.multiselect("Field", ["Volume", "Nilai", "Frekuensi"])
mode     = c3.selectbox("Mode", ["Daily","Monthly","Yearly"])

if mode=="Daily":
    d_from = c1.date_input("From", min_d, min_d, max_d)
    d_to   = c2.date_input("To",   max_d, min_d, max_d)
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

filt = df[
    (df["Tanggal"]>=pd.to_datetime(d_from)) &
    (df["Tanggal"]<=pd.to_datetime(d_to)) &
    (df["Broker"].isin(brokers))
]
if filt.empty:
    st.warning("No data for selected filters.")
    st.stop()

# ╭────────────────────────────────────────────────────────╮
# │  Transform & display                                   │
# ╰────────────────────────────────────────────────────────╯
m   = filt.melt(id_vars=["Tanggal","Broker"], value_vars=fields,
                var_name="Field", value_name="Value")
tot = filt.melt(id_vars=["Tanggal"], value_vars=fields,
                var_name="Field", value_name="Total").groupby(
                ["Tanggal","Field"]).sum().reset_index()
mg  = pd.merge(m, tot, on=["Tanggal","Field"])
mg["%"] = mg.apply(lambda r: 0 if r["Total"]==0 else r["Value"]/r["Total"]*100, axis=1)

if mode=="Monthly": mg["Tanggal"]=mg["Tanggal"].dt.to_period("M").dt.to_timestamp()
if mode=="Yearly":  mg["Tanggal"]=mg["Tanggal"].dt.to_period("Y").dt.to_timestamp()

grp = mg.groupby(["Tanggal","Broker","Field"]).agg({"Value":"sum","%":"mean"}).reset_index()
grp["Val_fmt"] = grp["Value"].apply(lambda x:f"{x:,.0f}")
grp["Pct_fmt"] = grp["%"].apply(lambda x:f"{x:.2f}%")

st.subheader("📋 Table")
table = grp.sort_values("Tanggal",ascending=False).reset_index(drop=True).iloc[::-1]
table["Tanggal"] = table["Tanggal"].dt.strftime(
    "%d %b %Y" if mode=="Daily" else "%b %Y" if mode=="Monthly" else "%Y")
st.dataframe(table[["Tanggal","Broker","Field","Val_fmt","Pct_fmt"]], use_container_width=True)

# ⬇ download
dl = grp[["Tanggal","Broker","Field","Val_fmt","Pct_fmt"]].copy()
dl.columns = ["Tanggal","Broker","Field","Value","Percentage"]
st.download_button("💾 CSV", dl.to_csv(index=False).encode(), "broker_summary.csv")

# ╭────────────────────────────────────────────────────────╮
# │  Charts                                                │
# ╰────────────────────────────────────────────────────────╯
tab1, tab2 = st.tabs(["📈 Value","📊 % Share"])
for f in fields:
    sub = grp[grp["Field"]==f]
    with tab1:
        fig = px.line(sub, x="Tanggal", y="Value", color="Broker", title=f"{f} over time")
        fig.update_traces(mode="lines" if mode=="Daily" else "lines+markers")
        fig.update_layout(hovermode="x unified", yaxis_tickformat=".2s")
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        fig = px.line(sub, x="Tanggal", y="%", color="Broker", title=f"{f} % share")
        fig.update_traces(mode="lines" if mode=="Daily" else "lines+markers")
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
