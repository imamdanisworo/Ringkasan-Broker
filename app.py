import streamlit as st
import pandas as pd
import re, io, os
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
from pandas.errors import EmptyDataError

# ───────────────  CONSTANTS  ─────────────────────────────
REPO_ID  = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]        # add in Secrets tab!

# nice value scalers for charts / table
UNIT = {
    "Original":      (1,   ""),
    "Thousands (K)": (1e3, " K"),
    "Millions (M)":  (1e6, " M"),
    "Billions (B)":  (1e9, " B"),
}

st.set_page_config("📊 Ringkasan Broker", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

# ───────────────  SMALL HELPERS  ─────────────────────────
def clean_name(fname: str) -> str:
    """Remove ' (1)' etc. before the extension so it overwrites."""
    base, ext = os.path.splitext(fname)
    base = re.sub(r"\s*\(\d+\)$", "", base)
    return f"{base}{ext}"

def read_broker_file(buf_or_path, file_name):
    """Return a validated DataFrame from Excel or Parquet."""
    df = (pd.read_parquet(buf_or_path)
          if file_name.lower().endswith(".parquet")
          else pd.read_excel(buf_or_path, sheet_name=0))
    df.columns = df.columns.str.strip()

    need = {"Kode Perusahaan","Nama Perusahaan","Volume","Nilai","Frekuensi"}
    if not need.issubset(df.columns):
        raise ValueError("missing required columns")

    m = re.search(r"(\d{8})", file_name)
    df["Tanggal"] = (datetime.strptime(m.group(1), "%Y%m%d").date()
                     if m else datetime.today().date())
    df["Broker"]  = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
    return df

@st.cache_data(show_spinner="📥 Loading data…")
def load_repo():
    """Download every Parquet file from HF repo → big DataFrame."""
    api   = HfApi(token=HF_TOKEN)
    files = [f for f in api.list_repo_files(REPO_ID, repo_type="dataset")
             if f.endswith(".parquet")]
    df_list = []
    for f in files:
        try:
            p = hf_hub_download(REPO_ID, f, repo_type="dataset", token=HF_TOKEN)
            df_list.append(read_broker_file(p, f))
        except Exception as e:
            st.warning(f"⚠️ {f} skipped ({e})")
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

# ───────────────  SESSION INITIALISATION  ───────────────
if "data" not in st.session_state:
    st.session_state.data = load_repo()

# ───────────────  SIDEBAR  ──────────────────────────────
with st.sidebar:
    st.header("🔧 Controls")
    if st.button("🔄 Clear cache & reload"):
        st.cache_data.clear()
        st.session_state.data = load_repo()
        st.experimental_rerun()

    with st.expander("📂 Upload Excel (.xlsx)"):
        files = st.file_uploader(
            "Drag & drop or browse",
            type=["xlsx"],
            accept_multiple_files=True,
            key="uploader",
        )

# ───────────────  UPLOAD HANDLING  ──────────────────────
if files:
    placeholder = st.empty()             # shows progress & messages
    prog_bar    = st.progress(0.)
    total       = len(files)
    success_ct  = 0

    api = HfApi(token=HF_TOKEN)

    for i, f in enumerate(files, 1):
        placeholder.info(f"Processing **{f.name}** ({i}/{total})…")
        try:
            df_local  = read_broker_file(f, f.name)
            buf       = io.BytesIO(); df_local.to_parquet(buf, index=False); buf.seek(0)
            tgt_name  = re.sub(r"\.xlsx$", ".parquet", clean_name(f.name), flags=re.I)

            # upload – will overwrite if exists
            upload_file(buf, tgt_name, REPO_ID,
                        repo_type="dataset", token=HF_TOKEN,
                        commit_message="Add/replace broker file")
            success_ct += 1

            # merge into in-memory data so UI updates instantly
            st.session_state.data = pd.concat(
                [st.session_state.data, df_local], ignore_index=True
            )

            placeholder.success(f"✅ {f.name} uploaded")
        except Exception as e:
            placeholder.error(f"❌ {f.name} failed: {e}")

        prog_bar.progress(i/total)

    placeholder.info(f"Done: {success_ct}/{total} succeeded.")
    prog_bar.empty()

# ───────────────  MAIN DATA  ────────────────────────────
df = st.session_state.data.copy()
if df.empty:
    st.info("No data — please upload files."); st.stop()

# ───────────────  FILTERS  ──────────────────────────────
df["Tanggal"] = pd.to_datetime(df["Tanggal"])
min_d, max_d  = df["Tanggal"].min().date(), df["Tanggal"].max().date()
today         = datetime.today().date()
year_start    = datetime(today.year,1,1).date()

st.subheader("🎛️ Filter")
c1,c2,c3,c4 = st.columns([2,2,2,2])
brokers = c1.multiselect("Broker", sorted(df["Broker"].unique()))
fields  = c2.multiselect("Field",  ["Volume","Nilai","Frekuensi"])
mode    = c3.selectbox("Mode", ["Daily","Monthly","Yearly"])
unit    = c4.selectbox("Units", list(UNIT.keys()), index=2)   # default Millions

# date pickers
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
    st.warning("Select broker(s), field(s) and date range."); st.stop()

mask = (df["Tanggal"].between(pd.to_datetime(d_from), pd.to_datetime(d_to))
        & df["Broker"].isin(brokers))
filt = df[mask]
if filt.empty:
    st.warning("No data for selected filters."); st.stop()

# ───────────────  TRANSFORM  ────────────────────────────
m   = filt.melt(id_vars=["Tanggal","Broker"], value_vars=fields,
                var_name="Field", value_name="Value")
tot = filt.melt(id_vars=["Tanggal"], value_vars=fields,
                var_name="Field", value_name="Total").groupby(
                ["Tanggal","Field"]).sum().reset_index()
mg  = pd.merge(m, tot, on=["Tanggal","Field"])
mg["Pct"] = (mg["Value"]/mg["Total"]).fillna(0)*100

if mode=="Monthly": mg["Tanggal"] = mg["Tanggal"].dt.to_period("M").dt.to_timestamp()
if mode=="Yearly" : mg["Tanggal"] = mg["Tanggal"].dt.to_period("Y").dt.to_timestamp()

grp = mg.groupby(["Tanggal","Broker","Field"]
         ).agg(Value=("Value","sum"), Pct=("Pct","mean")).reset_index()

scale, suf = UNIT[unit]
grp["Scaled"]  = grp["Value"]/scale
grp["Val_fmt"] = grp["Scaled"].apply(lambda x:f"{x:,.0f}{suf}")
grp["Pct_fmt"] = grp["Pct"].apply(lambda x:f"{x:.2f}%")

# ───────────────  TABLE  ────────────────────────────────
st.subheader("📋 Data Table")
show = grp.sort_values("Tanggal")
show["Tanggal"] = show["Tanggal"].dt.strftime(
        "%d %b %Y" if mode=="Daily" else "%b %Y" if mode=="Monthly" else "%Y")
st.dataframe(show[["Tanggal","Broker","Field","Val_fmt","Pct_fmt"]],
             use_container_width=True, hide_index=True)

# ───────────────  CHARTS  ───────────────────────────────
tab1, tab2 = st.tabs(["📈 Value","📊 % Share"])
for f in fields:
    sub = grp[grp["Field"]==f]
    with tab1:
        fig = px.line(sub, x="Tanggal", y="Scaled", color="Broker",
                      title=f"{f} ({unit})")
        fig.update_traces(mode="lines" if mode=="Daily" else "lines+markers")
        fig.update_layout(hovermode="x unified", yaxis_tickformat=".2s")
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        fig = px.line(sub, x="Tanggal", y="Pct", color="Broker",
                      title=f"{f} % share")
        fig.update_traces(mode="lines" if mode=="Daily" else "lines+markers")
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
