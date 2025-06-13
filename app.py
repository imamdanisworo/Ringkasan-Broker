import streamlit as st
import pandas as pd
import re, io, os
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
from pandas.errors import EmptyDataError

# ────────── CONFIG ──────────
REPO_ID  = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]          # token must have WRITE scope

st.set_page_config("📊 Ringkasan Broker", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

UNIT = {
    "Original":      (1,   ""),
    "Thousands (K)": (1e3, " K"),
    "Millions (M)":  (1e6, " M"),
    "Billions (B)":  (1e9, " B"),
}

# ────────── HELPERS ─────────
def strip_copy_suffix(name: str) -> str:
    """data (1).xlsx  → data.xlsx  (for overwriting)."""
    base, ext = os.path.splitext(name)
    base = re.sub(r"\s*\(\d+\)$", "", base)
    return f"{base}{ext}"

def to_df(buf_or_path, fname):
    df = (pd.read_parquet(buf_or_path)
          if fname.lower().endswith(".parquet")
          else pd.read_excel(buf_or_path, sheet_name=0))
    df.columns = df.columns.str.strip()
    need = {"Kode Perusahaan","Nama Perusahaan","Volume","Nilai","Frekuensi"}
    if not need.issubset(df.columns):
        raise ValueError("columns missing")
    m = re.search(r"(\d{8})", fname)
    df["Tanggal"] = datetime.strptime(m.group(1),"%Y%m%d").date() if m else datetime.today().date()
    df["Broker"]  = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
    return df

@st.cache_data(show_spinner="📥 Downloading broker files…")
def load_repo():
    api  = HfApi(token=HF_TOKEN)
    files= [f for f in api.list_repo_files(REPO_ID, repo_type="dataset") if f.endswith(".parquet")]
    outs = []
    for f in files:
        try:
            path = hf_hub_download(REPO_ID, f, repo_type="dataset", token=HF_TOKEN)
            outs.append(to_df(path, f))
        except Exception as e:
            st.warning(f"{f} skipped ({e})")
    return pd.concat(outs, ignore_index=True) if outs else pd.DataFrame()

# ────────── CALLBACK for uploads ─────────
def handle_upload():
    files = st.session_state.get("upload_buffer", [])
    if not files:
        return

    api = HfApi(token=HF_TOKEN)
    ok, fail = 0,0
    with st.spinner("⏫ Uploading…"):
        for up in files:
            try:
                df   = to_df(up, up.name)
                buf  = io.BytesIO(); df.to_parquet(buf, index=False); buf.seek(0)
                tgt  = re.sub(r"\.xlsx$", ".parquet", strip_copy_suffix(up.name), flags=re.I)
                upload_file(buf, tgt, REPO_ID, repo_type="dataset",
                            token=HF_TOKEN, commit_message="Add/replace broker file")
                # merge to session data so it shows instantly after rerun
                if "data" in st.session_state and not st.session_state.data.empty:
                    st.session_state.data = pd.concat([st.session_state.data, df], ignore_index=True)
                ok += 1
            except Exception as e:
                fail += 1
                st.error(f"{up.name} failed: {e}")

    st.success(f"Upload complete – {ok} success, {fail} failed.")
    # clear cache so fresh files load on next run
    st.cache_data.clear()
    # clear uploader widget & rerun once
    st.session_state["file_uploader"] = None
    st.session_state["upload_buffer"] = []
    st.experimental_rerun()

# ────────── SIDEBAR ─────────
with st.sidebar:
    st.header("🔧 Controls")
    if st.button("🔄 Full reload"):
        st.cache_data.clear(); st.session_state.pop("data",None); st.experimental_rerun()

    st.file_uploader(
        "Add Excel files", type=["xlsx"], accept_multiple_files=True,
        key="file_uploader", on_change=handle_upload,
        help="Drop one or more XLSX files here.",
    )

# Streamlit passes uploaded files to state key 'file_uploader'.  
# We copy them to 'upload_buffer' so callback can access:
if st.session_state.get("file_uploader") and not st.session_state.get("upload_buffer"):
    st.session_state["upload_buffer"] = st.session_state.file_uploader

# ────────── DATAFRAME ─────────
if "data" not in st.session_state:
    st.session_state.data = load_repo()

df = st.session_state.data.copy()
if df.empty:
    st.info("No data yet. Upload Excel files to begin."); st.stop()

# ────────── FILTER UI ─────────
df["Tanggal"] = pd.to_datetime(df["Tanggal"])
min_d,max_d   = df["Tanggal"].dt.date.min(), df["Tanggal"].dt.date.max()
today         = datetime.today().date()

st.subheader("🎛️ Filters")
c1,c2,c3,c4 = st.columns([2,2,2,2])
brokers = c1.multiselect("Broker", sorted(df["Broker"].unique()))
fields  = c2.multiselect("Field", ["Volume","Nilai","Frekuensi"])
mode    = c3.selectbox("Mode", ["Daily","Monthly","Yearly"])
unit    = c4.selectbox("Unit", list(UNIT), index=2)

# date controls
if mode=="Daily":
    d_from = c1.date_input("From", min_d)
    d_to   = c2.date_input("To",   max_d)
elif mode=="Monthly":
    months = sorted(df["Tanggal"].dt.to_period("M").unique())
    sel_m  = c1.multiselect("Months", months, default=months[-3:])
    d_from = min(m.to_timestamp() for m in sel_m) if sel_m else None
    d_to   = max((m+1).to_timestamp()-pd.Timedelta(days=1) for m in sel_m) if sel_m else None
else:
    years  = sorted(df["Tanggal"].dt.year.unique())
    sel_y  = c1.multiselect("Years", years, default=[today.year])
    d_from = datetime(min(sel_y),1,1) if sel_y else None
    d_to   = datetime(max(sel_y),12,31) if sel_y else None

if not brokers or not fields or not d_from or not d_to:
    st.warning("Select broker(s), field(s) and date range."); st.stop()

mask = (df["Tanggal"].between(pd.to_datetime(d_from), pd.to_datetime(d_to))
        & df["Broker"].isin(brokers))
data = df[mask]
if data.empty:
    st.warning("No data for those filters."); st.stop()

# ────────── TRANSFORM ─────────
m   = data.melt(id_vars=["Tanggal","Broker"], value_vars=fields,
                var_name="Field", value_name="Value")
tot = data.melt(id_vars=["Tanggal"], value_vars=fields,
                var_name="Field", value_name="Tot").groupby(
                ["Tanggal","Field"]).sum().reset_index()
mg  = m.merge(tot,on=["Tanggal","Field"])
mg["Pct"] = (mg["Value"]/mg["Tot"]).fillna(0)*100
if mode=="Monthly": mg["Tanggal"]=mg["Tanggal"].dt.to_period("M").dt.to_timestamp()
if mode=="Yearly" : mg["Tanggal"]=mg["Tanggal"].dt.to_period("Y").dt.to_timestamp()

grp = mg.groupby(["Tanggal","Broker","Field"]).agg(Value=("Value","sum"), Pct=("Pct","mean")).reset_index()

div,suf = UNIT[unit]
grp["Scaled"] = grp["Value"]/div
grp["Val_fmt"]= grp["Scaled"].apply(lambda x:f"{x:,.0f}{suf}")
grp["Pct_fmt"]= grp["Pct"].apply(lambda x:f"{x:.2f}%")

# ─── Table
st.subheader("📋 Table")
tab = grp.sort_values("Tanggal")
tab["Tanggal"]=tab["Tanggal"].dt.strftime("%d %b %Y" if mode=="Daily"
                                          else "%b %Y" if mode=="Monthly" else "%Y")
st.dataframe(tab[["Tanggal","Broker","Field","Val_fmt","Pct_fmt"]],
             use_container_width=True, hide_index=True)

# ─── Charts
tab1,tab2 = st.tabs(["📈 Value","📊 % Share"])
for f in fields:
    sub=grp[grp["Field"]==f]
    with tab1:
        fig=px.line(sub,x="Tanggal",y="Scaled",color="Broker",title=f"{f} ({unit})")
        fig.update_traces(mode="lines" if mode=="Daily" else "lines+markers")
        fig.update_layout(hovermode="x unified",yaxis_tickformat=".2s")
        st.plotly_chart(fig,use_container_width=True)
    with tab2:
        fig=px.line(sub,x="Tanggal",y="Pct",color="Broker",title=f"{f} % share")
        fig.update_traces(mode="lines" if mode=="Daily" else "lines+markers")
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig,use_container_width=True)
