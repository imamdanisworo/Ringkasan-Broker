import os
import re
import uuid
import tempfile
import logging
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from huggingface_hub import HfApi, hf_hub_download, upload_file
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# =========================
# Basic setup
# =========================
logging.basicConfig(level=logging.WARNING)
st.set_page_config(page_title="Ringkasan Broker", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = os.getenv("HF_TOKEN")
api = HfApi()

# =========================
# Utilities
# =========================
SAFE_INT = 2**53 - 1
BROKER_REQ_COLS = ["Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"]
NUMERIC_COLS = ["Volume", "Nilai", "Frekuensi"]
BROKER_COL = "Broker"
DATE_COL = "Tanggal"

# Fix sorting: force JS to always treat values as Number even if strings
JS_NUMBER_GETTER = "typeof value === 'string' ? Number(value) : value"

def aggrid_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert problematic types to JS-serializable values for AG Grid."""
    out = df.copy()
    # ints -> safe: use string if exceeds JS safe integer
    for c in out.select_dtypes(include=["int64", "Int64", "uint64"]).columns:
        out[c] = out[c].apply(
            lambda v: None if pd.isna(v) else (str(int(v)) if abs(int(v)) > SAFE_INT else int(v))
        )
    # datetime -> timezone-naive
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = pd.to_datetime(out[c]).dt.tz_localize(None)
    return out

def build_grid(df: pd.DataFrame, config_cb=None, height=400):
    """Generic AG Grid builder with optional column config callback."""
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(enabled=False)
    gb.configure_default_column(groupable=False, value=True, enableRowGroup=False, editable=False, resizable=True, flex=1)
    gb.configure_grid_options(domLayout='normal', suppressHorizontalScroll=False)
    if config_cb:
        config_cb(gb)
    grid_options = gb.build()
    col1, col2, col3 = st.columns([0.1, 0.8, 0.1])
    with col2:
        AgGrid(
            aggrid_safe(df),
            gridOptions=grid_options,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            fit_columns_on_grid_load=True,
            enable_enterprise_modules=True,
            height=height,
            width='100%',
            reload_data=False
        )

def format_hover_value(v: float) -> str:
    if v >= 1_000_000_000_000:
        return f"{v / 1_000_000_000_000:.4f}T"
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.4f}B"
    return f"{v:,.0f}"

def create_line_chart(df: pd.DataFrame, x: str, y: str, color: str, title: str, percentage=False):
    if df.empty:
        return
    df = df.sort_values(x)
    colors = ['#1f77b4', '#ff7f0e', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#aec7e8', '#ffbb78']
    fig = px.line(df, x=x, y=y, color=color, title=title, markers=True, color_discrete_sequence=colors)

    # rich hover
    if not percentage:
        for trace in fig.data:
            name = trace.name
            sub = df[df[color] == name]
            hover_texts = [
                f"<b>{name}</b><br>Tanggal: {d}<br>Nilai: {format_hover_value(v)}"
                for d, v in zip(sub[x].dt.strftime('%Y-%m-%d'), sub[y])
            ]
            trace.update(marker=dict(size=6), hovertemplate="%{text}<extra></extra>", text=hover_texts, legendgroup=name)
    else:
        for trace in fig.data:
            name = trace.name
            trace.update(marker=dict(size=6), hovertemplate=f"<b>{name}</b><br>Tanggal: %{{x}}<br>Kontribusi: %{{y:.2f}}%<extra></extra>", legendgroup=name)

    # add min/max markers
    for name in df[color].unique():
        sub = df[df[color] == name]
        if len(sub) < 1:
            continue
        min_idx, max_idx = sub[y].idxmin(), sub[y].idxmax()
        min_date, min_val = sub.loc[min_idx, x], float(sub.loc[min_idx, y])
        max_date, max_val = sub.loc[max_idx, x], float(sub.loc[max_idx, y])

        if percentage:
            fig.add_scatter(x=[min_date], y=[min_val], mode="markers", marker=dict(color="red", size=6),
                            name=f"{name} (Min %)", showlegend=False,
                            hovertemplate=f"<b>{name}</b><br>Tanggal: {min_date.strftime('%Y-%m-%d')}<br>Terendah: {min_val:.2f}%<extra></extra>")
            fig.add_scatter(x=[max_date], y=[max_val], mode="markers", marker=dict(color="green", size=6),
                            name=f"{name} (Max %)", showlegend=False,
                            hovertemplate=f"<b>{name}</b><br>Tanggal: {max_date.strftime('%Y-%m-%d')}<br>Tertinggi: {max_val:.2f}%<extra></extra>")
        else:
            fig.add_scatter(x=[min_date], y=[min_val], mode="markers", marker=dict(color="red", size=6),
                            name=f"{name} (Min)", showlegend=False,
                            hovertemplate=f"<b>{name}</b><br>Tanggal: {min_date.strftime('%Y-%m-%d')}<br>Terendah: {format_hover_value(min_val)}<extra></extra>")
            fig.add_scatter(x=[max_date], y=[max_val], mode="markers", marker=dict(color="green", size=6),
                            name=f"{name} (Max)", showlegend=False,
                            hovertemplate=f"<b>{name}</b><br>Tanggal: {max_date.strftime('%Y-%m-%d')}<br>Tertinggi: {format_hover_value(max_val)}<extra></extra>")

    fig.update_layout(xaxis_title="Tanggal", yaxis_title=("Percentage (%)" if percentage else "Nilai"), hovermode="closest")
    st.plotly_chart(fig, use_container_width=True)

# =========================
# Column configs for tables
# =========================
def _main_table_cols(gb: GridOptionsBuilder):
    gb.configure_column("No", width=80, pinned="left", type=["numericColumn"], flex=0,
                        valueGetter=JS_NUMBER_GETTER, sort="asc", sortingOrder=["asc","desc"])
    gb.configure_column(DATE_COL, minWidth=150, pinned="left", type=["dateColumn"], flex=1,
                        valueFormatter="new Date(value).toLocaleDateString('id-ID', {day: 'numeric', month: 'short', year: 'numeric'})")
    gb.configure_column(BROKER_COL, minWidth=300, pinned="left", flex=3)
    gb.configure_column("Field", minWidth=120, flex=1)
    gb.configure_column("Value", minWidth=200, type=["numericColumn"], flex=2,
                        valueGetter=JS_NUMBER_GETTER,
                        valueFormatter="'Rp ' + Number(value).toLocaleString()", headerName="Nilai")
    gb.configure_column("Percentage", minWidth=150, type=["numericColumn"], flex=1,
                        valueGetter=JS_NUMBER_GETTER,
                        valueFormatter="Number(value).toFixed(2) + '%'", headerName="Market Share (%)")

def _rank_cols_value(gb: GridOptionsBuilder, value_col: str):
    gb.configure_column("Peringkat", width=100, pinned="left", type=["numericColumn"], flex=0,
                        valueGetter=JS_NUMBER_GETTER, sort="asc", sortingOrder=["asc","desc"])
    gb.configure_column(BROKER_COL, minWidth=350, pinned="left", flex=3)
    gb.configure_column(value_col, minWidth=250, type=["numericColumn"], flex=2,
                        valueGetter=JS_NUMBER_GETTER,
                        valueFormatter=("'Rp ' + Number(value).toLocaleString()" if value_col == "Nilai" else "Number(value).toLocaleString()"))
    gb.configure_column("Market Share", minWidth=180, type=["numericColumn"], flex=1,
        valueGetter=JS_NUMBER_GETTER, valueFormatter="Number(value).toFixed(2) + '%'")

# =========================
# Session state
# =========================
if "upload_key" not in st.session_state:
    st.session_state.upload_key = str(uuid.uuid4())
if st.session_state.get("reset_upload_key"):
    st.session_state.upload_key = str(uuid.uuid4())
    st.session_state.reset_upload_key = False
if "file_load_status" not in st.session_state:
    st.session_state.file_load_status = {"success": 0, "total": 0, "failed": []}
if "refresh_trigger" not in st.session_state:
    st.session_state.refresh_trigger = False

# =========================
# Upload area
# =========================
@st.cache_data(ttl=3600)
def list_existing_files():
    try:
        return set(api.list_repo_files(REPO_ID, repo_type="dataset"))
    except Exception as e:
        st.error(f"❌ Error accessing repository: {e}")
        return set()

st.subheader("📤 Upload File Excel")
st.markdown("Format nama file: `YYYYMMDD_*.xlsx`. Kolom wajib: `Kode Perusahaan`, `Nama Perusahaan`, `Volume`, `Nilai`, `Frekuensi`.")

uploaded_files = st.file_uploader("Pilih file Excel", type=["xlsx"], accept_multiple_files=True, key=st.session_state.upload_key)
existing_files = list_existing_files()

if uploaded_files:
    upload_success = False
    for f in uploaded_files:
        try:
            m = re.search(r"(\d{8})", f.name)
            if not m:
                st.warning(f"⚠️ {f.name} dilewati: nama file tidak mengandung tanggal (YYYYMMDD).")
                continue
            try:
                datetime.strptime(m.group(1), "%Y%m%d")
            except ValueError:
                st.warning(f"⚠️ {f.name} dilewati: format tanggal tidak valid.")
                continue

            try:
                df = pd.read_excel(f, sheet_name="Sheet1")
            except Exception as e:
                st.warning(f"⚠️ {f.name} dilewati: tidak dapat membaca Excel - {e}")
                continue
            if df.empty:
                st.warning(f"⚠️ {f.name} dilewati: file kosong.")
                continue

            df.columns = df.columns.str.strip()
            if not set(BROKER_REQ_COLS).issubset(df.columns):
                miss = set(BROKER_REQ_COLS) - set(df.columns)
                st.warning(f"⚠️ {f.name} dilewati: kolom kurang: {miss}")
                continue

            for col in NUMERIC_COLS:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            if f.name in existing_files:
                st.warning(f"⚠️ File '{f.name}' sudah ada dan akan ditimpa.")

            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                tmp.write(f.getbuffer())
                tmp_path = tmp.name

            try:
                upload_file(
                    path_or_fileobj=tmp_path,
                    path_in_repo=f.name,
                    repo_id=REPO_ID,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                st.success(f"✅ Berhasil diunggah: {f.name}")
                upload_success = True
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        except Exception as e:
            st.error(f"❌ Gagal memproses {f.name}: {e}")

    if upload_success:
        st.session_state.reset_upload_key = True
        st.cache_data.clear()
        st.rerun()

# =========================
# Loading all Excel
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def load_all_excel():
    try:
        all_files = api.list_repo_files(REPO_ID, repo_type="dataset")
    except Exception as e:
        st.error(f"❌ Error accessing repository: {e}")
        return pd.DataFrame(), 0, []

    xlsx = sorted([f for f in all_files if f.endswith(".xlsx")], reverse=True)
    if not xlsx:
        return pd.DataFrame(), 0, []

    all_data, failed = [], []
    progress_bar = st.progress(0)
    status = st.empty()
    total = len(xlsx)

    for i, file in enumerate(xlsx, start=1):
        try:
            m = re.search(r"(\d{8})", file)
            if not m:
                failed.append(file); continue
            try:
                file_date = datetime.strptime(m.group(1), "%Y%m%d").date()
            except ValueError:
                failed.append(file); continue

            cache_dir = "./hf_cache"; os.makedirs(cache_dir, exist_ok=True)
            cache_path = os.path.join(cache_dir, file)
            if not os.path.exists(cache_path):
                try:
                    path = hf_hub_download(REPO_ID, filename=file, repo_type="dataset",
                                           token=HF_TOKEN, local_dir=cache_dir, force_download=False)
                except Exception:
                    failed.append(file); continue
            else:
                path = cache_path

            try:
                df = pd.read_excel(path, sheet_name="Sheet1", engine="openpyxl")
            except Exception:
                failed.append(file); continue

            if df.empty: failed.append(file); continue

            df.columns = df.columns.str.strip()
            if not set(BROKER_REQ_COLS).issubset(df.columns):
                failed.append(file); continue

            df = df[BROKER_REQ_COLS].copy()
            for c in NUMERIC_COLS:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

            df["Kode Perusahaan"] = df["Kode Perusahaan"].astype(str).str.strip()
            df["Nama Perusahaan"] = df["Nama Perusahaan"].astype(str).str.strip()
            df = df[(df["Kode Perusahaan"] != "") & (df["Nama Perusahaan"] != "")]
            if df.empty: failed.append(file); continue

            df[DATE_COL] = file_date
            all_data.append(df)
        finally:
            progress_bar.progress(i / total)
            status.text(f"📁 Loading {i}/{total} files...")

    progress_bar.empty(); status.empty()
    if not all_data: return pd.DataFrame(), total, failed

    combined = pd.concat(all_data, ignore_index=True, copy=False)
    if combined.empty: return pd.DataFrame(), total, failed

    # Broker name mapping
    latest_names = (
        combined.sort_values(DATE_COL)
        .drop_duplicates("Kode Perusahaan", keep="last")
        .set_index("Kode Perusahaan")["Nama Perusahaan"]
    )
    combined[BROKER_COL] = combined["Kode Perusahaan"].map(latest_names).fillna("Unknown")
    combined[BROKER_COL] = combined["Kode Perusahaan"] + "_" + combined[BROKER_COL]

    # Market total row per date
    market_agg = combined.groupby(DATE_COL, as_index=False)[NUMERIC_COLS].sum()
    market_agg[BROKER_COL] = "Total Market"
    market_agg["FieldSource"] = "Generated"
    market_agg["Kode Perusahaan"] = "TOTAL"
    market_agg["Nama Perusahaan"] = "Total Market"
    combined = pd.concat([combined, market_agg], ignore_index=True, copy=False)

    return combined, total, failed

with st.spinner("Loading data..."):
    try:
        combined_df, file_count, failed_files = load_all_excel()
        st.session_state.file_load_status.update(
            {"total": file_count, "success": file_count - len(failed_files), "failed": failed_files}
        )
    except Exception as e:
        combined_df = pd.DataFrame()
        st.error(f"⚠️ Gagal memuat data: {e}")

# Status
if st.session_state.file_load_status["total"] > 0:
    st.markdown("---")
    st.markdown("##### 📁 Status Pemuatan File:")
    st.markdown(f"✅ Berhasil dimuat: {st.session_state.file_load_status['success']} dari {st.session_state.file_load_status['total']} file")
    if st.session_state.file_load_status["failed"]:
        with st.expander("⚠️ File yang gagal dimuat"):
            st.write(st.session_state.file_load_status["failed"])

if st.button("🔁 Refresh Data"):
    st.cache_data.clear()
    st.session_state.refresh_trigger = True
    st.rerun()
st.session_state.refresh_trigger = False

# =========================
# Main
# =========================
if combined_df.empty:
    st.info("⬆️ Silakan unggah file Excel terlebih dahulu.")
    st.stop()

combined_df[DATE_COL] = pd.to_datetime(combined_df[DATE_COL])

with st.expander("⚙️ Filter Data", expanded=True):
    left, right = st.columns(2)
    with left:
        brokers = sorted(combined_df[BROKER_COL].unique())
        default_brokers = ["Total Market"] if "Total Market" in brokers else []
        selected_brokers = st.multiselect("📌 Pilih Broker", brokers, default=default_brokers)
    with right:
        selected_fields = st.multiselect("📊 Pilih Jenis Data", NUMERIC_COLS, default=["Nilai"])

    display_mode = st.radio("🗓️ Mode Tampilan", ["Daily", "Monthly", "Yearly"], horizontal=True)

    min_date, max_date = combined_df[DATE_COL].min().date(), combined_df[DATE_COL].max().date()
    today = datetime.today(); year_start = datetime(today.year, 1, 1).date()

    date_from = date_to = None
    if display_mode == "Daily":
        dr = st.date_input("Pilih Rentang Tanggal", value=(year_start, max_date), min_value=min_date, max_value=max_date,
                           help="Klik sekali untuk tanggal mulai, klik kedua untuk tanggal selesai")
        if isinstance(dr, tuple) and len(dr) == 2:
            date_from, date_to = dr
    elif display_mode == "Monthly":
        periods = combined_df[DATE_COL].dt.to_period("M")
        years = sorted(set(p.year for p in periods.unique()))
        sel_years = st.multiselect("Pilih Tahun", years, default=[today.year])
        months = sorted([p for p in periods.unique() if p.year in sel_years])
        sel_months = st.multiselect("Pilih Bulan", months, default=months)
        if sel_months:
            date_from = min(p.to_timestamp() for p in sel_months)
            date_to = max((p + 1).to_timestamp() - pd.Timedelta(days=1) for p in sel_months)
    else:  # Yearly
        years = sorted(combined_df[DATE_COL].dt.year.unique())
        sel_years = st.multiselect("Pilih Tahun", years, default=[today.year])
        if sel_years:
            date_from = datetime(min(sel_years), 1, 1).date()
            date_to = datetime(max(sel_years), 12, 31).date()

# validations
if not selected_brokers:
    st.warning("❗ Silakan pilih minimal satu broker."); st.stop()
if not selected_fields:
    st.warning("❗ Silakan pilih minimal satu jenis data."); st.stop()
if not date_from or not date_to:
    st.warning("❗ Rentang tanggal tidak valid."); st.stop()

st.markdown("### 📊 Hasil Ringkasan")

# Filter
filtered = combined_df[
    (combined_df[DATE_COL] >= pd.to_datetime(date_from)) &
    (combined_df[DATE_COL] <= pd.to_datetime(date_to)) &
    (combined_df[BROKER_COL].isin(selected_brokers))
].copy()

if filtered.empty:
    st.warning("❗ Tidak ada data untuk filter yang dipilih.")
    st.stop()

# Melt + percentage vs Total Market
melted = filtered.melt(id_vars=[DATE_COL, BROKER_COL], value_vars=selected_fields,
                       var_name="Field", value_name="Value")

tm = combined_df[combined_df[BROKER_COL] == "Total Market"].melt(
    id_vars=[DATE_COL, BROKER_COL], value_vars=selected_fields,
    var_name="Field", value_name="TotalMarketValue"
)[[DATE_COL, "Field", "TotalMarketValue"]]

merged = pd.merge(melted, tm, on=[DATE_COL, "Field"], how="left")
merged["Percentage"] = (merged["Value"] / merged["TotalMarketValue"].replace({0: pd.NA})) * 100
merged["Percentage"] = merged["Percentage"].fillna(0.0)

display_df = merged.copy()

# Monthly / Yearly aggregation for display
if display_mode == "Monthly":
    display_df["P"] = display_df[DATE_COL].dt.to_period("M")
    a = display_df.groupby(["P", BROKER_COL, "Field"])["Value"].sum().reset_index()
    a[DATE_COL] = a["P"].dt.end_time.dt.normalize(); a.drop(columns="P", inplace=True)

    market = combined_df[(combined_df[DATE_COL] >= pd.to_datetime(date_from)) & (combined_df[DATE_COL] <= pd.to_datetime(date_to))].copy()
    market["P"] = market[DATE_COL].dt.to_period("M")
    market = market[market[BROKER_COL] == "Total Market"].melt(id_vars=["P", BROKER_COL], value_vars=selected_fields,
                                                               var_name="Field", value_name="MarketTotal")
    market = market.groupby(["P", "Field"])["MarketTotal"].sum().reset_index()
    market[DATE_COL] = market["P"].dt.end_time.dt.normalize(); market.drop(columns="P", inplace=True)

    display_df = pd.merge(a, market, on=[DATE_COL, "Field"], how="left")
    display_df["Percentage"] = display_df.apply(
        lambda r: 100.0 if r[BROKER_COL] == "Total Market"
        else (r["Value"] / r["MarketTotal"] * 100 if r.get("MarketTotal", 0) else 0.0), axis=1
    )

elif display_mode == "Yearly":
    display_df["P"] = display_df[DATE_COL].dt.to_period("Y")
    a = display_df.groupby(["P", BROKER_COL, "Field"])["Value"].sum().reset_index()
    a[DATE_COL] = a["P"].dt.end_time.dt.normalize(); a.drop(columns="P", inplace=True)

    market = combined_df[(combined_df[DATE_COL] >= pd.to_datetime(date_from)) & (combined_df[DATE_COL] <= pd.to_datetime(date_to))].copy()
    market["P"] = market[DATE_COL].dt.to_period("Y")
    market = market[market[BROKER_COL] == "Total Market"].melt(id_vars=["P", BROKER_COL], value_vars=selected_fields,
                                                               var_name="Field", value_name="MarketTotal")
    market = market.groupby(["P", "Field"])["MarketTotal"].sum().reset_index()
    market[DATE_COL] = market["P"].dt.end_time.dt.normalize(); market.drop(columns="P", inplace=True)

    display_df = pd.merge(a, market, on=[DATE_COL, "Field"], how="left")
    display_df["Percentage"] = display_df.apply(
        lambda r: 100.0 if r[BROKER_COL] == "Total Market"
        else (r["Value"] / r["MarketTotal"] * 100 if r.get("MarketTotal", 0) else 0.0), axis=1
    )

# Table for display + download
display_df["Formatted Value"] = display_df["Value"].apply(lambda x: f"{x:,.0f}")
display_df["Formatted %"] = display_df["Percentage"].apply(lambda x: f"{x:.2f}%")

table_df = display_df[[DATE_COL, BROKER_COL, "Field", "Formatted Value", "Formatted %"]].copy()
table_df["Tanggal Display"] = display_df[DATE_COL].dt.strftime(
    '%-d %b %Y' if display_mode == "Daily" else '%b %Y' if display_mode == "Monthly" else '%Y'
)
table_df["Sort_Priority"] = table_df[BROKER_COL].eq("Total Market").map({True: 0, False: 1})
table_df = (table_df.sort_values([DATE_COL, "Sort_Priority", BROKER_COL], ascending=[False, True, True])
                     .drop(columns="Sort_Priority").reset_index(drop=True))

main_table = table_df[[DATE_COL, BROKER_COL, "Field", "Formatted Value", "Formatted %"]].copy()
main_table["No"] = range(1, len(main_table) + 1)
main_table = pd.merge(
    main_table,
    display_df[[DATE_COL, BROKER_COL, "Field", "Value", "Percentage"]],
    on=[DATE_COL, BROKER_COL, "Field"],
    how="left"
)[["No", DATE_COL, BROKER_COL, "Field", "Value", "Percentage"]]

build_grid(main_table, config_cb=_main_table_cols, height=400)

# Download CSV
download_df = table_df[[DATE_COL, BROKER_COL, "Field", "Formatted Value", "Formatted %"]].copy()
download_df.columns = ["Tanggal", "Broker", "Field", "Value", "%"]
st.download_button("⬇️ Unduh Tabel CSV", data=download_df.to_csv(index=False).encode("utf-8"),
                   file_name="broker_summary.csv", mime="text/csv")

# Charts
tab1, tab2 = st.tabs(["📈 Nilai", "📊 Kontribusi Terhadap Total (%)"])
with tab1:
    for f in selected_fields:
        create_line_chart(display_df[display_df["Field"] == f], DATE_COL, "Value", BROKER_COL, f"{f} dari waktu ke waktu")

with tab2:
    for f in selected_fields:
        create_line_chart(display_df[display_df["Field"] == f], DATE_COL, "Percentage", BROKER_COL,
                          f"Kontribusi {f} (%) dari waktu ke waktu", percentage=True)

# =========================
# Ranking
# =========================
st.markdown("---")
st.header("🏆 Top Broker Ranking")

mode = st.radio("📅 Mode Tanggal untuk Ranking", ["Harian", "Bulanan"], horizontal=True)
combined_df[DATE_COL] = pd.to_datetime(combined_df[DATE_COL])

filtered_rank_df = pd.DataFrame()
if mode == "Harian":
    min_rank_date = datetime(datetime.today().year, 1, 1).date()
    max_rank_date = combined_df[DATE_COL].max().date()
    dr = st.date_input("Pilih Rentang Tanggal untuk Ranking",
                       value=(min_rank_date, max_rank_date),
                       min_value=min_rank_date, max_value=max_rank_date,
                       help="Klik sekali untuk tanggal mulai, klik kedua untuk tanggal selesai",
                       key="rank_date_range")
    if isinstance(dr, tuple) and len(dr) == 2:
        start, end = dr
        filtered_rank_df = combined_df[
            (combined_df[DATE_COL] >= pd.to_datetime(start)) &
            (combined_df[DATE_COL] <= pd.to_datetime(end)) &
            (combined_df[BROKER_COL] != "Total Market")
        ].copy()
else:
    combined_df["MonthPeriod"] = combined_df[DATE_COL].dt.to_period("M")
    all_months = sorted(combined_df["MonthPeriod"].unique())
    all_years = sorted(set(m.year for m in all_months))
    current_year = datetime.today().year
    years_sel = st.multiselect("📅 Pilih Tahun", options=all_years, default=[current_year], key="rank_year_select")
    month_opts = [m for m in all_months if m.year in years_sel]
    months_sel = st.multiselect("📆 Pilih Bulan (bisa lebih dari satu)", options=month_opts, default=month_opts,
                                format_func=lambda m: m.strftime("%b %Y"), key="selected_months")
    if months_sel:
        filtered_rank_df = combined_df[
            combined_df["MonthPeriod"].isin(months_sel) & (combined_df[BROKER_COL] != "Total Market")
        ].copy()

def generate_ranking(df: pd.DataFrame, col: str):
    if df.empty:
        return pd.DataFrame(), 0.0
    ranked = (df.groupby(BROKER_COL)[col].sum().sort_values(ascending=False).reset_index())
    ranked["Peringkat"] = range(1, len(ranked) + 1)
    total = ranked[col].sum()
    ranked["Market Share"] = (ranked[col] / total * 100) if total > 0 else 0.0
    return ranked[["Peringkat", BROKER_COL, col, "Market Share"]], total

def _rank_cols_value(gb: GridOptionsBuilder, value_col: str):
    gb.configure_column("Peringkat", width=100, pinned="left", type=["numericColumn"], flex=0,
                        valueGetter=JS_NUMBER_GETTER, sort="asc", sortingOrder=["asc","desc"])
    gb.configure_column(BROKER_COL, minWidth=350, pinned="left", flex=3)
    gb.configure_column(value_col, minWidth=250, type=["numericColumn"], flex=2,
                        valueGetter=JS_NUMBER_GETTER,
                        valueFormatter=("'Rp ' + Number(value).toLocaleString()" if value_col == "Nilai" else "Number(value).toLocaleString()"))
    gb.configure_column("Market Share", minWidth=180, type=["numericColumn"], flex=1,
                        valueGetter=JS_NUMBER_GETTER,
                        valueFormatter="Number(value).toFixed(2) + '%'")

if not filtered_rank_df.empty:
    tab_val, tab_freq, tab_vol = st.tabs(["💰 Berdasarkan Nilai", "📈 Berdasarkan Frekuensi", "📊 Berdasarkan Volume"])

    with tab_val:
        st.subheader("🔝 Peringkat Berdasarkan Nilai")
        df_val, tot_val = generate_ranking(filtered_rank_df, "Nilai")
        if not df_val.empty:
            build_grid(df_val, config_cb=lambda gb: _rank_cols_value(gb, "Nilai"))
            st.markdown(f"**Total Nilai Seluruh Broker:** Rp {tot_val:,.0f}")
        else:
            st.warning("❗ Tidak ada data untuk ranking nilai.")

    with tab_freq:
        st.subheader("🔝 Peringkat Berdasarkan Frekuensi")
        df_freq, tot_freq = generate_ranking(filtered_rank_df, "Frekuensi")
        if not df_freq.empty:
            build_grid(df_freq, config_cb=lambda gb: _rank_cols_value(gb, "Frekuensi"))
            st.markdown(f"**Total Frekuensi Seluruh Broker:** {tot_freq:,.0f} transaksi")
        else:
            st.warning("❗ Tidak ada data untuk ranking frekuensi.")

    with tab_vol:
        st.subheader("🔝 Peringkat Berdasarkan Volume")
        df_vol, tot_vol = generate_ranking(filtered_rank_df, "Volume")
        if not df_vol.empty:
            build_grid(df_vol, config_cb=lambda gb: _rank_cols_value(gb, "Volume"))
            st.markdown(f"**Total Volume Seluruh Broker:** {tot_vol:,.0f} lot")
        else:
            st.warning("❗ Tidak ada data untuk ranking volume.")
elif mode == "Harian":
    st.info("📌 Silakan pilih kedua tanggal (mulai dan selesai) untuk melihat data ranking.")
else:
    st.info("📌 Tidak ada data untuk rentang yang dipilih.")
