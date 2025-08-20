
import streamlit as st
import pandas as pd
import re
import os
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
import uuid
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
import tempfile
import logging
import concurrent.futures
from threading import Lock

# ---------------------------------------------
# AG Grid safe helper (prevents BigInt errors)
# ---------------------------------------------
SAFE_INT = 2**53 - 1

def aggrid_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert int64/UInt64 to JS-safe values before passing to AgGrid."""
    out = df.copy()
    int_like = out.select_dtypes(include=["int64", "Int64", "uint64"]).columns
    for c in int_like:
        out[c] = out[c].apply(
            lambda v: None if pd.isna(v)
            else (str(int(v)) if abs(int(v)) > SAFE_INT else int(v))
        )
    # Ensure datetime is timezone-naive (ISO strings are fine for AgGrid)
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = pd.to_datetime(out[col]).dt.tz_localize(None)
    return out

# Configure logging
logging.basicConfig(level=logging.WARNING)

st.set_page_config(page_title="Ringkasan Broker", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = os.getenv("HF_TOKEN")

# Initialize session state
if "upload_key" not in st.session_state:
    st.session_state.upload_key = str(uuid.uuid4())
if "reset_upload_key" in st.session_state and st.session_state.reset_upload_key:
    st.session_state.upload_key = str(uuid.uuid4())
    st.session_state.reset_upload_key = False
if "file_load_status" not in st.session_state:
    st.session_state.file_load_status = {"success": 0, "total": 0, "failed": []}
if "refresh_trigger" not in st.session_state:
    st.session_state.refresh_trigger = False

api = HfApi()

@st.cache_data(ttl=3600)
def list_existing_files():
    try:
        return set(api.list_repo_files(REPO_ID, repo_type="dataset"))
    except Exception as e:
        st.error(f"❌ Error accessing repository: {e}")
        return set()

def configure_aggrid_table(df, table_type="main"):
    """Consolidated AgGrid configuration function"""
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(enabled=False)
    gb.configure_default_column(groupable=False, value=True, enableRowGroup=False, editable=False, resizable=True)
    gb.configure_grid_options(domLayout='normal', suppressHorizontalScroll=False)
    
    if table_type == "main":
        gb.configure_column("No", type=["numericColumn"], width=80)
        gb.configure_column("Tanggal", type=["dateColumn"], minWidth=150,
                           valueFormatter="new Date(value).toLocaleDateString('id-ID', {day: 'numeric', month: 'short', year: 'numeric'})")
        gb.configure_column("Broker", minWidth=300)
        gb.configure_column("Field", minWidth=120)
        gb.configure_column("Value", type=["numericColumn"], minWidth=200,
                           valueFormatter="'Rp ' + Number(value).toLocaleString()", headerName="Nilai")
        gb.configure_column("Percentage", type=["numericColumn"], minWidth=150,
                           valueFormatter="Number(value).toFixed(2) + '%'", headerName="Market Share (%)")
    
    elif table_type == "ranking":
        gb.configure_column("Peringkat", width=100, type=["numericColumn"], sort="asc")
        gb.configure_column("Broker", minWidth=350)
        
        # Dynamic configuration based on columns present
        if "Nilai" in df.columns:
            gb.configure_column("Nilai", minWidth=250, type=["numericColumn"],
                               valueFormatter="'Rp ' + Number(value).toLocaleString()")
        if "Frekuensi" in df.columns:
            gb.configure_column("Frekuensi", minWidth=200, type=["numericColumn"],
                               valueFormatter="Number(value).toLocaleString()")
        if "Volume" in df.columns:
            gb.configure_column("Volume", minWidth=200, type=["numericColumn"],
                               valueFormatter="Number(value).toLocaleString()")
        
        gb.configure_column("Market Share", minWidth=180, type=["numericColumn"],
                           valueFormatter="Number(value).toFixed(2) + '%'")
    
    return gb.build()

def safe_numeric_conversion(df, columns):
    """Safely convert columns to numeric types"""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
    return df

st.subheader("📤 Upload File Excel")
st.markdown("Format nama file: `YYYYMMDD_*.xlsx`. Kolom wajib: `Kode Perusahaan`, `Nama Perusahaan`, `Volume`, `Nilai`, `Frekuensi`.")

uploaded_files = st.file_uploader(
    "Pilih file Excel", 
    type=["xlsx"], 
    accept_multiple_files=True,
    key=st.session_state.upload_key
)

existing_files = list_existing_files()
upload_success = False

if uploaded_files:
    for file in uploaded_files:
        try:
            match = re.search(r"(\d{8})", file.name)
            if not match:
                st.warning(f"⚠️ {file.name} dilewati: nama file tidak mengandung tanggal (YYYYMMDD).")
                continue
            
            # Validate date
            try:
                file_date = datetime.strptime(match.group(1), "%Y%m%d").date()
            except ValueError:
                st.warning(f"⚠️ {file.name} dilewati: format tanggal tidak valid.")
                continue

            # Read and validate Excel file
            try:
                df = pd.read_excel(file, sheet_name="Sheet1")
            except Exception as e:
                st.warning(f"⚠️ {file.name} dilewati: tidak dapat membaca file Excel - {e}")
                continue
            
            if df.empty:
                st.warning(f"⚠️ {file.name} dilewati: file kosong.")
                continue
            
            df.columns = df.columns.str.strip()
            required_columns = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
            if not required_columns.issubset(df.columns):
                missing_cols = required_columns - set(df.columns)
                st.warning(f"⚠️ {file.name} dilewati: kolom tidak lengkap. Missing: {missing_cols}")
                continue

            # Validate and convert numeric columns
            numeric_cols = ["Volume", "Nilai", "Frekuensi"]
            for col in numeric_cols:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        if df[col].isna().any():
                            st.warning(f"⚠️ {file.name}: Beberapa nilai di kolom {col} tidak valid dan diubah ke 0")
                            df[col] = df[col].fillna(0)
                    except:
                        st.warning(f"⚠️ {file.name} dilewati: kolom {col} tidak dapat dikonversi ke angka.")
                        continue

            if file.name in existing_files:
                st.warning(f"⚠️ File '{file.name}' sudah ada dan akan ditimpa.")

            # Create temporary file for upload
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
                tmp_file.write(file.getbuffer())
                tmp_file_path = tmp_file.name

            try:
                upload_file(
                    path_or_fileobj=tmp_file_path,
                    path_in_repo=file.name,
                    repo_id=REPO_ID,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                st.success(f"✅ Berhasil diunggah: {file.name}")
                upload_success = True
            finally:
                # Clean up temporary file
                if os.path.exists(tmp_file_path):
                    os.unlink(tmp_file_path)

        except Exception as e:
            st.error(f"❌ Gagal memproses {file.name}: {e}")

    if upload_success:
        st.session_state.reset_upload_key = True
        st.cache_data.clear()
        st.rerun()

@st.cache_data(ttl=3600, show_spinner=False)
def load_all_excel():
    """Optimized loading function with enhanced error handling"""
    try:
        all_files = api.list_repo_files(REPO_ID, repo_type="dataset")
    except Exception as e:
        st.error(f"❌ Error accessing repository: {e}")
        return pd.DataFrame(), 0, []
    
    xlsx_files = [f for f in all_files if f.endswith(".xlsx")]
    if not xlsx_files:
        return pd.DataFrame(), 0, []

    xlsx_files.sort(reverse=True)
    all_data = []
    failed_files = []
    lock = Lock()

    progress_bar = st.progress(0)
    status_text = st.empty()
    total_files = len(xlsx_files)
    processed_count = 0

    def process_file(file):
        """Process a single Excel file with error handling"""
        try:
            match = re.search(r"(\d{8})", file)
            if not match:
                return None, file
            
            try:
                file_date = datetime.strptime(match.group(1), "%Y%m%d").date()
            except ValueError:
                return None, file

            # Check if file exists in cache first
            cache_dir = "./hf_cache"
            os.makedirs(cache_dir, exist_ok=True)
            cache_path = os.path.join(cache_dir, file)
            
            if not os.path.exists(cache_path):
                try:
                    hf_hub_download(
                        REPO_ID, 
                        filename=file, 
                        repo_type="dataset", 
                        token=HF_TOKEN,
                        local_dir=cache_dir,
                        force_download=False
                    )
                except Exception:
                    return None, file

            # Read Excel with error handling
            try:
                df = pd.read_excel(cache_path, sheet_name="Sheet1", engine='openpyxl')
            except Exception:
                return None, file
            
            if df.empty:
                return None, file
            
            # Clean and validate data
            df.columns = df.columns.str.strip()
            required_columns = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
            if not required_columns.issubset(df.columns):
                return None, file
            
            # Filter only required columns and clean data
            df = df[list(required_columns)].copy()
            
            # Convert numeric columns with error handling
            numeric_cols = ["Volume", "Nilai", "Frekuensi"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            
            # Clean string columns
            df["Kode Perusahaan"] = df["Kode Perusahaan"].astype(str).str.strip()
            df["Nama Perusahaan"] = df["Nama Perusahaan"].astype(str).str.strip()
            
            # Remove rows with invalid data
            df = df[
                (df["Kode Perusahaan"] != '') & 
                (df["Nama Perusahaan"] != '') &
                (df[numeric_cols] >= 0).all(axis=1)
            ]
            
            if df.empty:
                return None, file
            
            df["Tanggal"] = file_date
            return df, None

        except Exception:
            return None, file

    # Process files with controlled concurrency
    max_workers = min(8, len(xlsx_files), os.cpu_count() or 1)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_file, file) for file in xlsx_files]
        
        for future in concurrent.futures.as_completed(futures):
            with lock:
                processed_count += 1
                progress = processed_count / total_files
                progress_bar.progress(progress)
                status_text.text(f"📁 Loading {processed_count}/{total_files} files...")
            
            try:
                df, failed_file = future.result()
                
                if df is not None and not df.empty:
                    with lock:
                        all_data.append(df)
                elif failed_file:
                    with lock:
                        failed_files.append(failed_file)
            except Exception:
                continue

    progress_bar.empty()
    status_text.empty()

    if not all_data:
        return pd.DataFrame(), len(xlsx_files), failed_files

    # Optimized concatenation
    try:
        combined = pd.concat(all_data, ignore_index=True, sort=False, copy=False)
    except Exception as e:
        st.error(f"❌ Error combining data: {e}")
        return pd.DataFrame(), len(xlsx_files), failed_files

    if combined.empty:
        return pd.DataFrame(), len(xlsx_files), failed_files

    # Create broker names mapping
    try:
        latest_names = (
            combined.sort_values("Tanggal")
            .drop_duplicates("Kode Perusahaan", keep="last")
            .set_index("Kode Perusahaan")["Nama Perusahaan"]
        )
        combined["Broker"] = combined["Kode Perusahaan"].map(latest_names).fillna('Unknown')
        combined["Broker"] = combined["Kode Perusahaan"] + "_" + combined["Broker"]
    except Exception as e:
        st.error(f"❌ Error creating broker mapping: {e}")
        combined["Broker"] = combined["Kode Perusahaan"] + "_" + combined["Nama Perusahaan"]

    # Create market aggregation
    try:
        market_aggregated = combined.groupby("Tanggal", as_index=False)[["Volume", "Nilai", "Frekuensi"]].sum()
        market_aggregated["Broker"] = "Total Market"
        market_aggregated["Kode Perusahaan"] = "TOTAL"
        market_aggregated["Nama Perusahaan"] = "Total Market"
        
        combined = pd.concat([combined, market_aggregated], ignore_index=True, sort=False, copy=False)
    except Exception as e:
        st.error(f"❌ Error creating market aggregation: {e}")

    return combined, len(xlsx_files), failed_files

# Load data with error handling
with st.spinner("Loading data..."):
    try:
        combined_df, file_count, failed_files = load_all_excel()
        st.session_state.file_load_status = {
            "total": file_count,
            "success": file_count - len(failed_files),
            "failed": failed_files
        }
    except Exception as e:
        combined_df = pd.DataFrame()
        st.error(f"⚠️ Gagal memuat data: {e}")

# Display file loading status
success_count = st.session_state.file_load_status["success"]
total_count = st.session_state.file_load_status["total"]
failed_files = st.session_state.file_load_status["failed"]

if total_count > 0:
    st.markdown("---")
    st.markdown("##### 📁 Status Pemuatan File:")
    st.markdown(f"✅ Berhasil dimuat: {success_count} dari {total_count} file")
    if failed_files:
        with st.expander("⚠️ File yang gagal dimuat"):
            st.write(failed_files)

if st.button("🔁 Refresh Data"):
    st.cache_data.clear()
    st.session_state.refresh_trigger = True
    st.rerun()

# Main application logic
if not combined_df.empty:
    combined_df["Tanggal"] = pd.to_datetime(combined_df["Tanggal"])

    with st.expander("⚙️ Filter Data", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            unique_brokers = sorted(combined_df["Broker"].unique())
            default_selection = ["Total Market"] if "Total Market" in unique_brokers else []
            selected_brokers = st.multiselect("📌 Pilih Broker", unique_brokers, default=default_selection)
        
        with col2:
            selected_fields = st.multiselect("📊 Pilih Jenis Data", ["Volume", "Nilai", "Frekuensi"], default=["Nilai"])

        display_mode = st.radio("🗓️ Mode Tampilan", ["Daily", "Monthly", "Yearly"], horizontal=True)

        min_date, max_date = combined_df["Tanggal"].min().date(), combined_df["Tanggal"].max().date()
        today = datetime.today()
        year_start = datetime(today.year, 1, 1).date()

        if display_mode == "Daily":
            date_range = st.date_input(
                "Pilih Rentang Tanggal",
                value=(year_start, max_date),
                min_value=min_date,
                max_value=max_date,
                help="Klik sekali untuk tanggal mulai, klik kedua untuk tanggal selesai"
            )
            if isinstance(date_range, tuple) and len(date_range) == 2:
                date_from, date_to = date_range
            else:
                date_from = date_to = None
        elif display_mode == "Monthly":
            all_months = combined_df["Tanggal"].dt.to_period("M")
            unique_years = sorted(set(m.year for m in all_months.unique()))
            selected_years = st.multiselect("Pilih Tahun", unique_years, default=[today.year])
            months = sorted([m for m in all_months.unique() if m.year in selected_years])
            selected_months = st.multiselect("Pilih Bulan", months, default=months)
            if selected_months:
                date_from = min(m.to_timestamp() for m in selected_months)
                date_to = max((m + 1).to_timestamp() - pd.Timedelta(days=1) for m in selected_months)
            else:
                date_from = date_to = None
        elif display_mode == "Yearly":
            years = sorted(combined_df["Tanggal"].dt.year.unique())
            selected_years = st.multiselect("Pilih Tahun", years, default=[today.year])
            if selected_years:
                date_from = datetime(min(selected_years), 1, 1).date()
                date_to = datetime(max(selected_years), 12, 31).date()
            else:
                date_from = date_to = None

    # Validation checks
    if not selected_brokers:
        st.warning("❗ Silakan pilih minimal satu broker.")
    elif not selected_fields:
        st.warning("❗ Silakan pilih minimal satu jenis data.")
    elif not date_from or not date_to:
        st.warning("❗ Rentang tanggal tidak valid.")
    else:
        st.markdown("### 📊 Hasil Ringkasan")

        filtered_df = combined_df[
            (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
            (combined_df["Tanggal"] <= pd.to_datetime(date_to)) &
            (combined_df["Broker"].isin(selected_brokers))
        ].copy()

        if filtered_df.empty:
            st.warning("❗ Tidak ada data untuk filter yang dipilih.")
        else:
            # Process data for display
            melted_df = filtered_df.melt(id_vars=["Tanggal", "Broker"], value_vars=selected_fields,
                                       var_name="Field", value_name="Value")

            total_market_df = combined_df[combined_df["Broker"] == "Total Market"].melt(
                id_vars=["Tanggal", "Broker"],
                value_vars=selected_fields,
                var_name="Field",
                value_name="TotalMarketValue"
            )
            total_market_df = total_market_df[["Tanggal", "Field", "TotalMarketValue"]]

            merged_df = pd.merge(melted_df, total_market_df, on=["Tanggal", "Field"], how="left")
            merged_df["Percentage"] = merged_df.apply(
                lambda row: (row["Value"] / row["TotalMarketValue"] * 100) if pd.notna(row["TotalMarketValue"]) and row["TotalMarketValue"] != 0 
                else 0.0, axis=1)

            display_df = merged_df.copy()

            # Handle different display modes with consolidated logic
            if display_mode in ["Monthly", "Yearly"]:
                period_col = "MonthPeriod" if display_mode == "Monthly" else "YearPeriod"
                period_freq = "M" if display_mode == "Monthly" else "Y"
                
                display_df[period_col] = display_df["Tanggal"].dt.to_period(period_freq)
                aggregated_df = display_df.groupby([period_col, "Broker", "Field"])["Value"].sum().reset_index()
                aggregated_df["Tanggal"] = aggregated_df[period_col].dt.end_time.dt.normalize()
                aggregated_df = aggregated_df.drop(period_col, axis=1)
                
                # Market totals calculation
                market_totals = combined_df[
                    (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (combined_df["Tanggal"] <= pd.to_datetime(date_to))
                ].copy()
                market_totals[period_col] = market_totals["Tanggal"].dt.to_period(period_freq)
                
                market_melted = market_totals[market_totals["Broker"] == "Total Market"].melt(
                    id_vars=[period_col, "Broker"],
                    value_vars=selected_fields,
                    var_name="Field",
                    value_name="MarketTotal"
                )
                market_aggregated = market_melted.groupby([period_col, "Field"])["MarketTotal"].sum().reset_index()
                market_aggregated["Tanggal"] = market_aggregated[period_col].dt.end_time.dt.normalize()
                market_aggregated = market_aggregated.drop(period_col, axis=1)
                
                display_df = pd.merge(aggregated_df, market_aggregated, on=["Tanggal", "Field"], how="left")
                
                display_df["Percentage"] = display_df.apply(
                    lambda row: (row["Value"] / row["MarketTotal"] * 100) if pd.notna(row["MarketTotal"]) and row["MarketTotal"] != 0 and row["Broker"] != "Total Market"
                    else (100.0 if row["Broker"] == "Total Market" else 0.0), axis=1)

            # Prepare table data
            display_df_for_table = display_df[["Tanggal", "Broker", "Field"]].copy()
            display_df_for_table["Tanggal Display"] = display_df["Tanggal"].dt.strftime(
                '%-d %b %Y' if display_mode == "Daily" else '%b %Y' if display_mode == "Monthly" else '%Y'
            )
            
            display_df_for_table["Sort_Priority"] = display_df_for_table["Broker"].apply(
                lambda x: 0 if x == "Total Market" else 1
            )
            
            display_df_for_table = display_df_for_table.sort_values(
                ["Tanggal", "Sort_Priority", "Broker"], ascending=[False, True, True]
            ).drop("Sort_Priority", axis=1).reset_index(drop=True)

            # Create main table with proper data types
            main_table_df = pd.merge(display_df_for_table, display_df[["Tanggal", "Broker", "Field", "Value", "Percentage"]], 
                                   on=["Tanggal", "Broker", "Field"], how="left")
            
            main_table_df["No"] = range(1, len(main_table_df) + 1)
            main_table_df = main_table_df[["No", "Tanggal", "Broker", "Field", "Value", "Percentage"]]
            
            # Display table with BigInt safe conversion
            grid_options_main = configure_aggrid_table(main_table_df, "main")
            
            col1, col2, col3 = st.columns([0.1, 0.8, 0.1])
            with col2:
                AgGrid(
                    aggrid_safe(main_table_df),
                    gridOptions=grid_options_main,
                    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                    update_mode=GridUpdateMode.MODEL_CHANGED,
                    fit_columns_on_grid_load=True,
                    enable_enterprise_modules=True,
                    height=500,
                    width='100%',
                    reload_data=False
                )

            # Download functionality
            to_download = display_df_for_table.copy()
            to_download["Value"] = display_df["Value"].apply(lambda x: f"{x:,.0f}")
            to_download["Percentage"] = display_df["Percentage"].apply(lambda x: f"{x:.2f}%")
            to_download = to_download[["Tanggal", "Broker", "Field", "Value", "Percentage"]]
            to_download.columns = ["Tanggal", "Broker", "Field", "Value", "%"]
            csv = to_download.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Unduh Tabel CSV", data=csv, file_name="broker_summary.csv", mime="text/csv")

            # Charts
            tab1, tab2 = st.tabs(["📈 Nilai", "📊 Kontribusi Terhadap Total (%)"])

            def format_hover_value(value):
                if value >= 1_000_000_000_000:
                    return f"{value / 1_000_000_000_000:.4f}T"
                elif value >= 1_000_000_000:
                    return f"{value / 1_000_000_000:.4f}B"
                else:
                    return f"{value:,.0f}"

            broker_colors = ['#1f77b4', '#ff7f0e', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#aec7e8', '#ffbb78']

            with tab1:
                for field in selected_fields:
                    chart_data = display_df[display_df["Field"] == field].copy()
                    if chart_data.empty:
                        continue
                    
                    chart_data = chart_data.sort_values("Tanggal")
                    
                    fig = px.line(
                        chart_data,
                        x="Tanggal",
                        y="Value",
                        color="Broker",
                        title=f"{field} dari waktu ke waktu",
                        markers=True,
                        color_discrete_sequence=broker_colors
                    )
                    
                    # Enhance hover information
                    for i, trace in enumerate(fig.data):
                        broker_name = trace.name
                        broker_data = chart_data[chart_data["Broker"] == broker_name]
                        if not broker_data.empty:
                            hover_texts = [f"<b>{broker_name}</b><br>Tanggal: {date}<br>{field}: {format_hover_value(value)}" 
                                          for date, value in zip(broker_data["Tanggal"].dt.strftime('%Y-%m-%d'), 
                                                               broker_data["Value"])]
                            trace.update(
                                marker=dict(size=6),
                                hovertemplate="%{text}<extra></extra>",
                                text=hover_texts,
                                legendgroup=broker_name
                            )
                    
                    fig.update_layout(
                        xaxis_title="Tanggal",
                        yaxis_title=field,
                        hovermode="closest"
                    )
                    st.plotly_chart(fig, use_container_width=True)

            with tab2:
                for field in selected_fields:
                    chart_data = display_df[display_df["Field"] == field].copy()
                    if chart_data.empty:
                        continue
                        
                    chart_data = chart_data.sort_values("Tanggal")
                    
                    fig = px.line(
                        chart_data,
                        x="Tanggal",
                        y="Percentage",
                        color="Broker",
                        title=f"Kontribusi {field} (%) dari waktu ke waktu",
                        markers=True,
                        color_discrete_sequence=broker_colors
                    )
                    
                    for i, trace in enumerate(fig.data):
                        broker_name = trace.name
                        trace.update(
                            marker=dict(size=6),
                            hovertemplate=f"<b>{broker_name}</b><br>Tanggal: %{{x}}<br>Kontribusi: %{{y:.2f}}%<extra></extra>",
                            legendgroup=broker_name
                        )
                    
                    fig.update_layout(
                        xaxis_title="Tanggal",
                        yaxis_title="Percentage (%)",
                        hovermode="closest"
                    )
                    st.plotly_chart(fig, use_container_width=True)

# Ranking section
if not combined_df.empty:
    st.markdown("---")
    st.header("🏆 Top Broker Ranking")

    combined_df["Tanggal"] = pd.to_datetime(combined_df["Tanggal"])
    mode = st.radio("📅 Mode Tanggal untuk Ranking", ["Harian", "Bulanan"], horizontal=True)

    if mode == "Harian":
        min_rank_date = datetime(datetime.today().year, 1, 1).date()
        max_rank_date = combined_df["Tanggal"].max().date()

        rank_date_range = st.date_input(
            "Pilih Rentang Tanggal untuk Ranking",
            value=(min_rank_date, max_rank_date),
            min_value=min_rank_date,
            max_value=max_rank_date,
            help="Klik sekali untuk tanggal mulai, klik kedua untuk tanggal selesai",
            key="rank_date_range"
        )
        if isinstance(rank_date_range, tuple) and len(rank_date_range) == 2:
            rank_date_from, rank_date_to = rank_date_range
            filtered_rank_df = combined_df[
                (combined_df["Tanggal"] >= pd.to_datetime(rank_date_from)) &
                (combined_df["Tanggal"] <= pd.to_datetime(rank_date_to)) &
                (combined_df["Broker"] != "Total Market")
            ].copy()
        else:
            filtered_rank_df = pd.DataFrame()

    else:  # Bulanan
        combined_df["MonthPeriod"] = combined_df["Tanggal"].dt.to_period("M")
        all_months = sorted(combined_df["MonthPeriod"].unique())
        all_years = sorted(set(m.year for m in all_months))
        current_year = datetime.today().year

        selected_years = st.multiselect(
            "📅 Pilih Tahun",
            options=all_years,
            default=[current_year],
            key="rank_year_select"
        )

        month_options = [m for m in all_months if m.year in selected_years]
        selected_months = st.multiselect(
            "📆 Pilih Bulan (bisa lebih dari satu)",
            options=month_options,
            default=month_options,
            format_func=lambda m: m.strftime("%b %Y"),
            key="selected_months"
        )

        if selected_months:
            filtered_rank_df = combined_df[
                combined_df["MonthPeriod"].isin(selected_months) &
                (combined_df["Broker"] != "Total Market")
            ].copy()
        else:
            filtered_rank_df = pd.DataFrame()

    def generate_ranking_table(df: pd.DataFrame, column: str):
        """Generate ranking table with error handling"""
        if df.empty:
            return pd.DataFrame(), 0
        
        ranked_df = (
            df.groupby("Broker")[column].sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        
        if ranked_df.empty:
            return pd.DataFrame(), 0
        
        ranked_df["Peringkat"] = range(1, len(ranked_df) + 1)
        total = ranked_df[column].sum()
        
        # Calculate market share percentage
        if total > 0:
            ranked_df["Market Share"] = (ranked_df[column] / total * 100)
        else:
            ranked_df["Market Share"] = 0.0
        
        # Reorder columns
        ranked_df = ranked_df[["Peringkat", "Broker", column, "Market Share"]]
        
        return ranked_df, total

    if not filtered_rank_df.empty:
        tab_val, tab_freq, tab_vol = st.tabs(["💰 Berdasarkan Nilai", "📈 Berdasarkan Frekuensi", "📊 Berdasarkan Volume"])

        ranking_configs = [
            (tab_val, "Nilai", "🔝 Peringkat Berdasarkan Nilai", "Rp"),
            (tab_freq, "Frekuensi", "🔝 Peringkat Berdasarkan Frekuensi", ""),
            (tab_vol, "Volume", "🔝 Peringkat Berdasarkan Volume", "")
        ]

        for tab, column, title, unit_prefix in ranking_configs:
            with tab:
                st.subheader(title)
                df_ranked, total = generate_ranking_table(filtered_rank_df, column)
                
                if not df_ranked.empty:
                    grid_options = configure_aggrid_table(df_ranked, "ranking")
                    
                    col1, col2, col3 = st.columns([0.1, 0.8, 0.1])
                    with col2:
                        AgGrid(
                            aggrid_safe(df_ranked),
                            gridOptions=grid_options,
                            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                            update_mode=GridUpdateMode.MODEL_CHANGED,
                            fit_columns_on_grid_load=True,
                            enable_enterprise_modules=True,
                            height=400,
                            width='100%',
                            reload_data=False
                        )
                    
                    unit_text = f" {column.lower()}" if column in ["Frekuensi"] else " lot" if column == "Volume" else ""
                    st.markdown(f"**Total {column} Seluruh Broker:** {unit_prefix} {total:,.0f}{unit_text}")
                else:
                    st.warning(f"❗ Tidak ada data untuk ranking {column.lower()}.")
                    
    elif mode == "Harian":
        st.info("📌 Silakan pilih kedua tanggal (mulai dan selesai) untuk melihat data ranking.")
    else:
        st.info("📌 Tidak ada data untuk rentang tanggal yang dipilih.")

else:
    st.info("⬆️ Silakan unggah file Excel terlebih dahulu.")
