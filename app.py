
import streamlit as st
import pandas as pd
import re
import os
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
import uuid
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

st.set_page_config(page_title="Ringkasan Broker", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = os.getenv("HF_TOKEN")

if "upload_key" not in st.session_state:
    st.session_state.upload_key = str(uuid.uuid4())
if "reset_upload_key" in st.session_state and st.session_state.reset_upload_key:
    st.session_state.upload_key = str(uuid.uuid4())
    st.session_state.reset_upload_key = False

api = HfApi()

@st.cache_data
def list_existing_files():
    return set(api.list_repo_files(REPO_ID, repo_type="dataset"))

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
            file_date = datetime.strptime(match.group(1), "%Y%m%d").date()

            df = pd.read_excel(file, sheet_name="Sheet1")
            df.columns = df.columns.str.strip()
            required_columns = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
            if not required_columns.issubset(df.columns):
                st.warning(f"⚠️ {file.name} dilewati: kolom tidak lengkap.")
                continue

            if file.name in existing_files:
                st.warning(f"⚠️ File '{file.name}' sudah ada dan akan ditimpa.")

            upload_file(
                path_or_fileobj=file,
                path_in_repo=file.name,
                repo_id=REPO_ID,
                repo_type="dataset",
                token=HF_TOKEN
            )
            st.success(f"✅ Berhasil diunggah: {file.name}")
            upload_success = True

        except Exception as e:
            st.error(f"❌ Gagal memproses {file.name}: {e}")

    if upload_success:
        st.session_state.reset_upload_key = True
        st.cache_data.clear()
        st.rerun()

@st.cache_data(ttl=3600, show_spinner=False)
def load_all_excel():
    """Ultra-optimized loading function with enhanced performance"""
    import concurrent.futures
    from threading import Lock
    import os
    
    all_files = api.list_repo_files(REPO_ID, repo_type="dataset")
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
        """Process a single Excel file with optimizations"""
        try:
            match = re.search(r"(\d{8})", file)
            file_date = datetime.strptime(match.group(1), "%Y%m%d").date() if match else None

            # Check if file exists in cache first
            cache_path = f"./hf_cache/{file}"
            if not os.path.exists(cache_path):
                path = hf_hub_download(
                    REPO_ID, 
                    filename=file, 
                    repo_type="dataset", 
                    token=HF_TOKEN,
                    local_dir="./hf_cache",
                    force_download=False
                )
            else:
                path = cache_path

            # Optimized Excel reading with minimal processing
            df = pd.read_excel(
                path, 
                sheet_name="Sheet1",
                engine='openpyxl',
                dtype={
                    'Kode Perusahaan': 'string',
                    'Nama Perusahaan': 'string',
                    'Volume': 'int64',
                    'Nilai': 'int64',
                    'Frekuensi': 'int64'
                },
                usecols=['Kode Perusahaan', 'Nama Perusahaan', 'Volume', 'Nilai', 'Frekuensi']
            )
            
            # Minimal processing - defer heavy operations
            df.columns = df.columns.str.strip()
            df["Tanggal"] = file_date
            df["Kode Perusahaan"] = df["Kode Perusahaan"].astype(str).str.strip()
            df["Nama Perusahaan"] = df["Nama Perusahaan"].astype(str).str.strip()

            return df, None

        except Exception as e:
            return None, file

    # Increased worker count for better throughput
    max_workers = min(16, len(xlsx_files), os.cpu_count() * 2)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks at once for better scheduling
        futures = [executor.submit(process_file, file) for file in xlsx_files]
        
        for future in concurrent.futures.as_completed(futures):
            with lock:
                processed_count += 1
                progress = processed_count / total_files
                progress_bar.progress(progress)
                status_text.text(f"📁 Loading {processed_count}/{total_files} files...")
            
            df, failed_file = future.result()
            
            if df is not None:
                with lock:
                    all_data.append(df)
            else:
                with lock:
                    failed_files.append(failed_file)

    # Skip retry for failed files to speed up loading
    # Users can refresh if needed

    progress_bar.empty()
    status_text.empty()

    if not all_data:
        st.error("❌ No files could be loaded successfully.")
        return pd.DataFrame(), len(xlsx_files), failed_files

    # Optimized concatenation
    combined = pd.concat(all_data, ignore_index=True, sort=False, copy=False)

    # Optimized broker name mapping
    latest_names = (
        combined.sort_values("Tanggal")
        .drop_duplicates("Kode Perusahaan", keep="last")
        .set_index("Kode Perusahaan")["Nama Perusahaan"]
    )
    combined["Broker"] = combined["Kode Perusahaan"].map(latest_names).fillna('') 
    combined["Broker"] = combined["Kode Perusahaan"] + "_" + combined["Broker"]

    # Optimized market aggregation
    market_aggregated = combined.groupby("Tanggal", as_index=False)[["Volume", "Nilai", "Frekuensi"]].sum()
    market_aggregated["Broker"] = "Total Market"
    market_aggregated["FieldSource"] = "Generated"
    market_aggregated["Kode Perusahaan"] = "TOTAL"
    market_aggregated["Nama Perusahaan"] = "Total Market"
    
    combined = pd.concat([combined, market_aggregated], ignore_index=True, sort=False, copy=False)

    return combined, len(xlsx_files), failed_files

# Initialize session state for file loading status
if "file_load_status" not in st.session_state:
    st.session_state.file_load_status = {"success": 0, "total": 0, "failed": []}

with st.spinner(None):
    try:
        combined_df, file_count, failed_files = load_all_excel()
        st.session_state.file_load_status["total"] = file_count
        st.session_state.file_load_status["success"] = file_count - len(failed_files)
        st.session_state.file_load_status["failed"] = failed_files
    except Exception as e:
        combined_df = pd.DataFrame()
        st.warning(f"⚠️ Gagal memuat data: {e}")

# Display file loading status
success_count = st.session_state.file_load_status["success"]
total_count = st.session_state.file_load_status["total"]
failed_files = st.session_state.file_load_status["failed"]

if total_count > 0:
    st.markdown("---")
    st.markdown("##### 📁 Status Pemuatan File:")
    st.markdown(f"✅ Berhasil dimuat: {success_count} dari {total_count} file")
    if failed_files:
        st.warning(f"⚠️ Gagal memuat file: {', '.join(failed_files)}")

if "refresh_trigger" not in st.session_state:
    st.session_state.refresh_trigger = False

if st.button("🔁 Refresh Data"):
    st.cache_data.clear()
    st.session_state.refresh_trigger = True
    st.rerun()

if st.session_state.refresh_trigger:
    st.session_state.refresh_trigger = False

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
                # Only single date selected, don't show data yet
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
        ]

        if not filtered_df.empty:
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

            if display_mode == "Monthly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("M").dt.to_timestamp()
                monthly_df = display_df.groupby(["Tanggal", "Broker", "Field"])["Value"].sum().reset_index()
                
                monthly_market_totals = combined_df[
                    (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (combined_df["Tanggal"] <= pd.to_datetime(date_to))
                ].copy()
                monthly_market_totals["Tanggal"] = monthly_market_totals["Tanggal"].dt.to_period("M").dt.to_timestamp()
                
                monthly_market_melted = monthly_market_totals[monthly_market_totals["Broker"] == "Total Market"].melt(
                    id_vars=["Tanggal", "Broker"],
                    value_vars=selected_fields,
                    var_name="Field",
                    value_name="MarketTotal"
                )
                monthly_market_aggregated = monthly_market_melted.groupby(["Tanggal", "Field"])["MarketTotal"].sum().reset_index()
                
                display_df = pd.merge(monthly_df, monthly_market_aggregated, on=["Tanggal", "Field"], how="left")
                
                display_df["Percentage"] = display_df.apply(
                    lambda row: (row["Value"] / row["MarketTotal"] * 100) if pd.notna(row["MarketTotal"]) and row["MarketTotal"] != 0 and row["Broker"] != "Total Market"
                    else (100.0 if row["Broker"] == "Total Market" else 0.0), axis=1)
                
                display_df = display_df[
                    (display_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (display_df["Tanggal"] <= pd.to_datetime(date_to))
                ]
            elif display_mode == "Yearly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                yearly_df = display_df.groupby(["Tanggal", "Broker", "Field"])["Value"].sum().reset_index()
                
                yearly_market_totals = combined_df[
                    (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (combined_df["Tanggal"] <= pd.to_datetime(date_to))
                ].copy()
                yearly_market_totals["Tanggal"] = yearly_market_totals["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                
                yearly_market_melted = yearly_market_totals[yearly_market_totals["Broker"] == "Total Market"].melt(
                    id_vars=["Tanggal", "Broker"],
                    value_vars=selected_fields,
                    var_name="Field",
                    value_name="MarketTotal"
                )
                yearly_market_aggregated = yearly_market_melted.groupby(["Tanggal", "Field"])["MarketTotal"].sum().reset_index()
                
                display_df = pd.merge(yearly_df, yearly_market_aggregated, on=["Tanggal", "Field"], how="left")
                
                display_df["Percentage"] = display_df.apply(
                    lambda row: (row["Value"] / row["MarketTotal"] * 100) if pd.notna(row["MarketTotal"]) and row["MarketTotal"] != 0 and row["Broker"] != "Total Market"
                    else (100.0 if row["Broker"] == "Total Market" else 0.0), axis=1)
                
                display_df = display_df[
                    (display_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (display_df["Tanggal"] <= pd.to_datetime(date_to))
                ]

            display_df["Formatted Value"] = display_df["Value"].apply(lambda x: f"{x:,.0f}")
            display_df["Formatted %"] = display_df["Percentage"].apply(lambda x: f"{x:.2f}%")

            display_df_for_table = display_df[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]].copy()
            display_df_for_table["Tanggal Display"] = display_df["Tanggal"].dt.strftime(
                '%-d %b %Y' if display_mode == "Daily" else '%b %Y' if display_mode == "Monthly" else '%Y'
            )
            
            display_df_for_table["Sort_Priority"] = display_df_for_table["Broker"].apply(
                lambda x: 0 if x == "Total Market" else 1
            )
            
            display_df_for_table = display_df_for_table.sort_values(
                ["Tanggal", "Sort_Priority", "Broker"]
            )
            
            display_df_for_table = display_df_for_table.drop("Sort_Priority", axis=1)
            display_df_for_table = display_df_for_table.reset_index(drop=True)
            display_df_for_table.index = display_df_for_table.index + 1

            # Add row numbering and prepare main table data with raw numeric values for sorting
            main_table_df = display_df_for_table[["Tanggal", "Tanggal Display", "Broker", "Field", "Formatted Value", "Formatted %"]].copy()
            main_table_df["No"] = range(1, len(main_table_df) + 1)
            
            # Add raw numeric values from original display_df for proper sorting
            main_table_df = pd.merge(main_table_df, display_df[["Tanggal", "Broker", "Field", "Value", "Percentage"]], 
                                   on=["Tanggal", "Broker", "Field"], how="left")
            
            main_table_df = main_table_df[["No", "Tanggal", "Broker", "Field", "Value", "Percentage", "Formatted Value", "Formatted %"]]
            
            # Configure AgGrid for main summary table
            gb_main = GridOptionsBuilder.from_dataframe(main_table_df)
            gb_main.configure_pagination(enabled=False)
            gb_main.configure_default_column(groupable=False, value=True, enableRowGroup=False, aggFunc="sum", editable=False, resizable=True)
            gb_main.configure_grid_options(domLayout='normal', suppressHorizontalScroll=False)
            gb_main.configure_column("No", width=70, pinned="left", type=["numericColumn"])
            gb_main.configure_column("Tanggal", width=120, pinned="left", 
                                   valueFormatter="new Date(value).toLocaleDateString('id-ID', {day: 'numeric', month: 'short', year: 'numeric'})")
            gb_main.configure_column("Broker", width=250, pinned="left")
            gb_main.configure_column("Field", width=100)
            gb_main.configure_column("Value", width=180, type=["numericColumn"], 
                                   valueFormatter="'Rp ' + Math.floor(value).toLocaleString()", headerName="Nilai")
            gb_main.configure_column("Percentage", width=120, type=["numericColumn"],
                                   valueFormatter="value.toFixed(2) + '%'", headerName="Market Share (%)")
            # Hide the formatted columns since we're showing formatted versions of numeric columns
            gb_main.configure_column("Formatted Value", hide=True)
            gb_main.configure_column("Formatted %", hide=True)
            
            grid_options_main = gb_main.build()
            
            AgGrid(
                main_table_df,
                gridOptions=grid_options_main,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                fit_columns_on_grid_load=True,
                enable_enterprise_modules=True,
                height=400,
                width='100%',
                reload_data=False
            )

            to_download = display_df_for_table[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]].copy()
            to_download.columns = ["Tanggal", "Broker", "Field", "Value", "%"]
            csv = to_download.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Unduh Tabel CSV", data=csv, file_name="broker_summary.csv", mime="text/csv")

            tab1, tab2 = st.tabs(["📈 Nilai", "📊 Kontribusi Terhadap Total (%)"])

            def format_hover_value(value):
                if value >= 1_000_000_000_000:
                    return f"{value / 1_000_000_000_000:.4f}T"
                elif value >= 1_000_000_000:
                    return f"{value / 1_000_000_000:.4f}B"
                else:
                    return f"{value:,.0f}"

            with tab1:
                for field in selected_fields:
                    chart_data = display_df[display_df["Field"] == field].copy()
                    chart_data = chart_data.sort_values("Tanggal")
                    
                    broker_colors = ['#1f77b4', '#ff7f0e', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#aec7e8', '#ffbb78']
                    
                    fig = px.line(
                        chart_data,
                        x="Tanggal",
                        y="Value",
                        color="Broker",
                        title=f"{field} dari waktu ke waktu",
                        markers=True,
                        color_discrete_sequence=broker_colors
                    )
                    
                    for i, trace in enumerate(fig.data):
                        broker_name = trace.name
                        hover_texts = [f"<b>{broker_name}</b><br>Tanggal: {date}<br>{field}: {format_hover_value(value)}" 
                                      for date, value in zip(chart_data[chart_data["Broker"] == broker_name]["Tanggal"].dt.strftime('%Y-%m-%d'), 
                                                           chart_data[chart_data["Broker"] == broker_name]["Value"])]
                        trace.update(
                            marker=dict(size=6),
                            hovertemplate="%{text}<extra></extra>",
                            text=hover_texts,
                            legendgroup=broker_name
                        )
                    
                    for broker in chart_data["Broker"].unique():
                        broker_data = chart_data[chart_data["Broker"] == broker].copy()
                        if len(broker_data) > 1:
                            min_idx = broker_data["Value"].idxmin()
                            max_idx = broker_data["Value"].idxmax()
                            
                            min_date = broker_data.loc[min_idx, "Tanggal"]
                            min_value = broker_data.loc[min_idx, "Value"]
                            max_date = broker_data.loc[max_idx, "Tanggal"]
                            max_value = broker_data.loc[max_idx, "Value"]
                            
                            min_formatted = format_hover_value(min_value)
                            fig.add_scatter(
                                x=[min_date],
                                y=[min_value],
                                mode="markers",
                                marker=dict(color="red", size=6, symbol="circle"),
                                name=f"{broker} (Min)",
                                showlegend=False,
                                legendgroup=broker,
                                hovertemplate=f"<b>{broker}</b><br>Tanggal: {min_date.strftime('%Y-%m-%d')}<br>Nilai Terendah: {min_formatted}<extra></extra>"
                            )
                            
                            max_formatted = format_hover_value(max_value)
                            fig.add_scatter(
                                x=[max_date],
                                y=[max_value],
                                mode="markers",
                                marker=dict(color="green", size=6, symbol="circle"),
                                name=f"{broker} (Max)",
                                showlegend=False,
                                legendgroup=broker,
                                hovertemplate=f"<b>{broker}</b><br>Tanggal: {max_date.strftime('%Y-%m-%d')}<br>Nilai Tertinggi: {max_formatted}<extra></extra>"
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
                    chart_data = chart_data.sort_values("Tanggal")
                    
                    broker_colors = ['#1f77b4', '#ff7f0e', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#aec7e8', '#ffbb78']
                    
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
                    
                    for broker in chart_data["Broker"].unique():
                        broker_data = chart_data[chart_data["Broker"] == broker].copy()
                        if len(broker_data) > 1:
                            min_idx = broker_data["Percentage"].idxmin()
                            max_idx = broker_data["Percentage"].idxmax()
                            
                            min_date = broker_data.loc[min_idx, "Tanggal"]
                            min_percentage = broker_data.loc[min_idx, "Percentage"]
                            max_date = broker_data.loc[max_idx, "Tanggal"]
                            max_percentage = broker_data.loc[max_idx, "Percentage"]
                            
                            fig.add_scatter(
                                x=[min_date],
                                y=[min_percentage],
                                mode="markers",
                                marker=dict(color="red", size=6, symbol="circle"),
                                name=f"{broker} (Min %)",
                                showlegend=False,
                                legendgroup=broker,
                                hovertemplate=f"<b>{broker}</b><br>Tanggal: {min_date.strftime('%Y-%m-%d')}<br>Kontribusi Terendah: {min_percentage:.2f}%<extra></extra>"
                            )
                            
                            fig.add_scatter(
                                x=[max_date],
                                y=[max_percentage],
                                mode="markers",
                                marker=dict(color="green", size=6, symbol="circle"),
                                name=f"{broker} (Max %)",
                                showlegend=False,
                                legendgroup=broker,
                                hovertemplate=f"<b>{broker}</b><br>Tanggal: {max_date.strftime('%Y-%m-%d')}<br>Kontribusi Tertinggi: {max_percentage:.2f}%<extra></extra>"
                            )
                    
                    fig.update_layout(
                        xaxis_title="Tanggal",
                        yaxis_title="Percentage (%)",
                        hovermode="closest"
                    )
                    st.plotly_chart(fig, use_container_width=True)

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
        else:
            # Only single date selected, don't show data yet
            rank_date_from = rank_date_to = None

        if rank_date_from is not None and rank_date_to is not None:
            filtered_rank_df = combined_df[
                (combined_df["Tanggal"] >= pd.to_datetime(rank_date_from)) &
                (combined_df["Tanggal"] <= pd.to_datetime(rank_date_to)) &
                (combined_df["Broker"] != "Total Market")
            ]
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
            ]
        else:
            filtered_rank_df = pd.DataFrame()

    def generate_full_table(df: pd.DataFrame, column: str):
        ranked_df = (
            df.groupby("Broker")[column].sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        
        ranked_df["Peringkat"] = [str(i) for i in range(1, len(ranked_df) + 1)]
        total = ranked_df[column].sum()
        
        # Calculate market share percentage as numeric values
        ranked_df["Market Share"] = (ranked_df[column] / total * 100)
        
        # Keep original numeric values for proper sorting
        ranked_df = ranked_df[["Peringkat", "Broker", column, "Market Share"]]

        return ranked_df, total

    if not filtered_rank_df.empty:
        tab_val, tab_freq, tab_vol = st.tabs(["💰 Berdasarkan Nilai", "📈 Berdasarkan Frekuensi", "📊 Berdasarkan Volume"])

        with tab_val:
            st.subheader("🔝 Peringkat Berdasarkan Nilai")
            df_val, total_val = generate_full_table(filtered_rank_df, "Nilai")
            
            # Configure AgGrid for ranking table with proper numeric sorting
            gb_val = GridOptionsBuilder.from_dataframe(df_val)
            gb_val.configure_pagination(enabled=False)
            gb_val.configure_default_column(groupable=False, value=True, enableRowGroup=False, editable=False, resizable=True)
            gb_val.configure_grid_options(domLayout='normal', suppressHorizontalScroll=False)
            gb_val.configure_column("Peringkat", width=80, pinned="left", type=["numericColumn"])
            gb_val.configure_column("Broker", width=300, pinned="left")
            gb_val.configure_column("Nilai", width=200, type=["numericColumn"], 
                                  valueFormatter="'Rp ' + Math.floor(value).toLocaleString()")
            gb_val.configure_column("Market Share", width=150, type=["numericColumn"],
                                  valueFormatter="value.toFixed(2) + '%'")
            
            grid_options_val = gb_val.build()
            
            AgGrid(
                df_val,
                gridOptions=grid_options_val,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                fit_columns_on_grid_load=True,
                enable_enterprise_modules=True,
                height=400,
                width='100%',
                reload_data=False
            )
            st.markdown(f"**Total Nilai Seluruh Broker:** Rp {total_val:,.0f}")

        with tab_freq:
            st.subheader("🔝 Peringkat Berdasarkan Frekuensi")
            df_freq, total_freq = generate_full_table(filtered_rank_df, "Frekuensi")
            
            # Configure AgGrid for ranking table with proper numeric sorting
            gb_freq = GridOptionsBuilder.from_dataframe(df_freq)
            gb_freq.configure_pagination(enabled=False)
            gb_freq.configure_default_column(groupable=False, value=True, enableRowGroup=False, editable=False, resizable=True, flex=1)
            gb_freq.configure_grid_options(domLayout='normal', suppressHorizontalScroll=False)
            gb_freq.configure_column("Peringkat", width=80, pinned="left", type=["numericColumn"], flex=0)
            gb_freq.configure_column("Broker", minWidth=250, pinned="left", flex=3)
            gb_freq.configure_column("Frekuensi", minWidth=150, type=["numericColumn"],
                                   valueFormatter="Math.floor(value).toLocaleString()", flex=2)
            gb_freq.configure_column("Market Share", minWidth=120, type=["numericColumn"],
                                   valueFormatter="value.toFixed(2) + '%'", flex=1)
            
            grid_options_freq = gb_freq.build()
            
            AgGrid(
                df_freq,
                gridOptions=grid_options_freq,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                fit_columns_on_grid_load=True,
                enable_enterprise_modules=True,
                height=400,
                width='100%',
                reload_data=False
            )
            st.markdown(f"**Total Frekuensi Seluruh Broker:** {total_freq:,.0f} transaksi")

        with tab_vol:
            st.subheader("🔝 Peringkat Berdasarkan Volume")
            df_vol, total_vol = generate_full_table(filtered_rank_df, "Volume")
            
            # Configure AgGrid for ranking table with proper numeric sorting
            gb_vol = GridOptionsBuilder.from_dataframe(df_vol)
            gb_vol.configure_pagination(enabled=False)
            gb_vol.configure_default_column(groupable=False, value=True, enableRowGroup=False, editable=False, resizable=True, flex=1)
            gb_vol.configure_grid_options(domLayout='normal', suppressHorizontalScroll=False)
            gb_vol.configure_column("Peringkat", width=80, pinned="left", type=["numericColumn"], flex=0)
            gb_vol.configure_column("Broker", minWidth=250, pinned="left", flex=3)
            gb_vol.configure_column("Volume", minWidth=150, type=["numericColumn"],
                                  valueFormatter="Math.floor(value).toLocaleString()", flex=2)
            gb_vol.configure_column("Market Share", minWidth=120, type=["numericColumn"],
                                  valueFormatter="value.toFixed(2) + '%'", flex=1)
            
            grid_options_vol = gb_vol.build()
            
            AgGrid(
                df_vol,
                gridOptions=grid_options_vol,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                fit_columns_on_grid_load=True,
                enable_enterprise_modules=True,
                height=400,
                width='100%',
                reload_data=False
            )
            st.markdown(f"**Total Volume Seluruh Broker:** {total_vol:,.0f} lot")
    elif mode == "Harian" and (rank_date_from is None or rank_date_to is None):
        st.info("📌 Silakan pilih kedua tanggal (mulai dan selesai) untuk melihat data ranking.")
    else:
        st.info("📌 Tidak ada data untuk rentang tanggal yang dipilih.")

else:
    st.info("⬆️ Silakan unggah file Excel terlebih dahulu.")
