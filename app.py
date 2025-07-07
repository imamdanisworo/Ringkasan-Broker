import streamlit as st
import pandas as pd
import re
import os
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
import uuid

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
        # Clear the cache to reload data with new files
        st.cache_data.clear()
        st.rerun()

@st.cache_data(ttl=3600, show_spinner=False)  # Cache for 1 hour, disable spinner
def load_all_excel():
    """Highly optimized loading function with concurrent processing"""
    import concurrent.futures
    from threading import Lock
    
    all_files = api.list_repo_files(REPO_ID, repo_type="dataset")
    xlsx_files = [f for f in all_files if f.endswith(".xlsx")]

    if not xlsx_files:
        return pd.DataFrame(), 0, []

    # Sort files by date (newest first) for better user experience
    xlsx_files.sort(reverse=True)

    all_data = []
    failed_files = []
    lock = Lock()

    # Create progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    total_files = len(xlsx_files)
    processed_count = 0

    def process_file(file):
        """Process a single Excel file"""
        try:
            # Extract date from filename
            match = re.search(r"(\d{8})", file)
            file_date = datetime.strptime(match.group(1), "%Y%m%d").date() if match else None

            # Use cached download with optimized settings
            path = hf_hub_download(
                REPO_ID, 
                filename=file, 
                repo_type="dataset", 
                token=HF_TOKEN,
                local_dir="./hf_cache",
                force_download=False  # Use cache when available
            )

            # Read Excel file with optimized settings
            df = pd.read_excel(
                path, 
                sheet_name="Sheet1",
                engine='openpyxl',  # Use openpyxl for better performance
                dtype={
                    'Kode Perusahaan': 'string',
                    'Nama Perusahaan': 'string',
                    'Volume': 'int64',
                    'Nilai': 'int64',
                    'Frekuensi': 'int64'
                }
            )
            df.columns = df.columns.str.strip()

            # Add metadata efficiently
            df["Tanggal"] = file_date
            df["Kode Perusahaan"] = df["Kode Perusahaan"].astype(str).str.strip()
            df["Nama Perusahaan"] = df["Nama Perusahaan"].astype(str).str.strip()

            return df, None

        except Exception as e:
            return None, file

    # Process files concurrently with thread pool
    max_workers = min(8, len(xlsx_files))  # Limit concurrent downloads
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(process_file, file): file for file in xlsx_files}
        
        # Process completed tasks
        for future in concurrent.futures.as_completed(future_to_file):
            with lock:
                processed_count += 1
                progress = processed_count / total_files
                progress_bar.progress(progress)
                status_text.text(f"📁 Loading {processed_count} of {total_files} files...")
            
            df, failed_file = future.result()
            
            if df is not None:
                with lock:
                    all_data.append(df)
            else:
                with lock:
                    failed_files.append(failed_file)

    # Retry failed files once (sequential for reliability)
    if failed_files:
        status_text.text(f"🔄 Retrying {len(failed_files)} failed files...")
        retry_failed = []

        for i, file in enumerate(failed_files):
            progress = (total_files - len(failed_files) + i + 1) / total_files
            progress_bar.progress(progress)
            status_text.text(f"🔄 Retrying {i + 1} of {len(failed_files)} failed files...")
            
            df, failed_file = process_file(file)
            if df is not None:
                all_data.append(df)
            else:
                retry_failed.append(failed_file)

        failed_files = retry_failed

    # Clear progress indicators
    progress_bar.empty()
    status_text.empty()

    if not all_data:
        st.error("❌ No files could be loaded successfully.")
        return pd.DataFrame(), len(xlsx_files), failed_files

    # Combine all data efficiently with optimized concat
    combined = pd.concat(all_data, ignore_index=True, sort=False)

    # Process broker names efficiently using vectorized operations
    latest_names = (
        combined.sort_values("Tanggal")
        .drop_duplicates("Kode Perusahaan", keep="last")
        .set_index("Kode Perusahaan")["Nama Perusahaan"]
    )
    combined["Broker"] = combined["Kode Perusahaan"].map(latest_names).fillna('') 
    combined["Broker"] = combined["Kode Perusahaan"] + "_" + combined["Broker"]

    # Add Total Market efficiently with single groupby operation
    market_aggregated = combined.groupby("Tanggal", as_index=False)[["Volume", "Nilai", "Frekuensi"]].sum()
    market_aggregated["Broker"] = "Total Market"
    market_aggregated["FieldSource"] = "Generated"
    market_aggregated["Kode Perusahaan"] = "TOTAL"
    market_aggregated["Nama Perusahaan"] = "Total Market"
    
    combined = pd.concat([combined, market_aggregated], ignore_index=True, sort=False)

    return combined, len(xlsx_files), failed_files

# Initialize session state for file loading status
if "file_load_status" not in st.session_state:
    st.session_state.file_load_status = {"success": 0, "total": 0, "failed": []}

with st.spinner(None):  # Disable default spinner
    try:
        combined_df, file_count, failed_files = load_all_excel()
        st.session_state.file_load_status["total"] = file_count
        st.session_state.file_load_status["success"] = file_count - len(failed_files)
        st.session_state.file_load_status["failed"] = failed_files
    except Exception as e:
        combined_df = pd.DataFrame()
        st.warning(f"⚠️ Gagal memuat data: {e}")

# Display permanent file loading status
success_count = st.session_state.file_load_status["success"]
total_count = st.session_state.file_load_status["total"]
failed_files = st.session_state.file_load_status["failed"]

if total_count > 0:
    st.markdown("---")
    st.markdown("##### 📁 Status Pemuatan File:")
    st.markdown(f"✅ Berhasil dimuat: {success_count} dari {total_count} file")
    if failed_files:
        st.warning(f"⚠️ Gagal memuat file: {', '.join(failed_files)}")

st.button("🔁 Refresh Data", on_click=lambda: (st.cache_data.clear(), st.rerun()))

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
            col1, col2 = st.columns(2)
            with col1:
                date_from = st.date_input("Dari Tanggal", min_value=min_date, max_value=max_date, value=year_start)
            with col2:
                date_to = st.date_input("Sampai Tanggal", min_value=min_date, max_value=max_date, value=max_date)
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

            # ✅ FIX: Use Total Market values as denominator for percentage calculation
            # Get Total Market values for each date and field
            total_market_df = combined_df[combined_df["Broker"] == "Total Market"].melt(
                id_vars=["Tanggal", "Broker"],
                value_vars=selected_fields,
                var_name="Field",
                value_name="TotalMarketValue"
            )
            total_market_df = total_market_df[["Tanggal", "Field", "TotalMarketValue"]]

            merged_df = pd.merge(melted_df, total_market_df, on=["Tanggal", "Field"], how="left")
            # Calculate percentage: (Broker Value / Total Market Value) × 100
            merged_df["Percentage"] = merged_df.apply(
                lambda row: (row["Value"] / row["TotalMarketValue"] * 100) if pd.notna(row["TotalMarketValue"]) and row["TotalMarketValue"] != 0 
                else 0.0, axis=1)

            display_df = merged_df.copy()

            if display_mode == "Monthly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("M").dt.to_timestamp()
                # Aggregate values first
                monthly_df = display_df.groupby(["Tanggal", "Broker", "Field"])["Value"].sum().reset_index()
                
                # Get the complete market totals from the original data (not just selected brokers)
                monthly_market_totals = combined_df[
                    (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (combined_df["Tanggal"] <= pd.to_datetime(date_to))
                ].copy()
                monthly_market_totals["Tanggal"] = monthly_market_totals["Tanggal"].dt.to_period("M").dt.to_timestamp()
                
                # Get Total Market values for each month/field
                monthly_market_melted = monthly_market_totals[monthly_market_totals["Broker"] == "Total Market"].melt(
                    id_vars=["Tanggal", "Broker"],
                    value_vars=selected_fields,
                    var_name="Field",
                    value_name="MarketTotal"
                )
                monthly_market_aggregated = monthly_market_melted.groupby(["Tanggal", "Field"])["MarketTotal"].sum().reset_index()
                
                display_df = pd.merge(monthly_df, monthly_market_aggregated, on=["Tanggal", "Field"], how="left")
                
                # Calculate percentage: (Broker Value / Total Market Value) × 100
                display_df["Percentage"] = display_df.apply(
                    lambda row: (row["Value"] / row["MarketTotal"] * 100) if pd.notna(row["MarketTotal"]) and row["MarketTotal"] != 0 and row["Broker"] != "Total Market"
                    else (100.0 if row["Broker"] == "Total Market" else 0.0), axis=1)
                
                display_df = display_df[
                    (display_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (display_df["Tanggal"] <= pd.to_datetime(date_to))
                ]
            elif display_mode == "Yearly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                # Aggregate values first
                yearly_df = display_df.groupby(["Tanggal", "Broker", "Field"])["Value"].sum().reset_index()
                
                # Get the complete market totals from the original data (not just selected brokers)
                yearly_market_totals = combined_df[
                    (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (combined_df["Tanggal"] <= pd.to_datetime(date_to))
                ].copy()
                yearly_market_totals["Tanggal"] = yearly_market_totals["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                
                # Get Total Market values for each year/field
                yearly_market_melted = yearly_market_totals[yearly_market_totals["Broker"] == "Total Market"].melt(
                    id_vars=["Tanggal", "Broker"],
                    value_vars=selected_fields,
                    var_name="Field",
                    value_name="MarketTotal"
                )
                yearly_market_aggregated = yearly_market_melted.groupby(["Tanggal", "Field"])["MarketTotal"].sum().reset_index()
                
                display_df = pd.merge(yearly_df, yearly_market_aggregated, on=["Tanggal", "Field"], how="left")
                
                # Calculate percentage: (Broker Value / Total Market Value) × 100
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
            display_df_for_table = display_df_for_table.sort_values("Tanggal")

            st.dataframe(
                display_df_for_table[["Tanggal Display", "Broker", "Field", "Formatted Value", "Formatted %"]]
                .rename(columns={"Tanggal Display": "Tanggal"})
            )

            to_download = display_df_for_table[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]].copy()
            to_download.columns = ["Tanggal", "Broker", "Field", "Value", "%"]
            csv = to_download.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Unduh Tabel CSV", data=csv, file_name="broker_summary.csv", mime="text/csv")

            tab1, tab2 = st.tabs(["📈 Nilai", "📊 Kontribusi Terhadap Total (%)"])

            with tab1:
                for field in selected_fields:
                    # Use the same processed data that's shown in the table
                    chart_data = display_df[display_df["Field"] == field].copy()
                    
                    # Sort by date to ensure proper line connections
                    chart_data = chart_data.sort_values("Tanggal")
                    
                    # Create the base line chart with custom color palette (avoiding red/green)
                    custom_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
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
                    
                    # Function to format values for hover display
                    def format_hover_value(value):
                        if value >= 1_000_000_000_000:  # Trillion
                            return f"{value / 1_000_000_000_000:.4f}T"
                        elif value >= 1_000_000_000:  # Billion
                            return f"{value / 1_000_000_000:.4f}B"
                        else:
                            return f"{value:,.0f}"
                    
                    # Update all traces with proper hover (let Plotly handle colors automatically)
                    for i, trace in enumerate(fig.data):
                        broker_name = trace.name
                        # Create custom hover text for each point
                        hover_texts = [f"<b>{broker_name}</b><br>Tanggal: {date}<br>{field}: {format_hover_value(value)}" 
                                      for date, value in zip(chart_data[chart_data["Broker"] == broker_name]["Tanggal"].dt.strftime('%Y-%m-%d'), 
                                                           chart_data[chart_data["Broker"] == broker_name]["Value"])]
                        trace.update(
                            marker=dict(size=6),
                            hovertemplate="%{text}<extra></extra>",
                            text=hover_texts
                        )
                    
                    # Add color coding for min/max values for each broker
                    for broker in chart_data["Broker"].unique():
                        broker_data = chart_data[chart_data["Broker"] == broker].copy()
                        if len(broker_data) > 1:  # Only add min/max if there's more than one point
                            min_idx = broker_data["Value"].idxmin()
                            max_idx = broker_data["Value"].idxmax()
                            
                            min_date = broker_data.loc[min_idx, "Tanggal"]
                            min_value = broker_data.loc[min_idx, "Value"]
                            max_date = broker_data.loc[max_idx, "Tanggal"]
                            max_value = broker_data.loc[max_idx, "Value"]
                            
                            # Add red dot for minimum value
                            min_formatted = format_hover_value(min_value)
                            fig.add_scatter(
                                x=[min_date],
                                y=[min_value],
                                mode="markers",
                                marker=dict(color="red", size=6, symbol="circle"),
                                name=f"{broker} (Min)",
                                showlegend=False,
                                hovertemplate=f"<b>{broker}</b><br>Tanggal: {min_date.strftime('%Y-%m-%d')}<br>Nilai Terendah: {min_formatted}<extra></extra>"
                            )
                            
                            # Add green dot for maximum value
                            max_formatted = format_hover_value(max_value)
                            fig.add_scatter(
                                x=[max_date],
                                y=[max_value],
                                mode="markers",
                                marker=dict(color="green", size=6, symbol="circle"),
                                name=f"{broker} (Max)",
                                showlegend=False,
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
                    # Use the same processed data that's shown in the table
                    chart_data = display_df[display_df["Field"] == field].copy()
                    
                    # Sort by date to ensure proper line connections
                    chart_data = chart_data.sort_values("Tanggal")
                    
                    # Create the base line chart with custom color palette (avoiding red/green)
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
                    
                    # Update all traces with proper hover (let Plotly handle colors automatically)
                    for i, trace in enumerate(fig.data):
                        broker_name = trace.name
                        trace.update(
                            marker=dict(size=6),
                            hovertemplate=f"<b>{broker_name}</b><br>Tanggal: %{{x}}<br>Kontribusi: %{{y:.2f}}%<extra></extra>"
                        )
                    
                    # Add color coding for min/max values for each broker
                    for broker in chart_data["Broker"].unique():
                        broker_data = chart_data[chart_data["Broker"] == broker].copy()
                        if len(broker_data) > 1:  # Only add min/max if there's more than one point
                            min_idx = broker_data["Percentage"].idxmin()
                            max_idx = broker_data["Percentage"].idxmax()
                            
                            min_date = broker_data.loc[min_idx, "Tanggal"]
                            min_percentage = broker_data.loc[min_idx, "Percentage"]
                            max_date = broker_data.loc[max_idx, "Tanggal"]
                            max_percentage = broker_data.loc[max_idx, "Percentage"]
                            
                            # Add red dot for minimum percentage
                            fig.add_scatter(
                                x=[min_date],
                                y=[min_percentage],
                                mode="markers",
                                marker=dict(color="red", size=6, symbol="circle"),
                                name=f"{broker} (Min %)",
                                showlegend=False,
                                hovertemplate=f"<b>{broker}</b><br>Tanggal: {min_date.strftime('%Y-%m-%d')}<br>Kontribusi Terendah: {min_percentage:.2f}%<extra></extra>"
                            )
                            
                            # Add green dot for maximum percentage
                            fig.add_scatter(
                                x=[max_date],
                                y=[max_percentage],
                                mode="markers",
                                marker=dict(color="green", size=6, symbol="circle"),
                                name=f"{broker} (Max %)",
                                showlegend=False,
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

        col1, col2 = st.columns(2)
        with col1:
            rank_date_from = st.date_input("Dari Tanggal", value=min_rank_date, min_value=min_rank_date, max_value=max_rank_date, key="rank_date_from")
        with col2:
            rank_date_to = st.date_input("Sampai Tanggal", value=max_rank_date, min_value=min_rank_date, max_value=max_rank_date, key="rank_date_to")

        filtered_rank_df = combined_df[
            (combined_df["Tanggal"] >= pd.to_datetime(rank_date_from)) &
            (combined_df["Tanggal"] <= pd.to_datetime(rank_date_to)) &
            (combined_df["Broker"] != "Total Market")
        ]

    else:  # Bulanan
        # Extract month periods and unique years
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

        # Filter available months based on selected years
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
        ranked_df.index += 1
        ranked_df.reset_index(inplace=True)
        ranked_df.columns = ["Peringkat", "Broker", column]
        total = ranked_df[column].sum()

        # Convert to string to prevent Arrow serialization issues
        ranked_df["Peringkat"] = ranked_df["Peringkat"].astype(str)
        ranked_df[column] = ranked_df[column].apply(lambda x: f"{x:,.0f}")

        total_row = pd.DataFrame([{
            "Peringkat": "TOTAL",
            "Broker": "TOTAL",
            column: f"{total:,.0f}"
        }])

        ranked_df = pd.concat([ranked_df, total_row], ignore_index=True)

        return ranked_df, total

    if not filtered_rank_df.empty:
        tab_val, tab_freq, tab_vol = st.tabs(["💰 Berdasarkan Nilai", "📈 Berdasarkan Frekuensi", "📊 Berdasarkan Volume"])

        with tab_val:
            st.subheader("🔝 Peringkat Berdasarkan Nilai")
            df_val, total_val = generate_full_table(filtered_rank_df, "Nilai")
            st.dataframe(df_val, use_container_width=True)
            st.markdown(f"**Total Nilai Seluruh Broker:** Rp {total_val:,.0f}")

        with tab_freq:
            st.subheader("🔝 Peringkat Berdasarkan Frekuensi")
            df_freq, total_freq = generate_full_table(filtered_rank_df, "Frekuensi")
            st.dataframe(df_freq, use_container_width=True)
            st.markdown(f"**Total Frekuensi Seluruh Broker:** {total_freq:,.0f} transaksi")

        with tab_vol:
            st.subheader("🔝 Peringkat Berdasarkan Volume")
            df_vol, total_vol = generate_full_table(filtered_rank_df, "Volume")
            st.dataframe(df_vol, use_container_width=True)
            st.markdown(f"**Total Volume Seluruh Broker:** {total_vol:,.0f} lot")
    else:
        st.info("📌 Tidak ada data untuk rentang tanggal yang dipilih.")

else:
    st.info("⬆️ Silakan unggah file Excel terlebih dahulu.")
