import streamlit as st
import pandas as pd
import re
from datetime import datetime
import plotly.express as px
from huggingface_hub import HfApi, hf_hub_download, upload_file
import uuid

st.set_page_config(page_title="Ringkasan Broker", layout="wide")
st.title("📊 Ringkasan Aktivitas Broker Saham")

REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]

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
        st.rerun()

@st.cache_data
def load_all_excel():
    all_files = api.list_repo_files(REPO_ID, repo_type="dataset")
    xlsx_files = [f for f in all_files if f.endswith(".xlsx")]
    all_data = []

    for file in xlsx_files:
        try:
            match = re.search(r"(\d{8})", file)
            file_date = datetime.strptime(match.group(1), "%Y%m%d").date() if match else None

            path = hf_hub_download(REPO_ID, filename=file, repo_type="dataset", token=HF_TOKEN)
            df = pd.read_excel(path, sheet_name="Sheet1")
            df.columns = df.columns.str.strip()
            df["Tanggal"] = file_date
            df["Kode Perusahaan"] = df["Kode Perusahaan"].astype(str).str.strip()
            df["Nama Perusahaan"] = df["Nama Perusahaan"].astype(str).str.strip()
            all_data.append(df)
        except Exception:
            continue

    combined = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    if not combined.empty:
        latest_names = (
            combined.sort_values("Tanggal")
            .drop_duplicates("Kode Perusahaan", keep="last")
            .set_index("Kode Perusahaan")["Nama Perusahaan"]
        )
        combined["Broker"] = combined["Kode Perusahaan"].apply(
            lambda kode: f"{kode}_{latest_names.get(kode, '')}"
        )

        # ✅ ADD TOTAL MARKET CORRECTLY
        total_market = (
            combined.groupby("Tanggal")[["Volume", "Nilai", "Frekuensi"]]
            .sum()
            .reset_index()
            .assign(
                Kode_Perusahaan="TOTAL",
                Nama_Perusahaan="Total Market",
                Broker="Total Market"
            )
        )
        combined = pd.concat([combined, total_market[combined.columns]], ignore_index=True)

    return combined, len(xlsx_files)

try:
    combined_df, file_count = load_all_excel()
    st.info(f"📂 {file_count} file Excel dimuat dari repositori.")
except Exception as e:
    combined_df = pd.DataFrame()
    st.warning(f"⚠️ Gagal memuat data: {e}")

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
            selected_fields = st.multiselect("📊 Pilih Jenis Data", ["Volume", "Nilai", "Frekuensi"])

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
            total_df = combined_df.melt(id_vars=["Tanggal", "Broker"], value_vars=selected_fields,
                                        var_name="Field", value_name="Value")
            total_df = total_df.groupby(["Tanggal", "Field"])["Value"].sum().reset_index()
            total_df.rename(columns={"Value": "TotalValue"}, inplace=True)

            merged_df = pd.merge(melted_df, total_df, on=["Tanggal", "Field"])
            merged_df["Percentage"] = merged_df.apply(
                lambda row: (row["Value"] / row["TotalValue"] * 100) if row["TotalValue"] != 0 else 0, axis=1)

            display_df = merged_df.copy()

            if display_mode == "Monthly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("M").dt.to_timestamp()
                display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                display_df = display_df[
                    (display_df["Tanggal"] >= pd.to_datetime(date_from)) &
                    (display_df["Tanggal"] <= pd.to_datetime(date_to))
                ]
            elif display_mode == "Yearly":
                display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()
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
                    chart_data = display_df[display_df["Field"] == field].copy()
                    fig = px.line(
                        chart_data,
                        x="Tanggal",
                        y="Value",
                        color="Broker",
                        title=f"{field} dari waktu ke waktu",
                        markers=True
                    )
                    fig.update_layout(
                        yaxis_tickformat=".2s",
                        xaxis_title="Tanggal",
                        hovermode="x unified"
                    )
                    st.plotly_chart(fig, use_container_width=True)

            with tab2:
                for field in selected_fields:
                    chart_data = display_df[display_df["Field"] == field].copy()
                    fig = px.line(
                        chart_data,
                        x="Tanggal",
                        y="Percentage",
                        color="Broker",
                        title=f"Kontribusi {field} (%) dari waktu ke waktu",
                        markers=True
                    )
                    fig.update_layout(
                        xaxis_title="Tanggal",
                        hovermode="x unified"
                    )
                    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("⬆️ Silakan unggah file Excel terlebih dahulu.")
