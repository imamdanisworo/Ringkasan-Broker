import streamlit as st
import pandas as pd
import re
from datetime import datetime
import plotly.express as px
import requests
from bs4 import BeautifulSoup

st.set_page_config(page_title="📊 Ringkasan Broker", layout="wide")

st.markdown("<h1 style='text-align:center;'>📊 Ringkasan Aktivitas Broker Saham</h1>", unsafe_allow_html=True)
st.markdown("### 📂 Pilih File Excel dari Google Drive Folder (otomatis)")

# ========== LINK FOLDER GOOGLE DRIVE ==========
FOLDER_ID = "17gDaKfBzTCLGQkGdsUFZ-CXayGjtYlvD?usp=sharing"

@st.cache_data
def list_excel_files_in_folder(folder_id):
    url = f"https://drive.google.com/embeddedfolderview?id={folder_id}#list"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    links = soup.find_all('a')
    file_dict = {}
    for link in links:
        href = link.get('href')
        if href and href.startswith("https://drive.google.com/file/d/") and href.endswith("/view"):
            file_id = href.split("/d/")[1].split("/")[0]
            file_name = link.text.strip()
            if file_name.endswith(".xlsx"):
                direct_url = f"https://drive.google.com/uc?id={file_id}&export=download"
                file_dict[file_name] = direct_url
    return file_dict

@st.cache_data
def load_excel_from_url(url):
    try:
        df = pd.read_excel(url, sheet_name="Sheet1")
        df.columns = df.columns.str.strip()
        df["Tanggal"] = pd.to_datetime(df["Tanggal"])
        df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
        return df
    except Exception as e:
        st.error(f"❌ Gagal memuat file: {e}")
        return pd.DataFrame()

excel_links = list_excel_files_in_folder(FOLDER_ID)

if excel_links:
    selected_file = st.selectbox("📁 Pilih File:", list(excel_links.keys()))
    df = load_excel_from_url(excel_links[selected_file])

    if not df.empty:
        st.markdown("### 🎛️ Filter & Tampilan Data")

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            selected_brokers = st.multiselect("🏢 Pilih Broker", sorted(df["Broker"].unique()))
        with col2:
            selected_fields = st.multiselect("📈 Pilih Metode", ["Volume", "Nilai", "Frekuensi"])
        with col3:
            min_date, max_date = df["Tanggal"].min().date(), df["Tanggal"].max().date()
            display_mode = st.selectbox("🗓️ Mode Tampilan", ["Daily", "Monthly", "Yearly"])

            if display_mode == "Daily":
                date_from = st.date_input("Dari", min_value=min_date, max_value=max_date, value=min_date)
                date_to = st.date_input("Sampai", min_value=min_date, max_value=max_date, value=max_date)
            elif display_mode == "Monthly":
                months = sorted(df["Tanggal"].dt.to_period("M").unique())
                selected_months = st.multiselect("🗓️ Bulan", months, default=[months[0]])
                date_from = min(m.to_timestamp() for m in selected_months)
                date_to = max((m + 1).to_timestamp() - pd.Timedelta(days=1) for m in selected_months)
            elif display_mode == "Yearly":
                years = sorted(df["Tanggal"].dt.year.unique())
                selected_years = st.multiselect("📆 Tahun", years, default=[years[0]])
                date_from = datetime(min(selected_years), 1, 1).date()
                date_to = datetime(max(selected_years), 12, 31).date()

        if selected_brokers and selected_fields:
            filtered_df = df[
                (df["Tanggal"] >= pd.to_datetime(date_from)) &
                (df["Tanggal"] <= pd.to_datetime(date_to)) &
                (df["Broker"].isin(selected_brokers))
            ]

            if not filtered_df.empty:
                melted_df = filtered_df.melt(
                    id_vars=["Tanggal", "Broker"],
                    value_vars=selected_fields,
                    var_name="Field",
                    value_name="Value"
                )

                total_all_df = df.melt(id_vars=["Tanggal", "Broker"], value_vars=selected_fields, var_name="Field", value_name="Value")
                total_all_df = total_all_df.groupby(["Tanggal", "Field"])["Value"].sum().reset_index()
                total_all_df.rename(columns={"Value": "TotalValue"}, inplace=True)

                merged_df = pd.merge(melted_df, total_all_df, on=["Tanggal", "Field"])
                merged_df["Percentage"] = merged_df.apply(
                    lambda row: (row["Value"] / row["TotalValue"] * 100) if row["TotalValue"] != 0 else 0,
                    axis=1
                )

                display_df = merged_df.copy()

                if display_mode == "Monthly":
                    display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("M").dt.to_timestamp()
                    display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                elif display_mode == "Yearly":
                    display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                    display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()

                display_df["Tanggal"] = display_df["Tanggal"].dt.strftime('%d-%b-%y' if display_mode == "Daily" else '%b-%y' if display_mode == "Monthly" else '%Y')
                display_df["Formatted Value"] = display_df["Value"].apply(lambda x: f"{x:,.0f}")
                display_df["Formatted %"] = display_df["Percentage"].apply(lambda x: f"{x:.2f}%")
                display_df = display_df.sort_values(["Tanggal", "Broker", "Field"])

                st.markdown("### 📋 Tabel Ringkasan")
                st.dataframe(display_df[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]], use_container_width=True)

                st.markdown("---")
                st.markdown("### 📊 Grafik Nilai Asli")

                def format_value_short(val):
                    if val >= 1_000_000_000_000:
                        return f"{val / 1_000_000_000_000:.1f} T"
                    elif val >= 1_000_000_000:
                        return f"{val / 1_000_000_000:.1f} B"
                    elif val >= 1_000_000:
                        return f"{val / 1_000_000:.1f} M"
                    elif val >= 1_000:
                        return f"{val / 1_000:.1f} K"
                    return f"{val:.0f}"

                for field in selected_fields:
                    chart_data = merged_df[merged_df["Field"] == field].dropna()

                    if display_mode == "Monthly":
                        chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("M").dt.to_timestamp()
                        chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                    elif display_mode == "Yearly":
                        chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                        chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()

                    chart_data["ValueShort"] = chart_data["Value"].apply(format_value_short)

                    fig = px.line(
                        chart_data,
                        x="Tanggal",
                        y="Value",
                        color="Broker",
                        title=f"{field} over Time",
                        markers=True,
                        hover_data={"ValueShort": True, "Broker": True, "Tanggal": True}
                    )

                    fig.update_traces(
                        hovertemplate="<b>%{x|%d %b %Y}</b><br>Broker: %{customdata[1]}<br>Value: %{customdata[0]}"
                    )

                    fig.update_layout(
                        yaxis_title=field,
                        xaxis_title="Tanggal",
                        xaxis_tickformat='%d %b %Y',
                        xaxis=dict(tickmode='array', tickvals=chart_data['Tanggal'].unique())
                    )

                    st.plotly_chart(fig, use_container_width=True)

                st.markdown("---")
                st.markdown("""### 📈 Grafik Kontribusi (%)
#### ℹ️ Menunjukkan kontribusi broker terhadap total nilai yang diperdagangkan di BEI pada hari tersebut.
""")

                for field in selected_fields:
                    chart_data = merged_df[merged_df["Field"] == field].dropna()

                    if display_mode == "Monthly":
                        chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("M").dt.to_timestamp()
                        chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                    elif display_mode == "Yearly":
                        chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                        chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()

                    fig = px.line(
                        chart_data,
                        x="Tanggal",
                        y="Percentage",
                        color="Broker",
                        title=f"{field} Contribution (%) Over Time",
                        markers=True
                    )
                    fig.update_layout(
                        yaxis_title="Persentase (%)",
                        xaxis_title="Tanggal",
                        xaxis_tickformat='%d %b %Y',
                        xaxis=dict(tickmode='array', tickvals=chart_data['Tanggal'].unique())
                    )
                    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("⚠️ Tidak ada data ditemukan. Pastikan folder Google Drive dapat diakses publik dan berisi file .xlsx.")
