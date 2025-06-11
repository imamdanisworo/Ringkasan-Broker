import streamlit as st
import pandas as pd
import re
from datetime import datetime
import plotly.express as px
import os
import io
from huggingface_hub import HfApi, hf_hub_download, upload_file

st.set_page_config(page_title="Financial Broker Summary", layout="wide")
st.title("📊 Ringkasan Broker")

# === CONFIG ===
REPO_ID = "imamdanisworo/broker-storage"
HF_TOKEN = st.secrets["HF_TOKEN"]

# === Upload all .xlsx from local folder ===
def upload_all_excels():
    folder_path = "."
    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(folder_path, filename)
            try:
                upload_file(
                    path_or_fileobj=file_path,
                    path_in_repo=filename,
                    repo_id=REPO_ID,
                    repo_type="dataset",
                    token=HF_TOKEN
                )
                st.success(f"Uploaded: {filename}")
            except Exception as e:
                st.error(f"Failed to upload {filename}: {e}")

# === Load previously uploaded Excel files from HF ===
@st.cache_data
def load_excel_files_from_hf():
    api = HfApi()
    files = api.list_repo_files(REPO_ID, repo_type="dataset")
    xlsx_files = [f for f in files if f.endswith(".xlsx")]
    all_data = []

    for file in xlsx_files:
        try:
            file_path = hf_hub_download(repo_id=REPO_ID, filename=file, repo_type="dataset", token=HF_TOKEN)
            match = re.search(r'(\d{8})', file)
            if match:
                file_date = datetime.strptime(match.group(1), "%Y%m%d").date()
            else:
                file_date = datetime.today().date()

            df = pd.read_excel(file_path, sheet_name="Sheet1")
            df.columns = df.columns.str.strip()

            required_cols = {"Kode Perusahaan", "Nama Perusahaan", "Volume", "Nilai", "Frekuensi"}
            if not required_cols.issubset(set(df.columns)):
                continue

            df["Tanggal"] = file_date
            df["Broker"] = df["Kode Perusahaan"] + " / " + df["Nama Perusahaan"]
            all_data.append(df)

        except Exception as e:
            st.warning(f"Failed to load {file} from HF: {e}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

# === Upload newly uploaded Excel files to HF ===
def upload_to_hf(file):
    from pathlib import Path
    clean_name = re.sub(r"\s*\(\d+\)", "", Path(file.name).stem) + ".xlsx"
    try:
        upload_file(
            path_or_fileobj=file,
            path_in_repo=clean_name,
            repo_id=REPO_ID,
            repo_type="dataset",
            token=HF_TOKEN
        )
        st.success(f"Uploaded {clean_name} to Hugging Face (overwriting if existed)")
    except Exception as e:
        st.error(f"Upload failed: {e}")

# === Optional: Button to re-upload all local .xlsx files ===


# === Handle File Uploads ===
uploaded_files = st.file_uploader("Upload Multiple Excel Files (Sheet1 expected)", type=["xlsx"], accept_multiple_files=True)

if uploaded_files:
    for file in uploaded_files:
        upload_to_hf(file)

# === Combine uploaded + previously stored data ===
combined_df = load_excel_files_from_hf()
if not combined_df.empty:
    combined_df["Tanggal"] = pd.to_datetime(combined_df["Tanggal"])

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        selected_brokers = st.multiselect("Select Broker(s)", sorted(combined_df["Broker"].unique()))
    with col2:
        selected_fields = st.multiselect("Select Fields", ["Volume", "Nilai", "Frekuensi"])
    with col3:
        min_date, max_date = combined_df["Tanggal"].min().date(), combined_df["Tanggal"].max().date()
        display_mode = st.selectbox("Display Mode", ["Daily", "Monthly", "Yearly"])

        if display_mode == "Daily":
            st.markdown("**Select Date Range**")
            date_from = st.date_input("From", min_value=min_date, max_value=max_date, value=min_date)
            date_to = st.date_input("To", min_value=min_date, max_value=max_date, value=max_date)
        elif display_mode == "Monthly":
            st.markdown("**Select Month**")
            months = sorted(combined_df["Tanggal"].dt.to_period("M").unique())
            selected_month = st.selectbox("Month", months)
            date_from = selected_month.to_timestamp()
            date_to = (selected_month + 1).to_timestamp() - pd.Timedelta(days=1)
        elif display_mode == "Yearly":
            st.markdown("**Select Year**")
            years = sorted(combined_df["Tanggal"].dt.year.unique())
            selected_year = st.selectbox("Year", years)
            date_from = datetime(selected_year, 1, 1).date()
            date_to = datetime(selected_year, 12, 31).date()

    if selected_brokers and selected_fields and date_from and date_to:
        if date_from > date_to:
            st.warning("⚠️ 'From' date must be before or equal to 'To' date.")
        else:
            filtered_df = combined_df[
                (combined_df["Tanggal"] >= pd.to_datetime(date_from)) &
                (combined_df["Tanggal"] <= pd.to_datetime(date_to)) &
                (combined_df["Broker"].isin(selected_brokers))
            ]

            if filtered_df.empty:
                st.info("No data matches the selected filters.")
            else:
                melted_df = filtered_df.melt(
                    id_vars=["Tanggal", "Broker"],
                    value_vars=selected_fields,
                    var_name="Field",
                    value_name="Value"
                )

                display_df = merged_df.copy()

                if display_mode == "Monthly":
                    display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("M").dt.to_timestamp()
                    display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                elif display_mode == "Yearly":
                    display_df["Tanggal"] = display_df["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                    display_df = display_df.groupby(["Tanggal", "Broker", "Field"]).agg({"Value": "sum", "Percentage": "mean"}).reset_index()

                display_df["Formatted Value"] = display_df["Value"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "")
                display_df["Formatted %"] = display_df["Percentage"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")

                

                if display_mode == "Monthly":
    display_df["Tanggal"] = display_df["Tanggal"].dt.strftime('%b-%y')
elif display_mode == "Yearly":
    display_df["Tanggal"] = display_df["Tanggal"].dt.strftime('%Y')
else:
    display_df["Tanggal"] = display_df["Tanggal"].dt.strftime('%d-%b-%y')

display_df = display_df.sort_values(["Tanggal", "Broker", "Field"])

                # Calculate total per day and field for percentage
                total_df = combined_df.melt(id_vars=["Tanggal", "Broker"], value_vars=selected_fields, var_name="Field", value_name="Value")
                total_df = total_df.groupby(["Tanggal", "Field"])["Value"].sum().reset_index()
                total_df.rename(columns={"Value": "TotalValue"}, inplace=True)
                merged_df = pd.merge(melted_df, total_df, on=["Tanggal", "Field"])
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

                display_df["Formatted Value"] = display_df["Value"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "")
                display_df["Formatted %"] = display_df["Percentage"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
                display_df["Tanggal"] = display_df["Tanggal"].dt.strftime('%d-%b-%y')
                display_df = display_df.sort_values(["Tanggal", "Broker", "Field"])

                st.dataframe(display_df[["Tanggal", "Broker", "Field", "Formatted Value", "Formatted %"]], use_container_width=True)

                st.markdown("---")
                st.subheader("📈 Chart - Original Values")

                for field in selected_fields:
                    chart_data = merged_df[merged_df["Field"] == field].dropna()

                    if display_mode == "Monthly":
                        chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("M").dt.to_timestamp()
                        chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                    elif display_mode == "Yearly":
                        chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                        chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()

                    if display_mode == "Monthly":
                        chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("M").dt.to_timestamp()
                        chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                    elif display_mode == "Yearly":
                        chart_data["Tanggal"] = chart_data["Tanggal"].dt.to_period("Y").dt.to_timestamp()
                        chart_data = chart_data.groupby(["Tanggal", "Broker"])[["Value", "Percentage"]].agg({"Value": "sum", "Percentage": "mean"}).reset_index()
                    if not chart_data.empty:
                        fig = px.line(
                            chart_data,
                            x="Tanggal",
                            y="Value",
                            color="Broker",
                            title=f"{field} over Time",
                            markers=True
                        )
                        fig.update_layout(
                            yaxis_title=field,
                            xaxis_title="Tanggal",
                            xaxis_tickformat='%d %b %Y',
                            xaxis=dict(tickmode='array', tickvals=chart_data['Tanggal'].unique())
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info(f"No data to chart for {field}.")

                st.markdown("---")
                st.subheader("💸 Chart - Percentage Contribution (%)")

                for field in selected_fields:
                    chart_data = merged_df[merged_df["Field"] == field].dropna()
                    if not chart_data.empty:
                        fig = px.line(
                            chart_data,
                            x="Tanggal",
                            y="Percentage",
                            color="Broker",
                            title=f"{field} Contribution (%) Over Time",
                            markers=True
                        )
                        fig.update_layout(yaxis_title="Percentage (%)", xaxis_title="Tanggal", xaxis_tickformat='%d %b %Y', xaxis=dict(tickmode='array', tickvals=chart_data['Tanggal'].unique()))
                        st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No previously saved data found. Please upload Excel files.")
